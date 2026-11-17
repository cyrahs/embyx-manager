//! The service's tests, ported from `tests/test_fill_actor_service.py`,
//! `tests/test_fill_actor_recovery.py` and `tests/test_cloud_move_service.py`.
//!
//! The Python service tests run on the in-memory repository unless they pass one; that one is
//! not ported (D110), so every test here runs on a PostgreSQL database of its own. Python's
//! monkeypatched `_rename_no_replace`, `os.link`, `os.rename` and `_apply_one` become faults
//! and hooks of the recording file system ([`crate::fs::recording`]).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use chrono::{DateTime, NaiveDate, TimeZone as _, Utc};
use embyx_db::database::Database;
use embyx_db::testdb::TestDatabase;

use super::{
    Clock, FillActorPaths, FillActorRuntime, FillActorService, ServiceOptions, ServiceParts, TokenFactory,
    static_runtime,
};
use crate::cloud_moves::BoxFuture;
use crate::fs::recording::RecordingMoveFs;
use crate::ports::{
    AcquisitionError, AcquisitionGateway, AcquisitionOutcome, ActorCatalog, BrandResolver, CatalogError,
    CatalogListing, PageProgressCallback,
};
use crate::postgres_repository::PostgresFillActorRepository;

mod cloud;
mod recovery;
mod scenarios;
mod service;

/// A database of its own, and a fresh repository on it for each "restart"
/// (`make_postgres_repository()`).
pub(super) struct Db {
    test: TestDatabase,
    databases: Mutex<Vec<Database>>,
}

impl Db {
    pub(super) async fn new() -> Self {
        Self {
            test: TestDatabase::create().await,
            databases: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn repository(&self) -> Arc<PostgresFillActorRepository> {
        let database = self.test.database();
        self.databases
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(database.clone());
        Arc::new(PostgresFillActorRepository::new(database))
    }

    pub(super) async fn finish(self) {
        let databases = std::mem::take(&mut *self.databases.lock().unwrap_or_else(PoisonError::into_inner));
        for database in &databases {
            database.close().await;
        }
        self.test.drop_database().await;
    }
}

/// `FakeActorCatalog`: a listing or an exception per actor.
pub(super) struct FakeCatalog(pub HashMap<String, Result<Vec<String>, CatalogError>>);

impl FakeCatalog {
    pub(super) fn new(values: &[(&str, &[&str])]) -> Self {
        Self(
            values
                .iter()
                .map(|(actor, videos)| ((*actor).to_owned(), Ok(strings(videos))))
                .collect(),
        )
    }

    pub(super) fn failing(mut self, actor: &str, class: &str, message: &str) -> Self {
        self.0.insert(
            actor.to_owned(),
            Err(CatalogError {
                class: class.to_owned(),
                message: message.to_owned(),
            }),
        );
        self
    }
}

impl ActorCatalog for FakeCatalog {
    fn list_videos<'a>(
        &'a self,
        actor_ref: &'a str,
        _progress_callback: Option<&'a PageProgressCallback>,
    ) -> BoxFuture<'a, Result<CatalogListing, CatalogError>> {
        let value = self.0.get(actor_ref).cloned().expect("an actor the test lists");
        Box::pin(async move {
            value.map(|video_ids| CatalogListing {
                video_ids,
                ..CatalogListing::default()
            })
        })
    }
}

/// `FakeAcquisitionGateway`: an outcome or an exception per video, `NO_MAGNET` by default.
#[derive(Default)]
pub(super) struct FakeGateway {
    pub values: HashMap<String, Result<AcquisitionOutcome, String>>,
    pub calls: Mutex<Vec<String>>,
}

impl FakeGateway {
    pub(super) fn new(values: &[(&str, AcquisitionOutcome)]) -> Self {
        Self {
            values: values
                .iter()
                .map(|(video, outcome)| ((*video).to_owned(), Ok(*outcome)))
                .collect(),
            calls: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

impl AcquisitionGateway for FakeGateway {
    fn queue_missing<'a>(
        &'a self,
        video_id: &'a str,
        _release_date: Option<NaiveDate>,
    ) -> BoxFuture<'a, Result<AcquisitionOutcome, AcquisitionError>> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(video_id.to_owned());
        let value = self
            .values
            .get(video_id)
            .cloned()
            .unwrap_or(Ok(AcquisitionOutcome::NoMagnet));
        Box::pin(async move {
            value.map_err(|message| AcquisitionError::Ledger(embyx_monitor::acquisitions::LedgerError::Value(message)))
        })
    }
}

/// `MappingBrandResolver`.
pub(super) struct MappingBrands(pub HashMap<String, Option<String>>);

impl MappingBrands {
    pub(super) fn new(values: &[(&str, Option<&str>)]) -> Self {
        Self(
            values
                .iter()
                .map(|(video, brand)| ((*video).to_owned(), brand.map(str::to_owned)))
                .collect(),
        )
    }

    /// Every video of the brand.
    pub(super) fn all(brand: &str) -> FixedBrand {
        FixedBrand(brand.to_owned())
    }
}

impl BrandResolver for MappingBrands {
    fn resolve_brand(&self, video_id: &str) -> Option<String> {
        self.0.get(video_id).cloned().flatten()
    }
}

/// `BrandResolver` of the recovery and cloud tests: one brand for every video.
pub(super) struct FixedBrand(pub String);

impl BrandResolver for FixedBrand {
    fn resolve_brand(&self, _video_id: &str) -> Option<String> {
        Some(self.0.clone())
    }
}

/// `TokenFactory`: `token-1`, `token-2`, ...
pub(super) fn tokens() -> TokenFactory {
    let counter = Arc::new(AtomicUsize::new(0));
    Arc::new(move || format!("token-{}", counter.fetch_add(1, Ordering::SeqCst) + 1))
}

/// `MutableClock`.
#[derive(Clone)]
pub(super) struct MutableClock(Arc<Mutex<DateTime<Utc>>>);

impl MutableClock {
    pub(super) fn at(year: i32, month: u32, day: u32) -> Self {
        Self(Arc::new(Mutex::new(
            Utc.with_ymd_and_hms(year, month, day, 0, 0, 0).unwrap(),
        )))
    }

    pub(super) fn advance(&self, by: chrono::TimeDelta) {
        let mut value = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        *value += by;
    }

    pub(super) fn clock(&self) -> Clock {
        let value = Arc::clone(&self.0);
        Arc::new(move || *value.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

pub(super) fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

/// The `paths` fixture: `actor`, `additional-one`, `additional-two` and `move-in` under a
/// temporary directory.
pub(super) fn library(root: &Path) -> FillActorPaths {
    let actor = root.join("actor");
    let additional = [root.join("additional-one"), root.join("additional-two")];
    let move_in = root.join("move-in");
    for path in [&actor, &additional[0], &additional[1], &move_in] {
        std::fs::create_dir(path).unwrap();
    }
    FillActorPaths::from_iterable(actor, additional, move_in)
}

/// The arguments of the tests' `make_service`, with their defaults.
pub(super) struct Setup {
    pub runtime: FillActorRuntime,
    pub catalog: Arc<dyn ActorCatalog>,
    pub gateway: Arc<FakeGateway>,
    pub brands: Arc<dyn BrandResolver>,
    pub options: ServiceOptions,
}

impl Setup {
    pub(super) fn new(paths: &FillActorPaths, catalog: FakeCatalog, brands: impl BrandResolver + 'static) -> Self {
        Self {
            runtime: FillActorRuntime {
                paths: Some(paths.clone()),
                ..FillActorRuntime::default()
            },
            catalog: Arc::new(catalog),
            gateway: Arc::new(FakeGateway::default()),
            brands: Arc::new(brands),
            options: ServiceOptions {
                token_factory: Some(tokens()),
                ..ServiceOptions::default()
            },
        }
    }

    pub(super) fn apply_enabled(mut self) -> Self {
        self.runtime.apply_enabled = true;
        self
    }

    pub(super) fn gateway(mut self, gateway: FakeGateway) -> Self {
        self.gateway = Arc::new(gateway);
        self
    }

    pub(super) fn fs(mut self, fs: Arc<RecordingMoveFs>) -> Self {
        self.options.fs = fs;
        self
    }

    pub(super) fn build(self, repository: Arc<PostgresFillActorRepository>) -> FillActorService {
        FillActorService::new(
            ServiceParts {
                runtime: static_runtime(self.runtime),
                actor_catalog: self.catalog,
                acquisition_gateway: self.gateway,
                brand_resolver: self.brands,
                repository,
            },
            self.options,
        )
        .expect("valid options")
    }
}

/// `create_move_candidate(paths, video_id=...)`: `<additional-one>/ABC/<video_id>.mp4`.
pub(super) fn create_move_candidate(paths: &FillActorPaths, video_id: &str) -> PathBuf {
    let brand = paths.additional_brand_paths[0].join("ABC");
    if !brand.is_dir() {
        std::fs::create_dir(&brand).unwrap();
    }
    let source = brand.join(format!("{video_id}.mp4"));
    std::fs::write(&source, b"video").unwrap();
    source
}

pub(super) fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap()
}

/// `threading.Event`.
#[derive(Clone, Default)]
pub(super) struct Event(Arc<(Mutex<bool>, std::sync::Condvar)>);

impl Event {
    pub(super) fn set(&self) {
        let (flag, condvar) = &*self.0;
        *flag.lock().unwrap_or_else(PoisonError::into_inner) = true;
        condvar.notify_all();
    }

    pub(super) fn is_set(&self) -> bool {
        *self.0.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `event.wait(timeout)`: whether it was set in time.
    pub(super) fn wait(&self, seconds: u64) -> bool {
        let (flag, condvar) = &*self.0;
        let guard = flag.lock().unwrap_or_else(PoisonError::into_inner);
        let (guard, _) = condvar
            .wait_timeout_while(guard, std::time::Duration::from_secs(seconds), |set| !*set)
            .unwrap_or_else(PoisonError::into_inner);
        *guard
    }

    /// `await asyncio.to_thread(event.wait, seconds)`.
    pub(super) async fn wait_async(&self, seconds: u64) -> bool {
        let event = self.clone();
        tokio::task::spawn_blocking(move || event.wait(seconds)).await.unwrap()
    }
}

/// The fields of the `tracing` events a test records (the `caplog` fixture).
#[derive(Clone, Default)]
pub(super) struct Captured(Arc<Mutex<Vec<HashMap<String, String>>>>);

impl Captured {
    pub(super) fn events(&self) -> Vec<HashMap<String, String>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// Record the events of this thread until the guard is dropped.
    pub(super) fn install(&self) -> tracing::subscriber::DefaultGuard {
        use tracing_subscriber::layer::SubscriberExt as _;
        tracing::subscriber::set_default(tracing_subscriber::registry().with(self.clone()))
    }
}

struct Fields<'a>(&'a mut HashMap<String, String>);

impl tracing::field::Visit for Fields<'_> {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Captured {
    fn on_event(&self, event: &tracing::Event<'_>, _context: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = HashMap::new();
        fields.insert("level".to_owned(), event.metadata().level().to_string());
        event.record(&mut Fields(&mut fields));
        self.0.lock().unwrap_or_else(PoisonError::into_inner).push(fields);
    }
}
