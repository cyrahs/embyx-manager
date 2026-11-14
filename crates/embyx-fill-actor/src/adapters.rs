//! Client-backed implementations of the fill-actor service ports.
//!
//! Python origin: `src/embyx_manager/adapters.py`. The ports are satisfied with thin adapters
//! over the clients: AVBase and JavBus for the actor catalog, the monitor's acquisition
//! intake for missing videos, the AVID rules for brands, and CloudDrive for the moves. Each
//! adapter reaches its client through a small trait the client implements, so tests can
//! stand in for it as Python's tests do with `SimpleNamespace` and `AsyncMock`.

use std::sync::{Arc, Mutex, PoisonError};

use chrono::NaiveDate;
use embyx_clients::avbase::{AvbaseClient, AvbaseError, AvbaseTalent, AvbaseWork, TalentPages};
use embyx_clients::clouddrive::aio::{EnsureResult, MoveResult};
use embyx_clients::clouddrive::{AsyncCloudDrive, CloudDriveError, CloudFile};
use embyx_clients::javbus::{JavBusActor, JavBusActorPage, JavBusClient, JavBusError, PageProgress};
use embyx_core::pyjson::{Dict, Value};
use embyx_core::pytext::{casefold, py_repr, py_strip};
use embyx_monitor::acquisitions::AcquisitionSource;
use embyx_monitor::intake::{AcquisitionIntake, IntakeError, IntakeOutcome};
use embyx_monitor::reports::RunContext;
use indexmap::{IndexMap, IndexSet};

use crate::cloud_moves::{BoxFuture, CheckError, CloudFileMetadata, CloudFileMover, CloudMoveResponse, MoverError};
use crate::ports::{
    AcquisitionError, AcquisitionGateway, AcquisitionOutcome, ActorCatalog, BrandResolver, CatalogError, CatalogListing,
};

/// The `logging` logger the Python module writes to (`__name__`).
const LOGGER: &str = "embyx_manager.adapters";

/// `NANOSECONDS_PER_SECOND`.
pub const NANOSECONDS_PER_SECOND: i128 = 1_000_000_000;

/// `ActorNotFoundError`'s class name: the error [`UnionActorCatalog`] raises for an actor
/// neither catalog knows.
pub const ACTOR_NOT_FOUND: &str = "ActorNotFoundError";

/// `_STAR_ID_RE`: `^[A-Za-z0-9_-]{1,32}$`. A name that happens to fit is simply also tried
/// as a JavBus star id. (`$` also matches before a final newline, but `fullmatch` is what
/// the module calls, so a trailing newline does not fit.)
fn is_star_id(value: &str) -> bool {
    (1..=32).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

// ---------- the actor catalog ----------

/// One page of an AVBase talent listing: `(page, pages, works)`.
pub type TalentPage = (i128, i128, Vec<AvbaseWork>);

/// The walk over a talent's pages (`async for ... in avbase.talent_pages(name)`).
pub trait TalentPageWalk: Send {
    fn next_page(&mut self) -> BoxFuture<'_, Result<Option<TalentPage>, AvbaseError>>;
}

impl TalentPageWalk for TalentPages<'_> {
    fn next_page(&mut self) -> BoxFuture<'_, Result<Option<TalentPage>, AvbaseError>> {
        Box::pin(self.next())
    }
}

/// What [`UnionActorCatalog`] needs from AVBase.
pub trait AvbaseCatalog: Send + Sync {
    fn talent<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Option<AvbaseTalent>, AvbaseError>>;
    fn talent_pages<'a>(&'a self, name: &'a str) -> Box<dyn TalentPageWalk + 'a>;
}

impl AvbaseCatalog for AvbaseClient {
    fn talent<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Option<AvbaseTalent>, AvbaseError>> {
        Box::pin(AvbaseClient::talent(self, name))
    }

    fn talent_pages<'a>(&'a self, name: &'a str) -> Box<dyn TalentPageWalk + 'a> {
        Box::new(AvbaseClient::talent_pages(self, name))
    }
}

/// What [`UnionActorCatalog`] needs from JavBus.
pub trait JavBusCatalog: Send + Sync {
    fn search_stars<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<JavBusActor>, JavBusError>>;
    fn get_actor<'a>(&'a self, actor_id: &'a str) -> BoxFuture<'a, Result<Option<JavBusActorPage>, JavBusError>>;
    fn scrape<'a>(
        &'a self,
        actor_id: &'a str,
        progress: Option<&'a PageProgress>,
    ) -> BoxFuture<'a, Result<Vec<String>, JavBusError>>;
}

impl JavBusCatalog for JavBusClient {
    fn search_stars<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<JavBusActor>, JavBusError>> {
        Box::pin(JavBusClient::search_stars(self, name))
    }

    fn get_actor<'a>(&'a self, actor_id: &'a str) -> BoxFuture<'a, Result<Option<JavBusActorPage>, JavBusError>> {
        Box::pin(JavBusClient::get_actor(self, actor_id))
    }

    fn scrape<'a>(
        &'a self,
        actor_id: &'a str,
        progress: Option<&'a PageProgress>,
    ) -> BoxFuture<'a, Result<Vec<String>, JavBusError>> {
        Box::pin(JavBusClient::scrape(self, actor_id, progress))
    }
}

fn catalog_error(error: &AvbaseError) -> CatalogError {
    CatalogError {
        class: error.class_name().to_owned(),
        message: error.to_string(),
    }
}

/// `UnionActorCatalog`: AVBase's catalog joined with JavBus's, so neither's gaps become the
/// scan's.
///
/// AVBase knows the actor under every alias and dates every work, but drops delisted titles
/// and an actor's pre-rename catalog; JavBus keeps those but holds one star page per alias.
/// The actor may be named either way: an AVBase name (any alias) or a JavBus star id. JavBus
/// being unreachable only costs its share of the listing; AVBase being unreachable is
/// reported when JavBus cannot stand in.
#[derive(Debug)]
pub struct UnionActorCatalog<A = AvbaseClient, J = JavBusClient> {
    pub avbase: A,
    pub javbus: J,
}

impl<A: AvbaseCatalog, J: JavBusCatalog> UnionActorCatalog<A, J> {
    pub fn new(avbase: A, javbus: J) -> Self {
        Self { avbase, javbus }
    }

    async fn list(&self, actor_ref: &str, progress: Option<&PageProgress>) -> Result<CatalogListing, CatalogError> {
        let reference = py_strip(actor_ref);
        let talent = self
            .find_talent(reference)
            .await
            .map_err(|error| catalog_error(&error))?;
        let names = talent.as_ref().map(AvbaseTalent::names).unwrap_or_default();
        let stars = self.find_stars(reference, &names).await;
        if talent.is_none() && stars.is_empty() {
            return Err(CatalogError {
                class: ACTOR_NOT_FOUND.to_owned(),
                message: format!("neither AVBase nor JavBus knows an actor called {}", py_repr(reference)),
            });
        }

        let mut video_ids: IndexSet<String> = IndexSet::new();
        let mut release_dates: IndexMap<String, NaiveDate> = IndexMap::new();
        let mut counts: IndexMap<String, i64> = IndexMap::new();
        if let Some(talent) = &talent {
            let mut listed = 0_i64;
            let mut pages = self.avbase.talent_pages(&talent.name);
            while let Some((page, pages_total, works)) =
                pages.next_page().await.map_err(|error| catalog_error(&error))?
            {
                if let Some(progress) = progress {
                    let page = u64::try_from(page).unwrap_or_default();
                    let total = u64::try_from(pages_total).unwrap_or_default();
                    progress(page, Some(total), Some(page)).await;
                }
                for work in works {
                    listed += 1;
                    if let Some(day) = work.release_date {
                        release_dates.entry(work.work_id.clone()).or_insert(day);
                    }
                    video_ids.insert(work.work_id);
                }
            }
            counts.insert("avbase".to_owned(), listed);
        }
        for star in &stars {
            let key = format!("javbus:{}", star.actor_id);
            if let Ok(ids) = self.javbus.scrape(&star.actor_id, progress).await {
                counts.insert(key, i64::try_from(ids.len()).unwrap_or(i64::MAX));
                video_ids.extend(ids);
            } else {
                // JavBus is the supplement, not the source of truth.
                tracing::warn!(
                    logger = LOGGER,
                    "JavBus star {} could not be listed; continuing without it",
                    star.actor_id
                );
                counts.insert(key, -1);
            }
        }
        Ok(CatalogListing {
            actor_name: Some(match &talent {
                Some(talent) => talent.name.clone(),
                None => stars[0].name.clone(),
            }),
            talent_id: talent.as_ref().map(|talent| talent.talent_id),
            aliases: talent.map(|talent| talent.aliases).unwrap_or_default(),
            video_ids: video_ids.into_iter().collect(),
            release_dates,
            source_counts: counts,
        })
    }

    /// `_find_talent`: only an `AvbaseError` (the catalog or a stand-in page) falls back to
    /// JavBus alone; a failed request propagates, as Python's `except AvbaseError` lets it.
    async fn find_talent(&self, reference: &str) -> Result<Option<AvbaseTalent>, AvbaseError> {
        let found = async {
            let mut talent = self.avbase.talent(reference).await?;
            if talent.is_none() && is_star_id(reference) {
                // A JavBus star id: the star page names the actor, and AVBase may know that name.
                if let Some(page) = self.star_page(reference).await
                    && !page.name.is_empty()
                {
                    talent = self.avbase.talent(&page.name).await?;
                }
            }
            Ok::<_, AvbaseError>(talent)
        };
        match found.await {
            Err(error) if error.is_avbase_error() => {
                tracing::warn!(
                    logger = LOGGER,
                    "AVBase could not be read for {}; falling back to JavBus alone",
                    py_repr(reference)
                );
                Ok(None)
            }
            other => other,
        }
    }

    async fn star_page(&self, star_id: &str) -> Option<JavBusActorPage> {
        let Ok(page) = self.javbus.get_actor(star_id).await else {
            tracing::warn!(logger = LOGGER, "JavBus star page {star_id} could not be read");
            return None;
        };
        page
    }

    /// `_find_stars`: every JavBus star credited under one of the actor's names, the primary
    /// name first, then the aliases, so the listing is reproducible.
    async fn find_stars(&self, reference: &str, names: &[String]) -> Vec<JavBusActor> {
        let mut wanted: IndexSet<String> = if names.is_empty() {
            IndexSet::from([reference.to_owned()])
        } else {
            names.iter().cloned().collect()
        };
        let mut stars: IndexMap<String, JavBusActor> = IndexMap::new();
        if is_star_id(reference)
            && let Some(page) = self.star_page(reference).await
        {
            stars.insert(
                casefold(&page.actor_id),
                JavBusActor {
                    actor_id: page.actor_id,
                    name: page.name.clone(),
                },
            );
            wanted.insert(page.name);
        }
        for name in &wanted {
            let Ok(found) = self.javbus.search_stars(name).await else {
                tracing::warn!(logger = LOGGER, "JavBus star search for {} failed", py_repr(name));
                continue;
            };
            for star in found {
                // JavBus search is fuzzy: only a star credited under one of the actor's names
                // counts.
                if wanted.contains(&star.name) {
                    stars.entry(casefold(&star.actor_id)).or_insert(star);
                }
            }
        }
        stars.into_values().collect()
    }
}

impl<A: AvbaseCatalog, J: JavBusCatalog> ActorCatalog for UnionActorCatalog<A, J> {
    fn list_videos<'a>(
        &'a self,
        actor_ref: &'a str,
        progress_callback: Option<&'a PageProgress>,
    ) -> BoxFuture<'a, Result<CatalogListing, CatalogError>> {
        Box::pin(self.list(actor_ref, progress_callback))
    }
}

// ---------- the acquisition gateway ----------

/// What [`LedgerAcquisitionGateway`] needs from the monitor's intake.
pub trait Intake: Send + Sync {
    fn queue<'a>(
        &'a self,
        avid: &'a str,
        source: &'a str,
        task_dir_path: &'a str,
        ctx: &'a RunContext,
        release_date: Option<NaiveDate>,
    ) -> BoxFuture<'a, Result<IntakeOutcome, IntakeError>>;
}

impl Intake for AcquisitionIntake {
    fn queue<'a>(
        &'a self,
        avid: &'a str,
        source: &'a str,
        task_dir_path: &'a str,
        ctx: &'a RunContext,
        release_date: Option<NaiveDate>,
    ) -> BoxFuture<'a, Result<IntakeOutcome, IntakeError>> {
        Box::pin(AcquisitionIntake::queue(
            self,
            avid,
            source,
            task_dir_path,
            ctx,
            release_date,
        ))
    }
}

/// Resolves the intake from the live configuration per call; `None` while CloudDrive is
/// unconfigured.
pub type IntakeFactory = Arc<dyn Fn() -> Option<Arc<dyn Intake>> + Send + Sync>;
/// The offline directory fill-actor submissions are queued under; `""` while unset.
pub type TaskDir = Arc<dyn Fn() -> String + Send + Sync>;
/// Pokes the tracker's background pass awake.
pub type OnQueued = Arc<dyn Fn() + Send + Sync>;

/// `LedgerAcquisitionGateway`: the acquisition port backed by the monitor's intake.
///
/// Either configuration gap (no intake, no task directory) fails the submission with an
/// outcome naming it, without creating ledger rows nothing would ever advance. Queueing only
/// records the AVID; the tracker's background pass resolves magnets and submits later.
/// `on_queued` pokes that pass awake so the first resolve starts within the fast-check
/// window instead of a full poll interval.
#[derive(Clone)]
pub struct LedgerAcquisitionGateway {
    pub intake_factory: IntakeFactory,
    pub task_dir: TaskDir,
    pub on_queued: Option<OnQueued>,
}

impl std::fmt::Debug for LedgerAcquisitionGateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LedgerAcquisitionGateway").finish_non_exhaustive()
    }
}

impl LedgerAcquisitionGateway {
    async fn queue(
        &self,
        video_id: &str,
        release_date: Option<NaiveDate>,
    ) -> Result<AcquisitionOutcome, AcquisitionError> {
        let Some(intake) = (self.intake_factory)() else {
            return Ok(AcquisitionOutcome::CloudNotConfigured);
        };
        let task_dir_path = (self.task_dir)();
        if task_dir_path.is_empty() {
            return Ok(AcquisitionOutcome::TaskDirNotConfigured);
        }
        let ctx = RunContext::new(LOGGER);
        let outcome = intake
            .queue(
                video_id,
                AcquisitionSource::FillActor.as_str(),
                &task_dir_path,
                &ctx,
                release_date,
            )
            .await?;
        if outcome == IntakeOutcome::Queued
            && let Some(on_queued) = &self.on_queued
        {
            on_queued();
        }
        // The intake's outcomes are a subset of the port's, by value.
        Ok(AcquisitionOutcome::parse(outcome.as_str()).expect("every intake outcome is an acquisition outcome"))
    }
}

impl AcquisitionGateway for LedgerAcquisitionGateway {
    fn queue_missing<'a>(
        &'a self,
        video_id: &'a str,
        release_date: Option<NaiveDate>,
    ) -> BoxFuture<'a, Result<AcquisitionOutcome, AcquisitionError>> {
        Box::pin(self.queue(video_id, release_date))
    }
}

// ---------- the brand resolver ----------

/// `AvidBrandResolver`: the library brand of an AVID, by the AVID rules.
#[derive(Debug, Clone, Copy, Default)]
pub struct AvidBrandResolver;

impl BrandResolver for AvidBrandResolver {
    fn resolve_brand(&self, video_id: &str) -> Option<String> {
        embyx_core::avid::get_brand(video_id)
    }
}

// ---------- the cloud mover ----------

/// What [`CloudDriveFileMover`] needs from a CloudDrive handle; [`AsyncCloudDrive`] is the
/// one the app wires in.
pub trait CloudDriveApi: Clone + Send + Sync + 'static {
    /// `list_directory(api_dir)`: a fresh listing (`force_refresh=True`).
    fn list_directory<'a>(&'a self, api_dir: &'a str) -> BoxFuture<'a, Result<Vec<CloudFile>, CloudDriveError>>;
    fn ensure_directory<'a>(
        &'a self,
        parent_api_dir: &'a str,
        folder_name: &'a str,
    ) -> BoxFuture<'a, Result<EnsureResult, CloudDriveError>>;
    fn move_file<'a>(
        &'a self,
        source_api_path: &'a str,
        destination_api_dir: &'a str,
    ) -> BoxFuture<'a, Result<MoveResult, CloudDriveError>>;
    /// [`AsyncCloudDrive::scoped`].
    #[must_use]
    fn scoped(&self) -> Self;
    /// [`AsyncCloudDrive::settle`].
    fn settle(&self) -> BoxFuture<'_, ()>;
}

impl CloudDriveApi for AsyncCloudDrive {
    fn list_directory<'a>(&'a self, api_dir: &'a str) -> BoxFuture<'a, Result<Vec<CloudFile>, CloudDriveError>> {
        Box::pin(AsyncCloudDrive::list_directory(self, api_dir, true))
    }

    fn ensure_directory<'a>(
        &'a self,
        parent_api_dir: &'a str,
        folder_name: &'a str,
    ) -> BoxFuture<'a, Result<EnsureResult, CloudDriveError>> {
        Box::pin(AsyncCloudDrive::ensure_directory(self, parent_api_dir, folder_name))
    }

    fn move_file<'a>(
        &'a self,
        source_api_path: &'a str,
        destination_api_dir: &'a str,
    ) -> BoxFuture<'a, Result<MoveResult, CloudDriveError>> {
        Box::pin(AsyncCloudDrive::move_file(self, source_api_path, destination_api_dir))
    }

    fn scoped(&self) -> Self {
        AsyncCloudDrive::scoped(self)
    }

    fn settle(&self) -> BoxFuture<'_, ()> {
        Box::pin(AsyncCloudDrive::settle(self))
    }
}

/// Resolves the CloudDrive handle built from the live configuration; `None` while CloudDrive
/// is unconfigured.
pub type CloudResolver<C> = Arc<dyn Fn() -> Option<C> + Send + Sync>;

/// `CloudDriveFileMover`: the mover port over CloudDrive, resolving the current client per
/// call.
///
/// While CloudDrive is unconfigured every call fails with [`MoverError::Unconfigured`]
/// (`CloudDriveUnconfiguredError`), which the service's readiness checks treat as "cloud not
/// ready". A [`CloudFileMover::scoped`] mover runs each call on a scoped handle of the client
/// it resolved and remembers them, so [`CloudFileMover::settle`] can wait for every call it
/// started, also those whose caller was dropped (D41).
pub struct CloudDriveFileMover<C: CloudDriveApi = AsyncCloudDrive> {
    get_cloud: CloudResolver<C>,
    /// The scoped handles a scoped mover's calls ran on; `None` when not scoped.
    scope: Option<Arc<Mutex<Vec<C>>>>,
}

impl<C: CloudDriveApi> Clone for CloudDriveFileMover<C> {
    fn clone(&self) -> Self {
        Self {
            get_cloud: Arc::clone(&self.get_cloud),
            scope: self.scope.clone(),
        }
    }
}

impl<C: CloudDriveApi> std::fmt::Debug for CloudDriveFileMover<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudDriveFileMover")
            .field("scoped", &self.scope.is_some())
            .finish_non_exhaustive()
    }
}

/// `TypeError('CloudDrive listing returned invalid write time')`.
pub const INVALID_WRITE_TIME: &str = "CloudDrive listing returned invalid write time";

/// One listing entry as `CloudFileMetadata.from_mapping` reads it, or the `TypeError` the
/// adapter raises for a write time that is missing or out of range.
fn file_metadata(file: &CloudFile) -> Result<CloudFileMetadata, CheckError> {
    let Some(write_time) = file.write_time.filter(|time| (0..1_000_000_000).contains(&time.nanos)) else {
        return Err(CheckError::Type(INVALID_WRITE_TIME.to_owned()));
    };
    let mut mapping = Dict::new();
    mapping.insert("path", Value::from(file.full_path.as_str()));
    mapping.insert("id", Value::from(file.id.as_str()));
    mapping.insert("name", Value::from(file.name.as_str()));
    mapping.insert("size", Value::Int(i128::from(file.size)));
    mapping.insert(
        "write_time",
        Value::Int(i128::from(write_time.seconds) * NANOSECONDS_PER_SECOND + i128::from(write_time.nanos)),
    );
    let mut hashes = Dict::new();
    for (kind, value) in &file.hashes {
        hashes.insert(kind.as_str(), Value::from(value.as_str()));
    }
    mapping.insert("hashes", Value::Dict(hashes));
    CloudFileMetadata::from_mapping(&mapping)
}

impl<C: CloudDriveApi> CloudDriveFileMover<C> {
    pub fn new(get_cloud: CloudResolver<C>) -> Self {
        Self { get_cloud, scope: None }
    }

    /// `_cloud()`: the current client, scoped when this mover is.
    fn cloud(&self) -> Result<C, MoverError> {
        let cloud = (self.get_cloud)().ok_or(MoverError::Unconfigured)?;
        let Some(scope) = &self.scope else {
            return Ok(cloud);
        };
        let scoped = cloud.scoped();
        scope
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(scoped.clone());
        Ok(scoped)
    }

    async fn list(&self, api_directory: &str) -> Result<Vec<CloudFileMetadata>, MoverError> {
        let values = self.cloud()?.list_directory(api_directory).await?;
        let mut files = Vec::new();
        for value in values.iter().filter(|value| !value.is_directory) {
            files.push(file_metadata(value)?);
        }
        Ok(files)
    }

    async fn ensure(&self, parent_api_directory: &str, folder_name: &str) -> Result<bool, MoverError> {
        let value = self
            .cloud()?
            .ensure_directory(parent_api_directory, folder_name)
            .await?;
        Ok(value.success)
    }

    async fn relocate(
        &self,
        source_api_path: &str,
        destination_api_directory: &str,
    ) -> Result<CloudMoveResponse, MoverError> {
        let value = self
            .cloud()?
            .move_file(source_api_path, destination_api_directory)
            .await?;
        Ok(CloudMoveResponse {
            success: value.success,
            error_message: Some(value.error_message).filter(|text| !text.is_empty()),
            result_paths: value.result_file_paths,
        })
    }

    async fn settle_scope(&self) {
        let Some(scope) = &self.scope else {
            return;
        };
        // A call may start on a new handle while the earlier ones settle: go round until a
        // pass saw every handle. Settled handles are dropped, so a scoped mover kept for
        // more than one unit of work does not hold every handle it ever used.
        loop {
            let handles: Vec<C> = scope.lock().unwrap_or_else(PoisonError::into_inner).clone();
            if handles.is_empty() {
                return;
            }
            for handle in &handles {
                handle.settle().await;
            }
            let mut held = scope.lock().unwrap_or_else(PoisonError::into_inner);
            // Another `settle` on the same scope may have dropped some already.
            let done = handles.len().min(held.len());
            held.drain(..done);
        }
    }
}

impl<C: CloudDriveApi> CloudFileMover for CloudDriveFileMover<C> {
    fn list_directory<'a>(
        &'a self,
        api_directory: &'a str,
    ) -> BoxFuture<'a, Result<Vec<CloudFileMetadata>, MoverError>> {
        Box::pin(self.list(api_directory))
    }

    fn ensure_directory<'a>(
        &'a self,
        parent_api_directory: &'a str,
        folder_name: &'a str,
    ) -> BoxFuture<'a, Result<bool, MoverError>> {
        Box::pin(self.ensure(parent_api_directory, folder_name))
    }

    fn move_file<'a>(
        &'a self,
        source_api_path: &'a str,
        destination_api_directory: &'a str,
    ) -> BoxFuture<'a, Result<CloudMoveResponse, MoverError>> {
        Box::pin(self.relocate(source_api_path, destination_api_directory))
    }

    fn scoped(&self) -> Arc<dyn CloudFileMover> {
        Arc::new(Self {
            get_cloud: Arc::clone(&self.get_cloud),
            scope: Some(Arc::new(Mutex::new(Vec::new()))),
        })
    }

    fn settle(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.settle_scope())
    }
}
