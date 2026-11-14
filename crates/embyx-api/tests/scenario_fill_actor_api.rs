//! The fill-actor job manager and API scenarios (`tools/scenarios/specs/fill_actor/api.toml`,
//! operation `fill_actor.api`): the Rust port of `tools/scenarios/operations/fill_actor_api.py`.
//! The service over the seeded library, the job manager over it (the scenario clock, ids from
//! one counter, one worker, a 10 ms poll) and the app with the fill-actor router and health
//! block run each step and must render the bytes the Python baseline committed under
//! `goldens/scenarios/fill_actor/`. Job rows seeded as another build left them are picked up
//! (a queued scan), failed (an expired lease) or left to their owner (a live lease), and the
//! rows this build writes are compared with Python's, so either build resumes the other's.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::body::Body;
use chrono::{DateTime, TimeDelta, Utc};
use embyx_api::app::{AppParts, ReadyProbe, create_app};
use embyx_api::auth::MutationAuth;
use embyx_api::fill_actor_api::{FillActorApi, create_fill_actor_router, fill_actor_health};
use embyx_api::http::Internal;
use embyx_api::routing::App;
use embyx_clients::scenario::ScenarioFakes;
use embyx_core::pyjson::{self, Dict, DumpOptions, Key, Value};
use embyx_core::scenario::{self, Parts, Recorder, Seeded, Spec};
use embyx_db::database::Database;
use embyx_db::testdb::TestDatabase;
use embyx_fill_actor::cloud_moves::BoxFuture;
use embyx_fill_actor::jobs::{FillActorJobManager, JobManagerOptions};
use embyx_fill_actor::ports::{
    AcquisitionError, AcquisitionGateway, AcquisitionOutcome, ActorCatalog, BrandResolver, CatalogError,
    CatalogListing, PageProgressCallback,
};
use embyx_fill_actor::postgres_repository::PostgresFillActorRepository;
use embyx_fill_actor::service::{
    Clock, FillActorPaths, FillActorRuntime, FillActorService, ServiceOptions, ServiceParts, TokenFactory,
    static_runtime,
};
use http_body_util::BodyExt as _;

/// `WAIT_SECONDS`.
const WAIT: Duration = Duration::from_secs(30);
/// `ACTIVE_SQL`.
const ACTIVE_SQL: &str = "
SELECT count(*) FROM fill_actor_jobs
WHERE state = 'queued' OR (state = 'running' AND ($1::text IS NULL OR owner_id = $1))
";

fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value.lock().unwrap_or_else(PoisonError::into_inner)
}

fn texts(value: &Value) -> Vec<String> {
    value
        .as_list()
        .expect("a list")
        .iter()
        .map(|item| item.as_str().expect("a string").to_owned())
        .collect()
}

fn string_map(value: Option<&Value>) -> HashMap<String, Value> {
    value
        .and_then(Value::as_dict)
        .map(|values| {
            values
                .iter()
                .map(|(key, value)| {
                    let Key::Str(key) = key else {
                        panic!("a string key, not {key:?}")
                    };
                    (key.clone(), value.clone())
                })
                .collect()
        })
        .unwrap_or_default()
}

// ---------- the fakes (`operations/fill_actor_service.py`) ----------

/// `_Catalog`: the spec's videos per actor.
struct Catalog(HashMap<String, Vec<String>>);

impl ActorCatalog for Catalog {
    fn list_videos<'a>(
        &'a self,
        actor_ref: &'a str,
        _progress_callback: Option<&'a PageProgressCallback>,
    ) -> BoxFuture<'a, Result<CatalogListing, CatalogError>> {
        let video_ids = self.0.get(actor_ref).cloned();
        Box::pin(async move {
            // `self._values[actor_ref]`: an unknown actor is a `KeyError`.
            let video_ids = video_ids.ok_or_else(|| CatalogError {
                class: "KeyError".to_owned(),
                message: format!("'{actor_ref}'"),
            })?;
            Ok(CatalogListing {
                video_ids,
                ..CatalogListing::default()
            })
        })
    }
}

/// `_Gateway`: never a magnet.
struct Gateway;

impl AcquisitionGateway for Gateway {
    fn queue_missing<'a>(
        &'a self,
        _video_id: &'a str,
        _release_date: Option<chrono::NaiveDate>,
    ) -> BoxFuture<'a, Result<AcquisitionOutcome, AcquisitionError>> {
        Box::pin(async { Ok(AcquisitionOutcome::NoMagnet) })
    }
}

/// `_Brands`: a brand per video, `args.brand` (default `ABC`) for the rest.
struct Brands {
    values: HashMap<String, String>,
    default: Option<String>,
}

impl BrandResolver for Brands {
    fn resolve_brand(&self, video_id: &str) -> Option<String> {
        self.values.get(video_id).cloned().or_else(|| self.default.clone())
    }
}

// ---------- one process ----------

/// `_World`: the clock and the token counter, which outlive a restart.
struct World {
    now: Arc<Mutex<DateTime<Utc>>>,
    tokens: Arc<Mutex<u64>>,
}

impl World {
    fn clock(&self) -> Clock {
        let now = Arc::clone(&self.now);
        Arc::new(move || *lock(&now))
    }

    fn token(&self) -> TokenFactory {
        let tokens = Arc::clone(&self.tokens);
        Arc::new(move || {
            let mut count = lock(&tokens);
            *count += 1;
            format!("token-{count}")
        })
    }
}

/// `_Process`: one process's service, job manager, app and pool.
struct Process {
    database: Database,
    service: FillActorService,
    jobs: FillActorJobManager,
    app: App,
}

impl Process {
    fn new(test: &TestDatabase, seeded: &Seeded, args: &Value, world: &World) -> Self {
        let library = args.get("library").cloned().unwrap_or_else(|| Value::Dict(Dict::new()));
        let additional = library
            .get("additional")
            .map_or_else(|| vec!["additional".to_owned()], texts);
        let paths = FillActorPaths::from_iterable(
            seeded.resolve(library.get("actor").and_then(Value::as_str).unwrap_or("actor")),
            additional.iter().map(|path| seeded.resolve(path)),
            seeded.resolve(library.get("move_in").and_then(Value::as_str).unwrap_or("movein")),
        );
        let runtime = FillActorRuntime {
            paths: Some(paths),
            root_sentinel: args.get("root_sentinel").and_then(Value::as_str).map(str::to_owned),
            move_in_by_brand: args.get("move_in_by_brand") == Some(&Value::Bool(true)),
            apply_enabled: args.get("apply_enabled") != Some(&Value::Bool(false)),
            ..FillActorRuntime::default()
        };
        let database = test.database();
        let repository = Arc::new(PostgresFillActorRepository::new(database.clone()));
        let catalog = string_map(args.get("catalog"))
            .into_iter()
            .map(|(actor, videos)| (actor, texts(&videos)))
            .collect();
        let brands = Brands {
            values: string_map(args.get("brands"))
                .into_iter()
                .map(|(video, brand)| (video, brand.as_str().expect("a brand").to_owned()))
                .collect(),
            default: Some(args.get("brand").and_then(Value::as_str).unwrap_or("ABC").to_owned()),
        };
        let service = FillActorService::new(
            ServiceParts {
                runtime: static_runtime(runtime),
                actor_catalog: Arc::new(Catalog(catalog)),
                acquisition_gateway: Arc::new(Gateway),
                brand_resolver: Arc::new(brands),
                repository: Arc::clone(&repository) as Arc<_>,
            },
            ServiceOptions {
                clock: Some(world.clock()),
                token_factory: Some(world.token()),
                ..ServiceOptions::default()
            },
        )
        .expect("valid options");
        let workers = match args.get("max_concurrent_jobs") {
            Some(Value::Int(count)) => usize::try_from(*count).expect("a count"),
            _ => 1,
        };
        let jobs = FillActorJobManager::new(
            Arc::new(service.clone()),
            Arc::clone(&repository) as Arc<_>,
            JobManagerOptions {
                clock: Some(world.clock()),
                token_factory: Some(world.token()),
                max_concurrent_jobs: workers,
                poll_interval: Duration::from_millis(10),
                ..JobManagerOptions::default()
            },
        )
        .expect("valid options");
        let api_token = args.get("api_token").and_then(Value::as_str);
        let router = create_fill_actor_router(FillActorApi {
            service: service.clone(),
            repository: Arc::clone(&repository),
            jobs: jobs.clone(),
            auth: MutationAuth::new(api_token),
            avid_actor_lookup: None,
        });
        let ready_repository = Arc::clone(&repository);
        let app_ready: ReadyProbe = Arc::new(move || {
            let repository = Arc::clone(&ready_repository);
            Box::pin(async move { repository.health_check().await.map_err(Internal::new) })
        });
        let app = create_app(AppParts {
            app_ready,
            routers: vec![router],
            feature_health: vec![("fill_actor".to_owned(), fill_actor_health(service.clone(), repository))],
            api_token: api_token.map(str::to_owned),
            max_request_bytes: 65_536,
            frontend_dist: None,
        })
        .expect("an app");
        Self {
            database,
            service,
            jobs,
            app,
        }
    }

    async fn aclose(&self) {
        self.jobs.aclose().await;
        self.service.aclose().await;
        self.database.close().await;
    }
}

/// `_wait`: until no job is queued and none runs under this manager (with `settled`, none
/// at all).
async fn wait(test: &TestDatabase, process: &Process, settled: bool) {
    let owner = (!settled).then(|| process.jobs.owner_id().to_owned());
    let deadline = Instant::now() + WAIT;
    loop {
        let active: i64 = sqlx::query_scalar(ACTIVE_SQL)
            .bind(owner.as_deref())
            .fetch_one(test.pool())
            .await
            .expect("count the active jobs");
        if active == 0 {
            return;
        }
        assert!(Instant::now() < deadline, "jobs still active");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// `_mask` of `operations/api.py`.
fn mask(value: &Value, keys: &[String]) -> Value {
    match value {
        Value::List(items) => Value::List(items.iter().map(|item| mask(item, keys)).collect()),
        Value::Dict(fields) => {
            let mut masked = Dict::new();
            for (key, item) in fields.iter() {
                let named = key.as_str().is_some_and(|key| keys.iter().any(|name| name == key));
                let item = if named && *item != Value::Null {
                    Value::Str("<set>".to_owned())
                } else {
                    mask(item, keys)
                };
                masked.insert(key.clone(), item);
            }
            Value::Dict(masked)
        }
        other => other.clone(),
    }
}

/// `client.request(method, path, headers=..., json=...)` through the ASGI transport.
async fn request(app: &App, args: &Value, mask_keys: &[String], entry: &mut Dict) {
    let mut builder = http::Request::builder()
        .method(args.get("method").and_then(Value::as_str).expect("a method"))
        .uri(args.get("path").and_then(Value::as_str).expect("a path"))
        .header("host", "testserver");
    for (name, value) in string_map(args.get("headers")) {
        builder = builder.header(name.as_str(), value.as_str().expect("a header value"));
    }
    let body = match args.get("json") {
        Some(json) => {
            builder = builder.header("content-type", "application/json");
            Body::from(pyjson::dumps(json, &DumpOptions::python_default()).expect("dumps"))
        }
        None => Body::empty(),
    };
    let response = app.call(builder.body(body).expect("a request"), None).await;
    let status = response.status();
    let bytes = response.into_body().collect().await.expect("the body").to_bytes();
    entry.insert("status", Value::Int(i128::from(status.as_u16())));
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        mask(
            &pyjson::loads(std::str::from_utf8(&bytes).expect("UTF-8")).expect("a JSON body"),
            mask_keys,
        )
    };
    entry.insert("body", body);
}

async fn run_scenario(name: &str) {
    let spec = Spec::load(name);
    assert_eq!(spec.operation(), "fill_actor.api", "{name}");
    let seeded = Seeded::new(&spec);
    let test = TestDatabase::create().await;
    embyx_db::scenario::seed(test.pool(), &spec).await;
    let recorder = Recorder::new();
    let fakes = ScenarioFakes::start(&spec, &recorder).await;
    let args = spec.section("args");
    let world = World {
        now: Arc::new(Mutex::new(
            DateTime::parse_from_rfc3339(spec.clock())
                .expect("an RFC 3339 clock")
                .to_utc(),
        )),
        tokens: Arc::new(Mutex::new(0)),
    };
    let mask_keys: Vec<String> = args.get("mask_keys").map(texts).unwrap_or_default();

    let mut process = Process::new(&test, &seeded, &args, &world);
    let mut reported = Vec::new();
    for step in args.get("steps").and_then(Value::as_list).expect("steps") {
        let op = step.get("op").and_then(Value::as_str).expect("an op");
        let step_args = step.get("args").cloned().unwrap_or_else(|| Value::Dict(Dict::new()));
        if let Some(Value::Int(seconds)) = step_args.get("advance_seconds") {
            *lock(&world.now) += TimeDelta::seconds(i64::try_from(*seconds).expect("seconds"));
        }
        let mut entry = Dict::new();
        entry.insert("op", Value::from(op));
        match op {
            "request" => request(&process.app, &step_args, &mask_keys, &mut entry).await,
            "start" => process.jobs.start().await.expect("started"),
            "recover" => {
                let recovered = process.jobs.recover_interrupted_jobs().await.expect("recovered");
                entry.insert("result", Value::Int(i128::try_from(recovered).expect("a count")));
            }
            "wait" => wait(&test, &process, step_args.get("settled") == Some(&Value::Bool(true))).await,
            "restart" => {
                process.aclose().await;
                process = Process::new(&test, &seeded, &args, &world);
            }
            other => panic!("unknown step {other:?}"),
        }
        reported.push(Value::Dict(entry));
    }
    process.aclose().await;

    let mut result = Dict::new();
    result.insert("steps", Value::List(reported));
    let rows = embyx_db::scenario::dump_rows(test.pool(), &spec).await;
    let fakes_dump = fakes.dump();
    drop(fakes);
    scenario::assert_transcript(Parts {
        spec: &spec,
        seeded: &seeded,
        outcome: scenario::result(Value::Dict(result)),
        calls: recorder.calls(),
        rows,
        fakes: fakes_dump,
    });
    test.drop_database().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fill_actor_api_matches_the_python_transcript() {
    run_scenario("fill_actor/api").await;
}
