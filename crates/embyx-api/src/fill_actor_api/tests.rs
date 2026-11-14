//! Ported from `tests/test_api.py` (its fill-actor tests; the two `auth_session` tests are
//! WP4's): the router, health probe and lifespan of the real service and job manager in an
//! app of their own, on a temporary PostgreSQL database each (the in-memory repository is
//! not ported, D110) and a temporary library. Python's `with client:` is the lifespan's
//! start and stop around the calls.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{TimeDelta, TimeZone as _, Utc};
use embyx_core::pyjson::{self, Value};
use embyx_db::database::Database;
use embyx_db::testdb::TestDatabase;
use embyx_fill_actor::cloud_moves::BoxFuture;
use embyx_fill_actor::jobs::{FillActorJobManager, JobManagerOptions};
use embyx_fill_actor::ports::{
    AcquisitionError, AcquisitionGateway, AcquisitionOutcome, ActorCatalog, BrandResolver, CatalogError,
    CatalogListing, PageProgressCallback,
};
use embyx_fill_actor::postgres_repository::PostgresFillActorRepository;
use embyx_fill_actor::records::{
    JOB_CANCELLED_ERROR_CODE, JobOperation, JobProgress, JobProgressUnit, JobRecord, JobStage, JobState,
};
use embyx_fill_actor::service::{
    FillActorPaths, FillActorRuntime, FillActorService, ServiceOptions, ServiceParts, static_runtime,
};
use http::{Method, StatusCode};

use super::*;
use crate::app::{AppParts, ReadyProbe, create_app};
use crate::routing::App;
use crate::testing::{request, send};

// ---------- the fakes ----------

/// `ActorCatalog`: every actor lists `ABC-001`.
struct OneVideoCatalog;

fn one_video() -> CatalogListing {
    CatalogListing {
        video_ids: vec!["ABC-001".to_owned()],
        ..CatalogListing::default()
    }
}

impl ActorCatalog for OneVideoCatalog {
    fn list_videos<'a>(
        &'a self,
        _actor_ref: &'a str,
        _progress_callback: Option<&'a PageProgressCallback>,
    ) -> BoxFuture<'a, Result<CatalogListing, CatalogError>> {
        Box::pin(async { Ok(one_video()) })
    }
}

/// `BlockingActorCatalog`: lists `ABC-001` once released.
#[derive(Default)]
struct BlockingCatalog {
    started: AtomicBool,
    release: AtomicBool,
}

impl BlockingCatalog {
    /// `started.wait(timeout=1)`.
    async fn wait_started(&self) -> bool {
        for _ in 0..200 {
            if self.started.load(Ordering::SeqCst) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        false
    }
}

impl ActorCatalog for BlockingCatalog {
    fn list_videos<'a>(
        &'a self,
        _actor_ref: &'a str,
        _progress_callback: Option<&'a PageProgressCallback>,
    ) -> BoxFuture<'a, Result<CatalogListing, CatalogError>> {
        Box::pin(async {
            self.started.store(true, Ordering::SeqCst);
            while !self.release.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok(one_video())
        })
    }
}

/// `AcquisitionGateway`: never a magnet.
struct NoMagnets;

impl AcquisitionGateway for NoMagnets {
    fn queue_missing<'a>(
        &'a self,
        _video_id: &'a str,
        _release_date: Option<chrono::NaiveDate>,
    ) -> BoxFuture<'a, Result<AcquisitionOutcome, AcquisitionError>> {
        Box::pin(async { Ok(AcquisitionOutcome::NoMagnet) })
    }
}

/// `BrandResolver`: every video is `ABC`.
struct AbcBrand;

impl BrandResolver for AbcBrand {
    fn resolve_brand(&self, _video_id: &str) -> Option<String> {
        Some("ABC".to_owned())
    }
}

// ---------- the client ----------

/// `make_client(...)`'s keywords.
struct Options {
    api_token: Option<&'static str>,
    max_request_bytes: usize,
    actor_catalog: Option<Arc<dyn ActorCatalog>>,
    avid_actor_lookup: Option<AvidActorLookup>,
    apply_enabled: bool,
    /// `None` is the unconfigured section (`runtime=FillActorRuntime`).
    library: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            api_token: None,
            max_request_bytes: 65_536,
            actor_catalog: None,
            avid_actor_lookup: None,
            apply_enabled: true,
            library: true,
        }
    }
}

/// The app, its lifespan and what the tests look at.
struct Client {
    app: App,
    lifespan: FillActorLifespan,
    repository: Arc<PostgresFillActorRepository>,
    paths: FillActorPaths,
    _dir: tempfile::TempDir,
    database: Database,
    test: TestDatabase,
}

/// `make_client(tmp_path, ...)`.
async fn make_client(options: Options) -> Client {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let paths = FillActorPaths::from_iterable(root.join("actor"), [root.join("additional")], root.join("move-in"));
    if options.library {
        for path in [
            &paths.actor_brand_path,
            &paths.additional_brand_paths[0],
            &paths.move_in_path,
        ] {
            std::fs::create_dir(path).unwrap();
        }
    }
    let test = TestDatabase::create().await;
    let database = test.database();
    let repository = Arc::new(PostgresFillActorRepository::new(database.clone()));
    let runtime = if options.library {
        FillActorRuntime {
            paths: Some(paths.clone()),
            apply_enabled: options.apply_enabled,
            ..FillActorRuntime::default()
        }
    } else {
        FillActorRuntime::default()
    };
    let service = FillActorService::new(
        ServiceParts {
            runtime: static_runtime(runtime),
            actor_catalog: options.actor_catalog.unwrap_or_else(|| Arc::new(OneVideoCatalog)),
            acquisition_gateway: Arc::new(NoMagnets),
            brand_resolver: Arc::new(AbcBrand),
            repository: Arc::clone(&repository) as Arc<_>,
        },
        ServiceOptions::default(),
    )
    .unwrap();
    let jobs = FillActorJobManager::new(
        Arc::new(service.clone()),
        Arc::clone(&repository) as Arc<_>,
        JobManagerOptions::default(),
    )
    .unwrap();
    let ready_repository = Arc::clone(&repository);
    let app_ready: ReadyProbe = Arc::new(move || {
        let repository = Arc::clone(&ready_repository);
        Box::pin(async move { repository.health_check().await.map_err(Internal::new) })
    });
    let router = create_fill_actor_router(FillActorApi {
        service: service.clone(),
        repository: Arc::clone(&repository),
        jobs: jobs.clone(),
        auth: MutationAuth::new(options.api_token),
        avid_actor_lookup: options.avid_actor_lookup,
    });
    let app = create_app(AppParts {
        app_ready,
        routers: vec![router],
        feature_health: vec![(
            "fill_actor".to_owned(),
            fill_actor_health(service.clone(), Arc::clone(&repository)),
        )],
        api_token: options.api_token.map(str::to_owned),
        max_request_bytes: options.max_request_bytes,
        frontend_dist: None,
    })
    .unwrap();
    Client {
        app,
        lifespan: FillActorLifespan::new(service, Arc::clone(&repository), jobs),
        repository,
        paths,
        _dir: dir,
        database,
        test,
    }
}

impl Client {
    /// `with client:` entered.
    async fn start(&self) {
        self.lifespan.start().await.expect("the lifespan starts");
    }

    /// `with client:` left, then the database dropped.
    async fn finish(self) {
        self.lifespan.stop().await.expect("the lifespan stops");
        self.database.close().await;
        self.test.drop_database().await;
    }

    async fn call(&self, method: Method, path: &str, token: Option<&str>, body: Option<&str>) -> (StatusCode, Value) {
        let (parts, body) = self.send(method, path, token, body).await;
        let text = std::str::from_utf8(&body).expect("UTF-8");
        let value = pyjson::loads(text).unwrap_or_else(|error| panic!("not JSON ({error}): {text}"));
        (parts.status, value)
    }

    async fn send(
        &self,
        method: Method,
        path: &str,
        token: Option<&str>,
        body: Option<&str>,
    ) -> (http::response::Parts, bytes::Bytes) {
        let header = token.map(|token| format!("Bearer {token}"));
        let headers: Vec<(&str, &str)> = header
            .as_deref()
            .map(|value| vec![("authorization", value)])
            .unwrap_or_default();
        send(&self.app, request(method, path, &headers, body)).await
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        self.call(Method::GET, path, None, None).await
    }

    async fn post(&self, path: &str, body: Option<&str>) -> (StatusCode, Value) {
        self.call(Method::POST, path, None, body).await
    }

    /// `client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})`.
    async fn create_plan(&self) -> String {
        let (status, created) = self.post("/api/fill-actor/plans", Some(PLAN)).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{created:?}");
        text(&created, &["job", "plan_id"])
    }

    /// `wait_for_plan(client, plan_id)`.
    async fn wait_for_plan(&self, plan_id: &str) -> Value {
        // Python waits 0.5 s; a scan on PostgreSQL beside the parallel tests can take longer.
        for _ in 0..1000 {
            let (_, payload) = self.get(&format!("/api/fill-actor/plans/{plan_id}")).await;
            if !matches!(text(&payload, &["job", "state"]).as_str(), "queued" | "running") {
                return payload;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("plan job did not complete");
    }

    /// `wait_for_apply_job(client, job_id)`.
    async fn wait_for_apply_job(&self, job_id: &str) -> Value {
        for _ in 0..1000 {
            let (status, payload) = self.get(&format!("/api/fill-actor/apply-jobs/{job_id}")).await;
            assert_eq!(status, StatusCode::OK);
            if !matches!(text(&payload, &["job", "state"]).as_str(), "queued" | "running") {
                return payload;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("apply job did not complete");
    }

    fn move_in(&self, name: &str) -> PathBuf {
        self.paths.move_in_path.join(name)
    }

    /// `<additional>/ABC/ABC-001.mp4` holding `video`.
    fn source(&self) -> PathBuf {
        let brand = self.paths.additional_brand_paths[0].join("ABC");
        std::fs::create_dir(&brand).unwrap();
        let source = brand.join("ABC-001.mp4");
        std::fs::write(&source, b"video").unwrap();
        source
    }
}

const PLAN: &str = r#"{"actor_ids": ["actor"]}"#;

/// The value at `keys`.
fn at<'a>(value: &'a Value, keys: &[&str]) -> &'a Value {
    keys.iter().fold(value, |value, key| {
        value.get(key).unwrap_or_else(|| panic!("no {key} in {value:?}"))
    })
}

fn text(value: &Value, keys: &[&str]) -> String {
    at(value, keys)
        .as_str()
        .unwrap_or_else(|| panic!("{keys:?} is not text"))
        .to_owned()
}

fn first_candidate(payload: &Value) -> Value {
    let videos = at(payload, &["plan", "videos"]).as_list().expect("videos");
    videos[0]
        .get("move_candidates")
        .and_then(Value::as_list)
        .expect("candidates")[0]
        .clone()
}

fn load(text: &str) -> Value {
    pyjson::loads(text).expect("JSON")
}

fn error(code: &str) -> Value {
    load(&format!(r#"{{"error": {{"code": "{code}"}}}}"#))
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap()
}

// ---------- the tests ----------

/// An `avid_actor_lookup` answering `actors` and recording what it was asked.
fn lookup(actors: &[(&str, &str)], asked: &Arc<std::sync::Mutex<Vec<String>>>) -> AvidActorLookup {
    let actors: Vec<(String, String)> = actors
        .iter()
        .map(|(id, name)| ((*id).to_owned(), (*name).to_owned()))
        .collect();
    let asked = Arc::clone(asked);
    Arc::new(move |avid| {
        asked.lock().unwrap().push(avid);
        let actors = actors.clone();
        Box::pin(async move { Ok(actors) })
    })
}

// py: tests/test_api.py::test_resolves_an_avid_to_javbus_actors
#[tokio::test]
async fn resolves_an_avid_to_javbus_actors() {
    let asked = Arc::default();
    let client = make_client(Options {
        avid_actor_lookup: Some(lookup(&[("a123", "演员甲"), ("B456", "演员乙")], &asked)),
        ..Options::default()
    })
    .await;
    client.start().await;
    let (status, body) = client
        .post("/api/fill-actor/avid-actors", Some(r#"{"avid": " abc-123 "}"#))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        load(
            r#"{"avid": "ABC-123", "actors": [
                {"actor_id": "a123", "name": "演员甲"}, {"actor_id": "B456", "name": "演员乙"}]}"#
        )
    );
    assert_eq!(*asked.lock().unwrap(), vec!["ABC-123".to_owned()]);
    client.finish().await;
}

// py: tests/test_api.py::test_reports_when_javbus_has_no_actors_for_an_avid
#[tokio::test]
async fn reports_when_javbus_has_no_actors_for_an_avid() {
    let client = make_client(Options {
        avid_actor_lookup: Some(lookup(&[], &Arc::default())),
        ..Options::default()
    })
    .await;
    client.start().await;
    let (status, body) = client
        .post("/api/fill-actor/avid-actors", Some(r#"{"avid": "ABC-123"}"#))
        .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, error("avid_actors_not_found"));
    client.finish().await;
}

// py: tests/test_api.py::test_plan_job_and_apply_end_to_end
#[tokio::test]
async fn plan_job_and_apply_end_to_end() {
    let client = make_client(Options::default()).await;
    let source = client.source();
    client.start().await;

    let (parts, bytes) = client
        .send(Method::POST, "/api/fill-actor/plans", None, Some(PLAN))
        .await;
    assert_eq!(parts.status, StatusCode::ACCEPTED);
    assert_eq!(parts.headers.get("cache-control").unwrap(), "no-store");
    let created = load(std::str::from_utf8(&bytes).unwrap());
    assert_eq!(text(&created, &["job", "progress", "stage"]), "queued");
    assert_eq!(at(&created, &["job", "progress", "total"]), &Value::from(1_i64));
    let plan_id = text(&created, &["job", "plan_id"]);
    let payload = client.wait_for_plan(&plan_id).await;
    assert_eq!(text(&payload, &["job", "state"]), "completed");
    assert_eq!(text(&payload, &["job", "progress", "stage"]), "done");
    assert_eq!(at(&payload, &["job", "progress", "eta_seconds"]), &Value::from(0_i64));
    let candidate = first_candidate(&payload);

    let apply = format!(
        r#"{{"revision": "{}", "candidate_ids": ["{}"]}}"#,
        text(&payload, &["plan", "revision"]),
        text(&candidate, &["candidate_id"])
    );
    let (status, applied) = client
        .post(&format!("/api/fill-actor/plans/{plan_id}/apply"), Some(&apply))
        .await;
    client.finish_keeping_files().await;

    assert_eq!(status, StatusCode::OK, "{applied:?}");
    assert_eq!(text(&applied, &["state"]), "succeeded");
    let results = at(&applied, &["results"]).as_list().unwrap();
    assert_eq!(text(&results[0], &["state"]), "moved");
    assert!(!source.exists());
}

impl Client {
    /// Like [`Client::finish`], for a test that still looks at the library afterwards: the
    /// temporary directory is checked before it goes.
    async fn finish_keeping_files(self) {
        assert_eq!(read(&self.move_in("ABC-001.mp4")), b"video");
        self.finish().await;
    }
}

// py: tests/test_api.py::test_persistent_apply_job_moves_serially_and_is_idempotent
#[tokio::test]
async fn persistent_apply_job_moves_serially_and_is_idempotent() {
    let client = make_client(Options::default()).await;
    let source = client.source();
    let request_id = "apply-request-0001";
    client.start().await;

    let plan_id = client.create_plan().await;
    let payload = client.wait_for_plan(&plan_id).await;
    let candidate_id = text(&first_candidate(&payload), &["candidate_id"]);
    let revision = text(&payload, &["plan", "revision"]);
    let request = format!(
        r#"{{"revision": "{revision}", "candidate_ids": ["{candidate_id}", "{candidate_id}"], "request_id": "{request_id}"}}"#
    );
    let path = format!("/api/fill-actor/plans/{plan_id}/apply-jobs");

    let (started_status, started) = client.post(&path, Some(&request)).await;
    let terminal = client.wait_for_apply_job(request_id).await;
    let (repeated_status, repeated) = client.post(&path, Some(&request)).await;
    let conflict_request =
        format!(r#"{{"revision": "{revision}", "candidate_ids": [], "request_id": "{request_id}"}}"#);
    let (conflict_status, conflict) = client.post(&path, Some(&conflict_request)).await;
    let (cancelled_status, cancelled) = client
        .post(&format!("/api/fill-actor/plans/{request_id}/cancel"), None)
        .await;
    client.finish_keeping_files().await;

    assert_eq!(started_status, StatusCode::ACCEPTED);
    assert_eq!(text(&started, &["job", "operation"]), "apply");
    assert_eq!(text(&terminal, &["job", "state"]), "completed");
    assert_eq!(at(&terminal, &["job", "progress", "completed"]), &Value::from(1_i64));
    assert_eq!(at(&terminal, &["job", "progress", "total"]), &Value::from(1_i64));
    assert_eq!(at(&terminal, &["job", "progress", "current"]), &Value::Null);
    assert_eq!(text(&terminal, &["result", "state"]), "succeeded");
    let results = at(&terminal, &["result", "results"]).as_list().unwrap();
    assert_eq!(text(&results[0], &["state"]), "moved");
    assert_eq!(repeated_status, StatusCode::ACCEPTED);
    assert_eq!(without_ages(&repeated), without_ages(&terminal));
    assert_eq!(conflict_status, StatusCode::CONFLICT);
    assert_eq!(conflict, error("apply_request_conflict"));
    assert_eq!(cancelled_status, StatusCode::CONFLICT);
    assert_eq!(cancelled, error("apply_job_not_cancellable"));
    assert!(!source.exists());
}

/// The view without the two ages measured at the time of the call: Python compares whole
/// bodies, and a second ticking over between two calls changes them in either build.
fn without_ages(payload: &Value) -> Value {
    let mut payload = payload.clone();
    if let Value::Dict(fields) = &mut payload
        && let Some(Value::Dict(job)) = fields.get_mut("job")
        && let Some(Value::Dict(progress)) = job.get_mut("progress")
    {
        progress.remove("elapsed_seconds");
        progress.remove("last_progress_seconds");
    }
    payload
}

// py: tests/test_api.py::test_empty_apply_job_recovers_by_id_when_readiness_changes
#[tokio::test]
async fn empty_apply_job_recovers_by_id_when_readiness_changes() {
    let client = make_client(Options::default()).await;
    let request_id = "empty-apply-request-0001";
    client.start().await;
    let plan_id = client.create_plan().await;
    let payload = client.wait_for_plan(&plan_id).await;
    let revision = text(&payload, &["plan", "revision"]);
    let request = |request_id: &str| {
        format!(r#"{{"revision": "{revision}", "candidate_ids": [], "request_id": "{request_id}"}}"#)
    };
    let path = format!("/api/fill-actor/plans/{plan_id}/apply-jobs");
    let (started_status, _) = client.post(&path, Some(&request(request_id))).await;
    let terminal = client.wait_for_apply_job(request_id).await;
    std::fs::remove_dir(&client.paths.move_in_path).unwrap();

    let (recovered_status, recovered) = client.get(&format!("/api/fill-actor/apply-jobs/{request_id}")).await;
    let (repeated_status, repeated) = client.post(&path, Some(&request(request_id))).await;
    let (unavailable_status, unavailable) = client.post(&path, Some(&request("empty-apply-request-0002"))).await;
    client.finish().await;

    assert_eq!(started_status, StatusCode::ACCEPTED);
    assert_eq!(text(&terminal, &["job", "state"]), "completed");
    assert_eq!(at(&terminal, &["job", "progress", "completed"]), &Value::from(0_i64));
    assert_eq!(at(&terminal, &["job", "progress", "total"]), &Value::from(0_i64));
    assert_eq!(at(&terminal, &["job", "progress", "percent"]), &Value::Float(100.0));
    assert_eq!(text(&terminal, &["result", "state"]), "succeeded");
    assert_eq!(at(&terminal, &["result", "results"]), &Value::List(Vec::new()));
    assert_eq!(recovered_status, StatusCode::OK);
    assert_eq!(without_ages(&recovered), without_ages(&terminal));
    assert_eq!(repeated_status, StatusCode::ACCEPTED);
    assert_eq!(without_ages(&repeated), without_ages(&terminal));
    assert_eq!(unavailable_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(unavailable, error("not_ready"));
}

// py: tests/test_api.py::test_apply_job_requires_a_published_create_plan
#[tokio::test]
async fn apply_job_requires_a_published_create_plan() {
    let client = make_client(Options::default()).await;
    client.start().await;
    let plan_id = client.create_plan().await;
    let payload = client.wait_for_plan(&plan_id).await;
    assert!(client.repository.delete_plan(&plan_id).await.unwrap());

    let body = format!(
        r#"{{"revision": "{}", "candidate_ids": [], "request_id": "missing-plan-request-0001"}}"#,
        text(&payload, &["plan", "revision"])
    );
    let (status, missing) = client
        .post(&format!("/api/fill-actor/plans/{plan_id}/apply-jobs"), Some(&body))
        .await;
    client.finish().await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing, error("unknown_plan"));
}

// py: tests/test_api.py::test_apply_job_does_not_accept_an_apply_operation_as_its_parent
#[tokio::test]
async fn apply_job_does_not_accept_an_apply_operation_as_its_parent() {
    let client = make_client(Options::default()).await;
    let now = normalize_datetime(Utc::now());
    let impostor = JobRecord::new(
        "not-a-create-plan",
        JobOperation::Apply,
        JobState::Completed,
        now,
        now,
        Some("some-parent".to_owned()),
        Vec::new(),
    );
    client.start().await;
    client.repository.save_job(&impostor).await.unwrap();
    let (status, response) = client
        .post(
            &format!("/api/fill-actor/plans/{}/apply-jobs", impostor.job_id),
            Some(r#"{"revision": "revision", "candidate_ids": [], "request_id": "operation-check-request-0001"}"#),
        )
        .await;
    client.finish().await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(response, error("unknown_plan"));
}

// py: tests/test_api.py::test_mutations_require_configured_bearer_token
#[tokio::test]
async fn mutations_require_configured_bearer_token() {
    let token = "test-bearer-value";
    let client = make_client(Options {
        api_token: Some(token),
        ..Options::default()
    })
    .await;
    client.start().await;
    let (denied, denied_body) = client
        .send(Method::POST, "/api/fill-actor/plans", None, Some(PLAN))
        .await;
    let (allowed, _) = client
        .call(Method::POST, "/api/fill-actor/plans", Some(token), Some(PLAN))
        .await;
    let poll = "/api/fill-actor/apply-jobs/predictable-request";
    let (denied_poll, denied_poll_body) = client.send(Method::GET, poll, None, None).await;
    let (allowed_poll, allowed_poll_body) = client.call(Method::GET, poll, Some(token), None).await;
    client.finish().await;

    assert_eq!(denied.status, StatusCode::UNAUTHORIZED);
    assert_eq!(load(std::str::from_utf8(&denied_body).unwrap()), error("unauthorized"));
    assert!(denied.headers.contains_key("www-authenticate"));
    assert_eq!(allowed, StatusCode::ACCEPTED);
    assert_eq!(denied_poll.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        load(std::str::from_utf8(&denied_poll_body).unwrap()),
        error("unauthorized")
    );
    assert!(denied_poll.headers.contains_key("www-authenticate"));
    assert_eq!(allowed_poll, StatusCode::NOT_FOUND);
    assert_eq!(allowed_poll_body, error("unknown_apply_job"));
}

// py: tests/test_api.py::test_cancel_running_plan_is_authenticated_idempotent_and_does_not_require_roots
#[tokio::test]
async fn cancel_running_plan_is_authenticated_idempotent_and_does_not_require_roots() {
    let token = "cancel-token";
    let catalog = Arc::new(BlockingCatalog::default());
    let client = make_client(Options {
        api_token: Some(token),
        actor_catalog: Some(Arc::clone(&catalog) as Arc<dyn ActorCatalog>),
        ..Options::default()
    })
    .await;
    client.start().await;
    let (_, created) = client
        .call(Method::POST, "/api/fill-actor/plans", Some(token), Some(PLAN))
        .await;
    let plan_id = text(&created, &["job", "plan_id"]);
    assert!(catalog.wait_started().await);
    std::fs::remove_dir(&client.paths.move_in_path).unwrap();

    let cancel = format!("/api/fill-actor/plans/{plan_id}/cancel");
    let (denied, _) = client.post(&cancel, None).await;
    let (cancelled_status, cancelled) = client.call(Method::POST, &cancel, Some(token), None).await;
    let (repeated_status, repeated) = client.call(Method::POST, &cancel, Some(token), None).await;
    let (current_status, current) = client.get(&format!("/api/fill-actor/plans/{plan_id}")).await;
    catalog.release.store(true, Ordering::SeqCst);
    client.finish().await;

    assert_eq!(denied, StatusCode::UNAUTHORIZED);
    assert_eq!(cancelled_status, StatusCode::OK);
    assert_eq!(text(&cancelled, &["job", "state"]), "failed");
    assert_eq!(text(&cancelled, &["job", "error_code"]), JOB_CANCELLED_ERROR_CODE);
    assert_eq!(at(&cancelled, &["plan"]), &Value::Null);
    assert_eq!(repeated_status, StatusCode::OK);
    assert_eq!(text(&repeated, &["job", "error_code"]), JOB_CANCELLED_ERROR_CODE);
    assert_eq!(current_status, StatusCode::OK);
    assert_eq!(text(&current, &["job", "error_code"]), JOB_CANCELLED_ERROR_CODE);
    assert_eq!(at(&current, &["plan"]), &Value::Null);
}

// py: tests/test_api.py::test_cancel_unknown_or_completed_plan_has_stable_response
#[tokio::test]
async fn cancel_unknown_or_completed_plan_has_stable_response() {
    let client = make_client(Options::default()).await;
    client.start().await;
    let (unknown_status, unknown) = client.post("/api/fill-actor/plans/not-found/cancel", None).await;
    let plan_id = client.create_plan().await;
    assert_eq!(
        text(&client.wait_for_plan(&plan_id).await, &["job", "state"]),
        "completed"
    );
    let (completed_status, completed) = client
        .post(&format!("/api/fill-actor/plans/{plan_id}/cancel"), None)
        .await;
    client.finish().await;

    assert_eq!(unknown_status, StatusCode::NOT_FOUND);
    assert_eq!(unknown, error("unknown_plan"));
    assert_eq!(completed_status, StatusCode::CONFLICT);
    assert_eq!(completed, error("plan_not_cancellable"));
}

// py: tests/test_api.py::test_api_maps_service_errors_without_raw_messages
#[tokio::test]
async fn api_maps_service_errors_without_raw_messages() {
    let client = make_client(Options::default()).await;
    client.start().await;
    let (invalid_status, invalid) = client
        .post("/api/fill-actor/plans", Some(r#"{"actor_ids": ["bad/actor"]}"#))
        .await;
    let (malformed_status, malformed) = client
        .post(
            "/api/fill-actor/plans",
            Some(r#"{"actor_ids": ["actor"], "unexpected": true}"#),
        )
        .await;
    let long_id = "a".repeat(129);
    let (request_id_status, request_id) = client
        .post(
            "/api/fill-actor/plans/not-found/apply-jobs",
            Some(&format!(
                r#"{{"revision": "revision", "candidate_ids": [], "request_id": "{long_id}"}}"#
            )),
        )
        .await;
    let (unknown_status, unknown) = client.get("/api/fill-actor/plans/not-found").await;
    client.finish().await;

    assert_eq!(invalid_status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(invalid, error("invalid_actor_id"));
    assert_eq!(malformed_status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(malformed, error("invalid_request"));
    assert_eq!(request_id_status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(request_id, error("invalid_request"));
    assert_eq!(unknown_status, StatusCode::NOT_FOUND);
    assert_eq!(unknown, error("unknown_plan"));
}

// py: tests/test_api.py::test_request_size_limit_and_health
#[tokio::test]
async fn request_size_limit_and_health() {
    let client = make_client(Options {
        max_request_bytes: 20,
        ..Options::default()
    })
    .await;
    client.start().await;
    let (oversized_status, oversized) = client.post("/api/fill-actor/plans", Some(PLAN)).await;
    let (health_status, health) = client.get("/api/health").await;
    client.finish().await;

    assert_eq!(oversized_status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(oversized, error("request_too_large"));
    assert_eq!(health_status, StatusCode::OK);
    assert_eq!(
        health,
        load(
            r#"{"status": "ok", "database": true, "auth_required": false, "fill_actor": {
                "configured": true, "roots": true, "cloud": true, "legacy_journal": true,
                "apply_enabled": true, "scan_ready": true, "apply_ready": true}}"#
        )
    );
}

// py: tests/test_api.py::test_unavailable_fill_actor_roots_do_not_mark_the_whole_app_unhealthy
#[tokio::test]
async fn unavailable_fill_actor_roots_do_not_mark_the_whole_app_unhealthy() {
    let client = make_client(Options::default()).await;
    // An unmounted library root is fill-actor's problem; the monitor pipelines and the
    // settings page keep working, so probes and the top bar must not see a dead app.
    std::fs::remove_dir(&client.paths.move_in_path).unwrap();
    client.start().await;
    let (health_status, health) = client.get("/api/health").await;
    let (scan_status, scan) = client.post("/api/fill-actor/plans", Some(PLAN)).await;
    client.finish().await;

    assert_eq!(health_status, StatusCode::OK);
    assert_eq!(text(&health, &["status"]), "ok");
    assert_eq!(
        at(&health, &["fill_actor"]),
        &load(
            r#"{"configured": true, "roots": false, "cloud": true, "legacy_journal": true,
                "apply_enabled": true, "scan_ready": false, "apply_ready": false}"#
        )
    );
    assert_eq!(scan_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(scan, error("not_ready"));
}

// py: tests/test_api.py::test_unconfigured_fill_actor_reports_itself_without_failing_the_app
#[tokio::test]
async fn unconfigured_fill_actor_reports_itself_without_failing_the_app() {
    let client = make_client(Options {
        library: false,
        ..Options::default()
    })
    .await;
    client.start().await;
    let (health_status, health) = client.get("/api/health").await;
    let (scan_status, scan) = client.post("/api/fill-actor/plans", Some(PLAN)).await;
    client.finish().await;

    assert_eq!(health_status, StatusCode::OK);
    assert_eq!(text(&health, &["status"]), "ok");
    assert_eq!(at(&health, &["fill_actor", "configured"]), &Value::Bool(false));
    assert_eq!(at(&health, &["fill_actor", "scan_ready"]), &Value::Bool(false));
    assert_eq!(scan_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(scan, error("not_ready"));
}

// py: tests/test_api.py::test_disabled_apply_is_exposed_without_affecting_scan_or_readiness
#[tokio::test]
async fn disabled_apply_is_exposed_without_affecting_scan_or_readiness() {
    let client = make_client(Options {
        apply_enabled: false,
        ..Options::default()
    })
    .await;
    let source = client.source();
    client.start().await;
    let (health_status, health) = client.get("/api/health").await;
    let (created_status, created) = client.post("/api/fill-actor/plans", Some(PLAN)).await;
    let plan_id = text(&created, &["job", "plan_id"]);
    let payload = client.wait_for_plan(&plan_id).await;
    let candidate = first_candidate(&payload);
    let apply = format!(
        r#"{{"revision": "{}", "candidate_ids": ["{}"]}}"#,
        text(&payload, &["plan", "revision"]),
        text(&candidate, &["candidate_id"])
    );
    let (applied_status, applied) = client
        .post(&format!("/api/fill-actor/plans/{plan_id}/apply"), Some(&apply))
        .await;

    assert_eq!(health_status, StatusCode::OK);
    assert_eq!(text(&health, &["status"]), "ok");
    assert_eq!(at(&health, &["fill_actor", "apply_enabled"]), &Value::Bool(false));
    assert_eq!(created_status, StatusCode::ACCEPTED);
    assert_eq!(text(&payload, &["job", "state"]), "completed");
    assert_eq!(applied_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(applied, error("move_disabled"));
    assert_eq!(read(&source), b"video");
    assert!(!client.move_in("ABC-001.mp4").exists());
    client.finish().await;
}

// py: tests/test_api.py::test_running_job_does_not_publish_or_apply_plan
#[tokio::test]
async fn running_job_does_not_publish_or_apply_plan() {
    let catalog = Arc::new(BlockingCatalog::default());
    let client = make_client(Options {
        actor_catalog: Some(Arc::clone(&catalog) as Arc<dyn ActorCatalog>),
        ..Options::default()
    })
    .await;
    client.start().await;
    let plan_id = client.create_plan().await;
    assert!(catalog.wait_started().await);

    let (current_status, current) = client.get(&format!("/api/fill-actor/plans/{plan_id}")).await;
    let (applied_status, applied) = client
        .post(
            &format!("/api/fill-actor/plans/{plan_id}/apply"),
            Some(r#"{"revision": "not-published", "candidate_ids": []}"#),
        )
        .await;

    assert_eq!(current_status, StatusCode::OK);
    assert_eq!(text(&current, &["job", "state"]), "running");
    assert_eq!(at(&current, &["plan"]), &Value::Null);
    assert_eq!(applied_status, StatusCode::CONFLICT);
    assert_eq!(applied, error("plan_not_ready"));
    catalog.release.store(true, Ordering::SeqCst);
    assert_eq!(
        text(&client.wait_for_plan(&plan_id).await, &["job", "state"]),
        "completed"
    );
    client.finish().await;
}

// py: tests/test_api.py::test_completed_job_without_plan_returns_not_found
#[tokio::test]
async fn completed_job_without_plan_returns_not_found() {
    let client = make_client(Options::default()).await;
    client.start().await;
    let plan_id = client.create_plan().await;
    assert_eq!(
        text(&client.wait_for_plan(&plan_id).await, &["job", "state"]),
        "completed"
    );
    assert!(client.repository.delete_plan(&plan_id).await.unwrap());

    let (status, missing) = client.get(&format!("/api/fill-actor/plans/{plan_id}")).await;
    client.finish().await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing, error("unknown_plan"));
}

// py: tests/test_api.py::test_job_progress_view_derives_stage_eta_and_activity_ages
#[test]
fn job_progress_view_derives_stage_eta_and_activity_ages() {
    let started = Utc.with_ymd_and_hms(2026, 7, 13, 10, 0, 0).unwrap();
    let view = progress_view(
        &JobProgress {
            stage: JobStage::LibraryScan,
            completed: 4,
            total: Some(10),
            unit: JobProgressUnit::Videos,
            current: Some("ABC-004".to_owned()),
            stage_started_at: started,
            updated_at: started + TimeDelta::seconds(30),
        },
        JobState::Running,
        started + TimeDelta::seconds(40),
    );

    assert_eq!(view.get("percent"), Some(&Value::Float(40.0)));
    assert_eq!(view.get("eta_seconds"), Some(&Value::from(60_i64)));
    assert_eq!(view.get("elapsed_seconds"), Some(&Value::from(40_i64)));
    assert_eq!(view.get("last_progress_seconds"), Some(&Value::from(10_i64)));
}

/// `round(x, 2)` and the ETA's float arithmetic, at the edges Python's rules name.
#[test]
fn progress_view_rounds_as_python_and_stops_the_eta_where_python_does() {
    let started = Utc.with_ymd_and_hms(2026, 7, 13, 10, 0, 0).unwrap();
    let progress = |completed, total| JobProgress {
        stage: JobStage::LibraryScan,
        completed,
        total,
        unit: JobProgressUnit::Videos,
        current: None,
        stage_started_at: started,
        updated_at: started,
    };
    let later = started + TimeDelta::milliseconds(7_999);
    let view = progress_view(&progress(1, Some(3)), JobState::Running, later);
    // 33.333... rounds to 33.33; 7.999 s is 7 whole seconds; 7 / 1 * 2 = 14.
    assert_eq!(view.get("percent"), Some(&Value::Float(33.33)));
    assert_eq!(view.get("elapsed_seconds"), Some(&Value::from(7_i64)));
    assert_eq!(view.get("eta_seconds"), Some(&Value::from(14_i64)));
    let over = progress_view(&progress(5, Some(4)), JobState::Running, later);
    assert_eq!(over.get("percent"), Some(&Value::Float(100.0)));
    assert_eq!(over.get("eta_seconds"), Some(&Value::from(0_i64)));
    let unknown = progress_view(&progress(0, None), JobState::Queued, later);
    assert_eq!(unknown.get("percent"), Some(&Value::Null));
    assert_eq!(unknown.get("eta_seconds"), Some(&Value::Null));
    let finished = progress_view(&progress(0, None), JobState::Failed, later);
    assert_eq!(finished.get("eta_seconds"), Some(&Value::from(0_i64)));
    let at_start = progress_view(&progress(1, Some(3)), JobState::Running, started);
    assert_eq!(at_start.get("eta_seconds"), Some(&Value::Null));
    // A clock behind the stage start counts as no time at all.
    let behind = progress_view(
        &progress(1, Some(3)),
        JobState::Running,
        started - TimeDelta::seconds(5),
    );
    assert_eq!(behind.get("elapsed_seconds"), Some(&Value::from(0_i64)));
    // Python's round(x, 2) on the binary value: 2.675 is stored below the half, 0.125 ties
    // to even.
    assert_eq!(round2(2.675).to_string(), "2.67");
    assert_eq!(round2(0.125).to_string(), "0.12");
}

/// FastAPI's order: a refused token wins over a body that does not validate, the readiness
/// dependency over it too; the body's validation comes after both.
#[tokio::test]
async fn dependencies_run_before_the_body_is_validated() {
    let token = "order-token";
    let client = make_client(Options {
        api_token: Some(token),
        apply_enabled: false,
        ..Options::default()
    })
    .await;
    client.start().await;
    let (unauthorized, _) = client
        .post("/api/fill-actor/plans/p/apply", Some(r#"{"revision": ""}"#))
        .await;
    let (disabled, disabled_body) = client
        .call(
            Method::POST,
            "/api/fill-actor/plans/p/apply",
            Some(token),
            Some(r#"{"revision": ""}"#),
        )
        .await;
    let (broken, broken_body) = client
        .call(Method::POST, "/api/fill-actor/plans", Some(token), Some("{"))
        .await;
    let (unknown, unknown_body) = client
        .call(
            Method::POST,
            "/api/fill-actor/plans/p/apply-jobs",
            Some(token),
            Some(r#"{"revision": "r", "request_id": "request-id-0000001"}"#),
        )
        .await;
    client.finish().await;

    assert_eq!(unauthorized, StatusCode::UNAUTHORIZED);
    assert_eq!(disabled, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(disabled_body, error("move_disabled"));
    assert_eq!(broken, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(broken_body, error("invalid_request"));
    // A valid request for a plan nobody made, with moves disabled: the readiness check of the
    // apply-job route runs inside the handler, after the lookup by request id.
    assert_eq!(unknown, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(unknown_body, error("move_disabled"));
}

/// The request models' bounds (`ResolveAvidRequest`, `ApplyPlanRequest`,
/// `CreateApplyJobRequest`).
#[test]
fn request_models_hold_pydantic_bounds() {
    let body = |text: &str| JsonBody::Json(load(text));
    assert_eq!(resolve_avid_request(&body(r#"{"avid": ""}"#)), None);
    assert_eq!(
        resolve_avid_request(&body(&format!(r#"{{"avid": "{}"}}"#, "演".repeat(128)))),
        Some("演".repeat(128))
    );
    assert_eq!(
        resolve_avid_request(&body(&format!(r#"{{"avid": "{}"}}"#, "a".repeat(129)))),
        None
    );
    assert_eq!(resolve_avid_request(&body(r#"{"avid": 1}"#)), None);
    assert_eq!(create_plan_request(&body(r#"{"actor_ids": []}"#)), None);
    assert_eq!(create_plan_request(&body(r#"{"actor_ids": "actor"}"#)), None);
    let apply = apply_request(&body(r#"{"revision": "r"}"#), false).expect("valid");
    assert!(apply.candidate_ids.is_empty());
    assert_eq!(
        apply_request(&body(r#"{"revision": "r", "request_id": "x"}"#), false),
        None
    );
    let many: Vec<String> = (0..5_001).map(|index| format!("\"c{index}\"")).collect();
    assert_eq!(
        apply_request(
            &body(&format!(
                r#"{{"revision": "r", "candidate_ids": [{}]}}"#,
                many.join(",")
            )),
            false
        ),
        None
    );
    assert_eq!(
        apply_request(&body(r#"{"revision": "r", "request_id": "request-id-00001"}"#), true)
            .and_then(|apply| apply.request_id),
        Some("request-id-00001".to_owned())
    );
    assert_eq!(
        apply_request(&body(r#"{"revision": "r", "request_id": "request-id-0001"}"#), true),
        None
    );
    assert_eq!(
        apply_request(&body(r#"{"revision": "r", "request_id": "request id 000001"}"#), true),
        None
    );
    assert_eq!(apply_request(&body(r#"{"revision": "r"}"#), true), None);
}

/// `_service_error_status`, class by class.
#[test]
fn service_errors_map_to_python_statuses() {
    use FillActorErrorKind as Kind;
    let statuses: Vec<(Kind, u16)> = Kind::ALL
        .iter()
        .map(|kind| (*kind, service_error_status(*kind).as_u16()))
        .collect();
    assert_eq!(
        statuses,
        vec![
            (Kind::Base, 500),
            (Kind::InvalidActorId, 422),
            (Kind::TooManyActors, 422),
            (Kind::TooManyVideos, 422),
            (Kind::JobQueueFull, 429),
            (Kind::ApplyRequestConflict, 409),
            (Kind::ApplyJobNotCancellable, 409),
            (Kind::UnknownApplyJob, 404),
            (Kind::MoveDisabled, 503),
            (Kind::NotConfigured, 500),
            (Kind::LegacyPlan, 409),
            (Kind::UnknownPlan, 404),
            (Kind::ExpiredPlan, 410),
            (Kind::RevisionMismatch, 409),
            (Kind::UnknownCandidate, 422),
        ]
    );
}
