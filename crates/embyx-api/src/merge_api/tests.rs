//! The merge endpoints over a scratch mapping tree, PostgreSQL and the real merge service
//! (Python: `tests/test_merge_api.py`, which uses a fake repository and service, and the two
//! titles-endpoint tests of `tests/test_merge_detect.py`).

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use chrono::{TimeZone as _, Utc};
use embyx_core::config::models::{ArchiveConfig, CloudDriveConfig, MappingConfig, MergeConfig};
use embyx_core::pyjson::{self, Value};
use embyx_db::testdb::TestDatabase;
use embyx_merge::catalog::{
    AutoState, AutoStatus, MergeCatalog, SkippedTitle, TaskDirsFor, TitleScanCache, warm_title_cache,
};
use embyx_merge::fakes::{FakeCloud, FakeKube};
use embyx_merge::ports::{MergeCloud, MergeKube, Trigger};
use embyx_merge::service::{MergeService, NO_JOB_TEMPLATE, ServiceParts};
use embyx_merge::tasks::{MergeState, MergeTaskRepository, NewTask, TaskField};
use http::{Method, StatusCode};

use super::*;
use crate::routing::{App, Layers};
use crate::testing::call;

const LIBRARY: &str = "/mnt/cd2/115/embyx";

fn routes(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(source, destination)| ((*source).to_owned(), (*destination).to_owned()))
        .collect()
}

/// `tests.test_merge_detect.ARCHIVE`.
fn archive() -> ArchiveConfig {
    ArchiveConfig {
        src_dir: "/mnt/cd2/115/embyx_in".to_owned(),
        dst_dir: LIBRARY.to_owned(),
        mapping: routes(&[
            ("hh", "type/hh"),
            ("rank", "rank"),
            ("rest", "rest"),
            ("special", "type/special"),
        ]),
        priority_mapping: routes(&[("vr", "type/vr"), ("clt", "actor/clt")]),
        ..ArchiveConfig::default()
    }
}

fn strm(root: &Path, directory: &str, name: &str, target: &str) {
    let path = root.join(directory).join(name);
    std::fs::create_dir_all(path.parent().expect("has a parent")).expect("mkdir");
    std::fs::write(path, format!("{target}\n")).expect("write strm");
}

fn parts(root: &Path, directory: &str, avid: &str, count: i32, library_dir: &str, skip: &[i32]) {
    let brand = avid.split('-').next().expect("has a brand");
    for index in 1..=count {
        if skip.contains(&index) {
            continue;
        }
        strm(
            root,
            directory,
            &format!("{avid}-cd{index}.strm"),
            &format!("{LIBRARY}/{library_dir}/{brand}/{avid}-cd{index}.mp4"),
        );
    }
}

fn task_dirs(found: &[(&str, &str)]) -> TaskDirsFor {
    let found: HashMap<String, String> = found
        .iter()
        .map(|(avid, path)| ((*avid).to_owned(), (*path).to_owned()))
        .collect();
    Arc::new(move |avids: Vec<String>| {
        let found: HashMap<String, String> = found
            .iter()
            .filter(|(avid, _)| avids.contains(avid))
            .map(|(avid, path)| (avid.clone(), path.clone()))
            .collect();
        Box::pin(async move { Ok(found) })
    })
}

fn catalog(root: &Path, found: &[(&str, &str)]) -> MergeCatalog {
    let dst_dir = root.to_string_lossy().into_owned();
    MergeCatalog {
        archive: Arc::new(archive),
        mapping: Arc::new(move || MappingConfig {
            src_dir: "/remote".to_owned(),
            dst_dir: dst_dir.clone(),
            ..MappingConfig::default()
        }),
        task_dirs_for: task_dirs(found),
    }
}

/// The router in a bare app, with `ApiError` rendered as the shell renders it.
fn app(router: ApiRouter) -> App {
    App::new(
        router.into_routes(),
        None,
        Layers {
            max_request_bytes: None,
            api_no_store: false,
        },
    )
}

/// Which merge service backs the task routes.
#[derive(Default)]
struct Setup<'a> {
    task_dirs: &'a [(&'a str, &'a str)],
    /// `FakeService('...')`: without a Job template merging is unavailable.
    unavailable: bool,
    cache: Option<TitleScanCache>,
    auto_status: Option<AutoStatusSource>,
}

struct Harness {
    test: TestDatabase,
    tmp: tempfile::TempDir,
    repository: MergeTaskRepository,
    app: App,
    service: MergeService,
}

impl Harness {
    /// `make_client(tmp_path, repository, service, ...)`.
    async fn new(setup: Setup<'_>) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let work = tempfile::tempdir_in(tmp.path()).expect("work dir");
        let work_dir = work.keep().to_string_lossy().into_owned();
        let test = TestDatabase::create().await;
        let repository = MergeTaskRepository::new(test.database());
        let cloud = FakeCloud::new(&["/", "/115"]);
        let kube = (!setup.unavailable).then(|| Arc::new(FakeKube::default()) as Arc<dyn MergeKube>);
        let trigger: Trigger = Arc::new(|_| Box::pin(async { Ok(()) }));
        let service = MergeService::new(ServiceParts::new(
            repository.clone(),
            Arc::new(move || Some(Arc::new(cloud.clone()) as Arc<dyn MergeCloud>)),
            kube,
            Arc::new(move || MergeConfig {
                work_dir: work_dir.clone(),
                ..MergeConfig::default()
            }),
            Arc::new(archive),
            Arc::new(|| CloudDriveConfig {
                address: "cd:19798".to_owned(),
                api_token: "t".to_owned(),
                ..CloudDriveConfig::default()
            }),
            trigger,
        ));
        let router = create_merge_router(
            catalog(&tmp.path().join("mapping"), setup.task_dirs),
            Some(MergeTasksApi {
                service: service.clone(),
                auto_status: setup.auto_status,
            }),
            MutationAuth::new(None),
            setup.cache,
        );
        Self {
            test,
            tmp,
            repository,
            app: app(router),
            service,
        }
    }

    fn root(&self) -> std::path::PathBuf {
        self.tmp.path().join("mapping")
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        call(&self.app, Method::GET, path, &[], None).await
    }

    async fn post(&self, path: &str, body: Option<&str>) -> (StatusCode, Value) {
        call(&self.app, Method::POST, path, &[], body).await
    }

    async fn create(&self, avid: &str) -> i64 {
        self.repository
            .create(&NewTask {
                avid: avid.to_owned(),
                source: "vr".to_owned(),
                library_dir: "type/vr".to_owned(),
                brand: avid.split('-').next().expect("a brand").to_owned(),
                // Python's fake service never resolves them; the real one needs them on the mount.
                parts: (1..=2)
                    .map(|index| format!("{LIBRARY}/type/vr/{avid}-cd{index}.mp4"))
                    .collect(),
                merged_name: format!("{avid}.mp4"),
            })
            .await
            .expect("create")
            .id
    }

    /// `task(..., MergeState.FAILED, failed_state=..., error='x')`.
    async fn fail(&self, task_id: i64, failed_state: MergeState) {
        self.repository
            .transition(
                task_id,
                MergeState::Queued,
                MergeState::Failed,
                vec![
                    TaskField::FailedState(Some(failed_state)),
                    TaskField::Error(Some("x".to_owned())),
                ],
            )
            .await
            .expect("transition")
            .expect("the task was queued");
    }

    async fn close(self) {
        self.repository.database().close().await;
        self.test.drop_database().await;
    }
}

fn json(text: &str) -> Value {
    pyjson::loads(text).expect("JSON")
}

fn field<'a>(value: &'a Value, key: &str) -> &'a Value {
    value.get(key).unwrap_or_else(|| panic!("no {key} in {value:?}"))
}

fn text(value: &Value, key: &str) -> String {
    match field(value, key) {
        Value::Str(text) => text.clone(),
        other => panic!("{key} is not a string: {other:?}"),
    }
}

fn avids(body: &Value) -> Vec<String> {
    match field(body, "items") {
        Value::List(items) => items.iter().map(|item| text(item, "avid")).collect(),
        other => panic!("items is not a list: {other:?}"),
    }
}

fn items(body: &Value) -> Vec<Value> {
    match field(body, "items") {
        Value::List(items) => items.clone(),
        other => panic!("items is not a list: {other:?}"),
    }
}

// py: tests/test_merge_api.py::test_create_queues_a_title_with_its_parts_and_route
#[tokio::test]
async fn create_queues_a_title_with_its_parts_and_route() {
    let harness = Harness::new(Setup::default()).await;
    parts(
        &harness.root(),
        "type/vr/SQTEVR/SQTEVR-009",
        "SQTEVR-009",
        12,
        "type/vr",
        &[],
    );

    let (status, body) = harness
        .post("/api/merge/tasks", Some(r#"{"avid": "sqtevr-009"}"#))
        .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        (
            text(&body, "avid"),
            text(&body, "source"),
            field(&body, "part_count"),
            text(&body, "state")
        ),
        (
            "SQTEVR-009".to_owned(),
            "vr".to_owned(),
            &Value::Int(12),
            "queued".to_owned()
        )
    );
    assert_eq!(
        (field(&body, "cancellable"), field(&body, "retryable")),
        (&Value::Bool(true), &Value::Bool(false))
    );
    let created = harness.repository.listing().await.expect("listing");
    let [created] = created.as_slice() else {
        panic!("one task: {created:?}");
    };
    assert_eq!(created.parts[0], "/mnt/cd2/115/embyx/type/vr/SQTEVR/SQTEVR-009-cd1.mp4");
    assert!(created.parts.last().expect("parts").ends_with("-cd12.mp4"));
    assert_eq!(created.merged_name, "SQTEVR-009.mp4");
    assert_eq!(harness.service.woken(), 1);

    let (_, again) = harness
        .post("/api/merge/tasks", Some(r#"{"avid": "SQTEVR-009"}"#))
        .await;
    assert_eq!(again, json(r#"{"error": {"code": "merge_task_exists"}}"#));
    harness.close().await;
}

// py: tests/test_merge_api.py::test_create_needs_a_route_when_the_library_cannot_tell
#[tokio::test]
async fn create_needs_a_route_when_the_library_cannot_tell() {
    let harness = Harness::new(Setup::default()).await;
    parts(&harness.root(), "type/x/DEF/DEF-001", "DEF-001", 2, "type/x", &[]);

    let (_, missing) = harness.post("/api/merge/tasks", Some(r#"{"avid": "DEF-001"}"#)).await;
    assert_eq!(missing, json(r#"{"error": {"code": "merge_source_required"}}"#));
    let (_, unknown) = harness
        .post("/api/merge/tasks", Some(r#"{"avid": "DEF-001", "source": "nope"}"#))
        .await;
    assert_eq!(unknown, json(r#"{"error": {"code": "merge_source_unknown"}}"#));
    let (status, created) = harness
        .post("/api/merge/tasks", Some(r#"{"avid": "DEF-001", "source": "rank"}"#))
        .await;
    assert_eq!(
        (status, text(&created, "source")),
        (StatusCode::CREATED, "rank".to_owned())
    );
    harness.close().await;
}

// py: tests/test_merge_api.py::test_create_falls_back_to_the_ledger_route
#[tokio::test]
async fn create_falls_back_to_the_ledger_route() {
    let harness = Harness::new(Setup {
        task_dirs: &[("DEF-001", "/115/embyx_in/special")],
        ..Setup::default()
    })
    .await;
    parts(&harness.root(), "type/x/DEF/DEF-001", "DEF-001", 2, "type/x", &[]);

    let (_, created) = harness.post("/api/merge/tasks", Some(r#"{"avid": "DEF-001"}"#)).await;

    assert_eq!(text(&created, "source"), "special");
    harness.close().await;
}

// py: tests/test_merge_api.py::test_create_refuses_what_cannot_be_merged
#[tokio::test]
async fn create_refuses_what_cannot_be_merged() {
    let harness = Harness::new(Setup::default()).await;
    let root = harness.root();
    parts(&root, "rest/XYZ/XYZ-001", "XYZ-001", 4, "rest", &[2]);
    strm(
        &root,
        "rank/MIX/MIX-001",
        "MIX-001-cd1.strm",
        "/mnt/cd2/115/embyx/rank/MIX/MIX-001-cd1.mp4",
    );
    strm(
        &root,
        "rank/MIX/MIX-001",
        "MIX-001-cd2.strm",
        "/mnt/cd2/115/embyx/rank/MIX/MIX-001-cd2.mkv",
    );

    let (_, incomplete) = harness.post("/api/merge/tasks", Some(r#"{"avid": "XYZ-001"}"#)).await;
    assert_eq!(incomplete, json(r#"{"error": {"code": "merge_title_incomplete"}}"#));
    let (_, mixed) = harness.post("/api/merge/tasks", Some(r#"{"avid": "MIX-001"}"#)).await;
    assert_eq!(mixed, json(r#"{"error": {"code": "merge_container_unsupported"}}"#));
    let missing = harness.post("/api/merge/tasks", Some(r#"{"avid": "NONE-001"}"#)).await;
    assert_eq!(
        missing,
        (
            StatusCode::NOT_FOUND,
            json(r#"{"error": {"code": "merge_title_not_found"}}"#)
        )
    );
    harness.close().await;
}

// py: tests/test_merge_api.py::test_create_is_refused_while_merging_is_unavailable
#[tokio::test]
async fn create_is_refused_while_merging_is_unavailable() {
    let harness = Harness::new(Setup {
        unavailable: true,
        ..Setup::default()
    })
    .await;
    parts(
        &harness.root(),
        "type/vr/SQTEVR/SQTEVR-009",
        "SQTEVR-009",
        12,
        "type/vr",
        &[],
    );

    let response = harness
        .post("/api/merge/tasks", Some(r#"{"avid": "SQTEVR-009"}"#))
        .await;

    assert_eq!(
        response,
        (
            StatusCode::CONFLICT,
            json(r#"{"error": {"code": "merge_unavailable"}}"#)
        )
    );
    // Python's fake service gives its own reason; the real one names the missing template.
    let (_, listing) = harness.get("/api/merge/tasks").await;
    let mut expected = json(r#"{"items": [], "unavailable": null, "auto": null, "worker": null}"#);
    if let Value::Dict(dict) = &mut expected {
        dict.insert("unavailable", Value::from(NO_JOB_TEMPLATE));
    }
    assert_eq!(listing, expected);
    harness.close().await;
}

// py: tests/test_merge_api.py::test_cancel_and_retry_report_refusals_as_codes
#[tokio::test]
async fn cancel_and_retry_report_refusals_as_codes() {
    let harness = Harness::new(Setup::default()).await;
    let first = harness.create("SQTEVR-009").await;
    let second = harness.create("ABP-123").await;

    let (status, cancelled) = harness.post(&format!("/api/merge/tasks/{first}/cancel"), None).await;
    assert_eq!(
        (status, text(&cancelled, "state")),
        (StatusCode::OK, "cancelled".to_owned())
    );
    let missing = harness.post("/api/merge/tasks/9/cancel", None).await;
    assert_eq!(
        missing,
        (
            StatusCode::NOT_FOUND,
            json(r#"{"error": {"code": "merge_task_not_found"}}"#)
        )
    );
    let refused = harness.post(&format!("/api/merge/tasks/{second}/retry"), None).await;
    assert_eq!(
        refused,
        (
            StatusCode::CONFLICT,
            json(r#"{"error": {"code": "merge_task_not_failed"}}"#)
        )
    );
    harness.close().await;
}

// py: tests/test_merge_api.py::test_a_task_that_failed_while_replacing_offers_retry_only
#[tokio::test]
async fn a_task_that_failed_while_replacing_offers_retry_only() {
    let harness = Harness::new(Setup::default()).await;
    let id = harness.create("SQTEVR-009").await;
    harness.fail(id, MergeState::Replacing).await;

    let (_, body) = harness.get("/api/merge/tasks").await;
    let [item] = items(&body).try_into().expect("one task");

    assert_eq!(
        (
            text(&item, "state"),
            text(&item, "failed_state"),
            field(&item, "cancellable"),
            field(&item, "retryable")
        ),
        (
            "failed".to_owned(),
            "replacing".to_owned(),
            &Value::Bool(false),
            &Value::Bool(true)
        )
    );
    harness.close().await;
}

// py: tests/test_merge_api.py::test_a_task_that_failed_while_archiving_may_be_removed
#[tokio::test]
async fn a_task_that_failed_while_archiving_may_be_removed() {
    let harness = Harness::new(Setup::default()).await;
    let id = harness.create("AVOPVR-117").await;
    harness.fail(id, MergeState::Archiving).await;

    let (_, body) = harness.get("/api/merge/tasks").await;
    let [item] = items(&body).try_into().expect("one task");

    assert_eq!(
        (field(&item, "cancellable"), field(&item, "retryable")),
        (&Value::Bool(true), &Value::Bool(true))
    );
    harness.close().await;
}

// py: tests/test_merge_api.py::test_titles_are_scanned_once_until_a_refresh_is_asked_for
#[tokio::test]
async fn titles_are_scanned_once_until_a_refresh_is_asked_for() {
    let harness = Harness::new(Setup::default()).await;
    let root = harness.root();
    parts(&root, "type/vr/SQTEVR/SQTEVR-009", "SQTEVR-009", 12, "type/vr", &[]);

    let (_, first) = harness.get("/api/merge/titles").await;
    parts(&root, "rank/ABP/ABP-123", "ABP-123", 3, "rank", &[]);
    let (_, cached) = harness.get("/api/merge/titles").await;
    let (_, refreshed) = harness.get("/api/merge/titles?refresh=true").await;

    assert_eq!(avids(&cached), ["SQTEVR-009"]);
    assert_eq!(field(&cached, "scanned_at"), field(&first, "scanned_at"));
    assert_eq!(avids(&refreshed), ["SQTEVR-009", "ABP-123"]);
    // ISO 8601 in UTC: later sorts later.
    assert!(text(&refreshed, "scanned_at") > text(&first, "scanned_at"));
    harness.close().await;
}

// py: tests/test_merge_api.py::test_create_reads_the_title_directory_as_it_is_now
#[tokio::test]
async fn create_reads_the_title_directory_as_it_is_now() {
    let harness = Harness::new(Setup::default()).await;
    let root = harness.root();
    parts(&root, "type/vr/SQTEVR/SQTEVR-009", "SQTEVR-009", 12, "type/vr", &[]);
    harness.get("/api/merge/titles").await;

    parts(&root, "type/vr/SQTEVR/SQTEVR-009", "SQTEVR-009", 13, "type/vr", &[]);
    parts(&root, "rank/ABP/ABP-123", "ABP-123", 3, "rank", &[]);

    let (_, created) = harness
        .post("/api/merge/tasks", Some(r#"{"avid": "SQTEVR-009"}"#))
        .await;
    assert_eq!(field(&created, "part_count"), &Value::Int(13));
    let (status, _) = harness.post("/api/merge/tasks", Some(r#"{"avid": "ABP-123"}"#)).await;
    assert_eq!(status, StatusCode::CREATED);
    let mut queued: Vec<String> = harness
        .repository
        .listing()
        .await
        .expect("listing")
        .into_iter()
        .map(|task| task.avid)
        .collect();
    queued.reverse();
    assert_eq!(queued, ["SQTEVR-009", "ABP-123"]);
    harness.close().await;
}

// py: tests/test_merge_api.py::test_create_refuses_a_title_whose_parts_left_since_the_scan
#[tokio::test]
async fn create_refuses_a_title_whose_parts_left_since_the_scan() {
    let harness = Harness::new(Setup::default()).await;
    let root = harness.root();
    parts(&root, "type/vr/SQTEVR/SQTEVR-009", "SQTEVR-009", 12, "type/vr", &[]);
    harness.get("/api/merge/titles").await;

    for entry in std::fs::read_dir(root.join("type/vr/SQTEVR/SQTEVR-009")).expect("read_dir") {
        let path = entry.expect("entry").path();
        if path.extension().is_some_and(|extension| extension == "strm") {
            std::fs::remove_file(path).expect("unlink");
        }
    }

    let missing = harness
        .post("/api/merge/tasks", Some(r#"{"avid": "SQTEVR-009"}"#))
        .await;
    assert_eq!(
        missing,
        (
            StatusCode::NOT_FOUND,
            json(r#"{"error": {"code": "merge_title_not_found"}}"#)
        )
    );
    harness.close().await;
}

// py: tests/test_merge_api.py::test_a_warmed_cache_answers_the_first_visit
#[tokio::test]
async fn a_warmed_cache_answers_the_first_visit() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("mapping");
    parts(&root, "type/vr/SQTEVR/SQTEVR-009", "SQTEVR-009", 12, "type/vr", &[]);
    let cache = TitleScanCache::default();
    warm_title_cache(&catalog(&root, &[]), &cache).await;
    parts(&root, "rank/ABP/ABP-123", "ABP-123", 3, "rank", &[]);

    let client = app(create_merge_router(
        catalog(&root, &[]),
        None,
        MutationAuth::new(None),
        Some(cache),
    ));

    let (_, body) = call(&client, Method::GET, "/api/merge/titles", &[], None).await;
    assert_eq!(avids(&body), ["SQTEVR-009"]);
}

/// Every event of this thread's subscriber, as `field -> text`.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<HashMap<String, String>>>>);

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
        event.record(&mut Fields(&mut fields));
        self.0.lock().unwrap_or_else(PoisonError::into_inner).push(fields);
    }
}

// py: tests/test_merge_api.py::test_warming_logs_instead_of_raising
#[tokio::test]
async fn warming_logs_instead_of_raising() {
    use tracing_subscriber::layer::SubscriberExt as _;

    // A section that cannot be read panics where Python's raises.
    let catalog = MergeCatalog {
        archive: Arc::new(archive),
        mapping: Arc::new(|| panic!("config store not loaded")),
        task_dirs_for: task_dirs(&[]),
    };
    let captured = Captured::default();
    {
        let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(captured.clone()));
        warm_title_cache(&catalog, &TitleScanCache::default()).await;
    }

    let events = captured.0.lock().unwrap_or_else(PoisonError::into_inner).clone();
    let event = events
        .iter()
        .find(|event| {
            event
                .get("message")
                .is_some_and(|message| message.contains("could not scan the mapping tree"))
        })
        .unwrap_or_else(|| panic!("the failure is logged: {events:?}"));
    assert!(
        event
            .get("error")
            .is_some_and(|error| error.contains("config store not loaded"))
    );
}

// py: tests/test_merge_api.py::test_task_listing_reports_automatic_merging
#[tokio::test]
async fn task_listing_reports_automatic_merging() {
    let status = AutoStatus {
        state: AutoState::Idle,
        room: Some(10),
        skipped: vec![SkippedTitle {
            avid: "SQTEVR-009".to_owned(),
            size: Some(20),
            reason: "too_big".to_owned(),
        }],
        skipped_count: 1,
        checked_at: Some(Utc.with_ymd_and_hms(2026, 10, 2, 20, 0, 0).unwrap()),
    };
    let harness = Harness::new(Setup {
        auto_status: Some(Arc::new(move || status.clone())),
        ..Setup::default()
    })
    .await;

    let (_, body) = harness.get("/api/merge/tasks").await;
    let auto = field(&body, "auto");

    assert_eq!(text(auto, "state"), "idle");
    assert_eq!(
        field(auto, "skipped"),
        &json(r#"[{"avid": "SQTEVR-009", "size": 20, "reason": "too_big"}]"#)
    );
    harness.close().await;
}

/// `make_client(tmp_path, task_dirs, mapping_dst=...)` of `tests/test_merge_detect.py`: the
/// titles route alone.
fn titles_client(root: &Path, found: &[(&str, &str)], mapping_dst: Option<&str>) -> App {
    let mut catalog = catalog(root, found);
    if let Some(dst) = mapping_dst {
        let dst = dst.to_owned();
        catalog.mapping = Arc::new(move || MappingConfig {
            src_dir: "/remote".to_owned(),
            dst_dir: dst.clone(),
            ..MappingConfig::default()
        });
    }
    app(create_merge_router(catalog, None, MutationAuth::new(None), None))
}

// py: tests/test_merge_detect.py::test_titles_endpoint_lists_titles_with_their_sources
#[tokio::test]
async fn titles_endpoint_lists_titles_with_their_sources() {
    let tmp = tempfile::tempdir().expect("tempdir");
    parts(
        tmp.path(),
        "type/vr/SQTEVR/SQTEVR-009",
        "SQTEVR-009",
        20,
        "type/vr",
        &[],
    );
    parts(tmp.path(), "type/x/DEF/DEF-001", "DEF-001", 2, "type/x", &[]);
    let client = titles_client(tmp.path(), &[("DEF-001", "/115/embyx_in/special")], None);

    let (_, body) = call(&client, Method::GET, "/api/merge/titles", &[], None).await;

    assert_eq!(
        field(&body, "routes"),
        &json(r#"["vr", "clt", "hh", "rank", "rest", "special"]"#)
    );
    assert_eq!(field(&body, "reason"), &Value::Null);
    assert_ne!(field(&body, "scanned_at"), &Value::Null);
    let [first, second] = items(&body).try_into().expect("two titles");
    let parts: Vec<String> = (1..=20).map(|index| index.to_string()).collect();
    assert_eq!(
        first,
        json(&format!(
            r#"{{"avid": "SQTEVR-009", "directory": "type/vr/SQTEVR/SQTEVR-009", "part_count": 20,
                "parts": [{}], "missing": [], "library_dir": "type/vr", "brand": "SQTEVR",
                "problem": null, "source": "vr", "source_basis": "library", "stackable": false,
                "mergeable": true}}"#,
            parts.join(", ")
        ))
    );
    assert_eq!(
        (
            text(&second, "avid"),
            text(&second, "source"),
            text(&second, "source_basis"),
            field(&second, "stackable")
        ),
        (
            "DEF-001".to_owned(),
            "special".to_owned(),
            "ledger".to_owned(),
            &Value::Bool(true)
        )
    );
}

// py: tests/test_merge_detect.py::test_titles_endpoint_explains_missing_configuration
#[tokio::test]
async fn titles_endpoint_explains_missing_configuration() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let client = titles_client(tmp.path(), &[], Some(""));

    let (_, body) = call(&client, Method::GET, "/api/merge/titles", &[], None).await;

    assert_eq!(field(&body, "items"), &Value::List(Vec::new()));
    assert_eq!(field(&body, "scanned_at"), &Value::Null);
    assert_eq!(
        text(&body, "reason"),
        "mapping.dst_dir and archive.dst_dir must be configured"
    );
}

/// The token is checked after the body is read and before it is validated, and a task id
/// beyond `int64` is a validation error (FastAPI's order; D106).
#[tokio::test]
async fn mutations_check_the_body_then_the_token_then_the_fields() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let test = TestDatabase::create().await;
    let repository = MergeTaskRepository::new(test.database());
    let service = MergeService::new(ServiceParts::new(
        repository.clone(),
        Arc::new(|| None),
        None,
        Arc::new(MergeConfig::default),
        Arc::new(archive),
        Arc::new(CloudDriveConfig::default),
        Arc::new(|_| Box::pin(async { Ok(()) })),
    ));
    let client = app(create_merge_router(
        catalog(tmp.path(), &[]),
        Some(MergeTasksApi {
            service,
            auto_status: None,
        }),
        MutationAuth::new(Some("secret")),
        None,
    ));
    let bearer = [("authorization", "Bearer secret")];

    let (status, _) = call(&client, Method::POST, "/api/merge/tasks", &[], Some("{")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let (status, _) = call(&client, Method::POST, "/api/merge/tasks", &[], Some("{}")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(&client, Method::POST, "/api/merge/tasks", &bearer, Some("{}")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let long = format!(r#"{{"avid": "{}"}}"#, "A".repeat(65));
    let (status, _) = call(&client, Method::POST, "/api/merge/tasks", &bearer, Some(&long)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let (status, body) = call(
        &client,
        Method::POST,
        "/api/merge/tasks",
        &bearer,
        Some(r#"{"avid": "X-1"}"#),
    )
    .await;
    assert_eq!(
        (status, body),
        (
            StatusCode::CONFLICT,
            json(r#"{"error": {"code": "merge_unavailable"}}"#)
        )
    );

    let (status, _) = call(&client, Method::POST, "/api/merge/tasks/1/cancel", &[], None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(&client, Method::POST, "/api/merge/tasks/x/cancel", &bearer, None).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let (status, _) = call(
        &client,
        Method::POST,
        "/api/merge/tasks/9223372036854775808/retry",
        &bearer,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let (status, _) = call(&client, Method::GET, "/api/merge/titles?refresh=maybe", &[], None).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    repository.database().close().await;
    test.drop_database().await;
}
