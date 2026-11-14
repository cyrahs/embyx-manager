//! Ported from `tests/test_fill_actor_service.py`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use chrono::TimeDelta;

use super::{
    Captured, Db, Event, FakeCatalog, FakeGateway, MappingBrands, MutableClock, Setup, create_move_candidate, library,
    read, strings,
};
use crate::cloud_moves::BoxFuture;
use crate::errors::FillActorErrorKind;
use crate::fs::recording::{Fault, RecordingMoveFs};
use crate::fs::{EEXIST, EINVAL, EPERM, PyOsError};
use crate::models::{ApplyState, FillActorPlan, MoveCandidate, MoveState, VideoState};
use crate::ports::{AcquisitionOutcome, ActorCatalog, CatalogError, CatalogListing, PageProgressCallback};
use crate::records::{JobProgressEvent, JobProgressUnit, JobStage};
use crate::service::{
    FillActorPaths, FillActorRuntime, FillActorService, MAX_ERROR_MESSAGE_LENGTH, ProgressCallback, ServiceError,
    ServiceOptions, ServiceParts,
};

/// A temporary library and a database.
struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    paths: FillActorPaths,
    db: Db,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let paths = library(&root);
        Self {
            _dir: dir,
            root,
            paths,
            db: Db::new().await,
        }
    }

    /// `make_service(paths, catalog=..., brands=..., ...)`.
    fn setup(&self, catalog: &[(&str, &[&str])], brands: &[(&str, Option<&str>)]) -> Setup {
        Setup::new(&self.paths, FakeCatalog::new(catalog), MappingBrands::new(brands))
    }

    fn build(&self, setup: Setup) -> FillActorService {
        setup.build(self.db.repository())
    }

    /// `create_move_plan(paths, video_id=..., ...)`, with apply enabled.
    async fn move_plan(&self, setup: Setup) -> (FillActorService, FillActorPlan, MoveCandidate) {
        let service = self.build(setup);
        let plan = create_plan(&service, &["actor"]).await;
        let candidate = plan.videos[0].move_candidates[0].clone();
        (service, plan, candidate)
    }

    fn abc(&self) -> Setup {
        self.setup(&[("actor", &["ABC-001"])], &[("ABC-001", Some("ABC"))])
            .apply_enabled()
    }

    async fn finish(self) {
        self.db.finish().await;
    }
}

async fn create_plan(service: &FillActorService, actors: &[&str]) -> FillActorPlan {
    service.create_plan(&strings(actors), None, None, None).await.unwrap()
}

async fn apply(
    service: &FillActorService,
    plan: &FillActorPlan,
    candidates: &[&MoveCandidate],
) -> Result<crate::models::ApplyResult, ServiceError> {
    let ids: Vec<String> = candidates
        .iter()
        .map(|candidate| candidate.candidate_id.clone())
        .collect();
    service.apply(&plan.plan_id, &plan.revision, &ids, None, None).await
}

fn videos(plan: &FillActorPlan) -> HashMap<String, crate::models::VideoPlan> {
    plan.videos
        .iter()
        .map(|video| (video.video_id.clone(), video.clone()))
        .collect()
}

fn kind(error: &ServiceError) -> FillActorErrorKind {
    error.as_fill_actor().expect("a FillActorError").kind
}

fn collector() -> (ProgressCallback, Arc<Mutex<Vec<JobProgressEvent>>>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let writer = Arc::clone(&events);
    let callback: ProgressCallback = Arc::new(move |event| {
        writer.lock().unwrap_or_else(PoisonError::into_inner).push(event);
        Box::pin(async {})
    });
    (callback, events)
}

fn take(events: &Arc<Mutex<Vec<JobProgressEvent>>>) -> Vec<JobProgressEvent> {
    std::mem::take(&mut *events.lock().unwrap_or_else(PoisonError::into_inner))
}

// py: tests/test_fill_actor_service.py::test_managed_scan_cancellation_waits_for_worker_exit_and_wins_worker_error
#[tokio::test]
async fn managed_scan_cancellation_waits_for_worker_exit_and_wins_worker_error() {
    let fixture = Fixture::new().await;
    let service = fixture.build(fixture.setup(&[("actor", &[])], &[]));
    let (started, stop_seen, release, exited) =
        (Event::default(), Event::default(), Event::default(), Event::default());

    let scan = {
        let (service, started, stop_seen, release, exited) = (
            service.clone(),
            started.clone(),
            stop_seen.clone(),
            release.clone(),
            exited.clone(),
        );
        tokio::spawn(async move {
            service
                .run_scan(move |stop| -> Result<Vec<PathBuf>, PyOsError> {
                    started.set();
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
                    while !stop.load(Ordering::SeqCst) {
                        assert!(std::time::Instant::now() < deadline, "scan stop was not delivered");
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    stop_seen.set();
                    assert!(release.wait(2), "test did not release scan worker");
                    exited.set();
                    Err(PyOsError::message("NFS failed while the cancelled scan was unwinding"))
                })
                .await
        })
    };
    assert!(started.wait_async(1).await);
    scan.abort();
    // The dropped scan sets the stop flag; the thread finishes on its own and
    // `settle_scans` is what waits for it (D123).
    assert!(stop_seen.wait_async(1).await);
    let settle = {
        let service = service.clone();
        tokio::spawn(async move { service.settle_scans().await })
    };
    tokio::task::yield_now().await;
    assert!(!settle.is_finished());
    assert_eq!(service.scan_workers(), 1);
    release.set();

    // Cancellation wins over the worker's error.
    assert!(scan.await.unwrap_err().is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(1), settle)
        .await
        .unwrap()
        .unwrap();
    assert!(exited.is_set());
    assert_eq!(service.scan_workers(), 0);
    fixture.finish().await;
}

/// The real file system, but `iterdir` of one directory runs `hook` first: where Python's test
/// monkeypatches `_find_matching_files` to block.
#[derive(Default)]
struct BlockingIterdir {
    path: Mutex<Option<PathBuf>>,
    #[allow(clippy::type_complexity, reason = "a test hook")]
    hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl std::fmt::Debug for BlockingIterdir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BlockingIterdir")
    }
}

impl crate::fs::MoveFs for BlockingIterdir {
    fn stat(&self, path: &std::path::Path, follow_symlinks: bool) -> Result<crate::fs::Stat, PyOsError> {
        crate::fs::LocalMoveFs.stat(path, follow_symlinks)
    }

    fn readlink(&self, path: &std::path::Path) -> Result<std::ffi::OsString, PyOsError> {
        crate::fs::LocalMoveFs.readlink(path)
    }

    fn iterdir(&self, path: &std::path::Path) -> Result<Vec<PathBuf>, PyOsError> {
        let blocked = self.path.lock().unwrap_or_else(PoisonError::into_inner).as_deref() == Some(path);
        let hook = self.hook.lock().unwrap_or_else(PoisonError::into_inner).clone();
        if blocked && let Some(hook) = hook {
            hook();
            return Ok(Vec::new());
        }
        crate::fs::LocalMoveFs.iterdir(path)
    }

    fn mkdir(&self, path: &std::path::Path, mode: u32) -> Result<(), PyOsError> {
        crate::fs::LocalMoveFs.mkdir(path, mode)
    }

    fn rename_noreplace(&self, source: &std::path::Path, destination: &std::path::Path) -> Result<(), PyOsError> {
        crate::fs::LocalMoveFs.rename_noreplace(source, destination)
    }

    fn link(&self, source: &std::path::Path, destination: &std::path::Path) -> Result<(), PyOsError> {
        crate::fs::LocalMoveFs.link(source, destination)
    }

    fn rename(&self, source: &std::path::Path, destination: &std::path::Path) -> Result<(), PyOsError> {
        crate::fs::LocalMoveFs.rename(source, destination)
    }

    fn unlink(&self, path: &std::path::Path) -> Result<(), PyOsError> {
        crate::fs::LocalMoveFs.unlink(path)
    }

    fn read_limited(&self, path: &std::path::Path, limit: usize) -> Result<Vec<u8>, PyOsError> {
        crate::fs::LocalMoveFs.read_limited(path, limit)
    }

    fn getcwd(&self) -> Result<PathBuf, PyOsError> {
        crate::fs::LocalMoveFs.getcwd()
    }
}

// py: tests/test_fill_actor_service.py::test_job_cancel_does_not_return_before_managed_scan_worker_exits
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn job_cancel_does_not_return_before_managed_scan_worker_exits() {
    use crate::jobs::{FillActorJobManager, JobManagerOptions, JobRepository, JobService};
    use crate::records::CancelJobOutcome;

    let fixture = Fixture::new().await;
    let fs = Arc::new(BlockingIterdir::default());
    let mut setup = fixture.setup(&[("actor", &["ABC-001"])], &[("ABC-001", Some("ABC"))]);
    setup.options.fs = fs.clone();
    let repository = fixture.db.repository();
    let service = setup.build(Arc::clone(&repository));
    let manager = FillActorJobManager::new(
        Arc::new(service.clone()) as Arc<dyn JobService>,
        repository as Arc<dyn JobRepository>,
        JobManagerOptions {
            max_concurrent_jobs: 1,
            max_active_jobs: 1,
            poll_interval: std::time::Duration::from_millis(10),
            ..JobManagerOptions::default()
        },
    )
    .unwrap();
    let (started, stop_seen, release, exited) =
        (Event::default(), Event::default(), Event::default(), Event::default());

    // `blocked_scan`: the scan of the actor root's brand directory waits for its stop.
    let brand = fixture.paths.actor_brand_path.join("ABC");
    std::fs::create_dir(&brand).unwrap();
    *fs.path.lock().unwrap() = Some(brand);
    *fs.hook.lock().unwrap() = Some({
        let (service, started, stop_seen, release, exited) = (
            service.clone(),
            started.clone(),
            stop_seen.clone(),
            release.clone(),
            exited.clone(),
        );
        Arc::new(move || {
            started.set();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while !service.scan_stop_requested() {
                assert!(std::time::Instant::now() < deadline, "scan stop was not delivered");
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            stop_seen.set();
            assert!(release.wait(2), "test did not release scan worker");
            exited.set();
        })
    });
    let job = manager.start_plan(&strings(&["actor"])).await.unwrap();
    assert!(started.wait_async(1).await);
    let cancel = tokio::spawn({
        let (manager, job_id) = (manager.clone(), job.job_id.clone());
        async move { manager.cancel_plan(&job_id).await }
    });
    let seen = stop_seen.wait_async(1).await;
    tokio::task::yield_now().await;
    let returned_early = cancel.is_finished();
    release.set();
    assert!(seen);
    assert!(!returned_early);

    let result = tokio::time::timeout(std::time::Duration::from_secs(1), cancel)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.outcome, CancelJobOutcome::Cancelled);
    assert!(exited.is_set());
    assert_eq!(service.scan_workers(), 0);
    // The hook holds a clone of the service; let it go before the service closes.
    *fs.hook.lock().unwrap() = None;
    manager.aclose().await;
    service.aclose().await;
    fixture.finish().await;
}

/// `PageProgressActorCatalog`.
struct PageProgressCatalog;

impl ActorCatalog for PageProgressCatalog {
    fn list_videos<'a>(
        &'a self,
        _actor_ref: &'a str,
        progress_callback: Option<&'a PageProgressCallback>,
    ) -> BoxFuture<'a, Result<CatalogListing, CatalogError>> {
        Box::pin(async move {
            let progress = progress_callback.expect("a page progress callback");
            progress(0, None, None).await;
            progress(0, Some(26), None).await;
            progress(1, Some(26), Some(1)).await;
            progress(26, Some(26), Some(26)).await;
            Ok(CatalogListing {
                video_ids: strings(&["ABC-001", "ABC-002"]),
                ..CatalogListing::default()
            })
        })
    }
}

// py: tests/test_fill_actor_service.py::test_create_plan_reports_durable_stage_and_page_progress
#[tokio::test]
async fn create_plan_reports_durable_stage_and_page_progress() {
    let fixture = Fixture::new().await;
    let mut setup = fixture.setup(&[], &[("ABC-001", Some("ABC")), ("ABC-002", Some("ABC"))]);
    setup.catalog = Arc::new(PageProgressCatalog);
    let service = fixture.build(setup);
    let (report, events) = collector();

    service
        .create_plan(&strings(&["actor"]), None, None, Some(report))
        .await
        .unwrap();

    let events = take(&events);
    let mut transitions: Vec<JobStage> = Vec::new();
    for event in &events {
        if transitions.last() != Some(&event.stage) {
            transitions.push(event.stage);
        }
    }
    assert_eq!(
        transitions,
        [
            JobStage::ActorCatalog,
            JobStage::LibraryScan,
            JobStage::Submitting,
            JobStage::Persisting
        ]
    );
    assert_eq!(events[0].stage, JobStage::ActorCatalog);
    let pages: Vec<&JobProgressEvent> = events
        .iter()
        .filter(|event| event.unit == JobProgressUnit::Pages)
        .collect();
    assert_eq!(
        pages
            .iter()
            .map(|event| (event.completed, event.total))
            .collect::<Vec<_>>(),
        [(0, None), (0, Some(26)), (1, Some(26)), (26, Some(26))]
    );
    assert!(pages[2].current.as_deref().unwrap().contains("页面 1/26"));
    assert!(pages[3].current.as_deref().unwrap().contains("页面 26/26"));
    let actors: Vec<&JobProgressEvent> = events
        .iter()
        .filter(|event| event.stage == JobStage::ActorCatalog && event.unit == JobProgressUnit::Actors)
        .collect();
    let last = actors.last().unwrap();
    assert_eq!((last.completed, last.total), (1, Some(1)));
    let stage = |stage: JobStage| {
        events
            .iter()
            .filter(|event| event.stage == stage)
            .map(|event| (event.completed, event.total))
            .collect::<Vec<_>>()
    };
    assert_eq!(stage(JobStage::LibraryScan), [(0, Some(2)), (1, Some(2)), (2, Some(2))]);
    assert_eq!(stage(JobStage::Submitting), [(0, Some(2)), (1, Some(2)), (2, Some(2))]);
    assert_eq!(
        events[events.len() - 2..]
            .iter()
            .map(|event| (event.stage, event.completed, event.total))
            .collect::<Vec<_>>(),
        [(JobStage::Persisting, 0, Some(1)), (JobStage::Persisting, 1, Some(1))]
    );
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_create_plan_classifies_and_sorts_results
#[tokio::test]
async fn create_plan_classifies_and_sorts_results() {
    let fixture = Fixture::new().await;
    let paths = &fixture.paths;
    let actor_brand = paths.actor_brand_path.join("ABC");
    std::fs::create_dir(&actor_brand).unwrap();
    std::fs::write(actor_brand.join("ABC-001.mp4"), b"existing").unwrap();
    for (index, root) in paths.additional_brand_paths.iter().enumerate() {
        let brand = root.join("ABC");
        std::fs::create_dir(&brand).unwrap();
        std::fs::write(
            brand.join(format!("ABC-002-cd{}.mp4", index + 1)),
            format!("part-{}", index + 1),
        )
        .unwrap();
    }
    let setup = fixture
        .setup(
            &[
                ("actor-a", &["abc-003", "abc-001_2026-07-01", "abc-002", "bad"]),
                ("actor-b", &["abc-002"]),
            ],
            &[
                ("ABC-001", Some("ABC")),
                ("ABC-002", Some("ABC")),
                ("ABC-003", Some("ABC")),
                ("BAD", None),
            ],
        )
        .gateway(FakeGateway::new(&[("ABC-003", AcquisitionOutcome::Submitted)]));
    let gateway = Arc::clone(&setup.gateway);
    let service = fixture.build(setup);

    let plan = create_plan(&service, &["actor-a", "actor-b", "actor-a"]).await;

    assert_eq!(
        plan.actors
            .iter()
            .map(|actor| actor.actor_id.as_str())
            .collect::<Vec<_>>(),
        ["actor-a", "actor-b"]
    );
    assert_eq!(plan.actors[0].video_ids, ["ABC-001", "ABC-002", "ABC-003", "BAD"]);
    assert_eq!(
        plan.videos
            .iter()
            .map(|video| video.video_id.as_str())
            .collect::<Vec<_>>(),
        ["ABC-001", "ABC-002", "ABC-003", "BAD"]
    );
    let videos = videos(&plan);
    assert_eq!(videos["ABC-001"].state, VideoState::Exists);
    assert_eq!(videos["ABC-001"].existing_files, ["ABC-001.mp4"]);
    assert_eq!(videos["ABC-002"].state, VideoState::AdditionalFound);
    let candidates = &videos["ABC-002"].move_candidates;
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.file_name.as_str())
            .collect::<Vec<_>>(),
        ["ABC-002-cd1.mp4", "ABC-002-cd2.mp4"]
    );
    assert!(!candidates.iter().any(|candidate| candidate.destination_conflict));
    assert_eq!(videos["ABC-002"].actor_ids, ["actor-a", "actor-b"]);
    assert_eq!(videos["ABC-003"].state, VideoState::Submitted);
    assert_eq!(videos["BAD"].state, VideoState::InvalidVideoId);
    assert_eq!(gateway.calls(), ["ABC-003"]);
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.source_label.as_str())
            .collect::<Vec<_>>(),
        [
            paths.additional_brand_paths[0].to_str().unwrap(),
            paths.additional_brand_paths[1].to_str().unwrap()
        ]
    );
    assert!(!plan.dump_json().contains(paths.actor_brand_path.to_str().unwrap()));
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_create_plan_preserves_partial_external_failures
#[tokio::test]
async fn create_plan_preserves_partial_external_failures() {
    let fixture = Fixture::new().await;
    let mut setup = fixture.setup(&[], &[("ABC-001", Some("ABC")), ("ABC-002", Some("ABC"))]);
    setup.catalog = Arc::new(FakeCatalog::new(&[("working", &["abc-001", "abc-002"])]).failing(
        "broken",
        "RuntimeError",
        "upstream unavailable",
    ));
    let mut gateway = FakeGateway::default();
    gateway
        .values
        .insert("ABC-002".to_owned(), Err("search failed".to_owned()));
    let service = fixture.build(setup.gateway(gateway));

    let plan = create_plan(&service, &["broken", "working"]).await;

    assert_eq!(plan.actors[0].error_code.as_deref(), Some("actor_catalog_error"));
    assert_eq!(
        plan.actors[0].error_message.as_deref(),
        Some("RuntimeError: upstream unavailable")
    );
    assert_eq!(plan.actors[1].error_code, None);
    assert_eq!(plan.actors[1].error_message, None);
    let videos = videos(&plan);
    assert_eq!(videos["ABC-001"].state, VideoState::Missing);
    assert!(videos["ABC-001"].warnings.is_empty());
    assert_eq!(videos["ABC-002"].state, VideoState::SubmitFailed);
    assert_eq!(videos["ABC-002"].warnings, ["acquisition_failed"]);
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_actor_catalog_failure_is_logged_with_job_and_actor
#[tokio::test]
async fn actor_catalog_failure_is_logged_with_job_and_actor() {
    let fixture = Fixture::new().await;
    let mut setup = fixture.setup(&[], &[]);
    setup.catalog = Arc::new(FakeCatalog::new(&[]).failing("broken", "RuntimeError", "upstream unavailable"));
    let service = fixture.build(setup);
    let captured = Captured::default();

    {
        let _guard = captured.install();
        service
            .create_plan(&strings(&["broken"]), Some("job-42"), None, None)
            .await
            .unwrap();
    }

    let events = captured.events();
    let record = events
        .iter()
        .find(|event| {
            event
                .get("message")
                .is_some_and(|message| message.contains("actor catalog failed"))
        })
        .expect("the failure is logged");
    assert_eq!(record["level"], "ERROR");
    assert_eq!(record["logger"], "embyx_manager.fill_actor.service");
    assert_eq!(record["job_id"], "job-42");
    assert_eq!(record["actor_id"], "broken");
    assert_eq!(record["error_type"], "RuntimeError");
    // The exception's text instead of the traceback (D60).
    assert_eq!(record["error"], "RuntimeError: upstream unavailable");
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_actor_catalog_failure_message_is_redacted_and_truncated
#[tokio::test]
async fn actor_catalog_failure_message_is_redacted_and_truncated() {
    let fixture = Fixture::new().await;
    let leaky = format!(
        "GET https://catalog.invalid/actors?token=super-secret failed; Authorization: Bearer abcdef123456; cookie=session-value; {}",
        "x".repeat(400)
    );
    let mut setup = fixture.setup(&[], &[]);
    setup.catalog = Arc::new(FakeCatalog::new(&[]).failing("broken", "RuntimeError", &leaky));
    let service = fixture.build(setup);

    let plan = create_plan(&service, &["broken"]).await;

    let message = plan.actors[0].error_message.clone().unwrap();
    assert!(message.chars().count() <= MAX_ERROR_MESSAGE_LENGTH);
    assert!(message.starts_with("RuntimeError: GET https://catalog.invalid/actors?[redacted]"));
    for secret in ["super-secret", "abcdef123456", "session-value"] {
        assert!(!message.contains(secret));
    }
    fixture.finish().await;
}

/// `make_service(..., outcomes={'ABC-001': outcome})` and one scan of `ABC-001`.
async fn plan_with_outcome(outcome: AcquisitionOutcome) -> (Fixture, FillActorPlan, Vec<String>) {
    let fixture = Fixture::new().await;
    let setup = fixture
        .setup(&[("actor", &["ABC-001"])], &[("ABC-001", Some("ABC"))])
        .gateway(FakeGateway::new(&[("ABC-001", outcome)]));
    let gateway = Arc::clone(&setup.gateway);
    let service = fixture.build(setup);
    let plan = create_plan(&service, &["actor"]).await;
    let calls = gateway.calls();
    (fixture, plan, calls)
}

// py: tests/test_fill_actor_service.py::test_create_plan_queues_missing_videos_for_the_background_pass
#[tokio::test]
async fn create_plan_queues_missing_videos_for_the_background_pass() {
    let (fixture, plan, calls) = plan_with_outcome(AcquisitionOutcome::Queued).await;
    assert_eq!(calls, ["ABC-001"]);
    assert_eq!(plan.videos[0].state, VideoState::Queued);
    assert!(plan.videos[0].warnings.is_empty());
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_create_plan_reports_submit_failures
#[tokio::test]
async fn create_plan_reports_submit_failures() {
    let (fixture, plan, _) = plan_with_outcome(AcquisitionOutcome::SubmitFailed).await;
    assert_eq!(plan.videos[0].state, VideoState::SubmitFailed);
    assert_eq!(plan.videos[0].warnings, ["submit_failed"]);
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_create_plan_names_the_missing_configuration cases=2
#[tokio::test]
async fn create_plan_names_the_missing_configuration() {
    for (outcome, warning) in [
        (AcquisitionOutcome::CloudNotConfigured, "cloud_not_configured"),
        (AcquisitionOutcome::TaskDirNotConfigured, "task_dir_not_configured"),
    ] {
        let (fixture, plan, _) = plan_with_outcome(outcome).await;
        assert_eq!(plan.videos[0].state, VideoState::SubmitFailed);
        assert_eq!(plan.videos[0].warnings, [warning]);
        fixture.finish().await;
    }
}

// py: tests/test_fill_actor_service.py::test_create_plan_reports_already_tracked_videos
#[tokio::test]
async fn create_plan_reports_already_tracked_videos() {
    let (fixture, plan, _) = plan_with_outcome(AcquisitionOutcome::AlreadyTracked).await;
    assert_eq!(plan.videos[0].state, VideoState::AlreadyTracked);
    assert!(plan.videos[0].warnings.is_empty());
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_create_plan_rejects_brand_path_escape
#[tokio::test]
async fn create_plan_rejects_brand_path_escape() {
    let fixture = Fixture::new().await;
    let setup = fixture.setup(&[("actor", &["ABC-001"])], &[("ABC-001", Some("../../outside"))]);
    let gateway = Arc::clone(&setup.gateway);
    let service = fixture.build(setup);

    let plan = create_plan(&service, &["actor"]).await;

    assert_eq!(plan.videos[0].state, VideoState::InvalidVideoId);
    assert!(gateway.calls().is_empty());
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_create_plan_does_not_follow_brand_directory_symlinks cases=2
#[tokio::test]
async fn create_plan_does_not_follow_brand_directory_symlinks() {
    for root_kind in ["actor", "additional"] {
        let fixture = Fixture::new().await;
        let outside = fixture.root.join(format!("outside-{root_kind}"));
        std::fs::create_dir(&outside).unwrap();
        let outside_file_name = "ABC-001-outside.mp4";
        std::fs::write(outside.join(outside_file_name), b"outside").unwrap();
        let root = if root_kind == "actor" {
            &fixture.paths.actor_brand_path
        } else {
            &fixture.paths.additional_brand_paths[0]
        };
        std::os::unix::fs::symlink(&outside, root.join("ABC")).unwrap();
        let service = fixture.build(fixture.setup(&[("actor", &["ABC-001"])], &[("ABC-001", Some("ABC"))]));

        let plan = create_plan(&service, &["actor"]).await;

        assert_eq!(plan.videos[0].state, VideoState::ScanFailed, "{root_kind}");
        assert_eq!(plan.videos[0].warnings, ["scan_failed"]);
        assert!(!plan.dump_json().contains(outside_file_name));
        fixture.finish().await;
    }
}

// py: tests/test_fill_actor_service.py::test_create_plan_reports_unavailable_scan_root cases=2
#[tokio::test]
async fn create_plan_reports_unavailable_scan_root() {
    for root_kind in ["actor", "additional"] {
        let fixture = Fixture::new().await;
        let root = if root_kind == "actor" {
            &fixture.paths.actor_brand_path
        } else {
            &fixture.paths.additional_brand_paths[0]
        };
        std::fs::remove_dir(root).unwrap();
        let setup = fixture.setup(&[("actor", &["ABC-001"])], &[("ABC-001", Some("ABC"))]);
        let gateway = Arc::clone(&setup.gateway);
        let service = fixture.build(setup);

        let plan = create_plan(&service, &["actor"]).await;

        assert_eq!(plan.videos[0].state, VideoState::ScanFailed, "{root_kind}");
        assert_eq!(plan.videos[0].warnings, ["scan_failed"]);
        assert!(gateway.calls().is_empty());
        fixture.finish().await;
    }
}

// py: tests/test_fill_actor_service.py::test_create_plan_rejects_invalid_actor_ids cases=3
#[tokio::test]
async fn create_plan_rejects_invalid_actor_ids() {
    let fixture = Fixture::new().await;
    let service = fixture.build(fixture.setup(&[], &[]));
    let long = "x".repeat(65);
    for actor_ids in [Vec::new(), strings(&["bad/actor"]), vec![long]] {
        let error = service.create_plan(&actor_ids, None, None, None).await.unwrap_err();
        assert_eq!(kind(&error), FillActorErrorKind::InvalidActorId, "{actor_ids:?}");
    }
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_create_plan_enforces_actor_limit
#[tokio::test]
async fn create_plan_enforces_actor_limit() {
    let fixture = Fixture::new().await;
    let mut setup = fixture.setup(&[], &[]);
    setup.options.max_actors = 1;
    let service = fixture.build(setup);

    let error = service
        .create_plan(&strings(&["actor-a", "actor-b"]), None, None, None)
        .await
        .unwrap_err();

    assert_eq!(kind(&error), FillActorErrorKind::TooManyActors);
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_disabled_apply_does_not_change_scan_results
#[tokio::test]
async fn disabled_apply_does_not_change_scan_results() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let setup = fixture.setup(&[("actor", &["ABC-001"])], &[("ABC-001", Some("ABC"))]);
    let (service, plan, candidate) = fixture.move_plan(setup).await;

    assert_eq!(plan.videos[0].move_candidates, std::slice::from_ref(&candidate));
    assert!(!service.apply_enabled());
    let error = apply(&service, &plan, &[&candidate]).await.unwrap_err();
    assert_eq!(kind(&error), FillActorErrorKind::MoveDisabled);

    assert_eq!(read(&source), b"video");
    assert!(!fixture.paths.move_in_path.join("ABC-001.mp4").exists());
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_disabled_apply_does_not_affect_submission
#[tokio::test]
async fn disabled_apply_does_not_affect_submission() {
    let fixture = Fixture::new().await;
    let setup = fixture
        .setup(&[("actor", &["ABC-001"])], &[("ABC-001", Some("ABC"))])
        .gateway(FakeGateway::new(&[("ABC-001", AcquisitionOutcome::Submitted)]));
    let gateway = Arc::clone(&setup.gateway);
    let service = fixture.build(setup);

    let plan = create_plan(&service, &["actor"]).await;

    assert!(!service.apply_enabled());
    assert_eq!(gateway.calls(), ["ABC-001"]);
    assert_eq!(plan.videos[0].state, VideoState::Submitted);
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_moves_candidate_and_is_idempotent
#[tokio::test]
async fn apply_moves_candidate_and_is_idempotent() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let (service, plan, candidate) = fixture.move_plan(fixture.abc()).await;

    let selected = [&candidate];
    let (first, second) = tokio::join!(apply(&service, &plan, &selected), apply(&service, &plan, &selected));
    let (first, second) = (first.unwrap(), second.unwrap());

    assert_eq!(first.state, ApplyState::Succeeded);
    assert_eq!(second.state, ApplyState::Succeeded);
    assert_eq!(first.results[0].state, MoveState::Moved);
    assert_eq!(second.results[0].state, MoveState::Moved);
    assert!(!source.exists());
    assert_eq!(read(&fixture.paths.move_in_path.join("ABC-001.mp4")), b"video");
    fixture.finish().await;
}

fn counting_on_moved(setup: &mut Setup) -> Arc<AtomicUsize> {
    let notified = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&notified);
    setup.options.on_moved = Some(Arc::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }));
    notified
}

// py: tests/test_fill_actor_service.py::test_apply_notifies_when_files_moved
#[tokio::test]
async fn apply_notifies_when_files_moved() {
    let fixture = Fixture::new().await;
    create_move_candidate(&fixture.paths, "ABC-001");
    let mut setup = fixture.abc();
    let notified = counting_on_moved(&mut setup);
    let (service, plan, candidate) = fixture.move_plan(setup).await;

    apply(&service, &plan, &[&candidate]).await.unwrap();

    assert_eq!(notified.load(Ordering::SeqCst), 1);
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_does_not_notify_when_nothing_moved
#[tokio::test]
async fn apply_does_not_notify_when_nothing_moved() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let mut setup = fixture.abc();
    let notified = counting_on_moved(&mut setup);
    let (service, plan, candidate) = fixture.move_plan(setup).await;
    std::fs::write(&source, b"changed after the scan").unwrap();

    let result = apply(&service, &plan, &[&candidate]).await.unwrap();

    assert_eq!(result.results[0].state, MoveState::Stale);
    assert_eq!(notified.load(Ordering::SeqCst), 0);
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_survives_a_failing_moved_notification
#[tokio::test]
async fn apply_survives_a_failing_moved_notification() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let mut setup = fixture.abc();
    setup.options.on_moved = Some(Arc::new(|| Err("RuntimeError: the scheduler is gone".to_owned())));
    let (service, plan, candidate) = fixture.move_plan(setup).await;

    let result = apply(&service, &plan, &[&candidate]).await.unwrap();

    assert_eq!(result.state, ApplyState::Succeeded);
    assert!(!source.exists());
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_reports_file_level_progress_and_stops_between_candidates
#[tokio::test]
async fn apply_reports_file_level_progress_and_stops_between_candidates() {
    let fixture = Fixture::new().await;
    let first_source = create_move_candidate(&fixture.paths, "ABC-001");
    let second_source = create_move_candidate(&fixture.paths, "ABC-002");
    let service = fixture.build(
        fixture
            .setup(
                &[("actor", &["ABC-001", "ABC-002"])],
                &[("ABC-001", Some("ABC")), ("ABC-002", Some("ABC"))],
            )
            .apply_enabled(),
    );
    let plan = create_plan(&service, &["actor"]).await;
    let ids: Vec<String> = plan
        .videos
        .iter()
        .flat_map(|video| {
            video
                .move_candidates
                .iter()
                .map(|candidate| candidate.candidate_id.clone())
        })
        .collect();
    let events = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let progress: ProgressCallback = {
        let (events, stop) = (Arc::clone(&events), Arc::clone(&stop));
        Arc::new(move |event: JobProgressEvent| {
            if event.completed == 1 {
                stop.store(true, Ordering::SeqCst);
            }
            events.lock().unwrap_or_else(PoisonError::into_inner).push(event);
            Box::pin(async {})
        })
    };
    let stop_requested = {
        let stop = Arc::clone(&stop);
        Arc::new(move || stop.load(Ordering::SeqCst))
    };

    let result = service
        .apply(
            &plan.plan_id,
            &plan.revision,
            &ids,
            Some(progress),
            Some(stop_requested),
        )
        .await
        .unwrap();

    let summary = |events: &[JobProgressEvent]| {
        events
            .iter()
            .map(|event| (event.completed, event.total, event.current.clone()))
            .collect::<Vec<_>>()
    };
    let seen = take(&events);
    assert_eq!(
        summary(&seen),
        [
            (0, Some(2), Some("ABC-001.mp4".to_owned())),
            (1, Some(2), Some("ABC-002.mp4".to_owned()))
        ]
    );
    assert!(seen.iter().all(|event| event.stage == JobStage::Unknown));
    assert!(seen.iter().all(|event| event.unit == JobProgressUnit::Items));
    assert_eq!(result.results.len(), 1);
    assert!(!first_source.exists());
    assert!(second_source.exists());

    let (resumed_progress, resumed_events) = collector();
    let resumed = service
        .apply(&plan.plan_id, &plan.revision, &ids, Some(resumed_progress), None)
        .await
        .unwrap();

    assert_eq!(
        summary(&take(&resumed_events)),
        [
            (0, Some(2), Some("ABC-001.mp4".to_owned())),
            (1, Some(2), Some("ABC-002.mp4".to_owned())),
            (2, Some(2), None)
        ]
    );
    assert_eq!(resumed.results.len(), 2);
    assert!(!second_source.exists());
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_can_move_into_brand_subdirectory
#[tokio::test]
async fn apply_can_move_into_brand_subdirectory() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let mut setup = fixture.abc();
    setup.runtime.move_in_by_brand = true;
    let (service, plan, candidate) = fixture.move_plan(setup).await;

    let result = apply(&service, &plan, &[&candidate]).await.unwrap();

    let destination = fixture.paths.move_in_path.join("ABC").join("ABC-001.mp4");
    assert_eq!(result.state, ApplyState::Succeeded);
    assert!(!source.exists());
    assert_eq!(read(&destination), b"video");
    fixture.finish().await;
}

/// The plan of one candidate, on a recording file system that the test then faults.
async fn recorded_move_plan(
    fixture: &Fixture,
) -> (Arc<RecordingMoveFs>, FillActorService, FillActorPlan, MoveCandidate) {
    let fs = Arc::new(RecordingMoveFs::default());
    let (service, plan, candidate) = fixture.move_plan(fixture.abc().fs(Arc::clone(&fs))).await;
    (fs, service, plan, candidate)
}

// py: tests/test_fill_actor_service.py::test_apply_falls_back_to_hardlink_when_renameat2_is_unsupported
#[tokio::test]
async fn apply_falls_back_to_hardlink_when_renameat2_is_unsupported() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let (fs, service, plan, candidate) = recorded_move_plan(&fixture).await;
    fs.add_fault(Fault::errno("rename_noreplace", None, EINVAL));

    let result = apply(&service, &plan, &[&candidate]).await.unwrap();

    assert_eq!(result.results[0].state, MoveState::Moved);
    assert!(!source.exists());
    assert_eq!(read(&fixture.paths.move_in_path.join("ABC-001.mp4")), b"video");
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_hardlink_fallback_preserves_source_replacement
#[tokio::test]
async fn hardlink_fallback_preserves_source_replacement() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let (fs, service, plan, candidate) = recorded_move_plan(&fixture).await;
    fs.add_fault(Fault::errno("rename_noreplace", None, EINVAL));
    let replaced = source.clone();
    fs.set_before(Some(Arc::new(move |op, path, _| {
        if op == "rename" && path == replaced {
            std::fs::remove_file(path).unwrap();
            std::fs::write(path, b"external-replacement").unwrap();
        }
    })));

    let result = apply(&service, &plan, &[&candidate]).await.unwrap();

    assert_eq!(result.results[0].state, MoveState::Moved);
    assert_eq!(read(&source), b"external-replacement");
    assert_eq!(read(&fixture.paths.move_in_path.join("ABC-001.mp4")), b"video");
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_reports_filesystem_without_rename_or_hardlink_support
#[tokio::test]
async fn apply_reports_filesystem_without_rename_or_hardlink_support() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let (fs, service, plan, candidate) = recorded_move_plan(&fixture).await;
    fs.add_fault(Fault::errno("rename_noreplace", None, EINVAL));
    fs.add_fault(Fault::errno("link", None, EPERM));

    let result = apply(&service, &plan, &[&candidate]).await.unwrap();

    assert_eq!(result.results[0].state, MoveState::Failed);
    assert_eq!(result.results[0].error_code.as_deref(), Some("move_unsupported"));
    assert_eq!(read(&source), b"video");
    assert!(!fixture.paths.move_in_path.join("ABC-001.mp4").exists());
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_plan_marks_duplicate_destinations_as_conflicts
#[tokio::test]
async fn plan_marks_duplicate_destinations_as_conflicts() {
    let fixture = Fixture::new().await;
    for root in &fixture.paths.additional_brand_paths {
        let brand = root.join("ABC");
        std::fs::create_dir(&brand).unwrap();
        std::fs::write(brand.join("ABC-001.mp4"), b"video").unwrap();
    }
    let service = fixture.build(fixture.setup(&[("actor", &["ABC-001"])], &[("ABC-001", Some("ABC"))]));

    let plan = create_plan(&service, &["actor"]).await;

    let candidates = &plan.videos[0].move_candidates;
    assert_eq!(candidates.len(), 2);
    assert!(candidates.iter().all(|candidate| candidate.destination_conflict));
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_reports_destination_conflict_without_overwrite
#[tokio::test]
async fn apply_reports_destination_conflict_without_overwrite() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let (service, plan, candidate) = fixture.move_plan(fixture.abc()).await;
    let destination = fixture.paths.move_in_path.join("ABC-001.mp4");
    std::fs::write(&destination, b"keep-me").unwrap();

    let result = apply(&service, &plan, &[&candidate]).await.unwrap();

    assert_eq!(result.state, ApplyState::Failed);
    assert_eq!(result.results[0].state, MoveState::Conflict);
    assert!(source.exists());
    assert_eq!(read(&destination), b"keep-me");
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_reports_partial_failure
#[tokio::test]
async fn apply_reports_partial_failure() {
    let fixture = Fixture::new().await;
    let first = create_move_candidate(&fixture.paths, "ABC-001");
    let second = create_move_candidate(&fixture.paths, "ABC-002");
    let service = fixture.build(
        fixture
            .setup(
                &[("actor", &["ABC-001", "ABC-002"])],
                &[("ABC-001", Some("ABC")), ("ABC-002", Some("ABC"))],
            )
            .apply_enabled(),
    );
    let plan = create_plan(&service, &["actor"]).await;
    let candidates: Vec<&MoveCandidate> = plan.videos.iter().map(|video| &video.move_candidates[0]).collect();
    std::fs::write(fixture.paths.move_in_path.join("ABC-002.mp4"), b"conflict").unwrap();

    let result = apply(&service, &plan, &candidates).await.unwrap();

    assert_eq!(result.state, ApplyState::PartialFailed);
    assert_eq!(
        result.results.iter().map(|item| item.state).collect::<Vec<_>>(),
        [MoveState::Moved, MoveState::Conflict]
    );
    assert!(!first.exists());
    assert!(second.exists());
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_rejects_changed_source_as_stale
#[tokio::test]
async fn apply_rejects_changed_source_as_stale() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let (service, plan, candidate) = fixture.move_plan(fixture.abc()).await;
    std::fs::write(&source, b"changed-size").unwrap();

    let result = apply(&service, &plan, &[&candidate]).await.unwrap();

    assert_eq!(result.results[0].state, MoveState::Stale);
    assert_eq!(result.results[0].error_code.as_deref(), Some("source_changed"));
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_rejects_same_size_replacement_as_stale
#[tokio::test]
async fn apply_rejects_same_size_replacement_as_stale() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let (service, plan, candidate) = fixture.move_plan(fixture.abc()).await;
    let modified = std::fs::metadata(&source).unwrap().modified().unwrap();
    std::fs::remove_file(&source).unwrap();
    std::fs::write(&source, b"video").unwrap();
    std::fs::File::options()
        .write(true)
        .open(&source)
        .unwrap()
        .set_modified(modified)
        .unwrap();

    let result = apply(&service, &plan, &[&candidate]).await.unwrap();

    assert_eq!(result.results[0].state, MoveState::Stale);
    assert_eq!(result.results[0].error_code.as_deref(), Some("source_changed"));
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_rejects_symlink_candidate
#[tokio::test]
async fn apply_rejects_symlink_candidate() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let (service, plan, candidate) = fixture.move_plan(fixture.abc()).await;
    let outside = fixture.root.join("outside.mp4");
    std::fs::write(&outside, b"outside").unwrap();
    std::fs::remove_file(&source).unwrap();
    std::os::unix::fs::symlink(&outside, &source).unwrap();

    let result = apply(&service, &plan, &[&candidate]).await.unwrap();

    assert_eq!(result.results[0].state, MoveState::InvalidPath);
    assert_eq!(result.results[0].error_code.as_deref(), Some("source_symlink"));
    assert_eq!(read(&outside), b"outside");
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_reports_missing_destination_root
#[tokio::test]
async fn apply_reports_missing_destination_root() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let (service, plan, candidate) = fixture.move_plan(fixture.abc()).await;
    std::fs::remove_dir(&fixture.paths.move_in_path).unwrap();

    let result = apply(&service, &plan, &[&candidate]).await.unwrap();

    assert_eq!(result.results[0].state, MoveState::Failed);
    assert_eq!(result.results[0].error_code.as_deref(), Some("roots_unavailable"));
    assert!(source.exists());
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_handles_destination_creation_race
#[tokio::test]
async fn apply_handles_destination_creation_race() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let (fs, service, plan, candidate) = recorded_move_plan(&fixture).await;
    fs.add_fault(Fault::errno("rename_noreplace", None, EEXIST));

    let result = apply(&service, &plan, &[&candidate]).await.unwrap();

    assert_eq!(result.results[0].state, MoveState::Conflict);
    assert_eq!(result.results[0].error_code.as_deref(), Some("destination_exists"));
    assert!(source.exists());
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_atomic_move_never_unlinks_external_replacement cases=2
#[tokio::test]
async fn atomic_move_never_unlinks_external_replacement() {
    for replace_target in ["source", "destination"] {
        let fixture = Fixture::new().await;
        let source = create_move_candidate(&fixture.paths, "ABC-001");
        let (fs, service, plan, candidate) = recorded_move_plan(&fixture).await;
        let destination = fixture.paths.move_in_path.join("ABC-001.mp4");
        let calls = Arc::new(AtomicUsize::new(0));
        {
            let calls = Arc::clone(&calls);
            fs.set_before(Some(Arc::new(move |op, path, _| {
                if op != "rename_noreplace" {
                    return;
                }
                let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if call == 1 && replace_target == "source" {
                    std::fs::remove_file(path).unwrap();
                    std::fs::write(path, b"new-content").unwrap();
                }
            })));
        }
        {
            let calls = Arc::clone(&calls);
            fs.set_after(Some(Arc::new(move |op, _, path2| {
                if op == "rename_noreplace" && calls.load(Ordering::SeqCst) == 1 && replace_target == "destination" {
                    let destination = path2.unwrap();
                    std::fs::remove_file(destination).unwrap();
                    std::fs::write(destination, b"new-content").unwrap();
                }
            })));
        }

        let result = apply(&service, &plan, &[&candidate]).await.unwrap();

        assert_eq!(result.results[0].state, MoveState::Stale, "{replace_target}");
        assert_eq!(
            result.results[0].error_code.as_deref(),
            Some("source_changed_during_move")
        );
        assert_eq!(read(&source), b"new-content");
        assert!(!destination.exists());
        fixture.finish().await;
    }
}

// py: tests/test_fill_actor_service.py::test_apply_converts_unexpected_move_error_to_structured_failure
#[tokio::test]
async fn apply_converts_unexpected_move_error_to_structured_failure() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let (fs, service, plan, candidate) = recorded_move_plan(&fixture).await;
    // `_apply_one` raising: its first look at the source fails.
    fs.add_fault(Fault::message("stat", Some(source), "filesystem unavailable"));

    let result = apply(&service, &plan, &[&candidate]).await.unwrap();

    assert_eq!(result.state, ApplyState::Failed);
    assert_eq!(result.results[0].state, MoveState::Failed);
    assert_eq!(result.results[0].error_code.as_deref(), Some("move_failed"));
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_double_cancellation_keeps_one_in_flight_move
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_double_cancellation_keeps_one_in_flight_move() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let (fs, service, plan, candidate) = recorded_move_plan(&fixture).await;
    let (started, release) = (Event::default(), Event::default());
    let apply_calls = Arc::new(AtomicUsize::new(0));
    {
        let (started, release, apply_calls) = (started.clone(), release.clone(), Arc::clone(&apply_calls));
        fs.set_before(Some(Arc::new(move |op, _, _| {
            if op == "rename_noreplace" {
                apply_calls.fetch_add(1, Ordering::SeqCst);
                started.set();
                assert!(release.wait(2), "test move did not receive release signal");
            }
        })));
    }
    let spawn_apply = || {
        let (service, plan, candidate) = (service.clone(), plan.clone(), candidate.clone());
        tokio::spawn(async move { apply(&service, &plan, &[&candidate]).await })
    };

    let task = spawn_apply();
    assert!(started.wait_async(2).await);
    task.abort();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    let retry_task = spawn_apply();
    tokio::task::yield_now().await;
    assert!(!retry_task.is_finished());
    release.set();
    let retry = retry_task.await.unwrap().unwrap();

    assert_eq!(retry.results[0].state, MoveState::Moved);
    assert_eq!(apply_calls.load(Ordering::SeqCst), 1);
    assert!(!source.exists());
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_validates_all_candidate_ids_before_moving
#[tokio::test]
async fn apply_validates_all_candidate_ids_before_moving() {
    let fixture = Fixture::new().await;
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let (service, plan, candidate) = fixture.move_plan(fixture.abc()).await;

    let error = service
        .apply(
            &plan.plan_id,
            &plan.revision,
            &[candidate.candidate_id.clone(), "unknown".to_owned()],
            None,
            None,
        )
        .await
        .unwrap_err();

    assert_eq!(kind(&error), FillActorErrorKind::UnknownCandidate);
    assert!(source.exists());
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_rejects_unknown_plan_and_revision
#[tokio::test]
async fn apply_rejects_unknown_plan_and_revision() {
    let fixture = Fixture::new().await;
    create_move_candidate(&fixture.paths, "ABC-001");
    let (service, plan, candidate) = fixture.move_plan(fixture.abc()).await;

    let unknown = service
        .apply("unknown", &plan.revision, &[], None, None)
        .await
        .unwrap_err();
    assert_eq!(kind(&unknown), FillActorErrorKind::UnknownPlan);
    let old = service
        .apply(
            &plan.plan_id,
            "old",
            std::slice::from_ref(&candidate.candidate_id),
            None,
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(kind(&old), FillActorErrorKind::RevisionMismatch);
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_rejects_expired_plan
#[tokio::test]
async fn apply_rejects_expired_plan() {
    let fixture = Fixture::new().await;
    let clock = MutableClock::at(2026, 7, 12);
    create_move_candidate(&fixture.paths, "ABC-001");
    let mut setup = fixture.abc();
    setup.options.clock = Some(clock.clock());
    let service = fixture.build(setup);
    let plan = create_plan(&service, &["actor"]).await;
    clock.advance(TimeDelta::hours(2));

    let error = service
        .apply(&plan.plan_id, &plan.revision, &[], None, None)
        .await
        .unwrap_err();

    assert_eq!(kind(&error), FillActorErrorKind::ExpiredPlan);
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_apply_refuses_a_plan_scanned_under_different_library_roots
#[tokio::test]
async fn apply_refuses_a_plan_scanned_under_different_library_roots() {
    // A settings change must invalidate plans, not silently redirect their moves.
    let fixture = Fixture::new().await;
    let runtime = Arc::new(Mutex::new(Arc::new(FillActorRuntime {
        version: 1,
        paths: Some(fixture.paths.clone()),
        apply_enabled: true,
        ..FillActorRuntime::default()
    })));
    let source = create_move_candidate(&fixture.paths, "ABC-001");
    let provider = {
        let runtime = Arc::clone(&runtime);
        Arc::new(move || Arc::clone(&runtime.lock().unwrap_or_else(PoisonError::into_inner)))
    };
    let service = FillActorService::new(
        ServiceParts {
            runtime: provider,
            actor_catalog: Arc::new(FakeCatalog::new(&[("actor", &["ABC-001"])])),
            acquisition_gateway: Arc::new(FakeGateway::default()),
            brand_resolver: Arc::new(MappingBrands::new(&[("ABC-001", Some("ABC"))])),
            repository: fixture.db.repository(),
        },
        ServiceOptions {
            token_factory: Some(super::tokens()),
            ..ServiceOptions::default()
        },
    )
    .unwrap();
    let plan = create_plan(&service, &["actor"]).await;
    let candidate = plan.videos[0].move_candidates[0].clone();

    *runtime.lock().unwrap_or_else(PoisonError::into_inner) = Arc::new(FillActorRuntime {
        version: 2,
        paths: Some(fixture.paths.clone()),
        apply_enabled: true,
        ..FillActorRuntime::default()
    });

    let error = apply(&service, &plan, &[&candidate]).await.unwrap_err();
    assert_eq!(kind(&error), FillActorErrorKind::LegacyPlan);
    assert!(source.exists());
    fixture.finish().await;
}

// py: tests/test_fill_actor_service.py::test_unconfigured_service_is_not_ready_instead_of_raising
#[tokio::test]
async fn unconfigured_service_is_not_ready_instead_of_raising() {
    let db = Db::new().await;
    let service = FillActorService::new(
        ServiceParts {
            runtime: crate::service::static_runtime(FillActorRuntime::default()),
            actor_catalog: Arc::new(FakeCatalog::new(&[("actor", &[])])),
            acquisition_gateway: Arc::new(FakeGateway::default()),
            brand_resolver: Arc::new(MappingBrands::new(&[])),
            repository: db.repository(),
        },
        ServiceOptions::default(),
    )
    .unwrap();

    assert!(!service.configured());
    assert!(!service.roots_ready().await.unwrap());
    assert!(!service.scan_ready().await.unwrap());
    assert!(!service.apply_ready().await.unwrap());
    // Cloud health is a separate question and must not be dragged down by empty paths.
    assert!(service.cloud_ready().await);
    db.finish().await;
}

/// A file name that is not UTF-8 reaches the public plan as a surrogate, which
/// `model_dump_json()` refuses when the PostgreSQL repository saves the plan: the scan fails
/// with pydantic's error and nothing is stored (D125; the text is what `CPython` 3.13.18 and
/// pydantic print for `ABC-001.\xffx.mp4`, in the actor root or an additional one).
#[tokio::test]
async fn a_file_name_that_is_not_utf8_fails_the_scan_as_python_does() {
    use std::os::unix::ffi::OsStrExt as _;
    for root_kind in ["actor", "additional"] {
        let fixture = Fixture::new().await;
        let root = if root_kind == "actor" {
            &fixture.paths.actor_brand_path
        } else {
            &fixture.paths.additional_brand_paths[0]
        };
        std::fs::create_dir(root.join("ABC")).unwrap();
        std::fs::write(
            root.join("ABC").join(std::ffi::OsStr::from_bytes(b"ABC-001.\xffx.mp4")),
            b"",
        )
        .unwrap();
        let service = fixture.build(fixture.abc());

        let error = service
            .create_plan(&strings(&["actor"]), Some("plan-1"), None, None)
            .await
            .unwrap_err();

        assert_eq!(error.class_name(), "PydanticSerializationError", "{root_kind}");
        assert_eq!(
            error.to_string(),
            "Error serializing to JSON: UnicodeEncodeError: 'utf-8' codec can't encode character '\\udcff' in position 8: surrogates not allowed"
        );
        assert_eq!(
            service
                .get_plan("plan-1")
                .await
                .unwrap_err()
                .as_fill_actor()
                .unwrap()
                .kind,
            FillActorErrorKind::UnknownPlan
        );
        fixture.finish().await;
    }
}
