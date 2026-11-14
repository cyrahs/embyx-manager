//! Ported from `tests/test_cloud_move_service.py`, with `FakeCloudMover` as the mover.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use embyx_clients::clouddrive::CloudDriveError;
use indexmap::IndexMap;

use super::{Db, FakeCatalog, MappingBrands, MutableClock, Setup, strings};
use crate::cloud_moves::{
    BoxFuture, CheckError, CloudFileMetadata, CloudFileMover, CloudMovePaths, CloudMoveResponse, MoverError,
};
use crate::models::{FillActorPlan, MoveCandidate, MoveState, VideoState};
use crate::postgres_repository::PostgresFillActorRepository;
use crate::records::{CloudMoveOperationState, MoveJournalRecord, MoveJournalState};
use crate::service::{CLOUD_MOVE_OBSERVATION_GRACE, FillActorPaths, FillActorService, posix_parent};

const SOURCE_API_PATH: &str = "/cloud/library/source-b/ABC/ABC-001.mp4";
const DESTINATION_API_DIR: &str = "/cloud/library/destination/ABC";
const SOURCE_API_ROOT: &str = "/cloud/library/source-b";
const DESTINATION_API_ROOT: &str = "/cloud/library/destination";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Behavior {
    Success,
    RaiseBeforeMove,
    RaiseAfterMove,
    EventualDoubleVisibility,
}

struct MoverState {
    directories: IndexMap<String, IndexMap<String, CloudFileMetadata>>,
    move_calls: Vec<(String, String)>,
    behavior: Behavior,
    replace_source_on_ensure: bool,
    pending_source_removal_observations: u32,
}

/// `FakeCloudMover`. Its scoped movers share its state; `settle()` has nothing to wait for.
#[derive(Clone)]
struct FakeCloudMover(Arc<Mutex<MoverState>>);

fn name(path: &str) -> &str {
    embyx_core::pypath::name(path)
}

fn moved(file: &CloudFileMetadata, path: String) -> CloudFileMetadata {
    CloudFileMetadata { path, ..file.clone() }
}

/// What `raise TimeoutError` stands for: a call that failed in transport.
fn timeout() -> MoverError {
    MoverError::Check(CheckError::Value("TimeoutError".to_owned()))
}

impl FakeCloudMover {
    fn new(source: &CloudFileMetadata) -> Self {
        let mut directories = IndexMap::new();
        directories.insert(SOURCE_API_ROOT.to_owned(), IndexMap::new());
        directories.insert(
            posix_parent(&source.path),
            IndexMap::from([(source.name.clone(), source.clone())]),
        );
        directories.insert(DESTINATION_API_ROOT.to_owned(), IndexMap::new());
        directories.insert(DESTINATION_API_DIR.to_owned(), IndexMap::new());
        Self(Arc::new(Mutex::new(MoverState {
            directories,
            move_calls: Vec::new(),
            behavior: Behavior::Success,
            replace_source_on_ensure: false,
            pending_source_removal_observations: 0,
        })))
    }

    fn state(&self) -> std::sync::MutexGuard<'_, MoverState> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn move_calls(&self) -> Vec<(String, String)> {
        self.state().move_calls.clone()
    }

    fn set_behavior(&self, behavior: Behavior) {
        self.state().behavior = behavior;
    }

    fn external_move(&self, source_api_path: &str, destination_api_directory: &str) {
        external_move(&mut self.state(), source_api_path, destination_api_directory);
    }
}

fn external_move(state: &mut MoverState, source_api_path: &str, destination_api_directory: &str) {
    let file_name = name(source_api_path).to_owned();
    let source = state.directories[&posix_parent(source_api_path)]
        .shift_remove(&file_name)
        .unwrap();
    let destination = embyx_core::pypath::join(destination_api_directory, &file_name);
    state.directories[destination_api_directory].insert(file_name, moved(&source, destination));
}

impl CloudFileMover for FakeCloudMover {
    fn list_directory<'a>(
        &'a self,
        api_directory: &'a str,
    ) -> BoxFuture<'a, Result<Vec<CloudFileMetadata>, MoverError>> {
        let result = {
            let mut state = self.state();
            if state.directories.contains_key(api_directory) {
                if api_directory == posix_parent(SOURCE_API_PATH) && state.pending_source_removal_observations > 0 {
                    state.pending_source_removal_observations += 1;
                    if state.pending_source_removal_observations > 2 {
                        state.directories[api_directory].shift_remove(name(SOURCE_API_PATH));
                    }
                }
                Ok(state.directories[api_directory].values().cloned().collect())
            } else {
                Err(MoverError::CloudDrive(CloudDriveError::FileNotFound {
                    message: "No such file or directory".to_owned(),
                    path: api_directory.to_owned(),
                }))
            }
        };
        Box::pin(async move { result })
    }

    fn ensure_directory<'a>(
        &'a self,
        parent_api_directory: &'a str,
        folder_name: &'a str,
    ) -> BoxFuture<'a, Result<bool, MoverError>> {
        let mut state = self.state();
        if state.replace_source_on_ensure {
            let parent = posix_parent(SOURCE_API_PATH);
            let file = &mut state.directories[&parent][name(SOURCE_API_PATH)];
            file.file_id = "replacement-file".to_owned();
        }
        let directory = embyx_core::pypath::join(parent_api_directory, folder_name);
        state.directories.entry(directory).or_default();
        Box::pin(async { Ok(true) })
    }

    fn move_file<'a>(
        &'a self,
        source_api_path: &'a str,
        destination_api_directory: &'a str,
    ) -> BoxFuture<'a, Result<CloudMoveResponse, MoverError>> {
        let result = (|| {
            let mut state = self.state();
            state
                .move_calls
                .push((source_api_path.to_owned(), destination_api_directory.to_owned()));
            match state.behavior {
                Behavior::RaiseBeforeMove => return Err(timeout()),
                Behavior::EventualDoubleVisibility => {
                    let file_name = name(source_api_path).to_owned();
                    let destination = embyx_core::pypath::join(destination_api_directory, &file_name);
                    let source = state.directories[&posix_parent(source_api_path)][&file_name].clone();
                    state.directories[destination_api_directory].insert(file_name, moved(&source, destination));
                    state.pending_source_removal_observations = 1;
                }
                Behavior::Success | Behavior::RaiseAfterMove => {
                    external_move(&mut state, source_api_path, destination_api_directory);
                }
            }
            if state.behavior == Behavior::RaiseAfterMove {
                return Err(timeout());
            }
            Ok(CloudMoveResponse {
                success: true,
                error_message: None,
                result_paths: vec![embyx_core::pypath::join(
                    destination_api_directory,
                    name(source_api_path),
                )],
            })
        })();
        Box::pin(async move { result })
    }

    fn scoped(&self) -> Arc<dyn CloudFileMover> {
        Arc::new(self.clone())
    }

    fn settle(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// The `cloud_file` fixture.
fn cloud_file() -> CloudFileMetadata {
    CloudFileMetadata {
        path: SOURCE_API_PATH.to_owned(),
        file_id: "cloud-file-id".to_owned(),
        name: "ABC-001.mp4".to_owned(),
        size: 123,
        write_time: 456,
        hashes: vec![("sha1".to_owned(), "abcd".to_owned())],
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    db: Db,
}

struct Made {
    service: FillActorService,
    repository: Arc<PostgresFillActorRepository>,
    mover: FakeCloudMover,
    mapping: PathBuf,
}

#[derive(Default)]
struct Options {
    repository: Option<Arc<PostgresFillActorRepository>>,
    apply_disabled: bool,
    brand: Option<&'static str>,
    clock: Option<MutableClock>,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        Self {
            _dir: dir,
            root,
            db: Db::new().await,
        }
    }

    /// `make_service(tmp_path, cloud_file, repository=..., apply_enabled=..., brand=..., clock=...)`.
    fn make_service(&self, options: Options) -> Made {
        let actor = self.root.join("actor");
        let additional = self.root.join("additional");
        let mapping_dir = additional.join("ABC");
        for path in [&actor, &additional, &mapping_dir] {
            if !path.is_dir() {
                std::fs::create_dir(path).unwrap();
            }
        }
        let mapping = mapping_dir.join("ABC-001.strm");
        std::fs::write(&mapping, "/mounted-cloud/cloud/library/source-b/ABC/ABC-001.mp4\n").unwrap();
        let paths = FillActorPaths::from_iterable(actor.clone(), [additional], actor);
        let repository = options.repository.unwrap_or_else(|| self.db.repository());
        let mover = FakeCloudMover::new(&cloud_file());
        let mut setup = Setup::new(
            &paths,
            FakeCatalog::new(&[("actor", &["ABC-001"])]),
            MappingBrands::all(options.brand.unwrap_or("ABC")),
        );
        setup.runtime.move_in_by_brand = true;
        setup.runtime.apply_enabled = !options.apply_disabled;
        setup.runtime.cloud_move_paths = Some(
            CloudMovePaths::from_values(
                "/mounted-cloud",
                &strings(&["/cloud/library/source-b"]),
                "/cloud/library/destination",
            )
            .unwrap(),
        );
        setup.options.token_factory = None;
        setup.options.cloud_file_mover = Some(Arc::new(mover.clone()));
        setup.options.clock = options.clock.map(|clock| clock.clock());
        // `asyncio.sleep(0.5)` between verification rounds, without the wait.
        setup.options.sleeper = Arc::new(|_| Box::pin(async {}));
        Made {
            service: setup.build(Arc::clone(&repository)),
            repository,
            mover,
            mapping,
        }
    }

    async fn finish(self) {
        self.db.finish().await;
    }
}

/// `create_candidate(service)`.
async fn create_candidate(service: &FillActorService) -> (FillActorPlan, MoveCandidate) {
    let plan = service
        .create_plan(&strings(&["actor"]), None, None, None)
        .await
        .unwrap();
    assert_eq!(plan.videos[0].state, VideoState::AdditionalFound);
    let candidate = plan.videos[0].move_candidates[0].clone();
    (plan, candidate)
}

async fn apply(
    service: &FillActorService,
    plan: &FillActorPlan,
    candidate: &MoveCandidate,
) -> crate::models::MoveResult {
    service
        .apply(
            &plan.plan_id,
            &plan.revision,
            std::slice::from_ref(&candidate.candidate_id),
            None,
            None,
        )
        .await
        .unwrap()
        .results
        .remove(0)
}

fn source_and_destination() -> (String, String) {
    (SOURCE_API_PATH.to_owned(), DESTINATION_API_DIR.to_owned())
}

// py: tests/test_cloud_move_service.py::test_cloud_scan_rejects_unsafe_brand_segments cases=3
#[tokio::test]
async fn cloud_scan_rejects_unsafe_brand_segments() {
    for brand in ["nested/ABC", "bad\\name", "bad\0name"] {
        let fixture = Fixture::new().await;
        let made = fixture.make_service(Options {
            brand: Some(brand),
            ..Options::default()
        });

        let plan = made
            .service
            .create_plan(&strings(&["actor"]), None, None, None)
            .await
            .unwrap();

        assert_eq!(plan.videos[0].state, VideoState::InvalidVideoId, "{brand:?}");
        assert!(made.mover.move_calls().is_empty());
        fixture.finish().await;
    }
}

// py: tests/test_cloud_move_service.py::test_cloud_readiness_checks_configured_api_roots
#[tokio::test]
async fn cloud_readiness_checks_configured_api_roots() {
    let fixture = Fixture::new().await;
    let made = fixture.make_service(Options::default());

    assert!(made.service.scan_ready().await.unwrap());
    assert!(made.service.apply_ready().await.unwrap());

    let unavailable = fixture.make_service(Options::default());
    unavailable.mover.state().directories.shift_remove(DESTINATION_API_ROOT);

    assert!(!unavailable.service.scan_ready().await.unwrap());
    assert!(!unavailable.service.apply_ready().await.unwrap());
    assert!(made.mover.move_calls().is_empty());
    fixture.finish().await;
}

// py: tests/test_cloud_move_service.py::test_legacy_local_journal_blocks_only_cloud_apply_readiness
#[tokio::test]
async fn legacy_local_journal_blocks_only_cloud_apply_readiness() {
    let fixture = Fixture::new().await;
    let made = fixture.make_service(Options::default());
    let (plan, candidate) = create_candidate(&made.service).await;
    made.repository
        .save_move_journal(&MoveJournalRecord {
            plan_id: plan.plan_id.clone(),
            candidate_id: candidate.candidate_id.clone(),
            state: MoveJournalState::Prepared,
            updated_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

    assert!(made.service.scan_ready().await.unwrap());
    assert!(!made.service.legacy_journal_ready().await.unwrap());
    assert!(!made.service.apply_ready().await.unwrap());
    fixture.finish().await;
}

// py: tests/test_cloud_move_service.py::test_cloud_apply_moves_real_file_and_never_mutates_strm
#[tokio::test]
async fn cloud_apply_moves_real_file_and_never_mutates_strm() {
    let fixture = Fixture::new().await;
    let made = fixture.make_service(Options::default());
    let (plan, candidate) = create_candidate(&made.service).await;
    let original_mapping = std::fs::read(&made.mapping).unwrap();

    let result = apply(&made.service, &plan, &candidate).await;

    assert_eq!(result.state, MoveState::Moved);
    assert_eq!(made.mover.move_calls(), [source_and_destination()]);
    assert!(!made.mover.move_calls()[0].0.contains(".strm"));
    assert_eq!(std::fs::read(&made.mapping).unwrap(), original_mapping);
    let listing = made.mover.list_directory(&posix_parent(SOURCE_API_PATH)).await.unwrap();
    assert!(!listing.iter().any(|file| file.path == SOURCE_API_PATH));
    let operation = made
        .repository
        .get_cloud_move_operation(&plan.plan_id, &candidate.candidate_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.state, CloudMoveOperationState::Succeeded);

    let converging = made
        .service
        .create_plan(&strings(&["actor"]), None, None, None)
        .await
        .unwrap();
    assert_eq!(converging.videos[0].state, VideoState::Exists);
    assert_eq!(converging.videos[0].warnings, ["mapping_convergence_pending"]);
    fixture.finish().await;
}

// py: tests/test_cloud_move_service.py::test_cloud_apply_detects_destination_conflict_without_calling_move
#[tokio::test]
async fn cloud_apply_detects_destination_conflict_without_calling_move() {
    let fixture = Fixture::new().await;
    let made = fixture.make_service(Options::default());
    let file = cloud_file();
    made.mover.state().directories[DESTINATION_API_DIR].insert(
        file.name.clone(),
        CloudFileMetadata {
            path: format!("{DESTINATION_API_DIR}/{}", file.name),
            file_id: "foreign-file".to_owned(),
            ..file.clone()
        },
    );
    let (plan, candidate) = create_candidate(&made.service).await;
    assert!(candidate.destination_conflict);

    let result = apply(&made.service, &plan, &candidate).await;

    assert_eq!(result.state, MoveState::Conflict);
    assert!(made.mover.move_calls().is_empty());
    fixture.finish().await;
}

// py: tests/test_cloud_move_service.py::test_cloud_apply_creates_only_the_missing_brand_directory
#[tokio::test]
async fn cloud_apply_creates_only_the_missing_brand_directory() {
    let fixture = Fixture::new().await;
    let made = fixture.make_service(Options::default());
    made.mover.state().directories.shift_remove(DESTINATION_API_DIR);
    let (plan, candidate) = create_candidate(&made.service).await;

    let result = apply(&made.service, &plan, &candidate).await;

    assert_eq!(result.state, MoveState::Moved);
    assert!(made.mover.state().directories.contains_key(DESTINATION_API_DIR));
    assert_eq!(made.mover.move_calls(), [source_and_destination()]);
    fixture.finish().await;
}

// py: tests/test_cloud_move_service.py::test_cloud_apply_rechecks_source_identity_after_creating_destination
#[tokio::test]
async fn cloud_apply_rechecks_source_identity_after_creating_destination() {
    let fixture = Fixture::new().await;
    let made = fixture.make_service(Options::default());
    {
        let mut state = made.mover.state();
        state.directories.shift_remove(DESTINATION_API_DIR);
        state.replace_source_on_ensure = true;
    }
    let (plan, candidate) = create_candidate(&made.service).await;

    let result = apply(&made.service, &plan, &candidate).await;

    assert_eq!(result.state, MoveState::Stale);
    assert!(made.mover.move_calls().is_empty());
    fixture.finish().await;
}

// py: tests/test_cloud_move_service.py::test_cloud_apply_rejects_mapping_changed_after_plan
#[tokio::test]
async fn cloud_apply_rejects_mapping_changed_after_plan() {
    let fixture = Fixture::new().await;
    let made = fixture.make_service(Options::default());
    let (plan, candidate) = create_candidate(&made.service).await;
    std::fs::write(&made.mapping, "/mounted-cloud/cloud/library/source-b/ABC/ABC-999.mp4\n").unwrap();

    let result = apply(&made.service, &plan, &candidate).await;

    assert_eq!(result.state, MoveState::Stale);
    assert!(made.mover.move_calls().is_empty());
    fixture.finish().await;
}

// py: tests/test_cloud_move_service.py::test_cloud_timeout_after_server_move_is_verified_as_success
#[tokio::test]
async fn cloud_timeout_after_server_move_is_verified_as_success() {
    let fixture = Fixture::new().await;
    let made = fixture.make_service(Options::default());
    made.mover.set_behavior(Behavior::RaiseAfterMove);
    let (plan, candidate) = create_candidate(&made.service).await;

    let result = apply(&made.service, &plan, &candidate).await;

    assert_eq!(result.state, MoveState::Moved);
    assert_eq!(made.mover.move_calls().len(), 1);
    fixture.finish().await;
}

// py: tests/test_cloud_move_service.py::test_cloud_move_waits_when_same_identity_is_temporarily_visible_at_both_paths
#[tokio::test]
async fn cloud_move_waits_when_same_identity_is_temporarily_visible_at_both_paths() {
    let fixture = Fixture::new().await;
    let made = fixture.make_service(Options::default());
    made.mover.set_behavior(Behavior::EventualDoubleVisibility);
    let (plan, candidate) = create_candidate(&made.service).await;

    let result = apply(&made.service, &plan, &candidate).await;

    assert_eq!(result.state, MoveState::Moved);
    assert_eq!(made.mover.move_calls().len(), 1);
    fixture.finish().await;
}

// py: tests/test_cloud_move_service.py::test_unknown_cloud_move_is_observation_only_until_remote_state_converges
#[tokio::test]
async fn unknown_cloud_move_is_observation_only_until_remote_state_converges() {
    let fixture = Fixture::new().await;
    let made = fixture.make_service(Options::default());
    made.mover.set_behavior(Behavior::RaiseBeforeMove);
    let (plan, candidate) = create_candidate(&made.service).await;

    let first = apply(&made.service, &plan, &candidate).await;
    let second = apply(&made.service, &plan, &candidate).await;

    assert_eq!(first.error_code.as_deref(), Some("cloud_move_status_unknown"));
    assert_eq!(second.error_code.as_deref(), Some("cloud_move_status_unknown"));
    assert_eq!(made.mover.move_calls().len(), 1);
    assert_eq!(
        made.repository
            .get_move_result(&plan.plan_id, &candidate.candidate_id)
            .await
            .unwrap(),
        None
    );
    let operation = made
        .repository
        .get_cloud_move_operation(&plan.plan_id, &candidate.candidate_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.state, CloudMoveOperationState::Unknown);

    made.mover.external_move(SOURCE_API_PATH, DESTINATION_API_DIR);
    let reconciled = made.service.reconcile_moves().await.unwrap();

    assert_eq!(reconciled[0].state, MoveState::Moved);
    assert_eq!(made.mover.move_calls().len(), 1);
    fixture.finish().await;
}

// py: tests/test_cloud_move_service.py::test_never_observed_move_is_concluded_after_grace_and_retryable
#[tokio::test]
async fn never_observed_move_is_concluded_after_grace_and_retryable() {
    let fixture = Fixture::new().await;
    let clock = MutableClock::at(2026, 8, 18);
    let made = fixture.make_service(Options {
        clock: Some(clock.clone()),
        ..Options::default()
    });
    made.mover.set_behavior(Behavior::RaiseBeforeMove);
    let (plan, candidate) = create_candidate(&made.service).await;

    let applied = apply(&made.service, &plan, &candidate).await;
    assert_eq!(applied.error_code.as_deref(), Some("cloud_move_status_unknown"));

    // Within the grace period recovery still only observes.
    let observed = made.service.reconcile_moves().await.unwrap();
    assert_eq!(observed[0].error_code.as_deref(), Some("cloud_move_status_unknown"));

    clock.advance(CLOUD_MOVE_OBSERVATION_GRACE);
    let concluded = made.service.reconcile_moves().await.unwrap();

    assert_eq!(concluded[0].state, MoveState::Failed);
    assert_eq!(concluded[0].error_code.as_deref(), Some("cloud_move_not_observed"));
    let operation = made
        .repository
        .get_cloud_move_operation(&plan.plan_id, &candidate.candidate_id)
        .await
        .unwrap()
        .unwrap();
    assert!(operation.state.terminal());

    // The source is free again: a fresh scan may move it successfully.
    made.mover.set_behavior(Behavior::Success);
    let (new_plan, new_candidate) = create_candidate(&made.service).await;
    let retried = apply(&made.service, &new_plan, &new_candidate).await;

    assert_eq!(retried.state, MoveState::Moved);
    assert_eq!(made.mover.move_calls().len(), 2);
    fixture.finish().await;
}

// py: tests/test_cloud_move_service.py::test_disabled_apply_does_not_reconcile_unknown_cloud_operation
#[tokio::test]
async fn disabled_apply_does_not_reconcile_unknown_cloud_operation() {
    let fixture = Fixture::new().await;
    let made = fixture.make_service(Options::default());
    made.mover.set_behavior(Behavior::RaiseBeforeMove);
    let (plan, candidate) = create_candidate(&made.service).await;
    apply(&made.service, &plan, &candidate).await;
    let disabled = fixture.make_service(Options {
        repository: Some(Arc::clone(&made.repository)),
        apply_disabled: true,
        ..Options::default()
    });

    assert!(disabled.service.reconcile_moves().await.unwrap().is_empty());
    assert!(disabled.mover.move_calls().is_empty());
    fixture.finish().await;
}
