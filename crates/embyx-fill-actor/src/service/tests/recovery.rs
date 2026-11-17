//! Ported from `tests/test_fill_actor_recovery.py`: every service here runs with the
//! `move.lock` file lock, and a "restart" is a new service on a new repository pool.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use embyx_db::locking::AsyncFileLock;

use super::{Db, Event, FakeCatalog, MappingBrands, Setup, read, strings};
use crate::fs::recording::{Fault, RecordingMoveFs};
use crate::fs::{EINVAL, LocalMoveFs, MoveFs as _};
use crate::models::{FillActorPlan, MoveCandidate, MoveState};
use crate::postgres_repository::PostgresFillActorRepository;
use crate::records::{CandidateRecord, MoveJournalRecord, MoveJournalState};
use crate::service::{FillActorPaths, FillActorService, local};

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    db: Db,
}

/// `make_service(tmp_path, repository, root_sentinel=..., apply_enabled=...)`.
struct Options {
    root_sentinel: Option<&'static str>,
    apply_enabled: bool,
    fs: Option<Arc<RecordingMoveFs>>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            root_sentinel: None,
            apply_enabled: true,
            fs: None,
        }
    }
}

/// One scanned candidate: the plan, its candidate and its private record.
struct Scanned {
    plan: FillActorPlan,
    candidate: MoveCandidate,
    record: CandidateRecord,
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

    fn make_service(
        &self,
        repository: Arc<PostgresFillActorRepository>,
        options: Options,
    ) -> (FillActorService, FillActorPaths) {
        let paths = FillActorPaths::from_iterable(
            self.root.join("actor"),
            [self.root.join("additional")],
            self.root.join("move-in"),
        );
        for path in [
            &paths.actor_brand_path,
            &paths.additional_brand_paths[0],
            &paths.move_in_path,
        ] {
            if !path.is_dir() {
                std::fs::create_dir(path).unwrap();
            }
            if let Some(sentinel) = options.root_sentinel {
                std::fs::write(path.join(sentinel), "ready").unwrap();
            }
        }
        let mut setup = Setup::new(
            &paths,
            FakeCatalog::new(&[("actor", &["ABC-001"])]),
            MappingBrands::all("ABC"),
        );
        setup.runtime.root_sentinel = options.root_sentinel.map(str::to_owned);
        setup.runtime.apply_enabled = options.apply_enabled;
        setup.options.token_factory = None;
        setup.options.mutation_lock = Some(AsyncFileLock::new(self.root.join("move.lock")).into());
        if let Some(fs) = options.fs {
            setup = setup.fs(fs);
        }
        (setup.build(repository), paths)
    }

    /// `<additional>/ABC/ABC-001.mp4` and a scan of it.
    async fn scan(
        &self,
        service: &FillActorService,
        repository: &PostgresFillActorRepository,
        paths: &FillActorPaths,
    ) -> Scanned {
        let brand = paths.additional_brand_paths[0].join("ABC");
        std::fs::create_dir(&brand).unwrap();
        std::fs::write(brand.join("ABC-001.mp4"), b"video").unwrap();
        let plan = service
            .create_plan(&strings(&["actor"]), None, None, None)
            .await
            .unwrap();
        let candidate = plan.videos[0].move_candidates[0].clone();
        let record = repository
            .get_candidate(&plan.plan_id, &candidate.candidate_id)
            .await
            .unwrap()
            .expect("the candidate");
        Scanned {
            plan,
            candidate,
            record,
        }
    }

    async fn finish(self) {
        self.db.finish().await;
    }
}

async fn save_journal(repository: &PostgresFillActorRepository, scanned: &Scanned, state: MoveJournalState) {
    repository
        .save_move_journal(&MoveJournalRecord {
            plan_id: scanned.plan.plan_id.clone(),
            candidate_id: scanned.candidate.candidate_id.clone(),
            state,
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
}

async fn journal_state(repository: &PostgresFillActorRepository, scanned: &Scanned) -> MoveJournalState {
    repository
        .get_move_journal(&scanned.plan.plan_id, &scanned.candidate.candidate_id)
        .await
        .unwrap()
        .expect("a journal")
        .state
}

fn link(source: &Path, destination: &Path) {
    std::fs::hard_link(source, destination).unwrap();
}

// py: tests/test_fill_actor_recovery.py::test_startup_reconciles_each_move_journal_state cases=4
#[tokio::test]
async fn startup_reconciles_each_move_journal_state() {
    for (journal, filesystem) in [
        (MoveJournalState::Prepared, "source_only"),
        (MoveJournalState::Prepared, "both_linked"),
        (MoveJournalState::Linked, "both_linked"),
        (MoveJournalState::SourceRemoved, "destination_only"),
    ] {
        let fixture = Fixture::new().await;
        let repository = fixture.db.repository();
        let (service, paths) = fixture.make_service(Arc::clone(&repository), Options::default());
        let scanned = fixture.scan(&service, &repository, &paths).await;
        let states = [
            MoveJournalState::Prepared,
            MoveJournalState::Linked,
            MoveJournalState::SourceRemoved,
        ];
        let reached = states.iter().position(|state| *state == journal).unwrap();
        for state in &states[..=reached] {
            save_journal(&repository, &scanned, *state).await;
        }
        let record = &scanned.record;
        if matches!(filesystem, "both_linked" | "destination_only") {
            link(&record.source, &record.destination);
        }
        if filesystem == "destination_only" {
            std::fs::remove_file(&record.source).unwrap();
        }

        let restarted_repository = fixture.db.repository();
        let (restarted, _) = fixture.make_service(Arc::clone(&restarted_repository), Options::default());
        let results = restarted.reconcile_moves().await.unwrap();

        assert_eq!(results[0].state, MoveState::Moved, "{journal:?} {filesystem}");
        assert!(!record.source.exists());
        assert_eq!(read(&record.destination), b"video");
        let saved = restarted_repository
            .get_move_result(&scanned.plan.plan_id, &scanned.candidate.candidate_id)
            .await
            .unwrap();
        assert_eq!(saved.as_ref(), Some(&results[0]));
        assert_eq!(
            journal_state(&restarted_repository, &scanned).await,
            MoveJournalState::Reconciled
        );
        fixture.finish().await;
    }
}

// py: tests/test_fill_actor_recovery.py::test_disabled_apply_leaves_unreconciled_move_untouched
#[tokio::test]
async fn disabled_apply_leaves_unreconciled_move_untouched() {
    let fixture = Fixture::new().await;
    let repository = fixture.db.repository();
    let (service, paths) = fixture.make_service(Arc::clone(&repository), Options::default());
    let scanned = fixture.scan(&service, &repository, &paths).await;
    save_journal(&repository, &scanned, MoveJournalState::Prepared).await;

    let restarted_repository = fixture.db.repository();
    let (restarted, _) = fixture.make_service(
        Arc::clone(&restarted_repository),
        Options {
            apply_enabled: false,
            ..Options::default()
        },
    );

    assert!(restarted.reconcile_moves().await.unwrap().is_empty());
    assert_eq!(read(&scanned.record.source), b"video");
    assert!(!paths.move_in_path.join("ABC-001.mp4").exists());
    assert_eq!(
        journal_state(&restarted_repository, &scanned).await,
        MoveJournalState::Prepared
    );
    fixture.finish().await;
}

// py: tests/test_fill_actor_recovery.py::test_postgres_service_result_is_idempotent_after_restart
#[tokio::test]
async fn postgres_service_result_is_idempotent_after_restart() {
    let fixture = Fixture::new().await;
    let repository = fixture.db.repository();
    let (service, paths) = fixture.make_service(Arc::clone(&repository), Options::default());
    let scanned = fixture.scan(&service, &repository, &paths).await;
    let ids = [scanned.candidate.candidate_id.clone()];
    let first = service
        .apply(&scanned.plan.plan_id, &scanned.plan.revision, &ids, None, None)
        .await
        .unwrap();

    let (restarted, _) = fixture.make_service(fixture.db.repository(), Options::default());
    let second = restarted
        .apply(&scanned.plan.plan_id, &scanned.plan.revision, &ids, None, None)
        .await
        .unwrap();

    assert_eq!(first, second);
    fixture.finish().await;
}

// py: tests/test_fill_actor_recovery.py::test_cancelled_mutation_keeps_file_lock_until_native_move_finishes
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_mutation_keeps_file_lock_until_native_move_finishes() {
    let fixture = Fixture::new().await;
    let repository = fixture.db.repository();
    let fs = Arc::new(RecordingMoveFs::default());
    let (service, paths) = fixture.make_service(
        Arc::clone(&repository),
        Options {
            fs: Some(Arc::clone(&fs)),
            ..Options::default()
        },
    );
    let scanned = fixture.scan(&service, &repository, &paths).await;
    let (renamed, release) = (Event::default(), Event::default());
    {
        let (renamed, release) = (renamed.clone(), release.clone());
        fs.set_after(Some(Arc::new(move |op, _, _| {
            if op == "rename_noreplace" {
                renamed.set();
                assert!(release.wait(2), "native move was not released");
            }
        })));
    }
    let move_task = {
        let (service, plan_id, record) = (service.clone(), scanned.plan.plan_id.clone(), scanned.record.clone());
        tokio::spawn(async move { service.run_move(&plan_id, &record).await })
    };
    assert!(renamed.wait_async(2).await);
    move_task.abort();

    let (restarted, _) = fixture.make_service(fixture.db.repository(), Options::default());
    let reconcile_task = tokio::spawn(async move { restarted.reconcile_moves().await });
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert!(!reconcile_task.is_finished());
    release.set();

    assert!(move_task.await.unwrap_err().is_cancelled());
    let results = reconcile_task.await.unwrap().unwrap();
    assert_eq!(results[0].state, MoveState::Moved);
    assert!(!scanned.record.source.exists());
    assert_eq!(read(&scanned.record.destination), b"video");
    fixture.finish().await;
}

// py: tests/test_fill_actor_recovery.py::test_offline_root_keeps_journal_until_mount_identity_returns
#[tokio::test]
async fn offline_root_keeps_journal_until_mount_identity_returns() {
    let fixture = Fixture::new().await;
    let repository = fixture.db.repository();
    let (service, paths) = fixture.make_service(
        Arc::clone(&repository),
        Options {
            root_sentinel: Some(".root-ready"),
            ..Options::default()
        },
    );
    let scanned = fixture.scan(&service, &repository, &paths).await;
    save_journal(&repository, &scanned, MoveJournalState::Prepared).await;
    let mounted_root = paths.additional_brand_paths[0].clone();
    let offline_root = fixture.root.join("additional-mounted");
    std::fs::rename(&mounted_root, &offline_root).unwrap();
    std::fs::create_dir(&mounted_root).unwrap();

    assert!(service.reconcile_moves().await.unwrap().is_empty());
    assert_eq!(journal_state(&repository, &scanned).await, MoveJournalState::Prepared);

    std::fs::remove_dir(&mounted_root).unwrap();
    std::fs::rename(&offline_root, &mounted_root).unwrap();
    let results = service.reconcile_moves().await.unwrap();
    assert_eq!(results[0].state, MoveState::Moved);
    fixture.finish().await;
}

// py: tests/test_fill_actor_recovery.py::test_prepared_reconcile_rolls_destination_replacement_back_to_source
#[tokio::test]
async fn prepared_reconcile_rolls_destination_replacement_back_to_source() {
    let fixture = Fixture::new().await;
    let repository = fixture.db.repository();
    let (service, paths) = fixture.make_service(Arc::clone(&repository), Options::default());
    let scanned = fixture.scan(&service, &repository, &paths).await;
    let record = &scanned.record;
    save_journal(&repository, &scanned, MoveJournalState::Prepared).await;
    LocalMoveFs
        .rename_noreplace(&record.source, &record.destination)
        .unwrap();
    std::fs::remove_file(&record.destination).unwrap();
    std::fs::write(&record.destination, b"external-replacement").unwrap();

    let results = service.reconcile_moves().await.unwrap();

    assert_eq!(results[0].state, MoveState::Stale);
    assert_eq!(results[0].error_code.as_deref(), Some("source_changed_during_move"));
    assert_eq!(read(&record.source), b"external-replacement");
    assert!(!record.destination.exists());
    assert_eq!(journal_state(&repository, &scanned).await, MoveJournalState::Reconciled);
    fixture.finish().await;
}

// py: tests/test_fill_actor_recovery.py::test_prepared_reconcile_preserves_journal_when_both_paths_were_replaced
#[tokio::test]
async fn prepared_reconcile_preserves_journal_when_both_paths_were_replaced() {
    let fixture = Fixture::new().await;
    let repository = fixture.db.repository();
    let (service, paths) = fixture.make_service(Arc::clone(&repository), Options::default());
    let scanned = fixture.scan(&service, &repository, &paths).await;
    let record = &scanned.record;
    save_journal(&repository, &scanned, MoveJournalState::Prepared).await;
    LocalMoveFs
        .rename_noreplace(&record.source, &record.destination)
        .unwrap();
    std::fs::remove_file(&record.destination).unwrap();
    std::fs::write(&record.destination, b"destination-replacement").unwrap();
    std::fs::write(&record.source, b"source-replacement").unwrap();

    let results = service.reconcile_moves().await.unwrap();

    assert_eq!(results[0].state, MoveState::Failed);
    assert_eq!(results[0].error_code.as_deref(), Some("reconcile_rollback_conflict"));
    assert_eq!(read(&record.source), b"source-replacement");
    assert_eq!(read(&record.destination), b"destination-replacement");
    assert_eq!(journal_state(&repository, &scanned).await, MoveJournalState::Prepared);
    fixture.finish().await;
}

/// A scanned candidate with a `prepared` journal, on a recording file system.
async fn prepared_on_recording(
    fixture: &Fixture,
) -> (
    Arc<RecordingMoveFs>,
    FillActorService,
    Arc<PostgresFillActorRepository>,
    Scanned,
) {
    let repository = fixture.db.repository();
    let fs = Arc::new(RecordingMoveFs::default());
    let (service, paths) = fixture.make_service(
        Arc::clone(&repository),
        Options {
            fs: Some(Arc::clone(&fs)),
            ..Options::default()
        },
    );
    let scanned = fixture.scan(&service, &repository, &paths).await;
    save_journal(&repository, &scanned, MoveJournalState::Prepared).await;
    (fs, service, repository, scanned)
}

// py: tests/test_fill_actor_recovery.py::test_legacy_link_reconcile_retries_when_quarantine_move_fails
#[tokio::test]
async fn legacy_link_reconcile_retries_when_quarantine_move_fails() {
    let fixture = Fixture::new().await;
    let (fs, service, repository, scanned) = prepared_on_recording(&fixture).await;
    let record = &scanned.record;
    link(&record.source, &record.destination);
    fs.add_fault(Fault::message("rename_noreplace", None, "temporary failure"));

    let results = service.reconcile_moves().await.unwrap();

    assert_eq!(results[0].error_code.as_deref(), Some("reconcile_quarantine_failed"));
    assert!(record.source.exists());
    assert!(record.destination.exists());
    assert_eq!(journal_state(&repository, &scanned).await, MoveJournalState::Prepared);
    fixture.finish().await;
}

// py: tests/test_fill_actor_recovery.py::test_reconcile_never_restores_foreign_quarantine_file
#[tokio::test]
async fn reconcile_never_restores_foreign_quarantine_file() {
    let fixture = Fixture::new().await;
    let (_fs, service, repository, scanned) = prepared_on_recording(&fixture).await;
    let quarantine = local::quarantine_path(&fixture.root.join("move-in"), &scanned.record.candidate_id);
    std::fs::write(&quarantine, b"foreign").unwrap();

    let results = service.reconcile_moves().await.unwrap();

    assert_eq!(results[0].error_code.as_deref(), Some("reconcile_quarantine_conflict"));
    assert_eq!(read(&scanned.record.source), b"video");
    assert_eq!(read(&quarantine), b"foreign");
    assert_eq!(journal_state(&repository, &scanned).await, MoveJournalState::Prepared);
    fixture.finish().await;
}

// py: tests/test_fill_actor_recovery.py::test_nfs_fallback_reconciles_legacy_both_linked_move
#[tokio::test]
async fn nfs_fallback_reconciles_legacy_both_linked_move() {
    let fixture = Fixture::new().await;
    let (fs, service, repository, scanned) = prepared_on_recording(&fixture).await;
    let record = &scanned.record;
    link(&record.source, &record.destination);
    fs.add_fault(Fault::errno("rename_noreplace", None, EINVAL));

    let results = service.reconcile_moves().await.unwrap();

    assert_eq!(results[0].state, MoveState::Moved);
    assert!(!record.source.exists());
    assert_eq!(read(&record.destination), b"video");
    assert_eq!(journal_state(&repository, &scanned).await, MoveJournalState::Reconciled);
    fixture.finish().await;
}

// py: tests/test_fill_actor_recovery.py::test_nfs_fallback_restores_replacement_from_private_quarantine
#[tokio::test]
async fn nfs_fallback_restores_replacement_from_private_quarantine() {
    let fixture = Fixture::new().await;
    let (fs, service, repository, scanned) = prepared_on_recording(&fixture).await;
    let record = &scanned.record;
    link(&record.source, &record.destination);
    std::fs::remove_file(&record.source).unwrap();
    let quarantine = local::quarantine_path(&fixture.root.join("move-in"), &record.candidate_id);
    std::fs::write(&quarantine, b"external-replacement").unwrap();
    fs.add_fault(Fault::errno("rename_noreplace", None, EINVAL));

    let results = service.reconcile_moves().await.unwrap();

    assert_eq!(results[0].state, MoveState::Moved);
    assert_eq!(read(&record.source), b"external-replacement");
    assert_eq!(read(&record.destination), b"video");
    assert!(!quarantine.exists());
    assert_eq!(journal_state(&repository, &scanned).await, MoveJournalState::Reconciled);
    fixture.finish().await;
}
