//! Ported from `tests/test_fill_actor_persistence.py` and `tests/test_cloud_move_persistence.py`,
//! against a temporary PostgreSQL database each.
//!
//! The Python contract tests run once on the in-memory repository and once on PostgreSQL
//! (`repository_kind`); the in-memory one is not ported (DIVERGENCES.md D110), so the one
//! PostgreSQL test claims both cases.

use std::os::unix::fs::MetadataExt as _;
use std::path::Path;

use chrono::{DateTime, Duration, TimeZone as _, Utc};
use embyx_db::database::{Database, DatabaseError};
use embyx_db::testdb::TestDatabase;
use embyx_fill_actor::cloud_moves::CloudFileMetadata;
use embyx_fill_actor::models::{
    ActorPlan, ApplyResult, ApplyState, FillActorPlan, MoveCandidate, MoveResult, MoveState, VideoPlan, VideoState,
};
use embyx_fill_actor::postgres_repository::{PostgresFillActorRepository, RepositoryError};
use embyx_fill_actor::pydatetime::PyDateTime;
use embyx_fill_actor::records::{
    ApplyJobRecord, CancelJobOutcome, CandidateKind, CandidateRecord, CloudMoveOperationRecord,
    CloudMoveOperationState, EnqueueApplyJobOutcome, FileFingerprint, JOB_CANCELLED_ERROR_CODE, JobOperation,
    JobProgress, JobProgressUnit, JobRecord, JobStage, JobState, MoveJournalRecord, MoveJournalState, PlanRecord,
    normalize_datetime,
};
use embyx_monitor::support::StoreError;
use sqlx::Row as _;

struct Fixture {
    test: TestDatabase,
    databases: Vec<Database>,
    repository: PostgresFillActorRepository,
}

impl Fixture {
    async fn new() -> Self {
        let test = TestDatabase::create().await;
        let database = test.database();
        let repository = PostgresFillActorRepository::new(database.clone());
        Self {
            test,
            databases: vec![database],
            repository,
        }
    }

    /// `make_postgres_repository()` again: a repository on a fresh pool of the same database.
    fn reopen(&mut self) -> PostgresFillActorRepository {
        let database = self.test.database();
        self.databases.push(database.clone());
        PostgresFillActorRepository::new(database)
    }

    async fn finish(self) {
        for database in &self.databases {
            database.close().await;
        }
        self.test.drop_database().await;
    }
}

fn at(hour: u32, minute: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 13, hour, minute, 0).unwrap()
}

fn seconds(value: i64) -> Duration {
    Duration::seconds(value)
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn actor(actor_id: &str, scraped_count: i64, video_ids: &[&str]) -> ActorPlan {
    ActorPlan {
        actor_id: actor_id.to_owned(),
        scraped_count,
        video_ids: strings(video_ids),
        error_code: None,
        actor_name: None,
        talent_id: None,
        aliases: Vec::new(),
        error_message: None,
    }
}

fn video(video_id: &str, actor_ids: &[&str], candidates: Vec<MoveCandidate>) -> VideoPlan {
    VideoPlan {
        video_id: video_id.to_owned(),
        actor_ids: strings(actor_ids),
        state: VideoState::AdditionalFound,
        release_date: None,
        existing_files: Vec::new(),
        move_candidates: candidates,
        warnings: Vec::new(),
    }
}

fn candidate(candidate_id: &str) -> MoveCandidate {
    MoveCandidate {
        candidate_id: candidate_id.to_owned(),
        video_id: "ABC-001".to_owned(),
        file_name: "ABC-001.mp4".to_owned(),
        source_label: "additional-1".to_owned(),
        destination_conflict: false,
    }
}

fn moved(state: MoveState, error_code: Option<&str>) -> MoveResult {
    MoveResult {
        candidate_id: "candidate-1".to_owned(),
        video_id: "ABC-001".to_owned(),
        file_name: "ABC-001.mp4".to_owned(),
        state,
        error_code: error_code.map(str::to_owned),
    }
}

/// `make_plan_record(tmp_path)`: a plan with one local candidate whose source exists.
fn make_plan_record(tmp: &Path) -> PlanRecord {
    let created_at = at(10, 0);
    let source_root = tmp.join("additional");
    let destination_root = tmp.join("move-in");
    std::fs::create_dir_all(&source_root).unwrap();
    std::fs::create_dir_all(&destination_root).unwrap();
    let source = source_root.join("ABC-001.mp4");
    std::fs::write(&source, b"video").unwrap();
    let destination = destination_root.join("ABC-001.mp4");
    let stat = std::fs::metadata(&source).unwrap();
    let public = FillActorPlan {
        plan_id: "plan-1".to_owned(),
        revision: "revision-1".to_owned(),
        created_at: PyDateTime::utc(created_at),
        expires_at: PyDateTime::utc(created_at + Duration::hours(1)),
        actors: vec![actor("actor-1", 1, &["ABC-001"])],
        videos: vec![video("ABC-001", &["actor-1"], vec![candidate("candidate-1")])],
    };
    let fingerprint = FileFingerprint {
        device: i64::try_from(stat.dev()).unwrap(),
        inode: i64::try_from(stat.ino()).unwrap(),
        size: i64::try_from(stat.size()).unwrap(),
        mtime_ns: stat.mtime() * 1_000_000_000 + stat.mtime_nsec(),
        ctime_ns: stat.ctime() * 1_000_000_000 + stat.ctime_nsec(),
    };
    PlanRecord {
        public,
        candidates: vec![CandidateRecord::local(
            "candidate-1",
            "ABC-001",
            source,
            source_root,
            destination,
            fingerprint,
        )],
        config_version: 0,
    }
}

fn job(job_id: &str, operation: JobOperation, state: JobState, now: DateTime<Utc>, plan_id: Option<&str>) -> JobRecord {
    JobRecord::new(
        job_id,
        operation,
        state,
        now,
        now,
        plan_id.map(str::to_owned),
        Vec::new(),
    )
}

fn journal(plan_id: &str, state: MoveJournalState, updated_at: DateTime<Utc>) -> MoveJournalRecord {
    MoveJournalRecord {
        plan_id: plan_id.to_owned(),
        candidate_id: "candidate-1".to_owned(),
        state,
        updated_at,
    }
}

fn progress(
    stage: JobStage,
    (completed, total): (i64, i64),
    unit: JobProgressUnit,
    current: Option<&str>,
    stage_started_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
) -> JobProgress {
    JobProgress {
        stage,
        completed,
        total: Some(total),
        unit,
        current: current.map(str::to_owned),
        stage_started_at,
        updated_at,
    }
}

// py: tests/test_fill_actor_persistence.py::test_plan_record_rejects_private_candidate_mismatch
#[test]
fn plan_record_rejects_private_candidate_mismatch() {
    let tmp = tempfile::tempdir().unwrap();
    let plan = make_plan_record(tmp.path());
    let mismatched = PlanRecord {
        candidates: Vec::new(),
        ..plan
    };
    let error = mismatched.validate().unwrap_err();
    assert_eq!(
        error.to_string(),
        "private candidate records must match public plan candidates"
    );
}

// py: tests/test_fill_actor_persistence.py::test_repository_contract_round_trips_persistent_records cases=2
#[tokio::test]
async fn repository_contract_round_trips_persistent_records() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    let tmp = tempfile::tempdir().unwrap();
    let plan = make_plan_record(tmp.path());
    let now = plan.public.created_at.to_utc();

    let queued_job = job("job-1", JobOperation::CreatePlan, JobState::Queued, now, Some("plan-1"));
    repository.save_job(&queued_job).await.unwrap();
    assert_eq!(repository.get_job("job-1").await.unwrap(), Some(queued_job.clone()));
    assert_eq!(
        repository.list_jobs(Some(&[JobState::Queued])).await.unwrap(),
        vec![queued_job]
    );

    repository.save_plan(&plan).await.unwrap();

    let loaded_plan = repository.get_plan("plan-1").await.unwrap().expect("the plan");
    assert_eq!(loaded_plan, plan);
    assert_eq!(
        repository.get_candidate("plan-1", "candidate-1").await.unwrap(),
        Some(plan.candidates[0].clone())
    );
    let source = plan.candidates[0].source.to_str().unwrap();
    assert!(!loaded_plan.public.dump_json().contains(source));
    assert_eq!(repository.get_plan("unknown").await.unwrap(), None);

    let result = moved(MoveState::Moved, None);
    repository.save_move_result("plan-1", &result).await.unwrap();
    assert_eq!(
        repository.get_move_result("plan-1", "candidate-1").await.unwrap(),
        Some(result.clone())
    );
    assert_eq!(repository.list_move_results("plan-1").await.unwrap(), vec![result]);

    let running = job("job-1", JobOperation::Apply, JobState::Running, now, Some("plan-1"));
    repository.save_job(&running).await.unwrap();
    assert_eq!(repository.get_job("job-1").await.unwrap(), Some(running.clone()));
    assert_eq!(repository.list_jobs(Some(&[JobState::Queued])).await.unwrap(), vec![]);
    assert_eq!(
        repository.list_jobs(Some(&[JobState::Running])).await.unwrap(),
        vec![running.clone()]
    );
    assert_eq!(repository.list_jobs(None).await.unwrap(), vec![running]);
    assert!(repository.health_check().await.unwrap());

    let prepared = journal("plan-1", MoveJournalState::Prepared, now);
    repository.save_move_journal(&prepared).await.unwrap();
    assert_eq!(
        repository.get_move_journal("plan-1", "candidate-1").await.unwrap(),
        Some(prepared.clone())
    );
    assert_eq!(repository.list_unreconciled_moves().await.unwrap(), vec![prepared]);
    fixture.finish().await;
}

// py: tests/test_fill_actor_persistence.py::test_apply_jobs_are_idempotent_atomic_and_protect_their_plan cases=2
#[tokio::test]
#[allow(clippy::too_many_lines, reason = "one Python test, step for step")]
async fn apply_jobs_are_idempotent_atomic_and_protect_their_plan() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    let tmp = tempfile::tempdir().unwrap();
    let plan = make_plan_record(tmp.path());
    repository.save_plan(&plan).await.unwrap();
    let now = plan.public.created_at.to_utc();
    let mut queued = job(
        "apply-request-0001",
        JobOperation::Apply,
        JobState::Queued,
        now,
        Some("plan-1"),
    );
    queued.progress = progress(JobStage::Queued, (0, 1), JobProgressUnit::Items, None, now, now);
    let record = ApplyJobRecord {
        job: queued.clone(),
        revision: "revision-1".to_owned(),
        candidate_ids: strings(&["candidate-1"]),
        result: None,
    };

    let enqueued = repository.enqueue_apply_job(&record, 1).await.unwrap();
    let repeated = repository.enqueue_apply_job(&record, 1).await.unwrap();
    let conflict = repository
        .enqueue_apply_job(
            &ApplyJobRecord {
                revision: "different-revision".to_owned(),
                ..record.clone()
            },
            1,
        )
        .await
        .unwrap();

    assert_eq!(enqueued.outcome, EnqueueApplyJobOutcome::Enqueued);
    assert_eq!(enqueued.record.as_ref(), Some(&record));
    assert_eq!(repeated.outcome, EnqueueApplyJobOutcome::Existing);
    assert_eq!(repeated.record.as_ref(), Some(&record));
    assert_eq!(conflict.outcome, EnqueueApplyJobOutcome::Conflict);
    assert_eq!(conflict.record, None);
    assert!(!repository.delete_plan("plan-1").await.unwrap());
    let expires_at = plan.public.expires_at.to_utc();
    assert_eq!(
        repository.purge_expired_plans(expires_at + seconds(1)).await.unwrap(),
        0
    );

    let claimed = repository
        .claim_next_job("owner", now + seconds(1), now + seconds(31))
        .await
        .unwrap();
    assert!(claimed.is_some());
    let terminal = progress(
        JobStage::Done,
        (1, 1),
        JobProgressUnit::Items,
        None,
        now + seconds(2),
        now + seconds(2),
    );
    let result = ApplyResult {
        plan_id: "plan-1".to_owned(),
        revision: "revision-1".to_owned(),
        state: ApplyState::Succeeded,
        results: vec![moved(MoveState::Moved, None)],
    };
    assert!(
        !repository
            .finish_owned_job(
                "apply-request-0001",
                "stale-owner",
                JobState::Completed,
                None,
                now + seconds(2),
                &terminal,
                Some(&result),
            )
            .await
            .unwrap()
    );
    let unfinished = repository.get_apply_job("apply-request-0001").await.unwrap().unwrap();
    assert_eq!(unfinished.result, None);
    assert!(
        repository
            .finish_owned_job(
                "apply-request-0001",
                "owner",
                JobState::Completed,
                None,
                now + seconds(2),
                &terminal,
                Some(&result),
            )
            .await
            .unwrap()
    );
    let saved = repository.get_apply_job("apply-request-0001").await.unwrap().unwrap();
    assert_eq!(saved.job.state, JobState::Completed);
    assert_eq!(saved.result.as_ref(), Some(&result));

    assert!(repository.delete_plan("plan-1").await.unwrap());
    let detached = repository.get_apply_job("apply-request-0001").await.unwrap().unwrap();
    assert_eq!(detached.job.plan_id, None);
    assert_eq!(detached.result, Some(result));
    fixture.finish().await;
}

// py: tests/test_fill_actor_persistence.py::test_repository_contract_enforces_move_journal_transitions cases=2
#[tokio::test]
async fn repository_contract_enforces_move_journal_transitions() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    let tmp = tempfile::tempdir().unwrap();
    let plan = make_plan_record(tmp.path());
    repository.save_plan(&plan).await.unwrap();
    let now = plan.public.created_at.to_utc();

    for (offset, state) in [
        MoveJournalState::Prepared,
        MoveJournalState::Linked,
        MoveJournalState::SourceRemoved,
        MoveJournalState::Reconciled,
    ]
    .into_iter()
    .enumerate()
    {
        let record = journal("plan-1", state, now + seconds(i64::try_from(offset).unwrap()));
        repository.save_move_journal(&record).await.unwrap();
        assert_eq!(
            repository.get_move_journal("plan-1", "candidate-1").await.unwrap(),
            Some(record)
        );
    }

    assert_eq!(repository.list_unreconciled_moves().await.unwrap(), vec![]);
    let error = repository
        .save_move_journal(&journal("plan-1", MoveJournalState::Linked, now + seconds(5)))
        .await
        .unwrap_err();
    assert_eq!(error.class_name(), "InvalidMoveJournalTransitionError");
    assert_eq!(
        error.to_string(),
        "invalid move journal transition: reconciled -> linked"
    );
    fixture.finish().await;
}

// py: tests/test_fill_actor_persistence.py::test_repository_contract_purges_expired_plan_dependents cases=2
#[tokio::test]
async fn repository_contract_purges_expired_plan_dependents() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    let tmp = tempfile::tempdir().unwrap();
    let plan = make_plan_record(tmp.path());
    let (created_at, expires_at) = (plan.public.created_at.to_utc(), plan.public.expires_at.to_utc());
    repository.save_plan(&plan).await.unwrap();
    repository
        .save_move_result("plan-1", &moved(MoveState::Moved, None))
        .await
        .unwrap();
    repository
        .save_move_journal(&journal("plan-1", MoveJournalState::Prepared, created_at))
        .await
        .unwrap();
    let completed = job(
        "job-1",
        JobOperation::Apply,
        JobState::Completed,
        created_at,
        Some("plan-1"),
    );
    repository.save_job(&completed).await.unwrap();

    assert_eq!(repository.purge_expired_plans(expires_at).await.unwrap(), 0);
    assert!(repository.get_plan("plan-1").await.unwrap().is_some());
    repository
        .save_move_journal(&journal("plan-1", MoveJournalState::Reconciled, expires_at))
        .await
        .unwrap();
    assert_eq!(repository.purge_expired_plans(expires_at).await.unwrap(), 1);
    assert_eq!(repository.get_plan("plan-1").await.unwrap(), None);
    assert_eq!(repository.get_move_result("plan-1", "candidate-1").await.unwrap(), None);
    assert_eq!(
        repository.get_move_journal("plan-1", "candidate-1").await.unwrap(),
        None
    );
    assert_eq!(
        repository.get_job("job-1").await.unwrap(),
        Some(job("job-1", JobOperation::Apply, JobState::Completed, created_at, None))
    );
    fixture.finish().await;
}

// py: tests/test_fill_actor_persistence.py::test_repository_refuses_to_delete_plan_with_unreconciled_move cases=2
#[tokio::test]
async fn repository_refuses_to_delete_plan_with_unreconciled_move() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    let tmp = tempfile::tempdir().unwrap();
    let plan = make_plan_record(tmp.path());
    repository.save_plan(&plan).await.unwrap();
    repository
        .save_move_journal(&journal(
            "plan-1",
            MoveJournalState::Prepared,
            plan.public.created_at.to_utc(),
        ))
        .await
        .unwrap();

    assert!(!repository.delete_plan("plan-1").await.unwrap());
    assert_eq!(repository.get_plan("plan-1").await.unwrap(), Some(plan));
    assert_eq!(repository.list_unreconciled_moves().await.unwrap().len(), 1);
    fixture.finish().await;
}

// py: tests/test_fill_actor_persistence.py::test_postgres_repository_survives_reconnect
#[tokio::test]
async fn postgres_repository_survives_reconnect() {
    let mut fixture = Fixture::new().await;
    let tmp = tempfile::tempdir().unwrap();
    let plan = make_plan_record(tmp.path());
    let now = plan.public.created_at.to_utc();
    let result = moved(MoveState::Moved, None);
    let mut partial = job(
        "job-1",
        JobOperation::Apply,
        JobState::PartialFailed,
        now,
        Some("plan-1"),
    );
    partial.updated_at = now + seconds(1);
    partial.error_code = Some("one_or_more_moves_failed".to_owned());
    let prepared = journal("plan-1", MoveJournalState::Prepared, now);
    let repository = fixture.repository.clone();
    repository.save_plan(&plan).await.unwrap();
    repository.save_move_result("plan-1", &result).await.unwrap();
    repository.save_job(&partial).await.unwrap();
    repository.save_move_journal(&prepared).await.unwrap();

    let reopened = fixture.reopen();

    assert_eq!(reopened.get_plan("plan-1").await.unwrap(), Some(plan));
    assert_eq!(
        reopened.get_move_result("plan-1", "candidate-1").await.unwrap(),
        Some(result)
    );
    assert_eq!(reopened.get_job("job-1").await.unwrap(), Some(partial));
    assert_eq!(reopened.list_unreconciled_moves().await.unwrap(), vec![prepared]);
    fixture.finish().await;
}

// py: tests/test_fill_actor_persistence.py::test_postgres_repository_applies_explicit_schema_migration
#[tokio::test]
async fn postgres_repository_applies_explicit_schema_migration() {
    let fixture = Fixture::new().await;
    assert!(fixture.repository.health_check().await.unwrap());

    let version: i32 = sqlx::query_scalar("SELECT MAX(version) FROM schema_migrations")
        .fetch_one(fixture.test.pool())
        .await
        .unwrap();
    let rows = sqlx::query("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        .fetch_all(fixture.test.pool())
        .await
        .unwrap();
    assert_eq!(version, embyx_db::migrations::CURRENT_SCHEMA_VERSION);
    let mut tables: Vec<String> = rows.iter().map(|row| row.get("tablename")).collect();
    tables.sort();
    let mut expected = strings(&[
        "schema_migrations",
        "app_config",
        "pipeline_runs",
        "archive_acquisitions",
        "archive_magnet_attempts",
        "fill_actor_apply_jobs",
        "fill_actor_plans",
        "fill_actor_candidates",
        "fill_actor_cloud_move_operations",
        "fill_actor_move_results",
        "fill_actor_jobs",
        "fill_actor_move_journal",
        "feed_subscriptions",
        "playlists",
        "playlist_source",
        "merge_tasks",
        "merge_transcode_chunks",
        "merge_transcode_parts",
        "health_probe",
    ]);
    expected.sort();
    assert_eq!(tables, expected);
    fixture.finish().await;
}

// py: tests/test_fill_actor_persistence.py::test_lease_and_progress_updates_do_not_overwrite_each_other cases=2
#[tokio::test]
async fn lease_and_progress_updates_do_not_overwrite_each_other() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    let now = at(10, 0);
    let queued = JobRecord::new(
        "progress-job",
        JobOperation::CreatePlan,
        JobState::Queued,
        now,
        now,
        Some("progress-job".to_owned()),
        strings(&["actor"]),
    );
    assert!(repository.enqueue_job(&queued, 1).await.unwrap());
    let claimed = repository
        .claim_next_job("owner", now, now + seconds(30))
        .await
        .unwrap()
        .expect("claimed");
    let scanning = progress(
        JobStage::LibraryScan,
        (4, 10),
        JobProgressUnit::Videos,
        Some("ABC-004"),
        now + seconds(1),
        now + seconds(5),
    );

    let (progress_saved, lease_saved) = tokio::join!(
        repository.update_owned_job_progress(&claimed.job_id, "owner", &scanning, now + seconds(6)),
        repository.renew_owned_job_lease(&claimed.job_id, "owner", now + seconds(6), now + seconds(36)),
    );

    assert!(progress_saved.unwrap());
    assert!(lease_saved.unwrap());
    let current = repository.get_job(&claimed.job_id).await.unwrap().unwrap();
    assert_eq!(current.progress, scanning);
    assert_eq!(current.updated_at, now + seconds(6));
    assert_eq!(current.lease_expires_at, Some(now + seconds(36)));
    assert!(
        !repository
            .update_owned_job_progress(&claimed.job_id, "other-owner", &scanning, now + seconds(7))
            .await
            .unwrap()
    );
    assert!(
        !repository
            .update_owned_job_progress(&claimed.job_id, "owner", &scanning, now + seconds(37))
            .await
            .unwrap()
    );
    let after_lease = progress(
        JobStage::Persisting,
        (1, 1),
        JobProgressUnit::Steps,
        Some("saved too late"),
        now + seconds(30),
        now + seconds(37),
    );
    assert!(
        !repository
            .update_owned_job_progress(&claimed.job_id, "owner", &after_lease, now + seconds(37))
            .await
            .unwrap()
    );
    assert!(
        !repository
            .finish_owned_job(
                &claimed.job_id,
                "owner",
                JobState::Completed,
                None,
                now + seconds(37),
                &after_lease,
                None,
            )
            .await
            .unwrap()
    );
    let still_running = repository.get_job(&claimed.job_id).await.unwrap().unwrap();
    assert_eq!(still_running.state, JobState::Running);
    fixture.finish().await;
}

// py: tests/test_fill_actor_persistence.py::test_cancel_queued_job_prevents_claim_and_releases_capacity cases=2
#[tokio::test]
async fn cancel_queued_job_prevents_claim_and_releases_capacity() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    let now = at(10, 0);
    let queued = job(
        "queued-cancel",
        JobOperation::CreatePlan,
        JobState::Queued,
        now,
        Some("queued-cancel"),
    );
    assert!(repository.enqueue_job(&queued, 1).await.unwrap());

    let result = repository.cancel_job("queued-cancel", now + seconds(1)).await.unwrap();

    assert_eq!(result.outcome, CancelJobOutcome::Cancelled);
    assert_eq!(result.previous_state, Some(JobState::Queued));
    assert_eq!(
        repository
            .claim_next_job("owner", now + seconds(2), now + seconds(32))
            .await
            .unwrap(),
        None
    );
    let replacement = job(
        "replacement",
        JobOperation::CreatePlan,
        JobState::Queued,
        now + seconds(2),
        None,
    );
    assert!(repository.enqueue_job(&replacement, 1).await.unwrap());
    fixture.finish().await;
}

// py: tests/test_fill_actor_persistence.py::test_apply_job_cancellation_is_rejected_atomically_before_and_after_enqueue cases=2
#[tokio::test]
async fn apply_job_cancellation_is_rejected_atomically_before_and_after_enqueue() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    let now = at(10, 0);
    let job_id = "apply-cancel-race";

    let before_enqueue = repository.cancel_job(job_id, now).await.unwrap();
    assert_eq!(before_enqueue.outcome, CancelJobOutcome::NotFound);

    let queued = job(job_id, JobOperation::Apply, JobState::Queued, now, Some("plan-1"));
    let record = ApplyJobRecord {
        job: queued.clone(),
        revision: "revision-1".to_owned(),
        candidate_ids: strings(&["candidate-1"]),
        result: None,
    };
    assert_eq!(
        repository.enqueue_apply_job(&record, 1).await.unwrap().outcome,
        EnqueueApplyJobOutcome::Enqueued
    );

    let while_queued = repository.cancel_job(job_id, now + seconds(1)).await.unwrap();
    assert_eq!(while_queued.outcome, CancelJobOutcome::NotCancellable);
    assert_eq!(while_queued.job.as_ref(), Some(&queued));
    assert_eq!(repository.get_job(job_id).await.unwrap(), Some(queued));

    let claimed = repository
        .claim_next_job("owner", now + seconds(2), now + seconds(32))
        .await
        .unwrap()
        .expect("claimed");
    let while_running = repository.cancel_job(job_id, now + seconds(3)).await.unwrap();
    assert_eq!(while_running.outcome, CancelJobOutcome::NotCancellable);
    assert_eq!(while_running.job.as_ref(), Some(&claimed));
    assert_eq!(repository.get_job(job_id).await.unwrap(), Some(claimed));
    fixture.finish().await;
}

// py: tests/test_fill_actor_persistence.py::test_cancel_and_finish_have_one_atomic_winner cases=2
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_and_finish_have_one_atomic_winner() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    let now = at(10, 0);
    let queued = job(
        "finish-race",
        JobOperation::CreatePlan,
        JobState::Queued,
        now,
        Some("finish-race"),
    );
    assert!(repository.enqueue_job(&queued, 1).await.unwrap());
    let claimed = repository
        .claim_next_job("owner", now, now + seconds(30))
        .await
        .unwrap()
        .expect("claimed");

    let (cancel_result, finished) = tokio::join!(
        repository.cancel_job("finish-race", now + seconds(1)),
        repository.finish_owned_job(
            "finish-race",
            "owner",
            JobState::Completed,
            None,
            now + seconds(1),
            &claimed.progress,
            None,
        ),
    );
    let (cancel_result, finished) = (cancel_result.unwrap(), finished.unwrap());
    let current = repository.get_job("finish-race").await.unwrap().unwrap();
    if cancel_result.outcome == CancelJobOutcome::Cancelled {
        assert!(!finished);
        assert_eq!(current.state, JobState::Failed);
        assert_eq!(current.error_code.as_deref(), Some(JOB_CANCELLED_ERROR_CODE));
    } else {
        assert_eq!(cancel_result.outcome, CancelJobOutcome::AlreadyTerminal);
        assert!(finished);
        assert_eq!(current.state, JobState::Completed);
    }
    fixture.finish().await;
}

// py: tests/test_fill_actor_persistence.py::test_postgres_repository_rejects_unknown_future_schema
#[tokio::test]
async fn postgres_repository_rejects_unknown_future_schema() {
    let test = TestDatabase::create().await;
    sqlx::query("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL)")
        .execute(test.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO schema_migrations (version, applied_at) VALUES ($1, $2)")
        .bind(embyx_db::migrations::CURRENT_SCHEMA_VERSION + 1)
        .bind(Utc::now())
        .execute(test.pool())
        .await
        .unwrap();

    let database = test.database();
    let repository = PostgresFillActorRepository::new(database.clone());
    let error = repository.get_job("any").await.unwrap_err();
    assert!(
        matches!(
            error,
            RepositoryError::Store(StoreError::Database(DatabaseError::UnsupportedSchemaVersion(20)))
        ),
        "{error:?}"
    );
    assert_eq!(error.class_name(), "UnsupportedSchemaVersionError");
    database.close().await;
    test.drop_database().await;
}

// py: tests/test_fill_actor_persistence.py::test_job_states_are_stable_public_values
#[test]
fn job_states_are_stable_public_values() {
    assert_eq!(
        JobState::ALL,
        [
            JobState::Queued,
            JobState::Running,
            JobState::Completed,
            JobState::PartialFailed,
            JobState::Failed,
        ]
    );
}

// ---------- test_cloud_move_persistence.py ----------

/// `plan_record(plan_id, candidate_id)`: a plan with one CloudDrive candidate.
fn cloud_plan_record(plan_id: &str, candidate_id: &str) -> PlanRecord {
    let source_path = "/cloud/library/source-b/ABC/ABC-001.mp4";
    let now = now_micros();
    PlanRecord {
        public: FillActorPlan {
            plan_id: plan_id.to_owned(),
            revision: format!("revision-{plan_id}"),
            created_at: PyDateTime::utc(now),
            expires_at: PyDateTime::utc(now + Duration::hours(1)),
            actors: Vec::new(),
            videos: vec![video("ABC-001", &["actor"], vec![candidate(candidate_id)])],
        },
        candidates: vec![CandidateRecord {
            kind: CandidateKind::CloudStrm,
            mapping_sha256: Some("a".repeat(64)),
            cloud_source_path: Some(source_path.to_owned()),
            cloud_destination_dir: Some("/cloud/library/destination/ABC".to_owned()),
            cloud_file: Some(CloudFileMetadata {
                path: source_path.to_owned(),
                file_id: format!("file-{plan_id}"),
                name: "ABC-001.mp4".to_owned(),
                size: 123,
                write_time: 456,
                hashes: vec![("sha1".to_owned(), "abcd".to_owned())],
            }),
            ..CandidateRecord::local(
                candidate_id,
                "ABC-001",
                format!("/mapping/{plan_id}/ABC-001.strm"),
                format!("/mapping/{plan_id}"),
                "/mapping/actor/ABC/ABC-001.strm",
                FileFingerprint {
                    device: 1,
                    inode: 2,
                    size: 3,
                    mtime_ns: 4,
                    ctime_ns: 5,
                },
            )
        }],
        config_version: 0,
    }
}

/// `datetime.now(UTC)`: Python's clock has microseconds.
fn now_micros() -> DateTime<Utc> {
    normalize_datetime(Utc::now())
}

fn operation(
    record: &PlanRecord,
    state: CloudMoveOperationState,
    error_code: Option<&str>,
) -> CloudMoveOperationRecord {
    let candidate = &record.candidates[0];
    CloudMoveOperationRecord {
        plan_id: record.public.plan_id.clone(),
        candidate_id: candidate.candidate_id.clone(),
        attempt_id: format!("attempt-{}", record.public.plan_id),
        source_path: candidate.cloud_source_path.clone().unwrap(),
        destination_dir: candidate.cloud_destination_dir.clone().unwrap(),
        state,
        updated_at: now_micros(),
        error_code: error_code.map(str::to_owned),
    }
}

fn terminal_result(record: &PlanRecord, state: MoveState, error_code: Option<&str>) -> MoveResult {
    let candidate = &record.candidates[0];
    MoveResult {
        candidate_id: candidate.candidate_id.clone(),
        video_id: candidate.video_id.clone(),
        file_name: "ABC-001.mp4".to_owned(),
        state,
        error_code: error_code.map(str::to_owned),
    }
}

// py: tests/test_cloud_move_persistence.py::test_postgres_round_trips_cloud_candidate_and_operation_state
#[tokio::test]
async fn postgres_round_trips_cloud_candidate_and_operation_state() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    let record = cloud_plan_record("plan-1", "candidate-1");
    repository.save_plan(&record).await.unwrap();

    let saved = repository.get_candidate("plan-1", "candidate-1").await.unwrap();

    assert_eq!(saved.as_ref(), Some(&record.candidates[0]));
    for (state, error_code) in [
        (CloudMoveOperationState::Prepared, None),
        (CloudMoveOperationState::Submitting, None),
        (CloudMoveOperationState::Unknown, Some("cloud_move_interrupted")),
    ] {
        repository
            .save_cloud_move_operation(&operation(&record, state, error_code))
            .await
            .unwrap();
    }
    let unresolved = repository.list_unresolved_cloud_moves().await.unwrap();
    assert_eq!(unresolved.len(), 1);
    assert_eq!(unresolved[0].state, CloudMoveOperationState::Unknown);
    assert!(!repository.delete_plan("plan-1").await.unwrap());

    repository
        .finalize_cloud_move(
            &operation(&record, CloudMoveOperationState::Succeeded, None),
            &terminal_result(&record, MoveState::Moved, None),
        )
        .await
        .unwrap();
    assert_eq!(repository.list_unresolved_cloud_moves().await.unwrap(), vec![]);
    assert_eq!(
        repository.get_move_result("plan-1", "candidate-1").await.unwrap(),
        Some(terminal_result(&record, MoveState::Moved, None))
    );
    assert!(repository.delete_plan("plan-1").await.unwrap());
    fixture.finish().await;
}

// py: tests/test_cloud_move_persistence.py::test_postgres_terminal_operation_and_result_finalize_atomically
#[tokio::test]
async fn postgres_terminal_operation_and_result_finalize_atomically() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    let record = cloud_plan_record("plan-1", "candidate-1");
    repository.save_plan(&record).await.unwrap();
    for state in [CloudMoveOperationState::Prepared, CloudMoveOperationState::Submitting] {
        repository
            .save_cloud_move_operation(&operation(&record, state, None))
            .await
            .unwrap();
    }
    let unknown = operation(
        &record,
        CloudMoveOperationState::Unknown,
        Some("cloud_move_interrupted"),
    );
    repository.save_cloud_move_operation(&unknown).await.unwrap();
    let succeeded = operation(&record, CloudMoveOperationState::Succeeded, None);

    let error = repository.save_cloud_move_operation(&succeeded).await.unwrap_err();
    assert!(error.is_value_error(), "{error:?}");
    assert!(error.to_string().contains("finalized with their result"), "{error}");
    let other = MoveResult {
        video_id: "OTHER-001".to_owned(),
        ..terminal_result(&record, MoveState::Moved, None)
    };
    let error = repository.finalize_cloud_move(&succeeded, &other).await.unwrap_err();
    assert!(error.is_value_error(), "{error:?}");
    assert!(
        error.to_string().contains("video id does not match candidate"),
        "{error}"
    );

    assert_eq!(
        repository
            .get_cloud_move_operation("plan-1", "candidate-1")
            .await
            .unwrap(),
        Some(unknown)
    );
    assert_eq!(repository.get_move_result("plan-1", "candidate-1").await.unwrap(), None);
    fixture.finish().await;
}

// py: tests/test_cloud_move_persistence.py::test_postgres_prevents_two_unresolved_operations_for_one_cloud_source
#[tokio::test]
async fn postgres_prevents_two_unresolved_operations_for_one_cloud_source() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    let first = cloud_plan_record("plan-1", "candidate-1");
    let second = cloud_plan_record("plan-2", "candidate-2");
    repository.save_plan(&first).await.unwrap();
    repository.save_plan(&second).await.unwrap();
    repository
        .save_cloud_move_operation(&operation(&first, CloudMoveOperationState::Prepared, None))
        .await
        .unwrap();

    let error = repository
        .save_cloud_move_operation(&operation(&second, CloudMoveOperationState::Prepared, None))
        .await
        .unwrap_err();
    assert!(error.is_value_error(), "{error:?}");
    assert!(
        error.to_string().contains("already has an unresolved operation"),
        "{error}"
    );

    repository
        .finalize_cloud_move(
            &operation(&first, CloudMoveOperationState::Failed, Some("cloud_move_failed")),
            &terminal_result(&first, MoveState::Failed, Some("cloud_move_failed")),
        )
        .await
        .unwrap();
    repository
        .save_cloud_move_operation(&operation(&second, CloudMoveOperationState::Prepared, None))
        .await
        .unwrap();
    fixture.finish().await;
}
