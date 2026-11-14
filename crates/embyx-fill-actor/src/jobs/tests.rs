//! Ported from `tests/test_fill_actor_jobs.py`, against a temporary PostgreSQL database each.
//!
//! The tests Python runs on both repositories (`repository_kind`) run once on PostgreSQL and
//! claim both cases, as the in-memory repository is not ported (D110); the ones Python runs on
//! `MemoryFillActorRepository` alone run on PostgreSQL too. Python's subclassed repositories
//! become [`TestRepository`], one wrapper whose behaviours each test switches on, and its
//! duck-typed services one [`FakeService`] with a mode per class. Where Python's tests list
//! `asyncio.all_tasks()` for the progress timer, these ask the manager how many timers of the
//! job are alive (D132).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, TimeZone as _, Utc};
use embyx_db::database::Database;
use embyx_db::testdb::TestDatabase;

use super::{
    FillActorJobManager, Finish, Heartbeat, HeartbeatStopper, JOB_INTERRUPTED, JobManagerOptions, JobRepository,
    JobService, RepoResult, Wake,
};
use crate::cloud_moves::BoxFuture;
use crate::errors::{FillActorError, FillActorErrorKind};
use crate::models::{ApplyResult, ApplyState, FillActorPlan, MoveResult, MoveState};
use crate::postgres_repository::{PostgresFillActorRepository, RepositoryError};
use crate::records::{
    ApplyJobRecord, CancelJobOutcome, CancelJobResult, EnqueueApplyJobResult, JOB_CANCELLED_ERROR_CODE, JobOperation,
    JobProgress, JobProgressEvent, JobProgressUnit, JobRecord, JobStage, JobState, PlanRecord, normalize_datetime,
};
use crate::service::{Clock, ProgressCallback, ServiceError, StopCallback, TokenFactory};

// ---------- fixtures ----------

/// A database of its own.
struct Db {
    test: TestDatabase,
    databases: Mutex<Vec<Database>>,
}

impl Db {
    async fn new() -> Self {
        Self {
            test: TestDatabase::create().await,
            databases: Mutex::new(Vec::new()),
        }
    }

    /// `make_postgres_repository()`.
    fn postgres(&self) -> PostgresFillActorRepository {
        let database = self.test.database();
        self.databases
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(database.clone());
        PostgresFillActorRepository::new(database)
    }

    fn repository(&self) -> Arc<TestRepository> {
        Arc::new(TestRepository::new(self.postgres()))
    }

    async fn finish(self) {
        let databases = std::mem::take(&mut *self.databases.lock().unwrap_or_else(PoisonError::into_inner));
        for database in &databases {
            database.close().await;
        }
        self.test.drop_database().await;
    }
}

/// `asyncio.Event`, shared by clones.
#[derive(Debug, Clone, Default)]
struct Event(Arc<Wake>);

impl Event {
    fn set(&self) {
        self.0.set();
    }

    fn is_set(&self) -> bool {
        self.0.flag.load(Ordering::SeqCst)
    }

    async fn wait(&self) {
        self.0.wait().await;
    }

    /// `await asyncio.wait_for(event.wait(), timeout=seconds)`.
    async fn wait_for(&self, seconds: f64) {
        tokio::time::timeout(Duration::from_secs_f64(seconds), self.wait())
            .await
            .expect("the event was set in time");
    }
}

/// Sets its event when dropped before [`Self::disarm`]: what an `except asyncio.CancelledError`
/// sees.
struct SetOnDrop(Option<Event>);

impl SetOnDrop {
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for SetOnDrop {
    fn drop(&mut self) {
        if let Some(event) = self.0.take() {
            event.set();
        }
    }
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn event(
    stage: JobStage,
    completed: i64,
    total: i64,
    unit: JobProgressUnit,
    current: Option<&str>,
) -> JobProgressEvent {
    JobProgressEvent {
        stage,
        completed,
        total: Some(total),
        unit,
        current: current.map(str::to_owned),
    }
}

fn plan_at(plan_id: Option<&str>, now: DateTime<Utc>) -> FillActorPlan {
    FillActorPlan {
        plan_id: plan_id.unwrap_or("plan").to_owned(),
        revision: "revision".to_owned(),
        created_at: now.into(),
        expires_at: (now + TimeDelta::hours(1)).into(),
        actors: Vec::new(),
        videos: Vec::new(),
    }
}

/// `MutableProgressClock`.
#[derive(Clone)]
struct MutableClock(Arc<Mutex<DateTime<Utc>>>);

impl MutableClock {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(
            Utc.with_ymd_and_hms(2026, 7, 13, 10, 0, 0).unwrap(),
        )))
    }

    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn advance(&self, seconds: i64) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) += TimeDelta::seconds(seconds);
    }

    fn clock(&self) -> Clock {
        let clock = self.clone();
        Arc::new(move || clock.now())
    }
}

// ---------- the services ----------

/// How `create_plan` behaves: one mode per Python service class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlanMode {
    /// `ControlledService` (and `CancellationAwareService`, whose `cancelled` every mode sets).
    Controlled,
    /// `DirtyProgressService`.
    Dirty,
    /// `FailingDirtyProgressService(FillActorError)`.
    FailingDomain,
    /// `FailingDirtyProgressService(RuntimeError)`.
    FailingUnexpected,
    /// `ScopeProgressService(clock)`.
    Scope,
}

/// The duck-typed services of the Python tests.
struct FakeService {
    mode: PlanMode,
    block: AtomicBool,
    repository: Option<Arc<TestRepository>>,
    rapid_updates: i64,
    started: Event,
    release: Event,
    /// Set when a `create_plan` call is dropped before it returns (`CancellationAwareService`).
    cancelled: Event,
    clock: Option<MutableClock>,
    // `ControlledApplyService`
    apply_started: Event,
    apply_release: Event,
    block_apply: bool,
    result_state: ApplyState,
    apply_calls: Mutex<Vec<(String, String, Vec<String>)>>,
    processed_candidates: Mutex<Vec<String>>,
    /// `roots_ready()` never answers: a library mount that hangs.
    stalled_roots: AtomicBool,
}

impl FakeService {
    fn new(mode: PlanMode) -> Self {
        Self {
            mode,
            block: AtomicBool::new(false),
            repository: None,
            rapid_updates: 0,
            started: Event::default(),
            release: Event::default(),
            cancelled: Event::default(),
            clock: None,
            apply_started: Event::default(),
            apply_release: Event::default(),
            block_apply: false,
            result_state: ApplyState::Succeeded,
            apply_calls: Mutex::new(Vec::new()),
            processed_candidates: Mutex::new(Vec::new()),
            stalled_roots: AtomicBool::new(false),
        }
    }

    /// `ControlledService(block=...)`.
    fn controlled(block: bool) -> Self {
        let service = Self::new(PlanMode::Controlled);
        service.block.store(block, Ordering::SeqCst);
        service
    }

    /// `CancellationAwareService()`.
    fn cancellation_aware() -> Self {
        Self::controlled(true)
    }

    fn with_repository(mut self, repository: &Arc<TestRepository>) -> Self {
        self.repository = Some(Arc::clone(repository));
        self
    }

    /// `ControlledApplyService(repository=, block_apply=, result_state=)`.
    fn apply(repository: &Arc<TestRepository>, block_apply: bool, result_state: ApplyState) -> Self {
        let mut service = Self::new(PlanMode::Controlled).with_repository(repository);
        service.block_apply = block_apply;
        service.result_state = result_state;
        service
    }

    fn processed(&self) -> Vec<String> {
        self.processed_candidates
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// `ControlledService.create_plan`.
    async fn controlled_plan(
        &self,
        plan_id: Option<&str>,
        progress: &ProgressCallback,
    ) -> Result<FillActorPlan, ServiceError> {
        self.started.set();
        progress(event(
            JobStage::ActorCatalog,
            0,
            1,
            JobProgressUnit::Actors,
            Some("actor"),
        ))
        .await;
        if self.block.load(Ordering::SeqCst) {
            self.release.wait().await;
        }
        if self.rapid_updates > 0 {
            for completed in 0..=self.rapid_updates {
                let current = format!("video-{completed}");
                progress(event(
                    JobStage::LibraryScan,
                    completed,
                    self.rapid_updates,
                    JobProgressUnit::Videos,
                    Some(&current),
                ))
                .await;
            }
        }
        progress(event(JobStage::Persisting, 1, 1, JobProgressUnit::Steps, Some("saved"))).await;
        let plan = plan_at(plan_id, Utc::now());
        if let Some(repository) = &self.repository {
            let record = PlanRecord {
                public: plan.clone(),
                candidates: Vec::new(),
                config_version: 0,
            };
            repository.inner.save_plan(&record).await?;
        }
        Ok(plan)
    }

    /// The two dirty events `DirtyProgressService` and `FailingDirtyProgressService` start with.
    async fn dirty_events(&self, progress: &ProgressCallback) {
        progress(event(
            JobStage::ActorCatalog,
            0,
            2,
            JobProgressUnit::Actors,
            Some("actor-0"),
        ))
        .await;
        progress(event(
            JobStage::ActorCatalog,
            1,
            2,
            JobProgressUnit::Actors,
            Some("actor-1"),
        ))
        .await;
        self.started.set();
    }

    /// `ScopeProgressService.create_plan`.
    async fn scope_plan(&self, plan_id: Option<&str>, progress: &ProgressCallback) -> FillActorPlan {
        let clock = self.clock.as_ref().expect("a scope service has a clock");
        progress(JobProgressEvent {
            stage: JobStage::ActorCatalog,
            completed: 0,
            total: Some(1),
            unit: JobProgressUnit::Actors,
            current: None,
        })
        .await;
        clock.advance(5);
        progress(event(
            JobStage::ActorCatalog,
            1,
            26,
            JobProgressUnit::Pages,
            Some("页面 1/26"),
        ))
        .await;
        clock.advance(5);
        progress(event(
            JobStage::ActorCatalog,
            2,
            26,
            JobProgressUnit::Pages,
            Some("页面 2/26"),
        ))
        .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        clock.advance(5);
        progress(event(
            JobStage::ActorCatalog,
            0,
            26,
            JobProgressUnit::Pages,
            Some("下一页范围"),
        ))
        .await;
        clock.advance(5);
        progress(event(
            JobStage::ActorCatalog,
            1,
            1,
            JobProgressUnit::Actors,
            Some("actor"),
        ))
        .await;
        plan_at(plan_id, clock.now())
    }
}

impl JobService for FakeService {
    fn validate_actor_ids(&self, actor_ids: &[String]) -> Result<Vec<String>, FillActorError> {
        Ok(actor_ids.to_vec())
    }

    fn roots_ready(&self) -> BoxFuture<'_, Result<bool, ServiceError>> {
        Box::pin(async {
            if self.stalled_roots.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            Ok(true)
        })
    }

    fn apply_ready(&self) -> BoxFuture<'_, Result<bool, ServiceError>> {
        Box::pin(async { Ok(true) })
    }

    fn create_plan<'a>(
        &'a self,
        _actor_ids: &'a [String],
        plan_id: Option<&'a str>,
        progress: ProgressCallback,
    ) -> BoxFuture<'a, Result<FillActorPlan, ServiceError>> {
        Box::pin(async move {
            let dropped = SetOnDrop(Some(self.cancelled.clone()));
            let result = match self.mode {
                PlanMode::Controlled => self.controlled_plan(plan_id, &progress).await,
                PlanMode::Dirty => {
                    self.dirty_events(&progress).await;
                    self.release.wait().await;
                    self.controlled_plan(plan_id, &progress).await
                }
                PlanMode::FailingDomain => {
                    self.dirty_events(&progress).await;
                    Err(FillActorError::new(FillActorErrorKind::Base, "simulated plan failure").into())
                }
                PlanMode::FailingUnexpected => {
                    self.dirty_events(&progress).await;
                    Err(ServiceError::Raised {
                        class: "RuntimeError".to_owned(),
                        message: "simulated plan failure".to_owned(),
                    })
                }
                PlanMode::Scope => Ok(self.scope_plan(plan_id, &progress).await),
            };
            dropped.disarm();
            result
        })
    }

    fn get_plan<'a>(&'a self, plan_id: &'a str) -> BoxFuture<'a, Result<FillActorPlan, ServiceError>> {
        Box::pin(async move {
            let repository = self.repository.as_ref().expect("a service with a repository");
            let record = repository.inner.get_plan(plan_id).await?;
            Ok(record.expect("a stored plan").public)
        })
    }

    fn apply<'a>(
        &'a self,
        plan_id: &'a str,
        revision: &'a str,
        candidate_ids: &'a [String],
        progress: ProgressCallback,
        stop_requested: StopCallback,
    ) -> BoxFuture<'a, Result<ApplyResult, ServiceError>> {
        Box::pin(async move {
            let selected = candidate_ids.to_vec();
            self.apply_calls.lock().unwrap_or_else(PoisonError::into_inner).push((
                plan_id.to_owned(),
                revision.to_owned(),
                selected.clone(),
            ));
            self.apply_started.set();
            let total = i64::try_from(selected.len()).unwrap();
            progress(event(
                JobStage::Unknown,
                0,
                total,
                JobProgressUnit::Items,
                (!selected.is_empty()).then_some("ABC-001.mp4"),
            ))
            .await;
            if self.block_apply {
                self.apply_release.wait().await;
            }
            let succeeded = self.result_state == ApplyState::Succeeded;
            let mut results = Vec::new();
            for (index, candidate_id) in selected.iter().enumerate() {
                if index > 0 && stop_requested() {
                    break;
                }
                self.processed_candidates
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(candidate_id.clone());
                results.push(MoveResult {
                    candidate_id: candidate_id.clone(),
                    video_id: "ABC-001".to_owned(),
                    file_name: "ABC-001.mp4".to_owned(),
                    state: if succeeded { MoveState::Moved } else { MoveState::Failed },
                    error_code: (!succeeded).then(|| "simulated_failure".to_owned()),
                });
                let completed = i64::try_from(index + 1).unwrap();
                progress(JobProgressEvent {
                    stage: JobStage::Unknown,
                    completed,
                    total: Some(total),
                    unit: JobProgressUnit::Items,
                    current: None,
                })
                .await;
            }
            Ok(ApplyResult {
                plan_id: plan_id.to_owned(),
                revision: revision.to_owned(),
                state: self.result_state,
                results,
            })
        })
    }
}

// ---------- the repositories ----------

/// The PostgreSQL repository, with the behaviours of Python's repository subclasses.
struct TestRepository {
    inner: PostgresFillActorRepository,
    /// `FlakyHeartbeatRepository.remaining_heartbeat_failures`.
    heartbeat_failures: AtomicUsize,
    /// `CountingProgressRepository`.
    progress_writes: AtomicUsize,
    progress_records: Mutex<Vec<JobProgress>>,
    /// `FailingFinishRepository`.
    failing_finish: bool,
    /// `PausedClaimRepository`.
    pause_claim: bool,
    claimed: Event,
    release_claim: Event,
    /// `PausedCancelRepository`.
    pause_cancel: bool,
    cancel_persisted: Event,
    release_cancel: Event,
    /// `OwnershipLossRepository`.
    rejected_renewal: Event,
}

impl TestRepository {
    fn new(inner: PostgresFillActorRepository) -> Self {
        Self {
            inner,
            heartbeat_failures: AtomicUsize::new(0),
            progress_writes: AtomicUsize::new(0),
            progress_records: Mutex::new(Vec::new()),
            failing_finish: false,
            pause_claim: false,
            claimed: Event::default(),
            release_claim: Event::default(),
            pause_cancel: false,
            cancel_persisted: Event::default(),
            release_cancel: Event::default(),
            rejected_renewal: Event::default(),
        }
    }

    fn records(&self) -> Vec<JobProgress> {
        self.progress_records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    async fn job(&self, job_id: &str) -> JobRecord {
        self.inner.get_job(job_id).await.unwrap().expect("a stored job")
    }

    /// `wait_for_state(repository, job_id, states)`.
    async fn wait_for_state(&self, job_id: &str, states: &[JobState]) -> JobRecord {
        for _ in 0..400 {
            if let Some(job) = self.inner.get_job(job_id).await.unwrap()
                && states.contains(&job.state)
            {
                return job;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("job did not reach expected state");
    }
}

impl JobRepository for TestRepository {
    fn enqueue_job<'a>(&'a self, record: &'a JobRecord, max_active: i64) -> BoxFuture<'a, RepoResult<bool>> {
        JobRepository::enqueue_job(&self.inner, record, max_active)
    }

    fn enqueue_apply_job<'a>(
        &'a self,
        record: &'a ApplyJobRecord,
        max_active: i64,
    ) -> BoxFuture<'a, RepoResult<EnqueueApplyJobResult>> {
        JobRepository::enqueue_apply_job(&self.inner, record, max_active)
    }

    fn get_job<'a>(&'a self, job_id: &'a str) -> BoxFuture<'a, RepoResult<Option<JobRecord>>> {
        JobRepository::get_job(&self.inner, job_id)
    }

    fn get_apply_job<'a>(&'a self, job_id: &'a str) -> BoxFuture<'a, RepoResult<Option<ApplyJobRecord>>> {
        JobRepository::get_apply_job(&self.inner, job_id)
    }

    fn get_plan<'a>(&'a self, plan_id: &'a str) -> BoxFuture<'a, RepoResult<Option<PlanRecord>>> {
        JobRepository::get_plan(&self.inner, plan_id)
    }

    fn claim_next_job<'a>(
        &'a self,
        owner_id: &'a str,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> BoxFuture<'a, RepoResult<Option<JobRecord>>> {
        Box::pin(async move {
            let job = self.inner.claim_next_job(owner_id, now, lease_expires_at).await?;
            if self.pause_claim && job.is_some() {
                self.claimed.set();
                self.release_claim.wait().await;
            }
            Ok(job)
        })
    }

    fn renew_owned_job_lease<'a>(
        &'a self,
        job_id: &'a str,
        owner_id: &'a str,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> BoxFuture<'a, RepoResult<bool>> {
        Box::pin(async move {
            if self
                .heartbeat_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| left.checked_sub(1))
                .is_ok()
            {
                return Err(RepositoryError::Runtime("temporary database error"));
            }
            let owned = self
                .inner
                .renew_owned_job_lease(job_id, owner_id, now, lease_expires_at)
                .await?;
            if !owned {
                self.rejected_renewal.set();
            }
            Ok(owned)
        })
    }

    fn update_owned_job_progress<'a>(
        &'a self,
        job_id: &'a str,
        owner_id: &'a str,
        progress: &'a JobProgress,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, RepoResult<bool>> {
        Box::pin(async move {
            self.progress_writes.fetch_add(1, Ordering::SeqCst);
            let saved = self
                .inner
                .update_owned_job_progress(job_id, owner_id, progress, now)
                .await?;
            if saved {
                self.progress_records
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(progress.clone());
            }
            Ok(saved)
        })
    }

    fn finish_owned_job<'a>(&'a self, finish: Finish<'a>) -> BoxFuture<'a, RepoResult<bool>> {
        if self.failing_finish {
            return Box::pin(async { Err(RepositoryError::Runtime("terminal persistence unavailable")) });
        }
        JobRepository::finish_owned_job(&self.inner, finish)
    }

    fn cancel_job<'a>(&'a self, job_id: &'a str, now: DateTime<Utc>) -> BoxFuture<'a, RepoResult<CancelJobResult>> {
        Box::pin(async move {
            let result = self.inner.cancel_job(job_id, now).await?;
            if self.pause_cancel {
                self.cancel_persisted.set();
                self.release_cancel.wait().await;
            }
            Ok(result)
        })
    }

    fn fail_expired_jobs<'a>(&'a self, now: DateTime<Utc>, error_code: &'a str) -> BoxFuture<'a, RepoResult<usize>> {
        JobRepository::fail_expired_jobs(&self.inner, now, error_code)
    }
}

// ---------- the manager ----------

/// Python's `lease_duration=timedelta(milliseconds=150)`, tripled: a claim must renew its lease
/// before it runs out, and the parallel tests on one PostgreSQL can take longer than 150 ms
/// for that.
const SHORT_LEASE: Duration = Duration::from_millis(450);

/// `FillActorJobManager(..., max_concurrent_jobs=, max_active_jobs=, poll_interval=0.01)`.
fn options(max_concurrent_jobs: usize, max_active_jobs: usize) -> JobManagerOptions {
    JobManagerOptions {
        max_concurrent_jobs,
        max_active_jobs,
        poll_interval: Duration::from_millis(10),
        ..JobManagerOptions::default()
    }
}

fn manager(
    service: &Arc<FakeService>,
    repository: &Arc<TestRepository>,
    options: JobManagerOptions,
) -> FillActorJobManager {
    let service: Arc<dyn JobService> = service.clone();
    let repository: Arc<dyn JobRepository> = repository.clone();
    FillActorJobManager::new(service, repository, options).expect("valid options")
}

/// A `_stop_heartbeat` held open until `release` is set, as the tests monkeypatch it.
struct BlockedStop {
    calls: Arc<AtomicUsize>,
    started: Event,
    release: Event,
    cancelled: Event,
}

impl BlockedStop {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            started: Event::default(),
            release: Event::default(),
            cancelled: Event::default(),
        }
    }

    /// The stopper; `failure` is what it raises after stopping the heartbeat.
    fn stopper(&self, failure: Option<&'static str>) -> HeartbeatStopper {
        let (calls, started, release, cancelled) = (
            Arc::clone(&self.calls),
            self.started.clone(),
            self.release.clone(),
            self.cancelled.clone(),
        );
        Arc::new(move |heartbeat: Heartbeat| {
            let (calls, started, release, cancelled) =
                (Arc::clone(&calls), started.clone(), release.clone(), cancelled.clone());
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                started.set();
                let dropped = SetOnDrop(Some(cancelled));
                release.wait().await;
                dropped.disarm();
                heartbeat.stop().await;
                failure.map_or(Ok(()), |message| Err(message.to_owned()))
            })
        })
    }
}

fn is_kind(error: &ServiceError, kind: FillActorErrorKind) -> bool {
    error.as_fill_actor().is_some_and(|error| error.kind == kind)
}

async fn sleep_ms(milliseconds: u64) {
    tokio::time::sleep(Duration::from_millis(milliseconds)).await;
}

fn apply_job(now: DateTime<Utc>) -> ApplyJobRecord {
    let mut job = JobRecord::new(
        "apply-request-0001",
        JobOperation::Apply,
        JobState::Queued,
        now,
        now,
        Some("plan-1".to_owned()),
        Vec::new(),
    );
    job.progress = JobProgress {
        stage: JobStage::Queued,
        completed: 0,
        total: Some(1),
        unit: JobProgressUnit::Items,
        current: None,
        stage_started_at: now,
        updated_at: now,
    };
    ApplyJobRecord {
        job,
        revision: "revision-1".to_owned(),
        candidate_ids: strings(&["candidate-1"]),
        result: None,
    }
}

// ---------- the tests ----------

// py: tests/test_fill_actor_jobs.py::test_repository_atomically_enqueues_claims_and_expires_leases cases=2
#[tokio::test]
async fn repository_atomically_enqueues_claims_and_expires_leases() {
    let db = Db::new().await;
    let repository = db.postgres();
    let now = Utc.with_ymd_and_hms(2026, 7, 13, 0, 0, 0).unwrap();
    let seconds = TimeDelta::seconds;
    let queued = JobRecord::new(
        "job-1",
        JobOperation::CreatePlan,
        JobState::Queued,
        now,
        now,
        Some("job-1".to_owned()),
        strings(&["actor"]),
    );
    assert!(repository.enqueue_job(&queued, 1).await.unwrap());
    let second = JobRecord::new(
        "job-2",
        JobOperation::CreatePlan,
        JobState::Queued,
        now,
        now,
        None,
        Vec::new(),
    );
    assert!(!repository.enqueue_job(&second, 1).await.unwrap());

    let (first, second) = tokio::join!(
        repository.claim_next_job("owner-a", now, now + seconds(10)),
        repository.claim_next_job("owner-b", now, now + seconds(10)),
    );
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_ne!(first.is_none(), second.is_none());
    let claimed = first.or(second).expect("one claim");
    assert_eq!(claimed.actor_ids, strings(&["actor"]));
    let owner = claimed.owner_id.clone().unwrap();

    assert!(
        !repository
            .renew_owned_job_lease(&claimed.job_id, "old-owner", now + seconds(5), now + seconds(20))
            .await
            .unwrap()
    );
    assert!(
        repository
            .renew_owned_job_lease(&claimed.job_id, &owner, now + seconds(5), now + seconds(20))
            .await
            .unwrap()
    );
    let progress = JobProgress {
        stage: JobStage::ActorCatalog,
        completed: 1,
        total: Some(1),
        unit: JobProgressUnit::Actors,
        current: Some("actor".to_owned()),
        stage_started_at: now,
        updated_at: now + seconds(6),
    };
    assert!(
        !repository
            .update_owned_job_progress(&claimed.job_id, "old-owner", &progress, now + seconds(6))
            .await
            .unwrap()
    );
    assert!(
        repository
            .update_owned_job_progress(&claimed.job_id, &owner, &progress, now + seconds(6))
            .await
            .unwrap()
    );
    assert_eq!(
        repository
            .fail_expired_jobs(now + seconds(15), JOB_INTERRUPTED)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        repository
            .fail_expired_jobs(now + seconds(21), JOB_INTERRUPTED)
            .await
            .unwrap(),
        1
    );
    let failed = repository.get_job(&claimed.job_id).await.unwrap().unwrap();
    assert_eq!(failed.state, JobState::Failed);
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_manager_recovers_queued_apply_and_persists_business_result cases=2
#[tokio::test]
async fn manager_recovers_queued_apply_and_persists_business_result() {
    let db = Db::new().await;
    let repository = db.repository();
    let queued = apply_job(normalize_datetime(Utc::now()));
    assert_eq!(
        repository
            .inner
            .enqueue_apply_job(&queued, 1)
            .await
            .unwrap()
            .record
            .as_ref(),
        Some(&queued)
    );
    let service = Arc::new(FakeService::apply(&repository, false, ApplyState::Failed));
    let manager = manager(&service, &repository, options(1, 1));

    manager.start().await.unwrap();
    let completed = repository
        .wait_for_state(&queued.job.job_id, &[JobState::Completed])
        .await;
    let stored = repository.inner.get_apply_job(&queued.job.job_id).await.unwrap();
    manager.aclose().await;

    assert_eq!(
        *service.apply_calls.lock().unwrap(),
        vec![("plan-1".to_owned(), "revision-1".to_owned(), strings(&["candidate-1"]))]
    );
    assert!(!service.started.is_set());
    assert_eq!(completed.operation, JobOperation::Apply);
    assert_eq!(completed.progress.completed, 1);
    assert_eq!(completed.progress.total, Some(1));
    assert_eq!(completed.progress.current, None);
    let stored = stored.expect("the apply job");
    assert_eq!(stored.result.expect("a result").state, ApplyState::Failed);
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_apply_job_rejects_cancel_and_shutdown_drains_current_move
#[tokio::test]
async fn apply_job_rejects_cancel_and_shutdown_drains_current_move() {
    let db = Db::new().await;
    let repository = db.repository();
    let service = Arc::new(FakeService::apply(&repository, true, ApplyState::Succeeded));
    let manager = manager(&service, &repository, options(1, 1));
    let record = manager
        .start_apply("plan-1", "revision-1", &strings(&["candidate-1"]), "apply-request-0001")
        .await
        .unwrap();
    service.apply_started.wait_for(1.0).await;

    let refused = manager.cancel_plan(&record.job.job_id).await.unwrap_err();
    assert!(is_kind(&refused, FillActorErrorKind::ApplyJobNotCancellable));
    let closing = tokio::spawn({
        let manager = manager.clone();
        async move { manager.aclose().await }
    });
    sleep_ms(20).await;
    let running = repository.job(&record.job.job_id).await;
    assert!(!closing.is_finished());
    assert_eq!(running.state, JobState::Running);
    assert_eq!(running.error_code, None);

    service.apply_release.set();
    tokio::time::timeout(Duration::from_secs(1), closing)
        .await
        .unwrap()
        .unwrap();
    let completed = repository
        .inner
        .get_apply_job(&record.job.job_id)
        .await
        .unwrap()
        .expect("the apply job");

    assert_eq!(completed.job.state, JobState::Completed);
    assert_eq!(completed.job.error_code, None);
    assert_eq!(completed.result.expect("a result").results[0].state, MoveState::Moved);
    db.finish().await;
}

/// A worker held up in the roots check by a hung mount does not hold up `aclose()`, and the
/// job it was about to claim stays queued for the next process: Python cancels the worker
/// there (D131).
#[tokio::test]
async fn shutdown_does_not_wait_for_a_stalled_roots_check() {
    let db = Db::new().await;
    let repository = db.repository();
    let service = Arc::new(FakeService::new(PlanMode::Controlled));
    service.stalled_roots.store(true, Ordering::SeqCst);
    let manager = manager(&service, &repository, options(1, 1));
    let job = manager.start_plan(&strings(&["actor"])).await.unwrap();
    sleep_ms(30).await;

    tokio::time::timeout(Duration::from_secs(1), manager.aclose())
        .await
        .expect("aclose returned");
    assert_eq!(repository.job(&job.job_id).await.state, JobState::Queued);
    assert!(!service.started.is_set());
    db.finish().await;
}

/// A shutdown that lands after the apply ended, while the job is still stopping its
/// heartbeat, keeps what the apply did: Python hands the finished task to
/// `_cleanup_stopped_apply`, which completes the job with its result.
#[tokio::test]
async fn shutdown_after_the_apply_ended_keeps_its_result() {
    let db = Db::new().await;
    let repository = db.repository();
    let service = Arc::new(FakeService::apply(&repository, false, ApplyState::Succeeded));
    let stopping = BlockedStop::new();
    let manager = manager(
        &service,
        &repository,
        JobManagerOptions {
            stop_heartbeat: Some(stopping.stopper(None)),
            ..options(1, 1)
        },
    );
    let record = manager
        .start_apply("plan-1", "revision-1", &strings(&["candidate-1"]), "apply-request-0001")
        .await
        .unwrap();
    // The apply has returned; the job is stopping its heartbeat before it saves the result.
    stopping.started.wait_for(1.0).await;
    tokio::time::timeout(Duration::from_secs(1), manager.aclose())
        .await
        .unwrap();

    assert!(stopping.cancelled.is_set());
    let stored = repository
        .inner
        .get_apply_job(&record.job.job_id)
        .await
        .unwrap()
        .expect("the apply job");
    assert_eq!(stored.job.state, JobState::Completed);
    assert_eq!(stored.job.error_code, None);
    assert_eq!(stored.result.expect("a result").results[0].state, MoveState::Moved);
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_apply_job_ownership_loss_drains_only_current_candidate
#[tokio::test]
async fn apply_job_ownership_loss_drains_only_current_candidate() {
    let db = Db::new().await;
    let repository = db.repository();
    let service = Arc::new(FakeService::apply(&repository, true, ApplyState::Succeeded));
    let manager = manager(
        &service,
        &repository,
        JobManagerOptions {
            lease_duration: SHORT_LEASE,
            ..options(1, 1)
        },
    );
    let record = manager
        .start_apply(
            "plan-1",
            "revision-1",
            &strings(&["candidate-1", "candidate-2"]),
            "apply-ownership-loss",
        )
        .await
        .unwrap();
    service.apply_started.wait_for(1.0).await;
    let running = repository
        .wait_for_state(&record.job.job_id, &[JobState::Running])
        .await;
    let expires = running.lease_expires_at.expect("a lease");

    assert_eq!(
        repository
            .inner
            .fail_expired_jobs(expires + TimeDelta::seconds(1), JOB_INTERRUPTED)
            .await
            .unwrap(),
        1
    );
    repository.rejected_renewal.wait_for(1.0).await;
    tokio::task::yield_now().await;
    service.apply_release.set();
    for _ in 0..100 {
        if !service.processed().is_empty() {
            break;
        }
        sleep_ms(5).await;
    }
    manager.aclose().await;

    let stored = repository
        .inner
        .get_apply_job(&record.job.job_id)
        .await
        .unwrap()
        .expect("the apply job");
    assert_eq!(service.processed(), strings(&["candidate-1"]));
    assert_eq!(stored.job.state, JobState::Failed);
    assert_eq!(stored.job.error_code.as_deref(), Some(JOB_INTERRUPTED));
    assert_eq!(stored.result, None);
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_manager_capacity_and_shutdown_are_persisted
#[tokio::test]
async fn manager_capacity_and_shutdown_are_persisted() {
    let db = Db::new().await;
    let repository = db.repository();
    let service = Arc::new(FakeService::controlled(true));
    let manager = manager(&service, &repository, options(1, 1));
    let first = manager.start_plan(&strings(&["actor-a"])).await.unwrap();
    service.started.wait_for(1.0).await;

    let full = manager.start_plan(&strings(&["actor-b"])).await.unwrap_err();
    assert!(is_kind(&full, FillActorErrorKind::JobQueueFull));

    manager.aclose().await;
    let interrupted = repository.job(&first.job_id).await;
    assert_eq!(interrupted.state, JobState::Failed);
    assert_eq!(interrupted.error_code.as_deref(), Some(JOB_INTERRUPTED));
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_shutdown_remains_bounded_when_terminal_persistence_fails
#[tokio::test]
async fn shutdown_remains_bounded_when_terminal_persistence_fails() {
    let db = Db::new().await;
    let repository = Arc::new(TestRepository {
        failing_finish: true,
        ..TestRepository::new(db.postgres())
    });
    let service = Arc::new(FakeService::controlled(true));
    let manager = manager(&service, &repository, options(1, 1));
    manager.start_plan(&strings(&["actor"])).await.unwrap();
    service.started.wait_for(1.0).await;

    tokio::time::timeout(Duration::from_secs(1), manager.aclose())
        .await
        .expect("shutdown is bounded");
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_running_user_cancel_waits_for_execution_cleanup_and_worker_stays_available
#[tokio::test]
async fn running_user_cancel_waits_for_execution_cleanup_and_worker_stays_available() {
    let db = Db::new().await;
    let repository = db.repository();
    let service = Arc::new(FakeService::cancellation_aware());
    let manager = manager(&service, &repository, options(1, 2));
    let first = manager.start_plan(&strings(&["actor-a"])).await.unwrap();
    service.started.wait_for(1.0).await;

    let result = tokio::time::timeout(Duration::from_secs(1), manager.cancel_plan(&first.job_id))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.outcome, CancelJobOutcome::Cancelled);
    assert!(service.cancelled.is_set());
    let cancelled = repository.job(&first.job_id).await;
    assert_eq!(cancelled.state, JobState::Failed);
    assert_eq!(cancelled.error_code.as_deref(), Some(JOB_CANCELLED_ERROR_CODE));

    service.block.store(false, Ordering::SeqCst);
    let second = manager.start_plan(&strings(&["actor-b"])).await.unwrap();
    let completed = repository.wait_for_state(&second.job_id, &[JobState::Completed]).await;
    manager.aclose().await;

    assert_eq!(completed.state, JobState::Completed);
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_repeated_cancel_waits_for_one_cleanup_and_clears_progress_timer
#[tokio::test]
async fn repeated_cancel_waits_for_one_cleanup_and_clears_progress_timer() {
    let db = Db::new().await;
    let repository = db.repository();
    let service = Arc::new(FakeService::new(PlanMode::Dirty));
    let cleanup = BlockedStop::new();
    let manager = manager(
        &service,
        &repository,
        JobManagerOptions {
            progress_flush_interval: Duration::from_mins(1),
            // The cleanup's one awaited step, held open so a second cancel has to wait for it.
            stop_heartbeat: Some(cleanup.stopper(Some("simulated cleanup repository failure"))),
            ..options(1, 1)
        },
    );
    let job = manager.start_plan(&strings(&["actor"])).await.unwrap();
    service.started.wait_for(1.0).await;
    let cancel = |manager: &FillActorJobManager| {
        let (manager, job_id) = (manager.clone(), job.job_id.clone());
        tokio::spawn(async move { manager.cancel_plan(&job_id).await })
    };
    let first = cancel(&manager);
    cleanup.started.wait_for(1.0).await;
    let second = cancel(&manager);
    sleep_ms(50).await;

    assert!(!first.is_finished());
    assert!(!second.is_finished());
    assert!(!cleanup.cancelled.is_set());
    assert_eq!(cleanup.calls.load(Ordering::SeqCst), 1);
    assert!(manager.progress_timers(&job.job_id) > 0);

    cleanup.release.set();
    let (first, second) = tokio::join!(first, second);
    tokio::task::yield_now().await;
    let mut outcomes = vec![first.unwrap().unwrap().outcome, second.unwrap().unwrap().outcome];
    outcomes.sort_by_key(|outcome| format!("{outcome:?}"));
    assert_eq!(
        outcomes,
        vec![CancelJobOutcome::AlreadyCancelled, CancelJobOutcome::Cancelled]
    );
    assert!(!cleanup.cancelled.is_set());
    assert_eq!(cleanup.calls.load(Ordering::SeqCst), 1);
    assert_eq!(manager.progress_timers(&job.job_id), 0);
    assert!(manager.running_jobs().is_empty());
    manager.aclose().await;
    db.finish().await;
}

/// `test_cancel_during_failed_plan_cleanup_waits_and_clears_progress_timer[failure_type]`.
async fn cancel_during_failed_plan_cleanup_waits_and_clears_progress_timer(mode: PlanMode) {
    let db = Db::new().await;
    let repository = db.repository();
    let service = Arc::new(FakeService::new(mode));
    let cleanup = BlockedStop::new();
    let manager = manager(
        &service,
        &repository,
        JobManagerOptions {
            progress_flush_interval: Duration::from_mins(1),
            stop_heartbeat: Some(cleanup.stopper(None)),
            ..options(1, 1)
        },
    );
    let job = manager.start_plan(&strings(&["actor"])).await.unwrap();
    cleanup.started.wait_for(1.0).await;
    assert!(service.started.is_set());
    assert!(manager.progress_timers(&job.job_id) > 0);

    let request = tokio::spawn({
        let (manager, job_id) = (manager.clone(), job.job_id.clone());
        async move { manager.cancel_plan(&job_id).await }
    });
    sleep_ms(50).await;
    let completed_before_release = request.is_finished();
    cleanup.release.set();
    let result = tokio::time::timeout(Duration::from_secs(1), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::task::yield_now().await;
    let progress_timers = manager.progress_timers(&job.job_id);
    manager.aclose().await;

    let cancelled = repository.job(&job.job_id).await;
    assert_eq!(result.outcome, CancelJobOutcome::Cancelled);
    assert!(!completed_before_release);
    assert!(!cleanup.cancelled.is_set());
    assert_eq!(progress_timers, 0);
    assert_eq!(cancelled.error_code.as_deref(), Some(JOB_CANCELLED_ERROR_CODE));
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_cancel_during_failed_plan_cleanup_waits_and_clears_progress_timer cases=1
#[tokio::test]
async fn cancel_during_failed_plan_cleanup_waits_and_clears_progress_timer_domain_error() {
    cancel_during_failed_plan_cleanup_waits_and_clears_progress_timer(PlanMode::FailingDomain).await;
}

// py: tests/test_fill_actor_jobs.py::test_cancel_during_failed_plan_cleanup_waits_and_clears_progress_timer cases=1
#[tokio::test]
async fn cancel_during_failed_plan_cleanup_waits_and_clears_progress_timer_unexpected_error() {
    cancel_during_failed_plan_cleanup_waits_and_clears_progress_timer(PlanMode::FailingUnexpected).await;
}

// py: tests/test_fill_actor_jobs.py::test_already_cancelled_result_still_signals_local_execution
#[tokio::test]
async fn already_cancelled_result_still_signals_local_execution() {
    let db = Db::new().await;
    let repository = db.repository();
    let service = Arc::new(FakeService::cancellation_aware());
    let manager = manager(&service, &repository, options(1, 1));
    let job = manager.start_plan(&strings(&["actor"])).await.unwrap();
    service.started.wait_for(1.0).await;
    let persisted = repository.inner.cancel_job(&job.job_id, Utc::now()).await.unwrap();
    assert_eq!(persisted.outcome, CancelJobOutcome::Cancelled);

    let mut request = tokio::spawn({
        let (manager, job_id) = (manager.clone(), job.job_id.clone());
        async move { manager.cancel_plan(&job_id).await }
    });
    let answered = tokio::time::timeout(Duration::from_millis(200), &mut request).await;
    let signalled_before_release = service.cancelled.is_set();
    service.release.set();
    if answered.is_err() {
        let _ = request.await;
    }
    manager.aclose().await;

    let result = answered.expect("the cancel returned in time").unwrap().unwrap();
    assert_eq!(result.outcome, CancelJobOutcome::AlreadyCancelled);
    assert!(signalled_before_release);
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_cancel_in_claim_register_gap_prevents_service_execution
#[tokio::test]
async fn cancel_in_claim_register_gap_prevents_service_execution() {
    let db = Db::new().await;
    let repository = Arc::new(TestRepository {
        pause_claim: true,
        ..TestRepository::new(db.postgres())
    });
    let service = Arc::new(FakeService::controlled(false));
    let manager = manager(&service, &repository, options(1, 1));
    let job = manager.start_plan(&strings(&["actor"])).await.unwrap();
    repository.claimed.wait_for(1.0).await;

    let result = manager.cancel_plan(&job.job_id).await.unwrap();
    repository.release_claim.set();
    sleep_ms(50).await;

    assert_eq!(result.outcome, CancelJobOutcome::Cancelled);
    assert!(!service.started.is_set());
    let cancelled = repository.job(&job.job_id).await;
    assert_eq!(cancelled.error_code.as_deref(), Some(JOB_CANCELLED_ERROR_CODE));
    manager.aclose().await;
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_cancel_operation_survives_request_task_cancellation_and_stops_execution
#[tokio::test]
async fn cancel_operation_survives_request_task_cancellation_and_stops_execution() {
    let db = Db::new().await;
    let repository = Arc::new(TestRepository {
        pause_cancel: true,
        ..TestRepository::new(db.postgres())
    });
    let service = Arc::new(FakeService::cancellation_aware());
    let manager = manager(&service, &repository, options(1, 1));
    let job = manager.start_plan(&strings(&["actor"])).await.unwrap();
    service.started.wait_for(1.0).await;
    let request = tokio::spawn({
        let (manager, job_id) = (manager.clone(), job.job_id.clone());
        async move { manager.cancel_plan(&job_id).await }
    });
    repository.cancel_persisted.wait_for(1.0).await;

    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    repository.release_cancel.set();
    service.cancelled.wait_for(1.0).await;
    for _ in 0..100 {
        if manager.cancel_operations() == 0 {
            break;
        }
        sleep_ms(5).await;
    }
    manager.aclose().await;

    let cancelled = repository.job(&job.job_id).await;
    assert_eq!(cancelled.error_code.as_deref(), Some(JOB_CANCELLED_ERROR_CODE));
    assert_eq!(manager.cancel_operations(), 0);
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_heartbeat_stops_execution_after_external_cancellation
#[tokio::test]
async fn heartbeat_stops_execution_after_external_cancellation() {
    let db = Db::new().await;
    let repository = db.repository();
    let service = Arc::new(FakeService::cancellation_aware());
    let manager = manager(
        &service,
        &repository,
        JobManagerOptions {
            lease_duration: SHORT_LEASE,
            ..options(1, 1)
        },
    );
    let job = manager.start_plan(&strings(&["actor"])).await.unwrap();
    service.started.wait_for(1.0).await;

    let result = repository.inner.cancel_job(&job.job_id, Utc::now()).await.unwrap();
    assert_eq!(result.outcome, CancelJobOutcome::Cancelled);
    service.cancelled.wait_for(1.0).await;
    manager.aclose().await;

    let cancelled = repository.job(&job.job_id).await;
    assert_eq!(cancelled.error_code.as_deref(), Some(JOB_CANCELLED_ERROR_CODE));
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_new_manager_executes_durable_queued_job
#[tokio::test]
async fn new_manager_executes_durable_queued_job() {
    let db = Db::new().await;
    let repository = db.repository();
    let now = normalize_datetime(Utc::now());
    let queued = JobRecord::new(
        "queued-plan",
        JobOperation::CreatePlan,
        JobState::Queued,
        now,
        now,
        Some("queued-plan".to_owned()),
        strings(&["actor"]),
    );
    assert!(repository.inner.enqueue_job(&queued, 4).await.unwrap());
    let service = Arc::new(FakeService::controlled(false));
    let manager = manager(&service, &repository, options(1, 4));

    manager.start().await.unwrap();
    let completed = repository.wait_for_state(&queued.job_id, &[JobState::Completed]).await;
    manager.aclose().await;

    assert_eq!(completed.state, JobState::Completed);
    assert_eq!(completed.progress.stage, JobStage::Done);
    assert_eq!(completed.progress.completed, 1);
    assert_eq!(completed.progress.total, Some(1));
    assert_eq!(completed.progress.current.as_deref(), Some("saved"));
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_manager_coalesces_same_stage_progress_and_flushes_terminal_snapshot
#[tokio::test]
async fn manager_coalesces_same_stage_progress_and_flushes_terminal_snapshot() {
    let db = Db::new().await;
    let repository = db.repository();
    let service = Arc::new(FakeService {
        rapid_updates: 50,
        ..FakeService::controlled(false)
    });
    let manager = manager(
        &service,
        &repository,
        JobManagerOptions {
            progress_flush_interval: Duration::from_secs(10),
            ..options(1, 2)
        },
    );

    let job = manager.start_plan(&strings(&["actor"])).await.unwrap();
    let completed = repository.wait_for_state(&job.job_id, &[JobState::Completed]).await;
    manager.aclose().await;

    assert_eq!(repository.progress_writes.load(Ordering::SeqCst), 3);
    assert_eq!(completed.progress.stage, JobStage::Done);
    assert_eq!(completed.progress.current.as_deref(), Some("saved"));
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_reporter_resets_stage_timer_when_progress_scope_changes
#[tokio::test]
async fn reporter_resets_stage_timer_when_progress_scope_changes() {
    let db = Db::new().await;
    let repository = db.repository();
    let clock = MutableClock::new();
    let service = Arc::new(FakeService {
        clock: Some(clock.clone()),
        ..FakeService::new(PlanMode::Scope)
    });
    let tokens = Arc::new(Mutex::new(vec!["job", "owner"]));
    let token_factory: TokenFactory = Arc::new(move || {
        tokens
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()
            .expect("no more tokens (StopIteration)")
            .to_owned()
    });
    let manager = manager(
        &service,
        &repository,
        JobManagerOptions {
            clock: Some(clock.clock()),
            token_factory: Some(token_factory),
            progress_flush_interval: Duration::from_millis(10),
            ..options(1, 2)
        },
    );

    let job = manager.start_plan(&strings(&["actor"])).await.unwrap();
    repository.wait_for_state(&job.job_id, &[JobState::Completed]).await;
    manager.aclose().await;

    let at = |seconds: u32| Utc.with_ymd_and_hms(2026, 7, 13, 10, 0, seconds).unwrap();
    let records = repository.records();
    let pages: Vec<&JobProgress> = records
        .iter()
        .filter(|record| record.unit == JobProgressUnit::Pages)
        .collect();
    assert_eq!(
        pages
            .iter()
            .map(|record| (record.completed, record.total))
            .collect::<Vec<_>>(),
        vec![(1, Some(26)), (2, Some(26)), (0, Some(26))]
    );
    assert_eq!(pages[0].stage_started_at, at(5));
    assert_eq!(pages[1].stage_started_at, pages[0].stage_started_at);
    assert_eq!(pages[2].stage_started_at, at(15));
    let actors: Vec<&JobProgress> = records
        .iter()
        .filter(|record| record.unit == JobProgressUnit::Actors)
        .collect();
    assert_eq!(actors.last().expect("actor progress").stage_started_at, at(20));
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_lost_job_lease_keeps_completed_plan_hidden_for_ttl_cleanup
#[tokio::test]
async fn lost_job_lease_keeps_completed_plan_hidden_for_ttl_cleanup() {
    let db = Db::new().await;
    let repository = db.repository();
    let service = Arc::new(FakeService::controlled(true).with_repository(&repository));
    let manager = manager(&service, &repository, options(1, 2));
    let job = manager.start_plan(&strings(&["actor"])).await.unwrap();
    service.started.wait_for(1.0).await;
    let running = repository.wait_for_state(&job.job_id, &[JobState::Running]).await;
    let expires = running.lease_expires_at.expect("a lease");
    assert_eq!(
        repository
            .inner
            .fail_expired_jobs(expires + TimeDelta::seconds(1), JOB_INTERRUPTED)
            .await
            .unwrap(),
        1
    );

    service.release.set();
    let plan_id = job.plan_id.clone().unwrap();
    let mut persisted = false;
    for _ in 0..200 {
        if repository.inner.get_plan(&plan_id).await.unwrap().is_some() {
            persisted = true;
            break;
        }
        sleep_ms(5).await;
    }
    assert!(persisted, "completed orphan plan was not persisted");

    manager.aclose().await;
    let failed = repository.job(&job.job_id).await;
    assert_eq!(failed.state, JobState::Failed);
    assert!(manager.get_plan(&plan_id).await.unwrap().is_none());
    assert!(repository.inner.get_plan(&plan_id).await.unwrap().is_some());
    db.finish().await;
}

// py: tests/test_fill_actor_jobs.py::test_heartbeat_retries_transient_repository_failure
#[tokio::test]
async fn heartbeat_retries_transient_repository_failure() {
    let db = Db::new().await;
    let repository = Arc::new(TestRepository {
        heartbeat_failures: AtomicUsize::new(1),
        ..TestRepository::new(db.postgres())
    });
    let service = Arc::new(FakeService::controlled(true).with_repository(&repository));
    let manager = manager(
        &service,
        &repository,
        JobManagerOptions {
            lease_duration: SHORT_LEASE,
            ..options(1, 2)
        },
    );
    let job = manager.start_plan(&strings(&["actor"])).await.unwrap();
    service.started.wait_for(1.0).await;
    // Python waits 250 ms on its 150 ms lease: longer than one lease.
    sleep_ms(750).await;
    service.release.set();

    let completed = repository
        .wait_for_state(&job.job_id, &[JobState::Completed, JobState::Failed])
        .await;
    manager.aclose().await;

    assert_eq!(repository.heartbeat_failures.load(Ordering::SeqCst), 0);
    assert_eq!(completed.state, JobState::Completed);
    assert!(manager.get_plan(&job.job_id).await.unwrap().is_some());
    db.finish().await;
}
