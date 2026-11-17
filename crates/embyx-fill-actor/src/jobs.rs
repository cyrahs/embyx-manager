//! The fill actor's durable job queue: plan and apply jobs, their leases, progress and
//! cancellation.
//!
//! Python origin: `src/embyx_manager/fill_actor/jobs.py` (`FillActorJobManager`,
//! `_OwnedProgressReporter`, `_JobExecution`). Jobs live in `fill_actor_jobs` and
//! `fill_actor_apply_jobs`; a worker claims the oldest queued one with a lease, renews the lease
//! while it runs (the heartbeat) and writes the terminal state only while it still owns the
//! row. A job whose lease ran out (a crash, a restart, a lost heartbeat) is failed as
//! `job_interrupted` by the reaper of whichever build runs next, so the rows either build
//! writes are picked up by the other (cross-version contract 3).
//!
//! How Python's cancellation maps onto Tokio (DIVERGENCES.md D130-D132):
//!
//! - A stop (a user's cancel, a lost lease, shutdown) is a one-shot token per execution, where
//!   Python cancels the job's task once. The parts Python lets a cancel interrupt (claim
//!   revalidation, the plan scan, the wait for an apply) run in a `select!` against it; the
//!   cleanups Python runs in a task of their own that further cancels cannot reach run after
//!   it, so they always finish.
//! - A stopped plan's scan future is dropped, which sets its threads' stop flags; the job then
//!   awaits [`ScanScope::settle`] for that plan's own scan threads before it reports anything
//!   (Python's cancelled scan waits for its thread, D123).
//! - An apply runs in a task of its own that a stop never aborts: the stop is only seen through
//!   `stop_requested`, between two candidates, and the job awaits the apply to its end, CloudDrive
//!   calls included (the service settles each execution's scoped mover before it returns, D116),
//!   before it touches the job row or lets the lease go.
//! - A cancel runs in a task of its own (`asyncio.shield`): a dropped request does not stop it,
//!   and it returns only after the job's execution has finished its cleanup.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::cloud_moves::BoxFuture;
use crate::errors::{FillActorError, FillActorErrorKind};
use crate::models::{ApplyResult, FillActorPlan, VideoState};
use crate::postgres_repository::{PostgresFillActorRepository, RepositoryError};
use crate::records::{
    ApplyJobRecord, CancelJobOutcome, CancelJobResult, EnqueueApplyJobOutcome, EnqueueApplyJobResult, JobOperation,
    JobProgress, JobProgressEvent, JobProgressUnit, JobRecord, JobStage, JobState, PlanRecord, normalize_datetime,
};
use crate::service::{
    Clock, FillActorService, ProgressCallback, ScanScope, ServiceError, StopCallback, TokenFactory, token_urlsafe,
};

#[cfg(test)]
mod tests;

/// The Python logger the process log names (D60).
pub const LOGGER: &str = "embyx_manager.fill_actor.jobs";
/// The error code of a job whose lease ran out or whose run was cut short.
pub const JOB_INTERRUPTED: &str = "job_interrupted";

type RepoResult<T> = Result<T, RepositoryError>;

// ---------- the collaborators ----------

/// What the job manager asks the service (`FillActorService`, duck-typed in Python's tests).
pub trait JobService: Send + Sync {
    /// `validate_actor_ids(actor_ids)`.
    fn validate_actor_ids(&self, actor_ids: &[String]) -> Result<Vec<String>, FillActorError>;
    /// `roots_ready()`.
    fn roots_ready(&self) -> BoxFuture<'_, Result<bool, ServiceError>>;
    /// `apply_ready()`.
    fn apply_ready(&self) -> BoxFuture<'_, Result<bool, ServiceError>>;
    /// `create_plan(actor_ids, plan_id=..., progress=...)`.
    fn create_plan<'a>(
        &'a self,
        actor_ids: &'a [String],
        plan_id: Option<&'a str>,
        progress: ProgressCallback,
    ) -> BoxFuture<'a, Result<FillActorPlan, ServiceError>>;
    /// `get_plan(plan_id)`.
    fn get_plan<'a>(&'a self, plan_id: &'a str) -> BoxFuture<'a, Result<FillActorPlan, ServiceError>>;
    /// `apply(plan_id=, revision=, candidate_ids=, progress=, stop_requested=)`.
    fn apply<'a>(
        &'a self,
        plan_id: &'a str,
        revision: &'a str,
        candidate_ids: &'a [String],
        progress: ProgressCallback,
        stop_requested: StopCallback,
    ) -> BoxFuture<'a, Result<ApplyResult, ServiceError>>;
}

impl JobService for FillActorService {
    fn validate_actor_ids(&self, actor_ids: &[String]) -> Result<Vec<String>, FillActorError> {
        Self::validate_actor_ids(self, actor_ids)
    }

    fn roots_ready(&self) -> BoxFuture<'_, Result<bool, ServiceError>> {
        Box::pin(Self::roots_ready(self))
    }

    fn apply_ready(&self) -> BoxFuture<'_, Result<bool, ServiceError>> {
        Box::pin(Self::apply_ready(self))
    }

    fn create_plan<'a>(
        &'a self,
        actor_ids: &'a [String],
        plan_id: Option<&'a str>,
        progress: ProgressCallback,
    ) -> BoxFuture<'a, Result<FillActorPlan, ServiceError>> {
        Box::pin(Self::create_plan(self, actor_ids, plan_id, None, Some(progress)))
    }

    fn get_plan<'a>(&'a self, plan_id: &'a str) -> BoxFuture<'a, Result<FillActorPlan, ServiceError>> {
        Box::pin(Self::get_plan(self, plan_id))
    }

    fn apply<'a>(
        &'a self,
        plan_id: &'a str,
        revision: &'a str,
        candidate_ids: &'a [String],
        progress: ProgressCallback,
        stop_requested: StopCallback,
    ) -> BoxFuture<'a, Result<ApplyResult, ServiceError>> {
        Box::pin(Self::apply(
            self,
            plan_id,
            revision,
            candidate_ids,
            Some(progress),
            Some(stop_requested),
        ))
    }
}

/// `finish_owned_job(...)`'s keyword arguments.
#[derive(Debug, Clone, Copy)]
pub struct Finish<'a> {
    pub job_id: &'a str,
    pub owner_id: &'a str,
    pub state: JobState,
    pub error_code: Option<&'a str>,
    pub now: DateTime<Utc>,
    pub progress: &'a JobProgress,
    pub apply_result: Option<&'a ApplyResult>,
}

/// The repository calls the job manager makes (`FillActorRepository`'s job half and
/// `get_plan`). [`PostgresFillActorRepository`] is the application's; the tests wrap it to
/// pause or fail single calls, as Python's tests subclass the repository.
pub trait JobRepository: Send + Sync {
    fn enqueue_job<'a>(&'a self, record: &'a JobRecord, max_active: i64) -> BoxFuture<'a, RepoResult<bool>>;
    fn enqueue_apply_job<'a>(
        &'a self,
        record: &'a ApplyJobRecord,
        max_active: i64,
    ) -> BoxFuture<'a, RepoResult<EnqueueApplyJobResult>>;
    fn get_job<'a>(&'a self, job_id: &'a str) -> BoxFuture<'a, RepoResult<Option<JobRecord>>>;
    fn get_apply_job<'a>(&'a self, job_id: &'a str) -> BoxFuture<'a, RepoResult<Option<ApplyJobRecord>>>;
    fn get_plan<'a>(&'a self, plan_id: &'a str) -> BoxFuture<'a, RepoResult<Option<PlanRecord>>>;
    fn claim_next_job<'a>(
        &'a self,
        owner_id: &'a str,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> BoxFuture<'a, RepoResult<Option<JobRecord>>>;
    fn renew_owned_job_lease<'a>(
        &'a self,
        job_id: &'a str,
        owner_id: &'a str,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> BoxFuture<'a, RepoResult<bool>>;
    fn update_owned_job_progress<'a>(
        &'a self,
        job_id: &'a str,
        owner_id: &'a str,
        progress: &'a JobProgress,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, RepoResult<bool>>;
    fn finish_owned_job<'a>(&'a self, finish: Finish<'a>) -> BoxFuture<'a, RepoResult<bool>>;
    fn cancel_job<'a>(&'a self, job_id: &'a str, now: DateTime<Utc>) -> BoxFuture<'a, RepoResult<CancelJobResult>>;
    fn fail_expired_jobs<'a>(&'a self, now: DateTime<Utc>, error_code: &'a str) -> BoxFuture<'a, RepoResult<usize>>;
}

impl JobRepository for PostgresFillActorRepository {
    fn enqueue_job<'a>(&'a self, record: &'a JobRecord, max_active: i64) -> BoxFuture<'a, RepoResult<bool>> {
        Box::pin(Self::enqueue_job(self, record, max_active))
    }

    fn enqueue_apply_job<'a>(
        &'a self,
        record: &'a ApplyJobRecord,
        max_active: i64,
    ) -> BoxFuture<'a, RepoResult<EnqueueApplyJobResult>> {
        Box::pin(Self::enqueue_apply_job(self, record, max_active))
    }

    fn get_job<'a>(&'a self, job_id: &'a str) -> BoxFuture<'a, RepoResult<Option<JobRecord>>> {
        Box::pin(Self::get_job(self, job_id))
    }

    fn get_apply_job<'a>(&'a self, job_id: &'a str) -> BoxFuture<'a, RepoResult<Option<ApplyJobRecord>>> {
        Box::pin(Self::get_apply_job(self, job_id))
    }

    fn get_plan<'a>(&'a self, plan_id: &'a str) -> BoxFuture<'a, RepoResult<Option<PlanRecord>>> {
        Box::pin(Self::get_plan(self, plan_id))
    }

    fn claim_next_job<'a>(
        &'a self,
        owner_id: &'a str,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> BoxFuture<'a, RepoResult<Option<JobRecord>>> {
        Box::pin(Self::claim_next_job(self, owner_id, now, lease_expires_at))
    }

    fn renew_owned_job_lease<'a>(
        &'a self,
        job_id: &'a str,
        owner_id: &'a str,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> BoxFuture<'a, RepoResult<bool>> {
        Box::pin(Self::renew_owned_job_lease(
            self,
            job_id,
            owner_id,
            now,
            lease_expires_at,
        ))
    }

    fn update_owned_job_progress<'a>(
        &'a self,
        job_id: &'a str,
        owner_id: &'a str,
        progress: &'a JobProgress,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, RepoResult<bool>> {
        Box::pin(Self::update_owned_job_progress(self, job_id, owner_id, progress, now))
    }

    fn finish_owned_job<'a>(&'a self, finish: Finish<'a>) -> BoxFuture<'a, RepoResult<bool>> {
        Box::pin(Self::finish_owned_job(
            self,
            finish.job_id,
            finish.owner_id,
            finish.state,
            finish.error_code,
            finish.now,
            finish.progress,
            finish.apply_result,
        ))
    }

    fn cancel_job<'a>(&'a self, job_id: &'a str, now: DateTime<Utc>) -> BoxFuture<'a, RepoResult<CancelJobResult>> {
        Box::pin(Self::cancel_job(self, job_id, now))
    }

    fn fail_expired_jobs<'a>(&'a self, now: DateTime<Utc>, error_code: &'a str) -> BoxFuture<'a, RepoResult<usize>> {
        Box::pin(Self::fail_expired_jobs(self, now, error_code))
    }
}

// ---------- executions ----------

/// `_ExecutionStopReason`, by priority: a later reason replaces an earlier one only when it
/// ranks higher (`_STOP_REASON_PRIORITY`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopReason {
    Shutdown,
    OwnershipLost,
    UserCancel,
}

impl StopReason {
    fn priority(self) -> u8 {
        match self {
            Self::Shutdown => 0,
            Self::OwnershipLost => 1,
            Self::UserCancel => 2,
        }
    }

    /// `{USER_CANCEL, OWNERSHIP_LOST}`: the row is someone else's to finish.
    fn releases_ownership(reason: Option<Self>) -> bool {
        matches!(reason, Some(Self::UserCancel | Self::OwnershipLost))
    }
}

/// `_JobExecution`: one claimed job's run, its stop reason and the one stop it is delivered.
#[derive(Debug)]
struct Execution {
    stop_reason: Mutex<Option<StopReason>>,
    /// Cancelled once, when the first stop is delivered (`cancel_delivered`).
    stop: CancellationToken,
    /// `true` once the run, cleanup included, has ended (`task.done()`).
    done: watch::Sender<bool>,
}

impl Execution {
    fn new() -> Self {
        Self {
            stop_reason: Mutex::new(None),
            stop: CancellationToken::new(),
            done: watch::Sender::new(false),
        }
    }

    fn stop_reason(&self) -> Option<StopReason> {
        *self.stop_reason.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `request_stop(reason)`: record the reason (a higher-priority one wins) and deliver the
    /// stop if it has not been; `true` when this call delivered it.
    fn request_stop(&self, reason: StopReason) -> bool {
        {
            let mut current = self.stop_reason.lock().unwrap_or_else(PoisonError::into_inner);
            if current.is_none_or(|current| reason.priority() > current.priority()) {
                *current = Some(reason);
            }
        }
        if self.stop.is_cancelled() || self.is_done() {
            return false;
        }
        self.stop.cancel();
        true
    }

    fn is_done(&self) -> bool {
        *self.done.borrow()
    }

    /// Wait until the run has ended (`asyncio.gather(execution.task)`).
    async fn finished(&self) {
        let mut receiver = self.done.subscribe();
        let _ = receiver.wait_for(|done| *done).await;
    }
}

/// Marks an execution done when its run ends, also when it panicked.
struct DoneOnDrop(Arc<Execution>);

impl Drop for DoneOnDrop {
    fn drop(&mut self) {
        self.0.done.send_replace(true);
    }
}

// ---------- the heartbeat and its stop ----------

/// A running heartbeat task; dropping it aborts the task.
#[derive(Debug)]
pub struct Heartbeat(Option<JoinHandle<()>>);

impl Heartbeat {
    /// `task.cancel(); await asyncio.gather(task, return_exceptions=True)`.
    pub async fn stop(mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

/// `_stop_heartbeat(task)`: how a job stops its heartbeat. `Err` is an exception it raised.
/// The default stops the task; tests replace it to hold a cleanup open or fail it, as
/// Python's tests monkeypatch `_stop_heartbeat` (D132).
pub type HeartbeatStopper = Arc<dyn Fn(Heartbeat) -> BoxFuture<'static, Result<(), String>> + Send + Sync>;

fn default_stopper() -> HeartbeatStopper {
    Arc::new(|heartbeat: Heartbeat| {
        Box::pin(async move {
            heartbeat.stop().await;
            Ok(())
        })
    })
}

/// A heartbeat that may not have started yet, or was handed to the stopper already.
#[derive(Default)]
struct HeartbeatSlot(Mutex<Option<Heartbeat>>);

impl HeartbeatSlot {
    fn set(&self, heartbeat: Heartbeat) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(heartbeat);
    }

    fn take(&self) -> Option<Heartbeat> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

// ---------- progress ----------

/// The progress timers alive per job (the tasks Python names `fill-actor-progress-<job>`):
/// what the tests look for where Python lists `asyncio.all_tasks()`.
#[derive(Debug, Clone, Default)]
struct TimerRegistry(Arc<Mutex<HashMap<String, usize>>>);

impl TimerRegistry {
    fn count(&self, job_id: &str) -> usize {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(job_id)
            .copied()
            .unwrap_or(0)
    }

    fn enter(&self, job_id: &str) -> TimerGuard {
        *self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(job_id.to_owned())
            .or_default() += 1;
        TimerGuard {
            registry: self.clone(),
            job_id: job_id.to_owned(),
        }
    }
}

struct TimerGuard {
    registry: TimerRegistry,
    job_id: String,
}

impl Drop for TimerGuard {
    fn drop(&mut self) {
        let mut timers = self.registry.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(count) = timers.get_mut(&self.job_id) {
            *count -= 1;
            if *count == 0 {
                timers.remove(&self.job_id);
            }
        }
    }
}

#[derive(Debug)]
struct ReporterState {
    current: JobProgress,
    dirty: bool,
    owned: bool,
    closed: bool,
    /// `_last_flush` (`0.0` before the first flush).
    last_flush: Option<Instant>,
    timer: Option<JoinHandle<()>>,
}

/// `_OwnedProgressReporter`: coalesces a job's progress events into at most one write per
/// flush interval while the job's lease is held; a new stage, unit or a count going back is
/// written at once and restarts the stage timer.
struct Reporter {
    repository: Arc<dyn JobRepository>,
    job_id: String,
    operation: JobOperation,
    owner_id: String,
    clock: Clock,
    flush_interval: Duration,
    timers: TimerRegistry,
    state: tokio::sync::Mutex<ReporterState>,
    me: Weak<Reporter>,
}

impl Reporter {
    fn new(inner: &Inner, job: &JobRecord) -> Arc<Self> {
        let now_clock = Arc::clone(&inner.clock);
        let clock: Clock = Arc::new(move || normalize_datetime(now_clock()));
        Arc::new_cyclic(|me| Self {
            repository: Arc::clone(&inner.repository),
            job_id: job.job_id.clone(),
            operation: job.operation,
            owner_id: inner.owner_id.clone(),
            clock,
            flush_interval: inner.progress_flush_interval,
            timers: inner.timers.clone(),
            state: tokio::sync::Mutex::new(ReporterState {
                current: job.progress.clone(),
                dirty: false,
                owned: true,
                closed: false,
                last_flush: None,
                timer: None,
            }),
            me: me.clone(),
        })
    }

    /// The service's progress callback (`progress=reporter`).
    fn callback(self: &Arc<Self>) -> ProgressCallback {
        let reporter = Arc::clone(self);
        Arc::new(move |event| {
            let reporter = Arc::clone(&reporter);
            Box::pin(async move { reporter.report(event).await })
        })
    }

    /// `__call__(event)`. A progress the record refuses is logged and dropped: the service
    /// never reports one (D124).
    async fn report(&self, event: JobProgressEvent) {
        let mut state = self.state.lock().await;
        if state.closed || !state.owned {
            return;
        }
        let current = &state.current;
        if event.stage == current.stage
            && event.completed == current.completed
            && event.total == current.total
            && event.unit == current.unit
            && event.current == current.current
        {
            return;
        }
        let now = (self.clock)();
        let scope_changed =
            event.stage != current.stage || event.unit != current.unit || event.completed < current.completed;
        let progress = JobProgress {
            stage: event.stage,
            completed: event.completed,
            total: event.total,
            unit: event.unit,
            current: event.current,
            stage_started_at: if scope_changed { now } else { current.stage_started_at },
            updated_at: now,
        };
        if let Err(error) = progress.validate() {
            tracing::error!(target: LOGGER, error = %error, "fill-actor job progress refused");
            return;
        }
        state.current = progress;
        state.dirty = true;
        let due = state
            .last_flush
            .is_none_or(|last| Instant::now().saturating_duration_since(last) >= self.flush_interval);
        if scope_changed || due {
            self.flush_locked(&mut state).await;
        } else {
            self.schedule_locked(&mut state);
        }
    }

    /// `finish(state, error_code=, apply_result=)`: the terminal write, if the job is still
    /// owned; `false` when it was not written.
    async fn finish(
        &self,
        state: JobState,
        error_code: Option<&str>,
        apply_result: Option<&ApplyResult>,
    ) -> RepoResult<bool> {
        let (timer, now, progress) = {
            let mut locked = self.state.lock().await;
            if locked.closed {
                return Ok(false);
            }
            locked.closed = true;
            let now = (self.clock)();
            let current = &locked.current;
            let progress = JobProgress {
                stage: JobStage::Done,
                completed: current.completed,
                total: current.total,
                unit: current.unit,
                current: if self.operation == JobOperation::Apply {
                    None
                } else {
                    current.current.clone()
                },
                stage_started_at: now,
                updated_at: now,
            };
            (locked.timer.take(), now, progress)
        };
        stop_timer(timer).await;
        if !self.state.lock().await.owned {
            return Ok(false);
        }
        self.repository
            .finish_owned_job(Finish {
                job_id: &self.job_id,
                owner_id: &self.owner_id,
                state,
                error_code,
                now,
                progress: &progress,
                apply_result,
            })
            .await
    }

    /// `mark_unowned()`: the row is no longer ours; nothing more is written.
    async fn mark_unowned(&self) {
        let timer = {
            let mut state = self.state.lock().await;
            state.owned = false;
            state.dirty = false;
            state.timer.take()
        };
        stop_timer(timer).await;
    }

    /// `_flush_locked()`.
    async fn flush_locked(&self, state: &mut ReporterState) {
        if !state.dirty || !state.owned || state.closed {
            return;
        }
        let attempted_at = Instant::now();
        let owned = self
            .repository
            .update_owned_job_progress(&self.job_id, &self.owner_id, &state.current, (self.clock)())
            .await;
        match owned {
            Err(error) => {
                tracing::error!(target: LOGGER, error = %error, "fill-actor job progress update failed; retrying");
                state.last_flush = Some(attempted_at);
                self.schedule_locked(state);
            }
            Ok(false) => {
                state.owned = false;
                state.dirty = false;
            }
            Ok(true) => {
                state.dirty = false;
                state.last_flush = Some(attempted_at);
            }
        }
    }

    /// `_schedule_locked()`: one timer that flushes once the interval since the last flush
    /// has passed.
    fn schedule_locked(&self, state: &mut ReporterState) {
        if state.timer.is_some() || state.closed || !state.owned {
            return;
        }
        let delay = state.last_flush.map_or(Duration::ZERO, |last| {
            self.flush_interval
                .saturating_sub(Instant::now().saturating_duration_since(last))
        });
        let guard = self.timers.enter(&self.job_id);
        let me = self.me.clone();
        state.timer = Some(tokio::spawn(async move {
            let _guard = guard;
            tokio::time::sleep(delay).await;
            let Some(reporter) = me.upgrade() else {
                return;
            };
            let mut state = reporter.state.lock().await;
            // This task's own handle: detached, the task ends on its own.
            drop(state.timer.take());
            reporter.flush_locked(&mut state).await;
            if state.dirty {
                reporter.schedule_locked(&mut state);
            }
        }));
    }
}

/// `timer.cancel(); await asyncio.gather(timer, return_exceptions=True)`.
async fn stop_timer(timer: Option<JoinHandle<()>>) {
    if let Some(timer) = timer {
        timer.abort();
        let _ = timer.await;
    }
}

// ---------- the manager ----------

/// `FillActorJobManager(...)`'s keyword arguments, with Python's defaults.
#[derive(Clone)]
pub struct JobManagerOptions {
    pub clock: Option<Clock>,
    pub token_factory: Option<TokenFactory>,
    pub max_concurrent_jobs: usize,
    pub max_active_jobs: usize,
    pub lease_duration: Duration,
    pub poll_interval: Duration,
    pub progress_flush_interval: Duration,
    /// `_stop_heartbeat`, replaceable for tests (D132).
    pub stop_heartbeat: Option<HeartbeatStopper>,
}

impl Default for JobManagerOptions {
    fn default() -> Self {
        Self {
            clock: None,
            token_factory: None,
            max_concurrent_jobs: 2,
            max_active_jobs: 32,
            lease_duration: Duration::from_secs(30),
            poll_interval: Duration::from_millis(250),
            progress_flush_interval: Duration::from_millis(750),
            stop_heartbeat: None,
        }
    }
}

impl std::fmt::Debug for JobManagerOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobManagerOptions")
            .field("max_concurrent_jobs", &self.max_concurrent_jobs)
            .field("max_active_jobs", &self.max_active_jobs)
            .field("lease_duration", &self.lease_duration)
            .field("poll_interval", &self.poll_interval)
            .field("progress_flush_interval", &self.progress_flush_interval)
            .finish_non_exhaustive()
    }
}

/// A constructor argument Python refuses with `ValueError`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct InvalidJobOptions(pub &'static str);

/// `asyncio.Event`: set until cleared; a wait returns at once while it is set.
#[derive(Debug, Default)]
struct Wake {
    flag: AtomicBool,
    notify: Notify,
}

impl Wake {
    fn set(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    fn clear(&self) {
        self.flag.store(false, Ordering::SeqCst);
    }

    async fn wait(&self) {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.flag.load(Ordering::SeqCst) {
            return;
        }
        notified.await;
    }
}

#[derive(Debug, Default)]
struct Lifecycle {
    workers: Vec<JoinHandle<()>>,
    reaper: Option<JoinHandle<()>>,
    shutdown: CancellationToken,
}

struct Inner {
    service: Arc<dyn JobService>,
    repository: Arc<dyn JobRepository>,
    clock: Clock,
    token_factory: TokenFactory,
    owner_id: String,
    max_concurrent_jobs: usize,
    max_active_jobs: usize,
    lease_duration: Duration,
    poll_interval: Duration,
    progress_flush_interval: Duration,
    stop_heartbeat: HeartbeatStopper,
    wake: Wake,
    lifecycle: tokio::sync::Mutex<Lifecycle>,
    executions: Mutex<HashMap<String, Arc<Execution>>>,
    /// `_cancel_operations`.
    cancel_operations: TaskTracker,
    closing: AtomicBool,
    timers: TimerRegistry,
}

/// `FillActorJobManager`. Cheap to clone; clones share everything.
#[derive(Clone)]
pub struct FillActorJobManager {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for FillActorJobManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FillActorJobManager")
            .field("max_concurrent_jobs", &self.inner.max_concurrent_jobs)
            .field("max_active_jobs", &self.inner.max_active_jobs)
            .finish_non_exhaustive()
    }
}

/// How a job's body ended, when it was not stopped.
enum Body {
    /// Finished, terminal state written (or the job was not ours any more).
    Done,
    /// Raised: the failed job's cleanup writes this error code.
    Failed(String),
}

/// An exception that escaped a job's cleanup (only the replaceable heartbeat stop raises).
type Escaped = Result<(), String>;

fn max_active(inner: &Inner) -> i64 {
    i64::try_from(inner.max_active_jobs).unwrap_or(i64::MAX)
}

fn lease(inner: &Inner) -> TimeDelta {
    TimeDelta::from_std(inner.lease_duration).unwrap_or(TimeDelta::MAX)
}

/// `tuple(dict.fromkeys(values))`.
fn dedupe(values: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    values
        .iter()
        .filter(|value| seen.insert(value.as_str()))
        .cloned()
        .collect()
}

/// Sleep, or return early when `shutdown` is cancelled.
async fn pause(shutdown: &CancellationToken, duration: Duration) {
    tokio::select! {
        () = shutdown.cancelled() => {}
        () = tokio::time::sleep(duration) => {}
    }
}

impl FillActorJobManager {
    /// `FillActorJobManager(service=, repository=, ...)`.
    pub fn new(
        service: Arc<dyn JobService>,
        repository: Arc<dyn JobRepository>,
        options: JobManagerOptions,
    ) -> Result<Self, InvalidJobOptions> {
        if options.max_concurrent_jobs < 1 || options.max_active_jobs < options.max_concurrent_jobs {
            return Err(InvalidJobOptions(
                "job capacity must be positive and at least the worker count",
            ));
        }
        if options.lease_duration.is_zero()
            || options.poll_interval.is_zero()
            || options.progress_flush_interval.is_zero()
        {
            return Err(InvalidJobOptions("job lease and poll interval must be positive"));
        }
        let token_factory = options.token_factory.unwrap_or_else(|| Arc::new(token_urlsafe));
        let owner_id = token_factory();
        Ok(Self {
            inner: Arc::new(Inner {
                service,
                repository,
                clock: options.clock.unwrap_or_else(|| Arc::new(Utc::now)),
                token_factory,
                owner_id,
                max_concurrent_jobs: options.max_concurrent_jobs,
                max_active_jobs: options.max_active_jobs,
                lease_duration: options.lease_duration,
                poll_interval: options.poll_interval,
                progress_flush_interval: options.progress_flush_interval,
                stop_heartbeat: options.stop_heartbeat.unwrap_or_else(default_stopper),
                wake: Wake::default(),
                lifecycle: tokio::sync::Mutex::new(Lifecycle::default()),
                executions: Mutex::new(HashMap::new()),
                cancel_operations: TaskTracker::new(),
                closing: AtomicBool::new(false),
                timers: TimerRegistry::default(),
            }),
        })
    }

    /// The lease owner this manager claims jobs as.
    pub fn owner_id(&self) -> &str {
        &self.inner.owner_id
    }

    /// `start()`: fail the jobs whose lease ran out, then start the workers and the reaper.
    /// Starting a started manager does nothing.
    pub async fn start(&self) -> Result<(), ServiceError> {
        let mut lifecycle = self.inner.lifecycle.lock().await;
        if !lifecycle.workers.is_empty() {
            return Ok(());
        }
        self.inner.closing.store(false, Ordering::SeqCst);
        self.recover_interrupted_jobs().await?;
        let shutdown = CancellationToken::new();
        lifecycle.workers = (0..self.inner.max_concurrent_jobs)
            .map(|_| tokio::spawn(worker_loop(Arc::clone(&self.inner), shutdown.clone())))
            .collect();
        lifecycle.reaper = Some(tokio::spawn(reaper_loop(Arc::clone(&self.inner), shutdown.clone())));
        lifecycle.shutdown = shutdown;
        self.inner.wake.set();
        Ok(())
    }

    /// `start_plan(actor_ids)`: queue a scan; the job (its id is the plan's) as stored.
    pub async fn start_plan(&self, actor_ids: &[String]) -> Result<JobRecord, ServiceError> {
        self.start().await?;
        let normalized = self.inner.service.validate_actor_ids(actor_ids)?;
        let plan_id = (self.inner.token_factory)();
        let now = self.inner.now();
        let mut job = JobRecord::new(
            plan_id.clone(),
            JobOperation::CreatePlan,
            JobState::Queued,
            now,
            now,
            Some(plan_id),
            normalized,
        );
        job.progress = JobProgress {
            stage: JobStage::Queued,
            completed: 0,
            total: Some(i64::try_from(job.actor_ids.len()).unwrap_or(i64::MAX)),
            unit: JobProgressUnit::Actors,
            current: None,
            stage_started_at: now,
            updated_at: now,
        };
        if !self.inner.repository.enqueue_job(&job, max_active(&self.inner)).await? {
            return Err(queue_full(self.inner.max_active_jobs));
        }
        self.inner.wake.set();
        Ok(job)
    }

    /// `start_apply(plan_id=, revision=, candidate_ids=, request_id=)`: queue an apply, or
    /// return the one queued under the same request id with the same request.
    pub async fn start_apply(
        &self,
        plan_id: &str,
        revision: &str,
        candidate_ids: &[String],
        request_id: &str,
    ) -> Result<ApplyJobRecord, ServiceError> {
        self.start().await?;
        let normalized = dedupe(candidate_ids);
        let now = self.inner.now();
        let mut job = JobRecord::new(
            request_id,
            JobOperation::Apply,
            JobState::Queued,
            now,
            now,
            Some(plan_id.to_owned()),
            Vec::new(),
        );
        job.progress = JobProgress {
            stage: JobStage::Queued,
            completed: 0,
            total: Some(i64::try_from(normalized.len()).unwrap_or(i64::MAX)),
            unit: JobProgressUnit::Items,
            current: None,
            stage_started_at: now,
            updated_at: now,
        };
        let record = ApplyJobRecord {
            job,
            revision: revision.to_owned(),
            candidate_ids: normalized,
            result: None,
        };
        let enqueued = self
            .inner
            .repository
            .enqueue_apply_job(&record, max_active(&self.inner))
            .await?;
        match enqueued.outcome {
            EnqueueApplyJobOutcome::Full => return Err(queue_full(self.inner.max_active_jobs)),
            EnqueueApplyJobOutcome::Conflict => {
                return Err(FillActorError::new(FillActorErrorKind::ApplyRequestConflict, request_id).into());
            }
            EnqueueApplyJobOutcome::Enqueued => self.inner.wake.set(),
            EnqueueApplyJobOutcome::Existing => {}
        }
        enqueued.record.ok_or_else(|| ServiceError::Raised {
            class: "RuntimeError".to_owned(),
            message: "enqueued apply job must return its record".to_owned(),
        })
    }

    /// `get_job(plan_id)`.
    pub async fn get_job(&self, plan_id: &str) -> Result<Option<JobRecord>, ServiceError> {
        Ok(self.inner.repository.get_job(plan_id).await?)
    }

    /// `get_apply_job(job_id)`.
    pub async fn get_apply_job(&self, job_id: &str) -> Result<Option<ApplyJobRecord>, ServiceError> {
        Ok(self.inner.repository.get_apply_job(job_id).await?)
    }

    /// `get_plan(plan_id)`: the plan of a scan that completed (fully or partly) and is still
    /// stored; a plan whose job lost its lease stays hidden until its TTL purges it.
    pub async fn get_plan(&self, plan_id: &str) -> Result<Option<FillActorPlan>, ServiceError> {
        let job = self.inner.repository.get_job(plan_id).await?;
        if !job.is_some_and(|job| matches!(job.state, JobState::Completed | JobState::PartialFailed)) {
            return Ok(None);
        }
        if self.inner.repository.get_plan(plan_id).await?.is_none() {
            return Ok(None);
        }
        Ok(Some(self.inner.service.get_plan(plan_id).await?))
    }

    /// `cancel_plan(plan_id)`: cancel a queued or running scan and wait until its execution
    /// here has cleaned up. The cancel runs in a task of its own: dropping this future does
    /// not stop it. An apply job is refused (`ApplyJobNotCancellableError`).
    pub async fn cancel_plan(&self, plan_id: &str) -> Result<CancelJobResult, ServiceError> {
        let inner = Arc::clone(&self.inner);
        let id = plan_id.to_owned();
        let operation = self
            .inner
            .cancel_operations
            .spawn(async move { cancel_and_signal(&inner, &id).await });
        let result = match operation.await {
            Ok(result) => result?,
            Err(error) => std::panic::resume_unwind(error.into_panic()),
        };
        if result.outcome == CancelJobOutcome::NotCancellable {
            return Err(FillActorError::new(FillActorErrorKind::ApplyJobNotCancellable, plan_id).into());
        }
        Ok(result)
    }

    /// `recover_interrupted_jobs()`: fail every running job whose lease ran out.
    pub async fn recover_interrupted_jobs(&self) -> Result<usize, ServiceError> {
        Ok(self
            .inner
            .repository
            .fail_expired_jobs(self.inner.now(), JOB_INTERRUPTED)
            .await?)
    }

    /// `aclose()`: wait for the cancels in flight, stop every execution and wait for its
    /// cleanup (an apply drains its current candidate), then stop the workers and the reaper.
    pub async fn aclose(&self) {
        let mut lifecycle = self.inner.lifecycle.lock().await;
        self.inner.closing.store(true, Ordering::SeqCst);
        let operations = &self.inner.cancel_operations;
        operations.close();
        operations.wait().await;
        operations.reopen();
        let executions: Vec<Arc<Execution>> = self
            .inner
            .executions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        for execution in &executions {
            execution.request_stop(StopReason::Shutdown);
        }
        for execution in &executions {
            execution.finished().await;
        }
        lifecycle.shutdown.cancel();
        let mut tasks: Vec<JoinHandle<()>> = lifecycle.workers.drain(..).collect();
        tasks.extend(lifecycle.reaper.take());
        for task in tasks {
            let _ = task.await;
        }
    }

    /// How many cancels are still running (`len(manager._cancel_operations)`).
    pub fn cancel_operations(&self) -> usize {
        self.inner.cancel_operations.len()
    }

    /// How many progress timers of a job are alive (the `fill-actor-progress-<job>` tasks).
    pub fn progress_timers(&self, job_id: &str) -> usize {
        self.inner.timers.count(job_id)
    }

    /// The jobs this manager is running (`_executions`).
    pub fn running_jobs(&self) -> Vec<String> {
        self.inner
            .executions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .cloned()
            .collect()
    }
}

fn queue_full(max_active_jobs: usize) -> ServiceError {
    FillActorError::new(FillActorErrorKind::JobQueueFull, max_active_jobs.to_string()).into()
}

impl Inner {
    /// `_now()`: the clock in UTC, at Python's microsecond resolution.
    fn now(&self) -> DateTime<Utc> {
        normalize_datetime((self.clock)())
    }

    fn closing(&self) -> bool {
        self.closing.load(Ordering::SeqCst)
    }

    async fn stop_heartbeat(&self, slot: &HeartbeatSlot) -> Escaped {
        match slot.take() {
            Some(heartbeat) => (self.stop_heartbeat)(heartbeat).await,
            None => Ok(()),
        }
    }

    /// `_finish_terminal(reporter, state, ...)`: a failed write is logged, never raised.
    async fn finish_terminal(
        reporter: &Reporter,
        state: JobState,
        error_code: Option<&str>,
        apply_result: Option<&ApplyResult>,
    ) -> bool {
        match reporter.finish(state, error_code, apply_result).await {
            Ok(saved) => saved,
            Err(error) => {
                tracing::error!(target: LOGGER, error = %error, "fill-actor terminal job update failed");
                false
            }
        }
    }

    /// `_revalidate_claim(job)`: renew the lease the claim took before any work starts;
    /// `false` when the job is no longer ours (cancelled or expired in between).
    async fn revalidate_claim(&self, job: &JobRecord) -> bool {
        loop {
            let now = self.now();
            if job.lease_expires_at.is_none_or(|expires| expires <= now) {
                return false;
            }
            match self
                .repository
                .renew_owned_job_lease(&job.job_id, &self.owner_id, now, now + lease(self))
                .await
            {
                Ok(owned) => return owned,
                Err(error) => {
                    tracing::error!(
                        target: LOGGER,
                        error = %error,
                        "fill-actor claimed job revalidation failed; retrying"
                    );
                    tokio::time::sleep(self.poll_interval).await;
                }
            }
        }
    }

    fn start_heartbeat(
        self: &Arc<Self>,
        job: &JobRecord,
        reporter: &Arc<Reporter>,
        execution: &Arc<Execution>,
    ) -> Heartbeat {
        Heartbeat(Some(tokio::spawn(heartbeat(
            Arc::clone(self),
            job.job_id.clone(),
            Arc::clone(reporter),
            Arc::clone(execution),
        ))))
    }
}

/// `_worker_loop()`.
async fn worker_loop(inner: Arc<Inner>, shutdown: CancellationToken) {
    loop {
        if inner.closing() || shutdown.is_cancelled() {
            return;
        }
        if let Err(error) = worker_iteration(&inner, &shutdown).await {
            tracing::error!(target: LOGGER, error = %error, "fill-actor worker iteration failed");
            pause(&shutdown, inner.poll_interval).await;
        }
    }
}

/// What one worker iteration found before it runs anything.
enum Claimed {
    NotReady,
    Nothing,
    Job(Box<JobRecord>),
}

/// One pass of `_worker_loop`'s body. The steps before a job is claimed (the roots check, the
/// claim) give way to the shutdown as Python's cancelled worker does, so a stalled mount
/// cannot hold `aclose()` up (D131); a claimed job always runs to its own cleanup.
async fn worker_iteration(inner: &Arc<Inner>, shutdown: &CancellationToken) -> Result<(), String> {
    let claimed = tokio::select! {
        () = shutdown.cancelled() => return Ok(()),
        claimed = claim(inner) => claimed?,
    };
    match claimed {
        Claimed::NotReady => pause(shutdown, inner.poll_interval).await,
        Claimed::Job(job) => return run_claimed_job(inner, *job).await,
        Claimed::Nothing => {
            inner.wake.clear();
            tokio::select! {
                () = shutdown.cancelled() => {}
                _ = tokio::time::timeout(inner.poll_interval, inner.wake.wait()) => {}
            }
        }
    }
    Ok(())
}

async fn claim(inner: &Arc<Inner>) -> Result<Claimed, String> {
    if !inner.service.roots_ready().await.map_err(|error| error.to_string())? {
        return Ok(Claimed::NotReady);
    }
    // A manager that started closing while the roots were checked claims nothing more: the
    // job stays queued for the next process (D131).
    if inner.closing() {
        return Ok(Claimed::Nothing);
    }
    let now = inner.now();
    let job = inner
        .repository
        .claim_next_job(&inner.owner_id, now, now + lease(inner))
        .await
        .map_err(|error| error.to_string())?;
    Ok(job.map_or(Claimed::Nothing, |job| Claimed::Job(Box::new(job))))
}

/// `_run_claimed_job(job)`: register the execution, run it, forget it.
async fn run_claimed_job(inner: &Arc<Inner>, job: JobRecord) -> Result<(), String> {
    let execution = Arc::new(Execution::new());
    let job_id = job.job_id.clone();
    inner
        .executions
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(job_id.clone(), Arc::clone(&execution));
    // A job claimed while the manager closes is stopped like the ones it found running
    // (Python cancels the worker, and so the job, after taking its snapshot).
    if inner.closing() {
        execution.request_stop(StopReason::Shutdown);
    }
    let task = {
        let (inner, execution) = (Arc::clone(inner), Arc::clone(&execution));
        tokio::spawn(async move {
            let _done = DoneOnDrop(Arc::clone(&execution));
            if job.operation == JobOperation::Apply {
                run_apply(inner, job, execution).await
            } else {
                run_plan(inner, job, execution).await
            }
        })
    };
    let outcome = task.await;
    {
        let mut executions = inner.executions.lock().unwrap_or_else(PoisonError::into_inner);
        if executions
            .get(&job_id)
            .is_some_and(|current| Arc::ptr_eq(current, &execution))
        {
            executions.remove(&job_id);
        }
    }
    match outcome {
        Ok(result) => result,
        Err(error) => Err(format!("the job task ended abnormally: {error}")),
    }
}

/// `_run_plan(job, execution)`.
async fn run_plan(inner: Arc<Inner>, job: JobRecord, execution: Arc<Execution>) -> Escaped {
    let reporter = Reporter::new(&inner, &job);
    let heartbeat = HeartbeatSlot::default();
    let scans = ScanScope::new();
    let body = async {
        if !inner.revalidate_claim(&job).await {
            reporter.mark_unowned().await;
            return Body::Done;
        }
        heartbeat.set(inner.start_heartbeat(&job, &reporter, &execution));
        let created = scans
            .run(
                inner
                    .service
                    .create_plan(&job.actor_ids, job.plan_id.as_deref(), reporter.callback()),
            )
            .await;
        let plan = match created {
            Ok(plan) => plan,
            Err(ServiceError::FillActor(error)) => return Body::Failed(error.code().to_owned()),
            Err(ServiceError::Raised { .. }) => return Body::Failed("plan_creation_failed".to_owned()),
        };
        if inner.stop_heartbeat(&heartbeat).await.is_err() {
            return Body::Failed("plan_creation_failed".to_owned());
        }
        let partial = plan.actors.iter().any(|actor| actor.error_code.is_some())
            || plan
                .videos
                .iter()
                .any(|video| video.state == VideoState::ScanFailed || !video.warnings.is_empty());
        let state = if partial {
            JobState::PartialFailed
        } else {
            JobState::Completed
        };
        if !Inner::finish_terminal(&reporter, state, None, None).await {
            tracing::warn!(
                target: LOGGER,
                "plan {} completed after its job lease was lost; retaining it for audited TTL cleanup",
                plan.plan_id
            );
        }
        Body::Done
    };
    let outcome = tokio::select! {
        biased;
        () = execution.stop.cancelled() => None,
        body = body => Some(body),
    };
    match outcome {
        Some(Body::Done) => Ok(()),
        // `_cleanup_failed_plan`: the terminal write happens whatever the heartbeat's stop did.
        Some(Body::Failed(error_code)) => {
            let stopped = inner.stop_heartbeat(&heartbeat).await;
            Inner::finish_terminal(&reporter, JobState::Failed, Some(&error_code), None).await;
            stopped
        }
        // `_cleanup_stopped_plan`, after the stopped scan's own threads have ended.
        None => {
            scans.settle().await;
            let stopped = inner.stop_heartbeat(&heartbeat).await;
            if StopReason::releases_ownership(execution.stop_reason()) {
                reporter.mark_unowned().await;
            } else {
                Inner::finish_terminal(&reporter, JobState::Failed, Some(JOB_INTERRUPTED), None).await;
            }
            stopped
        }
    }
}

/// `_run_apply(job, execution)`.
#[allow(clippy::too_many_lines, reason = "Python's method, branch for branch")]
async fn run_apply(inner: Arc<Inner>, job: JobRecord, execution: Arc<Execution>) -> Escaped {
    let reporter = Reporter::new(&inner, &job);
    let heartbeat = HeartbeatSlot::default();
    // The apply task, kept outside the body so a stop can drain it.
    let apply_task: Mutex<ApplySlot> = Mutex::new(ApplySlot::Empty);
    let body = async {
        if !inner.revalidate_claim(&job).await {
            reporter.mark_unowned().await;
            return Ok(Body::Done);
        }
        match inner.service.apply_ready().await {
            Ok(true) => {}
            Ok(false) => {
                Inner::finish_terminal(&reporter, JobState::Failed, Some("not_ready"), None).await;
                return Ok(Body::Done);
            }
            Err(error) => return Err(error),
        }
        let stored = inner
            .repository
            .get_apply_job(&job.job_id)
            .await
            .map_err(ServiceError::from)?;
        let Some((plan_id, stored)) = stored.and_then(|stored| stored.job.plan_id.clone().map(|id| (id, stored)))
        else {
            Inner::finish_terminal(&reporter, JobState::Failed, Some("apply_job_payload_missing"), None).await;
            return Ok(Body::Done);
        };
        heartbeat.set(inner.start_heartbeat(&job, &reporter, &execution));
        let handle = {
            let service = Arc::clone(&inner.service);
            let progress = reporter.callback();
            let stop_execution = Arc::clone(&execution);
            let stop_requested: StopCallback = Arc::new(move || stop_execution.stop_reason().is_some());
            tokio::spawn(async move {
                service
                    .apply(
                        &plan_id,
                        &stored.revision,
                        &stored.candidate_ids,
                        progress,
                        stop_requested,
                    )
                    .await
            })
        };
        *apply_task.lock().unwrap_or_else(PoisonError::into_inner) = ApplySlot::Running(handle);
        // The handle stays in the slot until it ends, and its outcome after that, so a stop
        // finds the apply there however late it lands (Python hands the finished task to
        // `_cleanup_stopped_apply`).
        let result = ApplyWait(&apply_task).await?;
        if let Err(error) = inner.stop_heartbeat(&heartbeat).await {
            return Err(ServiceError::Raised {
                class: "Exception".to_owned(),
                message: error,
            });
        }
        if !Inner::finish_terminal(&reporter, JobState::Completed, None, Some(&result)).await {
            tracing::warn!(target: LOGGER, "apply job {} completed after its lease was lost", job.job_id);
        }
        Ok(Body::Done)
    };
    let outcome = tokio::select! {
        biased;
        () = execution.stop.cancelled() => None,
        body = body => Some(body),
    };
    match outcome {
        Some(Ok(Body::Done | Body::Failed(_))) => Ok(()),
        Some(Err(ServiceError::FillActor(error))) => {
            let stopped = inner.stop_heartbeat(&heartbeat).await;
            Inner::finish_terminal(&reporter, JobState::Failed, Some(error.code()), None).await;
            stopped
        }
        Some(Err(error @ ServiceError::Raised { .. })) => {
            tracing::error!(target: LOGGER, error = %error, "fill-actor apply job failed unexpectedly");
            let stopped = inner.stop_heartbeat(&heartbeat).await;
            Inner::finish_terminal(&reporter, JobState::Failed, Some("apply_failed"), None).await;
            stopped
        }
        // `_cleanup_stopped_apply`: drain the current move, then settle the row.
        None => {
            let slot = std::mem::replace(
                &mut *apply_task.lock().unwrap_or_else(PoisonError::into_inner),
                ApplySlot::Empty,
            );
            let outcome = match slot {
                ApplySlot::Empty => None,
                ApplySlot::Finished(outcome) => Some(outcome),
                ApplySlot::Running(task) => Some(joined(task.await)),
            };
            let mut result = None;
            match outcome {
                None | Some(Err(ServiceError::FillActor(_))) => {}
                Some(Ok(applied)) => result = Some(applied),
                Some(Err(error)) => tracing::error!(
                    target: LOGGER,
                    error = %error,
                    "stopped apply job failed while its active move was draining"
                ),
            }
            inner.stop_heartbeat(&heartbeat).await?;
            if StopReason::releases_ownership(execution.stop_reason()) {
                reporter.mark_unowned().await;
                return Ok(());
            }
            let stored = inner
                .repository
                .get_apply_job(&job.job_id)
                .await
                .map_err(|error| error.to_string())?;
            let completed_all = match (&stored, &result) {
                (Some(stored), Some(result)) => result.results.len() == stored.candidate_ids.len(),
                _ => false,
            };
            if completed_all {
                Inner::finish_terminal(&reporter, JobState::Completed, None, result.as_ref()).await;
            } else {
                Inner::finish_terminal(&reporter, JobState::Failed, Some(JOB_INTERRUPTED), None).await;
            }
            Ok(())
        }
    }
}

/// An apply job's work: not started, running, or its outcome once it ended.
enum ApplySlot {
    Empty,
    Running(JoinHandle<Result<ApplyResult, ServiceError>>),
    Finished(Result<ApplyResult, ServiceError>),
}

/// The apply task's outcome, a panic or abort as the exception it would be.
fn joined(
    joined: Result<Result<ApplyResult, ServiceError>, tokio::task::JoinError>,
) -> Result<ApplyResult, ServiceError> {
    joined.unwrap_or_else(|error| {
        Err(ServiceError::Raised {
            class: "RuntimeError".to_owned(),
            message: format!("the apply task ended abnormally: {error}"),
        })
    })
}

/// Waits for the apply task in the slot without taking it out, and leaves its outcome there,
/// so a stop that drops the waiting body (even after the apply ended) finds what it did.
struct ApplyWait<'a>(&'a Mutex<ApplySlot>);

impl Future for ApplyWait<'_> {
    type Output = Result<ApplyResult, ServiceError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let mut slot = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let handle = match &mut *slot {
            ApplySlot::Running(handle) => handle,
            ApplySlot::Finished(outcome) => return std::task::Poll::Ready(outcome.clone()),
            ApplySlot::Empty => {
                return std::task::Poll::Ready(Err(ServiceError::Raised {
                    class: "RuntimeError".to_owned(),
                    message: "the apply task is gone".to_owned(),
                }));
            }
        };
        match std::pin::Pin::new(handle).poll(cx) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(outcome) => {
                let outcome = joined(outcome);
                *slot = ApplySlot::Finished(outcome.clone());
                std::task::Poll::Ready(outcome)
            }
        }
    }
}

/// `_heartbeat(job, reporter, execution)`: renew the lease every third of its duration; a
/// renewal the row refuses stops the job (the lease is gone).
async fn heartbeat(inner: Arc<Inner>, job_id: String, reporter: Arc<Reporter>, execution: Arc<Execution>) {
    let interval = (inner.lease_duration / 3).max(Duration::from_millis(50));
    let retry_interval = inner.poll_interval.min(interval);
    loop {
        tokio::time::sleep(interval).await;
        loop {
            let now = inner.now();
            match inner
                .repository
                .renew_owned_job_lease(&job_id, &inner.owner_id, now, now + lease(&inner))
                .await
            {
                Err(error) => {
                    tracing::error!(target: LOGGER, error = %error, "fill-actor job heartbeat failed; retrying");
                    tokio::time::sleep(retry_interval).await;
                }
                Ok(false) => {
                    execution.request_stop(StopReason::OwnershipLost);
                    reporter.mark_unowned().await;
                    return;
                }
                Ok(true) => break,
            }
        }
    }
}

/// `_cancel_and_signal(plan_id)`: cancel the row, stop the local execution and wait for it.
async fn cancel_and_signal(inner: &Inner, plan_id: &str) -> Result<CancelJobResult, ServiceError> {
    let started = Instant::now();
    let cleanup_complete = |execution: &Option<Arc<Execution>>| execution.as_ref().is_none_or(|e| e.is_done());
    let elapsed_ms = || (started.elapsed().as_secs_f64() * 10_000.0).round() / 10.0;
    let result = match inner.repository.cancel_job(plan_id, inner.now()).await {
        Ok(result) => result,
        Err(error) => {
            tracing::error!(
                target: LOGGER,
                error = %error,
                embyx_event = "fill_actor_cancel_failed",
                embyx_local_execution = false,
                embyx_cleanup_complete = true,
                embyx_elapsed_ms = elapsed_ms(),
                "fill-actor cancellation failed local_execution=False cleanup_complete=True elapsed_ms={}",
                elapsed_ms()
            );
            tracing::error!(target: LOGGER, error = %error, "fill-actor cancellation operation failed");
            return Err(error.into());
        }
    };
    let execution = inner
        .executions
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(plan_id)
        .cloned();
    let signalled = matches!(
        result.outcome,
        CancelJobOutcome::Cancelled | CancelJobOutcome::AlreadyCancelled
    );
    if signalled && let Some(execution) = &execution {
        execution.request_stop(StopReason::UserCancel);
        execution.finished().await;
    }
    let previous_state = result.previous_state.map_or("None", JobState::as_str);
    tracing::info!(
        target: LOGGER,
        embyx_event = "fill_actor_cancel_complete",
        embyx_cancel_outcome = result.outcome.as_str(),
        embyx_previous_state = previous_state,
        embyx_local_execution = execution.is_some(),
        embyx_cleanup_complete = cleanup_complete(&execution),
        embyx_elapsed_ms = elapsed_ms(),
        "fill-actor cancellation completed outcome={} previous_state={} local_execution={} cleanup_complete={} elapsed_ms={}",
        result.outcome.as_str(),
        previous_state,
        py_bool(execution.is_some()),
        py_bool(cleanup_complete(&execution)),
        elapsed_ms()
    );
    Ok(result)
}

fn py_bool(value: bool) -> &'static str {
    if value { "True" } else { "False" }
}

/// `_reaper_loop()`: fail expired leases every third of a lease (at least one poll interval).
async fn reaper_loop(inner: Arc<Inner>, shutdown: CancellationToken) {
    let interval = (inner.lease_duration / 3).max(inner.poll_interval);
    loop {
        pause(&shutdown, interval).await;
        if shutdown.is_cancelled() {
            return;
        }
        if let Err(error) = inner.repository.fail_expired_jobs(inner.now(), JOB_INTERRUPTED).await {
            tracing::error!(target: LOGGER, error = %error, "fill-actor job reaper iteration failed");
        }
    }
}
