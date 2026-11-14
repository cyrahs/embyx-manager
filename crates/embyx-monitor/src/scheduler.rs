//! Scheduler that runs the monitor pipelines.
//!
//! Python origin: `src/embyx_manager/monitor/scheduler.py`. Tasks on the runtime replace
//! embyx-monitor's thread loops:
//!
//! - an rss loop running every `rss.interval_seconds`;
//! - an archive loop running the fallback scan on `archive.scan_cron`, in the server's
//!   local time ([`cron`]);
//! - a mapping loop with one full sync at startup, a periodic full sync as a safety net, and
//!   debounced incremental syncs fed by the `.strm` watcher ([`watch`]);
//! - a playlists loop running every `playlists.interval_seconds`;
//! - the acquisition tracker's loop, with bursts of fast checks after a submission;
//! - manual triggers from the dashboard, one in-flight run per pipeline.
//!
//! Configuration is read from the live config store at every decision point, so edits apply
//! without a restart.
//!
//! Cancelling: an operator's cancel sets the run's stop token (`RunContext::request_stop`),
//! which the pipeline answers with `RunCancelledError` at its next check. Shutdown also
//! fires the scheduler's [`CancellationToken`], handed to every runner in its [`RunScope`]:
//! Python cancels the task (`asyncio.CancelledError` at its current await), and a runner
//! here answers the token the same way, after awaiting the CloudDrive mutations it already
//! sent ([`until_shutdown`]; cross-version contract 6). The scheduler awaits every runner
//! to its end rather than dropping it (D101).

pub mod cron;
mod watch;

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use chrono::{DateTime, SubsecRound as _, TimeDelta, Utc};
use embyx_core::config::{ArchiveConfig, MappingConfig, PlaylistsConfig, RssConfig};
use embyx_db::config_store::ConfigStore;
use notify::Watcher as _;
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub use self::cron::{next_cron_fire, next_cron_fire_in};
use self::watch::WatchState;
use crate::mapping::{MappingError, MappingPipeline};
use crate::reports::{
    Counters, PipelineName, PyException, Raised, RunCancelledError, RunContext, RunState, RunTrigger,
};
use crate::runs::PipelineRunRepository;
use crate::support::{BoxFuture, Clock, StoreError, py_add, system_clock};

/// The `logging.getLogger('embyx-manager.scheduler')` name.
pub const LOGGER: &str = "embyx-manager.scheduler";

/// `FULL_SYNC_RETRY_SECONDS`.
pub const FULL_SYNC_RETRY_SECONDS: f64 = 300.0;
/// `IDLE_POLL_SECONDS`.
pub const IDLE_POLL_SECONDS: f64 = 0.5;
/// `STARTUP_RETRY_SECONDS`.
pub const STARTUP_RETRY_SECONDS: f64 = 60.0;
/// `CONFIG_REFRESH_SECONDS`.
pub const CONFIG_REFRESH_SECONDS: f64 = 15.0;
/// `CRON_POLL_SECONDS`.
pub const CRON_POLL_SECONDS: f64 = 20.0;
/// `TRACKER_FAST_CHECKS_SECONDS`: seconds after a magnet submission at which the tracker
/// takes an extra look. Popular magnets often finish within moments on the provider's
/// cache; whatever is still downloading after the last one waits for
/// `tracker_interval_seconds`.
pub const TRACKER_FAST_CHECKS_SECONDS: [f64; 4] = [15.0, 30.0, 60.0, 120.0];
/// `TRACKER_SUBMISSION_MERGE_SECONDS`: submissions closer together than this share one
/// burst of fast checks, so an RSS run queueing a batch of magnets does not multiply polls.
pub const TRACKER_SUBMISSION_MERGE_SECONDS: f64 = 5.0;

/// `'process restarted while the run was in flight'`.
const STALE_RUN_ERROR: &str = "process restarted while the run was in flight";

/// The module constants Python's tests monkeypatch, as values.
#[derive(Debug, Clone, PartialEq)]
pub struct Timings {
    pub full_sync_retry: f64,
    pub idle_poll: f64,
    pub startup_retry: f64,
    pub config_refresh: f64,
    pub cron_poll: f64,
    pub tracker_fast_checks: Vec<f64>,
}

impl Default for Timings {
    fn default() -> Self {
        Self {
            full_sync_retry: FULL_SYNC_RETRY_SECONDS,
            idle_poll: IDLE_POLL_SECONDS,
            startup_retry: STARTUP_RETRY_SECONDS,
            config_refresh: CONFIG_REFRESH_SECONDS,
            cron_poll: CRON_POLL_SECONDS,
            tracker_fast_checks: TRACKER_FAST_CHECKS_SECONDS.to_vec(),
        }
    }
}

/// A non-negative number of seconds as a sleep; NaN is none, and a span past what a
/// `Duration` holds is the longest one (tokio sleeps "forever" then).
fn seconds(value: f64) -> Duration {
    if value.is_nan() || value <= 0.0 {
        Duration::ZERO
    } else {
        Duration::try_from_secs_f64(value).unwrap_or(Duration::MAX)
    }
}

/// `timedelta(seconds=value)`: rounded to the microsecond; `None` where Python raises
/// `OverflowError`.
fn timedelta(value: f64) -> Option<TimeDelta> {
    let micros = (value * 1e6).round_ties_even();
    // timedelta's limit: 999999999 days.
    let limit = 999_999_999.0 * 86_400.0 * 1e6;
    if !micros.is_finite() || micros.abs() > limit {
        return None;
    }
    #[expect(clippy::cast_possible_truncation, reason = "bounded above")]
    Some(TimeDelta::microseconds(micros as i64))
}

/// `PipelineBusyError` / `PipelineNotConfiguredError`, and a start that could not be
/// recorded (an exception Python lets escape the trigger).
#[derive(Debug, thiserror::Error)]
pub enum TriggerError {
    #[error("pipeline {0} already has a run in flight")]
    Busy(PipelineName),
    #[error("pipeline {pipeline} is not runnable: {reason}")]
    NotConfigured { pipeline: PipelineName, reason: String },
    #[error(transparent)]
    Start(#[from] StoreError),
    /// The run's task ended before it said whether the start was recorded (it panicked).
    #[error("the run ended before its start was recorded")]
    Lost,
}

impl PyException for TriggerError {
    fn class_name(&self) -> &str {
        match self {
            Self::Busy(_) => "PipelineBusyError",
            Self::NotConfigured { .. } => "PipelineNotConfiguredError",
            Self::Start(error) => error.class_name(),
            Self::Lost => "RuntimeError",
        }
    }
}

/// How a run ended other than completing.
pub enum RunFailure {
    /// `RunCancelledError`: an operator's cancel (or a stop the pipeline saw).
    Cancelled,
    /// `asyncio.CancelledError`: the scheduler is shutting down.
    Shutdown,
    /// Any other exception.
    Raised(Box<dyn PyException + Send + Sync>),
}

impl RunFailure {
    pub fn raised(error: impl PyException + Send + Sync + 'static) -> Self {
        Self::Raised(Box::new(error))
    }
}

impl fmt::Debug for RunFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => f.write_str("Cancelled"),
            Self::Shutdown => f.write_str("Shutdown"),
            Self::Raised(error) => write!(f, "Raised({})", error.py_repr()),
        }
    }
}

impl From<RunCancelledError> for RunFailure {
    fn from(_: RunCancelledError) -> Self {
        Self::Cancelled
    }
}

impl From<MappingError> for RunFailure {
    fn from(error: MappingError) -> Self {
        match error {
            MappingError::Cancelled(_) => Self::Cancelled,
            other => Self::raised(other),
        }
    }
}

/// What a runner gets: the run's context and the scheduler's shutdown token.
#[derive(Debug, Clone)]
pub struct RunScope {
    pub ctx: Arc<RunContext>,
    pub shutdown: CancellationToken,
}

/// One pipeline pass (`rss_runner(ctx)`, `archive_runner(ctx)`, `playlists_runner(ctx)`,
/// `tracker_poll(ctx)`). It must end soon after `scope.shutdown` fires, answering
/// [`RunFailure::Shutdown`]; [`until_shutdown`] does that around a future.
pub type Runner = Arc<dyn Fn(RunScope) -> BoxFuture<'static, Result<(), RunFailure>> + Send + Sync>;

/// `*_ready()`: `None` when runnable, else the reason.
pub type Ready = Arc<dyn Fn() -> Option<String> + Send + Sync>;

/// What `mapping_factory()` builds: a `MappingPipeline`-compatible object. Its passes run
/// on a blocking thread, as Python runs them in `asyncio.to_thread`.
pub trait MappingRun: Send + Sync {
    fn run_full(&self, ctx: &RunContext) -> Result<(), MappingError>;
    /// The changed and deleted paths, and back the ones that failed.
    fn run_incremental(
        &self,
        ctx: &RunContext,
        changed: &[PathBuf],
        deleted: &[PathBuf],
    ) -> Result<(Vec<PathBuf>, Vec<PathBuf>), RunCancelledError>;
}

impl MappingRun for MappingPipeline {
    fn run_full(&self, ctx: &RunContext) -> Result<(), MappingError> {
        Self::run_full(self, ctx)
    }

    fn run_incremental(
        &self,
        ctx: &RunContext,
        changed: &[PathBuf],
        deleted: &[PathBuf],
    ) -> Result<(Vec<PathBuf>, Vec<PathBuf>), RunCancelledError> {
        Self::run_incremental(self, ctx, changed, deleted)
    }
}

/// `mapping_factory()`, called for every mapping run so it sees the current configuration.
pub type MappingFactory = Arc<dyn Fn() -> Box<dyn MappingRun> + Send + Sync>;

/// Run `work` until it ends or `shutdown` fires. On shutdown the work is dropped, `settle`
/// is awaited (the CloudDrive mutations the work sent end first: contract 6) and the
/// answer is [`RunFailure::Shutdown`], Python's `CancelledError`.
pub async fn until_shutdown<W, S>(shutdown: &CancellationToken, work: W, settle: S) -> Result<(), RunFailure>
where
    W: Future<Output = Result<(), RunFailure>>,
    S: Future<Output = ()>,
{
    tokio::select! {
        biased;
        () = shutdown.cancelled() => {}
        outcome = work => return outcome,
    }
    settle.await;
    Err(RunFailure::Shutdown)
}

/// The live sections the scheduler decides on, and `store.refresh()`.
pub trait SchedulerConfig: Send + Sync {
    fn rss(&self) -> RssConfig;
    fn archive(&self) -> ArchiveConfig;
    fn mapping(&self) -> MappingConfig;
    fn playlists(&self) -> PlaylistsConfig;
    /// Converge on config changes written by other replicas; the error's text for the log.
    fn refresh(&self) -> BoxFuture<'_, Result<(), String>>;
}

impl SchedulerConfig for ConfigStore {
    fn rss(&self) -> RssConfig {
        self.get::<RssConfig>()
    }

    fn archive(&self) -> ArchiveConfig {
        self.get::<ArchiveConfig>()
    }

    fn mapping(&self) -> MappingConfig {
        self.get::<MappingConfig>()
    }

    fn playlists(&self) -> PlaylistsConfig {
        self.get::<PlaylistsConfig>()
    }

    fn refresh(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move { Self::refresh(self).await.map_err(|error| error.to_string()) })
    }
}

/// The run history calls the scheduler makes (`PipelineRunRepository`).
pub trait RunStore: Send + Sync {
    fn start_run(&self, pipeline: PipelineName, trigger: RunTrigger) -> BoxFuture<'_, Result<String, StoreError>>;
    fn finish_run<'a>(
        &'a self,
        run_id: &'a str,
        state: RunState,
        counters: &'a Counters,
        errors: &'a [String],
        log_tail: &'a [String],
    ) -> BoxFuture<'a, Result<(), StoreError>>;
    fn fail_stale_running<'a>(&'a self, error: &'a str) -> BoxFuture<'a, Result<u64, StoreError>>;
}

impl RunStore for PipelineRunRepository {
    fn start_run(&self, pipeline: PipelineName, trigger: RunTrigger) -> BoxFuture<'_, Result<String, StoreError>> {
        Box::pin(Self::start_run(self, pipeline, trigger))
    }

    fn finish_run<'a>(
        &'a self,
        run_id: &'a str,
        state: RunState,
        counters: &'a Counters,
        errors: &'a [String],
        log_tail: &'a [String],
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(Self::finish_run(self, run_id, state, counters, errors, log_tail))
    }

    fn fail_stale_running<'a>(&'a self, error: &'a str) -> BoxFuture<'a, Result<u64, StoreError>> {
        Box::pin(Self::fail_stale_running(self, error))
    }
}

/// `TrackerState`: what the tracker did on its last pass, for the dashboard.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrackerState {
    pub last_polled_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_stats: Counters,
}

/// `PipelineStatus`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineStatus {
    pub pipeline: PipelineName,
    pub enabled: bool,
    pub configured: bool,
    pub reason: Option<String>,
    pub running_run_id: Option<String>,
    pub next_scheduled_at: Option<DateTime<Utc>>,
}

/// `asyncio.Event`: set until cleared; waiters wake when it is set.
#[derive(Debug, Default)]
struct Event {
    flag: AtomicBool,
    notify: Notify,
}

impl Event {
    fn set(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    fn clear(&self) {
        self.flag.store(false, Ordering::SeqCst);
    }

    fn is_set(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_set() {
                return;
            }
            notified.await;
        }
    }
}

/// The tracker's submission bookkeeping: `notify_submission` and the bursts it owes. Made
/// before the scheduler, so the intake, the RSS run and the manual source can hold it
/// (Python resolves `scheduler.notify_submission` lazily).
#[derive(Debug)]
pub struct TrackerSubmissions {
    /// Monotonic times of recent submissions, each owed a burst of fast checks.
    submitted_at: Mutex<Vec<Instant>>,
    event: Event,
    merge_window: Duration,
}

impl Default for TrackerSubmissions {
    fn default() -> Self {
        Self::new()
    }
}

impl TrackerSubmissions {
    pub fn new() -> Self {
        Self {
            submitted_at: Mutex::new(Vec::new()),
            event: Event::default(),
            merge_window: seconds(TRACKER_SUBMISSION_MERGE_SECONDS),
        }
    }

    fn submitted_at(&self) -> MutexGuard<'_, Vec<Instant>> {
        self.submitted_at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `notify_submission()`: a magnet was just queued; owe it the burst of fast checks.
    pub fn notify(&self) {
        let now = Instant::now();
        {
            let mut submitted = self.submitted_at();
            let merged = submitted
                .last()
                .is_some_and(|last| now.saturating_duration_since(*last) < self.merge_window);
            if !merged {
                submitted.push(now);
            }
        }
        self.event.set();
    }

    /// `_pending_fast_checks()`: the deadlines of the fast checks still owed.
    fn pending(&self, offsets: &[Duration]) -> Vec<Instant> {
        let now = Instant::now();
        let horizon = offsets.last().copied().unwrap_or(Duration::ZERO);
        let mut submitted = self.submitted_at();
        submitted.retain(|at| now.saturating_duration_since(*at) < horizon);
        submitted
            .iter()
            .flat_map(|at| offsets.iter().map(move |offset| *at + *offset))
            .filter(|due| now < *due)
            .collect()
    }
}

/// The next scheduled times the status reports.
#[derive(Debug, Default)]
struct NextTimes {
    rss: Option<DateTime<Utc>>,
    archive: Option<DateTime<Utc>>,
    full_sync: Option<DateTime<Utc>>,
    playlists: Option<DateTime<Utc>>,
}

/// A running watcher and the directory it watches.
struct Observer {
    _watcher: notify::RecommendedWatcher,
    directory: String,
}

/// `MonitorScheduler(...)`'s keyword arguments.
pub struct SchedulerParts {
    pub config: Arc<dyn SchedulerConfig>,
    pub runs: Arc<dyn RunStore>,
    pub rss_runner: Runner,
    pub archive_runner: Runner,
    pub mapping_factory: MappingFactory,
    pub rss_ready: Ready,
    pub archive_ready: Ready,
    pub mapping_ready: Ready,
    pub tracker_poll: Option<Runner>,
    pub tracker_ready: Option<Ready>,
    /// Without a playlists runner that pipeline reports itself unavailable and never
    /// schedules.
    pub playlists_runner: Option<Runner>,
    pub playlists_ready: Option<Ready>,
    pub submissions: Arc<TrackerSubmissions>,
    pub timings: Timings,
    pub clock: Clock,
}

impl SchedulerParts {
    /// The parts with the defaults Python's constructor has: no tracker, no playlists, the
    /// module's timings and the wall clock.
    pub fn new(
        config: Arc<dyn SchedulerConfig>,
        runs: Arc<dyn RunStore>,
        runners: (Runner, Runner, MappingFactory),
        ready: (Ready, Ready, Ready),
    ) -> Self {
        Self {
            config,
            runs,
            rss_runner: runners.0,
            archive_runner: runners.1,
            mapping_factory: runners.2,
            rss_ready: ready.0,
            archive_ready: ready.1,
            mapping_ready: ready.2,
            tracker_poll: None,
            tracker_ready: None,
            playlists_runner: None,
            playlists_ready: None,
            submissions: Arc::new(TrackerSubmissions::new()),
            timings: Timings::default(),
            clock: system_clock(),
        }
    }
}

impl fmt::Debug for SchedulerParts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchedulerParts")
            .field("timings", &self.timings)
            .finish_non_exhaustive()
    }
}

/// `_playlists_unavailable()`.
pub fn playlists_unavailable() -> String {
    "playlist sync is not available in this deployment".to_owned()
}

struct Inner {
    config: Arc<dyn SchedulerConfig>,
    runs: Arc<dyn RunStore>,
    rss_runner: Runner,
    archive_runner: Runner,
    mapping_factory: MappingFactory,
    playlists_runner: Option<Runner>,
    tracker_poll: Option<Runner>,
    tracker_ready: Option<Ready>,
    ready: HashMap<PipelineName, Ready>,
    tracker_state: Mutex<TrackerState>,
    submissions: Arc<TrackerSubmissions>,
    active: Mutex<HashMap<PipelineName, (String, Arc<RunContext>)>>,
    locks: HashMap<PipelineName, Arc<AsyncMutex<()>>>,
    tasks: TaskTracker,
    shutdown: CancellationToken,
    next: Mutex<NextTimes>,
    watch: Arc<Mutex<WatchState>>,
    observer: Mutex<Option<Observer>>,
    timings: Timings,
    clock: Clock,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// `MonitorScheduler`. Cloning shares the scheduler.
#[derive(Clone)]
pub struct MonitorScheduler {
    inner: Arc<Inner>,
}

impl fmt::Debug for MonitorScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MonitorScheduler")
            .field("timings", &self.inner.timings)
            .finish_non_exhaustive()
    }
}

impl MonitorScheduler {
    pub fn new(parts: SchedulerParts) -> Self {
        let playlists_ready = parts
            .playlists_ready
            .unwrap_or_else(|| Arc::new(|| Some(playlists_unavailable())));
        let ready = HashMap::from([
            (PipelineName::Rss, parts.rss_ready),
            (PipelineName::Archive, parts.archive_ready),
            (PipelineName::Mapping, parts.mapping_ready),
            (PipelineName::Playlists, playlists_ready),
        ]);
        let locks = PipelineName::ALL
            .iter()
            .map(|name| (*name, Arc::new(AsyncMutex::new(()))))
            .collect();
        Self {
            inner: Arc::new(Inner {
                config: parts.config,
                runs: parts.runs,
                rss_runner: parts.rss_runner,
                archive_runner: parts.archive_runner,
                mapping_factory: parts.mapping_factory,
                playlists_runner: parts.playlists_runner,
                tracker_poll: parts.tracker_poll,
                tracker_ready: parts.tracker_ready,
                ready,
                tracker_state: Mutex::new(TrackerState::default()),
                submissions: parts.submissions,
                active: Mutex::new(HashMap::new()),
                locks,
                tasks: TaskTracker::new(),
                shutdown: CancellationToken::new(),
                next: Mutex::new(NextTimes::default()),
                watch: Arc::new(Mutex::new(WatchState::default())),
                observer: Mutex::new(None),
                timings: parts.timings,
                clock: parts.clock,
            }),
        }
    }

    fn now(&self) -> DateTime<Utc> {
        (self.inner.clock)()
    }

    fn ready(&self, pipeline: PipelineName) -> Option<String> {
        (self.inner.ready[&pipeline])()
    }

    fn stopped(&self) -> bool {
        self.inner.shutdown.is_cancelled()
    }

    fn next(&self) -> MutexGuard<'_, NextTimes> {
        locked(&self.inner.next)
    }

    // -- lifecycle ---------------------------------------------------------

    /// `start()`: orphaned runs failed, then the loops started. A failure to reach the run
    /// history is the start's failure, as in Python.
    pub async fn start(&self) -> Result<(), StoreError> {
        let recovered = self.inner.runs.fail_stale_running(STALE_RUN_ERROR).await?;
        if recovered > 0 {
            tracing::warn!(logger = LOGGER, "marked {recovered} orphaned pipeline runs as failed");
        }
        let tasks = &self.inner.tasks;
        tasks.spawn(self.clone().rss_loop());
        tasks.spawn(self.clone().archive_loop());
        tasks.spawn(self.clone().mapping_loop());
        tasks.spawn(self.clone().config_refresh_loop());
        if self.inner.tracker_poll.is_some() {
            tasks.spawn(self.clone().tracker_loop());
        }
        if self.inner.playlists_runner.is_some() {
            tasks.spawn(self.clone().playlists_loop());
        }
        Ok(())
    }

    /// `aclose()`: stop every loop and run, wait for them to end, stop the watcher.
    pub async fn aclose(&self) {
        self.inner.shutdown.cancel();
        for (_, ctx) in locked(&self.inner.active).values() {
            ctx.request_stop();
        }
        self.inner.tasks.close();
        self.inner.tasks.wait().await;
        self.stop_observer();
    }

    // -- public API ----------------------------------------------------------

    /// `trigger(pipeline)`: start a manual run; its id once the start is recorded.
    pub async fn trigger(&self, pipeline: PipelineName) -> Result<String, TriggerError> {
        if let Some(reason) = self.ready(pipeline) {
            return Err(TriggerError::NotConfigured { pipeline, reason });
        }
        let Ok(guard) = Arc::clone(&self.inner.locks[&pipeline]).try_lock_owned() else {
            return Err(TriggerError::Busy(pipeline));
        };
        let (started, run_id) = oneshot::channel();
        let this = self.clone();
        self.inner.tasks.spawn(async move {
            this.execute(pipeline, RunTrigger::Manual, guard, Some(started), None)
                .await;
        });
        match run_id.await {
            Ok(outcome) => outcome,
            // The run's task ended without saying (it panicked): nothing was started.
            Err(_) => Err(TriggerError::Lost),
        }
    }

    /// `cancel_running(pipeline)`: ask the run in flight to stop; whether there was one.
    pub fn cancel_running(&self, pipeline: PipelineName) -> bool {
        match locked(&self.inner.active).get(&pipeline) {
            Some((_, ctx)) => {
                ctx.request_stop();
                true
            }
            None => false,
        }
    }

    /// `status()`.
    pub fn status(&self) -> Vec<PipelineStatus> {
        PipelineName::ALL
            .iter()
            .map(|&name| {
                let reason = self.ready(name);
                let enabled = self.pipeline_enabled(name);
                let running_run_id = locked(&self.inner.active).get(&name).map(|(id, _)| id.clone());
                let next_at = {
                    let next = self.next();
                    match name {
                        PipelineName::Mapping => next.full_sync,
                        PipelineName::Archive => next.archive,
                        PipelineName::Playlists => next.playlists,
                        PipelineName::Rss => next.rss,
                    }
                };
                PipelineStatus {
                    pipeline: name,
                    enabled,
                    configured: reason.is_none(),
                    next_scheduled_at: if enabled && reason.is_none() { next_at } else { None },
                    reason,
                    running_run_id,
                }
            })
            .collect()
    }

    /// `tracker_state()`.
    pub fn tracker_state(&self) -> TrackerState {
        locked(&self.inner.tracker_state).clone()
    }

    /// `notify_submission()`.
    pub fn notify_submission(&self) {
        self.inner.submissions.notify();
    }

    /// The shared submission bookkeeping.
    pub fn submissions(&self) -> Arc<TrackerSubmissions> {
        Arc::clone(&self.inner.submissions)
    }

    // -- execution -------------------------------------------------------------

    /// A scheduled run: skipped (`None`) while another run of the pipeline is in flight.
    async fn execute_scheduled(
        &self,
        pipeline: PipelineName,
        trigger: RunTrigger,
        batch: Option<(BTreeSet<PathBuf>, BTreeSet<PathBuf>)>,
    ) -> Option<RunState> {
        let guard = Arc::clone(&self.inner.locks[&pipeline]).try_lock_owned().ok()?;
        self.execute(pipeline, trigger, guard, None, batch).await
    }

    /// `_execute(...)`: the single dispatch point for run bookkeeping.
    async fn execute(
        &self,
        pipeline: PipelineName,
        trigger: RunTrigger,
        _guard: OwnedMutexGuard<()>,
        started: Option<oneshot::Sender<Result<String, TriggerError>>>,
        batch: Option<(BTreeSet<PathBuf>, BTreeSet<PathBuf>)>,
    ) -> Option<RunState> {
        let ctx = Arc::new(RunContext::new(format!("embyx-manager.{pipeline}")));
        let run_id = match self.inner.runs.start_run(pipeline, trigger).await {
            Ok(run_id) => run_id,
            Err(error) => {
                tracing::error!(logger = LOGGER, error = %error, "failed to persist run start for {pipeline}");
                if let Some(started) = started {
                    let _ = started.send(Err(TriggerError::Start(error)));
                }
                return None;
            }
        };
        if let Some(started) = started {
            let _ = started.send(Ok(run_id.clone()));
        }
        locked(&self.inner.active).insert(pipeline, (run_id.clone(), Arc::clone(&ctx)));
        let scope = RunScope {
            ctx: Arc::clone(&ctx),
            shutdown: self.inner.shutdown.clone(),
        };
        let outcome = match pipeline {
            PipelineName::Rss => (self.inner.rss_runner)(scope).await,
            PipelineName::Archive => (self.inner.archive_runner)(scope).await,
            PipelineName::Playlists => self.run_playlists(scope).await,
            PipelineName::Mapping => match batch {
                Some(batch) => self.run_mapping_incremental(&scope, batch).await,
                None => self.run_mapping_full(&scope).await,
            },
        };
        let state = match outcome {
            Ok(()) => RunState::Completed,
            Err(RunFailure::Cancelled) => {
                ctx.warning("run cancelled by operator");
                RunState::Cancelled
            }
            Err(RunFailure::Shutdown) => {
                ctx.warning("run cancelled by shutdown");
                RunState::Cancelled
            }
            Err(RunFailure::Raised(error)) => {
                ctx.exception(&format!("{pipeline} run failed"), Some(&*error));
                RunState::Failed
            }
        };
        locked(&self.inner.active).remove(&pipeline);
        // Suppressed, as in Python: the run stays `running` until the next start fails it.
        let _ = self
            .inner
            .runs
            .finish_run(&run_id, state, &ctx.stats(), &ctx.errors(), &ctx.log_tail())
            .await;
        Some(state)
    }

    async fn run_playlists(&self, scope: RunScope) -> Result<(), RunFailure> {
        match &self.inner.playlists_runner {
            Some(runner) => runner(scope).await,
            None => Err(RunFailure::raised(Raised::new(
                "PipelineNotConfiguredError",
                format!(
                    "pipeline {} is not runnable: {}",
                    PipelineName::Playlists,
                    playlists_unavailable()
                ),
            ))),
        }
    }

    /// A mapping pass on a blocking thread (`asyncio.to_thread`). On shutdown the answer is
    /// `CancelledError` at once while the thread finishes on its stop token, as in Python;
    /// the thread is tracked, so `aclose` waits for it.
    async fn on_thread<T, F>(&self, scope: &RunScope, work: F) -> Result<T, RunFailure>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, RunFailure> + Send + 'static,
    {
        let handle = self.inner.tasks.spawn_blocking(work);
        tokio::select! {
            biased;
            () = scope.shutdown.cancelled() => Err(RunFailure::Shutdown),
            joined = handle => match joined {
                Ok(outcome) => outcome,
                Err(error) => Err(RunFailure::raised(Raised::new("RuntimeError", error.to_string()))),
            },
        }
    }

    async fn run_mapping_full(&self, scope: &RunScope) -> Result<(), RunFailure> {
        let pipeline = (self.inner.mapping_factory)();
        let ctx = Arc::clone(&scope.ctx);
        self.on_thread(scope, move || pipeline.run_full(&ctx).map_err(RunFailure::from))
            .await
    }

    /// `_run_mapping_incremental(ctx, batch)`: the paths that failed are queued again.
    async fn run_mapping_incremental(
        &self,
        scope: &RunScope,
        batch: (BTreeSet<PathBuf>, BTreeSet<PathBuf>),
    ) -> Result<(), RunFailure> {
        let pipeline = (self.inner.mapping_factory)();
        let ctx = Arc::clone(&scope.ctx);
        let (changed, deleted): (Vec<PathBuf>, Vec<PathBuf>) =
            (batch.0.into_iter().collect(), batch.1.into_iter().collect());
        let (failed_changed, failed_deleted) = self
            .on_thread(scope, move || {
                pipeline
                    .run_incremental(&ctx, &changed, &deleted)
                    .map_err(RunFailure::from)
            })
            .await?;
        if !failed_changed.is_empty() || !failed_deleted.is_empty() {
            let requeued = failed_changed.len() + failed_deleted.len();
            scope
                .ctx
                .warning(&format!("incremental mapping failed for {requeued} paths, requeueing"));
            let mut watch = locked(&self.inner.watch);
            watch.changed.extend(failed_changed);
            watch.deleted.extend(failed_deleted);
            watch.last = Some(Instant::now());
        }
        Ok(())
    }

    // -- loops ----------------------------------------------------------------------

    /// `_wait_stop(seconds)`: sleep unless shutdown comes first; whether stopping.
    async fn wait_stop(&self, value: f64) -> bool {
        if value.is_nan() || value <= 0.0 {
            return self.stopped();
        }
        tokio::select! {
            () = self.inner.shutdown.cancelled() => {}
            () = tokio::time::sleep(seconds(value)) => {}
        }
        self.stopped()
    }

    /// `datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=value)`; `None` where
    /// Python raises `OverflowError`, which ends the loop.
    fn after(&self, value: f64) -> Option<DateTime<Utc>> {
        py_add(self.now().trunc_subsecs(0), timedelta(value)?).ok()
    }

    /// The rss and playlists loops: a run (when enabled and ready), then the rest of the
    /// interval.
    async fn interval_loop(self, pipeline: PipelineName) {
        while !self.stopped() {
            let configured = match pipeline {
                PipelineName::Playlists => self.inner.config.playlists().interval_seconds,
                _ => self.inner.config.rss().interval_seconds,
            };
            #[expect(clippy::cast_precision_loss, reason = "float(int), as Python's arithmetic")]
            let interval = configured.max(60) as f64;
            let started = Instant::now();
            self.set_next(pipeline, None);
            if self.pipeline_enabled(pipeline) && self.ready(pipeline).is_none() {
                self.execute_scheduled(pipeline, RunTrigger::Scheduled, None).await;
            }
            let delay = (interval - started.elapsed().as_secs_f64()).max(0.0);
            let Some(next) = self.after(delay) else {
                tracing::error!(
                    logger = LOGGER,
                    "the {pipeline} loop stopped: the next run is out of range"
                );
                return;
            };
            self.set_next(pipeline, Some(next));
            if self.wait_stop(delay).await {
                break;
            }
        }
    }

    fn set_next(&self, pipeline: PipelineName, at: Option<DateTime<Utc>>) {
        let mut next = self.next();
        match pipeline {
            PipelineName::Rss => next.rss = at,
            PipelineName::Archive => next.archive = at,
            PipelineName::Mapping => next.full_sync = at,
            PipelineName::Playlists => next.playlists = at,
        }
    }

    async fn rss_loop(self) {
        self.interval_loop(PipelineName::Rss).await;
    }

    async fn playlists_loop(self) {
        self.interval_loop(PipelineName::Playlists).await;
    }

    /// `_archive_loop()`: run the fallback scan whenever `archive.scan_cron` next fires. The
    /// expression is re-read every pass, so an edit moves the next fire without a restart;
    /// an occurrence is computed strictly after now, so a fire is never replayed after the
    /// run it triggered.
    async fn archive_loop(self) {
        let mut cron: Option<String> = None;
        while !self.stopped() {
            let configured = self.inner.config.archive().scan_cron;
            if cron.as_deref() != Some(configured.as_str()) {
                self.set_next(PipelineName::Archive, next_cron_fire(&configured, self.now()));
                cron = Some(configured);
            }
            let due = self.next().archive.is_some_and(|at| self.now() >= at);
            if due {
                if self.pipeline_enabled(PipelineName::Archive) && self.ready(PipelineName::Archive).is_none() {
                    self.execute_scheduled(PipelineName::Archive, RunTrigger::Scheduled, None)
                        .await;
                }
                let expr = cron.as_deref().unwrap_or_default();
                self.set_next(PipelineName::Archive, next_cron_fire(expr, self.now()));
            }
            if self.wait_stop(self.inner.timings.cron_poll).await {
                break;
            }
        }
    }

    /// `_tracker_loop()`: poll CloudDrive for the acquisitions in flight. A service loop
    /// rather than a pipeline run: it wakes on the configured interval (and would swamp the
    /// run history). A fresh submission pulls extra polls at the fast-check offsets.
    async fn tracker_loop(self) {
        let Some(poll) = self.inner.tracker_poll.clone() else {
            return;
        };
        while !self.stopped() {
            #[expect(clippy::cast_precision_loss, reason = "float(int), as Python's arithmetic")]
            let interval = self.inner.config.archive().tracker_interval_seconds as f64;
            let ready = self.inner.tracker_ready.as_ref().is_none_or(|ready| ready().is_none());
            if ready {
                let ctx = Arc::new(RunContext::new("embyx-manager.tracker"));
                let scope = RunScope {
                    ctx: Arc::clone(&ctx),
                    shutdown: self.inner.shutdown.clone(),
                };
                match poll(scope).await {
                    Err(RunFailure::Cancelled | RunFailure::Shutdown) => return,
                    Err(RunFailure::Raised(error)) => {
                        tracing::error!(logger = LOGGER, error = %error, "acquisition tracker poll failed");
                        self.record_tracker(Some(error.to_string()), Counters::new());
                    }
                    Ok(()) => self.record_tracker(None, ctx.stats()),
                }
            }
            let regular_at = Instant::now().checked_add(seconds(interval));
            if self.wait_next_tracker_poll(regular_at).await {
                return;
            }
        }
    }

    /// `TrackerState.record(error=, stats=)`.
    fn record_tracker(&self, error: Option<String>, stats: Counters) {
        let mut tracker = locked(&self.inner.tracker_state);
        tracker.last_polled_at = Some(self.now().trunc_subsecs(0));
        tracker.last_error = error;
        tracker.last_stats = stats;
    }

    /// `_wait_next_tracker_poll(regular_at)`: sleep until the next poll is due; whether
    /// stopping instead. The regular deadline stands however often the sleep is
    /// interrupted; a submission only pulls the wake-up forward to its first fast check.
    /// `None` is a deadline past what the clock holds (never).
    async fn wait_next_tracker_poll(&self, regular_at: Option<Instant>) -> bool {
        let offsets: Vec<Duration> = self
            .inner
            .timings
            .tracker_fast_checks
            .iter()
            .map(|value| seconds(*value))
            .collect();
        let submissions = &self.inner.submissions;
        while !self.stopped() {
            submissions.event.clear();
            let due = submissions.pending(&offsets).into_iter().chain(regular_at).min();
            let wait = due.map(|due| due.saturating_duration_since(Instant::now()));
            if self.wait_stop_or_submission(wait).await {
                return true;
            }
            if !submissions.event.is_set() {
                return false;
            }
        }
        true
    }

    /// `_wait_stop_or_submission(seconds)`: wait unless a submission or shutdown cuts it
    /// short; whether stopping. `None` waits without a deadline.
    async fn wait_stop_or_submission(&self, wait: Option<Duration>) -> bool {
        if wait == Some(Duration::ZERO) {
            return self.stopped();
        }
        let sleep = async {
            match wait {
                Some(wait) => tokio::time::sleep(wait).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            () = self.inner.shutdown.cancelled() => {}
            () = self.inner.submissions.event.wait() => {}
            () = sleep => {}
        }
        self.stopped()
    }

    /// `_config_refresh_loop()`: converge on config changes written by other replicas.
    async fn config_refresh_loop(self) {
        while !self.wait_stop(self.inner.timings.config_refresh).await {
            if let Err(error) = self.inner.config.refresh().await {
                tracing::error!(logger = LOGGER, error = %error, "config refresh failed");
            }
        }
    }

    /// `_mapping_loop()`: the startup full sync (retried until it succeeds or mapping is
    /// disabled), then periodic full syncs and debounced incremental batches.
    async fn mapping_loop(self) {
        let timings = self.inner.timings.clone();
        while !self.stopped() {
            self.sync_observer();
            if !self.mapping_runnable() {
                if self.wait_stop(timings.startup_retry).await {
                    return;
                }
                continue;
            }
            let state = self
                .execute_scheduled(PipelineName::Mapping, RunTrigger::Startup, None)
                .await;
            if state == Some(RunState::Completed) {
                break;
            }
            if self.wait_stop(timings.full_sync_retry).await {
                return;
            }
        }

        if !self.schedule_next_full_sync(false) {
            return;
        }
        while !self.stopped() {
            self.sync_observer();
            if !self.mapping_runnable() {
                self.set_next(PipelineName::Mapping, None);
                if self.wait_stop(timings.startup_retry).await {
                    return;
                }
                if !self.schedule_next_full_sync(false) {
                    return;
                }
                continue;
            }

            let due = self.next().full_sync.is_some_and(|at| self.now() >= at);
            if due {
                let events_before = locked(&self.inner.watch).count;
                let state = self
                    .execute_scheduled(PipelineName::Mapping, RunTrigger::Scheduled, None)
                    .await;
                {
                    let mut watch = locked(&self.inner.watch);
                    if state == Some(RunState::Completed) && watch.count == events_before {
                        // Nothing changed during the sync: pending events are covered by it.
                        watch.changed.clear();
                        watch.deleted.clear();
                    }
                }
                if !self.schedule_next_full_sync(state != Some(RunState::Completed)) {
                    return;
                }
                continue;
            }

            if let Some(batch) = self.take_debounced_batch() {
                self.execute_scheduled(PipelineName::Mapping, RunTrigger::Watchdog, Some(batch))
                    .await;
                continue;
            }

            if self.wait_stop(timings.idle_poll).await {
                return;
            }
        }
    }

    /// `_take_debounced_batch()`: the pending paths once no event came for the debounce.
    fn take_debounced_batch(&self) -> Option<(BTreeSet<PathBuf>, BTreeSet<PathBuf>)> {
        let debounce = self.inner.config.mapping().debounce_seconds;
        let mut watch = locked(&self.inner.watch);
        if watch.changed.is_empty() && watch.deleted.is_empty() {
            return None;
        }
        // Python's `monotonic() - _last_fs_event < debounce`, with no event yet long ago.
        let quiet = watch.last.map_or(f64::INFINITY, |last| last.elapsed().as_secs_f64());
        if quiet < debounce {
            return None;
        }
        Some((std::mem::take(&mut watch.changed), std::mem::take(&mut watch.deleted)))
    }

    /// `_schedule_next_full_sync(retry=)`; `false` where Python raises `OverflowError`
    /// (the loop ends).
    fn schedule_next_full_sync(&self, retry: bool) -> bool {
        #[expect(clippy::cast_precision_loss, reason = "float(int), as Python's arithmetic")]
        let configured = self.inner.config.mapping().full_sync_interval_seconds as f64;
        let interval = if retry {
            self.inner.timings.full_sync_retry
        } else {
            configured
        };
        let Some(at) = self.after(interval) else {
            tracing::error!(
                logger = LOGGER,
                "the mapping loop stopped: the next full sync is out of range"
            );
            return false;
        };
        self.set_next(PipelineName::Mapping, Some(at));
        true
    }

    // -- the .strm watcher ---------------------------------------------------------

    /// `_on_fs_event(path, deleted)`.
    pub fn on_fs_event(&self, path: impl Into<PathBuf>, deleted: bool) {
        locked(&self.inner.watch).record(path.into(), deleted);
    }

    /// `_sync_observer()`: start, restart or stop the watcher to match the configuration.
    fn sync_observer(&self) {
        let config = self.inner.config.mapping();
        let wanted =
            (config.enabled && config.configured() && Path::new(&config.src_dir).is_dir()).then_some(config.src_dir);
        let observed = locked(&self.inner.observer)
            .as_ref()
            .map(|observer| observer.directory.clone());
        if wanted == observed {
            return;
        }
        self.stop_observer();
        let Some(wanted) = wanted else {
            return;
        };
        let watch = Arc::clone(&self.inner.watch);
        let handler = move |event: notify::Result<notify::Event>| {
            if let Ok(event) = event {
                let forwarded = watch::translate(event.kind, &event.paths);
                if !forwarded.is_empty() {
                    let mut state = locked(&watch);
                    for (path, deleted) in forwarded {
                        state.record(path, deleted);
                    }
                }
            }
        };
        let started = notify::recommended_watcher(handler).and_then(|mut watcher| {
            watcher.watch(Path::new(&wanted), notify::RecursiveMode::Recursive)?;
            Ok(watcher)
        });
        match started {
            Ok(watcher) => {
                tracing::info!(logger = LOGGER, "watching {wanted} for .strm changes");
                *locked(&self.inner.observer) = Some(Observer {
                    _watcher: watcher,
                    directory: wanted,
                });
            }
            Err(error) => {
                tracing::error!(logger = LOGGER, error = %error, "failed to watch {wanted}");
            }
        }
    }

    /// `_stop_observer()`.
    fn stop_observer(&self) {
        // Dropping the watcher stops its thread.
        locked(&self.inner.observer).take();
    }

    /// The directory the watcher watches, if any.
    pub fn observed_dir(&self) -> Option<String> {
        locked(&self.inner.observer)
            .as_ref()
            .map(|observer| observer.directory.clone())
    }

    // -- helpers ------------------------------------------------------------------

    fn pipeline_enabled(&self, pipeline: PipelineName) -> bool {
        let config = &self.inner.config;
        match pipeline {
            PipelineName::Rss => config.rss().enabled,
            PipelineName::Archive => config.archive().enabled,
            PipelineName::Playlists => config.playlists().enabled,
            PipelineName::Mapping => config.mapping().enabled,
        }
    }

    fn mapping_runnable(&self) -> bool {
        self.pipeline_enabled(PipelineName::Mapping) && self.ready(PipelineName::Mapping).is_none()
    }
}

#[cfg(test)]
mod tests;
