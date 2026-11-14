//! The fill-actor service: scan an actor's catalog against the library, plan the moves, apply
//! them with a journal, and recover what a crash or a restart left half done.
//!
//! Python origin: `src/embyx_manager/fill_actor/service.py` (`FillActorService`,
//! `FillActorPaths`, `FillActorRuntime`, `static_runtime`). Every check, error code, journal
//! step and progress event is Python's, in Python's order, because the journal rows and the
//! library files are what the other build resumes from (cross-version contracts 1, 2 and 6):
//!
//! - local moves write `prepared`, rename without replacing (or, where the file system refuses
//!   the flag, hard-link and park the source in a private quarantine), then `linked`,
//!   `source_removed`, the result and `reconciled`; a journal left at any state is finished or
//!   undone by [`FillActorService::reconcile_moves`] from what the paths hold ([`local`]);
//! - CloudDrive moves of `.strm`-mapped files record their operation (`prepared`, `submitting`,
//!   `verifying`, `unknown`) around `MoveFile` and only ever conclude from what the listings
//!   show ([`cloud`]).
//!
//! How the Rust build keeps Python's concurrency rules (DIVERGENCES.md D121-D123):
//!
//! - A move runs in a task of its own that the caller only joins (`asyncio.shield`): dropping
//!   [`FillActorService::apply`] leaves the move running to its end, a second `apply` of the same
//!   candidate joins it, and [`FillActorService::aclose`] waits for it.
//! - File work runs on blocking threads that cannot be abandoned; the mutation lock is held by
//!   the thread as well as by the future, so it is released only after the file system call
//!   returned, also when the future was dropped (`_await_mutation_future`).
//! - A scan's file work gets a stop flag that a dropped scan future sets; the thread finishes on
//!   its own and [`FillActorService::settle_scans`] (or `aclose`) waits for it
//!   (`_await_executor_future(cancellation_wins=True)`).
//! - Each CloudDrive execution (one candidate's move, one recovery pass) runs on its own
//!   [`CloudFileMover::scoped`] mover and awaits `settle()` before it lets go of the mutation
//!   lock (D116, contract 6).

use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use embyx_clients::net::retry::{Sleeper, tokio_sleeper};
use embyx_core::avid::AvidParser;
use embyx_core::pyre;
use embyx_core::pytext::py_strip;
use embyx_db::locking::{AsyncFileLock, FileLockGuard};
use embyx_monitor::reports::PyException as _;
use indexmap::IndexMap;
use tokio::sync::{Semaphore, watch};
use tokio_util::task::TaskTracker;

use crate::cloud_moves::{BoxFuture, CloudFileMetadata, CloudFileMover, CloudMovePaths};
use crate::errors::{FillActorError, FillActorErrorKind};
use crate::fs::{self, LocalMoveFs, MoveFs, PyOsError};
use crate::models::{
    ActorPlan, ApplyResult, ApplyState, FillActorPlan, MoveCandidate, MoveResult, MoveState, VideoPlan, VideoState,
};
use crate::ports::{AcquisitionGateway, AcquisitionOutcome, ActorCatalog, BrandResolver, PageProgressCallback};
use crate::postgres_repository::RepositoryError;
use crate::pydatetime::PyDateTime;
use crate::pystr;
use crate::records::{
    CandidateKind, CandidateRecord, JobProgressEvent, JobProgressUnit, JobStage, MoveJournalRecord, MoveJournalState,
    PlanRecord,
};

mod cloud;
pub(crate) mod local;
pub mod redact;
pub mod store;

#[cfg(test)]
mod tests;

pub use redact::{MAX_ERROR_MESSAGE_LENGTH, redact_error_message};
pub use store::FillActorRepository;

use local::{Local, ScanError};

/// The Python logger the process log names (D60).
pub const LOGGER: &str = "embyx_manager.fill_actor.service";

/// `CLOUD_VERIFY_ATTEMPTS`.
pub const CLOUD_VERIFY_ATTEMPTS: usize = 5;
/// `CLOUD_MOVE_OBSERVATION_GRACE`: how long an unresolved cloud move is observed before an
/// untouched source and an absent destination are read as "the move never happened".
pub const CLOUD_MOVE_OBSERVATION_GRACE: TimeDelta = TimeDelta::minutes(15);
/// `CLOUD_HEALTH_SUCCESS_TTL_SECONDS`.
pub const CLOUD_HEALTH_SUCCESS_TTL: std::time::Duration = std::time::Duration::from_secs(30);
/// `CLOUD_HEALTH_FAILURE_TTL_SECONDS`.
pub const CLOUD_HEALTH_FAILURE_TTL: std::time::Duration = std::time::Duration::from_secs(5);

/// `ProgressCallback`: `await progress(event)`. It cannot fail: WP12's reporter logs and
/// swallows its own errors, as Python's does (D124).
pub type ProgressCallback = Arc<dyn Fn(JobProgressEvent) -> BoxFuture<'static, ()> + Send + Sync>;
/// `StopCallback`.
pub type StopCallback = Arc<dyn Fn() -> bool + Send + Sync>;
/// The settings provider (`runtime: Callable[[], FillActorRuntime]`).
pub type RuntimeProvider = Arc<dyn Fn() -> Arc<FillActorRuntime> + Send + Sync>;
/// `clock: Callable[[], datetime]`.
pub type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;
/// `token_factory: Callable[[], str]`.
pub type TokenFactory = Arc<dyn Fn() -> String + Send + Sync>;
/// `on_moved: Callable[[], None]`; an `Err` is what it raised (logged, never propagated).
pub type OnMoved = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

/// `FillActorPaths`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FillActorPaths {
    pub actor_brand_path: PathBuf,
    pub additional_brand_paths: Vec<PathBuf>,
    pub move_in_path: PathBuf,
}

impl FillActorPaths {
    /// `FillActorPaths.from_iterable(...)`.
    pub fn from_iterable(
        actor_brand_path: impl Into<PathBuf>,
        additional_brand_paths: impl IntoIterator<Item = PathBuf>,
        move_in_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            actor_brand_path: actor_brand_path.into(),
            additional_brand_paths: additional_brand_paths.into_iter().collect(),
            move_in_path: move_in_path.into(),
        }
    }
}

/// `FillActorRuntime`: a coherent view of the settings a scan or move must agree on. `version`
/// stamps the plans a snapshot produced: plans from an older version are refused rather than
/// applied against paths that have moved.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FillActorRuntime {
    pub version: i32,
    pub paths: Option<FillActorPaths>,
    pub root_sentinel: Option<String>,
    pub move_in_by_brand: bool,
    pub apply_enabled: bool,
    pub cloud_move_paths: Option<CloudMovePaths>,
}

impl FillActorRuntime {
    /// `.configured`.
    pub fn configured(&self) -> bool {
        self.paths.is_some()
    }
}

/// `static_runtime(...)`: a provider for a deployment whose settings never change while it
/// runs (version 0).
pub fn static_runtime(runtime: FillActorRuntime) -> RuntimeProvider {
    let runtime = Arc::new(runtime);
    Arc::new(move || Arc::clone(&runtime))
}

/// An exception the service lets through to its caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceError {
    /// A `FillActorError` the API maps to its code.
    FillActor(FillActorError),
    /// Any other exception: its class and `str(exc)`.
    Raised { class: String, message: String },
}

impl ServiceError {
    fn fill_actor(kind: FillActorErrorKind, message: impl Into<String>) -> Self {
        Self::FillActor(FillActorError::new(kind, message))
    }

    /// `type(exc).__name__`.
    pub fn class_name(&self) -> &str {
        match self {
            Self::FillActor(error) => error.class_name(),
            Self::Raised { class, .. } => class,
        }
    }

    /// The `FillActorError`, if this is one.
    pub fn as_fill_actor(&self) -> Option<&FillActorError> {
        match self {
            Self::FillActor(error) => Some(error),
            Self::Raised { .. } => None,
        }
    }
}

impl std::fmt::Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FillActor(error) => error.fmt(f),
            Self::Raised { message, .. } => f.write_str(message),
        }
    }
}

impl std::error::Error for ServiceError {}

impl From<FillActorError> for ServiceError {
    fn from(error: FillActorError) -> Self {
        Self::FillActor(error)
    }
}

impl From<RepositoryError> for ServiceError {
    fn from(error: RepositoryError) -> Self {
        Self::Raised {
            class: error.class_name().to_owned(),
            message: error.to_string(),
        }
    }
}

impl From<PyOsError> for ServiceError {
    fn from(error: PyOsError) -> Self {
        Self::Raised {
            class: error.class_name().to_owned(),
            message: error.to_string(),
        }
    }
}

/// A constructor argument Python refuses with `ValueError`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct InvalidOption(pub &'static str);

/// What the service is built from (the required keyword arguments).
pub struct ServiceParts {
    pub runtime: RuntimeProvider,
    pub actor_catalog: Arc<dyn ActorCatalog>,
    pub acquisition_gateway: Arc<dyn AcquisitionGateway>,
    pub brand_resolver: Arc<dyn BrandResolver>,
    /// Required here: the in-memory fallback is not ported (D110).
    pub repository: Arc<dyn FillActorRepository>,
}

impl std::fmt::Debug for ServiceParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceParts").finish_non_exhaustive()
    }
}

/// The optional keyword arguments, with Python's defaults.
pub struct ServiceOptions {
    pub max_actors: usize,
    pub max_videos: usize,
    pub magnet_concurrency: usize,
    pub plan_ttl: TimeDelta,
    pub clock: Option<Clock>,
    pub token_factory: Option<TokenFactory>,
    pub mutation_lock: Option<AsyncFileLock>,
    pub cloud_file_mover: Option<Arc<dyn CloudFileMover>>,
    pub on_moved: Option<OnMoved>,
    /// The file system (the real one by default).
    pub fs: Arc<dyn MoveFs>,
    /// The pause between two verification rounds of a cloud move (`asyncio.sleep(0.5)`).
    pub sleeper: Sleeper,
}

impl std::fmt::Debug for ServiceOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceOptions")
            .field("max_actors", &self.max_actors)
            .field("max_videos", &self.max_videos)
            .field("magnet_concurrency", &self.magnet_concurrency)
            .field("plan_ttl", &self.plan_ttl)
            .finish_non_exhaustive()
    }
}

impl Default for ServiceOptions {
    fn default() -> Self {
        Self {
            max_actors: 20,
            max_videos: 2_000,
            magnet_concurrency: 8,
            plan_ttl: TimeDelta::hours(1),
            clock: None,
            token_factory: None,
            mutation_lock: None,
            cloud_file_mover: None,
            on_moved: None,
            fs: Arc::new(LocalMoveFs),
            sleeper: tokio_sleeper(),
        }
    }
}

/// How an in-flight move ended: its result, its error, or the panic that ended its task.
type Outcome = Result<Result<MoveResult, ServiceError>, String>;
/// `(plan_id, candidate_id)`.
type MoveKey = (String, String);

struct Inner {
    runtime: RuntimeProvider,
    catalog: Arc<dyn ActorCatalog>,
    gateway: Arc<dyn AcquisitionGateway>,
    brands: Arc<dyn BrandResolver>,
    repository: Arc<dyn FillActorRepository>,
    max_actors: usize,
    max_videos: usize,
    magnet_concurrency: usize,
    magnet_semaphore: Arc<Semaphore>,
    plan_ttl: TimeDelta,
    clock: Clock,
    token_factory: TokenFactory,
    mutation_lock: Option<AsyncFileLock>,
    mover: Option<Arc<dyn CloudFileMover>>,
    on_moved: Option<OnMoved>,
    fs: Arc<dyn MoveFs>,
    sleeper: Sleeper,
    /// `_in_flight`: the running move of each `(plan_id, candidate_id)`.
    in_flight: Mutex<HashMap<MoveKey, watch::Receiver<Option<Outcome>>>>,
    moves: TaskTracker,
    /// `_mutation_futures`.
    mutations: TaskTracker,
    /// `_scan_futures`: each running scan's stop flag.
    scans: Mutex<HashMap<u64, Arc<AtomicBool>>>,
    scan_tasks: TaskTracker,
    next_scan: AtomicU64,
    apply_lock: tokio::sync::Mutex<()>,
    cloud_health: tokio::sync::Mutex<Option<(tokio::time::Instant, bool)>>,
}

/// `FillActorService`. Cheap to clone; clones share everything.
#[derive(Clone)]
pub struct FillActorService {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for FillActorService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FillActorService").finish_non_exhaustive()
    }
}

/// `secrets.token_urlsafe(18)`: 18 random bytes, URL-safe base64 without padding.
fn token_urlsafe() -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut bytes = [0_u8; 18];
    getrandom::fill(&mut bytes).expect("the operating system's random source");
    let mut out = String::with_capacity(24);
    for chunk in bytes.chunks(3) {
        let value = (u32::from(chunk[0]) << 16) | (u32::from(chunk[1]) << 8) | u32::from(chunk[2]);
        for shift in [18, 12, 6, 0] {
            out.push(char::from(ALPHABET[((value >> shift) & 0x3f) as usize]));
        }
    }
    out
}

/// `_OUTCOME_STATES[outcome]`.
fn outcome_state(outcome: AcquisitionOutcome) -> VideoState {
    match outcome {
        AcquisitionOutcome::Queued => VideoState::Queued,
        AcquisitionOutcome::Submitted => VideoState::Submitted,
        AcquisitionOutcome::AlreadyTracked => VideoState::AlreadyTracked,
        AcquisitionOutcome::NoMagnet => VideoState::Missing,
        AcquisitionOutcome::SubmitFailed
        | AcquisitionOutcome::CloudNotConfigured
        | AcquisitionOutcome::TaskDirNotConfigured => VideoState::SubmitFailed,
    }
}

/// `_OUTCOME_WARNINGS.get(outcome)`: failed outcomes keep their name as the row warning.
fn outcome_warning(outcome: AcquisitionOutcome) -> Option<&'static str> {
    match outcome {
        AcquisitionOutcome::SubmitFailed => Some("submit_failed"),
        AcquisitionOutcome::CloudNotConfigured => Some("cloud_not_configured"),
        AcquisitionOutcome::TaskDirNotConfigured => Some("task_dir_not_configured"),
        _ => None,
    }
}

/// `ACTOR_REF_RE`: an AVBase name (any alias) or a JavBus star id; any short text without
/// path separators or control characters.
fn actor_ref_matches(actor_id: &str) -> bool {
    static PATTERN: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| pyre::compile(r"^[^\x00-\x1f/\\]{1,64}$", false, false));
    PATTERN.is_match(actor_id)
}

/// `_normalize_video_id(video_id)`: the id as the ledger keys it, JavBus's date suffix
/// dropped, then the parser's spelling.
pub fn normalize_video_id(video_id: &str) -> String {
    static DATED: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| pyre::compile(r"^(.+)_\d{4}-\d{2}-\d{2}$", false, false));
    static PARSER: std::sync::LazyLock<AvidParser> = std::sync::LazyLock::new(AvidParser::default);
    let mut cleaned = py_strip(video_id);
    if let Some(captures) = DATED.captures(cleaned) {
        cleaned = captures.get(1).map_or(cleaned, |group| group.as_str());
    }
    let avid = PARSER.get_avid(cleaned);
    if avid.is_empty() { cleaned.to_uppercase() } else { avid }
}

/// One video's scan: its public plan, its private candidates, whether it needs acquiring,
/// and the names in it that are not UTF-8 (in the order `model_dump_json` meets them).
struct VideoScan {
    plan: VideoPlan,
    records: Vec<CandidateRecord>,
    needs_acquisition: bool,
    unencodable: Vec<OsString>,
}

impl VideoScan {
    fn only(plan: VideoPlan, needs_acquisition: bool) -> Self {
        Self {
            plan,
            records: Vec::new(),
            needs_acquisition,
            unencodable: Vec::new(),
        }
    }
}

fn video_plan(video_id: &str, actor_ids: &[String], state: VideoState) -> VideoPlan {
    VideoPlan {
        video_id: video_id.to_owned(),
        actor_ids: actor_ids.to_vec(),
        state,
        release_date: None,
        existing_files: Vec::new(),
        move_candidates: Vec::new(),
        warnings: Vec::new(),
    }
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

/// `PydanticSerializationError` for a plan holding a name that is not UTF-8.
fn unencodable_plan_error(name: &OsString) -> ServiceError {
    use std::os::unix::ffi::OsStrExt as _;
    let chars = pystr::surrogateescape(name.as_bytes());
    let what = pystr::encode_error_what(&chars).unwrap_or_default();
    ServiceError::Raised {
        class: "PydanticSerializationError".to_owned(),
        message: format!(
            "Error serializing to JSON: UnicodeEncodeError: 'utf-8' codec can't encode {what}: surrogates not allowed"
        ),
    }
}

/// `name` as the public plan holds it; a name that is not UTF-8 is noted so the plan is refused
/// where Python's `model_dump_json` refuses it.
fn public_name(name: &std::ffi::OsStr, unencodable: &mut Vec<OsString>) -> String {
    if let Some(text) = name.to_str() {
        return text.to_owned();
    }
    unencodable.push(name.to_owned());
    name.to_string_lossy().into_owned()
}

/// Marks the scan's stop flag when the scan future is dropped (`on_cancel=stop_requested.set`).
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Re-raise a panic that ended a blocking call or a move task (a crash point in a test).
fn resume(message: String) -> ! {
    std::panic::resume_unwind(Box::new(message))
}

fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|text| (*text).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a fill-actor task panicked".to_owned())
}

fn join_error_text(error: tokio::task::JoinError) -> String {
    match error.try_into_panic() {
        Ok(payload) => panic_text(&*payload),
        Err(error) => error.to_string(),
    }
}

/// The mutation lock as held: by the future that took it and by every blocking call it starts.
type HeldLock = Option<Arc<FileLockGuard>>;

impl FillActorService {
    /// `FillActorService(...)`: `ValueError` for a non-positive limit or TTL.
    pub fn new(parts: ServiceParts, options: ServiceOptions) -> Result<Self, InvalidOption> {
        if options.max_actors < 1 {
            return Err(InvalidOption("max_actors must be positive"));
        }
        if options.magnet_concurrency < 1 {
            return Err(InvalidOption("magnet_concurrency must be positive"));
        }
        if options.max_videos < 1 {
            return Err(InvalidOption("max_videos must be positive"));
        }
        if options.plan_ttl <= TimeDelta::zero() {
            return Err(InvalidOption("plan_ttl must be positive"));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                runtime: parts.runtime,
                catalog: parts.actor_catalog,
                gateway: parts.acquisition_gateway,
                brands: parts.brand_resolver,
                repository: parts.repository,
                max_actors: options.max_actors,
                max_videos: options.max_videos,
                magnet_concurrency: options.magnet_concurrency,
                magnet_semaphore: Arc::new(Semaphore::new(options.magnet_concurrency)),
                plan_ttl: options.plan_ttl,
                clock: options.clock.unwrap_or_else(|| Arc::new(Utc::now)),
                token_factory: options.token_factory.unwrap_or_else(|| Arc::new(token_urlsafe)),
                mutation_lock: options.mutation_lock,
                mover: options.cloud_file_mover,
                on_moved: options.on_moved,
                fs: options.fs,
                sleeper: options.sleeper,
                in_flight: Mutex::new(HashMap::new()),
                moves: TaskTracker::new(),
                mutations: TaskTracker::new(),
                scans: Mutex::new(HashMap::new()),
                scan_tasks: TaskTracker::new(),
                next_scan: AtomicU64::new(0),
                apply_lock: tokio::sync::Mutex::new(()),
                cloud_health: tokio::sync::Mutex::new(None),
            }),
        })
    }

    // Settings are read through the provider rather than frozen at construction, so saving the
    // Fill Actor section takes effect without a restart.
    fn runtime(&self) -> Arc<FillActorRuntime> {
        (self.inner.runtime)()
    }

    fn token(&self) -> String {
        (self.inner.token_factory)()
    }

    /// `_now()`.
    fn now(&self) -> DateTime<Utc> {
        (self.inner.clock)()
    }

    fn fs(&self) -> &dyn MoveFs {
        &*self.inner.fs
    }

    /// `_cloud_mode`: cloud moves run only when a mover exists *and* the runtime maps API paths.
    fn cloud_mode(&self, runtime: &FillActorRuntime) -> bool {
        self.inner.mover.is_some() && runtime.cloud_move_paths.is_some()
    }

    /// `.apply_enabled`.
    pub fn apply_enabled(&self) -> bool {
        self.runtime().apply_enabled
    }

    /// `.configured`: false until library roots are saved; the feature is idle rather than
    /// broken.
    pub fn configured(&self) -> bool {
        self.runtime().configured()
    }

    /// `validate_actor_ids(actor_ids)`.
    pub fn validate_actor_ids(&self, actor_ids: &[String]) -> Result<Vec<String>, FillActorError> {
        let mut seen = HashSet::new();
        let normalized: Vec<String> = actor_ids
            .iter()
            .map(|actor_id| py_strip(actor_id).to_owned())
            .filter(|actor_id| seen.insert(actor_id.clone()))
            .collect();
        if normalized.is_empty() {
            return Err(FillActorError::new(FillActorErrorKind::InvalidActorId, ""));
        }
        if normalized.len() > self.inner.max_actors {
            return Err(FillActorError::new(
                FillActorErrorKind::TooManyActors,
                normalized.len().to_string(),
            ));
        }
        if let Some(bad) = normalized.iter().find(|actor_id| !actor_ref_matches(actor_id)) {
            return Err(FillActorError::new(FillActorErrorKind::InvalidActorId, bad.clone()));
        }
        Ok(normalized)
    }

    async fn report(progress: Option<&ProgressCallback>, event: JobProgressEvent) {
        if let Some(progress) = progress {
            progress(event).await;
        }
    }

    // ---------- scans ----------

    /// `_run_scan(function, *args)`: the file work on a blocking thread with a stop flag. A
    /// dropped future sets the flag and returns at once; the thread finishes on its own and
    /// [`Self::settle_scans`] waits for it.
    pub(crate) async fn run_scan<T: Send + 'static>(&self, work: impl FnOnce(&AtomicBool) -> T + Send + 'static) -> T {
        let stop = Arc::new(AtomicBool::new(false));
        let id = self.inner.next_scan.fetch_add(1, Ordering::SeqCst);
        self.inner
            .scans
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, Arc::clone(&stop));
        let _stop_on_drop = StopOnDrop(Arc::clone(&stop));
        let inner = Arc::clone(&self.inner);
        let handle = self.inner.scan_tasks.spawn_blocking(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work(&stop)));
            inner.scans.lock().unwrap_or_else(PoisonError::into_inner).remove(&id);
            match result {
                Ok(value) => value,
                Err(payload) => std::panic::resume_unwind(payload),
            }
        });
        match handle.await {
            Ok(value) => value,
            Err(error) => resume(join_error_text(error)),
        }
    }

    /// How many scan threads are still running (`len(service._scan_futures)`).
    pub fn scan_workers(&self) -> usize {
        self.inner.scans.lock().unwrap_or_else(PoisonError::into_inner).len()
    }

    /// Ask every running scan to stop and wait until each thread has finished: what a caller
    /// that cancelled a scan awaits before it acts on the outcome (WP12's job cancel).
    pub async fn settle_scans(&self) {
        for stop in self.inner.scans.lock().unwrap_or_else(PoisonError::into_inner).values() {
            stop.store(true, Ordering::SeqCst);
        }
        let tracker = &self.inner.scan_tasks;
        tracker.close();
        tracker.wait().await;
        tracker.reopen();
    }

    /// `_run_mutation(function, *args)`: file work that must not be abandoned. The thread holds
    /// the mutation lock too, so a dropped caller releases it only after the work returned.
    async fn run_mutation<T: Send + 'static>(&self, lock: &HeldLock, work: impl FnOnce() -> T + Send + 'static) -> T {
        let held = lock.clone();
        let handle = self.inner.mutations.spawn_blocking(move || {
            let _held = held;
            work()
        });
        match handle.await {
            Ok(value) => value,
            Err(error) => resume(join_error_text(error)),
        }
    }

    async fn acquire_lock(&self) -> Result<HeldLock, ServiceError> {
        match &self.inner.mutation_lock {
            None => Ok(None),
            Some(lock) => lock
                .acquire()
                .await
                .map(|guard| Some(Arc::new(guard)))
                .map_err(|error| ServiceError::from(PyOsError::from_io(&error, Some(lock.path()), None))),
        }
    }

    // ---------- create_plan ----------

    /// `create_plan(actor_ids, plan_id=..., revision=..., progress=...)`.
    #[allow(clippy::too_many_lines, reason = "Python's method, step for step")]
    pub async fn create_plan(
        &self,
        actor_ids: &[String],
        plan_id: Option<&str>,
        revision: Option<&str>,
        progress: Option<ProgressCallback>,
    ) -> Result<FillActorPlan, ServiceError> {
        // Stamped now and re-checked before saving, so a plan is always the product of
        // exactly one configuration.
        let config_version = self.runtime().version;
        let actor_ids = self.validate_actor_ids(actor_ids)?;
        let actor_total = i64::try_from(actor_ids.len()).unwrap_or(i64::MAX);
        let mut actor_plans = Vec::new();
        let mut video_actors: IndexMap<String, Vec<String>> = IndexMap::new();
        let mut release_dates: HashMap<String, NaiveDate> = HashMap::new();
        let progress = progress.as_ref();

        Self::report(
            progress,
            JobProgressEvent {
                stage: JobStage::ActorCatalog,
                completed: 0,
                total: Some(actor_total),
                unit: JobProgressUnit::Actors,
                current: Some(format!("演员 1/{actor_total}")),
            },
        )
        .await;

        for (index, actor_id) in actor_ids.iter().enumerate() {
            let actor_index = index + 1;
            let report_page: Option<Box<PageProgressCallback>> = progress.map(|progress| {
                let progress = Arc::clone(progress);
                let actor_id = actor_id.clone();
                let callback: Box<PageProgressCallback> = Box::new(
                    move |page_completed: u64, page_total: Option<u64>, current_page: Option<u64>| {
                        let page = match (current_page, page_total) {
                            (Some(current), Some(total)) => format!("页面 {current}/{total}"),
                            (Some(current), None) => format!("页面 {current}"),
                            (None, Some(total)) => format!("已发现 {total} 页"),
                            (None, None) if page_completed > 0 => format!("已完成 {page_completed} 页"),
                            (None, None) => "正在发现页面".to_owned(),
                        };
                        let event = JobProgressEvent {
                            stage: JobStage::ActorCatalog,
                            completed: i64::try_from(page_completed).unwrap_or(i64::MAX),
                            total: page_total.map(|total| i64::try_from(total).unwrap_or(i64::MAX)),
                            unit: JobProgressUnit::Pages,
                            current: Some(format!("演员 {actor_index}/{actor_total} · {actor_id} · {page}")),
                        };
                        progress(event)
                    },
                );
                callback
            });
            match self.inner.catalog.list_videos(actor_id, report_page.as_deref()).await {
                Err(error) => {
                    // The traceback only ever reaches the log; the plan carries a redacted
                    // one-liner. Job-backed scans always pass plan_id, so job_id + actor_id
                    // locates the original exception.
                    let job_id = plan_id.filter(|id| !id.is_empty()).unwrap_or("unassigned");
                    tracing::error!(
                        logger = LOGGER,
                        job_id,
                        actor_id = actor_id.as_str(),
                        error_type = error.class.as_str(),
                        error = format!("{}: {}", error.class, error.message),
                        "fill-actor actor catalog failed job_id={job_id} actor_id={actor_id} error_type={}",
                        error.class
                    );
                    actor_plans.push(ActorPlan {
                        actor_id: actor_id.clone(),
                        scraped_count: 0,
                        video_ids: Vec::new(),
                        error_code: Some("actor_catalog_error".to_owned()),
                        actor_name: None,
                        talent_id: None,
                        aliases: Vec::new(),
                        error_message: redact_error_message(&error.class, &error.message),
                    });
                }
                Ok(listing) => {
                    let raw: HashSet<&String> = listing.video_ids.iter().collect();
                    let mut video_ids: Vec<String> = listing
                        .video_ids
                        .iter()
                        .filter(|video_id| !py_strip(video_id).is_empty())
                        .map(|video_id| normalize_video_id(video_id))
                        .collect::<HashSet<_>>()
                        .into_iter()
                        .collect();
                    video_ids.sort();
                    actor_plans.push(ActorPlan {
                        actor_id: actor_id.clone(),
                        scraped_count: i64::try_from(raw.len()).unwrap_or(i64::MAX),
                        video_ids: video_ids.clone(),
                        actor_name: listing.actor_name.clone(),
                        talent_id: listing.talent_id.map(|id| i64::try_from(id).unwrap_or(i64::MAX)),
                        aliases: listing.aliases.clone(),
                        error_code: None,
                        error_message: None,
                    });
                    for video_id in video_ids {
                        let actors = video_actors.entry(video_id).or_default();
                        if !actors.contains(actor_id) {
                            actors.push(actor_id.clone());
                        }
                    }
                    for (raw_video_id, release_date) in &listing.release_dates {
                        release_dates
                            .entry(normalize_video_id(raw_video_id))
                            .or_insert(*release_date);
                    }
                    if video_actors.len() > self.inner.max_videos {
                        return Err(ServiceError::fill_actor(
                            FillActorErrorKind::TooManyVideos,
                            video_actors.len().to_string(),
                        ));
                    }
                }
            }
            Self::report(
                progress,
                JobProgressEvent {
                    stage: JobStage::ActorCatalog,
                    completed: i64::try_from(actor_index).unwrap_or(i64::MAX),
                    total: Some(actor_total),
                    unit: JobProgressUnit::Actors,
                    current: Some(format!("演员 {actor_index}/{actor_total} · {actor_id}")),
                },
            )
            .await;
        }

        let mut public_videos: HashMap<String, VideoPlan> = HashMap::new();
        let mut unencodable: HashMap<String, Vec<OsString>> = HashMap::new();
        let mut records: IndexMap<String, CandidateRecord> = IndexMap::new();
        let mut cloud_cache: HashMap<String, Option<Vec<CloudFileMetadata>>> = HashMap::new();
        let mut missing: Vec<String> = Vec::new();
        let mut sorted_videos: Vec<&String> = video_actors.keys().collect();
        sorted_videos.sort();
        let video_total = i64::try_from(sorted_videos.len()).unwrap_or(i64::MAX);

        Self::report(
            progress,
            JobProgressEvent {
                stage: JobStage::LibraryScan,
                completed: 0,
                total: Some(video_total),
                unit: JobProgressUnit::Videos,
                current: None,
            },
        )
        .await;

        for (index, video_id) in sorted_videos.iter().enumerate() {
            let mut membership = video_actors[*video_id].clone();
            membership.sort();
            match self.create_video_plan(video_id, &membership, &mut cloud_cache).await {
                Err(_) => {
                    let mut plan = video_plan(video_id, &membership, VideoState::ScanFailed);
                    plan.warnings = strings(&["scan_failed"]);
                    public_videos.insert((*video_id).clone(), plan);
                }
                Ok(scan) => {
                    let mut plan = scan.plan;
                    plan.release_date = release_dates.get(*video_id).copied();
                    public_videos.insert((*video_id).clone(), plan);
                    for record in scan.records {
                        records.insert(record.candidate_id.clone(), record);
                    }
                    unencodable.insert((*video_id).clone(), scan.unencodable);
                    if scan.needs_acquisition {
                        missing.push((*video_id).clone());
                    }
                }
            }
            Self::report(
                progress,
                JobProgressEvent {
                    stage: JobStage::LibraryScan,
                    completed: i64::try_from(index + 1).unwrap_or(i64::MAX),
                    total: Some(video_total),
                    unit: JobProgressUnit::Videos,
                    current: Some((*video_id).clone()),
                },
            )
            .await;
        }

        mark_duplicate_destination_conflicts(&mut public_videos, &records);
        let missing_total = i64::try_from(missing.len()).unwrap_or(i64::MAX);
        Self::report(
            progress,
            JobProgressEvent {
                stage: JobStage::Submitting,
                completed: 0,
                total: Some(missing_total),
                unit: JobProgressUnit::Videos,
                current: None,
            },
        )
        .await;
        for (video_id, outcome, warning) in self.queue_missing(&missing, &release_dates, progress).await {
            if let Some(current) = public_videos.get_mut(&video_id) {
                current.state = outcome_state(outcome);
                current.warnings = warning.into_iter().map(str::to_owned).collect();
            }
        }

        let created_at = self.now();
        let plan_id = plan_id
            .filter(|id| !id.is_empty())
            .map_or_else(|| self.token(), str::to_owned);
        let revision = revision
            .filter(|id| !id.is_empty())
            .map_or_else(|| self.token(), str::to_owned);
        let expires_at =
            embyx_monitor::support::py_add(created_at, self.inner.plan_ttl).map_err(|error| ServiceError::Raised {
                class: "OverflowError".to_owned(),
                message: error.to_string(),
            })?;
        let mut video_order: Vec<String> = public_videos.keys().cloned().collect();
        video_order.sort();
        let plan = FillActorPlan {
            plan_id,
            revision,
            created_at: PyDateTime::utc(created_at),
            expires_at: PyDateTime::utc(expires_at),
            actors: actor_plans,
            videos: video_order
                .iter()
                .filter_map(|video_id| public_videos.remove(video_id))
                .collect(),
        };
        Self::report(
            progress,
            JobProgressEvent {
                stage: JobStage::Persisting,
                completed: 0,
                total: Some(1),
                unit: JobProgressUnit::Steps,
                current: Some("保存扫描结果".to_owned()),
            },
        )
        .await;
        if self.runtime().version != config_version {
            // Settings were saved while this scan ran; its results mix two configurations.
            return Err(ServiceError::fill_actor(FillActorErrorKind::LegacyPlan, ""));
        }
        if let Some(name) = video_order
            .iter()
            .find_map(|video_id| unencodable.get(video_id).and_then(|names| names.first()))
        {
            // `model_dump_json()` cannot encode a surrogate-escaped name (D125).
            return Err(unencodable_plan_error(name));
        }
        self.inner
            .repository
            .save_plan(&PlanRecord {
                public: plan.clone(),
                candidates: records.into_values().collect(),
                config_version,
            })
            .await?;
        Self::report(
            progress,
            JobProgressEvent {
                stage: JobStage::Persisting,
                completed: 1,
                total: Some(1),
                unit: JobProgressUnit::Steps,
                current: Some("扫描结果已保存".to_owned()),
            },
        )
        .await;
        Ok(plan)
    }

    /// `_create_video_plan(video_id, actor_membership, cloud_directory_cache=...)`. `Err` is
    /// whatever Python raises out of it (the video becomes `scan_failed`).
    async fn create_video_plan(
        &self,
        video_id: &str,
        membership: &[String],
        cloud_cache: &mut HashMap<String, Option<Vec<CloudFileMetadata>>>,
    ) -> Result<VideoScan, ScanError> {
        let brand = self.inner.brands.resolve_brand(video_id);
        let Some(brand) = brand.filter(|brand| local::is_safe_segment(brand)) else {
            let mut plan = video_plan(video_id, membership, VideoState::InvalidVideoId);
            plan.warnings = strings(&["brand_not_found"]);
            return Ok(VideoScan::only(plan, false));
        };
        let runtime = self.runtime();
        let Some(paths) = runtime.paths.clone() else {
            return Err(ScanError::Value("fill actor is not configured"));
        };

        let existing = {
            let fs = Arc::clone(&self.inner.fs);
            let (root, brand, video_id) = (paths.actor_brand_path.clone(), brand.clone(), video_id.to_owned());
            self.run_scan(move |stop| {
                local::find_matching_files(&*fs, &root, &brand, &video_id, &|| stop.load(Ordering::SeqCst))
            })
            .await?
        };
        if !existing.is_empty() {
            let mut unencodable = Vec::new();
            let mut plan = video_plan(video_id, membership, VideoState::Exists);
            plan.existing_files = existing
                .iter()
                .map(|path| public_name(fs::name(path), &mut unencodable))
                .collect();
            return Ok(VideoScan {
                plan,
                records: Vec::new(),
                needs_acquisition: false,
                unencodable,
            });
        }

        let additional = {
            let fs = Arc::clone(&self.inner.fs);
            let (roots, brand, video_id) = (paths.additional_brand_paths.clone(), brand.clone(), video_id.to_owned());
            self.run_scan(move |stop| find_additional_files(&*fs, &roots, &brand, &video_id, stop))
                .await?
        };
        if additional.is_empty() {
            return Ok(VideoScan::only(
                video_plan(video_id, membership, VideoState::Missing),
                true,
            ));
        }

        if self.cloud_mode(&runtime) {
            return self
                .create_cloud_video_plan(&runtime, &paths, video_id, membership, &brand, &additional, cloud_cache)
                .await;
        }

        let fs = self.fs();
        let mut candidates = Vec::new();
        let mut records = Vec::new();
        let mut unencodable = Vec::new();
        for (source_index, source) in &additional {
            let candidate_id = self.token();
            let destination_root = if runtime.move_in_by_brand {
                paths.move_in_path.join(&brand)
            } else {
                paths.move_in_path.clone()
            };
            let destination = destination_root.join(fs::name(source));
            let source_root = paths.additional_brand_paths[*source_index].clone();
            records.push(CandidateRecord::local(
                candidate_id.clone(),
                video_id,
                source.clone(),
                source_root.clone(),
                destination.clone(),
                local::fingerprint(fs, source)?,
            ));
            candidates.push(MoveCandidate {
                candidate_id,
                video_id: video_id.to_owned(),
                file_name: public_name(fs::name(source), &mut unencodable),
                source_label: embyx_monitor::fs::path_text(&source_root),
                destination_conflict: fs::exists(fs, &destination)?,
            });
        }
        let mut plan = video_plan(video_id, membership, VideoState::AdditionalFound);
        plan.move_candidates = candidates;
        Ok(VideoScan {
            plan,
            records,
            needs_acquisition: false,
            unencodable,
        })
    }

    /// `_queue_missing(video_ids, release_dates, progress=...)`: hand every missing video to the
    /// acquisition ledger, `magnet_concurrency` at a time; results in the videos' order.
    async fn queue_missing(
        &self,
        video_ids: &[String],
        release_dates: &HashMap<String, NaiveDate>,
        progress: Option<&ProgressCallback>,
    ) -> Vec<(String, AcquisitionOutcome, Option<&'static str>)> {
        let queue = Mutex::new(video_ids.iter().collect::<std::collections::VecDeque<_>>());
        let results: Mutex<HashMap<String, (AcquisitionOutcome, Option<&'static str>)>> = Mutex::new(HashMap::new());
        let completed = tokio::sync::Mutex::new(0_i64);
        let total = i64::try_from(video_ids.len()).unwrap_or(i64::MAX);
        let worker = || async {
            loop {
                let Some(video_id) = queue.lock().unwrap_or_else(PoisonError::into_inner).pop_front() else {
                    return;
                };
                let submitted = {
                    let _permit = self.inner.magnet_semaphore.acquire().await;
                    match self
                        .inner
                        .gateway
                        .queue_missing(video_id, release_dates.get(video_id).copied())
                        .await
                    {
                        Err(_) => (AcquisitionOutcome::SubmitFailed, Some("acquisition_failed")),
                        Ok(outcome) => (outcome, outcome_warning(outcome)),
                    }
                };
                results
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(video_id.clone(), submitted);
                let mut done = completed.lock().await;
                *done += 1;
                Self::report(
                    progress,
                    JobProgressEvent {
                        stage: JobStage::Submitting,
                        completed: *done,
                        total: Some(total),
                        unit: JobProgressUnit::Videos,
                        current: Some(video_id.clone()),
                    },
                )
                .await;
            }
        };
        let workers = self.inner.magnet_concurrency.min(video_ids.len());
        futures_util::future::join_all((0..workers).map(|_| worker())).await;
        let results = results.into_inner().unwrap_or_else(PoisonError::into_inner);
        video_ids
            .iter()
            .filter_map(|video_id| {
                results
                    .get(video_id)
                    .map(|(outcome, warning)| (video_id.clone(), *outcome, *warning))
            })
            .collect()
    }

    // ---------- plans ----------

    /// `get_plan(plan_id)`.
    pub async fn get_plan(&self, plan_id: &str) -> Result<FillActorPlan, ServiceError> {
        Ok(self.get_plan_record(plan_id, None).await?.public)
    }

    /// `_get_plan(plan_id, revision)`.
    async fn get_plan_record(&self, plan_id: &str, revision: Option<&str>) -> Result<PlanRecord, ServiceError> {
        let Some(stored) = self.inner.repository.get_plan(plan_id).await? else {
            return Err(ServiceError::fill_actor(FillActorErrorKind::UnknownPlan, plan_id));
        };
        if let Some(revision) = revision
            && stored.public.revision != revision
        {
            return Err(ServiceError::fill_actor(FillActorErrorKind::RevisionMismatch, revision));
        }
        if stored.config_version != self.runtime().version {
            // The library roots moved under this plan; its destinations no longer mean what
            // they meant when it was scanned, so it must not be applied.
            return Err(ServiceError::fill_actor(FillActorErrorKind::LegacyPlan, ""));
        }
        if self.now() >= stored.public.expires_at.to_utc() {
            return Err(ServiceError::fill_actor(FillActorErrorKind::ExpiredPlan, plan_id));
        }
        Ok(stored)
    }

    // ---------- apply ----------

    /// `apply(plan_id=..., revision=..., candidate_ids=..., progress=..., stop_requested=...)`.
    #[allow(clippy::too_many_lines, reason = "Python's method, step for step")]
    pub async fn apply(
        &self,
        plan_id: &str,
        revision: &str,
        candidate_ids: &[String],
        progress: Option<ProgressCallback>,
        stop_requested: Option<StopCallback>,
    ) -> Result<ApplyResult, ServiceError> {
        if !self.runtime().apply_enabled {
            return Err(ServiceError::fill_actor(FillActorErrorKind::MoveDisabled, ""));
        }
        let progress = progress.as_ref();
        let results = {
            let _apply = self.inner.apply_lock.lock().await;
            let stored = self.get_plan_record(plan_id, Some(revision)).await?;
            let mut seen = HashSet::new();
            let selected: Vec<&String> = candidate_ids.iter().filter(|id| seen.insert(id.as_str())).collect();
            let candidates: HashMap<&str, &CandidateRecord> = stored
                .candidates
                .iter()
                .map(|candidate| (candidate.candidate_id.as_str(), candidate))
                .collect();
            if let Some(unknown) = selected.iter().find(|id| !candidates.contains_key(id.as_str())) {
                return Err(ServiceError::fill_actor(
                    FillActorErrorKind::UnknownCandidate,
                    (*unknown).clone(),
                ));
            }
            if self.cloud_mode(&self.runtime())
                && selected
                    .iter()
                    .any(|id| candidates[id.as_str()].kind != CandidateKind::CloudStrm)
            {
                return Err(ServiceError::fill_actor(FillActorErrorKind::LegacyPlan, ""));
            }
            let total = i64::try_from(selected.len()).unwrap_or(i64::MAX);
            let file_name = |index: usize| {
                selected
                    .get(index)
                    .map(|id| candidate_file_name(candidates[id.as_str()]))
            };
            Self::report(
                progress,
                JobProgressEvent {
                    stage: JobStage::Unknown,
                    completed: 0,
                    total: Some(total),
                    unit: JobProgressUnit::Items,
                    current: file_name(0),
                },
            )
            .await;
            let mut results = Vec::new();
            for (index, candidate_id) in selected.iter().enumerate() {
                if stop_requested.as_ref().is_some_and(|stop| stop()) {
                    break;
                }
                if let Some(cached) = self.inner.repository.get_move_result(plan_id, candidate_id).await? {
                    results.push(cached);
                } else {
                    let record = candidates[candidate_id.as_str()].clone();
                    let outcome = self.join_move(plan_id, record).await;
                    match outcome {
                        Ok(Ok(result)) => results.push(result),
                        Ok(Err(error)) => return Err(error),
                        Err(panic) => resume(panic),
                    }
                }
                let completed = index + 1;
                Self::report(
                    progress,
                    JobProgressEvent {
                        stage: JobStage::Unknown,
                        completed: i64::try_from(completed).unwrap_or(i64::MAX),
                        total: Some(total),
                        unit: JobProgressUnit::Items,
                        current: file_name(completed),
                    },
                )
                .await;
            }
            results
        };

        if let Some(on_moved) = &self.inner.on_moved
            && results.iter().any(|result| result.state == MoveState::Moved)
        {
            // Files are waiting in the staging tree; whoever archives them wants to know now.
            if let Err(error) = on_moved() {
                tracing::error!(logger = LOGGER, error, "the moved-files notification failed");
            }
        }
        Ok(ApplyResult {
            plan_id: plan_id.to_owned(),
            revision: revision.to_owned(),
            state: apply_state(&results),
            results,
        })
    }

    /// The running move of a candidate, started if none is (`asyncio.shield(worker)`): the move
    /// runs in its own task, so a dropped caller leaves it running.
    async fn join_move(&self, plan_id: &str, record: CandidateRecord) -> Outcome {
        let key = (plan_id.to_owned(), record.candidate_id.clone());
        let mut receiver = {
            let mut in_flight = self.inner.in_flight.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(receiver) = in_flight.get(&key) {
                receiver.clone()
            } else {
                let (sender, receiver) = watch::channel(None);
                in_flight.insert(key.clone(), receiver.clone());
                let service = self.clone();
                let plan_id = plan_id.to_owned();
                self.inner.moves.spawn(async move {
                    let worker = {
                        let service = service.clone();
                        tokio::spawn(async move { service.run_move(&plan_id, &record).await })
                    };
                    let outcome = worker.await.map_err(join_error_text);
                    // `_discard_in_flight`, before the result is seen.
                    service
                        .inner
                        .in_flight
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .remove(&key);
                    sender.send_replace(Some(outcome));
                });
                receiver
            }
        };
        match receiver.wait_for(Option::is_some).await {
            Ok(outcome) => outcome
                .clone()
                .unwrap_or_else(|| Err("the move ended without an outcome".to_owned())),
            Err(_) => Err("the move ended without an outcome".to_owned()),
        }
    }

    /// `_run_move(plan_id, record)`.
    async fn run_move(&self, plan_id: &str, record: &CandidateRecord) -> Result<MoveResult, ServiceError> {
        if record.kind == CandidateKind::CloudStrm {
            return self.run_cloud_move(plan_id, record).await;
        }
        if !self.candidate_roots_ready(record).await? {
            return Ok(root_unavailable(record));
        }
        let outcome = async {
            let lock = self.acquire_lock().await?;
            if !self.candidate_roots_ready(record).await? {
                return Ok(root_unavailable(record));
            }
            if let Some(cached) = self
                .inner
                .repository
                .get_move_result(plan_id, &record.candidate_id)
                .await?
            {
                return Ok(cached);
            }
            let journal = self
                .inner
                .repository
                .get_move_journal(plan_id, &record.candidate_id)
                .await?;
            if let Some(journal) = journal.filter(|journal| journal.state != MoveJournalState::Reconciled) {
                return self.reconcile_candidate(plan_id, record, &journal, &lock).await;
            }
            self.save_journal(plan_id, &record.candidate_id, MoveJournalState::Prepared)
                .await?;
            let move_in = self.move_in_path()?;
            let result = {
                let (fs, record) = (Arc::clone(&self.inner.fs), record.clone());
                self.run_mutation(&lock, move || {
                    Local {
                        fs: &*fs,
                        move_in: &move_in,
                    }
                    .apply_one(&record)
                })
                .await?
            };
            if result.state != MoveState::Moved && !self.candidate_roots_ready(record).await? {
                return Ok(result);
            }
            if result.error_code.as_deref() == Some("move_rollback_failed")
                || result
                    .error_code
                    .as_deref()
                    .is_some_and(|code| code.starts_with("reconcile_quarantine_"))
            {
                return Ok(result);
            }
            self.persist_move_completion(plan_id, record, &result, MoveJournalState::Prepared)
                .await?;
            Ok::<_, ServiceError>(result)
        }
        .await;
        // `except Exception`: a structured failure, never an escaped error.
        Ok(outcome.unwrap_or_else(|_| unexpected_move_failure(record)))
    }

    fn move_in_path(&self) -> Result<PathBuf, ServiceError> {
        self.runtime()
            .paths
            .as_ref()
            .map(|paths| paths.move_in_path.clone())
            .ok_or_else(|| ServiceError::fill_actor(FillActorErrorKind::NotConfigured, ""))
    }

    /// `_reconcile_candidate(plan_id, record, journal)`.
    async fn reconcile_candidate(
        &self,
        plan_id: &str,
        record: &CandidateRecord,
        journal: &MoveJournalRecord,
        lock: &HeldLock,
    ) -> Result<MoveResult, ServiceError> {
        if !self.candidate_roots_ready(record).await? {
            return Ok(root_unavailable(record));
        }
        if let Some(cached) = self
            .inner
            .repository
            .get_move_result(plan_id, &record.candidate_id)
            .await?
        {
            self.save_journal(plan_id, &record.candidate_id, MoveJournalState::Reconciled)
                .await?;
            return Ok(cached);
        }
        let move_in = self.move_in_path()?;
        let result = {
            let (fs, record, state) = (Arc::clone(&self.inner.fs), record.clone(), journal.state);
            self.run_mutation(lock, move || {
                Local {
                    fs: &*fs,
                    move_in: &move_in,
                }
                .reconcile_filesystem(&record, state)
            })
            .await?
        };
        if result.state != MoveState::Moved && !self.candidate_roots_ready(record).await? {
            return Ok(result);
        }
        if matches!(
            result.error_code.as_deref(),
            Some("reconcile_rollback_conflict" | "reconcile_rollback_failed")
        ) || result
            .error_code
            .as_deref()
            .is_some_and(|code| code.starts_with("reconcile_quarantine_"))
        {
            return Ok(result);
        }
        self.persist_move_completion(plan_id, record, &result, journal.state)
            .await?;
        Ok(result)
    }

    /// `_persist_move_completion(plan_id, record, result, journal_state)`.
    async fn persist_move_completion(
        &self,
        plan_id: &str,
        record: &CandidateRecord,
        result: &MoveResult,
        mut journal_state: MoveJournalState,
    ) -> Result<(), ServiceError> {
        if result.state == MoveState::Moved {
            if journal_state == MoveJournalState::Prepared {
                self.save_journal(plan_id, &record.candidate_id, MoveJournalState::Linked)
                    .await?;
                journal_state = MoveJournalState::Linked;
            }
            if journal_state == MoveJournalState::Linked {
                self.save_journal(plan_id, &record.candidate_id, MoveJournalState::SourceRemoved)
                    .await?;
            }
        }
        self.inner.repository.save_move_result(plan_id, result).await?;
        self.save_journal(plan_id, &record.candidate_id, MoveJournalState::Reconciled)
            .await?;
        Ok(())
    }

    /// `_save_journal(plan_id, candidate_id, state)`.
    async fn save_journal(
        &self,
        plan_id: &str,
        candidate_id: &str,
        state: MoveJournalState,
    ) -> Result<(), ServiceError> {
        self.inner
            .repository
            .save_move_journal(&MoveJournalRecord {
                plan_id: plan_id.to_owned(),
                candidate_id: candidate_id.to_owned(),
                state,
                updated_at: self.now(),
            })
            .await?;
        Ok(())
    }

    // ---------- recovery ----------

    /// `reconcile_moves()`: finish what a crash or a restart left half done. Local journals in
    /// local mode, CloudDrive operations in cloud mode.
    pub async fn reconcile_moves(&self) -> Result<Vec<MoveResult>, ServiceError> {
        let runtime = self.runtime();
        if !runtime.apply_enabled {
            return Ok(Vec::new());
        }
        if !self.roots_ready().await? {
            return Ok(Vec::new());
        }
        if self.cloud_mode(&self.runtime()) {
            return self.reconcile_cloud_moves().await;
        }
        let mut results = Vec::new();
        for journal in self.inner.repository.list_unreconciled_moves().await? {
            let Some(record) = self
                .inner
                .repository
                .get_candidate(&journal.plan_id, &journal.candidate_id)
                .await?
            else {
                continue;
            };
            let lock = self.acquire_lock().await?;
            let current = self
                .inner
                .repository
                .get_move_journal(&journal.plan_id, &journal.candidate_id)
                .await?;
            let Some(current) = current.filter(|current| current.state != MoveJournalState::Reconciled) else {
                continue;
            };
            results.push(
                self.reconcile_candidate(&journal.plan_id, &record, &current, &lock)
                    .await?,
            );
            drop(lock);
        }
        Ok(results)
    }

    // ---------- readiness ----------

    /// `ready()`.
    pub async fn ready(&self) -> Result<bool, ServiceError> {
        self.scan_ready().await
    }

    /// `scan_ready()`.
    pub async fn scan_ready(&self) -> Result<bool, ServiceError> {
        Ok(self.roots_ready().await? && self.cloud_ready().await)
    }

    /// `apply_ready()`.
    pub async fn apply_ready(&self) -> Result<bool, ServiceError> {
        Ok(self.scan_ready().await? && self.legacy_journal_ready().await?)
    }

    /// `cloud_ready()`: every configured CloudDrive root can be listed (cached 30 s after a
    /// success, 5 s after a failure).
    pub async fn cloud_ready(&self) -> bool {
        let runtime = self.runtime();
        let Some(mover) = &self.inner.mover else {
            // A deployment without a mover must not claim to be ready for cloud moves.
            return runtime.cloud_move_paths.is_none();
        };
        let Some(paths) = runtime.cloud_move_paths.clone() else {
            return true;
        };
        let mut cache = self.inner.cloud_health.lock().await;
        let now = tokio::time::Instant::now();
        if let Some((until, result)) = *cache
            && now < until
        {
            return result;
        }
        let mut roots: Vec<String> = paths
            .source_api_roots
            .iter()
            .map(|root| root.as_str().to_owned())
            .collect();
        roots.push(paths.move_in_api_root.as_str().to_owned());
        let mut result = true;
        for root in &roots {
            if mover.list_directory(root).await.is_err() {
                result = false;
                break;
            }
        }
        let ttl = if result {
            CLOUD_HEALTH_SUCCESS_TTL
        } else {
            CLOUD_HEALTH_FAILURE_TTL
        };
        *cache = Some((now + ttl, result));
        result
    }

    /// `legacy_journal_ready()`: in cloud mode, no local journal may be left unreconciled.
    pub async fn legacy_journal_ready(&self) -> Result<bool, ServiceError> {
        if !self.cloud_mode(&self.runtime()) {
            return Ok(true);
        }
        Ok(self.inner.repository.list_unreconciled_moves().await?.is_empty())
    }

    /// `roots_ready()`: every root is a real directory (with the sentinel file, when one is
    /// configured) and the additional roots share the move-in root's device.
    pub async fn roots_ready(&self) -> Result<bool, ServiceError> {
        let runtime = self.runtime();
        let Some(paths) = runtime.paths.clone() else {
            // Nothing configured yet is "not ready", not an error.
            return Ok(false);
        };
        let sentinel = runtime.root_sentinel.clone();
        let fs = Arc::clone(&self.inner.fs);
        let check = move || -> Result<bool, PyOsError> {
            let fs = &*fs;
            let mut roots = vec![paths.actor_brand_path.clone()];
            roots.extend(paths.additional_brand_paths.iter().cloned());
            roots.push(paths.move_in_path.clone());
            for root in &roots {
                if !fs::is_dir(fs, root)? || fs::is_symlink(fs, root)? {
                    return Ok(false);
                }
            }
            if let Some(sentinel) = &sentinel {
                for root in &roots {
                    let marker = root.join(sentinel);
                    if !fs::is_file(fs, &marker)? || fs::is_symlink(fs, &marker)? {
                        return Ok(false);
                    }
                }
            }
            let Ok(destination) = fs.stat(&paths.move_in_path, true) else {
                return Ok(false);
            };
            for root in &paths.additional_brand_paths {
                match fs.stat(root, true) {
                    Ok(stat) if stat.device == destination.device => {}
                    _ => return Ok(false),
                }
            }
            Ok(true)
        };
        Ok(to_thread(check).await?)
    }

    /// `_candidate_roots_ready(record)`: both roots of a candidate are mounted, carry the
    /// sentinel, and are on the device the candidate was fingerprinted on.
    async fn candidate_roots_ready(&self, record: &CandidateRecord) -> Result<bool, ServiceError> {
        let runtime = self.runtime();
        let Some(paths) = runtime.paths.clone() else {
            return Err(ServiceError::fill_actor(FillActorErrorKind::NotConfigured, ""));
        };
        let sentinel = runtime.root_sentinel.clone();
        let fs = Arc::clone(&self.inner.fs);
        let (source_root, device) = (record.source_root.clone(), record.fingerprint.device);
        let check = move || -> Result<bool, PyOsError> {
            let fs = &*fs;
            let move_in = &paths.move_in_path;
            let (Ok(source_stat), Ok(destination_stat)) = (fs.stat(&source_root, true), fs.stat(move_in, true)) else {
                return Ok(false);
            };
            if !fs::is_dir(fs, &source_root)?
                || fs::is_symlink(fs, &source_root)?
                || !fs::is_dir(fs, move_in)?
                || fs::is_symlink(fs, move_in)?
            {
                return Ok(false);
            }
            if let Some(sentinel) = &sentinel {
                let (source_marker, destination_marker) = (source_root.join(sentinel), move_in.join(sentinel));
                if !fs::is_file(fs, &source_marker)?
                    || fs::is_symlink(fs, &source_marker)?
                    || !fs::is_file(fs, &destination_marker)?
                    || fs::is_symlink(fs, &destination_marker)?
                {
                    return Ok(false);
                }
            }
            Ok(source_stat.device == device && destination_stat.device == device)
        };
        Ok(to_thread(check).await?)
    }

    // ---------- shutdown ----------

    /// `aclose()`: stop the scans and wait for them, then wait for every file call and every
    /// running move (with its CloudDrive calls) to end.
    pub async fn aclose(&self) {
        self.settle_scans().await;
        for tracker in [&self.inner.mutations, &self.inner.moves] {
            tracker.close();
            tracker.wait().await;
            tracker.reopen();
        }
        // A move's last blocking call can be started after the mutation tracker was waited on.
        let mutations = &self.inner.mutations;
        mutations.close();
        mutations.wait().await;
        mutations.reopen();
    }
}

/// `asyncio.to_thread(check)`, the result raised as it was.
async fn to_thread<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    match tokio::task::spawn_blocking(work).await {
        Ok(value) => value,
        Err(error) => resume(join_error_text(error)),
    }
}

/// `_find_additional_files(brand, video_id, stop_requested)`: the matching files of every
/// additional root, by root then name.
fn find_additional_files(
    fs: &dyn MoveFs,
    roots: &[PathBuf],
    brand: &str,
    video_id: &str,
    stop: &AtomicBool,
) -> Result<Vec<(usize, PathBuf)>, ScanError> {
    let mut result = Vec::new();
    for (index, root) in roots.iter().enumerate() {
        if stop.load(Ordering::SeqCst) {
            return Ok(Vec::new());
        }
        let matches = local::find_matching_files(fs, root, brand, video_id, &|| stop.load(Ordering::SeqCst))?;
        result.extend(matches.into_iter().map(|path| (index, path)));
    }
    result.sort_by_cached_key(|(index, path)| (*index, pystr::sort_key(fs::name(path))));
    Ok(result)
}

/// `_mark_duplicate_destination_conflicts(videos, records)`: two candidates bound for the same
/// destination both conflict. Keys are compared as bytes, never through a lossy decode.
fn mark_duplicate_destination_conflicts(
    videos: &mut HashMap<String, VideoPlan>,
    records: &IndexMap<String, CandidateRecord>,
) {
    let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();
    for record in records.values() {
        *counts.entry(destination_key(record)).or_default() += 1;
    }
    for video in videos.values_mut() {
        for candidate in &mut video.move_candidates {
            if let Some(record) = records.get(&candidate.candidate_id) {
                candidate.destination_conflict =
                    candidate.destination_conflict || counts.get(&destination_key(record)).copied().unwrap_or(0) > 1;
            }
        }
    }
}

/// `_candidate_destination_key(record)`: `str(destination)`, or for a CloudDrive candidate its
/// API destination.
fn destination_key(record: &CandidateRecord) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt as _;
    if record.kind == CandidateKind::CloudStrm
        && let (Some(directory), Some(file)) = (&record.cloud_destination_dir, &record.cloud_file)
    {
        return embyx_core::pypath::join(directory, &file.name).into_bytes();
    }
    crate::pystr::normalized_path_bytes(record.destination.as_os_str().as_bytes())
}

/// `_candidate_file_name(record)`: the name the progress shows.
fn candidate_file_name(record: &CandidateRecord) -> String {
    match &record.cloud_file {
        Some(file) => embyx_core::pypath::name(&file.name.replace('\\', "/")).to_owned(),
        None => fs::name(&record.source).to_string_lossy().into_owned(),
    }
}

/// `_get_apply_state(results)`.
fn apply_state(results: &[MoveResult]) -> ApplyState {
    if results.iter().all(|result| result.state == MoveState::Moved) {
        ApplyState::Succeeded
    } else if results.iter().any(|result| result.state == MoveState::Moved) {
        ApplyState::PartialFailed
    } else {
        ApplyState::Failed
    }
}

/// `_unexpected_move_failure(record)`.
fn unexpected_move_failure(record: &CandidateRecord) -> MoveResult {
    local::move_result(record, MoveState::Failed, Some("move_failed"))
}

/// `_root_unavailable(record)`.
fn root_unavailable(record: &CandidateRecord) -> MoveResult {
    local::move_result(record, MoveState::Failed, Some("roots_unavailable"))
}

/// `str(PurePosixPath(path).parent)` for a normalised path.
pub(crate) fn posix_parent(path: &str) -> String {
    let path = embyx_core::pypath::PyPath::new(path);
    let text = path.as_str();
    let anchor = if text.starts_with("//") {
        "//"
    } else if text.starts_with('/') {
        "/"
    } else {
        ""
    };
    let body = &text[anchor.len()..];
    match body.rfind('/') {
        Some(at) => format!("{anchor}{}", &body[..at]),
        None if anchor.is_empty() => ".".to_owned(),
        None => anchor.to_owned(),
    }
}
