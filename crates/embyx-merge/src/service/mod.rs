//! Driving merge tasks from the queue to the library.
//!
//! Python origin: `src/embyx_manager/merge/service.py`. Each task walks one path, one step
//! per poll, and every step can be repeated after a restart without harm:
//!
//! 1. **queued → merging**: start the title's merge Job, one Job at a time. A title the
//!    library already holds whole, next to its parts, is refused.
//! 2. **merging → uploading**: the Job wrote the merged file and its SHA-1 to the work volume.
//! 3. **uploading → verifying**: CloudDrive copied it into a staging folder of its own under
//!    the 115 staging directory. A copy that failed is restarted after a rest; a merged file
//!    that went missing before the copy finished is merged again.
//! 4. **verifying → replacing**: the staged file has the merged file's size and SHA-1, and the
//!    library still holds no whole copy of the title.
//! 5. **replacing → archiving**: the original parts are deleted (into 115's recycle bin), the
//!    merged file moves into `{intake}/{source}/{AVID}/` and the local copy is removed.
//! 6. **archiving → done**: the archive pipeline filed the folder away.
//!
//! A step that cannot go on fails the task and records which state it failed in, so a retry
//! resumes there. A passing problem is shown as a notice while the step is retried.
//!
//! Every statement, name, path and text is Python's, so a task left in any state by one build
//! is continued by the other (cross-version contracts 3 and 4). The poll loop runs each tick
//! on a [`MergeCloud::scoped`] handle; when the service is closed in the middle of a tick, the
//! tick is dropped and that handle settled, so a CloudDrive mutation it sent ends first
//! (contract 6), as Python's cancelled task waits for the RPC's thread.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration as StdDuration;

use chrono::Duration;
use embyx_clients::clouddrive::{CloudDriveError, CopyStatus, CopyTask};
use embyx_clients::kube::{KubeJobsError, job_failure, job_name, job_outcome};
use embyx_clients::pyvalue::{self, Raised};
use embyx_core::config::models::{ArchiveConfig, CloudDriveConfig, MergeConfig};
use embyx_core::pyjson::Value;
use embyx_core::pypath;
use embyx_db::database::DatabaseError;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::detect::whole_copies;
use crate::ports::{Clock, MergeCloud, MergeKube, Pipeline, Trigger, system_clock};
use crate::tasks::{MergeState, MergeTask, MergeTaskRepository, TaskError, TaskField};
use crate::transcode::{TranscodeRepository, WorkerPresence};
use crate::worker::{partial_path, read_status, staging_dir};

mod actions;
mod home;
pub(crate) mod pyops;

pub use self::actions::ActionError;
pub use self::home::{ChunkRefusedError, WorkerCallError};

/// `LOGGER`: the module logger's name.
pub(crate) const LOGGER: &str = "embyx_manager.merge.service";

/// `POLL_SECONDS`.
pub const POLL_SECONDS: f64 = 10.0;
/// `MAX_UPLOAD_ATTEMPTS`.
pub const MAX_UPLOAD_ATTEMPTS: i32 = 5;
/// `HASH_WAIT`: 115 computes a fresh upload's SHA-1 shortly after it lands; wait this long.
pub fn hash_wait() -> Duration {
    Duration::minutes(30)
}
/// `STAGED_FILE_WAIT`: CloudDrive's listing can trail a finished copy by a little.
pub fn staged_file_wait() -> Duration {
    Duration::minutes(10)
}
/// `ARCHIVE_NUDGE_INTERVAL`: how often an unfiled title asks the archive for another pass.
pub fn archive_nudge_interval() -> Duration {
    Duration::minutes(5)
}
/// `COPY_LISTING_GRACE`: how long a copy CloudDrive accepted may take to show up among its
/// copy tasks.
pub fn copy_listing_grace() -> Duration {
    Duration::minutes(2)
}
/// `ARCHIVE_WAIT`: after this long without the archive filing the title, the task fails.
pub fn archive_wait() -> Duration {
    Duration::hours(2)
}
/// `PERMANENT_UPLOAD_ERRORS`: CloudDrive's words for an upload 115 will never take.
pub const PERMANENT_UPLOAD_ERRORS: [&str; 2] = ["code: 10025", "单文件不能大于"];
/// `UPLOAD_RETRY_DELAYS`, in minutes: the rest before the next upload attempt, by the
/// attempts made so far (cross-version contract 4: 5 m, 15 m, 1 h, 3 h).
pub const UPLOAD_RETRY_DELAY_MINUTES: [i64; 4] = [5, 15, 60, 180];
/// `SHA1_HASH_KEY`.
pub const SHA1_HASH_KEY: &str = "2";
/// `NO_JOB_TEMPLATE`.
pub const NO_JOB_TEMPLATE: &str = "the merge Job template is not mounted (EMBYX_MANAGER_MERGE_JOB_TEMPLATE)";
/// `WAITING_FOR_WORKER`.
pub const WAITING_FOR_WORKER: &str = "waiting for the home transcode worker";
/// `WAITING_FOR_SLOT`.
pub const WAITING_FOR_SLOT: &str = "every part is re-encoded; waiting for the running merge Job to finish";

/// `HOME_RETRY_STATES`: where a home transcode may go after a failure, by the state it
/// failed in.
pub fn is_home_retry_state(state: Option<MergeState>) -> bool {
    matches!(state, Some(MergeState::Transcoding | MergeState::Merging))
}

/// `upload_retry_delay(attempts)`: none after a retry the operator asked for.
pub fn upload_retry_delay(attempts: i32) -> Duration {
    if attempts <= 0 {
        return Duration::zero();
    }
    let index = usize::try_from(attempts)
        .unwrap_or(usize::MAX)
        .min(UPLOAD_RETRY_DELAY_MINUTES.len())
        - 1;
    Duration::minutes(UPLOAD_RETRY_DELAY_MINUTES[index])
}

/// `retry_notice(attempts, delay, error)`.
pub fn retry_notice(attempts: i32, delay: Duration, error: &str) -> String {
    let minutes = delay.num_seconds().div_euclid(60);
    let wait = if minutes % 60 == 0 {
        format!("{} h", minutes / 60)
    } else {
        format!("{minutes} min")
    };
    format!(
        "upload attempt {} of {MAX_UPLOAD_ATTEMPTS} starts in {wait}: {error}",
        i64::from(attempts) + 1
    )
}

/// Why a step stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepError {
    /// `MergeFailedError`: the task cannot go on; the message is shown to the operator.
    Failed(String),
    /// Any other exception: its class and `str(exc)`.
    Raised(Raised),
}

impl StepError {
    pub(crate) fn failed(message: impl Into<String>) -> Self {
        Self::Failed(message.into())
    }

    pub(crate) fn raised(class: &'static str, message: impl Into<String>) -> Self {
        Self::Raised(Raised::new(class, message))
    }

    /// `RuntimeError(...)`.
    pub(crate) fn runtime(message: impl Into<String>) -> Self {
        Self::raised("RuntimeError", message)
    }

    /// `str(exc) or type(exc).__name__`: what a step's notice says.
    pub fn notice(&self) -> String {
        match self {
            Self::Failed(message) => {
                if message.is_empty() {
                    "MergeFailedError".to_owned()
                } else {
                    message.clone()
                }
            }
            Self::Raised(raised) => {
                if raised.message.is_empty() {
                    raised.class.to_owned()
                } else {
                    raised.message.clone()
                }
            }
        }
    }

    /// `type(exc).__name__`.
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Failed(_) => "MergeFailedError",
            Self::Raised(raised) => raised.class,
        }
    }
}

impl std::fmt::Display for StepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(message) => f.write_str(message),
            Self::Raised(raised) => f.write_str(&raised.message),
        }
    }
}

impl std::error::Error for StepError {}

impl From<Raised> for StepError {
    fn from(raised: Raised) -> Self {
        Self::Raised(raised)
    }
}

impl From<CloudDriveError> for StepError {
    fn from(error: CloudDriveError) -> Self {
        Self::raised(error.class_name(), error.to_string())
    }
}

impl From<KubeJobsError> for StepError {
    fn from(error: KubeJobsError) -> Self {
        Self::raised(error.class_name(), error.to_string())
    }
}

impl From<TaskError> for StepError {
    fn from(error: TaskError) -> Self {
        match error {
            TaskError::Conflict(message) => Self::raised("MergeTaskConflictError", message),
            TaskError::Corrupt(message) => Self::raised("ValueError", message),
            TaskError::Sqlx(error) | TaskError::Database(DatabaseError::Sqlx(error)) => database_error(&error),
            TaskError::Database(error @ DatabaseError::UnsupportedSchemaVersion(_)) => {
                Self::raised("UnsupportedSchemaVersionError", error.to_string())
            }
            TaskError::Database(error) => Self::raised("ClientConfigurationError", error.to_string()),
        }
    }
}

/// A database failure as asyncpg raises it (DIVERGENCES.md D61, as the monitor words it): the
/// class by SQLSTATE where asyncpg has one, and the server's own message for a statement
/// PostgreSQL refused.
fn database_error(error: &sqlx::Error) -> StepError {
    let class = match error {
        sqlx::Error::Database(database) => match database.code().as_deref() {
            Some("23505") => "UniqueViolationError",
            Some("23503") => "ForeignKeyViolationError",
            Some("23514") => "CheckViolationError",
            Some("23502") => "NotNullViolationError",
            Some("22003") => "NumericValueOutOfRangeError",
            Some("57014") => "QueryCanceledError",
            Some("40001") => "SerializationError",
            Some("40P01") => "DeadlockDetectedError",
            _ => "PostgresError",
        },
        sqlx::Error::Io(_) | sqlx::Error::Tls(_) => "ConnectionError",
        sqlx::Error::PoolTimedOut => "TimeoutError",
        _ => "InterfaceError",
    };
    let message = match error {
        sqlx::Error::Database(database) => database.message().to_owned(),
        other => other.to_string(),
    };
    StepError::raised(class, message)
}

impl From<crate::PyException> for StepError {
    fn from(error: crate::PyException) -> Self {
        Self::raised(error.class, error.message)
    }
}

/// `suppress(KubeError)`: a Kubernetes refusal is dropped, any other error goes on.
pub(crate) fn suppress_kube<T>(result: Result<T, KubeJobsError>) -> Result<Option<T>, StepError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.is_kube() => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// An `OSError` as Python words it: `[Errno 2] No such file or directory: '/x'`.
pub(crate) fn os_error(error: &std::io::Error, path: &Path) -> StepError {
    StepError::raised(os_class(error), crate::py::os_error(error, Some(path), None))
}

/// The `OSError` subclass Python raises for an errno.
pub(crate) fn os_class(error: &std::io::Error) -> &'static str {
    match error.raw_os_error() {
        Some(2) => "FileNotFoundError",
        Some(1 | 13) => "PermissionError",
        Some(17) => "FileExistsError",
        Some(20) => "NotADirectoryError",
        Some(21) => "IsADirectoryError",
        _ => "OSError",
    }
}

/// `path.unlink(missing_ok=True)`.
pub(crate) fn unlink_missing_ok(path: &Path) -> Result<(), StepError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(os_error(&error, path)),
    }
}

/// `shutil.rmtree(path, ignore_errors=True)`: a symbolic link is left alone (rmtree refuses
/// one), every other error is ignored.
pub(crate) fn rmtree_ignore_errors(path: &Path) {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {}
        Ok(meta) if meta.is_dir() => {
            let _ = std::fs::remove_dir_all(path);
        }
        _ => {}
    }
}

/// `path.is_file()`: follows links, `false` on any error.
pub(crate) fn is_file(path: &Path) -> bool {
    std::fs::metadata(path).is_ok_and(|meta| meta.is_file())
}

/// `str(path)` of a path built from text.
pub(crate) fn show(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// `PurePosixPath(text)` split into its anchor and parts.
fn anchor_and_parts(text: &str) -> (&'static str, Vec<String>) {
    let normal = pypath::PyPath::new(text);
    let normal = normal.as_str();
    let (anchor, rest) = if let Some(rest) = normal.strip_prefix("//") {
        ("//", rest)
    } else if let Some(rest) = normal.strip_prefix('/') {
        ("/", rest)
    } else {
        ("", normal)
    };
    let parts = rest
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .map(str::to_owned)
        .collect();
    (anchor, parts)
}

/// `str(PurePosixPath(text).parent)`.
pub fn pure_parent(text: &str) -> String {
    let (anchor, mut parts) = anchor_and_parts(text);
    parts.pop();
    let joined = format!("{anchor}{}", parts.join("/"));
    if joined.is_empty() { ".".to_owned() } else { joined }
}

/// `api_path(mounted, mount_prefix)`: the CloudDrive API path of a file seen through the
/// mount; `MergeFailedError` when it is outside the mount.
pub fn api_path(mounted: &str, mount_prefix: &str) -> Result<String, StepError> {
    let (anchor, parts) = anchor_and_parts(mounted);
    let (prefix_anchor, prefix_parts) = anchor_and_parts(mount_prefix);
    if anchor != prefix_anchor || parts.len() < prefix_parts.len() || parts[..prefix_parts.len()] != *prefix_parts {
        return Err(StepError::failed(format!(
            "{mounted} is outside the CloudDrive mount {mount_prefix}"
        )));
    }
    Ok(format!("/{}", parts[prefix_parts.len()..].join("/")))
}

/// `MergePaths`: where one task's files live, local and in CloudDrive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergePaths {
    pub output: PathBuf,
    pub status: PathBuf,
    pub cloud_source: String,
    pub staging_dir: String,
    pub staged_file: String,
    pub intake_dir: String,
    pub target_dir: String,
    pub part_api_paths: Vec<String>,
}

/// `Path(text)`, written as pathlib writes it.
pub(crate) fn py_path(text: &str) -> PathBuf {
    PathBuf::from(pypath::PyPath::new(text).as_str())
}

/// `Path(base) / name`.
pub(crate) fn py_join(base: &str, name: &str) -> PathBuf {
    PathBuf::from(pypath::join(base, name))
}

/// `merge_paths(task, merge, archive)`.
pub fn merge_paths(task: &MergeTask, merge: &MergeConfig, archive: &ArchiveConfig) -> Result<MergePaths, StepError> {
    let staging_dir = format!("{}/merge-{}", merge.cloud_staging_dir, task.id);
    let intake_dir = api_path(&archive.src_dir, &merge.cloud_mount_prefix)?;
    let work = pypath::PyPath::new(&merge.work_dir).as_str().to_owned();
    Ok(MergePaths {
        output: py_join(&work, &task.merged_name),
        status: py_join(&pypath::join(&work, ".merge"), &format!("{}.json", task.id)),
        cloud_source: format!("{}/{}", merge.cloud_work_dir, task.merged_name),
        staged_file: format!("{staging_dir}/{}", task.merged_name),
        staging_dir,
        target_dir: format!("{intake_dir}/{}/{}", task.source, task.avid),
        intake_dir: format!("{intake_dir}/{}", task.source),
        part_api_paths: task
            .parts
            .iter()
            .map(|part| api_path(part, &merge.cloud_mount_prefix))
            .collect::<Result<_, _>>()?,
    })
}

/// The parts' total size as the CloudDrive mount reports it (`part_sizes`).
pub fn part_sizes(parts: &[String]) -> std::io::Result<i64> {
    let mut total: i64 = 0;
    for part in parts {
        let size = std::fs::metadata(part)?.len();
        total = total.saturating_add(i64::try_from(size).unwrap_or(i64::MAX));
    }
    Ok(total)
}

/// Where a configuration section comes from: read each time it is needed, as Python's
/// `lambda: store.get(...)`.
pub type Section<T> = Arc<dyn Fn() -> T + Send + Sync>;

/// `cloud()`: the current CloudDrive client, `None` while it is not configured.
pub type CloudSource = Arc<dyn Fn() -> Option<Arc<dyn MergeCloud>> + Send + Sync>;

/// `sizes(parts)`: the parts' total size, read from the mount.
pub type Sizes = Arc<dyn Fn(&[String]) -> std::io::Result<i64> + Send + Sync>;

/// What a [`MergeService`] is built from (`MergeService(...)`'s keyword arguments).
#[expect(missing_debug_implementations, reason = "closures and trait objects")]
pub struct ServiceParts {
    pub repository: MergeTaskRepository,
    pub cloud: CloudSource,
    pub kube: Option<Arc<dyn MergeKube>>,
    pub merge_config: Section<MergeConfig>,
    pub archive_config: Section<ArchiveConfig>,
    pub clouddrive_config: Section<CloudDriveConfig>,
    pub trigger: Trigger,
    pub poll_seconds: f64,
    pub transcodes: Option<TranscodeRepository>,
    pub presence: Option<WorkerPresence>,
    pub sizes: Sizes,
    pub clock: Clock,
}

impl ServiceParts {
    /// The parts with Python's defaults: 10 s polls, no transcodes, a fresh presence, sizes
    /// from the mount and the wall clock.
    pub fn new(
        repository: MergeTaskRepository,
        cloud: CloudSource,
        kube: Option<Arc<dyn MergeKube>>,
        merge_config: Section<MergeConfig>,
        archive_config: Section<ArchiveConfig>,
        clouddrive_config: Section<CloudDriveConfig>,
        trigger: Trigger,
    ) -> Self {
        Self {
            repository,
            cloud,
            kube,
            merge_config,
            archive_config,
            clouddrive_config,
            trigger,
            poll_seconds: POLL_SECONDS,
            transcodes: None,
            presence: None,
            sizes: Arc::new(part_sizes),
            clock: system_clock(),
        }
    }
}

struct Inner {
    repository: MergeTaskRepository,
    cloud: CloudSource,
    kube: Option<Arc<dyn MergeKube>>,
    merge_config: Section<MergeConfig>,
    archive_config: Section<ArchiveConfig>,
    clouddrive_config: Section<CloudDriveConfig>,
    trigger: Trigger,
    poll: StdDuration,
    wake: Notify,
    /// `_tick_lock`: a tick, a claim and the operator's actions run one at a time.
    tick_lock: tokio::sync::Mutex<()>,
    transcodes: Option<TranscodeRepository>,
    presence: WorkerPresence,
    sizes: Sizes,
    /// `_totals`: the parts' total size per task, read once from the mount.
    totals: Mutex<HashMap<i64, i64>>,
    clock: Clock,
    /// The poll loop and the token that stops it.
    running: Mutex<Option<(tokio::task::JoinHandle<()>, CancellationToken)>>,
}

/// `MergeService`. Clones share the service.
#[derive(Clone)]
pub struct MergeService {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for MergeService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergeService").finish_non_exhaustive()
    }
}

/// The handlers' view of one tick: the CloudDrive handle it uses and whether the merge Job
/// slot is taken (`_slot_taken`).
pub(crate) struct Tick {
    cloud: Option<Arc<dyn MergeCloud>>,
    slot_taken: bool,
}

impl MergeService {
    pub fn new(parts: ServiceParts) -> Self {
        let presence = parts
            .presence
            .unwrap_or_else(|| WorkerPresence::new(Arc::clone(&parts.clock)));
        Self {
            inner: Arc::new(Inner {
                repository: parts.repository,
                cloud: parts.cloud,
                kube: parts.kube,
                merge_config: parts.merge_config,
                archive_config: parts.archive_config,
                clouddrive_config: parts.clouddrive_config,
                trigger: parts.trigger,
                poll: StdDuration::from_secs_f64(parts.poll_seconds.max(0.0)),
                wake: Notify::new(),
                tick_lock: tokio::sync::Mutex::new(()),
                transcodes: parts.transcodes,
                presence,
                sizes: parts.sizes,
                totals: Mutex::new(HashMap::new()),
                clock: parts.clock,
                running: Mutex::new(None),
            }),
        }
    }

    /// `presence`.
    pub fn presence(&self) -> &WorkerPresence {
        &self.inner.presence
    }

    pub fn repository(&self) -> &MergeTaskRepository {
        &self.inner.repository
    }

    pub(crate) fn now(&self) -> chrono::DateTime<chrono::Utc> {
        (self.inner.clock)()
    }

    fn merge_config(&self) -> MergeConfig {
        (self.inner.merge_config)()
    }

    fn archive_config(&self) -> ArchiveConfig {
        (self.inner.archive_config)()
    }

    pub(crate) fn paths(&self, task: &MergeTask) -> Result<MergePaths, StepError> {
        merge_paths(task, &self.merge_config(), &self.archive_config())
    }

    // -- lifecycle -----------------------------------------------------------

    /// `start()`: run the poll loop in the background.
    pub fn start(&self) {
        let token = CancellationToken::new();
        let service = self.clone();
        let stop = token.clone();
        let handle = tokio::spawn(async move { service.run_loop(stop).await });
        *self.inner.running.lock().unwrap_or_else(PoisonError::into_inner) = Some((handle, token));
    }

    /// `aclose()`: stop the loop (a tick under way is dropped once its CloudDrive calls have
    /// their answers) and close the Job client.
    pub async fn aclose(&self) {
        let running = self.inner.running.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some((handle, token)) = running {
            token.cancel();
            let _ = handle.await;
        }
        if let Some(kube) = &self.inner.kube {
            kube.aclose().await;
        }
    }

    /// `wake()`: poll now rather than at the next interval.
    pub fn wake(&self) {
        self.inner.wake.notify_one();
    }

    /// `unavailable()`: why merging cannot run here, or `None` when it can.
    pub fn unavailable(&self) -> Option<String> {
        if self.inner.kube.is_none() {
            return Some(NO_JOB_TEMPLATE.to_owned());
        }
        if !(self.inner.clouddrive_config)().configured() {
            return Some("CloudDrive is not configured".to_owned());
        }
        let archive = self.archive_config();
        if archive.src_dir.is_empty() || archive.dst_dir.is_empty() {
            return Some("archive.src_dir and archive.dst_dir must be configured".to_owned());
        }
        let merge = self.merge_config();
        if !merge.configured() {
            return Some("merge directories must be configured".to_owned());
        }
        if api_path(&archive.src_dir, &merge.cloud_mount_prefix).is_err() {
            return Some("archive.src_dir must lie under merge.cloud_mount_prefix".to_owned());
        }
        None
    }

    async fn run_loop(&self, stop: CancellationToken) {
        loop {
            let cloud = (self.inner.cloud)().map(|cloud| cloud.scoped());
            let mut tick = Box::pin(self.tick_on(cloud.clone()));
            let outcome = tokio::select! {
                biased;
                () = stop.cancelled() => None,
                outcome = &mut tick => Some(outcome),
            };
            drop(tick);
            match outcome {
                None => {
                    if let Some(cloud) = &cloud {
                        cloud.settle().await;
                    }
                    return;
                }
                Some(Err(error)) => {
                    tracing::error!(logger = LOGGER, error = %error, "merge poll failed");
                }
                Some(Ok(())) => {}
            }
            tokio::select! {
                biased;
                () = stop.cancelled() => return,
                () = self.inner.wake.notified() => {}
                () = tokio::time::sleep(self.inner.poll) => {}
            }
        }
    }

    // -- polling -------------------------------------------------------------

    /// `tick()`: advance every open task by at most one step.
    pub async fn tick(&self) -> Result<(), StepError> {
        let cloud = (self.inner.cloud)();
        self.tick_on(cloud).await
    }

    async fn tick_on(&self, cloud: Option<Arc<dyn MergeCloud>>) -> Result<(), StepError> {
        let _guard = self.inner.tick_lock.lock().await;
        if self.unavailable().is_some() {
            return Ok(());
        }
        let tasks = self.inner.repository.open_tasks().await?;
        for task in &tasks {
            if task.state == MergeState::Queued {
                self.total_bytes(task).await?;
            }
        }
        let online = self.inner.presence.online();
        let mut tick = Tick {
            cloud,
            slot_taken: tasks.iter().any(|task| task.state == MergeState::Merging),
        };
        // The home GPU at work holds the queue as a merge Job does.
        let gpu_busy = online && tasks.iter().any(|task| task.state == MergeState::Transcoding);
        for task in &tasks {
            if task.state == MergeState::Failed {
                continue;
            }
            if task.state == MergeState::Queued {
                if self.home_bound(task) {
                    self.note_waiting(task, online).await?;
                    continue;
                }
                if tick.slot_taken || gpu_busy {
                    continue;
                }
                tick.slot_taken = true;
            }
            self.step(&mut tick, task).await?;
        }
        Ok(())
    }

    async fn step(&self, tick: &mut Tick, task: &MergeTask) -> Result<(), StepError> {
        let outcome = match self.paths(task) {
            Ok(paths) => match task.state {
                MergeState::Queued => self.start_merge(tick, task, &paths).await,
                MergeState::Transcoding => self.watch_transcode(tick, task, &paths).await,
                MergeState::Merging => self.watch_merge(task, &paths).await,
                MergeState::Uploading => self.watch_upload(tick, task, &paths).await,
                MergeState::Verifying => self.verify(tick, task, &paths).await,
                MergeState::Replacing => self.replace(tick, task, &paths).await,
                MergeState::Archiving => self.watch_archive(tick, task, &paths).await,
                // `handlers[task.state]`: the open states only.
                MergeState::Done | MergeState::Failed | MergeState::Cancelled => Err(StepError::raised(
                    "KeyError",
                    format!(
                        "<MergeState.{}: '{}'>",
                        task.state.as_str().to_uppercase(),
                        task.state.as_str()
                    ),
                )),
            },
            Err(error) => Err(error),
        };
        match outcome {
            Ok(()) => Ok(()),
            Err(StepError::Failed(failure)) => {
                tracing::warn!(
                    logger = LOGGER,
                    "merge of {} failed while {}: {failure}",
                    task.avid,
                    task.state.as_str()
                );
                self.inner
                    .repository
                    .transition(
                        task.id,
                        task.state,
                        MergeState::Failed,
                        vec![
                            TaskField::FailedState(Some(task.state)),
                            TaskField::Error(Some(failure)),
                            TaskField::Notice(None),
                        ],
                    )
                    .await?;
                Ok(())
            }
            Err(error) => {
                tracing::error!(
                    logger = LOGGER,
                    error = %error,
                    "merge of {} could not advance while {}",
                    task.avid,
                    task.state.as_str()
                );
                self.inner
                    .repository
                    .update(task.id, vec![TaskField::Notice(Some(error.notice()))])
                    .await?;
                Ok(())
            }
        }
    }

    fn require_cloud(tick: &Tick) -> Result<&Arc<dyn MergeCloud>, StepError> {
        tick.cloud
            .as_ref()
            .ok_or_else(|| StepError::runtime("CloudDrive is not configured"))
    }

    fn require_kube(&self) -> Result<&Arc<dyn MergeKube>, StepError> {
        self.inner
            .kube
            .as_ref()
            .ok_or_else(|| StepError::runtime(NO_JOB_TEMPLATE))
    }

    /// The worker's command-line arguments ahead of its mode: output, status and reserve.
    fn worker_args(&self, paths: &MergePaths, status: &Path) -> Vec<String> {
        vec![
            "--output".to_owned(),
            show(&paths.output),
            "--status".to_owned(),
            show(status),
            "--reserve-gib".to_owned(),
            self.merge_config().free_space_reserve_gib.to_string(),
        ]
    }

    // -- queued / merging ------------------------------------------------------

    async fn start_merge(&self, tick: &Tick, task: &MergeTask, paths: &MergePaths) -> Result<(), StepError> {
        let kube = self.require_kube()?;
        self.refuse_whole_copy(tick.cloud.as_ref(), task, paths).await?;
        let name = job_name(task.id, &task.avid);
        unlink_missing_ok(&paths.status)?;
        let mut args = self.worker_args(paths, &paths.status);
        args.push("--".to_owned());
        args.extend(task.parts.iter().cloned());
        kube.create_job(&name, &args, task.id).await?;
        self.inner
            .repository
            .transition(
                task.id,
                MergeState::Queued,
                MergeState::Merging,
                vec![
                    TaskField::JobName(Some(name)),
                    TaskField::Phase(Some("starting".to_owned())),
                    TaskField::Progress(Some(0.0)),
                    TaskField::Error(None),
                    TaskField::Notice(None),
                ],
            )
            .await?;
        Ok(())
    }

    async fn watch_merge(&self, task: &MergeTask, paths: &MergePaths) -> Result<(), StepError> {
        let kube = self.require_kube()?;
        let status = read_status(&paths.status).unwrap_or_default();
        let state = status.get("state").cloned().unwrap_or(Value::Null);
        if pyops::is_str(&state, "done") {
            return self.finish_merge(kube, task, paths, &status).await;
        }
        if pyops::is_str(&state, "failed") {
            let error = status.get("error").cloned().unwrap_or(Value::Null);
            return Err(StepError::failed(if error.is_truthy() {
                pyvalue::py_str(&error)
            } else {
                "the merge Job failed".to_owned()
            }));
        }
        let job_name = task.job_name.as_deref().filter(|job| !job.is_empty());
        let job = match job_name {
            Some(name) => kube.get_job(name).await?,
            None => None,
        };
        let Some(job) = job else {
            return Err(StepError::failed("the merge Job disappeared before it finished"));
        };
        let outcome = job_outcome(&job);
        if outcome == "failed" {
            // Killed (out of memory, past its deadline) before it could clean up after itself.
            unlink_missing_ok(&partial_path(&paths.output))?;
            rmtree_ignore_errors(&staging_dir(&paths.output));
            let tail = suppress_kube(kube.log_tail(job_name.unwrap_or_default()).await)?.unwrap_or_default();
            return Err(StepError::failed(job_failed("the merge Job failed", &job, &tail)));
        }
        if outcome == "succeeded" {
            // A refusal ends the Job cleanly too, but the worker reports before it exits; a
            // report written since the read above waits for the next tick.
            let fresh = read_status(&paths.status).unwrap_or_default();
            if pyops::state_in(fresh.get("state").unwrap_or(&Value::Null), &["done", "failed"])? {
                return Ok(());
            }
            return Err(StepError::failed("the merge Job ended without reporting a result"));
        }
        let phase = status.get("phase").cloned().unwrap_or(Value::Null);
        let phase = if phase.is_truthy() {
            pyops::text_argument(&phase, 3)?
        } else {
            task.phase.clone()
        };
        let progress = match status.get("progress") {
            Some(value) => pyops::float_argument(value, 4)?,
            None => task.progress,
        };
        self.inner
            .repository
            .update(
                task.id,
                vec![
                    TaskField::Phase(phase),
                    TaskField::Progress(progress),
                    TaskField::Notice(None),
                ],
            )
            .await?;
        Ok(())
    }

    /// The Job reported the merged file done: check it and move on to the upload.
    async fn finish_merge(
        &self,
        kube: &Arc<dyn MergeKube>,
        task: &MergeTask,
        paths: &MergePaths,
        status: &embyx_core::pyjson::Dict,
    ) -> Result<(), StepError> {
        let size = status.get("size").cloned().unwrap_or(Value::Null);
        let on_disk = if is_file(&paths.output) {
            let meta = std::fs::metadata(&paths.output).map_err(|error| os_error(&error, &paths.output))?;
            Some(meta.len())
        } else {
            None
        };
        if on_disk.is_none_or(|bytes| !crate::py::eq(&Value::Int(i128::from(bytes)), &size)) {
            return Err(StepError::failed(format!(
                "the Job reported {} done, but it is not on the work volume",
                pypath::name(&show(&paths.output))
            )));
        }
        let merged_bytes = pyops::int_field(&size, 4)?;
        let sha1 = pyops::py_upper(&pyvalue::py_str(
            status.get("sha1").unwrap_or(&Value::Str(String::new())),
        ));
        self.inner
            .repository
            .transition(
                task.id,
                MergeState::Merging,
                MergeState::Uploading,
                vec![
                    TaskField::MergedBytes(Some(merged_bytes)),
                    TaskField::MergedSha1(Some(sha1)),
                    TaskField::Phase(None),
                    TaskField::Progress(None),
                    TaskField::UploadedBytes(Some(0)),
                    TaskField::UploadAttempts(0),
                    TaskField::Notice(None),
                ],
            )
            .await?;
        if let Some(job) = task.job_name.as_deref().filter(|job| !job.is_empty()) {
            suppress_kube(kube.delete_job(job).await)?;
        }
        unlink_missing_ok(&paths.status)?;
        if task.transcoder.as_deref() == Some("home") {
            // The chunks are in the merged file now, and would only crowd its upload.
            self.discard_transcode(task).await?;
        }
        Ok(())
    }

    // -- uploading / verifying -------------------------------------------------

    async fn watch_upload(&self, tick: &Tick, task: &MergeTask, paths: &MergePaths) -> Result<(), StepError> {
        let cloud = Self::require_cloud(tick)?;
        let copy = copy_task(cloud.as_ref(), paths).await?;
        if copy.as_ref().is_some_and(|copy| copy.status == CopyStatus::Completed) {
            self.to_verifying(task).await?;
            return Ok(());
        }
        if !is_file(&paths.output) {
            // Accepted for now: a redeploy can take the work volume's file with it.
            self.inner
                .repository
                .transition(
                    task.id,
                    MergeState::Uploading,
                    MergeState::Queued,
                    vec![
                        TaskField::JobName(None),
                        TaskField::MergedBytes(None),
                        TaskField::MergedSha1(None),
                        TaskField::UploadedBytes(None),
                        TaskField::Notice(Some(
                            "the merged file was lost before the upload finished; merging again".to_owned(),
                        )),
                    ],
                )
                .await?;
            return Ok(());
        }
        let Some(copy) = copy else {
            if task.upload_attempts != 0 {
                // CloudDrive may have dropped a finished task from its list, or not listed a
                // fresh one yet; neither calls for uploading it all again.
                let staged = cloud.stat_file(&paths.staged_file).await?;
                if staged.is_some_and(|staged| Some(staged.size) == task.merged_bytes) {
                    self.to_verifying(task).await?;
                    return Ok(());
                }
                if self.now() - task.updated_at < copy_listing_grace() {
                    return Ok(());
                }
            }
            return self.issue_copy(task, paths, cloud.as_ref()).await;
        };
        if copy.status == CopyStatus::Failed {
            let joined = copy.errors.join("; ");
            let errors = if joined.is_empty() {
                "CloudDrive reported the copy as failed".to_owned()
            } else {
                joined
            };
            if task.upload_attempts >= MAX_UPLOAD_ATTEMPTS {
                return Err(StepError::failed(format!(
                    "upload failed {} times: {errors}",
                    task.upload_attempts
                )));
            }
            return self
                .restart_upload_in_time(task, paths, cloud.as_ref(), &copy.source_path, &errors)
                .await;
        }
        self.follow_upload(task, paths, cloud.as_ref(), &copy).await
    }

    async fn to_verifying(&self, task: &MergeTask) -> Result<(), StepError> {
        self.inner
            .repository
            .transition(
                task.id,
                MergeState::Uploading,
                MergeState::Verifying,
                vec![TaskField::UploadedBytes(task.merged_bytes), TaskField::Notice(None)],
            )
            .await?;
        Ok(())
    }

    /// Record how far a running upload got, or deal with CloudDrive giving up on it.
    async fn follow_upload(
        &self,
        task: &MergeTask,
        paths: &MergePaths,
        cloud: &dyn MergeCloud,
        copy: &CopyTask,
    ) -> Result<(), StepError> {
        let upload = cloud.upload_state(&paths.staged_file).await?;
        if let Some(fatal) = upload.as_ref().and_then(|upload| upload.fatal()) {
            let fatal = fatal.to_owned();
            return self
                .handle_fatal_upload(task, paths, cloud, &copy.source_path, &fatal)
                .await;
        }
        // The copy task counts nothing until the whole file is up; the upload queue counts as
        // it goes.
        let uploaded = copy
            .uploaded_bytes
            .max(upload.as_ref().map_or(0, |upload| upload.transferred));
        self.inner
            .repository
            .update(
                task.id,
                vec![
                    TaskField::UploadedBytes(Some(pyops::int8_argument(i128::from(uploaded), 3)?)),
                    TaskField::Notice(copy.paused.then(|| "CloudDrive has paused the upload".to_owned())),
                ],
            )
            .await?;
        Ok(())
    }

    async fn handle_fatal_upload(
        &self,
        task: &MergeTask,
        paths: &MergePaths,
        cloud: &dyn MergeCloud,
        source_path: &str,
        fatal: &str,
    ) -> Result<(), StepError> {
        let permanent = PERMANENT_UPLOAD_ERRORS.iter().any(|marker| fatal.contains(marker));
        if permanent || task.upload_attempts >= MAX_UPLOAD_ATTEMPTS {
            return Err(StepError::failed(format!(
                "CloudDrive gave up uploading {}: {fatal}",
                task.merged_name
            )));
        }
        // A network error ends an upload the same way; restart it once its rest is over.
        self.restart_upload_in_time(task, paths, cloud, source_path, fatal)
            .await
    }

    /// Restart an upload that ended in `error` after the rest its attempt count calls for.
    async fn restart_upload_in_time(
        &self,
        task: &MergeTask,
        paths: &MergePaths,
        cloud: &dyn MergeCloud,
        source_path: &str,
        error: &str,
    ) -> Result<(), StepError> {
        let elapsed = self.now() - task.updated_at;
        if task.notice.as_deref() == Some(error) && elapsed < copy_listing_grace() {
            return Ok(());
        }
        let delay = upload_retry_delay(task.upload_attempts);
        if !delay.is_zero() {
            let waiting = retry_notice(task.upload_attempts, delay, error);
            if task.notice.as_deref() != Some(waiting.as_str()) {
                self.inner
                    .repository
                    .update(task.id, vec![TaskField::Notice(Some(waiting))])
                    .await?;
                return Ok(());
            }
            if elapsed < delay {
                return Ok(());
            }
        }
        cloud.restart_copy_task(source_path, &paths.staging_dir).await?;
        self.inner
            .repository
            .update(
                task.id,
                vec![
                    TaskField::UploadAttempts(task.upload_attempts.saturating_add(1)),
                    TaskField::Notice(Some(error.to_owned())),
                ],
            )
            .await?;
        Ok(())
    }

    async fn issue_copy(&self, task: &MergeTask, paths: &MergePaths, cloud: &dyn MergeCloud) -> Result<(), StepError> {
        if task.upload_attempts >= MAX_UPLOAD_ATTEMPTS {
            return Err(StepError::failed(format!(
                "CloudDrive did not keep the upload after {} attempts",
                task.upload_attempts
            )));
        }
        let (staging_parent, staging_name) = pyops::rsplit_once_slash(&paths.staging_dir)?;
        let parent = if staging_parent.is_empty() { "/" } else { staging_parent };
        cloud.ensure_directory(parent, staging_name).await?;
        let result = cloud.copy_file(&paths.cloud_source, &paths.staging_dir).await?;
        let notice = if result.success {
            None
        } else if result.error_message.is_empty() {
            Some("CloudDrive refused the copy".to_owned())
        } else {
            Some(result.error_message)
        };
        self.inner
            .repository
            .update(
                task.id,
                vec![
                    TaskField::UploadAttempts(task.upload_attempts.saturating_add(1)),
                    TaskField::Notice(notice),
                ],
            )
            .await?;
        Ok(())
    }

    async fn verify(&self, tick: &Tick, task: &MergeTask, paths: &MergePaths) -> Result<(), StepError> {
        let cloud = Self::require_cloud(tick)?;
        let waited = self.now() - task.state_changed_at;
        let staged = cloud.stat_file(&paths.staged_file).await?;
        let Some(staged) = staged else {
            if waited < staged_file_wait() {
                self.inner
                    .repository
                    .update(
                        task.id,
                        vec![TaskField::Notice(Some(
                            "waiting for CloudDrive to list the uploaded file".to_owned(),
                        ))],
                    )
                    .await?;
                return Ok(());
            }
            return Err(StepError::failed(format!(
                "CloudDrive finished the copy, but {} is not there",
                paths.staged_file
            )));
        };
        if Some(staged.size) != task.merged_bytes {
            return Err(StepError::failed(format!(
                "uploaded size {} differs from the merged file ({})",
                staged.size,
                pyops::opt_display(task.merged_bytes.as_ref())
            )));
        }
        let sha1 = pyops::py_upper(staged.hashes.get(SHA1_HASH_KEY).map_or("", String::as_str));
        if sha1.is_empty() {
            if waited < hash_wait() {
                self.inner
                    .repository
                    .update(
                        task.id,
                        vec![TaskField::Notice(Some(
                            "waiting for 115 to report the SHA-1".to_owned(),
                        ))],
                    )
                    .await?;
                return Ok(());
            }
            return Err(StepError::failed("115 never reported a SHA-1 for the uploaded file"));
        }
        if Some(sha1.as_str()) != task.merged_sha1.as_deref() {
            return Err(StepError::failed(format!(
                "uploaded SHA-1 {sha1} differs from the merged file ({})",
                pyops::opt_display(task.merged_sha1.as_ref())
            )));
        }
        // The last look before the parts are deleted.
        self.refuse_whole_copy(tick.cloud.as_ref(), task, paths).await?;
        self.inner
            .repository
            .transition(
                task.id,
                MergeState::Verifying,
                MergeState::Replacing,
                vec![TaskField::Notice(None)],
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn refuse_whole_copy(
        &self,
        cloud: Option<&Arc<dyn MergeCloud>>,
        task: &MergeTask,
        paths: &MergePaths,
    ) -> Result<(), StepError> {
        if let Some(whole) = self.whole_copy(cloud, task, paths).await? {
            return Err(StepError::failed(format!(
                "the library already holds {whole}, a whole copy of {}; merging its parts would only duplicate it",
                task.avid
            )));
        }
        Ok(())
    }

    /// `_whole_copy`: a video beside the parts that holds the whole title, by name.
    pub(crate) async fn whole_copy(
        &self,
        cloud: Option<&Arc<dyn MergeCloud>>,
        task: &MergeTask,
        paths: &MergePaths,
    ) -> Result<Option<String>, StepError> {
        let cloud = cloud.ok_or_else(|| StepError::runtime("CloudDrive is not configured"))?;
        let first = paths
            .part_api_paths
            .first()
            .ok_or_else(|| StepError::raised("IndexError", "tuple index out of range"))?;
        let directory = pure_parent(first);
        let entries = match cloud.list_directory(&directory, true).await {
            Ok(entries) => entries,
            Err(error) if error.is_file_not_found() => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let names: Vec<&str> = entries
            .iter()
            .filter(|entry| !entry.is_directory)
            .map(|entry| entry.name.as_str())
            .collect();
        Ok(whole_copies(&task.avid, names).into_iter().next())
    }

    // -- replacing / archiving -------------------------------------------------

    async fn replace(&self, tick: &Tick, task: &MergeTask, paths: &MergePaths) -> Result<(), StepError> {
        let cloud = Self::require_cloud(tick)?;
        let remaining = present(cloud.as_ref(), &paths.part_api_paths).await?;
        if !remaining.is_empty() {
            let result = cloud.delete_files(&remaining).await?;
            let remaining = present(cloud.as_ref(), &paths.part_api_paths).await?;
            if !remaining.is_empty() {
                let why = if result.error_message.is_empty() {
                    "retrying".to_owned()
                } else {
                    result.error_message
                };
                self.inner
                    .repository
                    .update(
                        task.id,
                        vec![TaskField::Notice(Some(format!(
                            "{} original parts are still listed: {why}",
                            remaining.len()
                        )))],
                    )
                    .await?;
                return Ok(());
            }
        }
        let (intake_parent, intake_name) = pyops::rsplit_once_slash(&paths.intake_dir)?;
        let parent = if intake_parent.is_empty() { "/" } else { intake_parent };
        cloud.ensure_directory(parent, intake_name).await?;
        cloud.ensure_directory(&paths.intake_dir, &task.avid).await?;
        let target_file = format!("{}/{}", paths.target_dir, task.merged_name);
        if cloud.stat_file(&paths.staged_file).await?.is_some() {
            let moved = cloud.move_file(&paths.staged_file, &paths.target_dir).await?;
            if !moved.success {
                let notice = if moved.error_message.is_empty() {
                    "the move was refused".to_owned()
                } else {
                    moved.error_message
                };
                self.inner
                    .repository
                    .update(task.id, vec![TaskField::Notice(Some(notice))])
                    .await?;
                return Ok(());
            }
        }
        if cloud.stat_file(&target_file).await?.is_none() {
            return Err(StepError::failed(format!(
                "the merged file is neither in {} nor in {}",
                paths.staging_dir, paths.target_dir
            )));
        }
        // `with suppress(Exception)`.
        let _ = cloud.delete_files(std::slice::from_ref(&paths.staging_dir)).await;
        unlink_missing_ok(&paths.output)?;
        unlink_missing_ok(&partial_path(&paths.output))?;
        unlink_missing_ok(&paths.status)?;
        let moved = self
            .inner
            .repository
            .transition(
                task.id,
                MergeState::Replacing,
                MergeState::Archiving,
                vec![TaskField::Notice(None)],
            )
            .await?;
        if moved.is_some() {
            (self.inner.trigger)(Pipeline::Archive).await;
        }
        Ok(())
    }

    async fn watch_archive(&self, tick: &Tick, task: &MergeTask, paths: &MergePaths) -> Result<(), StepError> {
        let cloud = Self::require_cloud(tick)?;
        let entries = match cloud.list_directory(&paths.intake_dir, true).await {
            Ok(entries) => entries,
            Err(error) if error.is_file_not_found() => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        if !entries.iter().any(|entry| entry.full_path == paths.target_dir) {
            self.inner
                .repository
                .transition(
                    task.id,
                    MergeState::Archiving,
                    MergeState::Done,
                    vec![TaskField::FinishedAt(Some(self.now())), TaskField::Notice(None)],
                )
                .await?;
            (self.inner.trigger)(Pipeline::Mapping).await;
            return Ok(());
        }
        let now = self.now();
        if now - task.state_changed_at > archive_wait() {
            if let Some(whole) = self.whole_copy(tick.cloud.as_ref(), task, paths).await? {
                return Err(StepError::failed(format!(
                    "the library already holds {whole}, so the archive leaves {} alone; remove the task to delete the merged copy",
                    paths.target_dir
                )));
            }
            return Err(StepError::failed(format!(
                "the archive has not filed {}; check the archive run log",
                paths.target_dir
            )));
        }
        if now - task.updated_at > archive_nudge_interval() {
            (self.inner.trigger)(Pipeline::Archive).await;
            self.inner
                .repository
                .update(
                    task.id,
                    vec![TaskField::Notice(Some(
                        "waiting for the archive run to file it".to_owned(),
                    ))],
                )
                .await?;
        }
        Ok(())
    }
}

/// `'the merge Job failed' + (': ' + ' | '.join(details) if details else '')`.
fn job_failed(prefix: &str, job: &Value, tail: &str) -> String {
    let failure = job_failure(job);
    let details: Vec<&str> = [failure.as_str(), tail]
        .into_iter()
        .filter(|detail| !detail.is_empty())
        .collect();
    if details.is_empty() {
        prefix.to_owned()
    } else {
        format!("{prefix}: {}", details.join(" | "))
    }
}

/// `_copy_task`: the latest copy into this task's staging folder; the folder is the task's
/// alone.
pub(crate) async fn copy_task(cloud: &dyn MergeCloud, paths: &MergePaths) -> Result<Option<CopyTask>, StepError> {
    Ok(cloud
        .copy_tasks()
        .await?
        .into_iter()
        .rev()
        .find(|entry| entry.dest_path == paths.staging_dir))
}

/// `_present`: which of `files` CloudDrive still lists, one fresh listing per directory.
async fn present(cloud: &dyn MergeCloud, files: &[String]) -> Result<Vec<String>, StepError> {
    let mut directories: Vec<String> = files.iter().map(|path| pure_parent(path)).collect();
    directories.sort();
    directories.dedup();
    let mut listed: Vec<String> = Vec::new();
    for directory in directories {
        match cloud.list_directory(&directory, true).await {
            Ok(entries) => listed.extend(
                entries
                    .into_iter()
                    .filter(|entry| !entry.is_directory)
                    .map(|entry| entry.full_path),
            ),
            Err(error) if error.is_file_not_found() => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(files.iter().filter(|path| listed.contains(path)).cloned().collect())
}

impl MergeService {
    /// The tick's view without a tick: the current CloudDrive client.
    pub(crate) fn tick_now(&self) -> Tick {
        Tick {
            cloud: (self.inner.cloud)(),
            slot_taken: false,
        }
    }

    pub(crate) fn inner_presence_online(&self) -> bool {
        self.inner.presence.online()
    }

    pub(crate) fn kube(&self) -> Option<&Arc<dyn MergeKube>> {
        self.inner.kube.as_ref()
    }

    pub(crate) fn transcodes(&self) -> Option<&TranscodeRepository> {
        self.inner.transcodes.as_ref()
    }

    pub(crate) fn work_dir(&self) -> String {
        self.merge_config().work_dir
    }

    pub(crate) fn home_transcode_enabled(&self) -> bool {
        self.merge_config().home_transcode_enabled
    }

    pub(crate) async fn lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.inner.tick_lock.lock().await
    }

    pub(crate) fn total_of(&self, task_id: i64) -> Option<i64> {
        self.inner
            .totals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&task_id)
            .copied()
    }

    /// `_total_bytes`: the parts' total size, read from the mount once per task; `None` while
    /// it cannot be read.
    pub(crate) async fn total_bytes(&self, task: &MergeTask) -> Result<Option<i64>, StepError> {
        if let Some(total) = self.total_of(task.id) {
            return Ok(Some(total));
        }
        let sizes = Arc::clone(&self.inner.sizes);
        let parts = task.parts.clone();
        let read = tokio::task::spawn_blocking(move || sizes(&parts))
            .await
            .map_err(|error| StepError::runtime(error.to_string()))?;
        if let Ok(total) = read {
            self.inner
                .totals
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(task.id, total);
            Ok(Some(total))
        } else {
            tracing::warn!(logger = LOGGER, "could not read the part sizes of {}", task.avid);
            Ok(None)
        }
    }
}
