//! The CloudDrive wrapper the pipelines use: validated paths, plain values, and calls that
//! run to their end even when the caller goes away.
//!
//! Python origin: `src/embyx_manager/clients/clouddrive/aio.py` (`AsyncCloudDrive`).
//!
//! Python runs each blocking gRPC call in a worker thread and, when the awaiting task is
//! cancelled, still waits for the thread before re-raising the cancellation
//! (`_run_sync_complete`), so a caller can never see a cancelled move while the RPC is in
//! flight. A dropped Rust future cannot wait, so here every call runs in a task of its own
//! ([`AsyncCloudDrive::complete`]): dropping the caller leaves the task running to its end,
//! and [`AsyncCloudDrive::close`] waits for every such task before it returns, which is what
//! shutdown and a configuration swap must await (contract 6; DIVERGENCES.md D41).
//!
//! Python's cancelled caller itself only finishes once the RPC has (a fill-actor execution
//! that is cancelled and then gathered releases its lease after the move ended). A dropped
//! Rust future finishes at once, so code that cancels work around a mutation and then acts
//! on the outcome runs that work on [`AsyncCloudDrive::scoped`] and awaits
//! [`AsyncCloudDrive::settle`] after cancelling: it returns once every call started in the
//! scope has its answer, without closing the client.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use embyx_core::posixpath;
use embyx_core::pyjson::{Dict, Value};
use tokio::sync::Notify;
use tokio_util::task::TaskTracker;

use super::client::{CONFLICT_OVERWRITE, CONFLICT_SKIP, ClientOptions, CloudDriveClient, CloudDriveError};
use super::proto;

/// `validate_api_path`'s refusal.
pub const INVALID_API_PATH: &str = "CloudDrive API path must be a canonical absolute POSIX path";
/// `validate_path_segment`'s refusal.
pub const INVALID_SEGMENT: &str = "CloudDrive folder name must be one safe path segment";

/// `MOVE_CONFLICT_SKIP`: a move never overwrites.
pub const MOVE_CONFLICT_SKIP: i32 = CONFLICT_SKIP;
/// `COPY_CONFLICT_OVERWRITE`: a copy replaces a same-named file at the destination.
pub const COPY_CONFLICT_OVERWRITE: i32 = CONFLICT_OVERWRITE;

fn is_control(ch: char) -> bool {
    // `ord(c) < 32 or ord(c) == 127`: ASCII controls only; U+0080-U+009F are allowed.
    (ch as u32) < 32 || ch == '\u{7f}'
}

/// `validate_api_path(value, allow_root=...)`: a canonical absolute POSIX path without
/// controls, backslashes or a leading `//`; `/` only when `allow_root`.
pub fn validate_api_path(value: &str, allow_root: bool) -> Result<&str, CloudDriveError> {
    if !value.starts_with('/')
        || value.starts_with("//")
        || value.contains('\0')
        || value.contains('\\')
        || value.chars().any(is_control)
        || posixpath::normpath(value) != value
        || (!allow_root && value == "/")
    {
        return Err(CloudDriveError::InvalidPath(INVALID_API_PATH));
    }
    Ok(value)
}

/// `validate_path_segment(value)`: one non-empty name, not `.` or `..`, without slashes,
/// backslashes or controls.
pub fn validate_path_segment(value: &str) -> Result<&str, CloudDriveError> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains('\0')
        || value.chars().any(is_control)
    {
        return Err(CloudDriveError::InvalidPath(INVALID_SEGMENT));
    }
    Ok(value)
}

/// `CloudFile['write_time']`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteTime {
    pub seconds: i64,
    pub nanos: i32,
}

/// `CloudFile`: one entry of a fresh listing (`_cloud_file_to_dict`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudFile {
    pub id: String,
    pub name: String,
    pub full_path: String,
    pub size: i64,
    pub is_directory: bool,
    /// `None` when the message has no `writeTime` (`HasField`), not when it is zero.
    pub write_time: Option<WriteTime>,
    /// Hash kind number (as text) to hash, sorted by the text as Python sorts the keys.
    pub hashes: BTreeMap<String, String>,
}

impl CloudFile {
    pub fn from_proto(file: &proto::CloudDriveFile) -> Self {
        Self {
            id: file.id.clone(),
            name: file.name.clone(),
            full_path: file.full_path_name.clone(),
            size: file.size,
            is_directory: file.is_directory,
            write_time: file.write_time.as_ref().map(|time| WriteTime {
                seconds: time.seconds,
                nanos: time.nanos,
            }),
            hashes: file
                .file_hashes
                .iter()
                .map(|(kind, value)| (kind.to_string(), value.clone()))
                .collect(),
        }
    }

    /// The dict, keys in Python's order.
    pub fn to_value(&self) -> Value {
        let mut dict = Dict::new();
        dict.insert("id", Value::from(self.id.as_str()));
        dict.insert("name", Value::from(self.name.as_str()));
        dict.insert("full_path", Value::from(self.full_path.as_str()));
        dict.insert("size", Value::Int(i128::from(self.size)));
        dict.insert("is_directory", Value::Bool(self.is_directory));
        dict.insert(
            "write_time",
            self.write_time.map_or(Value::Null, |time| {
                let mut write_time = Dict::new();
                write_time.insert("seconds", Value::Int(i128::from(time.seconds)));
                write_time.insert("nanos", Value::Int(i128::from(time.nanos)));
                Value::Dict(write_time)
            }),
        );
        let mut hashes = Dict::new();
        for (kind, value) in &self.hashes {
            hashes.insert(kind.as_str(), Value::from(value.as_str()));
        }
        dict.insert("hashes", Value::Dict(hashes));
        Value::Dict(dict)
    }
}

/// `OfflineStatus`: CloudDrive's offline-task status, by proto enum name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OfflineStatus {
    Init,
    Downloading,
    Finished,
    Error,
    Unknown,
}

impl OfflineStatus {
    /// The status of an `OfflineFileStatus` number; one the descriptor does not name is
    /// `Unknown`.
    pub fn from_number(number: i32) -> Self {
        match proto::OfflineFileStatus::try_from(number) {
            Ok(proto::OfflineFileStatus::OfflineInit) => Self::Init,
            Ok(proto::OfflineFileStatus::OfflineDownloading) => Self::Downloading,
            Ok(proto::OfflineFileStatus::OfflineFinished) => Self::Finished,
            Ok(proto::OfflineFileStatus::OfflineError) => Self::Error,
            Ok(proto::OfflineFileStatus::OfflineUnknown) | Err(_) => Self::Unknown,
        }
    }

    /// The `StrEnum` value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Init => "OFFLINE_INIT",
            Self::Downloading => "OFFLINE_DOWNLOADING",
            Self::Finished => "OFFLINE_FINISHED",
            Self::Error => "OFFLINE_ERROR",
            Self::Unknown => "OFFLINE_UNKNOWN",
        }
    }
}

/// `OfflineTask`: one offline task as the tracker joins on it (`_offline_file_to_dict`).
#[derive(Debug, Clone, PartialEq)]
pub struct OfflineTask {
    pub name: String,
    pub size: u64,
    pub url: String,
    pub status: OfflineStatus,
    /// Upper-cased (`str.upper()`), to match the hashes extracted from our own magnets.
    pub info_hash: String,
    pub file_id: String,
    pub add_time: u64,
    pub progress: f64,
    pub peers: u64,
}

impl OfflineTask {
    pub fn from_proto(file: &proto::OfflineFile) -> Self {
        Self {
            name: file.name.clone(),
            size: file.size,
            url: file.url.clone(),
            status: OfflineStatus::from_number(file.status),
            info_hash: file.info_hash.to_uppercase(),
            file_id: file.file_id.clone(),
            add_time: file.add_time,
            progress: file.percend_done,
            peers: file.peers,
        }
    }

    pub fn to_value(&self) -> Value {
        let mut dict = Dict::new();
        dict.insert("name", Value::from(self.name.as_str()));
        dict.insert("size", Value::Int(i128::from(self.size)));
        dict.insert("url", Value::from(self.url.as_str()));
        dict.insert("status", Value::from(self.status.as_str()));
        dict.insert("info_hash", Value::from(self.info_hash.as_str()));
        dict.insert("file_id", Value::from(self.file_id.as_str()));
        dict.insert("add_time", Value::Int(i128::from(self.add_time)));
        dict.insert("progress", Value::Float(self.progress));
        dict.insert("peers", Value::Int(i128::from(self.peers)));
        Value::Dict(dict)
    }
}

/// `CopyStatus`: CloudDrive's copy-task status, by proto enum name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CopyStatus {
    Pending,
    Scanning,
    Scanned,
    Completed,
    Failed,
    Unknown,
}

impl CopyStatus {
    pub fn from_number(number: i32) -> Self {
        use proto::copy_task::TaskStatus;
        match TaskStatus::try_from(number) {
            Ok(TaskStatus::Pending) => Self::Pending,
            Ok(TaskStatus::Scanning) => Self::Scanning,
            Ok(TaskStatus::Scanned) => Self::Scanned,
            Ok(TaskStatus::Completed) => Self::Completed,
            Ok(TaskStatus::Failed) => Self::Failed,
            Err(_) => Self::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Scanning => "Scanning",
            Self::Scanned => "Scanned",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
            Self::Unknown => "Unknown",
        }
    }
}

/// `CopyTask` (`_copy_task_to_dict`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyTask {
    pub source_path: String,
    pub dest_path: String,
    pub status: CopyStatus,
    pub total_bytes: u64,
    pub uploaded_bytes: u64,
    pub failed_files: u64,
    pub paused: bool,
    pub errors: Vec<String>,
}

impl CopyTask {
    pub fn from_proto(task: &proto::CopyTask) -> Self {
        Self {
            source_path: task.source_path.clone(),
            dest_path: task.dest_path.clone(),
            status: CopyStatus::from_number(task.status),
            total_bytes: task.total_bytes,
            uploaded_bytes: task.uploaded_bytes,
            failed_files: task.failed_files,
            paused: task.paused,
            errors: task.errors.iter().map(|error| error.message.clone()).collect(),
        }
    }

    pub fn to_value(&self) -> Value {
        let mut dict = Dict::new();
        dict.insert("source_path", Value::from(self.source_path.as_str()));
        dict.insert("dest_path", Value::from(self.dest_path.as_str()));
        dict.insert("status", Value::from(self.status.as_str()));
        dict.insert("total_bytes", Value::Int(i128::from(self.total_bytes)));
        dict.insert("uploaded_bytes", Value::Int(i128::from(self.uploaded_bytes)));
        dict.insert("failed_files", Value::Int(i128::from(self.failed_files)));
        dict.insert("paused", Value::Bool(self.paused));
        dict.insert(
            "errors",
            Value::List(self.errors.iter().map(|error| Value::from(error.as_str())).collect()),
        );
        Value::Dict(dict)
    }
}

/// `UploadState`: one file in CloudDrive's upload queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadState {
    /// `Transfer`, `FatalError` and the like, as CloudDrive words it.
    pub status: String,
    pub transferred: u64,
    pub error: String,
}

impl UploadState {
    /// Why CloudDrive gave up on the file for good, or `None` while it has not.
    pub fn fatal(&self) -> Option<&str> {
        if self.status != "FatalError" {
            return None;
        }
        Some(if self.error.is_empty() {
            "fatal upload error"
        } else {
            &self.error
        })
    }

    fn from_proto(info: &proto::UploadFileInfo) -> Self {
        Self {
            status: info.status.clone(),
            transferred: info.transfered_bytes,
            error: info.error_message.clone(),
        }
    }
}

/// `check()`'s answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckResult {
    pub reachable: bool,
    pub authenticated: bool,
    /// `getattr(info, 'cloudAppVersion', '')`: the descriptor has no such field, so always
    /// empty, as in Python.
    pub version: String,
}

/// `ensure_directory`'s answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnsureResult {
    pub success: bool,
    pub created: bool,
    pub path: String,
}

impl EnsureResult {
    pub fn to_value(&self) -> Value {
        let mut dict = Dict::new();
        dict.insert("success", Value::Bool(self.success));
        dict.insert("created", Value::Bool(self.created));
        dict.insert("path", Value::from(self.path.as_str()));
        Value::Dict(dict)
    }
}

/// `move_file`'s answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveResult {
    pub success: bool,
    pub error_message: String,
    pub result_file_paths: Vec<String>,
}

impl MoveResult {
    pub fn to_value(&self) -> Value {
        let mut dict = Dict::new();
        dict.insert("success", Value::Bool(self.success));
        dict.insert("error_message", Value::from(self.error_message.as_str()));
        dict.insert(
            "result_file_paths",
            Value::List(
                self.result_file_paths
                    .iter()
                    .map(|path| Value::from(path.as_str()))
                    .collect(),
            ),
        );
        Value::Dict(dict)
    }
}

/// `copy_file` and `delete_files`' answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationResult {
    pub success: bool,
    pub error_message: String,
}

impl OperationResult {
    pub fn to_value(&self) -> Value {
        let mut dict = Dict::new();
        dict.insert("success", Value::Bool(self.success));
        dict.insert("error_message", Value::from(self.error_message.as_str()));
        Value::Dict(dict)
    }
}

/// grpcio's refusal on a closed channel: `ValueError('Cannot invoke RPC on closed channel!')`.
pub const CLOSED_CHANNEL: &str = "Cannot invoke RPC on closed channel!";

/// `AsyncCloudDrive`: the async, cancel-safe CloudDrive API the pipelines use. Clones share
/// the client and the set of calls in flight.
#[derive(Debug, Clone)]
pub struct AsyncCloudDrive {
    client: CloudDriveClient,
    in_flight: TaskTracker,
    closed: Arc<AtomicBool>,
    /// The calls of one [`Self::scoped`] handle and its clones.
    scope: Option<Arc<Scope>>,
}

/// Calls running in one scope, and a wake-up for [`AsyncCloudDrive::settle`].
#[derive(Debug, Default)]
struct Scope {
    running: AtomicUsize,
    idle: Notify,
}

/// One call counted in its scope until the call's task ends.
struct ScopeEntry(Arc<Scope>);

impl ScopeEntry {
    fn enter(scope: &Arc<Scope>) -> Self {
        scope.running.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(scope))
    }
}

impl Drop for ScopeEntry {
    fn drop(&mut self) {
        if self.0.running.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

impl AsyncCloudDrive {
    pub fn new(client: CloudDriveClient) -> Self {
        Self {
            client,
            in_flight: TaskTracker::new(),
            closed: Arc::new(AtomicBool::new(false)),
            scope: None,
        }
    }

    /// A handle on the same client whose calls (and those of its clones) [`Self::settle`]
    /// can wait for. [`Self::close`] still covers them.
    #[must_use]
    pub fn scoped(&self) -> Self {
        Self {
            scope: Some(Arc::new(Scope::default())),
            ..self.clone()
        }
    }

    /// Wait until no call started through this scoped handle is running, also those whose
    /// caller was dropped: what Python's cancelled caller waits for before it re-raises the
    /// cancellation. Returns at once on a handle that is not scoped.
    pub async fn settle(&self) {
        let Some(scope) = &self.scope else {
            return;
        };
        loop {
            let idle = scope.idle.notified();
            if scope.running.load(Ordering::SeqCst) == 0 {
                return;
            }
            idle.await;
        }
    }

    /// `AsyncCloudDrive(CloudDriveClient(address=..., api_token=..., secure=...))`.
    pub fn connect(options: &ClientOptions) -> Self {
        Self::new(CloudDriveClient::new(options))
    }

    pub fn client(&self) -> &CloudDriveClient {
        &self.client
    }

    /// Calls sent and not finished yet, including those whose caller was dropped.
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    /// `aclose()`: refuse new calls, then wait until every call already sent has finished,
    /// also those whose caller is gone.
    pub async fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.in_flight.close();
        self.in_flight.wait().await;
    }

    /// `_run_sync_complete`: run one call to its end in a task of its own. If the caller's
    /// future is dropped the task keeps going and [`Self::close`] waits for it.
    async fn complete<T, F>(&self, call: F) -> Result<T, CloudDriveError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, CloudDriveError>> + Send + 'static,
    {
        // Counted before the closed check, so a `close()` that runs between the check and the
        // spawn cannot see an empty tracker and return while this call is still admitted.
        let admitted = self.in_flight.token();
        let entry = self.scope.as_ref().map(ScopeEntry::enter);
        if self.closed.load(Ordering::SeqCst) {
            return Err(CloudDriveError::Closed(CLOSED_CHANNEL));
        }
        let call = async move {
            let _counted = entry;
            call.await
        };
        let task = self.in_flight.spawn(call);
        // The task holds its own place in the tracker now.
        drop(admitted);
        match task.await {
            Ok(result) => result,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            // Only a runtime shutting down cancels the task, and then this caller is
            // being dropped with it.
            Err(error) => panic!("a CloudDrive call was cancelled by the runtime: {error}"),
        }
    }

    /// Connectivity probe: unauthenticated server ping plus one authenticated listing.
    pub async fn check(&self) -> Result<CheckResult, CloudDriveError> {
        let client = self.client.clone();
        self.complete(async move { client.get_system_info().await.map_err(CloudDriveError::from) })
            .await?;
        self.list_directory("/", true).await?;
        Ok(CheckResult {
            reachable: true,
            authenticated: true,
            version: String::new(),
        })
    }

    /// Fresh CloudDrive metadata for one API-native directory path (`force_refresh=True`
    /// unless the caller says otherwise).
    pub async fn list_directory(&self, api_dir: &str, force_refresh: bool) -> Result<Vec<CloudFile>, CloudDriveError> {
        let directory = validate_api_path(api_dir, true)?.to_owned();
        let client = self.client.clone();
        let files = self
            .complete(async move { client.get_sub_files(&directory, force_refresh).await })
            .await?;
        Ok(files.iter().map(CloudFile::from_proto).collect())
    }

    /// Fresh metadata for an exact CloudDrive API path, if it exists.
    pub async fn stat_file(&self, api_path: &str) -> Result<Option<CloudFile>, CloudDriveError> {
        let path = validate_api_path(api_path, false)?;
        let (parent, name) = posixpath::split(path);
        let files = match self.list_directory(parent, true).await {
            Ok(files) => files,
            Err(error) if error.is_file_not_found() => return Ok(None),
            Err(error) => return Err(error),
        };
        Ok(files
            .into_iter()
            .find(|file| file.name == name && file.full_path == path))
    }

    /// Ensure one direct child directory exists and verify it through a fresh listing.
    pub async fn ensure_directory(
        &self,
        parent_api_dir: &str,
        folder_name: &str,
    ) -> Result<EnsureResult, CloudDriveError> {
        let parent = validate_api_path(parent_api_dir, true)?;
        let name = validate_path_segment(folder_name)?;
        let expected_path = posixpath::join(parent, name);
        let find = |files: Vec<CloudFile>| {
            files
                .into_iter()
                .find(|file| file.full_path == expected_path && file.name == name)
        };
        if let Some(existing) = find(self.list_directory(parent, true).await?) {
            return Ok(EnsureResult {
                success: existing.is_directory,
                created: false,
                path: expected_path,
            });
        }
        let client = self.client.clone();
        let (create_parent, create_name) = (parent.to_owned(), name.to_owned());
        // Whatever the create says, the follow-up listing decides: it resolves a timeout or
        // an "already exists" race.
        let create_error = self
            .complete(async move {
                client
                    .create_folder(&create_parent, &create_name)
                    .await
                    .map_err(CloudDriveError::from)
            })
            .await
            .err();
        let created = find(self.list_directory(parent, true).await?);
        if created.as_ref().is_some_and(|file| file.is_directory) {
            return Ok(EnsureResult {
                success: true,
                created: true,
                path: expected_path,
            });
        }
        if let Some(error) = create_error {
            return Err(error);
        }
        Ok(EnsureResult {
            success: false,
            created: false,
            path: expected_path,
        })
    }

    /// Move one CloudDrive file without overwriting an existing destination.
    pub async fn move_file(
        &self,
        source_api_path: &str,
        destination_api_dir: &str,
    ) -> Result<MoveResult, CloudDriveError> {
        let source = validate_api_path(source_api_path, false)?.to_owned();
        let destination = validate_api_path(destination_api_dir, true)?.to_owned();
        let client = self.client.clone();
        let result = self
            .complete(async move {
                client
                    .move_file(&[source], &destination, MOVE_CONFLICT_SKIP)
                    .await
                    .map_err(CloudDriveError::from)
            })
            .await?;
        Ok(MoveResult {
            success: result.success,
            error_message: result.error_message,
            result_file_paths: result.result_file_paths,
        })
    }

    /// `add_offline_files(urls, dst_dir)`: CloudDrive's own answer, which callers read
    /// `success` from.
    pub async fn add_offline_files(
        &self,
        urls: &[String],
        dst_dir: &str,
    ) -> Result<proto::FileOperationResult, CloudDriveError> {
        let directory = validate_api_path(dst_dir, true)?.to_owned();
        let client = self.client.clone();
        let urls = urls.to_vec();
        self.complete(async move {
            client
                .add_offline_file(&urls, &directory)
                .await
                .map_err(CloudDriveError::from)
        })
        .await
    }

    /// Every offline task under `path`, whatever its status: the tracker's view of what
    /// CloudDrive is doing.
    pub async fn list_offline_files(&self, path: &str) -> Result<Vec<OfflineTask>, CloudDriveError> {
        let directory = validate_api_path(path, true)?.to_owned();
        let client = self.client.clone();
        let files = self
            .complete(async move {
                client
                    .list_offline_files_by_path(&directory)
                    .await
                    .map_err(CloudDriveError::from)
            })
            .await?;
        Ok(files.iter().map(OfflineTask::from_proto).collect())
    }

    /// Drop offline tasks by info hash, optionally with the data they downloaded. The path
    /// names the cloud the tasks live in; any directory of that cloud works.
    pub async fn remove_offline_files(
        &self,
        info_hashes: &[String],
        path: &str,
        delete_files: bool,
    ) -> Result<proto::FileOperationResult, CloudDriveError> {
        let directory = validate_api_path(path, true)?.to_owned();
        let client = self.client.clone();
        let hashes = info_hashes.to_vec();
        self.complete(async move {
            client
                .remove_offline_files(&hashes, &directory, delete_files)
                .await
                .map_err(CloudDriveError::from)
        })
        .await
    }

    /// Queue a background copy of one file, replacing a same-named file at the destination.
    pub async fn copy_file(
        &self,
        source_api_path: &str,
        destination_api_dir: &str,
    ) -> Result<OperationResult, CloudDriveError> {
        let source = validate_api_path(source_api_path, false)?.to_owned();
        let destination = validate_api_path(destination_api_dir, true)?.to_owned();
        let client = self.client.clone();
        let result = self
            .complete(async move {
                client
                    .copy_file(&[source], &destination, COPY_CONFLICT_OVERWRITE)
                    .await
                    .map_err(CloudDriveError::from)
            })
            .await?;
        Ok(OperationResult {
            success: result.success,
            error_message: result.error_message,
        })
    }

    /// Every copy task CloudDrive remembers, finished ones included.
    pub async fn copy_tasks(&self) -> Result<Vec<CopyTask>, CloudDriveError> {
        let client = self.client.clone();
        let tasks = self
            .complete(async move { client.get_copy_tasks().await.map_err(CloudDriveError::from) })
            .await?;
        Ok(tasks.iter().map(CopyTask::from_proto).collect())
    }

    /// `dest_path`'s entry in CloudDrive's upload queue, or `None` when it has none.
    ///
    /// A copy task stays "Scanned" with nothing uploaded while CloudDrive uploads its file,
    /// and when the upload hits a fatal error; only the upload queue tells either. The path
    /// is not validated (Python does not either): it is only matched.
    pub async fn upload_state(&self, dest_path: &str) -> Result<Option<UploadState>, CloudDriveError> {
        let client = self.client.clone();
        let filter = posixpath::basename(dest_path).to_owned();
        let files = self
            .complete(async move { client.get_upload_files(&filter).await.map_err(CloudDriveError::from) })
            .await?;
        Ok(files
            .iter()
            .find(|file| file.dest_path == dest_path)
            .map(UploadState::from_proto))
    }

    pub async fn restart_copy_task(&self, source_path: &str, dest_path: &str) -> Result<(), CloudDriveError> {
        let client = self.client.clone();
        let (source, dest) = (source_path.to_owned(), dest_path.to_owned());
        self.complete(async move {
            client
                .restart_copy_task(&source, &dest)
                .await
                .map_err(CloudDriveError::from)
        })
        .await
    }

    pub async fn cancel_copy_task(&self, source_path: &str, dest_path: &str) -> Result<(), CloudDriveError> {
        let client = self.client.clone();
        let (source, dest) = (source_path.to_owned(), dest_path.to_owned());
        self.complete(async move {
            client
                .cancel_copy_task(&source, &dest)
                .await
                .map_err(CloudDriveError::from)
        })
        .await
    }

    /// Delete files into the cloud's recycle bin.
    pub async fn delete_files(&self, api_paths: &[String]) -> Result<OperationResult, CloudDriveError> {
        let paths = api_paths
            .iter()
            .map(|path| validate_api_path(path, false).map(str::to_owned))
            .collect::<Result<Vec<_>, _>>()?;
        let client = self.client.clone();
        let result = self
            .complete(async move { client.delete_files(&paths).await.map_err(CloudDriveError::from) })
            .await?;
        Ok(OperationResult {
            success: result.success,
            error_message: result.error_message,
        })
    }
}
