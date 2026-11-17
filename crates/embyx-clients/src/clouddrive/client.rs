//! The CloudDrive gRPC client: one method per RPC, its timeout, its error mapping.
//!
//! Python origin: `src/embyx_manager/clients/clouddrive/client.py` (`CloudDriveClient`). The
//! Python client is synchronous and `aio.py` runs it in a worker thread; here every call is
//! async and [`super::AsyncCloudDrive`] runs it in a task of its own, which is what makes a
//! sent mutation finish even when its caller is dropped (contract 6, `docs/rewrite/PLAN.md`).
//!
//! What is kept from Python, because callers branch on it or users read it:
//!
//! - the deadlines: 30 s, 300 s for `MoveFile` and `DeleteFiles`, 120 s for
//!   `ListOfflineFilesByPath`; each is sent as `grpc-timeout` and enforced here around the
//!   whole call, connecting and reading a stream included, as grpcio does. An expired one is
//!   `DEADLINE_EXCEEDED` / `Deadline Exceeded`;
//! - `authorization: Bearer <token>` on every call but `GetSystemInfo`;
//! - `GetSubFiles`: `NOT_FOUND` is [`CloudDriveError::FileNotFound`], `INVALID_ARGUMENT` whose
//!   details say "can't open a file as directory" is [`CloudDriveError::NotADirectory`];
//! - the request fields Python sets, explicit-presence ones included (`conflictPolicy = 0`
//!   is sent, `checkFolderAfterSecs = 10`, `RemoveOfflineFiles.path`), info hashes lowered on
//!   removal, URLs joined with `\n`;
//! - grpcio's error text (`str(exc)`), `<_InactiveRpcError of RPC that terminated with: ...>`,
//!   for statuses a server sends. Failures of the connection itself have tonic's details
//!   (DIVERGENCES.md D40).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use embyx_core::pytext::py_repr;
use tonic::Code;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

use super::proto;
use super::proto::cloud_drive_file_srv_client::CloudDriveFileSrvClient;

/// `GRPC_TIMEOUT_SECONDS`.
pub const GRPC_TIMEOUT: Duration = Duration::from_secs(30);
/// `MOVE_TIMEOUT_SECONDS`: a cloud-side move (and a delete) is executed by the provider and
/// can take far longer than a listing.
pub const MOVE_TIMEOUT: Duration = Duration::from_mins(5);
/// `LIST_OFFLINE_TIMEOUT_SECONDS`: CloudDrive pages through the cloud's whole offline
/// history to answer.
pub const LIST_OFFLINE_TIMEOUT: Duration = Duration::from_mins(2);
/// `CHECK_FOLDER_AFTER_SECONDS`: how soon CloudDrive re-checks the destination folder after
/// an offline task is added. Zero would leave a persistently cached folder (115) stale.
pub const CHECK_FOLDER_AFTER_SECONDS: u64 = 10;

/// `MoveFileRequest.ConflictPolicy` / `CopyFileRequest.ConflictPolicy` numbers.
pub const CONFLICT_OVERWRITE: i32 = 0;
pub const CONFLICT_RENAME: i32 = 1;
pub const CONFLICT_SKIP: i32 = 2;

/// The three deadlines. Production uses [`Timeouts::default`] (Python's constants); tests
/// shorten them to see an expiry without waiting half a minute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeouts {
    pub general: Duration,
    pub move_or_delete: Duration,
    pub list_offline: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            general: GRPC_TIMEOUT,
            move_or_delete: MOVE_TIMEOUT,
            list_offline: LIST_OFFLINE_TIMEOUT,
        }
    }
}

/// grpcio's `StatusCode` member name for a code (`StatusCode.NOT_FOUND`).
pub fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

/// The code for a grpcio `StatusCode` member name, the inverse of [`code_name`].
pub fn code_by_name(name: &str) -> Option<Code> {
    const ALL: [Code; 17] = [
        Code::Ok,
        Code::Cancelled,
        Code::Unknown,
        Code::InvalidArgument,
        Code::DeadlineExceeded,
        Code::NotFound,
        Code::AlreadyExists,
        Code::PermissionDenied,
        Code::ResourceExhausted,
        Code::FailedPrecondition,
        Code::Aborted,
        Code::OutOfRange,
        Code::Unimplemented,
        Code::Internal,
        Code::Unavailable,
        Code::DataLoss,
        Code::Unauthenticated,
    ];
    ALL.into_iter().find(|code| code_name(*code) == name)
}

/// A failed RPC: grpcio's `grpc.RpcError` (`exc.code()`, `exc.details()`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcError {
    pub code: Code,
    /// `exc.details()`: the server's `grpc-message`, or the client's own words.
    pub details: String,
    /// grpcio's `debug_error_string`: `NOT_FOUND:<details>` for a status the server sent or
    /// a deadline, empty for a call refused before it was sent.
    pub debug: String,
    /// A server-streaming call (`GetSubFiles`) fails with grpcio's `_MultiThreadedRendezvous`,
    /// a unary one with `_InactiveRpcError`.
    pub streaming: bool,
}

impl RpcError {
    fn new(code: Code, details: impl Into<String>, streaming: bool) -> Self {
        let details = details.into();
        Self {
            debug: format!("{}:{details}", code_name(code)),
            code,
            details,
            streaming,
        }
    }

    fn deadline(streaming: bool) -> Self {
        Self::new(Code::DeadlineExceeded, "Deadline Exceeded", streaming)
    }

    /// grpcio refuses a metadata value outside printable ASCII before sending anything.
    fn invalid_metadata(streaming: bool) -> Self {
        Self {
            code: Code::Internal,
            details: "Invalid metadata".to_owned(),
            debug: String::new(),
            streaming,
        }
    }

    /// A tonic status, as grpcio would report the same failure.
    fn from_status(status: &tonic::Status, streaming: bool) -> Self {
        // tonic's own deadline layer (the `grpc-timeout` it reads back) says CANCELLED.
        if status.code() == Code::Cancelled && status.message() == "Timeout expired" {
            return Self::deadline(streaming);
        }
        // A status tonic made from a transport failure carries the failure as its source;
        // grpcio reports every such failure as UNAVAILABLE.
        if std::error::Error::source(status).is_some() && status.code() == Code::Unknown {
            return Self::new(Code::Unavailable, status.message(), streaming);
        }
        Self::new(status.code(), status.message(), streaming)
    }

    /// `type(exc).__name__`.
    pub fn class_name(&self) -> &'static str {
        if self.streaming {
            "_MultiThreadedRendezvous"
        } else {
            "_InactiveRpcError"
        }
    }
}

impl fmt::Display for RpcError {
    /// `str(exc)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "<{} of RPC that terminated with:\n\tstatus = StatusCode.{}\n\tdetails = \"{}\"\n\tdebug_error_string = \"{}\"\n>",
            self.class_name(),
            code_name(self.code),
            self.details,
            self.debug
        )
    }
}

impl std::error::Error for RpcError {}

/// What a CloudDrive call raises, by the Python exception it stands for.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CloudDriveError {
    /// `grpc.RpcError`.
    #[error("{0}")]
    Rpc(RpcError),
    /// `FileNotFoundError(errno.ENOENT, message, path)` from `get_sub_files`.
    #[error("[Errno 2] {message}: {}", py_repr(.path))]
    FileNotFound { message: String, path: String },
    /// `NotADirectoryError(errno.ENOTDIR, details, path)` from `get_sub_files`.
    #[error("[Errno 20] {message}: {}", py_repr(.path))]
    NotADirectory { message: String, path: String },
    /// `ValueError` from the wrapper's path validation.
    #[error("{0}")]
    InvalidPath(&'static str),
    /// `ValueError` from a call on a closed channel (after `aclose()`).
    #[error("{0}")]
    Closed(&'static str),
}

impl CloudDriveError {
    /// `type(exc).__name__`.
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Rpc(error) => error.class_name(),
            Self::FileNotFound { .. } => "FileNotFoundError",
            Self::NotADirectory { .. } => "NotADirectoryError",
            Self::InvalidPath(_) | Self::Closed(_) => "ValueError",
        }
    }

    /// The RPC error, for callers that catch `grpc.RpcError`.
    pub fn as_rpc(&self) -> Option<&RpcError> {
        match self {
            Self::Rpc(error) => Some(error),
            _ => None,
        }
    }

    /// `exc.details()` of an RPC error, `None` for the other kinds.
    pub fn details(&self) -> Option<&str> {
        self.as_rpc().map(|error| error.details.as_str())
    }

    /// `isinstance(exc, FileNotFoundError)`.
    pub fn is_file_not_found(&self) -> bool {
        matches!(self, Self::FileNotFound { .. })
    }
}

impl From<RpcError> for CloudDriveError {
    fn from(error: RpcError) -> Self {
        Self::Rpc(error)
    }
}

/// How to reach one CloudDrive server.
#[derive(Clone)]
pub struct ClientOptions {
    /// `host:port`, as the `clouddrive.address` setting holds it.
    pub address: String,
    pub api_token: String,
    /// TLS (`grpc.secure_channel` with grpcio's bundled roots, here the same Mozilla set
    /// from `webpki-roots`) or plain HTTP/2 (`grpc.insecure_channel`).
    pub secure: bool,
    pub timeouts: Timeouts,
    /// Trust only this PEM authority instead of the bundled roots (tests).
    pub tls_roots_pem: Option<Vec<u8>>,
}

impl ClientOptions {
    pub fn new(address: impl Into<String>, api_token: impl Into<String>, secure: bool) -> Self {
        Self {
            address: address.into(),
            api_token: api_token.into(),
            secure,
            timeouts: Timeouts::default(),
            tls_roots_pem: None,
        }
    }
}

impl fmt::Debug for ClientOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientOptions")
            .field("address", &self.address)
            .field("api_token", &"<redacted>")
            .field("secure", &self.secure)
            .field("timeouts", &self.timeouts)
            .field("tls_roots_pem", &self.tls_roots_pem.as_ref().map(Vec::len))
            .finish()
    }
}

/// gRPC client for one CloudDrive server. Cheap to clone: clones share the channel.
#[derive(Clone)]
pub struct CloudDriveClient {
    /// `None` when the address cannot be a URI; every call then fails as UNAVAILABLE, the
    /// way grpcio's lazily connecting channel fails on its first call.
    stub: Result<CloudDriveFileSrvClient<Channel>, Arc<str>>,
    authorization: Arc<str>,
    timeouts: Timeouts,
}

impl fmt::Debug for CloudDriveClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudDriveClient")
            .field("connected", &self.stub.is_ok())
            .field("timeouts", &self.timeouts)
            .finish_non_exhaustive()
    }
}

fn endpoint(options: &ClientOptions) -> Result<Endpoint, String> {
    let scheme = if options.secure { "https" } else { "http" };
    let endpoint = Endpoint::from_shared(format!("{scheme}://{}", options.address))
        .map_err(|error| format!("invalid CloudDrive address: {error}"))?;
    if !options.secure {
        return Ok(endpoint);
    }
    let tls = match &options.tls_roots_pem {
        Some(pem) => ClientTlsConfig::new().ca_certificate(Certificate::from_pem(pem)),
        None => ClientTlsConfig::new().with_webpki_roots(),
    };
    endpoint
        .tls_config(tls)
        .map_err(|error| format!("invalid CloudDrive TLS setup: {error}"))
}

impl CloudDriveClient {
    /// `CloudDriveClient(address=..., api_token=..., secure=...)`. Connects lazily, on the
    /// first call, as grpcio's channel does; never fails here. Must be called inside a Tokio
    /// runtime (the channel's worker is spawned on it).
    pub fn new(options: &ClientOptions) -> Self {
        let stub = endpoint(options)
            .map(|endpoint| CloudDriveFileSrvClient::new(endpoint.connect_lazy()))
            .map_err(Arc::from);
        Self {
            stub,
            authorization: Arc::from(format!("Bearer {}", options.api_token)),
            timeouts: options.timeouts,
        }
    }

    pub fn timeouts(&self) -> Timeouts {
        self.timeouts
    }

    fn stub(&self, streaming: bool) -> Result<CloudDriveFileSrvClient<Channel>, RpcError> {
        self.stub
            .clone()
            .map_err(|message| RpcError::new(Code::Unavailable, message.as_ref(), streaming))
    }

    /// A request with its deadline and, unless `anonymous`, the bearer token.
    fn request<T>(
        &self,
        message: T,
        timeout: Duration,
        anonymous: bool,
        streaming: bool,
    ) -> Result<tonic::Request<T>, RpcError> {
        let mut request = tonic::Request::new(message);
        request.set_timeout(timeout);
        if !anonymous {
            // grpcio accepts printable ASCII only and refuses the call before sending it.
            if !self.authorization.bytes().all(|byte| (0x20..=0x7e).contains(&byte)) {
                return Err(RpcError::invalid_metadata(streaming));
            }
            let value = MetadataValue::try_from(self.authorization.as_ref())
                .map_err(|_| RpcError::invalid_metadata(streaming))?;
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }

    /// One unary call under `timeout`, connecting included.
    async fn unary<T, R, F>(&self, message: T, timeout: Duration, anonymous: bool, call: F) -> Result<R, RpcError>
    where
        F: AsyncFnOnce(
            CloudDriveFileSrvClient<Channel>,
            tonic::Request<T>,
        ) -> Result<tonic::Response<R>, tonic::Status>,
    {
        let stub = self.stub(false)?;
        let request = self.request(message, timeout, anonymous, false)?;
        match tokio::time::timeout(timeout, call(stub, request)).await {
            Err(_) => Err(RpcError::deadline(false)),
            Ok(Err(status)) => Err(RpcError::from_status(&status, false)),
            Ok(Ok(response)) => Ok(response.into_inner()),
        }
    }

    /// `get_system_info`: unauthenticated server ping.
    pub async fn get_system_info(&self) -> Result<proto::CloudDriveSystemInfo, RpcError> {
        self.unary((), self.timeouts.general, true, async |mut stub, request| {
            stub.get_system_info(request).await
        })
        .await
    }

    /// `get_sub_files`: every entry of the streamed listing.
    pub async fn get_sub_files(
        &self,
        path: &str,
        force_refresh: bool,
    ) -> Result<Vec<proto::CloudDriveFile>, CloudDriveError> {
        let timeout = self.timeouts.general;
        let message = proto::ListSubFileRequest {
            path: path.to_owned(),
            force_refresh,
            ..Default::default()
        };
        let mut stub = self.stub(true)?;
        let request = self.request(message, timeout, false, true)?;
        let listing = async move {
            let mut stream = stub.get_sub_files(request).await?.into_inner();
            let mut files = Vec::new();
            while let Some(reply) = stream.message().await? {
                files.extend(reply.sub_files);
            }
            Ok::<_, tonic::Status>(files)
        };
        let error = match tokio::time::timeout(timeout, listing).await {
            Ok(Ok(files)) => return Ok(files),
            Ok(Err(status)) => RpcError::from_status(&status, true),
            Err(_) => RpcError::deadline(true),
        };
        Err(match error.code {
            Code::NotFound => CloudDriveError::FileNotFound {
                message: if error.details.is_empty() {
                    format!("CloudDrive path not found: \"{path}\"")
                } else {
                    error.details
                },
                path: path.to_owned(),
            },
            Code::InvalidArgument if error.details.contains("can't open a file as directory") => {
                CloudDriveError::NotADirectory {
                    message: error.details,
                    path: path.to_owned(),
                }
            }
            _ => CloudDriveError::Rpc(error),
        })
    }

    pub async fn create_folder(
        &self,
        parent_path: &str,
        folder_name: &str,
    ) -> Result<proto::CreateFolderResult, RpcError> {
        let message = proto::CreateFolderRequest {
            parent_path: parent_path.to_owned(),
            folder_name: folder_name.to_owned(),
        };
        self.unary(message, self.timeouts.general, false, async |mut stub, request| {
            stub.create_folder(request).await
        })
        .await
    }

    pub async fn rename_file(&self, file_path: &str, new_name: &str) -> Result<proto::FileOperationResult, RpcError> {
        let message = proto::RenameFileRequest {
            the_file_path: file_path.to_owned(),
            new_name: new_name.to_owned(),
        };
        self.unary(message, self.timeouts.general, false, async |mut stub, request| {
            stub.rename_file(request).await
        })
        .await
    }

    /// `conflict_policy`: [`CONFLICT_OVERWRITE`], [`CONFLICT_RENAME`] or [`CONFLICT_SKIP`].
    pub async fn move_file(
        &self,
        source_paths: &[String],
        dest_path: &str,
        conflict_policy: i32,
    ) -> Result<proto::FileOperationResult, RpcError> {
        let message = proto::MoveFileRequest {
            the_file_paths: source_paths.to_vec(),
            dest_path: dest_path.to_owned(),
            conflict_policy: Some(conflict_policy),
            ..Default::default()
        };
        self.unary(
            message,
            self.timeouts.move_or_delete,
            false,
            async |mut stub, request| stub.move_file(request).await,
        )
        .await
    }

    /// Queue a copy task; CloudDrive runs it in the background.
    pub async fn copy_file(
        &self,
        source_paths: &[String],
        dest_path: &str,
        conflict_policy: i32,
    ) -> Result<proto::FileOperationResult, RpcError> {
        let message = proto::CopyFileRequest {
            the_file_paths: source_paths.to_vec(),
            dest_path: dest_path.to_owned(),
            conflict_policy: Some(conflict_policy),
            ..Default::default()
        };
        self.unary(message, self.timeouts.general, false, async |mut stub, request| {
            stub.copy_file(request).await
        })
        .await
    }

    pub async fn get_copy_tasks(&self) -> Result<Vec<proto::CopyTask>, RpcError> {
        let result = self
            .unary((), self.timeouts.general, false, async |mut stub, request| {
                stub.get_copy_tasks(request).await
            })
            .await?;
        Ok(result.copy_tasks)
    }

    /// Files in CloudDrive's upload queue whose name contains `name_filter`.
    pub async fn get_upload_files(&self, name_filter: &str) -> Result<Vec<proto::UploadFileInfo>, RpcError> {
        let message = proto::GetUploadFileListRequest {
            get_all: true,
            filter: name_filter.to_owned(),
            ..Default::default()
        };
        let result = self
            .unary(message, self.timeouts.general, false, async |mut stub, request| {
                stub.get_upload_file_list(request).await
            })
            .await?;
        Ok(result.upload_files)
    }

    pub async fn restart_copy_task(&self, source_path: &str, dest_path: &str) -> Result<(), RpcError> {
        let message = proto::CopyTaskRequest {
            source_path: source_path.to_owned(),
            dest_path: dest_path.to_owned(),
        };
        self.unary(message, self.timeouts.general, false, async |mut stub, request| {
            stub.restart_copy_task(request).await
        })
        .await
    }

    pub async fn cancel_copy_task(&self, source_path: &str, dest_path: &str) -> Result<(), RpcError> {
        let message = proto::CopyTaskRequest {
            source_path: source_path.to_owned(),
            dest_path: dest_path.to_owned(),
        };
        self.unary(message, self.timeouts.general, false, async |mut stub, request| {
            stub.cancel_copy_task(request).await
        })
        .await
    }

    /// Delete into the cloud's recycle bin, not permanently.
    pub async fn delete_files(&self, paths: &[String]) -> Result<proto::FileOperationResult, RpcError> {
        let message = proto::MultiFileRequest { path: paths.to_vec() };
        self.unary(
            message,
            self.timeouts.move_or_delete,
            false,
            async |mut stub, request| stub.delete_files(request).await,
        )
        .await
    }

    /// `add_offline_file(urls, dst_dir)` with the default `check_folder_after_secs`.
    pub async fn add_offline_file(
        &self,
        urls: &[String],
        dst_dir: &str,
    ) -> Result<proto::FileOperationResult, RpcError> {
        self.add_offline_file_checking_after(urls, dst_dir, CHECK_FOLDER_AFTER_SECONDS)
            .await
    }

    pub async fn add_offline_file_checking_after(
        &self,
        urls: &[String],
        dst_dir: &str,
        check_folder_after_secs: u64,
    ) -> Result<proto::FileOperationResult, RpcError> {
        let message = proto::AddOfflineFileRequest {
            urls: urls.join("\n"),
            to_folder: dst_dir.to_owned(),
            check_folder_after_secs: Some(check_folder_after_secs),
        };
        self.unary(message, self.timeouts.general, false, async |mut stub, request| {
            stub.add_offline_files(request).await
        })
        .await
    }

    /// Every offline task under `path`, whatever its status.
    pub async fn list_offline_files_by_path(&self, path: &str) -> Result<Vec<proto::OfflineFile>, RpcError> {
        let message = proto::FileRequest {
            path: path.to_owned(),
            ..Default::default()
        };
        let result = self
            .unary(message, self.timeouts.list_offline, false, async |mut stub, request| {
                stub.list_offline_files_by_path(request).await
            })
            .await?;
        Ok(result.offline_files)
    }

    /// Drop offline tasks by info hash; the path names the cloud they live in.
    ///
    /// CloudDrive matches the hashes case-sensitively against the lower-case ones it
    /// lists, and answers success for a hash that matches nothing, so the upper-case form
    /// the ledger keys on is lowered here (`str.lower()`).
    pub async fn remove_offline_files(
        &self,
        info_hashes: &[String],
        path: &str,
        delete_files: bool,
    ) -> Result<proto::FileOperationResult, RpcError> {
        let message = proto::RemoveOfflineFilesRequest {
            info_hashes: info_hashes.iter().map(|hash| hash.to_lowercase()).collect(),
            path: Some(path.to_owned()),
            delete_files,
            ..Default::default()
        };
        self.unary(message, self.timeouts.general, false, async |mut stub, request| {
            stub.remove_offline_files(request).await
        })
        .await
    }
}
