//! The fake CloudDrive gRPC server of the effect transcripts, Rust side
//! (docs/rewrite/SCENARIOS.md, "Fakes").
//!
//! Port of `tools/scenarios/fakes/clouddrive.py`, rule for rule: the same seed, the same
//! state, the same call records (protobuf's JSON mapping, as `MessageToDict` with
//! `preserving_proto_field_name` and `always_print_fields_with_no_presence`), the same
//! dump. WP0 ports `GetSystemInfo`, `GetSubFiles` and `MoveFile`, which the harness
//! scenario and the tonic probe use; every other RPC answers UNIMPLEMENTED until WP3 ports
//! the rest of the Python fake.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};

use embyx_core::pyjson::{Dict, Value};
use embyx_core::scenario::Recorder;
use futures_util::stream;
use prost_reflect::{DescriptorPool, DynamicMessage, SerializeOptions};
use tonic::{Code, Request, Response, Status};

use super::proto::cloud_drive_file_srv_server::{CloudDriveFileSrv, CloudDriveFileSrvServer};
use super::proto::{self, move_file_request::ConflictPolicy};

/// The descriptor the stubs were generated from (`tools/codegen/clouddrive_descriptor.py`).
const DESCRIPTOR: &[u8] = include_bytes!("../../proto/clouddrive.fds.bin");

fn pool() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| DescriptorPool::decode(DESCRIPTOR).expect("the CloudDrive descriptor decodes"))
}

/// `fakes.clouddrive.to_json`: protobuf's JSON mapping with proto field names, fields at
/// their default value included (but not unset `optional` ones), enums by name, 64-bit
/// integers as strings.
pub fn to_json<M: prost::Message>(message: &M, full_name: &str) -> Value {
    bytes_to_json(full_name, &message.encode_to_vec())
}

/// [`to_json`] of a message's serialized bytes.
pub fn bytes_to_json(full_name: &str, bytes: &[u8]) -> Value {
    let descriptor = pool()
        .get_message_by_name(full_name)
        .unwrap_or_else(|| panic!("no message {full_name} in the CloudDrive descriptor"));
    let dynamic = DynamicMessage::decode(descriptor, bytes).expect("decode");
    let options = SerializeOptions::new()
        .use_proto_field_name(true)
        .skip_default_fields(false)
        .use_enum_numbers(false)
        .stringify_64_bit_integers(true);
    let json = dynamic
        .serialize_with_options(serde_json::value::Serializer, &options)
        .expect("JSON mapping");
    Value::from_serde(&json)
}

#[derive(Debug, Clone)]
struct FileEntry {
    size: i64,
    /// As seeded: TOML keys are strings, `fileHashes` keys are `uint32`.
    hashes: Dict,
}

#[derive(Debug)]
struct State {
    directories: BTreeSet<String>,
    files: BTreeMap<String, FileEntry>,
    offline_tasks: Vec<Value>,
    copy_tasks: Vec<Value>,
    uploads: Vec<Value>,
    errors: Vec<Dict>,
}

/// `posixpath.dirname`.
fn dirname(path: &str) -> &str {
    match path.rfind('/') {
        None => "",
        Some(index) => {
            let head = &path[..=index];
            let trimmed = head.trim_end_matches('/');
            if trimmed.is_empty() { head } else { trimmed }
        }
    }
}

/// `posixpath.basename`.
fn basename(path: &str) -> &str {
    path.rsplit_once('/').map_or(path, |(_, name)| name)
}

/// `posixpath.join` of two parts.
fn join(head: &str, tail: &str) -> String {
    if tail.starts_with('/') {
        tail.to_owned()
    } else if head.is_empty() || head.ends_with('/') {
        format!("{head}{tail}")
    } else {
        format!("{head}/{tail}")
    }
}

fn list_of(section: &Value, key: &str) -> Vec<Value> {
    section
        .get(key)
        .and_then(Value::as_list)
        .map(<[Value]>::to_vec)
        .unwrap_or_default()
}

impl State {
    fn new(seed: &Value) -> Self {
        let mut directories: BTreeSet<String> = ["/".to_owned()].into_iter().collect();
        for directory in list_of(seed, "directories") {
            directories.insert(directory.as_str().expect("directories are strings").to_owned());
        }
        let mut files = BTreeMap::new();
        for entry in list_of(seed, "files") {
            let path = entry
                .get("path")
                .and_then(Value::as_str)
                .expect("a seeded file has a path")
                .to_owned();
            let size = match entry.get("size") {
                Some(Value::Int(size)) => i64::try_from(*size).expect("size fits i64"),
                None => 0,
                Some(other) => panic!("size must be an integer, not {other:?}"),
            };
            let hashes = entry
                .get("hashes")
                .and_then(Value::as_dict)
                .cloned()
                .unwrap_or_default();
            files.insert(path, FileEntry { size, hashes });
        }
        let errors = list_of(seed, "errors")
            .into_iter()
            .map(|error| error.as_dict().expect("errors are tables").clone())
            .collect();
        let mut state = Self {
            directories,
            files,
            offline_tasks: list_of(seed, "offline_tasks"),
            copy_tasks: list_of(seed, "copy_tasks"),
            uploads: list_of(seed, "uploads"),
            errors,
        };
        let paths: Vec<String> = state.files.keys().cloned().collect();
        for path in paths {
            state.add_parents(&path);
        }
        state
    }

    fn add_parents(&mut self, path: &str) {
        let mut parent = dirname(path).to_owned();
        while !self.directories.contains(&parent) {
            let next = dirname(&parent).to_owned();
            assert_ne!(next, parent, "fake CloudDrive paths are absolute: {path:?}");
            self.directories.insert(parent);
            parent = next;
        }
    }

    fn exists(&self, path: &str) -> bool {
        self.directories.contains(path) || self.files.contains_key(path)
    }

    fn entry(&self, path: &str) -> proto::CloudDriveFile {
        let name = basename(path).to_owned();
        if self.directories.contains(path) {
            return proto::CloudDriveFile {
                id: path.to_owned(),
                name,
                full_path_name: path.to_owned(),
                is_directory: true,
                ..Default::default()
            };
        }
        let file = &self.files[path];
        let file_hashes = file
            .hashes
            .iter()
            .map(|(kind, value)| {
                let kind = match kind {
                    embyx_core::pyjson::Key::Str(text) => text.parse().expect("hash kinds are integers"),
                    other => panic!("hash kind {other:?}"),
                };
                (kind, value.as_str().expect("hashes are strings").to_owned())
            })
            .collect();
        proto::CloudDriveFile {
            id: path.to_owned(),
            name,
            full_path_name: path.to_owned(),
            size: file.size,
            file_hashes,
            ..Default::default()
        }
    }

    /// `_children`: sorted paths whose parent is `directory`.
    fn children(&self, directory: &str) -> Vec<String> {
        let mut paths: Vec<String> = self
            .directories
            .iter()
            .chain(self.files.keys())
            .filter(|path| path.as_str() != "/" && dirname(path) == directory)
            .cloned()
            .collect();
        paths.sort();
        paths
    }

    fn remove(&mut self, path: &str) {
        let prefix = format!("{}/", path.trim_end_matches('/'));
        self.files.retain(|key, _| key != path && !key.starts_with(&prefix));
        self.directories.retain(|key| key != path && !key.starts_with(&prefix));
    }

    fn relocate(&mut self, source: &str, target: &str) {
        let prefix = format!("{source}/");
        let moved = |key: &str| {
            if key == source || key.starts_with(&prefix) {
                format!("{target}{}", &key[source.len()..])
            } else {
                key.to_owned()
            }
        };
        self.files = std::mem::take(&mut self.files)
            .into_iter()
            .map(|(key, value)| (moved(&key), value))
            .collect();
        self.directories = std::mem::take(&mut self.directories)
            .iter()
            .map(|key| moved(key))
            .collect();
    }

    fn dump(&self) -> Value {
        let files = self
            .files
            .iter()
            .map(|(path, file)| {
                let mut entry = Dict::new();
                entry.insert("path", Value::from(path.as_str()));
                entry.insert("size", Value::Int(i128::from(file.size)));
                entry.insert("hashes", Value::Dict(file.hashes.clone()));
                Value::Dict(entry)
            })
            .collect();
        let mut dump = Dict::new();
        dump.insert(
            "directories",
            Value::List(self.directories.iter().map(|path| Value::from(path.as_str())).collect()),
        );
        dump.insert("files", Value::List(files));
        dump.insert("offline_tasks", Value::List(self.offline_tasks.clone()));
        dump.insert("copy_tasks", Value::List(self.copy_tasks.clone()));
        dump.insert("uploads", Value::List(self.uploads.clone()));
        Value::Dict(dump)
    }

    /// The next scripted failure of `method`, counting it down.
    fn scripted_error(&mut self, method: &str) -> Option<(String, String)> {
        let error = self
            .errors
            .iter_mut()
            .find(|error| error.get("method").and_then(Value::as_str) == Some(method) && times(error) > 0)?;
        let left = times(error) - 1;
        error.insert("times", Value::Int(left));
        let code = error
            .get("code")
            .and_then(Value::as_str)
            .expect("a scripted error has a code")
            .to_owned();
        let details = error
            .get("details")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        Some((code, details))
    }
}

fn times(error: &Dict) -> i128 {
    match error.get("times") {
        Some(Value::Int(times)) => *times,
        _ => 1,
    }
}

/// gRPC's status code by its Python name (`grpc.StatusCode.<NAME>`).
fn code_by_name(name: &str) -> Code {
    match name {
        "CANCELLED" => Code::Cancelled,
        "UNKNOWN" => Code::Unknown,
        "INVALID_ARGUMENT" => Code::InvalidArgument,
        "DEADLINE_EXCEEDED" => Code::DeadlineExceeded,
        "NOT_FOUND" => Code::NotFound,
        "ALREADY_EXISTS" => Code::AlreadyExists,
        "PERMISSION_DENIED" => Code::PermissionDenied,
        "RESOURCE_EXHAUSTED" => Code::ResourceExhausted,
        "FAILED_PRECONDITION" => Code::FailedPrecondition,
        "ABORTED" => Code::Aborted,
        "OUT_OF_RANGE" => Code::OutOfRange,
        "UNIMPLEMENTED" => Code::Unimplemented,
        "INTERNAL" => Code::Internal,
        "UNAVAILABLE" => Code::Unavailable,
        "DATA_LOSS" => Code::DataLoss,
        "UNAUTHENTICATED" => Code::Unauthenticated,
        other => panic!("unknown gRPC status {other}"),
    }
}

/// `_Abort`: a gRPC error with Python's code name and details.
struct Abort {
    code: &'static str,
    details: String,
}

/// The fake's state and recorder; [`FakeCloudDrive::serve`] puts it on a local port.
#[derive(Clone)]
pub struct FakeCloudDrive {
    state: Arc<Mutex<State>>,
    recorder: Recorder,
}

impl std::fmt::Debug for FakeCloudDrive {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("FakeCloudDrive").finish_non_exhaustive()
    }
}

/// A running fake: its address and a handle to its state.
#[derive(Debug)]
pub struct Served {
    pub fake: FakeCloudDrive,
    pub address: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeCloudDrive {
    /// The fake seeded from a spec's `clouddrive` section.
    pub fn new(seed: &Value, recorder: Recorder) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new(seed))),
            recorder,
        }
    }

    /// `FakeCloudDrive.dump`.
    pub fn dump(&self) -> Value {
        self.state.lock().expect("fake state").dump()
    }

    /// Serve on `127.0.0.1:0`, plain HTTP/2 (`CloudDriveClient(..., secure=False)`).
    pub async fn serve(self) -> Served {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let service = CloudDriveFileSrvServer::new(self.clone());
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await
                .expect("fake CloudDrive server");
        });
        Served {
            fake: self,
            address,
            task,
        }
    }

    /// `_call`: record the request, then the response or the error, under one lock.
    fn call<Q: prost::Message, R: prost::Message>(
        &self,
        method: &str,
        request: &Q,
        request_type: &str,
        response_type: &str,
        handle: impl FnOnce(&mut State, &Q) -> Result<Vec<R>, Abort>,
        streaming: bool,
    ) -> Result<Vec<R>, Status> {
        let mut state = self.state.lock().expect("fake state");
        let mut record = Dict::new();
        record.insert("fake", Value::from("clouddrive"));
        record.insert("method", Value::from(method));
        record.insert("request", to_json(request, request_type));
        if let Some((code, details)) = state.scripted_error(method) {
            record.insert("error", error_record(&code, &details));
            self.recorder.record(Value::Dict(record));
            return Err(Status::new(code_by_name(&code), details));
        }
        match handle(&mut state, request) {
            Err(abort) => {
                record.insert("error", error_record(abort.code, &abort.details));
                self.recorder.record(Value::Dict(record));
                Err(Status::new(code_by_name(abort.code), abort.details))
            }
            Ok(responses) => {
                let response = if streaming {
                    Value::List(responses.iter().map(|item| to_json(item, response_type)).collect())
                } else {
                    to_json(&responses[0], response_type)
                };
                record.insert("response", response);
                self.recorder.record(Value::Dict(record));
                Ok(responses)
            }
        }
    }

    fn unary<Q: prost::Message, R: prost::Message>(
        &self,
        method: &str,
        request: &Q,
        types: (&str, &str),
        handle: impl FnOnce(&mut State, &Q) -> Result<R, Abort>,
    ) -> Result<Response<R>, Status> {
        let mut responses = self.call(
            method,
            request,
            types.0,
            types.1,
            |state, request| handle(state, request).map(|item| vec![item]),
            false,
        )?;
        Ok(Response::new(responses.remove(0)))
    }
}

fn error_record(code: &str, details: &str) -> Value {
    let mut error = Dict::new();
    error.insert("code", Value::from(code));
    error.insert("details", Value::from(details));
    Value::Dict(error)
}

fn not_ported(method: &str) -> Status {
    Status::unimplemented(format!("the Rust fake CloudDrive does not serve {method} yet (WP3)"))
}

type SubFilesStream = stream::Iter<std::vec::IntoIter<Result<proto::SubFilesReply, Status>>>;

#[tonic::async_trait]
impl CloudDriveFileSrv for FakeCloudDrive {
    type GetSubFilesStream = SubFilesStream;

    async fn get_system_info(&self, _request: Request<()>) -> Result<Response<proto::CloudDriveSystemInfo>, Status> {
        self.unary(
            "GetSystemInfo",
            &(),
            ("google.protobuf.Empty", "clouddrive.CloudDriveSystemInfo"),
            |_, ()| {
                Ok(proto::CloudDriveSystemInfo {
                    is_login: true,
                    user_name: "fake".to_owned(),
                    system_ready: true,
                    ..Default::default()
                })
            },
        )
    }

    async fn get_sub_files(
        &self,
        request: Request<proto::ListSubFileRequest>,
    ) -> Result<Response<Self::GetSubFilesStream>, Status> {
        let request = request.into_inner();
        let replies = self.call(
            "GetSubFiles",
            &request,
            "clouddrive.ListSubFileRequest",
            "clouddrive.SubFilesReply",
            |state, request| {
                if state.files.contains_key(&request.path) {
                    return Err(Abort {
                        code: "INVALID_ARGUMENT",
                        details: format!("can't open a file as directory: {}", request.path),
                    });
                }
                if !state.directories.contains(&request.path) {
                    return Err(Abort {
                        code: "NOT_FOUND",
                        details: format!("path not found: {}", request.path),
                    });
                }
                let sub_files = state
                    .children(&request.path)
                    .iter()
                    .map(|path| state.entry(path))
                    .collect();
                Ok(vec![proto::SubFilesReply { sub_files }])
            },
            true,
        )?;
        Ok(Response::new(stream::iter(
            replies.into_iter().map(Ok).collect::<Vec<_>>(),
        )))
    }

    async fn move_file(
        &self,
        request: Request<proto::MoveFileRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        let request = request.into_inner();
        self.unary(
            "MoveFile",
            &request,
            ("clouddrive.MoveFileRequest", "clouddrive.FileOperationResult"),
            |state, request| {
                if !state.directories.contains(&request.dest_path) {
                    return Ok(proto::FileOperationResult {
                        success: false,
                        error_message: format!("destination not found: {}", request.dest_path),
                        result_file_paths: Vec::new(),
                    });
                }
                let mut moved = Vec::new();
                for source in &request.the_file_paths {
                    if !state.exists(source) {
                        return Ok(proto::FileOperationResult {
                            success: false,
                            error_message: format!("file not found: {source}"),
                            result_file_paths: moved,
                        });
                    }
                    let target = join(&request.dest_path, basename(source));
                    if state.exists(&target) {
                        // An unset policy reads as its default, Overwrite (0), as in Python.
                        match request.conflict_policy() {
                            ConflictPolicy::Skip => continue,
                            ConflictPolicy::Overwrite => state.remove(&target),
                            ConflictPolicy::Rename => {
                                return Err(Abort {
                                    code: "UNIMPLEMENTED",
                                    details: "the fake only overwrites or skips on a conflict".to_owned(),
                                });
                            }
                        }
                    }
                    state.relocate(source, &target);
                    moved.push(target);
                }
                Ok(proto::FileOperationResult {
                    success: true,
                    error_message: String::new(),
                    result_file_paths: moved,
                })
            },
        )
    }

    async fn create_folder(
        &self,
        _request: Request<proto::CreateFolderRequest>,
    ) -> Result<Response<proto::CreateFolderResult>, Status> {
        Err(not_ported("CreateFolder"))
    }

    async fn rename_file(
        &self,
        _request: Request<proto::RenameFileRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        Err(not_ported("RenameFile"))
    }

    async fn copy_file(
        &self,
        _request: Request<proto::CopyFileRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        Err(not_ported("CopyFile"))
    }

    async fn delete_files(
        &self,
        _request: Request<proto::MultiFileRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        Err(not_ported("DeleteFiles"))
    }

    async fn add_offline_files(
        &self,
        _request: Request<proto::AddOfflineFileRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        Err(not_ported("AddOfflineFiles"))
    }

    async fn remove_offline_files(
        &self,
        _request: Request<proto::RemoveOfflineFilesRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        Err(not_ported("RemoveOfflineFiles"))
    }

    async fn list_offline_files_by_path(
        &self,
        _request: Request<proto::FileRequest>,
    ) -> Result<Response<proto::OfflineFileListResult>, Status> {
        Err(not_ported("ListOfflineFilesByPath"))
    }

    async fn get_upload_file_list(
        &self,
        _request: Request<proto::GetUploadFileListRequest>,
    ) -> Result<Response<proto::GetUploadFileListResult>, Status> {
        Err(not_ported("GetUploadFileList"))
    }

    async fn get_copy_tasks(&self, _request: Request<()>) -> Result<Response<proto::GetCopyTaskResult>, Status> {
        Err(not_ported("GetCopyTasks"))
    }

    async fn cancel_copy_task(&self, _request: Request<proto::CopyTaskRequest>) -> Result<Response<()>, Status> {
        Err(not_ported("CancelCopyTask"))
    }

    async fn restart_copy_task(&self, _request: Request<proto::CopyTaskRequest>) -> Result<Response<()>, Status> {
        Err(not_ported("RestartCopyTask"))
    }
}

#[cfg(test)]
mod tests {
    use super::{basename, dirname, join};

    #[test]
    fn path_helpers_follow_posixpath() {
        assert_eq!(dirname("/115/in/a.txt"), "/115/in");
        assert_eq!(dirname("/115"), "/");
        assert_eq!(dirname("/"), "/");
        assert_eq!(dirname("a"), "");
        assert_eq!(dirname("//x"), "//");
        assert_eq!(basename("/115/in/a.txt"), "a.txt");
        assert_eq!(basename("/115/in/"), "");
        assert_eq!(join("/115/out", "a.txt"), "/115/out/a.txt");
        assert_eq!(join("/", "a.txt"), "/a.txt");
        assert_eq!(join("/x", "/abs"), "/abs");
    }
}
