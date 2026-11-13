//! The fake CloudDrive gRPC server of the effect transcripts and the client tests, Rust side
//! (docs/rewrite/SCENARIOS.md, "Fakes").
//!
//! Port of `tools/scenarios/fakes/clouddrive.py`, rule for rule: the same seed, the same
//! state, the same 14 RPCs, the same call records (protobuf's JSON mapping, as
//! `MessageToDict` with `preserving_proto_field_name` and
//! `always_print_fields_with_no_presence`), the same dump. The semantics are the fake's,
//! not CloudDrive's: both builds run against the same rules, which is what makes their
//! transcripts comparable.
//!
//! Two test hooks have no Python counterpart and never reach a transcript: [`Seen`], the
//! metadata each call arrived with (bearer token, `grpc-timeout`), and [`Hold`], which
//! parks the next calls of one method before they are handled, so a test can drop or time
//! out a caller while its call is on the server.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use embyx_core::posixpath::{basename, dirname, join};
use embyx_core::pyjson::{Dict, Key, Value};
use embyx_core::pyurl;
use embyx_core::scenario::Recorder;
use futures_util::stream;
use prost_reflect::{DescriptorPool, DynamicMessage, SerializeOptions};
use tokio::sync::Semaphore;
use tonic::metadata::MetadataMap;
use tonic::transport::{Identity, ServerTlsConfig};
use tonic::{Code, Request, Response, Status};

use super::client::code_by_name;
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

fn list_of(section: &Value, key: &str) -> Vec<Value> {
    section
        .get(key)
        .and_then(Value::as_list)
        .map(<[Value]>::to_vec)
        .unwrap_or_default()
}

/// `task[key]` of a seeded or queued task (a missing key fails the call in Python too).
fn field<'a>(task: &'a Value, key: &str) -> &'a Value {
    task.get(key)
        .unwrap_or_else(|| panic!("fake CloudDrive task without {key:?}: {task:?}"))
}

fn text<'a>(task: &'a Value, key: &str) -> &'a str {
    field(task, key)
        .as_str()
        .unwrap_or_else(|| panic!("{key} is not a string"))
}

fn int(value: &Value) -> i128 {
    match value {
        Value::Int(number) => *number,
        Value::Bool(flag) => i128::from(*flag),
        other => panic!("not an integer: {other:?}"),
    }
}

fn unsigned(value: &Value) -> u64 {
    u64::try_from(int(value)).expect("a uint64 field")
}

fn float(value: &Value) -> f64 {
    match value {
        Value::Float(number) => *number,
        #[allow(clippy::cast_precision_loss, reason = "Python's float(int) for a seeded progress")]
        Value::Int(number) => *number as f64,
        other => panic!("not a number: {other:?}"),
    }
}

fn flag(value: &Value) -> bool {
    match value {
        Value::Bool(flag) => *flag,
        other => other.is_truthy(),
    }
}

/// `value[key] if key in value else default`, for an `upload.get(...)`.
fn get_or<'a>(value: &'a Value, key: &str, default: &'a Value) -> &'a Value {
    value.get(key).unwrap_or(default)
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
                    Key::Str(text) => text.parse().expect("hash kinds are integers"),
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

    fn copy_task_matches(task: &Value, request: &proto::CopyTaskRequest) -> bool {
        text(task, "source_path") == request.source_path && text(task, "dest_path") == request.dest_path
    }
}

fn times(error: &Dict) -> i128 {
    match error.get("times") {
        Some(Value::Int(times)) => *times,
        _ => 1,
    }
}

/// gRPC's status code by its Python name (`grpc.StatusCode.<NAME>`).
fn code_of(name: &str) -> Code {
    code_by_name(name).unwrap_or_else(|| panic!("unknown gRPC status {name}"))
}

/// `_Abort`: a gRPC error with Python's code name and details.
struct Abort {
    code: &'static str,
    details: String,
}

fn result(
    success: bool,
    error_message: impl Into<String>,
    result_file_paths: Vec<String>,
) -> proto::FileOperationResult {
    proto::FileOperationResult {
        success,
        error_message: error_message.into(),
        result_file_paths,
    }
}

/// `_btih(url)`: the lower-cased hash after `urn:btih:`, or the URL itself.
fn btih(url: &str) -> String {
    const MARKER: &str = "urn:btih:";
    match url.find(MARKER) {
        None => url.to_owned(),
        Some(start) => {
            let rest = &url[start + MARKER.len()..];
            rest.split('&').next().unwrap_or_default().to_lowercase()
        }
    }
}

/// `parse_qs(urlsplit(url).query).get('dn', [''])[0]`.
fn display_name(url: &str) -> String {
    let query = pyurl::urlsplit(url, "")
        .unwrap_or_else(|error| panic!("urlsplit({url:?}) failed: {error:?}"))
        .query;
    let unquote_plus = |text: &str| pyurl::unquote(&text.replace('+', " "));
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .filter(|(_, value)| !value.is_empty())
        .find(|(name, _)| unquote_plus(name) == "dn")
        .map(|(_, value)| unquote_plus(value))
        .unwrap_or_default()
}

/// The metadata one call arrived with (test hook, not recorded in transcripts).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    pub method: String,
    /// The `authorization` header, if the call sent one.
    pub authorization: Option<String>,
    /// The `grpc-timeout` header, parsed.
    pub timeout: Option<Duration>,
}

/// `grpc-timeout`: an integer and a unit (`H`, `M`, `S`, `m`, `u`, `n`).
fn parse_grpc_timeout(value: &str) -> Option<Duration> {
    let (digits, unit) = value.split_at(value.len().checked_sub(1)?);
    let amount: u64 = digits.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(amount * 3600),
        "M" => Duration::from_secs(amount * 60),
        "S" => Duration::from_secs(amount),
        "m" => Duration::from_millis(amount),
        "u" => Duration::from_micros(amount),
        "n" => Duration::from_nanos(amount),
        _ => return None,
    })
}

/// Parks the calls of one method before they are handled (test hook).
#[derive(Debug, Clone)]
pub struct Hold {
    arrived: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

impl Hold {
    /// Wait until one more held call has arrived.
    pub async fn arrived(&self) {
        self.arrived.acquire().await.expect("hold").forget();
    }

    /// Let one held call go on.
    pub fn release(&self) {
        self.release.add_permits(1);
    }
}

/// The fake's state and recorder; [`FakeCloudDrive::serve`] puts it on a local port.
#[derive(Clone)]
pub struct FakeCloudDrive {
    state: Arc<Mutex<State>>,
    recorder: Recorder,
    seen: Arc<Mutex<Vec<Seen>>>,
    holds: Arc<Mutex<HashMap<String, Hold>>>,
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

impl Served {
    /// `host:port`, as `CloudDriveClient(address=...)` takes it.
    pub fn address(&self) -> String {
        self.address.to_string()
    }
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
            seen: Arc::new(Mutex::new(Vec::new())),
            holds: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// `FakeCloudDrive.dump`.
    pub fn dump(&self) -> Value {
        self.state.lock().expect("fake state").dump()
    }

    /// Every call's metadata, in arrival order.
    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("seen").clone()
    }

    /// Park every later call of `method` until [`Hold::release`] lets it go.
    pub fn hold(&self, method: &str) -> Hold {
        let hold = Hold {
            arrived: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
        };
        self.holds
            .lock()
            .expect("holds")
            .insert(method.to_owned(), hold.clone());
        hold
    }

    /// Serve on `127.0.0.1:0`, plain HTTP/2 (`CloudDriveClient(..., secure=False)`).
    pub async fn serve(self) -> Served {
        self.serve_with(None).await
    }

    /// Serve with TLS, the certificate and key in PEM (`secure=True`).
    pub async fn serve_tls(self, certificate_pem: &[u8], key_pem: &[u8]) -> Served {
        self.serve_with(Some(
            ServerTlsConfig::new().identity(Identity::from_pem(certificate_pem, key_pem)),
        ))
        .await
    }

    async fn serve_with(self, tls: Option<ServerTlsConfig>) -> Served {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let service = CloudDriveFileSrvServer::new(self.clone());
        let mut builder = tonic::transport::Server::builder();
        if let Some(tls) = tls {
            builder = builder.tls_config(tls).expect("server TLS");
        }
        let task = tokio::spawn(async move {
            builder
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

    /// Note the call's metadata and wait while its method is held.
    async fn arrive(&self, method: &str, metadata: &MetadataMap) {
        let header = |name: &str| {
            metadata
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        self.seen.lock().expect("seen").push(Seen {
            method: method.to_owned(),
            authorization: header("authorization"),
            timeout: header("grpc-timeout").as_deref().and_then(parse_grpc_timeout),
        });
        let hold = self.holds.lock().expect("holds").get(method).cloned();
        if let Some(hold) = hold {
            hold.arrived.add_permits(1);
            hold.release.acquire().await.expect("hold").forget();
        }
    }

    /// `_call`: record the request, then the response or the error, under one lock.
    fn call<Q: prost::Message, R: prost::Message>(
        &self,
        method: &str,
        request: &Q,
        types: (&str, &str),
        handle: impl FnOnce(&mut State, &Q) -> Result<Vec<R>, Abort>,
        streaming: bool,
    ) -> Result<Vec<R>, Status> {
        let mut state = self.state.lock().expect("fake state");
        let mut record = Dict::new();
        record.insert("fake", Value::from("clouddrive"));
        record.insert("method", Value::from(method));
        record.insert("request", to_json(request, types.0));
        if let Some((code, details)) = state.scripted_error(method) {
            record.insert("error", error_record(&code, &details));
            self.recorder.record(Value::Dict(record));
            return Err(Status::new(code_of(&code), details));
        }
        match handle(&mut state, request) {
            Err(abort) => {
                record.insert("error", error_record(abort.code, &abort.details));
                self.recorder.record(Value::Dict(record));
                Err(Status::new(code_of(abort.code), abort.details))
            }
            Ok(responses) => {
                let response = if streaming {
                    Value::List(responses.iter().map(|item| to_json(item, types.1)).collect())
                } else {
                    to_json(&responses[0], types.1)
                };
                record.insert("response", response);
                self.recorder.record(Value::Dict(record));
                Ok(responses)
            }
        }
    }

    async fn unary<Q: prost::Message, R: prost::Message>(
        &self,
        method: &str,
        request: Request<Q>,
        types: (&str, &str),
        handle: impl FnOnce(&mut State, &Q) -> Result<R, Abort>,
    ) -> Result<Response<R>, Status> {
        self.arrive(method, request.metadata()).await;
        let request = request.into_inner();
        let mut responses = self.call(
            method,
            &request,
            types,
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

type SubFilesStream = stream::Iter<std::vec::IntoIter<Result<proto::SubFilesReply, Status>>>;

const EMPTY: &str = "google.protobuf.Empty";
const OPERATION_RESULT: &str = "clouddrive.FileOperationResult";

#[tonic::async_trait]
impl CloudDriveFileSrv for FakeCloudDrive {
    type GetSubFilesStream = SubFilesStream;

    async fn get_system_info(&self, request: Request<()>) -> Result<Response<proto::CloudDriveSystemInfo>, Status> {
        self.unary(
            "GetSystemInfo",
            request,
            (EMPTY, "clouddrive.CloudDriveSystemInfo"),
            |_, ()| {
                Ok(proto::CloudDriveSystemInfo {
                    is_login: true,
                    user_name: "fake".to_owned(),
                    system_ready: true,
                    ..Default::default()
                })
            },
        )
        .await
    }

    async fn get_sub_files(
        &self,
        request: Request<proto::ListSubFileRequest>,
    ) -> Result<Response<Self::GetSubFilesStream>, Status> {
        self.arrive("GetSubFiles", request.metadata()).await;
        let request = request.into_inner();
        let replies = self.call(
            "GetSubFiles",
            &request,
            ("clouddrive.ListSubFileRequest", "clouddrive.SubFilesReply"),
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

    async fn create_folder(
        &self,
        request: Request<proto::CreateFolderRequest>,
    ) -> Result<Response<proto::CreateFolderResult>, Status> {
        self.unary(
            "CreateFolder",
            request,
            ("clouddrive.CreateFolderRequest", "clouddrive.CreateFolderResult"),
            |state, request| {
                if !state.directories.contains(&request.parent_path) {
                    return Err(Abort {
                        code: "NOT_FOUND",
                        details: format!("path not found: {}", request.parent_path),
                    });
                }
                let path = join(&request.parent_path, &request.folder_name);
                if state.exists(&path) {
                    return Ok(proto::CreateFolderResult {
                        folder_created: None,
                        result: Some(result(false, "file exists", Vec::new())),
                    });
                }
                state.directories.insert(path.clone());
                Ok(proto::CreateFolderResult {
                    folder_created: Some(state.entry(&path)),
                    result: Some(result(true, "", vec![path])),
                })
            },
        )
        .await
    }

    async fn rename_file(
        &self,
        request: Request<proto::RenameFileRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        self.unary(
            "RenameFile",
            request,
            ("clouddrive.RenameFileRequest", OPERATION_RESULT),
            |state, request| {
                let source = &request.the_file_path;
                let target = join(dirname(source), &request.new_name);
                if !state.exists(source) {
                    return Ok(result(false, format!("file not found: {source}"), Vec::new()));
                }
                if state.exists(&target) {
                    return Ok(result(false, format!("file exists: {target}"), Vec::new()));
                }
                state.relocate(source, &target);
                Ok(result(true, "", vec![target]))
            },
        )
        .await
    }

    async fn move_file(
        &self,
        request: Request<proto::MoveFileRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        self.unary(
            "MoveFile",
            request,
            ("clouddrive.MoveFileRequest", OPERATION_RESULT),
            |state, request| {
                if !state.directories.contains(&request.dest_path) {
                    return Ok(result(
                        false,
                        format!("destination not found: {}", request.dest_path),
                        Vec::new(),
                    ));
                }
                let mut moved = Vec::new();
                for source in &request.the_file_paths {
                    if !state.exists(source) {
                        return Ok(result(false, format!("file not found: {source}"), moved));
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
                Ok(result(true, "", moved))
            },
        )
        .await
    }

    async fn copy_file(
        &self,
        request: Request<proto::CopyFileRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        self.unary(
            "CopyFile",
            request,
            ("clouddrive.CopyFileRequest", OPERATION_RESULT),
            |state, request| {
                if !state.directories.contains(&request.dest_path) {
                    return Ok(result(
                        false,
                        format!("destination not found: {}", request.dest_path),
                        Vec::new(),
                    ));
                }
                for source in &request.the_file_paths {
                    // Queued only: a scenario finishes a copy through the seed of a later step.
                    let mut task = Dict::new();
                    task.insert("source_path", Value::from(source.as_str()));
                    task.insert("dest_path", Value::from(request.dest_path.as_str()));
                    task.insert("status", Value::from("Pending"));
                    task.insert("total_bytes", Value::Int(0));
                    task.insert("uploaded_bytes", Value::Int(0));
                    task.insert("failed_files", Value::Int(0));
                    task.insert("paused", Value::Bool(false));
                    task.insert("errors", Value::List(Vec::new()));
                    state.copy_tasks.push(Value::Dict(task));
                }
                Ok(result(true, "", Vec::new()))
            },
        )
        .await
    }

    async fn get_copy_tasks(&self, request: Request<()>) -> Result<Response<proto::GetCopyTaskResult>, Status> {
        self.unary(
            "GetCopyTasks",
            request,
            (EMPTY, "clouddrive.GetCopyTaskResult"),
            |state, ()| {
                let copy_tasks = state
                    .copy_tasks
                    .iter()
                    .map(|task| {
                        let status = proto::copy_task::TaskStatus::from_str_name(text(task, "status"))
                            .unwrap_or_else(|| panic!("unknown copy task status {:?}", text(task, "status")));
                        proto::CopyTask {
                            source_path: text(task, "source_path").to_owned(),
                            dest_path: text(task, "dest_path").to_owned(),
                            status: status.into(),
                            total_bytes: unsigned(field(task, "total_bytes")),
                            uploaded_bytes: unsigned(field(task, "uploaded_bytes")),
                            failed_files: unsigned(field(task, "failed_files")),
                            paused: flag(field(task, "paused")),
                            errors: field(task, "errors")
                                .as_list()
                                .expect("errors is a list")
                                .iter()
                                .map(|message| proto::TaskError {
                                    message: message.as_str().expect("error messages are strings").to_owned(),
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        }
                    })
                    .collect();
                Ok(proto::GetCopyTaskResult { copy_tasks })
            },
        )
        .await
    }

    async fn get_upload_file_list(
        &self,
        request: Request<proto::GetUploadFileListRequest>,
    ) -> Result<Response<proto::GetUploadFileListResult>, Status> {
        self.unary(
            "GetUploadFileList",
            request,
            (
                "clouddrive.GetUploadFileListRequest",
                "clouddrive.GetUploadFileListResult",
            ),
            |state, request| {
                let matching: Vec<&Value> = state
                    .uploads
                    .iter()
                    .filter(|upload| text(upload, "dest_path").contains(&request.filter))
                    .collect();
                let zero = Value::Int(0);
                let transfer = Value::from("Transfer");
                let empty = Value::from("");
                let upload_files = matching
                    .iter()
                    .map(|upload| proto::UploadFileInfo {
                        dest_path: text(upload, "dest_path").to_owned(),
                        size: unsigned(get_or(upload, "size", &zero)),
                        transfered_bytes: unsigned(get_or(upload, "transferred", &zero)),
                        status: get_or(upload, "status", &transfer)
                            .as_str()
                            .expect("status is a string")
                            .to_owned(),
                        error_message: get_or(upload, "error", &empty)
                            .as_str()
                            .expect("error is a string")
                            .to_owned(),
                        ..Default::default()
                    })
                    .collect();
                Ok(proto::GetUploadFileListResult {
                    total_count: u32::try_from(state.uploads.len()).expect("count"),
                    total_count_filtered: u32::try_from(matching.len()).expect("count"),
                    upload_files,
                    ..Default::default()
                })
            },
        )
        .await
    }

    async fn restart_copy_task(&self, request: Request<proto::CopyTaskRequest>) -> Result<Response<()>, Status> {
        self.unary(
            "RestartCopyTask",
            request,
            ("clouddrive.CopyTaskRequest", EMPTY),
            |state, request| {
                for task in &mut state.copy_tasks {
                    if State::copy_task_matches(task, request) {
                        let Value::Dict(task) = task else {
                            panic!("copy tasks are tables")
                        };
                        task.insert("status", Value::from("Pending"));
                        task.insert("failed_files", Value::Int(0));
                        task.insert("errors", Value::List(Vec::new()));
                    }
                }
                Ok(())
            },
        )
        .await
    }

    async fn cancel_copy_task(&self, request: Request<proto::CopyTaskRequest>) -> Result<Response<()>, Status> {
        self.unary(
            "CancelCopyTask",
            request,
            ("clouddrive.CopyTaskRequest", EMPTY),
            |state, request| {
                state.copy_tasks.retain(|task| !State::copy_task_matches(task, request));
                Ok(())
            },
        )
        .await
    }

    async fn delete_files(
        &self,
        request: Request<proto::MultiFileRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        self.unary(
            "DeleteFiles",
            request,
            ("clouddrive.MultiFileRequest", OPERATION_RESULT),
            |state, request| {
                if let Some(missing) = request.path.iter().find(|path| !state.exists(path)) {
                    return Ok(result(false, format!("file not found: {missing}"), Vec::new()));
                }
                for path in &request.path {
                    state.remove(path);
                }
                Ok(result(true, "", Vec::new()))
            },
        )
        .await
    }

    async fn add_offline_files(
        &self,
        request: Request<proto::AddOfflineFileRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        self.unary(
            "AddOfflineFiles",
            request,
            ("clouddrive.AddOfflineFileRequest", OPERATION_RESULT),
            |state, request| {
                if !state.directories.contains(&request.to_folder) {
                    return Ok(result(
                        false,
                        format!("destination not found: {}", request.to_folder),
                        Vec::new(),
                    ));
                }
                for url in request.urls.split('\n') {
                    let info_hash = btih(url);
                    if state
                        .offline_tasks
                        .iter()
                        .any(|task| text(task, "info_hash") == info_hash)
                    {
                        return Ok(result(false, "任务已存在", Vec::new()));
                    }
                    let name = display_name(url);
                    let mut task = Dict::new();
                    task.insert(
                        "name",
                        Value::from(if name.is_empty() {
                            info_hash.as_str()
                        } else {
                            name.as_str()
                        }),
                    );
                    task.insert("url", Value::from(url));
                    task.insert("folder", Value::from(request.to_folder.as_str()));
                    task.insert("info_hash", Value::from(info_hash.as_str()));
                    task.insert("status", Value::from("OFFLINE_INIT"));
                    task.insert("size", Value::Int(0));
                    task.insert("progress", Value::Float(0.0));
                    state.offline_tasks.push(Value::Dict(task));
                }
                Ok(result(true, "", Vec::new()))
            },
        )
        .await
    }

    async fn list_offline_files_by_path(
        &self,
        request: Request<proto::FileRequest>,
    ) -> Result<Response<proto::OfflineFileListResult>, Status> {
        self.unary(
            "ListOfflineFilesByPath",
            request,
            ("clouddrive.FileRequest", "clouddrive.OfflineFileListResult"),
            |state, request| {
                let offline_files = state
                    .offline_tasks
                    .iter()
                    .filter(|task| text(task, "folder") == request.path)
                    .map(|task| {
                        let status = proto::OfflineFileStatus::from_str_name(text(task, "status"))
                            .unwrap_or_else(|| panic!("unknown offline status {:?}", text(task, "status")));
                        proto::OfflineFile {
                            name: text(task, "name").to_owned(),
                            size: unsigned(field(task, "size")),
                            url: text(task, "url").to_owned(),
                            status: status.into(),
                            info_hash: text(task, "info_hash").to_owned(),
                            percend_done: float(field(task, "progress")),
                            ..Default::default()
                        }
                    })
                    .collect();
                Ok(proto::OfflineFileListResult {
                    offline_files,
                    ..Default::default()
                })
            },
        )
        .await
    }

    async fn remove_offline_files(
        &self,
        request: Request<proto::RemoveOfflineFilesRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        self.unary(
            "RemoveOfflineFiles",
            request,
            ("clouddrive.RemoveOfflineFilesRequest", OPERATION_RESULT),
            |state, request| {
                // Matched case-sensitively, as CloudDrive does; an unknown hash still answers success.
                let (removed, kept): (Vec<Value>, Vec<Value>) = std::mem::take(&mut state.offline_tasks)
                    .into_iter()
                    .partition(|task| request.info_hashes.iter().any(|hash| hash == text(task, "info_hash")));
                state.offline_tasks = kept;
                if request.delete_files {
                    for task in &removed {
                        state.remove(&join(text(task, "folder"), text(task, "name")));
                    }
                }
                Ok(result(true, "", Vec::new()))
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{btih, display_name, parse_grpc_timeout};

    #[test]
    fn magnet_helpers_follow_the_python_fake() {
        assert_eq!(btih("magnet:?xt=urn:btih:ABCdef&dn=x"), "abcdef");
        assert_eq!(btih("magnet:?xt=urn:btih:ABC"), "abc");
        assert_eq!(btih("https://example.test/a.torrent"), "https://example.test/a.torrent");
        assert_eq!(
            display_name("magnet:?xt=urn:btih:abc&dn=ABC-001+%E4%B8%AD"),
            "ABC-001 中"
        );
        assert_eq!(display_name("magnet:?xt=urn:btih:abc&dn=&dn=second"), "second");
        assert_eq!(display_name("magnet:?xt=urn:btih:abc&dn"), "");
        assert_eq!(display_name("magnet:?xt=urn:btih:abc"), "");
    }

    #[test]
    fn grpc_timeouts_parse() {
        assert_eq!(parse_grpc_timeout("30000000u"), Some(Duration::from_secs(30)));
        assert_eq!(parse_grpc_timeout("300S"), Some(Duration::from_mins(5)));
        assert_eq!(parse_grpc_timeout("2M"), Some(Duration::from_mins(2)));
        assert_eq!(parse_grpc_timeout("1H"), Some(Duration::from_hours(1)));
        assert_eq!(parse_grpc_timeout("5m"), Some(Duration::from_millis(5)));
        assert_eq!(parse_grpc_timeout("7n"), Some(Duration::from_nanos(7)));
        assert_eq!(parse_grpc_timeout("x"), None);
        assert_eq!(parse_grpc_timeout(""), None);
    }
}
