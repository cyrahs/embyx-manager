//! Tests of the CloudDrive client and wrapper: `tests/test_clouddrive.py`, ported, plus the
//! goldens of `tools/goldens/areas/clouddrive_wrapper.py` and the transport failures.
//!
//! Python's tests replace the gRPC stub or the sync client with mocks. Here every call goes
//! over a real socket, either to the scenario fake ([`FakeCloudDrive`], which also reports
//! the metadata each call arrived with) or, where a test needs answers the fake's rules
//! cannot give (a listing that names a file of another directory), to [`Scripted`], a server
//! that answers each method from a queue and keeps each request.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use embyx_core::golden;
use embyx_core::pyjson::{self, Value};
use embyx_core::scenario::Recorder;
use futures_util::stream;
use prost::Message;
use tokio::io::AsyncReadExt as _;
use tonic::{Code, Request, Response, Status};

use super::aio::{
    AsyncCloudDrive, CloudFile, CopyTask, INVALID_API_PATH, INVALID_SEGMENT, OfflineStatus, OfflineTask, UploadState,
    validate_api_path, validate_path_segment,
};
use super::client::{
    CHECK_FOLDER_AFTER_SECONDS, ClientOptions, CloudDriveClient, CloudDriveError, GRPC_TIMEOUT, LIST_OFFLINE_TIMEOUT,
    MOVE_TIMEOUT, Timeouts,
};
use super::fake::{FakeCloudDrive, Served, bytes_to_json};
use super::proto;
use super::proto::cloud_drive_file_srv_server::{CloudDriveFileSrv, CloudDriveFileSrvServer};

fn seed(json: &str) -> Value {
    pyjson::loads(json).expect("seed JSON")
}

fn options(address: &str) -> ClientOptions {
    ClientOptions::new(address, "test-token", false)
}

async fn fake(json: &str) -> (Served, Recorder) {
    let recorder = Recorder::new();
    let served = FakeCloudDrive::new(&seed(json), recorder.clone()).serve().await;
    (served, recorder)
}

fn calls_of(recorder: &Recorder, method: &str) -> Vec<Value> {
    recorder
        .calls()
        .into_iter()
        .filter(|call| call.get("method").and_then(Value::as_str) == Some(method))
        .collect()
}

fn request_of(call: &Value) -> &Value {
    call.get("request").expect("request")
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

/// One scripted answer: the encoded reply, or a status.
type Reply = Result<Vec<u8>, (Code, String)>;

/// A CloudDrive server that answers each method from a queue of encoded replies and keeps
/// every request (name and protobuf JSON).
#[derive(Clone, Default)]
struct Scripted {
    replies: Arc<Mutex<HashMap<&'static str, VecDeque<Reply>>>>,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
}

impl Scripted {
    fn reply<M: Message>(&self, method: &'static str, message: &M) -> &Self {
        self.replies
            .lock()
            .expect("replies")
            .entry(method)
            .or_default()
            .push_back(Ok(message.encode_to_vec()));
        self
    }

    fn fail(&self, method: &'static str, code: Code, details: &str) -> &Self {
        self.replies
            .lock()
            .expect("replies")
            .entry(method)
            .or_default()
            .push_back(Err((code, details.to_owned())));
        self
    }

    fn requests(&self, method: &str) -> Vec<Value> {
        self.requests
            .lock()
            .expect("requests")
            .iter()
            .filter(|(name, _)| name == method)
            .map(|(_, request)| request.clone())
            .collect()
    }

    fn answer<Q: Message, R: Message + Default>(
        &self,
        method: &'static str,
        request_type: &str,
        request: &Q,
    ) -> Result<R, Status> {
        self.requests
            .lock()
            .expect("requests")
            .push((method.to_owned(), bytes_to_json(request_type, &request.encode_to_vec())));
        let reply = self
            .replies
            .lock()
            .expect("replies")
            .get_mut(method)
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| panic!("no scripted reply for {method}"));
        match reply {
            Ok(bytes) => Ok(R::decode(bytes.as_slice()).expect("decode")),
            Err((code, details)) => Err(Status::new(code, details)),
        }
    }

    async fn serve(self) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(CloudDriveFileSrvServer::new(self))
                .serve_with_incoming(incoming)
                .await
                .expect("scripted server");
        });
        (address, task)
    }
}

type SubFilesStream = stream::Iter<std::vec::IntoIter<Result<proto::SubFilesReply, Status>>>;

#[tonic::async_trait]
impl CloudDriveFileSrv for Scripted {
    type GetSubFilesStream = SubFilesStream;

    async fn get_system_info(&self, request: Request<()>) -> Result<Response<proto::CloudDriveSystemInfo>, Status> {
        self.answer("GetSystemInfo", "google.protobuf.Empty", request.get_ref())
            .map(Response::new)
    }

    async fn get_sub_files(
        &self,
        request: Request<proto::ListSubFileRequest>,
    ) -> Result<Response<Self::GetSubFilesStream>, Status> {
        let reply: proto::SubFilesReply =
            self.answer("GetSubFiles", "clouddrive.ListSubFileRequest", request.get_ref())?;
        Ok(Response::new(stream::iter(vec![Ok(reply)])))
    }

    async fn create_folder(
        &self,
        request: Request<proto::CreateFolderRequest>,
    ) -> Result<Response<proto::CreateFolderResult>, Status> {
        self.answer("CreateFolder", "clouddrive.CreateFolderRequest", request.get_ref())
            .map(Response::new)
    }

    async fn rename_file(
        &self,
        request: Request<proto::RenameFileRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        self.answer("RenameFile", "clouddrive.RenameFileRequest", request.get_ref())
            .map(Response::new)
    }

    async fn move_file(
        &self,
        request: Request<proto::MoveFileRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        self.answer("MoveFile", "clouddrive.MoveFileRequest", request.get_ref())
            .map(Response::new)
    }

    async fn copy_file(
        &self,
        request: Request<proto::CopyFileRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        self.answer("CopyFile", "clouddrive.CopyFileRequest", request.get_ref())
            .map(Response::new)
    }

    async fn delete_files(
        &self,
        request: Request<proto::MultiFileRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        self.answer("DeleteFiles", "clouddrive.MultiFileRequest", request.get_ref())
            .map(Response::new)
    }

    async fn add_offline_files(
        &self,
        request: Request<proto::AddOfflineFileRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        self.answer("AddOfflineFiles", "clouddrive.AddOfflineFileRequest", request.get_ref())
            .map(Response::new)
    }

    async fn remove_offline_files(
        &self,
        request: Request<proto::RemoveOfflineFilesRequest>,
    ) -> Result<Response<proto::FileOperationResult>, Status> {
        self.answer(
            "RemoveOfflineFiles",
            "clouddrive.RemoveOfflineFilesRequest",
            request.get_ref(),
        )
        .map(Response::new)
    }

    async fn list_offline_files_by_path(
        &self,
        request: Request<proto::FileRequest>,
    ) -> Result<Response<proto::OfflineFileListResult>, Status> {
        self.answer("ListOfflineFilesByPath", "clouddrive.FileRequest", request.get_ref())
            .map(Response::new)
    }

    async fn get_upload_file_list(
        &self,
        request: Request<proto::GetUploadFileListRequest>,
    ) -> Result<Response<proto::GetUploadFileListResult>, Status> {
        self.answer(
            "GetUploadFileList",
            "clouddrive.GetUploadFileListRequest",
            request.get_ref(),
        )
        .map(Response::new)
    }

    async fn get_copy_tasks(&self, request: Request<()>) -> Result<Response<proto::GetCopyTaskResult>, Status> {
        self.answer("GetCopyTasks", "google.protobuf.Empty", request.get_ref())
            .map(Response::new)
    }

    async fn cancel_copy_task(&self, request: Request<proto::CopyTaskRequest>) -> Result<Response<()>, Status> {
        self.answer("CancelCopyTask", "clouddrive.CopyTaskRequest", request.get_ref())
            .map(Response::new)
    }

    async fn restart_copy_task(&self, request: Request<proto::CopyTaskRequest>) -> Result<Response<()>, Status> {
        self.answer("RestartCopyTask", "clouddrive.CopyTaskRequest", request.get_ref())
            .map(Response::new)
    }
}

async fn scripted(script: Scripted) -> (AsyncCloudDrive, Scripted, tokio::task::JoinHandle<()>) {
    let (address, task) = script.clone().serve().await;
    (AsyncCloudDrive::connect(&options(&address)), script, task)
}

fn listing(files: Vec<proto::CloudDriveFile>) -> proto::SubFilesReply {
    proto::SubFilesReply { sub_files: files }
}

// ---------- client.py: deadlines, request fields, channels ----------

// py: tests/test_clouddrive.py::test_clouddrive_calls_include_timeout
#[tokio::test]
async fn clouddrive_calls_include_timeout() {
    let (served, _) = fake(
        r#"{"directories": ["/media", "/media/dst"], "files": [{"path": "/media/old"}, {"path": "/media/file"}]}"#,
    )
    .await;
    let client = CloudDriveClient::new(&options(&served.address()));

    client.get_system_info().await.expect("GetSystemInfo");
    client.get_sub_files("/media", false).await.expect("GetSubFiles");
    client.create_folder("/media", "new").await.expect("CreateFolder");
    client.rename_file("/media/old", "renamed").await.expect("RenameFile");
    client
        .move_file(&strings(&["/media/file"]), "/media/dst", 0)
        .await
        .expect("MoveFile");
    client
        .add_offline_file(&strings(&["magnet:?xt=urn:btih:abc"]), "/media")
        .await
        .expect("AddOfflineFiles");
    client
        .list_offline_files_by_path("/media")
        .await
        .expect("ListOfflineFilesByPath");
    client
        .remove_offline_files(&strings(&["ABC"]), "/media", true)
        .await
        .expect("RemoveOfflineFiles");

    let timeouts: HashMap<String, Option<Duration>> = served
        .fake
        .seen()
        .into_iter()
        .map(|seen| (seen.method, seen.timeout))
        .collect();
    for method in [
        "GetSystemInfo",
        "GetSubFiles",
        "CreateFolder",
        "RenameFile",
        "AddOfflineFiles",
        "RemoveOfflineFiles",
    ] {
        assert_eq!(timeouts[method], Some(GRPC_TIMEOUT), "{method}");
    }
    // A cloud-side move is executed by the provider and gets a budget of its own.
    assert_eq!(timeouts["MoveFile"], Some(MOVE_TIMEOUT));
    assert!(MOVE_TIMEOUT > GRPC_TIMEOUT);
    // So does listing offline tasks, which pages through the cloud's whole history.
    assert_eq!(timeouts["ListOfflineFilesByPath"], Some(LIST_OFFLINE_TIMEOUT));
    assert!(LIST_OFFLINE_TIMEOUT > GRPC_TIMEOUT);
    assert_eq!(GRPC_TIMEOUT, Duration::from_secs(30));
    assert_eq!(MOVE_TIMEOUT, Duration::from_mins(5));
    assert_eq!(LIST_OFFLINE_TIMEOUT, Duration::from_mins(2));
}

#[tokio::test]
async fn the_other_rpcs_send_their_deadlines_too() {
    let (served, _) = fake(r#"{"directories": ["/media", "/dst"], "files": [{"path": "/media/a"}]}"#).await;
    let client = CloudDriveClient::new(&options(&served.address()));
    client
        .copy_file(&strings(&["/media/a"]), "/dst", 0)
        .await
        .expect("CopyFile");
    client.get_copy_tasks().await.expect("GetCopyTasks");
    client.get_upload_files("a").await.expect("GetUploadFileList");
    client
        .restart_copy_task("/media/a", "/dst")
        .await
        .expect("RestartCopyTask");
    client
        .cancel_copy_task("/media/a", "/dst")
        .await
        .expect("CancelCopyTask");
    client.delete_files(&strings(&["/media/a"])).await.expect("DeleteFiles");
    let seen = served.fake.seen();
    let timeout = |method: &str| seen.iter().find(|seen| seen.method == method).expect(method).timeout;
    for method in [
        "CopyFile",
        "GetCopyTasks",
        "GetUploadFileList",
        "RestartCopyTask",
        "CancelCopyTask",
    ] {
        assert_eq!(timeout(method), Some(GRPC_TIMEOUT), "{method}");
    }
    // `delete_files` shares the move's budget in client.py.
    assert_eq!(timeout("DeleteFiles"), Some(MOVE_TIMEOUT));
}

// py: tests/test_clouddrive.py::test_add_offline_file_asks_clouddrive_to_check_the_folder
#[tokio::test]
async fn add_offline_file_asks_clouddrive_to_check_the_folder() {
    // A zero check interval leaves a persistently cached folder stale forever.
    let (served, recorder) = fake(r#"{"directories": ["/media"]}"#).await;
    let client = CloudDriveClient::new(&options(&served.address()));

    client
        .add_offline_file(&strings(&["magnet:?xt=urn:btih:abc"]), "/media")
        .await
        .expect("AddOfflineFiles");

    let calls = calls_of(&recorder, "AddOfflineFiles");
    let request = request_of(&calls[0]);
    assert_eq!(request.get("checkFolderAfterSecs"), Some(&Value::from("10")));
    assert_eq!(CHECK_FOLDER_AFTER_SECONDS, 10);
    const { assert!(CHECK_FOLDER_AFTER_SECONDS > 0) };
    assert_eq!(request.get("urls"), Some(&Value::from("magnet:?xt=urn:btih:abc")));
    assert_eq!(request.get("toFolder"), Some(&Value::from("/media")));
}

#[tokio::test]
async fn several_offline_urls_are_sent_one_per_line() {
    let (served, recorder) = fake(r#"{"directories": ["/media"]}"#).await;
    let client = CloudDriveClient::new(&options(&served.address()));
    client
        .add_offline_file(
            &strings(&["magnet:?xt=urn:btih:aaa", "magnet:?xt=urn:btih:bbb"]),
            "/media",
        )
        .await
        .expect("AddOfflineFiles");
    let calls = calls_of(&recorder, "AddOfflineFiles");
    assert_eq!(
        request_of(&calls[0]).get("urls"),
        Some(&Value::from("magnet:?xt=urn:btih:aaa\nmagnet:?xt=urn:btih:bbb"))
    );
}

// py: tests/test_clouddrive.py::test_remove_offline_files_sends_hashes_path_and_delete_flag
#[tokio::test]
async fn remove_offline_files_sends_hashes_path_and_delete_flag() {
    let (served, recorder) = fake("{}").await;
    let client = CloudDriveClient::new(&options(&served.address()));

    client
        .remove_offline_files(&strings(&["HASHA", "HASHB"]), "/media/tasks", true)
        .await
        .expect("RemoveOfflineFiles");

    let calls = calls_of(&recorder, "RemoveOfflineFiles");
    let request = request_of(&calls[0]);
    // Lowered: CloudDrive lists them that way, matches case-sensitively, and reports
    // success for an upper-case hash it silently ignores.
    assert_eq!(
        request.get("infoHashes"),
        Some(&Value::List(vec![Value::from("hasha"), Value::from("hashb")]))
    );
    assert_eq!(request.get("path"), Some(&Value::from("/media/tasks")));
    assert_eq!(request.get("deleteFiles"), Some(&Value::Bool(true)));
}

#[tokio::test]
async fn explicit_presence_fields_are_sent_as_python_sets_them() {
    // `conflictPolicy=0` and `RemoveOfflineFiles.path=''` are set fields in Python, so the
    // JSON mapping shows them; an unset optional field would be left out.
    let (served, recorder) = fake(r#"{"directories": ["/dst"], "files": [{"path": "/a"}]}"#).await;
    let client = CloudDriveClient::new(&options(&served.address()));
    client.copy_file(&strings(&["/a"]), "/dst", 0).await.expect("CopyFile");
    client
        .remove_offline_files(&[], "", false)
        .await
        .expect("RemoveOfflineFiles");
    client
        .list_offline_files_by_path("/dst")
        .await
        .expect("ListOfflineFilesByPath");
    let copy = &calls_of(&recorder, "CopyFile")[0];
    assert_eq!(request_of(copy).get("conflictPolicy"), Some(&Value::from("Overwrite")));
    let remove = &calls_of(&recorder, "RemoveOfflineFiles")[0];
    assert_eq!(request_of(remove).get("path"), Some(&Value::from("")));
    let list = &calls_of(&recorder, "ListOfflineFilesByPath")[0];
    assert_eq!(request_of(list).get("forceRefresh"), None);
    let upload = proto::GetUploadFileListRequest {
        get_all: true,
        filter: "x".to_owned(),
        ..Default::default()
    };
    // `GetUploadFileListRequest(getAll=True, filter=...)`: the optional filters stay unset.
    let json = bytes_to_json("clouddrive.GetUploadFileListRequest", &upload.encode_to_vec());
    assert_eq!(json.get("statusFilter"), None);
}

fn self_signed_localhost() -> (String, String) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("certificate");
    (certified.cert.pem(), certified.signing_key.serialize_pem())
}

// py: tests/test_clouddrive.py::test_clouddrive_channel_selection_and_auth_metadata cases=1
#[tokio::test]
async fn a_secure_client_speaks_tls_and_sends_the_bearer_token() {
    let (certificate, key) = self_signed_localhost();
    let recorder = Recorder::new();
    let served = FakeCloudDrive::new(&seed(r#"{"directories": ["/cloud/library"]}"#), recorder)
        .serve_tls(certificate.as_bytes(), key.as_bytes())
        .await;
    let mut secure = ClientOptions::new(format!("localhost:{}", served.address.port()), "test-token", true);
    secure.tls_roots_pem = Some(certificate.clone().into_bytes());
    let client = CloudDriveClient::new(&secure);

    assert!(
        client
            .get_sub_files("/cloud/library", false)
            .await
            .expect("listing over TLS")
            .is_empty()
    );
    let seen = served.fake.seen();
    assert_eq!(seen[0].authorization.as_deref(), Some("Bearer test-token"));
    // `metadata == [('authorization', 'Bearer test-token')]`: nothing else is sent.
    assert_eq!(seen[0].application_metadata, ["authorization"]);

    // A plain-text client cannot talk to the TLS server: the channel really is TLS.
    let plain = CloudDriveClient::new(&options(&format!("localhost:{}", served.address.port())));
    let error = plain
        .get_sub_files("/cloud/library", false)
        .await
        .expect_err("plain to TLS");
    assert_eq!(
        error.as_rpc().map(|error| error.code),
        Some(Code::Unavailable),
        "{error}"
    );
}

// py: tests/test_clouddrive.py::test_clouddrive_channel_selection_and_auth_metadata cases=1
#[tokio::test]
async fn an_insecure_client_speaks_plain_http2_and_sends_the_bearer_token() {
    let (served, _) = fake(r#"{"directories": ["/cloud/library"]}"#).await;
    let client = CloudDriveClient::new(&options(&served.address()));

    assert!(
        client
            .get_sub_files("/cloud/library", false)
            .await
            .expect("listing")
            .is_empty()
    );
    let seen = served.fake.seen();
    assert_eq!(seen[0].authorization.as_deref(), Some("Bearer test-token"));
    // `metadata == [('authorization', 'Bearer test-token')]`: nothing else is sent.
    assert_eq!(seen[0].application_metadata, ["authorization"]);

    // And a secure client does not fall back to plain text.
    let secure = CloudDriveClient::new(&ClientOptions::new(served.address(), "test-token", true));
    let error = secure
        .get_sub_files("/cloud/library", false)
        .await
        .expect_err("TLS to plain");
    assert_eq!(
        error.as_rpc().map(|error| error.code),
        Some(Code::Unavailable),
        "{error}"
    );
}

#[tokio::test]
async fn the_system_ping_is_sent_without_the_token() {
    let (served, _) = fake("{}").await;
    let client = CloudDriveClient::new(&options(&served.address()));
    let info = client.get_system_info().await.expect("GetSystemInfo");
    assert!(info.is_login);
    assert_eq!(served.fake.seen()[0].authorization, None);
}

#[tokio::test]
async fn a_token_grpc_cannot_send_fails_before_the_call() {
    let (served, recorder) = fake("{}").await;
    for token in ["tö", "a\nb"] {
        let client = CloudDriveClient::new(&ClientOptions::new(served.address(), token, false));
        let error = client.get_sub_files("/", false).await.expect_err("invalid metadata");
        assert_eq!(
            error.to_string(),
            "<_MultiThreadedRendezvous of RPC that terminated with:\n\tstatus = StatusCode.INTERNAL\n\tdetails = \"Invalid metadata\"\n\tdebug_error_string = \"\"\n>"
        );
    }
    assert!(recorder.calls().is_empty());
}

#[tokio::test]
async fn options_and_clients_keep_the_token_out_of_debug() {
    let options = ClientOptions::new("clouddrive:19798", "very-secret", true);
    assert!(!format!("{options:?}").contains("very-secret"));
    let client = CloudDriveClient::new(&options);
    assert!(!format!("{client:?}").contains("very-secret"));
    assert!(!format!("{:?}", AsyncCloudDrive::new(client)).contains("very-secret"));
}

// ---------- client.py: error mapping and transport failures ----------

#[tokio::test]
async fn sub_files_maps_not_found_and_not_a_directory() {
    let (served, _) = fake(r#"{"directories": ["/115"], "files": [{"path": "/115/f"}]}"#).await;
    let client = CloudDriveClient::new(&options(&served.address()));

    let missing = client.get_sub_files("/nope", false).await.expect_err("missing");
    assert_eq!(missing.class_name(), "FileNotFoundError");
    assert_eq!(missing.to_string(), "[Errno 2] path not found: /nope: '/nope'");
    let file = client.get_sub_files("/115/f", false).await.expect_err("a file");
    assert_eq!(file.class_name(), "NotADirectoryError");
    assert_eq!(
        file.to_string(),
        "[Errno 20] can't open a file as directory: /115/f: '/115/f'"
    );
}

#[tokio::test]
async fn sub_files_errors_keep_grpcs_text() {
    let (served, _) = fake(
        r#"{"errors": [
            {"method": "GetSubFiles", "code": "PERMISSION_DENIED", "details": "no"},
            {"method": "GetSubFiles", "code": "NOT_FOUND", "details": ""},
            {"method": "GetSubFiles", "code": "INVALID_ARGUMENT", "details": "bad"},
            {"method": "MoveFile", "code": "INTERNAL", "details": "任务已存在 \"q\""}
        ]}"#,
    )
    .await;
    let client = CloudDriveClient::new(&options(&served.address()));

    let denied = client.get_sub_files("/", false).await.expect_err("denied");
    assert_eq!(denied.class_name(), "_MultiThreadedRendezvous");
    assert_eq!(
        denied.to_string(),
        "<_MultiThreadedRendezvous of RPC that terminated with:\n\tstatus = StatusCode.PERMISSION_DENIED\n\tdetails = \"no\"\n\tdebug_error_string = \"PERMISSION_DENIED:no\"\n>"
    );
    let not_found = client.get_sub_files("/", false).await.expect_err("not found");
    assert_eq!(not_found.to_string(), "[Errno 2] CloudDrive path not found: \"/\": '/'");
    let invalid = client.get_sub_files("/", false).await.expect_err("invalid");
    assert_eq!(invalid.class_name(), "_MultiThreadedRendezvous");
    assert_eq!(invalid.details(), Some("bad"));

    let error = client
        .move_file(&strings(&["/a"]), "/b", 0)
        .await
        .expect_err("scripted INTERNAL");
    assert_eq!(error.class_name(), "_InactiveRpcError");
    assert_eq!(error.details, "任务已存在 \"q\"");
    assert_eq!(
        error.to_string(),
        "<_InactiveRpcError of RPC that terminated with:\n\tstatus = StatusCode.INTERNAL\n\tdetails = \"任务已存在 \"q\"\"\n\tdebug_error_string = \"INTERNAL:任务已存在 \"q\"\"\n>"
    );
}

fn short(timeout: Duration) -> Timeouts {
    Timeouts {
        general: timeout,
        move_or_delete: timeout,
        list_offline: timeout,
    }
}

/// A listener that accepts and then says nothing: a server that stalls.
async fn stalling_listener() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address").to_string();
    let task = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    (address, task)
}

/// A listener that reads a little of each connection and hangs up.
async fn hanging_up_listener() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address").to_string();
    let task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let mut buffer = [0_u8; 16];
            let _ = stream.read(&mut buffer).await;
            drop(stream);
        }
    });
    (address, task)
}

#[tokio::test]
async fn a_stalled_server_is_a_deadline_exceeded() {
    let (address, _task) = stalling_listener().await;
    let mut options = options(&address);
    options.timeouts = short(Duration::from_millis(300));
    let client = CloudDriveClient::new(&options);
    let started = std::time::Instant::now();
    let error = client.get_system_info().await.expect_err("stall");
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(
        error.to_string(),
        "<_InactiveRpcError of RPC that terminated with:\n\tstatus = StatusCode.DEADLINE_EXCEEDED\n\tdetails = \"Deadline Exceeded\"\n\tdebug_error_string = \"DEADLINE_EXCEEDED:Deadline Exceeded\"\n>"
    );
    let error = client.get_sub_files("/", false).await.expect_err("stall");
    assert_eq!(error.class_name(), "_MultiThreadedRendezvous");
    assert_eq!(error.details(), Some("Deadline Exceeded"));
}

#[tokio::test]
async fn a_held_call_times_out_on_the_server_too() {
    // The deadline covers the whole call, not only the connection: a call the server sits on.
    let (served, _) = fake(r#"{"directories": ["/a"]}"#).await;
    let hold = served.fake.hold("GetSubFiles");
    let mut options = options(&served.address());
    options.timeouts = short(Duration::from_millis(300));
    let client = CloudDriveClient::new(&options);
    let error = client.get_sub_files("/a", true).await.expect_err("held");
    assert_eq!(error.details(), Some("Deadline Exceeded"));
    assert_eq!(error.as_rpc().map(|error| error.code), Some(Code::DeadlineExceeded));
    hold.release();
}

#[tokio::test]
async fn a_refused_connection_or_a_hang_up_is_unavailable() {
    let client = CloudDriveClient::new(&options("127.0.0.1:1"));
    let error = client.get_system_info().await.expect_err("refused");
    assert_eq!(error.code, Code::Unavailable, "{error}");
    assert_eq!(error.class_name(), "_InactiveRpcError");
    let error = client.get_sub_files("/", false).await.expect_err("refused");
    assert_eq!(
        error.as_rpc().map(|error| error.code),
        Some(Code::Unavailable),
        "{error}"
    );

    let (address, _task) = hanging_up_listener().await;
    let client = CloudDriveClient::new(&options(&address));
    let error = client.get_system_info().await.expect_err("hang-up");
    assert_eq!(error.code, Code::Unavailable, "{error}");
    let error = client.move_file(&strings(&["/a"]), "/b", 2).await.expect_err("hang-up");
    assert_eq!(error.code, Code::Unavailable, "{error}");
}

#[tokio::test]
async fn an_address_that_is_not_a_uri_fails_each_call_as_unavailable() {
    let client = CloudDriveClient::new(&options("not a host"));
    let error = client.get_system_info().await.expect_err("bad address");
    assert_eq!(error.code, Code::Unavailable);
    assert!(error.details.starts_with("invalid CloudDrive address"), "{error}");
}

// ---------- aio.py: listings, stat, ensure_directory, move ----------

fn file(id: &str, name: &str, full_path: &str) -> proto::CloudDriveFile {
    proto::CloudDriveFile {
        id: id.to_owned(),
        name: name.to_owned(),
        full_path_name: full_path.to_owned(),
        ..Default::default()
    }
}

// py: tests/test_clouddrive.py::test_cloud_file_metadata_uses_fresh_exact_parent_listing
#[tokio::test]
async fn cloud_file_metadata_uses_fresh_exact_parent_listing() {
    let mut matching = file("file-id", "ABC-001.mp4", "/cloud/library/source-b/ABC/ABC-001.mp4");
    matching.size = 123;
    matching.write_time = Some(prost_types::Timestamp {
        seconds: 456,
        nanos: 789,
    });
    matching.file_hashes.insert(2, "sha1-value".to_owned());
    let other = file("other-id", "ABC-001.mp4", "/different/path/ABC-001.mp4");
    let script = Scripted::default();
    let reply = listing(vec![other, matching]);
    script.reply("GetSubFiles", &reply).reply("GetSubFiles", &reply);
    let (cloud, script, _task) = scripted(script).await;

    let files = cloud
        .list_directory("/cloud/library/source-b/ABC", true)
        .await
        .expect("listing");
    let metadata = cloud
        .stat_file("/cloud/library/source-b/ABC/ABC-001.mp4")
        .await
        .expect("stat");

    assert_eq!(
        pyjson::dumps(&files[1].to_value(), &pyjson::DumpOptions::plain()).expect("dumps"),
        r#"{"id": "file-id", "name": "ABC-001.mp4", "full_path": "/cloud/library/source-b/ABC/ABC-001.mp4", "size": 123, "is_directory": false, "write_time": {"seconds": 456, "nanos": 789}, "hashes": {"2": "sha1-value"}}"#
    );
    assert_eq!(metadata.as_ref(), Some(&files[1]));
    let requests = script.requests("GetSubFiles");
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert_eq!(request.get("path"), Some(&Value::from("/cloud/library/source-b/ABC")));
        assert_eq!(request.get("forceRefresh"), Some(&Value::Bool(true)));
    }
}

// py: tests/test_clouddrive.py::test_cloud_file_stat_returns_none_for_missing_parent
#[tokio::test]
async fn cloud_file_stat_returns_none_for_missing_parent() {
    let script = Scripted::default();
    script.fail("GetSubFiles", Code::NotFound, "");
    let (cloud, _, _task) = scripted(script).await;

    assert_eq!(
        cloud
            .stat_file("/cloud/library/source-b/ABC/missing.mp4")
            .await
            .expect("stat"),
        None
    );
}

#[tokio::test]
async fn stat_lets_other_listing_errors_through() {
    let (served, _) = fake(r#"{"files": [{"path": "/115/file"}]}"#).await;
    let cloud = AsyncCloudDrive::connect(&options(&served.address()));
    let error = cloud.stat_file("/115/file/child").await.expect_err("parent is a file");
    assert_eq!(error.class_name(), "NotADirectoryError");
    assert_eq!(
        cloud.stat_file("/115/file").await.expect("stat").map(|file| file.size),
        Some(0)
    );
    assert_eq!(cloud.stat_file("/115/missing").await.expect("stat"), None);
    let error = cloud.stat_file("/").await.expect_err("root");
    assert_eq!(error.to_string(), INVALID_API_PATH);
}

// py: tests/test_clouddrive.py::test_move_cloud_file_always_uses_skip_conflict_policy
#[tokio::test]
async fn move_cloud_file_always_uses_skip_conflict_policy() {
    let script = Scripted::default();
    script.reply(
        "MoveFile",
        &proto::FileOperationResult {
            success: true,
            error_message: String::new(),
            result_file_paths: strings(&["/cloud/library/move-in/ABC/ABC-001.mp4"]),
        },
    );
    let (cloud, script, _task) = scripted(script).await;

    let response = cloud
        .move_file("/cloud/library/source-b/ABC/ABC-001.mp4", "/cloud/library/move-in/ABC")
        .await
        .expect("move");

    assert!(response.success);
    assert_eq!(response.error_message, "");
    assert_eq!(
        response.result_file_paths,
        strings(&["/cloud/library/move-in/ABC/ABC-001.mp4"])
    );
    let requests = script.requests("MoveFile");
    assert_eq!(requests.len(), 1);
    assert_eq!(
        pyjson::dumps(&requests[0], &pyjson::DumpOptions::python_default()).expect("dumps"),
        r#"{"theFilePaths": ["/cloud/library/source-b/ABC/ABC-001.mp4"], "destPath": "/cloud/library/move-in/ABC", "conflictPolicy": "Skip"}"#
    );
}

// py: tests/test_clouddrive.py::test_ensure_cloud_directory_creates_and_force_refreshes_one_safe_child
#[tokio::test]
async fn ensure_cloud_directory_creates_and_force_refreshes_one_safe_child() {
    let mut folder = file("folder-id", "ABC", "/cloud/library/move-in/ABC");
    folder.is_directory = true;
    let script = Scripted::default();
    script
        .reply("GetSubFiles", &listing(vec![]))
        .reply("GetSubFiles", &listing(vec![folder]))
        .reply("CreateFolder", &proto::CreateFolderResult::default());
    let (cloud, script, _task) = scripted(script).await;

    let result = cloud
        .ensure_directory("/cloud/library/move-in", "ABC")
        .await
        .expect("ensure");

    assert!(result.success && result.created);
    assert_eq!(result.path, "/cloud/library/move-in/ABC");
    let creates = script.requests("CreateFolder");
    assert_eq!(creates.len(), 1);
    assert_eq!(
        creates[0].get("parentPath"),
        Some(&Value::from("/cloud/library/move-in"))
    );
    assert_eq!(creates[0].get("folderName"), Some(&Value::from("ABC")));
    for request in script.requests("GetSubFiles") {
        assert_eq!(request.get("forceRefresh"), Some(&Value::Bool(true)));
    }
}

// py: tests/test_clouddrive.py::test_ensure_cloud_directory_accepts_verified_folder_after_create_error
#[tokio::test]
async fn ensure_cloud_directory_accepts_verified_folder_after_create_error() {
    let mut folder = file("folder-id", "ABC", "/cloud/library/move-in/ABC");
    folder.is_directory = true;
    let script = Scripted::default();
    script
        .reply("GetSubFiles", &listing(vec![]))
        .reply("GetSubFiles", &listing(vec![folder]))
        .fail("CreateFolder", Code::DeadlineExceeded, "Deadline Exceeded");
    let (cloud, _, _task) = scripted(script).await;

    let result = cloud
        .ensure_directory("/cloud/library/move-in", "ABC")
        .await
        .expect("ensure");

    assert!(result.success && result.created);
    assert_eq!(result.path, "/cloud/library/move-in/ABC");
}

#[tokio::test]
async fn ensure_cloud_directory_raises_the_create_error_when_nothing_appeared() {
    let script = Scripted::default();
    script
        .reply("GetSubFiles", &listing(vec![]))
        .reply("GetSubFiles", &listing(vec![]))
        .fail("CreateFolder", Code::PermissionDenied, "read-only");
    let (cloud, _, _task) = scripted(script).await;
    let error = cloud
        .ensure_directory("/cloud/library/move-in", "ABC")
        .await
        .expect_err("create failed");
    assert_eq!(error.details(), Some("read-only"));

    // Without an error and without the folder, the answer is a plain failure.
    let script = Scripted::default();
    script
        .reply("GetSubFiles", &listing(vec![]))
        .reply("GetSubFiles", &listing(vec![]))
        .reply("CreateFolder", &proto::CreateFolderResult::default());
    let (cloud, _, _task) = scripted(script).await;
    let result = cloud
        .ensure_directory("/cloud/library/move-in", "ABC")
        .await
        .expect("ensure");
    assert!(!result.success && !result.created);
}

// py: tests/test_clouddrive.py::test_ensure_cloud_directory_rejects_same_named_file_without_creating
#[tokio::test]
async fn ensure_cloud_directory_rejects_same_named_file_without_creating() {
    let script = Scripted::default();
    script.reply(
        "GetSubFiles",
        &listing(vec![file("file-id", "ABC", "/cloud/library/move-in/ABC")]),
    );
    let (cloud, script, _task) = scripted(script).await;

    let result = cloud
        .ensure_directory("/cloud/library/move-in", "ABC")
        .await
        .expect("ensure");

    assert!(!result.success && !result.created);
    assert_eq!(result.path, "/cloud/library/move-in/ABC");
    assert!(script.requests("CreateFolder").is_empty());
}

// py: tests/test_clouddrive.py::test_ensure_cloud_directory_rejects_unsafe_child_name cases=6
#[tokio::test]
async fn ensure_cloud_directory_rejects_unsafe_child_name() {
    // Nothing listens there: the refusal comes before any call.
    let cloud = AsyncCloudDrive::connect(&options("127.0.0.1:1"));
    for name in ["", ".", "..", "nested/ABC", "bad\\name", "bad\nname"] {
        let error = cloud
            .ensure_directory("/cloud/library/move-in", name)
            .await
            .expect_err(name);
        assert_eq!(error.class_name(), "ValueError");
        assert!(error.to_string().contains("safe path segment"), "{name:?}: {error}");
    }
}

// py: tests/test_clouddrive.py::test_async_clouddrive_rejects_noncanonical_api_paths cases=5
#[tokio::test]
async fn async_clouddrive_rejects_noncanonical_api_paths() {
    let cloud = AsyncCloudDrive::connect(&options("127.0.0.1:1"));
    for value in [
        "relative/path",
        "//host/path",
        "/path/../escape",
        "/path/",
        "/path\nname",
    ] {
        let error = cloud.list_directory(value, true).await.expect_err(value);
        assert_eq!(error.class_name(), "ValueError");
        assert!(
            error.to_string().contains("canonical absolute POSIX path"),
            "{value:?}: {error}"
        );
    }
}

#[tokio::test]
async fn every_path_taking_call_validates_before_sending() {
    let (served, recorder) = fake("{}").await;
    let cloud = AsyncCloudDrive::connect(&options(&served.address()));
    let bad = "/a/../b";
    assert!(cloud.move_file(bad, "/x").await.is_err());
    assert!(cloud.move_file("/x/a", bad).await.is_err());
    assert!(cloud.move_file("/", "/x").await.is_err());
    assert!(cloud.copy_file(bad, "/x").await.is_err());
    assert!(cloud.copy_file("/x/a", bad).await.is_err());
    assert!(cloud.add_offline_files(&strings(&["magnet:"]), bad).await.is_err());
    assert!(cloud.list_offline_files(bad).await.is_err());
    assert!(cloud.remove_offline_files(&strings(&["H"]), bad, false).await.is_err());
    assert!(cloud.delete_files(&strings(&["/ok", "/"])).await.is_err());
    assert!(cloud.ensure_directory(bad, "x").await.is_err());
    assert!(recorder.calls().is_empty());
}

// ---------- aio.py: the cancel-safe wrapper ----------

// py: tests/test_clouddrive.py::test_cancelled_cloud_move_waits_for_sync_call_before_returning
#[tokio::test]
async fn cancelled_cloud_move_waits_for_sync_call_before_returning() {
    // Python's cancelled caller only finishes once the RPC has, and the channel stays open.
    // A dropped Rust future finishes at once, so the work that is cancelled runs on a scoped
    // handle and whoever cancelled it awaits `settle()`: that is what must not return early.
    let (served, recorder) = fake(
        r#"{"directories": ["/cloud/library/source-b/ABC", "/cloud/library/move-in/ABC"],
            "errors": [{"method": "MoveFile", "code": "INTERNAL", "details": "late gRPC failure"}]}"#,
    )
    .await;
    let hold = served.fake.hold("MoveFile");
    let cloud = AsyncCloudDrive::connect(&options(&served.address()));
    let scope = cloud.scoped();
    let caller = scope.clone();
    let task = tokio::spawn(async move {
        caller
            .move_file("/cloud/library/source-b/ABC/ABC-001.mp4", "/cloud/library/move-in/ABC")
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), hold.arrived())
        .await
        .expect("the move reached the server");

    task.abort();
    assert!(task.await.expect_err("aborted").is_cancelled());
    let settled = tokio::spawn({
        let scope = scope.clone();
        async move { scope.settle().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!settled.is_finished(), "settle() returned while the move was in flight");
    assert!(calls_of(&recorder, "MoveFile").is_empty());
    // `close.assert_not_called()`: the client is still open and takes other calls.
    assert_eq!(cloud.in_flight(), 1);
    assert!(cloud.check().await.is_ok());

    hold.release();
    tokio::time::timeout(Duration::from_secs(5), settled)
        .await
        .expect("settle() returns once the move is done")
        .expect("settle");
    assert_eq!(cloud.in_flight(), 0);
    // The server saw the whole call and answered it: the late failure, swallowed.
    let calls = calls_of(&recorder, "MoveFile");
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0].get("error").and_then(|error| error.get("details")),
        Some(&Value::from("late gRPC failure"))
    );
    cloud.close().await;
}

#[tokio::test]
async fn settle_waits_only_for_its_own_scope() {
    let (served, _) = fake(r#"{"directories": ["/in", "/out"], "files": [{"path": "/in/a"}]}"#).await;
    let hold = served.fake.hold("MoveFile");
    let cloud = AsyncCloudDrive::connect(&options(&served.address()));
    let (busy, quiet) = (cloud.scoped(), cloud.scoped());
    let caller = busy.clone();
    let task = tokio::spawn(async move { caller.move_file("/in/a", "/out").await });
    hold.arrived().await;
    // Another scope and an unscoped handle settle at once; the busy scope does not.
    tokio::time::timeout(Duration::from_secs(1), quiet.settle())
        .await
        .expect("quiet scope");
    tokio::time::timeout(Duration::from_secs(1), cloud.settle())
        .await
        .expect("no scope");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), busy.settle())
            .await
            .is_err()
    );
    hold.release();
    tokio::time::timeout(Duration::from_secs(5), busy.settle())
        .await
        .expect("busy scope");
    task.await.expect("task").expect("move");
    cloud.close().await;
}

#[tokio::test]
async fn close_waits_for_a_move_whose_caller_is_gone() {
    let (served, recorder) = fake(
        r#"{"directories": ["/cloud/library/source-b/ABC", "/cloud/library/move-in/ABC"],
            "errors": [{"method": "MoveFile", "code": "INTERNAL", "details": "late gRPC failure"}]}"#,
    )
    .await;
    let hold = served.fake.hold("MoveFile");
    let cloud = AsyncCloudDrive::connect(&options(&served.address()));
    let caller = cloud.clone();
    let task = tokio::spawn(async move {
        caller
            .move_file("/cloud/library/source-b/ABC/ABC-001.mp4", "/cloud/library/move-in/ABC")
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), hold.arrived())
        .await
        .expect("the move reached the server");

    // The caller goes away while the RPC is on the server; shutdown must still wait.
    task.abort();
    assert!(task.await.expect_err("aborted").is_cancelled());
    assert_eq!(cloud.in_flight(), 1);
    let closing = tokio::spawn({
        let cloud = cloud.clone();
        async move { cloud.close().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!closing.is_finished(), "close() returned while the move was in flight");
    assert!(calls_of(&recorder, "MoveFile").is_empty());

    hold.release();
    tokio::time::timeout(Duration::from_secs(5), closing)
        .await
        .expect("close() returns once the move is done")
        .expect("close");
    assert_eq!(cloud.in_flight(), 0);
    assert_eq!(calls_of(&recorder, "MoveFile").len(), 1);
    // A closed wrapper refuses new calls, as a closed grpcio channel does.
    let error = cloud.list_directory("/", true).await.expect_err("closed");
    assert_eq!(error.class_name(), "ValueError");
    assert_eq!(error.to_string(), "Cannot invoke RPC on closed channel!");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_admitted_while_closing_is_waited_for_or_refused() {
    // close() racing new calls: every call either is refused or finishes before close()
    // returns; none is sent after it.
    for _ in 0..50 {
        let (served, recorder) = fake(r#"{"directories": ["/in"]}"#).await;
        let cloud = AsyncCloudDrive::connect(&options(&served.address()));
        let callers: Vec<_> = (0..8)
            .map(|_| {
                let cloud = cloud.clone();
                tokio::spawn(async move { cloud.list_directory("/in", true).await.is_ok() })
            })
            .collect();
        cloud.close().await;
        let seen_at_close = calls_of(&recorder, "GetSubFiles").len();
        let mut answered = 0;
        for caller in callers {
            answered += usize::from(caller.await.expect("caller"));
        }
        assert_eq!(seen_at_close, answered, "a call ran after close() returned");
        assert_eq!(calls_of(&recorder, "GetSubFiles").len(), answered);
    }
}

#[tokio::test]
async fn a_dropped_plain_tonic_call_is_abandoned_which_is_why_the_wrapper_exists() {
    // The control for the tests above: without the wrapper, dropping the caller resets the
    // stream and the server never finishes the move. The wrapper's test is only meaningful
    // because this one shows the difference.
    let (served, recorder) = fake(r#"{"directories": ["/in", "/out"], "files": [{"path": "/in/a"}]}"#).await;
    let hold = served.fake.hold("MoveFile");
    let client = CloudDriveClient::new(&options(&served.address()));
    let task = tokio::spawn(async move { client.move_file(&strings(&["/in/a"]), "/out", 2).await });
    tokio::time::timeout(Duration::from_secs(5), hold.arrived())
        .await
        .expect("the move reached the server");
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    hold.release();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        calls_of(&recorder, "MoveFile").is_empty(),
        "the server finished an abandoned call"
    );
    assert!(
        served
            .fake
            .dump()
            .get("files")
            .and_then(Value::as_list)
            .is_some_and(|files| { files.iter().any(|file| file.get("path") == Some(&Value::from("/in/a"))) })
    );
}

#[tokio::test]
async fn a_caller_that_stays_gets_its_answer_through_the_task() {
    let (served, _) = fake(r#"{"directories": ["/in", "/out"], "files": [{"path": "/in/a"}]}"#).await;
    let hold = served.fake.hold("MoveFile");
    let cloud = AsyncCloudDrive::connect(&options(&served.address()));
    let caller = cloud.clone();
    let task = tokio::spawn(async move { caller.move_file("/in/a", "/out").await });
    hold.arrived().await;
    assert_eq!(cloud.in_flight(), 1);
    hold.release();
    let moved = task.await.expect("task").expect("move");
    assert_eq!(moved.result_file_paths, strings(&["/out/a"]));
    assert_eq!(cloud.in_flight(), 0);
    cloud.close().await;
}

// ---------- aio.py: offline tasks, copies, uploads ----------

fn offline_file() -> proto::OfflineFile {
    proto::OfflineFile {
        name: "ABC-123".to_owned(),
        size: 1024,
        url: "magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335aa7c1367a88a".to_owned(),
        status: proto::OfflineFileStatus::OfflineDownloading.into(),
        info_hash: "c12fe1c06bba254a9dc9f519b335aa7c1367a88a".to_owned(),
        file_id: "42".to_owned(),
        add_time: 1_755_000_000,
        percend_done: 33.33,
        peers: 7,
        ..Default::default()
    }
}

// py: tests/test_clouddrive.py::test_list_offline_files_by_path_keeps_every_status
#[tokio::test]
async fn list_offline_files_by_path_keeps_every_status() {
    let statuses = [
        proto::OfflineFileStatus::OfflineDownloading,
        proto::OfflineFileStatus::OfflineFinished,
        proto::OfflineFileStatus::OfflineError,
    ];
    let result = proto::OfflineFileListResult {
        offline_files: statuses
            .iter()
            .map(|status| proto::OfflineFile {
                status: (*status).into(),
                ..offline_file()
            })
            .collect(),
        ..Default::default()
    };
    let script = Scripted::default();
    script.reply("ListOfflineFilesByPath", &result);
    let (address, _task) = script.clone().serve().await;
    let client = CloudDriveClient::new(&options(&address));

    let files = client.list_offline_files_by_path("/media/tasks").await.expect("list");

    let seen: Vec<i32> = files.iter().map(|file| file.status).collect();
    assert_eq!(seen, statuses.map(i32::from).to_vec());
    assert_eq!(
        script.requests("ListOfflineFilesByPath")[0].get("path"),
        Some(&Value::from("/media/tasks"))
    );
}

// py: tests/test_clouddrive.py::test_offline_task_view_exposes_the_fields_the_tracker_joins_on
#[tokio::test]
async fn offline_task_view_exposes_the_fields_the_tracker_joins_on() {
    let script = Scripted::default();
    script.reply(
        "ListOfflineFilesByPath",
        &proto::OfflineFileListResult {
            offline_files: vec![offline_file()],
            ..Default::default()
        },
    );
    let (cloud, _, _task) = scripted(script).await;

    let tasks = cloud.list_offline_files("/media/tasks").await.expect("list");

    assert_eq!(tasks.len(), 1);
    let task = &tasks[0];
    assert_eq!(
        (task.name.as_str(), task.size, task.url.as_str()),
        (
            "ABC-123",
            1024,
            "magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335aa7c1367a88a"
        )
    );
    assert_eq!(task.status, OfflineStatus::Downloading);
    assert_eq!(task.status.as_str(), "OFFLINE_DOWNLOADING");
    assert_eq!(task.info_hash, "C12FE1C06BBA254A9DC9F519B335AA7C1367A88A");
    assert_eq!(
        (task.file_id.as_str(), task.add_time, task.peers),
        ("42", 1_755_000_000, 7)
    );
    assert!((task.progress - 33.33).abs() < 1e-6);
}

// py: tests/test_clouddrive.py::test_offline_task_hash_matches_the_hash_parsed_from_our_magnet
#[tokio::test]
async fn offline_task_hash_matches_the_hash_parsed_from_our_magnet() {
    let task = offline_file();
    let script = Scripted::default();
    script.reply(
        "ListOfflineFilesByPath",
        &proto::OfflineFileListResult {
            offline_files: vec![task.clone()],
            ..Default::default()
        },
    );
    let (cloud, _, _task) = scripted(script).await;

    let tasks = cloud.list_offline_files("/media/tasks").await.expect("list");

    assert_eq!(
        Some(tasks[0].info_hash.clone()),
        embyx_core::magnet::extract_info_hash(&task.url)
    );
}

fn upload(dest: &str, status: &str, error: &str, sent: u64) -> proto::UploadFileInfo {
    proto::UploadFileInfo {
        dest_path: dest.to_owned(),
        status: status.to_owned(),
        error_message: error.to_owned(),
        transfered_bytes: sent,
        ..Default::default()
    }
}

// py: tests/test_clouddrive.py::test_upload_state_reads_the_upload_queue_for_the_exact_path
#[tokio::test]
async fn upload_state_reads_the_upload_queue_for_the_exact_path() {
    let script = Scripted::default();
    script
        .reply(
            "GetUploadFileList",
            &proto::GetUploadFileListResult {
                upload_files: vec![
                    upload("/115/upload/merge-9/ABC-001.mp4", "FatalError", "elsewhere", 0),
                    upload(
                        "/115/upload/merge-1/ABC-001.mp4",
                        "FatalError",
                        "code: 10025, message: 单文件不能大于115GB",
                        0,
                    ),
                ],
                ..Default::default()
            },
        )
        .reply(
            "GetUploadFileList",
            &proto::GetUploadFileListResult {
                upload_files: vec![upload(
                    "/115/upload/merge-1/ABC-001.mp4",
                    "Transfer",
                    "",
                    15_888_023_552,
                )],
                ..Default::default()
            },
        )
        .reply("GetUploadFileList", &proto::GetUploadFileListResult::default());
    let (cloud, script, _task) = scripted(script).await;

    let state = cloud
        .upload_state("/115/upload/merge-1/ABC-001.mp4")
        .await
        .expect("state")
        .expect("queued");
    assert_eq!(state.fatal(), Some("code: 10025, message: 单文件不能大于115GB"));
    let request = &script.requests("GetUploadFileList")[0];
    assert_eq!(request.get("filter"), Some(&Value::from("ABC-001.mp4")));
    assert_eq!(request.get("getAll"), Some(&Value::Bool(true)));

    let state = cloud
        .upload_state("/115/upload/merge-1/ABC-001.mp4")
        .await
        .expect("state")
        .expect("queued");
    assert_eq!(
        state,
        UploadState {
            status: "Transfer".to_owned(),
            transferred: 15_888_023_552,
            error: String::new(),
        }
    );
    assert_eq!(state.fatal(), None);

    assert_eq!(
        cloud
            .upload_state("/115/upload/merge-1/ABC-001.mp4")
            .await
            .expect("state"),
        None
    );
}

#[tokio::test]
async fn copy_tasks_copies_restarts_cancels_and_deletes_against_the_fake() {
    let (served, _) = fake(
        r#"{"directories": ["/115/upload"], "files": [{"path": "/upload/merge/a.mp4", "size": 5}, {"path": "/115/old.mp4"}],
            "copy_tasks": [{"source_path": "/upload/merge/b.mp4", "dest_path": "/115/upload", "status": "Failed",
                            "total_bytes": 9, "uploaded_bytes": 3, "failed_files": 1, "paused": true, "errors": ["boom"]}]}"#,
    )
    .await;
    let cloud = AsyncCloudDrive::connect(&options(&served.address()));

    let copied = cloud
        .copy_file("/upload/merge/a.mp4", "/115/upload")
        .await
        .expect("copy");
    assert!(copied.success);
    let tasks = cloud.copy_tasks().await.expect("tasks");
    assert_eq!(tasks.len(), 2);
    assert_eq!(
        tasks[0],
        CopyTask {
            source_path: "/upload/merge/b.mp4".to_owned(),
            dest_path: "/115/upload".to_owned(),
            status: super::aio::CopyStatus::Failed,
            total_bytes: 9,
            uploaded_bytes: 3,
            failed_files: 1,
            paused: true,
            errors: strings(&["boom"]),
        }
    );
    assert_eq!(tasks[1].status.as_str(), "Pending");
    cloud
        .restart_copy_task("/upload/merge/b.mp4", "/115/upload")
        .await
        .expect("restart");
    assert_eq!(cloud.copy_tasks().await.expect("tasks")[0].errors, Vec::<String>::new());
    cloud
        .cancel_copy_task("/upload/merge/a.mp4", "/115/upload")
        .await
        .expect("cancel");
    assert_eq!(cloud.copy_tasks().await.expect("tasks").len(), 1);
    let deleted = cloud.delete_files(&strings(&["/115/old.mp4"])).await.expect("delete");
    assert!(deleted.success);
    let missing = cloud.delete_files(&strings(&["/115/old.mp4"])).await.expect("delete");
    assert_eq!(
        (missing.success, missing.error_message.as_str()),
        (false, "file not found: /115/old.mp4")
    );
    let check = cloud.check().await.expect("check");
    assert!(check.reachable && check.authenticated);
    assert_eq!(check.version, "");
}

// ---------- goldens ----------

#[test]
fn validation_matches_python() {
    let document = golden::load("clouddrive/validation.json");
    let cases = golden::cases(&document);
    assert!(cases.len() > 50);
    let outcome = |result: Result<&str, CloudDriveError>| match result {
        Ok(value) => format!("value {value}"),
        Err(error) => format!("{} {error}", error.class_name()),
    };
    let expected = |case: &Value| {
        if let Some(value) = case.get("value") {
            format!("value {}", golden::string(value))
        } else {
            let error = golden::get(case, "error");
            format!(
                "{} {}",
                golden::string(golden::get(error, "type")),
                golden::string(golden::get(error, "message"))
            )
        }
    };
    for case in cases {
        let value = golden::string(golden::get(case, "value"));
        if golden::string(golden::get(case, "kind")) == "api_path" {
            assert_eq!(
                outcome(validate_api_path(&value, true)),
                expected(golden::get(case, "allow_root")),
                "{value:?}"
            );
            assert_eq!(
                outcome(validate_api_path(&value, false)),
                expected(golden::get(case, "deny_root")),
                "{value:?}"
            );
        } else {
            assert_eq!(
                outcome(validate_path_segment(&value)),
                expected(golden::get(case, "outcome")),
                "{value:?}"
            );
        }
    }
    assert_eq!(INVALID_SEGMENT, "CloudDrive folder name must be one safe path segment");
}

#[test]
fn conversions_match_python() {
    let document = golden::load("clouddrive/conversions.json");
    let mut kinds = HashMap::new();
    for case in golden::cases(&document) {
        let kind = golden::string(golden::get(case, "kind"));
        let bytes = hex::decode(golden::string(golden::get(case, "bytes"))).expect("hex");
        let expected = golden::get(case, "value");
        let have = match kind.as_str() {
            "cloud_file" => {
                CloudFile::from_proto(&proto::CloudDriveFile::decode(bytes.as_slice()).expect("decode")).to_value()
            }
            "offline_file" => {
                OfflineTask::from_proto(&proto::OfflineFile::decode(bytes.as_slice()).expect("decode")).to_value()
            }
            "copy_task" => CopyTask::from_proto(&proto::CopyTask::decode(bytes.as_slice()).expect("decode")).to_value(),
            "upload_state" => {
                let info = proto::UploadFileInfo::decode(bytes.as_slice()).expect("decode");
                let state = UploadState {
                    status: info.status,
                    transferred: info.transfered_bytes,
                    error: info.error_message,
                };
                let mut dict = pyjson::Dict::new();
                dict.insert("status", Value::from(state.status.as_str()));
                dict.insert("transferred", Value::Int(i128::from(state.transferred)));
                dict.insert("error", Value::from(state.error.as_str()));
                dict.insert("fatal", state.fatal().map_or(Value::Null, Value::from));
                Value::Dict(dict)
            }
            other => panic!("unknown kind {other}"),
        };
        assert_eq!(
            pyjson::dumps(&have, &pyjson::DumpOptions::plain()).expect("dumps"),
            pyjson::dumps(expected, &pyjson::DumpOptions::plain()).expect("dumps"),
            "{kind}"
        );
        *kinds.entry(kind).or_insert(0) += 1;
    }
    assert_eq!(kinds.len(), 4, "{kinds:?}");
}
