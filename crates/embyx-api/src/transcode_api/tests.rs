//! Titles over 115 GB handed to the home transcode worker, against PostgreSQL and the worker's
//! API (Python: `tests/test_merge_transcode.py`), and the listener's own corners: FastAPI's
//! order of body, token and validation, the refusal envelope, and part reads as Starlette's
//! `FileResponse` answers them.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use axum::body::Body;
use chrono::Utc;
use embyx_core::config::models::{ArchiveConfig, CloudDriveConfig, MergeConfig};
use embyx_core::pyjson::{self, Value};
use embyx_db::testdb::TestDatabase;
use embyx_merge::fakes::{FakeCloud, FakeKube};
use embyx_merge::ports::{MergeCloud, MergeKube, Trigger};
use embyx_merge::service::{ActionError, MergeService, ServiceParts, WAITING_FOR_SLOT, WAITING_FOR_WORKER};
use embyx_merge::tasks::{MergeState, MergeTask, MergeTaskRepository, NewTask, TaskField};
use embyx_merge::transcode::{
    TranscodeRepository, WorkerReport, audio_job_name, audio_status_path, chunk_path, transcode_dir,
};
use http::{Method, StatusCode};
use sha2::{Digest as _, Sha256};

use super::create_transcode_router;
use crate::app::create_transcode_app;
use crate::routing::App;
use crate::testing::send;

const LIBRARY: &str = "/mnt/cd2/115/embyx";
const BIG: i64 = 200 * 1_000_000_000;
const DURATIONS: [f64; 3] = [30.0, 20.0, 25.0];
const SHA1: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

fn parts() -> Vec<String> {
    (1..=3)
        .map(|index| format!("{LIBRARY}/type/vr/SQTEVR/SQTEVR-009-cd{index}.mp4"))
        .collect()
}

fn small_parts() -> Vec<String> {
    (1..=2)
        .map(|index| format!("{LIBRARY}/rank/ABP-123-cd{index}.mp4"))
        .collect()
}

fn archive() -> ArchiveConfig {
    ArchiveConfig {
        src_dir: "/mnt/cd2/115/embyx_in".to_owned(),
        dst_dir: LIBRARY.to_owned(),
        mapping: vec![("rank".to_owned(), "rank".to_owned())],
        priority_mapping: vec![("vr".to_owned(), "type/vr".to_owned())],
        ..ArchiveConfig::default()
    }
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

struct Harness {
    test: TestDatabase,
    _tmp: tempfile::TempDir,
    work: PathBuf,
    part_file: PathBuf,
    kube: FakeKube,
    config: Arc<Mutex<MergeConfig>>,
    repository: MergeTaskRepository,
    transcodes: TranscodeRepository,
    service: MergeService,
    app: App,
}

/// One answer: status and body (JSON when it is JSON, else the bytes as text).
struct Answer {
    status: StatusCode,
    headers: http::HeaderMap,
    bytes: bytes::Bytes,
}

impl Answer {
    fn json(&self) -> Value {
        let text = std::str::from_utf8(&self.bytes).expect("UTF-8");
        pyjson::loads(text).unwrap_or_else(|error| panic!("not JSON ({error}): {text}"))
    }

    fn error(&self) -> Value {
        self.json().get("error").cloned().expect("an error envelope")
    }
}

fn json(text: &str) -> Value {
    pyjson::loads(text).expect("JSON")
}

impl Harness {
    async fn new() -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let work = tmp.path().join("upload");
        std::fs::create_dir(&work).expect("mkdir");
        let part_file = tmp.path().join("part.bin");
        let pattern: Vec<u8> = (0..=255).collect();
        std::fs::write(&part_file, pattern.repeat(64)).expect("part");
        let cloud = FakeCloud::new(&["/", "/115", "/115/upload", "/115/embyx_in", "/115/embyx/type/vr/SQTEVR"]);
        let kube = FakeKube::default();
        let test = TestDatabase::create().await;
        let repository = MergeTaskRepository::new(test.database());
        let transcodes = TranscodeRepository::new(test.database());
        let config = Arc::new(Mutex::new(MergeConfig {
            work_dir: work.to_string_lossy().into_owned(),
            home_transcode_enabled: true,
            ..MergeConfig::default()
        }));
        let trigger: Trigger = Arc::new(|_| Box::pin(async { Ok(()) }));
        let shared = Arc::clone(&config);
        let mut service_parts = ServiceParts::new(
            repository.clone(),
            Arc::new(move || Some(Arc::new(cloud.clone()) as Arc<dyn MergeCloud>)),
            Some(Arc::new(kube.clone()) as Arc<dyn MergeKube>),
            Arc::new(move || shared.lock().unwrap_or_else(PoisonError::into_inner).clone()),
            Arc::new(archive),
            Arc::new(|| CloudDriveConfig {
                address: "cd:19798".to_owned(),
                api_token: "t".to_owned(),
                ..CloudDriveConfig::default()
            }),
            trigger,
        );
        service_parts.transcodes = Some(transcodes.clone());
        // The SQTEVR parts are the oversize title; anything else is small.
        service_parts.sizes = Arc::new(|parts: &[String]| Ok(if parts[0].contains("SQTEVR") { BIG } else { 300 }));
        let service = MergeService::new(service_parts);
        let app = create_transcode_app(create_transcode_router(service.clone(), Some("secret")));
        Self {
            test,
            _tmp: tmp,
            work,
            part_file,
            kube,
            config,
            repository,
            transcodes,
            service,
            app,
        }
    }

    async fn close(self) {
        self.repository.database().close().await;
        self.test.drop_database().await;
    }

    async fn create_with(&self, avid: &str, parts: Vec<String>) -> MergeTask {
        self.repository
            .create(&NewTask {
                avid: avid.to_owned(),
                source: "vr".to_owned(),
                library_dir: "type/vr".to_owned(),
                brand: avid.split('-').next().expect("a brand").to_owned(),
                parts,
                merged_name: format!("{avid}.mp4"),
            })
            .await
            .expect("create")
    }

    async fn create(&self) -> MergeTask {
        self.create_with("SQTEVR-009", parts()).await
    }

    async fn state(&self, task: &MergeTask) -> MergeTask {
        self.repository.get(task.id).await.expect("get").expect("the task")
    }

    async fn request(&self, method: Method, path: &str, headers: &[(&str, String)], body: Vec<u8>) -> Answer {
        let mut builder = http::Request::builder()
            .method(method)
            .uri(path)
            .header("host", "worker")
            .header("authorization", "Bearer secret");
        for (name, value) in headers {
            builder = builder.header(*name, value.as_str());
        }
        let (parts, bytes) = send(&self.app, builder.body(Body::from(body)).expect("a request")).await;
        Answer {
            status: parts.status,
            headers: parts.headers,
            bytes,
        }
    }

    async fn post_json(&self, path: &str, body: &str) -> Answer {
        self.request(
            Method::POST,
            path,
            &[("content-type", "application/json".to_owned())],
            body.as_bytes().to_vec(),
        )
        .await
    }

    async fn claim(&self) -> Value {
        let answer = self
            .post_json("/api/transcode/claim", r#"{"name": "pc", "state": "idle"}"#)
            .await;
        assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.bytes);
        answer.json()
    }

    async fn heartbeat(&self, body: &str) -> Value {
        self.post_json("/api/transcode/heartbeat", body).await.json()
    }

    /// What the audio Job reports once it has probed the parts.
    fn prepare(&self, task: &MergeTask, state: &str) {
        let path = audio_status_path(&transcode_dir(&self.work, task.id));
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("mkdir");
        let parts: Vec<String> = DURATIONS
            .iter()
            .map(|seconds| format!(r#"{{"duration": {seconds:?}, "size": {}}}"#, BIG / 3))
            .collect();
        std::fs::write(
            &path,
            format!(
                r#"{{"state": "{state}", "video_bitrate": 20000000, "duration": 75.0, "parts": [{}]}}"#,
                parts.join(", ")
            ),
        )
        .expect("write");
    }

    async fn put(&self, task: &MergeTask, part: i64, index: i64, start: f64, duration: f64, body: &[u8]) -> Answer {
        self.put_raw(task.id, part, index, start, duration, body, &sha256(body))
            .await
    }

    #[allow(clippy::too_many_arguments, reason = "the chunk's headers")]
    async fn put_raw(
        &self,
        task_id: i64,
        part: i64,
        index: i64,
        start: f64,
        duration: f64,
        body: &[u8],
        digest: &str,
    ) -> Answer {
        // `str(float)` as Python's test sends it.
        let text = |value: f64| format!("{value:?}");
        #[allow(clippy::cast_possible_truncation, reason = "small test durations")]
        let frames = (duration * 30.0) as i64;
        self.request(
            Method::PUT,
            &format!("/api/transcode/tasks/{task_id}/parts/{part}/chunks/{index}"),
            &[
                ("x-chunk-start", text(start)),
                ("x-chunk-duration", text(duration)),
                ("x-chunk-frames", frames.to_string()),
                ("x-chunk-sha256", digest.to_owned()),
                ("content-length", body.len().to_string()),
            ],
            body.to_vec(),
        )
        .await
    }

    async fn finish(&self, task: &MergeTask, part: i64) -> Answer {
        self.request(
            Method::POST,
            &format!("/api/transcode/tasks/{}/parts/{part}/finish", task.id),
            &[],
            Vec::new(),
        )
        .await
    }

    async fn encode_all(&self, task: &MergeTask) {
        for (part, seconds) in DURATIONS.iter().enumerate() {
            let part = i64::try_from(part).expect("small");
            let body = format!("cd{part}");
            assert_eq!(
                self.put(task, part, 0, 0.0, *seconds, body.as_bytes()).await.status,
                StatusCode::OK
            );
            assert_eq!(self.finish(task, part).await.status, StatusCode::OK);
        }
    }

    fn created(&self) -> Vec<(String, Vec<String>)> {
        self.kube.state().created.clone()
    }
}

fn wait(wait: &str, task_id: Option<i64>) -> Value {
    match task_id {
        Some(id) => json(&format!(r#"{{"task": null, "wait": "{wait}", "task_id": {id}}}"#)),
        None => json(&format!(r#"{{"task": null, "wait": "{wait}"}}"#)),
    }
}

// py: tests/test_merge_transcode.py::test_an_oversize_title_waits_for_the_worker_without_holding_up_the_queue
#[tokio::test]
async fn an_oversize_title_waits_for_the_worker_without_holding_up_the_queue() {
    let harness = Harness::new().await;
    let big = harness.create().await;
    let small = harness.create_with("ABP-123", small_parts()).await;

    harness.service.tick().await.expect("tick");

    let big = harness.state(&big).await;
    assert_eq!(
        (big.state, big.notice.as_deref()),
        (MergeState::Queued, Some(WAITING_FOR_WORKER))
    );
    assert!(harness.service.parked(&big));
    assert_eq!(harness.state(&small).await.state, MergeState::Merging);
    // The worker waits while the cluster merges something else, and once it is there the
    // title holds back auto-merge so that it comes next.
    assert_eq!(harness.claim().await, wait("busy", None));
    assert!(!harness.service.parked(&big));
    assert!(harness.service.reserved_bytes(std::slice::from_ref(&big)) > 0);
    harness.close().await;
}

// py: tests/test_merge_transcode.py::test_the_worker_starts_while_an_earlier_title_is_only_uploading
#[tokio::test]
async fn the_worker_starts_while_an_earlier_title_is_only_uploading() {
    let harness = Harness::new().await;
    let big = harness.create().await;
    let small = harness.create_with("ABP-123", small_parts()).await;
    harness
        .repository
        .transition(small.id, MergeState::Queued, MergeState::Uploading, Vec::new())
        .await
        .expect("transition");

    assert_eq!(harness.claim().await, wait("probing", Some(big.id)));
    assert_eq!(harness.state(&big).await.state, MergeState::Transcoding);
    harness.close().await;
}

// py: tests/test_merge_transcode.py::test_without_the_setting_an_oversize_title_goes_to_the_clusters_own_encoder
#[tokio::test]
async fn without_the_setting_an_oversize_title_goes_to_the_clusters_own_encoder() {
    let harness = Harness::new().await;
    harness
        .config
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .home_transcode_enabled = false;
    let task = harness.create().await;

    harness.service.tick().await.expect("tick");

    assert_eq!(harness.state(&task).await.state, MergeState::Merging);
    assert_eq!(harness.claim().await, wait("idle", None));
    harness.close().await;
}

fn part_progress(task: &Value) -> Vec<(Value, Value, Value, Value)> {
    task.get("parts")
        .and_then(Value::as_list)
        .expect("parts")
        .iter()
        .map(|part| {
            let field = |name: &str| part.get(name).cloned().expect("a field");
            (field("index"), field("next_chunk"), field("resume_at"), field("done"))
        })
        .collect()
}

// py: tests/test_merge_transcode.py::test_the_worker_streams_chunks_and_the_cluster_assembles_them
#[tokio::test]
#[allow(clippy::too_many_lines, reason = "one Python test, step for step")]
async fn the_worker_streams_chunks_and_the_cluster_assembles_them() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    harness.service.tick().await.expect("tick");

    assert_eq!(harness.claim().await, wait("probing", Some(task.id)));
    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.transcoder.as_deref()),
        (MergeState::Transcoding, Some("home"))
    );
    let created = harness.created();
    assert_eq!(created.len(), 1);
    let (name, args) = &created[0];
    assert_eq!(*name, audio_job_name(task.id, &task.avid));
    assert!(args.iter().any(|arg| arg == "--home-prepare"));
    assert_eq!(args[args.len() - 3..].to_vec(), parts());

    harness.prepare(&task, "audio");
    let answer = harness.claim().await;
    let assigned = answer.get("task").cloned().expect("a task");
    assert_eq!(assigned.get("id"), Some(&Value::Int(i128::from(task.id))));
    assert_eq!(assigned.get("video_bitrate"), Some(&Value::Int(20_000_000)));
    let start = |index: i128| (Value::Int(index), Value::Int(0), Value::Float(0.0), Value::Bool(false));
    assert_eq!(part_progress(&assigned), vec![start(0), start(1), start(2)]);

    let first = harness.put(&task, 0, 0, 0.0, 20.0, b"first").await;
    assert_eq!(first.json(), json(r#"{"stored": true}"#));
    // A repeat of a stored chunk is taken as already there.
    assert_eq!(
        harness.put(&task, 0, 0, 0.0, 20.0, b"first").await.json(),
        json(r#"{"stored": false}"#)
    );
    // Not the chunk the cluster has under that number.
    let conflict = harness.put(&task, 0, 0, 0.0, 20.0, b"other").await;
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(
        conflict.error().get("code"),
        Some(&Value::from("transcode_chunk_conflict"))
    );
    // A gap in the part.
    let gap = harness.put(&task, 0, 1, 25.0, 5.0, b"chunk").await;
    assert_eq!(gap.status, StatusCode::CONFLICT);
    assert_eq!(
        gap.error(),
        json(r#"{"code": "transcode_chunk_out_of_order", "next_chunk": 1, "resume_at": 20.0}"#)
    );
    // Bytes that do not match their hash.
    let corrupt = harness
        .request(
            Method::PUT,
            &format!("/api/transcode/tasks/{}/parts/0/chunks/1", task.id),
            &[
                ("x-chunk-start", "20".to_owned()),
                ("x-chunk-duration", "10".to_owned()),
                ("x-chunk-frames", "300".to_owned()),
                ("x-chunk-sha256", "0".repeat(64)),
                ("content-length", "6".to_owned()),
            ],
            b"broken".to_vec(),
        )
        .await;
    assert_eq!(corrupt.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        corrupt.error().get("code"),
        Some(&Value::from("transcode_chunk_corrupt"))
    );
    let incomplete = harness.finish(&task, 0).await;
    assert_eq!(
        incomplete.error(),
        json(r#"{"code": "transcode_part_incomplete", "covered": 20.0, "duration": 30.0}"#)
    );

    assert_eq!(
        harness.put(&task, 0, 1, 20.0, 10.0, b"second").await.json(),
        json(r#"{"stored": true}"#)
    );
    assert_eq!(harness.finish(&task, 0).await.json(), json(r#"{"finished": true}"#));
    let directory = transcode_dir(&harness.work, task.id);
    assert_eq!(std::fs::read(chunk_path(&directory, 0, 1)).expect("chunk"), b"second");
    assert!(no_staging_left(&directory));
    let resumed = harness.claim().await;
    let first_part = &part_progress(resumed.get("task").expect("a task"))[0];
    assert_eq!(
        (&first_part.1, &first_part.2, &first_part.3),
        (&Value::Int(2), &Value::Float(30.0), &Value::Bool(true))
    );
    let late = harness.put(&task, 0, 2, 30.0, 1.0, b"chunk").await;
    assert_eq!(late.error().get("code"), Some(&Value::from("transcode_part_finished")));

    for (part, seconds) in [(1, 20.0), (2, 25.0)] {
        harness.put(&task, part, 0, 0.0, seconds, b"chunk").await;
        harness.finish(&task, part).await;
    }
    harness.service.tick().await.expect("tick");
    let task = harness.state(&task).await;
    let view = harness
        .service
        .transcode_view(&task)
        .await
        .expect("view")
        .expect("a view");
    assert_eq!(
        (view.get("parts_done"), view.get("encoded_seconds")),
        (Some(&Value::Int(3)), Some(&Value::Float(75.0)))
    );
    // Everything is encoded, but the audio is still being copied.
    assert_eq!(task.state, MergeState::Transcoding);
    assert_eq!(harness.claim().await, wait("assembling", Some(task.id)));

    harness.prepare(&task, "done");
    harness.service.tick().await.expect("tick");

    let task = harness.state(&task).await;
    assert_eq!(task.state, MergeState::Merging);
    let (name, args) = harness.created().last().cloned().expect("a Job");
    assert_eq!(Some(name), task.job_name.clone());
    assert_eq!(
        args[args.len() - 2..].to_vec(),
        vec!["--home-assemble".to_owned(), directory.to_string_lossy().into_owned()]
    );
    let manifest = json(&std::fs::read_to_string(directory.join("manifest.json")).expect("manifest"));
    let durations: Vec<Vec<Value>> = manifest
        .get("parts")
        .and_then(Value::as_list)
        .expect("parts")
        .iter()
        .map(|part| {
            part.get("chunks")
                .and_then(Value::as_list)
                .expect("chunks")
                .iter()
                .map(|chunk| chunk.get("duration").cloned().expect("a duration"))
                .collect()
        })
        .collect();
    assert_eq!(
        durations,
        vec![
            vec![Value::Float(20.0), Value::Float(10.0)],
            vec![Value::Float(20.0)],
            vec![Value::Float(25.0)],
        ]
    );
    assert!(
        harness
            .kube
            .state()
            .deleted
            .contains(&audio_job_name(task.id, &task.avid))
    );
    // The worker has nothing more of it.
    assert_eq!(harness.claim().await, wait("idle", None));
    let refused = harness.put(&task, 1, 1, 20.0, 1.0, b"chunk").await;
    assert_eq!(
        refused.error().get("code"),
        Some(&Value::from("transcode_task_not_active"))
    );

    std::fs::write(harness.work.join("SQTEVR-009.mp4"), vec![b'x'; 300]).expect("merged");
    let status = harness.work.join(".merge").join(format!("{}.json", task.id));
    std::fs::create_dir_all(status.parent().expect("a parent")).expect("mkdir");
    std::fs::write(
        &status,
        format!(r#"{{"state": "done", "size": 300, "sha1": "{}"}}"#, SHA1.to_lowercase()),
    )
    .expect("status");
    harness.service.tick().await.expect("tick");

    assert_eq!(harness.state(&task).await.state, MergeState::Uploading);
    assert!(!directory.exists());
    assert!(harness.transcodes.chunks(task.id).await.expect("chunks").is_empty());
    harness.close().await;
}

/// `not list(directory.rglob('.*.part'))`.
fn no_staging_left(directory: &Path) -> bool {
    fn walk(directory: &Path) -> bool {
        std::fs::read_dir(directory).expect("a directory").all(|entry| {
            let entry = entry.expect("an entry");
            let name = entry.file_name().to_string_lossy().into_owned();
            let path = entry.path();
            if path.is_dir() {
                walk(&path)
            } else {
                // `.<chunk>.part`: the staging name, exactly as the service writes it.
                !(name.starts_with('.') && path.extension().is_some_and(|extension| extension == "part"))
            }
        })
    }
    walk(directory)
}

// py: tests/test_merge_transcode.py::test_assembly_waits_for_the_merge_slot
#[tokio::test]
async fn assembly_waits_for_the_merge_slot() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    harness.claim().await;
    harness.prepare(&task, "done");
    harness.encode_all(&task).await;
    let other = harness.create_with("ABP-123", small_parts()).await;
    harness
        .repository
        .transition(
            other.id,
            MergeState::Queued,
            MergeState::Merging,
            vec![TaskField::JobName(Some("elsewhere".to_owned()))],
        )
        .await
        .expect("transition");

    harness.service.tick().await.expect("tick");

    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.notice.as_deref()),
        (MergeState::Transcoding, Some(WAITING_FOR_SLOT))
    );
    harness.close().await;
}

// py: tests/test_merge_transcode.py::test_the_worker_can_be_paused_and_is_told_so
#[tokio::test]
async fn the_worker_can_be_paused_and_is_told_so() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    harness.claim().await;

    harness.service.set_paused(task.id, true).await.expect("paused");

    let body = format!(r#"{{"task_id": {}, "state": "paused"}}"#, task.id);
    assert_eq!(
        harness.heartbeat(&body).await,
        json(r#"{"active": true, "paused": true}"#)
    );
    harness.service.set_paused(task.id, false).await.expect("resumed");
    let body = format!(r#"{{"task_id": {}}}"#, task.id);
    assert_eq!(
        harness.heartbeat(&body).await,
        json(r#"{"active": true, "paused": false}"#)
    );
    harness.close().await;
}

// py: tests/test_merge_transcode.py::test_moving_a_transcode_to_the_cluster_cpu_drops_the_workers_chunks
#[tokio::test]
async fn moving_a_transcode_to_the_cluster_cpu_drops_the_workers_chunks() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    harness.claim().await;
    harness.prepare(&task, "audio");
    harness.put(&task, 0, 0, 0.0, 30.0, b"chunk").await;

    let moved = harness.service.use_cpu(task.id).await.expect("moved");

    assert_eq!(
        (moved.state, moved.transcoder.as_deref()),
        (MergeState::Queued, Some("cpu"))
    );
    assert!(!transcode_dir(&harness.work, task.id).exists());
    assert!(harness.transcodes.chunks(task.id).await.expect("chunks").is_empty());
    let body = format!(r#"{{"task_id": {}}}"#, task.id);
    assert_eq!(
        harness.heartbeat(&body).await,
        json(r#"{"active": false, "paused": false}"#)
    );
    harness.service.tick().await.expect("tick");
    let task = harness.state(&task).await;
    assert_eq!(task.state, MergeState::Merging);
    let (_, args) = harness.created().last().cloned().expect("a Job");
    assert!(!args.iter().any(|arg| arg == "--home-assemble"));
    assert!(matches!(
        harness.service.use_cpu(task.id).await,
        Err(ActionError::Refused(_))
    ));
    harness.close().await;
}

// py: tests/test_merge_transcode.py::test_cancelling_a_transcode_removes_its_chunks_and_audio_job
#[tokio::test]
async fn cancelling_a_transcode_removes_its_chunks_and_audio_job() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    harness.claim().await;
    harness.prepare(&task, "audio");
    harness.put(&task, 0, 0, 0.0, 30.0, b"chunk").await;

    let cancelled = harness.service.cancel(task.id).await.expect("cancelled");

    assert_eq!(cancelled.state, MergeState::Cancelled);
    assert!(
        harness
            .kube
            .state()
            .deleted
            .contains(&audio_job_name(task.id, &task.avid))
    );
    assert!(!transcode_dir(&harness.work, task.id).exists());
    assert!(harness.transcodes.chunks(task.id).await.expect("chunks").is_empty());
    harness.close().await;
}

// py: tests/test_merge_transcode.py::test_a_failed_audio_job_fails_the_task_and_a_retry_keeps_the_chunks
#[tokio::test]
async fn a_failed_audio_job_fails_the_task_and_a_retry_keeps_the_chunks() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    harness.claim().await;
    harness.prepare(&task, "audio");
    harness.put(&task, 0, 0, 0.0, 30.0, b"chunk").await;
    harness
        .kube
        .set_condition(&audio_job_name(task.id, &task.avid), "Failed", None);

    harness.service.tick().await.expect("tick");

    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.failed_state),
        (MergeState::Failed, Some(MergeState::Transcoding))
    );
    assert_eq!(task.error.as_deref(), Some("the audio Job failed: ffmpeg: boom"));
    assert!(!harness.service.parked(&task));

    let retried = harness.service.retry(task.id).await.expect("retried");

    assert_eq!(retried.state, MergeState::Transcoding);
    assert_eq!(harness.transcodes.chunks(task.id).await.expect("chunks").len(), 1);
    harness.service.tick().await.expect("tick");
    assert_eq!(
        harness.created().last().map(|(name, _)| name.clone()),
        Some(audio_job_name(task.id, &task.avid))
    );
    harness.close().await;
}

// py: tests/test_merge_transcode.py::test_the_worker_reads_parts_with_range_requests
#[tokio::test]
#[allow(clippy::too_many_lines, reason = "one part read every way ffmpeg reads it")]
async fn the_worker_reads_parts_with_range_requests() {
    let harness = Harness::new().await;
    let mut task_parts = parts();
    task_parts[0] = harness.part_file.to_string_lossy().into_owned();
    let task = harness.create_with("SQTEVR-009", task_parts).await;
    harness
        .repository
        .transition(
            task.id,
            MergeState::Queued,
            MergeState::Transcoding,
            vec![TaskField::Transcoder(Some("home".to_owned()))],
        )
        .await
        .expect("transition");

    let response = harness
        .request(
            Method::GET,
            &format!("/api/transcode/tasks/{}/parts/0", task.id),
            &[("range", "bytes=10-19".to_owned())],
            Vec::new(),
        )
        .await;

    assert_eq!(response.status, StatusCode::PARTIAL_CONTENT);
    let whole = std::fs::read(&harness.part_file).expect("the part");
    assert_eq!(response.bytes.as_ref(), &whole[10..20]);
    assert_eq!(
        response
            .headers
            .get("content-range")
            .map(|value| value.to_str().expect("text")),
        Some("bytes 10-19/16384")
    );
    assert_eq!(
        response
            .headers
            .get("content-type")
            .map(|value| value.to_str().expect("text")),
        Some("application/octet-stream")
    );
    assert_eq!(
        response
            .headers
            .get("content-length")
            .map(|value| value.to_str().expect("text")),
        Some("10")
    );
    // Past the end: Starlette's 416 with the size; two ranges: one multipart answer.
    let past = harness
        .request(
            Method::GET,
            &format!("/api/transcode/tasks/{}/parts/0", task.id),
            &[("range", "bytes=20000-".to_owned())],
            Vec::new(),
        )
        .await;
    assert_eq!(past.status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        past.headers
            .get("content-range")
            .map(|value| value.to_str().expect("text")),
        Some("bytes */16384")
    );
    let two = harness
        .request(
            Method::GET,
            &format!("/api/transcode/tasks/{}/parts/0", task.id),
            &[("range", "bytes=0-1, 100-101".to_owned())],
            Vec::new(),
        )
        .await;
    assert_eq!(two.status, StatusCode::PARTIAL_CONTENT);
    let content_type = two
        .headers
        .get("content-type")
        .map(|value| value.to_str().expect("text").to_owned())
        .unwrap_or_default();
    assert!(
        content_type.starts_with("multipart/byteranges; boundary="),
        "{content_type}"
    );
    assert_eq!(
        two.headers.get("content-length").map(|value| value
            .to_str()
            .expect("text")
            .parse::<usize>()
            .expect("a length")),
        Some(two.bytes.len())
    );
    let missing = harness
        .request(
            Method::GET,
            &format!("/api/transcode/tasks/{}/parts/7", task.id),
            &[],
            Vec::new(),
        )
        .await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    // The whole part without a range, streamed, with Starlette's headers.
    let whole_answer = harness
        .request(
            Method::GET,
            &format!("/api/transcode/tasks/{}/parts/0", task.id),
            &[],
            Vec::new(),
        )
        .await;
    assert_eq!(whole_answer.status, StatusCode::OK);
    assert_eq!(whole_answer.bytes.as_ref(), whole.as_slice());
    assert_eq!(
        whole_answer
            .headers
            .get("accept-ranges")
            .map(|value| value.to_str().expect("text")),
        Some("bytes")
    );
    // A part that is not on the mount is Starlette's RuntimeError: a plain 500.
    let gone = harness
        .request(
            Method::GET,
            &format!("/api/transcode/tasks/{}/parts/1", task.id),
            &[],
            Vec::new(),
        )
        .await;
    assert_eq!(gone.status, StatusCode::INTERNAL_SERVER_ERROR);
    harness.close().await;
}

// py: tests/test_merge_transcode.py::test_the_worker_api_needs_its_token
#[tokio::test]
async fn the_worker_api_needs_its_token() {
    let harness = Harness::new().await;
    let (parts, bytes) = send(
        &harness.app,
        http::Request::builder()
            .method(Method::POST)
            .uri("/api/transcode/claim")
            .header("authorization", "Bearer nope")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .expect("a request"),
    )
    .await;

    assert_eq!(parts.status, StatusCode::UNAUTHORIZED);
    assert_eq!(bytes.as_ref(), br#"{"error":{"code":"unauthorized"}}"#);
    // The transcode app's handler sends no WWW-Authenticate (the main app's does).
    assert!(parts.headers.get("www-authenticate").is_none());
    harness.close().await;
}

// py: tests/test_merge_transcode.py::test_the_worker_shows_as_online_after_it_reports
#[tokio::test]
async fn the_worker_shows_as_online_after_it_reports() {
    let harness = Harness::new().await;
    assert!(!harness.service.presence().online());

    let mut report = WorkerReport::at(Utc::now());
    report.name = "pc".to_owned();
    harness.service.heartbeat(report).await.expect("heartbeat");

    assert!(harness.service.presence().online());
    harness.close().await;
}

/// FastAPI's order on the worker's routes: a JSON body that does not parse is refused before
/// the token, the token before validation; the report's fields are pydantic's.
#[tokio::test]
async fn reports_are_validated_after_the_token_as_fastapi_orders_it() {
    let harness = Harness::new().await;
    let call = |authorization: &'static str, body: &'static str| {
        http::Request::builder()
            .method(Method::POST)
            .uri("/api/transcode/heartbeat")
            .header("authorization", authorization)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .expect("a request")
    };
    let status = |request| async { send(&harness.app, request).await.0.status };
    assert_eq!(status(call("Bearer nope", "{")).await, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(status(call("Bearer nope", "[]")).await, StatusCode::UNAUTHORIZED);
    assert_eq!(
        status(call("Bearer secret", "[]")).await,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        status(call("Bearer secret", "")).await,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let long = Box::leak(format!(r#"{{"name": "{}"}}"#, "x".repeat(129)).into_boxed_str());
    assert_eq!(
        status(call("Bearer secret", long)).await,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let parts = Box::leak(format!(r#"{{"parts": [{}]}}"#, vec![r#"{"index": 0}"#; 17].join(",")).into_boxed_str());
    assert_eq!(
        status(call("Bearer secret", parts)).await,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        status(call("Bearer secret", r#"{"parts": [{"fps": 1}]}"#)).await,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        status(call(
            "Bearer secret",
            r#"{"task_id": null, "extra": 1, "parts": [{"index": "2", "fps": "29.97"}], "upload_bps": true}"#
        ))
        .await,
        StatusCode::OK
    );
    let (report, online) = harness.service.presence().view().expect("a report");
    assert!(online);
    assert_eq!(report.parts[0].index, 2);
    assert!((report.parts[0].fps - 29.97).abs() < 1e-9);
    assert!((report.upload_bps - 1.0).abs() < f64::EPSILON);
    harness.close().await;
}

/// The chunk route: the token, then the path and headers (422), then the task and part, then a
/// negative index, then the body.
/// A refusal sent before the body is read leaves the connection open, as uvicorn does: the
/// rest of the body is read and dropped, so a worker streaming a large chunk to a task that
/// is gone reads the refusal instead of a reset, and the next request uses the same
/// connection. Over a real socket: a wrong token, an unknown task, then one with
/// `Connection: close`, all on one connection.
#[tokio::test]
async fn a_refused_upload_reads_the_rest_of_the_body_and_keeps_the_connection() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let harness = Harness::new().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("an address");
    let routes = crate::serve::router(harness.app.clone(), address)
        .into_make_service_with_connect_info::<std::net::SocketAddr>();
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });

    let body = vec![7_u8; 8 * 1024 * 1024];
    let digest = sha256(&body);
    let upload = |token: &str, close: bool| {
        let mut head = format!(
            "PUT /api/transcode/tasks/999/parts/0/chunks/0 HTTP/1.1\r\nhost: worker\r\n\
             authorization: Bearer {token}\r\nx-chunk-start: 0.0\r\nx-chunk-duration: 2.0\r\n\
             x-chunk-frames: 60\r\nx-chunk-sha256: {digest}\r\ncontent-length: {}\r\n",
            body.len()
        )
        .into_bytes();
        if close {
            head.extend_from_slice(b"connection: close\r\n");
        }
        head.extend_from_slice(b"\r\n");
        head.extend_from_slice(&body);
        head
    };
    let mut requests = upload("wrong", false);
    requests.extend(upload("secret", false));
    requests.extend(upload("secret", true));

    let stream = tokio::net::TcpStream::connect(address).await.expect("connect");
    let (mut reader, mut writer) = stream.into_split();
    // The write half is handed back, not dropped: a FIN would read as the client gone to
    // hyper, which does not keep half-closed connections; a worker never half-closes.
    let sending = tokio::spawn(async move { writer.write_all(&requests).await.map(|()| writer) });
    let mut answers = Vec::new();
    tokio::time::timeout(std::time::Duration::from_mins(1), reader.read_to_end(&mut answers))
        .await
        .expect("the server closes after the third request")
        .expect("read");
    let _writer = sending.await.expect("the writer").expect("every byte is taken");
    let answers = String::from_utf8_lossy(&answers);
    // Each answer's status, in order (a JSON body runs into the next status line).
    let statuses: Vec<&str> = answers
        .split("HTTP/1.1 ")
        .skip(1)
        .map(|answer| answer.get(..3).unwrap_or(answer))
        .collect();
    assert_eq!(statuses, ["401", "404", "404"], "{answers}");
    assert_eq!(
        answers
            .matches(r#"{"error":{"code":"transcode_task_not_found"}}"#)
            .count(),
        2,
        "{answers}"
    );

    server.abort();
    harness.close().await;
}

#[tokio::test]
async fn chunk_uploads_are_checked_in_fastapis_order() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    harness.claim().await;
    harness.prepare(&task, "audio");
    let digest = sha256(b"chunk");
    // Not a number, a duration of 0, frames of 0, a short hash: 422 before the task is read.
    for (name, value) in [
        ("x-chunk-start", "soon"),
        ("x-chunk-duration", "0"),
        ("x-chunk-duration", "nan"),
        ("x-chunk-frames", "0"),
        ("x-chunk-sha256", "abc"),
    ] {
        let mut headers = vec![
            ("x-chunk-start", "0".to_owned()),
            ("x-chunk-duration", "1".to_owned()),
            ("x-chunk-frames", "30".to_owned()),
            ("x-chunk-sha256", digest.clone()),
            ("content-length", "5".to_owned()),
        ];
        for header in &mut headers {
            if header.0 == name {
                header.1 = value.to_owned();
            }
        }
        let answer = harness
            .request(
                Method::PUT,
                "/api/transcode/tasks/999/parts/0/chunks/0",
                &headers,
                b"chunk".to_vec(),
            )
            .await;
        assert_eq!(answer.status, StatusCode::UNPROCESSABLE_ENTITY, "{name}: {value}");
    }
    let unknown = harness.put_raw(999, 0, 0, 0.0, 1.0, b"chunk", &digest).await;
    assert_eq!(
        (unknown.status, unknown.error()),
        (StatusCode::NOT_FOUND, json(r#"{"code": "transcode_task_not_found"}"#))
    );
    let part = harness.put_raw(task.id, 3, 0, 0.0, 1.0, b"chunk", &digest).await;
    assert_eq!(
        (part.status, part.error()),
        (StatusCode::NOT_FOUND, json(r#"{"code": "transcode_part_not_found"}"#))
    );
    let negative = harness.put_raw(task.id, 0, -1, 0.0, 1.0, b"chunk", &digest).await;
    assert_eq!(
        (negative.status, negative.error()),
        (
            StatusCode::CONFLICT,
            json(r#"{"code": "transcode_chunk_out_of_order"}"#)
        )
    );
    // A chunk number past PostgreSQL's INTEGER is ahead of the part, as Python finds.
    let far = harness
        .put_raw(task.id, 0, 3_000_000_000, 0.0, 1.0, b"chunk", &digest)
        .await;
    assert_eq!(
        far.error(),
        json(r#"{"code": "transcode_chunk_out_of_order", "next_chunk": 0, "resume_at": 0.0}"#)
    );
    // An upper-case hash of the right bytes is the same hash.
    let upper = harness
        .put_raw(task.id, 0, 0, 0.0, 1.0, b"chunk", &digest.to_uppercase())
        .await;
    assert_eq!(upper.json(), json(r#"{"stored": true}"#));
    harness.close().await;
}

#[path = "tests/protocol.rs"]
mod protocol;
