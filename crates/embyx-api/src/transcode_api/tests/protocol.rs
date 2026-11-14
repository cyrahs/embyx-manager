//! Cross-version contract 5: a home worker of protocol v1 written in Python works against the
//! Rust listener. The listener is served on a local port over a scratch database, a fake Job
//! API and a fake CloudDrive; the baseline's own `home_worker` (`tools/home_worker/python_worker.py`)
//! claims the task, reads the parts with ffmpeg over HTTP (range requests), uploads chunks
//! with their headers, sends heartbeats, is stopped midway and resumes, and reads a refusal
//! as `ServerError`. The Rust merge worker then prepares the audio and assembles what the
//! Python worker sent, with the arguments of the Jobs the service asked for.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command as Process;
use std::sync::Arc;

use embyx_core::config::models::{ArchiveConfig, CloudDriveConfig, MergeConfig};
use embyx_core::pyjson::{self, Dict, DumpOptions, Value};
use embyx_db::testdb::TestDatabase;
use embyx_merge::fakes::{FakeCloud, FakeKube};
use embyx_merge::ports::{MergeCloud, MergeKube, Trigger};
use embyx_merge::service::{MergeService, ServiceParts};
use embyx_merge::tasks::{MergeState, MergeTaskRepository, NewTask};
use embyx_merge::transcode::{TranscodeRepository, audio_job_name};
use embyx_merge::worker::{self, Command, Environment, Settings, StatusFile, Worker};

use super::super::create_transcode_router;
use crate::app::create_transcode_app;

const BIG: i64 = 200 * 1_000_000_000;
const TOKEN: &str = "protocol-secret";

fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn ffmpeg(arguments: &[&str]) {
    let status = Process::new("ffmpeg")
        .args(["-hide_banner", "-loglevel", "error", "-y"])
        .args(arguments)
        .status()
        .expect("ffmpeg runs (it must be installed)");
    assert!(status.success(), "ffmpeg {arguments:?}");
}

/// `tests/test_home_worker.py::make_part`.
fn make_part(path: &Path, seconds: f64, audio_extra: f64) {
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("mkdir");
    let video = format!("testsrc=size=160x120:rate=30000/1001:duration={seconds}");
    let audio = format!("sine=frequency=440:duration={}", seconds + audio_extra);
    let path = path.to_string_lossy();
    ffmpeg(&[
        "-f", "lavfi", "-i", &video, "-f", "lavfi", "-i", &audio, "-c:v", "libx264", "-g", "30", "-pix_fmt", "yuv420p",
        "-c:a", "aac", &path,
    ]);
}

fn packets(path: &Path, entries: &str, stream: &str) -> Vec<String> {
    let output = Process::new("ffprobe")
        .args([
            "-v",
            "error",
            "-select_streams",
            stream,
            "-count_packets",
            "-show_entries",
            entries,
        ])
        .args(["-of", "csv=p=0"])
        .arg(path)
        .output()
        .expect("ffprobe runs");
    assert!(output.status.success(), "ffprobe {path:?}");
    String::from_utf8(output.stdout)
        .expect("utf-8")
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

fn frames(path: &Path) -> i64 {
    packets(path, "stream=nb_read_packets", "v:0")[0]
        .parse()
        .expect("a count")
}

/// One run of the Python worker; its printed JSON.
async fn python_worker(run: &Value) -> Value {
    let argument = pyjson::dumps(run, &DumpOptions::plain()).expect("JSON");
    let output = tokio::task::spawn_blocking(move || {
        let repository = repository();
        Process::new("uv")
            .args(["run", "--locked", "python"])
            .arg(repository.join("tools/home_worker/python_worker.py"))
            .arg(argument)
            .current_dir(&repository)
            .output()
            .expect("uv runs (it must be installed)")
    })
    .await
    .expect("the worker thread");
    let stdout = String::from_utf8(output.stdout).expect("utf-8");
    assert!(
        output.status.success(),
        "the Python worker failed: {}{stdout}",
        String::from_utf8_lossy(&output.stderr)
    );
    pyjson::loads(stdout.trim_end()).expect("the worker prints JSON")
}

/// The value after `flag` in a Job's arguments.
fn after(args: &[String], flag: &str) -> String {
    let at = args
        .iter()
        .position(|arg| arg == flag)
        .unwrap_or_else(|| panic!("no {flag} in {args:?}"));
    args[at + 1].clone()
}

/// The Rust merge worker run with a Job's arguments, as the Job's container would run it.
fn run_job(args: &[String], parts: Vec<String>, tmp: &Path) {
    let command = Command {
        parts,
        output: after(args, "--output"),
        status: after(args, "--status"),
        reserve_gib: 0.0,
        home_prepare: args
            .iter()
            .any(|arg| arg == "--home-prepare")
            .then(|| after(args, "--home-prepare")),
        home_assemble: args
            .iter()
            .any(|arg| arg == "--home-assemble")
            .then(|| after(args, "--home-assemble")),
    };
    let mut status = StatusFile::new(worker::py_path(&command.status));
    let environment = Environment {
        tmpdir: tmp.to_string_lossy().into_owned(),
        ..Environment::from_process()
    };
    let mut stderr = Vec::new();
    let code = worker::run(
        &Worker::new(Settings::default()),
        &command,
        &mut status,
        &environment,
        &mut stderr,
    );
    assert_eq!(code, 0, "the merge worker: {}", String::from_utf8_lossy(&stderr));
}

fn field<'a>(value: &'a Value, key: &str) -> &'a Value {
    value.get(key).unwrap_or_else(|| panic!("no {key} in {value:?}"))
}

fn dict(entries: Vec<(&str, Value)>) -> Value {
    let mut dict = Dict::new();
    for (key, value) in entries {
        dict.insert(key, value);
    }
    Value::Dict(dict)
}

#[tokio::test]
#[allow(clippy::too_many_lines, reason = "one worker's whole task, step for step")]
async fn a_python_worker_transcodes_a_title_through_the_rust_listener() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mount = tmp.path().join("cd2");
    let library = format!("{}/115/embyx", mount.display());
    let parts: Vec<String> = (1..=2)
        .map(|index| format!("{library}/type/vr/SQTEVR/SQTEVR-009-cd{index}.mp4"))
        .collect();
    make_part(Path::new(&parts[0]), 14.0, 0.2);
    make_part(Path::new(&parts[1]), 5.0, 0.0);
    let work = tmp.path().join("upload");
    std::fs::create_dir_all(&work).expect("mkdir");
    let scratch = tmp.path().join("tmp");
    std::fs::create_dir_all(&scratch).expect("mkdir");

    let test = TestDatabase::create().await;
    let repository = MergeTaskRepository::new(test.database());
    let kube = FakeKube::default();
    let cloud = FakeCloud::new(&["/", "/115"]);
    let merge = MergeConfig {
        work_dir: work.to_string_lossy().into_owned(),
        cloud_mount_prefix: mount.to_string_lossy().into_owned(),
        home_transcode_enabled: true,
        ..MergeConfig::default()
    };
    let archive = ArchiveConfig {
        src_dir: format!("{}/115/embyx_in", mount.display()),
        dst_dir: library.clone(),
        priority_mapping: vec![("vr".to_owned(), "type/vr".to_owned())],
        ..ArchiveConfig::default()
    };
    let trigger: Trigger = Arc::new(|_| Box::pin(async { Ok(()) }));
    let mut service_parts = ServiceParts::new(
        repository.clone(),
        Arc::new(move || Some(Arc::new(cloud.clone()) as Arc<dyn MergeCloud>)),
        Some(Arc::new(kube.clone()) as Arc<dyn MergeKube>),
        Arc::new(move || merge.clone()),
        Arc::new(move || archive.clone()),
        Arc::new(|| CloudDriveConfig {
            address: "cd:19798".to_owned(),
            api_token: "t".to_owned(),
            ..CloudDriveConfig::default()
        }),
        trigger,
    );
    service_parts.transcodes = Some(TranscodeRepository::new(test.database()));
    // Over 115 GB as far as the service can tell, so the title goes to the home worker.
    service_parts.sizes = Arc::new(|_: &[String]| Ok(BIG));
    let service = MergeService::new(service_parts);
    let task = repository
        .create(&NewTask {
            avid: "SQTEVR-009".to_owned(),
            source: "vr".to_owned(),
            library_dir: "type/vr".to_owned(),
            brand: "SQTEVR".to_owned(),
            parts: parts.clone(),
            merged_name: "SQTEVR-009.mp4".to_owned(),
        })
        .await
        .expect("create");
    service.tick().await.expect("tick");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("an address");
    let app = create_transcode_app(create_transcode_router(service.clone(), Some(TOKEN)));
    let routes = crate::serve::router(app, address).into_make_service_with_connect_info::<SocketAddr>();
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });

    let encoder = pyjson::loads(
        r#"{"codec": "libx264", "extra": ["-preset", "veryfast", "-bf", "3", "-vf", "realtime=speed=4"]}"#,
    )
    .expect("JSON");
    let run = |stop_after: Value, refuse: bool| {
        dict(vec![
            ("server", Value::Str(format!("http://{address}"))),
            ("token", Value::from(TOKEN)),
            (
                "cache_dir",
                Value::Str(tmp.path().join("cache").to_string_lossy().into_owned()),
            ),
            ("encoder", encoder.clone()),
            ("chunk_seconds", Value::Int(2)),
            ("stop_after", stop_after),
            ("refuse", Value::Bool(refuse)),
            // The worker's 15 s, shortened so these short runs send heartbeats.
            ("heartbeat_seconds", Value::Float(0.5)),
        ])
    };

    // The first claim starts the audio Job; the worker is told to wait while it probes.
    let first = python_worker(&run(Value::Null, false)).await;
    assert_eq!(
        field(&first, "claim"),
        &pyjson::loads(&format!(
            r#"{{"task": null, "wait": "probing", "task_id": {}}}"#,
            task.id
        ))
        .expect("JSON")
    );
    let (name, args) = kube.state().created[0].clone();
    assert_eq!(name, audio_job_name(task.id, &task.avid));
    run_job(&args, parts.clone(), &scratch);

    // Stopped once the cluster took the first chunk, then a chunk out of order is refused.
    let stopped = python_worker(&run(Value::Int(1), true)).await;
    assert_eq!(field(&stopped, "outcome"), &Value::from("stopped"));
    let assigned = field(field(&stopped, "claim"), "task");
    assert_eq!(field(assigned, "id"), &Value::Int(i128::from(task.id)));
    assert_eq!(field(assigned, "paused"), &Value::Bool(false));
    let refusal = field(&stopped, "refusal");
    assert_eq!(field(refusal, "status"), &Value::Int(409));
    assert_eq!(field(refusal, "code"), &Value::from("transcode_chunk_out_of_order"));
    let details = field(refusal, "details");
    let Value::Int(next) = field(details, "next_chunk") else {
        panic!("next_chunk in {details:?}");
    };
    assert!(*next >= 1, "the first chunk was taken: {details:?}");
    assert!(matches!(field(details, "resume_at"), Value::Float(at) if *at > 0.0));
    let held = TranscodeRepository::new(test.database())
        .chunks(task.id)
        .await
        .expect("chunks");
    assert!(
        !held.is_empty() && held.iter().all(|chunk| chunk.part_index == 0),
        "{held:?}"
    );

    // The second run resumes where the cluster's chunks end and sends the rest.
    let done = python_worker(&run(Value::Null, false)).await;
    assert_eq!(field(&done, "outcome"), &Value::from("done"));
    let resumed = field(field(&done, "claim"), "task");
    let Some(Value::List(views)) = resumed.get("parts") else {
        panic!("parts in {resumed:?}");
    };
    assert_eq!(views[0].get("next_chunk"), Some(&Value::Int(*next)));
    // Heartbeats went through while it encoded, each answered active and not paused.
    assert_eq!(field(&done, "heartbeat_failures"), &Value::List(Vec::new()));
    let Value::List(heartbeats) = field(&done, "heartbeats") else {
        panic!("heartbeats in {done:?}");
    };
    assert!(!heartbeats.is_empty(), "no heartbeat was sent: {done:?}");
    for answer in heartbeats {
        assert_eq!(answer.get("active"), Some(&Value::Bool(true)), "{answer:?}");
        assert_eq!(answer.get("paused"), Some(&Value::Bool(false)), "{answer:?}");
    }

    // The service assembles once every part is in, with the Rust merge worker.
    service.tick().await.expect("tick");
    let task = repository.get(task.id).await.expect("get").expect("the task");
    assert_eq!(task.state, MergeState::Merging);
    let (_, args) = kube.state().created.last().cloned().expect("the merge Job");
    assert!(args.iter().any(|arg| arg == "--home-assemble"), "{args:?}");
    run_job(&args, Vec::new(), &scratch);
    let output = PathBuf::from(after(&args, "--output"));
    let sources: i64 = parts.iter().map(|part| frames(Path::new(part))).sum();
    assert_eq!(frames(&output), sources);

    server.abort();
    repository.database().close().await;
    test.drop_database().await;
}
