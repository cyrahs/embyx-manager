//! The Rust worker against the Python listener (cross-version contract 5, home worker
//! protocol version 1): the baseline's `MergeService` and transcode endpoints, served by
//! `tools/home_worker/python_listener.py` on a PostgreSQL database of their own, assign a
//! title; this worker encodes it with real ffmpeg reading the parts over HTTP ranges, is
//! stopped after its first chunk and resumes, and the merge worker assembles what the Python
//! cluster kept, frame for frame.
//!
//! Needs `uv`, ffmpeg and `EMBYX_MANAGER_TEST_DATABASE_URL`, as the merge crate's Python
//! cross-checks do; Unix only, as the cluster is.
#![cfg(unix)]

use std::io::{BufRead as _, BufReader, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command as Process, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use embyx_core::pyjson::{self, Dict, DumpOptions, Value};
use embyx_merge::transcode::audio_status_path;
use embyx_merge::worker::{self, Command, Environment, StatusFile, Worker as MergeWorker};
use embyx_transcode_worker::WorkerConfig;
use embyx_transcode_worker::chunks::{LocalChunk, RateLimiter, RateMeter};
use embyx_transcode_worker::console::MemoryLog;
use embyx_transcode_worker::process::SystemRunner;
use embyx_transcode_worker::run::{Worker, idle_report};
use embyx_transcode_worker::server::{CallError, Cluster, HttpServer};
use embyx_transcode_worker::task::Timing;

const TOKEN: &str = "listener-token";

fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The Python listener and its command channel.
struct Listener {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    port: u16,
}

impl Listener {
    fn start(work: &Path, mount: &Path) -> Self {
        let settings = format!(
            r#"{{"work": {}, "mount": {}, "token": "{TOKEN}"}}"#,
            quoted(&work.to_string_lossy()),
            quoted(&mount.to_string_lossy())
        );
        let mut child = Process::new("uv")
            .args(["run", "--locked", "python"])
            .arg(repository().join("tools/home_worker/python_listener.py"))
            .arg(settings)
            .current_dir(repository())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("uv runs (it must be installed)");
        let input = child.stdin.take().expect("stdin");
        let mut output = BufReader::new(child.stdout.take().expect("stdout"));
        let first = read_line(&mut output);
        let port = match first.get("port") {
            Some(Value::Int(port)) => u16::try_from(*port).expect("a port"),
            other => panic!("the listener did not start: {other:?}"),
        };
        Self {
            child,
            input,
            output,
            port,
        }
    }

    fn ask(&mut self, command: &str) -> Value {
        writeln!(self.input, "{command}").expect("send");
        self.input.flush().expect("flush");
        read_line(&mut self.output)
    }

    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        let _ = writeln!(self.input, r#"{{"op": "quit"}}"#);
        let _ = self.input.flush();
        let _ = self.child.wait();
    }
}

fn read_line(output: &mut BufReader<ChildStdout>) -> Value {
    let mut line = String::new();
    output.read_line(&mut line).expect("read");
    assert!(!line.is_empty(), "the listener ended");
    pyjson::loads(line.trim_end()).expect("the listener answers JSON")
}

fn quoted(text: &str) -> String {
    pyjson::dumps(&Value::Str(text.to_owned()), &DumpOptions::python_default()).expect("JSON")
}

fn make_part(path: &Path, seconds: f64, audio_extra: f64) -> PathBuf {
    let status = Process::new("ffmpeg")
        .args(["-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i"])
        .arg(format!("testsrc=size=160x120:rate=30000/1001:duration={seconds}"))
        .args(["-f", "lavfi", "-i"])
        .arg(format!("sine=frequency=440:duration={}", seconds + audio_extra))
        .args(["-c:v", "libx264", "-g", "30", "-pix_fmt", "yuv420p", "-c:a", "aac"])
        .arg(path)
        .status()
        .expect("ffmpeg runs");
    assert!(status.success());
    path.to_owned()
}

fn probe(path: &Path, entries: &str, stream: &str) -> Vec<String> {
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
    assert!(output.status.success());
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

fn frames(path: &Path) -> i64 {
    probe(path, "stream=nb_read_packets", "v:0")[0]
        .parse()
        .expect("a count")
}

/// The merge worker's command from a Job's arguments (those after `merge-worker`), as
/// `__main__.py` parses them.
fn job_command(args: &[Value]) -> Command {
    let args: Vec<String> = args.iter().map(|arg| arg.as_str().expect("text").to_owned()).collect();
    let mut command = Command {
        parts: Vec::new(),
        output: String::new(),
        status: String::new(),
        reserve_gib: 0.0,
        home_prepare: None,
        home_assemble: None,
    };
    let mut items = args.iter();
    while let Some(arg) = items.next() {
        let mut value = || items.next().expect("an option's value").clone();
        match arg.as_str() {
            "--output" => command.output = value(),
            "--status" => command.status = value(),
            // The free-space reserve is the cluster's; a test directory has less to spare.
            "--reserve-gib" => {
                value();
            }
            "--home-prepare" => command.home_prepare = Some(value()),
            "--home-assemble" => command.home_assemble = Some(value()),
            "--" => command.parts.extend(items.by_ref().cloned()),
            other => command.parts.push(other.to_owned()),
        }
    }
    command
}

/// What the cluster's Job runs: `merge-worker` with those arguments.
fn run_job(args: &[Value]) {
    let command = job_command(args);
    let mut status = StatusFile::new(worker::py_path(&command.status));
    let mut stderr = Vec::new();
    let code = worker::run(
        &MergeWorker::default(),
        &command,
        &mut status,
        &Environment::from_process(),
        &mut stderr,
    );
    assert_eq!(code, 0, "{}", String::from_utf8_lossy(&stderr));
}

/// The HTTP client, counting the chunks the cluster took.
struct Counted {
    server: HttpServer,
    stored: AtomicUsize,
}

impl Cluster for Counted {
    fn part_url(&self, task_id: i128, part_index: i128) -> String {
        self.server.part_url(task_id, part_index)
    }

    fn claim(&self, report: &Value) -> Result<Dict, CallError> {
        self.server.claim(report)
    }

    fn heartbeat(&self, report: &Value) -> Result<Dict, CallError> {
        self.server.heartbeat(report)
    }

    fn upload(
        &self,
        task_id: i128,
        chunk: &LocalChunk,
        limiter: &RateLimiter,
        meter: &RateMeter,
    ) -> Result<Dict, CallError> {
        let answer = self.server.upload(task_id, chunk, limiter, meter);
        if answer.is_ok() {
            self.stored.fetch_add(1, Ordering::SeqCst);
        }
        answer
    }

    fn finish(&self, task_id: i128, part_index: i128) -> Result<Dict, CallError> {
        self.server.finish(task_id, part_index)
    }
}

fn worker(
    config: &WorkerConfig,
    cluster: Arc<Counted>,
    log: Arc<MemoryLog>,
    stop: impl Fn() -> usize + Send + Sync + 'static,
) -> Worker {
    Worker {
        config: config.clone(),
        cluster,
        runner: Arc::new(SystemRunner),
        log,
        timing: Timing::default(),
        stop: Arc::new(stop),
        sleep: Some(Arc::new(|_| std::thread::sleep(Duration::from_millis(50)))),
    }
}

fn get<'a>(value: &'a Value, key: &str) -> &'a Value {
    value.get(key).unwrap_or_else(|| panic!("no {key} in {value:?}"))
}

#[test]
#[allow(clippy::too_many_lines, reason = "one end-to-end story, told step by step")]
fn the_rust_worker_transcodes_for_the_python_listener() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let tmp = tmp.path();
    let mount = tmp.join("cd2");
    let library = mount.join("library");
    let work = tmp.join("upload");
    std::fs::create_dir_all(&library).expect("library");
    std::fs::create_dir_all(&work).expect("work");
    let parts = [
        // Over two of the cluster's 60-second chunks, so the stop falls inside the part.
        make_part(&library.join("ABC-001-cd1.mp4"), 130.0, 0.2),
        make_part(&library.join("ABC-001-cd2.mp4"), 5.0, 0.0),
    ];
    let mut listener = Listener::start(&work, &mount);
    let names: Vec<String> = parts.iter().map(|part| quoted(&part.to_string_lossy())).collect();
    let created = listener.ask(&format!(
        r#"{{"op": "create", "avid": "ABC-001", "parts": [{}]}}"#,
        names.join(", ")
    ));
    let Value::Int(id) = *get(&created, "id") else {
        panic!("{created:?}")
    };
    let directory = PathBuf::from(get(&created, "directory").as_str().expect("a path"));

    let mut config = WorkerConfig::new(listener.base());
    config.token = TOKEN.to_owned();
    config.cache_dir = tmp.join("cache");
    config.jobs = 1;
    config.name = "pc".to_owned();
    config.encoder = pyjson::loads(
        r#"{"codec": "libx264", "extra": ["-preset", "veryfast", "-bf", "3", "-vf", "realtime=speed=20"]}"#,
    )
    .expect("JSON")
    .as_dict()
    .expect("object")
    .clone();
    let server = HttpServer::new(&config);

    // The first claim starts the title: the cluster probes the parts and copies their audio.
    let mut wrong = config.clone();
    wrong.token = "nope".to_owned();
    let Err(CallError::Server(refused)) = HttpServer::new(&wrong).claim(&idle_report(&config)) else {
        panic!("a wrong token is refused");
    };
    assert_eq!((refused.status, refused.code.as_str()), (401, "unauthorized"));
    let answer = server.claim(&idle_report(&config)).expect("claim");
    assert_eq!(
        answer.get("wait").and_then(Value::as_str),
        Some("probing"),
        "{answer:?}"
    );
    let state = listener.ask(&format!(r#"{{"op": "state", "id": {id}}}"#));
    assert_eq!(get(&state, "state").as_str(), Some("transcoding"));
    assert_eq!(get(&state, "transcoder").as_str(), Some("home"));
    let jobs = get(&state, "jobs").as_list().expect("jobs").to_vec();
    let prepare = jobs.last().and_then(Value::as_list).expect("the audio Job")[1]
        .as_list()
        .expect("args")
        .to_vec();
    assert!(
        prepare.iter().any(|arg| arg.as_str() == Some("--home-prepare")),
        "{prepare:?}"
    );
    run_job(&prepare);

    // Ctrl+C once the cluster holds a chunk: the run finishes the chunk under way and stops.
    let first = Arc::new(Counted {
        server: HttpServer::new(&config),
        stored: AtomicUsize::new(0),
    });
    let held = Arc::clone(&first);
    let log = Arc::new(MemoryLog::default());
    worker(&config, first, log.clone(), move || {
        usize::from(held.stored.load(Ordering::SeqCst) >= 1)
    })
    .serve()
    .expect("the first run");
    let events = log.events();
    assert!(
        events
            .iter()
            .any(|line| line.starts_with("开始 ABC-001：2 盘，还剩 2 盘")),
        "{events:?}"
    );
    let kept = listener.ask(&format!(r#"{{"op": "chunks", "id": {id}}}"#));
    let kept = get(&kept, "chunks").as_list().expect("chunks").to_vec();
    assert!((1..3).contains(&kept.len()), "{kept:?} after the stop");
    assert!(
        kept.iter()
            .all(|chunk| chunk.as_list().expect("fields")[0] == Value::Int(0)),
        "{kept:?}"
    );

    // Started again, it carries on from the last chunk the cluster has.
    let second = Arc::new(Counted {
        server: HttpServer::new(&config),
        stored: AtomicUsize::new(0),
    });
    let log = Arc::new(MemoryLog::default());
    let watched = Arc::clone(&log);
    let seen = Arc::new(Mutex::new(false));
    worker(&config, second, log.clone(), move || {
        let done = watched
            .events()
            .iter()
            .any(|line| line.ends_with("全部分盘已传到集群，接下来由集群拼接、上传 115"));
        *seen.lock().expect("seen") |= done;
        usize::from(done)
    })
    .serve()
    .expect("the second run");
    let events = log.events();
    assert!(
        events
            .iter()
            .any(|line| line.starts_with("开始 ABC-001：2 盘，还剩 2 盘")),
        "{events:?}"
    );
    assert!(!events.iter().any(|line| line.contains("被拒")), "{events:?}");
    let answer = server.claim(&idle_report(&config)).expect("claim");
    assert_eq!(
        answer.get("wait").and_then(Value::as_str),
        Some("assembling"),
        "{answer:?}"
    );

    // The chunks the cluster holds are contiguous, part by part.
    let held = listener.ask(&format!(r#"{{"op": "chunks", "id": {id}}}"#));
    let mut ends = [0.0_f64; 2];
    let mut next = [0_i128; 2];
    for chunk in get(&held, "chunks").as_list().expect("chunks") {
        let fields = chunk.as_list().expect("fields");
        let (Value::Int(part), Value::Int(index), Value::Float(start), Value::Float(duration)) =
            (&fields[0], &fields[1], &fields[2], &fields[3])
        else {
            panic!("{fields:?}")
        };
        let at = usize::try_from(*part).expect("part");
        assert_eq!(*index, next[at], "{held:?}");
        assert!((start - ends[at]).abs() <= 0.25, "{held:?}");
        next[at] += 1;
        ends[at] = start + duration;
    }

    // The cluster joins them once the audio is ready; the merge Job assembles the title.
    listener.ask(r#"{"op": "tick"}"#);
    let state = listener.ask(&format!(r#"{{"op": "state", "id": {id}}}"#));
    assert_eq!(get(&state, "state").as_str(), Some("merging"), "{state:?}");
    let jobs = get(&state, "jobs").as_list().expect("jobs").to_vec();
    let assemble = jobs.last().and_then(Value::as_list).expect("the merge Job")[1]
        .as_list()
        .expect("args")
        .to_vec();
    assert_eq!(
        assemble[assemble.len() - 2..]
            .iter()
            .map(|arg| arg.as_str().expect("text"))
            .collect::<Vec<_>>(),
        ["--home-assemble", directory.to_string_lossy().as_ref()]
    );
    run_job(&assemble);
    let output = PathBuf::from(job_command(&assemble).output);
    let source_frames: i64 = parts.iter().map(|part| frames(part)).sum();
    assert_eq!(frames(&output), source_frames);
    let prepared: Value =
        pyjson::loads(&std::fs::read_to_string(audio_status_path(&directory)).expect("audio status")).expect("JSON");
    let durations: Vec<f64> = get(&prepared, "parts")
        .as_list()
        .expect("parts")
        .iter()
        .map(|part| match get(part, "duration") {
            Value::Float(duration) => *duration,
            other => panic!("a duration, not {other:?}"),
        })
        .collect();
    let mut times: Vec<f64> = probe(&output, "packet=pts_time", "v:0")
        .iter()
        .map(|value| value.parse().expect("a time"))
        .collect();
    times.sort_by(f64::total_cmp);
    // The second part's first frame sits where the first part ended, audio and all.
    let first_part_frames = usize::try_from(frames(&parts[0])).expect("count");
    assert!(
        (times[first_part_frames] - durations[0]).abs() <= 0.02,
        "{} vs {}",
        times[first_part_frames],
        durations[0]
    );
    let audio: f64 = probe(&output, "stream=duration", "a:0")[0].parse().expect("a duration");
    let total: f64 = durations.iter().sum();
    assert!((audio - total).abs() <= 0.1, "{audio} vs {total}");
}
