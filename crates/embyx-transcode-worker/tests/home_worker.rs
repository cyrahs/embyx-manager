//! The home transcode worker against a stand-in cluster, with real ffmpeg on tiny parts.
//!
//! Python origin: `tests/test_home_worker.py`. The stand-in keeps chunks as the cluster's
//! service does, in order and contiguous; on Unix the merge worker (`embyx-merge`, no
//! database) prepares the audio beforehand and assembles the title afterwards, as the
//! cluster's Job does.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command as Process;
use std::sync::{Arc, Mutex, PoisonError};

use embyx_core::pyjson::{self, Dict, DumpOptions, Value};
#[cfg(unix)]
use embyx_merge::transcode::audio_status_path;
use embyx_merge::transcode::chunk_path;
#[cfg(unix)]
use embyx_merge::worker::{self, Command, Environment, StatusFile, Worker};
use embyx_transcode_worker::WorkerConfig;
use embyx_transcode_worker::chunks::{self, LocalChunk, RateLimiter, RateMeter};
use embyx_transcode_worker::config::{self, Args};
use embyx_transcode_worker::console::MemoryLog;
use embyx_transcode_worker::process::SystemRunner;
use embyx_transcode_worker::server::{CallError, Cluster};
use embyx_transcode_worker::task::{Assignment, Outcome, RunClock, RunParts, TaskRun, Timing};

/// B-frames give the encoder a reorder delay, as NVENC has; `realtime` slows it enough to
/// stop it midway.
const ENCODER: &str =
    r#"{"codec": "libx264", "extra": ["-preset", "veryfast", "-bf", "3", "-vf", "realtime=speed=4"]}"#;

fn encoder() -> Dict {
    pyjson::loads(ENCODER).expect("JSON").as_dict().expect("object").clone()
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

#[cfg(unix)]
fn frames(path: &Path) -> i64 {
    probe(path, "stream=nb_read_packets", "v:0")[0]
        .parse()
        .expect("a count")
}

/// Stands in for the cluster's side: keeps chunks as the service does, in order and
/// contiguous. What the Python stand-in asserts is collected and checked by the test, since
/// a panic in the uploader's thread would only end that thread.
struct StandIn {
    directory: PathBuf,
    parts: Vec<PathBuf>,
    durations: Vec<f64>,
    chunks: Mutex<BTreeMap<i128, Vec<LocalChunk>>>,
    finished: Mutex<BTreeSet<i128>>,
    broken: Mutex<Vec<String>>,
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn answer(key: &str) -> Dict {
    let mut dict = Dict::new();
    dict.insert(key, Value::Bool(true));
    dict
}

impl StandIn {
    fn held(&self) -> usize {
        lock(&self.chunks).values().map(Vec::len).sum()
    }

    fn assignment(&self) -> Assignment {
        let chunks = lock(&self.chunks);
        let finished = lock(&self.finished);
        let parts: Vec<String> = self
            .parts
            .iter()
            .enumerate()
            .map(|(at, part)| {
                let index = i128::try_from(at).expect("small");
                let mine = &chunks[&index];
                format!(
                    r#"{{"index": {index}, "name": {}, "size": {}, "duration": {}, "done": {}, "next_chunk": {}, "resume_at": {}}}"#,
                    pyjson::dumps(
                        &Value::Str(part.file_name().expect("name").to_string_lossy().into_owned()),
                        &DumpOptions::python_default()
                    )
                    .expect("name"),
                    std::fs::metadata(part).expect("part").len(),
                    pyjson::float_repr(self.durations[at]),
                    finished.contains(&index),
                    mine.len(),
                    pyjson::float_repr(mine.last().map_or(0.0, LocalChunk::end)),
                )
            })
            .collect();
        let text = format!(
            r#"{{"id": 7, "avid": "SQTEVR-009", "video_bitrate": 300000, "chunk_seconds": 2, "encoder": {ENCODER}, "paused": false, "parts": [{}]}}"#,
            parts.join(", ")
        );
        let task = pyjson::loads(&text).expect("assignment");
        Assignment::from_dict(task.as_dict().expect("object")).expect("assignment")
    }

    fn manifest(&self) -> String {
        let parts: Vec<String> = lock(&self.chunks)
            .iter()
            .map(|(index, chunks)| {
                let chunks: Vec<String> = chunks
                    .iter()
                    .map(|chunk| {
                        format!(
                            r#"{{"index": {}, "duration": {}}}"#,
                            chunk.index,
                            pyjson::float_repr(chunk.duration)
                        )
                    })
                    .collect();
                format!(r#"{{"index": {index}, "chunks": [{}]}}"#, chunks.join(", "))
            })
            .collect();
        format!(r#"{{"parts": [{}]}}"#, parts.join(", "))
    }
}

impl Cluster for StandIn {
    fn part_url(&self, _task_id: i128, part_index: i128) -> String {
        self.parts[usize::try_from(part_index).expect("index")]
            .to_string_lossy()
            .into_owned()
    }

    fn claim(&self, _report: &Value) -> Result<Dict, CallError> {
        Err(CallError::Transport("the stand-in assigns nothing".to_owned()))
    }

    fn heartbeat(&self, _report: &Value) -> Result<Dict, CallError> {
        let mut dict = answer("active");
        dict.insert("paused", Value::Bool(false));
        Ok(dict)
    }

    fn upload(
        &self,
        _task_id: i128,
        chunk: &LocalChunk,
        _limiter: &RateLimiter,
        _meter: &RateMeter,
    ) -> Result<Dict, CallError> {
        let mut chunks = lock(&self.chunks);
        let mine = chunks.entry(chunk.part_index).or_default();
        let expected = mine.last().map_or(0.0, LocalChunk::end);
        if chunk.index != i128::try_from(mine.len()).expect("small") || (chunk.start - expected).abs() > 0.25 {
            lock(&self.broken).push(format!("out of order: {chunk:?} after {} chunks", mine.len()));
        }
        let target = chunk_path(&self.directory, chunk.part_index, chunk.index);
        std::fs::create_dir_all(target.parent().expect("parent")).map_err(CallError::Io)?;
        std::fs::copy(&chunk.path, &target).map_err(CallError::Io)?;
        mine.push(chunk.clone());
        Ok(answer("stored"))
    }

    fn finish(&self, _task_id: i128, part_index: i128) -> Result<Dict, CallError> {
        let chunks = lock(&self.chunks);
        let end = chunks[&part_index].last().map_or(0.0, LocalChunk::end);
        let duration = self.durations[usize::try_from(part_index).expect("index")];
        if (end - duration).abs() > 2.0 {
            lock(&self.broken).push(format!("cd{} ends at {end}, not {duration}", part_index + 1));
        }
        lock(&self.finished).insert(part_index);
        Ok(answer("finished"))
    }
}

#[cfg(unix)]
fn merge_run(command: &Command) -> i32 {
    let mut status = StatusFile::new(worker::py_path(&command.status));
    let mut stderr = Vec::new();
    let code = worker::run(
        &Worker::default(),
        command,
        &mut status,
        &Environment::from_process(),
        &mut stderr,
    );
    assert_eq!(code, 0, "{}", String::from_utf8_lossy(&stderr));
    code
}

#[cfg(unix)]
fn read(path: &Path) -> Value {
    pyjson::loads(&std::fs::read_to_string(path).expect("read")).expect("JSON")
}

fn task_run(config: &WorkerConfig, cluster: &Arc<StandIn>) -> TaskRun {
    TaskRun::new(
        RunParts {
            config: config.clone(),
            cluster: cluster.clone(),
            runner: Arc::new(SystemRunner),
            log: Arc::new(MemoryLog::default()),
            timing: Timing::default(),
        },
        cluster.assignment(),
    )
    .expect("a run")
}

// py: tests/test_home_worker.py::test_a_stopped_and_resumed_transcode_assembles_with_every_frame_in_sync
#[test]
fn a_stopped_and_resumed_transcode_assembles_with_every_frame_in_sync() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let tmp = tmp.path();
    let library = tmp.join("library");
    std::fs::create_dir(&library).expect("library");
    let parts = vec![
        make_part(&library.join("SQTEVR-009-cd1.mp4"), 14.0, 0.2),
        make_part(&library.join("SQTEVR-009-cd2.mp4"), 5.0, 0.0),
    ];
    let directory = tmp.join("upload").join(".transcode").join("7");
    let output = tmp.join("upload").join("SQTEVR-009.mp4");

    let durations = prepare(&parts, &directory, &output);

    let cluster = Arc::new(StandIn {
        directory: directory.clone(),
        parts: parts.clone(),
        durations: durations.clone(),
        chunks: Mutex::new((0..2).map(|index| (index, Vec::new())).collect()),
        finished: Mutex::new(BTreeSet::new()),
        broken: Mutex::new(Vec::new()),
    });
    let mut config = WorkerConfig::new("http://cluster");
    config.cache_dir = tmp.join("cache");
    config.jobs = 1;
    config.encoder = encoder();

    // Ctrl+C once the first chunk is in: the run finishes the chunk under way and stops.
    let stop = {
        let cluster = Arc::clone(&cluster);
        move || usize::from(cluster.held() >= 1)
    };
    let outcome = task_run(&config, &cluster).run(&stop).expect("first run");
    assert_eq!(outcome, Outcome::Stopped);
    let held: Vec<usize> = lock(&cluster.chunks).values().map(Vec::len).collect();
    assert!((1..6).contains(&held[0]), "{held:?}");
    assert_eq!(held[1], 0);
    assert!(lock(&cluster.finished).is_empty());

    let outcome = task_run(&config, &cluster).run(&|| 0).expect("second run");
    assert_eq!(outcome, Outcome::Done);
    assert_eq!(*lock(&cluster.finished), BTreeSet::from([0, 1]));
    assert_eq!(*lock(&cluster.broken), Vec::<String>::new());
    std::fs::write(directory.join("manifest.json"), cluster.manifest()).expect("manifest");

    assemble(&parts, &directory, &output, &durations);
}

/// The cluster's audio Job: the merge worker probes the parts and copies their audio; the
/// parts' durations as it found them.
#[cfg(unix)]
fn prepare(parts: &[PathBuf], directory: &Path, output: &Path) -> Vec<f64> {
    merge_run(&Command {
        parts: parts.iter().map(|part| text(part)).collect(),
        output: text(output),
        status: text(&audio_status_path(directory)),
        reserve_gib: 0.0,
        home_prepare: Some(text(directory)),
        home_assemble: None,
    });
    let prepared = read(&audio_status_path(directory));
    assert_eq!(prepared.get("state").and_then(Value::as_str), Some("done"));
    prepared
        .get("parts")
        .and_then(Value::as_list)
        .expect("parts")
        .iter()
        .map(|part| match part.get("duration") {
            Some(Value::Float(duration)) => *duration,
            other => panic!("a duration, not {other:?}"),
        })
        .collect()
}

/// The merge worker runs in the cluster's Linux Job only (D144): on Windows the parts'
/// durations come from ffprobe and the worker's side is tested without the assembly.
#[cfg(not(unix))]
fn prepare(parts: &[PathBuf], _directory: &Path, _output: &Path) -> Vec<f64> {
    parts
        .iter()
        .map(|part| probe(part, "format=duration", "v:0")[0].parse().expect("a duration"))
        .collect()
}

/// The cluster's merge Job: the title assembled from the chunks and the audio, every frame
/// in place.
#[cfg(unix)]
fn assemble(parts: &[PathBuf], directory: &Path, output: &Path, durations: &[f64]) {
    let status = output.parent().expect("the work folder").join(".merge").join("7.json");
    merge_run(&Command {
        parts: Vec::new(),
        output: text(output),
        status: text(&status),
        reserve_gib: 0.0,
        home_prepare: None,
        home_assemble: Some(text(directory)),
    });
    assert_eq!(read(&status).get("state").and_then(Value::as_str), Some("done"));
    let source_frames: i64 = parts.iter().map(|part| frames(part)).sum();
    assert_eq!(frames(output), source_frames);
    // The second part's first frame sits where the first part ended, audio and all.
    let mut times: Vec<f64> = probe(output, "packet=pts_time", "v:0")
        .iter()
        .map(|value| value.parse().expect("a time"))
        .collect();
    times.sort_by(f64::total_cmp);
    let first_part_frames = usize::try_from(frames(&parts[0])).expect("count");
    assert!(
        (times[first_part_frames] - durations[0]).abs() <= 0.02,
        "{} vs {}",
        times[first_part_frames],
        durations[0]
    );
    let audio: f64 = probe(output, "stream=duration", "a:0")[0].parse().expect("a duration");
    let total: f64 = durations.iter().sum();
    assert!((audio - total).abs() <= 0.1, "{audio} vs {total}");
}

#[cfg(not(unix))]
fn assemble(_parts: &[PathBuf], _directory: &Path, _output: &Path, _durations: &[f64]) {}

#[cfg(unix)]
fn text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

// py: tests/test_home_worker.py::test_chunk_times_drop_the_encoders_reorder_delay
#[test]
fn chunk_times_drop_the_encoders_reorder_delay() {
    let clock = RunClock {
        anchor: 10.01,
        shift: Some(0.0833),
    };
    // As the segment list reports a run resumed at 10.01 s: shifted by the B-frame delay.
    let (start, end) = (0.0, 10.0933);
    let shift = clock.shift.expect("shift");
    assert!((clock.anchor + (start - shift).max(0.0) - 10.01).abs() < 1e-9);
    assert!((clock.end(shift, end) - 20.02).abs() < 1e-9);
}

// py: tests/test_home_worker.py::test_kept_chunks_resume_only_a_contiguous_run
#[test]
fn kept_chunks_resume_only_a_contiguous_run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let keep = |index: i128, start: f64| {
        let path = tmp.path().join(format!("chunk-{index:05}.mkv"));
        std::fs::write(&path, b"x").expect("chunk");
        LocalChunk {
            part_index: 0,
            index,
            start,
            duration: 2.0,
            frames: 60,
            bytes: 1,
            sha256: "0".repeat(64),
            path,
        }
        .save()
        .expect("sidecar");
    };
    keep(3, 6.0);
    keep(4, 8.0);
    keep(6, 12.0);

    let kept = chunks::kept_chunks(tmp.path(), 3, 6.0);

    assert_eq!(kept.iter().map(|chunk| chunk.index).collect::<Vec<_>>(), [3, 4]);
}

// py: tests/test_home_worker.py::test_the_config_file_and_flags_combine
#[test]
fn the_config_file_and_flags_combine() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("config.toml");
    std::fs::write(
        &path,
        "server = \"http://10.117.200.1:8080\"\nupload_mbps = 25\n[encoder]\ntune = \"hq\"\n",
    )
    .expect("config");

    let config = config::load_config(&Args {
        config: Some(path),
        jobs: Some(3),
        ..Args::default()
    })
    .expect("a config");

    let mut tune = Dict::new();
    tune.insert("tune", Value::Str("hq".to_owned()));
    assert_eq!(
        (config.server.as_str(), config.upload_mbps, config.jobs, &config.encoder),
        ("http://10.117.200.1:8080", 25.0, 3, &tune)
    );
}
