//! `home_worker_run.json`: a run's pieces one by one (the status line and the heartbeat
//! report, picking up listed chunks, the run clock, the upload pace) and the small functions
//! around them. Inside `task` because they reach the run's private state.

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use embyx_core::golden;
use embyx_core::pyjson::Value;

use super::{Assignment, Inner, RunClock, RunParts, TaskRun, Timing, VERSION, lock};
use crate::chunks::{LocalChunk, RateLimiter, RateMeter};
use crate::config::WorkerConfig;
use crate::console::MemoryLog;
use crate::golden_tests::{cases, dict, float, object, slashes};
use crate::process::{Captured, RunError};
use crate::run;
use crate::server::UPLOAD_PIECE_BYTES;
use crate::testing::{FakeClock, FakeCluster, IdleProcess, ScriptedRunner};

fn config(cache: &Path) -> WorkerConfig {
    let mut config = WorkerConfig::new("http://cluster");
    config.name = "pc".to_owned();
    config.cache_dir = cache.to_owned();
    config
}

/// `_assignment(parts)` of the generator.
fn assignment(parts: &Value) -> Assignment {
    let task = embyx_core::pyjson::loads(&format!(
        r#"{{"id": 7, "avid": "SQTEVR-009", "video_bitrate": 6140000, "chunk_seconds": 60,
            "encoder": {{"codec": "av1_nvenc", "preset": "p7", "tune": "uhq", "multipass": "fullres",
                        "lookahead": 32, "spatial_aq": true, "bit_depth": 10, "gop_seconds": 5}},
            "paused": false, "parts": {}}}"#,
        embyx_core::pyjson::dumps(parts, &embyx_core::pyjson::DumpOptions::python_default()).expect("parts")
    ))
    .expect("task");
    Assignment::from_dict(task.as_dict().expect("object")).expect("assignment")
}

fn task_run(
    config: WorkerConfig,
    task: Assignment,
    runner: Arc<ScriptedRunner>,
    clock: Arc<FakeClock>,
) -> (TaskRun, Arc<MemoryLog>) {
    let log = Arc::new(MemoryLog::default());
    let run = TaskRun::with_clock(
        RunParts {
            config,
            cluster: Arc::new(FakeCluster::default()),
            runner,
            log: log.clone(),
            timing: Timing::default(),
        },
        task,
        clock,
    )
    .expect("run");
    (run, log)
}

fn log_lines(log: &MemoryLog) -> Value {
    Value::List(
        log.lines()
            .into_iter()
            .map(|(status, line)| object([("status", Value::Bool(status)), ("line", Value::Str(line))]))
            .collect(),
    )
}

fn no_probe() -> Arc<ScriptedRunner> {
    Arc::new(ScriptedRunner::new(|argv| panic!("no ffprobe expected: {argv:?}")))
}

fn status(case: &Value) {
    let clock = Arc::new(FakeClock::at(1000.0));
    let (run, log) = task_run(
        config(Path::new("/nonexistent")),
        assignment(golden::get(case, "parts")),
        no_probe(),
        clock.clone(),
    );
    let inner: &Inner = &run.inner;
    for (part, state) in inner.parts.iter().zip(golden::list(golden::get(case, "states"))) {
        let mut part = lock(part);
        for (key, value) in dict(state).iter() {
            match key.as_str().expect("name") {
                "fps" => part.fps = float(value),
                "position" => part.position = float(value),
                "resume_at" => part.resume_at = float(value),
                "finished" => part.finished = value == &Value::Bool(true),
                other => panic!("unknown field {other}"),
            }
        }
    }
    for at in golden::list(golden::get(case, "active")) {
        let at = usize::try_from(golden::int(at)).expect("position");
        lock(&inner.processes).insert(at, Arc::new(IdleProcess));
    }
    for (number, size) in golden::list(golden::get(case, "queue")).iter().enumerate() {
        inner.enqueue(LocalChunk {
            part_index: 0,
            index: i128::try_from(number).expect("small"),
            start: 0.0,
            duration: 1.0,
            frames: 1,
            bytes: golden::int(size),
            sha256: String::new(),
            path: "x".into(),
        });
    }
    for upload in golden::list(golden::get(case, "uploads")) {
        let pair = golden::list(upload);
        clock.set(float(&pair[0]));
        inner.meter.add(usize::try_from(golden::int(&pair[1])).expect("count"));
    }
    clock.set(1000.0);
    inner.paused.store(
        golden::get(case, "paused") == &Value::Bool(true),
        std::sync::atomic::Ordering::SeqCst,
    );
    if golden::get(case, "wind_down") == &Value::Bool(true) {
        inner.wind_down.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    inner.log_status();
    assert_eq!(log_lines(&log), *golden::get(case, "lines"), "case {case:?}");
    let reports = golden::get(case, "reports");
    for state in ["encoding", "paused", "stopping"] {
        assert_eq!(
            run.report(state),
            *golden::get(reports, state),
            "case {case:?}, {state}"
        );
    }
}

#[allow(clippy::too_many_lines, reason = "one golden case: set up, run, compare every field")]
fn collect(case: &Value, number: usize, root: &Path) {
    let folder = root.join(format!("collect{number}"));
    let directory = folder.join("7").join("cd01");
    fs::create_dir_all(&directory).expect("folders");
    let pts = golden::string(golden::get(case, "pts"));
    let frames = golden::string(golden::get(case, "frames"));
    let runner = Arc::new(ScriptedRunner::new(move |argv| {
        let stdout = if argv.iter().any(|item| item == "packet=pts_time") {
            &pts
        } else {
            &frames
        };
        Ok::<_, RunError>(Captured {
            returncode: 0,
            stdout: stdout.clone(),
            stderr: String::new(),
        })
    }));
    let parts = embyx_core::pyjson::loads(
        r#"[{"index": 0, "name": "SQTEVR-009-cd1.mp4", "size": 1000000000, "duration": 600.0,
             "done": false, "next_chunk": 0, "resume_at": 0.0}]"#,
    )
    .expect("parts");
    let (run, log) = task_run(
        config(&folder),
        assignment(&parts),
        runner.clone(),
        Arc::new(FakeClock::at(1000.0)),
    );
    let inner: &Inner = &run.inner;
    let given = golden::get(case, "part");
    let next_index = golden::int(golden::get(given, "next_index"));
    let resume_at = float(golden::get(given, "resume_at"));
    {
        let mut part = lock(&inner.parts[0]);
        part.next_index = next_index;
        part.resume_at = resume_at;
    }
    for index in 0..8 {
        fs::write(
            directory.join(format!("chunk-{index:05}.mkv")),
            format!("chunk {index}"),
        )
        .expect("chunk");
    }
    let listing = directory.join(format!("list-{next_index:05}.csv"));
    if let Some(text) = golden::get(case, "listing").as_str() {
        fs::write(&listing, text).expect("listing");
    }
    let mut clock = RunClock {
        anchor: resume_at,
        shift: case.get("shift").map(float),
    };
    let seen = usize::try_from(golden::int(golden::get(case, "seen"))).expect("seen");
    let returned = inner.collect(0, &listing, &mut clock, seen).expect("collect");
    let rooted = |text: &str| slashes(&text.replace(&*folder.to_string_lossy(), "<root>"));
    let part = lock(&inner.parts[0]).clone();
    let queued: Vec<Value> = lock(&inner.queue)
        .iter()
        .map(|chunk| {
            object([
                ("part_index", Value::Int(chunk.part_index)),
                ("index", Value::Int(chunk.index)),
                ("start", Value::Float(chunk.start)),
                ("duration", Value::Float(chunk.duration)),
                ("frames", Value::Int(chunk.frames)),
                ("bytes", Value::Int(chunk.bytes)),
                ("sha256", Value::Str(chunk.sha256.clone())),
                ("path", Value::Str(rooted(&chunk.path.to_string_lossy()))),
            ])
        })
        .collect();
    let mut sidecars: Vec<String> = fs::read_dir(&directory)
        .expect("listing")
        .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
        .filter(|name| Path::new(name).extension().is_some_and(|suffix| suffix == "json"))
        .collect();
    sidecars.sort();
    let sidecars: Vec<Value> = sidecars
        .into_iter()
        .map(|name| {
            let text = fs::read_to_string(directory.join(&name)).expect("sidecar");
            object([("name", Value::Str(name)), ("text", Value::Str(text))])
        })
        .collect();
    let calls: Vec<Value> = runner
        .calls()
        .into_iter()
        .map(|(argv, _)| Value::List(argv.iter().map(|item| Value::Str(rooted(item))).collect()))
        .collect();
    let got = object([
        ("returned", Value::Int(i128::try_from(returned).expect("small"))),
        ("shift_after", clock.shift.map_or(Value::Null, Value::Float)),
        ("next_index", Value::Int(part.next_index)),
        ("resume_at", Value::Float(part.resume_at)),
        ("queued", Value::List(queued)),
        ("sidecars", Value::List(sidecars)),
        ("calls", Value::List(calls)),
        ("log", log_lines(&log)),
    ]);
    let want = object(
        [
            "returned",
            "shift_after",
            "next_index",
            "resume_at",
            "queued",
            "sidecars",
            "calls",
            "log",
        ]
        .map(|key| (key, golden::get(case, key).clone())),
    );
    assert_eq!(got, want, "case {case:?}");
}

fn limiter(case: &Value) {
    let clock = Arc::new(FakeClock::at(1000.0));
    let limiter = RateLimiter::new(float(golden::get(case, "bits_per_second")), clock.clone());
    for step in golden::list(golden::get(case, "steps")) {
        let pair = golden::list(step);
        clock.set(1000.0 + float(&pair[0]));
        limiter.take(usize::try_from(golden::int(&pair[1])).expect("count"));
    }
    let sleeps = Value::List(clock.sleeps().into_iter().map(Value::Float).collect());
    assert_eq!(sleeps, *golden::get(case, "sleeps"), "case {case:?}");
}

fn meter(case: &Value) {
    let clock = Arc::new(FakeClock::at(1000.0));
    let meter = RateMeter::new(clock.clone());
    for sample in golden::list(golden::get(case, "samples")) {
        let pair = golden::list(sample);
        clock.set(1000.0 + float(&pair[0]));
        meter.add(usize::try_from(golden::int(&pair[1])).expect("count"));
    }
    for rate in golden::list(golden::get(case, "rates")) {
        clock.set(1000.0 + float(golden::get(rate, "at")));
        assert_eq!(Value::Float(meter.rate()), *golden::get(rate, "rate"), "rate {rate:?}");
    }
}

fn task(case: &Value) {
    let given = dict(golden::get(case, "task"));
    let task = Assignment::from_dict(&given).expect("assignment");
    let (run, _) = task_run(
        config(Path::new("/nonexistent")),
        task.clone(),
        no_probe(),
        Arc::new(FakeClock::at(0.0)),
    );
    let parts: Vec<Value> = run
        .parts()
        .into_iter()
        .map(|part| {
            object([
                ("name", Value::Str(part.name)),
                ("index", Value::Int(part.index)),
                ("duration", Value::Float(part.duration)),
                ("next_index", Value::Int(part.next_index)),
                ("resume_at", Value::Float(part.resume_at)),
                ("fps", Value::Float(part.fps)),
                ("position", Value::Float(part.position)),
                ("encoded", Value::Bool(part.encoded)),
                ("finished", Value::Bool(part.finished)),
                ("error", part.error.map_or(Value::Null, Value::Str)),
                ("uploads", Value::Int(i128::from(part.uploads))),
            ])
        })
        .collect();
    assert_eq!(Value::List(parts), *golden::get(case, "parts"));
    assert_eq!(
        Value::Bool(run.inner.paused.load(std::sync::atomic::Ordering::SeqCst)),
        *golden::get(case, "paused")
    );
    assert_eq!(
        Value::Str(slashes(&run.inner.directory.to_string_lossy())),
        *golden::get(case, "directory")
    );
    assert_eq!(
        Value::Int(run.inner.max_queue_bytes),
        *golden::get(case, "max_queue_bytes")
    );
    let mut overridden = WorkerConfig::new("x");
    overridden.encoder = dict(golden::get(case, "encoder_override"));
    let (run, _) = task_run(overridden, task, no_probe(), Arc::new(FakeClock::at(0.0)));
    assert_eq!(Value::Dict(run.inner.encoder.clone()), *golden::get(case, "encoder"));
}

#[test]
fn run_pieces_match_python() {
    let root = tempfile::tempdir().expect("temporary directory");
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    for case in cases("run") {
        let kind = golden::string(golden::get(&case, "kind"));
        let seen = *counts.get(&kind).unwrap_or(&0);
        match kind.as_str() {
            "status" => status(&case),
            "collect" => collect(&case, seen, root.path()),
            "clock" => {
                let clock = RunClock {
                    anchor: float(golden::get(&case, "anchor")),
                    shift: Some(float(golden::get(&case, "shift"))),
                };
                let shift = float(golden::get(&case, "shift"));
                let begin = clock.begin(
                    shift,
                    float(golden::get(&case, "start")),
                    float(golden::get(&case, "resume_at")),
                );
                assert_eq!(Value::Float(begin), *golden::get(&case, "begin"), "case {case:?}");
                let duration = clock.end(shift, float(golden::get(&case, "end"))) - begin;
                assert_eq!(Value::Float(duration), *golden::get(&case, "duration"), "case {case:?}");
            }
            "limiter" => limiter(&case),
            "meter" => meter(&case),
            "idle_report" => {
                let mut config = WorkerConfig::new("x");
                config.name = golden::string(golden::get(&case, "name"));
                assert_eq!(run::idle_report(&config), *golden::get(&case, "value"));
            }
            "wait_reason" => {
                let wait = golden::string(golden::get(&case, "wait"));
                assert_eq!(
                    run::wait_reason(&wait),
                    Some(golden::string(golden::get(&case, "reason")).as_str())
                );
            }
            "constants" => {
                let timing = Timing::default();
                assert_eq!(Value::Str(VERSION.to_owned()), *golden::get(&case, "version"));
                let delays = Value::List(
                    timing
                        .retry_delays
                        .iter()
                        .map(|delay| Value::Float(delay.as_secs_f64()))
                        .collect(),
                );
                assert_eq!(delays, *golden::get(&case, "retry_delays"));
                for (key, duration) in [
                    ("heartbeat", timing.heartbeat),
                    ("idle_poll", timing.idle_poll),
                    ("status", timing.status),
                ] {
                    assert_eq!(Value::Float(duration.as_secs_f64()), *golden::get(&case, key), "{key}");
                }
                assert_eq!(
                    Value::Int(i128::try_from(UPLOAD_PIECE_BYTES).expect("small")),
                    *golden::get(&case, "upload_piece")
                );
                assert_eq!(timing.short_poll, Duration::from_secs(10));
            }
            "tools" => {
                // The generator found `sh` on the PATH; a Windows runner has no `sh`.
                if cfg!(unix) {
                    let mut config = WorkerConfig::new("x");
                    config.ffmpeg = golden::string(golden::get(&case, "ffmpeg"));
                    config.ffprobe = golden::string(golden::get(&case, "ffprobe"));
                    let problem = run::check_tools(&config).map_or(Value::Null, Value::Str);
                    assert_eq!(problem, *golden::get(&case, "problem"), "case {case:?}");
                }
            }
            "task" => task(&case),
            other => panic!("unknown kind {other}"),
        }
        counts.insert(kind, seen + 1);
    }
    let counts: Vec<(&str, usize)> = counts.iter().map(|(kind, count)| (kind.as_str(), *count)).collect();
    assert_eq!(
        counts,
        [
            ("clock", 5),
            ("collect", 9),
            ("constants", 1),
            ("idle_report", 2),
            ("limiter", 4),
            ("meter", 1),
            ("status", 4),
            ("task", 1),
            ("tools", 3),
            ("wait_reason", 5),
        ]
    );
}
