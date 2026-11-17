//! The encoder's control of its ffmpeg (`_encode_part`): suspended while the upload queue is
//! full, resumed below half of it and when a stop comes, killed once the chunk under way at
//! the stop is listed, and started again after a failed run. A puppet stands in for ffmpeg;
//! the test writes the chunks and the segment list ffmpeg would write.

use std::ffi::OsString;
use std::fmt::Write as _;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::{Assignment, Inner, RunParts, TaskRun, Timing, lock};
use crate::config::WorkerConfig;
use crate::console::MemoryLog;
use crate::process::{Captured, RunError, Runner, Running};
use crate::testing::{FakeClock, FakeCluster};

fn fast() -> Timing {
    Timing {
        tick: Duration::from_millis(5),
        encoder_tick: Duration::from_millis(5),
        heartbeat: Duration::from_millis(5),
        status: Duration::from_millis(5),
        retry_delays: vec![Duration::from_millis(5), Duration::from_millis(10)],
        join: Duration::from_secs(5),
        idle_poll: Duration::from_millis(5),
        short_poll: Duration::from_millis(5),
    }
}

/// An ffmpeg that does nothing by itself: it records what it is told and ends when killed,
/// or at once with `exit`.
struct Puppet {
    calls: Mutex<Vec<&'static str>>,
    exited: Mutex<Option<i64>>,
}

impl Running for Puppet {
    fn take_stdout(&self) -> Option<Box<dyn Read + Send>> {
        None
    }
    fn take_stderr(&self) -> Option<Box<dyn Read + Send>> {
        None
    }
    fn poll(&self) -> io::Result<Option<i64>> {
        Ok(*lock(&self.exited))
    }
    fn kill(&self) -> io::Result<()> {
        lock(&self.calls).push("kill");
        lock(&self.exited).get_or_insert(-9);
        Ok(())
    }
    fn suspend(&self) -> io::Result<()> {
        lock(&self.calls).push("suspend");
        Ok(())
    }
    fn resume(&self) -> io::Result<()> {
        lock(&self.calls).push("resume");
        Ok(())
    }
    fn id(&self) -> u32 {
        1
    }
}

/// Answers the probes and hands out one puppet per start.
struct Puppets {
    exit: Option<i64>,
    started: Mutex<Vec<Arc<Puppet>>>,
    probes: AtomicUsize,
}

impl Puppets {
    fn new(exit: Option<i64>) -> Arc<Self> {
        Arc::new(Self {
            exit,
            started: Mutex::new(Vec::new()),
            probes: AtomicUsize::new(0),
        })
    }

    fn current(&self) -> Option<Arc<Puppet>> {
        lock(&self.started).last().cloned()
    }
}

impl Runner for Puppets {
    fn run(&self, argv: &[OsString], _timeout: Option<Duration>) -> Result<Captured, RunError> {
        self.probes.fetch_add(1, Ordering::SeqCst);
        let argv: Vec<String> = argv.iter().map(|arg| arg.to_string_lossy().into_owned()).collect();
        let stdout = if argv.iter().any(|arg| arg == "json") {
            r#"{"streams": [{"avg_frame_rate": "30/1", "r_frame_rate": "30/1"}]}"#
        } else if argv.iter().any(|arg| arg == "packet=pts_time") {
            "0.000000\n"
        } else {
            "60\n"
        };
        Ok(Captured {
            returncode: 0,
            stdout: stdout.to_owned(),
            stderr: String::new(),
        })
    }

    fn spawn(&self, _argv: &[OsString]) -> io::Result<Arc<dyn Running>> {
        let puppet = Arc::new(Puppet {
            calls: Mutex::new(Vec::new()),
            exited: Mutex::new(self.exit),
        });
        lock(&self.started).push(Arc::clone(&puppet));
        Ok(puppet)
    }
}

fn assignment() -> Assignment {
    let task = embyx_core::pyjson::loads(
        r#"{"id": 7, "avid": "SQTEVR-009", "video_bitrate": 1000000, "chunk_seconds": 2,
            "encoder": {"codec": "libx264"}, "paused": false,
            "parts": [{"index": 0, "name": "a-cd1.mp4", "size": 1, "duration": 20.0, "done": false,
                       "next_chunk": 0, "resume_at": 0.0}]}"#,
    )
    .expect("task");
    Assignment::from_dict(task.as_dict().expect("object")).expect("assignment")
}

/// A run whose upload queue holds two 3-byte chunks.
fn run(cache: &Path, runner: Arc<Puppets>) -> (TaskRun, Arc<MemoryLog>) {
    let log = Arc::new(MemoryLog::default());
    let mut config = WorkerConfig::new("http://cluster");
    config.cache_dir = cache.to_owned();
    config.max_queue_gb = 6.0 / 1024_f64.powi(3);
    let run = TaskRun::with_clock(
        RunParts {
            config,
            cluster: Arc::new(FakeCluster::default()),
            runner,
            log: log.clone(),
            timing: fast(),
        },
        assignment(),
        Arc::new(FakeClock::at(0.0)),
    )
    .expect("run");
    (run, log)
}

fn wait_until(what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// What ffmpeg does when it closes a segment: the chunk, then its line in the list.
fn write_chunk(directory: &Path, index: u32) {
    std::fs::write(directory.join(format!("chunk-{index:05}.mkv")), b"abc").expect("chunk");
    let listing = directory.join("list-00000.csv");
    let mut text = std::fs::read_to_string(&listing).unwrap_or_default();
    let start = 2.0 * f64::from(index);
    writeln!(text, "chunk-{index:05}.mkv,{start:.6},{:.6}", start + 2.0).expect("a line");
    std::fs::write(&listing, text).expect("listing");
}

fn calls(runner: &Puppets) -> Vec<&'static str> {
    runner
        .current()
        .map(|puppet| lock(&puppet.calls).clone())
        .unwrap_or_default()
}

fn queued(inner: &Inner) -> Vec<i128> {
    lock(&inner.queue).iter().map(|chunk| chunk.index).collect()
}

#[test]
fn the_encoder_follows_the_queue_and_stops_after_the_chunk_under_way() {
    let cache = tempfile::tempdir().expect("cache");
    let runner = Puppets::new(None);
    let (run, _) = run(cache.path(), Arc::clone(&runner));
    let inner = Arc::clone(&run.inner);
    let directory: PathBuf = inner.directory.join("cd01");
    let worker = Arc::clone(&inner);
    let encoder = std::thread::spawn(move || worker.encode_part(0));
    wait_until("ffmpeg's start", || runner.current().is_some());

    // One chunk is under the limit; two fill the queue and ffmpeg is suspended.
    write_chunk(&directory, 0);
    wait_until("the first chunk", || queued(&inner) == [0]);
    assert_eq!(calls(&runner), Vec::<&str>::new());
    write_chunk(&directory, 1);
    wait_until("the suspend", || calls(&runner) == ["suspend"]);
    assert_eq!(queued(&inner), [0, 1]);

    // Uploaded down to half of the limit is not enough; below it, ffmpeg goes on.
    lock(&inner.queue).pop_front();
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(calls(&runner), ["suspend"]);
    lock(&inner.queue).pop_front();
    wait_until("the resume", || calls(&runner) == ["suspend", "resume"]);

    // Full again, then a stop: resumed to finish the chunk under way, then killed once it is
    // listed, and not before.
    write_chunk(&directory, 2);
    write_chunk(&directory, 3);
    wait_until("the second suspend", || calls(&runner).len() == 3);
    inner.wind_down.store(true, Ordering::SeqCst);
    wait_until("the resume at the stop", || calls(&runner).len() == 4);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(calls(&runner), ["suspend", "resume", "suspend", "resume"]);
    write_chunk(&directory, 4);
    encoder.join().expect("the encoder");
    assert_eq!(calls(&runner), ["suspend", "resume", "suspend", "resume", "kill"]);
    assert_eq!(queued(&inner), [2, 3, 4]);
    let part = lock(&inner.parts[0]);
    assert!(!part.encoded);
    assert_eq!((part.next_index, part.resume_at), (5, 10.0));
    assert_eq!(lock(&runner.started).len(), 1);
    assert!(!directory.join("list-00000.csv").exists());
}

#[test]
fn a_failed_ffmpeg_run_is_logged_and_started_again_after_a_rest() {
    let cache = tempfile::tempdir().expect("cache");
    let runner = Puppets::new(Some(1));
    let (run, log) = run(cache.path(), Arc::clone(&runner));
    let inner = Arc::clone(&run.inner);
    let worker = Arc::clone(&inner);
    let encoder = std::thread::spawn(move || worker.encode_part(0));
    wait_until("three starts", || lock(&runner.started).len() >= 3);
    inner.wind_down.store(true, Ordering::SeqCst);
    encoder.join().expect("the encoder");
    let failures: Vec<String> = log
        .events()
        .into_iter()
        .filter(|line| line.starts_with("cd1: ffmpeg 退出"))
        .collect();
    assert!(failures.len() >= 2, "{failures:?}");
    assert!(
        failures
            .iter()
            .all(|line| line == "cd1: ffmpeg 退出（1）：没有输出，从第 0 块重来"),
        "{failures:?}"
    );
    // The frame rate is probed once for the whole run.
    assert_eq!(runner.probes.load(Ordering::SeqCst), 1);
    assert!(!lock(&inner.parts[0]).encoded);
}
