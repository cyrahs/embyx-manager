//! The uploader's and the heartbeat's answers to the cluster: Python's `_uploader` and
//! `_heartbeat` branch by branch, with short waits.

use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use embyx_core::pyjson::{Dict, Value};

use super::{Assignment, Inner, RunParts, TaskRun, Timing, lock};
use crate::chunks::LocalChunk;
use crate::config::WorkerConfig;
use crate::console::MemoryLog;
use crate::server::{CallError, ServerError};
use crate::testing::{FakeClock, FakeCluster, ScriptedRunner, answer};

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

fn refused(status: u16, code: &str) -> CallError {
    let mut details = Dict::new();
    details.insert("code", Value::Str(code.to_owned()));
    details.insert("next_chunk", Value::Int(1));
    CallError::Server(ServerError {
        status,
        code: code.to_owned(),
        details,
    })
}

fn assignment() -> Assignment {
    let task = embyx_core::pyjson::loads(
        r#"{"id": 7, "avid": "SQTEVR-009", "video_bitrate": 1000000, "chunk_seconds": 2,
            "encoder": {"codec": "libx264"}, "paused": false,
            "parts": [{"index": 0, "name": "a-cd1.mp4", "size": 1, "duration": 10.0, "done": false,
                       "next_chunk": 0, "resume_at": 0.0},
                      {"index": 1, "name": "a-cd2.mp4", "size": 1, "duration": 10.0, "done": false,
                       "next_chunk": 0, "resume_at": 0.0}]}"#,
    )
    .expect("task");
    Assignment::from_dict(task.as_dict().expect("object")).expect("assignment")
}

fn run(cluster: FakeCluster, cache: &Path) -> (TaskRun, Arc<FakeCluster>, Arc<MemoryLog>) {
    let cluster = Arc::new(cluster);
    let log = Arc::new(MemoryLog::default());
    let mut config = WorkerConfig::new("http://cluster");
    config.cache_dir = cache.to_owned();
    config.upload_mbps = 0.0;
    let run = TaskRun::with_clock(
        RunParts {
            config,
            cluster: cluster.clone(),
            runner: Arc::new(ScriptedRunner::new(|argv| panic!("no process expected: {argv:?}"))),
            log: log.clone(),
            timing: fast(),
        },
        assignment(),
        Arc::new(FakeClock::at(0.0)),
    )
    .expect("run");
    (run, cluster, log)
}

/// A chunk file with its sidecar, queued.
fn queue(inner: &Inner, cache: &Path, part_index: i128, index: i128) -> LocalChunk {
    let path = cache.join(format!("p{part_index}-chunk-{index:05}.mkv"));
    std::fs::write(&path, b"abc").expect("chunk");
    let chunk = LocalChunk {
        part_index,
        index,
        start: 2.0 * f64::from(u8::try_from(index).expect("small")),
        duration: 2.0,
        frames: 60,
        bytes: 3,
        sha256: "0".repeat(64),
        path,
    };
    chunk.save().expect("sidecar");
    inner.enqueue(chunk.clone());
    chunk
}

fn wait_until(what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// The uploader on a thread of its own until `done`, then stopped as `run` stops it.
fn upload_until(inner: &Arc<Inner>, what: &str, done: impl Fn(&Inner) -> bool) {
    let worker = Arc::clone(inner);
    let thread = std::thread::spawn(move || worker.uploader());
    wait_until(what, || done(inner) || thread.is_finished());
    inner.stop_threads(vec![thread]);
}

#[test]
fn uploaded_chunks_leave_the_queue_and_the_cache() {
    let cache = tempfile::tempdir().expect("cache");
    let (run, cluster, _) = run(FakeCluster::default(), cache.path());
    let first = queue(&run.inner, cache.path(), 0, 0);
    let second = queue(&run.inner, cache.path(), 1, 0);
    upload_until(&run.inner, "the uploads", |inner| lock(&inner.queue).is_empty());
    assert_eq!(cluster.uploaded(), [first.clone(), second]);
    assert!(!first.path.exists() && !first.sidecar().exists());
    assert!(run.parts().iter().all(|part| part.uploads == 0 && part.error.is_none()));
}

#[test]
fn a_corrupt_upload_is_sent_again() {
    let cache = tempfile::tempdir().expect("cache");
    let tries = Arc::new(Mutex::new(0));
    let counted = Arc::clone(&tries);
    let cluster = FakeCluster {
        on_upload: Box::new(move |_| {
            let mut tries = counted.lock().expect("tries");
            *tries += 1;
            if *tries == 1 {
                Err(refused(400, "transcode_chunk_corrupt"))
            } else {
                Ok(answer(&[]))
            }
        }),
        ..FakeCluster::default()
    };
    let (run, cluster, log) = run(cluster, cache.path());
    queue(&run.inner, cache.path(), 0, 0);
    upload_until(&run.inner, "the second try", |inner| lock(&inner.queue).is_empty());
    assert_eq!(*tries.lock().expect("tries"), 2);
    assert_eq!(cluster.uploaded().len(), 1);
    assert!(log.events().is_empty(), "{:?}", log.events());
}

#[test]
fn a_refused_chunk_drops_the_parts_queue_for_a_resync() {
    let cache = tempfile::tempdir().expect("cache");
    let cluster = FakeCluster {
        on_upload: Box::new(|chunk| {
            if chunk.part_index == 0 {
                Err(refused(409, "transcode_chunk_out_of_order"))
            } else {
                Ok(answer(&[]))
            }
        }),
        ..FakeCluster::default()
    };
    let (run, cluster, log) = run(cluster, cache.path());
    let refused_chunk = queue(&run.inner, cache.path(), 0, 0);
    let dropped = queue(&run.inner, cache.path(), 0, 1);
    let other = queue(&run.inner, cache.path(), 1, 0);
    upload_until(&run.inner, "the queue to empty", |inner| lock(&inner.queue).is_empty());
    assert_eq!(cluster.uploaded(), [other]);
    let parts = run.parts();
    assert_eq!(parts[0].error.as_deref(), Some("transcode_chunk_out_of_order"));
    assert!(parts[1].error.is_none());
    assert!(!refused_chunk.path.exists());
    // Python removes only the refused chunk; the rest of the part's queue is left in the
    // cache for `kept_chunks` to sort out on the next run.
    assert!(dropped.path.exists());
    assert_eq!(
        log.events(),
        [
            "cd1 第 0 块被拒：transcode_chunk_out_of_order {'code': 'transcode_chunk_out_of_order', 'next_chunk': 1}，重新同步"
        ]
    );
}

#[test]
fn a_task_no_longer_active_stops_the_run() {
    for code in ["transcode_task_not_active", "transcode_task_not_found"] {
        let cache = tempfile::tempdir().expect("cache");
        let cluster = FakeCluster {
            on_upload: Box::new(move |_| Err(refused(409, code))),
            ..FakeCluster::default()
        };
        let (run, _, log) = run(cluster, cache.path());
        queue(&run.inner, cache.path(), 0, 0);
        upload_until(&run.inner, "the drop", |inner| inner.abort.is_set());
        assert!(run.inner.dropped.load(Ordering::SeqCst));
        assert_eq!(
            log.events(),
            ["集群那边这部作品已经不在转码了（取消或改用了 CPU），停下这部"]
        );
        assert_eq!(lock(&run.inner.queue).len(), 1, "the chunk stays for the next run");
    }
}

#[test]
fn a_failed_upload_waits_and_tries_again() {
    let cache = tempfile::tempdir().expect("cache");
    let tries = Arc::new(Mutex::new(0));
    let counted = Arc::clone(&tries);
    let cluster = FakeCluster {
        on_upload: Box::new(move |_| {
            let mut tries = counted.lock().expect("tries");
            *tries += 1;
            if *tries <= 2 {
                Err(CallError::Transport("connection reset".to_owned()))
            } else {
                Ok(answer(&[]))
            }
        }),
        ..FakeCluster::default()
    };
    let (run, cluster, log) = run(cluster, cache.path());
    queue(&run.inner, cache.path(), 0, 0);
    upload_until(&run.inner, "the third try", |inner| lock(&inner.queue).is_empty());
    assert_eq!(cluster.uploaded().len(), 1);
    assert_eq!(
        log.events(),
        [
            "上传失败，0 秒后重试：connection reset",
            "上传失败，0 秒后重试：connection reset"
        ]
    );
    assert_eq!(run.parts()[0].uploads, 0);
}

fn beat_until(inner: &Arc<Inner>, what: &str, done: impl Fn(&Inner) -> bool) {
    let worker = Arc::clone(inner);
    let thread = std::thread::spawn(move || worker.heartbeat());
    wait_until(what, || done(inner) || thread.is_finished());
    inner.stop_threads(vec![thread]);
}

#[test]
fn the_heartbeat_follows_pauses_and_reports_the_state() {
    let cache = tempfile::tempdir().expect("cache");
    let beats = Arc::new(Mutex::new(0_usize));
    let counted = Arc::clone(&beats);
    let cluster = FakeCluster {
        on_heartbeat: Box::new(move |_| {
            let mut beats = counted.lock().expect("beats");
            *beats += 1;
            let paused = (2..4).contains(&*beats);
            Ok(answer(&[
                ("active", Value::Bool(true)),
                ("paused", Value::Bool(paused)),
            ]))
        }),
        ..FakeCluster::default()
    };
    let (run, cluster, log) = run(cluster, cache.path());
    beat_until(&run.inner, "five beats", |_| *beats.lock().expect("beats") >= 5);
    assert_eq!(
        log.events(),
        ["网页上点了暂停：写完当前小块后暂停编码", "网页上点了继续"]
    );
    let states: Vec<String> = cluster
        .heartbeats
        .lock()
        .expect("heartbeats")
        .iter()
        .take(5)
        .map(|report| report.get("state").and_then(Value::as_str).expect("state").to_owned())
        .collect();
    assert_eq!(states, ["encoding", "encoding", "paused", "paused", "encoding"]);
    assert!(!run.inner.dropped.load(Ordering::SeqCst));
}

#[test]
fn a_heartbeat_that_says_inactive_stops_the_run_and_failures_are_ignored() {
    let cache = tempfile::tempdir().expect("cache");
    let beats = Arc::new(Mutex::new(0_usize));
    let counted = Arc::clone(&beats);
    let cluster = FakeCluster {
        on_heartbeat: Box::new(move |_| {
            let mut beats = counted.lock().expect("beats");
            *beats += 1;
            match *beats {
                1 => Err(CallError::Transport("timed out".to_owned())),
                2 => Err(refused(502, "Bad Gateway")),
                _ => Ok(answer(&[("active", Value::Bool(false))])),
            }
        }),
        ..FakeCluster::default()
    };
    let (run, _, log) = run(cluster, cache.path());
    run.inner.wind_down.store(true, Ordering::SeqCst);
    beat_until(&run.inner, "the drop", |inner| inner.abort.is_set());
    assert!(run.inner.dropped.load(Ordering::SeqCst));
    assert_eq!(*beats.lock().expect("beats"), 3);
    assert_eq!(
        log.events(),
        ["集群那边这部作品已经不在转码了（取消或改用了 CPU），停下这部"]
    );
}
