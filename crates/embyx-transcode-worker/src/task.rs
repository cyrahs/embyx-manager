//! One task: encode its parts with ffmpeg, upload the chunks, report the parts finished.
//!
//! Python origin: `RunClock`, `PartState` and `TaskRun` of
//! `src/embyx_manager/merge/home_worker.py`. The thread layout is Python's: the run's own
//! loop (start encoders, report finished parts, print the status), one encoder thread per
//! part being encoded (each with a progress reader and a stderr drain), one uploader and one
//! heartbeat thread. Shared state sits behind short locks where Python relies on the GIL.

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead as _, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use embyx_core::pyjson::{Dict, Value};

use crate::chunks::{self, Clock, LocalChunk, RateLimiter, RateMeter, SystemClock};
use crate::config::WorkerConfig;
use crate::console::Log;
use crate::ffmpeg::{self, EncodeRun, ProbeError};
use crate::process::{Runner, Running};
use crate::py::{self, PyError};
use crate::server::{CallError, Cluster};

#[cfg(test)]
mod tests;
#[cfg(test)]
mod thread_tests;

/// `VERSION`: the protocol version the worker reports.
pub const VERSION: &str = "1";

/// The waits of a run. `Timing::default()` is Python's; tests shorten them.
#[derive(Debug, Clone)]
pub struct Timing {
    /// `time.sleep(0.25)` of the run's loop.
    pub tick: Duration,
    /// `time.sleep(0.5)` of an encoder's loop and of `_rest`.
    pub encoder_tick: Duration,
    /// `HEARTBEAT_SECONDS`.
    pub heartbeat: Duration,
    /// `STATUS_SECONDS`.
    pub status: Duration,
    /// `RETRY_DELAYS`: backoff after a failed request or a crashed ffmpeg, doubling up to
    /// the last.
    pub retry_delays: Vec<Duration>,
    /// `thread.join(timeout=5)`.
    pub join: Duration,
    /// `IDLE_POLL_SECONDS`, and the 10 s waits of the main loop.
    pub idle_poll: Duration,
    pub short_poll: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            tick: Duration::from_millis(250),
            encoder_tick: Duration::from_millis(500),
            heartbeat: Duration::from_secs(15),
            status: Duration::from_secs(2),
            retry_delays: [5, 10, 20, 40, 80, 160, 300].map(Duration::from_secs).to_vec(),
            join: Duration::from_secs(5),
            idle_poll: Duration::from_secs(30),
            short_poll: Duration::from_secs(10),
        }
    }
}

impl Timing {
    /// `RETRY_DELAYS[min(failures, len(RETRY_DELAYS)) - 1]`.
    pub fn retry_delay(&self, failures: usize) -> Duration {
        let at = failures.min(self.retry_delays.len()).saturating_sub(1);
        self.retry_delays.get(at).copied().unwrap_or_default()
    }
}

/// One part of an assignment, as the cluster describes it.
#[derive(Debug, Clone)]
pub struct AssignedPart {
    pub index: i128,
    pub name: String,
    pub duration: f64,
    pub next_chunk: i128,
    pub resume_at: f64,
    pub done: bool,
}

/// The `task` of a claim's answer, read with Python's conversions.
#[derive(Debug, Clone)]
pub struct Assignment {
    pub id: i128,
    pub avid: String,
    /// `int(task['video_bitrate'])`.
    pub video_bits: i128,
    /// `task['video_bitrate'] / 1e6`, for the log.
    pub video_mbps: f64,
    pub chunk_seconds: i128,
    pub encoder: Dict,
    pub paused: bool,
    pub parts: Vec<AssignedPart>,
}

fn field<'a>(dict: &'a Dict, key: &str) -> Result<&'a Value, PyError> {
    dict.get(key).ok_or_else(|| PyError::key(key))
}

impl Assignment {
    /// The fields `main` and `TaskRun` read from `task`. Python reads them lazily and a bad
    /// one ends a thread or the worker with a traceback; here a bad one refuses the whole
    /// assignment up front (D169).
    pub fn from_dict(task: &Dict) -> Result<Self, PyError> {
        let video = field(task, "video_bitrate")?;
        let video_mbps = match video {
            #[allow(clippy::cast_precision_loss, reason = "Python's int / float rounds the same way")]
            Value::Int(number) => *number as f64 / 1e6,
            Value::Float(number) => number / 1e6,
            Value::Bool(flag) => f64::from(u8::from(*flag)) / 1e6,
            other => {
                return Err(PyError::new(
                    "TypeError",
                    format!(
                        "unsupported operand type(s) for /: '{}' and 'float'",
                        py::type_name(other)
                    ),
                ));
            }
        };
        let encoder = match field(task, "encoder")? {
            Value::Dict(dict) => dict.clone(),
            other => {
                return Err(PyError::new(
                    "TypeError",
                    format!("'{}' object is not a mapping", py::type_name(other)),
                ));
            }
        };
        let Value::List(parts) = field(task, "parts")? else {
            return Err(PyError::new("TypeError", "'parts' is not a list"));
        };
        let parts = parts
            .iter()
            .map(|part| {
                let Value::Dict(part) = part else {
                    return Err(PyError::new(
                        "TypeError",
                        format!("'{}' object is not subscriptable", py::type_name(part)),
                    ));
                };
                Ok(AssignedPart {
                    index: py::int(field(part, "index")?)?,
                    name: py::str_of(field(part, "name")?),
                    duration: py::float(field(part, "duration")?)?,
                    next_chunk: py::int(field(part, "next_chunk")?)?,
                    resume_at: py::float(field(part, "resume_at")?)?,
                    done: py::truthy(field(part, "done")?),
                })
            })
            .collect::<Result<Vec<_>, PyError>>()?;
        Ok(Self {
            id: py::int(field(task, "id")?)?,
            avid: py::str_of(field(task, "avid")?),
            video_bits: py::int(video)?,
            video_mbps,
            chunk_seconds: py::int(field(task, "chunk_seconds")?)?,
            encoder,
            paused: task.get("paused").is_some_and(py::truthy),
            parts,
        })
    }
}

/// `RunClock`: maps one ffmpeg run's segment-list times onto times in the part.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunClock {
    /// Where in the part the run's first frame is.
    pub anchor: f64,
    /// The run's first presentation time, read from its first chunk.
    pub shift: Option<f64>,
}

impl RunClock {
    /// Where a listed chunk starts in the part: `anchor + max(start - shift, 0)`, snapped to
    /// `resume_at` when within 0.25 s of it.
    pub fn begin(&self, shift: f64, start: f64, resume_at: f64) -> f64 {
        let begin = self.anchor + py::max(start - shift, 0.0);
        if (begin - resume_at).abs() <= 0.25 {
            resume_at
        } else {
            begin
        }
    }

    /// Where a listed chunk ends in the part: `anchor + (end - shift)`.
    pub fn end(&self, shift: f64, end: f64) -> f64 {
        self.anchor + (end - shift)
    }
}

/// `PartState`.
#[derive(Debug, Clone)]
pub struct PartState {
    pub index: i128,
    pub name: String,
    pub duration: f64,
    pub next_index: i128,
    pub resume_at: f64,
    pub fps: f64,
    pub position: f64,
    /// ffmpeg wrote the part to its end.
    pub encoded: bool,
    /// The cluster has every chunk and took the part as finished.
    pub finished: bool,
    pub error: Option<String>,
    pub uploads: i64,
}

/// How a run ended (`run()`'s return value).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Done,
    Stopped,
    Dropped,
    Resync,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Done => "done",
            Self::Stopped => "stopped",
            Self::Dropped => "dropped",
            Self::Resync => "resync",
        }
    }
}

/// A run that could not start: `self.directory.mkdir(...)` failed, say.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{}: {}", .0.class, .0.message)]
    Py(#[from] PyError),
}

/// `threading.Event`.
#[derive(Debug, Default)]
struct Event {
    flag: Mutex<bool>,
    changed: Condvar,
}

impl Event {
    fn set(&self) {
        *self.flag.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.changed.notify_all();
    }

    fn is_set(&self) -> bool {
        *self.flag.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `event.wait(timeout)`: the flag, once set or once the time is up.
    fn wait(&self, timeout: Duration) -> bool {
        let flag = self.flag.lock().unwrap_or_else(PoisonError::into_inner);
        let (flag, _) = self
            .changed
            .wait_timeout_while(flag, timeout, |flag| !*flag)
            .unwrap_or_else(PoisonError::into_inner);
        *flag
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What a `TaskRun` is made from.
#[derive(Clone)]
pub struct RunParts {
    pub config: WorkerConfig,
    pub cluster: Arc<dyn Cluster>,
    pub runner: Arc<dyn Runner>,
    pub log: Arc<dyn Log>,
    pub timing: Timing,
}

impl std::fmt::Debug for RunParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunParts")
            .field("config", &self.config)
            .field("timing", &self.timing)
            .finish_non_exhaustive()
    }
}

struct Inner {
    config: WorkerConfig,
    cluster: Arc<dyn Cluster>,
    runner: Arc<dyn Runner>,
    log: Arc<dyn Log>,
    timing: Timing,
    task: Assignment,
    task_id: i128,
    encoder: Dict,
    directory: PathBuf,
    parts: Vec<Arc<Mutex<PartState>>>,
    queue: Mutex<VecDeque<LocalChunk>>,
    queue_changed: Condvar,
    limiter: RateLimiter,
    meter: RateMeter,
    max_queue_bytes: i128,
    /// Finish the current chunk, then stop encoding (Ctrl+C once, or a pause).
    wind_down: AtomicBool,
    /// Stop everything now (Ctrl+C twice, the task taken away, the cluster out of step).
    abort: Event,
    paused: AtomicBool,
    dropped: AtomicBool,
    fps: Mutex<Option<f64>>,
    /// The ffmpeg of each part being encoded, by the part's position.
    processes: Mutex<HashMap<usize, Arc<dyn Running>>>,
}

/// `TaskRun`: encodes and uploads one task until it is done, the worker stops, or the
/// cluster drops it.
pub struct TaskRun {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for TaskRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRun")
            .field("task_id", &self.inner.task_id)
            .finish_non_exhaustive()
    }
}

/// `cd{n}` of a part: one-based.
fn label(index: i128) -> String {
    format!("cd{}", index + 1)
}

/// `cd{n:02d}`: the part's folder.
fn folder(index: i128) -> String {
    format!("cd{:02}", index + 1)
}

/// `shutil.rmtree(path, ignore_errors=True)`.
fn rmtree(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
}

impl TaskRun {
    pub fn new(parts: RunParts, task: Assignment) -> Result<Self, PyError> {
        Self::with_clock(parts, task, Arc::new(SystemClock::default()))
    }

    pub fn with_clock(parts: RunParts, task: Assignment, clock: Arc<dyn Clock>) -> Result<Self, PyError> {
        let RunParts {
            config,
            cluster,
            runner,
            log,
            timing,
        } = parts;
        let mut encoder = task.encoder.clone();
        for (key, value) in config.encoder.iter() {
            encoder.insert(key.clone(), value.clone());
        }
        let directory = config.cache_dir.join(task.id.to_string());
        let states = task
            .parts
            .iter()
            .map(|part| {
                Arc::new(Mutex::new(PartState {
                    index: part.index,
                    name: part.name.clone(),
                    duration: part.duration,
                    next_index: part.next_chunk,
                    resume_at: part.resume_at,
                    fps: 0.0,
                    position: 0.0,
                    encoded: false,
                    finished: part.done,
                    error: None,
                    uploads: 0,
                }))
            })
            .collect();
        let max_queue_bytes = py::int_of_float(config.max_queue_gb * 1024_f64.powi(3))?;
        Ok(Self {
            inner: Arc::new(Inner {
                limiter: RateLimiter::new(config.upload_mbps * 1_000_000.0, Arc::clone(&clock)),
                meter: RateMeter::new(clock),
                task_id: task.id,
                paused: AtomicBool::new(task.paused),
                directory,
                encoder,
                parts: states,
                queue: Mutex::new(VecDeque::new()),
                queue_changed: Condvar::new(),
                max_queue_bytes,
                wind_down: AtomicBool::new(false),
                abort: Event::default(),
                dropped: AtomicBool::new(false),
                fps: Mutex::new(None),
                processes: Mutex::new(HashMap::new()),
                config,
                cluster,
                runner,
                log,
                timing,
                task,
            }),
        })
    }

    /// The parts' states now, for tests.
    pub fn parts(&self) -> Vec<PartState> {
        self.inner.parts.iter().map(|part| lock(part).clone()).collect()
    }

    /// `report(state)`: what a heartbeat sends.
    pub fn report(&self, state: &str) -> Value {
        self.inner.report(state)
    }

    /// `run(stop_requested)`: work the task. `stop_requested` gives the Ctrl+C presses so far.
    pub fn run(&self, stop_requested: &(dyn Fn() -> usize + Sync)) -> Result<Outcome, RunError> {
        let inner = &self.inner;
        std::fs::create_dir_all(&inner.directory)?;
        for part in &inner.parts {
            let mut part = lock(part);
            let part_dir = inner.directory.join(folder(part.index));
            if part.finished {
                rmtree(&part_dir);
                continue;
            }
            for chunk in chunks::kept_chunks(&part_dir, part.next_index, part.resume_at) {
                part.next_index = chunk.index + 1;
                part.resume_at = chunk.end();
                inner.enqueue(chunk);
            }
            if part.resume_at >= part.duration - 0.5 {
                part.encoded = true;
            }
        }
        let threads = vec![
            spawn("uploader", Arc::clone(inner), Inner::uploader),
            spawn("heartbeat", Arc::clone(inner), Inner::heartbeat),
        ];
        let mut encoders: HashMap<usize, JoinHandle<()>> = HashMap::new();
        let mut last_status: Option<Instant> = None;
        loop {
            let presses = stop_requested();
            let paused = inner.paused.load(Ordering::SeqCst);
            if presses >= 2 {
                inner.abort.set();
            } else if presses == 1 || paused {
                inner.wind_down.store(true, Ordering::SeqCst);
            } else if inner.wind_down.load(Ordering::SeqCst) && !paused {
                inner.wind_down.store(false, Ordering::SeqCst);
            }
            if inner.abort.is_set() {
                inner.kill_all();
                break;
            }
            encoders.retain(|_, thread| !thread.is_finished());
            if !inner.wind_down.load(Ordering::SeqCst) {
                for (at, part) in inner.parts.iter().enumerate() {
                    if i128::try_from(encoders.len()).unwrap_or(i128::MAX) >= inner.config.jobs {
                        break;
                    }
                    {
                        let part = lock(part);
                        if part.encoded || part.finished || part.error.is_some() || encoders.contains_key(&at) {
                            continue;
                        }
                    }
                    let name = label(lock(part).index);
                    let encoder = Arc::clone(inner);
                    let handle = thread::Builder::new()
                        .name(name)
                        .spawn(move || encoder.encode_part(at))
                        .map_err(RunError::Io)?;
                    encoders.insert(at, handle);
                }
            }
            inner.finish_parts();
            if inner.parts.iter().all(|part| lock(part).finished) {
                break;
            }
            let queue_empty = lock(&inner.queue).is_empty();
            let idle = encoders.is_empty() && queue_empty;
            if inner.wind_down.load(Ordering::SeqCst) && idle && (presses > 0 || inner.dropped.load(Ordering::SeqCst)) {
                break;
            }
            if encoders.is_empty()
                && queue_empty
                && inner.parts.iter().all(|part| {
                    let part = lock(part);
                    part.finished || part.error.is_some()
                })
            {
                break;
            }
            if last_status.is_none_or(|at| at.elapsed() >= inner.timing.status) {
                last_status = Some(Instant::now());
                inner.log_status();
            }
            thread::sleep(inner.timing.tick);
        }
        for thread in encoders.into_values() {
            join_timeout(thread, inner.timing.join);
        }
        inner.stop_threads(threads);
        if inner.dropped.load(Ordering::SeqCst) {
            return Ok(Outcome::Dropped);
        }
        if inner.parts.iter().all(|part| lock(part).finished) {
            rmtree(&inner.directory);
            return Ok(Outcome::Done);
        }
        if inner.parts.iter().any(|part| lock(part).error.is_some()) && stop_requested() == 0 {
            return Ok(Outcome::Resync);
        }
        Ok(Outcome::Stopped)
    }
}

fn spawn(name: &str, inner: Arc<Inner>, body: fn(&Inner)) -> JoinHandle<()> {
    let fallback = Arc::clone(&inner);
    thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || body(&inner))
        .unwrap_or_else(|_| thread::spawn(move || body(&fallback)))
}

/// `thread.join(timeout=...)`: waits up to `timeout`; a thread still running is left to end
/// on its own, as Python leaves a daemon thread.
fn join_timeout(handle: JoinHandle<()>, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !handle.is_finished() {
        if Instant::now() >= deadline {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    let _ = handle.join();
}

/// Lines of a text pipe as Python's universal-newline reading gives them, bytes that are
/// not UTF-8 replaced (`errors='replace'`).
fn read_lines(reader: impl Read, mut each: impl FnMut(&str)) {
    let mut reader = BufReader::new(reader);
    let mut line = Vec::new();
    while let Ok(buffer) = reader.fill_buf() {
        if buffer.is_empty() {
            if !line.is_empty() {
                each(&String::from_utf8_lossy(&line));
            }
            break;
        }
        if let Some(end) = buffer.iter().position(|&byte| byte == b'\n' || byte == b'\r') {
            let carriage = buffer[end] == b'\r';
            line.extend_from_slice(&buffer[..end]);
            reader.consume(end + 1);
            if carriage && reader.fill_buf().ok().and_then(|rest| rest.first().copied()) == Some(b'\n') {
                reader.consume(1);
            }
            line.push(b'\n');
            each(&String::from_utf8_lossy(&line));
            line.clear();
        } else {
            let length = buffer.len();
            line.extend_from_slice(buffer);
            reader.consume(length);
        }
    }
}

/// Python's CSV reader (the default dialect) for the segment list's lines.
fn csv_row(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut chars = line.chars().peekable();
    let mut at_start = true;
    while let Some(ch) = chars.next() {
        if quoted {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    field.push('"');
                    chars.next();
                } else {
                    quoted = false;
                }
            } else {
                field.push(ch);
            }
        } else if ch == '"' && at_start {
            quoted = true;
            at_start = false;
        } else if ch == ',' {
            fields.push(std::mem::take(&mut field));
            at_start = true;
        } else {
            field.push(ch);
            at_start = false;
        }
    }
    if !line.is_empty() {
        fields.push(field);
    }
    fields
}

/// Why picking up a listed chunk failed in a way Python does not catch (a listing ffmpeg
/// did not write, a sidecar that could not be saved).
#[derive(Debug)]
struct CollectError(String);

impl Inner {
    fn enqueue(&self, chunk: LocalChunk) {
        lock(&self.queue).push_back(chunk);
        self.queue_changed.notify_all();
    }

    fn queued_bytes(&self) -> i128 {
        lock(&self.queue).iter().map(|chunk| chunk.bytes).sum()
    }

    fn report(&self, state: &str) -> Value {
        let (chunks, size) = {
            let queue = lock(&self.queue);
            (queue.len(), queue.iter().map(|chunk| chunk.bytes).sum::<i128>())
        };
        let active: Vec<usize> = {
            let processes = lock(&self.processes);
            (0..self.parts.len()).filter(|at| processes.contains_key(at)).collect()
        };
        let parts = active
            .into_iter()
            .map(|at| {
                let part = lock(&self.parts[at]);
                let mut item = Dict::new();
                item.insert("index", Value::Int(part.index));
                item.insert("fps", Value::Float(part.fps));
                item.insert("position", Value::Float(part.position));
                Value::Dict(item)
            })
            .collect();
        let mut report = Dict::new();
        report.insert("name", Value::Str(self.config.name.clone()));
        report.insert("version", Value::Str(VERSION.to_owned()));
        report.insert("task_id", Value::Int(self.task_id));
        report.insert("state", Value::Str(state.to_owned()));
        report.insert("parts", Value::List(parts));
        report.insert("upload_bps", Value::Float(self.meter.rate() * 8.0));
        report.insert("queue_chunks", Value::Int(i128::try_from(chunks).unwrap_or(i128::MAX)));
        report.insert("queue_bytes", Value::Int(size));
        Value::Dict(report)
    }

    fn stop_threads(&self, threads: Vec<JoinHandle<()>>) {
        self.abort.set();
        {
            let _queue = lock(&self.queue);
            self.queue_changed.notify_all();
        }
        for thread in threads {
            join_timeout(thread, self.timing.join);
        }
    }

    fn kill_all(&self) {
        for process in lock(&self.processes).values() {
            let _ = process.resume();
            let _ = process.kill();
        }
    }

    fn finish_parts(&self) {
        for (at, part) in self.parts.iter().enumerate() {
            let index = {
                let part = lock(part);
                if !part.encoded || part.finished || part.error.is_some() {
                    continue;
                }
                let waiting = lock(&self.queue).iter().any(|chunk| chunk.part_index == part.index) || part.uploads != 0;
                if waiting {
                    continue;
                }
                part.index
            };
            match self.cluster.finish(self.task_id, index) {
                Err(CallError::Server(error)) => {
                    let message = format!("集群不收这一盘：{} {}", error.code, error.details_repr());
                    self.log.event(&format!("{}: {message}", label(index)));
                    lock(&self.parts[at]).error = Some(message);
                }
                Err(error) => {
                    self.log
                        .event(&format!("{}: 报告完成失败，稍后重试：{error}", label(index)));
                }
                Ok(_) => {
                    lock(&self.parts[at]).finished = true;
                    rmtree(&self.directory.join(folder(index)));
                    self.log.event(&format!("{} 转完并已全部传到集群", label(index)));
                }
            }
        }
    }

    fn frame_rate(&self) -> Result<f64, ProbeError> {
        if let Some(fps) = *lock(&self.fps) {
            return Ok(fps);
        }
        let first = lock(&self.parts[0]).index;
        let fps = ffmpeg::probe_fps(
            self.runner.as_ref(),
            &self.cluster.part_url(self.task_id, first),
            &self.config,
        )?;
        *lock(&self.fps) = Some(fps);
        Ok(fps)
    }

    /// `_rest(failures)`: wait before another try; true when the task is being stopped
    /// meanwhile.
    fn rest(&self, failures: usize) -> bool {
        let deadline = Instant::now() + self.timing.retry_delay(failures);
        while Instant::now() < deadline {
            if self.abort.is_set() || self.wind_down.load(Ordering::SeqCst) {
                return true;
            }
            thread::sleep(self.timing.encoder_tick);
        }
        false
    }

    fn stopping(&self) -> bool {
        self.abort.is_set() || self.wind_down.load(Ordering::SeqCst)
    }

    /// `_encode_part(part)`: one ffmpeg run after another until the part is encoded or the
    /// run is stopped.
    fn encode_part(&self, at: usize) {
        let index = lock(&self.parts[at]).index;
        let directory = self.directory.join(folder(index));
        let mut failures = 0_usize;
        if let Err(error) = std::fs::create_dir_all(&directory) {
            self.log.event(&format!("{}: {error}", label(index)));
            return;
        }
        while !self.stopping() && !lock(&self.parts[at]).encoded {
            let fps = match self.frame_rate() {
                Ok(fps) => fps,
                Err(ProbeError::Os(error)) => {
                    // Python's thread ends on this `OSError` and the run starts another; here
                    // it waits like any other failure (D169).
                    self.log.event(&format!("{}: {error}", label(index)));
                    failures += 1;
                    if self.rest(failures) {
                        return;
                    }
                    continue;
                }
                Err(error) => {
                    self.log.event(&format!("{}: 读不到帧率：{error}", label(index)));
                    failures += 1;
                    if self.rest(failures) {
                        return;
                    }
                    continue;
                }
            };
            match self.encode_once(at, &directory, fps) {
                EncodeEnd::Encoded => {
                    lock(&self.parts[at]).encoded = true;
                    return;
                }
                EncodeEnd::Stopped => return,
                EncodeEnd::Failed(message) => {
                    failures += 1;
                    self.log.event(&message);
                    if self.rest(failures) {
                        return;
                    }
                }
            }
        }
    }

    /// One ffmpeg run of `_encode_part`'s loop.
    #[allow(clippy::too_many_lines, reason = "one ffmpeg run, as Python's loop body")]
    fn encode_once(&self, at: usize, directory: &Path, fps: f64) -> EncodeEnd {
        let (index, resume_at, next_index) = {
            let part = lock(&self.parts[at]);
            (part.index, part.resume_at, part.next_index)
        };
        // Half a frame early, so the frame the last chunk stopped before is the first one decoded.
        let seek = if resume_at > 0.0 {
            py::max(resume_at - 0.5 / fps, 0.0)
        } else {
            0.0
        };
        let listing = directory.join(format!("list-{next_index:05}.csv"));
        let _ = chunks::remove_missing_ok(&listing);
        let url = self.cluster.part_url(self.task_id, index);
        let command = match ffmpeg::encode_command(
            &self.config,
            &EncodeRun {
                url: &url,
                encoder: &self.encoder,
                video_bits: self.task.video_bits,
                fps,
                seek,
                chunk_seconds: self.task.chunk_seconds,
                first_index: next_index,
                directory,
                listing: &listing,
            },
        ) {
            Ok(command) => command,
            Err(error) => return EncodeEnd::Failed(format!("{}: {}: {}", label(index), error.class, error.message)),
        };
        let process = match self.runner.spawn(&command) {
            Ok(process) => process,
            Err(error) => return EncodeEnd::Failed(format!("{}: {error}", label(index))),
        };
        lock(&self.processes).insert(at, Arc::clone(&process));
        let tail: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));
        let mut readers = Vec::new();
        if let Some(stdout) = process.take_stdout() {
            let progress = progress_sink(Arc::clone(&self.parts[at]), seek);
            readers.push(thread::spawn(move || read_lines(stdout, |line| progress(line))));
        }
        if let Some(stderr) = process.take_stderr() {
            let tail = Arc::clone(&tail);
            readers.push(thread::spawn(move || {
                read_lines(stderr, |line| {
                    if !pytext_strip(line).is_empty() {
                        let mut tail = lock(&tail);
                        if tail.len() == 10 {
                            tail.pop_front();
                        }
                        tail.push_back(line.trim_end().to_owned());
                    }
                });
            }));
        }
        let mut clock = RunClock {
            anchor: resume_at,
            shift: None,
        };
        let mut listed = 0_usize;
        let mut suspended = false;
        let mut stopping = false;
        let mut boundary = 0_usize;
        let mut broken: Option<String> = None;
        let code = loop {
            let code = process.poll().ok().flatten();
            match self.collect(at, &listing, &mut clock, listed) {
                Ok(count) => listed = count,
                Err(CollectError(message)) => {
                    // Python's thread dies here and leaves ffmpeg running; here ffmpeg is
                    // stopped and the part tried again (D169).
                    broken = Some(message);
                    let _ = process.resume();
                    let _ = process.kill();
                }
            }
            if let Some(code) = code {
                break code;
            }
            if broken.is_some() {
                thread::sleep(Duration::from_millis(20));
                continue;
            }
            if self.abort.is_set() {
                let _ = process.kill();
                thread::sleep(Duration::from_millis(20));
                continue;
            }
            if self.wind_down.load(Ordering::SeqCst) && !stopping {
                stopping = true;
                boundary = listed;
                if suspended {
                    let _ = process.resume();
                    suspended = false;
                }
            }
            if stopping && listed > boundary {
                // The chunk under way when the stop came is written: that is all that was owed.
                let _ = process.kill();
                thread::sleep(Duration::from_millis(20));
                continue;
            }
            let queued = self.queued_bytes();
            let full = queued >= self.max_queue_bytes;
            if full && !suspended && !stopping {
                let _ = process.suspend();
                suspended = true;
            } else if suspended && queued < self.max_queue_bytes / 2 {
                let _ = process.resume();
                suspended = false;
            }
            thread::sleep(self.timing.encoder_tick);
        };
        for reader in readers {
            join_timeout(reader, self.timing.join);
        }
        if broken.is_none()
            && let Err(CollectError(message)) = self.collect(at, &listing, &mut clock, listed)
        {
            broken = Some(message);
        }
        lock(&self.processes).remove(&at);
        lock(&self.parts[at]).fps = 0.0;
        // Whatever ffmpeg left past the last listed chunk is half written.
        if let Ok(entries) = std::fs::read_dir(directory) {
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with("chunk-")
                    && path.extension().is_some_and(|suffix| suffix == "mkv")
                    && !path.with_extension("json").exists()
                {
                    let _ = chunks::remove_missing_ok(&path);
                }
            }
        }
        let _ = chunks::remove_missing_ok(&listing);
        if let Some(message) = broken {
            return EncodeEnd::Failed(format!("{}: {message}", label(index)));
        }
        if code == 0 && !stopping && !self.abort.is_set() {
            return EncodeEnd::Encoded;
        }
        if stopping || self.abort.is_set() {
            return EncodeEnd::Stopped;
        }
        let tail = lock(&tail).iter().cloned().collect::<Vec<_>>().join(" | ");
        let tail = if tail.is_empty() {
            "没有输出".to_owned()
        } else {
            tail
        };
        let next_index = lock(&self.parts[at]).next_index;
        EncodeEnd::Failed(format!(
            "{}: ffmpeg 退出（{code}）：{tail}，从第 {next_index} 块重来",
            label(index)
        ))
    }

    /// `_collect(part, listing, clock, seen)`: pick up the chunks ffmpeg listed since
    /// `seen`; returns how many it has listed.
    fn collect(&self, at: usize, listing: &Path, clock: &mut RunClock, seen: usize) -> Result<usize, CollectError> {
        let Ok(bytes) = std::fs::read(listing) else {
            return Ok(seen);
        };
        let text = String::from_utf8(bytes).map_err(|error| CollectError(format!("UnicodeDecodeError: {error}")))?;
        let rows: Vec<Vec<String>> = ffmpeg::splitlines(&text).into_iter().map(csv_row).collect();
        let mut seen = seen;
        for row in rows.iter().skip(seen) {
            if row.len() < 3 {
                return Ok(seen);
            }
            let number = |text: &str| {
                py::float_from_str(text).ok_or_else(|| {
                    CollectError(format!(
                        "ValueError: could not convert string to float: {}",
                        embyx_core::pytext::py_repr(text)
                    ))
                })
            };
            let (name, start, end) = (&row[0], number(&row[1])?, number(&row[2])?);
            let file = Path::new(name).file_name().map(ToOwned::to_owned).unwrap_or_default();
            let path = listing.parent().unwrap_or(Path::new("")).join(&file);
            let stem = path
                .file_stem()
                .map(|stem| stem.to_string_lossy().into_owned())
                .unwrap_or_default();
            let suffix = stem.rsplit_once('-').map(|(_, suffix)| suffix.to_owned());
            let Some(suffix) = suffix else {
                return Err(CollectError("IndexError: list index out of range".to_owned()));
            };
            let index = embyx_core::pytext::py_int(&suffix)
                .map_err(|message| CollectError(format!("ValueError: {message}")))?;
            let (next_index, resume_at) = {
                let part = lock(&self.parts[at]);
                (part.next_index, part.resume_at)
            };
            if index != next_index {
                self.log.event(&format!(
                    "{}: 块编号对不上（{index} != {next_index}），丢弃这次输出",
                    label(lock(&self.parts[at]).index)
                ));
                return Ok(rows.len());
            }
            let Ok(chunk) = self.describe(at, &path, clock, (start, end), (index, resume_at)) else {
                // Not readable yet, or broken: look again next time; once ffmpeg is gone it is
                // left without a sidecar and thrown away, and the part goes on from here.
                return Ok(seen);
            };
            chunk
                .save()
                .map_err(|error| CollectError(format!("OSError: {error}")))?;
            {
                let mut part = lock(&self.parts[at]);
                part.next_index = index + 1;
                part.resume_at = chunk.end();
            }
            seen += 1;
            self.enqueue(chunk);
        }
        Ok(seen)
    }

    /// The `try` block of `_collect`: the chunk's times, frames, size and hash.
    fn describe(
        &self,
        at: usize,
        path: &Path,
        clock: &mut RunClock,
        (start, end): (f64, f64),
        (index, resume_at): (i128, f64),
    ) -> Result<LocalChunk, ()> {
        let ffprobe = &self.config.ffprobe;
        let shift = if let Some(shift) = clock.shift {
            shift
        } else {
            // `first_pts` raises only `RuntimeError` or `OSError`, both caught.
            let shift = ffmpeg::first_pts(self.runner.as_ref(), path, ffprobe).map_err(|_| ())?;
            clock.shift = Some(shift);
            shift
        };
        let begin = clock.begin(shift, start, resume_at);
        let frames = ffmpeg::count_frames(self.runner.as_ref(), path, ffprobe).map_err(|_| ())?;
        let bytes = std::fs::metadata(path).map_err(|_| ())?.len();
        let sha256 = chunks::sha256_of(path).map_err(|_| ())?;
        Ok(LocalChunk {
            part_index: lock(&self.parts[at]).index,
            index,
            start: begin,
            duration: clock.end(shift, end) - begin,
            frames,
            bytes: i128::from(bytes),
            sha256,
            path: path.to_path_buf(),
        })
    }

    fn part_at(&self, index: i128) -> Option<usize> {
        self.parts.iter().position(|part| lock(part).index == index)
    }

    /// `_uploader()`: one chunk at a time, oldest first.
    fn uploader(&self) {
        let mut failures = 0_usize;
        while !self.abort.is_set() {
            let chunk = {
                let mut queue = lock(&self.queue);
                while queue.is_empty() && !self.abort.is_set() {
                    queue = self
                        .queue_changed
                        .wait_timeout(queue, Duration::from_secs(1))
                        .unwrap_or_else(PoisonError::into_inner)
                        .0;
                }
                if self.abort.is_set() {
                    return;
                }
                queue[0].clone()
            };
            let Some(at) = self.part_at(chunk.part_index) else {
                // Python's `next(...)` raises and the uploader ends; a chunk of a part the task
                // does not have can only come from a sidecar the worker did not write.
                lock(&self.queue).retain(|item| item.path != chunk.path);
                let _ = chunk.remove();
                continue;
            };
            lock(&self.parts[at]).uploads += 1;
            let result = self.cluster.upload(self.task_id, &chunk, &self.limiter, &self.meter);
            match result {
                Ok(_) => {
                    failures = 0;
                    {
                        let mut queue = lock(&self.queue);
                        if queue.front().is_some_and(|front| front.path == chunk.path) {
                            queue.pop_front();
                        }
                        self.queue_changed.notify_all();
                    }
                    let _ = chunk.remove();
                    lock(&self.parts[at]).uploads -= 1;
                }
                Err(CallError::Server(error)) => {
                    lock(&self.parts[at]).uploads -= 1;
                    if matches!(
                        error.code.as_str(),
                        "transcode_task_not_active" | "transcode_task_not_found"
                    ) {
                        self.log
                            .event("集群那边这部作品已经不在转码了（取消或改用了 CPU），停下这部");
                        self.dropped.store(true, Ordering::SeqCst);
                        self.abort.set();
                        return;
                    }
                    if error.status == 400 {
                        // A corrupt upload: send it again.
                        continue;
                    }
                    self.log.event(&format!(
                        "{} 第 {} 块被拒：{} {}，重新同步",
                        label(chunk.part_index),
                        chunk.index,
                        error.code,
                        error.details_repr()
                    ));
                    lock(&self.parts[at]).error = Some(error.code.clone());
                    lock(&self.queue).retain(|item| item.part_index != chunk.part_index);
                    let _ = chunk.remove();
                }
                Err(error) => {
                    lock(&self.parts[at]).uploads -= 1;
                    failures += 1;
                    let delay = self.timing.retry_delay(failures);
                    self.log.event(&format!(
                        "上传失败，{} 秒后重试：{error}",
                        py::fixed(delay.as_secs_f64(), 0)
                    ));
                    self.abort.wait(delay);
                }
            }
        }
    }

    /// `_heartbeat()`.
    fn heartbeat(&self) {
        while !self.abort.wait(self.timing.heartbeat) {
            let state = if self.paused.load(Ordering::SeqCst) {
                "paused"
            } else if self.wind_down.load(Ordering::SeqCst) {
                "stopping"
            } else {
                "encoding"
            };
            let Ok(answer) = self.cluster.heartbeat(&self.report(state)) else {
                continue;
            };
            if !answer.get("active").is_some_and(py::truthy) {
                self.log
                    .event("集群那边这部作品已经不在转码了（取消或改用了 CPU），停下这部");
                self.dropped.store(true, Ordering::SeqCst);
                self.abort.set();
                return;
            }
            let paused = answer.get("paused").is_some_and(py::truthy);
            if paused != self.paused.load(Ordering::SeqCst) {
                self.log.event(if paused {
                    "网页上点了暂停：写完当前小块后暂停编码"
                } else {
                    "网页上点了继续"
                });
            }
            self.paused.store(paused, Ordering::SeqCst);
        }
    }

    /// `log_status()`: the status line.
    fn log_status(&self) {
        self.log.status(&self.status_line());
    }

    fn status_line(&self) -> String {
        let active: Vec<usize> = {
            let processes = lock(&self.processes);
            (0..self.parts.len()).filter(|at| processes.contains_key(at)).collect()
        };
        let parts: Vec<PartState> = self.parts.iter().map(|part| lock(part).clone()).collect();
        let pieces: Vec<String> = active
            .iter()
            .map(|&at| &parts[at])
            .filter(|part| part.duration != 0.0)
            .map(|part| {
                format!(
                    "{} {} {}fps",
                    label(part.index),
                    py::percent(part.position / part.duration, 0),
                    py::fixed(part.fps, 0)
                )
            })
            .collect();
        let done = parts.iter().filter(|part| part.finished).count();
        let total = py::sum(parts.iter().map(|part| part.duration));
        let total = if total == 0.0 { 1.0 } else { total };
        let encoded = py::sum(parts.iter().map(|part| py::min(part.resume_at, part.duration)));
        let (chunks, size) = {
            let queue = lock(&self.queue);
            (queue.len(), queue.iter().map(|chunk| chunk.bytes).sum::<i128>())
        };
        let state = if self.paused.load(Ordering::SeqCst) {
            "已暂停 | "
        } else if self.wind_down.load(Ordering::SeqCst) {
            "收尾中（再按 Ctrl+C 立即退出）| "
        } else {
            ""
        };
        let pieces = if pieces.is_empty() {
            "没有在编码".to_owned()
        } else {
            pieces.join(" | ")
        };
        #[allow(clippy::cast_precision_loss, reason = "Python's int / float rounds the same way")]
        let gigabytes = size as f64 / 1024_f64.powi(3);
        format!(
            "[{}] {state}{pieces} | 完成 {done}/{} 盘 | 上传 {} Mbps，队列 {chunks} 块 {} GB | 总进度 {}",
            self.task.avid,
            parts.len(),
            py::fixed(self.meter.rate() * 8.0 / 1e6, 1),
            py::fixed(gigabytes, 2),
            py::percent(encoded / total, 1)
        )
    }
}

/// `line.strip()`.
fn pytext_strip(line: &str) -> &str {
    embyx_core::pytext::py_strip(line)
}

/// How one ffmpeg run of a part ended.
enum EncodeEnd {
    /// It wrote the part to its end.
    Encoded,
    /// The run was stopped (wind-down or abort).
    Stopped,
    /// It failed; the message is logged and the part tried again after a rest.
    Failed(String),
}

/// `_read_progress`: `fps=` and `out_time_us=` lines move the part's figures.
fn progress_sink(part: Arc<Mutex<PartState>>, seek: f64) -> impl Fn(&str) + Send + 'static {
    move |line: &str| {
        let stripped = embyx_core::pytext::py_strip(line);
        let (key, value) = stripped.split_once('=').unwrap_or((stripped, ""));
        if key == "fps" {
            if let Some(fps) = py::float_from_str(value) {
                lock(&part).fps = fps;
            }
        } else if key == "out_time_us"
            && embyx_core::pytext::py_isdigit(value)
            && let Some(micros) = embyx_core::pytext::py_int_decimal(value)
        {
            #[allow(clippy::cast_precision_loss, reason = "Python's int / int rounds the same way")]
            let position = seek + micros as f64 / 1_000_000.0;
            lock(&part).position = position;
        }
    }
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("task_id", &self.task_id)
            .finish_non_exhaustive()
    }
}
