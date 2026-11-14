//! Stand-ins for the tests: a small HTTP/1.1 server that records what it is sent and answers
//! as told, a runner with canned ffprobe answers, a process that does nothing, a clock that
//! only moves when told to, a cluster that answers as told.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::io::{self, BufRead as _, BufReader, Read, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::Duration;

use embyx_core::pyjson::{Dict, Value};

use crate::chunks::{Clock, LocalChunk, RateLimiter, RateMeter};
use crate::process::{Captured, RunError, Runner, Running};
use crate::server::{CallError, Cluster};

/// A request the fake server received.
#[derive(Debug, Clone)]
pub(crate) struct Recorded {
    pub method: String,
    pub path: String,
    /// Header names in lower case, in the order sent.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Recorded {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// How the fake server answers one request.
#[derive(Debug, Clone)]
pub(crate) enum Reply {
    /// Status, content type, body.
    Answer(u16, String, Vec<u8>),
    /// Close the connection without a word.
    HangUp,
    /// Headers announcing more body than is sent, then close.
    CutShort,
    /// Say nothing for this long, then close.
    Stall(Duration),
}

impl Reply {
    pub(crate) fn json(status: u16, body: &str) -> Self {
        Self::Answer(status, "application/json".to_owned(), body.as_bytes().to_vec())
    }
}

type Handler = dyn Fn(&Recorded) -> Reply + Send + Sync;
type Answer = dyn Fn(&[String]) -> Result<Captured, RunError> + Send + Sync;
type OnReport = dyn Fn(&Value) -> Result<Dict, CallError> + Send + Sync;
type OnUpload = dyn Fn(&LocalChunk) -> Result<Dict, CallError> + Send + Sync;
type OnFinish = dyn Fn(i128) -> Result<Dict, CallError> + Send + Sync;

/// A local HTTP/1.1 server on a free port.
pub(crate) struct FakeHttp {
    pub port: u16,
    pub seen: Arc<Mutex<Vec<Recorded>>>,
}

impl FakeHttp {
    pub(crate) fn start(handler: impl Fn(&Recorded) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("address").port();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<Handler> = Arc::new(handler);
        let record = Arc::clone(&seen);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let handler = Arc::clone(&handler);
                let record = Arc::clone(&record);
                thread::spawn(move || serve(stream, handler.as_ref(), &record));
            }
        });
        Self { port, seen }
    }

    pub(crate) fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub(crate) fn requests(&self) -> Vec<Recorded> {
        self.seen.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

fn serve(stream: TcpStream, handler: &Handler, seen: &Mutex<Vec<Recorded>>) {
    let mut writer = stream.try_clone().expect("clone");
    let mut reader = BufReader::new(stream);
    loop {
        let Some(request) = read_request(&mut reader) else {
            return;
        };
        seen.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request.clone());
        match handler(&request) {
            Reply::Answer(status, content_type, body) => {
                let head = format!(
                    "HTTP/1.1 {status} {}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\n\r\n",
                    reason(status),
                    body.len()
                );
                if writer.write_all(head.as_bytes()).is_err() || writer.write_all(&body).is_err() {
                    return;
                }
            }
            Reply::HangUp => return,
            Reply::CutShort => {
                let _ = writer.write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 100\r\n\r\n{\"ac",
                );
                return;
            }
            Reply::Stall(duration) => {
                thread::sleep(duration);
                return;
            }
        }
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        422 => "Unprocessable Entity",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Whatever",
    }
}

fn read_request(reader: &mut BufReader<TcpStream>) -> Option<Recorded> {
    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_owned();
    let path = parts.next()?.to_owned();
    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).ok()?;
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':')?;
        headers.push((name.trim().to_ascii_lowercase(), value.trim().to_owned()));
    }
    let find = |name: &str| {
        headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
    };
    let mut body = Vec::new();
    if let Some(length) = find("content-length") {
        let length: usize = length.parse().ok()?;
        body.resize(length, 0);
        reader.read_exact(&mut body).ok()?;
    } else if find("transfer-encoding").is_some_and(|value| value.eq_ignore_ascii_case("chunked")) {
        loop {
            let mut size = String::new();
            reader.read_line(&mut size).ok()?;
            let size = usize::from_str_radix(size.trim(), 16).ok()?;
            let mut piece = vec![0; size + 2];
            reader.read_exact(&mut piece).ok()?;
            if size == 0 {
                break;
            }
            body.extend_from_slice(&piece[..size]);
        }
    }
    Some(Recorded {
        method,
        path,
        headers,
        body,
    })
}

/// A runner whose `run` answers from a function of the argument list and records the
/// calls; `spawn` is refused.
pub(crate) struct ScriptedRunner {
    pub answer: Box<Answer>,
    pub calls: Mutex<Vec<(Vec<String>, Option<Duration>)>>,
}

impl ScriptedRunner {
    pub(crate) fn new(answer: impl Fn(&[String]) -> Result<Captured, RunError> + Send + Sync + 'static) -> Self {
        Self {
            answer: Box::new(answer),
            calls: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn calls(&self) -> Vec<(Vec<String>, Option<Duration>)> {
        self.calls.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

impl Runner for ScriptedRunner {
    fn run(&self, argv: &[OsString], timeout: Option<Duration>) -> Result<Captured, RunError> {
        let argv: Vec<String> = argv.iter().map(|item| item.to_string_lossy().into_owned()).collect();
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((argv.clone(), timeout));
        (self.answer)(&argv)
    }

    fn spawn(&self, _argv: &[OsString]) -> io::Result<Arc<dyn Running>> {
        Err(io::Error::other("the scripted runner starts no process"))
    }
}

/// A process that is never started: it only stands in the table of running encoders.
pub(crate) struct IdleProcess;

impl Running for IdleProcess {
    fn take_stdout(&self) -> Option<Box<dyn Read + Send>> {
        None
    }
    fn take_stderr(&self) -> Option<Box<dyn Read + Send>> {
        None
    }
    fn poll(&self) -> io::Result<Option<i64>> {
        Ok(None)
    }
    fn kill(&self) -> io::Result<()> {
        Ok(())
    }
    fn suspend(&self) -> io::Result<()> {
        Ok(())
    }
    fn resume(&self) -> io::Result<()> {
        Ok(())
    }
    fn id(&self) -> u32 {
        0
    }
}

/// A clock that moves only when told to, and records the sleeps asked of it.
#[derive(Debug, Default)]
pub(crate) struct FakeClock {
    pub now: Mutex<f64>,
    pub sleeps: Mutex<VecDeque<f64>>,
}

impl FakeClock {
    pub(crate) fn at(now: f64) -> Self {
        Self {
            now: Mutex::new(now),
            sleeps: Mutex::new(VecDeque::new()),
        }
    }

    pub(crate) fn set(&self, now: f64) {
        *self.now.lock().unwrap_or_else(PoisonError::into_inner) = now;
    }

    pub(crate) fn sleeps(&self) -> Vec<f64> {
        self.sleeps
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .collect()
    }
}

impl Clock for FakeClock {
    fn monotonic(&self) -> f64 {
        *self.now.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn sleep(&self, seconds: f64) {
        self.sleeps
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(seconds);
    }
}

/// A cluster that answers from what the test gave it and records what it was sent.
pub(crate) struct FakeCluster {
    /// The claim answers, in order; once they run out a claim fails as a transport error.
    pub claims: Mutex<VecDeque<Result<Dict, CallError>>>,
    pub claimed: Mutex<Vec<Value>>,
    pub heartbeats: Mutex<Vec<Value>>,
    pub uploads: Mutex<Vec<LocalChunk>>,
    pub finished: Mutex<Vec<(i128, i128)>>,
    pub on_heartbeat: Box<OnReport>,
    pub on_upload: Box<OnUpload>,
    pub on_finish: Box<OnFinish>,
}

impl Default for FakeCluster {
    fn default() -> Self {
        Self {
            claims: Mutex::new(VecDeque::new()),
            claimed: Mutex::new(Vec::new()),
            heartbeats: Mutex::new(Vec::new()),
            uploads: Mutex::new(Vec::new()),
            finished: Mutex::new(Vec::new()),
            on_heartbeat: Box::new(|_| Ok(answer(&[("active", Value::Bool(true)), ("paused", Value::Bool(false))]))),
            on_upload: Box::new(|_| Ok(answer(&[("stored", Value::Bool(true))]))),
            on_finish: Box::new(|_| Ok(answer(&[("finished", Value::Bool(true))]))),
        }
    }
}

/// A JSON object made of the pairs given.
pub(crate) fn answer(pairs: &[(&str, Value)]) -> Dict {
    let mut dict = Dict::new();
    for (key, value) in pairs {
        dict.insert(*key, value.clone());
    }
    dict
}

fn locked<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl FakeCluster {
    pub(crate) fn uploaded(&self) -> Vec<LocalChunk> {
        locked(&self.uploads).clone()
    }
}

impl Cluster for FakeCluster {
    fn part_url(&self, task_id: i128, part_index: i128) -> String {
        format!("http://cluster/api/transcode/tasks/{task_id}/parts/{part_index}")
    }

    fn claim(&self, report: &Value) -> Result<Dict, CallError> {
        locked(&self.claimed).push(report.clone());
        locked(&self.claims)
            .pop_front()
            .unwrap_or_else(|| Err(CallError::Transport("no more answers".to_owned())))
    }

    fn heartbeat(&self, report: &Value) -> Result<Dict, CallError> {
        locked(&self.heartbeats).push(report.clone());
        (self.on_heartbeat)(report)
    }

    fn upload(
        &self,
        _task_id: i128,
        chunk: &LocalChunk,
        limiter: &RateLimiter,
        meter: &RateMeter,
    ) -> Result<Dict, CallError> {
        let size = usize::try_from(chunk.bytes).unwrap_or_default();
        limiter.take(size);
        meter.add(size);
        let answer = (self.on_upload)(chunk);
        if answer.is_ok() {
            locked(&self.uploads).push(chunk.clone());
        }
        answer
    }

    fn finish(&self, task_id: i128, part_index: i128) -> Result<Dict, CallError> {
        let answer = (self.on_finish)(part_index);
        if answer.is_ok() {
            locked(&self.finished).push((task_id, part_index));
        }
        answer
    }
}
