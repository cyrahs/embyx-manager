//! Talking to the cluster: the transcode listener's five endpoints.
//!
//! Python origin: `ServerError` and `Server` of `src/embyx_manager/merge/home_worker.py`.
//! The requests are cross-version contract 5 (home worker protocol version 1): the claim
//! and heartbeat bodies, the bearer token, the chunk upload with its `X-Chunk-*` headers and
//! `Content-Length`, the part URL ffmpeg reads with ranges, and how a refusal's `code` and
//! details are read back. `goldens/merge/home_worker_requests.json` pins the requests Python
//! sends; the protocol tests run this client against the Python listener.

use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::time::Duration;

use embyx_core::pyjson::{self, Dict, DumpOptions, Value};

use crate::chunks::{LocalChunk, RateLimiter, RateMeter};
use crate::config::WorkerConfig;
use crate::py;

/// `UPLOAD_PIECE_BYTES`: the upload reads and paces the chunk in pieces of this size.
pub const UPLOAD_PIECE_BYTES: usize = 256 * 1024;

/// `ServerError`: the cluster answered with an error status.
#[derive(Debug, Clone)]
pub struct ServerError {
    pub status: u16,
    pub code: String,
    /// The `error` object of the answer (`{'code': ..., ...}`), `{}` when there is none.
    pub details: Dict,
}

impl fmt::Display for ServerError {
    /// `str(exc)`: `409 transcode_chunk_conflict`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.status, self.code)
    }
}

impl ServerError {
    /// `exc.details` as Python's f-string shows a dict.
    pub fn details_repr(&self) -> String {
        py::repr(&Value::Dict(self.details.clone()))
    }
}

/// Why a call to the cluster failed.
#[derive(Debug, thiserror::Error)]
pub enum CallError {
    /// `ServerError`.
    #[error("{0}")]
    Server(ServerError),
    /// `httpx.HTTPError`: no answer (refused, reset, timed out, ...). The text is this
    /// client's, not httpx's (D170).
    #[error("{0}")]
    Transport(String),
    /// `OSError` reading the chunk being uploaded.
    #[error("{0}")]
    Io(io::Error),
    /// A success status whose body is not a JSON object, where Python's `response.json()`
    /// or `.get` raises and ends the thread; here it is retried like a transport failure
    /// (D170).
    #[error("{0}")]
    Protocol(String),
}

/// What the worker needs from the cluster; `HttpServer` is the real one and tests stand in
/// for it (Python's tests pass a `Cluster` object in the same place).
pub trait Cluster: Send + Sync {
    /// `server.part_url(task_id, part_index)`: what ffmpeg and ffprobe read.
    fn part_url(&self, task_id: i128, part_index: i128) -> String;
    /// `POST /api/transcode/claim`.
    fn claim(&self, report: &Value) -> Result<Dict, CallError>;
    /// `POST /api/transcode/heartbeat`.
    fn heartbeat(&self, report: &Value) -> Result<Dict, CallError>;
    /// `PUT .../chunks/<index>` with the chunk's bytes.
    fn upload(
        &self,
        task_id: i128,
        chunk: &LocalChunk,
        limiter: &RateLimiter,
        meter: &RateMeter,
    ) -> Result<Dict, CallError>;
    /// `POST .../finish`.
    fn finish(&self, task_id: i128, part_index: i128) -> Result<Dict, CallError>;
}

/// `Server`: httpx's client with `REQUEST_TIMEOUT` (60 s to connect, 600 s for each read
/// and write), the bearer token on every request, proxies from the environment as httpx
/// takes them.
pub struct HttpServer {
    base: String,
    token: String,
    agent: ureq::Agent,
}

impl fmt::Debug for HttpServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpServer")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

impl HttpServer {
    pub fn new(config: &WorkerConfig) -> Self {
        Self::with_timeouts(config, Duration::from_mins(1), Duration::from_mins(10))
    }

    /// `new` with other timeouts: to connect, and for each read and write.
    pub fn with_timeouts(config: &WorkerConfig, connect: Duration, io: Duration) -> Self {
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            // httpx's client follows no redirect: a 3xx is the cluster's answer.
            .max_redirects(0)
            .timeout_connect(Some(connect))
            .timeout_per_read(Some(io))
            .timeout_per_write(Some(io))
            .user_agent("embyx-transcode-worker")
            .build()
            .new_agent();
        Self {
            base: config.server.clone(),
            token: config.token.clone(),
            agent,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    fn authorized<B>(&self, request: ureq::RequestBuilder<B>) -> ureq::RequestBuilder<B> {
        if self.token.is_empty() {
            request
        } else {
            request.header("Authorization", format!("Bearer {}", self.token))
        }
    }

    fn post_json(&self, path: &str, body: &Value) -> Result<Dict, CallError> {
        // httpx's `json=`: `json.dumps(..., ensure_ascii=False, separators=(',', ':'))`.
        let text =
            pyjson::dumps(body, &DumpOptions::compact()).map_err(|error| CallError::Protocol(error.to_string()))?;
        let request = self
            .authorized(self.agent.post(self.url(path)))
            .header("Content-Type", "application/json");
        answer(request.send(text.as_bytes()))
    }
}

/// `Server._call`'s reading of the answer.
fn answer(result: Result<ureq::http::Response<ureq::Body>, ureq::Error>) -> Result<Dict, CallError> {
    let mut response = result.map_err(transport)?;
    let status = response.status();
    let body = response
        .body_mut()
        .with_config()
        .limit(16 * 1024 * 1024)
        .read_to_vec()
        .map_err(transport)?;
    let parsed = std::str::from_utf8(&body)
        .ok()
        .and_then(|text| pyjson::loads(text).ok());
    if status.is_success() {
        return match parsed {
            Some(Value::Dict(dict)) => Ok(dict),
            Some(other) => Err(CallError::Protocol(format!(
                "the cluster answered {} with a JSON {}, not an object",
                status.as_u16(),
                py::type_name(&other)
            ))),
            None => Err(CallError::Protocol(format!(
                "the cluster answered {} with a body that is not JSON",
                status.as_u16()
            ))),
        };
    }
    // `error = response.json().get('error') or {}`; a body that is not JSON gives `{}`.
    let details = match parsed {
        Some(Value::Dict(dict)) => match dict.get("error") {
            Some(Value::Dict(error)) => error.clone(),
            _ => Dict::new(),
        },
        _ => Dict::new(),
    };
    let code = details
        .get("code")
        .map_or_else(|| status.canonical_reason().unwrap_or_default().to_owned(), py::str_of);
    Err(CallError::Server(ServerError {
        status: status.as_u16(),
        code,
        details,
    }))
}

fn transport(error: ureq::Error) -> CallError {
    match error {
        ureq::Error::Io(error) => CallError::Transport(error.to_string()),
        other => CallError::Transport(other.to_string()),
    }
}

/// The chunk's bytes as the upload sends them: read in pieces, each paced by the limiter
/// and counted by the meter (`Server.upload`'s `body()`).
struct PacedFile<'a> {
    file: File,
    limiter: &'a RateLimiter,
    meter: &'a RateMeter,
    piece: Vec<u8>,
    at: usize,
}

impl Read for PacedFile<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.at == self.piece.len() {
            self.piece.resize(UPLOAD_PIECE_BYTES, 0);
            let mut filled = 0;
            while filled < UPLOAD_PIECE_BYTES {
                let read = self.file.read(&mut self.piece[filled..])?;
                if read == 0 {
                    break;
                }
                filled += read;
            }
            self.piece.truncate(filled);
            self.at = 0;
            if filled == 0 {
                return Ok(0);
            }
            self.limiter.take(filled);
            self.meter.add(filled);
        }
        let count = buffer.len().min(self.piece.len() - self.at);
        buffer[..count].copy_from_slice(&self.piece[self.at..self.at + count]);
        self.at += count;
        Ok(count)
    }
}

impl Cluster for HttpServer {
    fn part_url(&self, task_id: i128, part_index: i128) -> String {
        format!("{}/api/transcode/tasks/{task_id}/parts/{part_index}", self.base)
    }

    fn claim(&self, report: &Value) -> Result<Dict, CallError> {
        self.post_json("/api/transcode/claim", report)
    }

    fn heartbeat(&self, report: &Value) -> Result<Dict, CallError> {
        self.post_json("/api/transcode/heartbeat", report)
    }

    fn upload(
        &self,
        task_id: i128,
        chunk: &LocalChunk,
        limiter: &RateLimiter,
        meter: &RateMeter,
    ) -> Result<Dict, CallError> {
        let file = File::open(&chunk.path).map_err(CallError::Io)?;
        let mut body = PacedFile {
            file,
            limiter,
            meter,
            piece: Vec::new(),
            at: 0,
        };
        let path = format!(
            "/api/transcode/tasks/{task_id}/parts/{}/chunks/{}",
            chunk.part_index, chunk.index
        );
        let request = self
            .authorized(self.agent.put(self.url(&path)))
            .header("Content-Length", chunk.bytes.to_string())
            .header("Content-Type", "application/octet-stream")
            .header("X-Chunk-Start", py::fixed(chunk.start, 6))
            .header("X-Chunk-Duration", py::fixed(chunk.duration, 6))
            .header("X-Chunk-Frames", chunk.frames.to_string())
            .header("X-Chunk-Sha256", chunk.sha256.as_str());
        answer(request.send(ureq::SendBody::from_reader(&mut body)))
    }

    fn finish(&self, task_id: i128, part_index: i128) -> Result<Dict, CallError> {
        let path = format!("/api/transcode/tasks/{task_id}/parts/{part_index}/finish");
        answer(self.authorized(self.agent.post(self.url(&path))).send_empty())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{CallError, Cluster as _, HttpServer};
    use crate::config::WorkerConfig;
    use crate::run::idle_report;
    use crate::testing::{FakeHttp, Reply};

    fn server(base: &str) -> HttpServer {
        let mut config = WorkerConfig::new(base);
        config.name = "pc".to_owned();
        HttpServer::with_timeouts(&config, Duration::from_secs(2), Duration::from_millis(300))
    }

    fn claim(reply: Reply) -> Result<embyx_core::pyjson::Dict, CallError> {
        let fake = FakeHttp::start(move |_| reply.clone());
        let server = server(&fake.base());
        server.claim(&idle_report(&WorkerConfig::new("x")))
    }

    // `httpx.HTTPError` in Python: the worker logs it and tries again (D170 for the text).
    #[test]
    fn no_answer_is_a_transport_error() {
        for reply in [Reply::HangUp, Reply::CutShort, Reply::Stall(Duration::from_secs(2))] {
            let result = claim(reply.clone());
            assert!(matches!(result, Err(CallError::Transport(_))), "{reply:?}: {result:?}");
        }
        let closed = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = closed.local_addr().expect("address").port();
        drop(closed);
        let result = server(&format!("http://127.0.0.1:{port}")).finish(7, 0);
        assert!(matches!(result, Err(CallError::Transport(_))), "{result:?}");
    }

    // Python's `response.json()` raises there and the thread ends; here it is retried (D170).
    #[test]
    fn a_success_that_is_not_a_json_object_is_a_protocol_error() {
        for (body, shown) in [
            ("[1, 2]", "a JSON list, not an object"),
            ("<html>", "a body that is not JSON"),
        ] {
            let result = claim(Reply::Answer(200, "text/html".to_owned(), body.as_bytes().to_vec()));
            let Err(CallError::Protocol(message)) = result else {
                panic!("{body}: {result:?}");
            };
            assert!(message.ends_with(shown), "{message}");
        }
    }

    // httpx follows no redirect, so a 3xx is a `ServerError` named by its reason phrase.
    #[test]
    fn a_redirect_is_an_answer_not_followed() {
        for status in [301, 307] {
            let fake = FakeHttp::start(move |_| Reply::Redirect(status, "/elsewhere".to_owned()));
            let result = server(&fake.base()).claim(&idle_report(&WorkerConfig::new("x")));
            let Err(CallError::Server(error)) = result else {
                panic!("{status}: {result:?}");
            };
            assert_eq!(error.status, status);
            assert_eq!(
                error.code,
                if status == 301 {
                    "Moved Permanently"
                } else {
                    "Temporary Redirect"
                }
            );
            assert_eq!(fake.requests().len(), 1, "{status}: the redirect was followed");
        }
    }

    #[test]
    fn the_token_goes_on_every_request() {
        let fake = FakeHttp::start(|_| Reply::json(200, "{}"));
        let mut config = WorkerConfig::new(fake.base());
        config.token = "t0k".to_owned();
        let server = HttpServer::new(&config);
        server.claim(&idle_report(&config)).expect("claim");
        server.heartbeat(&idle_report(&config)).expect("heartbeat");
        server.finish(7, 1).expect("finish");
        let requests = fake.requests();
        assert_eq!(requests.len(), 3);
        for request in requests {
            assert_eq!(request.header("authorization"), Some("Bearer t0k"), "{}", request.path);
            assert_eq!(request.header("user-agent"), Some("embyx-transcode-worker"));
        }
    }
}
