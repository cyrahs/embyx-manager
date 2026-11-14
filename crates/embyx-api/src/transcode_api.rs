//! The home transcode worker's endpoints, served on a listener of their own.
//!
//! Python origin: `merge/transcode_api.py`. `EMBYX_MANAGER_TRANSCODE_PORT` opens a second port
//! that serves only these; the WireGuard gateway pod forwards the worker's requests there, so
//! the worker reaches nothing else of the app. `EMBYX_MANAGER_TRANSCODE_TOKEN`, when set, is a
//! bearer token it must present as well. This is the server's half of the home worker
//! protocol, version 1 (cross-version contract 5): the claim and heartbeat bodies, the chunk
//! headers, the refusal codes with their details, and part reads answered as Starlette's
//! `FileResponse` answers them, ranges included.
//!
//! FastAPI's order per request: a JSON body is read first (claim, heartbeat), then the token
//! (the router's dependency), then the path, header and body parameters are validated; a chunk
//! body is read by the endpoint itself, after the task and part were checked.

use std::path::Path;
use std::sync::Arc;

use bytes::BytesMut;
use embyx_core::config::lax::{lax_float, lax_int};
use embyx_core::pyjson::{Dict, Value};
use embyx_merge::service::{ChunkRefusedError, MergeService, WorkerCallError};
use embyx_merge::transcode::{Chunk, MAX_CHUNK_BYTES};
use http::{Method, StatusCode};
use http_body_util::BodyExt as _;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncWriteExt as _;

use crate::auth::{bearer_credentials, compare_digest};
use crate::body::{invalid_request, read_json, report_in};
use crate::http::{Answer, Internal, Request, Response, error_envelope, json};
use crate::params::int_param;
use crate::routing::{ApiRouter, handler};
use crate::static_files::serve_file;

mod openapi;

/// `WRITE_BYTES`: received bytes are written out in pieces of about this size.
pub const WRITE_BYTES: usize = 4 * 1024 * 1024;

/// What the routes share: the service and the token.
struct Transcode {
    service: MergeService,
    token: Option<Arc<str>>,
}

type Shared = Arc<Transcode>;

impl Transcode {
    /// `require_token`: `None` lets the request through. The refusal is the transcode app's
    /// `ApiError` handler's: the envelope only, no `WWW-Authenticate` (the main app adds one).
    fn refuse(&self, request: &Request) -> Option<Answer> {
        let token = self.token.as_ref()?;
        let Some(credentials) = bearer_credentials(request) else {
            return Some(Ok(json(StatusCode::UNAUTHORIZED, &error_envelope("unauthorized"))));
        };
        match compare_digest(&credentials, token) {
            Ok(true) => None,
            Ok(false) => Some(Ok(json(StatusCode::UNAUTHORIZED, &error_envelope("unauthorized")))),
            Err(error) => Some(Err(error)),
        }
    }
}

/// The `ChunkRefusedError` handler: `{"error": {"code": ..., **details}}` with its status.
pub fn refusal(refused: &ChunkRefusedError) -> Response {
    let mut inner = Dict::new();
    inner.insert("code", Value::from(refused.code));
    for (key, value) in refused.details.iter() {
        inner.insert(key.clone(), value.clone());
    }
    let mut outer = Dict::new();
    outer.insert("error", Value::Dict(inner));
    json(
        StatusCode::from_u16(refused.status).unwrap_or(StatusCode::CONFLICT),
        &Value::Dict(outer),
    )
}

/// A worker call's failure as the server answers it: a refusal, or an exception nothing
/// handles (a plain 500).
fn worker_error(error: WorkerCallError) -> Answer {
    match error {
        WorkerCallError::Refused(refused) => Ok(refusal(&refused)),
        WorkerCallError::Error(error) => Err(Internal::new(format!("{}: {error}", error.class_name()))),
    }
}

fn dict(entries: Vec<(&str, Value)>) -> Value {
    let mut dict = Dict::new();
    for (key, value) in entries {
        dict.insert(key, value);
    }
    Value::Dict(dict)
}

fn route<F, Fut>(router: ApiRouter, state: &Shared, method: Method, path: &str, function: F) -> ApiRouter
where
    F: Fn(Shared, Request) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Answer> + Send + 'static,
{
    let state = Arc::clone(state);
    let text = openapi::operation(method.as_str(), path);
    router.route(
        method,
        path,
        Some(text),
        handler(move |request| function(Arc::clone(&state), request)),
    )
}

/// `create_transcode_router(service, token=...)`.
pub fn create_transcode_router(service: MergeService, token: Option<&str>) -> ApiRouter {
    let state: Shared = Arc::new(Transcode {
        service,
        token: token.map(Arc::from),
    });
    let mut router = ApiRouter::new("/api/transcode");
    router = route(router, &state, Method::POST, "/claim", claim);
    router = route(router, &state, Method::POST, "/heartbeat", heartbeat);
    router = route(
        router,
        &state,
        Method::GET,
        "/tasks/{task_id}/parts/{part_index}",
        read_part,
    );
    router = route(
        router,
        &state,
        Method::PUT,
        "/tasks/{task_id}/parts/{part_index}/chunks/{chunk_index}",
        put_chunk,
    );
    router = route(
        router,
        &state,
        Method::POST,
        "/tasks/{task_id}/parts/{part_index}/finish",
        finish_part,
    );
    router
}

/// `claim` and `heartbeat`: the body, the token, then the report.
async fn report(state: &Transcode, request: &mut Request) -> Result<embyx_merge::transcode::WorkerReport, Answer> {
    let body = read_json(request).await.map_err(Ok)?;
    if let Some(refusal) = state.refuse(request) {
        return Err(refusal);
    }
    report_in(&body, state.service.now()).ok_or_else(|| Ok(invalid_request()))
}

async fn claim(state: Shared, mut request: Request) -> Answer {
    let report = match report(&state, &mut request).await {
        Ok(report) => report,
        Err(answer) => return answer,
    };
    let answer = state.service.claim(report).await.map_err(Internal::new)?;
    Ok(json(StatusCode::OK, &answer))
}

async fn heartbeat(state: Shared, mut request: Request) -> Answer {
    let report = match report(&state, &mut request).await {
        Ok(report) => report,
        Err(answer) => return answer,
    };
    let answer = state.service.heartbeat(report).await.map_err(Internal::new)?;
    Ok(json(StatusCode::OK, &answer))
}

/// The token, then `task_id` and `part_index` as `int` path parameters.
#[allow(
    clippy::result_large_err,
    reason = "the refusal is the handler's answer, returned as is"
)]
fn task_and_part(state: &Transcode, request: &Request) -> Result<(i64, i64), Answer> {
    if let Some(refusal) = state.refuse(request) {
        return Err(refusal);
    }
    match (
        int_param(request.param("task_id")),
        int_param(request.param("part_index")),
    ) {
        (Some(task_id), Some(part_index)) => Ok((task_id, part_index)),
        _ => Err(Ok(invalid_request())),
    }
}

/// The part as the worker's ffmpeg reads it: Starlette's `FileResponse`, which answers
/// `Range` requests (how ffmpeg reads and resumes).
async fn read_part(state: Shared, request: Request) -> Answer {
    let (task_id, part_index) = match task_and_part(&state, &request) {
        Ok(found) => found,
        Err(answer) => return answer,
    };
    let task = match state.service.transcoding_task(task_id, part_index).await {
        Ok(task) => task,
        Err(error) => return worker_error(error),
    };
    let index = usize::try_from(part_index).expect("transcoding_task checked the index");
    serve_file(
        Path::new(&task.parts[index]),
        &request.head(),
        "application/octet-stream",
    )
    .await
}

/// The chunk headers, validated as FastAPI validates `Header()` parameters: the first value
/// of each, pydantic's lax `float` / `int` from the text.
struct ChunkHeaders {
    start: f64,
    duration: f64,
    frames: i32,
    sha256: String,
    content_length: i64,
}

fn chunk_headers(request: &Request) -> Option<ChunkHeaders> {
    let header = |name: &str| request.header(name).map(Value::Str);
    let start = lax_float(&header("x-chunk-start")?).ok()?;
    let duration = lax_float(&header("x-chunk-duration")?).ok()?;
    // `gt=0`: NaN is not greater than 0.
    if duration.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
        return None;
    }
    let frames = lax_int(&header("x-chunk-frames")?).ok()?;
    if frames <= 0 {
        return None;
    }
    // The column is an `INTEGER`: Python reads the body, keeps the chunk file and fails the
    // insert with a 500 beyond it; here it is refused up front (D153).
    let frames = i32::try_from(frames).ok()?;
    let sha256 = request.header("x-chunk-sha256")?;
    if sha256.chars().count() != 64 {
        return None;
    }
    let content_length = lax_int(&header("content-length")?).ok()?;
    if content_length <= 0 || content_length > MAX_CHUNK_BYTES {
        return None;
    }
    Some(ChunkHeaders {
        start,
        duration,
        frames,
        sha256,
        content_length,
    })
}

/// `PUT .../chunks/{chunk_index}`. A refusal answered before the body is read (the token, the
/// headers, the task, the part, a negative index) leaves the rest of the body to the server:
/// uvicorn reads it and throws it away, keeping the connection, where hyper would close it
/// with the bytes unread, which a Windows client can see as a reset before it reads the
/// answer. So what is left of the body is read and dropped here, up to a chunk's size (D154).
async fn put_chunk(state: Shared, mut request: Request) -> Answer {
    let answer = upload_chunk(&state, &mut request).await;
    discard_body(&mut request).await;
    answer
}

/// Read what is left of the request body and drop it, stopping past [`MAX_CHUNK_BYTES`] or
/// at a client that went away.
async fn discard_body(request: &mut Request) {
    let mut body = std::mem::replace(&mut request.body, axum::body::Body::empty());
    let mut size: u64 = 0;
    while let Some(Ok(frame)) = body.frame().await {
        if let Ok(piece) = frame.into_data() {
            size = size.saturating_add(piece.len() as u64);
            if size > MAX_CHUNK_BYTES.unsigned_abs() {
                break;
            }
        }
    }
}

async fn upload_chunk(state: &Shared, request: &mut Request) -> Answer {
    let (task_id, part_index) = match task_and_part(state, request) {
        Ok(found) => found,
        Err(answer) => return answer,
    };
    let (Some(chunk_index), Some(headers)) = (int_param(request.param("chunk_index")), chunk_headers(request)) else {
        return Ok(invalid_request());
    };
    let task = match state.service.transcoding_task(task_id, part_index).await {
        Ok(task) => task,
        Err(error) => return worker_error(error),
    };
    if chunk_index < 0 {
        return Ok(refusal(&ChunkRefusedError::new("transcode_chunk_out_of_order", 409)));
    }
    let staging = state.service.staging_chunk(&task, part_index, chunk_index);
    let (size, digest) = receive(request, &staging, headers.content_length).await?;
    if size != headers.content_length || digest != headers.sha256.to_lowercase() {
        match tokio::fs::remove_file(&staging).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Internal::new(format!("OSError: {error}"))),
        }
        return Ok(refusal(&ChunkRefusedError::new("transcode_chunk_corrupt", 400)));
    }
    let chunk = Chunk {
        part_index: i32::try_from(part_index).expect("transcoding_task checked the index"),
        // A chunk index past `INTEGER` is always ahead of the part's chunks, which is what
        // `accept_chunk` decides for it; Python refuses it the same way before any insert.
        chunk_index: i32::try_from(chunk_index).unwrap_or(i32::MAX),
        start: headers.start,
        duration: headers.duration,
        frames: headers.frames,
        bytes: size,
        sha256: digest,
    };
    match state.service.accept_chunk(&task, &chunk, &staging).await {
        Ok(stored) => Ok(json(StatusCode::OK, &dict(vec![("stored", Value::Bool(stored))]))),
        Err(error) => worker_error(error),
    }
}

/// `_receive`: write the request body to `target` as it arrives, in pieces of about
/// [`WRITE_BYTES`]; its size and SHA-256. Reading stops at the first piece past `limit` (that
/// piece is neither hashed nor written, its length counted). A client that goes away
/// mid-body is Starlette's `ClientDisconnect`, which nothing handles: a 500, the partial file
/// left where it is.
async fn receive(request: &mut Request, target: &Path, limit: i64) -> Result<(i64, String), Internal> {
    let os_error = |error: std::io::Error| Internal::new(format!("OSError: {error}"));
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(os_error)?;
    }
    let mut file = tokio::fs::File::create(target).await.map_err(os_error)?;
    let mut digest = Sha256::new();
    let mut size: i64 = 0;
    let mut pending = BytesMut::new();
    let mut body = std::mem::replace(&mut request.body, axum::body::Body::empty());
    let outcome = async {
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|_| Internal::new("ClientDisconnect"))?;
            let Ok(piece) = frame.into_data() else {
                continue;
            };
            size = size.saturating_add(i64::try_from(piece.len()).unwrap_or(i64::MAX));
            if size > limit {
                break;
            }
            digest.update(&piece);
            pending.extend_from_slice(&piece);
            if pending.len() >= WRITE_BYTES {
                file.write_all(&pending).await.map_err(os_error)?;
                pending.clear();
            }
        }
        if !pending.is_empty() {
            file.write_all(&pending).await.map_err(os_error)?;
        }
        Ok(())
    }
    .await;
    let closed = file.shutdown().await.map_err(os_error);
    outcome?;
    closed?;
    Ok((size, hex::encode(digest.finalize())))
}

async fn finish_part(state: Shared, request: Request) -> Answer {
    let (task_id, part_index) = match task_and_part(&state, &request) {
        Ok(found) => found,
        Err(answer) => return answer,
    };
    let task = match state.service.transcoding_task(task_id, part_index).await {
        Ok(task) => task,
        Err(error) => return worker_error(error),
    };
    let part_index = i32::try_from(part_index).expect("transcoding_task checked the index");
    match state.service.finish_part(&task, part_index).await {
        Ok(()) => Ok(json(StatusCode::OK, &dict(vec![("finished", Value::Bool(true))]))),
        Err(error) => worker_error(error),
    }
}

#[cfg(test)]
mod tests;
