//! The request and response types of the shell, and the answers Starlette and FastAPI give
//! on their own.
//!
//! Python origin: what `starlette.responses` (`JSONResponse`, `PlainTextResponse`,
//! `RedirectResponse`) and FastAPI's default `HTTPException` handler write. A JSON body is
//! `json.dumps(content, ensure_ascii=False, allow_nan=False, separators=(',', ':'))`, which
//! `pyjson`'s compact form writes byte for byte.

use std::net::SocketAddr;

use axum::body::Body;
use bytes::Bytes;
use embyx_core::pyjson::{self, Dict, DumpOptions, Value};
use embyx_core::pyurl::unquote;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderName, HeaderValue};
use http::{HeaderMap, Method, StatusCode};
use http_body_util::BodyExt as _;

/// What a handler answers.
pub type Response = http::Response<Body>;

/// One request as a route sees it.
///
/// `path` is the percent-decoded path (`scope['path']` as uvicorn hands it to Starlette:
/// `urllib.parse.unquote`, so `%2F` is a slash and invalid UTF-8 becomes U+FFFD); routing,
/// the `no-store` rule and the static file lookup all work on it.
#[derive(Debug)]
pub struct Request {
    pub method: Method,
    pub path: String,
    /// The query string as sent, without the `?` (empty when there is none).
    pub query: String,
    pub headers: HeaderMap,
    pub body: Body,
    /// Path parameters of the matched route, in template order.
    pub params: Vec<(&'static str, String)>,
    /// The listener's own address (`scope['server']`), for absolute redirect URLs when the
    /// `Host` header is missing or malformed.
    pub server: Option<SocketAddr>,
}

/// A request without its body, which (unlike the body) can be shared across an await.
#[derive(Debug, Clone)]
pub struct RequestHead {
    pub method: Method,
    pub path: String,
    pub query: String,
    pub headers: HeaderMap,
    pub server: Option<SocketAddr>,
}

impl RequestHead {
    /// `request.headers.get(name)`, decoded as latin-1.
    pub fn header(&self, name: &str) -> Option<String> {
        self.headers.get(name).map(|value| latin1(value.as_bytes()))
    }
}

impl Request {
    /// Everything but the body and the path parameters.
    pub fn head(&self) -> RequestHead {
        RequestHead {
            method: self.method.clone(),
            path: self.path.clone(),
            query: self.query.clone(),
            headers: self.headers.clone(),
            server: self.server,
        }
    }

    /// The request as the server hands it over: the path percent-decoded as uvicorn decodes
    /// it (`urllib.parse.unquote` of the raw target before `?`), the query kept as sent.
    pub fn from_http(request: http::Request<Body>, server: Option<SocketAddr>) -> Self {
        let (parts, body) = request.into_parts();
        Self {
            method: parts.method,
            path: unquote(parts.uri.path()),
            query: parts.uri.query().unwrap_or("").to_owned(),
            headers: parts.headers,
            body,
            params: Vec::new(),
            server,
        }
    }

    /// `await request.body()`: the whole body. A client that went away mid-body is
    /// Starlette's `ClientDisconnect`, which FastAPI turns into its 400.
    pub async fn body_bytes(&mut self) -> Result<Bytes, ClientDisconnect> {
        let body = std::mem::replace(&mut self.body, Body::empty());
        let bytes = body.collect().await.map_err(|_| ClientDisconnect)?.to_bytes();
        // Kept, so a second read sees the same bytes (`request._body`).
        self.body = Body::from(bytes.clone());
        Ok(bytes)
    }

    /// A path parameter of the matched route.
    pub fn param(&self, name: &str) -> &str {
        self.params
            .iter()
            .find(|(key, _)| *key == name)
            .map_or("", |(_, value)| value.as_str())
    }

    /// `request.headers.get(name)`, decoded as Starlette decodes header values (latin-1).
    pub fn header(&self, name: &str) -> Option<String> {
        self.headers.get(name).map(|value| latin1(value.as_bytes()))
    }
}

/// Starlette's `ClientDisconnect`: the body could not be read to its end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientDisconnect;

/// Bytes as latin-1 text: every byte is the code point of the same number.
pub fn latin1(bytes: &[u8]) -> String {
    bytes.iter().map(|&byte| char::from(byte)).collect()
}

/// A response with an exact body; `content-length` is set, as Starlette sets it for every
/// status but 1xx, 204 and 304.
pub fn with_body(status: StatusCode, content_type: Option<&str>, body: Bytes) -> Response {
    let mut response = http::Response::new(Body::empty());
    *response.status_mut() = status;
    let headers = response.headers_mut();
    if !(status.is_informational() || status == StatusCode::NO_CONTENT || status == StatusCode::NOT_MODIFIED) {
        headers.insert(CONTENT_LENGTH, HeaderValue::from(body.len()));
    }
    if let Some(content_type) = content_type {
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str(content_type).expect("a valid content type"),
        );
    }
    *response.body_mut() = Body::from(body);
    response
}

/// `JSONResponse(content, status_code)`.
pub fn json(status: StatusCode, content: &Value) -> Response {
    let text = pyjson::dumps(content, &DumpOptions::compact()).expect("API content is plain JSON");
    with_body(status, Some("application/json"), Bytes::from(text))
}

/// `PlainTextResponse(text, status_code)`: `text/plain; charset=utf-8`.
pub fn plain_text(status: StatusCode, text: &str) -> Response {
    with_body(status, Some("text/plain; charset=utf-8"), Bytes::from(text.to_owned()))
}

/// `{"error": {"code": code}}` (the `ApiError` envelope every feature shares).
pub fn error_envelope(code: &str) -> Value {
    let mut inner = Dict::new();
    inner.insert("code", Value::Str(code.to_owned()));
    let mut outer = Dict::new();
    outer.insert("error", Value::Dict(inner));
    Value::Dict(outer)
}

/// FastAPI's handler for `HTTPException` (Starlette raises it for 404, 405 and the static
/// files' refusals): `{"detail": <reason phrase>}` with the exception's headers.
pub fn detail(status: StatusCode, headers: &[(HeaderName, String)]) -> Response {
    let mut body = Dict::new();
    body.insert("detail", Value::Str(status.canonical_reason().unwrap_or("").to_owned()));
    let mut response = json(status, &Value::Dict(body));
    for (name, value) in headers {
        response.headers_mut().insert(
            name.clone(),
            HeaderValue::from_str(value).expect("a valid header value"),
        );
    }
    response
}

/// `FastAPI`'s 400 for a body that cannot be read at all (not UTF-8 / UTF-16 / UTF-32 where
/// JSON was declared, or nested past Python's recursion limit).
pub fn body_parse_error() -> Response {
    let mut body = Dict::new();
    body.insert("detail", Value::Str("There was an error parsing the body".to_owned()));
    json(StatusCode::BAD_REQUEST, &Value::Dict(body))
}

/// Starlette's `ServerErrorMiddleware` answer to an exception nothing handled.
pub fn internal_server_error() -> Response {
    plain_text(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error")
}

/// What a handler fails with when it has no answer of its own: logged like uvicorn logs an
/// exception in the application, answered with a plain 500.
#[derive(Debug)]
pub struct Internal(pub String);

impl Internal {
    pub fn new(error: impl std::fmt::Display) -> Self {
        Self(error.to_string())
    }

    /// Log it (`Exception in ASGI application`) and answer 500.
    pub fn into_response(self) -> Response {
        tracing::error!(target: "uvicorn.error", "Exception in ASGI application\n{}", self.0);
        internal_server_error()
    }
}

/// A route's answer: a response, or a failure nothing handles.
pub type Answer = Result<Response, Internal>;
