//! Test helpers: a `TestClient`-like call into an [`App`] and a fixed readiness probe.

use std::sync::Arc;

use axum::body::Body;
use embyx_core::pyjson::{self, Value};
use http::{Method, StatusCode};
use http_body_util::BodyExt as _;

use crate::app::ReadyProbe;
use crate::http::Response;
use crate::routing::App;

/// `app_ready` that answers `value`.
pub(crate) fn ready(value: bool) -> ReadyProbe {
    Arc::new(move || Box::pin(async move { Ok(value) }))
}

/// The request `TestClient` sends: `Host: testserver`, a JSON body as `application/json`.
pub(crate) fn request(method: Method, path: &str, headers: &[(&str, &str)], body: Option<&str>) -> http::Request<Body> {
    let mut builder = http::Request::builder()
        .method(method)
        .uri(path)
        .header("host", "testserver");
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    builder
        .body(body.map_or_else(Body::empty, |text| Body::from(text.to_owned())))
        .expect("a request")
}

/// The whole response, its body collected.
pub(crate) async fn send(app: &App, request: http::Request<Body>) -> (http::response::Parts, bytes::Bytes) {
    let response: Response = app.call(request, None).await;
    let (parts, body) = response.into_parts();
    let bytes = body.collect().await.expect("the body").to_bytes();
    (parts, bytes)
}

/// One call, its body read as JSON (`Null` when empty).
pub(crate) async fn call(
    app: &App,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> (StatusCode, Value) {
    let (parts, bytes) = send(app, request(method, path, headers, body)).await;
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        let text = std::str::from_utf8(&bytes).expect("UTF-8");
        pyjson::loads(text).unwrap_or_else(|error| panic!("not JSON ({error}): {text}"))
    };
    (parts.status, value)
}
