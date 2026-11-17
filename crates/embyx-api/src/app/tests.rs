//! The app root: login probe, health, the reserved health keys, `no-store`, the size limit
//! and Starlette's answers for unmatched requests.

use std::sync::Arc;

use embyx_core::pyjson::{self, Dict, Value};
use http::{Method, StatusCode};

use super::{AppParts, FeatureHealthProbe, create_app, create_transcode_app};
use crate::http::{Internal, json};
use crate::routing::{ApiRouter, App, handler};
use crate::testing::{call, ready, request, send};

fn parts(api_token: Option<&str>) -> AppParts {
    AppParts {
        app_ready: ready(true),
        routers: Vec::new(),
        feature_health: Vec::new(),
        api_token: api_token.map(str::to_owned),
        max_request_bytes: 65_536,
        frontend_dist: None,
    }
}

fn echo_router() -> ApiRouter {
    ApiRouter::new("/api/echo").route(
        Method::POST,
        "",
        None,
        handler(|_request| async { Ok(json(StatusCode::OK, &Value::from("echoed"))) }),
    )
}

fn app(parts: AppParts) -> App {
    create_app(parts).expect("an app")
}

fn loads(text: &str) -> Value {
    pyjson::loads(text).expect("JSON")
}

// py: tests/test_api.py::test_auth_session_gates_the_login_screen
#[tokio::test]
async fn auth_session_gates_the_login_screen() {
    let token = "login-screen-token";
    let app = app(parts(Some(token)));
    let (denied_status, denied) = call(&app, Method::GET, "/api/auth/session", &[], None).await;
    let (wrong, _) = call(
        &app,
        Method::GET,
        "/api/auth/session",
        &[("authorization", "Bearer nope")],
        None,
    )
    .await;
    let bearer = format!("Bearer {token}");
    let (allowed_status, allowed) = call(
        &app,
        Method::GET,
        "/api/auth/session",
        &[("authorization", &bearer)],
        None,
    )
    .await;
    let (_, health) = call(&app, Method::GET, "/api/health", &[], None).await;

    assert_eq!(denied_status, StatusCode::UNAUTHORIZED);
    assert_eq!(denied, loads(r#"{"error": {"code": "unauthorized"}}"#));
    assert_eq!(wrong, StatusCode::UNAUTHORIZED);
    assert_eq!(allowed_status, StatusCode::OK);
    assert_eq!(allowed, loads(r#"{"auth_required": true}"#));
    assert_eq!(health.get("auth_required"), Some(&Value::Bool(true)));
}

// py: tests/test_api.py::test_auth_session_accepts_anyone_when_no_token_is_configured
#[tokio::test]
async fn auth_session_accepts_anyone_when_no_token_is_configured() {
    let app = app(parts(None));
    let (status, body) = call(&app, Method::GET, "/api/auth/session", &[], None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, loads(r#"{"auth_required": false}"#));
}

fn probe(name: &str) -> (String, FeatureHealthProbe) {
    let probe: FeatureHealthProbe = Arc::new(|| {
        Box::pin(async {
            let mut block = Dict::new();
            block.insert("configured", Value::Bool(false));
            Ok(block)
        })
    });
    (name.to_owned(), probe)
}

#[test]
fn feature_health_may_not_use_the_apps_own_keys() {
    let mut taken = parts(None);
    taken.feature_health = vec![probe("status"), probe("fill_actor"), probe("database")];
    let error = create_app(taken).expect_err("reserved keys");
    assert_eq!(
        error,
        "feature health keys are reserved by the app: ['database', 'status']"
    );
}

#[tokio::test]
async fn health_reports_features_and_answers_503_until_the_database_is_ready() {
    let mut not_ready = parts(None);
    not_ready.app_ready = ready(false);
    not_ready.feature_health = vec![probe("fill_actor")];
    let (status, body) = call(&app(not_ready), Method::GET, "/api/health", &[], None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        body,
        loads(
            r#"{"status": "not_ready", "database": false, "auth_required": false, "fill_actor": {"configured": false}}"#
        )
    );

    let (status, body) = call(&app(parts(None)), Method::GET, "/api/health", &[], None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        loads(r#"{"status": "ok", "database": true, "auth_required": false}"#)
    );
}

#[tokio::test]
async fn a_failing_probe_is_a_plain_500_without_no_store() {
    let mut failing = parts(None);
    failing.app_ready = Arc::new(|| Box::pin(async { Err(Internal::new("database exploded")) }));
    let (head, body) = send(&app(failing), request(Method::GET, "/api/health", &[], None)).await;
    assert_eq!(head.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(&body[..], b"Internal Server Error");
    assert!(head.headers.get("cache-control").is_none());
}

#[tokio::test]
async fn api_answers_are_no_store_and_other_paths_are_not() {
    let app = app(parts(None));
    let (head, _) = send(&app, request(Method::GET, "/api/health", &[], None)).await;
    assert_eq!(head.headers["cache-control"], "no-store");
    // A 404 under /api/ is still an API answer.
    let (head, _) = send(&app, request(Method::GET, "/api/nope", &[], None)).await;
    assert_eq!(head.status, StatusCode::NOT_FOUND);
    assert_eq!(head.headers["cache-control"], "no-store");
    let (head, _) = send(&app, request(Method::GET, "/openapi.json", &[], None)).await;
    assert!(head.headers.get("cache-control").is_none());
}

#[tokio::test]
async fn the_size_limit_refuses_large_mutations_only() {
    let mut limited = parts(None);
    limited.max_request_bytes = 8;
    limited.routers = vec![echo_router()];
    let app = app(limited);
    let (status, body) = call(&app, Method::POST, "/api/echo", &[], Some(r#"{"a": "123456789"}"#)).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body, loads(r#"{"error": {"code": "request_too_large"}}"#));
    let (status, _) = call(&app, Method::POST, "/api/echo", &[], Some("{}")).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn unmatched_requests_get_starlettes_answers_without_a_front_end() {
    let mut routed = parts(None);
    routed.routers = vec![echo_router()];
    let app = app(routed);
    let (head, body) = send(&app, request(Method::GET, "/api/echo", &[], None)).await;
    assert_eq!(head.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(head.headers["allow"], "POST");
    assert_eq!(&body[..], br#"{"detail":"Method Not Allowed"}"#);

    let (head, _) = send(&app, request(Method::GET, "/api/health/?x=1", &[], None)).await;
    assert_eq!(head.status, StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(head.headers["location"], "http://testserver/api/health?x=1");

    let (head, body) = send(&app, request(Method::GET, "/", &[], None)).await;
    assert_eq!(head.status, StatusCode::NOT_FOUND);
    assert_eq!(&body[..], br#"{"detail":"Not Found"}"#);
}

#[tokio::test]
async fn the_document_answers_head_without_a_body() {
    let app = app(parts(None));
    let (get, document) = send(&app, request(Method::GET, "/openapi.json", &[], None)).await;
    let (head, empty) = send(&app, request(Method::HEAD, "/openapi.json", &[], None)).await;
    assert_eq!(get.status, StatusCode::OK);
    assert_eq!(head.status, StatusCode::OK);
    assert!(empty.is_empty());
    assert_eq!(head.headers["content-length"], document.len().to_string().as_str());
    // API routes have no implicit HEAD.
    let (status, _) = call(&app, Method::HEAD, "/api/health", &[], None).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn the_front_end_is_mounted_only_when_its_directory_exists() {
    let directory = tempfile::tempdir().expect("a temp dir");
    std::fs::write(directory.path().join("index.html"), "<html>app</html>").expect("index");
    let mut mounted = parts(None);
    mounted.frontend_dist = Some(directory.path().to_owned());
    let app = app(mounted);
    let (head, body) = send(&app, request(Method::GET, "/monitor/runs", &[], None)).await;
    assert_eq!(head.status, StatusCode::OK);
    assert_eq!(&body[..], b"<html>app</html>");
    // A missing method on an API route falls through to the mount, as in Starlette.
    let (head, _) = send(&app, request(Method::DELETE, "/api/health", &[], None)).await;
    assert_eq!(head.status, StatusCode::METHOD_NOT_ALLOWED);
    assert!(head.headers.get("allow").is_none());

    let mut missing = parts(None);
    missing.frontend_dist = Some(directory.path().join("absent"));
    let (status, _) = call(
        &create_app(missing).expect("an app"),
        Method::GET,
        "/monitor",
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn the_transcode_app_has_no_limit_no_store_or_document() {
    let mut limited = echo_router();
    limited = limited.route(
        Method::GET,
        "/status",
        None,
        handler(|_request| async { Ok(json(StatusCode::OK, &Value::Bool(true))) }),
    );
    let app = create_transcode_app(limited);
    let big = format!(r#"{{"a": "{}"}}"#, "x".repeat(70_000));
    let (status, _) = call(&app, Method::POST, "/api/echo", &[], Some(&big)).await;
    assert_eq!(status, StatusCode::OK);
    let (head, _) = send(&app, request(Method::GET, "/api/echo/status", &[], None)).await;
    assert_eq!(head.status, StatusCode::OK);
    assert!(head.headers.get("cache-control").is_none());
    let (status, _) = call(&app, Method::GET, "/openapi.json", &[], None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
