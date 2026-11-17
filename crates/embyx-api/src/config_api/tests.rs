//! The config API on a migrated test database, with the CloudDrive fake and a recording
//! Emby stand-in.

use std::sync::Arc;
use std::time::Duration;

use embyx_clients::clouddrive::fake::FakeCloudDrive;
use embyx_core::pyjson::{self, Dict, Value};
use embyx_core::scenario::Recorder;
use embyx_db::config_store::ConfigStore;
use embyx_db::testdb::TestDatabase;
use http::{Method, StatusCode};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;

use super::{ConfigApiOptions, create_config_router};
use crate::app::{AppParts, create_app};
use crate::auth::MutationAuth;
use crate::routing::App;
use crate::testing::{call, ready};

/// `make_config_client()`: the config router alone, no token, on a loaded store.
async fn config_app(database: &TestDatabase, options: ConfigApiOptions) -> (App, Arc<ConfigStore>) {
    let store = Arc::new(ConfigStore::new(database.database()));
    store.load().await.expect("load");
    let app = create_app(AppParts {
        app_ready: ready(true),
        routers: vec![create_config_router(
            Arc::clone(&store),
            MutationAuth::new(None),
            options,
        )],
        feature_health: Vec::new(),
        api_token: None,
        max_request_bytes: 65_536,
        frontend_dist: None,
    })
    .expect("an app");
    (app, store)
}

fn json(text: &str) -> Value {
    pyjson::loads(text).expect("JSON")
}

fn get<'a>(value: &'a Value, key: &str) -> &'a Value {
    value.get(key).unwrap_or_else(|| panic!("no {key} in {value:?}"))
}

// py: tests/test_config.py::test_config_api_round_trip_masks_secrets
#[tokio::test]
async fn config_api_round_trip_masks_secrets() {
    let database = TestDatabase::create().await;
    let (app, _store) = config_app(&database, ConfigApiOptions::default()).await;

    let (status, body) = call(
        &app,
        Method::PUT,
        "/api/config/clouddrive",
        &[],
        Some(r#"{"values": {"address": "cd.internal:19798", "api_token": "secret-token"}, "version": 0}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(get(get(&body, "values"), "api_token"), &Value::from(""));
    assert_eq!(get(&body, "secrets"), &json(r#"{"api_token": true}"#));
    assert_eq!(get(&body, "version"), &Value::Int(1));

    let (_, listing) = call(&app, Method::GET, "/api/config", &[], None).await;
    let clouddrive = listing
        .as_list()
        .expect("a list")
        .iter()
        .find(|item| item.get("section") == Some(&Value::from("clouddrive")))
        .expect("the clouddrive section")
        .clone();
    assert_eq!(
        get(get(&clouddrive, "values"), "address"),
        &Value::from("cd.internal:19798")
    );
    assert_eq!(get(get(&clouddrive, "values"), "api_token"), &Value::from(""));

    let (status, conflict) = call(
        &app,
        Method::PUT,
        "/api/config/clouddrive",
        &[],
        Some(r#"{"values": {"address": "cd2.internal:19798"}, "version": 0}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        get(get(&conflict, "error"), "code"),
        &Value::from("config_version_conflict")
    );

    let (status, _) = call(
        &app,
        Method::PUT,
        "/api/config/fill_actor",
        &[],
        Some(r#"{"values": {"actor_root": "not-absolute"}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    let (status, _) = call(&app, Method::GET, "/api/config/nope", &[], None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    database.drop_database().await;
}

/// A version past the int4 column cannot match: a conflict for a known section, 404 for
/// an unknown one.
#[tokio::test]
async fn a_version_past_int4_is_a_conflict() {
    let database = TestDatabase::create().await;
    let (app, _store) = config_app(&database, ConfigApiOptions::default()).await;
    for (path, expected) in [
        ("/api/config/emby", StatusCode::CONFLICT),
        ("/api/config/nope", StatusCode::NOT_FOUND),
    ] {
        let (status, _) = call(
            &app,
            Method::PUT,
            path,
            &[],
            Some(r#"{"values": {}, "version": 2147483648}"#),
        )
        .await;
        assert_eq!(status, expected, "{path}");
    }
    database.drop_database().await;
}

// py: tests/test_config.py::test_clouddrive_test_endpoint_uses_form_values_and_stored_secret
#[tokio::test]
async fn clouddrive_test_endpoint_uses_form_values_and_stored_secret() {
    let database = TestDatabase::create().await;
    let (app, _store) = config_app(&database, ConfigApiOptions::default()).await;
    let served = FakeCloudDrive::new(&Value::Dict(Dict::new()), Recorder::new())
        .serve()
        .await;
    let (proxy, mut closed) = closing_proxy(served.address()).await;

    call(
        &app,
        Method::PUT,
        "/api/config/clouddrive",
        &[],
        Some(r#"{"values": {"address": "cd.internal:19798", "api_token": "stored-secret", "secure": false}}"#),
    )
    .await;
    // The form names the fake's address (through a proxy that sees the connection end);
    // the token is the stored one.
    let form = format!(r#"{{"values": {{"address": "{proxy}"}}}}"#);
    let (status, body) = call(&app, Method::POST, "/api/config/clouddrive/test", &[], Some(&form)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json(r#"{"ok": true, "detail": "connected"}"#));
    let seen = served.fake.seen();
    let methods: Vec<&str> = seen.iter().map(|call| call.method.as_str()).collect();
    assert_eq!(methods, ["GetSystemInfo", "GetSubFiles"]);
    // GetSystemInfo is the unauthenticated ping; the listing carries the stored secret.
    assert_eq!(seen[1].authorization.as_deref(), Some("Bearer stored-secret"));
    // `captured['closed'] is True`: the throwaway client let its connection go.
    tokio::time::timeout(Duration::from_secs(5), closed.recv())
        .await
        .expect("the test's client closes its connection")
        .expect("the proxy reports it");
    database.drop_database().await;
}

/// The timeout answers only once the call still running has finished: the throwaway
/// client is closed, and its close waits for the call (D41).
#[tokio::test]
async fn a_timed_out_clouddrive_test_closes_its_client_after_the_call() {
    let database = TestDatabase::create().await;
    let options = ConfigApiOptions {
        test_timeout: Duration::from_millis(200),
    };
    let (app, _store) = config_app(&database, options).await;
    let fake = FakeCloudDrive::new(&Value::Dict(Dict::new()), Recorder::new());
    let hold = fake.hold("GetSystemInfo");
    let served = fake.serve().await;
    let form = format!(
        r#"{{"values": {{"address": "{}", "api_token": "t", "secure": false}}}}"#,
        served.address()
    );
    let request =
        tokio::spawn(async move { call(&app, Method::POST, "/api/config/clouddrive/test", &[], Some(&form)).await });
    hold.arrived().await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!request.is_finished(), "answered before the call it timed out ended");
    hold.release();
    let (status, body) = request.await.expect("the request");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json(r#"{"ok": false, "detail": "connection timed out"}"#));
    database.drop_database().await;
}

// py: tests/test_config.py::test_clouddrive_test_endpoint_requires_connection_values
#[tokio::test]
async fn clouddrive_test_endpoint_requires_connection_values() {
    let database = TestDatabase::create().await;
    let (app, _store) = config_app(&database, ConfigApiOptions::default()).await;
    let (status, body) = call(&app, Method::POST, "/api/config/clouddrive/test", &[], Some("{}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(get(&body, "ok"), &Value::Bool(false));
    assert!(get(&body, "detail").as_str().expect("text").contains("required"));
    database.drop_database().await;
}

/// A TCP proxy in front of `upstream` that reports each connection the client ends.
async fn closing_proxy(upstream: String) -> (String, tokio::sync::mpsc::UnboundedReceiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address").to_string();
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((mut client, _)) = listener.accept().await {
            let (upstream, sender) = (upstream.clone(), sender.clone());
            tokio::spawn(async move {
                let mut server = tokio::net::TcpStream::connect(upstream).await.expect("upstream");
                let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                let _ = sender.send(());
            });
        }
    });
    (address, receiver)
}

/// What the stand-in Emby saw: the request head as sent, and whether the client then
/// closed the kept-alive connection.
struct EmbySaw {
    head: String,
    closed: bool,
}

/// One HTTP exchange of a stand-in Emby: the request head it got, answered with `answer`
/// on a connection kept alive, then a wait for the client to close it.
async fn emby_once(status: u16, answer: &'static str) -> (String, tokio::task::JoinHandle<EmbySaw>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = format!("http://{}/", listener.local_addr().expect("address"));
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut head = Vec::new();
        let mut chunk = [0_u8; 4096];
        while !head.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut chunk).await.expect("read");
            if read == 0 {
                break;
            }
            head.extend_from_slice(&chunk[..read]);
        }
        let reply = format!(
            "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{answer}",
            answer.len()
        );
        stream.write_all(reply.as_bytes()).await.expect("write");
        let closed = matches!(
            tokio::time::timeout(Duration::from_secs(5), stream.read(&mut chunk)).await,
            Ok(Ok(0) | Err(_))
        );
        EmbySaw {
            head: String::from_utf8_lossy(&head).into_owned(),
            closed,
        }
    });
    (address, task)
}

// py: tests/test_config.py::test_emby_test_endpoint_uses_form_values_and_stored_secret
#[tokio::test]
async fn emby_test_endpoint_uses_form_values_and_stored_secret() {
    let database = TestDatabase::create().await;
    let (app, _store) = config_app(&database, ConfigApiOptions::default()).await;
    call(
        &app,
        Method::PUT,
        "/api/config/emby",
        &[],
        Some(r#"{"values": {"address": "http://embyx.media.svc.cluster.local", "api_key": "stored-key"}}"#),
    )
    .await;
    let (address, emby) = emby_once(200, r#"{"ServerName": "embyx", "Version": "4.10.0.40", "Id": "srv"}"#).await;
    // The form's address, trailing slash and all: the section normalizes it.
    let form = format!(r#"{{"values": {{"address": "{address}"}}}}"#);
    let (status, body) = call(&app, Method::POST, "/api/config/emby/test", &[], Some(&form)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json(r#"{"ok": true, "detail": "embyx 4.10.0.40"}"#));
    let saw = emby.await.expect("the stand-in");
    assert!(
        saw.head.starts_with("GET /emby/System/Info HTTP/1.1\r\n"),
        "{}",
        saw.head
    );
    let token = saw.head.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("x-emby-token")
            .then(|| value.trim().to_owned())
    });
    assert_eq!(token.as_deref(), Some("stored-key"), "{}", saw.head);
    // `captured['closed'] is True`.
    assert!(saw.closed, "the test's client closes its connection");
    database.drop_database().await;
}

// py: tests/test_config.py::test_emby_test_endpoint_reports_a_rejected_key
#[tokio::test]
async fn emby_test_endpoint_reports_a_rejected_key() {
    let database = TestDatabase::create().await;
    let (app, _store) = config_app(&database, ConfigApiOptions::default()).await;
    let (address, emby) = emby_once(401, "{}").await;
    let form = format!(r#"{{"values": {{"address": "{address}", "api_key": "bad"}}}}"#);
    let (_, body) = call(&app, Method::POST, "/api/config/emby/test", &[], Some(&form)).await;
    assert_eq!(body, json(r#"{"ok": false, "detail": "the API key was rejected"}"#));
    emby.await.expect("the stand-in");
    database.drop_database().await;
}

/// An answer that is not a JSON object is the client's own error, reported like a failed
/// connection.
#[tokio::test]
async fn an_emby_answer_of_the_wrong_shape_is_a_failed_connection() {
    let database = TestDatabase::create().await;
    let (app, _store) = config_app(&database, ConfigApiOptions::default()).await;
    let (address, emby) = emby_once(200, "[1]").await;
    let form = format!(r#"{{"values": {{"address": "{address}", "api_key": "k"}}}}"#);
    let (status, body) = call(&app, Method::POST, "/api/config/emby/test", &[], Some(&form)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json(r#"{"ok": false, "detail": "connection failed: GET /emby/System/Info: unexpected JSON shape"}"#)
    );
    emby.await.expect("the stand-in");
    database.drop_database().await;
}

// py: tests/test_config.py::test_emby_test_endpoint_requires_connection_values
#[tokio::test]
async fn emby_test_endpoint_requires_connection_values() {
    let database = TestDatabase::create().await;
    let (app, _store) = config_app(&database, ConfigApiOptions::default()).await;
    let (status, body) = call(&app, Method::POST, "/api/config/emby/test", &[], Some("{}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(get(&body, "ok"), &Value::Bool(false));
    assert!(get(&body, "detail").as_str().expect("text").contains("required"));
    database.drop_database().await;
}
