//! The CloudDrive handle's swap and close, and `build_app`'s shell wiring on a test
//! database. `tests/test_bootstrap.py` itself asserts fill-actor wiring and moves to WP12.

use std::sync::Arc;
use std::time::{Duration, Instant};

use embyx_clients::clouddrive::fake::FakeCloudDrive;
use embyx_core::pyjson::{self, Dict, Value};
use embyx_core::scenario::Recorder;
use embyx_core::settings::Settings;
use embyx_db::config_store::ConfigStore;
use embyx_db::testdb::{TestDatabase, URL_VARIABLE};
use http::{Method, StatusCode};

use super::{BuildError, CloudDriveHandle, build_app};
use crate::testing::call;

async fn configure(store: &ConfigStore, address: &str) {
    let values = pyjson::loads(&format!(
        r#"{{"address": "{address}", "api_token": "t", "secure": false}}"#
    ))
    .expect("JSON");
    let Value::Dict(values) = values else { unreachable!() };
    store.update("clouddrive", &values, None).await.expect("saved");
}

#[tokio::test]
async fn the_handle_swaps_its_client_when_the_section_changes() {
    let database = TestDatabase::create().await;
    let store = Arc::new(ConfigStore::new(database.database()));
    store.load().await.expect("load");
    let handle = CloudDriveHandle::new(Arc::clone(&store));
    assert!(handle.current().is_none(), "unconfigured");

    let served = FakeCloudDrive::new(&Value::Dict(Dict::new()), Recorder::new())
        .serve()
        .await;
    configure(&store, &served.address()).await;
    let first = handle.current().expect("a client");
    first.check().await.expect("connected");
    // The same version keeps the same client open.
    handle.current().expect("a client").check().await.expect("still open");
    first.check().await.expect("still open");

    configure(&store, &served.address()).await;
    let second = handle.current().expect("a new client");
    second.check().await.expect("connected");
    // The replaced client is closed in the background.
    let deadline = Instant::now() + Duration::from_secs(5);
    while first.check().await.is_ok() {
        assert!(Instant::now() < deadline, "the old client is still open");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    handle.close(Duration::from_secs(5)).await;
    assert!(second.check().await.is_err(), "closed on shutdown");
    database.drop_database().await;
}

#[tokio::test]
async fn close_waits_for_running_calls_up_to_its_limit() {
    let database = TestDatabase::create().await;
    let store = Arc::new(ConfigStore::new(database.database()));
    store.load().await.expect("load");
    let fake = FakeCloudDrive::new(&Value::Dict(Dict::new()), Recorder::new());
    let hold = fake.hold("GetSystemInfo");
    let served = fake.serve().await;
    configure(&store, &served.address()).await;
    let handle = CloudDriveHandle::new(Arc::clone(&store));
    let cloud = handle.current().expect("a client");
    let running = tokio::spawn(async move { cloud.check().await });
    hold.arrived().await;

    let started = Instant::now();
    handle.close(Duration::from_millis(300)).await;
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(300), "returned after {waited:?}");
    assert!(!running.is_finished(), "the held call ended early");
    hold.release();
    // The held ping completes; the listing after it meets the closed client.
    let outcome = running.await.expect("the call");
    assert!(
        matches!(outcome, Err(embyx_clients::clouddrive::CloudDriveError::Closed(_))),
        "{outcome:?}"
    );
    database.drop_database().await;
}

/// The test database's DSN: the server from the environment, the database `name`.
fn dsn(name: &str) -> String {
    let server = std::env::var(URL_VARIABLE).expect("the test database URL");
    let without_query = server.split('?').next().unwrap_or_default();
    let base = without_query.rsplit_once('/').map_or(without_query, |(base, _)| base);
    format!("{base}/{name}")
}

#[tokio::test]
async fn build_app_loads_the_store_and_serves_config() {
    let database = TestDatabase::create().await;
    let settings = Settings {
        database_url: dsn(database.name()),
        ..Settings::default()
    };
    let assembled = build_app(&settings, None).expect("assembled");
    assert_eq!(assembled.lifespans.len(), 1);
    for lifespan in &assembled.lifespans {
        lifespan.start().await.expect("started");
    }
    let (status, health) = call(&assembled.app, Method::GET, "/api/health", &[], None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(health.get("status"), Some(&Value::from("ok")));
    let (status, saved) = call(
        &assembled.app,
        Method::PUT,
        "/api/config/emby",
        &[],
        Some(r#"{"values": {"address": "http://emby.invalid", "api_key": "k"}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{saved:?}");
    assert_eq!(assembled.store.get_with_version("emby").expect("emby").version, 1);
    // The monitor's routers and scheduler (WP9): every pipeline reports, none is configured.
    let (status, pipelines) = call(&assembled.app, Method::GET, "/api/monitor/status", &[], None).await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<Value> = pipelines
        .as_list()
        .expect("a list")
        .iter()
        .map(|pipeline| pipeline.get("pipeline").expect("a name").clone())
        .collect();
    assert_eq!(
        names,
        ["rss", "archive", "mapping", "playlists"].map(Value::from).to_vec()
    );
    let (status, playlists) = call(&assembled.app, Method::GET, "/api/playlists", &[], None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        playlists.get("fill_reason"),
        Some(&Value::from(crate::playlists_api::NO_FILL_DIRECTORY))
    );
    let (status, tracker) = call(&assembled.app, Method::GET, "/api/monitor/tracker", &[], None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(tracker.get("reason"), Some(&Value::from("archive is disabled")));
    // The transcode listener's app has no routes yet (WP14).
    let (status, _) = call(&assembled.transcode_app, Method::GET, "/api/transcode/jobs", &[], None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    for lifespan in assembled.lifespans.iter().rev() {
        lifespan.stop().await.expect("stopped");
    }
    database.drop_database().await;
}

#[test]
fn build_app_refuses_an_exposed_bind_and_a_malformed_url() {
    let exposed = Settings {
        host: "0.0.0.0".to_owned(),
        ..Settings::default()
    };
    match build_app(&exposed, None) {
        Err(BuildError::Settings(error)) => assert!(error.0.contains("EMBYX_MANAGER_API_TOKEN"), "{error:?}"),
        other => panic!("{other:?}"),
    }
    let malformed = Settings {
        database_url: "mysql://nope".to_owned(),
        ..Settings::default()
    };
    assert!(matches!(build_app(&malformed, None), Err(BuildError::Database(_))));
}
