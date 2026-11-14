//! The CloudDrive handle's swap and close, `build_app`'s wiring on a test database
//! (`tests/test_bootstrap.py`), and the fill-actor runtime snapshot
//! (`tests/test_fill_actor_config.py`).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use embyx_clients::clouddrive::fake::FakeCloudDrive;
use embyx_core::config::models::FillActorConfig;
use embyx_core::pyjson::{self, Dict, Value};
use embyx_core::scenario::Recorder;
use embyx_core::settings::Settings;
use embyx_db::config_store::ConfigStore;
use embyx_db::testdb::{TestDatabase, URL_VARIABLE};
use http::{Method, StatusCode};

use super::{BuildError, CloudDriveHandle, build_app, build_fill_actor_runtime};
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
        transcode_token: Some("worker-secret".to_owned()),
        ..Settings::default()
    };
    let assembled = build_app(&settings, None).expect("assembled");
    assert_eq!(assembled.lifespans.len(), 2);
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
    // The merge (WP14): without the Job template merging is unavailable, and without the
    // mapping and library directories there are no titles to scan.
    let (status, tasks) = call(&assembled.app, Method::GET, "/api/merge/tasks", &[], None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        tasks.get("unavailable"),
        Some(&Value::from(embyx_merge::service::NO_JOB_TEMPLATE))
    );
    // Auto-merge reports (off until its first round, then unavailable).
    assert!(
        tasks.get("auto").and_then(|auto| auto.get("state")).is_some(),
        "{tasks:?}"
    );
    let (status, titles) = call(&assembled.app, Method::GET, "/api/merge/titles", &[], None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        titles.get("reason"),
        Some(&Value::from("mapping.dst_dir and archive.dst_dir must be configured"))
    );
    // The transcode listener serves the worker's routes and nothing else, behind
    // `transcode_token`.
    let (status, refused) = call(
        &assembled.transcode_app,
        Method::POST,
        "/api/transcode/claim",
        &[],
        Some(r#"{"name": "pc"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{refused:?}");
    let (status, claimed) = call(
        &assembled.transcode_app,
        Method::POST,
        "/api/transcode/claim",
        &[("authorization", "Bearer worker-secret")],
        Some(r#"{"name": "pc"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{claimed:?}");
    assert_eq!(claimed.get("task"), Some(&Value::Null));
    let (status, _) = call(&assembled.transcode_app, Method::GET, "/api/merge/tasks", &[], None).await;
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

// py: tests/test_bootstrap.py::test_bootstrap_wires_repository_api_and_shutdown cases=1
#[tokio::test]
async fn bootstrap_wires_repository_api_and_shutdown() {
    let database = TestDatabase::create().await;
    let dir = tempfile::tempdir().expect("a temp dir");
    let [actor, additional, move_in] = ["actor", "additional", "move-in"].map(|name| dir.path().join(name));
    for path in [&actor, &additional, &move_in] {
        std::fs::create_dir(path).expect("mkdir");
        std::fs::write(path.join(".embyx-root"), "ready").expect("sentinel");
    }
    let settings = Settings {
        database_url: dsn(database.name()),
        ..Settings::default()
    };
    let assembled = build_app(&settings, None).expect("assembled");
    for lifespan in &assembled.lifespans {
        lifespan.start().await.expect("started");
    }
    let app = &assembled.app;
    // A fresh database has no library roots, which is "not configured" rather than an
    // error: the app is healthy and every other feature keeps working.
    let (_, unconfigured) = call(app, Method::GET, "/api/health", &[], None).await;
    assert_eq!(unconfigured.get("status"), Some(&Value::from("ok")));
    let fill_actor = unconfigured.get("fill_actor").expect("fill_actor");
    assert_eq!(fill_actor.get("configured"), Some(&Value::Bool(false)));
    assert_eq!(fill_actor.get("apply_enabled"), Some(&Value::Bool(false)));
    let plan = Some(r#"{"actor_ids": ["actor"]}"#);
    let (status, _) = call(app, Method::POST, "/api/fill-actor/plans", &[], plan).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    let text = |path: &Path| {
        let value = Value::from(path.to_str().expect("UTF-8"));
        pyjson::dumps(&value, &pyjson::DumpOptions::python_default()).expect("JSON")
    };
    let body = format!(
        r#"{{"values": {{"actor_root": {}, "additional_roots": [{}], "move_in_root": {}, "move_in_by_brand": true}}}}"#,
        text(&actor),
        text(&additional),
        text(&move_in),
    );
    let (status, saved) = call(app, Method::PUT, "/api/config/fill_actor", &[], Some(&body)).await;
    assert_eq!(status, StatusCode::OK, "{saved:?}");

    // Saving takes effect without a restart.
    let (_, health) = call(app, Method::GET, "/api/health", &[], None).await;
    assert_eq!(health.get("status"), Some(&Value::from("ok")));
    let fill_actor = health.get("fill_actor").expect("fill_actor");
    assert_eq!(fill_actor.get("configured"), Some(&Value::Bool(true)));
    assert_eq!(fill_actor.get("apply_enabled"), Some(&Value::Bool(false)));
    // The scan would ask the real AVBase and JavBus about the actor, as Python's test lets it.
    // Here the job stays queued: the database refuses every claim, so no worker starts it and
    // the test never reaches the network (the cancel below then finds a queued job).
    sqlx::raw_sql(
        "CREATE FUNCTION refuse_claims() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'claims are refused in this test'; END $$;
         CREATE TRIGGER refuse_claims BEFORE UPDATE ON fill_actor_jobs
         FOR EACH ROW WHEN (NEW.state = 'running') EXECUTE FUNCTION refuse_claims();",
    )
    .execute(database.pool())
    .await
    .expect("the claim guard");
    let (status, accepted) = call(app, Method::POST, "/api/fill-actor/plans", &[], plan).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{accepted:?}");
    let plan_id = accepted
        .get("job")
        .and_then(|job| job.get("plan_id"))
        .and_then(Value::as_str)
        .expect("a plan id")
        .to_owned();
    let cancel = format!("/api/fill-actor/plans/{plan_id}/cancel");
    let (status, cancelled) = call(app, Method::POST, &cancel, &[], None).await;
    assert_eq!(status, StatusCode::OK, "{cancelled:?}");

    for lifespan in assembled.lifespans.iter().rev() {
        lifespan.stop().await.expect("stopped");
    }
    database.drop_database().await;
}

// py: tests/test_bootstrap.py::test_bootstrap_wires_fill_actor_onto_the_catalog_and_the_app_root cases=1
#[test]
fn bootstrap_wires_fill_actor_onto_the_catalog_and_the_app_root() {
    // Building connects to nothing: the pool and the clients are lazy.
    let settings = Settings {
        database_url: "postgresql://unused.invalid/db".to_owned(),
        ..Settings::default()
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let _entered = runtime.enter();
    let assembled = build_app(&settings, None).expect("assembled");
    // Fill Actor is mounted like every other feature, its routes in Python's order.
    let fill_actor: Vec<&str> = assembled
        .app
        .routes()
        .iter()
        .map(|route| route.path.as_str())
        .filter(|path| path.starts_with("/api/fill-actor/"))
        .collect();
    assert_eq!(
        fill_actor,
        [
            "/api/fill-actor/avid-actors",
            "/api/fill-actor/plans",
            "/api/fill-actor/plans/{plan_id}",
            "/api/fill-actor/plans/{plan_id}/cancel",
            "/api/fill-actor/plans/{plan_id}/apply-jobs",
            "/api/fill-actor/apply-jobs/{job_id}",
            "/api/fill-actor/plans/{plan_id}/apply",
        ]
    );
    // The store is not loaded, so the runtime is idle: unconfigured, moves off.
    let idle = super::FillActorRuntimeHandle::new(Arc::clone(&assembled.store)).current();
    assert!(!idle.configured());
    assert!(!idle.apply_enabled);
    // The runtime lifespan, then the fill actor's. (The transcode worker's app of its own is
    // a field of `Assembled`, so it is always there.)
    assert_eq!(assembled.lifespans.len(), 2);
}

// py: tests/test_fill_actor_config.py::test_runtime_snapshot_tracks_the_stored_version cases=1
#[test]
fn runtime_snapshot_tracks_the_stored_version() {
    let config = FillActorConfig {
        actor_root: "/media/actor".to_owned(),
        additional_roots: vec!["/media/extra".to_owned()],
        move_in_root: "/media/in".to_owned(),
        move_in_by_brand: true,
        ..FillActorConfig::default()
    };
    let runtime = build_fill_actor_runtime(&config, 7);
    assert_eq!(runtime.version, 7);
    assert!(runtime.configured());
    let paths = runtime.paths.as_ref().expect("paths");
    assert_eq!(paths.actor_brand_path, PathBuf::from("/media/actor"));
    assert_eq!(paths.additional_brand_paths, vec![PathBuf::from("/media/extra")]);
    assert!(runtime.move_in_by_brand);
    assert!(runtime.cloud_move_paths.is_none());
}

// py: tests/test_fill_actor_config.py::test_unconfigured_section_yields_an_idle_runtime cases=1
#[test]
fn unconfigured_section_yields_an_idle_runtime() {
    let runtime = build_fill_actor_runtime(&FillActorConfig::default(), 3);
    assert_eq!(runtime.version, 3);
    assert!(!runtime.configured());
    assert!(runtime.paths.is_none());
}
