//! The served documents against the baseline's (`goldens/api/openapi_*.json`).

use std::sync::Arc;

use embyx_core::golden;
use embyx_core::pyjson::{self, Value};
use embyx_db::config_store::ConfigStore;
use embyx_db::database::Database;
use http::Method;

use super::{PENDING, document, structural_diff};
use crate::app::{AppParts, TRANSCODE_TITLE, VERSION, create_app};
use crate::auth::MutationAuth;
use crate::config_api::{ConfigApiOptions, create_config_router};
use crate::routing::ApiRouter;
use crate::testing::{request, send};

fn baseline(name: &str) -> Value {
    golden::cases(&golden::load(&format!("api/openapi_{name}.json")))[0].clone()
}

/// The main app as `build_app` assembles it, without a database behind it.
async fn served_main() -> Value {
    let database = Database::new("postgresql://openapi@127.0.0.1:9/openapi").expect("a URL");
    let store = Arc::new(ConfigStore::new(database));
    let app = create_app(AppParts {
        app_ready: crate::testing::ready(true),
        routers: vec![create_config_router(
            store,
            MutationAuth::new(None),
            ConfigApiOptions::default(),
        )],
        feature_health: Vec::new(),
        api_token: None,
        max_request_bytes: 65_536,
        frontend_dist: None,
    })
    .expect("an app");
    let (_, body) = send(&app, request(Method::GET, "/openapi.json", &[], None)).await;
    pyjson::loads(std::str::from_utf8(&body).expect("UTF-8")).expect("JSON")
}

#[tokio::test]
async fn the_main_document_matches_the_baseline_but_for_pending_routes() {
    let problems = structural_diff("main", &baseline("main"), &served_main().await);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn the_transcode_document_matches_the_baseline_but_for_pending_routes() {
    let rust = pyjson::loads(&document(
        TRANSCODE_TITLE,
        VERSION,
        ApiRouter::new("/api/transcode").routes(),
    ))
    .expect("JSON");
    let problems = structural_diff("transcode", &baseline("transcode"), &rust);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[tokio::test]
async fn the_diff_reports_drift_and_stale_pending_entries() {
    let python = baseline("main");
    let mut rust = served_main().await;
    // A changed response schema on a ported route.
    let text = pyjson::dumps(&rust, &pyjson::DumpOptions::compact())
        .expect("dumps")
        .replace(
            r#""additionalProperties":{"type":"boolean"}"#,
            r#""additionalProperties":{"type":"string"}"#,
        );
    rust = pyjson::loads(&text).expect("JSON");
    let problems = structural_diff("main", &python, &rust);
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("get /api/auth/session differs")),
        "{problems:?}"
    );

    // Without its routes in the baseline, every pending entry is reported.
    let mut empty = python.clone();
    if let Value::Dict(document) = &mut empty {
        document.insert("paths", Value::Dict(pyjson::Dict::new()));
    }
    let problems = structural_diff("main", &empty, &served_main().await);
    let pending_main = PENDING.iter().filter(|(app, _, _)| *app == "main").count();
    assert_eq!(
        problems.iter().filter(|problem| problem.contains("PENDING")).count(),
        pending_main
    );
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("is not in the baseline"))
    );
}
