//! The served documents against the baseline's (`goldens/api/openapi_*.json`).

use embyx_core::golden;
use embyx_core::pyjson::{self, Value};
use http::Method;

use super::{PENDING, document, structural_diff};
use crate::app::{TRANSCODE_TITLE, VERSION};
use crate::routing::ApiRouter;
use crate::testing::{request, send};

fn baseline(name: &str) -> Value {
    golden::cases(&golden::load(&format!("api/openapi_{name}.json")))[0].clone()
}

/// The main app as `build_app` assembles it, without a database behind it.
async fn served_main() -> Value {
    let settings = embyx_core::settings::Settings {
        database_url: "postgresql://openapi@127.0.0.1:9/openapi".to_owned(),
        ..embyx_core::settings::Settings::default()
    };
    let app = crate::bootstrap::build_app(&settings, None).expect("assembled").app;
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

/// A field called `title` or `description` is structure, not documentation.
#[test]
fn property_names_survive_normalization() {
    let schema = pyjson::loads(
        r#"{"title": "MissingEntryView", "type": "object",
            "properties": {"title": {"title": "Title", "type": "string"}, "description": {"type": "integer"}}}"#,
    )
    .expect("JSON");
    let normalized = super::normalize(&schema, &pyjson::Dict::new());
    assert_eq!(
        normalized,
        pyjson::loads(
            r#"{"type": "object", "properties": {"title": {"type": "string"}, "description": {"type": "integer"}}}"#
        )
        .expect("JSON")
    );
}
