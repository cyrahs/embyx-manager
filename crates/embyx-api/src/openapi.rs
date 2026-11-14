//! `/openapi.json`, and its structural comparison with the Python baseline's document.
//!
//! Python origin: `FastAPI.openapi()` (`fastapi/openapi/utils.py`). Python generates the
//! document from the route signatures and pydantic models; here each route carries its
//! operation object as JSON, and the document inlines every schema instead of referring to
//! `components/schemas`. The two are compared structurally (D51): `$ref`s resolved, and the
//! documentation-only keys (`title`, `description`, `summary`, `operationId`) dropped.
//! Every operation of the baseline's two apps (`goldens/api/openapi_main.json`,
//! `openapi_transcode.json`) is either served here with the same structure or listed in
//! [`PENDING`] until its work package ports it; WP14 empties the list.

use embyx_core::pyjson::{self, Dict, DumpOptions, Value};

use crate::routing::Route;

/// The operations of the baseline not ported yet: `(app, method, path)`, by the work
/// package that ports them.
pub const PENDING: &[(&str, &str, &str)] = &[
    // WP12: fill actor.
    ("main", "post", "/api/fill-actor/avid-actors"),
    ("main", "post", "/api/fill-actor/plans"),
    ("main", "get", "/api/fill-actor/plans/{plan_id}"),
    ("main", "post", "/api/fill-actor/plans/{plan_id}/cancel"),
    ("main", "post", "/api/fill-actor/plans/{plan_id}/apply-jobs"),
    ("main", "get", "/api/fill-actor/apply-jobs/{job_id}"),
    ("main", "post", "/api/fill-actor/plans/{plan_id}/apply"),
    // WP9: monitor and playlists.
    ("main", "get", "/api/monitor/subscriptions"),
    ("main", "post", "/api/monitor/subscriptions"),
    ("main", "patch", "/api/monitor/subscriptions/{subscription_id}"),
    ("main", "delete", "/api/monitor/subscriptions/{subscription_id}"),
    ("main", "get", "/api/monitor/status"),
    ("main", "post", "/api/monitor/{pipeline}/trigger"),
    ("main", "post", "/api/monitor/{pipeline}/cancel"),
    ("main", "get", "/api/monitor/runs"),
    ("main", "get", "/api/monitor/runs/{run_id}"),
    ("main", "get", "/api/monitor/acquisitions"),
    ("main", "get", "/api/monitor/acquisitions/{avid}"),
    ("main", "post", "/api/monitor/acquisitions/{avid}/retry"),
    ("main", "post", "/api/monitor/acquisitions/{avid}/magnet"),
    ("main", "post", "/api/monitor/acquisitions/{avid}/ignore"),
    ("main", "post", "/api/monitor/acquisitions/{avid}/resume"),
    ("main", "get", "/api/monitor/manual/directories"),
    ("main", "post", "/api/monitor/manual"),
    ("main", "get", "/api/monitor/replace"),
    ("main", "post", "/api/monitor/replace"),
    ("main", "get", "/api/monitor/tracker"),
    ("main", "get", "/api/playlists"),
    ("main", "get", "/api/playlists/{key}/missing"),
    ("main", "patch", "/api/playlists/{key}"),
    ("main", "post", "/api/playlists/{key}/fill"),
    ("main", "get", "/api/playlists/{key}/fill"),
    // WP14: merge and the transcode listener.
    ("main", "get", "/api/merge/titles"),
    ("main", "get", "/api/merge/tasks"),
    ("main", "post", "/api/merge/tasks"),
    ("main", "post", "/api/merge/tasks/{task_id}/pause"),
    ("main", "post", "/api/merge/tasks/{task_id}/resume"),
    ("main", "post", "/api/merge/tasks/{task_id}/use-cpu"),
    ("main", "post", "/api/merge/tasks/{task_id}/cancel"),
    ("main", "post", "/api/merge/tasks/{task_id}/retry"),
    ("transcode", "post", "/api/transcode/claim"),
    ("transcode", "post", "/api/transcode/heartbeat"),
    ("transcode", "get", "/api/transcode/tasks/{task_id}/parts/{part_index}"),
    (
        "transcode",
        "put",
        "/api/transcode/tasks/{task_id}/parts/{part_index}/chunks/{chunk_index}",
    ),
    (
        "transcode",
        "post",
        "/api/transcode/tasks/{task_id}/parts/{part_index}/finish",
    ),
];

/// FastAPI's 422 response, which every operation with a parameter or a body lists.
const VALIDATION_ERROR: &str = r#"{
  "description": "Validation Error",
  "content": {"application/json": {"schema": {
    "properties": {"detail": {"items": {
      "properties": {
        "loc": {"items": {"anyOf": [{"type": "string"}, {"type": "integer"}]}, "type": "array"},
        "msg": {"type": "string"},
        "type": {"type": "string"},
        "input": {},
        "ctx": {"type": "object"}
      },
      "type": "object",
      "required": ["loc", "msg", "type"]
    }, "type": "array"}},
    "type": "object"
  }}}
}"#;

fn parse(text: &str) -> Value {
    pyjson::loads(text).unwrap_or_else(|error| panic!("an operation is JSON: {error}\n{text}"))
}

/// One route's operation object, with FastAPI's 422 added where FastAPI adds it.
pub fn operation(text: &str) -> Value {
    let mut operation = parse(text);
    let Value::Dict(fields) = &mut operation else {
        panic!("an operation is an object");
    };
    if fields.contains_key("parameters") || fields.contains_key("requestBody") {
        let Some(Value::Dict(responses)) = fields.get_mut("responses") else {
            panic!("an operation lists its responses");
        };
        responses.insert("422", parse(VALIDATION_ERROR));
    }
    operation
}

/// The document for an app's routes, compact JSON text.
pub fn document(title: &str, version: &str, routes: &[Route]) -> String {
    let mut paths = Dict::new();
    let mut secured = false;
    for route in routes {
        let Some(text) = route.openapi else {
            continue;
        };
        let operation = operation(text);
        secured |= operation.get("security").is_some();
        if paths.get(&route.path).is_none() {
            paths.insert(route.path.clone(), Value::Dict(Dict::new()));
        }
        let Some(Value::Dict(methods)) = paths.get_mut(&route.path) else {
            unreachable!("inserted above");
        };
        for method in &route.methods {
            methods.insert(method.as_str().to_lowercase(), operation.clone());
        }
    }
    let mut info = Dict::new();
    info.insert("title", Value::from(title));
    info.insert("version", Value::from(version));
    let mut document = Dict::new();
    document.insert("openapi", Value::from("3.1.0"));
    document.insert("info", Value::Dict(info));
    document.insert("paths", Value::Dict(paths));
    if secured {
        document.insert(
            "components",
            parse(r#"{"securitySchemes": {"HTTPBearer": {"type": "http", "scheme": "bearer"}}}"#),
        );
    }
    pyjson::dumps(&Value::Dict(document), &DumpOptions::compact()).expect("the document is plain JSON")
}

/// The keys that only document: dropped before comparing.
const DOCUMENTATION_KEYS: [&str; 4] = ["title", "description", "summary", "operationId"];

/// A document's value with `$ref`s resolved against its `components/schemas` and the
/// documentation keys dropped.
pub fn normalize(value: &Value, components: &Dict) -> Value {
    match value {
        Value::Dict(fields) => {
            if let Some(Value::Str(reference)) = fields.get("$ref") {
                let name = reference
                    .strip_prefix("#/components/schemas/")
                    .unwrap_or_else(|| panic!("an unknown reference {reference}"));
                let target = components.get(name).unwrap_or_else(|| panic!("no schema {name}"));
                return normalize(target, components);
            }
            let mut kept = Dict::new();
            for (key, item) in fields.iter() {
                if !key.as_str().is_some_and(|key| DOCUMENTATION_KEYS.contains(&key)) {
                    kept.insert(key.clone(), normalize(item, components));
                }
            }
            Value::Dict(kept)
        }
        Value::List(items) => Value::List(items.iter().map(|item| normalize(item, components)).collect()),
        other => other.clone(),
    }
}

/// Every operation of a document: `(method, path)` and the normalized operation object.
pub fn operations(document: &Value) -> Vec<((String, String), Value)> {
    let empty = Dict::new();
    let components = document
        .get("components")
        .and_then(|components| components.get("schemas"))
        .and_then(Value::as_dict)
        .unwrap_or(&empty);
    let mut found = Vec::new();
    if let Some(paths) = document.get("paths").and_then(Value::as_dict) {
        for (path, methods) in paths.iter() {
            let path = path.as_str().expect("a path").to_owned();
            for (method, operation) in methods.as_dict().expect("methods").iter() {
                let method = method.as_str().expect("a method").to_owned();
                found.push(((method, path.clone()), normalize(operation, components)));
            }
        }
    }
    found
}

/// The structural differences between the baseline's document and this port's, for one
/// app: an operation that differs, one that is neither ported nor pending, one served here
/// but not by Python, a pending one that is ported already, and differing metadata.
pub fn structural_diff(app: &str, python: &Value, rust: &Value) -> Vec<String> {
    let mut problems = Vec::new();
    for key in ["openapi", "info"] {
        if python.get(key) != rust.get(key) {
            problems.push(format!(
                "{app}: `{key}` differs: {:?} / {:?}",
                python.get(key),
                rust.get(key)
            ));
        }
    }
    let schemes = |document: &Value| {
        document
            .get("components")
            .and_then(|components| components.get("securitySchemes"))
            .cloned()
    };
    if rust
        .get("paths")
        .and_then(Value::as_dict)
        .is_some_and(|paths| !paths.is_empty())
        && schemes(python) != schemes(rust)
    {
        problems.push(format!("{app}: securitySchemes differ"));
    }
    let pending: Vec<(String, String)> = PENDING
        .iter()
        .filter(|(owner, _, _)| *owner == app)
        .map(|(_, method, path)| ((*method).to_owned(), (*path).to_owned()))
        .collect();
    let ported = operations(rust);
    let baseline = operations(python);
    for (key, operation) in &baseline {
        match ported.iter().find(|(served, _)| served == key) {
            Some((_, served)) => {
                // Key order is not structure.
                let canonical = |value: &Value| pyjson::dumps(value, &DumpOptions::hash()).unwrap_or_default();
                if canonical(served) != canonical(operation) {
                    let show = |value: &Value| pyjson::dumps(value, &DumpOptions::pretty()).unwrap_or_default();
                    problems.push(format!(
                        "{app}: {} {} differs\n--- python\n{}\n--- rust\n{}",
                        key.0,
                        key.1,
                        show(operation),
                        show(served)
                    ));
                }
                if pending.contains(key) {
                    problems.push(format!("{app}: {} {} is ported, drop it from PENDING", key.0, key.1));
                }
            }
            None if pending.contains(key) => {}
            None => problems.push(format!("{app}: {} {} is neither ported nor PENDING", key.0, key.1)),
        }
    }
    for (key, _) in &ported {
        if !baseline.iter().any(|(known, _)| known == key) {
            problems.push(format!("{app}: {} {} is not in the baseline", key.0, key.1));
        }
    }
    for key in &pending {
        if !baseline.iter().any(|(known, _)| known == key) {
            problems.push(format!("{app}: PENDING {} {} is not in the baseline", key.0, key.1));
        }
    }
    problems
}

#[cfg(test)]
mod tests;
