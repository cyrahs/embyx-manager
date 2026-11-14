//! Configuration endpoints: read and update sections, and probe CloudDrive and Emby.
//!
//! Python origin: `config/api.py`. The test endpoints build throwaway clients from the
//! submitted form values, filling omitted secrets from the stored configuration, so the
//! operator can verify a change before saving it. Answers never echo a secret.

use std::sync::Arc;
use std::time::Duration;

use embyx_clients::clouddrive::client::code_name;
use embyx_clients::clouddrive::{AsyncCloudDrive, ClientOptions, CloudDriveError};
use embyx_clients::emby::{EmbyClient, EmbyError};
use embyx_core::config::{CloudDriveConfig, EmbyConfig, Mode};
use embyx_core::pyjson::{Dict, Value};
use embyx_core::pytext::py_strip;
use embyx_db::config_store::{ConfigStore, ConfigUpdateError, UnknownConfigSectionError, Versioned};
use embyx_db::database::DatabaseError;
use http::{Method, StatusCode};

use crate::auth::MutationAuth;
use crate::body::{TestConnectionRequest, UpdateSectionRequest, invalid_request, read_json};
use crate::http::{Answer, Internal, Request, Response, error_envelope, json};
use crate::routing::{ApiRouter, handler};

/// `TEST_TIMEOUT_SECONDS`.
pub const TEST_TIMEOUT: Duration = Duration::from_secs(15);

const SECTION_VIEW: &str = r#"{
  "properties": {
    "section": {"type": "string"},
    "values": {"additionalProperties": true, "type": "object"},
    "secrets": {"additionalProperties": {"type": "boolean"}, "type": "object"},
    "version": {"type": "integer"}
  },
  "type": "object",
  "required": ["section", "values", "secrets", "version"]
}"#;

const TEST_CONNECTION_REQUEST: &str = r#"{
  "properties": {"values": {"additionalProperties": true, "type": "object"}},
  "additionalProperties": false,
  "type": "object"
}"#;

const TEST_CONNECTION_RESULT: &str = r#"{
  "properties": {"ok": {"type": "boolean"}, "detail": {"type": "string", "default": ""}},
  "type": "object",
  "required": ["ok"]
}"#;

/// The operation objects, built once with the schemas spliced in.
fn openapi(kind: &str) -> &'static str {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    static BUILT: OnceLock<Mutex<HashMap<String, &'static str>>> = OnceLock::new();
    let built = BUILT.get_or_init(|| Mutex::new(HashMap::new()));
    let mut built = built.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(text) = built.get(kind) {
        return text;
    }
    let ok = |schema: &str| {
        format!(
            r#"{{"200": {{"description": "Successful Response", "content": {{"application/json": {{"schema": {schema}}}}}}}}}"#
        )
    };
    let section_parameter = r#"[{"name": "section", "in": "path", "required": true, "schema": {"type": "string"}}]"#;
    let body =
        |schema: &str| format!(r#"{{"required": true, "content": {{"application/json": {{"schema": {schema}}}}}}}"#);
    let text = match kind {
        "list" => format!(
            r#"{{"responses": {}}}"#,
            ok(&format!(r#"{{"items": {SECTION_VIEW}, "type": "array"}}"#))
        ),
        "get" => format!(
            r#"{{"parameters": {section_parameter}, "responses": {}}}"#,
            ok(SECTION_VIEW)
        ),
        "put" => format!(
            r#"{{"security": [{{"HTTPBearer": []}}], "parameters": {section_parameter}, "requestBody": {}, "responses": {}}}"#,
            body(
                r#"{"properties": {"values": {"additionalProperties": true, "type": "object"},
                    "version": {"anyOf": [{"type": "integer", "minimum": 0.0}, {"type": "null"}]}},
                    "additionalProperties": false, "type": "object", "required": ["values"]}"#
            ),
            ok(SECTION_VIEW)
        ),
        "test" => format!(
            r#"{{"requestBody": {}, "responses": {}, "security": [{{"HTTPBearer": []}}]}}"#,
            body(TEST_CONNECTION_REQUEST),
            ok(TEST_CONNECTION_RESULT)
        ),
        other => panic!("no operation {other}"),
    };
    let leaked: &'static str = Box::leak(text.into_boxed_str());
    built.insert(kind.to_owned(), leaked);
    leaked
}

/// `ApiError(status, code)` as the app's handler renders it.
fn api_error(status: StatusCode, code: &str) -> Response {
    json(status, &error_envelope(code))
}

/// `SectionView`: `section`, `values` (secrets blanked), `secrets` (whether each is set),
/// `version`.
fn view(versioned: &Versioned) -> Value {
    let mut secrets = Dict::new();
    for (field, set) in versioned.section.secret_flags() {
        secrets.insert(field, Value::Bool(set));
    }
    let mut view = Dict::new();
    view.insert("section", Value::from(versioned.section.name()));
    view.insert("values", versioned.section.masked_values());
    view.insert("secrets", Value::Dict(secrets));
    view.insert("version", Value::Int(versioned.version.into()));
    Value::Dict(view)
}

/// `_section_view(store, section)`: the view, or 404 `unknown_config_section`.
fn section_view(store: &ConfigStore, section: &str) -> Response {
    match store.get_with_version(section) {
        Ok(versioned) => json(StatusCode::OK, &view(&versioned)),
        Err(UnknownConfigSectionError { .. }) => api_error(StatusCode::NOT_FOUND, "unknown_config_section"),
    }
}

/// `TestConnectionResult(ok, detail)`.
fn test_result(ok: bool, detail: &str) -> Response {
    let mut result = Dict::new();
    result.insert("ok", Value::Bool(ok));
    result.insert("detail", Value::from(detail));
    json(StatusCode::OK, &Value::Dict(result))
}

/// What the connection tests need besides the store.
#[derive(Debug, Clone, Copy)]
pub struct ConfigApiOptions {
    /// How long a connection test may take (`TEST_TIMEOUT_SECONDS`).
    pub test_timeout: Duration,
}

impl Default for ConfigApiOptions {
    fn default() -> Self {
        Self {
            test_timeout: TEST_TIMEOUT,
        }
    }
}

/// `create_config_router(store, mutation_auth=...)`.
pub fn create_config_router(store: Arc<ConfigStore>, auth: MutationAuth, options: ConfigApiOptions) -> ApiRouter {
    let list_store = Arc::clone(&store);
    let get_store = Arc::clone(&store);
    let (put_store, put_auth) = (Arc::clone(&store), auth.clone());
    let (cloud_store, cloud_auth) = (Arc::clone(&store), auth.clone());
    let (emby_store, emby_auth) = (store, auth);
    ApiRouter::new("/api/config")
        .route(
            Method::GET,
            "",
            Some(openapi("list")),
            handler(move |_request| {
                let store = Arc::clone(&list_store);
                async move {
                    let views = store.sections().iter().map(view).collect();
                    Ok(json(StatusCode::OK, &Value::List(views)))
                }
            }),
        )
        .route(
            Method::GET,
            "/{section}",
            Some(openapi("get")),
            handler(move |request| {
                let store = Arc::clone(&get_store);
                async move { Ok(section_view(&store, request.param("section"))) }
            }),
        )
        .route(
            Method::PUT,
            "/{section}",
            Some(openapi("put")),
            handler(move |request| update_section(Arc::clone(&put_store), put_auth.clone(), request)),
        )
        .route(
            Method::POST,
            "/clouddrive/test",
            Some(openapi("test")),
            handler(move |request| test_clouddrive(Arc::clone(&cloud_store), cloud_auth.clone(), options, request)),
        )
        .route(
            Method::POST,
            "/emby/test",
            Some(openapi("test")),
            handler(move |request| test_emby(Arc::clone(&emby_store), emby_auth.clone(), options, request)),
        )
}

/// `update_section`: the body is read first, then the token checked, then the body
/// validated; the store refuses an unknown section (404), a stale version (409) and values
/// the section does not accept (422).
async fn update_section(store: Arc<ConfigStore>, auth: MutationAuth, mut request: Request) -> Answer {
    let body = match read_json(&mut request).await {
        Ok(body) => body,
        Err(response) => return Ok(response),
    };
    if let Some(refusal) = auth.refuse(&request) {
        return refusal;
    }
    let Some(update) = UpdateSectionRequest::validate(&body) else {
        return Ok(invalid_request());
    };
    let section = request.param("section").to_owned();
    let expected = match update.version.map(i32::try_from) {
        None => None,
        Some(Ok(version)) => Some(version),
        // No stored version reaches this (the column is an int4): a conflict, once the
        // section is known to exist.
        Some(Err(_)) => {
            return Ok(match store.get_with_version(&section) {
                Ok(_) => api_error(StatusCode::CONFLICT, "config_version_conflict"),
                Err(_) => api_error(StatusCode::NOT_FOUND, "unknown_config_section"),
            });
        }
    };
    match store.update(&section, &update.values, expected).await {
        Ok(_) => Ok(section_view(&store, &section)),
        Err(ConfigUpdateError::UnknownSection(_)) => Ok(api_error(StatusCode::NOT_FOUND, "unknown_config_section")),
        Err(ConfigUpdateError::VersionConflict(_)) => Ok(api_error(StatusCode::CONFLICT, "config_version_conflict")),
        Err(ConfigUpdateError::Invalid(error)) => {
            tracing::info!(target: "embyx_manager.config.api", "rejected config update for {section}: {error}");
            Ok(api_error(StatusCode::UNPROCESSABLE_ENTITY, "invalid_config_values"))
        }
        Err(ConfigUpdateError::Database(error)) => Err(database_failure(&error)),
    }
}

/// A database failure nothing handles.
fn database_failure(error: &DatabaseError) -> Internal {
    Internal(format!("{error}"))
}

/// `_merge_for_test(store, section_type, values)`: the stored section with the form's
/// values over it, a blank secret keeping the stored one. A key the section does not have,
/// or a merge it refuses, is 422 `invalid_config_values`.
#[allow(
    clippy::result_large_err,
    reason = "the refusal is the handler's answer, built once per request"
)]
fn merge_for_test<S: embyx_core::config::Registered>(store: &ConfigStore, values: &Dict) -> Result<S, Response> {
    let refused = || api_error(StatusCode::UNPROCESSABLE_ENTITY, "invalid_config_values");
    let Value::Dict(mut merged) = store.get::<S>().dump() else {
        unreachable!("a section dumps as an object");
    };
    for (key, value) in values.iter() {
        let Some(name) = key.as_str() else {
            return Err(refused());
        };
        if !merged.contains_key(name) {
            return Err(refused());
        }
        let blank = matches!(value, Value::Null) || matches!(value, Value::Str(text) if text.is_empty());
        if S::SECRET_FIELDS.contains(&name) && blank {
            continue;
        }
        merged.insert(key.clone(), value.clone());
    }
    S::validate(&Value::Dict(merged), Mode::Python).map_err(|_| refused())
}

/// Body, token, then the request model: the form values, or the answer that ends it.
async fn test_values(auth: &MutationAuth, request: &mut Request) -> Result<Dict, Answer> {
    let body = match read_json(request).await {
        Ok(body) => body,
        Err(response) => return Err(Ok(response)),
    };
    if let Some(refusal) = auth.refuse(request) {
        return Err(refusal);
    }
    TestConnectionRequest::validate(&body)
        .map(|parsed| parsed.values)
        .ok_or_else(|| Ok(invalid_request()))
}

async fn test_clouddrive(
    store: Arc<ConfigStore>,
    auth: MutationAuth,
    options: ConfigApiOptions,
    mut request: Request,
) -> Answer {
    let values = match test_values(&auth, &mut request).await {
        Ok(values) => values,
        Err(answer) => return answer,
    };
    let config = match merge_for_test::<CloudDriveConfig>(&store, &values) {
        Ok(config) => config,
        Err(response) => return Ok(response),
    };
    if !config.configured() {
        return Ok(test_result(false, "address and api_token are required"));
    }
    Ok(run_clouddrive_test(&config, options.test_timeout).await)
}

/// `_run_clouddrive_test`: one `check()` under the timeout; the client is closed after,
/// which waits for a call still running (D41).
async fn run_clouddrive_test(config: &CloudDriveConfig, timeout: Duration) -> Response {
    let cloud = AsyncCloudDrive::connect(&ClientOptions::new(
        config.address.clone(),
        config.api_token.clone(),
        config.secure,
    ));
    let outcome = tokio::time::timeout(timeout, cloud.check()).await;
    cloud.close().await;
    match outcome {
        Err(_) => test_result(false, "connection timed out"),
        Ok(Ok(_)) => test_result(true, "connected"),
        Ok(Err(CloudDriveError::Rpc(error))) => {
            let details = if error.details.is_empty() {
                "gRPC call failed"
            } else {
                error.details.as_str()
            };
            test_result(false, &format!("{}: {details}", code_name(error.code)))
        }
        // `ValueError`: its text.
        Ok(Err(error @ (CloudDriveError::InvalidPath(_) | CloudDriveError::Closed(_)))) => {
            test_result(false, &error.to_string())
        }
        // `OSError` (`FileNotFoundError`, `NotADirectoryError`).
        Ok(Err(error)) => test_result(false, &format!("connection failed: {error}")),
    }
}

async fn test_emby(
    store: Arc<ConfigStore>,
    auth: MutationAuth,
    options: ConfigApiOptions,
    mut request: Request,
) -> Answer {
    let values = match test_values(&auth, &mut request).await {
        Ok(values) => values,
        Err(answer) => return answer,
    };
    let config = match merge_for_test::<EmbyConfig>(&store, &values) {
        Ok(config) => config,
        Err(response) => return Ok(response),
    };
    if !config.configured() {
        return Ok(test_result(false, "address and api_key are required"));
    }
    run_emby_test(&config, options.test_timeout).await
}

/// `_run_emby_test`: `system_info()` with the form's key, the client closed afterwards (it
/// is dropped here). The client's own errors (a refused key, a transport failure, an answer
/// that is not a JSON object) are the test's result; anything else is unhandled, as in
/// Python.
async fn run_emby_test(config: &EmbyConfig, timeout: Duration) -> Answer {
    let client = EmbyClient::with_client(&config.address, &EmbyClient::client_options(&config.api_key, timeout))
        .map_err(Internal::new)?;
    match client.system_info().await {
        Ok(info) => Ok(test_result(true, py_strip(&format!("{} {}", info.name, info.version)))),
        Err(EmbyError::Auth(_)) => Ok(test_result(false, "the API key was rejected")),
        Err(EmbyError::Server(message)) => Ok(test_result(false, &format!("connection failed: {message}"))),
        Err(EmbyError::Raised(raised)) => Err(Internal(format!("{}: {}", raised.class, raised.message))),
    }
}

#[cfg(test)]
mod tests;
