//! The application root: shell, auth, health and static hosting.
//!
//! Python origin: `api.py` (`create_app`) and `merge/transcode_api.py`
//! (`create_transcode_app`). Like Python's, the root knows nothing about any feature:
//! features hand in routers, health probes and lifespans. A feature's own errors are its
//! routes' answers here (Python registers exception handlers for them).

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use embyx_core::pyjson::{Dict, Value};
use futures_util::future::BoxFuture;
use http::{Method, StatusCode};

use crate::auth::MutationAuth;
use crate::http::{Internal, json, with_body};
use crate::openapi;
use crate::routing::{ApiRouter, App, Layers, Route, handler};
use crate::static_files::SpaStaticFiles;

/// Top-level health fields owned by the app (`RESERVED_HEALTH_KEYS`).
pub const RESERVED_HEALTH_KEYS: [&str; 3] = ["status", "database", "auth_required"];

/// `app_ready`: the readiness every feature depends on (the database). `Err` is an
/// exception nothing handles.
pub type ReadyProbe = Arc<dyn Fn() -> BoxFuture<'static, Result<bool, Internal>> + Send + Sync>;

/// One feature's health probe, reported under its own key.
pub type FeatureHealthProbe = Arc<dyn Fn() -> BoxFuture<'static, Result<Dict, Internal>> + Send + Sync>;

/// A feature's lifespan: started in order before the first request, stopped in reverse
/// order after the last (`AsyncExitStack`). A failed start stops what already started.
pub trait Lifespan: Send + Sync {
    fn start(&self) -> BoxFuture<'_, Result<(), Internal>>;
    fn stop(&self) -> BoxFuture<'_, Result<(), Internal>>;
}

/// `create_app(...)`'s keywords.
pub struct AppParts {
    pub app_ready: ReadyProbe,
    pub routers: Vec<ApiRouter>,
    pub feature_health: Vec<(String, FeatureHealthProbe)>,
    pub api_token: Option<String>,
    pub max_request_bytes: usize,
    /// Mounted only when it is a directory when the app is assembled.
    pub frontend_dist: Option<PathBuf>,
}

impl std::fmt::Debug for AppParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppParts")
            .field("routers", &self.routers)
            .field(
                "feature_health",
                &self.feature_health.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            )
            .field("api_token", &self.api_token.as_ref().map(|_| "<redacted>"))
            .field("max_request_bytes", &self.max_request_bytes)
            .field("frontend_dist", &self.frontend_dist)
            .finish_non_exhaustive()
    }
}

/// `FastAPI(title='embyx-manager', version='0.1.0')`.
pub const TITLE: &str = "embyx-manager";
pub const VERSION: &str = "0.1.0";
/// The transcode app's title; it serves no document (`openapi_url=None`), but the
/// structural comparison builds one.
pub const TRANSCODE_TITLE: &str = "embyx-manager transcode";

const AUTH_SESSION_OPENAPI: &str = r#"{
  "responses": {"200": {"description": "Successful Response", "content": {"application/json": {"schema":
    {"additionalProperties": {"type": "boolean"}, "type": "object"}}}}},
  "security": [{"HTTPBearer": []}]
}"#;

const HEALTH_OPENAPI: &str = r#"{
  "responses": {"200": {"description": "Successful Response", "content": {"application/json": {"schema": {}}}}}
}"#;

/// `create_app`: the routes in FastAPI's order (the OpenAPI document's own route first,
/// then the login probe, health, and the features' routers), the SPA mount last.
pub fn create_app(parts: AppParts) -> Result<App, String> {
    let mut reserved: Vec<&str> = parts
        .feature_health
        .iter()
        .map(|(name, _)| name.as_str())
        .filter(|name| RESERVED_HEALTH_KEYS.contains(name))
        .collect();
    if !reserved.is_empty() {
        reserved.sort_unstable();
        let listed = reserved
            .iter()
            .map(|name| format!("'{name}'"))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!("feature health keys are reserved by the app: [{listed}]"));
    }
    let auth = MutationAuth::new(parts.api_token.as_deref());
    let auth_required = auth.required();

    let document: Arc<OnceLock<Bytes>> = Arc::new(OnceLock::new());
    let served_document = Arc::clone(&document);
    let mut routes = vec![Route {
        path: "/openapi.json".to_owned(),
        // A plain Starlette route: HEAD comes with GET.
        methods: vec![Method::GET, Method::HEAD],
        handler: handler(move |_request| {
            let document = Arc::clone(&served_document);
            async move {
                let body = document.get().cloned().unwrap_or_default();
                Ok(with_body(StatusCode::OK, Some("application/json"), body))
            }
        }),
        openapi: None,
    }];

    let session_auth = auth.clone();
    routes.push(Route {
        path: "/api/auth/session".to_owned(),
        methods: vec![Method::GET],
        handler: handler(move |request| {
            let auth = session_auth.clone();
            async move {
                if let Some(refusal) = auth.refuse(&request) {
                    return refusal;
                }
                let mut body = Dict::new();
                body.insert("auth_required", Value::Bool(auth_required));
                Ok(json(StatusCode::OK, &Value::Dict(body)))
            }
        }),
        openapi: Some(AUTH_SESSION_OPENAPI),
    });

    let app_ready = Arc::clone(&parts.app_ready);
    let feature_health = Arc::new(parts.feature_health);
    routes.push(Route {
        path: "/api/health".to_owned(),
        methods: vec![Method::GET],
        handler: handler(move |_request| {
            let (app_ready, feature_health) = (Arc::clone(&app_ready), Arc::clone(&feature_health));
            async move {
                let database_ready = app_ready().await?;
                let mut payload = Dict::new();
                payload.insert("status", Value::from(if database_ready { "ok" } else { "not_ready" }));
                payload.insert("database", Value::Bool(database_ready));
                // Public, so the browser can show its login screen before any authorized call.
                payload.insert("auth_required", Value::Bool(auth_required));
                for (name, probe) in feature_health.iter() {
                    payload.insert(name.clone(), Value::Dict(probe().await?));
                }
                let status = if database_ready {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                };
                Ok(json(status, &Value::Dict(payload)))
            }
        }),
        openapi: Some(HEALTH_OPENAPI),
    });

    for router in parts.routers {
        routes.extend(router.into_routes());
    }
    let built = openapi::document(TITLE, VERSION, &routes);
    document.set(Bytes::from(built)).expect("the document is built once");

    let frontend = parts
        .frontend_dist
        .filter(|directory| directory.is_dir())
        .map(SpaStaticFiles::new);
    Ok(App::new(
        routes,
        frontend,
        Layers {
            max_request_bytes: Some(parts.max_request_bytes),
            api_no_store: true,
        },
    ))
}

/// `create_transcode_app(router)`: the worker's endpoints and nothing else, with no size
/// limit, no `no-store` and no OpenAPI document (`openapi_url=None`).
pub fn create_transcode_app(router: ApiRouter) -> App {
    App::new(
        router.into_routes(),
        None,
        Layers {
            max_request_bytes: None,
            api_no_store: false,
        },
    )
}

#[cfg(test)]
mod tests;
