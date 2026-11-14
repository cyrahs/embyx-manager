//! The API shell scenarios (`tools/scenarios/specs/api/`): the Rust port of
//! `tools/scenarios/operations/api.py` assembles the shell as `build_app` does (the config
//! store on the migrated database, the readiness probe, the token, the size limit, the
//! config router, the SPA root when `frontend` is set, the monitor and playlists routers
//! when `monitor` is set), sends each request through the app
//! as Starlette's `TestClient` does (`Host: testserver`), and must render the bytes the
//! Python baseline committed under `goldens/scenarios/api/`.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::body::Body;
use embyx_api::app::{AppParts, ReadyProbe, create_app, create_transcode_app};
use embyx_api::auth::MutationAuth;
use embyx_api::config_api::{ConfigApiOptions, create_config_router};
use embyx_api::http::Internal;
use embyx_api::monitor_api::{AcquisitionApi, SubscriptionsApi, create_monitor_router};
use embyx_api::playlists_api::create_playlists_router;
use embyx_api::routing::{ApiRouter, App};
use embyx_clients::scenario::ScenarioFakes;
use embyx_core::config::models::RssConfig;
use embyx_core::pyjson::{self, Dict, DumpOptions, Value};
use embyx_core::scenario::{self, Parts, Recorder, Seeded, Spec};
use embyx_db::config_store::ConfigStore;
use embyx_db::database::Database;
use embyx_db::testdb::TestDatabase;
use embyx_monitor::acquisitions::AcquisitionRepository;
use embyx_monitor::playlists::PlaylistRepository;
use embyx_monitor::runs::PipelineRunRepository;
use embyx_monitor::scheduler::{MappingFactory, MonitorScheduler, Ready, Runner, SchedulerParts};
use embyx_monitor::subscriptions::SubscriptionRepository;
use http_body_util::BodyExt as _;

/// `STATIC_MTIME_NS`: 2026-01-01T00:00:00Z.
const STATIC_MTIME_SECONDS: u64 = 1_767_225_600;

/// `REPORTED_HEADERS`, in order.
const REPORTED_HEADERS: [&str; 10] = [
    "content-type",
    "content-length",
    "cache-control",
    "www-authenticate",
    "allow",
    "location",
    "accept-ranges",
    "content-range",
    "etag",
    "last-modified",
];

/// `_substitute`: replace in every string value, recursively (keys stay).
fn substitute(value: &Value, replacements: &[(String, String)]) -> Value {
    match value {
        Value::Str(text) => {
            let mut text = text.clone();
            for (from, to) in replacements {
                text = text.replace(from.as_str(), to);
            }
            Value::Str(text)
        }
        Value::List(items) => Value::List(items.iter().map(|item| substitute(item, replacements)).collect()),
        Value::Dict(dict) => {
            let mut out = Dict::new();
            for (key, item) in dict.iter() {
                out.insert(key.clone(), substitute(item, replacements));
            }
            Value::Dict(out)
        }
        other => other.clone(),
    }
}

fn text(value: &Value) -> String {
    match value {
        Value::Str(text) => text.clone(),
        other => pyjson::dumps(other, &DumpOptions::compact()).expect("dumps"),
    }
}

/// `_request_parts` and `client.request(...)`.
fn build_request(request: &Value) -> http::Request<Body> {
    let method = request.get("method").and_then(Value::as_str).expect("a method");
    let path = request.get("path").and_then(Value::as_str).expect("a path");
    let mut headers: Vec<(String, String)> = request
        .get("headers")
        .and_then(Value::as_dict)
        .map(|headers| {
            headers
                .iter()
                .map(|(name, value)| (name.as_str().expect("a name").to_lowercase(), text(value)))
                .collect()
        })
        .unwrap_or_default();
    let mut content_type = request.get("content_type").and_then(Value::as_str).map(str::to_owned);
    let mut content: Option<Vec<u8>> = None;
    if let Some(json) = request.get("json") {
        content = Some(
            pyjson::dumps(json, &DumpOptions::compact())
                .expect("dumps")
                .into_bytes(),
        );
        content_type.get_or_insert_with(|| "application/json".to_owned());
    } else if let Some(body) = request.get("body") {
        content = Some(text(body).into_bytes());
    } else if let Some(hex_text) = request.get("body_hex").and_then(Value::as_str) {
        content = Some(
            (0..hex_text.len())
                .step_by(2)
                .map(|at| u8::from_str_radix(&hex_text[at..at + 2], 16).expect("hex"))
                .collect(),
        );
    } else if let Some(Value::Int(size)) = request.get("body_size") {
        content = Some(vec![b'x'; usize::try_from(*size).expect("a size")]);
    } else if let Some(Value::Int(depth)) = request.get("body_nested") {
        let depth = usize::try_from(*depth).expect("a depth");
        let mut bytes = b"{\"values\":".to_vec();
        bytes.extend(std::iter::repeat_n(b'[', depth));
        bytes.extend(std::iter::repeat_n(b']', depth));
        bytes.push(b'}');
        content = Some(bytes);
        content_type.get_or_insert_with(|| "application/json".to_owned());
    }
    if let Some(content_type) = content_type.filter(|content_type| !content_type.is_empty()) {
        headers.retain(|(name, _)| name != "content-type");
        headers.push(("content-type".to_owned(), content_type));
    }
    let mut builder = http::Request::builder()
        .method(method)
        .uri(path)
        .header("host", "testserver")
        .header("accept", "*/*")
        .header("accept-encoding", "gzip, deflate")
        .header("connection", "keep-alive")
        .header("user-agent", "testclient");
    if let Some(content) = &content {
        builder = builder.header("content-length", content.len().to_string());
    }
    for (name, value) in &headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    builder
        .body(content.map_or_else(Body::empty, Body::from))
        .expect("a request")
}

/// `_response`.
async fn report(response: http::Response<Body>) -> Value {
    let (parts, body) = response.into_parts();
    let bytes = body.collect().await.expect("the body").to_bytes();
    let mut headers = Dict::new();
    for name in REPORTED_HEADERS {
        if let Some(value) = parts.headers.get(name) {
            headers.insert(name, Value::Str(embyx_api::http::latin1(value.as_bytes())));
        }
    }
    let mut body_text = String::from_utf8_lossy(&bytes).into_owned();
    if let Some(Value::Str(content_type)) = headers.get("content-type").cloned()
        && let Some(at) = content_type.find("boundary=")
    {
        let boundary: String = content_type[at + "boundary=".len()..]
            .chars()
            .take_while(|ch| ch.is_ascii_digit() || ('a'..='f').contains(ch))
            .collect();
        if !boundary.is_empty() {
            headers.insert(
                "content-type",
                Value::Str(content_type.replace(&boundary, "<boundary>")),
            );
            body_text = body_text.replace(&boundary, "<boundary>");
        }
    }
    let is_json = headers
        .get("content-type")
        .and_then(Value::as_str)
        .is_some_and(|content_type| content_type.starts_with("application/json"));
    let body = if is_json && !body_text.is_empty() {
        pyjson::loads(&body_text).expect("a JSON body")
    } else {
        Value::Str(body_text)
    };
    let mut reported = Dict::new();
    reported.insert("status", Value::Int(i128::from(parts.status.as_u16())));
    reported.insert("headers", Value::Dict(headers));
    reported.insert("body", body);
    Value::Dict(reported)
}

/// `_mask`: a named key's non-null value, anywhere in the body, reads `<set>`.
fn mask(value: &Value, keys: &[String]) -> Value {
    match value {
        Value::List(items) => Value::List(items.iter().map(|item| mask(item, keys)).collect()),
        Value::Dict(fields) => {
            let mut masked = Dict::new();
            for (key, item) in fields.iter() {
                let named = key.as_str().is_some_and(|key| keys.iter().any(|name| name == key));
                let item = if named && *item != Value::Null {
                    Value::Str("<set>".to_owned())
                } else {
                    mask(item, keys)
                };
                masked.insert(key.clone(), item);
            }
            Value::Dict(masked)
        }
        other => other.clone(),
    }
}

/// `_send`.
async fn send(app: &App, requests: &[Value], replacements: &[(String, String)], mask_keys: &[String]) -> Value {
    let mut results = Vec::new();
    for raw in requests {
        let request = substitute(raw, replacements);
        let Value::Dict(mut result) = report(app.call(build_request(&request), None).await).await else {
            unreachable!("a report is an object");
        };
        let body = mask(result.get("body").expect("a body"), mask_keys);
        result.insert("body", body);
        results.push(Value::Dict(result));
    }
    Value::List(results)
}

/// `_monitor_routers`: the monitor and playlists routers over the real repositories, a
/// scheduler never started whose pipelines all report `not_ready`, no CloudDrive, no AVBase
/// and no fill directory.
fn monitor_routers(
    store: &Arc<ConfigStore>,
    database: &Database,
    auth: &MutationAuth,
    not_ready: &str,
) -> [ApiRouter; 2] {
    let never: Runner = Arc::new(|_scope| Box::pin(async { panic!("a scenario never runs a pipeline") }));
    let never_build: MappingFactory = Arc::new(|| panic!("a scenario never runs a pipeline"));
    let reason = not_ready.to_owned();
    let ready: Ready = Arc::new(move || Some(reason.clone()));
    let runs = Arc::new(PipelineRunRepository::new(database.clone()));
    let ledger = Arc::new(AcquisitionRepository::new(database.clone()));
    let mut parts = SchedulerParts::new(
        Arc::clone(store) as Arc<dyn embyx_monitor::scheduler::SchedulerConfig>,
        Arc::clone(&runs) as Arc<_>,
        (Arc::clone(&never), Arc::clone(&never), never_build),
        (Arc::clone(&ready), Arc::clone(&ready), Arc::clone(&ready)),
    );
    parts.playlists_runner = Some(never);
    parts.playlists_ready = Some(Arc::clone(&ready));
    let scheduler = MonitorScheduler::new(parts);
    let mut acquisitions = AcquisitionApi::new(Arc::clone(&ledger) as Arc<_>);
    acquisitions.tracker_ready = Some(ready);
    let categories_store = Arc::clone(store);
    let monitor = create_monitor_router(
        Arc::new(scheduler),
        runs,
        auth.clone(),
        Some(acquisitions),
        Some(SubscriptionsApi {
            repository: Arc::new(SubscriptionRepository::new(database.clone())),
            categories: Arc::new(move || {
                categories_store
                    .get::<RssConfig>()
                    .categories
                    .into_iter()
                    .map(|category| category.label)
                    .collect()
            }),
            resolve_talent: None,
        }),
    );
    let playlists = create_playlists_router(
        Arc::new(PlaylistRepository::new(database.clone())),
        auth.clone(),
        Some(ledger),
        None,
    );
    [monitor, playlists]
}

/// `_pin_static_mtimes`.
fn pin_mtimes(directory: &Path) {
    let when = SystemTime::UNIX_EPOCH + Duration::from_secs(STATIC_MTIME_SECONDS);
    for entry in std::fs::read_dir(directory).expect("read a directory") {
        let path = entry.expect("an entry").path();
        if path.is_dir() {
            pin_mtimes(&path);
        } else {
            let file = std::fs::File::options().write(true).open(&path).expect("open");
            file.set_times(std::fs::FileTimes::new().set_accessed(when).set_modified(when))
                .expect("set the times");
        }
    }
}

async fn run_scenario(name: &str) {
    let spec = Spec::load(name);
    let seeded = Seeded::new(&spec);
    let database = TestDatabase::create().await;
    embyx_db::scenario::seed(database.pool(), &spec).await;
    let recorder = Recorder::new();
    let fakes = ScenarioFakes::start(&spec, &recorder).await;
    assert_eq!(spec.operation(), "api.exchange", "{name}");
    let args = spec.section("args");
    let replacements = vec![
        ("{clouddrive}".to_owned(), fakes.clouddrive.address()),
        ("{emby}".to_owned(), fakes.emby.url.clone()),
    ];
    let frontend = args.get("frontend") == Some(&Value::Bool(true));
    if frontend {
        pin_mtimes(seeded.root("static"));
    }

    let app_database = database.database();
    let store = Arc::new(ConfigStore::new(app_database.clone()));
    let ready_database = app_database.clone();
    let app_ready: ReadyProbe = Arc::new(move || {
        let database = ready_database.clone();
        Box::pin(async move { embyx_db::health::health_check(&database).await.map_err(Internal::new) })
    });
    let api_token = args.get("api_token").and_then(Value::as_str);
    let max_request_bytes = match args.get("max_request_bytes") {
        Some(Value::Int(bytes)) => usize::try_from(*bytes).expect("a size"),
        _ => 65_536,
    };
    let auth = MutationAuth::new(api_token);
    let mut routers = vec![create_config_router(
        Arc::clone(&store),
        auth.clone(),
        ConfigApiOptions::default(),
    )];
    if args.get("monitor") == Some(&Value::Bool(true)) {
        let not_ready = args.get("not_ready").and_then(Value::as_str).unwrap_or("not ready");
        routers.extend(monitor_routers(&store, &app_database, &auth, not_ready));
    }
    let mask_keys: Vec<String> = args
        .get("mask_keys")
        .and_then(Value::as_list)
        .unwrap_or_default()
        .iter()
        .map(|key| key.as_str().expect("a key").to_owned())
        .collect();
    let app = create_app(AppParts {
        app_ready,
        routers,
        feature_health: Vec::new(),
        api_token: api_token.map(str::to_owned),
        max_request_bytes,
        frontend_dist: frontend.then(|| seeded.root("static").to_owned()),
    })
    .expect("an app");
    let transcode_app = create_transcode_app(ApiRouter::new(""));

    let list = |key: &str| args.get(key).and_then(Value::as_list).unwrap_or_default().to_vec();
    // `with TestClient(app)`: the runtime lifespan loads the store, and closes the database.
    store.load().await.expect("load");
    let main = send(&app, &list("requests"), &replacements, &mask_keys).await;
    app_database.close().await;
    let transcode = send(&transcode_app, &list("transcode_requests"), &replacements, &[]).await;
    let mut result = Dict::new();
    result.insert("responses", main);
    result.insert("transcode_responses", transcode);
    let back: Vec<(String, String)> = replacements.into_iter().map(|(from, to)| (to, from)).collect();
    let result = substitute(&Value::Dict(result), &back);

    let rows = embyx_db::scenario::dump_rows(database.pool(), &spec).await;
    let fakes_dump = fakes.dump();
    drop(fakes);
    scenario::assert_transcript(Parts {
        spec: &spec,
        seeded: &seeded,
        outcome: scenario::result(result),
        calls: recorder.calls(),
        rows,
        fakes: fakes_dump,
    });
    database.drop_database().await;
}

/// The nesting cases recurse deep; run on a stack as large as the server's workers.
fn on_worker_stack(name: &'static str) {
    std::thread::Builder::new()
        .stack_size(embyx_api::serve::WORKER_STACK_BYTES)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime")
                .block_on(run_scenario(name));
        })
        .expect("a thread")
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

#[test]
fn api_shell_matches_the_python_transcript() {
    on_worker_stack("api/shell");
}

#[test]
fn api_open_matches_the_python_transcript() {
    on_worker_stack("api/open");
}

#[test]
fn api_monitor_matches_the_python_transcript() {
    on_worker_stack("api/monitor");
}
