//! Playlist endpoints: the lists the sync manages, their gaps, the enable flag, and filling a
//! gap.
//!
//! Python origin: `monitor/playlists_api.py`. Filling hands a list's missing codes to the same
//! intake the manual source and the fill actor use, under a `playlist:<key>` source label,
//! into one directory: the playlists section's own, or failing that the RSS category named
//! like a ranking. A fill runs in the background (a list can lack hundreds of titles, each a
//! library lookup through the CloudDrive mount, more than a request behind Cloudflare's
//! 120-second proxy timeout can wait for); the request validates the target and answers at
//! once, and the page polls the fill's progress. Each title is only recorded in the ledger
//! (`queue`), so the tracker's background pass resolves and submits it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use embyx_core::config::models::{PlaylistsConfig, RssConfig};
use embyx_core::pyjson::{Dict, Value};
use embyx_monitor::acquisitions::{AcquisitionRepository, AcquisitionState, Ledger, LedgerError, playlist_source};
use embyx_monitor::manual::{ManualEntry, SubmitOptions};
use embyx_monitor::playlists::{PlaylistRecord, PlaylistRepository, PlaylistSourceState};
use embyx_monitor::support::{BoxFuture, StoreError};
use http::{Method, StatusCode};
use indexmap::IndexMap;

use crate::auth::MutationAuth;
use crate::body::{invalid_request, read_json};
use crate::http::{Answer, Internal, Request, json};
use crate::monitor_api::{ManualIntake, api_error, manual_refusal};
use crate::params::{model_fields, opt_str, py_datetime, py_opt_datetime};
use crate::routing::{ApiRouter, handler};

mod openapi;

/// `LOGGER`'s name.
pub const LOGGER: &str = "embyx-manager.playlists";

/// `NO_FILL_DIRECTORY`.
pub const NO_FILL_DIRECTORY: &str = "no fill directory: set playlists.task_dir_path or an RSS category labelled Rank";

/// `resolve_fill_dir(playlists, rss)`: where a fill goes, the configured directory else the
/// ranking category's; with the reason when neither.
pub fn resolve_fill_dir(playlists: &PlaylistsConfig, rss: &RssConfig) -> (Option<String>, Option<String>) {
    if !playlists.task_dir_path.is_empty() {
        return (Some(playlists.task_dir_path.clone()), None);
    }
    for category in &rss.categories {
        if category.label.to_lowercase().contains("rank") || category.label.contains('榜') {
            return (Some(category.task_dir_path.clone()), None);
        }
    }
    (None, Some(NO_FILL_DIRECTORY.to_owned()))
}

// -- the collaborators ---------------------------------------------------------------------

/// What the router asks the playlist table (`PlaylistRepository`).
pub trait PlaylistCatalog: Send + Sync {
    fn list(&self) -> BoxFuture<'_, Result<Vec<PlaylistRecord>, StoreError>>;
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<PlaylistRecord>, StoreError>>;
    fn source(&self) -> BoxFuture<'_, Result<Option<PlaylistSourceState>, StoreError>>;
    fn set_enabled<'a>(
        &'a self,
        key: &'a str,
        enabled: bool,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Option<PlaylistRecord>, StoreError>>;
}

impl PlaylistCatalog for PlaylistRepository {
    fn list(&self) -> BoxFuture<'_, Result<Vec<PlaylistRecord>, StoreError>> {
        Box::pin(Self::list(self))
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<PlaylistRecord>, StoreError>> {
        Box::pin(Self::get(self, key))
    }

    fn source(&self) -> BoxFuture<'_, Result<Option<PlaylistSourceState>, StoreError>> {
        Box::pin(Self::source(self))
    }

    fn set_enabled<'a>(
        &'a self,
        key: &'a str,
        enabled: bool,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Option<PlaylistRecord>, StoreError>> {
        Box::pin(Self::set_enabled(self, key, enabled, now))
    }
}

/// What the gap listing asks the ledger: `states_for(avids)`.
pub trait TrackedStates: Send + Sync {
    fn states_for<'a>(
        &'a self,
        avids: &'a [String],
    ) -> BoxFuture<'a, Result<BTreeMap<String, AcquisitionState>, LedgerError>>;
}

impl TrackedStates for AcquisitionRepository {
    fn states_for<'a>(
        &'a self,
        avids: &'a [String],
    ) -> BoxFuture<'a, Result<BTreeMap<String, AcquisitionState>, LedgerError>> {
        Ledger::states_for(self, avids)
    }
}

/// `task_dir()`: `(directory, None)` when a fill can go somewhere, `(None, reason)` otherwise.
pub type FillTarget = Arc<dyn Fn() -> (Option<String>, Option<String>) + Send + Sync>;

/// `PlaylistFillApi`: the fill route's needs; the route is mounted only when it is supplied.
#[derive(Clone)]
pub struct PlaylistFillApi {
    pub manual: Arc<dyn ManualIntake>,
    pub task_dir: FillTarget,
}

impl std::fmt::Debug for PlaylistFillApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlaylistFillApi").finish_non_exhaustive()
    }
}

// -- views -------------------------------------------------------------------------------------

fn dict(entries: Vec<(&str, Value)>) -> Value {
    let mut dict = Dict::new();
    for (key, value) in entries {
        dict.insert(key, value);
    }
    Value::Dict(dict)
}

fn count(len: usize) -> Value {
    Value::Int(i128::try_from(len).unwrap_or(i128::MAX))
}

/// `PlaylistView.from_record`.
fn playlist_view(record: &PlaylistRecord) -> Value {
    dict(vec![
        ("key", Value::from(record.key.as_str())),
        ("kind", Value::Int(record.kind.into())),
        ("note", Value::from(record.note.as_str())),
        ("name", Value::from(record.name.as_str())),
        ("enabled", Value::Bool(record.enabled)),
        ("total", count(record.entries.len())),
        ("present", count(record.present.len())),
        ("missing", count(record.missing.len())),
        ("emby_playlist_id", opt_str(record.emby_playlist_id.as_deref())),
        ("last_synced_at", py_opt_datetime(record.last_synced_at)),
        ("last_error", opt_str(record.last_error.as_deref())),
    ])
}

/// `FillState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FillState {
    Running,
    Done,
    Failed,
}

impl FillState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }
}

/// `_FillJob`: one list's fill as it runs, kept after it ends so the page can show the
/// outcome.
#[derive(Debug)]
struct FillJob {
    key: String,
    task_dir_path: String,
    total: usize,
    started_at: DateTime<Utc>,
    state: FillState,
    /// `FillEntryView(avid, outcome)` per handled code.
    items: Vec<(String, &'static str)>,
    error: Option<String>,
    finished_at: Option<DateTime<Utc>>,
}

impl FillJob {
    fn record(&mut self, entry: &ManualEntry) {
        let avid = entry.avid.clone().unwrap_or_else(|| entry.text.clone());
        self.items.push((avid, entry.outcome.as_str()));
    }

    fn finish(&mut self, error: Option<String>) {
        self.state = if error.is_some() {
            FillState::Failed
        } else {
            FillState::Done
        };
        self.error = error;
        self.finished_at = Some(Utc::now());
    }

    fn view(&self) -> Value {
        let mut tally: IndexMap<&str, i128> = IndexMap::new();
        let mut items = Vec::new();
        for (avid, outcome) in &self.items {
            *tally.entry(*outcome).or_insert(0) += 1;
            items.push(dict(vec![
                ("avid", Value::from(avid.as_str())),
                ("outcome", Value::from(*outcome)),
            ]));
        }
        let mut counts = Dict::new();
        for (outcome, seen) in tally {
            counts.insert(outcome, Value::Int(seen));
        }
        dict(vec![
            ("key", Value::from(self.key.as_str())),
            ("task_dir_path", Value::from(self.task_dir_path.as_str())),
            ("state", Value::from(self.state.as_str())),
            ("done", count(self.items.len())),
            ("total", count(self.total)),
            ("items", Value::List(items)),
            ("counts", Value::Dict(counts)),
            ("error", opt_str(self.error.as_deref())),
            ("started_at", py_datetime(self.started_at)),
            ("finished_at", py_opt_datetime(self.finished_at)),
        ])
    }
}

type Job = Arc<Mutex<FillJob>>;

fn locked(job: &Job) -> std::sync::MutexGuard<'_, FillJob> {
    job.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

// -- the router ----------------------------------------------------------------------------------

struct Playlists {
    repository: Arc<dyn PlaylistCatalog>,
    auth: MutationAuth,
    ledger: Option<Arc<dyn TrackedStates>>,
    fill: Option<PlaylistFillApi>,
    /// The latest fill of each list since the server started, in the order lists were
    /// first filled (a Python dict's).
    fills: Mutex<IndexMap<String, Job>>,
}

impl Playlists {
    fn fills(&self) -> std::sync::MutexGuard<'_, IndexMap<String, Job>> {
        self.fills.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

type Shared = Arc<Playlists>;

fn route<F, Fut>(router: ApiRouter, state: &Shared, method: Method, path: &str, function: F) -> ApiRouter
where
    F: Fn(Shared, Request) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Answer> + Send + 'static,
{
    let state = Arc::clone(state);
    let text = openapi::operation(method.as_str(), path);
    router.route(
        method,
        path,
        Some(text),
        handler(move |request| function(Arc::clone(&state), request)),
    )
}

/// `create_playlists_router(repository, mutation_auth=, ledger=, fill=)`.
pub fn create_playlists_router(
    repository: Arc<dyn PlaylistCatalog>,
    auth: MutationAuth,
    ledger: Option<Arc<dyn TrackedStates>>,
    fill: Option<PlaylistFillApi>,
) -> ApiRouter {
    let has_fill = fill.is_some();
    let state: Shared = Arc::new(Playlists {
        repository,
        auth,
        ledger,
        fill,
        fills: Mutex::new(IndexMap::new()),
    });
    let mut router = ApiRouter::new("/api/playlists");
    router = route(router, &state, Method::GET, "", list_playlists);
    router = route(router, &state, Method::GET, "/{key}/missing", list_missing);
    router = route(router, &state, Method::PATCH, "/{key}", update_playlist);
    if has_fill {
        router = route(router, &state, Method::POST, "/{key}/fill", fill_playlist);
        router = route(router, &state, Method::GET, "/{key}/fill", get_fill);
    }
    router
}

fn internal(error: impl std::fmt::Display) -> Internal {
    Internal::new(error)
}

/// `_load(repository, key)`: the record, or 404 `unknown_playlist`.
async fn load(state: &Playlists, key: &str) -> Result<Option<PlaylistRecord>, Internal> {
    state.repository.get(key).await.map_err(internal)
}

async fn list_playlists(state: Shared, _request: Request) -> Answer {
    let records = state.repository.list().await.map_err(internal)?;
    let source = state.repository.source().await.map_err(internal)?;
    let (fill_dir, fill_reason) = match &state.fill {
        Some(fill) => (fill.task_dir)(),
        None => (None, Some("filling is not available".to_owned())),
    };
    let fills: Vec<Value> = state.fills().values().map(|job| locked(job).view()).collect();
    let source = source.map_or(Value::Null, |source| {
        dict(vec![
            ("database_name", Value::from(source.database_name.as_str())),
            ("fetched_at", py_datetime(source.fetched_at)),
        ])
    });
    Ok(json(
        StatusCode::OK,
        &dict(vec![
            ("items", Value::List(records.iter().map(playlist_view).collect())),
            ("source", source),
            ("fill_task_dir", opt_str(fill_dir.as_deref())),
            ("fill_reason", opt_str(fill_reason.as_deref())),
            ("fills", Value::List(fills)),
        ]),
    ))
}

async fn list_missing(state: Shared, request: Request) -> Answer {
    let Some(record) = load(&state, request.param("key")).await? else {
        return Ok(api_error(404, "unknown_playlist"));
    };
    let states = match &state.ledger {
        Some(ledger) if !record.missing.is_empty() => ledger.states_for(&record.missing).await.map_err(internal)?,
        _ => BTreeMap::new(),
    };
    let items = record
        .entries
        .iter()
        .filter(|entry| record.missing.contains(&entry.avid))
        .map(|entry| {
            dict(vec![
                ("rank", count(entry.rank)),
                ("avid", Value::from(entry.avid.as_str())),
                ("title", Value::from(entry.title.as_str())),
                (
                    "tracked",
                    opt_str(states.get(&entry.avid).map(|tracked| tracked.as_str())),
                ),
            ])
        })
        .collect();
    Ok(json(
        StatusCode::OK,
        &dict(vec![
            ("key", Value::from(record.key.as_str())),
            ("name", Value::from(record.name.as_str())),
            ("items", Value::List(items)),
        ]),
    ))
}

async fn update_playlist(state: Shared, mut request: Request) -> Answer {
    let body = match read_json(&mut request).await {
        Ok(body) => body,
        Err(response) => return Ok(response),
    };
    if let Some(refusal) = state.auth.refuse(&request) {
        return refusal;
    }
    let enabled = model_fields(&body, &["enabled"]).and_then(|fields| {
        embyx_core::config::lax::lax_bool(fields.get("enabled")?, embyx_core::config::lax::Mode::Python).ok()
    });
    let Some(enabled) = enabled else {
        return Ok(invalid_request());
    };
    match state
        .repository
        .set_enabled(request.param("key"), enabled, Utc::now())
        .await
        .map_err(internal)?
    {
        Some(record) => Ok(json(StatusCode::OK, &playlist_view(&record))),
        None => Ok(api_error(404, "unknown_playlist")),
    }
}

/// `run_fill(job, avids)`: the batch, its end recorded on the job.
async fn run_fill(fill: PlaylistFillApi, job: Job, avids: Vec<String>) {
    let (key, task_dir_path) = {
        let job = locked(&job);
        (job.key.clone(), job.task_dir_path.clone())
    };
    let recorder = Arc::clone(&job);
    let options = SubmitOptions {
        source: playlist_source(&key),
        limit: None,
        queue: true,
        on_entry: Some(Arc::new(move |entry: &ManualEntry| locked(&recorder).record(entry))),
    };
    let outcome = fill.manual.submit(&avids, &task_dir_path, options).await;
    let error = match outcome {
        Ok(_) => None,
        Err(error) => {
            if let Some(code) = error.code() {
                Some(code.to_owned())
            } else {
                tracing::error!(target: "embyx-manager.playlists", "filling playlist {key} failed\n{error}");
                Some("fill_failed".to_owned())
            }
        }
    };
    locked(&job).finish(error);
}

/// Start queueing every title the list lacks, as one batch, into the fill directory.
///
/// Answers at once with the fill's progress; a fill already running for the list is
/// reported rather than started twice.
async fn fill_playlist(state: Shared, request: Request) -> Answer {
    if let Some(refusal) = state.auth.refuse(&request) {
        return refusal;
    }
    let fill = state.fill.clone().expect("mounted with the fill");
    let Some(record) = load(&state, request.param("key")).await? else {
        return Ok(api_error(404, "unknown_playlist"));
    };
    let running = state.fills().get(&record.key).cloned();
    if let Some(running) = running {
        let job = locked(&running);
        if job.state == FillState::Running {
            return Ok(json(StatusCode::ACCEPTED, &job.view()));
        }
    }
    let (task_dir, _reason) = (fill.task_dir)();
    let Some(task_dir) = task_dir else {
        // The list endpoint carries the reason; the refusal only needs the code.
        return Ok(api_error(422, "fill_directory_unavailable"));
    };
    let task_dir = match fill.manual.check_target(&task_dir).await {
        Ok(task_dir) => task_dir,
        Err(error) => return manual_refusal(error),
    };
    let job: Job = Arc::new(Mutex::new(FillJob {
        key: record.key.clone(),
        task_dir_path: task_dir,
        total: record.missing.len(),
        started_at: Utc::now(),
        state: FillState::Running,
        items: Vec::new(),
        error: None,
        finished_at: None,
    }));
    let view = locked(&job).view();
    state.fills().insert(record.key.clone(), Arc::clone(&job));
    // Detached, as Python's `asyncio.create_task`: a fill only records rows in the ledger, so
    // ending it with the process leaves nothing half-sent (D107).
    tokio::spawn(run_fill(fill, job, record.missing.clone()));
    Ok(json(StatusCode::ACCEPTED, &view))
}

/// The list's latest fill since the server started.
async fn get_fill(state: Shared, request: Request) -> Answer {
    let job = state.fills().get(request.param("key")).cloned();
    match job {
        Some(job) => Ok(json(StatusCode::OK, &locked(&job).view())),
        None => Ok(api_error(404, "no_fill")),
    }
}

#[cfg(test)]
mod tests;
