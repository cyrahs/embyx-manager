//! Merge endpoints: the multi-part titles the library holds, and the tasks merging them.
//!
//! Python origin: the routes of `merge/api.py` (`create_merge_router`, `_add_task_routes`,
//! `TitleView`, `TitlesView`, `TaskView`, `TasksView`, `_view`). The scan cache, `enqueue`,
//! `current_title` and the auto-merge status are `embyx_merge::catalog`'s. The task routes are
//! mounted only with [`MergeTasksApi`]; the mutations take the shared bearer token, checked
//! after a JSON body is read and before anything is validated (FastAPI's order).

use std::sync::Arc;

use embyx_core::config::lax::{Mode, lax_bool};
use embyx_core::pyjson::{Dict, Value};
use embyx_merge::catalog::{AutoStatus, EnqueueError, MergeCatalog, TitleScanCache, current_title, enqueue, scan};
use embyx_merge::detect::{MultipartTitle, SourceBasis, TitleProblem, resolve_source, route_sources};
use embyx_merge::service::{ActionError, MergeService, StepError};
use embyx_merge::tasks::{MergeState, MergeTask};
use embyx_merge::transcode::WorkerReport;
use http::{Method, StatusCode};

use crate::auth::MutationAuth;
use crate::body::{invalid_request, merge_request, read_json};
use crate::http::{Answer, Internal, Request, json};
use crate::monitor_api::api_error;
use crate::params::{int_param, opt_str, py_datetime, py_opt_datetime, query_get, str_list};
use crate::routing::{ApiRouter, handler};

mod openapi;

/// `merge.api`'s logger.
pub const LOGGER: &str = "embyx_manager.merge.api";

/// `auto_status()`: where automatic merging stands.
pub type AutoStatusSource = Arc<dyn Fn() -> AutoStatus + Send + Sync>;

/// `MergeTasksApi`: what the task endpoints need; they are mounted only when this is supplied.
/// The repository is the service's own.
#[derive(Clone)]
pub struct MergeTasksApi {
    pub service: MergeService,
    /// Where automatic merging stands; `None` where it is not wired in.
    pub auto_status: Option<AutoStatusSource>,
}

impl std::fmt::Debug for MergeTasksApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergeTasksApi").finish_non_exhaustive()
    }
}

struct Merge {
    catalog: MergeCatalog,
    cache: TitleScanCache,
    tasks: Option<MergeTasksApi>,
    auth: MutationAuth,
}

type Shared = Arc<Merge>;

impl Merge {
    fn tasks(&self) -> &MergeTasksApi {
        self.tasks.as_ref().expect("the task routes are mounted with the tasks")
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

fn opt_int(value: Option<i64>) -> Value {
    value.map_or(Value::Null, |value| Value::Int(i128::from(value)))
}

/// `_view(archive, title, ledger_task_dir)`: a `TitleView`.
fn title_view(
    archive: &embyx_core::config::models::ArchiveConfig,
    title: &MultipartTitle,
    ledger: Option<&str>,
) -> Value {
    let route = resolve_source(archive, title, ledger);
    dict(vec![
        ("avid", Value::from(title.avid.as_str())),
        ("directory", Value::from(title.directory.as_str())),
        ("part_count", count(title.part_count())),
        (
            "parts",
            Value::List(title.parts.iter().map(|part| Value::Int(part.index)).collect()),
        ),
        (
            "missing",
            Value::List(title.missing.iter().map(|index| Value::Int(*index)).collect()),
        ),
        ("library_dir", opt_str(title.library_dir.as_deref())),
        ("brand", opt_str(title.brand.as_deref())),
        ("problem", opt_str(title.problem.map(TitleProblem::as_str))),
        ("source", opt_str(route.source.as_deref())),
        ("source_basis", opt_str(route.basis.map(SourceBasis::as_str))),
        ("stackable", Value::Bool(title.stackable())),
        ("mergeable", Value::Bool(title.complete())),
    ])
}

/// `TaskView.from_task(task, home=..., transcode=...)`.
fn task_view(task: &MergeTask, home: bool, transcode: Option<Value>) -> Value {
    // A task failed while replacing may be removed only once the library holds the title
    // whole; the cancel itself checks that.
    let cancellable = task.state.is_cancellable()
        && !(task.state == MergeState::Failed && task.failed_state == Some(MergeState::Replacing));
    dict(vec![
        ("id", Value::Int(i128::from(task.id))),
        ("avid", Value::from(task.avid.as_str())),
        ("source", Value::from(task.source.as_str())),
        ("library_dir", Value::from(task.library_dir.as_str())),
        ("part_count", count(task.parts.len())),
        ("state", Value::from(task.state.as_str())),
        ("failed_state", opt_str(task.failed_state.map(MergeState::as_str))),
        ("phase", opt_str(task.phase.as_deref())),
        ("progress", task.progress.map_or(Value::Null, Value::Float)),
        ("merged_bytes", opt_int(task.merged_bytes)),
        ("uploaded_bytes", opt_int(task.uploaded_bytes)),
        ("upload_attempts", Value::Int(i128::from(task.upload_attempts))),
        ("error", opt_str(task.error.as_deref())),
        ("notice", opt_str(task.notice.as_deref())),
        ("created_at", py_datetime(task.created_at)),
        ("updated_at", py_datetime(task.updated_at)),
        ("finished_at", py_opt_datetime(task.finished_at)),
        ("cancellable", Value::Bool(cancellable)),
        ("retryable", Value::Bool(task.state == MergeState::Failed)),
        ("transcoder", opt_str(task.transcoder.as_deref())),
        ("home", Value::Bool(home)),
        ("transcode", transcode.unwrap_or(Value::Null)),
        ("transcode_paused", Value::Bool(task.transcode_paused)),
    ])
}

/// `AutoStatus` as FastAPI writes it.
fn auto_view(status: &AutoStatus) -> Value {
    dict(vec![
        ("state", Value::from(status.state.as_str())),
        ("room", status.room.map_or(Value::Null, Value::Int)),
        (
            "skipped",
            Value::List(
                status
                    .skipped
                    .iter()
                    .map(|skipped| {
                        dict(vec![
                            ("avid", Value::from(skipped.avid.as_str())),
                            ("size", skipped.size.map_or(Value::Null, Value::Int)),
                            ("reason", Value::from(skipped.reason.as_str())),
                        ])
                    })
                    .collect(),
            ),
        ),
        ("skipped_count", count(status.skipped_count)),
        ("checked_at", py_opt_datetime(status.checked_at)),
    ])
}

/// `presence.view()`: `asdict(report)` with `online`, as a `dict[str, Any]` field writes it.
pub fn worker_view(report: &WorkerReport, online: bool) -> Value {
    dict(vec![
        ("seen_at", py_datetime(report.seen_at)),
        ("name", Value::from(report.name.as_str())),
        ("version", Value::from(report.version.as_str())),
        ("task_id", opt_int(report.task_id)),
        ("state", Value::from(report.state.as_str())),
        (
            "parts",
            Value::List(
                report
                    .parts
                    .iter()
                    .map(|part| {
                        dict(vec![
                            ("index", Value::Int(i128::from(part.index))),
                            ("fps", Value::Float(part.fps)),
                            ("position", Value::Float(part.position)),
                        ])
                    })
                    .collect(),
            ),
        ),
        ("upload_bps", Value::Float(report.upload_bps)),
        ("queue_chunks", Value::Int(i128::from(report.queue_chunks))),
        ("queue_bytes", Value::Int(i128::from(report.queue_bytes))),
        ("online", Value::Bool(online)),
    ])
}

// -- the router ----------------------------------------------------------------------------------

fn internal(error: &StepError) -> Internal {
    Internal::new(format!("{}: {error}", error.class_name()))
}

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

/// `create_merge_router(catalog, tasks=, mutation_auth=, cache=)`.
pub fn create_merge_router(
    catalog: MergeCatalog,
    tasks: Option<MergeTasksApi>,
    auth: MutationAuth,
    cache: Option<TitleScanCache>,
) -> ApiRouter {
    let has_tasks = tasks.is_some();
    let state: Shared = Arc::new(Merge {
        catalog,
        cache: cache.unwrap_or_default(),
        tasks,
        auth,
    });
    let mut router = ApiRouter::new("/api/merge");
    router = route(router, &state, Method::GET, "/titles", list_titles);
    if has_tasks {
        router = route(router, &state, Method::GET, "/tasks", list_tasks);
        router = route(router, &state, Method::POST, "/tasks/{task_id}/pause", pause_task);
        router = route(router, &state, Method::POST, "/tasks/{task_id}/resume", resume_task);
        router = route(router, &state, Method::POST, "/tasks/{task_id}/use-cpu", use_cpu);
        router = route(router, &state, Method::POST, "/tasks", create_task);
        router = route(router, &state, Method::POST, "/tasks/{task_id}/cancel", cancel_task);
        router = route(router, &state, Method::POST, "/tasks/{task_id}/retry", retry_task);
    }
    router
}

/// `GET /titles?refresh=`: every multi-part title of the last scan (a new one when asked).
async fn list_titles(state: Shared, request: Request) -> Answer {
    let refresh = match query_get(&request.query, "refresh") {
        None => false,
        Some(text) => match lax_bool(&Value::Str(text), Mode::Python) {
            Ok(refresh) => refresh,
            Err(_) => return Ok(invalid_request()),
        },
    };
    let (archive, scanned) = scan(&state.catalog, &state.cache, refresh)
        .await
        .map_err(|error| internal(&error))?;
    let routes = str_list(&route_sources(&archive));
    let Some((titles, scanned_at)) = scanned else {
        return Ok(json(
            StatusCode::OK,
            &dict(vec![
                ("items", Value::List(Vec::new())),
                ("routes", routes),
                ("scanned_at", Value::Null),
                (
                    "reason",
                    Value::from("mapping.dst_dir and archive.dst_dir must be configured"),
                ),
            ]),
        ));
    };
    let avids: Vec<String> = titles.iter().map(|title| title.avid.clone()).collect();
    let task_dirs = (state.catalog.task_dirs_for)(avids)
        .await
        .map_err(|error| internal(&error))?;
    let items = titles
        .iter()
        .map(|title| title_view(&archive, title, task_dirs.get(&title.avid).map(String::as_str)))
        .collect();
    Ok(json(
        StatusCode::OK,
        &dict(vec![
            ("items", Value::List(items)),
            ("routes", routes),
            ("scanned_at", py_datetime(scanned_at)),
            ("reason", Value::Null),
        ]),
    ))
}

/// `view(task)`: with what the home worker does on it.
async fn view(service: &MergeService, task: &MergeTask) -> Result<Value, Internal> {
    let home = task.transcoder.as_deref() == Some("home") || service.home_bound(task);
    let transcode = if task.state == MergeState::Transcoding {
        service.transcode_view(task).await.map_err(|error| internal(&error))?
    } else {
        None
    };
    Ok(task_view(task, home, transcode))
}

async fn list_tasks(state: Shared, _request: Request) -> Answer {
    let tasks = state.tasks();
    let listing = tasks
        .service
        .repository()
        .listing()
        .await
        .map_err(|error| internal(&error.into()))?;
    let mut items = Vec::with_capacity(listing.len());
    for task in &listing {
        items.push(view(&tasks.service, task).await?);
    }
    let auto = tasks
        .auto_status
        .as_ref()
        .map_or(Value::Null, |status| auto_view(&status()));
    let worker = tasks
        .service
        .presence()
        .view()
        .map_or(Value::Null, |(report, online)| worker_view(&report, online));
    Ok(json(
        StatusCode::OK,
        &dict(vec![
            ("items", Value::List(items)),
            ("unavailable", opt_str(tasks.service.unavailable().as_deref())),
            ("auto", auto),
            ("worker", worker),
        ]),
    ))
}

/// The token, then `task_id` as an `int` path parameter.
#[allow(
    clippy::result_large_err,
    reason = "the refusal is the handler's answer, returned as is"
)]
fn task_id(state: &Merge, request: &Request) -> Result<i64, Answer> {
    if let Some(refusal) = state.auth.refuse(request) {
        return Err(refusal);
    }
    int_param(request.param("task_id")).ok_or_else(|| Ok(invalid_request()))
}

/// `_ACTION_STATUS`: a refused action answers 404 for a task that is not there, else 409.
fn action_error(error: ActionError) -> Answer {
    match error {
        ActionError::Refused(code) => Ok(api_error(if code == "merge_task_not_found" { 404 } else { 409 }, code)),
        ActionError::Error(error) => Err(internal(&error)),
    }
}

/// Which action a task route runs.
#[derive(Clone, Copy)]
enum Action {
    Pause,
    Resume,
    UseCpu,
    Cancel,
    Retry,
}

async fn act(state: &Merge, request: Request, action: Action) -> Answer {
    let id = match task_id(state, &request) {
        Ok(id) => id,
        Err(answer) => return answer,
    };
    let service = &state.tasks().service;
    let outcome = match action {
        Action::Pause => service.set_paused(id, true).await,
        Action::Resume => service.set_paused(id, false).await,
        Action::UseCpu => service.use_cpu(id).await,
        Action::Cancel => service.cancel(id).await,
        Action::Retry => service.retry(id).await,
    };
    let task = match outcome {
        Ok(task) => task,
        Err(error) => return action_error(error),
    };
    // Pause, resume and the CPU answer with the worker's view of the task; cancel and retry
    // with the bare task, as Python's routes do.
    let body = match action {
        Action::Pause | Action::Resume | Action::UseCpu => view(service, &task).await?,
        Action::Cancel | Action::Retry => task_view(&task, false, None),
    };
    Ok(json(StatusCode::OK, &body))
}

async fn pause_task(state: Shared, request: Request) -> Answer {
    act(&state, request, Action::Pause).await
}

async fn resume_task(state: Shared, request: Request) -> Answer {
    act(&state, request, Action::Resume).await
}

async fn use_cpu(state: Shared, request: Request) -> Answer {
    act(&state, request, Action::UseCpu).await
}

async fn cancel_task(state: Shared, request: Request) -> Answer {
    act(&state, request, Action::Cancel).await
}

async fn retry_task(state: Shared, request: Request) -> Answer {
    act(&state, request, Action::Retry).await
}

/// `POST /tasks`: queue one scanned title.
async fn create_task(state: Shared, mut request: Request) -> Answer {
    let body = match read_json(&mut request).await {
        Ok(body) => body,
        Err(response) => return Ok(response),
    };
    if let Some(refusal) = state.auth.refuse(&request) {
        return refusal;
    }
    let Some(merge) = merge_request(&body) else {
        return Ok(invalid_request());
    };
    let service = &state.tasks().service;
    if service.unavailable().is_some() {
        return Ok(api_error(409, "merge_unavailable"));
    }
    let title = current_title(&state.catalog, &state.cache, &merge.avid)
        .await
        .map_err(|error| internal(&error))?;
    let archive = state.catalog.archive();
    let task = match enqueue(&state.catalog, service.repository(), &archive, title.as_ref(), &merge).await {
        Ok(task) => task,
        Err(EnqueueError::Refused { status, code }) => return Ok(api_error(status, code)),
        Err(EnqueueError::Error(error)) => return Err(internal(&error)),
    };
    service.wake();
    Ok(json(StatusCode::CREATED, &task_view(&task, false, None)))
}

#[cfg(test)]
mod tests;
