//! Dashboard endpoints for the monitor pipelines: their status, triggering and cancelling a
//! run, the run history, the acquisition ledger, the manual input source, replacements
//! (换版), the tracker, and the subscription registry.
//!
//! Python origin: `monitor/api.py`. Every collaborator is a trait object, as Python's
//! router takes duck-typed objects, so the ported tests drive it with the same fakes.
//! Routes are registered in Python's order (Starlette matches in order); the ledger, manual
//! and replacement routes, and the subscription routes, only when their part is supplied.

use std::sync::Arc;

use chrono::Utc;
use embyx_clients::avbase::{AvbaseError, AvbaseTalent};
use embyx_core::magnet::extract_info_hash;
use embyx_core::pyjson::{Dict, Value};
use embyx_core::pytext::py_strip;
use embyx_monitor::acquisitions::{
    AcquisitionRecord, AcquisitionState, AttemptState, Ledger, LedgerError, MagnetAttemptRecord, MagnetCandidate,
    TransitionFields,
};
use embyx_monitor::manual::{
    DirectoryListing, ManualEntry, ManualError, ManualIntakeSource, ManualSubmission, SubmitOptions,
};
use embyx_monitor::replace::{ReplaceError, ReplacePlan, ReplaceResult, ReplacementSource};
use embyx_monitor::reports::PipelineName;
use embyx_monitor::runs::{PipelineRunRecord, PipelineRunRepository};
use embyx_monitor::scheduler::{MonitorScheduler, PipelineStatus, TrackerState, TriggerError};
use embyx_monitor::subscriptions::{
    SubscriptionError, SubscriptionKind, SubscriptionRecord, SubscriptionRepository, validate_feed_url,
};
use embyx_monitor::support::{BoxFuture, StoreError};
use embyx_monitor::tracker::SubmitMagnet;
use http::{Method, StatusCode};

use crate::auth::MutationAuth;
use crate::body::{invalid_request, read_json};
use crate::http::{Answer, Internal, Request, Response, error_envelope, json, with_body};
use crate::params::{
    field_opt_bool, field_opt_int, field_opt_str, field_str_list, int_param, int_query, model_fields, opt_str, py_date,
    py_datetime, py_opt_datetime, query_get, req_str, str_list,
};
use crate::routing::{ApiRouter, handler};

mod openapi;

/// `MAX_RUNS_PAGE`.
pub const MAX_RUNS_PAGE: i64 = 100;
/// `MAX_ACQUISITIONS_PAGE`.
pub const MAX_ACQUISITIONS_PAGE: i64 = 200;

/// `ApiError(status, code)`.
pub(crate) fn api_error(status: u16, code: &str) -> Response {
    json(
        StatusCode::from_u16(status).expect("a valid status"),
        &error_envelope(code),
    )
}

/// A failure the route does not handle: Python lets it escape (a 500).
fn internal(error: impl std::fmt::Display) -> Internal {
    Internal::new(error)
}

// -- the collaborators ---------------------------------------------------------------

/// What the router asks the scheduler (`MonitorScheduler`).
pub trait MonitorScheduling: Send + Sync {
    fn status(&self) -> Vec<PipelineStatus>;
    fn trigger(&self, pipeline: PipelineName) -> BoxFuture<'_, Result<String, TriggerError>>;
    fn cancel_running(&self, pipeline: PipelineName) -> bool;
    fn tracker_state(&self) -> TrackerState;
}

impl MonitorScheduling for MonitorScheduler {
    fn status(&self) -> Vec<PipelineStatus> {
        Self::status(self)
    }

    fn trigger(&self, pipeline: PipelineName) -> BoxFuture<'_, Result<String, TriggerError>> {
        Box::pin(Self::trigger(self, pipeline))
    }

    fn cancel_running(&self, pipeline: PipelineName) -> bool {
        Self::cancel_running(self, pipeline)
    }

    fn tracker_state(&self) -> TrackerState {
        Self::tracker_state(self)
    }
}

/// What the router asks the run history (`PipelineRunRepository`).
pub trait RunHistory: Send + Sync {
    fn latest_run(&self, pipeline: PipelineName) -> BoxFuture<'_, Result<Option<PipelineRunRecord>, StoreError>>;
    fn list_runs(
        &self,
        pipeline: Option<PipelineName>,
        limit: i64,
        offset: i64,
    ) -> BoxFuture<'_, Result<Vec<PipelineRunRecord>, StoreError>>;
    fn get_run<'a>(&'a self, run_id: &'a str) -> BoxFuture<'a, Result<Option<PipelineRunRecord>, StoreError>>;
}

impl RunHistory for PipelineRunRepository {
    fn latest_run(&self, pipeline: PipelineName) -> BoxFuture<'_, Result<Option<PipelineRunRecord>, StoreError>> {
        Box::pin(Self::latest_run(self, pipeline))
    }

    fn list_runs(
        &self,
        pipeline: Option<PipelineName>,
        limit: i64,
        offset: i64,
    ) -> BoxFuture<'_, Result<Vec<PipelineRunRecord>, StoreError>> {
        Box::pin(Self::list_runs(self, pipeline, limit, offset))
    }

    fn get_run<'a>(&'a self, run_id: &'a str) -> BoxFuture<'a, Result<Option<PipelineRunRecord>, StoreError>> {
        Box::pin(Self::get_run(self, run_id))
    }
}

/// What the subscriptions page asks the registry (`SubscriptionRepository`).
pub trait SubscriptionRegistry: Send + Sync {
    fn list(&self) -> BoxFuture<'_, Result<Vec<SubscriptionRecord>, SubscriptionError>>;
    fn get(&self, subscription_id: i64) -> BoxFuture<'_, Result<Option<SubscriptionRecord>, SubscriptionError>>;
    fn add_rss<'a>(
        &'a self,
        url: &'a str,
        category: &'a str,
        name: Option<&'a str>,
        seed_pending: bool,
    ) -> BoxFuture<'a, Result<SubscriptionRecord, SubscriptionError>>;
    fn add_talent<'a>(
        &'a self,
        talent_id: i64,
        name: &'a str,
        aliases: &'a [String],
        category: &'a str,
        seed_pending: bool,
    ) -> BoxFuture<'a, Result<SubscriptionRecord, SubscriptionError>>;
    fn update<'a>(
        &'a self,
        subscription_id: i64,
        enabled: Option<bool>,
        category: Option<&'a str>,
        url: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Option<SubscriptionRecord>, SubscriptionError>>;
    fn delete(&self, subscription_id: i64) -> BoxFuture<'_, Result<bool, SubscriptionError>>;
}

impl SubscriptionRegistry for SubscriptionRepository {
    fn list(&self) -> BoxFuture<'_, Result<Vec<SubscriptionRecord>, SubscriptionError>> {
        Box::pin(Self::list(self))
    }

    fn get(&self, subscription_id: i64) -> BoxFuture<'_, Result<Option<SubscriptionRecord>, SubscriptionError>> {
        Box::pin(Self::get(self, subscription_id))
    }

    fn add_rss<'a>(
        &'a self,
        url: &'a str,
        category: &'a str,
        name: Option<&'a str>,
        seed_pending: bool,
    ) -> BoxFuture<'a, Result<SubscriptionRecord, SubscriptionError>> {
        Box::pin(Self::add_rss(self, url, category, Utc::now(), name, seed_pending))
    }

    fn add_talent<'a>(
        &'a self,
        talent_id: i64,
        name: &'a str,
        aliases: &'a [String],
        category: &'a str,
        seed_pending: bool,
    ) -> BoxFuture<'a, Result<SubscriptionRecord, SubscriptionError>> {
        Box::pin(Self::add_talent(
            self,
            talent_id,
            name,
            aliases,
            category,
            Utc::now(),
            seed_pending,
        ))
    }

    fn update<'a>(
        &'a self,
        subscription_id: i64,
        enabled: Option<bool>,
        category: Option<&'a str>,
        url: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Option<SubscriptionRecord>, SubscriptionError>> {
        Box::pin(Self::update(self, subscription_id, Utc::now(), enabled, category, url))
    }

    fn delete(&self, subscription_id: i64) -> BoxFuture<'_, Result<bool, SubscriptionError>> {
        Box::pin(Self::delete(self, subscription_id))
    }
}

/// What the routes ask the manual input source (`ManualIntakeSource`); the playlists'
/// fill uses it too.
pub trait ManualIntake: Send + Sync {
    fn browse<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<DirectoryListing, ManualError>>;
    fn default_directory(&self) -> BoxFuture<'_, Result<Option<String>, ManualError>>;
    fn check_target<'a>(&'a self, task_dir_path: &'a str) -> BoxFuture<'a, Result<String, ManualError>>;
    fn submit<'a>(
        &'a self,
        inputs: &'a [String],
        task_dir_path: &'a str,
        options: SubmitOptions,
    ) -> BoxFuture<'a, Result<ManualSubmission, ManualError>>;
}

impl ManualIntake for ManualIntakeSource {
    fn browse<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<DirectoryListing, ManualError>> {
        Box::pin(Self::browse(self, path))
    }

    fn default_directory(&self) -> BoxFuture<'_, Result<Option<String>, ManualError>> {
        Box::pin(Self::default_directory(self))
    }

    fn check_target<'a>(&'a self, task_dir_path: &'a str) -> BoxFuture<'a, Result<String, ManualError>> {
        Box::pin(Self::check_target(self, task_dir_path))
    }

    fn submit<'a>(
        &'a self,
        inputs: &'a [String],
        task_dir_path: &'a str,
        options: SubmitOptions,
    ) -> BoxFuture<'a, Result<ManualSubmission, ManualError>> {
        Box::pin(Self::submit(self, inputs, task_dir_path, options))
    }
}

/// What the routes ask the replacement source (`ReplacementSource`).
pub trait Replacement: Send + Sync {
    fn plan<'a>(&'a self, text: &'a str) -> BoxFuture<'a, Result<ReplacePlan, ReplaceError>>;
    fn start<'a>(
        &'a self,
        text: &'a str,
        magnet: Option<&'a str>,
    ) -> BoxFuture<'a, Result<ReplaceResult, ReplaceError>>;
}

impl Replacement for ReplacementSource {
    fn plan<'a>(&'a self, text: &'a str) -> BoxFuture<'a, Result<ReplacePlan, ReplaceError>> {
        Box::pin(Self::plan(self, text))
    }

    fn start<'a>(
        &'a self,
        text: &'a str,
        magnet: Option<&'a str>,
    ) -> BoxFuture<'a, Result<ReplaceResult, ReplaceError>> {
        Box::pin(Self::start(self, text, magnet))
    }
}

/// `tracker_ready()`: why the tracker cannot run, `None` when it can.
pub type ReadyCheck = Arc<dyn Fn() -> Option<String> + Send + Sync>;
/// `resolve_talent(query)` (`AvbaseClient.find_talent`).
pub type TalentResolver =
    Arc<dyn Fn(String) -> BoxFuture<'static, Result<Option<AvbaseTalent>, AvbaseError>> + Send + Sync>;
/// `categories()`: the configured RSS category labels.
pub type Categories = Arc<dyn Fn() -> Vec<String> + Send + Sync>;

/// `AcquisitionApi`: everything the dashboard needs to show and act on the ledger. Optional
/// as a unit; the manual and replacement routes only when their source is supplied.
#[derive(Clone)]
pub struct AcquisitionApi {
    pub ledger: Arc<dyn Ledger>,
    pub submit_magnet: Option<SubmitMagnet>,
    pub tracker_ready: Option<ReadyCheck>,
    pub manual: Option<Arc<dyn ManualIntake>>,
    pub replacement: Option<Arc<dyn Replacement>>,
}

impl AcquisitionApi {
    /// `AcquisitionApi(ledger=...)` with every optional part left out.
    pub fn new(ledger: Arc<dyn Ledger>) -> Self {
        Self {
            ledger,
            submit_magnet: None,
            tracker_ready: None,
            manual: None,
            replacement: None,
        }
    }
}

/// `SubscriptionsApi`.
#[derive(Clone)]
pub struct SubscriptionsApi {
    pub repository: Arc<dyn SubscriptionRegistry>,
    pub categories: Categories,
    pub resolve_talent: Option<TalentResolver>,
}

impl std::fmt::Debug for AcquisitionApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcquisitionApi")
            .field("ledger", &self.ledger)
            .field("submit_magnet", &self.submit_magnet.is_some())
            .field("manual", &self.manual.is_some())
            .field("replacement", &self.replacement.is_some())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for SubscriptionsApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionsApi")
            .field("resolve_talent", &self.resolve_talent.is_some())
            .finish_non_exhaustive()
    }
}

// -- views --------------------------------------------------------------------------------

fn dict(entries: Vec<(&str, Value)>) -> Value {
    let mut dict = Dict::new();
    for (key, value) in entries {
        dict.insert(key, value);
    }
    Value::Dict(dict)
}

/// `RunSummaryView.from_record`, and `RunDetailView` with `errors` and `log_tail`.
fn run_view(record: &PipelineRunRecord, detail: bool) -> Value {
    let mut entries = vec![
        ("run_id", Value::from(record.run_id.as_str())),
        ("pipeline", Value::from(record.pipeline.as_str())),
        ("trigger", Value::from(record.trigger.as_str())),
        ("state", Value::from(record.state.as_str())),
        ("started_at", py_datetime(record.started_at)),
        ("finished_at", py_opt_datetime(record.finished_at)),
        ("stats", record.stats.to_value()),
        ("error_count", Value::Int(record.errors.len() as i128)),
    ];
    if detail {
        entries.push(("errors", str_list(&record.errors)));
        entries.push(("log_tail", str_list(&record.log_tail)));
    }
    dict(entries)
}

/// `AttemptView.from_record`.
fn attempt_view(record: &MagnetAttemptRecord) -> Value {
    dict(vec![
        ("attempt_no", Value::Int(record.attempt_no.into())),
        ("magnet_source", Value::from(record.magnet_source.as_str())),
        ("state", Value::from(record.state.as_str())),
        ("progress", record.progress.map_or(Value::Null, Value::Float)),
        ("error", opt_str(record.error.as_deref())),
        ("submitted_at", py_opt_datetime(record.submitted_at)),
        ("updated_at", py_datetime(record.updated_at)),
        ("info_hash", opt_str(record.info_hash.as_deref())),
    ])
}

/// `AcquisitionView.from_record`, with `attempts` last for `AcquisitionDetailView`.
fn acquisition_entries(record: &AcquisitionRecord) -> Vec<(&'static str, Value)> {
    vec![
        ("avid", Value::from(record.avid.as_str())),
        ("state", Value::from(record.state.as_str())),
        ("source", Value::from(record.source.as_str())),
        ("note", opt_str(record.note.as_deref())),
        ("archived_paths", str_list(&record.archived_paths)),
        ("next_action_at", py_opt_datetime(record.next_action_at)),
        ("release_date", record.release_date.map_or(Value::Null, py_date)),
        ("created_at", py_datetime(record.created_at)),
        ("updated_at", py_datetime(record.updated_at)),
        ("replacing", str_list(&record.replace_paths)),
    ]
}

fn acquisition_view(record: &AcquisitionRecord) -> Value {
    dict(acquisition_entries(record))
}

/// `ReplacePlanView.from_plan`.
fn plan_view(plan: &ReplacePlan) -> Value {
    dict(vec![
        ("avid", Value::from(plan.avid.as_str())),
        ("holdings", str_list(&plan.holdings)),
        ("state", opt_str(plan.state.map(AcquisitionState::as_str))),
        ("task_dir_path", opt_str(plan.task_dir_path.as_deref())),
        ("blocked", opt_str(plan.blocked)),
    ])
}

/// `ManualEntryView.from_entry`.
fn manual_entry_view(entry: &ManualEntry) -> Value {
    dict(vec![
        ("text", Value::from(entry.text.as_str())),
        ("avid", opt_str(entry.avid.as_deref())),
        ("outcome", Value::from(entry.outcome.as_str())),
        ("archived_paths", str_list(&entry.archived_paths)),
    ])
}

/// `DirectoryListingView.from_listing`.
fn listing_view(listing: &DirectoryListing, default_path: Option<&str>) -> Value {
    let entries = listing
        .entries
        .iter()
        .map(|entry| {
            dict(vec![
                ("path", Value::from(entry.path.as_str())),
                ("name", Value::from(entry.name.as_str())),
                ("configured", Value::Bool(entry.configured)),
                ("routed", Value::Bool(entry.routed)),
            ])
        })
        .collect();
    dict(vec![
        ("path", Value::from(listing.path.as_str())),
        ("parent", opt_str(listing.parent.as_deref())),
        ("entries", Value::List(entries)),
        ("default_path", opt_str(default_path)),
    ])
}

/// `SubscriptionView.from_record`.
fn subscription_view(record: &SubscriptionRecord) -> Value {
    dict(vec![
        ("id", Value::Int(record.id.into())),
        ("kind", Value::from(record.kind.as_str())),
        ("category", Value::from(record.category.as_str())),
        ("enabled", Value::Bool(record.enabled)),
        ("url", opt_str(record.url.as_deref())),
        ("feed_url", Value::from(record.feed_url())),
        (
            "talent_id",
            record.talent_id.map_or(Value::Null, |id| Value::Int(id.into())),
        ),
        ("name", opt_str(record.name.as_deref())),
        ("aliases", str_list(&record.aliases)),
        ("seed_pending", Value::Bool(record.seed_pending)),
        ("cursor_size", Value::Int(record.cursor.len() as i128)),
        ("last_polled_at", py_opt_datetime(record.last_polled_at)),
        ("last_error", opt_str(record.last_error.as_deref())),
        ("created_at", py_datetime(record.created_at)),
        ("updated_at", py_datetime(record.updated_at)),
    ])
}

#[allow(clippy::unnecessary_wraps, reason = "a handler's tail answer, in the handler's type")]
fn ok(content: &Value) -> Answer {
    Ok(json(StatusCode::OK, content))
}

/// `_parse_pipeline(pipeline)`: 404 `unknown_pipeline` for a name that is none.
#[allow(
    clippy::result_large_err,
    reason = "the refusal is the handler's answer, built once per request"
)]
fn parse_pipeline(name: &str) -> Result<PipelineName, Response> {
    PipelineName::parse(name).ok_or_else(|| api_error(404, "unknown_pipeline"))
}

/// The answer, from either arm.
macro_rules! answer {
    ($result:expr) => {
        match $result {
            Ok(value) => value,
            Err(response) => return Ok(response),
        }
    };
}

/// The auth dependency: its refusal is the route's answer.
macro_rules! authorize {
    ($auth:expr, $request:expr) => {
        if let Some(refusal) = $auth.refuse(&$request) {
            return refusal;
        }
    };
}

// -- the router ----------------------------------------------------------------------------

/// The shared state of one router's handlers.
struct Monitor {
    scheduler: Arc<dyn MonitorScheduling>,
    runs: Arc<dyn RunHistory>,
    auth: MutationAuth,
    acquisitions: Option<AcquisitionApi>,
    subscriptions: Option<SubscriptionsApi>,
}

type Shared = Arc<Monitor>;

/// One route: the handler gets the router's state and the request.
fn route<F, Fut>(router: ApiRouter, monitor: &Shared, method: Method, path: &str, function: F) -> ApiRouter
where
    F: Fn(Shared, Request) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Answer> + Send + 'static,
{
    let state = Arc::clone(monitor);
    let text = openapi::operation(method.as_str(), path);
    router.route(
        method,
        path,
        Some(text),
        handler(move |request| function(Arc::clone(&state), request)),
    )
}

/// `create_monitor_router(scheduler, runs, mutation_auth=, acquisitions=, subscriptions=)`.
pub fn create_monitor_router(
    scheduler: Arc<dyn MonitorScheduling>,
    runs: Arc<dyn RunHistory>,
    auth: MutationAuth,
    acquisitions: Option<AcquisitionApi>,
    subscriptions: Option<SubscriptionsApi>,
) -> ApiRouter {
    let has_subscriptions = subscriptions.is_some();
    let (has_manual, has_replacement) = acquisitions
        .as_ref()
        .map_or((false, false), |api| (api.manual.is_some(), api.replacement.is_some()));
    let has_ledger = acquisitions.is_some();
    let monitor: Shared = Arc::new(Monitor {
        scheduler,
        runs,
        auth,
        acquisitions,
        subscriptions,
    });
    let mut router = ApiRouter::new("/api/monitor");
    if has_subscriptions {
        router = route(router, &monitor, Method::GET, "/subscriptions", list_subscriptions);
        router = route(router, &monitor, Method::POST, "/subscriptions", create_subscription);
        router = route(
            router,
            &monitor,
            Method::PATCH,
            "/subscriptions/{subscription_id}",
            update_subscription,
        );
        router = route(
            router,
            &monitor,
            Method::DELETE,
            "/subscriptions/{subscription_id}",
            delete_subscription,
        );
    }
    router = route(router, &monitor, Method::GET, "/status", status);
    router = route(router, &monitor, Method::POST, "/{pipeline}/trigger", trigger);
    router = route(router, &monitor, Method::POST, "/{pipeline}/cancel", cancel);
    router = route(router, &monitor, Method::GET, "/runs", list_runs);
    router = route(router, &monitor, Method::GET, "/runs/{run_id}", get_run);
    if !has_ledger {
        return router;
    }
    router = route(router, &monitor, Method::GET, "/acquisitions", list_acquisitions);
    router = route(router, &monitor, Method::GET, "/acquisitions/{avid}", get_acquisition);
    router = route(
        router,
        &monitor,
        Method::POST,
        "/acquisitions/{avid}/retry",
        retry_acquisition,
    );
    router = route(
        router,
        &monitor,
        Method::POST,
        "/acquisitions/{avid}/magnet",
        add_magnet,
    );
    router = route(
        router,
        &monitor,
        Method::POST,
        "/acquisitions/{avid}/ignore",
        ignore_acquisition,
    );
    router = route(
        router,
        &monitor,
        Method::POST,
        "/acquisitions/{avid}/resume",
        resume_acquisition,
    );
    if has_manual {
        router = route(router, &monitor, Method::GET, "/manual/directories", browse_directories);
        router = route(router, &monitor, Method::POST, "/manual", submit_manual);
    }
    if has_replacement {
        router = route(router, &monitor, Method::GET, "/replace", plan_replacement);
        router = route(router, &monitor, Method::POST, "/replace", start_replacement);
    }
    route(router, &monitor, Method::GET, "/tracker", tracker_status)
}

// -- pipelines and runs ------------------------------------------------------------------

async fn status(monitor: Shared, _request: Request) -> Answer {
    let mut views = Vec::new();
    for pipeline_status in monitor.scheduler.status() {
        let latest = monitor
            .runs
            .latest_run(pipeline_status.pipeline)
            .await
            .map_err(internal)?;
        views.push(dict(vec![
            ("pipeline", Value::from(pipeline_status.pipeline.as_str())),
            ("enabled", Value::Bool(pipeline_status.enabled)),
            ("configured", Value::Bool(pipeline_status.configured)),
            ("reason", opt_str(pipeline_status.reason.as_deref())),
            ("running_run_id", opt_str(pipeline_status.running_run_id.as_deref())),
            ("next_scheduled_at", py_opt_datetime(pipeline_status.next_scheduled_at)),
            (
                "last_run",
                latest.as_ref().map_or(Value::Null, |record| run_view(record, false)),
            ),
        ]));
    }
    ok(&Value::List(views))
}

async fn trigger(monitor: Shared, request: Request) -> Answer {
    authorize!(monitor.auth, request);
    let name = answer!(parse_pipeline(request.param("pipeline")));
    match monitor.scheduler.trigger(name).await {
        Ok(run_id) => Ok(json(StatusCode::ACCEPTED, &dict(vec![("run_id", Value::from(run_id))]))),
        Err(TriggerError::Busy(_)) => Ok(api_error(409, "pipeline_busy")),
        Err(TriggerError::NotConfigured { .. }) => Ok(api_error(422, "pipeline_not_configured")),
        Err(error) => Err(internal(error)),
    }
}

async fn cancel(monitor: Shared, request: Request) -> Answer {
    authorize!(monitor.auth, request);
    let name = answer!(parse_pipeline(request.param("pipeline")));
    if !monitor.scheduler.cancel_running(name) {
        return Ok(api_error(409, "pipeline_not_running"));
    }
    ok(&dict(vec![("cancelling", Value::Bool(true))]))
}

async fn list_runs(monitor: Shared, request: Request) -> Answer {
    let pipeline = query_get(&request.query, "pipeline");
    let (Some(limit), Some(offset)) = (
        int_query(&request.query, "limit", 20),
        int_query(&request.query, "offset", 0),
    ) else {
        return Ok(invalid_request());
    };
    let name = match pipeline.as_deref() {
        None | Some("") => None,
        Some(text) => Some(answer!(parse_pipeline(text))),
    };
    let records = monitor
        .runs
        .list_runs(name, limit.clamp(1, MAX_RUNS_PAGE), offset.max(0))
        .await
        .map_err(internal)?;
    ok(&Value::List(
        records.iter().map(|record| run_view(record, false)).collect(),
    ))
}

async fn get_run(monitor: Shared, request: Request) -> Answer {
    match monitor.runs.get_run(request.param("run_id")).await.map_err(internal)? {
        Some(record) => ok(&run_view(&record, true)),
        None => Ok(api_error(404, "unknown_run")),
    }
}

// -- the acquisition ledger ----------------------------------------------------------------

fn acquisitions(monitor: &Monitor) -> &AcquisitionApi {
    monitor
        .acquisitions
        .as_ref()
        .expect("the ledger routes are mounted with the ledger")
}

/// `_load(avid)`: the record, or 404 `unknown_acquisition`.
async fn load(ledger: &dyn Ledger, avid: &str) -> Result<Result<AcquisitionRecord, Response>, Internal> {
    Ok(ledger
        .get(avid)
        .await
        .map_err(internal)?
        .ok_or_else(|| api_error(404, "unknown_acquisition")))
}

/// `_move(record, target, **fields)`: one CAS transition, a lost race a 409.
async fn move_to(
    ledger: &dyn Ledger,
    record: &AcquisitionRecord,
    target: AcquisitionState,
    fields: TransitionFields,
) -> Answer {
    let moved = ledger
        .transition(&record.avid, record.state, target, Utc::now(), fields)
        .await
        .map_err(internal)?;
    if !moved {
        return Ok(api_error(409, "acquisition_changed"));
    }
    let record = answer!(load(ledger, &record.avid).await?);
    ok(&acquisition_view(&record))
}

/// `_submit(avid, attempt_no=None)`: hand a magnet to CloudDrive, a chosen one or the next
/// untried one. `Ok(None)` when it was submitted, else the refusal.
async fn submit(api: &AcquisitionApi, avid: &str, attempt_no: Option<i32>) -> Result<Option<Response>, Internal> {
    let Some(submit_magnet) = &api.submit_magnet else {
        return Ok(Some(api_error(422, "clouddrive_not_configured")));
    };
    let now = Utc::now();
    let claimed = match attempt_no {
        Some(attempt_no) => api.ledger.claim_attempt(avid, attempt_no, now).await,
        None => api.ledger.claim_next_pending(avid, now).await,
    };
    let attempt = match claimed {
        Ok(Some(attempt)) => attempt,
        Ok(None) => return Ok(Some(api_error(409, "no_magnet_left"))),
        Err(LedgerError::MagnetInUse(_)) => return Ok(Some(api_error(409, "magnet_in_use"))),
        Err(error) => return Err(internal(error)),
    };
    if !submit_magnet(avid.to_owned(), attempt.magnet.clone())
        .await
        .map_err(internal)?
    {
        api.ledger
            .transition_attempt(
                avid,
                attempt.attempt_no,
                AttemptState::Submitted,
                AttemptState::Error,
                Utc::now(),
                Some("failed to add the offline task".to_owned()),
            )
            .await
            .map_err(internal)?;
        return Ok(Some(api_error(502, "offline_task_rejected")));
    }
    Ok(None)
}

async fn list_acquisitions(monitor: Shared, request: Request) -> Answer {
    let state = query_get(&request.query, "state");
    let (Some(limit), Some(offset)) = (
        int_query(&request.query, "limit", 50),
        int_query(&request.query, "offset", 0),
    ) else {
        return Ok(invalid_request());
    };
    let states = match state.as_deref() {
        None | Some("") => None,
        Some(text) => {
            let parsed: Option<Vec<AcquisitionState>> = text
                .split(',')
                .filter(|value| !value.is_empty())
                .map(AcquisitionState::parse)
                .collect();
            match parsed {
                Some(states) => Some(states),
                None => return Ok(api_error(400, "unknown_state")),
            }
        }
    };
    let ledger = &acquisitions(&monitor).ledger;
    let records = ledger
        .list_acquisitions(states.as_deref(), limit.clamp(1, MAX_ACQUISITIONS_PAGE), offset.max(0))
        .await
        .map_err(internal)?;
    let mut counts = Dict::new();
    for (state, count) in ledger.count_by_state().await.map_err(internal)? {
        counts.insert(state.as_str(), Value::Int(count.into()));
    }
    ok(&dict(vec![
        ("items", Value::List(records.iter().map(acquisition_view).collect())),
        ("counts", Value::Dict(counts)),
    ]))
}

async fn get_acquisition(monitor: Shared, request: Request) -> Answer {
    let avid = request.param("avid");
    let ledger = &acquisitions(&monitor).ledger;
    let record = answer!(load(ledger.as_ref(), avid).await?);
    let attempts = ledger.attempts_for(avid).await.map_err(internal)?;
    let mut entries = acquisition_entries(&record);
    entries.push(("attempts", Value::List(attempts.iter().map(attempt_view).collect())));
    ok(&dict(entries))
}

/// The end of a retry or a new magnet: a row already downloading is only reloaded.
async fn resume_download(ledger: &dyn Ledger, record: &AcquisitionRecord) -> Answer {
    if record.state == AcquisitionState::Downloading {
        let record = answer!(load(ledger, &record.avid).await?);
        return ok(&acquisition_view(&record));
    }
    move_to(
        ledger,
        record,
        AcquisitionState::Downloading,
        TransitionFields::default(),
    )
    .await
}

/// Try this AVID's next magnet now, whatever it is waiting on.
async fn retry_acquisition(monitor: Shared, request: Request) -> Answer {
    authorize!(monitor.auth, request);
    let api = acquisitions(&monitor);
    let avid = request.param("avid");
    let record = answer!(load(api.ledger.as_ref(), avid).await?);
    if record.state == AcquisitionState::Archived {
        return Ok(api_error(409, "already_archived"));
    }
    if let Some(refusal) = submit(api, avid, None).await? {
        return Ok(refusal);
    }
    resume_download(api.ledger.as_ref(), &record).await
}

/// Add an operator-supplied magnet and submit it straight away.
async fn add_magnet(monitor: Shared, mut request: Request) -> Answer {
    let body = answer!(read_json(&mut request).await);
    authorize!(monitor.auth, request);
    let Some(magnet) = model_fields(&body, &["magnet"]).and_then(|fields| req_str(fields, "magnet")) else {
        return Ok(invalid_request());
    };
    let api = acquisitions(&monitor);
    let avid = request.param("avid");
    let record = answer!(load(api.ledger.as_ref(), avid).await?);
    if record.state == AcquisitionState::Archived {
        return Ok(api_error(409, "already_archived"));
    }
    let info_hash = extract_info_hash(&magnet);
    // `magnet.lower().startswith('magnet:')`: no character lower-cases into these letters
    // but their own two cases, so an ASCII comparison is exact.
    let scheme = magnet
        .as_bytes()
        .get(..7)
        .is_some_and(|head| head.eq_ignore_ascii_case(b"magnet:"));
    let Some(info_hash) = info_hash.filter(|_| scheme) else {
        return Ok(api_error(400, "unusable_magnet"));
    };
    let candidate = MagnetCandidate {
        magnet: magnet.clone(),
        info_hash: Some(info_hash),
        source: "manual".to_owned(),
        size_hint: None,
    };
    let added = api
        .ledger
        .add_attempts(avid, std::slice::from_ref(&candidate), Utc::now())
        .await
        .map_err(internal)?;
    if added == 0 {
        return Ok(api_error(409, "magnet_already_tried"));
    }
    // Submit the magnet the operator chose, not whichever candidate happens to be next.
    let attempts = api.ledger.attempts_for(avid).await.map_err(internal)?;
    let last = attempts
        .last()
        .map(|attempt| attempt.attempt_no)
        .ok_or_else(|| internal("IndexError: tuple index out of range"))?;
    if let Some(refusal) = submit(api, avid, Some(last)).await? {
        return Ok(refusal);
    }
    resume_download(api.ledger.as_ref(), &record).await
}

async fn ignore_acquisition(monitor: Shared, request: Request) -> Answer {
    authorize!(monitor.auth, request);
    let ledger = acquisitions(&monitor).ledger.as_ref();
    let record = answer!(load(ledger, request.param("avid")).await?);
    if record.state == AcquisitionState::Archived {
        return Ok(api_error(409, "already_archived"));
    }
    move_to(
        ledger,
        &record,
        AcquisitionState::Ignored,
        TransitionFields::note("ignored by an operator"),
    )
    .await
}

/// Hand a parked AVID back to the tracker once its folder has been sorted out.
async fn resume_acquisition(monitor: Shared, request: Request) -> Answer {
    authorize!(monitor.auth, request);
    let ledger = acquisitions(&monitor).ledger.as_ref();
    let record = answer!(load(ledger, request.param("avid")).await?);
    if record.state != AcquisitionState::NeedsAttention {
        return Ok(api_error(409, "not_parked"));
    }
    move_to(
        ledger,
        &record,
        AcquisitionState::Downloading,
        TransitionFields::default(),
    )
    .await
}

// -- the manual input source -----------------------------------------------------------------

/// `_MANUAL_STATUS`: which status each refused manual submission answers with.
pub(crate) fn manual_refusal(error: ManualError) -> Answer {
    match error.code() {
        Some(code) => Ok(api_error(if code == "directory_not_found" { 404 } else { 422 }, code)),
        None => Err(internal(error)),
    }
}

/// The CloudDrive directories under `path`, for picking one to download into.
async fn browse_directories(monitor: Shared, request: Request) -> Answer {
    authorize!(monitor.auth, request);
    let path = query_get(&request.query, "path").unwrap_or_else(|| "/".to_owned());
    let manual = acquisitions(&monitor).manual.as_ref().expect("mounted with the source");
    let listing = match manual.browse(&path).await {
        Ok(listing) => listing,
        Err(error) => return manual_refusal(error),
    };
    let default_path = match manual.default_directory().await {
        Ok(default_path) => default_path,
        Err(error) => return manual_refusal(error),
    };
    ok(&listing_view(&listing, default_path.as_deref()))
}

/// Hand an operator's own list of AVIDs to the shared acquisition intake.
async fn submit_manual(monitor: Shared, mut request: Request) -> Answer {
    let body = answer!(read_json(&mut request).await);
    authorize!(monitor.auth, request);
    let parsed = model_fields(&body, &["inputs", "task_dir_path"]).and_then(|fields| {
        let inputs = field_str_list(fields.get("inputs")?)?;
        Some((inputs, req_str(fields, "task_dir_path")?))
    });
    let Some((inputs, task_dir_path)) = parsed else {
        return Ok(invalid_request());
    };
    let manual = acquisitions(&monitor).manual.as_ref().expect("mounted with the source");
    let submission = match manual.submit(&inputs, &task_dir_path, SubmitOptions::default()).await {
        Ok(submission) => submission,
        Err(error) => return manual_refusal(error),
    };
    ok(&dict(vec![
        ("task_dir_path", Value::from(submission.task_dir_path.as_str())),
        (
            "items",
            Value::List(submission.entries.iter().map(manual_entry_view).collect()),
        ),
    ]))
}

// -- replacements (换版) -----------------------------------------------------------------------

/// `_REPLACE_STATUS`: which status each refused replacement answers with; the rest 409.
fn replace_refusal(error: ReplaceError) -> Answer {
    match error.code() {
        Some(code) => {
            let status = match code {
                "unreadable_avid" | "no_task_dir" | "clouddrive_not_configured" => 422,
                "unusable_magnet" => 400,
                "not_in_library" => 404,
                _ => 409,
            };
            Ok(api_error(status, code))
        }
        None => Err(internal(error)),
    }
}

/// What replacing a title would delete and where the new copy would come from.
async fn plan_replacement(monitor: Shared, request: Request) -> Answer {
    authorize!(monitor.auth, request);
    let Some(avid) = query_get(&request.query, "avid") else {
        return Ok(invalid_request());
    };
    let replacement = acquisitions(&monitor)
        .replacement
        .as_ref()
        .expect("mounted with the source");
    match replacement.plan(&avid).await {
        Ok(plan) => ok(&plan_view(&plan)),
        Err(error) => replace_refusal(error),
    }
}

/// Fetch another copy of a title; the old one is deleted once the new one is filed.
async fn start_replacement(monitor: Shared, mut request: Request) -> Answer {
    let body = answer!(read_json(&mut request).await);
    authorize!(monitor.auth, request);
    let parsed = model_fields(&body, &["avid", "magnet"])
        .and_then(|fields| Some((req_str(fields, "avid")?, field_opt_str(fields, "magnet")?)));
    let Some((avid, magnet)) = parsed else {
        return Ok(invalid_request());
    };
    let replacement = acquisitions(&monitor)
        .replacement
        .as_ref()
        .expect("mounted with the source");
    match replacement.start(&avid, magnet.as_deref()).await {
        Ok(result) => ok(&dict(vec![
            ("plan", plan_view(&result.plan)),
            ("outcome", Value::from(result.outcome.as_str())),
            ("acquisition", acquisition_view(&result.record)),
        ])),
        Err(error) => replace_refusal(error),
    }
}

async fn tracker_status(monitor: Shared, _request: Request) -> Answer {
    let reason = match &acquisitions(&monitor).tracker_ready {
        Some(ready) => ready(),
        None => Some("the tracker is not wired up".to_owned()),
    };
    let state = monitor.scheduler.tracker_state();
    ok(&dict(vec![
        ("running", Value::Bool(reason.is_none())),
        ("reason", opt_str(reason.as_deref())),
        ("last_polled_at", py_opt_datetime(state.last_polled_at)),
        ("last_error", opt_str(state.last_error.as_deref())),
        ("last_stats", state.last_stats.to_value()),
    ]))
}

// -- subscriptions -----------------------------------------------------------------------------

fn subscriptions(monitor: &Monitor) -> &SubscriptionsApi {
    monitor
        .subscriptions
        .as_ref()
        .expect("the subscription routes are mounted with the registry")
}

/// A registry failure: 409 `subscription_exists`, or one Python lets escape.
fn subscription_failure(error: SubscriptionError) -> Answer {
    match error {
        SubscriptionError::Exists(_) => Ok(api_error(409, "subscription_exists")),
        SubscriptionError::Store(error) => Err(internal(error)),
    }
}

async fn list_subscriptions(monitor: Shared, _request: Request) -> Answer {
    let api = subscriptions(&monitor);
    let records = api.repository.list().await.map_err(internal)?;
    ok(&dict(vec![
        ("items", Value::List(records.iter().map(subscription_view).collect())),
        ("categories", str_list(&(api.categories)())),
    ]))
}

/// `CreateSubscriptionRequest`.
struct CreateRequest {
    kind: String,
    category: String,
    name: Option<String>,
    url: Option<String>,
    talent_id: Option<i64>,
    aliases: Vec<String>,
    seed: bool,
}

impl CreateRequest {
    fn validate(body: &crate::body::JsonBody) -> Option<Self> {
        let fields = model_fields(
            body,
            &["kind", "category", "name", "url", "talent_id", "aliases", "seed"],
        )?;
        let kind = match fields.get("kind") {
            None => "rss".to_owned(),
            Some(_) => req_str(fields, "kind")?,
        };
        let aliases = match fields.get("aliases") {
            None => Vec::new(),
            Some(value) => field_str_list(value)?,
        };
        let seed = match fields.get("seed") {
            None => false,
            Some(value) => embyx_core::config::lax::lax_bool(value, embyx_core::config::Mode::Python).ok()?,
        };
        Some(Self {
            kind,
            category: req_str(fields, "category")?,
            name: field_opt_str(fields, "name")?,
            url: field_opt_str(fields, "url")?,
            talent_id: field_opt_int(fields, "talent_id")?,
            aliases,
            seed,
        })
    }
}

/// `_talent_to_subscribe(api, request, name)`: the talent a create request names, given
/// outright or looked up on AVBase; `Err` is the refusal.
async fn talent_to_subscribe(
    api: &SubscriptionsApi,
    request: &CreateRequest,
    name: Option<String>,
) -> Result<Result<(i64, String, Vec<String>), Response>, Internal> {
    let mut name = name;
    let mut talent_id = request.talent_id.map(i128::from);
    let mut aliases_given = request.aliases.clone();
    if talent_id.is_none() {
        // No id: the name (or pasted URL) is looked up on AVBase.
        let (Some(query), Some(resolve)) = (name.clone(), api.resolve_talent.as_ref()) else {
            return Ok(Err(api_error(422, "invalid_talent")));
        };
        let talent = match resolve(query).await {
            Ok(Some(talent)) => talent,
            Ok(None) => return Ok(Err(api_error(404, "talent_not_found"))),
            Err(error) if error.is_avbase_error() => return Ok(Err(api_error(502, "avbase_unavailable"))),
            Err(error) => return Err(internal(format!("{}: {error}", error.class_name()))),
        };
        talent_id = Some(talent.talent_id);
        name = Some(talent.name);
        aliases_given = talent.aliases.into_iter().chain(aliases_given).collect();
    }
    let (Some(talent_id), Some(name)) = (talent_id.filter(|id| *id > 0), name) else {
        return Ok(Err(api_error(422, "invalid_talent")));
    };
    // An id past the column's reach fails in the database, as asyncpg's would.
    let talent_id = i64::try_from(talent_id).map_err(|_| internal("OverflowError: value out of int64 range"))?;
    let mut aliases: Vec<String> = Vec::new();
    for alias in &aliases_given {
        let alias = py_strip(alias);
        if !alias.is_empty() && alias != name && !aliases.iter().any(|known| known == alias) {
            aliases.push(alias.to_owned());
        }
    }
    Ok(Ok((talent_id, name, aliases)))
}

async fn create_subscription(monitor: Shared, mut request: Request) -> Answer {
    let body = answer!(read_json(&mut request).await);
    authorize!(monitor.auth, request);
    let Some(create) = CreateRequest::validate(&body) else {
        return Ok(invalid_request());
    };
    let api = subscriptions(&monitor);
    if !(api.categories)().contains(&create.category) {
        return Ok(api_error(422, "unknown_category"));
    }
    let name = create
        .name
        .as_deref()
        .map(py_strip)
        .filter(|name| !name.is_empty())
        .map(str::to_owned);
    let created = match create.kind.as_str() {
        "rss" => {
            let Ok(url) = validate_feed_url(create.url.as_deref().unwrap_or("")) else {
                return Ok(api_error(422, "invalid_feed_url"));
            };
            api.repository
                .add_rss(&url, &create.category, name.as_deref(), create.seed)
                .await
        }
        "avbase_talent" => {
            let (talent_id, name, aliases) = answer!(talent_to_subscribe(api, &create, name).await?);
            api.repository
                .add_talent(talent_id, &name, &aliases, &create.category, create.seed)
                .await
        }
        _ => return Ok(api_error(422, "unknown_subscription_kind")),
    };
    match created {
        Ok(record) => Ok(json(StatusCode::CREATED, &subscription_view(&record))),
        Err(error) => subscription_failure(error),
    }
}

async fn update_subscription(monitor: Shared, mut request: Request) -> Answer {
    let body = answer!(read_json(&mut request).await);
    authorize!(monitor.auth, request);
    let subscription_id = int_param(request.param("subscription_id"));
    let parsed = model_fields(&body, &["enabled", "category", "url"]).and_then(|fields| {
        Some((
            field_opt_bool(fields, "enabled")?,
            field_opt_str(fields, "category")?,
            field_opt_str(fields, "url")?,
        ))
    });
    let (Some(subscription_id), Some((enabled, category, url))) = (subscription_id, parsed) else {
        return Ok(invalid_request());
    };
    let api = subscriptions(&monitor);
    if let Some(category) = &category
        && !(api.categories)().contains(category)
    {
        return Ok(api_error(422, "unknown_category"));
    }
    let mut new_url = None;
    if let Some(url) = &url {
        let Ok(valid) = validate_feed_url(url) else {
            return Ok(api_error(422, "invalid_feed_url"));
        };
        new_url = Some(valid);
        let Some(current) = api.repository.get(subscription_id).await.map_err(internal)? else {
            return Ok(api_error(404, "unknown_subscription"));
        };
        if current.kind != SubscriptionKind::Rss {
            return Ok(api_error(422, "url_not_editable"));
        }
    }
    match api
        .repository
        .update(subscription_id, enabled, category.as_deref(), new_url.as_deref())
        .await
    {
        Ok(Some(record)) => ok(&subscription_view(&record)),
        Ok(None) => Ok(api_error(404, "unknown_subscription")),
        Err(error) => subscription_failure(error),
    }
}

async fn delete_subscription(monitor: Shared, request: Request) -> Answer {
    authorize!(monitor.auth, request);
    let Some(subscription_id) = int_param(request.param("subscription_id")) else {
        return Ok(invalid_request());
    };
    if !subscriptions(&monitor)
        .repository
        .delete(subscription_id)
        .await
        .map_err(internal)?
    {
        return Ok(api_error(404, "unknown_subscription"));
    }
    Ok(with_body(StatusCode::NO_CONTENT, None, bytes::Bytes::new()))
}

#[cfg(test)]
mod tests;
