//! The fill actor's HTTP surface: resolving an AVID's cast, queueing and polling a plan
//! scan, cancelling it, queueing and polling an apply job, and the direct apply; the
//! feature's health probe and its lifespan (the job queue and the move maintenance).
//!
//! Python origin: `fill_actor/api.py` (`create_fill_actor_router`, `fill_actor_health`,
//! `fill_actor_lifespan`, `handle_fill_actor_error`). FastAPI's order holds: the body is
//! read and parsed, the dependencies run (the token, then the readiness checks), then the
//! body is validated. A `FillActorError` from a handler is the feature's error handler's
//! answer (`{"error": {"code"}}` with the status of `_service_error_status`); any other
//! failure escapes as the 500 nothing handles.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use chrono::{DateTime, Utc};
use embyx_core::pyjson::{Dict, Value};
use embyx_core::pytext::py_strip;
use embyx_fill_actor::errors::{FillActorError, FillActorErrorKind};
use embyx_fill_actor::jobs::FillActorJobManager;
use embyx_fill_actor::models::{ApplyResult, FillActorPlan};
use embyx_fill_actor::postgres_repository::PostgresFillActorRepository;
use embyx_fill_actor::records::{
    CancelJobOutcome, JobOperation, JobProgress, JobRecord, JobStage, JobState, normalize_datetime,
};
use embyx_fill_actor::service::{FillActorService, ServiceError};
use futures_util::future::BoxFuture;
use http::{Method, StatusCode};
use tokio::task::JoinHandle;

use crate::app::{FeatureHealthProbe, Lifespan};
use crate::auth::MutationAuth;
use crate::body::{JsonBody, invalid_request, read_json};
use crate::http::{Answer, Internal, Request, Response, error_envelope, json};
use crate::params::{field_str_list, model_fields, opt_str, py_datetime, req_str};
use crate::routing::{ApiRouter, handler};

mod openapi;

/// The Python logger the process log names (D60).
const LOGGER: &str = "embyx_manager.fill_actor.api";

/// `MAINTENANCE_INTERVAL_SECONDS`.
pub const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5);

/// `avid_actor_lookup(avid)`: who is credited on a video, as `(actor_id, name)` pairs. `Err`
/// is the exception it raised, by its text.
pub type AvidActorLookup =
    Arc<dyn Fn(String) -> BoxFuture<'static, Result<Vec<(String, String)>, String>> + Send + Sync>;

// -- errors --------------------------------------------------------------------------------

/// `_service_error_status(exc)`.
pub fn service_error_status(kind: FillActorErrorKind) -> StatusCode {
    use FillActorErrorKind as Kind;
    match kind {
        Kind::InvalidActorId | Kind::TooManyActors | Kind::TooManyVideos | Kind::UnknownCandidate => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        Kind::ApplyRequestConflict | Kind::ApplyJobNotCancellable | Kind::LegacyPlan | Kind::RevisionMismatch => {
            StatusCode::CONFLICT
        }
        Kind::MoveDisabled => StatusCode::SERVICE_UNAVAILABLE,
        Kind::UnknownPlan | Kind::UnknownApplyJob => StatusCode::NOT_FOUND,
        Kind::ExpiredPlan => StatusCode::GONE,
        Kind::JobQueueFull => StatusCode::TOO_MANY_REQUESTS,
        Kind::Base | Kind::NotConfigured => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// `handle_fill_actor_error(request, exc)`.
pub fn fill_actor_error(error: &FillActorError) -> Response {
    json(service_error_status(error.kind), &error_envelope(error.code()))
}

/// `raise SomeError(...)`: the error handler's answer.
fn raise(kind: FillActorErrorKind) -> Response {
    fill_actor_error(&FillActorError::new(kind, ""))
}

/// `ApiError(status, code)`.
fn api_error(status: StatusCode, code: &str) -> Response {
    json(status, &error_envelope(code))
}

fn not_ready() -> Response {
    api_error(StatusCode::SERVICE_UNAVAILABLE, "not_ready")
}

/// A service call's failure: a `FillActorError` is the handler's answer, anything else
/// escapes.
fn failed(error: ServiceError) -> Result<Response, Internal> {
    match error {
        ServiceError::FillActor(error) => Ok(fill_actor_error(&error)),
        error @ ServiceError::Raised { .. } => Err(Internal::new(error)),
    }
}

/// The answer of a call that may fail with a `FillActorError`.
macro_rules! service {
    ($call:expr) => {
        match $call {
            Ok(value) => value,
            Err(error) => return failed(ServiceError::from(error)),
        }
    };
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

// -- request models -------------------------------------------------------------------------

/// pydantic's `min_length`/`max_length` on a `str`: counted in code points.
fn text_between(value: &str, min: usize, max: usize) -> bool {
    (min..=max).contains(&value.chars().count())
}

/// `CreatePlanRequest`: `actor_ids: list[str] = Field(min_length=1)`, `extra='forbid'`.
fn create_plan_request(body: &JsonBody) -> Option<Vec<String>> {
    let fields = model_fields(body, &["actor_ids"])?;
    let actor_ids = field_str_list(fields.get("actor_ids")?)?;
    (!actor_ids.is_empty()).then_some(actor_ids)
}

/// `ResolveAvidRequest`: `avid: str = Field(min_length=1, max_length=128)`.
fn resolve_avid_request(body: &JsonBody) -> Option<String> {
    let fields = model_fields(body, &["avid"])?;
    let avid = req_str(fields, "avid")?;
    text_between(&avid, 1, 128).then_some(avid)
}

/// `ApplyPlanRequest` and `CreateApplyJobRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ApplyRequest {
    revision: String,
    candidate_ids: Vec<String>,
    request_id: Option<String>,
}

/// `revision: str = Field(min_length=1, max_length=256)`, `candidate_ids: list[str] =
/// Field(default_factory=list, max_length=5_000)`, and for an apply job `request_id: str =
/// Field(min_length=16, max_length=128, pattern=r'^[A-Za-z0-9_-]+$')`.
fn apply_request(body: &JsonBody, with_request_id: bool) -> Option<ApplyRequest> {
    let allowed: &[&str] = if with_request_id {
        &["revision", "candidate_ids", "request_id"]
    } else {
        &["revision", "candidate_ids"]
    };
    let fields = model_fields(body, allowed)?;
    let revision = req_str(fields, "revision")?;
    if !text_between(&revision, 1, 256) {
        return None;
    }
    let candidate_ids = match fields.get("candidate_ids") {
        None => Vec::new(),
        Some(value) => field_str_list(value)?,
    };
    if candidate_ids.len() > 5_000 {
        return None;
    }
    let request_id = if with_request_id {
        let request_id = req_str(fields, "request_id")?;
        let pattern = request_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-');
        if !text_between(&request_id, 16, 128) || !pattern {
            return None;
        }
        Some(request_id)
    } else {
        None
    };
    Some(ApplyRequest {
        revision,
        candidate_ids,
        request_id,
    })
}

// -- views ----------------------------------------------------------------------------------

fn dict(entries: Vec<(&str, Value)>) -> Value {
    let mut dict = Dict::new();
    for (key, value) in entries {
        dict.insert(key, value);
    }
    Value::Dict(dict)
}

/// `math.floor((later - earlier).total_seconds())`, at least 0.
fn whole_seconds(later: DateTime<Utc>, earlier: DateTime<Utc>) -> i64 {
    let micros = (later - earlier).num_microseconds().unwrap_or(i64::MAX);
    micros.div_euclid(1_000_000).max(0)
}

/// `round(value, 2)`: the nearest double to the correctly rounded two-decimal text, as
/// `CPython` rounds.
fn round2(value: f64) -> f64 {
    format!("{value:.2}").parse().unwrap_or(value)
}

/// `JobProgressView.from_record(progress, state=, now=)`.
pub fn progress_view(progress: &JobProgress, state: JobState, now: DateTime<Utc>) -> Value {
    let elapsed_seconds = whole_seconds(now, progress.stage_started_at);
    let last_progress_seconds = whole_seconds(now, progress.updated_at);
    #[allow(clippy::cast_precision_loss, reason = "Python divides the integers as floats")]
    let percent = match progress.total {
        None => Value::Null,
        Some(0) => Value::Float(100.0),
        Some(total) => Value::Float(round2((progress.completed as f64 / total as f64 * 100.0).min(100.0))),
    };
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        reason = "Python's float arithmetic, then `math.ceil`"
    )]
    let eta_seconds = if progress.stage == JobStage::Done || !matches!(state, JobState::Queued | JobState::Running) {
        Value::from(0_i64)
    } else {
        match progress.total {
            None => Value::Null,
            Some(_) if progress.completed == 0 => Value::Null,
            Some(total) if progress.completed >= total => Value::from(0_i64),
            Some(_) if elapsed_seconds == 0 => Value::Null,
            Some(total) => {
                let eta = elapsed_seconds as f64 / progress.completed as f64 * (total - progress.completed) as f64;
                Value::from(eta.ceil() as i64)
            }
        }
    };
    dict(vec![
        ("stage", Value::from(progress.stage.as_str())),
        ("completed", Value::from(progress.completed)),
        ("total", progress.total.map_or(Value::Null, Value::from)),
        ("unit", Value::from(progress.unit.as_str())),
        ("current", opt_str(progress.current.as_deref())),
        ("stage_started_at", py_datetime(progress.stage_started_at)),
        ("updated_at", py_datetime(progress.updated_at)),
        ("percent", percent),
        ("eta_seconds", eta_seconds),
        ("elapsed_seconds", Value::from(elapsed_seconds)),
        ("last_progress_seconds", Value::from(last_progress_seconds)),
    ])
}

/// `JobView.from_record(record)`, at the current time.
fn job_view(record: &JobRecord) -> Value {
    let now = normalize_datetime(Utc::now());
    dict(vec![
        ("job_id", Value::from(record.job_id.as_str())),
        ("plan_id", opt_str(record.plan_id.as_deref())),
        ("operation", Value::from(record.operation.as_str())),
        ("state", Value::from(record.state.as_str())),
        ("created_at", py_datetime(record.created_at)),
        ("updated_at", py_datetime(record.updated_at)),
        ("error_code", opt_str(record.error_code.as_deref())),
        ("progress", progress_view(&record.progress, record.state, now)),
    ])
}

/// `PlanEnvelope(job=, plan=)` and `ApplyJobEnvelope(job=, result=)`.
fn envelope(record: &JobRecord, key: &str, value: Value) -> Value {
    dict(vec![("job", job_view(record)), (key, value)])
}

// -- the router -----------------------------------------------------------------------------

/// `create_fill_actor_router(...)`'s keywords.
#[derive(Clone)]
pub struct FillActorApi {
    pub service: FillActorService,
    pub repository: Arc<PostgresFillActorRepository>,
    pub jobs: FillActorJobManager,
    pub auth: MutationAuth,
    pub avid_actor_lookup: Option<AvidActorLookup>,
}

impl std::fmt::Debug for FillActorApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FillActorApi")
            .field("jobs", &self.jobs)
            .field("avid_actor_lookup", &self.avid_actor_lookup.is_some())
            .finish_non_exhaustive()
    }
}

type Shared = Arc<FillActorApi>;

/// The auth dependency: its refusal is the route's answer.
macro_rules! authorize {
    ($api:expr, $request:expr) => {
        if let Some(refusal) = $api.auth.refuse(&$request) {
            return refusal;
        }
    };
}

impl FillActorApi {
    async fn database_ready(&self) -> Result<bool, Internal> {
        self.repository.health_check().await.map_err(Internal::new)
    }

    /// `require_scan_ready()`.
    async fn require_scan_ready(&self) -> Result<Option<Response>, Internal> {
        if !self.database_ready().await? || !self.service.scan_ready().await.map_err(Internal::new)? {
            return Ok(Some(not_ready()));
        }
        Ok(None)
    }

    /// `require_apply_ready()`.
    async fn require_apply_ready(&self) -> Result<Option<Response>, Internal> {
        if !self.service.apply_enabled() {
            return Ok(Some(raise(FillActorErrorKind::MoveDisabled)));
        }
        if !self.database_ready().await? || !self.service.apply_ready().await.map_err(Internal::new)? {
            return Ok(Some(not_ready()));
        }
        Ok(None)
    }

    /// `require_repository_ready()`.
    async fn require_repository_ready(&self) -> Result<Option<Response>, Internal> {
        Ok((!self.database_ready().await?).then(not_ready))
    }
}

/// A dependency's refusal is the route's answer.
macro_rules! depend {
    ($check:expr) => {
        if let Some(refusal) = $check.await? {
            return Ok(refusal);
        }
    };
}

/// One route: the handler gets the router's state and the request.
fn route<F, Fut>(router: ApiRouter, api: &Shared, method: Method, path: &str, function: F) -> ApiRouter
where
    F: Fn(Shared, Request) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Answer> + Send + 'static,
{
    let state = Arc::clone(api);
    let text = openapi::operation(method.as_str(), path);
    router.route(
        method,
        path,
        Some(text),
        handler(move |request| function(Arc::clone(&state), request)),
    )
}

/// `create_fill_actor_router(service=, repository=, jobs=, mutation_auth=, avid_actor_lookup=)`.
pub fn create_fill_actor_router(api: FillActorApi) -> ApiRouter {
    let api: Shared = Arc::new(api);
    let mut router = ApiRouter::new("/api/fill-actor");
    router = route(router, &api, Method::POST, "/avid-actors", resolve_avid_actors);
    router = route(router, &api, Method::POST, "/plans", create_plan);
    router = route(router, &api, Method::GET, "/plans/{plan_id}", get_plan);
    router = route(router, &api, Method::POST, "/plans/{plan_id}/cancel", cancel_plan);
    router = route(
        router,
        &api,
        Method::POST,
        "/plans/{plan_id}/apply-jobs",
        create_apply_job,
    );
    router = route(router, &api, Method::GET, "/apply-jobs/{job_id}", get_apply_job);
    route(router, &api, Method::POST, "/plans/{plan_id}/apply", apply_plan)
}

fn finished_plan(state: JobState) -> bool {
    matches!(state, JobState::Completed | JobState::PartialFailed)
}

async fn resolve_avid_actors(api: Shared, mut request: Request) -> Answer {
    let body = answer!(read_json(&mut request).await);
    authorize!(api, request);
    let Some(avid) = resolve_avid_request(&body) else {
        return Ok(invalid_request());
    };
    let avid = py_strip(&avid).to_uppercase();
    let Some(lookup) = &api.avid_actor_lookup else {
        return Ok(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "avid_actor_lookup_unavailable",
        ));
    };
    let actors = match lookup(avid.clone()).await {
        Ok(actors) => actors,
        Err(error) => {
            tracing::error!(target: LOGGER, error = %error, "AVID actor lookup failed avid={avid}");
            return Ok(api_error(StatusCode::BAD_GATEWAY, "avid_actor_lookup_failed"));
        }
    };
    if actors.is_empty() {
        return Ok(api_error(StatusCode::NOT_FOUND, "avid_actors_not_found"));
    }
    let actors = actors
        .into_iter()
        .map(|(actor_id, name)| dict(vec![("actor_id", Value::from(actor_id)), ("name", Value::from(name))]))
        .collect();
    Ok(json(
        StatusCode::OK,
        &dict(vec![("avid", Value::from(avid)), ("actors", Value::List(actors))]),
    ))
}

async fn create_plan(api: Shared, mut request: Request) -> Answer {
    let body = answer!(read_json(&mut request).await);
    authorize!(api, request);
    let Some(actor_ids) = create_plan_request(&body) else {
        return Ok(invalid_request());
    };
    let actor_ids = service!(api.service.validate_actor_ids(&actor_ids));
    depend!(api.require_scan_ready());
    let job = service!(api.jobs.start_plan(&actor_ids).await);
    Ok(json(StatusCode::ACCEPTED, &envelope(&job, "plan", Value::Null)))
}

async fn get_plan(api: Shared, request: Request) -> Answer {
    let plan_id = request.param("plan_id").to_owned();
    let Some(job) = service!(api.jobs.get_job(&plan_id).await) else {
        return Ok(raise(FillActorErrorKind::UnknownPlan));
    };
    let plan = service!(api.jobs.get_plan(&plan_id).await);
    if plan.is_none() && (finished_plan(job.state) || (job.plan_id.is_none() && job.error_code.is_none())) {
        return Ok(raise(FillActorErrorKind::UnknownPlan));
    }
    let plan = plan.as_ref().map_or(Value::Null, FillActorPlan::to_value);
    Ok(json(StatusCode::OK, &envelope(&job, "plan", plan)))
}

async fn cancel_plan(api: Shared, request: Request) -> Answer {
    authorize!(api, request);
    depend!(api.require_repository_ready());
    let plan_id = request.param("plan_id");
    let result = service!(api.jobs.cancel_plan(plan_id).await);
    let job = match (result.outcome, result.job) {
        (CancelJobOutcome::NotFound, _) | (_, None) => return Ok(raise(FillActorErrorKind::UnknownPlan)),
        (CancelJobOutcome::AlreadyTerminal, _) => {
            return Ok(api_error(StatusCode::CONFLICT, "plan_not_cancellable"));
        }
        (_, Some(job)) => job,
    };
    Ok(json(StatusCode::OK, &envelope(&job, "plan", Value::Null)))
}

/// `tuple(dict.fromkeys(values))`.
fn dedupe(values: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    values
        .iter()
        .filter(|value| seen.insert(value.as_str()))
        .cloned()
        .collect()
}

async fn create_apply_job(api: Shared, mut request: Request) -> Answer {
    let body = answer!(read_json(&mut request).await);
    authorize!(api, request);
    let Some(apply) = apply_request(&body, true) else {
        return Ok(invalid_request());
    };
    let plan_id = request.param("plan_id").to_owned();
    let request_id = apply.request_id.clone().expect("an apply job request has an id");
    let normalized = dedupe(&apply.candidate_ids);
    if let Some(existing) = service!(api.jobs.get_apply_job(&request_id).await) {
        let existing_plan_id = existing
            .job
            .plan_id
            .clone()
            .or_else(|| existing.result.as_ref().map(|result| result.plan_id.clone()));
        if existing_plan_id.as_deref() == Some(plan_id.as_str())
            && existing.revision == apply.revision
            && existing.candidate_ids == normalized
        {
            let result = existing.result.as_ref().map_or(Value::Null, ApplyResult::to_value);
            return Ok(json(StatusCode::ACCEPTED, &envelope(&existing.job, "result", result)));
        }
        return Ok(raise(FillActorErrorKind::ApplyRequestConflict));
    }

    depend!(api.require_apply_ready());
    let plan_job = service!(api.jobs.get_job(&plan_id).await);
    let Some(plan_job) = plan_job.filter(|job| job.operation == JobOperation::CreatePlan) else {
        return Ok(raise(FillActorErrorKind::UnknownPlan));
    };
    if !finished_plan(plan_job.state) {
        return Ok(api_error(StatusCode::CONFLICT, "plan_not_ready"));
    }
    if service!(api.jobs.get_plan(&plan_id).await).is_none() {
        return Ok(raise(FillActorErrorKind::UnknownPlan));
    }
    let record = service!(
        api.jobs
            .start_apply(&plan_id, &apply.revision, &apply.candidate_ids, &request_id)
            .await
    );
    let result = record.result.as_ref().map_or(Value::Null, ApplyResult::to_value);
    Ok(json(StatusCode::ACCEPTED, &envelope(&record.job, "result", result)))
}

async fn get_apply_job(api: Shared, request: Request) -> Answer {
    authorize!(api, request);
    let Some(record) = service!(api.jobs.get_apply_job(request.param("job_id")).await) else {
        return Ok(raise(FillActorErrorKind::UnknownApplyJob));
    };
    let result = record.result.as_ref().map_or(Value::Null, ApplyResult::to_value);
    Ok(json(StatusCode::OK, &envelope(&record.job, "result", result)))
}

async fn apply_plan(api: Shared, mut request: Request) -> Answer {
    let body = answer!(read_json(&mut request).await);
    authorize!(api, request);
    depend!(api.require_apply_ready());
    let Some(apply) = apply_request(&body, false) else {
        return Ok(invalid_request());
    };
    let plan_id = request.param("plan_id").to_owned();
    let Some(job) = service!(api.jobs.get_job(&plan_id).await) else {
        return Ok(raise(FillActorErrorKind::UnknownPlan));
    };
    if !finished_plan(job.state) {
        return Ok(api_error(StatusCode::CONFLICT, "plan_not_ready"));
    }
    let result = service!(
        api.service
            .apply(&plan_id, &apply.revision, &apply.candidate_ids, None, None)
            .await
    );
    Ok(json(StatusCode::OK, &result.to_value()))
}

// -- health and lifespan ----------------------------------------------------------------------

/// `fill_actor_health(service=, repository=)`: the feature's readiness, reported beside the
/// app's own health, never as it.
pub fn fill_actor_health(
    service: FillActorService,
    repository: Arc<PostgresFillActorRepository>,
) -> FeatureHealthProbe {
    Arc::new(move || {
        let (service, repository) = (service.clone(), Arc::clone(&repository));
        Box::pin(async move {
            let database_ready = repository.health_check().await.map_err(Internal::new)?;
            let configured = service.configured();
            let roots_ready = service.roots_ready().await.map_err(Internal::new)?;
            let cloud_ready = service.cloud_ready().await;
            let legacy_journal_ready = service.legacy_journal_ready().await.map_err(Internal::new)?;
            let scan_ready = database_ready && roots_ready && cloud_ready;
            let apply_enabled = service.apply_enabled();
            let mut probe = Dict::new();
            // `configured` separates "nobody filled in the Settings card yet" from "the
            // configured roots are not mounted", which need different fixes.
            probe.insert("configured", Value::Bool(configured));
            probe.insert("roots", Value::Bool(roots_ready));
            probe.insert("cloud", Value::Bool(cloud_ready));
            probe.insert("legacy_journal", Value::Bool(legacy_journal_ready));
            probe.insert("apply_enabled", Value::Bool(apply_enabled));
            probe.insert("scan_ready", Value::Bool(scan_ready));
            probe.insert(
                "apply_ready",
                Value::Bool(apply_enabled && scan_ready && legacy_journal_ready),
            );
            Ok(probe)
        })
    })
}

/// `fill_actor_lifespan(service=, repository=, jobs=)`: starts and stops the feature's job
/// queue and move reconciliation.
pub struct FillActorLifespan {
    service: FillActorService,
    repository: Arc<PostgresFillActorRepository>,
    jobs: FillActorJobManager,
    maintenance: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for FillActorLifespan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FillActorLifespan")
            .field("jobs", &self.jobs)
            .finish_non_exhaustive()
    }
}

impl FillActorLifespan {
    pub fn new(
        service: FillActorService,
        repository: Arc<PostgresFillActorRepository>,
        jobs: FillActorJobManager,
    ) -> Self {
        Self {
            service,
            repository,
            jobs,
            maintenance: Mutex::new(None),
        }
    }
}

/// `maintain()`: every five seconds, reconcile the moves (when moves may run) and purge the
/// expired plans; a failed round is logged and the next one runs.
async fn maintain(service: FillActorService, repository: Arc<PostgresFillActorRepository>) {
    loop {
        let round = async {
            if service.apply_ready().await? {
                service.reconcile_moves().await?;
            }
            repository.purge_expired_plans(Utc::now()).await?;
            Ok::<(), ServiceError>(())
        };
        if let Err(error) = round.await {
            tracing::error!(target: LOGGER, error = %error, "fill-actor maintenance iteration failed");
        }
        tokio::time::sleep(MAINTENANCE_INTERVAL).await;
    }
}

impl Lifespan for FillActorLifespan {
    fn start(&self) -> BoxFuture<'_, Result<(), Internal>> {
        Box::pin(async move {
            if !self.repository.health_check().await.map_err(Internal::new)? {
                return Err(Internal::new("fill-actor repository is unavailable"));
            }
            if self.service.apply_ready().await.map_err(Internal::new)? {
                self.service.reconcile_moves().await.map_err(Internal::new)?;
            }
            self.jobs.start().await.map_err(Internal::new)?;
            let task = tokio::spawn(maintain(self.service.clone(), Arc::clone(&self.repository)));
            *self.maintenance.lock().unwrap_or_else(PoisonError::into_inner) = Some(task);
            Ok(())
        })
    }

    fn stop(&self) -> BoxFuture<'_, Result<(), Internal>> {
        Box::pin(async move {
            // A round cut short leaves any CloudDrive call it sent to settle on its own (D128).
            let task = self.maintenance.lock().unwrap_or_else(PoisonError::into_inner).take();
            if let Some(task) = task {
                task.abort();
                let _ = task.await;
            }
            self.jobs.aclose().await;
            self.service.aclose().await;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests;
