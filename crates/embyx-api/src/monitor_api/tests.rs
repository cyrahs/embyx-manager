//! Ported from `tests/test_monitor_api.py`: the router alone in an app, over a fake
//! scheduler, run history, the in-memory ledger and subscription registry, and stand-ins
//! for the manual and replacement sources.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use chrono::{TimeZone as _, Utc};
use embyx_clients::avbase::{AvbaseError, AvbaseTalent};
use embyx_core::pyjson::{self, Value};
use embyx_monitor::acquisitions::{AcquisitionState, AttemptState, Ledger, MagnetCandidate, Sighting};
use embyx_monitor::manual::{
    DirectoryListing, ManualEntry, ManualError, ManualOutcome, ManualSubmission, OfflineDirectory, SubmitOptions,
};
use embyx_monitor::replace::{ReplaceError, ReplaceOutcome, ReplacePlan, ReplaceResult};
use embyx_monitor::reports::{Counters, PipelineName, RunState, RunTrigger};
use embyx_monitor::runs::PipelineRunRecord;
use embyx_monitor::scheduler::{PipelineStatus, TrackerState, TriggerError};
use embyx_monitor::subscriptions::{SubscriptionError, SubscriptionRecord};
use embyx_monitor::support::{BoxFuture, StoreError};
use embyx_monitor::testing::{FakeLedger, FakeSubscriptions, SubscriptionSpec, make_subscription, now_stub};
use embyx_monitor::tracker::SubmitMagnet;
use http::{Method, StatusCode};

use super::*;
use crate::routing::{App, Layers};
use crate::testing::call;

const HASH_A: &str = "C12FE1C06BBA254A9DC9F519B335AA7C1367A88A";
const HASH_B: &str = "D23FE1C06BBA254A9DC9F519B335AA7C1367A88B";

fn make_record(run_id: &str, pipeline: PipelineName, state: RunState) -> PipelineRunRecord {
    PipelineRunRecord {
        run_id: run_id.to_owned(),
        pipeline,
        trigger: RunTrigger::Manual,
        state,
        started_at: Utc.with_ymd_and_hms(2026, 8, 12, 10, 0, 0).single().expect("a time"),
        finished_at: Some(Utc.with_ymd_and_hms(2026, 8, 12, 10, 5, 0).single().expect("a time")),
        stats: Counters::from([("items", 2)]),
        errors: if state == RunState::Failed {
            vec!["boom".to_owned()]
        } else {
            Vec::new()
        },
        log_tail: vec!["line-1".to_owned()],
    }
}

#[derive(Default)]
struct FakeScheduler {
    triggered: Mutex<Vec<PipelineName>>,
    busy: Mutex<bool>,
    unconfigured_reason: Mutex<Option<String>>,
    running: HashSet<PipelineName>,
}

impl FakeScheduler {
    fn triggered(&self) -> Vec<PipelineName> {
        self.triggered.lock().expect("triggered").clone()
    }
}

impl MonitorScheduling for FakeScheduler {
    fn status(&self) -> Vec<PipelineStatus> {
        vec![
            PipelineStatus {
                pipeline: PipelineName::Rss,
                enabled: true,
                configured: true,
                reason: None,
                running_run_id: None,
                next_scheduled_at: Utc.with_ymd_and_hms(2026, 8, 12, 11, 0, 0).single(),
            },
            PipelineStatus {
                pipeline: PipelineName::Archive,
                enabled: false,
                configured: false,
                reason: Some("archive source, destination, and mapping must be configured".to_owned()),
                running_run_id: None,
                next_scheduled_at: None,
            },
        ]
    }

    fn trigger(&self, pipeline: PipelineName) -> BoxFuture<'_, Result<String, TriggerError>> {
        let result = if let Some(reason) = self.unconfigured_reason.lock().expect("reason").clone() {
            Err(TriggerError::NotConfigured { pipeline, reason })
        } else if *self.busy.lock().expect("busy") {
            Err(TriggerError::Busy(pipeline))
        } else {
            self.triggered.lock().expect("triggered").push(pipeline);
            Ok("run-1".to_owned())
        };
        Box::pin(std::future::ready(result))
    }

    fn cancel_running(&self, pipeline: PipelineName) -> bool {
        self.running.contains(&pipeline)
    }

    fn tracker_state(&self) -> TrackerState {
        TrackerState::default()
    }
}

struct FakeRuns {
    ordered: Vec<PipelineRunRecord>,
}

impl RunHistory for FakeRuns {
    fn latest_run(&self, pipeline: PipelineName) -> BoxFuture<'_, Result<Option<PipelineRunRecord>, StoreError>> {
        let found = self.ordered.iter().find(|record| record.pipeline == pipeline).cloned();
        Box::pin(std::future::ready(Ok(found)))
    }

    fn list_runs(
        &self,
        pipeline: Option<PipelineName>,
        limit: i64,
        offset: i64,
    ) -> BoxFuture<'_, Result<Vec<PipelineRunRecord>, StoreError>> {
        let selected: Vec<PipelineRunRecord> = self
            .ordered
            .iter()
            .filter(|record| pipeline.is_none_or(|name| record.pipeline == name))
            .skip(usize::try_from(offset).expect("offset"))
            .take(usize::try_from(limit).expect("limit"))
            .cloned()
            .collect();
        Box::pin(std::future::ready(Ok(selected)))
    }

    fn get_run<'a>(&'a self, run_id: &'a str) -> BoxFuture<'a, Result<Option<PipelineRunRecord>, StoreError>> {
        let found = self.ordered.iter().find(|record| record.run_id == run_id).cloned();
        Box::pin(std::future::ready(Ok(found)))
    }
}

fn runs(records: Vec<PipelineRunRecord>) -> Arc<FakeRuns> {
    Arc::new(FakeRuns { ordered: records })
}

impl SubscriptionRegistry for FakeSubscriptions {
    fn list(&self) -> BoxFuture<'_, Result<Vec<SubscriptionRecord>, SubscriptionError>> {
        Box::pin(std::future::ready(Ok(self.list_now())))
    }

    fn get(&self, subscription_id: i64) -> BoxFuture<'_, Result<Option<SubscriptionRecord>, SubscriptionError>> {
        Box::pin(std::future::ready(Ok(Self::get(self, subscription_id))))
    }

    fn add_rss<'a>(
        &'a self,
        url: &'a str,
        category: &'a str,
        name: Option<&'a str>,
        seed_pending: bool,
    ) -> BoxFuture<'a, Result<SubscriptionRecord, SubscriptionError>> {
        Box::pin(std::future::ready(Self::add_rss(
            self,
            url,
            category,
            Utc::now(),
            name,
            seed_pending,
        )))
    }

    fn add_talent<'a>(
        &'a self,
        talent_id: i64,
        name: &'a str,
        aliases: &'a [String],
        category: &'a str,
        seed_pending: bool,
    ) -> BoxFuture<'a, Result<SubscriptionRecord, SubscriptionError>> {
        let talent_id = i32::try_from(talent_id).expect("a test talent id");
        Box::pin(std::future::ready(Self::add_talent(
            self,
            talent_id,
            name,
            aliases,
            category,
            Utc::now(),
            seed_pending,
        )))
    }

    fn update<'a>(
        &'a self,
        subscription_id: i64,
        enabled: Option<bool>,
        category: Option<&'a str>,
        url: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Option<SubscriptionRecord>, SubscriptionError>> {
        Box::pin(std::future::ready(Self::update(
            self,
            subscription_id,
            Utc::now(),
            enabled,
            category,
            url,
        )))
    }

    fn delete(&self, subscription_id: i64) -> BoxFuture<'_, Result<bool, SubscriptionError>> {
        Box::pin(std::future::ready(Ok(Self::delete(self, subscription_id))))
    }
}

/// `make_client(scheduler, runs, subscriptions=None)` and friends: the router in a bare app,
/// with `ApiError` rendered as the shell renders it.
fn app(
    scheduler: Arc<FakeScheduler>,
    runs: Arc<FakeRuns>,
    acquisitions: Option<AcquisitionApi>,
    subscriptions: Option<SubscriptionsApi>,
) -> App {
    let router = create_monitor_router(scheduler, runs, MutationAuth::new(None), acquisitions, subscriptions);
    App::new(
        router.into_routes(),
        None,
        Layers {
            max_request_bytes: None,
            api_no_store: false,
        },
    )
}

fn make_client(scheduler: Arc<FakeScheduler>, runs: Arc<FakeRuns>) -> App {
    app(scheduler, runs, None, None)
}

async fn get(app: &App, path: &str) -> (StatusCode, Value) {
    call(app, Method::GET, path, &[], None).await
}

async fn post(app: &App, path: &str, body: Option<&str>) -> (StatusCode, Value) {
    call(app, Method::POST, path, &[], body).await
}

fn field<'a>(value: &'a Value, key: &str) -> &'a Value {
    value.get(key).unwrap_or_else(|| panic!("no {key} in {value:?}"))
}

fn text(value: &Value, key: &str) -> String {
    field(value, key)
        .as_str()
        .unwrap_or_else(|| panic!("{key} is not a string"))
        .to_owned()
}

fn items(value: &Value) -> Vec<Value> {
    value.as_list().expect("a list").to_vec()
}

fn error_code(body: &Value) -> String {
    text(field(body, "error"), "code")
}

fn json(text: &str) -> Value {
    pyjson::loads(text).expect("JSON")
}

// py: tests/test_monitor_api.py::test_status_includes_latest_run
#[tokio::test]
async fn status_includes_latest_run() {
    let client = make_client(
        Arc::default(),
        runs(vec![make_record("run-1", PipelineName::Rss, RunState::Completed)]),
    );

    let (_, body) = get(&client, "/api/monitor/status").await;

    let body = items(&body);
    let rss = body.iter().find(|item| text(item, "pipeline") == "rss").expect("rss");
    assert_eq!(field(rss, "enabled"), &Value::Bool(true));
    assert_eq!(text(field(rss, "last_run"), "run_id"), "run-1");
    assert_eq!(field(field(rss, "last_run"), "stats"), &json(r#"{"items": 2}"#));
    let archive = body
        .iter()
        .find(|item| text(item, "pipeline") == "archive")
        .expect("archive");
    assert_eq!(field(archive, "configured"), &Value::Bool(false));
    assert_eq!(field(archive, "last_run"), &Value::Null);
}

// py: tests/test_monitor_api.py::test_trigger_starts_the_pipeline
#[tokio::test]
async fn trigger_starts_the_pipeline() {
    let scheduler = Arc::new(FakeScheduler::default());
    let client = make_client(Arc::clone(&scheduler), runs(Vec::new()));

    let (status, body) = post(&client, "/api/monitor/rss/trigger", None).await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body, json(r#"{"run_id": "run-1"}"#));
    assert_eq!(scheduler.triggered(), vec![PipelineName::Rss]);
}

// py: tests/test_monitor_api.py::test_every_pipeline_can_be_triggered_by_hand
#[tokio::test]
async fn every_pipeline_can_be_triggered_by_hand() {
    let scheduler = Arc::new(FakeScheduler::default());
    let client = make_client(Arc::clone(&scheduler), runs(Vec::new()));

    let mut codes = Vec::new();
    for name in ["rss", "archive", "mapping"] {
        codes.push(post(&client, &format!("/api/monitor/{name}/trigger"), None).await.0);
    }

    assert_eq!(codes, vec![StatusCode::ACCEPTED; 3]);
    assert_eq!(
        scheduler.triggered(),
        vec![PipelineName::Rss, PipelineName::Archive, PipelineName::Mapping]
    );
}

// py: tests/test_monitor_api.py::test_trigger_busy_and_unconfigured_and_unknown
#[tokio::test]
async fn trigger_busy_and_unconfigured_and_unknown() {
    let scheduler = Arc::new(FakeScheduler::default());
    let client = make_client(Arc::clone(&scheduler), runs(Vec::new()));

    *scheduler.busy.lock().expect("busy") = true;
    let busy = post(&client, "/api/monitor/rss/trigger", Some("{}")).await;
    *scheduler.busy.lock().expect("busy") = false;
    *scheduler.unconfigured_reason.lock().expect("reason") = Some("CloudDrive is not configured".to_owned());
    let unconfigured = post(&client, "/api/monitor/rss/trigger", Some("{}")).await;
    let unknown = post(&client, "/api/monitor/nope/trigger", Some("{}")).await;

    assert_eq!(busy.0, StatusCode::CONFLICT);
    assert_eq!(error_code(&busy.1), "pipeline_busy");
    assert_eq!(unconfigured.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(unknown.0, StatusCode::NOT_FOUND);
}

// py: tests/test_monitor_api.py::test_cancel_running_pipeline
#[tokio::test]
async fn cancel_running_pipeline() {
    let scheduler = Arc::new(FakeScheduler {
        running: HashSet::from([PipelineName::Mapping]),
        ..FakeScheduler::default()
    });
    let client = make_client(scheduler, runs(Vec::new()));

    let accepted = post(&client, "/api/monitor/mapping/cancel", None).await;
    let not_running = post(&client, "/api/monitor/rss/cancel", None).await;

    assert_eq!(accepted.0, StatusCode::OK);
    assert_eq!(accepted.1, json(r#"{"cancelling": true}"#));
    assert_eq!(not_running.0, StatusCode::CONFLICT);
}

// py: tests/test_monitor_api.py::test_runs_listing_and_detail
#[tokio::test]
async fn runs_listing_and_detail() {
    let client = make_client(
        Arc::default(),
        runs(vec![
            make_record("run-2", PipelineName::Rss, RunState::Failed),
            make_record("run-1", PipelineName::Mapping, RunState::Completed),
        ]),
    );

    let (_, everything) = get(&client, "/api/monitor/runs").await;
    let (_, rss_only) = get(&client, "/api/monitor/runs?pipeline=rss").await;
    let (_, detail) = get(&client, "/api/monitor/runs/run-2").await;
    let (missing, _) = get(&client, "/api/monitor/runs/none").await;

    let ids = |body: &Value| items(body).iter().map(|item| text(item, "run_id")).collect::<Vec<_>>();
    assert_eq!(ids(&everything), vec!["run-2", "run-1"]);
    assert_eq!(ids(&rss_only), vec!["run-2"]);
    assert_eq!(field(&detail, "errors"), &json(r#"["boom"]"#));
    assert_eq!(field(&detail, "log_tail"), &json(r#"["line-1"]"#));
    assert_eq!(missing, StatusCode::NOT_FOUND);
}

// -- acquisition ledger -------------------------------------------------------------------------

type Submitted = Arc<Mutex<Vec<(String, String)>>>;

/// `make_ledger_client(ledger, submit_ok=, tracker_reason=)`.
fn make_ledger_client(ledger: &Arc<FakeLedger>, submit_ok: bool, tracker_reason: Option<&str>) -> (App, Submitted) {
    let submitted: Submitted = Arc::default();
    let sink = Arc::clone(&submitted);
    let submit: SubmitMagnet = Arc::new(move |avid, magnet| {
        sink.lock().expect("submitted").push((avid, magnet));
        Box::pin(std::future::ready(Ok(submit_ok)))
    });
    let reason = tracker_reason.map(str::to_owned);
    let api = AcquisitionApi {
        submit_magnet: Some(submit),
        tracker_ready: Some(Arc::new(move || reason.clone())),
        ..AcquisitionApi::new(Arc::clone(ledger) as Arc<dyn Ledger>)
    };
    (app(Arc::default(), runs(Vec::new()), Some(api), None), submitted)
}

fn submitted(submitted: &Submitted) -> Vec<(String, String)> {
    submitted.lock().expect("submitted").clone()
}

/// `seed_ledger(**states)`: each AVID discovered with one sukebei magnet, then put in its
/// state.
async fn seed_ledger(states: &[(&str, AcquisitionState)]) -> Arc<FakeLedger> {
    let ledger = Arc::new(FakeLedger::new());
    for (avid, state) in states {
        ledger
            .discover(avid, "rss:Actor", now_stub(), Sighting::default())
            .await
            .expect("discover");
        ledger
            .add_attempts(
                avid,
                &[MagnetCandidate {
                    magnet: format!("magnet:?xt=urn:btih:{HASH_A}"),
                    info_hash: Some(HASH_A.to_owned()),
                    source: "sukebei".to_owned(),
                    size_hint: None,
                }],
                now_stub(),
            )
            .await
            .expect("attempts");
        ledger.state().states.insert((*avid).to_owned(), *state);
    }
    ledger
}

// py: tests/test_monitor_api.py::test_acquisitions_list_filters_by_state_and_reports_counts
#[tokio::test]
async fn acquisitions_list_filters_by_state_and_reports_counts() {
    let ledger = seed_ledger(&[
        ("ABC-123", AcquisitionState::NeedsAttention),
        ("DEF-456", AcquisitionState::Downloading),
    ])
    .await;
    let (client, _) = make_ledger_client(&ledger, true, None);

    let (_, everything) = get(&client, "/api/monitor/acquisitions").await;
    let avids: HashSet<String> = items(field(&everything, "items"))
        .iter()
        .map(|item| text(item, "avid"))
        .collect();
    assert_eq!(avids, HashSet::from(["ABC-123".to_owned(), "DEF-456".to_owned()]));
    assert_eq!(
        field(&everything, "counts"),
        &json(r#"{"needs_attention": 1, "downloading": 1}"#)
    );

    let (_, parked) = get(&client, "/api/monitor/acquisitions?state=needs_attention").await;
    let parked: Vec<String> = items(field(&parked, "items"))
        .iter()
        .map(|item| text(item, "avid"))
        .collect();
    assert_eq!(parked, vec!["ABC-123"]);
}

// py: tests/test_monitor_api.py::test_unknown_state_filter_is_rejected
#[tokio::test]
async fn unknown_state_filter_is_rejected() {
    let (client, _) = make_ledger_client(&seed_ledger(&[]).await, true, None);

    let (status, body) = get(&client, "/api/monitor/acquisitions?state=nonsense").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "unknown_state");
}

// py: tests/test_monitor_api.py::test_acquisition_detail_lists_its_attempts
#[tokio::test]
async fn acquisition_detail_lists_its_attempts() {
    let ledger = seed_ledger(&[("ABC-123", AcquisitionState::Downloading)]).await;
    let (client, _) = make_ledger_client(&ledger, true, None);

    let (_, body) = get(&client, "/api/monitor/acquisitions/ABC-123").await;

    assert_eq!(text(&body, "avid"), "ABC-123");
    let attempts = items(field(&body, "attempts"));
    assert_eq!(
        attempts
            .iter()
            .map(|attempt| field(attempt, "attempt_no").clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(1)]
    );
    assert_eq!(text(&attempts[0], "magnet_source"), "sukebei");
    assert_eq!(
        get(&client, "/api/monitor/acquisitions/ZZZ-999").await.0,
        StatusCode::NOT_FOUND
    );
}

// py: tests/test_monitor_api.py::test_retry_submits_the_next_magnet_and_resumes_downloading
#[tokio::test]
async fn retry_submits_the_next_magnet_and_resumes_downloading() {
    let ledger = seed_ledger(&[("ABC-123", AcquisitionState::Exhausted)]).await;
    let (client, sent) = make_ledger_client(&ledger, true, None);

    let (_, body) = post(&client, "/api/monitor/acquisitions/ABC-123/retry", None).await;

    assert_eq!(text(&body, "state"), "downloading");
    assert_eq!(
        submitted(&sent).into_iter().map(|(avid, _)| avid).collect::<Vec<_>>(),
        vec!["ABC-123"]
    );
}

// py: tests/test_monitor_api.py::test_retry_without_a_magnet_left_is_a_conflict
#[tokio::test]
async fn retry_without_a_magnet_left_is_a_conflict() {
    let ledger = seed_ledger(&[("ABC-123", AcquisitionState::Exhausted)]).await;
    ledger.claim_next_pending("ABC-123", now_stub()).await.expect("claim");
    let (client, sent) = make_ledger_client(&ledger, true, None);

    let (status, body) = post(&client, "/api/monitor/acquisitions/ABC-123/retry", None).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error_code(&body), "no_magnet_left");
    assert!(submitted(&sent).is_empty());
}

// py: tests/test_monitor_api.py::test_a_rejected_offline_task_marks_the_attempt_and_reports_upstream
#[tokio::test]
async fn a_rejected_offline_task_marks_the_attempt_and_reports_upstream() {
    let ledger = seed_ledger(&[("ABC-123", AcquisitionState::Exhausted)]).await;
    let (client, _) = make_ledger_client(&ledger, false, None);

    let (status, _) = post(&client, "/api/monitor/acquisitions/ABC-123/retry", None).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(ledger.attempt_states("ABC-123"), vec![AttemptState::Error]);
}

// py: tests/test_monitor_api.py::test_an_operator_magnet_is_recorded_and_submitted
#[tokio::test]
async fn an_operator_magnet_is_recorded_and_submitted() {
    let ledger = seed_ledger(&[("ABC-123", AcquisitionState::NeedsAttention)]).await;
    let (client, sent) = make_ledger_client(&ledger, true, None);
    let magnet = format!("magnet:?xt=urn:btih:{HASH_B}&dn=ABC-123");

    let (_, body) = post(
        &client,
        "/api/monitor/acquisitions/ABC-123/magnet",
        Some(&format!(r#"{{"magnet": "{magnet}"}}"#)),
    )
    .await;

    assert_eq!(text(&body, "state"), "downloading");
    assert_eq!(submitted(&sent), vec![("ABC-123".to_owned(), magnet.clone())]);
    assert_eq!(ledger.magnets("ABC-123").last(), Some(&magnet));
}

// py: tests/test_monitor_api.py::test_an_unusable_magnet_is_refused
#[tokio::test]
async fn an_unusable_magnet_is_refused() {
    let ledger = seed_ledger(&[("ABC-123", AcquisitionState::NeedsAttention)]).await;
    let (client, sent) = make_ledger_client(&ledger, true, None);

    let (status, body) = post(
        &client,
        "/api/monitor/acquisitions/ABC-123/magnet",
        Some(r#"{"magnet": "https://example.com/x"}"#),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "unusable_magnet");
    assert!(submitted(&sent).is_empty());
}

// py: tests/test_monitor_api.py::test_a_magnet_already_tried_is_not_queued_twice
#[tokio::test]
async fn a_magnet_already_tried_is_not_queued_twice() {
    let ledger = seed_ledger(&[("ABC-123", AcquisitionState::NeedsAttention)]).await;
    let (client, sent) = make_ledger_client(&ledger, true, None);

    let (status, body) = post(
        &client,
        "/api/monitor/acquisitions/ABC-123/magnet",
        Some(&format!(r#"{{"magnet": "magnet:?xt=urn:btih:{HASH_A}"}}"#)),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error_code(&body), "magnet_already_tried");
    assert!(submitted(&sent).is_empty());
}

// py: tests/test_monitor_api.py::test_a_magnet_live_for_another_avid_is_a_conflict
#[tokio::test]
async fn a_magnet_live_for_another_avid_is_a_conflict() {
    let ledger = seed_ledger(&[("ABC-123", AcquisitionState::NeedsAttention)]).await;
    ledger.state().claim_attempt_in_use = true;
    let (client, sent) = make_ledger_client(&ledger, true, None);

    let (status, body) = post(
        &client,
        "/api/monitor/acquisitions/ABC-123/magnet",
        Some(&format!(r#"{{"magnet": "magnet:?xt=urn:btih:{HASH_B}"}}"#)),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error_code(&body), "magnet_in_use");
    assert!(submitted(&sent).is_empty());
}

// py: tests/test_monitor_api.py::test_ignoring_an_avid_takes_it_out_of_the_pipeline
#[tokio::test]
async fn ignoring_an_avid_takes_it_out_of_the_pipeline() {
    let ledger = seed_ledger(&[("ABC-123", AcquisitionState::Exhausted)]).await;
    let (client, _) = make_ledger_client(&ledger, true, None);

    let (_, body) = post(&client, "/api/monitor/acquisitions/ABC-123/ignore", None).await;

    assert_eq!(text(&body, "state"), "ignored");
}

// py: tests/test_monitor_api.py::test_an_archived_avid_cannot_be_retried_or_ignored
#[tokio::test]
async fn an_archived_avid_cannot_be_retried_or_ignored() {
    let ledger = seed_ledger(&[("ABC-123", AcquisitionState::Archived)]).await;
    let (client, sent) = make_ledger_client(&ledger, true, None);

    assert_eq!(
        post(&client, "/api/monitor/acquisitions/ABC-123/retry", None).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        post(&client, "/api/monitor/acquisitions/ABC-123/ignore", None).await.0,
        StatusCode::CONFLICT
    );
    assert!(submitted(&sent).is_empty());
}

// py: tests/test_monitor_api.py::test_resume_hands_a_parked_avid_back_to_the_tracker
#[tokio::test]
async fn resume_hands_a_parked_avid_back_to_the_tracker() {
    let ledger = seed_ledger(&[("ABC-123", AcquisitionState::NeedsAttention)]).await;
    let (client, _) = make_ledger_client(&ledger, true, None);

    let (_, body) = post(&client, "/api/monitor/acquisitions/ABC-123/resume", None).await;

    assert_eq!(text(&body, "state"), "downloading");
}

// py: tests/test_monitor_api.py::test_resume_only_applies_to_parked_avids
#[tokio::test]
async fn resume_only_applies_to_parked_avids() {
    let ledger = seed_ledger(&[("ABC-123", AcquisitionState::Downloading)]).await;
    let (client, _) = make_ledger_client(&ledger, true, None);

    let (status, body) = post(&client, "/api/monitor/acquisitions/ABC-123/resume", None).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error_code(&body), "not_parked");
}

// py: tests/test_monitor_api.py::test_tracker_status_reports_why_it_is_not_running
#[tokio::test]
async fn tracker_status_reports_why_it_is_not_running() {
    let (client, _) = make_ledger_client(&seed_ledger(&[]).await, true, Some("archive is disabled"));

    let (_, body) = get(&client, "/api/monitor/tracker").await;

    assert_eq!(field(&body, "running"), &Value::Bool(false));
    assert_eq!(text(&body, "reason"), "archive is disabled");
}

// py: tests/test_monitor_api.py::test_ledger_routes_are_absent_without_a_ledger
#[tokio::test]
async fn ledger_routes_are_absent_without_a_ledger() {
    let client = make_client(Arc::default(), runs(Vec::new()));

    assert_eq!(get(&client, "/api/monitor/acquisitions").await.0, StatusCode::NOT_FOUND);
}

// -- the manual input source ------------------------------------------------------------------

/// `FakeManual`: stands in for `ManualIntakeSource`; its own behaviour is tested separately.
#[derive(Default)]
struct FakeManual {
    /// `ManualIntakeError(code)` for every call.
    error: Option<&'static str>,
    browsed: Mutex<Vec<String>>,
    submitted: Mutex<Vec<(Vec<String>, String)>>,
}

fn refusal(code: &str) -> ManualError {
    match code {
        "directory_not_found" => ManualError::DirectoryNotFound,
        "directory_not_routed" => ManualError::DirectoryNotRouted,
        other => panic!("no refusal {other}"),
    }
}

impl ManualIntake for FakeManual {
    fn browse<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<DirectoryListing, ManualError>> {
        let result = if let Some(code) = self.error {
            Err(refusal(code))
        } else {
            self.browsed.lock().expect("browsed").push(path.to_owned());
            Ok(DirectoryListing {
                path: path.to_owned(),
                parent: Some("/".to_owned()),
                entries: vec![
                    OfflineDirectory {
                        path: format!("{path}/embyx_in"),
                        name: "embyx_in".to_owned(),
                        configured: true,
                        routed: true,
                    },
                    OfflineDirectory {
                        path: format!("{path}/misc"),
                        name: "misc".to_owned(),
                        configured: false,
                        routed: false,
                    },
                ],
            })
        };
        Box::pin(std::future::ready(result))
    }

    fn default_directory(&self) -> BoxFuture<'_, Result<Option<String>, ManualError>> {
        Box::pin(std::future::ready(Ok(Some("/115/embyx_in".to_owned()))))
    }

    fn check_target<'a>(&'a self, task_dir_path: &'a str) -> BoxFuture<'a, Result<String, ManualError>> {
        Box::pin(std::future::ready(Ok(task_dir_path.to_owned())))
    }

    fn submit<'a>(
        &'a self,
        inputs: &'a [String],
        task_dir_path: &'a str,
        _options: SubmitOptions,
    ) -> BoxFuture<'a, Result<ManualSubmission, ManualError>> {
        let result = if let Some(code) = self.error {
            Err(refusal(code))
        } else {
            self.submitted
                .lock()
                .expect("submitted")
                .push((inputs.to_vec(), task_dir_path.to_owned()));
            Ok(ManualSubmission {
                task_dir_path: task_dir_path.to_owned(),
                entries: vec![
                    ManualEntry {
                        text: "abc-123".to_owned(),
                        avid: Some("ABC-123".to_owned()),
                        outcome: ManualOutcome::Submitted,
                        archived_paths: Vec::new(),
                    },
                    ManualEntry {
                        text: "def-456".to_owned(),
                        avid: Some("DEF-456".to_owned()),
                        outcome: ManualOutcome::AlreadyInLibrary,
                        archived_paths: vec!["embyx/DEF/DEF-456.mp4".to_owned()],
                    },
                ],
            })
        };
        Box::pin(std::future::ready(result))
    }
}

fn make_manual_client(manual: &Arc<FakeManual>) -> App {
    let api = AcquisitionApi {
        manual: Some(Arc::clone(manual) as Arc<dyn ManualIntake>),
        ..AcquisitionApi::new(Arc::new(FakeLedger::new()))
    };
    app(Arc::default(), runs(Vec::new()), Some(api), None)
}

// py: tests/test_monitor_api.py::test_browsing_reports_the_directories_and_where_the_picker_opens
#[tokio::test]
async fn browsing_reports_the_directories_and_where_the_picker_opens() {
    let manual = Arc::new(FakeManual::default());
    let client = make_manual_client(&manual);

    let (_, body) = get(&client, "/api/monitor/manual/directories?path=%2F115").await;

    assert_eq!(*manual.browsed.lock().expect("browsed"), vec!["/115"]);
    assert_eq!(text(&body, "parent"), "/");
    assert_eq!(text(&body, "default_path"), "/115/embyx_in");
    let entries: Vec<(String, Value)> = items(field(&body, "entries"))
        .iter()
        .map(|item| (text(item, "name"), field(item, "routed").clone()))
        .collect();
    assert_eq!(
        entries,
        vec![
            ("embyx_in".to_owned(), Value::Bool(true)),
            ("misc".to_owned(), Value::Bool(false)),
        ]
    );
}

// py: tests/test_monitor_api.py::test_submitting_reports_the_outcome_of_every_line
#[tokio::test]
async fn submitting_reports_the_outcome_of_every_line() {
    let manual = Arc::new(FakeManual::default());
    let client = make_manual_client(&manual);

    let (_, body) = post(
        &client,
        "/api/monitor/manual",
        Some(r#"{"inputs": ["abc-123", "def-456"], "task_dir_path": "/115/embyx_in"}"#),
    )
    .await;

    assert_eq!(
        *manual.submitted.lock().expect("submitted"),
        vec![(
            vec!["abc-123".to_owned(), "def-456".to_owned()],
            "/115/embyx_in".to_owned()
        )]
    );
    assert_eq!(text(&body, "task_dir_path"), "/115/embyx_in");
    let outcomes: Vec<(String, String)> = items(field(&body, "items"))
        .iter()
        .map(|item| (text(item, "avid"), text(item, "outcome")))
        .collect();
    assert_eq!(
        outcomes,
        vec![
            ("ABC-123".to_owned(), "submitted".to_owned()),
            ("DEF-456".to_owned(), "already_in_library".to_owned()),
        ]
    );
    assert_eq!(
        field(&items(field(&body, "items"))[1], "archived_paths"),
        &json(r#"["embyx/DEF/DEF-456.mp4"]"#)
    );
}

// py: tests/test_monitor_api.py::test_a_directory_without_a_route_is_refused_with_its_reason
#[tokio::test]
async fn a_directory_without_a_route_is_refused_with_its_reason() {
    let client = make_manual_client(&Arc::new(FakeManual {
        error: Some("directory_not_routed"),
        ..FakeManual::default()
    }));

    let (status, body) = post(
        &client,
        "/api/monitor/manual",
        Some(r#"{"inputs": ["abc-123"], "task_dir_path": "/115/misc"}"#),
    )
    .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(error_code(&body), "directory_not_routed");
}

// py: tests/test_monitor_api.py::test_a_missing_directory_is_a_404
#[tokio::test]
async fn a_missing_directory_is_a_404() {
    let client = make_manual_client(&Arc::new(FakeManual {
        error: Some("directory_not_found"),
        ..FakeManual::default()
    }));

    assert_eq!(
        get(&client, "/api/monitor/manual/directories?path=/nope").await.0,
        StatusCode::NOT_FOUND
    );
}

// py: tests/test_monitor_api.py::test_manual_routes_are_absent_without_the_source
#[tokio::test]
async fn manual_routes_are_absent_without_the_source() {
    let (client, _) = make_ledger_client(&Arc::new(FakeLedger::new()), true, None);

    assert_eq!(
        get(&client, "/api/monitor/manual/directories").await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        post(
            &client,
            "/api/monitor/manual",
            Some(r#"{"inputs": [], "task_dir_path": "/x"}"#)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

// -- replacements (换版) -------------------------------------------------------------------------

struct FakeReplacement {
    ledger: Arc<FakeLedger>,
    error: Option<&'static str>,
    started: Mutex<Vec<(String, Option<String>)>>,
}

impl FakeReplacement {
    fn new(ledger: FakeLedger, error: Option<&'static str>) -> Arc<Self> {
        Arc::new(Self {
            ledger: Arc::new(ledger),
            error,
            started: Mutex::default(),
        })
    }

    fn the_plan() -> ReplacePlan {
        ReplacePlan {
            avid: "IPZ-661".to_owned(),
            holdings: vec!["embyx/rank/IPZ/IPZ-661.mp4".to_owned()],
            state: Some(AcquisitionState::Archived),
            task_dir_path: Some("/115/embyx_in/rank".to_owned()),
            blocked: None,
        }
    }
}

impl Replacement for FakeReplacement {
    fn plan<'a>(&'a self, _text: &'a str) -> BoxFuture<'a, Result<ReplacePlan, ReplaceError>> {
        let result = match self.error {
            Some(code) => Err(ReplaceError::Refused(code)),
            None => Ok(Self::the_plan()),
        };
        Box::pin(std::future::ready(result))
    }

    fn start<'a>(
        &'a self,
        text: &'a str,
        magnet: Option<&'a str>,
    ) -> BoxFuture<'a, Result<ReplaceResult, ReplaceError>> {
        Box::pin(async move {
            if let Some(code) = self.error {
                return Err(ReplaceError::Refused(code));
            }
            self.started
                .lock()
                .expect("started")
                .push((text.to_owned(), magnet.map(str::to_owned)));
            let record = self
                .ledger
                .begin_replacement(
                    "IPZ-661",
                    "manual",
                    "/115/embyx_in/rank",
                    &["embyx/rank/IPZ/IPZ-661.mp4".to_owned()],
                    now_stub(),
                )
                .await?
                .expect("a replacement");
            Ok(ReplaceResult {
                plan: Self::the_plan(),
                outcome: ReplaceOutcome::Submitted,
                record,
            })
        })
    }
}

fn make_replace_client(replacement: &Arc<FakeReplacement>) -> App {
    let api = AcquisitionApi {
        replacement: Some(Arc::clone(replacement) as Arc<dyn Replacement>),
        ..AcquisitionApi::new(Arc::clone(&replacement.ledger) as Arc<dyn Ledger>)
    };
    app(Arc::default(), runs(Vec::new()), Some(api), None)
}

// py: tests/test_monitor_api.py::test_a_replacement_is_previewed_then_started
#[tokio::test]
async fn a_replacement_is_previewed_then_started() {
    let replacement = FakeReplacement::new(FakeLedger::with_known(&[("IPZ-661", AcquisitionState::Archived)]), None);
    let client = make_replace_client(&replacement);

    let (_, plan) = get(&client, "/api/monitor/replace?avid=ipz-661").await;
    assert_eq!(
        plan,
        json(
            r#"{"avid": "IPZ-661", "holdings": ["embyx/rank/IPZ/IPZ-661.mp4"], "state": "archived",
                "task_dir_path": "/115/embyx_in/rank", "blocked": null}"#
        )
    );

    let (_, body) = post(
        &client,
        "/api/monitor/replace",
        Some(r#"{"avid": "ipz-661", "magnet": "magnet:?xt=urn:btih:x"}"#),
    )
    .await;
    assert_eq!(
        *replacement.started.lock().expect("started"),
        vec![("ipz-661".to_owned(), Some("magnet:?xt=urn:btih:x".to_owned()))]
    );
    assert_eq!(text(&body, "outcome"), "submitted");
    assert_eq!(text(field(&body, "acquisition"), "state"), "discovered");
    assert_eq!(
        field(field(&body, "acquisition"), "replacing"),
        &json(r#"["embyx/rank/IPZ/IPZ-661.mp4"]"#)
    );
}

// py: tests/test_monitor_api.py::test_a_refused_replacement_answers_with_its_reason
#[tokio::test]
async fn a_refused_replacement_answers_with_its_reason() {
    let client = make_replace_client(&FakeReplacement::new(FakeLedger::new(), Some("not_in_library")));
    let (status, body) = post(&client, "/api/monitor/replace", Some(r#"{"avid": "XYZ-001"}"#)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body), "not_in_library");

    let client = make_replace_client(&FakeReplacement::new(FakeLedger::new(), Some("acquisition_active")));
    assert_eq!(
        get(&client, "/api/monitor/replace?avid=XYZ-001").await.0,
        StatusCode::CONFLICT
    );
}

// py: tests/test_monitor_api.py::test_replacement_routes_are_absent_without_the_source
#[tokio::test]
async fn replacement_routes_are_absent_without_the_source() {
    let (client, _) = make_ledger_client(&Arc::new(FakeLedger::new()), true, None);

    assert_eq!(
        get(&client, "/api/monitor/replace?avid=x").await.0,
        StatusCode::NOT_FOUND
    );
}

// -- subscriptions ------------------------------------------------------------------------------

/// `subscriptions_api(records, categories=, resolve_talent=)`.
fn subscriptions_api(records: Vec<SubscriptionRecord>, resolve_talent: Option<TalentResolver>) -> SubscriptionsApi {
    SubscriptionsApi {
        repository: Arc::new(FakeSubscriptions::new(records)),
        categories: Arc::new(|| vec!["Actor".to_owned(), "Rank".to_owned()]),
        resolve_talent,
    }
}

fn subscriptions_client(api: SubscriptionsApi) -> App {
    app(Arc::default(), runs(Vec::new()), None, Some(api))
}

fn rss(subscription_id: i64, url: &str) -> SubscriptionRecord {
    make_subscription(
        subscription_id,
        SubscriptionSpec {
            url: Some(url.to_owned()),
            ..SubscriptionSpec::default()
        },
    )
}

async fn patch(app: &App, path: &str, body: &str) -> (StatusCode, Value) {
    call(app, Method::PATCH, path, &[], Some(body)).await
}

// py: tests/test_monitor_api.py::test_subscriptions_are_listed_with_the_configured_categories
#[tokio::test]
async fn subscriptions_are_listed_with_the_configured_categories() {
    let record = make_subscription(
        1,
        SubscriptionSpec {
            url: Some("https://rsshub.test/javbus/star/rwt".to_owned()),
            name: Some("演员甲".to_owned()),
            ..SubscriptionSpec::default()
        },
    );
    let client = subscriptions_client(subscriptions_api(vec![record], None));

    let (_, body) = get(&client, "/api/monitor/subscriptions").await;

    assert_eq!(field(&body, "categories"), &json(r#"["Actor", "Rank"]"#));
    let listed: Vec<(String, String, String)> = items(field(&body, "items"))
        .iter()
        .map(|item| (text(item, "url"), text(item, "feed_url"), text(item, "name")))
        .collect();
    assert_eq!(
        listed,
        vec![(
            "https://rsshub.test/javbus/star/rwt".to_owned(),
            "https://rsshub.test/javbus/star/rwt".to_owned(),
            "演员甲".to_owned()
        )]
    );
}

// py: tests/test_monitor_api.py::test_subscription_routes_are_absent_without_the_registry
#[tokio::test]
async fn subscription_routes_are_absent_without_the_registry() {
    let client = make_client(Arc::default(), runs(Vec::new()));

    assert_eq!(
        get(&client, "/api/monitor/subscriptions").await.0,
        StatusCode::NOT_FOUND
    );
}

// py: tests/test_monitor_api.py::test_creating_a_subscription_validates_the_url_and_category
#[tokio::test]
async fn creating_a_subscription_validates_the_url_and_category() {
    let client = subscriptions_client(subscriptions_api(Vec::new(), None));
    let create = |body: &'static str| post(&client, "/api/monitor/subscriptions", Some(body));

    let (status, created) = create(r#"{"url": " https://rsshub.test/javbus/star/rwt ", "category": "Actor"}"#).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(text(&created, "url"), "https://rsshub.test/javbus/star/rwt");
    assert_eq!(text(&created, "category"), "Actor");
    assert_eq!(field(&created, "seed_pending"), &Value::Bool(false));

    let (status, bad_url) = create(r#"{"url": "ftp://x/y", "category": "Actor"}"#).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(bad_url, json(r#"{"error": {"code": "invalid_feed_url"}}"#));

    let (_, bad_category) = create(r#"{"url": "https://rsshub.test/o", "category": "Nope"}"#).await;
    assert_eq!(bad_category, json(r#"{"error": {"code": "unknown_category"}}"#));

    let (status, duplicate) = create(r#"{"url": "https://rsshub.test/javbus/star/rwt", "category": "Rank"}"#).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(duplicate, json(r#"{"error": {"code": "subscription_exists"}}"#));
}

// py: tests/test_monitor_api.py::test_a_subscription_can_be_disabled_moved_and_deleted
#[tokio::test]
async fn a_subscription_can_be_disabled_moved_and_deleted() {
    let client = subscriptions_client(subscriptions_api(
        vec![make_subscription(1, SubscriptionSpec::default())],
        None,
    ));

    let (_, disabled) = patch(&client, "/api/monitor/subscriptions/1", r#"{"enabled": false}"#).await;
    assert_eq!(field(&disabled, "enabled"), &Value::Bool(false));

    let (_, moved) = patch(&client, "/api/monitor/subscriptions/1", r#"{"category": "Rank"}"#).await;
    assert_eq!(text(&moved, "category"), "Rank");
    assert_eq!(field(&moved, "enabled"), &Value::Bool(false));

    assert_eq!(
        patch(&client, "/api/monitor/subscriptions/1", r#"{"category": "Nope"}"#)
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        patch(&client, "/api/monitor/subscriptions/9", r#"{"enabled": true}"#)
            .await
            .0,
        StatusCode::NOT_FOUND
    );

    let delete = || call(&client, Method::DELETE, "/api/monitor/subscriptions/1", &[], None);
    assert_eq!(delete().await.0, StatusCode::NO_CONTENT);
    assert_eq!(delete().await.0, StatusCode::NOT_FOUND);
}

// py: tests/test_monitor_api.py::test_creating_a_talent_subscription_stores_its_names_and_seeds_the_first_poll
#[tokio::test]
async fn creating_a_talent_subscription_stores_its_names_and_seeds_the_first_poll() {
    let client = subscriptions_client(subscriptions_api(Vec::new(), None));
    let create = |body: &'static str| post(&client, "/api/monitor/subscriptions", Some(body));

    let (status, body) = create(
        r#"{"kind": "avbase_talent", "category": "Actor", "talent_id": 5022, "name": "河北彩花",
            "aliases": ["河北彩伽", " 河北彩花 ", ""], "seed": true}"#,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(text(&body, "kind"), "avbase_talent");
    assert_eq!(text(&body, "feed_url"), "https://www.avbase.net/talents/5022/feed");
    assert_eq!(field(&body, "aliases"), &json(r#"["河北彩伽"]"#));
    assert_eq!(field(&body, "seed_pending"), &Value::Bool(true));
    assert_eq!(field(&body, "cursor_size"), &Value::Int(0));

    let (_, missing) = create(r#"{"kind": "avbase_talent", "category": "Actor", "name": "河北彩花"}"#).await;
    assert_eq!(missing, json(r#"{"error": {"code": "invalid_talent"}}"#));

    let (status, _) =
        create(r#"{"kind": "avbase_talent", "category": "Rank", "talent_id": 5022, "name": "河北彩花"}"#).await;
    assert_eq!(status, StatusCode::CONFLICT);

    let (_, unknown) = create(r#"{"kind": "javbus", "category": "Actor"}"#).await;
    assert_eq!(unknown, json(r#"{"error": {"code": "unknown_subscription_kind"}}"#));
}

// py: tests/test_monitor_api.py::test_a_talent_subscription_from_a_name_alone_is_resolved_on_avbase
#[tokio::test]
async fn a_talent_subscription_from_a_name_alone_is_resolved_on_avbase() {
    let resolve: TalentResolver = Arc::new(|query: String| {
        let answer = match query.as_str() {
            "down" => Err(AvbaseError::Unavailable("challenge page".to_owned())),
            "河北彩伽" | "https://www.avbase.net/talents/5022/feed" => Ok(Some(AvbaseTalent {
                talent_id: 5022,
                name: "河北彩花".to_owned(),
                aliases: vec!["河北彩伽".to_owned()],
                total_works: 2,
            })),
            _ => Ok(None),
        };
        Box::pin(std::future::ready(answer))
    });
    let client = subscriptions_client(subscriptions_api(Vec::new(), Some(resolve)));
    let create = |body: &'static str| post(&client, "/api/monitor/subscriptions", Some(body));

    let (status, body) = create(r#"{"kind": "avbase_talent", "category": "Actor", "name": " 河北彩伽 "}"#).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        (
            field(&body, "talent_id").clone(),
            text(&body, "name"),
            field(&body, "aliases").clone(),
            field(&body, "seed_pending").clone()
        ),
        (
            Value::Int(5022),
            "河北彩花".to_owned(),
            json(r#"["河北彩伽"]"#),
            Value::Bool(false)
        )
    );

    let (status, unknown) = create(r#"{"kind": "avbase_talent", "category": "Actor", "name": "nobody"}"#).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(unknown, json(r#"{"error": {"code": "talent_not_found"}}"#));

    let (status, down) = create(r#"{"kind": "avbase_talent", "category": "Actor", "name": "down"}"#).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(down, json(r#"{"error": {"code": "avbase_unavailable"}}"#));

    let (status, _) =
        create(r#"{"kind": "avbase_talent", "category": "Actor", "name": "https://www.avbase.net/talents/5022/feed"}"#)
            .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

// py: tests/test_monitor_api.py::test_a_feed_url_can_be_changed_but_not_into_another_subscription_or_onto_a_talent
#[tokio::test]
async fn a_feed_url_can_be_changed_but_not_into_another_subscription_or_onto_a_talent() {
    let client = subscriptions_client(subscriptions_api(
        vec![
            rss(1, "http://rsshub/javlibrary/rank"),
            rss(2, "https://rsshub.example/other"),
            make_subscription(
                3,
                SubscriptionSpec {
                    name: Some("石川澪".to_owned()),
                    talent_id: Some(46144),
                    ..SubscriptionSpec::default()
                },
            ),
        ],
        None,
    ));

    let (status, moved) = patch(
        &client,
        "/api/monitor/subscriptions/1",
        r#"{"url": "http://rsshub.rss.svc.cluster.local/javlibrary/rank"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        text(&moved, "url"),
        "http://rsshub.rss.svc.cluster.local/javlibrary/rank"
    );

    let (status, _) = patch(
        &client,
        "/api/monitor/subscriptions/1",
        r#"{"url": "https://rsshub.example/other"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    let (_, bad) = patch(&client, "/api/monitor/subscriptions/1", r#"{"url": "ftp://x"}"#).await;
    assert_eq!(bad, json(r#"{"error": {"code": "invalid_feed_url"}}"#));

    let (_, talent) = patch(
        &client,
        "/api/monitor/subscriptions/3",
        r#"{"url": "https://rsshub.example/x"}"#,
    )
    .await;
    assert_eq!(talent, json(r#"{"error": {"code": "url_not_editable"}}"#));
}

// -- beyond the Python tests: FastAPI's own answers -------------------------------------------

/// Query and path integers are validated as FastAPI validates them (a fixed 422), the token
/// is checked before them, and a body is read before the token.
#[tokio::test]
async fn parameters_are_validated_after_the_token() {
    let router = create_monitor_router(
        Arc::new(FakeScheduler::default()),
        runs(Vec::new()),
        MutationAuth::new(Some("secret")),
        None,
        Some(subscriptions_api(
            vec![make_subscription(1, SubscriptionSpec::default())],
            None,
        )),
    );
    let client = App::new(
        router.into_routes(),
        None,
        Layers {
            max_request_bytes: None,
            api_no_store: false,
        },
    );
    let bearer = [("authorization", "Bearer secret")];

    assert_eq!(
        get(&client, "/api/monitor/runs?limit=x").await,
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            json(r#"{"error": {"code": "invalid_request"}}"#)
        )
    );
    assert_eq!(
        get(&client, "/api/monitor/runs?limit=%205&offset=0").await.0,
        StatusCode::OK
    );
    assert_eq!(
        call(&client, Method::DELETE, "/api/monitor/subscriptions/x", &[], None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&client, Method::DELETE, "/api/monitor/subscriptions/x", &bearer, None)
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        call(&client, Method::POST, "/api/monitor/rss/trigger", &[], None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &client,
            Method::PATCH,
            "/api/monitor/subscriptions/1",
            &bearer,
            Some(r#"{"enabled": true, "extra": 1}"#)
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    // A subscription path with POST falls through to the pipeline route (Starlette's order).
    assert_eq!(
        call(
            &client,
            Method::POST,
            "/api/monitor/subscriptions/cancel",
            &bearer,
            None
        )
        .await,
        (
            StatusCode::NOT_FOUND,
            json(r#"{"error": {"code": "unknown_pipeline"}}"#)
        )
    );
}
