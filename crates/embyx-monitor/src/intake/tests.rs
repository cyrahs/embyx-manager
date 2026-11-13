//! Ported from `tests/test_monitor_intake.py`: the intake against the fake ledger, scripted
//! magnet sources and a scripted CloudDrive, on a fixed clock with recorded retry waits. Plus
//! the goldens of `resolve()` over stub sources and of `str(FileOperationResult)`.

use std::collections::VecDeque;
use std::sync::Mutex;

use chrono::{DateTime, TimeZone as _, Utc};
use embyx_clients::clouddrive::RpcError;
use embyx_clients::clouddrive::client::code_by_name;
use embyx_clients::net::StatusError;
use embyx_clients::net::retry::RecordingSleeper;
use embyx_core::golden;
use embyx_core::pyjson::Value;

use super::*;
use crate::acquisitions::AcquisitionSource;
use crate::testing::FakeLedger;

const TASK_DIR: &str = "/115/task";
const HASH_A: &str = "C12FE1C06BBA254A9DC9F519B335AA7C1367A88A";
const HASH_B: &str = "D23FE1C06BBA254A9DC9F519B335AA7C1367A88B";
const HASH_C: &str = "E34FE1C06BBA254A9DC9F519B335AA7C1367A88C";

fn magnet(info_hash: &str) -> String {
    format!("magnet:?xt=urn:btih:{info_hash}&dn=ABC-123")
}

/// `datetime.now(UTC)` for the whole test: the intake's clock and the fake rows' timers.
fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 3, 5, 30, 0).single().expect("valid")
}

fn make_ctx() -> RunContext {
    RunContext::new("test-intake")
}

fn runtime_error(message: &str) -> SourceError {
    Raised::new("RuntimeError", message).into()
}

fn success() -> FileOperationResult {
    FileOperationResult {
        success: true,
        ..FileOperationResult::default()
    }
}

/// `AsyncMock(return_value=...)` / `AsyncMock(side_effect=...)` for sukebei's `get_magnet`.
struct FakeSukebei {
    answer: Result<Option<String>, SourceError>,
    calls: Mutex<Vec<String>>,
}

impl SukebeiMagnets for FakeSukebei {
    fn get_magnet<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Option<String>, SourceError>> {
        self.calls.lock().expect("calls").push(avid.to_owned());
        Box::pin(std::future::ready(self.answer.clone()))
    }
}

/// The same for JavBus's `get_magnets`.
struct FakeJavBus {
    answer: Result<Vec<JavBusMagnet>, SourceError>,
    calls: Mutex<Vec<String>>,
}

impl JavBusMagnets for FakeJavBus {
    fn get_magnets<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Vec<JavBusMagnet>, SourceError>> {
        self.calls.lock().expect("calls").push(avid.to_owned());
        Box::pin(std::future::ready(self.answer.clone()))
    }
}

/// CloudDrive's `add_offline_files`: the queued side effects in order (a list `side_effect`),
/// then `fallback` for every further call (a single `side_effect` or the `return_value`).
struct FakeCloud {
    queued: Mutex<VecDeque<Result<FileOperationResult, SourceError>>>,
    fallback: Result<FileOperationResult, SourceError>,
    calls: Mutex<Vec<(Vec<String>, String)>>,
}

impl FakeCloud {
    fn calls(&self) -> Vec<(Vec<String>, String)> {
        self.calls.lock().expect("calls").clone()
    }
}

impl OfflineFiles for FakeCloud {
    fn add_offline_files<'a>(
        &'a self,
        urls: &'a [String],
        dst_dir: &'a str,
    ) -> BoxFuture<'a, Result<FileOperationResult, SourceError>> {
        self.calls
            .lock()
            .expect("calls")
            .push((urls.to_vec(), dst_dir.to_owned()));
        let next = self.queued.lock().expect("queued").pop_front();
        Box::pin(std::future::ready(next.unwrap_or_else(|| self.fallback.clone())))
    }
}

/// What a `make_intake` call can set (its keyword arguments).
#[derive(Default)]
struct Setup {
    ledger: Option<FakeLedger>,
    sukebei_magnet: Option<String>,
    sukebei_error: Option<SourceError>,
    javbus_magnets: Vec<JavBusMagnet>,
    javbus_error: Option<SourceError>,
    add_error: Option<SourceError>,
    add_sequence: Vec<Result<FileOperationResult, SourceError>>,
    release_date_lookup: Option<Arc<dyn ReleaseDateLookup>>,
}

/// `deps`: the fakes the intake was built from.
struct Deps {
    ledger: Arc<FakeLedger>,
    sukebei: Arc<FakeSukebei>,
    javbus: Arc<FakeJavBus>,
    cloud: Arc<FakeCloud>,
    sleeper: RecordingSleeper,
}

impl Deps {
    fn state(&self, avid: &str) -> AcquisitionState {
        self.ledger.state().states[avid]
    }

    fn next_action_at(&self, avid: &str) -> Option<DateTime<Utc>> {
        self.ledger.state().next_action_at.get(avid).copied().flatten()
    }

    /// `deps.ledger.release_dates[avid]` (the AVID has an entry).
    fn release_date(&self, avid: &str) -> Option<NaiveDate> {
        self.ledger.state().release_dates[avid]
    }
}

fn javbus(magnet: &str, size: i128) -> JavBusMagnet {
    JavBusMagnet {
        magnet: magnet.to_owned(),
        size: String::new(),
        size_int: size,
        tags: Vec::new(),
        score: size,
    }
}

fn make_intake(setup: Setup) -> (AcquisitionIntake, Deps) {
    let ledger = Arc::new(setup.ledger.unwrap_or_default());
    let sukebei = Arc::new(FakeSukebei {
        answer: setup.sukebei_error.map_or(Ok(setup.sukebei_magnet), Err),
        calls: Mutex::default(),
    });
    let javbus = Arc::new(FakeJavBus {
        answer: setup.javbus_error.map_or(Ok(setup.javbus_magnets), Err),
        calls: Mutex::default(),
    });
    let cloud = Arc::new(FakeCloud {
        queued: Mutex::new(setup.add_sequence.into()),
        fallback: setup.add_error.map_or_else(|| Ok(success()), Err),
        calls: Mutex::default(),
    });
    let sleeper = RecordingSleeper::new();
    let intake = AcquisitionIntake::new(IntakeParts {
        ledger: Arc::clone(&ledger) as Arc<dyn Ledger>,
        sukebei: Arc::clone(&sukebei) as Arc<dyn SukebeiMagnets>,
        javbus: Arc::clone(&javbus) as Arc<dyn JavBusMagnets>,
        cloud: Arc::clone(&cloud) as Arc<dyn OfflineFiles>,
        failed_cooldown_seconds: 3600,
        on_submitted: None,
        release_date_lookup: setup.release_date_lookup,
    })
    .expect("a valid cooldown")
    .with_clock(Arc::new(now))
    .with_sleeper(sleeper.sleeper());
    (
        intake,
        Deps {
            ledger,
            sukebei,
            javbus,
            cloud,
            sleeper,
        },
    )
}

async fn enqueue(intake: &AcquisitionIntake) -> IntakeOutcome {
    intake
        .enqueue(
            "ABC-123",
            AcquisitionSource::FillActor.as_str(),
            TASK_DIR,
            &make_ctx(),
            None,
            None,
        )
        .await
        .expect("enqueue")
}

fn submitted_once(deps: &Deps, link: &str, dir: &str) {
    assert_eq!(deps.cloud.calls(), [(vec![link.to_owned()], dir.to_owned())]);
}

// py: tests/test_monitor_intake.py::test_enqueue_submits_the_first_candidate
#[tokio::test]
async fn enqueue_submits_the_first_candidate() {
    let (intake, deps) = make_intake(Setup {
        sukebei_magnet: Some(magnet(HASH_A)),
        ..Setup::default()
    });

    let outcome = enqueue(&intake).await;

    assert_eq!(outcome, IntakeOutcome::Submitted);
    submitted_once(&deps, &magnet(HASH_A), TASK_DIR);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Downloading);
    assert_eq!(deps.ledger.state().sources["ABC-123"], "fill_actor");
    assert_eq!(deps.ledger.state().task_dirs["ABC-123"].as_deref(), Some(TASK_DIR));
    assert_eq!(deps.ledger.attempt_states("ABC-123"), [AttemptState::Submitted]);
}

// py: tests/test_monitor_intake.py::test_queue_records_a_due_discovery_without_resolving
#[tokio::test]
async fn queue_records_a_due_discovery_without_resolving() {
    let (intake, deps) = make_intake(Setup {
        sukebei_magnet: Some(magnet(HASH_A)),
        ..Setup::default()
    });

    let outcome = intake
        .queue(
            "ABC-123",
            AcquisitionSource::FillActor.as_str(),
            TASK_DIR,
            &make_ctx(),
            None,
        )
        .await
        .expect("queue");

    assert_eq!(outcome, IntakeOutcome::Queued);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Discovered);
    assert_eq!(deps.ledger.state().task_dirs["ABC-123"].as_deref(), Some(TASK_DIR));
    assert!(deps.next_action_at("ABC-123").is_some());
    assert!(deps.sukebei.calls.lock().expect("calls").is_empty());
    assert!(deps.cloud.calls().is_empty());
}

// py: tests/test_monitor_intake.py::test_queue_skips_an_avid_the_ledger_already_owns
#[tokio::test]
async fn queue_skips_an_avid_the_ledger_already_owns() {
    let (intake, _) = make_intake(Setup {
        ledger: Some(FakeLedger::with_known(&[("ABC-123", AcquisitionState::Archived)])),
        ..Setup::default()
    });

    let outcome = intake
        .queue(
            "ABC-123",
            AcquisitionSource::FillActor.as_str(),
            TASK_DIR,
            &make_ctx(),
            None,
        )
        .await
        .expect("queue");

    assert_eq!(outcome, IntakeOutcome::AlreadyTracked);
}

// py: tests/test_monitor_intake.py::test_retry_resolves_and_submits_a_queued_discovery
#[tokio::test]
async fn retry_resolves_and_submits_a_queued_discovery() {
    let (intake, deps) = make_intake(Setup {
        ledger: Some(FakeLedger::with_known(&[("ABC-123", AcquisitionState::Discovered)])),
        sukebei_magnet: Some(magnet(HASH_A)),
        ..Setup::default()
    });
    let record = deps.ledger.get("ABC-123").await.expect("get").expect("a row");

    let outcome = intake.retry(&record, &make_ctx(), Some(TASK_DIR)).await.expect("retry");

    assert_eq!(outcome, IntakeOutcome::Submitted);
    submitted_once(&deps, &magnet(HASH_A), TASK_DIR);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Downloading);
}

// py: tests/test_monitor_intake.py::test_retry_parks_a_queued_discovery_with_no_magnet
#[tokio::test]
async fn retry_parks_a_queued_discovery_with_no_magnet() {
    let (intake, deps) = make_intake(Setup {
        ledger: Some(FakeLedger::with_known(&[("ABC-123", AcquisitionState::Discovered)])),
        ..Setup::default()
    });
    let record = deps.ledger.get("ABC-123").await.expect("get").expect("a row");

    let outcome = intake.retry(&record, &make_ctx(), Some(TASK_DIR)).await.expect("retry");

    assert_eq!(outcome, IntakeOutcome::NoMagnet);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::ResolveFailed);
    assert!(deps.next_action_at("ABC-123").is_some());
}

// py: tests/test_monitor_intake.py::test_enqueue_skips_an_avid_the_ledger_already_owns
#[tokio::test]
async fn enqueue_skips_an_avid_the_ledger_already_owns() {
    let (intake, deps) = make_intake(Setup {
        ledger: Some(FakeLedger::with_known(&[("ABC-123", AcquisitionState::Archived)])),
        sukebei_magnet: Some(magnet(HASH_A)),
        ..Setup::default()
    });

    let outcome = enqueue(&intake).await;

    assert_eq!(outcome, IntakeOutcome::AlreadyTracked);
    assert!(deps.sukebei.calls.lock().expect("calls").is_empty());
    assert!(deps.cloud.calls().is_empty());
}

// py: tests/test_monitor_intake.py::test_enqueue_parks_an_avid_with_no_magnet
#[tokio::test]
async fn enqueue_parks_an_avid_with_no_magnet() {
    let (intake, deps) = make_intake(Setup::default());

    let outcome = enqueue(&intake).await;

    assert_eq!(outcome, IntakeOutcome::NoMagnet);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::ResolveFailed);
    assert!(deps.next_action_at("ABC-123").is_some());
    assert!(deps.cloud.calls().is_empty());
}

// py: tests/test_monitor_intake.py::test_enqueue_parks_when_resolution_raises
#[tokio::test]
async fn enqueue_parks_when_resolution_raises() {
    let (intake, deps) = make_intake(Setup {
        sukebei_error: Some(runtime_error("sukebei down")),
        ..Setup::default()
    });

    let outcome = enqueue(&intake).await;

    assert_eq!(outcome, IntakeOutcome::NoMagnet);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::ResolveFailed);
    assert!(deps.next_action_at("ABC-123").is_some());
}

// py: tests/test_monitor_intake.py::test_enqueue_reports_failure_when_every_submission_errors
#[tokio::test]
async fn enqueue_reports_failure_when_every_submission_errors() {
    let (intake, deps) = make_intake(Setup {
        sukebei_magnet: Some(magnet(HASH_A)),
        add_error: Some(runtime_error("offline queue rejected it")),
        ..Setup::default()
    });

    let outcome = enqueue(&intake).await;

    assert_eq!(outcome, IntakeOutcome::SubmitFailed);
    assert_eq!(deps.ledger.attempt_states("ABC-123"), [AttemptState::Error]);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Discovered);
}

// py: tests/test_monitor_intake.py::test_enqueue_falls_through_to_the_next_candidate
#[tokio::test]
async fn enqueue_falls_through_to_the_next_candidate() {
    let (intake, deps) = make_intake(Setup {
        sukebei_magnet: Some(magnet(HASH_A)),
        javbus_magnets: vec![javbus(&magnet(HASH_B), 1)],
        add_sequence: vec![Err(runtime_error("offline queue rejected it")), Ok(success())],
        ..Setup::default()
    });

    let outcome = enqueue(&intake).await;

    assert_eq!(outcome, IntakeOutcome::Submitted);
    assert_eq!(
        deps.ledger.attempt_states("ABC-123"),
        [AttemptState::Error, AttemptState::Submitted]
    );
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Downloading);
}

/// `parked_ledger(state, due=True, task_dir=TASK_DIR)`.
fn parked_ledger(state: AcquisitionState, due: bool, task_dir: Option<&str>) -> FakeLedger {
    let ledger = FakeLedger::with_known(&[("ABC-123", state)]);
    {
        let mut fake = ledger.state();
        let delta = TimeDelta::hours(if due { -1 } else { 1 });
        fake.next_action_at.insert("ABC-123".to_owned(), Some(now() + delta));
        fake.task_dirs.insert("ABC-123".to_owned(), task_dir.map(str::to_owned));
    }
    ledger
}

// py: tests/test_monitor_intake.py::test_retry_submits_to_the_directory_pinned_on_the_record
#[tokio::test]
async fn retry_submits_to_the_directory_pinned_on_the_record() {
    let (intake, deps) = make_intake(Setup {
        ledger: Some(parked_ledger(AcquisitionState::ResolveFailed, true, Some(TASK_DIR))),
        sukebei_magnet: Some(magnet(HASH_A)),
        ..Setup::default()
    });

    let processed = intake
        .retry_due(&make_ctx(), Some("/115/other"), 50)
        .await
        .expect("retry_due");

    assert_eq!(processed, 1);
    submitted_once(&deps, &magnet(HASH_A), TASK_DIR);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Downloading);
    assert_eq!(deps.ledger.attempt_states("ABC-123"), [AttemptState::Submitted]);
}

// py: tests/test_monitor_intake.py::test_retry_falls_back_when_the_record_has_no_directory
#[tokio::test]
async fn retry_falls_back_when_the_record_has_no_directory() {
    let (intake, deps) = make_intake(Setup {
        ledger: Some(parked_ledger(AcquisitionState::ResolveFailed, true, None)),
        sukebei_magnet: Some(magnet(HASH_A)),
        ..Setup::default()
    });

    intake
        .retry_due(&make_ctx(), Some("/115/other"), 50)
        .await
        .expect("retry_due");

    submitted_once(&deps, &magnet(HASH_A), "/115/other");
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Downloading);
}

// py: tests/test_monitor_intake.py::test_retry_reparks_when_no_directory_exists_at_all
#[tokio::test]
async fn retry_reparks_when_no_directory_exists_at_all() {
    let (intake, deps) = make_intake(Setup {
        ledger: Some(parked_ledger(AcquisitionState::ResolveFailed, true, None)),
        sukebei_magnet: Some(magnet(HASH_A)),
        ..Setup::default()
    });

    intake.retry_due(&make_ctx(), None, 50).await.expect("retry_due");

    assert!(deps.sukebei.calls.lock().expect("calls").is_empty());
    assert!(deps.cloud.calls().is_empty());
    assert_eq!(deps.state("ABC-123"), AcquisitionState::ResolveFailed);
    assert!(deps.next_action_at("ABC-123").expect("a timer") > now());
}

// py: tests/test_monitor_intake.py::test_retry_rearms_the_cooldown_when_nothing_resolves
#[tokio::test]
async fn retry_rearms_the_cooldown_when_nothing_resolves() {
    // An exhausted record parks back as resolve_failed with a fresh deadline; leaving the
    // expired one in place would re-select the row every pass.
    let (intake, deps) = make_intake(Setup {
        ledger: Some(parked_ledger(AcquisitionState::Exhausted, true, Some(TASK_DIR))),
        ..Setup::default()
    });

    intake.retry_due(&make_ctx(), None, 50).await.expect("retry_due");

    assert_eq!(deps.state("ABC-123"), AcquisitionState::ResolveFailed);
    assert!(deps.next_action_at("ABC-123").expect("a timer") > now());
    assert!(deps.cloud.calls().is_empty());
}

// py: tests/test_monitor_intake.py::test_retry_rearms_the_cooldown_when_every_candidate_was_already_tried
#[tokio::test]
async fn retry_rearms_the_cooldown_when_every_candidate_was_already_tried() {
    let ledger = parked_ledger(AcquisitionState::ResolveFailed, true, Some(TASK_DIR));
    ledger
        .add_attempts(
            "ABC-123",
            &[MagnetCandidate {
                magnet: magnet(HASH_A),
                info_hash: Some(HASH_A.to_owned()),
                source: "sukebei".to_owned(),
                size_hint: None,
            }],
            now(),
        )
        .await
        .expect("add");
    ledger.claim_next_pending("ABC-123", now()).await.expect("claim");
    ledger
        .transition_attempt("ABC-123", 1, AttemptState::Submitted, AttemptState::Error, now(), None)
        .await
        .expect("transition");
    let (intake, deps) = make_intake(Setup {
        ledger: Some(ledger),
        sukebei_magnet: Some(magnet(HASH_A)),
        ..Setup::default()
    });
    let record = deps.ledger.get("ABC-123").await.expect("get").expect("a row");

    let outcome = intake.retry(&record, &make_ctx(), None).await.expect("retry");

    assert_eq!(outcome, IntakeOutcome::SubmitFailed);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::ResolveFailed);
    assert!(deps.next_action_at("ABC-123").expect("a timer") > now());
    assert!(deps.cloud.calls().is_empty());
}

// py: tests/test_monitor_intake.py::test_retry_due_skips_records_still_cooling_down
#[tokio::test]
async fn retry_due_skips_records_still_cooling_down() {
    let (intake, deps) = make_intake(Setup {
        ledger: Some(parked_ledger(AcquisitionState::ResolveFailed, false, Some(TASK_DIR))),
        sukebei_magnet: Some(magnet(HASH_A)),
        ..Setup::default()
    });

    let processed = intake.retry_due(&make_ctx(), None, 50).await.expect("retry_due");

    assert_eq!(processed, 0);
    assert!(deps.sukebei.calls.lock().expect("calls").is_empty());
    assert_eq!(deps.state("ABC-123"), AcquisitionState::ResolveFailed);
}

// py: tests/test_monitor_intake.py::test_resolve_slots_the_item_magnet_between_sukebei_and_javbus
#[tokio::test]
async fn resolve_slots_the_item_magnet_between_sukebei_and_javbus() {
    let (intake, _) = make_intake(Setup {
        sukebei_magnet: Some(magnet(HASH_A)),
        javbus_magnets: vec![javbus(&magnet(HASH_C), 5)],
        ..Setup::default()
    });

    let candidates = intake
        .resolve("ABC-123", &make_ctx(), Some(&magnet(HASH_B)))
        .await
        .expect("resolve");

    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.magnet.clone())
            .collect::<Vec<_>>(),
        [magnet(HASH_A), magnet(HASH_B), magnet(HASH_C)]
    );
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.source.as_str())
            .collect::<Vec<_>>(),
        ["sukebei", "rss_item", "javbus"]
    );
}

// py: tests/test_monitor_intake.py::test_a_sighting_with_a_magnet_wakes_a_cooling_row
#[tokio::test]
async fn a_sighting_with_a_magnet_wakes_a_cooling_row() {
    let (intake, deps) = make_intake(Setup {
        ledger: Some(parked_ledger(AcquisitionState::ResolveFailed, false, Some(TASK_DIR))),
        ..Setup::default()
    });

    let outcome = intake
        .enqueue(
            "ABC-123",
            AcquisitionSource::Manual.as_str(),
            TASK_DIR,
            &make_ctx(),
            Some(&magnet(HASH_A)),
            None,
        )
        .await
        .expect("enqueue");

    assert_eq!(outcome, IntakeOutcome::Submitted);
    submitted_once(&deps, &magnet(HASH_A), TASK_DIR);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Downloading);
    assert_eq!(deps.ledger.attempt_states("ABC-123"), [AttemptState::Submitted]);
}

// py: tests/test_monitor_intake.py::test_a_sighting_without_a_magnet_leaves_a_cooling_row_alone
#[tokio::test]
async fn a_sighting_without_a_magnet_leaves_a_cooling_row_alone() {
    let (intake, deps) = make_intake(Setup {
        ledger: Some(parked_ledger(AcquisitionState::ResolveFailed, false, Some(TASK_DIR))),
        sukebei_magnet: Some(magnet(HASH_A)),
        ..Setup::default()
    });

    let outcome = enqueue(&intake).await;

    assert_eq!(outcome, IntakeOutcome::AlreadyTracked);
    assert!(deps.sukebei.calls.lock().expect("calls").is_empty());
}

// py: tests/test_monitor_intake.py::test_a_woken_row_that_resolves_nothing_is_reparked_from_its_own_state
#[tokio::test]
async fn a_woken_row_that_resolves_nothing_is_reparked_from_its_own_state() {
    // A magnet without a usable hash wakes the row but cannot be submitted; the park must
    // start from resolve_failed, not assume a fresh discovery.
    let ledger = parked_ledger(AcquisitionState::ResolveFailed, false, Some(TASK_DIR));
    let stale_deadline = ledger.state().next_action_at["ABC-123"];
    let (intake, deps) = make_intake(Setup {
        ledger: Some(ledger),
        ..Setup::default()
    });
    // Time has moved on since the row was parked, as it has in Python's test.
    let intake = intake.with_clock(Arc::new(|| now() + TimeDelta::seconds(1)));

    let outcome = intake
        .enqueue(
            "ABC-123",
            AcquisitionSource::Manual.as_str(),
            TASK_DIR,
            &make_ctx(),
            Some("magnet:?dn=ABC-123"),
            None,
        )
        .await
        .expect("enqueue");

    assert_eq!(outcome, IntakeOutcome::NoMagnet);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::ResolveFailed);
    assert_ne!(deps.next_action_at("ABC-123"), stale_deadline);
    assert!(deps.next_action_at("ABC-123").expect("a timer") > now());
}

// py: tests/test_monitor_intake.py::test_park_follows_the_release_schedule_when_the_date_is_known
#[tokio::test]
async fn park_follows_the_release_schedule_when_the_date_is_known() {
    let (intake, deps) = make_intake(Setup::default());
    let release = (now() + TimeDelta::days(60)).date_naive();

    let outcome = intake
        .enqueue(
            "ABC-123",
            AcquisitionSource::FillActor.as_str(),
            TASK_DIR,
            &make_ctx(),
            None,
            Some(release),
        )
        .await
        .expect("enqueue");

    assert_eq!(outcome, IntakeOutcome::NoMagnet);
    assert_eq!(deps.release_date("ABC-123"), Some(release));
    // Weeks ahead of the release: a weekly probe, not the one-hour fallback cooldown.
    let wait = deps.next_action_at("ABC-123").expect("a timer") - now();
    assert!(
        TimeDelta::days(6) + TimeDelta::hours(23) < wait && wait <= TimeDelta::days(7),
        "{wait}"
    );
}

// py: tests/test_monitor_intake.py::test_queue_stores_the_release_date
#[tokio::test]
async fn queue_stores_the_release_date() {
    let (intake, deps) = make_intake(Setup::default());
    let release = NaiveDate::from_ymd_opt(2026, 10, 2).expect("valid");

    intake
        .queue(
            "ABC-123",
            AcquisitionSource::FillActor.as_str(),
            TASK_DIR,
            &make_ctx(),
            Some(release),
        )
        .await
        .expect("queue");

    assert_eq!(deps.release_date("ABC-123"), Some(release));
}

// -- release dates filled in at parking time ---------------------------------

/// `AsyncMock(return_value=answer)` as a release-date lookup, recording who asked.
struct ScriptedLookup {
    answer: Option<NaiveDate>,
    asked: Mutex<Vec<String>>,
}

impl ScriptedLookup {
    fn new(answer: Option<NaiveDate>) -> Arc<Self> {
        Arc::new(Self {
            answer,
            asked: Mutex::default(),
        })
    }

    fn asked(&self) -> Vec<String> {
        self.asked.lock().expect("asked").clone()
    }
}

impl ReleaseDateLookup for ScriptedLookup {
    fn lookup<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Option<NaiveDate>> {
        self.asked.lock().expect("asked").push(avid.to_owned());
        Box::pin(std::future::ready(self.answer))
    }
}

// py: tests/test_monitor_intake.py::test_parking_a_row_without_a_release_date_looks_it_up_and_anchors_the_schedule
#[tokio::test]
async fn parking_a_row_without_a_release_date_looks_it_up_and_anchors_the_schedule() {
    let release = now().date_naive() + TimeDelta::days(30);
    let lookup = ScriptedLookup::new(Some(release));
    let (intake, deps) = make_intake(Setup {
        release_date_lookup: Some(Arc::clone(&lookup) as Arc<dyn ReleaseDateLookup>),
        ..Setup::default()
    });

    assert_eq!(enqueue(&intake).await, IntakeOutcome::NoMagnet);

    assert_eq!(lookup.asked(), ["ABC-123"]);
    assert_eq!(deps.release_date("ABC-123"), Some(release));
    // A month out: the weekly preheat, not the one-hour fallback cooldown.
    let wait = deps.next_action_at("ABC-123").expect("a timer") - now();
    assert!(
        TimeDelta::days(6) + TimeDelta::hours(23) < wait && wait <= TimeDelta::days(7),
        "{wait}"
    );
}

// py: tests/test_monitor_intake.py::test_a_row_that_already_has_a_release_date_is_not_looked_up
#[tokio::test]
async fn a_row_that_already_has_a_release_date_is_not_looked_up() {
    let lookup = ScriptedLookup::new(NaiveDate::from_ymd_opt(2026, 1, 1));
    let (intake, deps) = make_intake(Setup {
        release_date_lookup: Some(Arc::clone(&lookup) as Arc<dyn ReleaseDateLookup>),
        ..Setup::default()
    });
    let release = NaiveDate::from_ymd_opt(2030, 1, 1).expect("valid");

    intake
        .enqueue("ABC-123", "rss:Actor", TASK_DIR, &make_ctx(), None, Some(release))
        .await
        .expect("enqueue");

    assert!(lookup.asked().is_empty());
    assert_eq!(deps.release_date("ABC-123"), Some(release));
}

// py: tests/test_monitor_intake.py::test_an_unknown_release_date_keeps_the_fallback_cooldown
#[tokio::test]
async fn an_unknown_release_date_keeps_the_fallback_cooldown() {
    let (intake, deps) = make_intake(Setup {
        release_date_lookup: Some(ScriptedLookup::new(None) as Arc<dyn ReleaseDateLookup>),
        ..Setup::default()
    });

    assert_eq!(enqueue(&intake).await, IntakeOutcome::NoMagnet);

    assert_eq!(deps.release_date("ABC-123"), None);
    let wait = deps.next_action_at("ABC-123").expect("a timer") - now();
    assert!(TimeDelta::minutes(59) < wait && wait <= TimeDelta::hours(1), "{wait}");
}

// py: tests/test_monitor_intake.py::test_a_retry_fills_in_the_release_date_of_an_old_row
#[tokio::test]
async fn a_retry_fills_in_the_release_date_of_an_old_row() {
    let ledger = FakeLedger::new();
    ledger
        .discover("ABC-123", "rss:Actor", now(), Sighting::in_dir(TASK_DIR))
        .await
        .expect("discover");
    let released = now().date_naive() - TimeDelta::days(3);
    let (intake, deps) = make_intake(Setup {
        ledger: Some(ledger),
        release_date_lookup: Some(ScriptedLookup::new(Some(released)) as Arc<dyn ReleaseDateLookup>),
        ..Setup::default()
    });

    let record = deps.ledger.get("ABC-123").await.expect("get").expect("a row");
    assert_eq!(
        intake.retry(&record, &make_ctx(), None).await.expect("retry"),
        IntakeOutcome::NoMagnet
    );

    assert_eq!(deps.release_date("ABC-123"), Some(released));
    // Inside the release window: four-hourly.
    let wait = deps.next_action_at("ABC-123").expect("a timer") - now();
    assert!(
        TimeDelta::hours(3) + TimeDelta::minutes(59) < wait && wait <= TimeDelta::hours(4),
        "{wait}"
    );
}

fn javbus_status(status: u16, reason: &str) -> SourceError {
    JavBusError::Http(HttpxError::Status(StatusError {
        status,
        reason: reason.to_owned(),
        url: "https://www.javbus.com/ABC-123".to_owned(),
        location: None,
    }))
    .into()
}

// py: tests/test_monitor_intake.py::test_javbus_not_listing_a_work_yet_is_not_an_error
#[tokio::test]
async fn javbus_not_listing_a_work_yet_is_not_an_error() {
    let (intake, _) = make_intake(Setup {
        javbus_error: Some(javbus_status(404, "Not Found")),
        ..Setup::default()
    });
    let ctx = make_ctx();

    assert!(intake.resolve("ABC-123", &ctx, None).await.expect("resolve").is_empty());
    assert!(ctx.errors().is_empty());

    let (intake, _) = make_intake(Setup {
        javbus_error: Some(javbus_status(503, "Service Unavailable")),
        ..Setup::default()
    });
    let ctx = make_ctx();

    assert!(intake.resolve("ABC-123", &ctx, None).await.expect("resolve").is_empty());
    assert_eq!(ctx.errors().len(), 1);
}

// -- beyond the Python tests ------------------------------------------------------

fn rpc_error(details: &str) -> SourceError {
    let code = code_by_name("UNKNOWN").expect("a code");
    CloudDriveError::Rpc(RpcError {
        code,
        details: details.to_owned(),
        debug: format!("UNKNOWN:{details}"),
        streaming: false,
    })
    .into()
}

/// An RPC failure is retried by `CLOUD_RETRY` (three attempts, waits of 1 s and 2 s) and its
/// details are written into the run log; a refused submission writes `str(result)`.
#[tokio::test]
async fn rpc_failures_are_retried_and_logged_with_their_details() {
    let (intake, deps) = make_intake(Setup {
        sukebei_magnet: Some(magnet(HASH_A)),
        javbus_magnets: vec![javbus(&magnet(HASH_B), 1)],
        add_sequence: vec![
            Err(rpc_error("quota")),
            Err(rpc_error("quota")),
            Err(rpc_error("quota")),
            Ok(FileOperationResult {
                success: false,
                error_message: "full".to_owned(),
                ..FileOperationResult::default()
            }),
        ],
        ..Setup::default()
    });
    let ctx = make_ctx();

    let outcome = intake
        .enqueue("ABC-123", "rss:Actor", TASK_DIR, &ctx, None, None)
        .await
        .expect("enqueue");

    assert_eq!(outcome, IntakeOutcome::SubmitFailed);
    assert_eq!(deps.cloud.calls().len(), 4);
    assert_eq!(deps.sleeper.delays(), [1.0, 2.0]);
    assert_eq!(
        deps.ledger.attempt_states("ABC-123"),
        [AttemptState::Error, AttemptState::Error]
    );
    assert_eq!(
        ctx.log_tail(),
        [
            "ERROR: Failed to add magnet for ABC-123: quota: <_InactiveRpcError of RPC that terminated with:\n\
             \tstatus = StatusCode.UNKNOWN\n\tdetails = \"quota\"\n\tdebug_error_string = \"UNKNOWN:quota\"\n>",
            "ERROR: Failed to add magnet for ABC-123: errorMessage: \"full\"\n",
        ]
    );
    assert_eq!(ctx.stats().get("add_failed"), Some(2));
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Discovered);
}

/// CloudDrive's '任务已存在' means the magnet is already queued there: the attempt counts as
/// submitted (after `CLOUD_RETRY` has tried three times, as an RPC error is retried).
#[tokio::test]
async fn a_duplicate_task_at_clouddrive_counts_as_submitted() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let (intake, deps) = make_intake(Setup {
        sukebei_magnet: Some(magnet(HASH_A)),
        add_error: Some(rpc_error("任务已存在")),
        ..Setup::default()
    });
    let intake = AcquisitionIntake {
        on_submitted: Some(Arc::new(move || {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        })),
        ..intake
    };
    let ctx = make_ctx();

    let outcome = intake
        .enqueue("ABC-123", "rss:Actor", TASK_DIR, &ctx, None, None)
        .await
        .expect("enqueue");

    assert_eq!(outcome, IntakeOutcome::Submitted);
    assert_eq!(deps.cloud.calls().len(), 3);
    assert_eq!(deps.sleeper.delays(), [1.0, 2.0]);
    assert_eq!(ctx.log_tail(), ["WARNING: Duplicate magnet for ABC-123"]);
    assert_eq!(ctx.stats().get("duplicates"), Some(1));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Downloading);
}

/// A cooldown beyond `timedelta`'s range is refused at construction, as Python's constructor
/// raises.
#[test]
fn a_cooldown_beyond_timedelta_is_refused() {
    let (_, deps) = make_intake(Setup::default());
    let error = AcquisitionIntake::new(IntakeParts {
        ledger: Arc::clone(&deps.ledger) as Arc<dyn Ledger>,
        sukebei: Arc::clone(&deps.sukebei) as Arc<dyn SukebeiMagnets>,
        javbus: Arc::clone(&deps.javbus) as Arc<dyn JavBusMagnets>,
        cloud: Arc::clone(&deps.cloud) as Arc<dyn OfflineFiles>,
        failed_cooldown_seconds: i64::MAX,
        on_submitted: None,
        release_date_lookup: None,
    })
    .expect_err("refused");
    assert_eq!(error.0, "Python int too large to convert to C int");
}

/// `str(FileOperationResult)` as the run log quotes a refused submission.
#[test]
fn operation_results_print_as_python_does() {
    let document = golden::load("monitor/operation_result.json");
    for case in golden::cases(&document) {
        let fields = golden::get(case, "fields");
        let result = FileOperationResult {
            success: matches!(fields.get("success"), Some(Value::Bool(true))),
            error_message: fields.get("errorMessage").map(golden::string).unwrap_or_default(),
            result_file_paths: fields
                .get("resultFilePaths")
                .map(|paths| golden::list(paths).iter().map(golden::string).collect())
                .unwrap_or_default(),
        };
        assert_eq!(
            operation_result_text(&result),
            golden::string(golden::get(case, "str")),
            "{fields:?}"
        );
    }
}

fn raised(value: &Value) -> SourceError {
    let pair = golden::list(value);
    Raised::new(golden::string(&pair[0]), golden::string(&pair[1])).into()
}

fn strings(value: &Value) -> Vec<String> {
    golden::list(value).iter().map(golden::string).collect()
}

/// `resolve()` over stub sources: order, de-duplication by hash, unusable magnets, the cap
/// of five, JavBus's 404 and other failures, and what a sukebei failure raises.
#[tokio::test]
async fn resolve_matches_python_over_stub_sources() {
    let document = golden::load("monitor/resolve.json");
    for case in golden::cases(&document) {
        let name = golden::string(golden::get(case, "name"));
        let mut setup = Setup::default();
        match case.get("sukebei_raises") {
            Some(error) => setup.sukebei_error = Some(raised(error)),
            None => setup.sukebei_magnet = case.get("sukebei").and_then(Value::as_str).map(str::to_owned),
        }
        if let Some(status) = case.get("javbus_status") {
            let status = u16::try_from(golden::int(status)).expect("a status");
            let reason = match status {
                404 => "Not Found",
                503 => "Service Unavailable",
                other => panic!("no reason phrase for {other}"),
            };
            setup.javbus_error = Some(javbus_status(status, reason));
        } else if let Some(error) = case.get("javbus_raises") {
            setup.javbus_error = Some(raised(error));
        } else if let Some(magnets) = case.get("javbus") {
            setup.javbus_magnets = golden::list(magnets)
                .iter()
                .map(|entry| JavBusMagnet {
                    magnet: golden::string(golden::get(entry, "magnet")),
                    size: golden::string(golden::get(entry, "size")),
                    size_int: golden::int(golden::get(entry, "size_int")),
                    tags: strings(golden::get(entry, "tags")),
                    score: golden::int(golden::get(entry, "score")),
                })
                .collect();
        }
        let item = case.get("item").and_then(Value::as_str).map(str::to_owned);
        let (intake, _) = make_intake(setup);
        let ctx = make_ctx();

        let resolved = intake.resolve("ABC-123", &ctx, item.as_deref()).await;

        if let Some(expected) = case.get("raises") {
            let error = resolved.expect_err(&name);
            let pair = strings(expected);
            assert_eq!([error.class_name().to_owned(), error.to_string()], pair[..], "{name}");
        } else {
            let candidates = resolved.unwrap_or_else(|error| panic!("{name}: {error}"));
            let expected: Vec<MagnetCandidate> = golden::list(golden::get(case, "candidates"))
                .iter()
                .map(|entry| MagnetCandidate {
                    magnet: golden::string(golden::get(entry, "magnet")),
                    info_hash: golden::get(entry, "info_hash").as_str().map(str::to_owned),
                    source: golden::string(golden::get(entry, "source")),
                    size_hint: match golden::get(entry, "size_hint") {
                        Value::Int(size) => Some(*size),
                        _ => None,
                    },
                })
                .collect();
            assert_eq!(candidates, expected, "{name}");
        }
        assert_eq!(ctx.log_tail(), strings(golden::get(case, "log_tail")), "{name}");
        assert_eq!(ctx.errors(), strings(golden::get(case, "errors")), "{name}");
    }
}
