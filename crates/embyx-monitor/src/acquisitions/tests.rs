//! Ported from `tests/test_monitor_acquisitions.py`, against a temporary PostgreSQL database.

use chrono::{TimeDelta, TimeZone as _};
use embyx_db::testdb::TestDatabase;

use super::*;

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 13, 12, 0, 0).single().expect("valid")
}

fn date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).expect("valid")
}

struct Fixture {
    test: TestDatabase,
    database: Database,
    ledger: AcquisitionRepository,
}

impl Fixture {
    async fn new() -> Self {
        let test = TestDatabase::create().await;
        let database = test.database();
        let ledger = AcquisitionRepository::new(database.clone());
        Self { test, database, ledger }
    }

    async fn finish(self) {
        self.database.close().await;
        self.test.drop_database().await;
    }
}

fn candidate(name: &str) -> MagnetCandidate {
    MagnetCandidate {
        magnet: format!("magnet:?xt=urn:btih:{name}"),
        info_hash: Some(name.to_uppercase()),
        source: "sukebei".to_owned(),
        size_hint: None,
    }
}

fn actor() -> String {
    rss_source("Actor")
}

async fn discover(ledger: &AcquisitionRepository, avid: &str, source: &str) -> bool {
    ledger
        .discover(avid, source, now(), Sighting::default())
        .await
        .expect("discover")
}

async fn move_to(
    ledger: &AcquisitionRepository,
    avid: &str,
    expected: AcquisitionState,
    target: AcquisitionState,
) -> bool {
    ledger
        .transition(avid, expected, target, now(), TransitionFields::default())
        .await
        .expect("transition")
}

async fn move_attempt(
    ledger: &AcquisitionRepository,
    avid: &str,
    attempt_no: i32,
    expected: AttemptState,
    target: AttemptState,
) -> bool {
    ledger
        .transition_attempt(avid, attempt_no, expected, target, now(), None)
        .await
        .expect("transition_attempt")
}

/// `start_downloading`: discover an AVID, give it candidates, and submit the first one.
async fn start_downloading(ledger: &AcquisitionRepository, avid: &str, count: usize) {
    discover(ledger, avid, &actor()).await;
    let candidates: Vec<MagnetCandidate> = (0..count)
        .map(|index| candidate(&format!("{}hash{index}", avid.replace('-', ""))))
        .collect();
    ledger.add_attempts(avid, &candidates, now()).await.expect("add");
    ledger.claim_next_pending(avid, now()).await.expect("claim");
    move_to(
        ledger,
        avid,
        AcquisitionState::Discovered,
        AcquisitionState::Downloading,
    )
    .await;
}

async fn record(ledger: &AcquisitionRepository, avid: &str) -> AcquisitionRecord {
    ledger.get(avid).await.expect("get").expect("a row")
}

fn avids(records: &[AcquisitionRecord]) -> Vec<&str> {
    records.iter().map(|record| record.avid.as_str()).collect()
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

// py: tests/test_monitor_acquisitions.py::test_discover_inserts_then_reports_the_avid_as_taken
#[tokio::test]
async fn discover_inserts_then_reports_the_avid_as_taken() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;

    assert!(discover(ledger, "ABC-123", &actor()).await);
    // Still only discovered, so a second sighting should still ask for magnets.
    assert!(discover(ledger, "ABC-123", &rss_source("Rank")).await);

    let found = record(ledger, "ABC-123").await;
    assert_eq!(found.state, AcquisitionState::Discovered);
    assert_eq!(found.source, "rss:Actor"); // first sighting wins

    move_to(
        ledger,
        "ABC-123",
        AcquisitionState::Discovered,
        AcquisitionState::Downloading,
    )
    .await;
    assert!(!discover(ledger, "ABC-123", &actor()).await);
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_discover_accepts_the_fill_actor_source
#[tokio::test]
async fn discover_accepts_the_fill_actor_source() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;

    assert!(
        ledger
            .discover(
                "ABC-123",
                AcquisitionSource::FillActor.as_str(),
                now(),
                Sighting::in_dir("/115/fill")
            )
            .await
            .expect("discover")
    );

    let found = record(ledger, "ABC-123").await;
    assert_eq!(found.source, AcquisitionSource::FillActor.as_str());
    assert_eq!(found.task_dir_path.as_deref(), Some("/115/fill"));
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_discover_skips_terminal_avids cases=2
#[tokio::test]
async fn discover_skips_terminal_avids() {
    for state in [AcquisitionState::Archived, AcquisitionState::Ignored] {
        let fixture = Fixture::new().await;
        let ledger = &fixture.ledger;
        discover(ledger, "ABC-123", &actor()).await;
        move_to(ledger, "ABC-123", AcquisitionState::Discovered, state).await;

        assert!(!discover(ledger, "ABC-123", &actor()).await, "{state}");
        fixture.finish().await;
    }
}

// py: tests/test_monitor_acquisitions.py::test_discover_respects_the_resolve_cooldown
#[tokio::test]
async fn discover_respects_the_resolve_cooldown() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    discover(ledger, "ABC-123", &actor()).await;
    ledger
        .transition(
            "ABC-123",
            AcquisitionState::Discovered,
            AcquisitionState::ResolveFailed,
            now(),
            TransitionFields::next_action_at(now() + TimeDelta::hours(24)),
        )
        .await
        .expect("park");

    assert!(!discover(ledger, "ABC-123", &actor()).await);
    assert!(
        ledger
            .discover("ABC-123", &actor(), now() + TimeDelta::hours(25), Sighting::default())
            .await
            .expect("discover")
    );
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_transition_is_compare_and_set
#[tokio::test]
async fn transition_is_compare_and_set() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    discover(ledger, "ABC-123", &actor()).await;

    assert!(
        move_to(
            ledger,
            "ABC-123",
            AcquisitionState::Discovered,
            AcquisitionState::Downloading
        )
        .await
    );
    // The row already moved: the same CAS must not apply twice.
    assert!(
        !move_to(
            ledger,
            "ABC-123",
            AcquisitionState::Discovered,
            AcquisitionState::Downloading
        )
        .await
    );
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_transition_rejects_edges_outside_the_state_machine
#[tokio::test]
async fn transition_rejects_edges_outside_the_state_machine() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    discover(ledger, "ABC-123", &actor()).await;

    let error = ledger
        .transition(
            "ABC-123",
            AcquisitionState::Archived,
            AcquisitionState::Downloading,
            now(),
            TransitionFields::default(),
        )
        .await
        .expect_err("not an edge");
    assert!(matches!(error, LedgerError::IllegalTransition(_)), "{error:?}");
    assert_eq!(
        error.to_string(),
        "acquisition: archived -> downloading is not a legal transition"
    );
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_archiving_records_paths_with_the_state
#[tokio::test]
async fn archiving_records_paths_with_the_state() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    start_downloading(ledger, "ABC-123", 2).await;

    ledger
        .transition(
            "ABC-123",
            AcquisitionState::Downloading,
            AcquisitionState::Archived,
            now(),
            TransitionFields::archived_paths(strings(&["library/ABC/ABC-123.mp4"])),
        )
        .await
        .expect("archive");

    let found = record(ledger, "ABC-123").await;
    assert_eq!(found.state, AcquisitionState::Archived);
    assert_eq!(found.archived_paths, strings(&["library/ABC/ABC-123.mp4"]));
    // The stored bytes are `json.dumps(list(paths))`.
    let stored: String =
        sqlx::query_scalar("SELECT archived_paths_json FROM archive_acquisitions WHERE avid = 'ABC-123'")
            .fetch_one(fixture.test.pool())
            .await
            .expect("row");
    assert_eq!(stored, r#"["library/ABC/ABC-123.mp4"]"#);
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_add_attempts_numbers_sequentially_and_skips_known_hashes
#[tokio::test]
async fn add_attempts_numbers_sequentially_and_skips_known_hashes() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    discover(ledger, "ABC-123", &actor()).await;

    assert_eq!(
        ledger
            .add_attempts("ABC-123", &[candidate("aaa"), candidate("bbb")], now())
            .await
            .expect("add"),
        2
    );
    // 'aaa' was already tried; only the new hash is appended.
    assert_eq!(
        ledger
            .add_attempts("ABC-123", &[candidate("aaa"), candidate("ccc")], now())
            .await
            .expect("add"),
        1
    );

    let attempts = ledger.attempts_for("ABC-123").await.expect("attempts");
    assert_eq!(
        attempts.iter().map(|attempt| attempt.attempt_no).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(
        attempts
            .iter()
            .map(|attempt| attempt.info_hash.as_deref())
            .collect::<Vec<_>>(),
        [Some("AAA"), Some("BBB"), Some("CCC")]
    );
    assert!(attempts.iter().all(|attempt| attempt.state == AttemptState::Pending));
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_claim_next_pending_hands_each_attempt_out_once
#[tokio::test]
async fn claim_next_pending_hands_each_attempt_out_once() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    discover(ledger, "ABC-123", &actor()).await;
    ledger
        .add_attempts("ABC-123", &[candidate("aaa"), candidate("bbb")], now())
        .await
        .expect("add");

    let first = ledger
        .claim_next_pending("ABC-123", now())
        .await
        .expect("claim")
        .expect("one");
    assert_eq!((first.attempt_no, first.state), (1, AttemptState::Submitted));
    assert_eq!(first.submitted_at, Some(now()));

    let second = ledger
        .claim_next_pending("ABC-123", now())
        .await
        .expect("claim")
        .expect("one");
    assert_eq!(second.attempt_no, 2);

    assert_eq!(ledger.claim_next_pending("ABC-123", now()).await.expect("claim"), None);
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_concurrent_claims_never_hand_out_the_same_attempt
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claims_never_hand_out_the_same_attempt() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    discover(ledger, "ABC-123", &actor()).await;
    ledger
        .add_attempts("ABC-123", &[candidate("aaa"), candidate("bbb")], now())
        .await
        .expect("add");

    let claims: Vec<_> = (0..4)
        .map(|_| {
            let ledger = ledger.clone();
            tokio::spawn(async move { ledger.claim_next_pending("ABC-123", now()).await.expect("claim") })
        })
        .collect();
    let mut claimed = Vec::new();
    for claim in claims {
        if let Some(attempt) = claim.await.expect("task") {
            claimed.push(attempt.attempt_no);
        }
    }
    claimed.sort_unstable();

    assert_eq!(claimed, [1, 2]);
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_concurrent_archiving_claims_leave_one_winner
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_archiving_claims_leave_one_winner() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    start_downloading(ledger, "ABC-123", 2).await;
    move_attempt(ledger, "ABC-123", 1, AttemptState::Submitted, AttemptState::Finished).await;

    let claims: Vec<_> = (0..4)
        .map(|_| {
            let ledger = ledger.clone();
            tokio::spawn(async move {
                ledger
                    .transition_attempt(
                        "ABC-123",
                        1,
                        AttemptState::Finished,
                        AttemptState::Archiving,
                        now(),
                        None,
                    )
                    .await
                    .expect("cas")
            })
        })
        .collect();
    let mut winners = 0;
    for claim in claims {
        if claim.await.expect("task") {
            winners += 1;
        }
    }

    assert_eq!(winners, 1);
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_transition_attempt_rejects_illegal_edges
#[tokio::test]
async fn transition_attempt_rejects_illegal_edges() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    start_downloading(ledger, "ABC-123", 2).await;

    let error = ledger
        .transition_attempt("ABC-123", 1, AttemptState::Archived, AttemptState::Pending, now(), None)
        .await
        .expect_err("not an edge");
    assert!(matches!(error, LedgerError::IllegalTransition(_)), "{error:?}");
    assert_eq!(
        error.to_string(),
        "attempt: archived -> pending is not a legal transition"
    );
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_record_progress_only_writes_when_the_download_moved
#[tokio::test]
async fn record_progress_only_writes_when_the_download_moved() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    start_downloading(ledger, "ABC-123", 2).await;
    let later = now() + TimeDelta::minutes(5);
    let progress = |value: f64, at: DateTime<Utc>| {
        ledger.record_progress("ABC-123", 1, AttemptState::Downloading, Some(value), at)
    };

    assert!(progress(0.5, now()).await.expect("progress"));
    // Same progress on the next poll: updated_at must stay put so a stall stays visible.
    assert!(!progress(0.5, later).await.expect("progress"));

    let attempts = ledger.attempts_for("ABC-123").await.expect("attempts");
    assert!((attempts[0].progress.expect("progress") - 0.5).abs() < 1e-12);
    assert_eq!(attempts[0].updated_at, now());

    assert!(progress(0.75, later).await.expect("progress"));
    assert_eq!(
        ledger.attempts_for("ABC-123").await.expect("attempts")[0].updated_at,
        later
    );
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_record_progress_ignores_attempts_that_already_concluded
#[tokio::test]
async fn record_progress_ignores_attempts_that_already_concluded() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    start_downloading(ledger, "ABC-123", 2).await;
    move_attempt(ledger, "ABC-123", 1, AttemptState::Submitted, AttemptState::Finished).await;

    assert!(
        !ledger
            .record_progress("ABC-123", 1, AttemptState::Downloading, Some(0.9), now())
            .await
            .expect("progress")
    );
    let error = ledger
        .record_progress("ABC-123", 1, AttemptState::Archived, None, now())
        .await
        .expect_err("not in flight");
    assert!(matches!(error, LedgerError::Value(_)), "{error:?}");
    assert!(error.to_string().contains("in-flight"), "{error}");
    assert_eq!(
        error.to_string(),
        "record_progress only refreshes in-flight attempts, got archived"
    );
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_attempts_are_found_by_info_hash
#[tokio::test]
async fn attempts_are_found_by_info_hash() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    start_downloading(ledger, "ABC-123", 2).await;
    start_downloading(ledger, "DEF-456", 2).await;

    let found = ledger
        .attempts_by_info_hash(&strings(&["ABC123HASH0", "DEF456HASH0", "MISSING", ""]))
        .await
        .expect("lookup");

    assert_eq!(
        found.keys().map(String::as_str).collect::<Vec<_>>(),
        ["ABC123HASH0", "DEF456HASH0"]
    );
    assert_eq!(found["ABC123HASH0"].avid, "ABC-123");
    assert_eq!(found["DEF456HASH0"].avid, "DEF-456");
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_concluded_attempts_drop_out_of_the_hash_lookup
#[tokio::test]
async fn concluded_attempts_drop_out_of_the_hash_lookup() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    start_downloading(ledger, "ABC-123", 2).await;
    move_attempt(ledger, "ABC-123", 1, AttemptState::Submitted, AttemptState::Stalled).await;

    assert!(
        ledger
            .attempts_by_info_hash(&strings(&["ABC123HASH0"]))
            .await
            .expect("lookup")
            .is_empty()
    );
    // The hash stays on the row for history, and may be tried again later.
    assert_eq!(
        ledger.attempts_for("ABC-123").await.expect("attempts")[0]
            .info_hash
            .as_deref(),
        Some("ABC123HASH0")
    );
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_archived_hashes_are_those_no_live_attempt_holds
#[tokio::test]
async fn archived_hashes_are_those_no_live_attempt_holds() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    start_downloading(ledger, "ABC-123", 2).await;
    start_downloading(ledger, "DEF-456", 2).await;
    start_downloading(ledger, "GHI-789", 2).await;
    for (avid, hash) in [("ABC-123", "ABC123HASH0"), ("GHI-789", "GHI789HASH0")] {
        for (expected, target) in [
            (AttemptState::Submitted, AttemptState::Finished),
            (AttemptState::Finished, AttemptState::Archiving),
            (AttemptState::Archiving, AttemptState::Archived),
        ] {
            move_attempt(ledger, avid, 1, expected, target).await;
        }
        assert_eq!(
            ledger.attempts_for(avid).await.expect("attempts")[0]
                .info_hash
                .as_deref(),
            Some(hash)
        );
    }
    // Another code's attempt on the archived magnet is live: its task is not history yet.
    discover(ledger, "JKL-000", &actor()).await;
    ledger
        .add_attempts("JKL-000", &[candidate("GHI789hash0")], now())
        .await
        .expect("add");
    assert!(
        ledger
            .claim_next_pending("JKL-000", now())
            .await
            .expect("claim")
            .is_some()
    );

    let archived = ledger
        .archived_info_hashes(&strings(&["ABC123HASH0", "DEF456HASH0", "GHI789HASH0", "MISSING", ""]))
        .await
        .expect("lookup");

    assert_eq!(archived.into_iter().collect::<Vec<_>>(), ["ABC123HASH0"]);
    assert!(ledger.archived_info_hashes(&[]).await.expect("lookup").is_empty());
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_a_hash_cannot_be_live_for_two_attempts_at_once
#[tokio::test]
async fn a_hash_cannot_be_live_for_two_attempts_at_once() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    discover(ledger, "ABC-123", &actor()).await;
    discover(ledger, "DEF-456", &actor()).await;
    ledger
        .add_attempts("ABC-123", &[candidate("shared")], now())
        .await
        .expect("add");
    ledger
        .add_attempts("DEF-456", &[candidate("shared")], now())
        .await
        .expect("add");

    assert!(
        ledger
            .claim_next_pending("ABC-123", now())
            .await
            .expect("claim")
            .is_some()
    );
    // The claim passes the shared magnet over instead of failing, and the attempt stays
    // pending for later.
    assert_eq!(ledger.claim_next_pending("DEF-456", now()).await.expect("claim"), None);
    assert_eq!(
        ledger
            .attempts_for("DEF-456")
            .await
            .expect("attempts")
            .iter()
            .map(|attempt| attempt.state)
            .collect::<Vec<_>>(),
        [AttemptState::Pending]
    );
    let error = ledger.claim_attempt("DEF-456", 1, now()).await.expect_err("in use");
    assert!(matches!(error, LedgerError::MagnetInUse(_)), "{error:?}");
    assert_eq!(
        error.to_string(),
        "DEF-456 attempt 1: the magnet is already live on another attempt"
    );
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_a_claim_skips_a_shared_hash_and_takes_the_next_magnet
#[tokio::test]
async fn a_claim_skips_a_shared_hash_and_takes_the_next_magnet() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    discover(ledger, "ABC-123", &actor()).await;
    discover(ledger, "DEF-456", &actor()).await;
    ledger
        .add_attempts("ABC-123", &[candidate("shared")], now())
        .await
        .expect("add");
    ledger
        .add_attempts("DEF-456", &[candidate("shared"), candidate("own")], now())
        .await
        .expect("add");
    ledger.claim_next_pending("ABC-123", now()).await.expect("claim");

    let claimed = ledger
        .claim_next_pending("DEF-456", now())
        .await
        .expect("claim")
        .expect("one");

    assert_eq!(claimed.info_hash.as_deref(), Some("OWN"));
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_a_shared_hash_is_free_again_once_its_attempt_concludes
#[tokio::test]
async fn a_shared_hash_is_free_again_once_its_attempt_concludes() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    discover(ledger, "ABC-123", &actor()).await;
    discover(ledger, "DEF-456", &actor()).await;
    ledger
        .add_attempts("ABC-123", &[candidate("shared")], now())
        .await
        .expect("add");
    ledger
        .add_attempts("DEF-456", &[candidate("shared")], now())
        .await
        .expect("add");
    ledger.claim_next_pending("ABC-123", now()).await.expect("claim");
    move_attempt(ledger, "ABC-123", 1, AttemptState::Submitted, AttemptState::Error).await;

    let claimed = ledger
        .claim_next_pending("DEF-456", now())
        .await
        .expect("claim")
        .expect("one");

    assert_eq!(claimed.info_hash.as_deref(), Some("SHARED"));
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_in_flight_attempts_exclude_concluded_ones
#[tokio::test]
async fn in_flight_attempts_exclude_concluded_ones() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    start_downloading(ledger, "ABC-123", 2).await;
    start_downloading(ledger, "DEF-456", 2).await;
    ledger
        .transition_attempt(
            "ABC-123",
            1,
            AttemptState::Submitted,
            AttemptState::Error,
            now(),
            Some("offline task failed".to_owned()),
        )
        .await
        .expect("error");

    let in_flight = ledger.in_flight_attempts().await.expect("in flight");

    assert_eq!(
        in_flight
            .iter()
            .map(|attempt| attempt.avid.as_str())
            .collect::<Vec<_>>(),
        ["DEF-456"]
    );
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_due_for_retry_returns_expired_cooldowns_in_order
#[tokio::test]
async fn due_for_retry_returns_expired_cooldowns_in_order() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    for (avid, hours) in [("ABC-123", 1), ("DEF-456", 3), ("GHI-789", 5)] {
        discover(ledger, avid, &actor()).await;
        ledger
            .transition(
                avid,
                AcquisitionState::Discovered,
                AcquisitionState::ResolveFailed,
                now(),
                TransitionFields::next_action_at(now() + TimeDelta::hours(hours)),
            )
            .await
            .expect("park");
    }

    let due = ledger
        .due_for_retry(now() + TimeDelta::hours(4), 50)
        .await
        .expect("due");

    assert_eq!(avids(&due), ["ABC-123", "DEF-456"]);
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_due_for_retry_includes_queued_discoveries
#[tokio::test]
async fn due_for_retry_includes_queued_discoveries() {
    // A fill-actor scan queues rows as discovered-with-a-timer; the background pass must pick
    // those up, while inline sources' discovered rows (no timer) stay untouched.
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    ledger
        .discover(
            "ABC-123",
            AcquisitionSource::FillActor.as_str(),
            now(),
            Sighting {
                task_dir_path: Some("/115/fill".to_owned()),
                next_action_at: Some(now()),
                ..Sighting::default()
            },
        )
        .await
        .expect("queue");
    discover(ledger, "DEF-456", &actor()).await;

    let due = ledger
        .due_for_retry(now() + TimeDelta::minutes(1), 50)
        .await
        .expect("due");

    assert_eq!(avids(&due), ["ABC-123"]);
    assert_eq!(due[0].task_dir_path.as_deref(), Some("/115/fill"));
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_discover_rearms_the_timer_for_a_queueing_source
#[tokio::test]
async fn discover_rearms_the_timer_for_a_queueing_source() {
    // Re-queueing an orphaned discovered row (no timer) must give it one, or the background
    // pass would never look at it.
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    discover(ledger, "ABC-123", &actor()).await;

    let accepted = ledger
        .discover(
            "ABC-123",
            AcquisitionSource::FillActor.as_str(),
            now() + TimeDelta::hours(1),
            Sighting {
                next_action_at: Some(now() + TimeDelta::hours(1)),
                ..Sighting::default()
            },
        )
        .await
        .expect("discover");

    assert!(accepted);
    let due = ledger
        .due_for_retry(now() + TimeDelta::hours(2), 50)
        .await
        .expect("due");
    assert_eq!(avids(&due), ["ABC-123"]);
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_active_avids_covers_only_the_states_the_tracker_owns
#[tokio::test]
async fn active_avids_covers_only_the_states_the_tracker_owns() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    start_downloading(ledger, "ABC-123", 2).await;
    discover(ledger, "DEF-456", AcquisitionSource::Reconcile.as_str()).await;
    discover(ledger, "GHI-789", AcquisitionSource::Manual.as_str()).await;
    ledger
        .transition(
            "GHI-789",
            AcquisitionState::Discovered,
            AcquisitionState::NeedsAttention,
            now(),
            TransitionFields::note("multiple avids in folder"),
        )
        .await
        .expect("attention");

    assert_eq!(
        ledger
            .active_avids()
            .await
            .expect("active")
            .into_iter()
            .collect::<Vec<_>>(),
        ["ABC-123", "DEF-456"]
    );
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_active_task_dirs_covers_the_directories_still_in_flight
#[tokio::test]
async fn active_task_dirs_covers_the_directories_still_in_flight() {
    // What the tracker adds to its poll set, so a manual directory is watched too.
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    let manual = AcquisitionSource::Manual.as_str().to_owned();
    // A second AVID in one directory is one entry; a settled one is none at all.
    for (avid, source, dir) in [
        ("ABC-123", &manual, "/115/misc"),
        ("DEF-456", &actor(), "/115/embyx_in"),
        ("GHI-789", &manual, "/115/misc"),
        ("JKL-012", &manual, "/115/done"),
    ] {
        ledger
            .discover(avid, source, now(), Sighting::in_dir(dir))
            .await
            .expect("discover");
    }
    move_to(
        ledger,
        "JKL-012",
        AcquisitionState::Discovered,
        AcquisitionState::Archived,
    )
    .await;
    // Reconcile's rows carry no directory and must not become an empty entry.
    discover(ledger, "MNO-345", AcquisitionSource::Reconcile.as_str()).await;

    assert_eq!(
        ledger.active_task_dirs().await.expect("dirs"),
        ["/115/embyx_in", "/115/misc"]
    );
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_latest_task_dir_is_the_newest_row_of_that_source
#[tokio::test]
async fn latest_task_dir_is_the_newest_row_of_that_source() {
    // How the manual picker remembers where the last batch went.
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    let manual = AcquisitionSource::Manual.as_str();
    ledger
        .discover("ABC-123", manual, now(), Sighting::in_dir("/115/embyx_in"))
        .await
        .expect("discover");
    ledger
        .discover(
            "DEF-456",
            manual,
            now() + TimeDelta::hours(1),
            Sighting::in_dir("/115/embyx_in/rank"),
        )
        .await
        .expect("discover");
    // Another source's newer row is not this source's memory.
    ledger
        .discover(
            "GHI-789",
            &actor(),
            now() + TimeDelta::hours(2),
            Sighting::in_dir("/115/actor"),
        )
        .await
        .expect("discover");

    assert_eq!(
        ledger.latest_task_dir(manual).await.expect("latest").as_deref(),
        Some("/115/embyx_in/rank")
    );
    assert_eq!(
        ledger
            .latest_task_dir(AcquisitionSource::FillActor.as_str())
            .await
            .expect("latest"),
        None
    );
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_listing_filters_by_state_and_counts
#[tokio::test]
async fn listing_filters_by_state_and_counts() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    start_downloading(ledger, "ABC-123", 2).await;
    discover(ledger, "DEF-456", &actor()).await;
    ledger
        .transition(
            "DEF-456",
            AcquisitionState::Discovered,
            AcquisitionState::NeedsAttention,
            now(),
            TransitionFields::note("failed to parse avid"),
        )
        .await
        .expect("attention");

    let attention = ledger
        .list_acquisitions(Some(&[AcquisitionState::NeedsAttention]), 50, 0)
        .await
        .expect("list");
    assert_eq!(avids(&attention), ["DEF-456"]);
    assert_eq!(attention[0].note.as_deref(), Some("failed to parse avid"));

    let mut counts = ledger.count_by_state().await.expect("counts");
    counts.sort();
    assert_eq!(
        counts,
        [
            (AcquisitionState::Downloading, 1),
            (AcquisitionState::NeedsAttention, 1)
        ]
    );
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_the_offline_directory_is_pinned_at_discovery
#[tokio::test]
async fn the_offline_directory_is_pinned_at_discovery() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;

    ledger
        .discover(
            "ABC-123",
            &rss_source("Rank"),
            now(),
            Sighting::in_dir("/115/embyx_in/rank"),
        )
        .await
        .expect("discover");

    let found = record(ledger, "ABC-123").await;
    assert_eq!(found.task_dir_path.as_deref(), Some("/115/embyx_in/rank"));
    assert_eq!(found.source, "rss:Rank");
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_an_acquisition_with_no_directory_of_its_own_follows_the_default
#[tokio::test]
async fn an_acquisition_with_no_directory_of_its_own_follows_the_default() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;

    discover(ledger, "ABC-123", &actor()).await;

    assert_eq!(record(ledger, "ABC-123").await.task_dir_path, None);
    fixture.finish().await;
}

async fn park_now(ledger: &AcquisitionRepository, avid: &str, at: DateTime<Utc>) {
    ledger
        .transition(
            avid,
            AcquisitionState::Discovered,
            AcquisitionState::ResolveFailed,
            now(),
            TransitionFields::next_action_at(at),
        )
        .await
        .expect("park");
}

// py: tests/test_monitor_acquisitions.py::test_rediscovery_repoints_a_retryable_row_at_its_category_directory
#[tokio::test]
async fn rediscovery_repoints_a_retryable_row_at_its_category_directory() {
    // A category that moved takes its cooling-down AVIDs with it on the next pass.
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    ledger
        .discover("ABC-123", &rss_source("Rank"), now(), Sighting::in_dir("/115/old"))
        .await
        .expect("discover");
    park_now(ledger, "ABC-123", now()).await;

    assert!(
        ledger
            .discover("ABC-123", &rss_source("Rank"), now(), Sighting::in_dir("/115/new"))
            .await
            .expect("discover")
    );

    assert_eq!(
        record(ledger, "ABC-123").await.task_dir_path.as_deref(),
        Some("/115/new")
    );
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_a_sighting_that_yields_leaves_the_row_with_its_owner
#[tokio::test]
async fn a_sighting_that_yields_leaves_the_row_with_its_owner() {
    // The chart's sighting still wakes the row, but the actor keeps the directory and the source.
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    ledger
        .discover("ABC-123", &actor(), now(), Sighting::in_dir("/115/clt"))
        .await
        .expect("discover");
    park_now(ledger, "ABC-123", now() + TimeDelta::days(1)).await;

    let accepted = ledger
        .discover(
            "ABC-123",
            &rss_source("Rank"),
            now(),
            Sighting {
                task_dir_path: Some("/115/rank".to_owned()),
                wake: true,
                yields_to: vec![actor()],
                ..Sighting::default()
            },
        )
        .await
        .expect("discover");

    assert!(accepted);
    let found = record(ledger, "ABC-123").await;
    assert_eq!(found.task_dir_path.as_deref(), Some("/115/clt"));
    assert_eq!(found.source, "rss:Actor");
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_taking_a_row_into_another_directory_hands_it_to_that_source
#[tokio::test]
async fn taking_a_row_into_another_directory_hands_it_to_that_source() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    ledger
        .discover("ABC-123", &rss_source("Rank"), now(), Sighting::in_dir("/115/rank"))
        .await
        .expect("discover");
    park_now(ledger, "ABC-123", now()).await;

    assert!(
        ledger
            .discover("ABC-123", &actor(), now(), Sighting::in_dir("/115/clt"))
            .await
            .expect("discover")
    );

    let found = record(ledger, "ABC-123").await;
    assert_eq!(found.task_dir_path.as_deref(), Some("/115/clt"));
    assert_eq!(found.source, "rss:Actor");
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_an_avid_someone_else_owns_keeps_its_directory
#[tokio::test]
async fn an_avid_someone_else_owns_keeps_its_directory() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    start_downloading(ledger, "ABC-123", 2).await;

    assert!(
        !ledger
            .discover("ABC-123", &rss_source("Rank"), now(), Sighting::in_dir("/115/rank"))
            .await
            .expect("discover")
    );

    assert_eq!(record(ledger, "ABC-123").await.task_dir_path, None);
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_discover_keeps_the_first_release_date_it_learns
#[tokio::test]
async fn discover_keeps_the_first_release_date_it_learns() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    discover(ledger, "ABC-123", &actor()).await;
    assert_eq!(record(ledger, "ABC-123").await.release_date, None);

    for release_date in [date(2026, 10, 2), date(2026, 11, 2)] {
        ledger
            .discover(
                "ABC-123",
                AcquisitionSource::FillActor.as_str(),
                now(),
                Sighting {
                    release_date: Some(release_date),
                    ..Sighting::default()
                },
            )
            .await
            .expect("discover");
    }

    assert_eq!(record(ledger, "ABC-123").await.release_date, Some(date(2026, 10, 2)));
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_a_waking_sighting_accepts_a_cooling_row
#[tokio::test]
async fn a_waking_sighting_accepts_a_cooling_row() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    ledger
        .discover(
            "ABC-123",
            &actor(),
            now(),
            Sighting {
                release_date: Some(date(2026, 8, 1)),
                ..Sighting::default()
            },
        )
        .await
        .expect("discover");
    park_now(ledger, "ABC-123", now() + TimeDelta::hours(24)).await;

    assert!(!discover(ledger, "ABC-123", &actor()).await);
    assert!(
        ledger
            .discover(
                "ABC-123",
                &actor(),
                now(),
                Sighting {
                    wake: true,
                    ..Sighting::default()
                }
            )
            .await
            .expect("discover")
    );
    assert_eq!(record(ledger, "ABC-123").await.release_date, Some(date(2026, 8, 1)));
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_migration_13_renames_ledger_keys_to_the_catalog_spelling
#[tokio::test]
async fn migration_13_renames_ledger_keys_to_the_catalog_spelling() {
    // Keys written before the padding rule follow it; a taken key is left alone.
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    start_downloading(ledger, "HTTM-0066", 2).await;
    discover(ledger, "TBW-19", &actor()).await;
    discover(ledger, "TBW-019", &actor()).await;
    discover(ledger, "FC2-1234567", &actor()).await;

    let pool = fixture.database.get_pool().await.expect("pool");
    sqlx::raw_sql(sqlx::AssertSqlSafe(embyx_db::migrations::statements(13)[2]))
        .execute(&pool)
        .await
        .expect("migration 13");

    assert_eq!(ledger.get("HTTM-0066").await.expect("get"), None);
    let renamed = record(ledger, "HTTM-066").await;
    assert_eq!(renamed.state, AcquisitionState::Downloading);
    // The attempts followed the rename through the foreign key.
    assert_eq!(
        ledger
            .attempts_for("HTTM-066")
            .await
            .expect("attempts")
            .iter()
            .map(|attempt| attempt.avid.as_str())
            .collect::<Vec<_>>(),
        ["HTTM-066", "HTTM-066"]
    );
    for kept in ["TBW-19", "TBW-019", "FC2-1234567"] {
        assert!(ledger.get(kept).await.expect("get").is_some(), "{kept}");
    }
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_a_release_date_can_be_filled_in_once_but_never_overwritten
#[tokio::test]
async fn a_release_date_can_be_filled_in_once_but_never_overwritten() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    discover(ledger, "ABC-123", &actor()).await;

    assert!(
        ledger
            .set_release_date("ABC-123", date(2026, 10, 2))
            .await
            .expect("set")
    );
    assert!(!ledger.set_release_date("ABC-123", date(2027, 1, 1)).await.expect("set"));
    assert!(!ledger.set_release_date("NOPE-1", date(2026, 10, 2)).await.expect("set"));
    assert_eq!(record(ledger, "ABC-123").await.release_date, Some(date(2026, 10, 2)));
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_task_dirs_for_returns_only_rows_that_recorded_a_directory
#[tokio::test]
async fn task_dirs_for_returns_only_rows_that_recorded_a_directory() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    ledger
        .discover("ABC-123", &actor(), now(), Sighting::in_dir("/115/embyx_in/clt"))
        .await
        .expect("discover");
    discover(ledger, "DEF-456", &rss_source("Rank")).await;

    let dirs = ledger
        .task_dirs_for(&strings(&["ABC-123", "DEF-456", "GHI-789"]))
        .await
        .expect("dirs");
    assert_eq!(
        dirs.into_iter().collect::<Vec<_>>(),
        [("ABC-123".to_owned(), "/115/embyx_in/clt".to_owned())]
    );
    assert!(ledger.task_dirs_for(&[]).await.expect("dirs").is_empty());
    fixture.finish().await;
}

// -- replacements (换版) --------------------------------------------------------

async fn archive_first_attempt(ledger: &AcquisitionRepository, avid: &str) {
    start_downloading(ledger, avid, 2).await;
    for (expected, target) in [
        (AttemptState::Submitted, AttemptState::Finished),
        (AttemptState::Finished, AttemptState::Archiving),
        (AttemptState::Archiving, AttemptState::Archived),
    ] {
        assert!(move_attempt(ledger, avid, 1, expected, target).await);
    }
    ledger
        .transition(
            avid,
            AcquisitionState::Downloading,
            AcquisitionState::Archived,
            now(),
            TransitionFields::archived_paths(strings(&["sorted/ABC/ABC-123.mp4"])),
        )
        .await
        .expect("archive");
}

// py: tests/test_monitor_acquisitions.py::test_begin_replacement_reopens_an_archived_title_and_keeps_its_history
#[tokio::test]
async fn begin_replacement_reopens_an_archived_title_and_keeps_its_history() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    archive_first_attempt(ledger, "ABC-123").await;

    let reopened = ledger
        .begin_replacement(
            "ABC-123",
            AcquisitionSource::Manual.as_str(),
            "/115/rank",
            &strings(&["sorted/ABC/ABC-123.mp4"]),
            now() + TimeDelta::days(1),
        )
        .await
        .expect("replace")
        .expect("replaceable");

    assert_eq!(reopened.state, AcquisitionState::Discovered);
    assert_eq!(reopened.replace_paths, strings(&["sorted/ABC/ABC-123.mp4"]));
    assert_eq!(reopened.replace_after_attempt, Some(2)); // both earlier candidates are history
    assert_eq!(reopened.task_dir_path.as_deref(), Some("/115/rank"));
    assert_eq!(reopened.source, "rss:Actor"); // ownership stays
    // The hash that produced the old copy is never tried again.
    assert_eq!(
        ledger
            .add_attempts("ABC-123", &[candidate("ABC123hash0")], now())
            .await
            .expect("add"),
        0
    );

    // Filing the new copy concludes the replacement.
    ledger
        .add_attempts("ABC-123", &[candidate("fresh")], now())
        .await
        .expect("add");
    ledger.claim_next_pending("ABC-123", now()).await.expect("claim");
    move_to(
        ledger,
        "ABC-123",
        AcquisitionState::Discovered,
        AcquisitionState::Downloading,
    )
    .await;
    ledger
        .transition(
            "ABC-123",
            AcquisitionState::Downloading,
            AcquisitionState::Archived,
            now(),
            TransitionFields::archived_paths(strings(&["sorted/ABC/ABC-123.mp4"])),
        )
        .await
        .expect("archive");
    let done = record(ledger, "ABC-123").await;
    assert!(done.replace_paths.is_empty());
    assert_eq!(done.replace_after_attempt, None);
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_begin_replacement_creates_a_row_for_a_title_the_ledger_never_saw
#[tokio::test]
async fn begin_replacement_creates_a_row_for_a_title_the_ledger_never_saw() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;

    let created = ledger
        .begin_replacement(
            "OLD-001",
            AcquisitionSource::Manual.as_str(),
            "/115/in",
            &strings(&["sorted/OLD/OLD-001-cd1.mp4", "sorted/OLD/OLD-001-cd2.mp4"]),
            now(),
        )
        .await
        .expect("replace")
        .expect("created");

    assert_eq!(created.state, AcquisitionState::Discovered);
    assert_eq!(created.source, "manual");
    assert_eq!(
        created.replace_paths,
        strings(&["sorted/OLD/OLD-001-cd1.mp4", "sorted/OLD/OLD-001-cd2.mp4"])
    );
    assert_eq!(created.replace_after_attempt, Some(0));
    // The stored bytes are `json.dumps(list(replace_paths))`.
    let stored: String =
        sqlx::query_scalar("SELECT replace_paths_json FROM archive_acquisitions WHERE avid = 'OLD-001'")
            .fetch_one(fixture.test.pool())
            .await
            .expect("row");
    assert_eq!(
        stored,
        r#"["sorted/OLD/OLD-001-cd1.mp4", "sorted/OLD/OLD-001-cd2.mp4"]"#
    );
    fixture.finish().await;
}

// py: tests/test_monitor_acquisitions.py::test_begin_replacement_refuses_a_title_the_tracker_is_working_on
#[tokio::test]
async fn begin_replacement_refuses_a_title_the_tracker_is_working_on() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    start_downloading(ledger, "ABC-123", 2).await;

    assert_eq!(
        ledger
            .begin_replacement(
                "ABC-123",
                AcquisitionSource::Manual.as_str(),
                "/115/in",
                &strings(&["x"]),
                now()
            )
            .await
            .expect("replace"),
        None
    );
    let found = record(ledger, "ABC-123").await;
    assert_eq!(found.state, AcquisitionState::Downloading);
    assert!(found.replace_paths.is_empty());
    fixture.finish().await;
}

// -- beyond the Python tests ------------------------------------------------------

/// Every pair of states against the edge tables: the CAS refuses exactly the non-edges, with
/// Python's text (`goldens/monitor/state_machine.json` holds the tables themselves).
#[test]
fn illegal_transition_texts_name_both_states() {
    let error = IllegalTransitionError::acquisition(AcquisitionState::Ignored, AcquisitionState::Archived);
    assert_eq!(
        error.to_string(),
        "acquisition: ignored -> archived is not a legal transition"
    );
    let error = IllegalTransitionError::attempt(AttemptState::Lost, AttemptState::Pending);
    assert_eq!(error.to_string(), "attempt: lost -> pending is not a legal transition");
}

/// A JavBus size beyond `BIGINT` is refused with asyncpg's text, and nothing is written.
#[tokio::test]
async fn a_size_beyond_bigint_is_refused_and_nothing_is_written() {
    let fixture = Fixture::new().await;
    let ledger = &fixture.ledger;
    discover(ledger, "ABC-123", &actor()).await;
    let mut huge = candidate("bbb");
    huge.size_hint = Some(i128::from(i64::MAX) + 1);

    let error = ledger
        .add_attempts("ABC-123", &[candidate("aaa"), huge], now())
        .await
        .expect_err("out of range");
    assert_eq!(
        error.to_string(),
        "invalid input for query argument $6: 9223372036854775808 (value out of int64 range)"
    );
    assert_eq!(error.class_name(), "DataError");
    assert!(ledger.attempts_for("ABC-123").await.expect("attempts").is_empty());
    fixture.finish().await;
}

fn sorted_names<'a>(names: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut names: Vec<String> = names.map(str::to_owned).collect();
    names.sort();
    names
}

fn golden_names(value: &embyx_core::pyjson::Value) -> Vec<String> {
    embyx_core::golden::list(value)
        .iter()
        .map(embyx_core::golden::string)
        .collect()
}

/// The edge tables, the refusal text of every non-edge, and the state sets, against Python's
/// (cross-version contract 3: either build resumes rows the other wrote).
#[test]
fn the_state_machine_matches_python() {
    use embyx_core::golden;
    let document = golden::load("monitor/state_machine.json");
    let [case] = golden::cases(&document) else {
        panic!("one case");
    };
    let acquisition = golden::list(golden::get(case, "acquisition"));
    assert_eq!(acquisition.len(), AcquisitionState::ALL.len());
    for row in acquisition {
        let from = AcquisitionState::parse(&golden::string(golden::get(row, "from"))).expect("a state");
        assert_eq!(
            sorted_names(acquisition_edges(from).iter().map(|state| state.as_str())),
            golden_names(golden::get(row, "to")),
            "{from}"
        );
        let refused = golden::get(row, "refused");
        for to in AcquisitionState::ALL {
            match refused.get(to.as_str()) {
                Some(text) => {
                    assert!(!acquisition_edges(from).contains(to), "{from} -> {to}");
                    assert_eq!(
                        IllegalTransitionError::acquisition(from, *to).to_string(),
                        golden::string(text)
                    );
                }
                None => assert!(acquisition_edges(from).contains(to), "{from} -> {to}"),
            }
        }
    }
    let attempt = golden::list(golden::get(case, "attempt"));
    assert_eq!(attempt.len(), AttemptState::ALL.len());
    for row in attempt {
        let from = AttemptState::parse(&golden::string(golden::get(row, "from"))).expect("a state");
        assert_eq!(
            sorted_names(attempt_edges(from).iter().map(|state| state.as_str())),
            golden_names(golden::get(row, "to")),
            "{from}"
        );
        let refused = golden::get(row, "refused");
        for to in AttemptState::ALL {
            match refused.get(to.as_str()) {
                Some(text) => {
                    assert!(!attempt_edges(from).contains(to), "{from} -> {to}");
                    assert_eq!(
                        IllegalTransitionError::attempt(from, *to).to_string(),
                        golden::string(text)
                    );
                }
                None => assert!(attempt_edges(from).contains(to), "{from} -> {to}"),
            }
        }
    }
    let sets = golden::get(case, "sets");
    let acquisition_sets: [(&str, &[AcquisitionState]); 4] = [
        ("ACTIVE_STATES", ACTIVE_STATES),
        ("REPLACEABLE_STATES", REPLACEABLE_STATES),
        ("RETRYABLE_STATES", RETRYABLE_STATES),
        ("DUE_STATES", DUE_STATES),
    ];
    for (name, states) in acquisition_sets {
        assert_eq!(
            sorted_names(states.iter().map(|state| state.as_str())),
            golden_names(golden::get(sets, name)),
            "{name}"
        );
    }
    let attempt_sets: [(&str, &[AttemptState]); 2] = [
        ("IN_FLIGHT_ATTEMPT_STATES", IN_FLIGHT_ATTEMPT_STATES),
        ("LIVE_ATTEMPT_STATES", LIVE_ATTEMPT_STATES),
    ];
    for (name, states) in attempt_sets {
        assert_eq!(
            sorted_names(states.iter().map(|state| state.as_str())),
            golden_names(golden::get(sets, name)),
            "{name}"
        );
    }
}
