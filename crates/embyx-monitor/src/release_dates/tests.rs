//! Ported from `tests/test_monitor_release_dates.py`, with a scripted catalog search, a clock
//! the test moves, and the fake ledger.

use std::collections::{HashMap, HashSet};

use chrono::TimeZone as _;

use super::*;
use crate::acquisitions::Sighting;
use crate::testing::FakeLedger;

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 3, 0, 0, 0).single().expect("valid")
}

fn day(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).expect("valid")
}

fn work(work_id: &str, release_date: Option<NaiveDate>) -> CatalogWork {
    CatalogWork {
        work_id: work_id.to_owned(),
        release_date,
    }
}

/// The search `make_finder` builds: answers by query, raises for the queries in `fail`, and
/// records every query.
#[derive(Default)]
struct ScriptedSearch {
    answers: HashMap<String, Vec<CatalogWork>>,
    fail: HashSet<String>,
    calls: Mutex<Vec<String>>,
}

impl ScriptedSearch {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("calls").clone()
    }
}

impl CatalogSearch for ScriptedSearch {
    fn search<'a>(&'a self, query: &'a str) -> BoxFuture<'a, Result<Vec<CatalogWork>, SearchError>> {
        Box::pin(async move {
            self.calls.lock().expect("calls").push(query.to_owned());
            if self.fail.contains(query) {
                return Err("challenge page".into());
            }
            Ok(self.answers.get(query).cloned().unwrap_or_default())
        })
    }
}

type TestClock = Arc<Mutex<DateTime<Utc>>>;

fn make_finder(search: ScriptedSearch) -> (ReleaseDateFinder, Arc<ScriptedSearch>, TestClock) {
    let search = Arc::new(search);
    let clock: TestClock = Arc::new(Mutex::new(now()));
    let reading = Arc::clone(&clock);
    let finder = ReleaseDateFinder::with_options(
        Arc::clone(&search) as Arc<dyn CatalogSearch>,
        TimeDelta::days(7),
        Arc::new(move || *reading.lock().expect("clock")),
    );
    (finder, search, clock)
}

// py: tests/test_monitor_release_dates.py::test_the_date_comes_from_the_work_matching_the_avid
#[tokio::test]
async fn the_date_comes_from_the_work_matching_the_avid() {
    let (finder, search, _) = make_finder(ScriptedSearch {
        answers: HashMap::from([(
            "ABC-123".to_owned(),
            vec![
                work("ABC-1234", Some(day(2026, 1, 1))),
                work("abc-123", Some(day(2026, 10, 2))),
            ],
        )]),
        ..ScriptedSearch::default()
    });

    assert_eq!(finder.find("ABC-123").await, Some(day(2026, 10, 2)));
    assert_eq!(search.calls(), ["ABC-123"]);
}

// py: tests/test_monitor_release_dates.py::test_a_miss_is_remembered_for_a_week_but_a_failure_is_not
#[tokio::test]
async fn a_miss_is_remembered_for_a_week_but_a_failure_is_not() {
    let (finder, search, clock) = make_finder(ScriptedSearch {
        fail: HashSet::from(["DOWN-1".to_owned()]),
        ..ScriptedSearch::default()
    });

    assert_eq!(finder.find("ABC-123").await, None);
    assert_eq!(finder.find("ABC-123").await, None);
    assert_eq!(search.calls(), ["ABC-123"]);
    *clock.lock().expect("clock") = now() + TimeDelta::days(8);
    assert_eq!(finder.find("ABC-123").await, None);
    assert_eq!(search.calls(), ["ABC-123", "ABC-123"]);

    assert_eq!(finder.find("DOWN-1").await, None);
    assert_eq!(finder.find("DOWN-1").await, None);
    assert_eq!(search.calls()[2..], ["DOWN-1", "DOWN-1"]);
}

// py: tests/test_monitor_release_dates.py::test_a_work_without_a_date_yet_counts_as_a_miss
#[tokio::test]
async fn a_work_without_a_date_yet_counts_as_a_miss() {
    let (finder, search, _) = make_finder(ScriptedSearch {
        answers: HashMap::from([("ABC-123".to_owned(), vec![work("ABC-123", None)])]),
        ..ScriptedSearch::default()
    });

    assert_eq!(finder.find("ABC-123").await, None);
    assert_eq!(finder.find("ABC-123").await, None);
    assert_eq!(search.calls(), ["ABC-123"]);
}

/// The lookup `ensure_release_date` is handed: always 2026-10-02, recording who asked.
#[derive(Default)]
struct RecordingLookup {
    asked: Mutex<Vec<String>>,
}

impl ReleaseDateLookup for RecordingLookup {
    fn lookup<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Option<NaiveDate>> {
        Box::pin(async move {
            self.asked.lock().expect("asked").push(avid.to_owned());
            Some(day(2026, 10, 2))
        })
    }
}

// py: tests/test_monitor_release_dates.py::test_ensure_writes_the_found_date_back_and_skips_rows_that_have_one
#[tokio::test]
async fn ensure_writes_the_found_date_back_and_skips_rows_that_have_one() {
    let ledger = FakeLedger::new();
    ledger
        .discover("ABC-123", "rss:Actor", now(), Sighting::default())
        .await
        .expect("discover");
    ledger
        .discover(
            "DEF-456",
            "rss:Actor",
            now(),
            Sighting {
                release_date: Some(day(2026, 5, 5)),
                ..Sighting::default()
            },
        )
        .await
        .expect("discover");
    let lookup = RecordingLookup::default();

    let first = ledger.get("ABC-123").await.expect("get").expect("a row");
    let second = ledger.get("DEF-456").await.expect("get").expect("a row");

    assert_eq!(
        ensure_release_date(&ledger, &first, Some(&lookup))
            .await
            .expect("ensure"),
        Some(day(2026, 10, 2))
    );
    assert_eq!(
        ensure_release_date(&ledger, &second, Some(&lookup))
            .await
            .expect("ensure"),
        Some(day(2026, 5, 5))
    );
    assert_eq!(ensure_release_date(&ledger, &first, None).await.expect("ensure"), None);
    assert_eq!(*lookup.asked.lock().expect("asked"), ["ABC-123"]);
    let release_dates: Vec<(String, Option<NaiveDate>)> = ledger
        .state()
        .release_dates
        .iter()
        .map(|(avid, date)| (avid.clone(), *date))
        .collect();
    assert_eq!(
        release_dates,
        [
            ("ABC-123".to_owned(), Some(day(2026, 10, 2))),
            ("DEF-456".to_owned(), Some(day(2026, 5, 5)))
        ]
    );
}
