//! Ported from `tests/test_monitor_cadence.py`; `goldens/monitor/cadence.json` adds Python's
//! answers over a grid of release dates and moments.

use chrono::{DateTime, NaiveDate, TimeDelta, TimeZone as _, Utc};
use embyx_core::golden;
use embyx_core::pyjson::Value;

use super::*;

fn release() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 10, 2).expect("valid")
}

fn release_at() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 2, 0, 0, 0).single().expect("valid")
}

const FALLBACK: TimeDelta = TimeDelta::hours(24);

fn at(days: i64, hours: i64) -> DateTime<Utc> {
    release_at() + TimeDelta::days(days) + TimeDelta::hours(hours)
}

fn next(now: DateTime<Utc>, release_date: Option<NaiveDate>) -> DateTime<Utc> {
    next_resolve_at(now, release_date, FALLBACK).expect("in range")
}

// py: tests/test_monitor_cadence.py::test_no_release_date_means_the_fixed_cooldown
#[test]
fn no_release_date_means_the_fixed_cooldown() {
    let now = Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).single().expect("valid");

    assert_eq!(next(now, None), now + FALLBACK);
}

// py: tests/test_monitor_cadence.py::test_weeks_ahead_of_the_release_probes_weekly
#[test]
fn weeks_ahead_of_the_release_probes_weekly() {
    let now = at(-40, 0);

    assert_eq!(next(now, Some(release())), now + PREHEAT_INTERVAL);
}

// py: tests/test_monitor_cadence.py::test_the_last_preheat_probe_lands_on_the_first_day_of_the_window
#[test]
fn the_last_preheat_probe_lands_on_the_first_day_of_the_window() {
    assert_eq!(next(at(-10, 0), Some(release())), at(-7, 0));
}

// py: tests/test_monitor_cadence.py::test_the_release_window_probes_every_few_hours
#[test]
fn the_release_window_probes_every_few_hours() {
    for now in [at(-7, 0), at(0, 3), at(13, 23)] {
        assert_eq!(next(now, Some(release())), now + WINDOW_INTERVAL);
    }
}

// py: tests/test_monitor_cadence.py::test_the_long_tail_backs_off_with_the_age_of_the_release
#[test]
fn the_long_tail_backs_off_with_the_age_of_the_release() {
    assert_eq!(next(at(14, 0), Some(release())), at(15, 0));
    assert_eq!(next(at(30, 0), Some(release())), at(33, 0));
    assert_eq!(next(at(60, 0), Some(release())), at(67, 0));
}

// py: tests/test_monitor_cadence.py::test_an_old_release_is_still_probed_monthly
#[test]
fn an_old_release_is_still_probed_monthly() {
    let now = at(400, 0);

    assert_eq!(next(now, Some(release())), now + LONG_TAIL_FLOOR);
}

fn parse(value: &Value) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value.as_str().expect("a timestamp"))
        .expect("RFC 3339")
        .with_timezone(&Utc)
}

/// Python's answers over a grid of moments around several release dates, with and without a
/// date, and the `OverflowError` at the edges of `datetime`'s range.
#[test]
fn the_schedule_matches_python_over_a_grid() {
    let document = golden::load("monitor/cadence.json");
    let cases = golden::cases(&document);
    assert!(cases.len() > 100);
    for case in cases {
        let now = parse(case.get("now").expect("now"));
        let release_date = case
            .get("release_date")
            .and_then(Value::as_str)
            .map(|text| NaiveDate::parse_from_str(text, "%Y-%m-%d").expect("a date"));
        let Some(Value::Int(seconds)) = case.get("fallback_seconds") else {
            panic!("fallback_seconds");
        };
        let fallback = TimeDelta::seconds(i64::try_from(*seconds).expect("small"));
        let answer = next_resolve_at(now, release_date, fallback);
        match case.get("next") {
            Some(Value::Str(_)) => assert_eq!(answer, Ok(parse(case.get("next").expect("next"))), "{case:?}"),
            _ => assert_eq!(
                answer.map_err(|error| error.0),
                Err(case.get("error").and_then(Value::as_str).expect("error").to_owned()),
                "{case:?}"
            ),
        }
    }
}
