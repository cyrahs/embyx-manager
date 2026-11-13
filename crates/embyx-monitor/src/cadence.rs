//! When to look for magnets again: a schedule anchored on the release date.
//!
//! Python origin: `src/embyx_manager/monitor/cadence.py`. A work is registered at its catalog
//! weeks before it is released, and a magnet usually appears within days of the release, so
//! the frequent passes go around the release date: a weekly probe ahead of the window, every
//! four hours inside it, then an interval that grows with the age of the release.

use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta, Utc};

use crate::support::{OverflowError, py_add, py_sub};

/// `PREHEAT_INTERVAL`: ahead of the release window, one probe a week catches early leaks.
pub const PREHEAT_INTERVAL: TimeDelta = TimeDelta::days(7);
/// `WINDOW_BEFORE`.
pub const WINDOW_BEFORE: TimeDelta = TimeDelta::days(7);
/// `WINDOW_AFTER`.
pub const WINDOW_AFTER: TimeDelta = TimeDelta::days(14);
/// `WINDOW_INTERVAL`: inside the release window, where most magnets appear.
pub const WINDOW_INTERVAL: TimeDelta = TimeDelta::hours(4);
/// `LONG_TAIL`: after the window, (the age the band ends at, the interval within it).
pub const LONG_TAIL: [(TimeDelta, TimeDelta); 3] = [
    (TimeDelta::days(21), TimeDelta::days(1)),
    (TimeDelta::days(42), TimeDelta::days(3)),
    (TimeDelta::days(90), TimeDelta::days(7)),
];
/// `LONG_TAIL_FLOOR`: an old release with no magnet is still worth a monthly look.
pub const LONG_TAIL_FLOOR: TimeDelta = TimeDelta::days(30);

/// `next_resolve_at(now, release_date, fallback=)`: when to try resolving magnets again after
/// a pass came up empty.
///
/// Without a release date the fixed `fallback` cooldown applies. The date is a calendar day in
/// the catalog's time zone, taken as UTC midnight. `OverflowError` where Python's `datetime`
/// arithmetic leaves years 1 to 9999.
pub fn next_resolve_at(
    now: DateTime<Utc>,
    release_date: Option<NaiveDate>,
    fallback: TimeDelta,
) -> Result<DateTime<Utc>, OverflowError> {
    let Some(release_date) = release_date else {
        return py_add(now, fallback);
    };
    let release = release_date.and_time(NaiveTime::MIN).and_utc();
    let window_start = py_sub(release, WINDOW_BEFORE)?;
    if now < window_start {
        // Never overshoot the window: the last preheat probe lands on its first day.
        return Ok(py_add(now, PREHEAT_INTERVAL)?.min(window_start));
    }
    if now < py_add(release, WINDOW_AFTER)? {
        return py_add(now, WINDOW_INTERVAL);
    }
    let age = now - release;
    for (band_end, interval) in LONG_TAIL {
        if age < band_end {
            return py_add(now, interval);
        }
    }
    py_add(now, LONG_TAIL_FLOOR)
}

#[cfg(test)]
mod tests;
