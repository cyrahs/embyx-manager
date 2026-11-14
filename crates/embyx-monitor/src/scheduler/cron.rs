//! `_next_cron_fire`: the next occurrence of the archive scan's cron expression.
//!
//! Python origin: `monitor/scheduler.py::_next_cron_fire`, which runs `CronSim(expr,
//! now.astimezone())`. Validation is cronsim's own ([`embyx_core::cron::CronSpec`], ported
//! message for message in WP1); the fire is computed by `croner`, which gives cronsim's
//! chain for every expression cronsim accepts (`goldens/cron/next_fires.json`, the WP0
//! probe in `probes.rs`) once it is evaluated the way Python evaluates it: in the **fixed
//! UTC offset** the local zone has at `now` (`astimezone()` without a zone returns a
//! fixed-offset datetime). A fire across a DST change therefore keeps the old offset, and
//! the next call, after that fire, uses the new one. Evaluating in the zone itself would
//! differ around DST changes.

use chrono::{DateTime, FixedOffset, Local, SubsecRound as _, TimeZone, Utc};
use croner::Cron;
use croner::parser::{CronParser, Seconds, Year};
use embyx_core::cron::CronSpec;

/// cronsim's grammar for croner: five fields, or six with seconds first; no year field;
/// day-of-month and day-of-week OR-ed (cronsim ANDs them only when one starts with `*`,
/// which croner's OR gives the same fires for, as the probe shows).
fn parser() -> CronParser {
    CronParser::builder()
        .seconds(Seconds::Optional)
        .year(Year::Disallowed)
        .dom_and_dow(false)
        .build()
}

/// `CronSim(expr, base)` then `next()`: `None` where cronsim raises `CronSimError` (or
/// stops), and where croner, after cronsim accepted the expression, does not (no
/// expression in the goldens does that; D100).
fn fire_after(expr: &str, local: DateTime<FixedOffset>) -> Option<DateTime<Utc>> {
    CronSpec::parse(expr).ok()?;
    let cron: Cron = parser().parse(expr).ok()?;
    // cronsim drops the microseconds of its start and looks strictly after it.
    let start = local.trunc_subsecs(0);
    cron.find_next_occurrence(&start, false)
        .ok()
        .map(|fire| fire.with_timezone(&Utc).trunc_subsecs(0))
}

/// `_next_cron_fire(expr, now=now)` in the server's local time (`TZ`, else
/// `/etc/localtime`, as Python's `time.localtime` reads them).
pub fn next_cron_fire(expr: &str, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    fire_after(expr, now.with_timezone(&Local).fixed_offset())
}

/// `_next_cron_fire` with the local zone given (tests and the DST chains).
pub fn next_cron_fire_in<Z: TimeZone>(expr: &str, now: DateTime<Utc>, zone: &Z) -> Option<DateTime<Utc>> {
    fire_after(expr, now.with_timezone(zone).fixed_offset())
}
