//! Retry ladders, `Retry-After`, and the sleep they wait with.
//!
//! Python origin: the `_API_RETRY_DELAYS_SECONDS` ladders and `asyncio.sleep` of the
//! sources (Jandan's API, Kemono's API and file downloads), Kemono's
//! `retry_after_seconds` (`Retry-After` as delta-seconds or an HTTP date, parsed by
//! `email.utils.parsedate_to_datetime`) and `retry_delay_seconds`. The sleep is injected
//! ([`Sleeper`]) so tests record the delays instead of waiting, as Python's tests
//! monkeypatch `asyncio.sleep`.
//!
//! Copied from fav (`fav-net/src/retry.rs`); see docs/CONVENTIONS.md, "Code copied from fav". `D` numbers
//! marked fav's are fav's own DIVERGENCES.md entries.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone as _, Utc};
use embyx_core::pytext::{is_py_decimal, py_decimal_value, py_isdigit, py_split, py_strip};

/// `asyncio.sleep(seconds)`, injectable.
pub type Sleeper = Arc<dyn Fn(f64) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// The real sleep. A negative or non-finite delay returns at once, as `asyncio.sleep` does
/// for zero and below; one too long for a `Duration` (a setting past about 5.8e11 years)
/// sleeps for `Duration::MAX` instead of panicking.
pub fn tokio_sleeper() -> Sleeper {
    Arc::new(|seconds: f64| {
        Box::pin(async move {
            if seconds.is_finite() && seconds > 0.0 {
                tokio::time::sleep(Duration::try_from_secs_f64(seconds).unwrap_or(Duration::MAX)).await;
            }
        })
    })
}

/// A sleeper that returns at once and records every delay, for tests.
#[derive(Debug, Clone, Default)]
pub struct RecordingSleeper {
    delays: Arc<Mutex<Vec<f64>>>,
}

impl RecordingSleeper {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sleeper(&self) -> Sleeper {
        let delays = Arc::clone(&self.delays);
        Arc::new(move |seconds: f64| {
            delays
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(seconds);
            Box::pin(async {})
        })
    }

    /// Every delay asked for so far.
    pub fn delays(&self) -> Vec<f64> {
        self.delays
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// `retry_delay_seconds(attempt, retry_after)`: a server's `Retry-After`, capped at
/// `max_retry_after`; otherwise the ladder's step for `attempt` (1-based), the last step
/// repeating.
pub fn ladder_delay(delays: &[f64], attempt: usize, retry_after: Option<f64>, max_retry_after: f64) -> f64 {
    if let Some(retry_after) = retry_after {
        return retry_after.min(max_retry_after);
    }
    let index = attempt.min(delays.len()).saturating_sub(1);
    delays.get(index).copied().unwrap_or_default()
}

/// `retry_after_seconds(response)` for the header's value: delta-seconds, or the time
/// left until an HTTP date (never negative); `None` without a usable value.
///
/// `Err` carries the `ValueError` text Python raises when `isdigit()` accepts the value
/// but `float()` does not (superscript digits).
pub fn retry_after_seconds(value: Option<&str>, now: DateTime<Utc>) -> Result<Option<f64>, String> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let stripped = py_strip(value);
    if py_isdigit(stripped) {
        if !stripped.chars().all(is_py_decimal) {
            return Err(format!("could not convert string to float: {}", py_repr(stripped)));
        }
        let ascii: String = stripped
            .chars()
            .filter_map(py_decimal_value)
            .map(|digit| char::from_digit(digit, 10).unwrap_or('0'))
            .collect();
        return Ok(ascii.parse::<f64>().ok());
    }
    let Some(retry_at) = parsedate_to_datetime(value) else {
        return Ok(None);
    };
    let delta = retry_at.signed_duration_since(now);
    // Python's `total_seconds()` is a float too; a delay never needs more precision.
    #[allow(clippy::cast_precision_loss)]
    let seconds = delta
        .num_microseconds()
        .map_or(f64::MAX, |micros| micros as f64 / 1_000_000.0);
    Ok(Some(seconds.max(0.0)))
}

fn py_repr(text: &str) -> String {
    if text.contains('\'') && !text.contains('"') {
        format!("\"{text}\"")
    } else {
        format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
    }
}

const MONTH_NAMES: [&str; 24] = [
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec",
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
];
const DAY_NAMES: [&str; 7] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];

fn zone_offset(name: &str) -> Option<i64> {
    Some(match name {
        "UT" | "UTC" | "GMT" | "Z" => 0,
        "ADT" => -300,
        "AST" | "EDT" => -400,
        "EST" | "CDT" => -500,
        "CST" | "MDT" => -600,
        "MST" | "PDT" => -700,
        "PST" => -800,
        _ => return None,
    })
}

/// Python `int(token)`: an optional sign, then decimal digits with single underscores
/// between them.
fn py_int(token: &str) -> Option<i64> {
    let token = py_strip(token);
    let (negative, digits) = match token.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, token.strip_prefix('+').unwrap_or(token)),
    };
    if digits.is_empty() || digits.starts_with('_') || digits.ends_with('_') || digits.contains("__") {
        return None;
    }
    let mut value: i64 = 0;
    for ch in digits.chars().filter(|ch| *ch != '_') {
        let digit = py_decimal_value(ch)?;
        value = value.checked_mul(10)?.checked_add(i64::from(digit))?;
    }
    Some(if negative { -value } else { value })
}

/// `email.utils._parsedate_tz`: the date fields and the zone offset in seconds (`None`
/// for no zone, or `-0000`).
#[allow(clippy::type_complexity)] // a direct port of the tuple Python returns
fn parsedate_tz(data: &str) -> Option<([i64; 6], Option<i64>)> {
    let mut parts: Vec<String> = py_split(data).map(str::to_owned).collect();
    if parts.is_empty() {
        return None;
    }
    if parts[0].ends_with(',') || DAY_NAMES.contains(&parts[0].to_lowercase().as_str()) {
        parts.remove(0);
    } else if let Some(comma) = parts[0].rfind(',') {
        parts[0].drain(..=comma);
    }
    if parts.len() == 3 {
        let stuff: Vec<String> = parts[0].split('-').map(str::to_owned).collect();
        if stuff.len() == 3 {
            parts = stuff.into_iter().chain(parts[1..].iter().cloned()).collect();
        }
    }
    if parts.len() == 4 {
        let last = parts[3].clone();
        let sign = last.find('+').or_else(|| last.find('-'));
        match sign {
            Some(index) if index > 0 => {
                last[..index].clone_into(&mut parts[3]);
                parts.push(last[index..].to_owned());
            }
            _ => parts.push(String::new()),
        }
    }
    if parts.len() < 5 {
        return None;
    }
    let [mut dd, mut mm, mut yy, mut tm, mut tz] = [0, 1, 2, 3, 4].map(|index| parts[index].clone());
    if dd.is_empty() || mm.is_empty() || yy.is_empty() {
        return None;
    }
    mm = mm.to_lowercase();
    if !MONTH_NAMES.contains(&mm.as_str()) {
        let swapped = dd.to_lowercase();
        dd = mm;
        mm = swapped;
        if !MONTH_NAMES.contains(&mm.as_str()) {
            return None;
        }
    }
    let mut month = i64::try_from(MONTH_NAMES.iter().position(|name| *name == mm)?).ok()? + 1;
    if month > 12 {
        month -= 12;
    }
    if dd.ends_with(',') {
        dd.pop();
    }
    if yy.find(':').is_some_and(|index| index > 0) {
        std::mem::swap(&mut yy, &mut tm);
    }
    if yy.ends_with(',') {
        yy.pop();
        if yy.is_empty() {
            return None;
        }
    }
    if !yy.chars().next().is_some_and(embyx_core::pytext::is_py_digit) {
        std::mem::swap(&mut yy, &mut tz);
    }
    if tm.ends_with(',') {
        tm.pop();
    }
    let mut time: Vec<&str> = tm.split(':').collect();
    if time.len() == 1 && tm.contains('.') {
        time = tm.split('.').collect();
        if !(2..=3).contains(&time.len()) {
            return None;
        }
    } else if !(2..=3).contains(&time.len()) {
        return None;
    }
    let (thh, tmm, tss) = (time[0], time[1], time.get(2).copied().unwrap_or("0"));
    let mut year = py_int(&yy)?;
    let day = py_int(&dd)?;
    let hour = py_int(thh)?;
    let minute = py_int(tmm)?;
    let second = py_int(tss)?;
    if year < 100 {
        year += if year > 68 { 1900 } else { 2000 };
    }
    let tz = tz.to_uppercase();
    let mut offset = zone_offset(&tz).or_else(|| py_int(&tz));
    if offset == Some(0) && tz.starts_with('-') {
        offset = None;
    }
    let offset = offset.map(|offset| {
        if offset == 0 {
            0
        } else {
            let sign = offset.signum();
            let offset = offset.abs();
            sign * ((offset / 100) * 3600 + (offset % 100) * 60)
        }
    });
    Some(([year, month, day, hour, minute, second], offset))
}

/// `email.utils.parsedate_to_datetime(value)`, with a missing zone read as UTC (what
/// `retry_after_seconds` does with a naive result); `None` where Python raises.
pub fn parsedate_to_datetime(value: &str) -> Option<DateTime<Utc>> {
    let ([year, month, day, hour, minute, second], offset) = parsedate_tz(value)?;
    if !(1..=9999).contains(&year) {
        return None;
    }
    let date = NaiveDate::from_ymd_opt(
        i32::try_from(year).ok()?,
        u32::try_from(month).ok()?,
        u32::try_from(day).ok()?,
    )?;
    let naive = date.and_hms_opt(
        u32::try_from(hour).ok()?,
        u32::try_from(minute).ok()?,
        u32::try_from(second).ok()?,
    )?;
    let zone = FixedOffset::east_opt(i32::try_from(offset.unwrap_or(0)).ok()?)?;
    zone.from_local_datetime(&naive)
        .single()
        .map(|local| local.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::{ladder_delay, parsedate_to_datetime};

    #[test]
    fn ladder_repeats_its_last_step_and_caps_retry_after() {
        let delays = [2.0, 5.0, 15.0, 30.0];
        assert!((ladder_delay(&delays, 1, None, 300.0) - 2.0).abs() < f64::EPSILON);
        assert!((ladder_delay(&delays, 4, None, 300.0) - 30.0).abs() < f64::EPSILON);
        assert!((ladder_delay(&delays, 99, None, 300.0) - 30.0).abs() < f64::EPSILON);
        assert!((ladder_delay(&delays, 1, Some(17.0), 300.0) - 17.0).abs() < f64::EPSILON);
        assert!((ladder_delay(&delays, 1, Some(9999.0), 300.0) - 300.0).abs() < f64::EPSILON);
    }

    #[test]
    fn http_dates_parse() {
        let parsed = parsedate_to_datetime("Mon, 17 Aug 2020 00:00:00 GMT").expect("parses");
        assert_eq!(parsed.to_rfc3339(), "2020-08-17T00:00:00+00:00");
    }
}
