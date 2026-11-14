//! pydantic's `datetime` and `date` fields of the stored fill-actor models.
//!
//! Python origin: pydantic 2 / pydantic-core (speedate) as locked at the baseline, behind
//! `FillActorPlan.created_at` / `expires_at` and `VideoPlan.release_date` in
//! `fill_actor/models.py`. A plan is stored with `model_dump_json()` and read back with
//! `model_validate_json`, so a datetime keeps what pydantic keeps: the wall time to the
//! microsecond and its UTC offset, or no offset at all (a naive datetime). [`PyDateTime`]
//! holds exactly that, so a plan read back dumps to the bytes it was stored as.
//!
//! Parsing takes the forms pydantic takes in JSON mode (`goldens/fill_actor/models.json`):
//! `YYYY-MM-DD[Tt _]HH:MM[:SS[.f+]][Zz|±HH:MM|±HHMM]`, a bare date (midnight, naive), and
//! Unix timestamps as JSON numbers or numeric strings (seconds, milliseconds above 2e10,
//! UTC). Errors carry pydantic's `type`, and the reason text speedate gives for the cases the
//! goldens pin (DIVERGENCES.md D111).

use std::cmp::Ordering;
use std::fmt::Write as _;

use chrono::{
    DateTime, Datelike as _, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Timelike as _, Utc,
};
use embyx_core::config::lax::Refusal;
use embyx_core::pyjson::Value;

/// A Python `datetime` as pydantic reads and writes it: wall time (microseconds) and an
/// optional fixed offset.
#[derive(Debug, Clone, Copy)]
pub struct PyDateTime {
    local: NaiveDateTime,
    offset: Option<FixedOffset>,
}

impl PyDateTime {
    /// An aware datetime in UTC (`datetime.now(UTC)`), truncated to microseconds as a Python
    /// datetime is.
    pub fn utc(value: DateTime<Utc>) -> Self {
        Self::aware(value.fixed_offset())
    }

    /// An aware datetime at its own offset, truncated to microseconds.
    pub fn aware(value: DateTime<FixedOffset>) -> Self {
        Self {
            local: truncate_micros(value.naive_local()),
            offset: Some(*value.offset()),
        }
    }

    /// A naive datetime (no `tzinfo`), truncated to microseconds.
    pub fn naive(value: NaiveDateTime) -> Self {
        Self {
            local: truncate_micros(value),
            offset: None,
        }
    }

    /// The wall time.
    pub fn local(&self) -> NaiveDateTime {
        self.local
    }

    /// The UTC offset; `None` for a naive datetime.
    pub fn offset(&self) -> Option<FixedOffset> {
        self.offset
    }

    /// `persistence.normalize_datetime`: a naive datetime is taken as UTC, an aware one is
    /// converted to UTC.
    pub fn to_utc(&self) -> DateTime<Utc> {
        match self.offset {
            None => self.local.and_utc(),
            Some(offset) => (self.local - TimeDelta::seconds(i64::from(offset.local_minus_utc()))).and_utc(),
        }
    }

    /// The text `model_dump_json()` writes: `2026-07-13T10:00:00[.123456](Z|+09:00)`.
    pub fn pydantic_text(&self) -> String {
        let mut out = format!(
            "{}T{:02}:{:02}:{:02}",
            date_text(self.local.date()),
            self.local.hour(),
            self.local.minute(),
            self.local.second()
        );
        let micros = self.local.nanosecond() / 1000;
        if micros != 0 {
            let _ = write!(out, ".{micros:06}");
        }
        if let Some(offset) = self.offset {
            let seconds = offset.local_minus_utc();
            if seconds == 0 {
                out.push('Z');
            } else {
                let sign = if seconds < 0 { '-' } else { '+' };
                let seconds = seconds.unsigned_abs();
                let _ = write!(out, "{sign}{:02}:{:02}", seconds / 3600, (seconds / 60) % 60);
                if !seconds.is_multiple_of(60) {
                    let _ = write!(out, ":{:02}", seconds % 60);
                }
            }
        }
        out
    }

    /// The JSON value of the dump.
    pub fn to_value(&self) -> Value {
        Value::Str(self.pydantic_text())
    }
}

/// Python's `datetime.__eq__`: two aware datetimes are equal at the same instant, two naive
/// ones at the same wall time, and an aware one never equals a naive one.
impl PartialEq for PyDateTime {
    fn eq(&self, other: &Self) -> bool {
        match (self.offset, other.offset) {
            (Some(_), Some(_)) => self.to_utc() == other.to_utc(),
            (None, None) => self.local == other.local,
            _ => false,
        }
    }
}

impl From<DateTime<Utc>> for PyDateTime {
    fn from(value: DateTime<Utc>) -> Self {
        Self::utc(value)
    }
}

/// Python orders aware datetimes by instant; comparing an aware with a naive one raises,
/// which is `None` here.
impl PartialOrd for PyDateTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self.offset, other.offset) {
            (Some(_), Some(_)) => Some(self.to_utc().cmp(&other.to_utc())),
            (None, None) => Some(self.local.cmp(&other.local)),
            _ => None,
        }
    }
}

fn truncate_micros(value: NaiveDateTime) -> NaiveDateTime {
    let nanos = value.nanosecond();
    value.with_nanosecond(nanos - nanos % 1000).unwrap_or(value)
}

/// `date.isoformat()` / pydantic's date text: `YYYY-MM-DD` (years below 1000 padded).
pub fn date_text(value: NaiveDate) -> String {
    format!("{:04}-{:02}-{:02}", value.year(), value.month(), value.day())
}

const DATETIME_TYPE: &str = "Input should be a valid datetime";
const DATE_TYPE: &str = "Input should be a valid date";
const DATE_INEXACT: &str = "Datetimes provided to dates should have zero time - e.g. be exact dates";

/// A `datetime` field in JSON mode.
pub fn lax_datetime(value: &Value) -> Result<PyDateTime, Refusal> {
    match value {
        Value::Int(seconds) => {
            from_timestamp(int_seconds(*seconds)).ok_or_else(|| datetime_parsing("value is too large"))
        }
        Value::Float(seconds) => from_timestamp(*seconds).ok_or_else(|| datetime_parsing("value is too large")),
        Value::Str(text) => parse_datetime_text(text).map_err(|reason| datetime_parsing(&reason)),
        _ => Err(Refusal {
            kind: "datetime_type",
            msg: DATETIME_TYPE.to_owned(),
        }),
    }
}

/// A `date` field in JSON mode: a date, or a datetime (text or timestamp) at midnight.
pub fn lax_date(value: &Value) -> Result<NaiveDate, Refusal> {
    let datetime = match value {
        Value::Int(seconds) => {
            from_timestamp(int_seconds(*seconds)).ok_or_else(|| date_parsing("value is too large"))?
        }
        Value::Float(seconds) => from_timestamp(*seconds).ok_or_else(|| date_parsing("value is too large"))?,
        Value::Str(text) => {
            if let Ok(date) = parse_date_prefix(text)
                && text.len() == 10
            {
                return Ok(date);
            }
            parse_datetime_text(text).map_err(|reason| date_parsing(&reason))?
        }
        _ => {
            return Err(Refusal {
                kind: "date_type",
                msg: DATE_TYPE.to_owned(),
            });
        }
    };
    if datetime.local.time() == NaiveTime::MIN {
        Ok(datetime.local.date())
    } else {
        Err(Refusal {
            kind: "date_from_datetime_inexact",
            msg: DATE_INEXACT.to_owned(),
        })
    }
}

fn datetime_parsing(reason: &str) -> Refusal {
    Refusal {
        kind: "datetime_from_date_parsing",
        msg: format!("Input should be a valid datetime or date, {reason}"),
    }
}

fn date_parsing(reason: &str) -> Refusal {
    Refusal {
        kind: "date_from_datetime_parsing",
        msg: format!("Input should be a valid date or datetime, {reason}"),
    }
}

/// An integer timestamp as the float speedate works with; past 2**53 the date is out of
/// range either way.
#[allow(clippy::cast_precision_loss, reason = "exact within every representable date")]
fn int_seconds(seconds: i128) -> f64 {
    seconds as f64
}

/// speedate's timestamp rule: seconds, or milliseconds when above 2e10; UTC.
fn from_timestamp(value: f64) -> Option<PyDateTime> {
    if !value.is_finite() {
        return None;
    }
    let seconds = if value.abs() > 20_000_000_000.0 {
        value / 1000.0
    } else {
        value
    };
    let whole = seconds.floor();
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "range-checked by DateTime::from_timestamp; the fraction is in [0, 1)"
    )]
    let (whole_seconds, micros) = (whole as i64, ((seconds - whole) * 1_000_000.0).round() as u32);
    let (whole_seconds, micros) = if micros >= 1_000_000 {
        (whole_seconds.checked_add(1)?, 0)
    } else {
        (whole_seconds, micros)
    };
    DateTime::from_timestamp(whole_seconds, micros * 1000).map(PyDateTime::utc)
}

/// A numeric string speedate reads as a timestamp: an optional `-`, digits, an optional
/// fraction.
fn numeric_timestamp(text: &str) -> Option<f64> {
    let body = text.strip_prefix('-').unwrap_or(text);
    let (whole, fraction) = body.split_once('.').unwrap_or((body, ""));
    let digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if whole.is_empty() || !digits(whole) || !digits(fraction) || (body.contains('.') && fraction.is_empty()) {
        return None;
    }
    text.parse().ok()
}

/// The date at the start of the text, with speedate's reasons.
fn parse_date_prefix(text: &str) -> Result<NaiveDate, String> {
    let bytes = text.as_bytes();
    if bytes.len() < 10 {
        return Err("input is too short".to_owned());
    }
    let number = |range: std::ops::Range<usize>, what: &str| -> Result<u32, String> {
        let part = &bytes[range];
        if part.iter().all(u8::is_ascii_digit) {
            Ok(part.iter().fold(0, |total, digit| total * 10 + u32::from(digit - b'0')))
        } else {
            Err(format!("invalid character in {what}"))
        }
    };
    let year = number(0..4, "year")?;
    if bytes[4] != b'-' {
        return Err("invalid date separator, expected `-`".to_owned());
    }
    let month = number(5..7, "month")?;
    if bytes[7] != b'-' {
        return Err("invalid date separator, expected `-`".to_owned());
    }
    let day = number(8..10, "day")?;
    if !(1..=12).contains(&month) {
        return Err("month value is outside expected range".to_owned());
    }
    #[allow(clippy::cast_possible_wrap, reason = "four digits")]
    NaiveDate::from_ymd_opt(year as i32, month, day).ok_or_else(|| "day value is outside expected range".to_owned())
}

/// A datetime text. On failure the reason is the date parser's over the whole text, as
/// pydantic reports it (`datetime_from_date_parsing`).
fn parse_datetime_text(text: &str) -> Result<PyDateTime, String> {
    if let Some(seconds) = numeric_timestamp(text) {
        return from_timestamp(seconds).ok_or_else(|| "value is too large".to_owned());
    }
    let date = parse_date_prefix(text)?;
    if text.len() == 10 {
        return Ok(PyDateTime::naive(date.and_time(NaiveTime::MIN)));
    }
    parse_time_part(date, &text.as_bytes()[10..])
        .ok_or_else(|| "unexpected extra characters at the end of the input".to_owned())
}

fn two_digits(bytes: &[u8], at: usize) -> Option<u32> {
    let pair = bytes.get(at..at + 2)?;
    pair.iter()
        .all(u8::is_ascii_digit)
        .then(|| u32::from(pair[0] - b'0') * 10 + u32::from(pair[1] - b'0'))
}

/// `[Tt _]HH:MM[:SS[.f+]][Zz|±HH:MM|±HHMM]` after the date.
fn parse_time_part(date: NaiveDate, rest: &[u8]) -> Option<PyDateTime> {
    if !matches!(rest.first()?, b'T' | b't' | b' ' | b'_') {
        return None;
    }
    let hour = two_digits(rest, 1)?;
    if rest.get(3) != Some(&b':') {
        return None;
    }
    let minute = two_digits(rest, 4)?;
    let mut at = 6;
    let mut second = 0;
    let mut micros = 0;
    if rest.get(at) == Some(&b':') {
        second = two_digits(rest, at + 1)?;
        at += 3;
        if matches!(rest.get(at), Some(b'.' | b',')) {
            at += 1;
            let start = at;
            while rest.get(at).is_some_and(u8::is_ascii_digit) {
                at += 1;
            }
            if at == start {
                return None;
            }
            let digits = &rest[start..at.min(start + 6)];
            micros = digits
                .iter()
                .fold(0, |total, digit| total * 10 + u32::from(digit - b'0'));
            for _ in digits.len()..6 {
                micros *= 10;
            }
        }
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let local = date.and_time(NaiveTime::from_hms_micro_opt(hour, minute, second, micros)?);
    let offset = match rest.get(at) {
        None => return Some(PyDateTime::naive(local)),
        Some(b'Z' | b'z') => {
            at += 1;
            FixedOffset::east_opt(0)?
        }
        Some(sign @ (b'+' | b'-')) => {
            let hours = two_digits(rest, at + 1)?;
            at += 3;
            if rest.get(at) == Some(&b':') {
                at += 1;
            }
            let minutes = two_digits(rest, at)?;
            at += 2;
            if hours > 23 || minutes > 59 {
                return None;
            }
            #[allow(clippy::cast_possible_wrap, reason = "below a day")]
            let seconds = (hours * 3600 + minutes * 60) as i32;
            FixedOffset::east_opt(if *sign == b'-' { -seconds } else { seconds })?
        }
        Some(_) => return None,
    };
    if at != rest.len() {
        return None;
    }
    Some(PyDateTime {
        local,
        offset: Some(offset),
    })
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeZone as _, Utc};

    use super::PyDateTime;

    #[test]
    fn equality_and_order_follow_python() {
        let utc = PyDateTime::utc(Utc.with_ymd_and_hms(2026, 7, 13, 10, 0, 0).unwrap());
        let tokyo = PyDateTime::aware(
            chrono::FixedOffset::east_opt(9 * 3600)
                .unwrap()
                .with_ymd_and_hms(2026, 7, 13, 19, 0, 0)
                .unwrap(),
        );
        let naive = PyDateTime::naive(
            NaiveDate::from_ymd_opt(2026, 7, 13)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        );
        assert_eq!(utc, tokyo);
        assert_ne!(utc, naive);
        assert_eq!(utc.partial_cmp(&naive), None);
        assert_eq!(tokyo.pydantic_text(), "2026-07-13T19:00:00+09:00");
        assert_eq!(naive.to_utc(), utc.to_utc());
        // Nanoseconds are cut to microseconds, as a Python datetime holds them.
        let precise = PyDateTime::utc(Utc.timestamp_opt(1, 123_456_789).unwrap());
        assert_eq!(precise.pydantic_text(), "1970-01-01T00:00:01.123456Z");
    }
}
