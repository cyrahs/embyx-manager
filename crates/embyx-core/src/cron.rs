//! Cron expressions as `cronsim` (2.7) parses them.
//!
//! Python origin: the `CronSim` constructor of the `cronsim` package, which
//! `config/models.py` uses to validate `archive.scan_cron` (`CronSim(expr, datetime(2000, 1,
//! 1))`, never iterated) and whose error text ends up in the section's validation error.
//! This module is that parser, message for message; the next-fire evaluation is the
//! scheduler's (WP9), which may build on the parsed fields here.

use std::collections::BTreeSet;

use crate::pytext;

/// `CronSim.LAST`: `L` in the day-of-month field, or `<dow>L` in the day-of-week field.
pub const LAST: i32 = -1000;
/// `CronSim.LAST_WEEKDAY`: `LW` in the day-of-month field.
pub const LAST_WEEKDAY: i32 = -1001;

const SYMBOLIC_DAYS: [&str; 7] = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];
const SYMBOLIC_MONTHS: [&str; 12] = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];
const DAYS_IN_MONTH: [i32; 13] = [-1, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/// `CronSimError`; the text is cronsim's (`Wrong number of fields`, `Bad <field>`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct CronError(pub String);

/// The six fields, seconds first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Field {
    Second,
    Minute,
    Hour,
    Day,
    Month,
    Dow,
}

/// One item of a parsed field: a value, or for the day-of-week field `(dow, nth)` where
/// `nth` is 1-5 for `<dow>#<nth>` or [`LAST`] for `<dow>L`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Item {
    Value(i32),
    Nth(i32, i32),
}

impl Field {
    fn name(self) -> &'static str {
        match self {
            Self::Second => "second",
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day-of-month",
            Self::Month => "month",
            Self::Dow => "day-of-week",
        }
    }

    /// `RANGES[field]` as `(start, end)` inclusive.
    fn range(self) -> (i32, i32) {
        match self {
            Self::Second | Self::Minute => (0, 59),
            Self::Hour => (0, 23),
            Self::Day => (1, 31),
            Self::Month => (1, 12),
            Self::Dow => (0, 7),
        }
    }

    fn bad(self) -> CronError {
        CronError(format!("Bad {}", self.name()))
    }

    /// `Field._int`: ASCII digits only.
    fn int_digits(self, value: &str) -> Result<i32, CronError> {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(self.bad());
        }
        // A run of digits too long for i32 is out of every range anyway.
        Ok(value.parse::<i32>().unwrap_or(i32::MAX))
    }

    /// `Field.int`: a symbolic month or day name, or a number in the field's range.
    fn int(self, text: &str) -> Result<i32, CronError> {
        if self == Self::Month
            && let Some(index) = SYMBOLIC_MONTHS.iter().position(|&name| name == text)
        {
            return Ok(i32::try_from(index).unwrap_or(0) + 1);
        }
        if self == Self::Dow
            && let Some(index) = SYMBOLIC_DAYS.iter().position(|&name| name == text)
        {
            return Ok(i32::try_from(index).unwrap_or(0));
        }
        let value = self.int_digits(text)?;
        let (start, end) = self.range();
        if !(start..=end).contains(&value) {
            return Err(self.bad());
        }
        Ok(value)
    }

    fn full(self) -> BTreeSet<Item> {
        let (start, end) = self.range();
        (start..=end).map(Item::Value).collect()
    }

    /// `Field.parse`, branch for branch and in the same order.
    fn parse(self, text: &str) -> Result<BTreeSet<Item>, CronError> {
        if text == "*" {
            return Ok(self.full());
        }
        if text.contains(',') {
            let mut result = BTreeSet::new();
            for term in text.split(',') {
                result.extend(self.parse(term)?);
            }
            return Ok(result);
        }
        if self == Self::Dow && text.contains('L') {
            // `s[:-1]`: everything but the last character, whatever it is.
            let value = drop_last_char(text);
            if !pytext::py_isdigit(value) {
                return Err(self.bad());
            }
            let dow = self.int(value)?;
            return Ok(BTreeSet::from([Item::Nth(dow, LAST)]));
        }
        if text.contains('#') && self == Self::Dow {
            let (term, nth) = text.split_once('#').unwrap_or((text, ""));
            let nth = self.int_digits(nth)?;
            if !(1..=5).contains(&nth) {
                return Err(self.bad());
            }
            return Ok(BTreeSet::from([Item::Nth(self.int(term)?, nth)]));
        }
        if let Some((term, step)) = text.split_once('/') {
            let step = self.int_digits(step)?;
            if step == 0 {
                return Err(self.bad());
            }
            let items = self.parse(term)?;
            if items == BTreeSet::from([Item::Value(LAST)]) || items == BTreeSet::from([Item::Value(LAST_WEEKDAY)]) {
                return Ok(items);
            }
            let step = usize::try_from(step).unwrap_or(usize::MAX);
            if items.len() == 1 {
                let Some(Item::Value(start)) = items.first().copied() else {
                    return Err(self.bad());
                };
                let (_, end) = self.range();
                return Ok((start..=end).step_by(step).map(Item::Value).collect());
            }
            return Ok(items.into_iter().step_by(step).collect());
        }
        if let Some((start, end)) = text.split_once('-') {
            let start = self.int(start)?;
            let end = self.int(end)?;
            if end < start {
                return Err(self.bad());
            }
            return Ok((start..=end).map(Item::Value).collect());
        }
        if self == Self::Day && text == "LW" {
            return Ok(BTreeSet::from([Item::Value(LAST_WEEKDAY)]));
        }
        if self == Self::Day && text == "L" {
            return Ok(BTreeSet::from([Item::Value(LAST)]));
        }
        Ok(BTreeSet::from([Item::Value(self.int(text)?)]))
    }
}

fn drop_last_char(text: &str) -> &str {
    text.char_indices().last().map_or(text, |(index, _)| &text[..index])
}

fn values(items: &BTreeSet<Item>) -> BTreeSet<i32> {
    items
        .iter()
        .filter_map(|item| match item {
            Item::Value(value) => Some(*value),
            Item::Nth(..) => None,
        })
        .collect()
}

/// A parsed expression: what `CronSim.__init__` builds before it is iterated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronSpec {
    /// The expression had five fields (cronsim treats those as Debian cron).
    pub five_fields: bool,
    /// Day-of-month and day-of-week combine with AND (either field starts with `*`), else OR.
    pub day_and: bool,
    pub seconds: BTreeSet<i32>,
    pub minutes: BTreeSet<i32>,
    pub hours: BTreeSet<i32>,
    /// Days of the month; may hold [`LAST`] or [`LAST_WEEKDAY`].
    pub days: BTreeSet<i32>,
    pub months: BTreeSet<i32>,
    pub weekdays: BTreeSet<Item>,
}

impl CronSpec {
    /// `CronSim(expr, dt)`'s validation: `expr.upper().split()`, a leading seconds field of
    /// `0` added to five fields, and each field parsed; then a day-of-month that no chosen
    /// month has is refused.
    pub fn parse(expr: &str) -> Result<Self, CronError> {
        let upper = expr.to_uppercase();
        let mut parts: Vec<&str> = pytext::py_split(&upper).collect();
        let five_fields = parts.len() == 5;
        if five_fields {
            parts.insert(0, "0");
        }
        if parts.len() != 6 {
            return Err(CronError("Wrong number of fields".to_owned()));
        }
        let day_and = parts[3].starts_with('*') || parts[5].starts_with('*');
        let seconds = values(&Field::Second.parse(parts[0])?);
        let minutes = values(&Field::Minute.parse(parts[1])?);
        let hours = values(&Field::Hour.parse(parts[2])?);
        let days = values(&Field::Day.parse(parts[3])?);
        let months = values(&Field::Month.parse(parts[4])?);
        let weekdays = Field::Dow.parse(parts[5])?;
        if let Some(&first) = days.first()
            && first > 29
        {
            let longest = months
                .iter()
                .filter_map(|&month| DAYS_IN_MONTH.get(usize::try_from(month).ok()?).copied())
                .max()
                .unwrap_or(-1);
            if first > longest {
                return Err(Field::Day.bad());
            }
        }
        Ok(Self {
            five_fields,
            day_and,
            seconds,
            minutes,
            hours,
            days,
            months,
            weekdays,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::CronSpec;
    use crate::golden;

    #[test]
    fn validation_matches_cronsim() {
        let document = golden::load("cron/validation.json");
        for case in golden::cases(&document) {
            let normalized = golden::string(golden::get(case, "normalized"));
            let expected = golden::get(case, "error").as_str().map(str::to_owned);
            let got = CronSpec::parse(&normalized).err().map(|error| error.0);
            assert_eq!(got, expected, "{normalized:?}");
        }
    }

    #[test]
    fn fields_hold_what_cronsim_builds() {
        let spec = CronSpec::parse("*/15 0-23/7 L 1,jul 5L").expect("valid");
        assert_eq!(spec.minutes.iter().copied().collect::<Vec<_>>(), [0, 15, 30, 45]);
        assert_eq!(spec.hours.iter().copied().collect::<Vec<_>>(), [0, 7, 14, 21]);
        assert_eq!(spec.days.iter().copied().collect::<Vec<_>>(), [super::LAST]);
        assert_eq!(spec.months.iter().copied().collect::<Vec<_>>(), [1, 7]);
        assert_eq!(
            spec.weekdays.iter().copied().collect::<Vec<_>>(),
            [super::Item::Nth(5, super::LAST)]
        );
        assert!(spec.five_fields && !spec.day_and);
        let spec = CronSpec::parse("0,30/15 * * * *").expect("valid");
        assert_eq!(spec.minutes.iter().copied().collect::<Vec<_>>(), [0, 30, 45]);
        let spec = CronSpec::parse("1-10/3 * * * *").expect("valid");
        assert_eq!(spec.minutes.iter().copied().collect::<Vec<_>>(), [1, 4, 7, 10]);
        assert!(CronSpec::parse("0 4 * * 1#2").expect("valid").day_and);
    }
}
