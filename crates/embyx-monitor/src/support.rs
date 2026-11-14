//! Small pieces the monitor modules share: the injected clock, boxed futures for the
//! dyn-compatible traits, Python's `datetime` range, `uuid4().hex`, and the error the
//! repositories return for a database failure.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use embyx_core::pytext::py_repr;
use embyx_db::database::DatabaseError;

/// A boxed `Send` future, for traits that are used as `dyn` (the ledger, the intake's sources).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// `datetime.now(UTC)`, injected so tests and scenarios can pin it.
pub type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// The wall clock.
pub fn system_clock() -> Clock {
    Arc::new(Utc::now)
}

/// A clock that always answers `at`.
pub fn fixed_clock(at: DateTime<Utc>) -> Clock {
    Arc::new(move || at)
}

/// Python's `OverflowError` from `datetime` arithmetic or a `timedelta` out of range.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct OverflowError(pub String);

impl OverflowError {
    /// `datetime + timedelta` leaving years 1 to 9999.
    pub fn date() -> Self {
        Self("date value out of range".to_owned())
    }
}

/// `datetime.min` / `datetime.max` (aware, UTC): Python refuses anything outside.
fn in_python_range(value: DateTime<Utc>) -> bool {
    let first = NaiveDate::from_ymd_opt(1, 1, 1)
        .expect("valid")
        .and_hms_opt(0, 0, 0)
        .expect("valid");
    let last = NaiveDate::from_ymd_opt(9999, 12, 31)
        .expect("valid")
        .and_hms_micro_opt(23, 59, 59, 999_999)
        .expect("valid");
    let naive = value.naive_utc();
    naive >= first && naive <= last
}

/// `value + delta` with Python's `OverflowError` outside years 1 to 9999.
pub fn py_add(value: DateTime<Utc>, delta: TimeDelta) -> Result<DateTime<Utc>, OverflowError> {
    value
        .checked_add_signed(delta)
        .filter(|sum| in_python_range(*sum))
        .ok_or_else(OverflowError::date)
}

/// `value - delta`, as [`py_add`].
pub fn py_sub(value: DateTime<Utc>, delta: TimeDelta) -> Result<DateTime<Utc>, OverflowError> {
    value
        .checked_sub_signed(delta)
        .filter(|difference| in_python_range(*difference))
        .ok_or_else(OverflowError::date)
}

/// `timedelta(seconds=seconds)`: Python refuses more than 999,999,999 days either way.
pub fn py_timedelta_seconds(seconds: i64) -> Result<TimeDelta, OverflowError> {
    const MAX_DAYS: i64 = 999_999_999;
    let days = seconds.div_euclid(86_400);
    if i32::try_from(days).is_err() {
        return Err(OverflowError("Python int too large to convert to C int".to_owned()));
    }
    if !(-MAX_DAYS..=MAX_DAYS).contains(&days) {
        return Err(OverflowError(format!("days={days}; must have magnitude <= 999999999")));
    }
    Ok(TimeDelta::seconds(seconds))
}

/// `uuid.uuid4().hex`: 32 lower-case hex digits of a version 4 UUID.
pub fn uuid4_hex() -> String {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).expect("the OS random source");
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    bytes.iter().fold(String::with_capacity(32), |mut hex, byte| {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
        hex
    })
}

/// A repository call that failed: the database, or a stored value Python could not have read
/// either.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The text is `str(exc)` of asyncpg's exception: PostgreSQL's own message for a refusal by
    /// the server (sqlx would prefix it with "error returned from database: ").
    #[error("{}", database_text(.0))]
    Database(#[from] DatabaseError),
    /// A stored value that does not decode (`ValueError` for an unknown state,
    /// `JSONDecodeError` for broken JSON), with Python's message.
    #[error("{message}")]
    Decode { class: &'static str, message: String },
    /// asyncpg's `DataError` for an argument the column type cannot hold.
    #[error("{0}")]
    Argument(String),
}

impl From<sqlx::Error> for StoreError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(DatabaseError::Sqlx(error))
    }
}

impl StoreError {
    /// `ValueError("'x' is not a valid <Enum>")`.
    pub(crate) fn invalid_enum(value: &str, name: &str) -> Self {
        Self::Decode {
            class: "ValueError",
            message: format!("{} is not a valid {name}", py_repr(value)),
        }
    }

    /// Whether the database refused a write for a unique index (asyncpg's
    /// `UniqueViolationError`).
    pub(crate) fn is_unique_violation(error: &sqlx::Error) -> bool {
        matches!(error, sqlx::Error::Database(database) if database.code().as_deref() == Some("23505"))
    }

    /// `type(exc).__name__` as the Python build would raise it (DIVERGENCES.md D61: database
    /// failures are named by SQLSTATE where asyncpg has a class, `PostgresError` otherwise).
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Decode { class, .. } => class,
            Self::Argument(_) => "DataError",
            Self::Database(DatabaseError::UnsupportedSchemaVersion(_)) => "UnsupportedSchemaVersionError",
            Self::Database(DatabaseError::InvalidUrl(_)) => "ClientConfigurationError",
            Self::Database(DatabaseError::Sqlx(error)) => match error {
                sqlx::Error::Database(database) => match database.code().as_deref() {
                    Some("23505") => "UniqueViolationError",
                    Some("23503") => "ForeignKeyViolationError",
                    Some("23514") => "CheckViolationError",
                    Some("23502") => "NotNullViolationError",
                    Some("22003") => "NumericValueOutOfRangeError",
                    Some("57014") => "QueryCanceledError",
                    Some("40001") => "SerializationError",
                    Some("40P01") => "DeadlockDetectedError",
                    _ => "PostgresError",
                },
                sqlx::Error::Io(_) | sqlx::Error::Tls(_) => "ConnectionError",
                sqlx::Error::PoolTimedOut => "TimeoutError",
                _ => "InterfaceError",
            },
        }
    }
}

/// asyncpg's `DataError` text for an integer argument out of the column's range.
/// The text of a database failure as asyncpg words it: the server's message for a statement
/// PostgreSQL refused, the driver's text otherwise.
fn database_text(error: &DatabaseError) -> String {
    match error {
        DatabaseError::Sqlx(sqlx::Error::Database(database)) => database.message().to_owned(),
        other => other.to_string(),
    }
}

/// asyncpg's `DataError` for an argument of an `executemany()` row the column cannot hold
/// (`element` counts the rows from 0).
pub(crate) fn out_of_range_in_element(position: usize, element: usize, value: i128, bits: u32) -> StoreError {
    StoreError::Argument(format!(
        "invalid input for query argument ${position} in element #{element} of executemany() sequence: {value} \
         (value out of int{bits} range)"
    ))
}

pub(crate) fn out_of_range(position: usize, value: i128, bits: u32) -> StoreError {
    StoreError::Argument(format!(
        "invalid input for query argument ${position}: {value} (value out of int{bits} range)"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_hex_is_a_version_4_uuid() {
        let id = uuid4_hex();
        assert_eq!(id.len(), 32);
        assert!(id.chars().all(|ch| matches!(ch, '0'..='9' | 'a'..='f')));
        assert_eq!(&id[12..13], "4");
        assert!(matches!(&id[16..17], "8" | "9" | "a" | "b"));
        assert_ne!(uuid4_hex(), id);
    }

    #[test]
    fn datetime_arithmetic_keeps_python_s_range() {
        let late = DateTime::parse_from_rfc3339("9999-12-31T00:00:00Z")
            .expect("valid")
            .with_timezone(&Utc);
        assert_eq!(py_add(late, TimeDelta::days(1)), Err(OverflowError::date()));
        assert!(py_add(late, TimeDelta::hours(23)).is_ok());
        let early = DateTime::parse_from_rfc3339("0001-01-02T00:00:00Z")
            .expect("valid")
            .with_timezone(&Utc);
        assert_eq!(py_sub(early, TimeDelta::days(2)), Err(OverflowError::date()));
        assert!(py_timedelta_seconds(999_999_999 * 86_400 + 86_399).is_ok());
        // The texts of `timedelta(seconds=...)` at the baseline's Python.
        assert_eq!(
            py_timedelta_seconds(1_000_000_000 * 86_400).map_err(|error| error.0),
            Err("days=1000000000; must have magnitude <= 999999999".to_owned())
        );
        assert_eq!(
            py_timedelta_seconds(-86_400 * 1_000_000_000 - 1).map_err(|error| error.0),
            Err("days=-1000000001; must have magnitude <= 999999999".to_owned())
        );
        assert_eq!(
            py_timedelta_seconds((1 << 31) * 86_400).map_err(|error| error.0),
            Err("Python int too large to convert to C int".to_owned())
        );
    }
}
