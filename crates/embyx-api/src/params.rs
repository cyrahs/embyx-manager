//! What FastAPI does with a route's query and path parameters and its pydantic request and
//! response models, for the monitor and playlists routers.
//!
//! Python origin: `fastapi/dependencies/utils.py` (`request_params_to_args`: a scalar query
//! parameter is `request.query_params.get(name)`, the last of its values), Starlette's
//! `QueryParams` (`parse_qsl(query_string, keep_blank_values=True)`), and pydantic 2 in
//! Python mode for the request models (`model_config = ConfigDict(extra='forbid')`).
//! Response models are written as FastAPI writes them: `model_dump(mode='json')` in field
//! order, a UTC `datetime` as `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` (the fraction only when it is
//! not zero) and a `date` as `YYYY-MM-DD`.

use chrono::{DateTime, NaiveDate, Utc};
use embyx_core::config::lax::{Mode, lax_bool, lax_int, lax_str};
use embyx_core::pyjson::{Dict, Value};
use embyx_core::pyurl::unquote;
use embyx_fill_actor::pydatetime::{PyDateTime, date_text};

use crate::body::JsonBody;

/// `parse_qsl(query, keep_blank_values=True)`: `&`-separated pairs, `+` a space, then
/// percent-decoded; a pair without `=` has an empty value, an empty piece is skipped.
pub fn query_pairs(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|piece| !piece.is_empty())
        .map(|piece| {
            let (name, value) = piece.split_once('=').unwrap_or((piece, ""));
            let decode = |text: &str| unquote(&text.replace('+', " "));
            (decode(name), decode(value))
        })
        .collect()
}

/// `request.query_params.get(name)`: the last value given for it.
pub fn query_get(query: &str, name: &str) -> Option<String> {
    query_pairs(query)
        .into_iter()
        .rev()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)
}

/// An `int` query or path parameter: pydantic's lax integer from the text (`None` is
/// FastAPI's 422). Beyond `i64` it refuses, where Python takes any integer (D106).
pub fn int_param(text: &str) -> Option<i64> {
    lax_int(&Value::Str(text.to_owned())).ok()
}

/// An `int` query parameter with a default: absent is the default, a value that is not an
/// integer is `None` (422).
pub fn int_query(query: &str, name: &str, default: i64) -> Option<i64> {
    match query_get(query, name) {
        None => Some(default),
        Some(text) => int_param(&text),
    }
}

/// A UTC `datetime` as pydantic writes it in JSON mode (`embyx_fill_actor::pydatetime`, the
/// one copy of pydantic's datetime text since WP16).
pub fn py_datetime(value: DateTime<Utc>) -> Value {
    PyDateTime::utc(value).to_value()
}

/// `datetime | None`.
pub fn py_opt_datetime(value: Option<DateTime<Utc>>) -> Value {
    value.map_or(Value::Null, py_datetime)
}

/// A `date` as pydantic writes it.
pub fn py_date(value: NaiveDate) -> Value {
    Value::Str(date_text(value))
}

/// `str | None`.
pub fn opt_str(value: Option<&str>) -> Value {
    value.map_or(Value::Null, Value::from)
}

/// `tuple[str, ...]` / `list[str]`.
pub fn str_list(values: &[String]) -> Value {
    Value::List(values.iter().map(|value| Value::from(value.as_str())).collect())
}

/// A request model's fields: the body must be a JSON object holding only `allowed` keys
/// (`extra='forbid'`).
pub fn model_fields<'a>(body: &'a JsonBody, allowed: &[&str]) -> Option<&'a Dict> {
    let Some(Value::Dict(dict)) = body.value() else {
        return None;
    };
    dict.iter()
        .all(|(key, _)| key.as_str().is_some_and(|key| allowed.contains(&key)))
        .then_some(dict)
}

/// A required `str` field.
pub fn req_str(fields: &Dict, name: &str) -> Option<String> {
    lax_str(fields.get(name)?).ok()
}

/// A `str | None = None` field: `Some(None)` when absent or null, `None` when invalid.
pub fn field_opt_str(fields: &Dict, name: &str) -> Option<Option<String>> {
    match fields.get(name) {
        None | Some(Value::Null) => Some(None),
        Some(value) => lax_str(value).ok().map(Some),
    }
}

/// A `bool | None = None` field.
pub fn field_opt_bool(fields: &Dict, name: &str) -> Option<Option<bool>> {
    match fields.get(name) {
        None | Some(Value::Null) => Some(None),
        Some(value) => lax_bool(value, Mode::Python).ok().map(Some),
    }
}

/// An `int | None = None` field (beyond `i64` refused, D106).
pub fn field_opt_int(fields: &Dict, name: &str) -> Option<Option<i64>> {
    match fields.get(name) {
        None | Some(Value::Null) => Some(None),
        Some(value) => lax_int(value).ok().map(Some),
    }
}

/// A `list[str]` field: a JSON array of strings.
pub fn field_str_list(value: &Value) -> Option<Vec<String>> {
    let Value::List(items) = value else {
        return None;
    };
    items.iter().map(|item| lax_str(item).ok()).collect()
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone as _;

    use super::*;

    #[test]
    fn queries_are_read_as_starlette_reads_them() {
        assert_eq!(
            query_pairs("a=1&b&&c=x+y%2Bz&a=2"),
            vec![
                ("a".to_owned(), "1".to_owned()),
                ("b".to_owned(), String::new()),
                ("c".to_owned(), "x y+z".to_owned()),
                ("a".to_owned(), "2".to_owned()),
            ]
        );
        assert_eq!(query_get("a=1&a=2", "a").as_deref(), Some("2"));
        assert_eq!(int_query("limit=", "limit", 20), None);
        assert_eq!(int_query("", "limit", 20), Some(20));
        assert_eq!(int_query("limit=%205", "limit", 20), Some(5));
    }

    #[test]
    fn datetimes_are_written_as_pydantic_writes_them() {
        let at = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).single().expect("a time");
        assert_eq!(py_datetime(at), Value::from("2026-01-02T03:04:05Z"));
        let fraction = at + chrono::TimeDelta::microseconds(400_000);
        assert_eq!(py_datetime(fraction), Value::from("2026-01-02T03:04:05.400000Z"));
        assert_eq!(
            py_date(NaiveDate::from_ymd_opt(2026, 1, 2).expect("a date")),
            Value::from("2026-01-02")
        );
    }
}
