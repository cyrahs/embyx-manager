//! The few Python and asyncpg behaviours the service's handlers lean on: `==` against a text,
//! membership in a set of texts, `str.upper()`, `text.rsplit('/', 1)` unpacked into two
//! names, and the `DataError` asyncpg raises for an argument of the wrong type.

use embyx_clients::pyvalue::{self, Raised};
use embyx_core::pyjson::Value;

use super::StepError;

/// `value == text`: only a string equals a string.
pub(crate) fn is_str(value: &Value, text: &str) -> bool {
    matches!(value, Value::Str(own) if own == text)
}

/// `value in {texts...}`: a list or dict cannot be hashed, so the lookup raises.
pub(crate) fn state_in(value: &Value, texts: &[&str]) -> Result<bool, StepError> {
    match value {
        Value::List(_) | Value::Dict(_) => Err(StepError::Raised(Raised::new(
            "TypeError",
            format!("unhashable type: '{}'", pyvalue::type_name(value)),
        ))),
        Value::Str(own) => Ok(texts.contains(&own.as_str())),
        _ => Ok(false),
    }
}

/// `str.upper()`.
pub(crate) fn py_upper(text: &str) -> String {
    text.to_uppercase()
}

/// `int(value)` bound as a `bigint` column.
pub(crate) fn int_field(value: &Value, index: usize) -> Result<i64, StepError> {
    let number = pyvalue::py_int(value)?;
    int8_argument(number, index)
}

/// asyncpg's `DataError` for query argument `$index`.
fn data_error(index: usize, value: &str, reason: &str) -> StepError {
    StepError::Raised(Raised::new(
        "DataError",
        format!("invalid input for query argument ${index}: {value} ({reason})"),
    ))
}

/// A Python value bound to a `text` column: `None` or a `str`; anything else is refused the
/// way asyncpg refuses it.
pub(crate) fn text_argument(value: &Value, index: usize) -> Result<Option<String>, StepError> {
    match value {
        Value::Null => Ok(None),
        Value::Str(text) => Ok(Some(text.clone())),
        other => Err(data_error(
            index,
            &pyvalue::repr(other),
            &format!("expected str, got {}", pyvalue::type_name(other)),
        )),
    }
}

/// A Python value bound to a `double precision` column: `None`, a bool, an int or a float.
pub(crate) fn float_argument(value: &Value, index: usize) -> Result<Option<f64>, StepError> {
    match value {
        Value::Null => Ok(None),
        Value::Bool(flag) => Ok(Some(if *flag { 1.0 } else { 0.0 })),
        #[expect(clippy::cast_precision_loss, reason = "float(int) rounds the same way")]
        Value::Int(number) => Ok(Some(*number as f64)),
        Value::Float(number) => Ok(Some(*number)),
        other => Err(data_error(
            index,
            &pyvalue::repr(other),
            &format!("must be real number, not {}", pyvalue::type_name(other)),
        )),
    }
}

/// A Python int bound to a `bigint` column.
pub(crate) fn int8_argument(number: i128, index: usize) -> Result<i64, StepError> {
    i64::try_from(number).map_err(|_| data_error(index, &number.to_string(), "value out of int64 range"))
}

/// `head, tail = text.rsplit('/', 1)`.
pub(crate) fn rsplit_once_slash(text: &str) -> Result<(&str, &str), StepError> {
    text.rsplit_once('/').ok_or_else(|| {
        StepError::Raised(Raised::new(
            "ValueError",
            "not enough values to unpack (expected 2, got 1)",
        ))
    })
}

/// `f'{value}'` of an optional column: `None` when it is unset.
pub(crate) fn opt_display<T: std::fmt::Display>(value: Option<&T>) -> String {
    value.map_or_else(|| "None".to_owned(), ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn membership_of_an_unhashable_value_raises() {
        let error = state_in(&Value::List(vec![]), &["done"]).unwrap_err();
        assert_eq!(error.class_name(), "TypeError");
        assert_eq!(error.notice(), "unhashable type: 'list'");
        assert!(state_in(&Value::from("done"), &["done", "failed"]).unwrap());
        assert!(!state_in(&Value::Int(1), &["done"]).unwrap());
    }

    #[test]
    fn rsplit_needs_a_slash() {
        assert_eq!(rsplit_once_slash("/a/b").unwrap(), ("/a", "b"));
        assert_eq!(rsplit_once_slash("/b").unwrap(), ("", "b"));
        assert!(rsplit_once_slash("b").is_err());
    }
}
