//! Small pieces of Python semantics the merge code depends on: `sum()` of floats, `float()`,
//! `str()` and `==` on JSON values, `format(x, '.Nf')`, `format(n, ',')`, `str.splitlines()`
//! and the text of an `OSError`.
//!
//! Python origin: `CPython` 3.13 (`floatobject.c`, `Objects/abstract.c`). The float builtins,
//! `splitlines` and the `OSError` text live in `embyx-core` (`pyfloat`, `pytext`, `pyos`)
//! since WP16 and are re-exported here under the names the worker uses.

pub(crate) use embyx_core::pyfloat::{fixed, float_from_str, min, sum};
pub(crate) use embyx_core::pyos::{os_error_text as os_error, strerror};
pub(crate) use embyx_core::pytext::splitlines;

use embyx_core::pyjson::{self, Value};
use embyx_core::pytext;

/// An exception Python would raise and the merge worker does not catch: the class and text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PyException {
    pub class: &'static str,
    pub message: String,
}

impl PyException {
    pub(crate) fn new(class: &'static str, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
        }
    }
}

/// Python's `type(value).__name__` for a JSON value.
pub(crate) fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::Int(_) => "int",
        Value::Float(_) => "float",
        Value::Str(_) => "str",
        Value::List(_) => "list",
        Value::Dict(_) => "dict",
    }
}

/// Python `float(value)` for a JSON value.
pub(crate) fn float(value: &Value) -> Result<f64, PyException> {
    match value {
        Value::Float(number) => Ok(*number),
        #[allow(clippy::cast_precision_loss, reason = "Python's float(int) rounds the same way")]
        Value::Int(number) => Ok(*number as f64),
        Value::Bool(flag) => Ok(if *flag { 1.0 } else { 0.0 }),
        Value::Str(text) => float_from_str(text).ok_or_else(|| {
            PyException::new(
                "ValueError",
                format!("could not convert string to float: {}", pytext::py_repr(text)),
            )
        }),
        other => Err(PyException::new(
            "TypeError",
            format!(
                "float() argument must be a string or a real number, not '{}'",
                type_name(other)
            ),
        )),
    }
}

/// Python `int(value)` for a JSON value.
pub(crate) fn int(value: &Value) -> Result<i128, PyException> {
    match value {
        Value::Int(number) => Ok(*number),
        Value::Bool(flag) => Ok(i128::from(*flag)),
        Value::Float(number) => {
            if number.is_nan() {
                Err(PyException::new("ValueError", "cannot convert float NaN to integer"))
            } else if number.is_infinite() {
                Err(PyException::new(
                    "OverflowError",
                    "cannot convert float infinity to integer",
                ))
            } else {
                #[allow(clippy::cast_possible_truncation, reason = "int() truncates toward zero")]
                Ok(number.trunc() as i128)
            }
        }
        Value::Str(text) => pytext::py_int(text).map_err(|message| PyException::new("ValueError", message)),
        other => Err(PyException::new(
            "TypeError",
            format!(
                "int() argument must be a string, a bytes-like object or a real number, not '{}'",
                type_name(other)
            ),
        )),
    }
}

/// Python `int(x)` for a float.
pub(crate) fn int_of_float(number: f64) -> Result<i128, PyException> {
    int(&Value::Float(number))
}

/// Python `str(value)` for a JSON value (`repr` inside containers is close enough: the
/// callers only test the text with `isdigit()` or compare it).
pub(crate) fn str_of(value: &Value) -> String {
    match value {
        Value::Null => "None".to_owned(),
        Value::Bool(flag) => if *flag { "True" } else { "False" }.to_owned(),
        Value::Int(number) => number.to_string(),
        Value::Float(number) => pyjson::float_repr(*number),
        Value::Str(text) => text.clone(),
        Value::List(_) | Value::Dict(_) => {
            pyjson::dumps(value, &pyjson::DumpOptions::python_default()).unwrap_or_default()
        }
    }
}

/// Python `a == b` for JSON values: `1 == 1.0 == True`, NaN unequal to itself.
pub(crate) fn eq(a: &Value, b: &Value) -> bool {
    fn number(value: &Value) -> Option<Number> {
        match value {
            Value::Bool(flag) => Some(Number::Int(i128::from(*flag))),
            Value::Int(number) => Some(Number::Int(*number)),
            Value::Float(number) => Some(Number::Float(*number)),
            _ => None,
        }
    }
    if let (Some(left), Some(right)) = (number(a), number(b)) {
        return match (left, right) {
            (Number::Int(x), Number::Int(y)) => x == y,
            (Number::Float(x), Number::Float(y)) => x == y,
            (Number::Int(x), Number::Float(y)) | (Number::Float(y), Number::Int(x)) => int_eq_float(x, y),
        };
    }
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Str(x), Value::Str(y)) => x == y,
        (Value::List(x), Value::List(y)) => x.len() == y.len() && x.iter().zip(y).all(|(x, y)| eq(x, y)),
        (Value::Dict(x), Value::Dict(y)) => {
            x.len() == y.len()
                && x.iter().all(|(key, value)| {
                    key.as_str()
                        .and_then(|key| y.get(key))
                        .is_some_and(|other| eq(value, other))
                })
        }
        _ => false,
    }
}

enum Number {
    Int(i128),
    Float(f64),
}

fn int_eq_float(int: i128, float: f64) -> bool {
    if !float.is_finite() || float.fract() != 0.0 {
        return false;
    }
    #[allow(clippy::cast_precision_loss, reason = "compared both ways below")]
    let as_float = int as f64;
    #[allow(clippy::cast_possible_truncation, reason = "only equal when it round-trips")]
    let back = float as i128;
    as_float.to_bits() == float.to_bits() && back == int
}

/// Python `a // b` on ints (floor division).
pub(crate) fn floordiv(a: i128, b: i128) -> i128 {
    a.div_euclid(b) - i128::from(b < 0 && a.rem_euclid(b) != 0)
}

/// Python `a / b` on ints. Exact for operands up to 2**53, as every caller's are.
#[allow(clippy::cast_precision_loss, reason = "the operands stay below 2**53")]
pub(crate) fn truediv(a: i128, b: i128) -> f64 {
    a as f64 / b as f64
}

/// Python `format(value, ',')` for an int.
pub(crate) fn thousands(value: i128) -> String {
    let digits = value.unsigned_abs().to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3 + 1);
    if value < 0 {
        out.push('-');
    }
    for (at, ch) in digits.chars().enumerate() {
        if at > 0 && (digits.len() - at).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use embyx_core::pyjson::Value;

    use super::{eq, floordiv, splitlines, thousands};

    #[test]
    fn equality_follows_python() {
        assert!(eq(&Value::Int(1), &Value::Float(1.0)));
        assert!(eq(&Value::Bool(true), &Value::Int(1)));
        assert!(!eq(&Value::Float(f64::NAN), &Value::Float(f64::NAN)));
        assert!(!eq(&Value::Str("1".into()), &Value::Int(1)));
        assert!(eq(&Value::Null, &Value::Null));
    }

    #[test]
    fn formats_follow_python() {
        assert_eq!(thousands(1_234_567), "1,234,567");
        assert_eq!(thousands(-1000), "-1,000");
        assert_eq!(thousands(999), "999");
        assert_eq!(floordiv(-7, 2), -4);
        assert_eq!(floordiv(7, 2), 3);
        assert_eq!(splitlines("a\r\nb\rc\x0bd\n"), ["a", "b", "c", "d"]);
        assert_eq!(splitlines("a\n\nb"), ["a", "", "b"]);
    }
}
