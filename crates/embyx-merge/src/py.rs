//! Small pieces of Python semantics the merge code depends on: `sum()` of floats, `float()`,
//! `str()` and `==` on JSON values, `format(x, '.Nf')`, `format(n, ',')`, `str.splitlines()`
//! and the text of an `OSError`.
//!
//! Python origin: `CPython` 3.13 (`bltinmodule.c` `builtin_sum_impl`, `floatobject.c`,
//! `Objects/exceptions.c`). Kept private to this crate; `embyx-clients` has its own
//! `PyOsError` for downloads (PLAN.md, "Cleanup notes").

use std::fmt::Write as _;
use std::io;
use std::path::Path;

use embyx_core::pyjson::{self, Value};
use embyx_core::pytext;

/// `sum(values)` of Python floats. `CPython` starts from the int `0`, so the first value is
/// added to `0.0` (`sum([-0.0])` is `0.0`); the values are added with Neumaier's compensation
/// (Python 3.12 and later), and the compensation is added at the end unless it is zero or not
/// finite. An empty input gives `0.0` where Python gives the int `0` (D145).
pub(crate) fn sum(values: impl IntoIterator<Item = f64>) -> f64 {
    let mut total = 0.0_f64;
    let mut compensation = 0.0_f64;
    for value in values {
        let next = total + value;
        if total.abs() >= value.abs() {
            compensation += (total - next) + value;
        } else {
            compensation += (value - next) + total;
        }
        total = next;
    }
    if compensation != 0.0 && compensation.is_finite() {
        total += compensation;
    }
    total
}

/// Python `min(a, b)`: `b` only when `b < a`, so a NaN `a` stays.
pub(crate) fn min(a: f64, b: f64) -> f64 {
    if b < a { b } else { a }
}

/// Python `float(text)` for a `str`: surrounding whitespace dropped, digits of any script,
/// single underscores between digits, `inf`, `infinity` and `nan` in any case. `None` where
/// Python raises `ValueError`.
pub(crate) fn float_from_str(text: &str) -> Option<f64> {
    let stripped = pytext::py_strip(text);
    let mut ascii = String::with_capacity(stripped.len());
    for ch in stripped.chars() {
        if ch.is_ascii() {
            ascii.push(ch);
        } else {
            let digit = pytext::py_decimal_value(ch)?;
            ascii.push(char::from_digit(digit, 10)?);
        }
    }
    let bytes = ascii.as_bytes();
    for (at, byte) in bytes.iter().enumerate() {
        if *byte == b'_' {
            let before = at.checked_sub(1).and_then(|before| bytes.get(before));
            let after = bytes.get(at + 1);
            if !before.is_some_and(u8::is_ascii_digit) || !after.is_some_and(u8::is_ascii_digit) {
                return None;
            }
        }
    }
    let cleaned: String = ascii.chars().filter(|&ch| ch != '_').collect();
    let unsigned = cleaned.strip_prefix(['+', '-']).unwrap_or(&cleaned);
    let lower = unsigned.to_ascii_lowercase();
    if matches!(lower.as_str(), "inf" | "infinity" | "nan") {
        return cleaned.parse().ok();
    }
    // Python's grammar: digits with an optional fraction, or a fraction alone, then an
    // optional exponent. Rust's parser takes the same set, minus the words handled above.
    let mantissa_end = unsigned.find(['e', 'E']).unwrap_or(unsigned.len());
    let (mantissa, exponent) = unsigned.split_at(mantissa_end);
    let mantissa_ok = {
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
        digits(whole) && digits(fraction) && (!whole.is_empty() || !fraction.is_empty())
    };
    let exponent_ok = exponent.is_empty() || {
        let digits = exponent[1..].strip_prefix(['+', '-']).unwrap_or(&exponent[1..]);
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    };
    if !mantissa_ok || !exponent_ok {
        return None;
    }
    cleaned.parse().ok()
}

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

/// Python `format(value, '.{precision}f')`.
pub(crate) fn fixed(value: f64, precision: usize) -> String {
    if value.is_nan() {
        return "nan".to_owned();
    }
    if value.is_infinite() {
        return if value > 0.0 { "inf" } else { "-inf" }.to_owned();
    }
    format!("{value:.precision$}")
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

/// Python `str.splitlines()`: lines end at `\n`, `\r`, `\r\n`, `\v`, `\f`, `\x1c`-`\x1e`,
/// `\x85`, U+2028 and U+2029.
pub(crate) fn splitlines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut chars = text.char_indices().peekable();
    while let Some((at, ch)) = chars.next() {
        let ends = matches!(
            ch,
            '\n' | '\r' | '\u{0b}' | '\u{0c}' | '\u{1c}' | '\u{1d}' | '\u{1e}' | '\u{85}' | '\u{2028}' | '\u{2029}'
        );
        if !ends {
            continue;
        }
        lines.push(&text[start..at]);
        start = at + ch.len_utf8();
        if ch == '\r' && chars.peek().is_some_and(|&(_, next)| next == '\n') {
            chars.next();
            start += 1;
        }
    }
    if start < text.len() {
        lines.push(&text[start..]);
    }
    lines
}

/// `str(path)` quoted as Python's `repr` quotes it.
pub(crate) fn repr_path(path: &Path) -> String {
    pytext::py_repr(&path.to_string_lossy())
}

/// `str(OSError)`: `[Errno N] strerror: 'filename'`, with ` -> 'filename2'` for a rename.
pub(crate) fn os_error(error: &io::Error, filename: Option<&Path>, filename2: Option<&Path>) -> String {
    let Some(code) = error.raw_os_error() else {
        return error.to_string();
    };
    let text = io::Error::from_raw_os_error(code).to_string();
    let strerror = text.strip_suffix(&format!(" (os error {code})")).unwrap_or(&text);
    let mut out = format!("[Errno {code}] {strerror}");
    if let Some(filename) = filename {
        let _ = write!(out, ": {}", repr_path(filename));
        if let Some(filename2) = filename2 {
            let _ = write!(out, " -> {}", repr_path(filename2));
        }
    }
    out
}

/// `OSError.strerror`, or the whole text when the error has no errno.
pub(crate) fn strerror(error: &io::Error) -> String {
    let Some(code) = error.raw_os_error() else {
        return error.to_string();
    };
    let text = io::Error::from_raw_os_error(code).to_string();
    text.strip_suffix(&format!(" (os error {code})"))
        .unwrap_or(&text)
        .to_owned()
}

#[cfg(test)]
mod tests {
    use embyx_core::pyjson::Value;

    use super::{eq, fixed, float_from_str, floordiv, splitlines, sum, thousands};

    #[test]
    fn sum_compensates_like_python() {
        // sum([0.1] * 10) is 1.0 since Python 3.12; a plain fold gives 0.9999999999999999.
        assert!((sum([0.1; 10]) - 1.0).abs() < f64::EPSILON);
        assert_eq!(sum([0.1; 10]).to_bits(), 1.0_f64.to_bits());
        assert_eq!(sum([1e100, 1.0, -1e100, 1.0]).to_bits(), 2.0_f64.to_bits());
        assert_eq!(sum(std::iter::empty()).to_bits(), 0.0_f64.to_bits());
        assert!(sum([f64::INFINITY, 1.0]).is_infinite());
        // CPython starts from the int 0: a negative zero does not survive.
        assert_eq!(sum([-0.0, -0.0]).to_bits(), 0.0_f64.to_bits());
        assert_eq!(sum([-0.0]).to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn float_reads_what_python_reads() {
        assert_eq!(float_from_str(" 2.5\n"), Some(2.5));
        assert_eq!(float_from_str("1_0.5"), Some(10.5));
        assert_eq!(float_from_str("١٢"), Some(12.0));
        assert_eq!(float_from_str("-Infinity"), Some(f64::NEG_INFINITY));
        assert!(float_from_str("nan").is_some_and(f64::is_nan));
        assert_eq!(float_from_str(".5e1"), Some(5.0));
        for bad in ["", "1__0", "_1", "1_", "1e", "e5", ".", "0x10", "1.5.2", "infinit"] {
            assert_eq!(float_from_str(bad), None, "{bad:?}");
        }
    }

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
        assert_eq!(fixed(0.25, 1), "0.2");
        assert_eq!(fixed(2.5, 0), "2");
        assert_eq!(fixed(f64::NAN, 1), "nan");
        assert_eq!(thousands(1_234_567), "1,234,567");
        assert_eq!(thousands(-1000), "-1,000");
        assert_eq!(thousands(999), "999");
        assert_eq!(floordiv(-7, 2), -4);
        assert_eq!(floordiv(7, 2), 3);
        assert_eq!(splitlines("a\r\nb\rc\x0bd\n"), ["a", "b", "c", "d"]);
        assert_eq!(splitlines("a\n\nb"), ["a", "", "b"]);
    }
}
