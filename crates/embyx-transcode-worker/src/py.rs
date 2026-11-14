//! Python semantics the worker needs: `float()`, `int()`, `str()`, `repr()` and truth of a
//! JSON or TOML value, `round()`, `sum()` of floats and the format specs the console uses.
//!
//! Private copies: `embyx-merge`'s `py` module has the same `float`/`int`/`sum`/`fixed`
//! helpers but keeps them crate-private, and this crate builds without it (PLAN.md,
//! "Cleanup notes": WP16 moves what two crates use into `embyx-core`).

use embyx_core::pyjson::{self, Key, Value};
use embyx_core::pytext;

/// A Python exception the worker reports by its text: `ValueError: ...`, `TypeError: ...`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct PyError {
    /// The exception class (`ValueError`, `TypeError`, `KeyError`, `OverflowError`).
    pub class: &'static str,
    /// `str(exc)`.
    pub message: String,
}

impl PyError {
    pub(crate) fn new(class: &'static str, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
        }
    }

    /// `KeyError: 'name'`: `str()` of a `KeyError` is the key's `repr`.
    pub(crate) fn key(name: &str) -> Self {
        Self::new("KeyError", pytext::py_repr(name))
    }
}

/// Python's `type(value).__name__`.
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

/// Python `float(text)` for a `str`: surrounding whitespace dropped, digits of any script,
/// single underscores between digits, `inf`, `infinity` and `nan` in any case. `None` where
/// Python raises `ValueError`.
pub fn float_from_str(text: &str) -> Option<f64> {
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

/// Python `float(value)`.
pub(crate) fn float(value: &Value) -> Result<f64, PyError> {
    match value {
        Value::Float(number) => Ok(*number),
        #[allow(clippy::cast_precision_loss, reason = "Python's float(int) rounds the same way")]
        Value::Int(number) => Ok(*number as f64),
        Value::Bool(flag) => Ok(if *flag { 1.0 } else { 0.0 }),
        Value::Str(text) => float_from_str(text).ok_or_else(|| {
            PyError::new(
                "ValueError",
                format!("could not convert string to float: {}", pytext::py_repr(text)),
            )
        }),
        other => Err(PyError::new(
            "TypeError",
            format!(
                "float() argument must be a string or a real number, not '{}'",
                type_name(other)
            ),
        )),
    }
}

/// Python `int(value)`.
pub(crate) fn int(value: &Value) -> Result<i128, PyError> {
    match value {
        Value::Int(number) => Ok(*number),
        Value::Bool(flag) => Ok(i128::from(*flag)),
        Value::Float(number) => int_of_float(*number),
        Value::Str(text) => pytext::py_int(text).map_err(|message| PyError::new("ValueError", message)),
        other => Err(PyError::new(
            "TypeError",
            format!(
                "int() argument must be a string, a bytes-like object or a real number, not '{}'",
                type_name(other)
            ),
        )),
    }
}

/// Python `int(x)` for a float: truncated toward zero.
pub(crate) fn int_of_float(number: f64) -> Result<i128, PyError> {
    if number.is_nan() {
        Err(PyError::new("ValueError", "cannot convert float NaN to integer"))
    } else if number.is_infinite() {
        Err(PyError::new(
            "OverflowError",
            "cannot convert float infinity to integer",
        ))
    } else {
        #[allow(clippy::cast_possible_truncation, reason = "int() truncates toward zero")]
        Ok(number.trunc() as i128)
    }
}

/// Python `round(x)` for a float: the nearest int, ties to even.
pub(crate) fn round(number: f64) -> Result<i128, PyError> {
    if number.is_nan() {
        return Err(PyError::new("ValueError", "cannot convert float NaN to integer"));
    }
    if number.is_infinite() {
        return Err(PyError::new(
            "OverflowError",
            "cannot convert float infinity to integer",
        ));
    }
    int_of_float(number.round_ties_even())
}

/// Python's truth of a value: `bool(value)`.
pub(crate) fn truthy(value: &Value) -> bool {
    value.is_truthy()
}

/// Python `str(value)`; containers as Python writes them (their items by `repr`).
pub(crate) fn str_of(value: &Value) -> String {
    match value {
        Value::Str(text) => text.clone(),
        other => repr(other),
    }
}

/// Python `repr(value)` for a JSON or TOML value: `{'code': 'x', 'next_chunk': 1}`.
pub(crate) fn repr(value: &Value) -> String {
    match value {
        Value::Null => "None".to_owned(),
        Value::Bool(flag) => if *flag { "True" } else { "False" }.to_owned(),
        Value::Int(number) => number.to_string(),
        Value::Float(number) => pyjson::float_repr(*number),
        Value::Str(text) => pytext::py_repr(text),
        Value::List(items) => format!("[{}]", items.iter().map(repr).collect::<Vec<_>>().join(", ")),
        Value::Dict(dict) => {
            let items: Vec<String> = dict
                .iter()
                .map(|(key, value)| format!("{}: {}", key_repr(key), repr(value)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

fn key_repr(key: &Key) -> String {
    match key {
        Key::Str(text) => pytext::py_repr(text),
        Key::Int(number) => number.to_string(),
        Key::Float(number) => pyjson::float_repr(*number),
        Key::Bool(flag) => if *flag { "True" } else { "False" }.to_owned(),
        Key::Null => "None".to_owned(),
    }
}

/// Python's `sum()` of floats (3.12 and later): Neumaier's compensated summation.
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

/// Python `min(a, b)`: `b` only when `b < a`.
pub(crate) fn min(a: f64, b: f64) -> f64 {
    if b < a { b } else { a }
}

/// Python `max(a, b)`: `b` only when `b > a`.
pub(crate) fn max(a: f64, b: f64) -> f64 {
    if b > a { b } else { a }
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

/// Python `format(value, '.{precision}%')`: the value times 100 in fixed notation, then `%`.
pub(crate) fn percent(value: f64, precision: usize) -> String {
    format!("{}%", fixed(value * 100.0, precision))
}

/// Python `format(value, 'g')`: six significant digits, trailing zeros dropped, scientific
/// notation when the exponent is below -4 or at least 6.
pub(crate) fn general(value: f64) -> String {
    const PRECISION: i32 = 6;
    if value.is_nan() {
        return "nan".to_owned();
    }
    if value.is_infinite() {
        return if value > 0.0 { "inf" } else { "-inf" }.to_owned();
    }
    if value == 0.0 {
        return if value.is_sign_negative() { "-0" } else { "0" }.to_owned();
    }
    // The exponent after rounding to six significant digits decides the notation.
    let scientific = format!("{value:.5e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    if (-4..PRECISION).contains(&exponent) {
        let decimals = usize::try_from(PRECISION - 1 - exponent).unwrap_or(0);
        strip_zeros(&format!("{value:.decimals$}")).to_owned()
    } else {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{}e{sign}{:02}", strip_zeros(mantissa), exponent.abs())
    }
}

fn strip_zeros(text: &str) -> &str {
    if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.')
    } else {
        text
    }
}

/// `shutil.rmtree(path, ignore_errors=True)`: a symbolic link is refused (and the refusal
/// ignored), so the link and what it points at are both left.
pub(crate) fn rmtree(path: &std::path::Path) {
    if std::fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_symlink()) {
        return;
    }
    let _ = std::fs::remove_dir_all(path);
}

#[cfg(test)]
mod tests {
    use embyx_core::pyjson::{Dict, Value};

    use super::{general, percent, repr, round};

    #[test]
    fn general_format_matches_python() {
        let cases = [
            (30.0, "30"),
            (25.5, "25.5"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (123_456.0, "123456"),
            (1_234_567.0, "1.23457e+06"),
            (999_999.5, "1e+06"),
            (2.5, "2.5"),
            (-7.25, "-7.25"),
            (1e100, "1e+100"),
        ];
        for (value, want) in cases {
            assert_eq!(general(value), want, "{value}");
        }
    }

    #[test]
    fn round_and_percent_follow_python() {
        assert_eq!(round(2.5).expect("round"), 2);
        assert_eq!(round(3.5).expect("round"), 4);
        assert_eq!(round(-0.5).expect("round"), 0);
        assert!(round(f64::NAN).is_err());
        assert_eq!(percent(0.125, 0), "12%");
        assert_eq!(percent(0.4567, 1), "45.7%");
    }

    #[test]
    fn repr_writes_python_containers() {
        let mut dict = Dict::new();
        dict.insert("code", Value::Str("transcode_chunk_out_of_order".into()));
        dict.insert("next_chunk", Value::Int(1));
        dict.insert("resume_at", Value::Float(20.0));
        dict.insert("flags", Value::List(vec![Value::Bool(true), Value::Null]));
        assert_eq!(
            repr(&Value::Dict(dict)),
            "{'code': 'transcode_chunk_out_of_order', 'next_chunk': 1, 'resume_at': 20.0, 'flags': [True, None]}"
        );
    }

    // `shutil.rmtree(link, ignore_errors=True)` leaves a link to a folder alone.
    #[cfg(unix)]
    #[test]
    fn a_link_to_a_folder_is_left_alone() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let target = tmp.path().join("target");
        std::fs::create_dir(&target).expect("target");
        std::fs::write(target.join("kept"), b"x").expect("file");
        let link = tmp.path().join("link");
        std::os::unix::fs::symlink(&target, &link).expect("link");
        super::rmtree(&link);
        assert!(link.exists() && target.join("kept").exists());
        super::rmtree(&target);
        assert!(!target.exists());
    }
}
