//! Python's float builtins the merge worker and the home worker share: `sum()` of floats,
//! `min()` / `max()` of two, `float(text)` and `format(x, '.Nf')`.
//!
//! Python origin: `CPython` 3.13 (`bltinmodule.c` `builtin_sum_impl`, `floatobject.c`). One copy
//! since WP16 (the two workers each had one; docs/rewrite/PLAN.md, "Cleanup notes").

use crate::pytext;

/// `sum(values)` of Python floats. `CPython` starts from the int `0`, so the first value is
/// added to `0.0` (`sum([-0.0])` is `0.0`); the values are added with Neumaier's compensation
/// (Python 3.12 and later), and the compensation is added at the end unless it is zero or not
/// finite. An empty input gives `0.0` where Python gives the int `0` (D145).
pub fn sum(values: impl IntoIterator<Item = f64>) -> f64 {
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
pub fn min(a: f64, b: f64) -> f64 {
    if b < a { b } else { a }
}

/// Python `max(a, b)`: `b` only when `b > a`.
pub fn max(a: f64, b: f64) -> f64 {
    if b > a { b } else { a }
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

/// Python `format(value, '.{precision}f')`.
pub fn fixed(value: f64, precision: usize) -> String {
    if value.is_nan() {
        return "nan".to_owned();
    }
    if value.is_infinite() {
        return if value > 0.0 { "inf" } else { "-inf" }.to_owned();
    }
    format!("{value:.precision$}")
}

#[cfg(test)]
mod tests {
    use super::{fixed, float_from_str, max, min, sum};

    #[test]
    fn sum_compensates_like_python() {
        // sum([0.1] * 10) is 1.0 since Python 3.12; a plain fold gives 0.9999999999999999.
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
    fn min_max_and_fixed_follow_python() {
        assert!(min(f64::NAN, 1.0).is_nan());
        assert!(max(f64::NAN, 1.0).is_nan());
        assert_eq!(min(2.0, 1.0).to_bits(), 1.0_f64.to_bits());
        assert_eq!(max(1.0, 2.0).to_bits(), 2.0_f64.to_bits());
        assert_eq!(fixed(0.25, 1), "0.2");
        assert_eq!(fixed(2.5, 0), "2");
        assert_eq!(fixed(f64::NAN, 1), "nan");
        assert_eq!(fixed(f64::NEG_INFINITY, 1), "-inf");
    }
}
