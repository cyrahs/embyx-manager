//! `humanfriendly.parse_size` (humanfriendly 10.0), as the magnet pickers read listed sizes.
//!
//! Python origin: `humanfriendly.parse_size(text)` with `binary=False`, called by
//! `clients/javbus.py`, `clients/sukebei.py` and `clients/rss_magnet.py`. The text is split
//! into numbers (`\d+(?:\.\d+)?`, Unicode digits) and the strings between them; one number is
//! bytes, a number and a unit is scaled by the first unit whose binary symbol or name
//! (`KiB`, `kibibyte`) is the unit, or whose decimal symbol or name is, or whose initial the
//! unit starts with (`k`, `m`, `g`, ...), plurals stripped of their `s`. Sizes are `i128`;
//! one past it saturates where Python's integers grow (D28).

use embyx_core::pytext::{py_decimal_value, py_isdigit, py_strip};

/// Why a size could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SizeError {
    /// `humanfriendly.InvalidSize`.
    #[error("Failed to parse size! (input {0} was tokenized as {1})")]
    Invalid(String, String),
    /// The `ValueError` `int()` raises for a token of digits that are not decimal (`²`),
    /// which `parse_size` lets through.
    #[error("invalid literal for int() with base 10: {0}")]
    NotDecimal(String),
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Int(i128),
    Float(f64),
    Text(String),
}

impl Token {
    fn repr(&self) -> String {
        match self {
            Self::Int(value) => value.to_string(),
            Self::Float(value) => embyx_core::pyjson::float_repr(*value),
            Self::Text(text) => embyx_core::pytext::py_repr(text),
        }
    }
}

/// (decimal symbol, decimal name, binary symbol, binary name, power).
const UNITS: [(&str, &str, &str, &str, u32); 8] = [
    ("KB", "kilobyte", "KiB", "kibibyte", 1),
    ("MB", "megabyte", "MiB", "mebibyte", 2),
    ("GB", "gigabyte", "GiB", "gibibyte", 3),
    ("TB", "terabyte", "TiB", "tebibyte", 4),
    ("PB", "petabyte", "PiB", "pebibyte", 5),
    ("EB", "exabyte", "EiB", "exbibyte", 6),
    ("ZB", "zettabyte", "ZiB", "zebibyte", 7),
    ("YB", "yottabyte", "YiB", "yobibyte", 8),
];

/// `humanfriendly.text.tokenize`.
fn tokenize(text: &str) -> Result<Vec<Token>, SizeError> {
    let mut pieces: Vec<(bool, String)> = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    let mut index = 0;
    let mut plain = String::new();
    while index < chars.len() {
        if py_decimal_value(chars[index]).is_some() {
            let start = index;
            while index < chars.len() && py_decimal_value(chars[index]).is_some() {
                index += 1;
            }
            if index + 1 < chars.len() && chars[index] == '.' && py_decimal_value(chars[index + 1]).is_some() {
                index += 1;
                while index < chars.len() && py_decimal_value(chars[index]).is_some() {
                    index += 1;
                }
            }
            pieces.push((false, std::mem::take(&mut plain)));
            pieces.push((true, chars[start..index].iter().collect()));
        } else {
            plain.push(chars[index]);
            index += 1;
        }
    }
    pieces.push((false, plain));
    let mut tokens = Vec::new();
    for (is_number, piece) in pieces {
        let token = py_strip(&piece);
        if is_number && token.contains('.') {
            let ascii: String = token
                .chars()
                .map(|ch| py_decimal_value(ch).map_or(ch, |digit| char::from_digit(digit, 10).unwrap_or(ch)))
                .collect();
            tokens.push(Token::Float(ascii.parse().unwrap_or(f64::INFINITY)));
        } else if !token.is_empty() && py_isdigit(token) {
            // A digit run, or digits `\d` does not cover (`²`), which `int()` refuses.
            let mut value: i128 = 0;
            for ch in token.chars() {
                let Some(digit) = py_decimal_value(ch) else {
                    return Err(SizeError::NotDecimal(embyx_core::pytext::py_repr(token)));
                };
                value = value.saturating_mul(10).saturating_add(i128::from(digit));
            }
            tokens.push(Token::Int(value));
        } else if !token.is_empty() {
            tokens.push(Token::Text(token.to_owned()));
        }
    }
    Ok(tokens)
}

fn scaled(number: &Token, divider: i128) -> i128 {
    match number {
        Token::Int(value) => value.saturating_mul(divider),
        // `int(float * int)`: the product in floating point, truncated; `as` saturates.
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_precision_loss,
            reason = "Python's int(float)"
        )]
        Token::Float(value) => (value * divider as f64) as i128,
        Token::Text(_) => 0,
    }
}

/// `humanfriendly.parse_size(text)`.
pub fn parse_size(text: &str) -> Result<i128, SizeError> {
    let tokens = tokenize(text)?;
    if let Some(number @ (Token::Int(_) | Token::Float(_))) = tokens.first() {
        let unit = match tokens.get(1) {
            Some(Token::Text(unit)) if tokens.len() == 2 => unit.to_lowercase(),
            _ => String::new(),
        };
        if tokens.len() == 1 || unit.starts_with('b') {
            return Ok(scaled(number, 1));
        }
        if !unit.is_empty() {
            let unit = unit.trim_end_matches('s');
            for (decimal_symbol, decimal_name, binary_symbol, binary_name, power) in UNITS {
                if unit == binary_symbol.to_lowercase() || unit == binary_name {
                    return Ok(scaled(number, 1024_i128.pow(power)));
                }
                let initial = decimal_symbol[..1].to_lowercase();
                if unit == decimal_symbol.to_lowercase() || unit == decimal_name || unit.starts_with(&initial) {
                    return Ok(scaled(number, 1000_i128.pow(power)));
                }
            }
        }
    }
    let rendered = format!("[{}]", tokens.iter().map(Token::repr).collect::<Vec<_>>().join(", "));
    Err(SizeError::Invalid(embyx_core::pytext::py_repr(text), rendered))
}

#[cfg(test)]
mod tests {
    use embyx_core::golden;

    use super::*;

    // `goldens/clients/size.json`: what `humanfriendly.parse_size` makes of each text.
    #[test]
    fn sizes_match_humanfriendly() {
        let document = golden::load("clients/size.json");
        for case in golden::cases(&document) {
            let text = golden::string(golden::get(case, "text"));
            let got = parse_size(&text);
            if let Some(value) = case.get("value") {
                assert_eq!(got, Ok(golden::int(value)), "{text:?}");
            } else {
                let error = got.expect_err(&text);
                let class = match error {
                    SizeError::Invalid(..) => "InvalidSize",
                    SizeError::NotDecimal(_) => "ValueError",
                };
                assert_eq!(class, golden::string(golden::get(case, "error")), "{text:?}");
                assert_eq!(
                    error.to_string(),
                    golden::string(golden::get(case, "message")),
                    "{text:?}"
                );
            }
        }
    }

    #[test]
    fn a_size_past_i128_saturates() {
        // D28: Python's integers grow; these stop at i128::MAX.
        assert_eq!(parse_size("99999999999999999999999 YiB"), Ok(i128::MAX));
        assert_eq!(parse_size("999999999999999999999999999999999999999999"), Ok(i128::MAX));
    }
}
