//! Pydantic's lax-mode validation for the field types the config sections use.
//!
//! Python origin: pydantic 2 / pydantic-core as locked at the baseline (`uv.lock`), which
//! `config/models.py` builds its sections on. A section is read back from the database with
//! `model_validate_json` and edited through `model_validate` over a merge of JSON-decoded
//! values, so only JSON-shaped input matters: `null`, booleans, numbers, strings, lists and
//! objects. For those this module accepts, coerces and refuses exactly what pydantic does,
//! and reports the same error `type`, `loc` and `msg` (`goldens/config/`). The one deliberate
//! difference: integers outside `i64` are refused (DIVERGENCES.md D10).

use std::fmt;

use crate::pyjson::{Dict, Key, Value};
use crate::pytext;

/// Which pydantic entry point is emulated: `model_validate` over Python values, or
/// `model_validate_json` over text. They differ only in a few messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Python,
    Json,
}

/// One entry of `ValidationError.errors()`: `type`, `loc` and `msg`.
#[derive(Debug, Clone, PartialEq)]
pub struct ErrorDetail {
    pub kind: &'static str,
    pub loc: Vec<Value>,
    pub msg: String,
}

/// `pydantic.ValidationError` for one model; Python callers catch it as `ValueError`.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidationError {
    /// The model's class name (`RssConfig`), the title pydantic prints.
    pub title: &'static str,
    pub errors: Vec<ErrorDetail>,
}

impl ValidationError {
    /// The Python exception class.
    pub const CLASS: &'static str = "ValidationError";
}

fn loc_text(loc: &[Value]) -> String {
    loc.iter()
        .map(|item| match item {
            Value::Str(text) => text.clone(),
            Value::Int(index) => index.to_string(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Like `str(ValidationError)`, without the input value and documentation link pydantic
/// adds to each error (this text only reaches logs; DIVERGENCES.md D11).
impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.errors.len();
        write!(
            f,
            "{count} validation error{} for {}",
            if count == 1 { "" } else { "s" },
            self.title
        )?;
        for error in &self.errors {
            let loc = loc_text(&error.loc);
            if loc.is_empty() {
                write!(f, "\n  {} [type={}]", error.msg, error.kind)?;
            } else {
                write!(f, "\n{loc}\n  {} [type={}]", error.msg, error.kind)?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for ValidationError {}

/// A refusal before its location is known: pydantic's `type` and `msg`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub kind: &'static str,
    pub msg: String,
}

impl Refusal {
    fn new(kind: &'static str, msg: &str) -> Self {
        Self {
            kind,
            msg: msg.to_owned(),
        }
    }

    /// A `ValueError` raised by a validator: `Value error, <message>`.
    pub fn value_error(message: &str) -> Self {
        Self {
            kind: "value_error",
            msg: format!("Value error, {message}"),
        }
    }
}

const INT_TYPE: &str = "Input should be a valid integer";
const INT_PARSING: &str = "Input should be a valid integer, unable to parse string as an integer";
const INT_FROM_FLOAT: &str = "Input should be a valid integer, got a number with a fractional part";
const INT_PARSING_SIZE: &str = "Unable to parse input string as an integer, exceeded maximum size";
const FINITE_NUMBER: &str = "Input should be a finite number";
const FLOAT_TYPE: &str = "Input should be a valid number";
const FLOAT_PARSING: &str = "Input should be a valid number, unable to parse string as a number";
const BOOL_TYPE: &str = "Input should be a valid boolean";
const BOOL_PARSING: &str = "Input should be a valid boolean, unable to interpret input";
const STRING_TYPE: &str = "Input should be a valid string";

/// `str`: strings only.
pub fn lax_str(value: &Value) -> Result<String, Refusal> {
    match value {
        Value::Str(text) => Ok(text.clone()),
        _ => Err(Refusal::new("string_type", STRING_TYPE)),
    }
}

/// `bool`: booleans, 0 and 1 (as integers or integral floats), and the words pydantic
/// knows, in any ASCII case.
pub fn lax_bool(value: &Value) -> Result<bool, Refusal> {
    match value {
        Value::Bool(flag) => Ok(*flag),
        Value::Int(0) => Ok(false),
        Value::Int(1) => Ok(true),
        Value::Int(_) => Err(Refusal::new("bool_parsing", BOOL_PARSING)),
        // Exact comparisons: pydantic accepts only the floats equal to 0 and 1.
        #[allow(clippy::float_cmp)]
        Value::Float(number) if number.is_finite() && number.fract() == 0.0 => {
            if *number == 0.0 {
                Ok(false)
            } else if *number == 1.0 {
                Ok(true)
            } else {
                Err(Refusal::new("bool_parsing", BOOL_PARSING))
            }
        }
        Value::Str(text) => match text.to_ascii_lowercase().as_str() {
            "0" | "off" | "f" | "false" | "n" | "no" => Ok(false),
            "1" | "on" | "t" | "true" | "y" | "yes" => Ok(true),
            _ => Err(Refusal::new("bool_parsing", BOOL_PARSING)),
        },
        _ => Err(Refusal::new("bool_type", BOOL_TYPE)),
    }
}

/// Digits with single underscores between them (pydantic's integer strings).
fn underscored_digits(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let mut digits = String::with_capacity(bytes.len());
    for (index, &byte) in bytes.iter().enumerate() {
        if byte.is_ascii_digit() {
            digits.push(char::from(byte));
        } else if byte == b'_'
            && index > 0
            && bytes[index - 1].is_ascii_digit()
            && bytes.get(index + 1).is_some_and(u8::is_ascii_digit)
        {
        } else {
            return None;
        }
    }
    Some(digits)
}

/// `int` from a string: surrounding whitespace dropped, an optional sign, digits with single
/// underscores between them, and an optional fraction of zeros only (`1.00`).
fn int_from_str(text: &str) -> Result<i64, Refusal> {
    let parsing = || Refusal::new("int_parsing", INT_PARSING);
    let trimmed = text.trim();
    let (whole, fraction) = trimmed
        .split_once('.')
        .map_or((trimmed, None), |(whole, fraction)| (whole, Some(fraction)));
    if let Some(fraction) = fraction
        && (fraction.is_empty() || !fraction.bytes().all(|byte| byte == b'0'))
    {
        return Err(parsing());
    }
    let (negative, unsigned) = match whole.as_bytes().first() {
        Some(b'-') => (true, &whole[1..]),
        Some(b'+') => (false, &whole[1..]),
        _ => (false, whole),
    };
    let digits = underscored_digits(unsigned).ok_or_else(parsing)?;
    let signed = if negative { format!("-{digits}") } else { digits };
    signed
        .parse::<i64>()
        .map_err(|_| Refusal::new("int_parsing_size", INT_PARSING_SIZE))
}

/// `int`: integers, booleans, integral floats within `i64`, and integer strings.
pub fn lax_int(value: &Value) -> Result<i64, Refusal> {
    match value {
        Value::Int(int) => i64::try_from(*int).map_err(|_| Refusal::new("int_parsing_size", INT_PARSING_SIZE)),
        Value::Bool(flag) => Ok(i64::from(*flag)),
        Value::Float(number) => {
            if !number.is_finite() {
                return Err(Refusal::new("finite_number", FINITE_NUMBER));
            }
            if number.fract() != 0.0 {
                return Err(Refusal::new("int_from_float", INT_FROM_FLOAT));
            }
            // -2**63 <= number < 2**63, the range pydantic converts a float in.
            if *number >= -9_223_372_036_854_775_808.0 && *number < 9_223_372_036_854_775_808.0 {
                #[allow(clippy::cast_possible_truncation, reason = "integral and within i64, checked above")]
                Ok(*number as i64)
            } else {
                Err(Refusal::new("int_parsing_size", INT_PARSING_SIZE))
            }
        }
        Value::Str(text) => int_from_str(text),
        _ => Err(Refusal::new("int_type", INT_TYPE)),
    }
}

/// `float` from a string: surrounding whitespace dropped, underscores anywhere but at either
/// end or doubled, then Rust's float syntax (which, like pydantic's, takes `inf`,
/// `infinity` and `nan` in any case, with a sign).
fn float_from_str(text: &str) -> Result<f64, Refusal> {
    let parsing = || Refusal::new("float_parsing", FLOAT_PARSING);
    let trimmed = text.trim();
    if trimmed.starts_with('_') || trimmed.ends_with('_') || trimmed.contains("__") {
        return Err(parsing());
    }
    let cleaned: String = trimmed.chars().filter(|&ch| ch != '_').collect();
    if !cleaned.is_ascii() {
        return Err(parsing());
    }
    cleaned.parse::<f64>().map_err(|_| parsing())
}

/// `float`: numbers (infinities and NaN included), booleans and number strings.
pub fn lax_float(value: &Value) -> Result<f64, Refusal> {
    match value {
        Value::Float(number) => Ok(*number),
        #[allow(clippy::cast_precision_loss, reason = "Python's float(int) rounds the same way")]
        Value::Int(int) => Ok(*int as f64),
        Value::Bool(flag) => Ok(if *flag { 1.0 } else { 0.0 }),
        Value::Str(text) => float_from_str(text),
        _ => Err(Refusal::new("float_type", FLOAT_TYPE)),
    }
}

/// Collects the errors of one model, field by field in declaration order, as pydantic does.
#[derive(Debug)]
pub struct Validator<'a> {
    mode: Mode,
    input: &'a Dict,
    prefix: Vec<Value>,
    fields: &'static [&'static str],
    errors: Vec<ErrorDetail>,
}

impl<'a> Validator<'a> {
    /// The model's input object, or `model_type` at `prefix`.
    pub fn new(
        value: &'a Value,
        mode: Mode,
        model: &'static str,
        fields: &'static [&'static str],
        prefix: Vec<Value>,
    ) -> Result<Self, ErrorDetail> {
        let Value::Dict(input) = value else {
            let msg = match mode {
                Mode::Python => format!("Input should be a valid dictionary or instance of {model}"),
                Mode::Json => "Input should be an object".to_owned(),
            };
            return Err(ErrorDetail {
                kind: "model_type",
                loc: prefix,
                msg,
            });
        };
        Ok(Self {
            mode,
            input,
            prefix,
            fields,
            errors: Vec::new(),
        })
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    fn loc(&self, tail: &[Value]) -> Vec<Value> {
        let mut loc = self.prefix.clone();
        loc.extend_from_slice(tail);
        loc
    }

    fn push(&mut self, tail: &[Value], refusal: Refusal) -> Recorded {
        let loc = self.loc(tail);
        self.errors.push(ErrorDetail {
            kind: refusal.kind,
            loc,
            msg: refusal.msg,
        });
        Recorded
    }

    /// An error found by a nested validator, already located.
    pub fn push_detail(&mut self, detail: ErrorDetail) {
        self.errors.push(detail);
    }

    /// The location of `field` inside this model, for nested validators.
    pub fn field_loc(&self, field: &str) -> Vec<Value> {
        self.loc(&[Value::Str(field.to_owned())])
    }

    /// A field with a default: absent means the default, else `convert` and then the
    /// field's own validator (`check`), whose refusal is a `value_error` at the field.
    pub fn field<T>(
        &mut self,
        name: &'static str,
        default: impl FnOnce() -> T,
        convert: impl FnOnce(&mut Self, &Value) -> Result<T, Recorded>,
        check: impl FnOnce(T) -> Result<T, String>,
    ) -> Option<T> {
        let input = self.input;
        let Some(raw) = input.get(name) else {
            return Some(default());
        };
        let converted = convert(self, raw).ok()?;
        match check(converted) {
            Ok(value) => Some(value),
            Err(message) => {
                self.push(&[Value::Str(name.to_owned())], Refusal::value_error(&message));
                None
            }
        }
    }

    /// A required field: absent is `missing` (`Field required`).
    pub fn required<T>(
        &mut self,
        name: &'static str,
        convert: impl FnOnce(&mut Self, &Value) -> Result<T, Recorded>,
        check: impl FnOnce(T) -> Result<T, String>,
    ) -> Option<T> {
        if self.input.get(name).is_none() {
            self.push(
                &[Value::Str(name.to_owned())],
                Refusal::new("missing", "Field required"),
            );
            return None;
        }
        self.field(name, || unreachable_default(), convert, check)
    }

    /// `tuple[T, ...]` at `loc` (relative to this model): a list, each item converted at
    /// `[*loc, index]`.
    pub fn tuple<T>(
        &mut self,
        loc: &[Value],
        value: &Value,
        mut item: impl FnMut(&mut Self, Vec<Value>, &Value) -> Result<T, Recorded>,
    ) -> Result<Vec<T>, Recorded> {
        let Value::List(items) = value else {
            let msg = match self.mode {
                Mode::Python => "Input should be a valid tuple",
                Mode::Json => "Input should be a valid array",
            };
            self.push(loc, Refusal::new("tuple_type", msg));
            return Err(Recorded);
        };
        let mut out = Vec::with_capacity(items.len());
        let mut failed = false;
        for (index, raw) in items.iter().enumerate() {
            let mut item_loc = loc.to_vec();
            item_loc.push(Value::Int(i128::try_from(index).unwrap_or(i128::MAX)));
            match item(self, item_loc, raw) {
                Ok(converted) => out.push(converted),
                Err(Recorded) => failed = true,
            }
        }
        if failed { Err(Recorded) } else { Ok(out) }
    }

    /// `dict[str, T]` at `loc`: an object with string keys, each value converted at
    /// `[*loc, key]` (a key that is not a string fails at `[*loc, key, '[key]']`).
    pub fn dict<T>(
        &mut self,
        loc: &[Value],
        value: &Value,
        mut item: impl FnMut(&mut Self, Vec<Value>, &Value) -> Result<T, Recorded>,
    ) -> Result<Vec<(String, T)>, Recorded> {
        let Value::Dict(entries) = value else {
            let msg = match self.mode {
                Mode::Python => "Input should be a valid dictionary",
                Mode::Json => "Input should be an object",
            };
            self.push(loc, Refusal::new("dict_type", msg));
            return Err(Recorded);
        };
        let mut out = Vec::with_capacity(entries.len());
        let mut failed = false;
        for (key, raw) in entries.iter() {
            let mut item_loc = loc.to_vec();
            item_loc.push(key_to_value(key));
            let Key::Str(name) = key else {
                item_loc.push(Value::Str("[key]".to_owned()));
                self.push(&item_loc, Refusal::new("string_type", STRING_TYPE));
                failed = true;
                continue;
            };
            match item(self, item_loc, raw) {
                Ok(converted) => out.push((name.clone(), converted)),
                Err(Recorded) => failed = true,
            }
        }
        if failed { Err(Recorded) } else { Ok(out) }
    }

    /// A scalar inside a container, at `loc` (relative to this model).
    pub fn scalar_at<T>(
        &mut self,
        loc: &[Value],
        value: &Value,
        convert: fn(&Value) -> Result<T, Refusal>,
    ) -> Result<T, Recorded> {
        convert(value).map_err(|refusal| self.push(loc, refusal))
    }

    /// The prefix this model is validated under, for a nested model's own validator.
    pub fn nested_prefix(&self, tail: &[Value]) -> Vec<Value> {
        self.loc(tail)
    }

    /// `extra='forbid'`: every key that is not a field, in input order; then the result.
    pub fn finish(mut self) -> Result<(), Vec<ErrorDetail>> {
        for (key, _) in self.input.iter() {
            let known = matches!(key, Key::Str(name) if self.fields.contains(&name.as_str()));
            if !known {
                let loc = self.loc(&[key_to_value(key)]);
                self.errors.push(ErrorDetail {
                    kind: "extra_forbidden",
                    loc,
                    msg: "Extra inputs are not permitted".to_owned(),
                });
            }
        }
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self.errors)
        }
    }
}

fn unreachable_default<T>() -> T {
    panic!("a required field is present when its default is asked for")
}

fn key_to_value(key: &Key) -> Value {
    match key {
        Key::Str(text) => Value::Str(text.clone()),
        Key::Int(int) => Value::Int(*int),
        Key::Float(number) => Value::Float(*number),
        Key::Bool(flag) => Value::Bool(*flag),
        Key::Null => Value::Null,
    }
}

/// `model_validate_json`'s parse: the text as JSON (`NaN` and `Infinity` accepted, the last
/// of repeated keys kept), or a `json_invalid` error with the parser's message.
pub fn parse_json(text: &str) -> Result<Value, ErrorDetail> {
    crate::pyjson::loads(text).map_err(|error| {
        // pydantic's parser (jiter) words its errors as serde_json does.
        let reason = match serde_json::from_str::<serde_json::Value>(text) {
            Err(serde_error) => serde_error.to_string(),
            Ok(_) => error.to_string(),
        };
        ErrorDetail {
            kind: "json_invalid",
            loc: Vec::new(),
            msg: format!("Invalid JSON: {reason}"),
        }
    })
}

/// A conversion failed and its error is already recorded in the [`Validator`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recorded;

/// `str.strip()` (Python's whitespace, not Rust's).
pub fn py_strip(text: &str) -> &str {
    pytext::py_strip(text)
}
