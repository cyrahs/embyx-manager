//! Python-compatible JSON: a value model with Python's distinctions, `json.loads` and
//! `json.dumps` byte for byte.
//!
//! Stored data depends on Python's exact output: the game sources keep sha256 hashes of
//! `json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
//! default=str)` bytes, and `app_settings` rows are written with `json.dumps(value,
//! ensure_ascii=False)`. `serde_json` differs in float formatting (`1e16` vs `1e+16`),
//! escaping (`ensure_ascii`), separators and key ordering, so anything stored or hashed
//! goes through [`dumps`]. The value model keeps what Python keeps apart: `1` and `1.0`,
//! integers beyond 64 bits, non-finite floats and non-string dictionary keys.
//!
//! Python origin: the standard library `json` module (`encoder.py`, `decoder.py`,
//! `scanner.py`) and `float.__repr__`, as of Python 3.13 (fav's copy followed 3.12; the
//! trailing-comma messages are 3.13's).
//!
//! Copied from fav (`fav-core/src/pyjson.rs`); see docs/CONVENTIONS.md, "Code copied from fav".

use std::cmp::Ordering;
use std::fmt::Write as _;

use thiserror::Error;

/// A Python value as `json.loads` produces it or `json.dumps` accepts it.
///
/// Integers are held as `i128` (Python's are unbounded; nothing embyx-manager stores comes close).
/// Tuples are written like lists, so they are lists here.
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Str(String),
    List(Vec<Value>),
    Dict(Dict),
}

/// A dictionary key. `json.loads` only produces strings; Python callers can pass the
/// others, which `json.dumps` converts (and, with `sort_keys`, sorts by their own type).
#[derive(Debug, Clone)]
pub enum Key {
    Str(String),
    Int(i128),
    Float(f64),
    Bool(bool),
    Null,
}

impl Key {
    /// The key's text when it is a string, as every key `json.loads` reads is.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(text) => Some(text),
            _ => None,
        }
    }
}

/// An insertion-ordered dictionary, like Python's `dict`.
#[derive(Debug, Clone, Default)]
pub struct Dict {
    entries: Vec<(Key, Value)>,
}

fn float_eq(a: f64, b: f64) -> bool {
    a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan())
}

/// Strict equality: `1` is not `1.0`, `-0.0` is not `0.0`, and NaN equals NaN, so a test
/// comparing with a golden sees every difference `json.dumps` would print.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => float_eq(*a, *b),
            (Self::Str(a), Self::Str(b)) => a == b,
            (Self::List(a), Self::List(b)) => a == b,
            (Self::Dict(a), Self::Dict(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Str(a), Self::Str(b)) => a == b,
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => float_eq(*a, *b),
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}

impl PartialEq for Dict {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}

impl Dict {
    pub fn new() -> Self {
        Self::default()
    }

    /// Python `d[key] = value`: a new key goes last, an existing one keeps its place.
    pub fn insert(&mut self, key: impl Into<Key>, value: Value) {
        let key = key.into();
        if let Some(slot) = self.entries.iter_mut().find(|(existing, _)| *existing == key) {
            slot.1 = value;
        } else {
            self.entries.push((key, value));
        }
    }

    /// The value under a string key.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.entries.iter().find_map(|(existing, value)| match existing {
            Key::Str(name) if name == key => Some(value),
            _ => None,
        })
    }

    /// The value under a string key, to change in place.
    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.entries.iter_mut().find_map(|(existing, value)| match existing {
            Key::Str(name) if name == key => Some(value),
            _ => None,
        })
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    /// Python `d.pop(key, None)` for a string key.
    pub fn remove(&mut self, key: &str) -> Option<Value> {
        let index = self
            .entries
            .iter()
            .position(|(existing, _)| matches!(existing, Key::Str(name) if name == key))?;
        Some(self.entries.remove(index).1)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Key, &Value)> {
        self.entries.iter().map(|(key, value)| (key, value))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl FromIterator<(String, Value)> for Dict {
    fn from_iter<T: IntoIterator<Item = (String, Value)>>(iter: T) -> Self {
        let mut dict = Self::new();
        for (key, value) in iter {
            dict.insert(key, value);
        }
        dict
    }
}

impl From<String> for Key {
    fn from(value: String) -> Self {
        Self::Str(value)
    }
}

impl From<&str> for Key {
    fn from(value: &str) -> Self {
        Self::Str(value.to_owned())
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Str(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::Str(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Int(i128::from(value))
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<Dict> for Value {
    fn from(value: Dict) -> Self {
        Self::Dict(value)
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(value: Vec<T>) -> Self {
        Self::List(value.into_iter().map(Into::into).collect())
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(value: Option<T>) -> Self {
        value.map_or(Self::Null, Into::into)
    }
}

/// `type(x).__name__` of a JSON value: one copy for the clients, the merge worker and the home
/// worker.
pub fn type_name(value: &Value) -> &'static str {
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

impl Value {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(text) => Some(text),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&Dict> {
        match self {
            Self::Dict(dict) => Some(dict),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Self::List(items) => Some(items),
            _ => None,
        }
    }

    /// The value under a string key, if this is a dictionary.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.as_dict()?.get(key)
    }

    /// Python truthiness: `None`, `False`, zero, and empty containers are false.
    pub fn is_truthy(&self) -> bool {
        match self {
            Self::Null => false,
            Self::Bool(value) => *value,
            Self::Int(value) => *value != 0,
            Self::Float(value) => *value != 0.0,
            Self::Str(text) => !text.is_empty(),
            Self::List(items) => !items.is_empty(),
            Self::Dict(dict) => !dict.is_empty(),
        }
    }

    /// Convert from `serde_json` (for values that never needed Python's distinctions).
    pub fn from_serde(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(flag) => Self::Bool(*flag),
            serde_json::Value::Number(number) => {
                if let Some(int) = number.as_i64() {
                    Self::Int(i128::from(int))
                } else if let Some(int) = number.as_u64() {
                    Self::Int(i128::from(int))
                } else {
                    Self::Float(number.as_f64().unwrap_or(f64::NAN))
                }
            }
            serde_json::Value::String(text) => Self::Str(text.clone()),
            serde_json::Value::Array(items) => Self::List(items.iter().map(Self::from_serde).collect()),
            serde_json::Value::Object(map) => Self::Dict(
                map.iter()
                    .map(|(key, item)| (key.clone(), Self::from_serde(item)))
                    .collect(),
            ),
        }
    }
}

// ---------- json.dumps ----------

/// The keyword arguments of `json.dumps` that the baseline uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DumpOptions {
    pub ensure_ascii: bool,
    pub sort_keys: bool,
    pub indent: Option<usize>,
    pub item_separator: &'static str,
    pub key_separator: &'static str,
}

impl DumpOptions {
    /// `json.dumps(value)`: ASCII-only, default separators, insertion order.
    pub const fn python_default() -> Self {
        Self {
            ensure_ascii: true,
            sort_keys: false,
            indent: None,
            item_separator: ", ",
            key_separator: ": ",
        }
    }

    /// `json.dumps(value, ensure_ascii=False)`.
    pub const fn plain() -> Self {
        Self {
            ensure_ascii: false,
            ..Self::python_default()
        }
    }

    /// `json.dumps(value, separators=(',', ':'), sort_keys=True)`: the cloud file snapshots
    /// of `fill_actor/postgres_repository.py`.
    pub const fn sorted_compact_ascii() -> Self {
        Self {
            ensure_ascii: true,
            sort_keys: true,
            indent: None,
            item_separator: ",",
            key_separator: ":",
        }
    }

    /// `json.dumps(value, ensure_ascii=False, separators=(',', ':'))`.
    pub const fn compact() -> Self {
        Self {
            item_separator: ",",
            key_separator: ":",
            ..Self::plain()
        }
    }

    /// `json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str)`:
    /// the bytes fav hashes (kept with the copy).
    pub const fn hash() -> Self {
        Self {
            sort_keys: true,
            ..Self::compact()
        }
    }

    /// `json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str)`: the
    /// effect transcripts and the OpenAPI diff's display; writers add a trailing newline.
    pub const fn pretty() -> Self {
        Self {
            sort_keys: true,
            indent: Some(2),
            item_separator: ",",
            ..Self::plain()
        }
    }
}

/// Why `json.dumps` refused a value.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DumpError {
    /// Python raises `TypeError: '<' not supported between instances ...` when
    /// `sort_keys` meets keys of different types.
    #[error("cannot sort dictionary keys of different types")]
    MixedKeyTypes,
}

/// `json.dumps(value, **options)`.
pub fn dumps(value: &Value, options: &DumpOptions) -> Result<String, DumpError> {
    let mut out = String::new();
    write_value(&mut out, value, options, 0)?;
    Ok(out)
}

fn newline_indent(out: &mut String, options: &DumpOptions, level: usize) {
    if let Some(width) = options.indent {
        out.push('\n');
        for _ in 0..width * level {
            out.push(' ');
        }
    }
}

fn write_value(out: &mut String, value: &Value, options: &DumpOptions, level: usize) -> Result<(), DumpError> {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Int(int) => {
            let _ = write!(out, "{int}");
        }
        Value::Float(float) => out.push_str(&float_json(*float)),
        Value::Str(text) => write_string(out, text, options.ensure_ascii),
        Value::List(items) => {
            if items.is_empty() {
                out.push_str("[]");
                return Ok(());
            }
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push_str(options.item_separator);
                }
                newline_indent(out, options, level + 1);
                write_value(out, item, options, level + 1)?;
            }
            newline_indent(out, options, level);
            out.push(']');
        }
        Value::Dict(dict) => {
            if dict.is_empty() {
                out.push_str("{}");
                return Ok(());
            }
            let mut entries: Vec<&(Key, Value)> = dict.entries.iter().collect();
            if options.sort_keys {
                sort_entries(&mut entries)?;
            }
            out.push('{');
            for (index, (key, item)) in entries.into_iter().enumerate() {
                if index > 0 {
                    out.push_str(options.item_separator);
                }
                newline_indent(out, options, level + 1);
                write_string(out, &key_text(key), options.ensure_ascii);
                out.push_str(options.key_separator);
                write_value(out, item, options, level + 1)?;
            }
            newline_indent(out, options, level);
            out.push('}');
        }
    }
    Ok(())
}

/// `sorted(dct.items())`: keys compare by their own type, which must be the same for all
/// (Python's numbers compare across int, float and bool).
fn sort_entries(entries: &mut [&(Key, Value)]) -> Result<(), DumpError> {
    let numeric = |key: &Key| match key {
        Key::Int(int) => Some(NumberKey::Int(*int)),
        Key::Float(float) => Some(NumberKey::Float(*float)),
        Key::Bool(flag) => Some(NumberKey::Int(i128::from(*flag))),
        Key::Str(_) | Key::Null => None,
    };
    let all_str = entries.iter().all(|(key, _)| matches!(key, Key::Str(_)));
    let all_numeric = entries.iter().all(|(key, _)| numeric(key).is_some());
    if all_str {
        entries.sort_by(|(a, _), (b, _)| match (a, b) {
            (Key::Str(a), Key::Str(b)) => a.cmp(b),
            _ => Ordering::Equal,
        });
        Ok(())
    } else if all_numeric {
        entries.sort_by(|(a, _), (b, _)| {
            numeric(a)
                .zip(numeric(b))
                .map_or(Ordering::Equal, |(a, b)| a.compare(&b))
        });
        Ok(())
    } else if entries.len() == 1 {
        Ok(())
    } else {
        Err(DumpError::MixedKeyTypes)
    }
}

#[derive(Clone, Copy)]
enum NumberKey {
    Int(i128),
    Float(f64),
}

impl NumberKey {
    #[allow(clippy::cast_precision_loss)] // Python compares int and float by value; keys this large do not occur.
    fn compare(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => a.cmp(b),
            (Self::Int(a), Self::Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal),
            (Self::Float(a), Self::Int(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        }
    }
}

/// How `json.dumps` turns a key into a string.
fn key_text(key: &Key) -> String {
    match key {
        Key::Str(text) => text.clone(),
        Key::Int(int) => int.to_string(),
        Key::Float(float) => float_json(*float),
        Key::Bool(true) => "true".to_owned(),
        Key::Bool(false) => "false".to_owned(),
        Key::Null => "null".to_owned(),
    }
}

/// A float as `json.dumps` writes it (`allow_nan=True`).
fn float_json(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value.is_infinite() {
        if value > 0.0 {
            "Infinity".to_owned()
        } else {
            "-Infinity".to_owned()
        }
    } else {
        float_repr(value)
    }
}

/// Python's `repr(float)`: the shortest digits that round-trip, written in fixed notation
/// when the decimal exponent is in `-4 <= exp < 16` and in scientific notation otherwise.
pub fn float_repr(value: f64) -> String {
    if value.is_nan() {
        return "nan".to_owned();
    }
    if value.is_infinite() {
        return if value > 0.0 {
            "inf".to_owned()
        } else {
            "-inf".to_owned()
        };
    }
    // `{:e}` prints the shortest round-tripping digits: "-1.2345e-7", "1e16", "0e0".
    let formatted = format!("{value:e}");
    let (mantissa, exponent) = formatted.split_once('e').unwrap_or((&formatted, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let (sign, mantissa) = mantissa.strip_prefix('-').map_or(("", mantissa), |rest| ("-", rest));
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    // Python's `decpt`: the value is 0.<digits> * 10**decpt.
    let decpt = exponent + 1;
    let mut out = String::from(sign);
    if (-4 < decpt) && (decpt <= 16) {
        let decpt = usize::try_from(decpt.max(0)).unwrap_or(0);
        if exponent < 0 {
            out.push_str("0.");
            for _ in 0..(-exponent - 1) {
                out.push('0');
            }
            out.push_str(&digits);
        } else if digits.len() <= decpt {
            out.push_str(&digits);
            for _ in digits.len()..decpt {
                out.push('0');
            }
            out.push_str(".0");
        } else {
            out.push_str(&digits[..decpt]);
            out.push('.');
            out.push_str(&digits[decpt..]);
        }
    } else {
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        let _ = write!(out, "e{}{:02}", if exponent < 0 { '-' } else { '+' }, exponent.abs());
    }
    out
}

fn write_string(out: &mut String, text: &str, ensure_ascii: bool) {
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            ch if u32::from(ch) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", u32::from(ch));
            }
            ch if ensure_ascii && !(' '..='~').contains(&ch) => {
                let mut units = [0_u16; 2];
                for unit in ch.encode_utf16(&mut units) {
                    let _ = write!(out, "\\u{unit:04x}");
                }
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
}

// ---------- json.loads ----------

/// Why `json.loads` refused a document. Python's messages, minus line and column.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LoadError {
    #[error("Expecting value at char {0}")]
    ExpectingValue(usize),
    #[error("Extra data at char {0}")]
    ExtraData(usize),
    #[error("Unterminated string starting at char {0}")]
    UnterminatedString(usize),
    #[error("Invalid control character at char {0}")]
    ControlCharacter(usize),
    #[error("Invalid \\escape at char {0}")]
    InvalidEscape(usize),
    #[error("Expecting property name enclosed in double quotes at char {0}")]
    ExpectingPropertyName(usize),
    #[error("Expecting ':' delimiter at char {0}")]
    ExpectingColon(usize),
    #[error("Expecting ',' delimiter at char {0}")]
    ExpectingComma(usize),
    /// Python 3.13 names a comma right before `]` (the position is the comma's).
    #[error("Illegal trailing comma before end of array at char {0}")]
    TrailingCommaArray(usize),
    /// Python 3.13 names a comma right before `}` (the position is the comma's).
    #[error("Illegal trailing comma before end of object at char {0}")]
    TrailingCommaObject(usize),
    /// Python keeps a lone surrogate in a `str`; Rust strings cannot hold one.
    #[error("Lone surrogate escape at char {0}")]
    LoneSurrogate(usize),
    /// Python's integers are unbounded; this model keeps 128 bits.
    #[error("Integer out of range at char {0}")]
    IntegerTooLarge(usize),
}

impl LoadError {
    /// `str(JSONDecodeError)` for `doc`: the message, then `line L column C (char N)`.
    pub fn python_message(&self, doc: &str) -> String {
        let (message, pos) = match *self {
            Self::ExpectingValue(pos) => ("Expecting value", pos),
            Self::ExtraData(pos) => ("Extra data", pos),
            Self::UnterminatedString(pos) => ("Unterminated string starting at", pos),
            Self::ControlCharacter(pos) => ("Invalid control character at", pos),
            Self::InvalidEscape(pos) => ("Invalid \\escape", pos),
            Self::ExpectingPropertyName(pos) => ("Expecting property name enclosed in double quotes", pos),
            Self::ExpectingColon(pos) => ("Expecting ':' delimiter", pos),
            Self::ExpectingComma(pos) => ("Expecting ',' delimiter", pos),
            Self::TrailingCommaArray(pos) => ("Illegal trailing comma before end of array", pos),
            Self::TrailingCommaObject(pos) => ("Illegal trailing comma before end of object", pos),
            Self::LoneSurrogate(pos) => ("Lone surrogate escape", pos),
            Self::IntegerTooLarge(pos) => ("Integer out of range", pos),
        };
        let before: Vec<char> = doc.chars().take(pos).collect();
        let lineno = before.iter().filter(|ch| **ch == '\n').count() + 1;
        let colno = before
            .iter()
            .rposition(|ch| *ch == '\n')
            .map_or(pos + 1, |newline| pos - newline);
        format!("{message}: line {lineno} column {colno} (char {pos})")
    }
}

/// `json.loads(text)`.
pub fn loads(text: &str) -> Result<Value, LoadError> {
    let mut parser = Parser {
        chars: text.char_indices().collect(),
        pos: 0,
    };
    parser.skip_whitespace();
    let value = parser.value()?;
    parser.skip_whitespace();
    if parser.pos < parser.chars.len() {
        return Err(LoadError::ExtraData(parser.pos));
    }
    Ok(value)
}

struct Parser {
    chars: Vec<(usize, char)>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).map(|&(_, ch)| ch)
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(' ' | '\t' | '\n' | '\r')) {
            self.pos += 1;
        }
    }

    fn starts_with(&self, word: &str) -> bool {
        let mut index = self.pos;
        for expected in word.chars() {
            match self.chars.get(index) {
                Some(&(_, ch)) if ch == expected => index += 1,
                _ => return false,
            }
        }
        true
    }

    fn take_word(&mut self, word: &str, value: Value) -> Option<Value> {
        if self.starts_with(word) {
            self.pos += word.chars().count();
            Some(value)
        } else {
            None
        }
    }

    fn value(&mut self) -> Result<Value, LoadError> {
        let start = self.pos;
        match self.peek() {
            Some('"') => self.string().map(Value::Str),
            Some('{') => self.object(),
            Some('[') => self.array(),
            Some('n') => self
                .take_word("null", Value::Null)
                .ok_or(LoadError::ExpectingValue(start)),
            Some('t') => self
                .take_word("true", Value::Bool(true))
                .ok_or(LoadError::ExpectingValue(start)),
            Some('f') => self
                .take_word("false", Value::Bool(false))
                .ok_or(LoadError::ExpectingValue(start)),
            Some('N') => self
                .take_word("NaN", Value::Float(f64::NAN))
                .ok_or(LoadError::ExpectingValue(start)),
            Some('I') => self
                .take_word("Infinity", Value::Float(f64::INFINITY))
                .ok_or(LoadError::ExpectingValue(start)),
            Some('-') if self.starts_with("-Infinity") => {
                self.pos += "-Infinity".len();
                Ok(Value::Float(f64::NEG_INFINITY))
            }
            Some('-' | '0'..='9') => self.number(),
            _ => Err(LoadError::ExpectingValue(start)),
        }
    }

    /// `(-?(?:0|[1-9]\d*))(\.\d+)?([eE][-+]?\d+)?`, an int unless it has a fraction or exponent.
    fn number(&mut self) -> Result<Value, LoadError> {
        let start = self.pos;
        let mut text = String::new();
        if self.peek() == Some('-') {
            text.push('-');
            self.pos += 1;
        }
        match self.peek() {
            Some('0') => {
                text.push('0');
                self.pos += 1;
            }
            Some('1'..='9') => self.digits(&mut text),
            _ => return Err(LoadError::ExpectingValue(start)),
        }
        let mut is_float = false;
        if self.peek() == Some('.') && matches!(self.chars.get(self.pos + 1), Some((_, '0'..='9'))) {
            is_float = true;
            text.push('.');
            self.pos += 1;
            self.digits(&mut text);
        }
        if matches!(self.peek(), Some('e' | 'E')) {
            let mark = self.pos;
            let mut exponent = String::from("e");
            self.pos += 1;
            if let Some(sign @ ('+' | '-')) = self.peek() {
                exponent.push(sign);
                self.pos += 1;
            }
            if matches!(self.peek(), Some('0'..='9')) {
                self.digits(&mut exponent);
                is_float = true;
                text.push_str(&exponent);
            } else {
                self.pos = mark;
            }
        }
        if is_float {
            Ok(Value::Float(text.parse().unwrap_or(f64::NAN)))
        } else {
            text.parse()
                .map(Value::Int)
                .map_err(|_| LoadError::IntegerTooLarge(start))
        }
    }

    fn digits(&mut self, text: &mut String) {
        while let Some(ch @ '0'..='9') = self.peek() {
            text.push(ch);
            self.pos += 1;
        }
    }

    fn hex4(&mut self, escape_start: usize) -> Result<u32, LoadError> {
        let mut code = 0;
        for _ in 0..4 {
            let digit = self
                .peek()
                .and_then(|ch| ch.to_digit(16))
                .ok_or(LoadError::InvalidEscape(escape_start))?;
            code = code * 16 + digit;
            self.pos += 1;
        }
        Ok(code)
    }

    fn string(&mut self) -> Result<String, LoadError> {
        let start = self.pos;
        self.pos += 1;
        let mut out = String::new();
        loop {
            let Some(ch) = self.peek() else {
                return Err(LoadError::UnterminatedString(start));
            };
            match ch {
                '"' => {
                    self.pos += 1;
                    return Ok(out);
                }
                '\\' => {
                    let escape_start = self.pos;
                    self.pos += 1;
                    let Some(kind) = self.peek() else {
                        return Err(LoadError::UnterminatedString(start));
                    };
                    self.pos += 1;
                    match kind {
                        '"' => out.push('"'),
                        '\\' => out.push('\\'),
                        '/' => out.push('/'),
                        'b' => out.push('\u{8}'),
                        'f' => out.push('\u{c}'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => {
                            let mut code = self.hex4(escape_start)?;
                            if (0xD800..0xDC00).contains(&code) && self.starts_with("\\u") {
                                let mark = self.pos;
                                self.pos += 2;
                                let low = self.hex4(self.pos - 2)?;
                                if (0xDC00..0xE000).contains(&low) {
                                    code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                                } else {
                                    self.pos = mark;
                                }
                            }
                            out.push(char::from_u32(code).ok_or(LoadError::LoneSurrogate(escape_start))?);
                        }
                        _ => return Err(LoadError::InvalidEscape(escape_start)),
                    }
                }
                ch if u32::from(ch) < 0x20 => return Err(LoadError::ControlCharacter(self.pos)),
                ch => {
                    out.push(ch);
                    self.pos += 1;
                }
            }
        }
    }

    fn array(&mut self) -> Result<Value, LoadError> {
        self.pos += 1;
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(']') {
            self.pos += 1;
            return Ok(Value::List(items));
        }
        loop {
            self.skip_whitespace();
            items.push(self.value()?);
            self.skip_whitespace();
            match self.peek() {
                Some(',') => {
                    let comma = self.pos;
                    self.pos += 1;
                    self.skip_whitespace();
                    if self.peek() == Some(']') {
                        return Err(LoadError::TrailingCommaArray(comma));
                    }
                }
                Some(']') => {
                    self.pos += 1;
                    return Ok(Value::List(items));
                }
                _ => return Err(LoadError::ExpectingComma(self.pos)),
            }
        }
    }

    fn object(&mut self) -> Result<Value, LoadError> {
        self.pos += 1;
        let mut dict = Dict::new();
        self.skip_whitespace();
        if self.peek() == Some('}') {
            self.pos += 1;
            return Ok(Value::Dict(dict));
        }
        loop {
            self.skip_whitespace();
            if self.peek() != Some('"') {
                return Err(LoadError::ExpectingPropertyName(self.pos));
            }
            let key = self.string()?;
            self.skip_whitespace();
            if self.peek() != Some(':') {
                return Err(LoadError::ExpectingColon(self.pos));
            }
            self.pos += 1;
            self.skip_whitespace();
            let value = self.value()?;
            dict.insert(key, value);
            self.skip_whitespace();
            match self.peek() {
                Some(',') => {
                    let comma = self.pos;
                    self.pos += 1;
                    self.skip_whitespace();
                    if self.peek() == Some('}') {
                        return Err(LoadError::TrailingCommaObject(comma));
                    }
                }
                Some('}') => {
                    self.pos += 1;
                    return Ok(Value::Dict(dict));
                }
                _ => return Err(LoadError::ExpectingComma(self.pos)),
            }
        }
    }
}

// ---------- default=str ----------

/// `str(datetime)` for a naive datetime: `2026-11-09 22:56:54[.000123]`.
pub fn py_str_naive_datetime(value: &chrono::NaiveDateTime) -> String {
    use chrono::Timelike as _;
    let micros = value.nanosecond() / 1000;
    let mut out = value.format("%Y-%m-%d %H:%M:%S").to_string();
    if micros != 0 {
        let _ = write!(out, ".{micros:06}");
    }
    out
}

/// `str(datetime)` for an aware datetime: the naive form plus `+HH:MM` (`:SS` when needed).
pub fn py_str_datetime(value: &chrono::DateTime<chrono::FixedOffset>) -> String {
    let mut out = py_str_naive_datetime(&value.naive_local());
    let offset = value.offset().local_minus_utc();
    let sign = if offset < 0 { '-' } else { '+' };
    let offset = offset.unsigned_abs();
    let _ = write!(out, "{sign}{:02}:{:02}", offset / 3600, (offset / 60) % 60);
    if !offset.is_multiple_of(60) {
        let _ = write!(out, ":{:02}", offset % 60);
    }
    out
}

/// `datetime.isoformat()` for an aware datetime: like `str()` with a `T` separator.
pub fn py_isoformat_datetime(value: &chrono::DateTime<chrono::FixedOffset>) -> String {
    py_str_datetime(value).replacen(' ', "T", 1)
}

/// `str(date)`: `2026-11-09`.
pub fn py_str_date(value: &chrono::NaiveDate) -> String {
    value.format("%Y-%m-%d").to_string()
}

// ---------- `json.loads` on bytes ----------

/// What `json.detect_encoding` makes of a body, for `json.loads(bytes)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonEncoding<'a> {
    /// UTF-8 (a byte-order mark already stripped): the caller decodes it, so a failure can
    /// carry Python's `UnicodeDecodeError` text where it needs one.
    Utf8(&'a [u8]),
    /// UTF-16 or UTF-32 (by mark or by the position of the zero bytes), decoded; `None`
    /// when the bytes are not valid in that encoding.
    Decoded(Option<String>),
}

/// `json.detect_encoding(body)` and, for UTF-16 and UTF-32, the decode `json.loads` does.
/// What `httpx.Response.json()` does before parsing.
pub fn detect_json_encoding(body: &[u8]) -> JsonEncoding<'_> {
    if body.starts_with(b"\x00\x00\xfe\xff") {
        return JsonEncoding::Decoded(decode_utf32(&body[4..], true));
    }
    if body.starts_with(b"\xff\xfe\x00\x00") {
        return JsonEncoding::Decoded(decode_utf32(&body[4..], false));
    }
    if body.starts_with(b"\xfe\xff") {
        return JsonEncoding::Decoded(decode_utf16(&body[2..], true));
    }
    if body.starts_with(b"\xff\xfe") {
        return JsonEncoding::Decoded(decode_utf16(&body[2..], false));
    }
    if let Some(rest) = body.strip_prefix(b"\xef\xbb\xbf") {
        return JsonEncoding::Utf8(rest);
    }
    JsonEncoding::Decoded(match body {
        [0, second, ..] if body.len() >= 4 => {
            if *second == 0 {
                decode_utf32(body, true)
            } else {
                decode_utf16(body, true)
            }
        }
        [_, 0, third, fourth, ..] => {
            if *third != 0 || *fourth != 0 {
                decode_utf16(body, false)
            } else {
                decode_utf32(body, false)
            }
        }
        [0, _] => decode_utf16(body, true),
        [_, 0] => decode_utf16(body, false),
        _ => return JsonEncoding::Utf8(body),
    })
}

fn decode_utf16(bytes: &[u8], big_endian: bool) -> Option<String> {
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    let units = bytes.chunks_exact(2).map(|pair| {
        let pair = [pair[0], pair[1]];
        if big_endian {
            u16::from_be_bytes(pair)
        } else {
            u16::from_le_bytes(pair)
        }
    });
    char::decode_utf16(units).collect::<Result<String, _>>().ok()
}

fn decode_utf32(bytes: &[u8], big_endian: bool) -> Option<String> {
    if !bytes.len().is_multiple_of(4) {
        return None;
    }
    bytes
        .chunks_exact(4)
        .map(|quad| {
            let quad = [quad[0], quad[1], quad[2], quad[3]];
            char::from_u32(if big_endian {
                u32::from_be_bytes(quad)
            } else {
                u32::from_le_bytes(quad)
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        DumpError, DumpOptions, Key, Value, dumps, float_repr, loads, py_str_date, py_str_datetime,
        py_str_naive_datetime,
    };
    use crate::golden;

    fn options(name: &str) -> DumpOptions {
        match name {
            "hash" => DumpOptions::hash(),
            "pretty" => DumpOptions::pretty(),
            "compact" => DumpOptions::compact(),
            "plain" => DumpOptions::plain(),
            "sorted_compact_ascii" => DumpOptions::sorted_compact_ascii(),
            "ascii" => DumpOptions::python_default(),
            other => panic!("unknown format {other}"),
        }
    }

    fn render(name: &str, value: &Value) -> String {
        let text = dumps(value, &options(name)).unwrap();
        // The manifest writers append a newline to the pretty format.
        if name == "pretty" { text + "\n" } else { text }
    }

    #[test]
    fn every_format_the_code_uses_is_covered() {
        let document = golden::load("json/formats.json");
        let names: Vec<String> = golden::cases(&document)
            .iter()
            .map(|case| golden::string(golden::get(case, "name")))
            .collect();
        assert_eq!(
            names,
            ["hash", "pretty", "compact", "plain", "sorted_compact_ascii", "ascii"]
        );
    }

    #[test]
    fn dumps_matches_python_byte_for_byte() {
        let document = golden::load("json/dumps.json");
        for case in golden::cases(&document) {
            let input = golden::string(golden::get(case, "input"));
            let value = loads(&input).unwrap();
            for (name, expected) in golden::get(case, "outputs").as_dict().unwrap().iter() {
                let Key::Str(name) = name else { panic!() };
                assert_eq!(
                    render(name, &value),
                    golden::string(expected),
                    "format {name} of {input}"
                );
            }
        }
    }

    /// The values `json.loads` cannot produce, built by hand from their Python source.
    fn python_value(expr: &str) -> Value {
        let dict = |entries: Vec<(Key, Value)>| {
            let mut dict = super::Dict::new();
            for (key, value) in entries {
                dict.insert(key, value);
            }
            Value::Dict(dict)
        };
        match expr {
            "{10: 'a', 9: 'b', 100: 'c'}" => dict(vec![
                (Key::Int(10), "a".into()),
                (Key::Int(9), "b".into()),
                (Key::Int(100), "c".into()),
            ]),
            "{-1: 'neg', 0: 'zero', 2: 'two'}" => dict(vec![
                (Key::Int(-1), "neg".into()),
                (Key::Int(0), "zero".into()),
                (Key::Int(2), "two".into()),
            ]),
            "{1.5: 'x', 0.25: 'y'}" => dict(vec![(Key::Float(1.5), "x".into()), (Key::Float(0.25), "y".into())]),
            "{True: 't', False: 'f'}" => dict(vec![(Key::Bool(true), "t".into()), (Key::Bool(false), "f".into())]),
            "{None: 'none'}" => dict(vec![(Key::Null, "none".into())]),
            "{'k': (1, 'two', (3,))}" => dict(vec![(
                Key::from("k"),
                Value::List(vec![Value::Int(1), "two".into(), Value::List(vec![Value::Int(3)])]),
            )]),
            "[float('nan'), float('inf'), float('-inf')]" => Value::List(vec![
                Value::Float(f64::NAN),
                Value::Float(f64::INFINITY),
                Value::Float(f64::NEG_INFINITY),
            ]),
            "{'b': 1.0, 'a': -0.0}" => dict(vec![
                (Key::from("b"), Value::Float(1.0)),
                (Key::from("a"), Value::Float(-0.0)),
            ]),
            other => panic!("no Rust value for {other}"),
        }
    }

    #[test]
    fn dumps_handles_python_only_values() {
        let document = golden::load("json/python_values.json");
        for case in golden::cases(&document) {
            let expr = golden::string(golden::get(case, "expr"));
            let value = python_value(&expr);
            for (name, expected) in golden::get(case, "outputs").as_dict().unwrap().iter() {
                let Key::Str(name) = name else { panic!() };
                assert_eq!(
                    render(name, &value),
                    golden::string(expected),
                    "format {name} of {expr}"
                );
            }
        }
    }

    #[test]
    fn default_str_matches_python_str() {
        let document = golden::load("json/default_str.json");
        for case in golden::cases(&document) {
            let kind = golden::string(golden::get(case, "kind"));
            let text = golden::string(golden::get(case, "value"));
            let rendered = match kind.as_str() {
                "datetime" if text.len() > 19 && (text[19..].contains('+') || text[19..].contains('-')) => {
                    py_str_datetime(&chrono::DateTime::parse_from_rfc3339(&text).unwrap())
                }
                "datetime" => py_str_naive_datetime(&text.parse().unwrap()),
                "date" => py_str_date(&text.parse().unwrap()),
                "path" => text.clone(),
                other => panic!("unknown kind {other}"),
            };
            assert_eq!(rendered, golden::string(golden::get(case, "str")), "{kind} {text}");
            let mut object = super::Dict::new();
            object.insert("at", Value::Str(rendered));
            object.insert("n", Value::Int(1));
            assert_eq!(
                dumps(&Value::Dict(object), &DumpOptions::hash()).unwrap(),
                golden::string(golden::get(case, "hash_of_object"))
            );
        }
    }

    #[test]
    fn float_repr_follows_python() {
        for (value, expected) in [
            (0.1, "0.1"),
            (1.0, "1.0"),
            (-0.0, "-0.0"),
            (1e16, "1e+16"),
            (1e15, "1000000000000000.0"),
            (1.5e-7, "1.5e-07"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (12_345_678_901_234_567.0, "1.2345678901234568e+16"),
            (1e300, "1e+300"),
            (2.5e-320, "2.5e-320"),
            (123.456, "123.456"),
        ] {
            assert_eq!(float_repr(value), expected);
        }
    }

    #[test]
    fn loads_keeps_python_distinctions() {
        assert_eq!(loads("1").unwrap(), Value::Int(1));
        assert_eq!(loads("1.0").unwrap(), Value::Float(1.0));
        assert_eq!(loads("1e2").unwrap(), Value::Float(100.0));
        assert_eq!(
            loads("-9223372036854775809").unwrap(),
            Value::Int(-9_223_372_036_854_775_809)
        );
        assert_eq!(loads("\"\\ud83d\\ude00\"").unwrap(), Value::Str("😀".into()));
        assert!(loads("\"\\ud83d\"").is_err());
        assert!(loads("\"a\nb\"").is_err());
        assert!(loads("[1,]").is_err());
        assert!(loads("01").is_err());
        assert!(loads("1 2").is_err());
        let duplicated = loads(r#"{"a": 1, "b": 2, "a": 3}"#).unwrap();
        assert_eq!(dumps(&duplicated, &DumpOptions::compact()).unwrap(), r#"{"a":3,"b":2}"#);
    }

    #[test]
    fn mixed_key_types_cannot_be_sorted() {
        let mut dict = super::Dict::new();
        dict.insert(Key::Int(1), Value::Null);
        dict.insert("a", Value::Null);
        assert_eq!(
            dumps(&Value::Dict(dict.clone()), &DumpOptions::hash()),
            Err(DumpError::MixedKeyTypes)
        );
        assert_eq!(
            dumps(&Value::Dict(dict), &DumpOptions::compact()).unwrap(),
            r#"{"1":null,"a":null}"#
        );
    }

    #[test]
    fn decode_errors_read_like_python() {
        let document = golden::load("pyweb/json_errors.json");
        for case in golden::cases(&document) {
            let doc = golden::string(golden::get(case, "doc"));
            let message = loads(&doc).err().map(|error| error.python_message(&doc));
            assert_eq!(message.as_deref(), golden::get(case, "message").as_str(), "{doc:?}");
        }
    }
}
