//! Python's view of a decoded JSON value: `str(x)`, `int(x)`, `x.get(key)`, `for y in x`,
//! truthiness, and the exceptions they raise on the wrong type.
//!
//! Python origin: the dict walking of `clients/avbase.py` and `clients/emby.py`, which read
//! site answers with `.get(...)`, `str(...)` and `int(...)` and let a `TypeError`,
//! `AttributeError` or `KeyError` through when a value has an unexpected type.

use embyx_core::pyjson::{Key, Value};
use embyx_core::pytext::py_repr;

/// An exception the walk raises: class and `str(exc)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Raised {
    pub class: &'static str,
    pub message: String,
}

impl Raised {
    pub fn new(class: &'static str, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
        }
    }
}

/// `''`, the usual default of `x or ''`.
pub static EMPTY_STR: Value = Value::Str(String::new());

/// `type(x).__name__`.
pub use embyx_core::pyjson::type_name;

/// `x.get(key)` (`None` for a missing key); `AttributeError` when `x` is not a dict.
pub fn get<'a>(value: &'a Value, key: &str) -> Result<&'a Value, Raised> {
    match value {
        Value::Dict(dict) => Ok(dict.get(key).unwrap_or(&Value::Null)),
        other => Err(Raised::new(
            "AttributeError",
            format!("'{}' object has no attribute 'get'", type_name(other)),
        )),
    }
}

/// `x[key]` on a dict: `KeyError` for a missing key.
pub fn item<'a>(value: &'a Value, key: &str) -> Result<&'a Value, Raised> {
    match value {
        Value::Dict(dict) => dict.get(key).ok_or_else(|| Raised::new("KeyError", py_repr(key))),
        Value::List(_) => Err(Raised::new(
            "TypeError",
            "list indices must be integers or slices, not str",
        )),
        Value::Str(_) => Err(Raised::new("TypeError", "string indices must be integers, not 'str'")),
        other => Err(Raised::new(
            "TypeError",
            format!("'{}' object is not subscriptable", type_name(other)),
        )),
    }
}

/// `bool(x)`.
pub fn truthy(value: &Value) -> bool {
    value.is_truthy()
}

/// `x or default`.
pub fn or<'a>(value: &'a Value, default: &'a Value) -> &'a Value {
    if truthy(value) { value } else { default }
}

/// `for y in (x or [])`: a list's items, a dict's keys, a string's characters; `TypeError`
/// for a number.
pub fn iterate(value: &Value) -> Result<Vec<Value>, Raised> {
    if !truthy(value) {
        return Ok(Vec::new());
    }
    match value {
        Value::List(items) => Ok(items.clone()),
        Value::Dict(dict) => Ok(dict
            .iter()
            .map(|(key, _)| match key {
                Key::Str(text) => Value::Str(text.clone()),
                other => Value::Str(format!("{other:?}")),
            })
            .collect()),
        Value::Str(text) => Ok(text.chars().map(|ch| Value::Str(ch.to_string())).collect()),
        other => Err(Raised::new(
            "TypeError",
            format!("'{}' object is not iterable", type_name(other)),
        )),
    }
}

/// `str(x)`.
pub fn py_str(value: &Value) -> String {
    match value {
        Value::Str(text) => text.clone(),
        other => repr(other),
    }
}

/// `repr(x)`.
pub fn repr(value: &Value) -> String {
    match value {
        Value::Null => "None".to_owned(),
        Value::Bool(true) => "True".to_owned(),
        Value::Bool(false) => "False".to_owned(),
        Value::Int(number) => number.to_string(),
        Value::Float(number) => embyx_core::pyjson::float_repr(*number),
        Value::Str(text) => py_repr(text),
        Value::List(items) => format!("[{}]", items.iter().map(repr).collect::<Vec<_>>().join(", ")),
        Value::Dict(dict) => format!(
            "{{{}}}",
            dict.iter()
                .map(|(key, value)| {
                    let key = match key {
                        Key::Str(text) => py_repr(text),
                        other => format!("{other:?}"),
                    };
                    format!("{key}: {}", repr(value))
                })
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// `int(x)`.
pub fn py_int(value: &Value) -> Result<i128, Raised> {
    match value {
        Value::Bool(flag) => Ok(i128::from(*flag)),
        Value::Int(number) => Ok(*number),
        Value::Float(number) if number.is_nan() => {
            Err(Raised::new("ValueError", "cannot convert float NaN to integer"))
        }
        Value::Float(number) if number.is_infinite() => {
            Err(Raised::new("OverflowError", "cannot convert float infinity to integer"))
        }
        #[expect(clippy::cast_possible_truncation, reason = "int(float) truncates; JSON numbers fit")]
        Value::Float(number) => Ok(number.trunc() as i128),
        Value::Str(text) => embyx_core::pytext::py_int(text).map_err(|message| Raised::new("ValueError", message)),
        other => Err(Raised::new(
            "TypeError",
            format!(
                "int() argument must be a string, a bytes-like object or a real number, not '{}'",
                type_name(other)
            ),
        )),
    }
}

/// `isinstance(x, int)` (a `bool` is one) and its value.
pub fn as_int(value: &Value) -> Option<i128> {
    match value {
        Value::Bool(flag) => Some(i128::from(*flag)),
        Value::Int(number) => Some(*number),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use embyx_core::pyjson::loads;

    #[test]
    fn str_and_int_follow_python() {
        let value = loads(r#"[1, 1.5, true, null, "x", {"a": [1]}]"#).unwrap();
        let items = iterate(&value).unwrap();
        let rendered: Vec<String> = items.iter().map(py_str).collect();
        assert_eq!(rendered, ["1", "1.5", "True", "None", "x", "{'a': [1]}"]);
        assert_eq!(py_int(&items[1]).unwrap(), 1);
        assert_eq!(py_int(&items[2]).unwrap(), 1);
        assert_eq!(py_int(&items[3]).unwrap_err().class, "TypeError");
        assert_eq!(py_int(&Value::from(" 42 ")).unwrap(), 42);
        assert_eq!(py_int(&Value::from("x")).unwrap_err().class, "ValueError");
    }

    #[test]
    fn get_and_item_raise_like_python() {
        let value = loads(r#"{"a": 1}"#).unwrap();
        assert_eq!(get(&value, "b").unwrap(), &Value::Null);
        assert_eq!(item(&value, "b").unwrap_err(), Raised::new("KeyError", "'b'"));
        assert_eq!(
            get(&Value::List(Vec::new()), "a").unwrap_err().message,
            "'list' object has no attribute 'get'"
        );
        assert_eq!(
            iterate(&Value::Int(3)).unwrap_err().message,
            "'int' object is not iterable"
        );
    }
}
