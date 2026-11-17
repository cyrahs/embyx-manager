//! The worker against the goldens `tools/goldens/areas/home_worker.py` wrote from Python:
//! argument lists, ffprobe readings, the cache, the settings, the requests, a run's pieces
//! and the claim loop (PLAN.md, cross-version contract 5).

mod cache;
mod commands;
mod requests;
mod settings;
mod work;

use std::ffi::OsString;

use embyx_core::golden;
use embyx_core::pyjson::{Dict, Value};

use crate::py::PyError;

/// `goldens/merge/home_worker_<name>.json`'s cases.
pub(crate) fn cases(name: &str) -> Vec<Value> {
    golden::cases(&golden::load(&format!("merge/home_worker_{name}.json"))).to_vec()
}

/// The goldens were written on POSIX; a Windows path joins with `\`.
pub(crate) fn slashes(text: &str) -> String {
    if cfg!(windows) {
        text.replace('\\', "/")
    } else {
        text.to_owned()
    }
}

pub(crate) fn strings(argv: &[OsString]) -> Value {
    Value::List(
        argv.iter()
            .map(|item| Value::Str(slashes(&item.to_string_lossy())))
            .collect(),
    )
}

pub(crate) fn dict(value: &Value) -> Dict {
    value
        .as_dict()
        .unwrap_or_else(|| panic!("expected an object, got {value:?}"))
        .clone()
}

pub(crate) fn float(value: &Value) -> f64 {
    match value {
        Value::Float(number) => *number,
        #[allow(clippy::cast_precision_loss, reason = "the goldens' ints are small")]
        Value::Int(number) => *number as f64,
        other => panic!("expected a number, got {other:?}"),
    }
}

pub(crate) fn object(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut dict = Dict::new();
    for (key, value) in entries {
        dict.insert(key, value);
    }
    Value::Dict(dict)
}

/// `{'type': ..., 'message': ...}` as `outcome()` writes an exception.
pub(crate) fn error(class: &str, message: &str) -> Value {
    object([
        ("type", Value::Str(class.to_owned())),
        ("message", Value::Str(message.to_owned())),
    ])
}

pub(crate) fn py_error(error: &PyError) -> Value {
    self::error(error.class, &error.message)
}

/// The golden's `value` or `error`, whichever it has, as one value to compare with.
pub(crate) fn expected(case: &Value) -> Value {
    if let Some(value) = case.get("value") {
        object([("value", value.clone())])
    } else if let Some(error) = case.get("error") {
        object([("error", error.clone())])
    } else if let Some(exit) = case.get("exit") {
        object([("exit", exit.clone())])
    } else {
        panic!("the case has no outcome: {case:?}")
    }
}

pub(crate) fn value(value: Value) -> Value {
    object([("value", value)])
}

pub(crate) fn raised(error: Value) -> Value {
    object([("error", error)])
}
