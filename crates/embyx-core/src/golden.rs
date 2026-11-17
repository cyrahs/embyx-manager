//! Reading the golden files under `goldens/` in tests.
//!
//! Goldens are produced by `tools/goldens/generate.py` from the Python baseline and
//! parsed here with the Python-compatible reader, so `1` and `1.0` stay apart. These
//! helpers panic on a malformed golden: they exist for tests, where that is a failure.
//!
//! Copied from fav (`fav-core/src/golden.rs`); see docs/CONVENTIONS.md, "Code copied from fav".

use std::path::PathBuf;

use crate::pyjson::{self, Value};

/// The `goldens` directory of this checkout.
pub fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../goldens")
}

/// Load and parse `goldens/<relative>`.
pub fn load(relative: &str) -> Value {
    let path = dir().join(relative);
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("cannot read golden {}: {error}", path.display()));
    pyjson::loads(&text).unwrap_or_else(|error| panic!("golden {relative} is not JSON: {error}"))
}

/// The `cases` list of a golden document.
pub fn cases(document: &Value) -> &[Value] {
    list(get(document, "cases"))
}

/// A required key of a JSON object.
pub fn get<'a>(value: &'a Value, key: &str) -> &'a Value {
    value
        .get(key)
        .unwrap_or_else(|| panic!("golden value has no key {key:?}"))
}

pub fn list(value: &Value) -> &[Value] {
    value
        .as_list()
        .unwrap_or_else(|| panic!("expected a list, got {value:?}"))
}

pub fn string(value: &Value) -> String {
    value
        .as_str()
        .unwrap_or_else(|| panic!("expected a string, got {value:?}"))
        .to_owned()
}

pub fn int(value: &Value) -> i128 {
    match value {
        Value::Int(int) => *int,
        other => panic!("expected an int, got {other:?}"),
    }
}

pub fn uint(value: &Value) -> u32 {
    u32::try_from(int(value)).unwrap_or_else(|_| panic!("expected a u32, got {value:?}"))
}
