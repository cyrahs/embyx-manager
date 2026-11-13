//! The path and URL normalizers the section validators share.
//!
//! Python origin: `normalize_http_base_url`, `normalize_absolute_path`,
//! `normalize_relative_subpath` and `normalize_absolute_paths` in `config/models.py`. Each
//! returns the normalized value or the `ValueError` message.

use crate::pypath::PyPath;
use crate::pytext::{py_repr, py_rstrip_chars, py_strip};
use crate::pyurl::{self, SplitResult};

/// `'..' in PurePosixPath(normalized).parts`.
fn has_parent_part(normalized: &str) -> bool {
    normalized.split('/').any(|part| part == "..")
}

/// `normalize_http_base_url`: an absolute HTTP(S) URL without credentials, query or
/// fragment, its path's trailing slashes removed; `''` stays `''`.
pub fn normalize_http_base_url(name: &str, value: &str) -> Result<String, String> {
    if value.is_empty() {
        return Ok(String::new());
    }
    let invalid = || format!("{name} must be an absolute HTTP(S) URL");
    let parsed = pyurl::urlsplit(value, "").map_err(|_| invalid())?;
    let port = parsed.port().map_err(|_| invalid())?;
    if !(parsed.scheme == "http" || parsed.scheme == "https") || parsed.hostname().is_none() || port == Some(0) {
        return Err(invalid());
    }
    if parsed.username().is_some()
        || parsed.password().is_some()
        || !parsed.query.is_empty()
        || !parsed.fragment.is_empty()
    {
        return Err(format!("{name} must not include credentials, a query, or a fragment"));
    }
    let path = py_rstrip_chars(&parsed.path, &['/']).to_owned();
    Ok(pyurl::urlunsplit(&SplitResult {
        scheme: parsed.scheme,
        netloc: parsed.netloc,
        path,
        query: String::new(),
        fragment: String::new(),
    }))
}

/// `normalize_absolute_path`: an absolute path with no trailing slash, `.` parts and doubled
/// slashes removed, refused with a null byte or a `..` part; blank stays `''`.
pub fn normalize_absolute_path(name: &str, value: &str) -> Result<String, String> {
    let trimmed = py_strip(value);
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    if !trimmed.starts_with('/') {
        return Err(format!("{name} must be an absolute path"));
    }
    if trimmed.contains('\0') {
        return Err(format!("{name} must not contain a null byte"));
    }
    let normalized = PyPath::new(trimmed).as_str().to_owned();
    if has_parent_part(&normalized) {
        return Err(format!("{name} must not contain \"..\""));
    }
    Ok(normalized)
}

/// `normalize_relative_subpath`: a non-empty path relative to a configured root.
pub fn normalize_relative_subpath(name: &str, value: &str) -> Result<String, String> {
    let trimmed = py_rstrip_chars(py_strip(value), &['/']);
    if trimmed.is_empty() {
        return Err(format!("{name} must not be empty"));
    }
    if trimmed.starts_with('/') {
        return Err(format!("{name} must be relative to the configured root"));
    }
    if trimmed.contains('\0') {
        return Err(format!("{name} must not contain a null byte"));
    }
    let normalized = PyPath::new(trimmed).as_str().to_owned();
    if has_parent_part(&normalized) {
        return Err(format!("{name} must not contain \"..\""));
    }
    Ok(normalized)
}

/// `normalize_absolute_paths`: blanks dropped, each normalized, a repeated path refused.
pub fn normalize_absolute_paths(name: &str, values: &[String]) -> Result<Vec<String>, String> {
    let mut normalized = Vec::with_capacity(values.len());
    for value in values.iter().filter(|value| !py_strip(value).is_empty()) {
        normalized.push(normalize_absolute_path(name, value)?);
    }
    let mut seen = std::collections::HashSet::new();
    if !normalized.iter().all(|path| seen.insert(path.as_str())) {
        return Err(format!("{name} must not repeat a path"));
    }
    Ok(normalized)
}

/// `{value!r}` in a validator's message.
pub fn repr(value: &str) -> String {
    py_repr(value)
}
