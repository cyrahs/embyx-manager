//! Request bodies as FastAPI reads them, and the two request models of the config API.
//!
//! Python origin: `fastapi/routing.py` (`get_request_handler`: the body is read and parsed
//! before any dependency runs), `starlette.requests.Request.json` (`json.loads` of the
//! bytes), and `config/api.py` (`UpdateSectionRequest`, `TestConnectionRequest`, validated
//! by pydantic in Python mode after the dependencies).
//!
//! A body is parsed as JSON only when its content type is `application/json` or
//! `application/*+json` (as `email.message` reads the header); without a content type the
//! bytes are kept (`strict_content_type`), which no model accepts. An empty body is a
//! missing one. A JSON syntax error is FastAPI's 422 `invalid_request`; bytes that are not
//! text in the detected encoding, or nesting past Python's recursion limit, are its 400.

use embyx_core::config::lax::{Refusal, lax_int};
use embyx_core::pyjson::{self, Dict, JsonEncoding, LoadError, Value};
use embyx_core::pytext::py_strip;
use http::StatusCode;

use crate::http::{Request, Response, body_parse_error, error_envelope, json};

/// The deepest nesting `json.loads` reaches before `RecursionError` in FastAPI's request
/// path (measured on the baseline under its test client, `goldens/scenarios/api/shell.json`;
/// D55).
pub const MAX_JSON_DEPTH: usize = 9994;

/// What the route's body parameter receives.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonBody {
    /// No bytes at all: the required body is missing.
    Missing,
    /// Parsed JSON.
    Json(Value),
    /// Bytes that were not parsed (no JSON content type): no model accepts them.
    Raw,
}

impl JsonBody {
    /// The value a model is validated against, `None` when the body cannot be one.
    pub fn value(&self) -> Option<&Value> {
        match self {
            Self::Json(value) => Some(value),
            Self::Missing | Self::Raw => None,
        }
    }
}

/// The validation handler's answer: `{"error": {"code": "invalid_request"}}` with 422.
pub fn invalid_request() -> Response {
    json(StatusCode::UNPROCESSABLE_ENTITY, &error_envelope("invalid_request"))
}

/// `message.get_content_maintype() == 'application'` and a subtype of `json` or `*+json`,
/// with the header read as `email.message` reads it: the part before the first `;`,
/// stripped and lower-cased, must hold exactly one `/` (else it is `text/plain`).
pub fn is_json_content_type(value: &str) -> bool {
    let main = py_strip(value.split(';').next().unwrap_or("")).to_lowercase();
    if main.matches('/').count() != 1 {
        return false;
    }
    let (maintype, subtype) = main.split_once('/').expect("one slash");
    maintype == "application" && (subtype == "json" || subtype.ends_with("+json"))
}

/// Where the nesting first goes past [`MAX_JSON_DEPTH`]: the character index of that
/// bracket, scanning outside strings.
fn too_deep_at(text: &str) -> Option<usize> {
    let mut depth = 0_usize;
    let mut in_string = false;
    let mut escaped = false;
    for (index, ch) in text.chars().enumerate() {
        if in_string {
            match ch {
                _ if escaped => escaped = false,
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '[' | '{' => {
                depth += 1;
                if depth > MAX_JSON_DEPTH {
                    return Some(index);
                }
            }
            ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    None
}

/// Why a body could not be read as JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonError {
    /// `json.JSONDecodeError`: FastAPI's 422.
    Decode,
    /// `UnicodeDecodeError` or `RecursionError`: FastAPI's 400.
    Unreadable,
}

/// The position a `json.loads` error names.
fn error_position(error: &LoadError) -> usize {
    match *error {
        LoadError::ExpectingValue(pos)
        | LoadError::ExtraData(pos)
        | LoadError::UnterminatedString(pos)
        | LoadError::ControlCharacter(pos)
        | LoadError::InvalidEscape(pos)
        | LoadError::ExpectingPropertyName(pos)
        | LoadError::ExpectingColon(pos)
        | LoadError::ExpectingComma(pos)
        | LoadError::TrailingCommaArray(pos)
        | LoadError::TrailingCommaObject(pos)
        | LoadError::LoneSurrogate(pos)
        | LoadError::IntegerTooLarge(pos) => pos,
    }
}

/// `json.loads(text)` with Python's recursion limit: a syntax error before the bracket
/// that goes too deep is still a decode error, the bracket itself is `RecursionError`.
fn loads_limited(text: &str) -> Result<Value, JsonError> {
    if let Some(deep) = too_deep_at(text) {
        let prefix: String = text.chars().take(deep).collect();
        return match pyjson::loads(&prefix) {
            Err(error) if error_position(&error) < deep => Err(JsonError::Decode),
            _ => Err(JsonError::Unreadable),
        };
    }
    pyjson::loads(text).map_err(|_| JsonError::Decode)
}

/// `json.loads(body_bytes)`: the encoding detected as `json.detect_encoding` does, then the
/// text parsed. A lone surrogate (escaped or encoded) and an integer past 128 bits are
/// refused as decode errors, where Python reads them (D55).
pub fn loads_bytes(bytes: &[u8]) -> Result<Value, JsonError> {
    let text = match pyjson::detect_json_encoding(bytes) {
        JsonEncoding::Utf8(utf8) => std::str::from_utf8(utf8).map_err(|_| JsonError::Unreadable)?.to_owned(),
        JsonEncoding::Decoded(Some(text)) => text,
        JsonEncoding::Decoded(None) => return Err(JsonError::Unreadable),
    };
    loads_limited(&text)
}

/// Read the body as the route's body parameter: `Err` is the answer FastAPI gives before
/// any dependency runs.
pub async fn read_json(request: &mut Request) -> Result<JsonBody, Response> {
    let bytes = request.body_bytes().await.map_err(|_| body_parse_error())?;
    if bytes.is_empty() {
        return Ok(JsonBody::Missing);
    }
    match request.header("content-type") {
        Some(content_type) if !content_type.is_empty() && is_json_content_type(&content_type) => {}
        _ => return Ok(JsonBody::Raw),
    }
    match loads_bytes(&bytes) {
        Ok(value) => Ok(JsonBody::Json(value)),
        Err(JsonError::Decode) => Err(invalid_request()),
        Err(JsonError::Unreadable) => Err(body_parse_error()),
    }
}

/// `dict[str, Any]` in Python mode: a JSON object as it is.
fn any_dict(value: Option<&Value>) -> Option<Dict> {
    match value {
        Some(Value::Dict(dict)) => Some(dict.clone()),
        _ => None,
    }
}

/// Only these keys (`extra='forbid'`), the body an object.
fn fields<'a>(body: &'a JsonBody, allowed: &[&str]) -> Option<&'a Dict> {
    let Some(Value::Dict(dict)) = body.value() else {
        return None;
    };
    dict.iter()
        .all(|(key, _)| key.as_str().is_some_and(|key| allowed.contains(&key)))
        .then_some(dict)
}

/// `UpdateSectionRequest`: `values: dict[str, Any]` (required) and
/// `version: int | None = Field(default=None, ge=0)`.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateSectionRequest {
    pub values: Dict,
    pub version: Option<i64>,
}

impl UpdateSectionRequest {
    /// `None` when pydantic refuses the body (an integer past `i64` included, D55).
    pub fn validate(body: &JsonBody) -> Option<Self> {
        let dict = fields(body, &["values", "version"])?;
        let values = any_dict(dict.get("values"))?;
        let version = match dict.get("version") {
            None | Some(Value::Null) => None,
            Some(value) => {
                let version = lax_int(value).map_err(|_: Refusal| ()).ok()?;
                (version >= 0).then_some(version)?;
                Some(version)
            }
        };
        Some(Self { values, version })
    }
}

/// `TestConnectionRequest`: `values: dict[str, Any] = {}`.
#[derive(Debug, Clone, PartialEq)]
pub struct TestConnectionRequest {
    pub values: Dict,
}

impl TestConnectionRequest {
    pub fn validate(body: &JsonBody) -> Option<Self> {
        let dict = fields(body, &["values"])?;
        let values = match dict.get("values") {
            None => Dict::new(),
            value => any_dict(value)?,
        };
        Some(Self { values })
    }
}

#[cfg(test)]
mod tests;
