//! `body` against Python: content types, `json.loads` of bytes, the request models, and
//! the recursion limit.

use embyx_core::golden;
use embyx_core::pyjson::{self, DumpOptions, Value};

use super::{
    JsonBody, JsonError, MAX_JSON_DEPTH, TestConnectionRequest, UpdateSectionRequest, is_json_content_type, loads_bytes,
};

#[test]
fn content_types_are_read_as_email_message_reads_them() {
    let document = golden::load("api/content_types.json");
    for case in golden::cases(&document) {
        let value = golden::string(golden::get(case, "content_type"));
        assert_eq!(
            Value::Bool(is_json_content_type(&value)),
            *golden::get(case, "json"),
            "{value:?}"
        );
    }
}

/// Bodies Python reads and this port refuses: a lone surrogate, escaped or encoded, and
/// an integer past 128 bits (D55).
const KNOWN_DIVERGENT: &[&str] = &[
    "22eda08022",
    "225c756438303022",
    "313030303030303030303030303030303030303030303030303030303030303030303030303030303030",
];

#[test]
fn bodies_decode_as_json_loads_decodes_bytes() {
    let document = golden::load("api/json_bodies.json");
    for case in golden::cases(&document) {
        let hex_text = golden::string(golden::get(case, "hex"));
        let bytes = hex::decode(&hex_text).expect("hex");
        let outcome = loads_bytes(&bytes);
        if KNOWN_DIVERGENT.contains(&hex_text.as_str()) {
            assert!(case.get("value").is_some(), "{hex_text} is read by Python");
            assert!(outcome.is_err(), "{hex_text}");
            continue;
        }
        match case.get("error").map(golden::string).as_deref() {
            Some("JSONDecodeError") => assert_eq!(outcome, Err(JsonError::Decode), "{hex_text}"),
            Some(_) => assert_eq!(outcome, Err(JsonError::Unreadable), "{hex_text}"),
            None => {
                let value = outcome.unwrap_or_else(|error| panic!("{hex_text}: {error:?}"));
                // The golden holds Python's repr; compare through the parsed value.
                let repr = golden::string(golden::get(case, "value"));
                let expected = match repr.as_str() {
                    "{}" => Value::Dict(embyx_core::pyjson::Dict::new()),
                    "'é'" => Value::from("é"),
                    "nan" => Value::Float(f64::NAN),
                    "inf" => Value::Float(f64::INFINITY),
                    "-inf" => Value::Float(f64::NEG_INFINITY),
                    "1.0" => Value::Float(1.0),
                    "1e+30" => Value::Float(1e30),
                    other => panic!("no expectation for {other}"),
                };
                let dump = |value: &Value| pyjson::dumps(value, &DumpOptions::compact()).expect("dumps");
                assert_eq!(dump(&value), dump(&expected), "{hex_text}");
            }
        }
    }
}

#[test]
fn request_models_validate_as_pydantic_does() {
    let document = golden::load("api/request_models.json");
    for case in golden::cases(&document) {
        let model = golden::string(golden::get(case, "model"));
        let body = JsonBody::Json(golden::get(case, "body").clone());
        let ok = *golden::get(case, "ok") == Value::Bool(true);
        let validated = case.get("validated");
        match model.as_str() {
            "UpdateSectionRequest" => {
                let got = UpdateSectionRequest::validate(&body);
                assert_eq!(got.is_some(), ok, "{model} {body:?}");
                if let (Some(got), Some(validated)) = (got, validated) {
                    assert_eq!(Some(&Value::Dict(got.values)), validated.get("values"));
                    let version = got.version.map_or(Value::Null, |version| Value::Int(version.into()));
                    assert_eq!(Some(&version), validated.get("version"));
                }
            }
            "TestConnectionRequest" => {
                let got = TestConnectionRequest::validate(&body);
                assert_eq!(got.is_some(), ok, "{model} {body:?}");
                if let (Some(got), Some(validated)) = (got, validated) {
                    assert_eq!(Some(&Value::Dict(got.values)), validated.get("values"));
                }
            }
            other => panic!("unknown model {other}"),
        }
    }
    // No body, or bytes that were not parsed: the model is missing or of the wrong type.
    for body in [JsonBody::Missing, JsonBody::Raw] {
        assert!(UpdateSectionRequest::validate(&body).is_none());
        assert!(TestConnectionRequest::validate(&body).is_none());
    }
}

/// Runs on a thread with the server's stack size, as the serve runtime's workers have.
fn on_server_stack<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(crate::serve::WORKER_STACK_BYTES)
        .spawn(work)
        .expect("a thread")
        .join()
        .expect("no panic")
}

#[test]
fn nesting_past_the_recursion_limit_is_unreadable() {
    let nested = |depth: usize| format!("{{\"values\":{}{}}}", "[".repeat(depth - 1), "]".repeat(depth - 1));
    let (at_limit, past_limit) = on_server_stack(move || {
        (
            loads_bytes(nested(MAX_JSON_DEPTH).as_bytes()).map(|_| ()),
            loads_bytes(nested(MAX_JSON_DEPTH + 1).as_bytes()).map(|_| ()),
        )
    });
    assert_eq!(at_limit, Ok(()));
    assert_eq!(past_limit, Err(JsonError::Unreadable));
    // A syntax error before the deep part is still a decode error; a deep part inside a
    // string is no nesting at all.
    let early_error = format!("{{\"values\" 1 {}", "[".repeat(MAX_JSON_DEPTH + 5));
    assert_eq!(loads_bytes(early_error.as_bytes()), Err(JsonError::Decode));
    let in_string = format!("{{\"values\":\"{}\"}}", "[".repeat(MAX_JSON_DEPTH + 5));
    assert!(loads_bytes(in_string.as_bytes()).is_ok());
    let escaped_quote = format!("[\"\\\"{}\"]", "{".repeat(MAX_JSON_DEPTH + 5));
    assert!(loads_bytes(escaped_quote.as_bytes()).is_ok());
}
