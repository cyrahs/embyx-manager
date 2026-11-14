//! `_redact_error_message(exc)`: an exception as short diagnostic text with credential-shaped
//! fragments removed, for a plan's `ActorPlan.error_message`.
//!
//! Python origin: `src/embyx_manager/fill_actor/service.py` (`QUERY_STRING_RE`, `BEARER_RE`,
//! `SENSITIVE_ASSIGNMENT_RE`, `MAX_ERROR_MESSAGE_LENGTH`). The text is stored in the plan, so it
//! is Python's byte for byte (`goldens/fill_actor/service.json`).

use std::sync::LazyLock;

use embyx_core::pyre;
use embyx_core::pytext::py_split;
use regex::Regex;

/// `MAX_ERROR_MESSAGE_LENGTH`.
pub const MAX_ERROR_MESSAGE_LENGTH: usize = 200;

// Query strings carry signed URLs and access tokens; keep the endpoint, drop the parameters.
static QUERY_STRING_RE: LazyLock<Regex> = LazyLock::new(|| compile(r"(\bhttps?://[^\s?#]+)\?\S*"));
static SENSITIVE_ASSIGNMENT_RE: LazyLock<Regex> = LazyLock::new(|| {
    compile(
        r"\b(authorization|auth|api[-_ ]?key|apikey|access[-_ ]?key|token|secret|password|passwd|pwd|cookie|set-cookie|session|sessionid|signature)\b\s*[:=]\s*\S+",
    )
});
static BEARER_RE: LazyLock<Regex> = LazyLock::new(|| compile(r"\bbearer\s+\S+"));

/// `re.compile('(?i)' + pattern)`, with Python's `İ`/`ı` folding (D15).
fn compile(pattern: &str) -> Regex {
    pyre::compile(&pyre::python_ignore_case(pattern, true), true, false)
}

/// `_redact_error_message(exc)` for an exception of class `class` whose `str()` is `text`.
pub fn redact_error_message(class: &str, text: &str) -> Option<String> {
    let detail = py_split(text).collect::<Vec<_>>().join(" ");
    let message = if detail.is_empty() {
        class.to_owned()
    } else {
        format!("{class}: {detail}")
    };
    let message = QUERY_STRING_RE.replace_all(&message, "${1}?[redacted]");
    // Bearer first: `Authorization: Bearer <token>` would otherwise only lose the word "Bearer".
    let message = BEARER_RE.replace_all(&message, "bearer [redacted]");
    let message = SENSITIVE_ASSIGNMENT_RE.replace_all(&message, "${1}=[redacted]");
    let mut message = message.into_owned();
    if message.chars().count() > MAX_ERROR_MESSAGE_LENGTH {
        message = message.chars().take(MAX_ERROR_MESSAGE_LENGTH - 1).collect::<String>() + "…";
    }
    Some(message).filter(|message| !message.is_empty())
}
