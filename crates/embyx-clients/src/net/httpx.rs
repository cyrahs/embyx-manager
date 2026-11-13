//! httpx-shaped errors over a plain `reqwest` client, for the sources that call reqwest
//! directly rather than through [`crate::net::HttpClient`].
//!
//! Python origin: `httpx.HTTPError` and its subclasses as the sources catch and report
//! them, `Response.raise_for_status()`'s message and `httpx.codes.get_reason_phrase`.
//! The class of a transport failure comes from [`ErrorClass::of`], the one mapping every client uses.
//!
//! Copied from fav (`fav-net/src/httpx.rs`); see docs/CONVENTIONS.md, "Code copied from fav". `D` numbers
//! marked fav's are fav's own DIVERGENCES.md entries.

use std::fmt;
use std::path::Path;

use futures_util::StreamExt as _;
use reqwest::Response;
use tokio::io::AsyncWriteExt as _;

use crate::net::ErrorClass;
use crate::net::download::PyOsError;

/// An `httpx.HTTPError`: a transport failure or a `raise_for_status()` rejection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpError {
    /// A transport error, under httpx's class name (`ConnectError`, `ReadTimeout`, ...).
    Transport {
        class: &'static str,
        message: String,
        host: String,
    },
    /// `HTTPStatusError` from `raise_for_status()`.
    Status { status: u16, message: String },
}

impl HttpError {
    /// A reqwest failure, by the httpx class [`ErrorClass::of`] gives it.
    pub fn transport(error: &reqwest::Error) -> Self {
        let class = ErrorClass::of(error).name();
        // httpx's timeouts carry no text; other transport errors carry the OS error's.
        let message = if error.is_timeout() {
            String::new()
        } else {
            innermost_message(error)
        };
        let host = error
            .url()
            .and_then(|url| url.host_str())
            .unwrap_or_default()
            .to_owned();
        Self::Transport { class, message, host }
    }

    /// The Python class name.
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Transport { class, .. } => class,
            Self::Status { .. } => "HTTPStatusError",
        }
    }

    /// `str(exc)`.
    pub fn message(&self) -> &str {
        match self {
            Self::Transport { message, .. } | Self::Status { message, .. } => message,
        }
    }

    /// One of httpx's `ConnectError`, `TimeoutException` subclasses or `RemoteProtocolError`.
    pub fn is_connect_timeout_or_protocol(&self) -> bool {
        matches!(
            self,
            Self::Transport {
                class: "ConnectError"
                    | "ConnectTimeout"
                    | "ReadTimeout"
                    | "WriteTimeout"
                    | "PoolTimeout"
                    | "RemoteProtocolError",
                ..
            }
        )
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for HttpError {}

fn innermost_message(error: &reqwest::Error) -> String {
    let mut current: &dyn std::error::Error = error;
    while let Some(source) = current.source() {
        current = source;
    }
    current.to_string()
}

/// `response.raise_for_status()` with httpx 0.28's message.
pub fn raise_for_status(response: &Response) -> Result<(), HttpError> {
    let status = response.status().as_u16();
    let location = response
        .headers()
        .get("location")
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned());
    match status_error_message(status, response.url().as_str(), location.as_deref()) {
        None => Ok(()),
        Some(message) => Err(HttpError::Status { status, message }),
    }
}

/// The text of the `HTTPStatusError` `raise_for_status()` raises, `None` for a 2xx.
pub fn status_error_message(status: u16, url: &str, location: Option<&str>) -> Option<String> {
    if (200..300).contains(&status) {
        return None;
    }
    let error_type = match status / 100 {
        1 => "Informational response",
        3 => "Redirect response",
        4 => "Client error",
        5 => "Server error",
        _ => "Invalid status code",
    };
    let redirect = match location {
        // `has_redirect_location`: a redirect status that names where to go.
        Some(location) if matches!(status, 301 | 302 | 303 | 307 | 308) => format!("Redirect location: '{location}'\n"),
        _ => String::new(),
    };
    Some(format!(
        "{error_type} '{status} {}' for url '{url}'\n{redirect}For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/{status}",
        reason_phrase(status)
    ))
}

/// `httpx.codes.get_reason_phrase(status)`.
pub fn reason_phrase(status: u16) -> &'static str {
    match status {
        100 => "Continue",
        101 => "Switching Protocols",
        102 => "Processing",
        103 => "Early Hints",
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        203 => "Non-Authoritative Information",
        204 => "No Content",
        205 => "Reset Content",
        206 => "Partial Content",
        207 => "Multi-Status",
        208 => "Already Reported",
        226 => "IM Used",
        300 => "Multiple Choices",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        305 => "Use Proxy",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        402 => "Payment Required",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        407 => "Proxy Authentication Required",
        408 => "Request Timeout",
        409 => "Conflict",
        410 => "Gone",
        411 => "Length Required",
        412 => "Precondition Failed",
        413 => "Request Entity Too Large",
        414 => "Request-URI Too Long",
        415 => "Unsupported Media Type",
        416 => "Requested Range Not Satisfiable",
        417 => "Expectation Failed",
        418 => "I'm a teapot",
        421 => "Misdirected Request",
        422 => "Unprocessable Entity",
        423 => "Locked",
        424 => "Failed Dependency",
        425 => "Too Early",
        426 => "Upgrade Required",
        428 => "Precondition Required",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        451 => "Unavailable For Legal Reasons",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        505 => "HTTP Version Not Supported",
        506 => "Variant Also Negotiates",
        507 => "Insufficient Storage",
        508 => "Loop Detected",
        510 => "Not Extended",
        511 => "Network Authentication Required",
        _ => "",
    }
}

/// A failure while writing a streamed body: the HTTP side or the file side.
#[derive(Debug)]
pub enum StreamError {
    Http(HttpError),
    Io(std::io::Error),
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(error) => error.fmt(f),
            Self::Io(error) => f.write_str(&PyOsError::from_io(error, None).message),
        }
    }
}

/// Stream a response body into `part`, creating or truncating it. The caller renames it.
pub async fn stream_body_to(response: Response, part: &Path) -> Result<(), StreamError> {
    let mut file = tokio::fs::File::create(part).await.map_err(StreamError::Io)?;
    let mut body = response.bytes_stream();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|error| StreamError::Http(HttpError::transport(&error)))?;
        file.write_all(&chunk).await.map_err(StreamError::Io)?;
    }
    file.flush().await.map_err(StreamError::Io)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use embyx_core::golden;

    use super::{reason_phrase, status_error_message};

    #[test]
    fn reason_phrases_are_httpx_ones() {
        let document = golden::load("pyweb/httpx_reason_phrases.json");
        let cases = golden::cases(&document);
        assert_eq!(cases.len(), 500);
        for case in cases {
            let pair = golden::list(case);
            let status = u16::try_from(golden::int(&pair[0])).expect("a status");
            assert_eq!(reason_phrase(status), golden::string(&pair[1]), "{status}");
        }
    }

    #[test]
    fn status_errors_read_like_httpx() {
        let document = golden::load("pyweb/httpx_status_errors.json");
        for case in golden::cases(&document) {
            let status = u16::try_from(golden::int(golden::get(case, "status"))).expect("a status");
            let location = golden::get(case, "location").as_str();
            let expected = golden::get(case, "message").as_str();
            let url = golden::string(golden::get(case, "url"));
            assert_eq!(
                status_error_message(status, &url, location).as_deref(),
                expected,
                "{status}"
            );
        }
    }
}
