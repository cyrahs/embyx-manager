//! What the httpx-based clients share: their error type, `raise_for_status()`, query strings
//! and `response.text`.
//!
//! Python origin: `httpx.HTTPError` as `clients/javbus.py`, `sukebei.py`, `feeds.py`,
//! `jinjier.py` and `emby.py` catch and report it, `params=` on a request (httpx 0.28 encodes
//! them with `urllib.parse.urlencode`), and `Response.text` (the `charset` of the
//! `Content-Type`, else UTF-8, undecodable bytes replaced).
//!
//! The clients themselves are built from [`crate::net::ClientOptions`], one set per Python
//! constructor (the HTTP client factory of the plan: `JavBusClient::client_options`,
//! `SukebeiClient::client_options`, ...); AVBase's impersonating client is
//! [`crate::avbase::session`].

use std::fmt;

use crate::net::{ErrorClass, HttpClient, Request, Response, StatusError, TransportError};

/// An `httpx.HTTPError` (or `httpx.InvalidURL`) a request ended with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpxError {
    /// `httpx.HTTPStatusError` from `raise_for_status()`.
    Status(StatusError),
    /// A transport failure, by its httpx class.
    Transport(TransportError),
}

impl HttpxError {
    /// `type(exc).__name__`.
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Status(_) => "HTTPStatusError",
            Self::Transport(error) => error.class.name(),
        }
    }

    /// `isinstance(exc, (httpx.HTTPError, httpx.TimeoutException))`: everything but
    /// `InvalidURL`, which httpx does not derive from `HTTPError`.
    pub fn is_http_error(&self) -> bool {
        !matches!(self, Self::Transport(error) if error.class == ErrorClass::InvalidUrl)
    }

    /// The status of an `HTTPStatusError`.
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Status(error) => Some(error.status),
            Self::Transport(_) => None,
        }
    }
}

impl fmt::Display for HttpxError {
    /// `str(exc)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status(error) => error.fmt(f),
            Self::Transport(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for HttpxError {}

impl From<TransportError> for HttpxError {
    fn from(error: TransportError) -> Self {
        Self::Transport(error)
    }
}

impl From<StatusError> for HttpxError {
    fn from(error: StatusError) -> Self {
        Self::Status(error)
    }
}

/// `client.send(request)` then `response.raise_for_status()`.
pub async fn send_checked(client: &HttpClient, request: Request) -> Result<Response, HttpxError> {
    let response = client.send(request).await?;
    response.error_for_status()?;
    Ok(response)
}

/// `urllib.parse.urlencode(params)` (`quote_plus`, nothing safe), as httpx encodes `params=`.
pub fn urlencode(params: &[(&str, String)]) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{}={}", quote_plus(key), quote_plus(value)))
        .collect::<Vec<_>>()
        .join("&")
}

/// `urllib.parse.quote_plus(text, safe='')`.
pub fn quote_plus(text: &str) -> String {
    embyx_core::pyurl::quote(text, " ").replace(' ', "+")
}

/// The URL with `params=` added as httpx (and `curl_cffi`) add them. No URL the clients build
/// carries a query of its own, so the parameters are appended (`?` or `&`).
pub fn with_params(url: &str, params: &[(&str, String)]) -> String {
    if params.is_empty() {
        return url.to_owned();
    }
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}{}", urlencode(params))
}

/// `response.text` for a body: decoded with the `charset` of `content_type`, else UTF-8,
/// undecodable bytes replaced (U+FFFD), as httpx's `TextDecoder(errors="replace")` does.
pub fn decode_text(body: &[u8], content_type: Option<&str>) -> String {
    let charset = content_type.and_then(charset_of).map(str::to_ascii_lowercase);
    match charset.as_deref() {
        Some("iso-8859-1" | "iso8859-1" | "latin-1" | "latin1" | "l1") => {
            body.iter().map(|&byte| char::from(byte)).collect()
        }
        Some("ascii" | "us-ascii") => body
            .iter()
            .map(|&byte| if byte.is_ascii() { char::from(byte) } else { '\u{fffd}' })
            .collect(),
        Some(label) if !matches!(label, "utf-8" | "utf8") => match encoding_rs::Encoding::for_label(label.as_bytes()) {
            Some(encoding) => encoding.decode_without_bom_handling(body).0.into_owned(),
            None => String::from_utf8_lossy(body).into_owned(),
        },
        _ => String::from_utf8_lossy(body).into_owned(),
    }
}

/// The `charset` parameter of a `Content-Type` value.
fn charset_of(content_type: &str) -> Option<&str> {
    content_type.split(';').skip(1).find_map(|parameter| {
        let (name, value) = parameter.split_once('=')?;
        name.trim()
            .eq_ignore_ascii_case("charset")
            .then(|| value.trim().trim_matches('"').trim_matches('\''))
            .filter(|value| !value.is_empty())
    })
}

/// `UnicodeDecodeError`'s text for bytes that are not UTF-8, as Python's strict decoder
/// words it (the same maximal-subpart rule decides how many bytes it names).
pub fn utf8_error_message(bytes: &[u8], error: &std::str::Utf8Error) -> String {
    let position = error.valid_up_to();
    let start = bytes.get(position).copied().unwrap_or_default();
    let (length, reason) = match error.error_len() {
        None => (bytes.len() - position, "unexpected end of data"),
        Some(length) if matches!(start, 0x80..=0xc1 | 0xf5..=0xff) => (length, "invalid start byte"),
        Some(length) => (length, "invalid continuation byte"),
    };
    if length <= 1 {
        format!("'utf-8' codec can't decode byte 0x{start:02x} in position {position}: {reason}")
    } else {
        format!(
            "'utf-8' codec can't decode bytes in position {position}-{}: {reason}",
            position + length - 1
        )
    }
}

/// The whole body as `response.text`.
pub async fn response_text(response: Response) -> Result<String, HttpxError> {
    let content_type = response.header("content-type");
    let body = response.bytes().await?;
    Ok(decode_text(&body, content_type.as_deref()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_are_encoded_like_urlencode() {
        assert_eq!(
            with_params(
                "https://sukebei.test",
                &[("f", "0".into()), ("c", "2_2".into()), ("q", "ABC 1/2&x".into())]
            ),
            "https://sukebei.test?f=0&c=2_2&q=ABC+1%2F2%26x"
        );
        assert_eq!(urlencode(&[("Ids", "1,2".into())]), "Ids=1%2C2");
        assert_eq!(urlencode(&[("name", "河北".into())]), "name=%E6%B2%B3%E5%8C%97");
    }

    #[test]
    fn utf8_errors_are_worded_like_python() {
        let message = |bytes: &[u8]| utf8_error_message(bytes, &std::str::from_utf8(bytes).unwrap_err());
        assert_eq!(
            message(b"\xff{}"),
            "'utf-8' codec can't decode byte 0xff in position 0: invalid start byte"
        );
        assert_eq!(
            message(b"\xe3\x81A"),
            "'utf-8' codec can't decode bytes in position 0-1: invalid continuation byte"
        );
        assert_eq!(
            message(b"ab\xe3"),
            "'utf-8' codec can't decode byte 0xe3 in position 2: unexpected end of data"
        );
        assert_eq!(
            message(b"\xe3\x81"),
            "'utf-8' codec can't decode bytes in position 0-1: unexpected end of data"
        );
        assert_eq!(
            message(b"\xe0\x80"),
            "'utf-8' codec can't decode byte 0xe0 in position 0: invalid continuation byte"
        );
        assert_eq!(
            message(b"\xc0\xaf"),
            "'utf-8' codec can't decode byte 0xc0 in position 0: invalid start byte"
        );
    }

    #[test]
    fn text_follows_the_charset_or_falls_back_to_utf8() {
        assert_eq!(decode_text("é".as_bytes(), None), "é");
        assert_eq!(decode_text(b"\xe9", Some("text/html; charset=ISO-8859-1")), "é");
        assert_eq!(decode_text(b"a\xffb", Some("text/html")), "a\u{fffd}b");
        assert_eq!(decode_text(b"\x82\xa0", Some("text/html; charset=\"shift_jis\"")), "あ");
    }
}
