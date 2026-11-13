//! The impersonating HTTP session AVBase is read through (`curl_cffi`'s `AsyncSession` in Python).
//!
//! Python origin: `AsyncSession(impersonate='chrome', timeout=30.0, proxy=proxy,
//! headers={'Referer': f'{host}/', 'Accept-Language': 'ja,en;q=0.8'})` in
//! `clients/avbase.py`. AVBase's Cloudflare front refuses a plain client (the live run in
//! docs/rewrite/API-COVERAGE.md). The client here is `wreq` with `wreq-util`'s Chrome 149
//! profile tuned to what `curl_cffi` 0.16.3 sends for `chrome` (chrome150): its default header
//! list and order, the session's two headers merged into it the way `curl_cffi` merges them
//! (a default of the same name replaced in place, others appended), and the HEADERS frame's
//! priority weight 256. Like `curl_cffi`'s session it keeps cookies, follows redirects (at most
//! 30) and never retries; the timeout covers the whole request. `params` are encoded as
//! `curl_cffi` encodes them (`urlencode`). What still differs from `curl_cffi`: three ML-DSA
//! signature algorithms in the ClientHello, and the text of transport errors (D29).

use std::fmt;
use std::time::Duration;

use futures_util::future::BoxFuture;
use wreq::header::{HeaderMap, HeaderName, HeaderValue, OrigHeaderMap};
use wreq::http2::{StreamDependency, StreamId};

use super::{AvbaseError, AvbaseResponse, AvbaseSession};
use crate::http::{decode_text, with_params};

/// `curl_cffi` 0.16.3 `impersonate='chrome'` (chrome150): its default headers, in its order
/// (`tools/probes/tls_fingerprint.py` records them).
pub const CURL_CFFI_CHROME_HEADERS: [(&str, &str); 13] = [
    (
        "sec-ch-ua",
        "\"Not;A=Brand\";v=\"8\", \"Chromium\";v=\"150\", \"Google Chrome\";v=\"150\"",
    ),
    ("sec-ch-ua-mobile", "?0"),
    ("sec-ch-ua-platform", "\"macOS\""),
    ("upgrade-insecure-requests", "1"),
    (
        "user-agent",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) \
         Chrome/150.0.0.0 Safari/537.36",
    ),
    (
        "accept",
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,\
         application/signed-exchange;v=b3;q=0.7",
    ),
    ("sec-fetch-site", "none"),
    ("sec-fetch-mode", "navigate"),
    ("sec-fetch-user", "?1"),
    ("sec-fetch-dest", "document"),
    ("accept-encoding", "gzip, deflate, br, zstd"),
    ("accept-language", "en-US,en;q=0.9"),
    ("priority", "u=0, i"),
];

/// `curl_cffi`'s `max_redirects` default.
pub const MAX_REDIRECTS: usize = 30;

/// The emulation: Chrome 149's TLS and HTTP/2 settings with `curl_cffi`'s headers, `headers`
/// merged in (a default of the same name replaced in place, the rest appended).
pub fn chrome_emulation(headers: &[(&str, &str)]) -> Result<wreq::Emulation, String> {
    let mut emulation = wreq::IntoEmulation::into_emulation(wreq_util::Emulation::Chrome149);
    let mut list: Vec<(String, String)> = CURL_CFFI_CHROME_HEADERS
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect();
    for (name, value) in headers {
        let lower = name.to_ascii_lowercase();
        match list.iter_mut().find(|(existing, _)| *existing == lower) {
            Some(slot) => (*value).clone_into(&mut slot.1),
            None => list.push((lower, (*value).to_owned())),
        }
    }
    let mut map = HeaderMap::new();
    let mut order = OrigHeaderMap::new();
    for (name, value) in list {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| error.to_string())?;
        let value = HeaderValue::from_str(&value).map_err(|error| error.to_string())?;
        order.insert(name.clone());
        map.insert(name, value);
    }
    emulation.headers = map;
    emulation.orig_headers = order;
    if let Some(http2) = emulation.http2_options.as_mut() {
        // Chrome's (and curl_cffi's) HEADERS frame: exclusive on stream 0, weight 256.
        http2.headers_stream_dependency = Some(StreamDependency::new(StreamId::zero(), 255, true));
    }
    Ok(emulation)
}

/// What the session is built with.
#[derive(Clone, PartialEq, Eq)]
pub struct SessionOptions {
    pub headers: Vec<(String, String)>,
    pub timeout: Duration,
    pub proxy: Option<String>,
    /// Trust only these roots (DER) instead of the usual ones: tests and the local probe.
    pub roots: Vec<Vec<u8>>,
}

impl fmt::Debug for SessionOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionOptions")
            .field(
                "headers",
                &self.headers.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            )
            .field("timeout", &self.timeout)
            .field("proxy", &self.proxy.as_ref().map(|_| "<redacted>"))
            .field("roots", &self.roots.len())
            .finish()
    }
}

/// `curl_cffi`'s `AsyncSession` over wreq.
#[derive(Clone)]
pub struct ImpersonatingSession {
    client: wreq::Client,
}

impl fmt::Debug for ImpersonatingSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImpersonatingSession").finish_non_exhaustive()
    }
}

impl ImpersonatingSession {
    pub fn new(options: &SessionOptions) -> Result<Self, String> {
        let headers: Vec<(&str, &str)> = options
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let mut builder = wreq::Client::builder()
            .emulation(chrome_emulation(&headers)?)
            .timeout(options.timeout)
            .cookie_store(true)
            .redirect(wreq::redirect::Policy::limited(MAX_REDIRECTS))
            .retry(wreq::retry::Policy::never());
        if let Some(proxy) = options.proxy.as_deref().filter(|proxy| !proxy.is_empty()) {
            builder = builder.proxy(wreq::Proxy::all(proxy).map_err(|error| error.to_string())?);
        }
        if !options.roots.is_empty() {
            let mut store = wreq::tls::trust::CertStore::builder();
            for der in &options.roots {
                store = store.add_der_cert(der.as_slice());
            }
            builder = builder.tls_cert_store(store.build().map_err(|error| error.to_string())?);
        }
        Ok(Self {
            client: builder.build().map_err(|error| error.to_string())?,
        })
    }
}

/// `curl_cffi`'s exception class for a failed request.
fn transport_error(error: &wreq::Error) -> AvbaseError {
    let class = if error.is_timeout() {
        "Timeout"
    } else if error.is_connect() {
        "ConnectionError"
    } else if error.is_redirect() {
        "TooManyRedirects"
    } else {
        "RequestException"
    };
    // wreq's text names the URL, which carries nothing secret for AVBase.
    let mut message = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        message = format!("{message}: {cause}");
        source = cause.source();
    }
    AvbaseError::Transport { class, message }
}

impl AvbaseSession for ImpersonatingSession {
    fn get<'a>(
        &'a self,
        url: &'a str,
        params: &'a [(&'a str, String)],
    ) -> BoxFuture<'a, Result<AvbaseResponse, AvbaseError>> {
        Box::pin(async move {
            let response = self
                .client
                .get(with_params(url, params))
                .send()
                .await
                .map_err(|error| transport_error(&error))?;
            let status = response.status().as_u16();
            let content_type = response
                .headers()
                .get("content-type")
                .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned());
            let body = response
                .bytes()
                .await
                .map_err(|error| transport_error(&error))?
                .to_vec();
            Ok(AvbaseResponse::new(status, content_type, body))
        })
    }
}

/// `response.text` as `curl_cffi` decodes it: the `charset=` of the `Content-Type`, else
/// UTF-8, undecodable bytes replaced.
pub(super) fn curl_text(body: &[u8], content_type: Option<&str>) -> String {
    decode_text(body, content_type)
}
