//! Network plumbing shared by the clients: the one place HTTP clients are built (`net`).
//!
//! Python origin: every `httpx.AsyncClient(...)` in the tree. Keeping construction here
//! means the TLS backend (rustls with the `ring` provider, the same one sqlx uses) can be
//! swapped in one place if a site turns out to care about the TLS fingerprint
//! (`docs/rewrite/PLAN.md`, technology mapping). Like httpx, the clients honour
//! `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` from the environment.
//!
//! The sources use [`client::HttpClient`] (an `httpx.AsyncClient` look-alike: per-source
//! proxy, httpx's redirect rules and `response.history`, `raise_for_status()` text),
//! [`download`] (a body streamed into a temporary file beside its destination, hashed on
//! the way, renamed into place) and [`retry`] (the retry ladders, `Retry-After`, and an
//! injectable sleep so tests never wait). [`subprocess`] runs yt-dlp and gallery-dl.
//!
//! Copied from fav (`fav-net/src/lib.rs`); see docs/CONVENTIONS.md, "Code copied from fav". `D` numbers
//! marked fav's are fav's own DIVERGENCES.md entries.

use std::sync::Once;

pub use reqwest;

pub mod client;
pub mod download;
#[cfg(any(test, feature = "test-support"))]
pub mod fakeweb;
pub mod httpx;
pub mod proxy;
pub mod retry;
pub mod subprocess;

pub use client::{ClientOptions, HttpClient, Request, Response, StatusError, TransportError};

static CRYPTO_PROVIDER: Once = Once::new();

/// Install rustls' `ring` provider as the process default, once.
///
/// reqwest is built without a provider of its own so the binary carries one TLS stack;
/// it then uses the process default, which has to exist before the first client is built.
fn install_crypto_provider() {
    CRYPTO_PROVIDER.call_once(|| {
        // An error means another component installed one first, which is just as good.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// A client builder with fav's TLS setup; callers add their timeouts.
pub fn client_builder() -> reqwest::ClientBuilder {
    install_crypto_provider();
    reqwest::Client::builder()
}

/// The httpx exception classes a failure maps onto.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorClass {
    ConnectTimeout,
    ReadTimeout,
    ConnectError,
    ReadError,
    RemoteProtocolError,
    LocalProtocolError,
    UnsupportedProtocol,
    TooManyRedirects,
    DecodingError,
    InvalidUrl,
    /// The proxy refused a tunnel (`httpx.ProxyError`): CONNECT answered outside 2xx, or a
    /// SOCKS failure.
    ProxyError,
}

impl ErrorClass {
    /// The httpx exception class a reqwest transport error stands in for.
    ///
    /// reqwest wraps every failure while reading a body as a decode error, so the cause
    /// is read from the error's source chain: a connection that ends before the message
    /// is complete is h11's `RemoteProtocolError` (before any response, or with the body
    /// cut short), a reset or other socket error is `ReadError`, and only a failure to
    /// decode content stays `DecodingError`. reqwest has no write timeout, so the mapping
    /// is approximate (DIVERGENCES.md D1, fav's D18).
    pub fn of(error: &reqwest::Error) -> Self {
        if refused_tunnel(error) {
            return Self::ProxyError;
        }
        if error.is_timeout() {
            return if error.is_connect() {
                Self::ConnectTimeout
            } else {
                Self::ReadTimeout
            };
        }
        if error.is_connect() {
            return Self::ConnectError;
        }
        if error.is_redirect() {
            return Self::TooManyRedirects;
        }
        if error.is_builder() {
            return Self::UnsupportedProtocol;
        }
        if let Some(class) = Self::from_source_chain(error) {
            return class;
        }
        if error.is_decode() {
            Self::DecodingError
        } else if error.is_body() {
            Self::ReadError
        } else {
            Self::RemoteProtocolError
        }
    }

    fn from_source_chain(error: &reqwest::Error) -> Option<Self> {
        let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(error);
        while let Some(cause) = source {
            if let Some(io) = cause.downcast_ref::<std::io::Error>() {
                return Some(match io.kind() {
                    std::io::ErrorKind::TimedOut => Self::ReadTimeout,
                    std::io::ErrorKind::UnexpectedEof => Self::RemoteProtocolError,
                    _ => Self::ReadError,
                });
            }
            if let Some(hyper) = cause.downcast_ref::<hyper::Error>() {
                if hyper.is_incomplete_message() {
                    return Some(Self::RemoteProtocolError);
                }
                if hyper.is_timeout() {
                    return Some(Self::ReadTimeout);
                }
                if hyper.is_parse() || hyper.is_parse_status() {
                    return Some(Self::RemoteProtocolError);
                }
            }
            source = cause.source();
        }
        None
    }

    /// The class name, as `exc.__class__.__name__` prints it.
    pub fn name(self) -> &'static str {
        match self {
            Self::ConnectTimeout => "ConnectTimeout",
            Self::ReadTimeout => "ReadTimeout",
            Self::ConnectError => "ConnectError",
            Self::ReadError => "ReadError",
            Self::RemoteProtocolError => "RemoteProtocolError",
            Self::LocalProtocolError => "LocalProtocolError",
            Self::UnsupportedProtocol => "UnsupportedProtocol",
            Self::TooManyRedirects => "TooManyRedirects",
            Self::DecodingError => "DecodingError",
            Self::InvalidUrl => "InvalidURL",
            Self::ProxyError => "ProxyError",
        }
    }

    /// `isinstance(exc, httpx.TimeoutException)`.
    pub fn is_timeout(self) -> bool {
        matches!(self, Self::ConnectTimeout | Self::ReadTimeout)
    }

    /// `isinstance(exc, httpx.NetworkError)`: connect, read, write and close errors.
    pub fn is_network(self) -> bool {
        matches!(self, Self::ConnectError | Self::ReadError)
    }

    /// `isinstance(exc, httpx.ProtocolError)`.
    pub fn is_protocol(self) -> bool {
        matches!(self, Self::RemoteProtocolError | Self::LocalProtocolError)
    }

    /// `isinstance(exc, httpx.TransportError)`: everything but redirects, decoding and
    /// URL errors.
    pub fn is_transport(self) -> bool {
        !matches!(self, Self::TooManyRedirects | Self::DecodingError | Self::InvalidUrl)
    }
}

/// Whether reqwest failed because the proxy answered its CONNECT with something other than
/// 200 (hyper-util's tunnel errors are not exported, so they are known by their text).
fn refused_tunnel(error: &reqwest::Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(error);
    while let Some(cause) = source {
        let text = cause.to_string();
        if text == "tunnel error: unsuccessful" || text == "tunnel error: proxy authorization required" {
            return true;
        }
        source = cause.source();
    }
    false
}

/// The httpx exception class name a reqwest transport error stands in for.
///
/// Callers that report a failure by its class name (`... failed: ConnectTimeout`) use this so the text keeps httpx's names.
pub fn httpx_error_class(error: &reqwest::Error) -> &'static str {
    ErrorClass::of(error).name()
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_client_can_be_built() {
        super::client_builder()
            .build()
            .expect("client builds with the ring provider");
    }

    #[tokio::test]
    async fn a_refused_connection_is_a_connect_error() {
        let client = super::client_builder().build().expect("client");
        // Port 1 on loopback refuses at once.
        let error = client.get("http://127.0.0.1:1/").send().await.expect_err("refused");
        assert_eq!(super::httpx_error_class(&error), "ConnectError");
    }
}
