//! Fetching subscribed feeds over HTTP.
//!
//! Python origin: `src/embyx_manager/clients/feeds.py`. One shared client for every
//! subscription; `fetch` returns the raw body, refusing one past [`MAX_FEED_BYTES`] while it
//! streams in. RSSHub's javbus route builds its feed from one detail page per item, so a cold
//! fetch can take a minute; the read timeout allows for that.

use std::fmt;
use std::time::Duration;

use crate::http::HttpxError;
use crate::net::{BuildError, ClientOptions, HttpClient, Request};

/// A feed is a few hundred items at most; anything bigger is not one.
pub const MAX_FEED_BYTES: usize = 8 * 1024 * 1024;

/// What `fetch` fails with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedFetchError {
    Http(HttpxError),
    /// `FeedTooLargeError`.
    TooLarge {
        url: String,
        limit: usize,
    },
}

impl FeedFetchError {
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Http(error) => error.class_name(),
            Self::TooLarge { .. } => "FeedTooLargeError",
        }
    }

    /// The status of an `httpx.HTTPStatusError` (the poller relinks a talent on 404).
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Http(error) => error.status(),
            Self::TooLarge { .. } => None,
        }
    }
}

impl fmt::Display for FeedFetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(error) => error.fmt(f),
            Self::TooLarge { url, limit } => write!(f, "feed at {url} exceeds {limit} bytes"),
        }
    }
}

impl std::error::Error for FeedFetchError {}

impl From<HttpxError> for FeedFetchError {
    fn from(error: HttpxError) -> Self {
        Self::Http(error)
    }
}

/// `HttpFeedFetcher`.
#[derive(Debug)]
pub struct HttpFeedFetcher {
    client: HttpClient,
    max_bytes: usize,
}

impl HttpFeedFetcher {
    /// The httpx client `HttpFeedFetcher.__init__` builds (`timeout=120.0`, connect 20 s,
    /// redirects followed, at most five).
    pub fn client_options(timeout: Duration, proxy: Option<&str>) -> ClientOptions {
        ClientOptions {
            headers: vec![
                (
                    "Accept".to_owned(),
                    "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.5".to_owned(),
                ),
                ("User-Agent".to_owned(), "embyx-manager/1.0 (+feed poller)".to_owned()),
            ],
            timeout,
            connect_timeout: Some(Duration::from_secs(20)),
            follow_redirects: true,
            max_redirects: 5,
            proxy: proxy.filter(|proxy| !proxy.is_empty()).map(str::to_owned),
            ..ClientOptions::default()
        }
    }

    /// `HttpFeedFetcher()` with its defaults.
    pub fn new() -> Result<Self, BuildError> {
        Self::with_client(&Self::client_options(Duration::from_mins(2), None), MAX_FEED_BYTES)
    }

    pub fn with_client(options: &ClientOptions, max_bytes: usize) -> Result<Self, BuildError> {
        Ok(Self {
            client: HttpClient::new(options)?,
            max_bytes,
        })
    }

    /// The feed's body.
    pub async fn fetch(&self, url: &str) -> Result<Vec<u8>, FeedFetchError> {
        let mut response = self.client.send(Request::get(url)).await.map_err(HttpxError::from)?;
        response.error_for_status().map_err(HttpxError::from)?;
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(HttpxError::from)? {
            if body.len() + chunk.len() > self.max_bytes {
                return Err(FeedFetchError::TooLarge {
                    url: url.to_owned(),
                    limit: self.max_bytes,
                });
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}
