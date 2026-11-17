//! JavBus: star search, a star's catalog page by page, a video's cast and its magnets.
//!
//! Python origin: `src/embyx_manager/clients/javbus.py`. Requests go through the httpx-shaped
//! client with the baseline's settings (two default headers, no idle connections kept, at
//! most ten at once, no redirects followed); every public read except the paging logic is
//! wrapped in the same `tenacity` retry (three attempts, 1 s and 2 s apart, on any
//! `httpx.HTTPError`). Pages are read with [`crate::pyquery`]. Errors keep Python's classes
//! ([`JavBusError::class_name`]): `httpx` ones, `JavBusPaginationError`, lxml's `ParserError`
//! and the `ValueError`s `urlparse` and `int()` raise.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use embyx_core::pytext::{casefold, py_decimal_value, py_split, py_strip};
use embyx_core::pyurl::{quote, unquote, urlparse};
use futures_util::StreamExt as _;
use futures_util::stream::FuturesUnordered;

use crate::http::{HttpxError, response_text};
use crate::net::retry::{Sleeper, tokio_sleeper};
use crate::net::{BuildError, ClientOptions, HttpClient, Request};
use crate::pyquery::{Document, ParseError, Selection};
use crate::size::{SizeError, parse_size};
use crate::tenacity;

pub const DEFAULT_HOST: &str = "https://www.javbus.com";
pub const DEFAULT_MAX_ACTOR_PAGES: u64 = 200;
const NOT_FOUND_STATUSES: [u16; 2] = [404, 410];

/// `progress_callback(completed, total, current)`.
pub type PageProgress = dyn Fn(u64, Option<u64>, Option<u64>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavBusActor {
    pub actor_id: String,
    pub name: String,
}

/// A star page's display name and the video IDs listed on its first page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavBusActorPage {
    pub actor_id: String,
    pub name: String,
    pub video_ids: Vec<String>,
}

/// One entry of `get_magnets`: `{magnet, size, size_int, tags, score}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavBusMagnet {
    pub magnet: String,
    pub size: String,
    pub size_int: i128,
    pub tags: Vec<String>,
    pub score: i128,
}

/// What a JavBus read fails with, by Python's exception class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JavBusError {
    Http(HttpxError),
    /// `JavBusPaginationError`.
    Pagination(String),
    /// lxml's `ParserError` or `ValueError` while parsing a page.
    Parse(ParseError),
    /// A `ValueError` from `urlparse` (an unparsable link) or `int()` (a size of digits that
    /// are not decimal).
    Value(String),
}

impl JavBusError {
    /// `type(exc).__name__`.
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Http(error) => error.class_name(),
            Self::Pagination(_) => "JavBusPaginationError",
            Self::Parse(error) => error.class_name(),
            Self::Value(_) => "ValueError",
        }
    }

    /// Whether the `tenacity` retry runs again on it.
    fn is_retryable(&self) -> bool {
        matches!(self, Self::Http(error) if error.is_http_error())
    }
}

impl fmt::Display for JavBusError {
    /// `str(exc)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(error) => error.fmt(f),
            Self::Parse(error) => error.fmt(f),
            Self::Pagination(message) | Self::Value(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for JavBusError {}

impl From<HttpxError> for JavBusError {
    fn from(error: HttpxError) -> Self {
        Self::Http(error)
    }
}

impl From<ParseError> for JavBusError {
    fn from(error: ParseError) -> Self {
        Self::Parse(error)
    }
}

/// Rank a JavBus magnet: every quality tag outweighs any plausible size gap.
///
/// RSSHub's javbus route's weighting, `links^8 x size` with the title link counted. A magnet
/// whose size could not be read scores zero and sorts last.
pub fn magnet_score(size_int: i128, tags: usize) -> i128 {
    let links = i128::try_from(tags).unwrap_or(i128::MAX).saturating_add(1);
    links.saturating_pow(8).saturating_mul(size_int)
}

/// The constructor's arguments (`JavBusClient(host=..., proxy=..., ...)`).
#[derive(Clone, PartialEq)]
pub struct JavBusOptions {
    pub host: String,
    pub proxy: Option<String>,
    pub timeout: Duration,
    pub max_connections: usize,
    pub max_actor_pages: u64,
}

impl Default for JavBusOptions {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_owned(),
            proxy: None,
            timeout: Duration::from_mins(1),
            max_connections: 10,
            max_actor_pages: DEFAULT_MAX_ACTOR_PAGES,
        }
    }
}

impl fmt::Debug for JavBusOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JavBusOptions")
            .field("host", &self.host)
            .field("proxy", &self.proxy.as_ref().map(|_| "<redacted>"))
            .field("timeout", &self.timeout)
            .field("max_connections", &self.max_connections)
            .field("max_actor_pages", &self.max_actor_pages)
            .finish()
    }
}

/// `JavBusClient`.
pub struct JavBusClient {
    host: String,
    /// `urlparse(self.host).netloc`.
    netloc: String,
    pub max_actor_pages: u64,
    client: HttpClient,
    sleeper: Sleeper,
}

impl fmt::Debug for JavBusClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JavBusClient")
            .field("host", &self.host)
            .field("max_actor_pages", &self.max_actor_pages)
            .finish_non_exhaustive()
    }
}

/// `urlparse(href)`: netloc and path, or the `ValueError` it raises.
fn url_parts(href: &str) -> Result<(String, String), JavBusError> {
    let (split, _) = urlparse(href, "").map_err(|error| JavBusError::Value(error.0))?;
    Ok((split.netloc, split.path))
}

/// `' '.join(text.split())`.
fn squash(text: &str) -> String {
    py_split(text).collect::<Vec<_>>().join(" ")
}

/// `_ACTOR_ID_RE.fullmatch(text)`: `[A-Za-z0-9_-]{1,32}`.
fn is_actor_id(text: &str) -> bool {
    (1..=32).contains(&text.len())
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

/// `re.fullmatch(r'/star/([^/]+)', path)`.
fn star_segment(path: &str) -> Option<&str> {
    path.strip_prefix("/star/")
        .filter(|rest| !rest.is_empty() && !rest.contains('/'))
}

static GID: LazyLock<regex::Regex> = LazyLock::new(|| embyx_core::pyre::compile(r"var gid = (\d+);", false, false));
static UC: LazyLock<regex::Regex> = LazyLock::new(|| embyx_core::pyre::compile(r"var uc = (\d+);", false, false));
static IMG: LazyLock<regex::Regex> = LazyLock::new(|| embyx_core::pyre::compile(r"var img = '([^']+)';", false, false));

/// `re.search(r'urn:btih:([a-fA-F0-9]+)', magnet)`.
fn btih(magnet: &str) -> Option<&str> {
    magnet.match_indices("urn:btih:").find_map(|(at, marker)| {
        let rest = &magnet[at + marker.len()..];
        let end = rest.find(|ch: char| !ch.is_ascii_hexdigit()).unwrap_or(rest.len());
        (end > 0).then(|| &rest[..end])
    })
}

/// `random.randint(1, 1000)`.
fn random_floor() -> u32 {
    use std::hash::{BuildHasher as _, Hasher as _};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos()),
    );
    u32::try_from(hasher.finish() % 1000).unwrap_or(0) + 1
}

/// The page number of a pagination link: `/star/<id>/([1-9]\d*)/?`, digits read by `int()`
/// (Unicode decimals included). A number past `u64` saturates; anything above the page
/// limit fails the same way.
fn page_number(path: &str, actor_id: &str) -> Option<u64> {
    let rest = path.strip_prefix("/star/")?.strip_prefix(actor_id)?.strip_prefix('/')?;
    let digits = rest.strip_suffix('/').unwrap_or(rest);
    let mut chars = digits.chars();
    let first = chars.next()?;
    if !('1'..='9').contains(&first) {
        return None;
    }
    let mut value = u64::from(py_decimal_value(first)?);
    for ch in chars {
        let digit = py_decimal_value(ch)?;
        value = value.saturating_mul(10).saturating_add(u64::from(digit));
    }
    Some(value)
}

impl JavBusClient {
    /// The httpx client `JavBusClient.__init__` builds.
    pub fn client_options(options: &JavBusOptions) -> ClientOptions {
        ClientOptions {
            headers: vec![
                ("Accept-Language".to_owned(), "zh-CN,zh;q=0.9,en;q=0.8".to_owned()),
                ("Cookie".to_owned(), "existmag=all".to_owned()),
            ],
            timeout: options.timeout,
            proxy: options.proxy.clone().filter(|proxy| !proxy.is_empty()),
            max_keepalive_connections: 0,
            max_connections: Some(options.max_connections),
            ..ClientOptions::default()
        }
    }

    pub fn new(options: &JavBusOptions) -> Result<Self, BuildError> {
        Self::with_client(options, &Self::client_options(options), tokio_sleeper())
    }

    /// With the client options adjusted (tests point them at the fake web) and the retry
    /// sleep injected.
    pub fn with_client(options: &JavBusOptions, client: &ClientOptions, sleeper: Sleeper) -> Result<Self, BuildError> {
        let host = options.host.trim_end_matches('/').to_owned();
        let netloc = urlparse(&host, "").map(|(split, _)| split.netloc).unwrap_or_default();
        Ok(Self {
            host,
            netloc,
            max_actor_pages: options.max_actor_pages,
            client: HttpClient::new(client)?,
            sleeper,
        })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    async fn retry<T, Fut>(&self, operation: impl FnMut() -> Fut) -> Result<T, JavBusError>
    where
        Fut: Future<Output = Result<T, JavBusError>>,
    {
        tenacity::retry(&self.sleeper, JavBusError::is_retryable, operation).await
    }

    /// GET `url`, raising for anything but a 2xx unless it is one of `not_found`; the page
    /// text, or `None` for a not-found status.
    async fn get_text(&self, request: Request, not_found: &[u16]) -> Result<Option<String>, JavBusError> {
        let response = self.client.send(request).await.map_err(HttpxError::from)?;
        if not_found.contains(&response.status()) {
            return Ok(None);
        }
        response.error_for_status().map_err(HttpxError::from)?;
        Ok(Some(response_text(response).await?))
    }

    /// Stars JavBus lists for `name`; JavBus keeps one star page per alias, so a person may be
    /// several.
    pub async fn search_stars(&self, name: &str) -> Result<Vec<JavBusActor>, JavBusError> {
        let url = format!("{}/searchstar/{}", self.host, quote(name, ""));
        self.retry(|| async {
            let Some(text) = self.get_text(Request::get(url.clone()), &NOT_FOUND_STATUSES).await? else {
                return Ok(Vec::new());
            };
            let doc = Document::parse(&text)?;
            let mut found: Vec<JavBusActor> = Vec::new();
            let mut keys: HashMap<String, usize> = HashMap::new();
            for item in doc.select("a[href]").items() {
                let (_, path) = url_parts(&item.attr("href").unwrap_or_default())?;
                let Some(segment) = star_segment(path.trim_end_matches('/')) else {
                    continue;
                };
                let actor_id = py_strip(&unquote(segment)).to_owned();
                if !is_actor_id(&actor_id) {
                    continue;
                }
                let mut star_name = squash(&item.select(".pb10").text());
                if star_name.is_empty() {
                    star_name = squash(&item.text());
                }
                let key = casefold(&actor_id);
                if let std::collections::hash_map::Entry::Vacant(e) = keys.entry(key) {
                    e.insert(found.len());
                    let name = if star_name.is_empty() {
                        actor_id.clone()
                    } else {
                        star_name
                    };
                    found.push(JavBusActor { actor_id, name });
                }
            }
            Ok(found)
        })
        .await
    }

    /// The star page's display name and first-page video IDs; `None` when there is no such star.
    pub async fn get_actor(&self, actor_id: &str) -> Result<Option<JavBusActorPage>, JavBusError> {
        let Some(text) = self.fetch_actor_page(actor_id, 1).await? else {
            return Ok(None);
        };
        let doc = Document::parse(&text)?;
        let mut name = squash(&doc.select(".avatar-box .pb10").eq(0).text());
        if name.is_empty() {
            let title = doc.select("title").text();
            name = squash(title.split(" - ").next().unwrap_or_default());
        }
        let (video_ids, _) = self.parse_actor_page(actor_id, &text)?;
        Ok(Some(JavBusActorPage {
            actor_id: actor_id.to_owned(),
            name,
            video_ids,
        }))
    }

    pub async fn scrape_one_page(&self, actor_id: &str, page: u64) -> Result<Vec<String>, JavBusError> {
        match self.fetch_actor_page(actor_id, page).await? {
            None => Ok(Vec::new()),
            Some(text) => Ok(self.parse_actor_page(actor_id, &text)?.0),
        }
    }

    /// Discover the real final page instead of trusting one sliding pagination window.
    pub async fn get_total_page(&self, actor_id: &str, progress: Option<&PageProgress>) -> Result<u64, JavBusError> {
        let Some(first) = self.fetch_actor_page(actor_id, 1).await? else {
            return Err(JavBusError::Pagination(format!(
                "JavBus actor {} was not found",
                embyx_core::pytext::py_repr(actor_id)
            )));
        };
        let (ids, linked_pages) = self.parse_actor_page(actor_id, &first)?;
        notify(progress, 0, None, Some(1)).await;
        if ids.is_empty() && linked_pages.is_empty() {
            return Ok(1);
        }
        let mut fingerprints: Vec<Vec<String>> = Vec::new();
        if !ids.is_empty() {
            fingerprints.push(ids);
        }
        let mut highest: u64 = 1;
        let mut current_links = linked_pages;
        loop {
            let linked_max = current_links.iter().copied().max().unwrap_or(highest);
            if linked_max > self.max_actor_pages {
                return Err(JavBusError::Pagination(format!(
                    "JavBus actor pagination exceeds the {}-page safety limit",
                    self.max_actor_pages
                )));
            }
            if linked_max > highest {
                highest = linked_max;
                let Some(text) = self.fetch_actor_page(actor_id, highest).await? else {
                    return Err(JavBusError::Pagination(format!(
                        "JavBus actor pagination has a gap at page {highest}"
                    )));
                };
                let (page_ids, links) = self.parse_actor_page(actor_id, &text)?;
                current_links = links;
                if !page_ids.is_empty() && !fingerprints.contains(&page_ids) {
                    fingerprints.push(page_ids);
                }
                notify(progress, 0, None, Some(highest)).await;
                continue;
            }
            if highest >= self.max_actor_pages {
                return Err(JavBusError::Pagination(format!(
                    "JavBus actor pagination reached the {}-page safety limit without an end",
                    self.max_actor_pages
                )));
            }
            let probe = highest + 1;
            let Some(text) = self.fetch_actor_page(actor_id, probe).await? else {
                return Ok(highest);
            };
            let (page_ids, page_links) = self.parse_actor_page(actor_id, &text)?;
            notify(progress, 0, None, Some(probe)).await;
            if page_ids.is_empty() {
                if page_links.iter().copied().max().unwrap_or(highest) > highest {
                    return Err(JavBusError::Pagination(format!(
                        "JavBus actor pagination returned an empty gap at page {probe}"
                    )));
                }
                return Ok(highest);
            }
            if fingerprints.contains(&page_ids) {
                return Ok(highest);
            }
            fingerprints.push(page_ids);
            highest = probe;
            current_links = page_links;
        }
    }

    /// Every video ID of the star, every page fetched at once, in page order without repeats.
    pub async fn scrape(&self, actor_id: &str, progress: Option<&PageProgress>) -> Result<Vec<String>, JavBusError> {
        scrape_pages(progress, self.get_total_page(actor_id, progress), |page| {
            self.scrape_one_page(actor_id, page)
        })
        .await
    }

    /// Read the credited actors from a JavBus video detail page.
    pub async fn get_video_actors(&self, video_id: &str) -> Result<Vec<JavBusActor>, JavBusError> {
        let url = format!("{}/{video_id}", self.host);
        self.retry(|| async {
            let text = self.get_text(Request::get(url.clone()), &[]).await?.unwrap_or_default();
            let doc = Document::parse(&text)?;
            let mut actors: Vec<JavBusActor> = Vec::new();
            let mut keys: HashMap<String, usize> = HashMap::new();
            for item in doc.select("a[href]").items() {
                let (_, path) = url_parts(&item.attr("href").unwrap_or_default())?;
                let Some(segment) = star_segment(path.trim_end_matches('/')) else {
                    continue;
                };
                let actor_id = py_strip(&unquote(segment)).to_owned();
                let key = casefold(&actor_id);
                if !is_actor_id(&actor_id) {
                    continue;
                }
                let image = item.select("img");
                let candidates = [
                    Some(item.text()),
                    item.attr("title"),
                    item.attr("aria-label"),
                    image.attr("title"),
                    image.attr("alt"),
                ];
                let name = candidates
                    .into_iter()
                    .flatten()
                    .filter(|candidate| !candidate.is_empty())
                    .map(|candidate| squash(&candidate))
                    .find(|cleaned| casefold(cleaned) != key)
                    .unwrap_or_default();
                match keys.get(&key) {
                    None => {
                        keys.insert(key, actors.len());
                        let name = if name.is_empty() { actor_id.clone() } else { name };
                        actors.push(JavBusActor { actor_id, name });
                    }
                    Some(&index) => {
                        // JavBus can emit an empty metadata link before the named avatar card.
                        if casefold(&actors[index].name) == key && !name.is_empty() {
                            actors[index].name = name;
                        }
                    }
                }
            }
            Ok(actors)
        })
        .await
    }

    /// One star page's text; `None` for a 404 or 410 (retried like the other reads).
    async fn fetch_actor_page(&self, actor_id: &str, page: u64) -> Result<Option<String>, JavBusError> {
        let url = if page == 1 {
            format!("{}/star/{actor_id}", self.host)
        } else {
            format!("{}/star/{actor_id}/{page}", self.host)
        };
        self.retry(|| self.get_text(Request::get(url.clone()), &NOT_FOUND_STATUSES))
            .await
    }

    /// The video IDs of a star page (in order, without repeats) and the page numbers it links.
    fn parse_actor_page(&self, actor_id: &str, html: &str) -> Result<(Vec<String>, BTreeSet<u64>), JavBusError> {
        let doc = Document::parse(html)?;
        let mut ids: Vec<String> = Vec::new();
        for item in doc.select("a.movie-box").items() {
            let (_, path) = url_parts(&item.attr("href").unwrap_or_default())?;
            let path = path.trim_end_matches('/');
            let last = path.rsplit('/').next().unwrap_or_default();
            let video_id = py_strip(last).to_uppercase();
            if !video_id.is_empty() && !ids.contains(&video_id) {
                ids.push(video_id);
            }
        }
        let mut linked_pages = BTreeSet::new();
        for item in doc.select("a[href]").items() {
            let (netloc, path) = url_parts(&item.attr("href").unwrap_or_default())?;
            if !netloc.is_empty() && netloc != self.netloc {
                continue;
            }
            if let Some(page) = page_number(&path, actor_id) {
                linked_pages.insert(page);
            }
        }
        Ok((ids, linked_pages))
    }

    /// Magnet links with their sizes for a video ID, best first ([`magnet_score`]).
    ///
    /// JavBus marks a magnet's quality with tags next to its title (字幕, 高清); a tagged
    /// upload is the one a person would pick over a merely larger one.
    pub async fn get_magnets(&self, video_id: &str) -> Result<Vec<JavBusMagnet>, JavBusError> {
        self.retry(|| self.get_magnets_once(video_id)).await
    }

    async fn get_magnets_once(&self, video_id: &str) -> Result<Vec<JavBusMagnet>, JavBusError> {
        let url = format!("{}/{video_id}", self.host);
        let content = self.get_text(Request::get(url.clone()), &[]).await?.unwrap_or_default();
        let (Some(gid), Some(uc), Some(img)) = (
            GID.captures(&content).map(|found| found[1].to_owned()),
            UC.captures(&content).map(|found| found[1].to_owned()),
            IMG.captures(&content).map(|found| found[1].to_owned()),
        ) else {
            return Ok(Vec::new());
        };
        let floor = random_floor();
        let ajax_url = format!(
            "{}/ajax/uncledatoolsbyajax.php?gid={gid}&lang=zh&img={img}&uc={uc}&floor={floor}",
            self.host
        );
        let text = self
            .get_text(Request::get(ajax_url).header("Referer", url), &[])
            .await?
            .unwrap_or_default();
        let doc = Document::parse(&text)?;
        parse_magnet_rows(&doc, video_id)
    }
}

/// The rows of JavBus's magnet answer, ranked.
fn parse_magnet_rows(doc: &Document, video_id: &str) -> Result<Vec<JavBusMagnet>, JavBusError> {
    let mut results: Vec<JavBusMagnet> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for row in doc.select("tr").items() {
        if row.select("td").len() < 2 {
            continue;
        }
        let Some(raw_magnet) = row
            .select(r#"td:first-child a[href^="magnet"]"#)
            .attr("href")
            .filter(|href| !href.is_empty())
        else {
            continue;
        };
        let Some(hash) = btih(&raw_magnet) else {
            continue;
        };
        let hash = hash.to_lowercase();
        if !seen.insert(hash.clone()) {
            continue;
        }
        let size_text = py_strip(&row.select("td:nth-child(2)").text()).to_owned();
        let size_int = match parse_size(&size_text) {
            Ok(size) => size,
            Err(SizeError::Invalid(..)) => 0,
            Err(error @ SizeError::NotDecimal(_)) => return Err(JavBusError::Value(error.to_string())),
        };
        let tags = badge_tags(&row.select("td:first-child a"));
        let score = magnet_score(size_int, tags.len());
        results.push(JavBusMagnet {
            magnet: format!("magnet:?xt=urn:btih:{hash}&dn={video_id}"),
            size: size_text,
            size_int,
            tags,
            score,
        });
    }
    results.sort_by_key(|result| std::cmp::Reverse(result.score));
    Ok(results)
}

/// The first anchor is the title; the rest are the quality badges.
fn badge_tags(anchors: &Selection<'_>) -> Vec<String> {
    anchors
        .items()
        .skip(1)
        .map(|anchor| py_strip(&anchor.text()).to_owned())
        .filter(|tag| !tag.is_empty())
        .collect()
}

async fn notify(progress: Option<&PageProgress>, completed: u64, total: Option<u64>, current: Option<u64>) {
    if let Some(callback) = progress {
        callback(completed, total, current).await;
    }
}

/// `scrape`'s body over its two calls (`get_total_page`, `scrape_one_page`), so its order of
/// progress reports and its checks can be tested the way Python's test patches them.
pub(crate) async fn scrape_pages<Total, Fetch, Page>(
    progress: Option<&PageProgress>,
    total: Total,
    fetch: Fetch,
) -> Result<Vec<String>, JavBusError>
where
    Total: Future<Output = Result<u64, JavBusError>>,
    Fetch: Fn(u64) -> Page,
    Page: Future<Output = Result<Vec<String>, JavBusError>>,
{
    notify(progress, 0, None, None).await;
    let total_page = total.await?;
    notify(progress, 0, Some(total_page), None).await;
    let mut pending: FuturesUnordered<_> = (1..=total_page)
        .map(|page| {
            let fetched = fetch(page);
            async move { (page, fetched.await) }
        })
        .collect();
    let mut by_page: HashMap<u64, Vec<String>> = HashMap::new();
    let mut completed = 0;
    while let Some((page, result)) = pending.next().await {
        // An error drops (cancels) the pages still being fetched.
        by_page.insert(page, result?);
        completed += 1;
        notify(progress, completed, Some(total_page), Some(page)).await;
    }
    let pages: Vec<Vec<String>> = (1..=total_page)
        .map(|page| by_page.remove(&page).unwrap_or_default())
        .collect();
    if total_page > 1
        && let Some(empty) = pages.iter().position(Vec::is_empty)
    {
        return Err(JavBusError::Pagination(format!(
            "JavBus actor pagination returned an empty page at {}",
            empty + 1
        )));
    }
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    Ok(pages
        .into_iter()
        .flatten()
        .filter(|video_id| seen.insert(video_id.clone()))
        .collect())
}

/// Used by [`Arc`]-held callers that build a progress callback from a closure.
pub fn progress_callback(
    callback: impl Fn(u64, Option<u64>, Option<u64>) + Send + Sync + 'static,
) -> Arc<PageProgress> {
    Arc::new(move |completed, total, current| {
        callback(completed, total, current);
        Box::pin(async {})
    })
}

#[cfg(test)]
mod tests;
