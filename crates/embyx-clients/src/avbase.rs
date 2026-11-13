//! AVBase catalog client: talents with their aliases, their works, a work's cast.
//!
//! Python origin: `src/embyx_manager/clients/avbase.py`. AVBase puts a Cloudflare challenge in
//! front of its pages that plain HTTP clients fail, but the Next.js data routes behind them
//! answer to a browser TLS fingerprint ([`session::ImpersonatingSession`]). The data routes
//! are keyed by the site's build id, which changes on every deployment: it is read from the
//! home page once and refreshed when a route answers 404, so a real miss is only reported
//! after the id was confirmed.
//!
//! The answers are walked the way Python walks the decoded JSON (`.get`, `str()`, `int()`),
//! so a value of an unexpected type fails with the exception Python raises
//! ([`AvbaseError::Raised`]); only [`AvbaseError::Catalog`] and [`AvbaseError::Unavailable`]
//! are `AvbaseError`s to the callers that catch those.

pub mod session;

use std::fmt;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use chrono::{Datelike as _, NaiveDate};
use embyx_core::pyjson::{self, JsonEncoding, Value};
use embyx_core::pytext::{casefold, py_decimal_value, py_isdigit, py_repr, py_strip};
use embyx_core::pyurl::{quote, unquote, urlsplit};
use futures_util::future::BoxFuture;
use tokio::sync::{Mutex, Semaphore};

use crate::etree;
use crate::pyvalue::{self, Raised};

pub const DEFAULT_HOST: &str = "https://www.avbase.net";
pub const WORKS_PER_PAGE: i128 = 30;

/// One answer of the session (`curl_cffi`'s `Response`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvbaseResponse {
    pub status: u16,
    pub content_type: Option<String>,
    /// `response.text`.
    pub text: String,
    /// `response.content`.
    pub body: Vec<u8>,
}

impl AvbaseResponse {
    pub fn new(status: u16, content_type: Option<String>, body: Vec<u8>) -> Self {
        let text = session::curl_text(&body, content_type.as_deref());
        Self {
            status,
            content_type,
            text,
            body,
        }
    }

    /// `response.json()`: `json.loads(content)`, or the text when the charset is not one JSON
    /// reads natively.
    fn json(&self) -> Result<Value, AvbaseError> {
        let decode_error = |error: &pyjson::LoadError, text: &str| {
            AvbaseError::Raised(Raised::new("JSONDecodeError", error.python_message(text)))
        };
        let text = match pyjson::detect_json_encoding(&self.body) {
            JsonEncoding::Utf8(bytes) => match std::str::from_utf8(bytes) {
                Ok(text) => text.to_owned(),
                Err(error) => {
                    return Err(AvbaseError::Raised(Raised::new(
                        "UnicodeDecodeError",
                        crate::http::utf8_error_message(bytes, &error),
                    )));
                }
            },
            JsonEncoding::Decoded(Some(text)) => text,
            // Python names the codec and the bytes; this names neither (D29).
            JsonEncoding::Decoded(None) => {
                return Err(AvbaseError::Raised(Raised::new(
                    "UnicodeDecodeError",
                    "the body is not valid UTF-16 or UTF-32",
                )));
            }
        };
        pyjson::loads(&text).map_err(|error| decode_error(&error, &text))
    }
}

/// The session `AvbaseClient` reads through (`session=` in Python; a fake in tests).
pub trait AvbaseSession: Send + Sync {
    /// `await session.get(url, params=params)`.
    fn get<'a>(
        &'a self,
        url: &'a str,
        params: &'a [(&'a str, String)],
    ) -> BoxFuture<'a, Result<AvbaseResponse, AvbaseError>>;
}

/// What a catalog read fails with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AvbaseError {
    /// `AvbaseError`: the catalog could not be read.
    Catalog(String),
    /// `AvbaseUnavailableError`: a challenge page or another non-JSON answer stood in for the
    /// data route.
    Unavailable(String),
    /// A failed request (`curl_cffi`'s `RequestsError` family): not an `AvbaseError`.
    Transport { class: &'static str, message: String },
    /// An exception the walk over an answer raised (`JSONDecodeError`, `KeyError`, ...).
    Raised(Raised),
}

impl AvbaseError {
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Catalog(_) => "AvbaseError",
            Self::Unavailable(_) => "AvbaseUnavailableError",
            Self::Transport { class, .. } => class,
            Self::Raised(raised) => raised.class,
        }
    }

    /// `isinstance(exc, AvbaseError)`.
    pub fn is_avbase_error(&self) -> bool {
        matches!(self, Self::Catalog(_) | Self::Unavailable(_))
    }
}

impl fmt::Display for AvbaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(message) | Self::Unavailable(message) | Self::Transport { message, .. } => {
                f.write_str(message)
            }
            Self::Raised(raised) => f.write_str(&raised.message),
        }
    }
}

impl std::error::Error for AvbaseError {}

impl From<Raised> for AvbaseError {
    fn from(raised: Raised) -> Self {
        Self::Raised(raised)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvbaseTalent {
    pub talent_id: i128,
    /// The primary name; AVBase lists the talent's page under it.
    pub name: String,
    /// Every other name the talent is credited under.
    pub aliases: Vec<String>,
    pub total_works: i128,
}

impl AvbaseTalent {
    /// `names`: the primary name, then the aliases.
    pub fn names(&self) -> Vec<String> {
        std::iter::once(self.name.clone())
            .chain(self.aliases.iter().cloned())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvbaseCastMember {
    pub actor_id: i128,
    pub name: String,
    /// The talent the credited name belongs to; `None` when the listing omits it.
    pub talent_id: Option<i128>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvbaseWork {
    /// The bare ID, without the storefront prefix (`moodyz:MIZD-555` -> `MIZD-555`).
    pub work_id: String,
    pub prefix: String,
    pub title: String,
    /// The earliest product date; an approximation of the release.
    pub release_date: Option<NaiveDate>,
    pub cast: Vec<AvbaseCastMember>,
}

/// The constructor's arguments.
#[derive(Clone, PartialEq)]
pub struct AvbaseOptions {
    pub host: String,
    pub timeout: Duration,
    pub max_concurrency: usize,
    pub proxy: Option<String>,
}

impl Default for AvbaseOptions {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_owned(),
            timeout: Duration::from_secs(30),
            max_concurrency: 4,
            proxy: None,
        }
    }
}

impl fmt::Debug for AvbaseOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AvbaseOptions")
            .field("host", &self.host)
            .field("timeout", &self.timeout)
            .field("max_concurrency", &self.max_concurrency)
            .field("proxy", &self.proxy.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// `AvbaseClient`.
pub struct AvbaseClient {
    host: String,
    session: Arc<dyn AvbaseSession>,
    semaphore: Semaphore,
    build_id: Mutex<Option<String>>,
}

impl fmt::Debug for AvbaseClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AvbaseClient")
            .field("host", &self.host)
            .finish_non_exhaustive()
    }
}

static BUILD_ID: LazyLock<regex::Regex> =
    LazyLock::new(|| embyx_core::pyre::compile(r#""buildId":"([^"]+)""#, false, false));
static TALENT_PATH: LazyLock<regex::Regex> =
    LazyLock::new(|| embyx_core::pyre::compile(r"/talents/([^/?#]+)(?:/feed)?/?$", false, false));
static FEED_TITLE: LazyLock<regex::Regex> = LazyLock::new(|| embyx_core::pyre::compile("「(.+?)」", false, false));

impl AvbaseClient {
    /// The session options `AvbaseClient.__init__` builds its `curl_cffi` session with.
    pub fn session_options(options: &AvbaseOptions) -> session::SessionOptions {
        let host = options.host.trim_end_matches('/');
        session::SessionOptions {
            headers: vec![
                ("Referer".to_owned(), format!("{host}/")),
                ("Accept-Language".to_owned(), "ja,en;q=0.8".to_owned()),
            ],
            timeout: options.timeout,
            proxy: options.proxy.clone(),
            roots: Vec::new(),
        }
    }

    /// With the impersonating session.
    pub fn new(options: &AvbaseOptions) -> Result<Self, String> {
        let session = session::ImpersonatingSession::new(&Self::session_options(options))?;
        Ok(Self::with_session(options, Arc::new(session)))
    }

    /// With another session (`session=`).
    pub fn with_session(options: &AvbaseOptions, session: Arc<dyn AvbaseSession>) -> Self {
        Self {
            host: options.host.trim_end_matches('/').to_owned(),
            session,
            semaphore: Semaphore::new(options.max_concurrency.max(1)),
            build_id: Mutex::new(None),
        }
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    async fn get(&self, url: &str, params: &[(&str, String)]) -> Result<AvbaseResponse, AvbaseError> {
        let _slot = self.semaphore.acquire().await;
        self.session.get(url, params).await
    }

    /// The site's build id, read from the home page once (again with `refresh`).
    pub async fn build_id(&self, refresh: bool) -> Result<String, AvbaseError> {
        let mut cached = self.build_id.lock().await;
        if cached.is_none() || refresh {
            let response = self.get(&format!("{}/", self.host), &[]).await?;
            let found = (response.status == 200)
                .then(|| BUILD_ID.captures(&response.text).map(|found| found[1].to_owned()))
                .flatten();
            let Some(build) = found else {
                return Err(AvbaseError::Unavailable(format!(
                    "no build id on the AVBase home page (HTTP {})",
                    response.status
                )));
            };
            *cached = Some(build);
        }
        Ok(cached.clone().unwrap_or_default())
    }

    /// The talent credited as `name`; any of its aliases finds the same one.
    pub async fn talent(&self, name: &str) -> Result<Option<AvbaseTalent>, AvbaseError> {
        match self.talent_page(name, 1).await? {
            Some(props) => Ok(Some(talent_from_props(&props)?)),
            None => Ok(None),
        }
    }

    /// The talent behind whatever a person pastes: a name, an id, or a talent URL.
    ///
    /// Talent pages are addressed by name, so a numeric id (or an id URL, which is what the
    /// feed links carry) goes through the feed, whose channel link names the talent.
    pub async fn find_talent(&self, query: &str) -> Result<Option<AvbaseTalent>, AvbaseError> {
        let Ok((talent_id, name)) = parse_talent_query(query) else {
            return Ok(None);
        };
        if let Some(talent_id) = talent_id {
            return self.talent_by_id(talent_id).await;
        }
        if name.is_empty() {
            return Ok(None);
        }
        self.talent(&name).await
    }

    async fn talent_by_id(&self, talent_id: i128) -> Result<Option<AvbaseTalent>, AvbaseError> {
        let response = self
            .get(&format!("{}/talents/{talent_id}/feed", self.host), &[])
            .await?;
        if response.status == 404 {
            return Ok(None);
        }
        if response.status != 200 {
            return Err(AvbaseError::Unavailable(format!(
                "AVBase answered HTTP {} for the feed of talent {talent_id}",
                response.status
            )));
        }
        let Some(name) = talent_name_from_feed(&response.text)? else {
            return Ok(None);
        };
        let talent = self.talent(&name).await?;
        if let Some(talent) = talent
            && talent.talent_id == talent_id
        {
            return Ok(Some(talent));
        }
        Ok(Some(AvbaseTalent {
            talent_id,
            name,
            aliases: Vec::new(),
            total_works: 0,
        }))
    }

    /// The talent's works page by page, newest release first.
    pub fn talent_pages<'a>(&'a self, name: &'a str) -> TalentPages<'a> {
        TalentPages {
            client: self,
            name,
            next: 1,
            pages: None,
        }
    }

    /// Every work of the talent credited as `name`, newest release first.
    pub async fn talent_works(&self, name: &str) -> Result<Vec<AvbaseWork>, AvbaseError> {
        let mut pages = self.talent_pages(name);
        let mut works = Vec::new();
        while let Some((_, _, page)) = pages.next().await? {
            works.extend(page);
        }
        Ok(works)
    }

    /// Works matching a query (an ID, a name, a title fragment) as the site's search lists
    /// them. Listing entries credit actors by name only; talent ids come from [`Self::work`].
    pub async fn search_works(&self, query: &str) -> Result<Vec<AvbaseWork>, AvbaseError> {
        let Some(data) = self.data("works.json", &[("q", query.to_owned())]).await? else {
            return Ok(Vec::new());
        };
        let empty = Value::Dict(pyjson::Dict::new());
        let props = pyvalue::or(pyvalue::get(&data, "pageProps")?, &empty);
        let mut works = Vec::new();
        for entry in pyvalue::iterate(pyvalue::get(props, "works")?)? {
            works.push(work_from_listing(&entry)?);
        }
        Ok(works)
    }

    /// One work by its ID, with its credited cast and their talent ids.
    ///
    /// A work from a storefront with a prefix only answers under `<prefix>:<id>`; a bare ID
    /// goes through the search first, which reports the prefix.
    pub async fn work(&self, work_id: &str) -> Result<Option<AvbaseWork>, AvbaseError> {
        if work_id.contains(':') {
            return self.work_route(work_id).await;
        }
        let wanted = casefold(&strip_prefix(work_id));
        for candidate in self.search_works(work_id).await? {
            if casefold(&candidate.work_id) == wanted {
                let route_id = if candidate.prefix.is_empty() {
                    candidate.work_id.clone()
                } else {
                    format!("{}:{}", candidate.prefix, candidate.work_id)
                };
                return self.work_route(&route_id).await;
            }
        }
        Ok(None)
    }

    async fn work_route(&self, work_id: &str) -> Result<Option<AvbaseWork>, AvbaseError> {
        let path = format!("works/{}.json", quote(work_id, ""));
        let Some(data) = self.data(&path, &[("id", work_id.to_owned())]).await? else {
            return Ok(None);
        };
        let empty = Value::Dict(pyjson::Dict::new());
        let work = pyvalue::get(pyvalue::or(pyvalue::get(&data, "pageProps")?, &empty), "work")?;
        if pyvalue::truthy(work) {
            Ok(Some(work_from_detail(work)?))
        } else {
            Ok(None)
        }
    }

    async fn talent_page(&self, name: &str, page: i128) -> Result<Option<Value>, AvbaseError> {
        let mut params = vec![("name", name.to_owned())];
        if page > 1 {
            params.push(("page", page.to_string()));
        }
        let path = format!("talents/{}.json", quote(name, ""));
        let Some(data) = self.data(&path, &params).await? else {
            return Ok(None);
        };
        let empty = Value::Dict(pyjson::Dict::new());
        let props = pyvalue::or(pyvalue::get(&data, "pageProps")?, &empty).clone();
        let has_talent = pyvalue::truthy(pyvalue::get(&props, "talent")?);
        Ok(has_talent.then_some(props))
    }

    /// One Next.js data route; `None` when it does not exist.
    ///
    /// A 404 is ambiguous: the route may be gone, or the build id may be stale after a
    /// deployment. The id is refreshed once, and the request repeated only if it changed.
    async fn data(&self, path: &str, params: &[(&str, String)]) -> Result<Option<Value>, AvbaseError> {
        let mut build = self.build_id(false).await?;
        for _ in 0..2 {
            let response = self
                .get(&format!("{}/_next/data/{build}/{path}", self.host), params)
                .await?;
            if response.status == 404 {
                let refreshed = self.build_id(true).await?;
                if refreshed == build {
                    return Ok(None);
                }
                build = refreshed;
                continue;
            }
            if response.status != 200 {
                return Err(AvbaseError::Unavailable(format!(
                    "AVBase answered HTTP {} for {path}",
                    response.status
                )));
            }
            if !response.content_type.as_deref().unwrap_or_default().contains("json") {
                return Err(AvbaseError::Unavailable(format!(
                    "AVBase answered {path} with something other than JSON (a challenge page?)"
                )));
            }
            let payload = response.json()?;
            if !matches!(payload, Value::Dict(_)) {
                return Err(AvbaseError::Unavailable(format!(
                    "AVBase answered {path} with a non-object"
                )));
            }
            return Ok(Some(payload));
        }
        Ok(None)
    }
}

/// `talent_pages(name)`: an async iterator of `(page, pages, works)`.
pub struct TalentPages<'a> {
    client: &'a AvbaseClient,
    name: &'a str,
    next: i128,
    pages: Option<i128>,
}

impl fmt::Debug for TalentPages<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TalentPages")
            .field("name", &self.name)
            .field("next", &self.next)
            .field("pages", &self.pages)
            .finish_non_exhaustive()
    }
}

impl TalentPages<'_> {
    /// The next page, or `None` after the last (or a page that is gone).
    pub async fn next(&mut self) -> Result<Option<(i128, i128, Vec<AvbaseWork>)>, AvbaseError> {
        let page = self.next;
        if let Some(pages) = self.pages
            && page > pages
        {
            return Ok(None);
        }
        let Some(props) = self.client.talent_page(self.name, page).await? else {
            if self.pages.is_none() {
                return Err(AvbaseError::Catalog(format!(
                    "AVBase has no talent named {}",
                    py_repr(self.name)
                )));
            }
            self.next = i128::MAX;
            self.pages = Some(0);
            return Ok(None);
        };
        if self.pages.is_none() {
            let total = pyvalue::py_int(pyvalue::or(pyvalue::get(&props, "total")?, &Value::Int(0)))?;
            // `max(1, -(-total // 30))`: ceiling division, floor semantics for negatives.
            self.pages = Some(1.max(-((-total).div_euclid(WORKS_PER_PAGE))));
        }
        let mut works = Vec::new();
        for entry in pyvalue::iterate(pyvalue::get(&props, "works")?)? {
            works.push(work_from_listing(&entry)?);
        }
        self.next = page + 1;
        Ok(Some((page, self.pages.unwrap_or(1), works)))
    }
}

/// Split a pasted talent reference into a numeric id or a name.
///
/// Accepts a bare name or alias, a bare id, or an AVBase talent URL of either form (with or
/// without the trailing `/feed`). Anything else is a `ValueError` (the message is returned).
pub fn parse_talent_query(query: &str) -> Result<(Option<i128>, String), String> {
    let mut text = py_strip(query).to_owned();
    if text.contains("://") || text.starts_with('/') {
        let split = urlsplit(&text, "").map_err(|error| error.0)?;
        let Some(found) = TALENT_PATH.captures(&split.path) else {
            return Err(format!("not an AVBase talent URL: {}", py_repr(query)));
        };
        py_strip(&unquote(&found[1])).clone_into(&mut text);
    }
    if text.is_empty() {
        return Err("empty talent reference".to_owned());
    }
    if py_isdigit(&text) {
        return embyx_core::pytext::py_int(&text).map(|id| (Some(id), String::new()));
    }
    Ok((None, text))
}

/// The talent name from its feed: the channel link is the name-addressed page.
fn talent_name_from_feed(xml_text: &str) -> Result<Option<String>, AvbaseError> {
    let Ok(root) = etree::fromstring(xml_text.as_bytes()) else {
        // Any malformed feed simply names nobody.
        return Ok(None);
    };
    let Some(channel) = root.find("channel") else {
        return Ok(None);
    };
    let link = py_strip(channel.findtext("link").unwrap_or_default()).to_owned();
    let segment = if link.is_empty() {
        String::new()
    } else {
        let split = urlsplit(&link, "").map_err(|error| AvbaseError::Raised(Raised::new("ValueError", error.0)))?;
        let path = split.path.trim_end_matches('/');
        py_strip(&unquote(path.rsplit('/').next().unwrap_or_default())).to_owned()
    };
    if !segment.is_empty() && !py_isdigit(&segment) {
        return Ok(Some(segment));
    }
    let title = channel.findtext("title").unwrap_or_default();
    Ok(FEED_TITLE.captures(title).map(|found| py_strip(&found[1]).to_owned()))
}

/// `moodyz:MIZD-555` -> `MIZD-555`; a bare ID is returned as is.
pub fn strip_prefix(work_id: &str) -> String {
    py_strip(work_id.rsplit(':').next().unwrap_or_default()).to_owned()
}

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// The value of a run of Unicode decimal digits (`int()` on a `\d` group).
fn decimal(text: &str) -> Option<u32> {
    text.chars().try_fold(0_u32, |value, ch| {
        value.checked_mul(10)?.checked_add(py_decimal_value(ch)?)
    })
}

/// `^[A-Za-z]{3} ([A-Za-z]{3}) (\d{1,2}) (\d{4})`: month name, day, year.
fn js_date(value: &str) -> Option<(&str, &str, &str)> {
    let chars: Vec<(usize, char)> = value.char_indices().collect();
    let letters =
        |from: usize| (from..from + 3).all(|index| chars.get(index).is_some_and(|(_, ch)| ch.is_ascii_alphabetic()));
    let at = |index: usize| chars.get(index).map_or(value.len(), |(offset, _)| *offset);
    let is_digit = |index: usize| chars.get(index).is_some_and(|(_, ch)| py_decimal_value(*ch).is_some());
    if !letters(0) || chars.get(3).map(|(_, ch)| *ch) != Some(' ') || !letters(4) {
        return None;
    }
    if chars.get(7).map(|(_, ch)| *ch) != Some(' ') {
        return None;
    }
    // `\d{1,2}` is greedy but gives one back when the year needs it.
    for day_digits in [2_usize, 1] {
        if !(8..8 + day_digits).all(is_digit) {
            continue;
        }
        let space = 8 + day_digits;
        if chars.get(space).map(|(_, ch)| *ch) != Some(' ') {
            continue;
        }
        if (space + 1..space + 5).all(is_digit) {
            return Some((
                &value[at(4)..at(7)],
                &value[at(8)..at(space)],
                &value[at(space + 1)..at(space + 5)],
            ));
        }
    }
    None
}

/// `date.fromisoformat(text)` (Python 3.13's C parser): `YYYY-MM-DD`, `YYYYMMDD`, and the ISO
/// week forms `YYYY-Www`, `YYYY-Www-D`, `YYYYWww`, `YYYYWwwD`, ASCII digits only. Like Python
/// it looks only at strings of 7, 8 or 10 UTF-8 bytes and ignores what follows a complete
/// date, so `20260219T0` is 2026-02-19.
fn fromisoformat(text: &str) -> Option<NaiveDate> {
    let bytes = text.as_bytes();
    if !matches!(bytes.len(), 7 | 8 | 10) {
        return None;
    }
    let digits = |at: usize, count: usize| -> Option<u32> {
        let part = bytes.get(at..at + count)?;
        part.iter()
            .all(u8::is_ascii_digit)
            .then(|| part.iter().fold(0, |number, byte| number * 10 + u32::from(byte - b'0')))
    };
    let year = i32::try_from(digits(0, 4)?).ok()?;
    let dashed = bytes.get(4) == Some(&b'-');
    let mut at = if dashed { 5 } else { 4 };
    let date = if bytes.get(at) == Some(&b'W') {
        let week = digits(at + 1, 2)?;
        at += 3;
        let weekday = if at < bytes.len() {
            if dashed {
                if bytes.get(at) != Some(&b'-') {
                    return None;
                }
                at += 1;
            }
            digits(at, 1)?
        } else {
            1
        };
        let weekday = match weekday {
            1 => chrono::Weekday::Mon,
            2 => chrono::Weekday::Tue,
            3 => chrono::Weekday::Wed,
            4 => chrono::Weekday::Thu,
            5 => chrono::Weekday::Fri,
            6 => chrono::Weekday::Sat,
            7 => chrono::Weekday::Sun,
            _ => return None,
        };
        if year == 0 {
            return None;
        }
        NaiveDate::from_isoywd_opt(year, week, weekday)?
    } else {
        let month = digits(at, 2)?;
        at += 2;
        if dashed {
            if bytes.get(at) != Some(&b'-') {
                return None;
            }
            at += 1;
        }
        NaiveDate::from_ymd_opt(year, month, digits(at, 2)?)?
    };
    // `MINYEAR`..`MAXYEAR`: ISO week 52 of 9999 runs into the year 10000.
    (1..=9999).contains(&date.year()).then_some(date)
}

/// AVBase dates come as JavaScript `Date` strings or ISO text.
pub fn parse_release_date(value: &Value) -> Option<NaiveDate> {
    let Value::Str(value) = value else {
        return None;
    };
    if value.is_empty() {
        return None;
    }
    if let Some((month, day, year)) = js_date(value) {
        let month = MONTHS.iter().position(|name| *name == month)?;
        let month = u32::try_from(month + 1).ok()?;
        let year = i32::try_from(decimal(year)?).ok()?;
        if year == 0 {
            return None;
        }
        return NaiveDate::from_ymd_opt(year, month, decimal(day)?);
    }
    let head: String = value.chars().take(10).collect();
    fromisoformat(&head)
}

fn talent_from_props(props: &Value) -> Result<AvbaseTalent, AvbaseError> {
    let talent = pyvalue::item(props, "talent")?;
    let empty = Value::Dict(pyjson::Dict::new());
    let primary = pyvalue::or(pyvalue::get(talent, "primary")?, &empty);
    let primary_name = pyvalue::get(primary, "name")?;
    let name_value = if pyvalue::truthy(primary_name) {
        primary_name.clone()
    } else {
        pyvalue::or(pyvalue::get(props, "name")?, &pyvalue::EMPTY_STR).clone()
    };
    let name = py_strip(&pyvalue::py_str(&name_value)).to_owned();
    let mut seen = vec![name.clone()];
    let mut aliases = Vec::new();
    for actor in pyvalue::iterate(pyvalue::get(talent, "actors")?)? {
        let actor = pyvalue::or(&actor, &empty);
        let alias_value = pyvalue::or(pyvalue::get(actor, "name")?, &pyvalue::EMPTY_STR);
        let alias = py_strip(&pyvalue::py_str(alias_value)).to_owned();
        if !alias.is_empty() && !seen.contains(&alias) {
            seen.push(alias.clone());
            aliases.push(alias);
        }
    }
    Ok(AvbaseTalent {
        talent_id: pyvalue::py_int(pyvalue::item(talent, "id")?)?,
        name,
        aliases,
        total_works: pyvalue::py_int(pyvalue::or(pyvalue::get(props, "total")?, &Value::Int(0)))?,
    })
}

fn cast_from_actor(actor: &Value) -> Result<Option<AvbaseCastMember>, AvbaseError> {
    let actor_id = pyvalue::get(actor, "id")?;
    let name_value = pyvalue::or(pyvalue::get(actor, "name")?, &pyvalue::EMPTY_STR);
    let name = py_strip(&pyvalue::py_str(name_value)).to_owned();
    let Some(actor_id) = pyvalue::as_int(actor_id) else {
        return Ok(None);
    };
    if name.is_empty() {
        return Ok(None);
    }
    let talent = pyvalue::get(actor, "talent")?;
    let talent_id = match talent {
        Value::Dict(_) => pyvalue::as_int(pyvalue::get(talent, "id")?),
        _ => None,
    };
    Ok(Some(AvbaseCastMember {
        actor_id,
        name,
        talent_id,
    }))
}

fn text_field(entry: &Value, key: &str) -> Result<String, AvbaseError> {
    Ok(pyvalue::py_str(pyvalue::or(
        pyvalue::get(entry, key)?,
        &pyvalue::EMPTY_STR,
    )))
}

fn work_from_listing(entry: &Value) -> Result<AvbaseWork, AvbaseError> {
    let mut cast = Vec::new();
    for actor in pyvalue::iterate(pyvalue::get(entry, "actors")?)? {
        if matches!(actor, Value::Dict(_))
            && let Some(member) = cast_from_actor(&actor)?
        {
            cast.push(member);
        }
    }
    Ok(AvbaseWork {
        work_id: strip_prefix(&text_field(entry, "work_id")?),
        prefix: text_field(entry, "prefix")?,
        title: text_field(entry, "title")?,
        release_date: parse_release_date(pyvalue::get(entry, "min_date")?),
        cast,
    })
}

fn work_from_detail(work: &Value) -> Result<AvbaseWork, AvbaseError> {
    let mut members = Vec::new();
    for entry in pyvalue::iterate(pyvalue::get(work, "casts")?)? {
        let actor = match &entry {
            Value::Dict(_) => pyvalue::get(&entry, "actor")?.clone(),
            _ => Value::Null,
        };
        if matches!(actor, Value::Dict(_))
            && let Some(member) = cast_from_actor(&actor)?
        {
            members.push(member);
        }
    }
    Ok(AvbaseWork {
        work_id: strip_prefix(&text_field(work, "work_id")?),
        prefix: text_field(work, "prefix")?,
        title: text_field(work, "title")?,
        release_date: parse_release_date(pyvalue::get(work, "min_date")?),
        cast: members,
    })
}

#[cfg(test)]
mod tests;
