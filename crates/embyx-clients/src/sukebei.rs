//! Sukebei (nyaa.si) search, and the magnet picked from its results.
//!
//! Python origin: `src/embyx_manager/clients/sukebei.py` and the parts of nyaapy 0.7 it uses
//! (`nyaapy.parser.parse_nyaa`, `sukebei_categories`). The search page is read with libxml2's
//! HTML parser semantics ([`crate::pyquery::Document::parse_html`]); nyaapy's walk over it,
//! with its index arithmetic and the exceptions it lets through, is copied.

use std::fmt;
use std::time::Duration;

use embyx_core::pytext::{py_decimal_value, py_repr, py_strip};
use tokio::sync::Semaphore;

use crate::http::{HttpxError, response_text, with_params};
use crate::net::retry::{Sleeper, tokio_sleeper};
use crate::net::{BuildError, ClientOptions, HttpClient, Request};
use crate::pyquery::{Document, ParseError, lxml_text};
use crate::size::{SizeError, parse_size};
use crate::tenacity;

/// `nyaapy.torrent.TorrentSite.SUKEBEINYAASI`.
pub const SITE_URL: &str = "https://sukebei.nyaa.si";
/// Prefer the largest trusted result unless an untrusted one is materially larger.
pub const TRUSTED_SIZE_TOLERANCE: f64 = 0.8;

/// One result of `parse_nyaa` (its dict).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NyaaTorrent {
    pub id: String,
    /// `None` when nyaapy does not know the category.
    pub category: Option<String>,
    pub url: String,
    pub name: String,
    pub download_url: String,
    pub magnet: String,
    pub size: String,
    pub date: String,
    pub seeders: String,
    pub leechers: String,
    pub completed_downloads: String,
    /// `remake`, `trusted` or `default`.
    pub kind: String,
}

/// What a sukebei search fails with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SukebeiError {
    Http(HttpxError),
    /// An exception nyaapy or `parse_size` lets through (`AttributeError`, `IndexError`,
    /// `ValueError`), by class and message.
    Raised {
        class: &'static str,
        message: String,
    },
}

impl SukebeiError {
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Http(error) => error.class_name(),
            Self::Raised { class, .. } => class,
        }
    }

    fn index_error() -> Self {
        Self::Raised {
            class: "IndexError",
            message: "list index out of range".to_owned(),
        }
    }
}

impl fmt::Display for SukebeiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(error) => error.fmt(f),
            Self::Raised { message, .. } => f.write_str(message),
        }
    }
}

impl std::error::Error for SukebeiError {}

impl From<HttpxError> for SukebeiError {
    fn from(error: HttpxError) -> Self {
        Self::Http(error)
    }
}

/// `left >= right` for an `int` and a `float`, compared exactly as Python compares them.
fn int_at_least(left: i128, right: f64) -> bool {
    if right.is_nan() {
        return false;
    }
    let bound = right.ceil();
    // `i128::MAX as f64` is 2^127, exactly; every whole float below it converts exactly.
    #[expect(clippy::cast_precision_loss, reason = "2^127 is exact in f64")]
    let limit = i128::MAX as f64;
    if bound >= limit {
        return false;
    }
    if bound < -limit {
        return true;
    }
    #[expect(clippy::cast_possible_truncation, reason = "a whole number inside the i128 range")]
    let bound = bound as i128;
    left >= bound
}

/// The search's `params` dict as Python's `%s` shows it: `f` and `p` are integers.
fn params_repr(params: &[(&str, String)]) -> String {
    let items: Vec<String> = params
        .iter()
        .map(|(key, value)| {
            let value = if matches!(*key, "f" | "p") {
                value.clone()
            } else {
                py_repr(value)
            };
            format!("{}: {value}", py_repr(key))
        })
        .collect();
    format!("{{{}}}", items.join(", "))
}

/// `nyaapy.parser.sukebei_categories(b)`: `None` (after printing) for an unknown pair; a
/// value without `_` raises `IndexError`.
fn sukebei_category(text: &str) -> Result<Option<String>, SukebeiError> {
    let code = text.replace("?c=", "");
    let mut parts = code.split('_');
    let category = parts.next().unwrap_or_default();
    let Some(subcategory) = parts.next() else {
        return Err(SukebeiError::index_error());
    };
    let name = match (category, subcategory) {
        ("1", "1") => Some("Art - Anime"),
        ("1", "2") => Some("Art - Doujinshi"),
        ("1", "3") => Some("Art - Games"),
        ("1", "4") => Some("Art - Manga"),
        ("1", "5") => Some("Art - Pictures"),
        ("2", "1") => Some("Real Life - Photobooks & Pictures"),
        ("2", "2") => Some("Real Life - Videos"),
        _ => {
            // nyaapy prints "Unable to get Sukebei category name" to standard output.
            tracing::debug!("Unable to get Sukebei category name");
            None
        }
    };
    Ok(name.map(str::to_owned))
}

/// `nyaapy.parser.parse_nyaa(text, limit=None, site=SUKEBEINYAASI)`.
pub fn parse_nyaa(text: &str) -> Result<Vec<NyaaTorrent>, SukebeiError> {
    let tree = match Document::parse_html(text) {
        Ok(tree) => tree,
        // lxml refuses a `str` that declares its encoding.
        Err(error @ ParseError::EncodingDeclaration) => {
            return Err(SukebeiError::Raised {
                class: "ValueError",
                message: error.to_string(),
            });
        }
        // `etree.fromstring` gives None for an empty document; nyaapy calls `.xpath` on it.
        Err(_) => {
            return Err(SukebeiError::Raised {
                class: "AttributeError",
                message: "'NoneType' object has no attribute 'xpath'".to_owned(),
            });
        }
    };
    let mut torrents = Vec::new();
    // `//tbody//tr`: rows anywhere below a tbody the page itself has.
    for row in tree.select("tbody tr").elements() {
        let mut block: Vec<String> = Vec::new();
        for cell in row.children().filter_map(scraper::ElementRef::wrap) {
            if cell.value().name() != "td" {
                continue;
            }
            for link in cell.children().filter_map(scraper::ElementRef::wrap) {
                if link.value().name() != "a" {
                    continue;
                }
                let Some(href) = link.value().attr("href") else {
                    return Err(SukebeiError::Raised {
                        class: "AttributeError",
                        message: "'NoneType' object has no attribute 'split'".to_owned(),
                    });
                };
                let href = href.rsplit('/').next().unwrap_or_default();
                let tail: String = {
                    let chars: Vec<char> = href.chars().collect();
                    chars[chars.len().saturating_sub(9)..].iter().collect()
                };
                if tail != "#comments" {
                    block.push(href.to_owned());
                    if let Some(text) = lxml_text(link) {
                        let text = py_strip(&text);
                        if !text.is_empty() {
                            block.push(text.to_owned());
                        }
                    }
                }
            }
            if let Some(text) = lxml_text(cell) {
                let text = py_strip(&text);
                if !text.is_empty() {
                    block.push(text.to_owned());
                }
            }
        }
        let kind = match row.value().attr("class") {
            Some(class) if class.contains("danger") => "remake",
            Some(class) if class.contains("success") => "trusted",
            _ => "default",
        };
        block.push(kind.to_owned());
        let category = sukebei_category(block.first().ok_or_else(SukebeiError::index_error)?)?;
        if block.len() < 11 {
            // `torrent = {...}` raised IndexError, which nyaapy skips.
            continue;
        }
        torrents.push(NyaaTorrent {
            id: block[1].clone(),
            category,
            url: format!("{SITE_URL}/view/{}", block[1]),
            name: block[2].clone(),
            download_url: format!("{SITE_URL}/download/{}", block[3]),
            magnet: block[4].clone(),
            size: block[5].clone(),
            date: block[6].clone(),
            seeders: block[7].clone(),
            leechers: block[8].clone(),
            completed_downloads: block[9].clone(),
            kind: block[10].clone(),
        });
    }
    Ok(torrents)
}

/// `_LABEL_NUMBER_RE.fullmatch(keyword.upper())`: `([A-Z0-9]+)-(\d+)`.
fn label_number(keyword: &str) -> Option<(String, String)> {
    let upper = keyword.to_uppercase();
    let (label, number) = upper.split_once('-')?;
    let label_ok = !label.is_empty() && label.chars().all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit());
    let number_ok = !number.is_empty() && number.chars().all(|ch| py_decimal_value(ch).is_some());
    (label_ok && number_ok).then(|| (label.to_owned(), number.to_owned()))
}

/// A pattern for result titles that really are this AVID.
///
/// Sukebei matches the query as a prefix, so a search for NHDTC-180 also lists NHDTC-18006.
/// The number must therefore end where the title's number ends; the separator and zero
/// padding may differ (nhdtc00180, `NHDTC_180`). IDs that are not plain label-number
/// (heydouga), and FC2 ones, whose titles carry a PPV between label and number, are not
/// filtered.
pub fn title_pattern(keyword: &str) -> Option<fancy_regex::Regex> {
    let (label, digits) = label_number(keyword)?;
    if label == "FC2" {
        return None;
    }
    // `int(match.group(2))`, written back in ASCII digits.
    let trimmed: String = digits
        .chars()
        .filter_map(py_decimal_value)
        .map(|digit| char::from_digit(digit, 10).unwrap_or('0'))
        .collect::<String>()
        .trim_start_matches('0')
        .to_owned();
    let number = if trimmed.is_empty() { "0".to_owned() } else { trimmed };
    let pattern = format!(r"(?<![A-Z]){}[-_ ]?0*{number}(?!\d)", fancy_regex::escape(&label));
    // Python's IGNORECASE also lets `İ`/`ı` match `I` and `[A-Z]` (D15).
    Some(embyx_core::pyre::compile_fancy(
        &embyx_core::pyre::python_ignore_case(&pattern, true),
        true,
        false,
    ))
}

/// The magnet `get_magnet` picks from search results: the largest that is this AVID, or the
/// largest trusted one if it is at least [`TRUSTED_SIZE_TOLERANCE`] of that; its `dn` is the
/// keyword. `Err` only for the `ValueError` `parse_size` lets through.
pub fn choose_magnet(keyword: &str, results: Vec<NyaaTorrent>) -> Result<Option<String>, SukebeiError> {
    if results.is_empty() {
        return Ok(None);
    }
    let pattern = title_pattern(keyword);
    let mut parsed: Vec<(NyaaTorrent, i128)> = Vec::new();
    for mut item in results {
        if let Some(pattern) = &pattern
            && !pattern.is_match(&item.name).unwrap_or(false)
        {
            tracing::info!(target: "embyx-manager.sukebei", "Skipping result {}: not {keyword}", item.name);
            continue;
        }
        let size_int = match parse_size(&item.size) {
            Ok(size) => size,
            Err(SizeError::Invalid(..)) => {
                tracing::warn!(target: "embyx-manager.sukebei", "Skipping result with invalid size {} for {keyword}", item.size);
                continue;
            }
            Err(error @ SizeError::NotDecimal(_)) => {
                return Err(SukebeiError::Raised {
                    class: "ValueError",
                    message: error.to_string(),
                });
            }
        };
        item.magnet = format!("{}&dn={keyword}", item.magnet.split('&').next().unwrap_or_default());
        parsed.push((item, size_int));
    }
    if parsed.is_empty() {
        return Ok(None);
    }
    parsed.sort_by_key(|(_, size)| std::cmp::Reverse(*size));
    let trusted = parsed.iter().find(|(item, _)| item.kind == "trusted").cloned();
    #[expect(clippy::cast_precision_loss, reason = "Python's `int * float` rounds the int first")]
    let chosen = match &trusted {
        Some(first) if int_at_least(first.1, parsed[0].1 as f64 * TRUSTED_SIZE_TOLERANCE) => {
            parsed.iter().position(|entry| entry == first).unwrap_or(0)
        }
        _ => 0,
    };
    let mut display: Vec<&(NyaaTorrent, i128)> = parsed.iter().take(5).collect();
    if let Some(first) = &trusted
        && !display.contains(&first)
    {
        display.push(first);
    }
    tracing::info!(target: "embyx-manager.sukebei", "Found {} results for {keyword} from searching:", parsed.len());
    let mut lines = vec!["Display top 5 largest + trusted:".to_owned()];
    for (index, (item, _)) in display.iter().enumerate() {
        let trust_mark = if item.kind == "trusted" { "🔰" } else { "--" };
        let chosen_mark = if index == chosen { "✅" } else { "--" };
        lines.push(format!("{chosen_mark}{trust_mark}{} {}", item.size, item.name));
        lines.push(item.magnet.clone());
    }
    tracing::info!(target: "embyx-manager.sukebei", "{}", lines.join("\n"));
    Ok(Some(parsed[chosen].0.magnet.clone()))
}

/// The constructor's arguments.
#[derive(Clone, PartialEq)]
pub struct SukebeiOptions {
    pub proxy: Option<String>,
    pub timeout: Duration,
    pub max_concurrency: usize,
}

impl Default for SukebeiOptions {
    fn default() -> Self {
        Self {
            proxy: None,
            timeout: Duration::from_secs(20),
            max_concurrency: 5,
        }
    }
}

impl fmt::Debug for SukebeiOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SukebeiOptions")
            .field("proxy", &self.proxy.as_ref().map(|_| "<redacted>"))
            .field("timeout", &self.timeout)
            .field("max_concurrency", &self.max_concurrency)
            .finish()
    }
}

/// `SukebeiClient`.
pub struct SukebeiClient {
    client: HttpClient,
    semaphore: Semaphore,
    url: String,
    sleeper: Sleeper,
}

impl fmt::Debug for SukebeiClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SukebeiClient")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl SukebeiClient {
    /// The httpx client `SukebeiClient.__init__` builds.
    pub fn client_options(options: &SukebeiOptions) -> ClientOptions {
        ClientOptions {
            timeout: options.timeout,
            proxy: options.proxy.clone().filter(|proxy| !proxy.is_empty()),
            max_connections: Some(options.max_concurrency),
            ..ClientOptions::default()
        }
    }

    pub fn new(options: &SukebeiOptions) -> Result<Self, BuildError> {
        Self::with_client(options, &Self::client_options(options), tokio_sleeper())
    }

    pub fn with_client(options: &SukebeiOptions, client: &ClientOptions, sleeper: Sleeper) -> Result<Self, BuildError> {
        Ok(Self {
            client: HttpClient::new(client)?,
            semaphore: Semaphore::new(options.max_concurrency.max(1)),
            url: SITE_URL.to_owned(),
            sleeper,
        })
    }

    /// One search (category 2, subcategory 2: videos), retried three times on HTTP errors.
    pub async fn search(
        &self,
        keyword: &str,
        category: i64,
        subcategory: i64,
        filters: i64,
        page: i64,
    ) -> Result<Vec<NyaaTorrent>, SukebeiError> {
        let mut params = vec![
            ("f", filters.to_string()),
            ("c", format!("{category}_{subcategory}")),
            ("q", keyword.to_owned()),
        ];
        if page > 0 {
            params.push(("p", page.to_string()));
        }
        let url = with_params(&self.url, &params);
        let text = tenacity::retry(
            &self.sleeper,
            |error: &HttpxError| error.is_http_error(),
            || async {
                let _slot = self.semaphore.acquire().await;
                let result = async {
                    let response = self.client.send(Request::get(url.clone())).await?;
                    response.error_for_status()?;
                    response_text(response).await
                }
                .await;
                if let Err(error) = &result
                    && error.is_http_error()
                {
                    tracing::error!(
                        target: "embyx-manager.sukebei",
                        "Failed to get {} with {}: {error}",
                        self.url,
                        params_repr(&params)
                    );
                }
                result
            },
        )
        .await?;
        parse_nyaa(&text)
    }

    /// The magnet for `keyword`, or `None` when the search fails over HTTP or finds nothing
    /// usable.
    pub async fn get_magnet(&self, keyword: &str) -> Result<Option<String>, SukebeiError> {
        let results = match self.search(keyword, 2, 2, 0, 0).await {
            Ok(results) => results,
            Err(SukebeiError::Http(error)) if error.is_http_error() => return Ok(None),
            Err(error) => return Err(error),
        };
        choose_magnet(keyword, results)
    }
}

#[cfg(test)]
mod tests;
