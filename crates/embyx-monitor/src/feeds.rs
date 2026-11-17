//! Feed parsing for the subscription poller: RSS 2.0 and Atom into items.
//!
//! Python origin: `src/embyx_manager/monitor/feeds.py`. RSSHub, the AVBase talent feeds and
//! sukebei speak RSS 2.0; Atom is read too. An item is reduced to the four things the poller
//! needs: a key to remember it by, the title and link the AVID is read from, and the HTML
//! body a magnet table may sit in.

use std::fmt;

use embyx_clients::etree::{XmlElement, fromstring};
use embyx_core::pytext::{py_repr, py_split, py_strip};
use embyx_core::pyurl::{UrlError, unquote, urlsplit};

pub const ATOM_NS: &str = "http://www.w3.org/2005/Atom";
pub const CONTENT_NS: &str = "http://purl.org/rss/1.0/modules/content/";

/// `FeedParseError` (a `ValueError`): the body was not a feed this poller can read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedParseError(pub String);

impl fmt::Display for FeedParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FeedParseError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedItem {
    /// What the poller remembers the item by: guid or id, else link, else title.
    pub key: String,
    pub title: String,
    pub link: Option<String>,
    /// The item body as HTML; a magnet table lives here on JavBus-style feeds.
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedFeed {
    pub title: Option<String>,
    pub items: Vec<FeedItem>,
}

pub fn parse_feed(data: &[u8]) -> Result<ParsedFeed, FeedParseError> {
    let root = fromstring(data).map_err(|error| FeedParseError(format!("not well-formed XML: {error}")))?;
    if root.tag == "rss" {
        let channel = root
            .find("channel")
            .ok_or_else(|| FeedParseError("RSS document has no channel".to_owned()))?;
        return Ok(ParsedFeed {
            title: title(channel.find("title")),
            items: channel
                .findall("item")
                .map(rss_item)
                .filter(|item| !item.key.is_empty())
                .collect(),
        });
    }
    let atom = |name: &str| format!("{{{ATOM_NS}}}{name}");
    if root.tag == atom("feed") {
        let entry = atom("entry");
        return Ok(ParsedFeed {
            title: title(root.find(&atom("title"))),
            items: root
                .findall(&entry)
                .map(atom_entry)
                .filter(|item| !item.key.is_empty())
                .collect(),
        });
    }
    Err(FeedParseError(format!(
        "unsupported feed root element {}",
        py_repr(&root.tag)
    )))
}

/// The last path segment of an item link, with any catalog prefix stripped.
///
/// AVBase links look like `/works/MIZD-555` and, for prefixed works, `/works/moodyz:MIZD-555`;
/// the prefix is the storefront, not part of the ID, and left in place it misleads the parser.
/// `urlsplit`'s `ValueError` for a malformed link comes back as the error.
pub fn avid_candidate_from_link(link: Option<&str>) -> Result<String, UrlError> {
    let Some(link) = link.filter(|link| !link.is_empty()) else {
        return Ok(String::new());
    };
    let path = urlsplit(link, "")?.path;
    let trimmed = path.trim_end_matches('/');
    let segment = unquote(trimmed.rsplit_once('/').map_or(trimmed, |(_, last)| last));
    let candidate = segment.rsplit_once(':').map_or(segment.as_str(), |(_, last)| last);
    Ok(py_strip(candidate).to_owned())
}

fn rss_item(element: &XmlElement) -> FeedItem {
    let title = title(element.find("title")).unwrap_or_default();
    let link = text(element.find("link"));
    let guid = text(element.find("guid"));
    let content = text(element.find(&format!("{{{CONTENT_NS}}}encoded")))
        .or_else(|| text(element.find("description")))
        .unwrap_or_default();
    FeedItem {
        key: guid.or_else(|| link.clone()).unwrap_or_else(|| title.clone()),
        title,
        link,
        content,
    }
}

fn atom_entry(element: &XmlElement) -> FeedItem {
    let atom = |name: &str| format!("{{{ATOM_NS}}}{name}");
    let title = title(element.find(&atom("title"))).unwrap_or_default();
    let link_tag = atom("link");
    let link = element.findall(&link_tag).find_map(|candidate| {
        let href = candidate.get("href").filter(|href| !href.is_empty())?;
        let rel = candidate
            .get("rel")
            .filter(|rel| !rel.is_empty())
            .unwrap_or("alternate");
        (rel == "alternate").then(|| py_strip(href).to_owned())
    });
    let entry_id = text(element.find(&atom("id")));
    let content = text(element.find(&atom("content")))
        .or_else(|| text(element.find(&atom("summary"))))
        .unwrap_or_default();
    FeedItem {
        key: entry_id.or_else(|| link.clone()).unwrap_or_else(|| title.clone()),
        title,
        link,
        content,
    }
}

fn text(element: Option<&XmlElement>) -> Option<String> {
    let stripped = py_strip(element?.text.as_deref()?);
    (!stripped.is_empty()).then(|| stripped.to_owned())
}

fn title(element: Option<&XmlElement>) -> Option<String> {
    text(element).map(|text| py_split(&text).collect::<Vec<_>>().join(" "))
}

#[cfg(test)]
mod tests;
