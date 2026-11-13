//! The magnet table inside a feed item's body.
//!
//! Python origin: `src/embyx_manager/clients/rss_magnet.py`. JavBus-style feeds (RSSHub's
//! javbus route) render the magnet listing as a table of link, size and date; the largest
//! listed size wins. Only rows of a `<tbody>` the markup itself has count, as lxml builds no
//! implied one ([`crate::pyquery`]).

use std::fmt;

use embyx_core::pytext::py_strip;
use embyx_core::pyurl::unquote;

use crate::pyquery::{Document, ParseError};
use crate::size::{SizeError, parse_size};

/// What `get_magnet_from_html` raises instead of answering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedMagnetError {
    /// pyquery could not parse the body (`ParserError`, `ValueError`).
    Parse(ParseError),
    /// A row whose magnet is not in its first cell: `None.split(...)`.
    NoMagnetInFirstCell,
    /// `parse_size`'s `ValueError` for digits that are not decimal.
    Size(String),
}

impl FeedMagnetError {
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Parse(error) => error.class_name(),
            Self::NoMagnetInFirstCell => "AttributeError",
            Self::Size(_) => "ValueError",
        }
    }
}

impl fmt::Display for FeedMagnetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(error) => error.fmt(f),
            Self::NoMagnetInFirstCell => f.write_str("'NoneType' object has no attribute 'split'"),
            Self::Size(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for FeedMagnetError {}

struct Found {
    magnet: String,
    size: String,
    name: String,
    size_int: i128,
}

/// Pick the largest magnet from the HTML table inside one feed item's body, its `dn` set to
/// `avid`; `None` when the body has no such table.
pub fn get_magnet_from_html(content: &str, avid: &str) -> Result<Option<String>, FeedMagnetError> {
    if content.is_empty() {
        return Ok(None);
    }
    let doc = Document::parse(content).map_err(FeedMagnetError::Parse)?;
    let rows = doc
        .select("table tbody tr")
        .filter(|row| !row.select("a[href^='magnet:']").is_empty());
    let mut results: Vec<Found> = Vec::new();
    for row in rows.items() {
        let magnet = row.select("td:nth-child(1) a[href^='magnet:']").attr("href");
        let size = py_strip(&row.select("td:nth-child(2)").text()).to_owned();
        let size_int = match parse_size(&size) {
            Ok(size) => size,
            Err(SizeError::Invalid(..)) => {
                tracing::warn!(target: "embyx-manager.rss-magnet", "Skipping feed magnet with invalid size {size} for {avid}");
                continue;
            }
            Err(error @ SizeError::NotDecimal(_)) => return Err(FeedMagnetError::Size(error.to_string())),
        };
        let Some(magnet) = magnet else {
            return Err(FeedMagnetError::NoMagnetInFirstCell);
        };
        let parts: Vec<&str> = magnet.split("&dn=").collect();
        let (magnet, name) = if let [magnet, name] = parts.as_slice() {
            ((*magnet).to_owned(), unquote(name))
        } else {
            (
                magnet.split('&').next().unwrap_or_default().to_owned(),
                "Unknown".to_owned(),
            )
        };
        results.push(Found {
            magnet: format!("{magnet}&dn={avid}"),
            size,
            name,
            size_int,
        });
    }
    if results.is_empty() {
        return Ok(None);
    }
    results.sort_by_key(|result| std::cmp::Reverse(result.size_int));
    tracing::info!(target: "embyx-manager.rss-magnet", "Found {} results for {avid} from the feed item:", results.len());
    let mut lines = vec!["Display top 5 largest:".to_owned()];
    for (index, result) in results.iter().take(5).enumerate() {
        let mark = if index == 0 { "✅" } else { "--" };
        lines.push(format!("{mark}--{} {}", result.size, result.name));
        lines.push(result.magnet.clone());
    }
    tracing::info!(target: "embyx-manager.rss-magnet", "{}", lines.join("\n"));
    Ok(results.into_iter().next().map(|result| result.magnet))
}

#[cfg(test)]
mod tests {
    use super::*;

    // py: tests/test_magnet_sources.py::test_rss_magnet_skips_invalid_size
    #[test]
    fn rss_magnet_skips_invalid_size() {
        let content = r#"
    <table><tbody>
      <tr>
        <td><a href="magnet:?xt=urn:btih:abc&dn=name">name</a></td>
        <td>unknown</td>
      </tr>
    </tbody></table>
    "#;
        assert_eq!(get_magnet_from_html(content, "ABC-123"), Ok(None));
    }

    // py: tests/test_magnet_sources.py::test_rss_magnet_prefers_largest
    #[test]
    fn rss_magnet_prefers_largest() {
        let content = r#"
    <table><tbody>
      <tr>
        <td><a href="magnet:?xt=urn:btih:small&dn=small">small</a></td>
        <td>1 GiB</td>
      </tr>
      <tr>
        <td><a href="magnet:?xt=urn:btih:big&dn=big">big</a></td>
        <td>3 GiB</td>
      </tr>
    </tbody></table>
    "#;
        assert_eq!(
            get_magnet_from_html(content, "ABC-123"),
            Ok(Some("magnet:?xt=urn:btih:big&dn=ABC-123".to_owned()))
        );
    }

    // py: tests/test_magnet_sources.py::test_rss_magnet_handles_an_empty_body
    #[test]
    fn rss_magnet_handles_an_empty_body() {
        assert_eq!(get_magnet_from_html("", "ABC-123"), Ok(None));
        // Whitespace is an empty document to pyquery, not an error.
        assert_eq!(get_magnet_from_html(" \n", "ABC-123"), Ok(None));
    }

    // py: tests/test_magnet_sources.py::test_rss_magnet_is_absent_from_a_javlibrary_item
    #[test]
    fn rss_magnet_is_absent_from_a_javlibrary_item() {
        // javlibrary items carry a cover and an info table, never a magnet: they fall back on
        // sukebei and javbus.
        let content = "<img src=\"cover.jpg\"/><div id=\"video_info\"><table><tbody>\
            <tr><td>ID:</td><td>ABC-123</td></tr>\
            <tr><td>Release Date:</td><td>2026-01-01</td></tr>\
            </tbody></table></div>";
        assert_eq!(get_magnet_from_html(content, "ABC-123"), Ok(None));
    }

    #[test]
    fn rows_without_a_tbody_of_their_own_do_not_count() {
        // lxml builds no implied tbody, so `table tbody tr` misses these rows.
        let content = "<table><tr><td><a href=\"magnet:?xt=urn:btih:a&dn=A\">A</a></td><td>1 GB</td></tr></table>";
        assert_eq!(get_magnet_from_html(content, "ABC-123"), Ok(None));
    }

    #[test]
    fn a_magnet_outside_the_first_cell_and_markup_without_elements_raise() {
        let content = "<table><tbody><tr><td>x</td><td><a href=\"magnet:?xt=urn:btih:a\">1 GB</a></td></tr>\
            </tbody></table>";
        let error = get_magnet_from_html(content, "ABC-123").unwrap_err();
        assert_eq!(error.class_name(), "AttributeError");
        assert_eq!(error.to_string(), "'NoneType' object has no attribute 'split'");
        let error = get_magnet_from_html("</p>", "ABC-123").unwrap_err();
        assert_eq!(
            (error.class_name(), error.to_string()),
            ("ParserError", "Document is empty".to_owned())
        );
    }

    #[test]
    fn a_magnet_without_a_name_keeps_its_first_parameter_only() {
        let content = "<table><tbody>\
            <tr><td><a href=\"magnet:?xt=urn:btih:a&tr=x\">a</a></td><td>2 GB</td></tr>\
            <tr><td><a href=\"magnet:?xt=urn:btih:b&dn=B&dn=C\">b</a></td><td>1 GB</td></tr>\
            </tbody></table>";
        assert_eq!(
            get_magnet_from_html(content, "ABC-123"),
            Ok(Some("magnet:?xt=urn:btih:a&dn=ABC-123".to_owned()))
        );
    }

    // `goldens/clients/rss_magnet.json`: the baseline's pick, or what it raised, per table.
    #[test]
    fn feed_tables_match_the_baseline() {
        use embyx_core::golden;
        let document = golden::load("clients/rss_magnet.json");
        for case in golden::cases(&document) {
            let html = golden::string(golden::get(case, "html"));
            let got = get_magnet_from_html(&html, "ABC-123");
            match case.get("error") {
                None => {
                    let want = golden::get(case, "magnet").as_str().map(str::to_owned);
                    assert_eq!(got, Ok(want), "{html}");
                }
                Some(class) => {
                    let error = got.expect_err(&html);
                    assert_eq!(error.class_name(), golden::string(class), "{html}");
                    assert_eq!(
                        error.to_string(),
                        golden::string(golden::get(case, "message")),
                        "{html}"
                    );
                }
            }
        }
    }
}
