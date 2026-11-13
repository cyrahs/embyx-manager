//! The pyquery emulation against what pyquery and lxml read on the same markup
//! (`goldens/html/selectors.json`), and the parse order's corners.

use embyx_core::golden;
use embyx_core::pyjson::Value;

use super::*;

const DOCUMENT_SELECTORS: [&str; 6] = [
    "a[href]",
    "a.movie-box",
    ".avatar-box .pb10",
    "title",
    "tr",
    "table tbody tr",
];
const ROW_SELECTORS: [&str; 6] = [
    "td",
    "td:first-child a[href^=\"magnet\"]",
    "td:first-child a",
    "td:nth-child(1) a[href^='magnet:']",
    "td:nth-child(2)",
    "a[href^='magnet:']",
];
const ANCHOR_SELECTORS: [&str; 2] = [".pb10", "img"];

/// A match as the golden records it: tag, attributes, `.text()`, `text_content()`.
type Match = (String, Vec<(String, String)>, String, String);

/// `''.join(element.itertext())` / `text_content()`: every text node below the element.
fn text_content(element: ElementRef<'_>) -> String {
    element.text().collect()
}

fn matches(selection: &Selection<'_>) -> Vec<Match> {
    selection
        .elements()
        .iter()
        .map(|element| {
            // scraper keeps attributes sorted by name, lxml in source order; no caller
            // iterates them, so they are compared as a set.
            let mut attrs: Vec<(String, String)> = element
                .value()
                .attrs()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect();
            attrs.sort();
            (
                element.value().name().to_owned(),
                attrs,
                extract_text(*element),
                text_content(*element),
            )
        })
        .collect()
}

fn expected_matches(value: &Value) -> Vec<Match> {
    golden::list(value)
        .iter()
        .map(|found| {
            let mut attrs: Vec<(String, String)> = golden::list(golden::get(found, "attrs"))
                .iter()
                .map(|pair| {
                    let pair = golden::list(pair);
                    (golden::string(&pair[0]), golden::string(&pair[1]))
                })
                .collect();
            attrs.sort();
            (
                golden::string(golden::get(found, "tag")),
                attrs,
                golden::string(golden::get(found, "text")),
                golden::string(golden::get(found, "text_content")),
            )
        })
        .collect()
}

/// nyaapy's walk: rows under any tbody, their `td` children, the cells' direct links.
fn nyaapy_rows(document: &Document) -> Vec<String> {
    document
        .select("tbody tr")
        .elements()
        .iter()
        .map(|row| {
            let cells: Vec<String> = row
                .children()
                .filter_map(ElementRef::wrap)
                .filter(|cell| cell.value().name() == "td")
                .map(|cell| {
                    let links: Vec<String> = cell
                        .children()
                        .filter_map(ElementRef::wrap)
                        .filter(|link| link.value().name() == "a")
                        .map(|link| format!("{:?}|{:?}", link.value().attr("href"), lxml_text(link)))
                        .collect();
                    format!("{:?}[{}]", lxml_text(cell), links.join(","))
                })
                .collect();
            format!("{:?}:{}", row.value().attr("class"), cells.join(";"))
        })
        .collect()
}

fn expected_nyaapy(value: &Value) -> Vec<String> {
    let optional = |value: &Value| value.as_str().map(str::to_owned);
    golden::list(value)
        .iter()
        .map(|row| {
            let cells: Vec<String> = golden::list(golden::get(row, "cells"))
                .iter()
                .map(|cell| {
                    let links: Vec<String> = golden::list(golden::get(cell, "links"))
                        .iter()
                        .map(|link| {
                            format!(
                                "{:?}|{:?}",
                                optional(golden::get(link, "href")).as_deref(),
                                optional(golden::get(link, "text"))
                            )
                        })
                        .collect();
                    format!("{:?}[{}]", optional(golden::get(cell, "text")), links.join(","))
                })
                .collect();
            format!(
                "{:?}:{}",
                optional(golden::get(row, "class")).as_deref(),
                cells.join(";")
            )
        })
        .collect()
}

// The WP0 parser probe's inputs: every markup literal of the HTML-parsing tests and the
// synthetic pages, with every selector the clients use. No known differences remain.
#[test]
fn selections_match_pyquery_on_every_golden_page() {
    let document = golden::load("html/selectors.json");
    let mut failures = Vec::new();
    for case in golden::cases(&document) {
        let name = golden::string(golden::get(case, "name"));
        let html = golden::string(golden::get(case, "html"));
        if let Some(error) = case.get("error") {
            match Document::parse(&html) {
                Err(parsed) => assert_eq!(
                    format!("{}: {parsed}", parsed.class_name()),
                    golden::string(error),
                    "{name}"
                ),
                Ok(_) => failures.push(format!("{name}: parsed, pyquery raised {}", golden::string(error))),
            }
            continue;
        }
        let parsed = Document::parse(&html).unwrap_or_else(|error| panic!("{name}: {error}"));
        let as_xml = golden::string(golden::get(case, "parsed_as")) == "xml";
        if parsed.is_xml() != as_xml {
            failures.push(format!(
                "{name}: parsed as xml={}, pyquery xml={as_xml}",
                parsed.is_xml()
            ));
        }
        let selections = golden::get(case, "document");
        for selector in DOCUMENT_SELECTORS {
            let got = matches(&parsed.select(selector));
            let want = expected_matches(golden::get(selections, selector));
            if got != want {
                failures.push(format!("{name} {selector}:\n  got  {got:?}\n  want {want:?}"));
            }
        }
        let rows: Vec<Selection<'_>> = parsed.select("tr").items().collect();
        let want_rows = golden::list(golden::get(case, "rows"));
        if rows.len() == want_rows.len() {
            for (index, (row, want_row)) in rows.iter().zip(want_rows).enumerate() {
                for selector in ROW_SELECTORS {
                    let got = matches(&row.select(selector));
                    let want = expected_matches(golden::get(want_row, selector));
                    if got != want {
                        failures.push(format!(
                            "{name} row {index} {selector}:\n  got  {got:?}\n  want {want:?}"
                        ));
                    }
                }
            }
        } else {
            failures.push(format!("{name}: {} rows, pyquery {}", rows.len(), want_rows.len()));
        }
        let anchors: Vec<Selection<'_>> = parsed.select("a[href]").items().collect();
        let want_anchors = golden::list(golden::get(case, "anchors"));
        for (index, (anchor, want_anchor)) in anchors.iter().zip(want_anchors).enumerate() {
            for selector in ANCHOR_SELECTORS {
                let got = matches(&anchor.select(selector));
                let want = expected_matches(golden::get(want_anchor, selector));
                if got != want {
                    failures.push(format!(
                        "{name} anchor {index} {selector}:\n  got  {got:?}\n  want {want:?}"
                    ));
                }
            }
        }
        // nyaapy parses with lxml's HTML parser whatever the markup is.
        let want_nyaapy = expected_nyaapy(golden::get(case, "nyaapy"));
        let got_nyaapy = Document::parse_html(&html)
            .map(|tree| nyaapy_rows(&tree))
            .unwrap_or_default();
        if got_nyaapy != want_nyaapy {
            failures.push(format!("{name} nyaapy:\n  got  {got_nyaapy:?}\n  want {want_nyaapy:?}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} differences:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn blank_text_is_an_empty_document_and_markup_without_elements_is_refused() {
    for blank in ["", "  \n\t"] {
        let document = Document::parse(blank).expect("blank");
        assert!(document.select("a").is_empty());
        assert_eq!(document.select("*").len(), 0);
    }
    for empty in ["</a>", "<!-- c -->", "<!DOCTYPE html>", "<![CDATA[x]]>", "<?php x ?>"] {
        let error = Document::parse(empty).unwrap_err();
        assert_eq!(
            (error.class_name(), error.to_string()),
            ("ParserError", "Document is empty".to_owned()),
            "{empty}"
        );
    }
    let error = Document::parse("<?xml version=\"1.0\" encoding=\"utf-8\"?><a/>").unwrap_err();
    assert_eq!(error.class_name(), "ValueError");
    assert_eq!(
        error.to_string(),
        "Unicode strings with encoding declaration are not supported. Please use bytes input or XML fragments \
         without declaration."
    );
}

#[test]
fn bare_rows_survive_as_libxml2_keeps_them() {
    let rows = "<tr><td><a href=\"magnet:?xt=urn:btih:a\">A</a></td><td>1 GB</td></tr><tr><td>x<br></td></tr>";
    let document = Document::parse(rows).expect("rows");
    assert!(!document.is_xml());
    assert_eq!(document.select("tr").len(), 2);
    assert_eq!(document.select("tr").eq(0).select("td:nth-child(2)").text(), "1 GB");
    // No table or tbody the markup did not have.
    assert!(document.select("table").is_empty());
    assert!(document.select("tbody").is_empty());
    // Content after the rows stays after them.
    let document = Document::parse("<tr><td>a</td></tr><p>after</p>").expect("rows");
    // (lxml wraps such a fragment in a `div` of its own rather than `html`/`body`: D26.)
    let order: Vec<&str> = document
        .select("*")
        .elements()
        .iter()
        .map(|e| e.value().name())
        .filter(|name| ["tr", "td", "p"].contains(name))
        .collect();
    assert_eq!(order, ["tr", "td", "p"]);
}

#[test]
fn selections_include_the_element_itself_and_keep_document_order() {
    let document = Document::parse("<div class=\"a\"><div class=\"a\"><p>x</p></div><p>y</p></div>").expect("doc");
    let outer = document.select("div.a").eq(0);
    // descendant-or-self: the outer div matches `div.a` from itself.
    assert_eq!(outer.select("div.a").len(), 2);
    assert_eq!(outer.select("p").text(), "x y");
    let filtered = document.select("p").filter(|p| p.text() == "y");
    assert_eq!(filtered.len(), 1);
    assert_eq!(document.select("div").attr("class").as_deref(), Some("a"));
    assert_eq!(document.select("span").attr("class"), None);
}

#[test]
fn text_squashes_whitespace_and_breaks_blocks_like_pyquery() {
    let document = Document::parse_html("<div> a \n b <p>c</p><br>d<span> e </span></div>").expect("doc");
    assert_eq!(document.select("div").text(), "a b\nc\n\nd e");
}
