//! The feed parser: RSS 2.0 and Atom items, and what is not a feed.

use super::*;

const RSS: &str = r#"<?xml version="1.0"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>JavBus - 演员</title>
    <item>
      <title>ABC-123 title</title>
      <link>https://www.javbus.com/ABC-123</link>
      <guid isPermaLink="true">https://www.javbus.com/ABC-123</guid>
      <description><![CDATA[<table><tbody><tr>
        <td><a href="magnet:?xt=urn:btih:abc">x</a></td><td>2 GiB</td>
      </tr></tbody></table>]]></description>
    </item>
    <item>
      <title>  spaced   title </title>
      <content:encoded><![CDATA[<p>body</p>]]></content:encoded>
    </item>
    <item>
      <description>no key at all</description>
    </item>
  </channel>
</rss>
"#;

const ATOM: &str = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>feed</title>
  <entry>
    <title>DEF-456</title>
    <id>tag:1</id>
    <link rel="enclosure" href="https://x/enc"/>
    <link href="https://x/DEF-456"/>
    <summary>s</summary>
  </entry>
</feed>
"#;

// py: tests/test_monitor_feeds.py::test_rss_items_carry_key_title_link_and_body
#[test]
fn rss_items_carry_key_title_link_and_body() {
    let feed = parse_feed(RSS.as_bytes()).expect("feed");
    assert_eq!(feed.title.as_deref(), Some("JavBus - 演员"));
    let keys: Vec<&str> = feed.items.iter().map(|item| item.key.as_str()).collect();
    assert_eq!(keys, ["https://www.javbus.com/ABC-123", "spaced title"]);
    let first = &feed.items[0];
    assert_eq!(first.link.as_deref(), Some("https://www.javbus.com/ABC-123"));
    assert!(first.content.contains("magnet:?xt=urn:btih:abc"));
    // content:encoded wins over description; whitespace in titles is collapsed.
    assert_eq!(
        feed.items[1],
        FeedItem {
            key: "spaced title".to_owned(),
            title: "spaced title".to_owned(),
            link: None,
            content: "<p>body</p>".to_owned(),
        }
    );
}

// py: tests/test_monitor_feeds.py::test_atom_entries_take_the_alternate_link
#[test]
fn atom_entries_take_the_alternate_link() {
    let feed = parse_feed(ATOM.as_bytes()).expect("feed");
    assert_eq!(feed.title.as_deref(), Some("feed"));
    assert_eq!(
        feed.items,
        [FeedItem {
            key: "tag:1".to_owned(),
            title: "DEF-456".to_owned(),
            link: Some("https://x/DEF-456".to_owned()),
            content: "s".to_owned(),
        }]
    );
}

// py: tests/test_monitor_feeds.py::test_bodies_that_are_not_feeds_are_rejected cases=3
#[test]
fn bodies_that_are_not_feeds_are_rejected() {
    let cases: [(&[u8], &str); 3] = [
        (
            b"<html><body>not a feed</body></html>",
            "unsupported feed root element 'html'",
        ),
        (b"definitely not xml", "not well-formed XML: "),
        (
            br#"<?xml version="1.0"?><!DOCTYPE x [<!ENTITY e "x">]><rss version="2.0"><channel/></rss>"#,
            "not well-formed XML: DTDForbidden(name='x', system_id=None, public_id=None)",
        ),
    ];
    for (body, message) in cases {
        let error = parse_feed(body).unwrap_err();
        assert!(error.0.starts_with(message), "{error}");
    }
    let error = parse_feed(b"<rss version=\"2.0\"/>").unwrap_err();
    assert_eq!(error.0, "RSS document has no channel");
}

// py: tests/test_monitor_feeds.py::test_avid_candidate_is_the_last_path_segment_without_its_catalog_prefix cases=6
#[test]
fn avid_candidate_is_the_last_path_segment_without_its_catalog_prefix() {
    let cases = [
        (Some("https://www.avbase.net/works/MIZD-555"), "MIZD-555"),
        (Some("https://www.avbase.net/works/moodyz:MIZD-555"), "MIZD-555"),
        (
            Some("https://www.avbase.net/works/sodcreate%3A3DSVR-2013/"),
            "3DSVR-2013",
        ),
        (Some("https://www.javlibrary.com/cn/?v=javli6"), "cn"),
        (None, ""),
        (Some(""), ""),
    ];
    for (link, expected) in cases {
        assert_eq!(avid_candidate_from_link(link).expect("candidate"), expected, "{link:?}");
    }
    assert!(avid_candidate_from_link(Some("http://[::1/works/x")).is_err());
}

// `goldens/clients/feeds.json`: what `parse_feed` reads from feed bodies and the AVID
// candidate of item links. An expat error's own wording is not kept (D27): for those only
// the `not well-formed XML: ` prefix is compared.
#[test]
fn feeds_and_links_match_the_baseline() {
    use embyx_core::golden;
    use embyx_core::pyjson::Value;
    let optional = |value: &Value| value.as_str().map(str::to_owned);
    let document = golden::load("clients/feeds.json");
    let (mut feeds, mut links, mut expat) = (0, 0, 0);
    for case in golden::cases(&document) {
        let error = case
            .get("error")
            .map(|class| (golden::string(class), golden::string(golden::get(case, "message"))));
        match golden::string(golden::get(case, "kind")).as_str() {
            "feed" => {
                feeds += 1;
                // The body as bytes: one latin-1 character per byte.
                let body: Vec<u8> = golden::string(golden::get(case, "body_latin1"))
                    .chars()
                    .map(|char| u8::try_from(u32::from(char)).expect("a latin-1 body"))
                    .collect();
                let got = parse_feed(&body);
                if let Some((class, message)) = error {
                    assert_eq!(class, "FeedParseError");
                    let FeedParseError(got) = got.expect_err(&message);
                    let prefix = "not well-formed XML: ";
                    if message.starts_with(prefix) && !message.contains("DTDForbidden") {
                        expat += 1;
                        assert!(got.starts_with(prefix), "{got:?} for {message:?}");
                    } else {
                        assert_eq!(got, message);
                    }
                    continue;
                }
                let feed = got.unwrap_or_else(|error| panic!("{body:?}: {error:?}"));
                assert_eq!(feed.title, optional(golden::get(case, "title")));
                let items: Vec<FeedItem> = golden::list(golden::get(case, "items"))
                    .iter()
                    .map(|item| {
                        let item = golden::list(item);
                        FeedItem {
                            key: golden::string(&item[0]),
                            title: golden::string(&item[1]),
                            link: optional(&item[2]),
                            content: golden::string(&item[3]),
                        }
                    })
                    .collect();
                assert_eq!(feed.items, items);
            }
            "link" => {
                links += 1;
                let link = optional(golden::get(case, "link"));
                let got = avid_candidate_from_link(link.as_deref());
                match error {
                    Some((class, message)) => {
                        assert_eq!(class, "ValueError");
                        assert_eq!(got.map_err(|error| error.0), Err(message), "{link:?}");
                    }
                    None => assert_eq!(got, Ok(golden::string(golden::get(case, "candidate"))), "{link:?}"),
                }
            }
            other => panic!("unknown golden kind {other}"),
        }
    }
    assert_eq!((feeds, links, expat), (15, 12, 4));
}
