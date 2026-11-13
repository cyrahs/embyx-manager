//! sukebei: which search result `get_magnet` picks, the search request and its failures.

use std::sync::{Arc, Mutex};

use super::*;
use crate::net::fakeweb::{FakeWeb, Reply, Seen, test_roots};
use crate::net::retry::RecordingSleeper;

/// A search page in sukebei's layout (its nyaapy reading is in `goldens/html/selectors.json`
/// for the WP0 page and checked against nyaapy for this one by `goldens/clients/sukebei.json`).
const SEARCH_PAGE: &str = include_str!("search.html");

fn result(size: &str, magnet: &str, kind: &str, name: &str) -> NyaaTorrent {
    NyaaTorrent {
        size: size.to_owned(),
        magnet: magnet.to_owned(),
        kind: kind.to_owned(),
        name: name.to_owned(),
        ..NyaaTorrent::default()
    }
}

// py: tests/test_magnet_sources.py::test_sukebei_magnet_skips_invalid_size
#[test]
fn sukebei_magnet_skips_invalid_size() {
    let results = vec![
        result("unknown", "magnet:?xt=urn:btih:bad&dn=bad", "trusted", "ABC-123 bad"),
        result("2 GiB", "magnet:?xt=urn:btih:good&dn=good", "regular", "ABC-123 good"),
    ];
    assert_eq!(
        choose_magnet("ABC-123", results),
        Ok(Some("magnet:?xt=urn:btih:good&dn=ABC-123".to_owned()))
    );
}

// py: tests/test_magnet_sources.py::test_sukebei_prefers_trusted_within_tolerance
#[test]
fn sukebei_prefers_trusted_within_tolerance() {
    let results = vec![
        result("10 GiB", "magnet:?xt=urn:btih:big&dn=big", "regular", "ABC-123 big"),
        result(
            "9 GiB",
            "magnet:?xt=urn:btih:trusted&dn=trusted",
            "trusted",
            "ABC-123 trusted",
        ),
    ];
    assert_eq!(
        choose_magnet("ABC-123", results),
        Ok(Some("magnet:?xt=urn:btih:trusted&dn=ABC-123".to_owned()))
    );
}

// py: tests/test_magnet_sources.py::test_sukebei_prefers_largest_when_trusted_is_too_small
#[test]
fn sukebei_prefers_largest_when_trusted_is_too_small() {
    let results = vec![
        result("10 GiB", "magnet:?xt=urn:btih:big&dn=big", "regular", "ABC-123 big"),
        result(
            "1 GiB",
            "magnet:?xt=urn:btih:trusted&dn=trusted",
            "trusted",
            "ABC-123 trusted",
        ),
    ];
    assert_eq!(
        choose_magnet("ABC-123", results),
        Ok(Some("magnet:?xt=urn:btih:big&dn=ABC-123".to_owned()))
    );
}

// py: tests/test_magnet_sources.py::test_sukebei_returns_none_for_empty_results
#[test]
fn sukebei_returns_none_for_empty_results() {
    assert_eq!(choose_magnet("ABC-123", Vec::new()), Ok(None));
}

// py: tests/test_magnet_sources.py::test_sukebei_skips_results_for_a_longer_number
#[test]
fn sukebei_skips_results_for_a_longer_number() {
    // Sukebei matches the query as a prefix; NHDTC-18006 is not NHDTC-180.
    let results = vec![
        result("9 GiB", "magnet:?xt=urn:btih:other&dn=o", "trusted", "NHDTC-18006 FHD"),
        result("3 GiB", "magnet:?xt=urn:btih:right&dn=r", "regular", "[HD] nhdtc00180"),
    ];
    assert_eq!(
        choose_magnet("NHDTC-180", results),
        Ok(Some("magnet:?xt=urn:btih:right&dn=NHDTC-180".to_owned()))
    );
}

// py: tests/test_magnet_sources.py::test_sukebei_returns_none_when_no_title_is_the_avid
#[test]
fn sukebei_returns_none_when_no_title_is_the_avid() {
    let results = vec![result(
        "9 GiB",
        "magnet:?xt=urn:btih:other&dn=o",
        "trusted",
        "NHDTC-18006",
    )];
    assert_eq!(choose_magnet("NHDTC-180", results), Ok(None));
}

// py: tests/test_magnet_sources.py::test_sukebei_does_not_filter_fc2_titles
#[test]
fn sukebei_does_not_filter_fc2_titles() {
    let results = vec![result(
        "1 GiB",
        "magnet:?xt=urn:btih:fc2&dn=f",
        "regular",
        "FC2-PPV-1234567",
    )];
    assert_eq!(
        choose_magnet("FC2-1234567", results),
        Ok(Some("magnet:?xt=urn:btih:fc2&dn=FC2-1234567".to_owned()))
    );
}

#[test]
fn title_patterns_follow_python() {
    let pattern = title_pattern("nhdtc-0180").expect("pattern");
    for title in ["NHDTC-180", "nhdtc00180 FHD", "[HD] NHDTC_180", "NHDTC 180"] {
        assert!(pattern.is_match(title).expect("match"), "{title}");
    }
    for title in ["NHDTC-1800", "XNHDTC-180", "NHDTC--180"] {
        assert!(!pattern.is_match(title).expect("match"), "{title}");
    }
    assert!(title_pattern("FC2-123").is_none());
    assert!(title_pattern("heydouga-4030-123").is_none());
    assert!(title_pattern("ABC123").is_none());
    // `int('٠١٢')` reads Arabic-Indic digits; the pattern spells the number in ASCII.
    assert!(
        title_pattern("ABC-١٢٣")
            .expect("pattern")
            .is_match("ABC-123")
            .expect("match")
    );
}

#[test]
fn the_largest_trusted_is_shown_even_outside_the_top_five_and_ties_keep_their_order() {
    let mut results: Vec<NyaaTorrent> = (1..=6)
        .map(|n| {
            result(
                &format!("{} GiB", 20 - n),
                &format!("magnet:?xt=urn:btih:{n}"),
                "default",
                "ABC-123",
            )
        })
        .collect();
    results.push(result("1 GiB", "magnet:?xt=urn:btih:t", "trusted", "ABC-123"));
    assert_eq!(
        choose_magnet("ABC-123", results),
        Ok(Some("magnet:?xt=urn:btih:1&dn=ABC-123".to_owned()))
    );
    let tied = vec![
        result("2 GiB", "magnet:?xt=urn:btih:first", "default", "ABC-123"),
        result("2 GiB", "magnet:?xt=urn:btih:second", "default", "ABC-123"),
    ];
    assert_eq!(
        choose_magnet("ABC-123", tied),
        Ok(Some("magnet:?xt=urn:btih:first&dn=ABC-123".to_owned()))
    );
    // A size `parse_size` cannot read as decimal digits is the `ValueError` it raises.
    let error = choose_magnet(
        "ABC-123",
        vec![result("²", "magnet:?xt=urn:btih:x", "default", "ABC-123")],
    )
    .unwrap_err();
    assert_eq!(error.class_name(), "ValueError");
}

#[test]
fn parse_nyaa_reads_the_result_rows_as_nyaapy_does() {
    let torrents = parse_nyaa(SEARCH_PAGE).expect("rows");
    assert_eq!(torrents.len(), 3);
    assert_eq!(
        torrents[0],
        NyaaTorrent {
            id: "4000001".to_owned(),
            category: Some("Real Life - Videos".to_owned()),
            url: "https://sukebei.nyaa.si/view/4000001".to_owned(),
            name: "ABC-123 FHD".to_owned(),
            download_url: "https://sukebei.nyaa.si/download/4000001.torrent".to_owned(),
            magnet: "magnet:?xt=urn:btih:aaa&dn=ABC-123+FHD&tr=http%3A%2F%2Fsukebei.tracker.wf%3A8888%2Fannounce"
                .to_owned(),
            size: "2.1 GiB".to_owned(),
            date: "2023-11-14 22:13".to_owned(),
            seeders: "10".to_owned(),
            leechers: "2".to_owned(),
            completed_downloads: "100".to_owned(),
            kind: "trusted".to_owned(),
        }
    );
    let kinds: Vec<&str> = torrents.iter().map(|torrent| torrent.kind.as_str()).collect();
    assert_eq!(kinds, ["trusted", "default", "remake"]);
    assert_eq!(
        torrents[2].category.as_deref(),
        Some("Real Life - Photobooks & Pictures")
    );
}

#[test]
fn parse_nyaa_raises_where_nyaapy_does() {
    let raised = |text: &str| {
        let error = parse_nyaa(text).unwrap_err();
        (error.class_name(), error.to_string())
    };
    assert_eq!(
        raised(""),
        (
            "AttributeError",
            "'NoneType' object has no attribute 'xpath'".to_owned()
        )
    );
    assert_eq!(
        raised("<table><tbody><tr><td>short</td></tr></tbody></table>"),
        ("IndexError", "list index out of range".to_owned())
    );
    assert_eq!(
        raised("<table><tbody><tr><td><a>no href</a></td></tr></tbody></table>"),
        (
            "AttributeError",
            "'NoneType' object has no attribute 'split'".to_owned()
        )
    );
    // A row too short for a result is skipped, as nyaapy skips its IndexError.
    assert_eq!(
        parse_nyaa("<table><tbody><tr><td>2_2</td></tr></tbody></table>"),
        Ok(Vec::new())
    );
}

struct Site {
    web: FakeWeb,
    client: SukebeiClient,
    sleeps: RecordingSleeper,
}

async fn start_site(replies: Vec<Reply>) -> Site {
    let replies = Arc::new(Mutex::new(replies.into_iter()));
    let web = FakeWeb::start(move |_: &Seen| replies.lock().expect("lock").next().unwrap_or(Reply::status(500))).await;
    let sleeps = RecordingSleeper::new();
    let options = SukebeiOptions {
        timeout: Duration::from_millis(500),
        ..SukebeiOptions::default()
    };
    let client_options = ClientOptions {
        proxy: Some(web.proxy.clone()),
        roots: test_roots(),
        ..SukebeiClient::client_options(&options)
    };
    let client = SukebeiClient::with_client(&options, &client_options, sleeps.sleeper()).expect("client");
    Site { web, client, sleeps }
}

#[tokio::test]
async fn get_magnet_searches_videos_and_picks_from_the_page() {
    let site = start_site(vec![Reply::bytes(200, SEARCH_PAGE)]).await;
    assert_eq!(
        site.client.get_magnet("ABC-123").await,
        Ok(Some("magnet:?xt=urn:btih:bbb&dn=ABC-123".to_owned()))
    );
    assert_eq!(site.web.urls(), ["https://sukebei.nyaa.si/?f=0&c=2_2&q=ABC-123"]);
    let site = site_with_page().await;
    let found = site.client.search("ABC 123/x", 1, 3, 2, 4).await.expect("search");
    assert_eq!(found.len(), 3);
    assert_eq!(
        site.web.urls(),
        ["https://sukebei.nyaa.si/?f=2&c=1_3&q=ABC+123%2Fx&p=4"]
    );
}

async fn site_with_page() -> Site {
    start_site(vec![Reply::bytes(200, SEARCH_PAGE)]).await
}

#[tokio::test]
async fn http_failures_are_retried_three_times_then_give_no_magnet() {
    let site = start_site(vec![
        Reply::status(503),
        Reply::Close,
        Reply::Stall(Duration::from_secs(5)),
    ])
    .await;
    assert_eq!(site.client.get_magnet("ABC-123").await, Ok(None));
    assert_eq!(site.web.urls().len(), 3);
    assert_eq!(site.sleeps.delays(), [1.0, 2.0]);

    let site = start_site(vec![Reply::status(503), Reply::bytes(200, SEARCH_PAGE)]).await;
    assert_eq!(
        site.client.get_magnet("ABC-123").await,
        Ok(Some("magnet:?xt=urn:btih:bbb&dn=ABC-123".to_owned()))
    );
    assert_eq!(site.sleeps.delays(), [1.0]);

    // The error itself reaches a caller of `search`.
    let site = start_site(vec![
        Reply::Truncated {
            status: 200,
            sent: b"<html>".to_vec(),
            length: 100,
        };
        3
    ])
    .await;
    let error = site.client.search("ABC-123", 2, 2, 0, 0).await.unwrap_err();
    assert_eq!(error.class_name(), "RemoteProtocolError");
}

#[tokio::test]
async fn a_page_nyaapy_cannot_read_is_an_error_not_a_miss() {
    let site = start_site(vec![Reply::bytes(200, "")]).await;
    let error = site.client.get_magnet("ABC-123").await.unwrap_err();
    assert_eq!(error.class_name(), "AttributeError");
    assert!(site.sleeps.delays().is_empty());
}

fn golden_text(value: &embyx_core::pyjson::Value) -> String {
    value.as_str().unwrap_or_default().to_owned()
}

// `goldens/clients/sukebei.json`: nyaapy's reading of result pages and the magnet
// `get_magnet` picks from given results.
#[test]
fn pages_and_choices_match_the_baseline() {
    use embyx_core::golden;
    let document = golden::load("clients/sukebei.json");
    let (mut pages, mut choices) = (0, 0);
    for case in golden::cases(&document) {
        let error = case
            .get("error")
            .map(|class| (golden::string(class), golden::string(golden::get(case, "message"))));
        match golden::string(golden::get(case, "kind")).as_str() {
            "parse" => {
                pages += 1;
                let html = golden::string(golden::get(case, "html"));
                let got = parse_nyaa(&html);
                if let Some(error) = error {
                    let raised = got.expect_err(&html);
                    assert_eq!((raised.class_name().to_owned(), raised.to_string()), error, "{html:?}");
                    continue;
                }
                let want: Vec<NyaaTorrent> = golden::list(golden::get(case, "rows"))
                    .iter()
                    .map(|row| NyaaTorrent {
                        id: golden::string(golden::get(row, "id")),
                        category: golden::get(row, "category").as_str().map(str::to_owned),
                        url: golden::string(golden::get(row, "url")),
                        name: golden::string(golden::get(row, "name")),
                        download_url: golden::string(golden::get(row, "download_url")),
                        magnet: golden::string(golden::get(row, "magnet")),
                        size: golden::string(golden::get(row, "size")),
                        date: golden::string(golden::get(row, "date")),
                        seeders: golden::string(golden::get(row, "seeders")),
                        leechers: golden::string(golden::get(row, "leechers")),
                        completed_downloads: golden::string(golden::get(row, "completed_downloads")),
                        kind: golden::string(golden::get(row, "type")),
                    })
                    .collect();
                assert_eq!(got, Ok(want), "{html:?}");
            }
            "choose" => {
                choices += 1;
                let keyword = golden::string(golden::get(case, "keyword"));
                let results: Vec<NyaaTorrent> = golden::list(golden::get(case, "results"))
                    .iter()
                    .map(|row| NyaaTorrent {
                        size: golden::string(golden::get(row, "size")),
                        magnet: golden::string(golden::get(row, "magnet")),
                        kind: golden::string(golden::get(row, "type")),
                        // `item.get('name') or ''`
                        name: golden_text(golden::get(row, "name")),
                        ..NyaaTorrent::default()
                    })
                    .collect();
                let got = choose_magnet(&keyword, results);
                if let Some(error) = error {
                    let raised = got.expect_err(&keyword);
                    assert_eq!((raised.class_name().to_owned(), raised.to_string()), error, "{keyword}");
                } else {
                    let want = golden::get(case, "magnet").as_str().map(str::to_owned);
                    assert_eq!(got, Ok(want), "{keyword} {case:?}");
                }
            }
            other => panic!("unknown golden kind {other}"),
        }
    }
    assert_eq!((pages, choices), (10, 13));
}

#[test]
fn failed_searches_log_their_params_as_python_shows_the_dict() {
    let params = [
        ("f", "0".to_owned()),
        ("c", "2_2".to_owned()),
        ("q", "AB'C".to_owned()),
        ("p", "2".to_owned()),
    ];
    assert_eq!(params_repr(&params), r#"{'f': 0, 'c': '2_2', 'q': "AB'C", 'p': 2}"#);
}

#[test]
fn the_trusted_size_is_compared_with_the_float_exactly() {
    // 2^53 + 3 rounds to the float 2^53 + 4, but Python compares the int exactly:
    // `9007199254740995 >= 9007199254740996.0` is False.
    assert!(!int_at_least(9_007_199_254_740_995, 9_007_199_254_740_996.0));
    assert!(int_at_least(9_007_199_254_740_996, 9_007_199_254_740_996.0));
    assert!(int_at_least(5, 4.2) && !int_at_least(4, 4.2) && int_at_least(-4, -4.2));
    assert!(!int_at_least(i128::MAX, f64::INFINITY) && int_at_least(i128::MIN, f64::NEG_INFINITY));
    assert!(!int_at_least(0, f64::NAN));
}
