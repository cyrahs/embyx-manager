//! AVBase: build ids, talents and their aliases, works, dates, talent queries, the feed.

use std::collections::HashMap;
use std::sync::Mutex;

use chrono::NaiveDate;
use embyx_core::golden;
use serde_json::json;

use super::*;
use crate::net::fakeweb::{FakeWeb, Reply, Seen, test_roots};

const HOST: &str = "https://avbase.test";

/// A request the session saw: URL and params.
type Call = (String, Vec<(String, String)>);

/// A route's answer: a JSON body, or text served as HTML.
enum Body {
    Json(serde_json::Value),
    Html(String),
}

/// Serves the home page (with the current build id) and a table of data routes.
struct FakeSession {
    routes: HashMap<String, Body>,
    build_id: Mutex<String>,
    calls: Mutex<Vec<Call>>,
}

impl FakeSession {
    fn new(routes: Vec<(&str, Body)>) -> Arc<Self> {
        Arc::new(Self {
            routes: routes.into_iter().map(|(key, body)| (key.to_owned(), body)).collect(),
            build_id: Mutex::new("B1".to_owned()),
            calls: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().expect("lock").clone()
    }

    fn paths(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .map(|(url, _)| url.strip_prefix(HOST).unwrap_or(&url).to_owned())
            .collect()
    }

    fn answer(&self, url: &str, params: &[(&str, String)]) -> AvbaseResponse {
        let html =
            |status: u16, text: String| AvbaseResponse::new(status, Some("text/html".to_owned()), text.into_bytes());
        if url == format!("{HOST}/") {
            let build_id = self.build_id.lock().expect("lock").clone();
            return html(
                200,
                format!("<script id=\"__NEXT_DATA__\">{{\"buildId\":\"{build_id}\"}}</script>"),
            );
        }
        let mut key = url
            .strip_prefix(&format!("{HOST}/_next/data/"))
            .unwrap_or(url)
            .to_owned();
        if let Some((_, page)) = params.iter().find(|(name, _)| *name == "page") {
            key = format!("{key}?page={page}");
        }
        match self.routes.get(&key) {
            None => html(404, "<html>404</html>".to_owned()),
            Some(Body::Html(text)) => html(200, text.clone()),
            Some(Body::Json(value)) => AvbaseResponse::new(
                200,
                Some("application/json; charset=utf-8".to_owned()),
                serde_json::to_vec(value).expect("JSON"),
            ),
        }
    }
}

impl AvbaseSession for FakeSession {
    fn get<'a>(
        &'a self,
        url: &'a str,
        params: &'a [(&'a str, String)],
    ) -> BoxFuture<'a, Result<AvbaseResponse, AvbaseError>> {
        self.calls.lock().expect("lock").push((
            url.to_owned(),
            params
                .iter()
                .map(|(name, value)| ((*name).to_owned(), value.clone()))
                .collect(),
        ));
        let answer = self.answer(url, params);
        Box::pin(async move { Ok(answer) })
    }
}

fn talent_props(total: i64, works: Option<serde_json::Value>) -> Body {
    Body::Json(json!({
        "pageProps": {
            "name": "河北彩伽",
            "talent": {
                "id": 5022,
                "primary": {"id": 5031, "name": "河北彩花"},
                "actors": [{"id": 5031, "name": "河北彩花"}, {"id": 66947, "name": "河北彩伽"}, {"id": 1, "name": " "}],
            },
            "total": total,
            "works": works.unwrap_or_else(|| json!([{
                "work_id": "MIZD-555",
                "prefix": "moodyz",
                "title": "best",
                "min_date": "Fri Oct 02 2026 09:00:00 GMT+0900 (Japan Standard Time)",
                "actors": [{"id": 5031, "name": "河北彩花"}],
            }])),
        },
    }))
}

fn make_client(routes: Vec<(&str, Body)>) -> (AvbaseClient, Arc<FakeSession>) {
    let session = FakeSession::new(routes);
    let options = AvbaseOptions {
        host: HOST.to_owned(),
        ..AvbaseOptions::default()
    };
    let client = AvbaseClient::with_session(&options, Arc::clone(&session) as Arc<dyn AvbaseSession>);
    (client, session)
}

fn params(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect()
}

fn date(year: i32, month: u32, day: u32) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(year, month, day)
}

fn kawakita() -> AvbaseTalent {
    AvbaseTalent {
        talent_id: 5022,
        name: "河北彩花".to_owned(),
        aliases: vec!["河北彩伽".to_owned()],
        total_works: 2,
    }
}

// py: tests/test_avbase.py::test_a_talent_is_found_under_any_of_its_names_with_the_others_as_aliases
#[tokio::test]
async fn a_talent_is_found_under_any_of_its_names_with_the_others_as_aliases() {
    let (client, session) = make_client(vec![(
        "B1/talents/%E6%B2%B3%E5%8C%97%E5%BD%A9%E4%BC%BD.json",
        talent_props(2, None),
    )]);
    let talent = client.talent("河北彩伽").await.expect("talent").expect("found");
    assert_eq!(talent, kawakita());
    assert_eq!(talent.names(), ["河北彩花", "河北彩伽"]);
    let calls = session.calls();
    assert_eq!(calls[0].0, format!("{HOST}/"));
    assert_eq!(calls[1].1, params(&[("name", "河北彩伽")]));
}

// py: tests/test_avbase.py::test_a_stale_build_id_is_refreshed_once_and_the_route_retried
#[tokio::test]
async fn a_stale_build_id_is_refreshed_once_and_the_route_retried() {
    let (client, session) = make_client(vec![("B2/talents/x.json", talent_props(2, None))]);
    client.build_id(false).await.expect("build id"); // learned B1 before the site was redeployed
    "B2".clone_into(&mut session.build_id.lock().expect("lock"));

    assert!(client.talent("x").await.expect("talent").is_some());
    assert_eq!(
        session.paths(),
        [
            "/",
            "/_next/data/B1/talents/x.json",
            "/",
            "/_next/data/B2/talents/x.json"
        ]
    );
}

// py: tests/test_avbase.py::test_an_unknown_name_is_none_once_the_build_id_is_confirmed
#[tokio::test]
async fn an_unknown_name_is_none_once_the_build_id_is_confirmed() {
    let (client, session) = make_client(Vec::new());
    assert_eq!(client.talent("nobody").await.expect("talent"), None);
    // One refresh to rule out a stale id, no second data request.
    assert_eq!(session.paths(), ["/", "/_next/data/B1/talents/nobody.json", "/"]);
}

// py: tests/test_avbase.py::test_a_challenge_page_in_place_of_json_is_an_unavailable_error
#[tokio::test]
async fn a_challenge_page_in_place_of_json_is_an_unavailable_error() {
    let (client, _) = make_client(vec![(
        "B1/talents/x.json",
        Body::Html("<html>Just a moment...</html>".to_owned()),
    )]);
    let error = client.talent("x").await.unwrap_err();
    assert_eq!(error.class_name(), "AvbaseUnavailableError");
    assert!(error.is_avbase_error());
    assert_eq!(
        error.to_string(),
        "AVBase answered talents/x.json with something other than JSON (a challenge page?)"
    );
}

#[tokio::test]
async fn other_answers_are_unavailable_or_raise_as_python_does() {
    /// A home page answered with an error status.
    struct Home;
    impl AvbaseSession for Home {
        fn get<'a>(
            &'a self,
            _: &'a str,
            _: &'a [(&'a str, String)],
        ) -> BoxFuture<'a, Result<AvbaseResponse, AvbaseError>> {
            Box::pin(async { Ok(AvbaseResponse::new(403, None, b"\"buildId\":\"B1\"".to_vec())) })
        }
    }
    /// The home page, then the same answer for every data route.
    struct Fixed(AvbaseResponse);
    impl AvbaseSession for Fixed {
        fn get<'a>(
            &'a self,
            url: &'a str,
            _: &'a [(&'a str, String)],
        ) -> BoxFuture<'a, Result<AvbaseResponse, AvbaseError>> {
            let answer = if url.ends_with('/') {
                AvbaseResponse::new(200, None, b"\"buildId\":\"B1\"".to_vec())
            } else {
                self.0.clone()
            };
            Box::pin(async move { Ok(answer) })
        }
    }
    let json =
        |status: u16, body: &[u8]| AvbaseResponse::new(status, Some("application/json".to_owned()), body.to_vec());
    let failure = |answer: AvbaseResponse| async move {
        let client = AvbaseClient::with_session(&AvbaseOptions::default(), Arc::new(Fixed(answer)));
        client.talent("x").await.unwrap_err()
    };
    let error = failure(json(503, b"{}")).await;
    assert_eq!(
        (error.class_name(), error.to_string()),
        (
            "AvbaseUnavailableError",
            "AVBase answered HTTP 503 for talents/x.json".to_owned()
        )
    );
    let error = failure(json(200, b"[1]")).await;
    assert_eq!(error.to_string(), "AVBase answered talents/x.json with a non-object");
    let error = failure(json(200, b"{oops")).await;
    assert_eq!(error.class_name(), "JSONDecodeError");
    assert!(!error.is_avbase_error());
    assert_eq!(
        error.to_string(),
        "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"
    );
    let error = failure(json(200, b"\xff{}")).await;
    assert_eq!(error.class_name(), "UnicodeDecodeError");
    assert_eq!(
        error.to_string(),
        "'utf-8' codec can't decode byte 0xff in position 0: invalid start byte"
    );
    // A talent of the wrong shape raises what the walk raises.
    let error = failure(json(200, br#"{"pageProps": {"talent": {"primary": 3}}}"#)).await;
    assert_eq!(
        (error.class_name(), error.to_string()),
        ("AttributeError", "'int' object has no attribute 'get'".to_owned())
    );
    let error = failure(json(200, br#"{"pageProps": {"talent": {"actors": []}}}"#)).await;
    assert_eq!((error.class_name(), error.to_string()), ("KeyError", "'id'".to_owned()));

    // A home page without a build id.
    let client = AvbaseClient::with_session(&AvbaseOptions::default(), Arc::new(Home));
    let error = client.build_id(false).await.unwrap_err();
    assert_eq!(error.to_string(), "no build id on the AVBase home page (HTTP 403)");
}

// py: tests/test_avbase.py::test_works_are_read_across_every_page_of_the_talent
#[tokio::test]
async fn works_are_read_across_every_page_of_the_talent() {
    let second = talent_props(
        31,
        Some(json!([{"work_id": "REBD-1013", "prefix": "", "title": "old", "min_date": "2026-02-19", "actors": []}])),
    );
    let (client, session) = make_client(vec![
        ("B1/talents/x.json", talent_props(31, None)),
        ("B1/talents/x.json?page=2", second),
    ]);
    let works = client.talent_works("x").await.expect("works");
    let summary: Vec<(&str, &str, Option<NaiveDate>)> = works
        .iter()
        .map(|work| (work.work_id.as_str(), work.prefix.as_str(), work.release_date))
        .collect();
    assert_eq!(
        summary,
        [
            ("MIZD-555", "moodyz", date(2026, 10, 2)),
            ("REBD-1013", "", date(2026, 2, 19))
        ]
    );
    let pages: Vec<Option<String>> = session.calls()[1..]
        .iter()
        .map(|(_, params)| {
            params
                .iter()
                .find(|(name, _)| name == "page")
                .map(|(_, value)| value.clone())
        })
        .collect();
    assert_eq!(pages, [None, Some("2".to_owned())]);
    assert_eq!(
        works[0].cast,
        [AvbaseCastMember {
            actor_id: 5031,
            name: "河北彩花".to_owned(),
            talent_id: None,
        }]
    );

    // No talent of that name is an error for the page walk, a page gone later ends it.
    let (client, _) = make_client(vec![("B1/talents/y.json", talent_props(90, None))]);
    let mut pages = client.talent_pages("y");
    let (page, total, works) = pages.next().await.expect("page").expect("first");
    assert_eq!((page, total, works.len()), (1, 3, 1));
    assert_eq!(pages.next().await.expect("page"), None);
    assert_eq!(pages.next().await.expect("page"), None);
    let error = client.talent_works("nobody").await.unwrap_err();
    assert_eq!(
        (error.class_name(), error.to_string()),
        ("AvbaseError", "AVBase has no talent named 'nobody'".to_owned())
    );
}

// py: tests/test_avbase.py::test_a_work_carries_its_cast_with_talent_ids
#[tokio::test]
async fn a_work_carries_its_cast_with_talent_ids() {
    let (client, _) = make_client(vec![(
        "B1/works/moodyz%3AMIZD-555.json",
        Body::Json(json!({
            "pageProps": {
                "work": {
                    "work_id": "MIZD-555",
                    "prefix": "moodyz",
                    "title": "best",
                    "min_date": "Fri Oct 02 2026 09:00:00 GMT+0900 (Japan Standard Time)",
                    "casts": [
                        {"actor": {"id": 35, "name": "松本いちか", "talent": {"id": 35}}},
                        {"actor": {"id": 2041, "name": "七沢みあ", "talent": {"id": 2037}}},
                        {"actor": {"id": 9, "name": ""}},
                        {"actor": {"id": "10", "name": "id is text"}},
                        {"actor": {"id": true, "name": "a bool is an int", "talent": {"id": 1.5}}},
                        "not a cast entry",
                    ],
                },
            },
        })),
    )]);
    let work = client.work("moodyz:MIZD-555").await.expect("work").expect("found");
    assert_eq!(work.work_id, "MIZD-555");
    assert_eq!(work.release_date, date(2026, 10, 2));
    let member = |actor_id: i128, name: &str, talent_id: Option<i128>| AvbaseCastMember {
        actor_id,
        name: name.to_owned(),
        talent_id,
    };
    assert_eq!(
        work.cast,
        [
            member(35, "松本いちか", Some(35)),
            member(2041, "七沢みあ", Some(2037)),
            member(1, "a bool is an int", None),
        ]
    );
}

// py: tests/test_avbase.py::test_release_dates_are_read_from_javascript_or_iso_text cases=5
#[test]
fn release_dates_are_read_from_javascript_or_iso_text() {
    let cases = [
        (
            Value::from("Fri Oct 02 2026 09:00:00 GMT+0900 (Japan Standard Time)"),
            date(2026, 10, 2),
        ),
        (Value::from("2026-02-19T00:00:00.000Z"), date(2026, 2, 19)),
        (Value::from("Thu Feb 30 2026 09:00:00"), None),
        (Value::from(""), None),
        (Value::Null, None),
    ];
    for (value, expected) in cases {
        assert_eq!(parse_release_date(&value), expected, "{value:?}");
    }
    // The other forms `date.fromisoformat` takes, and what it refuses.
    assert_eq!(parse_release_date(&Value::from("20260219")), date(2026, 2, 19));
    assert_eq!(parse_release_date(&Value::from("2026-W08-4")), date(2026, 2, 19));
    assert_eq!(parse_release_date(&Value::from("2026W084")), date(2026, 2, 19));
    assert_eq!(parse_release_date(&Value::from("2026-W08")), date(2026, 2, 16));
    assert_eq!(parse_release_date(&Value::from("Fri Oct 2 2026")), date(2026, 10, 2));
    assert_eq!(parse_release_date(&Value::from("Fri Foo 02 2026")), None);
    assert_eq!(parse_release_date(&Value::from("0000-01-01")), None);
    assert_eq!(parse_release_date(&Value::from("2026-2-19")), None);
    assert_eq!(parse_release_date(&Value::Int(20_260_219)), None);
}

// py: tests/test_avbase.py::test_a_talent_query_is_a_name_an_id_or_a_talent_url cases=5
#[test]
fn a_talent_query_is_a_name_an_id_or_a_talent_url() {
    let cases = [
        ("河北彩伽", (None, "河北彩伽")),
        (" 5022 ", (Some(5022), "")),
        ("https://www.avbase.net/talents/5022", (Some(5022), "")),
        ("https://www.avbase.net/talents/5022/feed", (Some(5022), "")),
        (
            "https://www.avbase.net/talents/%E6%B2%B3%E5%8C%97%E5%BD%A9%E8%8A%B1?tab=works",
            (None, "河北彩花"),
        ),
    ];
    for (query, (talent_id, name)) in cases {
        assert_eq!(parse_talent_query(query), Ok((talent_id, name.to_owned())), "{query}");
    }
}

// py: tests/test_avbase.py::test_other_text_is_not_a_talent_query cases=4
#[test]
fn other_text_is_not_a_talent_query() {
    for query in ["", "   ", "https://www.avbase.net/works/MIZD-555", "/actors/1"] {
        let message = parse_talent_query(query).unwrap_err();
        assert!(message.contains("talent"), "{query}: {message}");
    }
    // `'²'.isdigit()` is true but `int('²')` raises: still a ValueError.
    assert!(parse_talent_query("²").is_err());
}

const FEED_XML: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><rss version=\"2.0\"><channel>\
    <title><![CDATA[「河北彩花」のフィード]]></title>\
    <link>https://www.avbase.net/talents/%E6%B2%B3%E5%8C%97%E5%BD%A9%E8%8A%B1</link>\
    </channel></rss>";

// py: tests/test_avbase.py::test_a_talent_is_found_from_its_id_through_the_feed
#[tokio::test]
async fn a_talent_is_found_from_its_id_through_the_feed() {
    let feed = format!("{HOST}/talents/5022/feed");
    let (client, session) = make_client(vec![
        (
            "B1/talents/%E6%B2%B3%E5%8C%97%E5%BD%A9%E8%8A%B1.json",
            talent_props(2, None),
        ),
        (
            "B1/talents/%E6%B2%B3%E5%8C%97%E5%BD%A9%E4%BC%BD.json",
            talent_props(2, None),
        ),
        (feed.as_str(), Body::Html(FEED_XML.to_owned())),
    ]);
    let by_url = client
        .find_talent("https://www.avbase.net/talents/5022/feed")
        .await
        .expect("by url");
    let by_name = client.find_talent("河北彩伽").await.expect("by name");
    let missing = client.find_talent("99999").await.expect("missing");
    let nonsense = client
        .find_talent("https://www.avbase.net/works/MIZD-555")
        .await
        .expect("nonsense");

    assert_eq!(by_url, Some(kawakita()));
    assert_eq!(by_name, by_url);
    assert_eq!(missing, None);
    assert_eq!(nonsense, None);
    assert!(session.calls().contains(&(feed, Vec::new())));
}

// py: tests/test_avbase.py::test_a_feed_naming_a_talent_whose_page_is_gone_still_yields_the_id
#[tokio::test]
async fn a_feed_naming_a_talent_whose_page_is_gone_still_yields_the_id() {
    let feed = format!("{HOST}/talents/5022/feed");
    let (client, _) = make_client(vec![(feed.as_str(), Body::Html(FEED_XML.to_owned()))]);
    assert_eq!(
        client.find_talent("5022").await.expect("talent"),
        Some(AvbaseTalent {
            talent_id: 5022,
            name: "河北彩花".to_owned(),
            aliases: Vec::new(),
            total_works: 0,
        })
    );
}

#[test]
fn the_talent_name_comes_from_the_feed_link_or_its_title() {
    let feed =
        |title: &str, link: &str| format!("<rss><channel><title>{title}</title><link>{link}</link></channel></rss>");
    assert_eq!(
        talent_name_from_feed(&feed("x", "https://www.avbase.net/talents/%E6%B2%B3%E5%8C%97/")),
        Ok(Some("河北".to_owned()))
    );
    // An id-addressed link falls back on the title.
    assert_eq!(
        talent_name_from_feed(&feed("「 名前 」の", "https://www.avbase.net/talents/5022")),
        Ok(Some("名前".to_owned()))
    );
    assert_eq!(talent_name_from_feed(&feed("no brackets", "")), Ok(None));
    assert_eq!(talent_name_from_feed("<rss/>"), Ok(None));
    assert_eq!(talent_name_from_feed("not xml"), Ok(None));
    assert_eq!(talent_name_from_feed("<!DOCTYPE x><rss/>"), Ok(None));
    let error = talent_name_from_feed(&feed("x", "http://[::1/talents/a")).unwrap_err();
    assert_eq!(error.class_name(), "ValueError");
}

// py: tests/test_avbase.py::test_the_storefront_prefix_is_stripped_from_work_ids
#[test]
fn the_storefront_prefix_is_stripped_from_work_ids() {
    assert_eq!(strip_prefix("moodyz:MIZD-555"), "MIZD-555");
    assert_eq!(strip_prefix("DLDSS-515"), "DLDSS-515");
    assert_eq!(strip_prefix("a:b: C-1 "), "C-1");
}

// py: tests/test_avbase.py::test_a_bare_work_id_is_found_through_the_search_and_its_prefixed_route
#[tokio::test]
async fn a_bare_work_id_is_found_through_the_search_and_its_prefixed_route() {
    let detail = Body::Json(json!({
        "pageProps": {
            "work": {
                "work_id": "MDVR-394",
                "prefix": "moodyz",
                "title": "vr",
                "min_date": "2026-01-01",
                "casts": [{"actor": {"id": 1, "name": "輝星きら", "talent": {"id": 67548}}}],
            },
        },
    }));
    let listing = Body::Json(json!({
        "pageProps": {"works": [{"work_id": "MDVR-394", "prefix": "moodyz", "actors": [{"id": 1, "name": "輝星きら"}]}]}
    }));
    let (client, session) = make_client(vec![
        ("B1/works.json", listing),
        ("B1/works/moodyz%3AMDVR-394.json", detail),
    ]);
    let work = client.work("MDVR-394").await.expect("work").expect("found");
    assert_eq!(work.cast[0].talent_id, Some(67548));
    let sent: Vec<Vec<(String, String)>> = session.calls()[1..].iter().map(|(_, params)| params.clone()).collect();
    assert_eq!(
        sent,
        [params(&[("q", "MDVR-394")]), params(&[("id", "moodyz:MDVR-394")])]
    );
    assert_eq!(client.work("NOPE-1").await.expect("work"), None);
}

// ---------- the impersonating session against the fake web ----------

fn impersonating(proxy: &str, timeout: Duration) -> AvbaseClient {
    let options = AvbaseOptions {
        host: "https://avbase.test".to_owned(),
        timeout,
        proxy: Some(proxy.to_owned()),
        ..AvbaseOptions::default()
    };
    let session = session::ImpersonatingSession::new(&session::SessionOptions {
        roots: test_roots(),
        ..AvbaseClient::session_options(&options)
    })
    .expect("session");
    AvbaseClient::with_session(&options, Arc::new(session))
}

#[tokio::test]
async fn the_impersonating_session_sends_curl_cffi_headers_and_reads_the_routes() {
    let web = FakeWeb::start(|seen: &Seen| {
        if seen.path() == "/" {
            Reply::bytes(200, "<script>{\"buildId\":\"B9\"}</script>")
        } else {
            Reply::json(200, &json!({"pageProps": {"works": [{"work_id": "A-1"}]}}))
        }
    })
    .await;
    let client = impersonating(&web.proxy, Duration::from_secs(5));
    let works = client.search_works("A 1&b").await.expect("works");
    assert_eq!(works[0].work_id, "A-1");
    let seen = web.seen();
    assert_eq!(seen[0].url, "https://avbase.test/");
    assert_eq!(seen[1].url, "https://avbase.test/_next/data/B9/works.json?q=A+1%26b");
    let header = |name: &str| seen[1].header(name).map(str::to_owned);
    assert_eq!(header("Referer").as_deref(), Some("https://avbase.test/"));
    assert_eq!(header("Accept-Language").as_deref(), Some("ja,en;q=0.8"));
    assert_eq!(header("sec-ch-ua-platform").as_deref(), Some("\"macOS\""));
    assert!(header("User-Agent").expect("user agent").contains("Chrome/150.0.0.0"));
    // curl_cffi's header order, the session's two merged in (Accept-Language in place,
    // Referer appended).
    let names: Vec<String> = seen[1]
        .headers
        .iter()
        .map(|(name, _)| name.to_ascii_lowercase())
        .collect();
    let position = |name: &str| names.iter().position(|seen| seen == name).unwrap_or(usize::MAX);
    assert!(position("sec-ch-ua") < position("user-agent"));
    assert!(position("user-agent") < position("accept-language"));
    assert!(position("accept-language") < position("priority"));
    assert!(position("priority") < position("referer"));
    assert_ne!(position("referer"), usize::MAX);
}

#[tokio::test]
async fn the_impersonating_session_reports_transport_failures_by_curl_cffi_class() {
    let web = FakeWeb::start(|_: &Seen| Reply::Stall(Duration::from_secs(5))).await;
    let error = impersonating(&web.proxy, Duration::from_millis(300))
        .build_id(false)
        .await
        .unwrap_err();
    assert_eq!(error.class_name(), "Timeout");
    assert!(!error.is_avbase_error());

    // The classes curl_cffi 0.16.3 raised for the same failures on a local server: curl 52
    // (empty reply), 18 (body cut short), 56 (reset), 7 (proxy refused).
    for (reply, class) in [
        (Reply::Close, "ConnectionError"),
        (
            Reply::Truncated {
                status: 200,
                sent: b"abc".to_vec(),
                length: 100,
            },
            "IncompleteRead",
        ),
        (Reply::Reset, "ConnectionError"),
    ] {
        let web = FakeWeb::start(move |_: &Seen| reply.clone()).await;
        let error = impersonating(&web.proxy, Duration::from_secs(5))
            .build_id(false)
            .await
            .unwrap_err();
        assert!(matches!(error, AvbaseError::Transport { .. }), "{error:?}");
        assert_eq!(error.class_name(), class, "{error:?}");
    }
    let error = impersonating("http://127.0.0.1:9", Duration::from_secs(5))
        .build_id(false)
        .await
        .unwrap_err();
    assert_eq!(error.class_name(), "ConnectionError", "{error:?}");

    // An untrusted certificate: curl 60.
    let web = FakeWeb::start(|_: &Seen| Reply::bytes(200, "x")).await;
    let options = AvbaseOptions {
        host: "https://avbase.test".to_owned(),
        proxy: Some(web.proxy.clone()),
        ..AvbaseOptions::default()
    };
    let session = session::ImpersonatingSession::new(&AvbaseClient::session_options(&options)).expect("session");
    let error = AvbaseClient::with_session(&options, Arc::new(session))
        .build_id(false)
        .await
        .unwrap_err();
    assert_eq!(error.class_name(), "CertificateVerifyError", "{error:?}");
}

#[test]
fn chrome_emulation_merges_the_session_headers_like_curl_cffi() {
    let emulation =
        session::chrome_emulation(&[("Referer", "https://x/"), ("Accept-Language", "ja")]).expect("emulation");
    let names: Vec<String> = emulation.headers.keys().map(|name| name.as_str().to_owned()).collect();
    assert_eq!(names.len(), 14);
    assert_eq!(names[11], "accept-language");
    assert_eq!(names[13], "referer");
    assert_eq!(emulation.headers["accept-language"], "ja");
    assert!(
        format!(
            "{:?}",
            AvbaseClient::session_options(&AvbaseOptions {
                proxy: Some("http://user:secret@proxy".to_owned()),
                ..AvbaseOptions::default()
            })
        )
        .contains("<redacted>")
    );
}

/// `(class, message)` of an error a golden case records.
fn golden_error(case: &pyjson::Value) -> (String, String) {
    (
        golden::string(golden::get(case, "error")),
        golden::string(golden::get(case, "message")),
    )
}

fn optional_int(value: &pyjson::Value) -> Option<i128> {
    match value {
        pyjson::Value::Null => None,
        other => Some(golden::int(other)),
    }
}

fn optional_string(value: &pyjson::Value) -> Option<String> {
    match value {
        pyjson::Value::Null => None,
        other => Some(golden::string(other)),
    }
}

/// A `listing` or `detail` case: the work `_work_from_listing`/`_work_from_detail` builds.
fn check_built_work(kind: &str, case: &pyjson::Value) {
    let work = golden::get(case, "work");
    let got = if kind == "listing" {
        work_from_listing(work)
    } else {
        work_from_detail(work)
    };
    if case.get("error").is_some() {
        let error = got.expect_err("an error");
        assert_eq!(
            (error.class_name().to_owned(), error.to_string()),
            golden_error(case),
            "{kind} {work:?}"
        );
    } else {
        let want = golden::list(golden::get(case, "built"));
        let built = got.unwrap_or_else(|error| panic!("{kind} {work:?}: {error}"));
        assert_eq!(built.work_id, golden::string(&want[0]), "{kind} {work:?}");
        assert_eq!(built.prefix, golden::string(&want[1]), "{kind} {work:?}");
        assert_eq!(built.title, golden::string(&want[2]), "{kind} {work:?}");
        assert_eq!(
            built.release_date.map(|date| date.format("%Y-%m-%d").to_string()),
            optional_string(&want[3]),
            "{kind} {work:?}"
        );
        let want_cast: Vec<(i128, String, Option<i128>)> = golden::list(&want[4])
            .iter()
            .map(|member| {
                let member = golden::list(member);
                (
                    golden::int(&member[0]),
                    golden::string(&member[1]),
                    optional_int(&member[2]),
                )
            })
            .collect();
        let got_cast: Vec<(i128, String, Option<i128>)> = built
            .cast
            .into_iter()
            .map(|member| (member.actor_id, member.name, member.talent_id))
            .collect();
        assert_eq!(got_cast, want_cast, "{kind} {work:?}");
    }
}

// `goldens/clients/avbase.json`: dates, talent queries, feed names, talent payloads and work
// listings/details as the Python parsers read them.
#[test]
fn parsers_match_the_baseline() {
    let document = golden::load("clients/avbase.json");
    let mut seen = HashMap::new();
    for case in golden::cases(&document) {
        let kind = golden::string(golden::get(case, "kind"));
        *seen.entry(kind.clone()).or_insert(0) += 1;
        match kind.as_str() {
            "date" => {
                let value = golden::get(case, "value");
                let want = optional_string(golden::get(case, "date"));
                let got = parse_release_date(value).map(|date| date.format("%Y-%m-%d").to_string());
                assert_eq!(got, want, "{value:?}");
            }
            "query" => {
                let query = golden::string(golden::get(case, "query"));
                let got = parse_talent_query(&query);
                if case.get("error").is_some() {
                    let (class, message) = golden_error(case);
                    assert_eq!(class, "ValueError", "{query:?}");
                    assert_eq!(got, Err(message), "{query:?}");
                } else {
                    let want = (
                        optional_int(golden::get(case, "talent_id")),
                        golden::string(golden::get(case, "name")),
                    );
                    assert_eq!(got, Ok(want), "{query:?}");
                }
            }
            "feed" => {
                let xml = golden::string(golden::get(case, "xml"));
                let got = talent_name_from_feed(&xml);
                if case.get("error").is_some() {
                    let error = got.expect_err(&xml);
                    assert_eq!(
                        (error.class_name().to_owned(), error.to_string()),
                        golden_error(case),
                        "{xml:?}"
                    );
                } else {
                    assert_eq!(got, Ok(optional_string(golden::get(case, "name"))), "{xml:?}");
                }
            }
            "talent" => {
                let props = golden::get(case, "props");
                let got = talent_from_props(props);
                if case.get("error").is_some() {
                    let error = got.expect_err("an error");
                    assert_eq!(
                        (error.class_name().to_owned(), error.to_string()),
                        golden_error(case),
                        "{props:?}"
                    );
                } else {
                    let want = golden::list(golden::get(case, "talent"));
                    let talent = got.unwrap_or_else(|error| panic!("{props:?}: {error}"));
                    assert_eq!(talent.talent_id, golden::int(&want[0]), "{props:?}");
                    assert_eq!(talent.name, golden::string(&want[1]), "{props:?}");
                    let aliases: Vec<String> = golden::list(&want[2]).iter().map(golden::string).collect();
                    assert_eq!(talent.aliases, aliases, "{props:?}");
                    assert_eq!(talent.total_works, golden::int(&want[3]), "{props:?}");
                }
            }
            "listing" | "detail" => check_built_work(&kind, case),
            other => panic!("unknown golden kind {other}"),
        }
    }
    for kind in ["date", "query", "feed", "talent", "listing", "detail"] {
        assert!(seen.get(kind).copied().unwrap_or(0) > 0, "no {kind} cases");
    }
}

#[test]
fn json_and_text_follow_curl_cffi_charset_rules() {
    // Each result checked against `curl_cffi.requests.models.Response` at the baseline.
    let response =
        |content_type: &str, body: &[u8]| AvbaseResponse::new(200, Some(content_type.to_owned()), body.to_vec());
    let decoded = |content_type: &str, body: &[u8]| {
        response(content_type, body)
            .json()
            .map(|value| pyjson::dumps(&value, &pyjson::DumpOptions::compact()).expect("json"))
            .map_err(|error| (error.class_name(), error.to_string()))
    };
    let invalid = Err((
        "UnicodeDecodeError",
        "'utf-8' codec can't decode byte 0xe9 in position 9: invalid continuation byte".to_owned(),
    ));
    // A charset JSON does not read natively: `json.loads(response.text)`.
    assert_eq!(
        decoded("application/json; charset=iso-8859-1", b"{\"a\":\"caf\xe9\"}"),
        Ok("{\"a\":\"café\"}".to_owned())
    );
    // An unknown codec: `text` falls back to a strict `utf-8-sig` decode.
    assert_eq!(
        decoded("application/json; charset=x-unknown", b"{\"a\":\"caf\xe9\"}"),
        invalid
    );
    assert_eq!(
        decoded("application/json; charset=x-unknown", b"\xef\xbb\xbf{\"a\":1}"),
        Ok("{\"a\":1}".to_owned())
    );
    assert_eq!(
        decoded("application/json; charset=UTF_8", b"{\"a\":1}"),
        Ok("{\"a\":1}".to_owned())
    );
    // `charset=` is matched case-sensitively, so this one reads the bytes.
    assert_eq!(
        decoded("application/json; Charset=latin1", b"{\"a\":\"caf\xe9\"}"),
        invalid
    );
    assert_eq!(
        response("text/html; charset=windows-1252", b"\x80").text(),
        Ok("€".to_owned())
    );
    assert_eq!(response("text/html", b"").text(), Ok(String::new()));
}

#[test]
fn work_ids_are_compared_with_full_case_folding() {
    // `str.casefold()` at the baseline: ϐ → β, ß → ss, ﬀ → ff, the Ohm sign → ω, İ → i̇.
    for (text, folded) in [
        ("ϐ", "β"),
        ("ẞ-1", "ss-1"),
        ("ﬀ", "ff"),
        ("\u{2126}", "ω"),
        ("İ", "i\u{307}"),
        ("ABC-1", "abc-1"),
    ] {
        assert_eq!(caseless::default_case_fold_str(text), folded, "{text}");
    }
}
