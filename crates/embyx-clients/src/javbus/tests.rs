//! JavBus: magnets, a video's cast, a star's pages and their pagination, star search.

use std::fmt::Write as _;
use std::sync::Mutex;

use super::*;
use crate::net::fakeweb::{FakeWeb, Reply, Seen, test_roots};
use crate::net::retry::RecordingSleeper;

const MAIN_PAGE_HTML: &str = "
<html>
<script>
    var gid = 12345;
    var uc = 0;
    var img = '/pics/cover/sample.jpg';
</script>
</html>
";

const AJAX_RESPONSE_HTML: &str = r#"
<html>
    <tr>
        <td width="70%">
            <a href="magnet:?xt=urn:btih:de439fca97a0365b47d9b087010115a94cad6853&dn=release1">release1</a>
        </td>
        <td style="text-align:center">
            <a href="magnet:?xt=urn:btih:de439fca97a0365b47d9b087010115a94cad6853&dn=release1">2.02GB</a>
        </td>
        <td style="text-align:center">
            <a href="magnet:?xt=urn:btih:de439fca97a0365b47d9b087010115a94cad6853&dn=release1">2025-01-01</a>
        </td>
    </tr>
    <tr>
        <td width="70%">
            <a href="magnet:?xt=urn:btih:a1b2c3d4e5f67890abcdef1234567890abcdef12&dn=release2">release2</a>
            <a class="btn btn-mini-new btn-primary"
               href="magnet:?xt=urn:btih:a1b2c3d4e5f67890abcdef1234567890abcdef12&dn=release2">高清</a>
            <a class="btn btn-mini-new btn-warning"
               href="magnet:?xt=urn:btih:a1b2c3d4e5f67890abcdef1234567890abcdef12&dn=release2">字幕</a>
        </td>
        <td style="text-align:center">
            <a href="magnet:?xt=urn:btih:a1b2c3d4e5f67890abcdef1234567890abcdef12&dn=release2">1.5GB</a>
        </td>
        <td style="text-align:center">
            <a href="magnet:?xt=urn:btih:a1b2c3d4e5f67890abcdef1234567890abcdef12&dn=release2">2025-01-02</a>
        </td>
    </tr>
    <a href="http://other-link.com">Other Link</a>
</html>
"#;

/// A client pointed at the fake web, its retry waits recorded.
struct Bus {
    web: FakeWeb,
    client: JavBusClient,
    sleeps: RecordingSleeper,
}

async fn start_bus(handler: impl Fn(&Seen) -> Reply + Send + Sync + 'static) -> Bus {
    let web = FakeWeb::start(handler).await;
    let sleeps = RecordingSleeper::new();
    let options = JavBusOptions {
        timeout: Duration::from_millis(500),
        ..JavBusOptions::default()
    };
    let client_options = ClientOptions {
        proxy: Some(web.proxy.clone()),
        roots: test_roots(),
        ..JavBusClient::client_options(&options)
    };
    let client = JavBusClient::with_client(&options, &client_options, sleeps.sleeper()).expect("client");
    Bus { web, client, sleeps }
}

/// The same page for every request.
async fn bus_serving(page: &'static str) -> Bus {
    start_bus(move |_: &Seen| Reply::bytes(200, page)).await
}

fn star_url(page: &str) -> String {
    format!("{DEFAULT_HOST}/star/ACTOR-1{page}")
}

// py: tests/test_javbus.py::test_get_magnets_success
#[tokio::test]
async fn get_magnets_success() {
    let bus = start_bus(|seen: &Seen| {
        if seen.path().contains("uncledatoolsbyajax.php") {
            Reply::bytes(200, AJAX_RESPONSE_HTML)
        } else {
            Reply::bytes(200, MAIN_PAGE_HTML)
        }
    })
    .await;
    let video_id = "TEST-001";
    let magnets = bus.client.get_magnets(video_id).await.expect("magnets");

    assert_eq!(magnets.len(), 2);
    let release1 = magnets
        .iter()
        .find(|m| m.magnet.contains("de439fca97a0365b47d9b087010115a94cad6853"))
        .expect("first");
    assert_eq!(
        release1.magnet,
        format!("magnet:?xt=urn:btih:de439fca97a0365b47d9b087010115a94cad6853&dn={video_id}")
    );
    assert_eq!(release1.size, "2.02GB");
    assert!(release1.size_int > 0);
    let release2 = magnets
        .iter()
        .find(|m| m.magnet.contains("a1b2c3d4e5f67890abcdef1234567890abcdef12"))
        .expect("second");
    assert_eq!(
        release2.magnet,
        format!("magnet:?xt=urn:btih:a1b2c3d4e5f67890abcdef1234567890abcdef12&dn={video_id}")
    );
    assert_eq!(release2.size, "1.5GB");
    // The smaller upload carries the quality badges, so it is offered first.
    assert!(release1.tags.is_empty());
    assert_eq!(release2.tags, ["高清", "字幕"]);
    assert_eq!(magnets, [release2.clone(), release1.clone()]);
    // humanfriendly reads GB as decimal: 2.02 * 1000^3.
    assert_eq!(release1.size_int, 2_020_000_000);
    assert_eq!(release2.score, 3_i128.pow(8) * 1_500_000_000);

    let seen = bus.web.seen();
    assert_eq!(seen.len(), 2);
    assert!(seen[0].url.ends_with(&format!("/{video_id}")));
    assert!(seen[1].url.contains("uncledatoolsbyajax.php"));
    assert!(seen[1].url.contains("gid=12345"));
    assert!(
        seen[1]
            .header("Referer")
            .expect("referer")
            .ends_with(&format!("/{video_id}"))
    );
    // The client's own headers go with every request.
    assert_eq!(seen[0].header("Cookie"), Some("existmag=all"));
    assert_eq!(seen[0].header("Accept-Language"), Some("zh-CN,zh;q=0.9,en;q=0.8"));
    let floor: u32 = seen[1]
        .url
        .rsplit("floor=")
        .next()
        .expect("floor")
        .parse()
        .expect("number");
    assert!((1..=1000).contains(&floor));
}

// py: tests/test_javbus.py::test_magnet_score_prefers_tags_over_size
#[test]
fn magnet_score_prefers_tags_over_size() {
    let gib: i128 = 1 << 30;
    assert!(magnet_score(gib, 1) > magnet_score(100 * gib, 0));
    // A second badge is worth a (3/2)^8 ≈ 25x size gap, more than any same-title spread.
    assert!(magnet_score(gib, 2) > magnet_score(20 * gib, 1));
    assert!(magnet_score(2 * gib, 0) > magnet_score(gib, 0));
    // An unreadable size cannot be trusted, badges or not.
    assert_eq!(magnet_score(0, 2), 0);
}

// py: tests/test_javbus.py::test_get_magnets_no_variables
#[tokio::test]
async fn get_magnets_no_variables() {
    let bus = bus_serving("<html>No variables here</html>").await;
    assert_eq!(bus.client.get_magnets("TEST-002").await.expect("magnets"), []);
    assert_eq!(bus.web.seen().len(), 1);
}

#[tokio::test]
async fn get_magnets_retries_http_errors_and_skips_unusable_rows() {
    let calls = Mutex::new(0);
    let bus = start_bus(move |seen: &Seen| {
        let mut calls = calls.lock().expect("lock");
        *calls += 1;
        if *calls == 1 {
            return Reply::status(502);
        }
        if seen.path().contains("uncledatoolsbyajax.php") {
            Reply::bytes(
                200,
                "<tr><td><a href=\"magnet:?xt=urn:btih:ABC\">a</a></td><td>huge</td></tr>\
                 <tr><td><a href=\"magnet:?xt=urn:btih:abc\">dup</a></td><td>1GB</td></tr>\
                 <tr><td><a href=\"magnet:?xt=urn:sha1:x\">no btih</a></td><td>1GB</td></tr>\
                 <tr><td>only one cell</td></tr>",
            )
        } else {
            Reply::bytes(200, MAIN_PAGE_HTML)
        }
    })
    .await;
    let magnets = bus.client.get_magnets("ABC-1").await.expect("magnets");
    assert_eq!(bus.sleeps.delays(), [1.0]);
    // An unreadable size scores zero; the hash is lower-cased and seen once.
    assert_eq!(
        magnets,
        [JavBusMagnet {
            magnet: "magnet:?xt=urn:btih:abc&dn=ABC-1".to_owned(),
            size: "huge".to_owned(),
            size_int: 0,
            tags: Vec::new(),
            score: 0,
        }]
    );
}

// py: tests/test_javbus.py::test_get_video_actors_reads_names_and_deduplicates_ids
#[tokio::test]
async fn get_video_actors_reads_names_and_deduplicates_ids() {
    let bus = bus_serving(
        r#"
    <html>
        <a href="/star/a123"></a>
        <a href="https://www.javbus.com/star/B456/"><img alt="演员乙"></a>
        <a class="avatar-box" href="/star/A123"><span>演员甲</span></a>
        <a href="/star/A123">重复演员</a>
        <a href="/genre/7">其他链接</a>
    </html>
    "#,
    )
    .await;
    let actors = bus.client.get_video_actors("ABC-123").await.expect("actors");
    let pairs: Vec<(&str, &str)> = actors.iter().map(|a| (a.actor_id.as_str(), a.name.as_str())).collect();
    assert_eq!(pairs, [("a123", "演员甲"), ("B456", "演员乙")]);
    assert_eq!(bus.web.urls(), [format!("{DEFAULT_HOST}/ABC-123")]);
}

// py: tests/test_javbus.py::test_scrape_one_page
#[tokio::test]
async fn scrape_one_page() {
    let bus = bus_serving(
        r#"
    <html>
        <a class="movie-box featured" href="https://www.javbus.com/VID-001/"></a>
        <a class="movie-box" href="https://www.javbus.com/VID-002"></a>
        <a class="movie-box" href="https://www.javbus.com/VID-001"></a> <!-- Duplicate -->
    </html>
    "#,
    )
    .await;
    let ids = bus.client.scrape_one_page("ACTOR-1", 1).await.expect("ids");
    assert_eq!(ids, ["VID-001", "VID-002"]);
    bus.client.scrape_one_page("ACTOR-1", 2).await.expect("ids");
    assert_eq!(bus.web.urls(), [star_url(""), star_url("/2")]);
}

// py: tests/test_javbus.py::test_get_total_page
#[tokio::test]
async fn get_total_page() {
    let bus = bus_serving(
        r#"
    <html>
        <a href="/star/ACTOR-1/1">1</a>
        <a href="/star/ACTOR-1/2">2</a>
        <a href="/star/ACTOR-1/3">3</a>
    </html>
    "#,
    )
    .await;
    assert_eq!(bus.client.get_total_page("ACTOR-1", None).await.expect("total"), 3);
    let urls = bus.web.urls();
    assert!(urls.contains(&star_url("")));
    assert!(urls.contains(&star_url("/4")));

    let bus = bus_serving("<html></html>").await;
    assert_eq!(bus.client.get_total_page("ACTOR-1", None).await.expect("total"), 1);
}

// py: tests/test_javbus.py::test_scrape
#[tokio::test]
async fn scrape() {
    let calls = Mutex::new(Vec::new());
    let ids = scrape_pages(None, async { Ok(2) }, |page| {
        calls.lock().expect("lock").push(page);
        async move {
            Ok(match page {
                1 => vec!["A".to_owned(), "B".to_owned()],
                _ => vec!["C".to_owned()],
            })
        }
    })
    .await
    .expect("ids");
    assert_eq!(ids, ["A", "B", "C"]);
    assert_eq!(calls.lock().expect("lock").len(), 2);
}

// py: tests/test_javbus.py::test_get_total_page_follows_sliding_windows_through_page_26
#[tokio::test]
async fn get_total_page_follows_sliding_windows_through_page_26() {
    let requested = std::sync::Arc::new(Mutex::new(Vec::new()));
    let log = std::sync::Arc::clone(&requested);
    let bus = start_bus(move |seen: &Seen| {
        let page: u64 = if seen.url.ends_with("/ACTOR-1") {
            1
        } else {
            seen.url.rsplit('/').next().expect("page").parse().expect("number")
        };
        log.lock().expect("lock").push(page);
        let window: Vec<u64> = match page {
            1 => (1..11).collect(),
            10 => (6..16).collect(),
            15 => (11..21).collect(),
            20 => (16..26).collect(),
            25 => (17..27).collect(),
            26 => Vec::new(),
            27 => return Reply::status(404),
            other => panic!("unexpected page {other}"),
        };
        let links = window.iter().fold(String::new(), |mut links, number| {
            let _ = write!(links, "<a href=\"/star/ACTOR-1/{number}\">{number}</a>");
            links
        });
        Reply::bytes(200, format!("<a class=\"movie-box\" href=\"/{page:03}\"></a>{links}"))
    })
    .await;
    assert_eq!(bus.client.get_total_page("ACTOR-1", None).await.expect("total"), 26);
    assert_eq!(*requested.lock().expect("lock"), [1, 10, 15, 20, 25, 26, 27]);
}

// py: tests/test_javbus.py::test_scrape_reports_page_progress_and_globally_deduplicates
#[tokio::test]
async fn scrape_reports_page_progress_and_globally_deduplicates() {
    let events = std::sync::Arc::new(Mutex::new(Vec::new()));
    let recorded = std::sync::Arc::clone(&events);
    let progress = progress_callback(move |completed, total, current| {
        recorded.lock().expect("lock").push((completed, total, current));
    });
    let ids = scrape_pages(Some(progress.as_ref()), async { Ok(3) }, |page| async move {
        tokio::time::sleep(Duration::from_millis((4 - page) * 5)).await;
        Ok(match page {
            1 => vec!["A".to_owned(), "B".to_owned()],
            2 => vec!["B".to_owned(), "C".to_owned()],
            _ => vec!["D".to_owned()],
        })
    })
    .await
    .expect("ids");
    assert_eq!(ids, ["A", "B", "C", "D"]);
    let events = events.lock().expect("lock").clone();
    assert_eq!(events[..2], [(0, None, None), (0, Some(3), None)]);
    let completed: Vec<u64> = events[2..].iter().map(|event| event.0).collect();
    assert_eq!(completed, [1, 2, 3]);
    let mut current: Vec<Option<u64>> = events[2..].iter().map(|event| event.2).collect();
    // Pages finish in reverse order here, and are reported as they finish.
    assert_eq!(current, [Some(3), Some(2), Some(1)]);
    current.sort_unstable();
    assert_eq!(current, [Some(1), Some(2), Some(3)]);
}

#[tokio::test]
async fn scrape_through_the_client_reports_discovery_progress_too() {
    let bus = start_bus(|seen: &Seen| {
        if seen.url.ends_with("/ACTOR-1") {
            Reply::bytes(
                200,
                "<a class=\"movie-box\" href=\"/A-1\"></a><a href=\"/star/ACTOR-1/2\">2</a>",
            )
        } else if seen.url.ends_with("/2") {
            Reply::bytes(
                200,
                "<a class=\"movie-box\" href=\"/A-2\"></a><a class=\"movie-box\" href=\"/a-1\"></a>",
            )
        } else {
            Reply::status(404)
        }
    })
    .await;
    let events = std::sync::Arc::new(Mutex::new(Vec::new()));
    let recorded = std::sync::Arc::clone(&events);
    let progress = progress_callback(move |completed, total, current| {
        recorded.lock().expect("lock").push((completed, total, current));
    });
    let ids = bus
        .client
        .scrape("ACTOR-1", Some(progress.as_ref()))
        .await
        .expect("ids");
    assert_eq!(ids, ["A-1", "A-2"]);
    let events = events.lock().expect("lock").clone();
    // Page 3 is a 404: the end, found without a report of its own.
    assert_eq!(
        events[..4],
        [
            (0, None, None),
            (0, None, Some(1)),
            (0, None, Some(2)),
            (0, Some(2), None)
        ]
    );
    assert_eq!(events.len(), 6);
}

// py: tests/test_javbus.py::test_terminal_404_is_not_retried
#[tokio::test]
async fn terminal_404_is_not_retried() {
    let bus = start_bus(|_: &Seen| Reply::status(404)).await;
    assert_eq!(
        bus.client.scrape_one_page("ACTOR-1", 2).await.expect("ids"),
        Vec::<String>::new()
    );
    assert_eq!(bus.web.urls(), [star_url("/2")]);
    assert!(bus.sleeps.delays().is_empty());
    // 410 is as final; other errors are retried and then raised.
    let bus = bus_serving_status(410).await;
    assert!(bus.client.scrape_one_page("ACTOR-1", 2).await.expect("ids").is_empty());
    let bus = bus_serving_status(503).await;
    let error = bus.client.scrape_one_page("ACTOR-1", 2).await.unwrap_err();
    assert_eq!(error.class_name(), "HTTPStatusError");
    assert_eq!(bus.web.urls().len(), 3);
    assert_eq!(bus.sleeps.delays(), [1.0, 2.0]);
}

async fn bus_serving_status(status: u16) -> Bus {
    start_bus(move |_: &Seen| Reply::status(status)).await
}

// py: tests/test_javbus.py::test_pagination_limit_fails_instead_of_returning_partial_results
#[tokio::test]
async fn pagination_limit_fails_instead_of_returning_partial_results() {
    let mut bus = bus_serving(
        r#"
    <a class="movie-box" href="/VID-001"></a>
    <a href="/star/ACTOR-1/3">3</a>
    "#,
    )
    .await;
    bus.client.max_actor_pages = 3;
    let error = bus.client.get_total_page("ACTOR-1", None).await.unwrap_err();
    assert_eq!(error.class_name(), "JavBusPaginationError");
    assert!(error.to_string().contains("safety limit"), "{error}");
    assert_eq!(
        error.to_string(),
        "JavBus actor pagination reached the 3-page safety limit without an end"
    );
    bus.client.max_actor_pages = 2;
    let error = bus.client.get_total_page("ACTOR-1", None).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "JavBus actor pagination exceeds the 2-page safety limit"
    );
}

#[tokio::test]
async fn a_missing_star_and_gaps_in_the_pages_are_pagination_errors() {
    let bus = start_bus(|_: &Seen| Reply::status(404)).await;
    let error = bus.client.get_total_page("ACTOR-1", None).await.unwrap_err();
    assert_eq!(error.to_string(), "JavBus actor 'ACTOR-1' was not found");
    assert!(bus.client.get_actor("ACTOR-1").await.expect("actor").is_none());

    let bus = start_bus(|seen: &Seen| {
        if seen.url.ends_with("/ACTOR-1") {
            Reply::bytes(
                200,
                "<a class=\"movie-box\" href=\"/A-1\"></a><a href=\"/star/ACTOR-1/5\">5</a>",
            )
        } else {
            Reply::status(404)
        }
    })
    .await;
    let error = bus.client.get_total_page("ACTOR-1", None).await.unwrap_err();
    assert_eq!(error.to_string(), "JavBus actor pagination has a gap at page 5");

    let bus = start_bus(|seen: &Seen| {
        if seen.url.ends_with("/ACTOR-1") {
            Reply::bytes(200, "<a class=\"movie-box\" href=\"/A-1\"></a>")
        } else {
            Reply::bytes(200, "<a href=\"/star/ACTOR-1/9\">9</a>")
        }
    })
    .await;
    let error = bus.client.get_total_page("ACTOR-1", None).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "JavBus actor pagination returned an empty gap at page 2"
    );

    // A page that repeats an earlier one ends the catalog; links to other hosts are ignored.
    let bus = start_bus(|seen: &Seen| {
        if seen.url.ends_with("/ACTOR-1") {
            Reply::bytes(
                200,
                "<a class=\"movie-box\" href=\"/A-1\"></a><a href=\"https://elsewhere.test/star/ACTOR-1/50\">x</a>",
            )
        } else {
            Reply::bytes(200, "<a class=\"movie-box\" href=\"/a-1/\"></a>")
        }
    })
    .await;
    assert_eq!(bus.client.get_total_page("ACTOR-1", None).await.expect("total"), 1);
}

// py: tests/test_javbus.py::test_scrape_rejects_an_empty_page_inside_discovered_range
#[tokio::test]
async fn scrape_rejects_an_empty_page_inside_discovered_range() {
    let error = scrape_pages(None, async { Ok(3) }, |page| async move {
        Ok(match page {
            1 => vec!["A".to_owned()],
            2 => Vec::new(),
            _ => vec!["C".to_owned()],
        })
    })
    .await
    .unwrap_err();
    assert_eq!(error.class_name(), "JavBusPaginationError");
    assert_eq!(error.to_string(), "JavBus actor pagination returned an empty page at 2");
    // One page may be empty: a star with no videos.
    assert_eq!(
        scrape_pages(None, async { Ok(1) }, |_| async { Ok(Vec::new()) }).await,
        Ok(Vec::new())
    );
}

// py: tests/test_javbus.py::test_search_stars_reads_every_star_page_the_search_lists
#[tokio::test]
async fn search_stars_reads_every_star_page_the_search_lists() {
    let bus = bus_serving(
        r#"
    <div id="waterfall">
      <a class="avatar-box text-center" href="https://www.javbus.com/star/sl1"><span class="pb10">河北彩花</span></a>
      <a class="avatar-box text-center" href="https://www.javbus.com/star/new1"><span class="pb10">河北彩伽</span></a>
      <a href="https://www.javbus.com/star/sl1">dup</a>
      <a href="https://www.javbus.com/genre/1">not a star</a>
    </div>
    "#,
    )
    .await;
    let stars = bus.client.search_stars("河北彩").await.expect("stars");
    assert_eq!(
        stars,
        [
            JavBusActor {
                actor_id: "sl1".to_owned(),
                name: "河北彩花".to_owned(),
            },
            JavBusActor {
                actor_id: "new1".to_owned(),
                name: "河北彩伽".to_owned(),
            },
        ]
    );
    assert_eq!(
        bus.web.urls(),
        ["https://www.javbus.com/searchstar/%E6%B2%B3%E5%8C%97%E5%BD%A9"]
    );
    // No such star: an empty list, not an error.
    let bus = bus_serving_status(404).await;
    assert_eq!(bus.client.search_stars("x").await.expect("stars"), []);
}

#[tokio::test]
async fn get_actor_reads_the_name_from_the_avatar_or_the_title() {
    let bus = bus_serving(
        "<html><head><title>河北彩花 - 女優 - 影片</title></head><body>\
         <div class=\"avatar-box\"><span class=\"pb10\"> 河北 彩花 </span></div>\
         <a class=\"movie-box\" href=\"/abc-1\"></a></body></html>",
    )
    .await;
    assert_eq!(
        bus.client.get_actor("sl1").await.expect("actor"),
        Some(JavBusActorPage {
            actor_id: "sl1".to_owned(),
            name: "河北 彩花".to_owned(),
            video_ids: vec!["ABC-1".to_owned()],
        })
    );
    let bus = bus_serving("<html><head><title>河北彩花 - 女優</title></head><body></body></html>").await;
    assert_eq!(
        bus.client.get_actor("sl1").await.expect("actor").expect("page").name,
        "河北彩花"
    );
}

#[tokio::test]
async fn transport_failures_are_retried_then_raised_by_class() {
    for (reply, class) in [
        (Reply::Close, "RemoteProtocolError"),
        (Reply::Stall(Duration::from_secs(5)), "ReadTimeout"),
        (
            Reply::Truncated {
                status: 200,
                sent: b"<html>".to_vec(),
                length: 100,
            },
            "RemoteProtocolError",
        ),
    ] {
        let bus = start_bus(move |_: &Seen| reply.clone()).await;
        let error = bus.client.get_video_actors("ABC-1").await.unwrap_err();
        assert_eq!(error.class_name(), class);
        assert_eq!(bus.web.seen().len(), 3, "{class}");
        assert_eq!(bus.sleeps.delays(), [1.0, 2.0]);
    }
    // A page that is not a document is lxml's error, not retried.
    let bus = bus_serving("</a>").await;
    let error = bus.client.get_video_actors("ABC-1").await.unwrap_err();
    assert_eq!(
        (error.class_name(), error.to_string()),
        ("ParserError", "Document is empty".to_owned())
    );
    assert_eq!(bus.web.seen().len(), 1);
}

#[test]
fn page_numbers_and_ids_follow_the_python_patterns() {
    assert_eq!(page_number("/star/ACTOR-1/12", "ACTOR-1"), Some(12));
    assert_eq!(page_number("/star/ACTOR-1/12/", "ACTOR-1"), Some(12));
    assert_eq!(page_number("/star/ACTOR-1/٣", "ACTOR-1"), None);
    assert_eq!(page_number("/star/ACTOR-1/1٣", "ACTOR-1"), Some(13));
    assert_eq!(page_number("/star/ACTOR-1/012", "ACTOR-1"), None);
    assert_eq!(page_number("/star/ACTOR-1/12//", "ACTOR-1"), None);
    assert_eq!(page_number("/star/ACTOR-12/3", "ACTOR-1"), None);
    assert!(is_actor_id("a_b-C9"));
    assert!(!is_actor_id(""));
    assert!(!is_actor_id(&"x".repeat(33)));
    assert!(!is_actor_id("é"));
    assert_eq!(btih("magnet:?xt=urn:btih:ABCdef12&dn=x"), Some("ABCdef12"));
    assert_eq!(btih("magnet:?xt=urn:btih:&dn=x"), None);
}

/// `(class, message)` of a JavBus error.
fn raised(error: &JavBusError) -> (String, String) {
    (error.class_name().to_owned(), error.to_string())
}

// `goldens/clients/javbus.json`: star pages, magnet tables, a video's cast and star search
// results as the Python client reads them (its HTTP calls answered with the recorded pages).
#[tokio::test]
async fn pages_match_the_baseline() {
    use embyx_core::golden;
    let document = golden::load("clients/javbus.json");
    let mut seen = std::collections::BTreeMap::new();
    for case in golden::cases(&document) {
        let kind = golden::string(golden::get(case, "kind"));
        *seen.entry(kind.clone()).or_insert(0) += 1;
        let html = golden::string(golden::get(case, "html"));
        let error = case
            .get("error")
            .map(|class| (golden::string(class), golden::string(golden::get(case, "message"))));
        match kind.as_str() {
            "actor_page" => {
                let client = JavBusClient::new(&JavBusOptions::default()).expect("client");
                let got = client.parse_actor_page("ACTOR-1", &html);
                if let Some(error) = error {
                    assert_eq!(raised(&got.expect_err(&html)), error, "{html:?}");
                    continue;
                }
                let (ids, pages) = got.unwrap_or_else(|error| panic!("{html:?}: {error}"));
                let want_ids: Vec<String> = golden::list(golden::get(case, "ids"))
                    .iter()
                    .map(golden::string)
                    .collect();
                let want_pages: Vec<u64> = golden::list(golden::get(case, "pages"))
                    .iter()
                    .map(|page| u64::from(golden::uint(page)))
                    .collect();
                assert_eq!(ids, want_ids, "{html:?}");
                assert_eq!(pages.into_iter().collect::<Vec<_>>(), want_pages, "{html:?}");
            }
            "magnets" => {
                let video_id = golden::string(golden::get(case, "video_id"));
                let answer = html.clone();
                let bus = start_bus(move |seen: &Seen| {
                    if seen.path().starts_with("/ajax/") {
                        Reply::bytes(200, answer.clone())
                    } else {
                        Reply::bytes(
                            200,
                            "<script>var gid = 12345;\nvar uc = 0;\nvar img = '/pics/cover/x.jpg';</script>",
                        )
                    }
                })
                .await;
                let got = bus.client.get_magnets(&video_id).await;
                if let Some(error) = error {
                    assert_eq!(raised(&got.expect_err(&html)), error, "{html:?}");
                    continue;
                }
                let magnets = got.unwrap_or_else(|error| panic!("{html:?}: {error}"));
                let want: Vec<JavBusMagnet> = golden::list(golden::get(case, "magnets"))
                    .iter()
                    .map(|magnet| JavBusMagnet {
                        magnet: golden::string(golden::get(magnet, "magnet")),
                        size: golden::string(golden::get(magnet, "size")),
                        size_int: golden::int(golden::get(magnet, "size_int")),
                        tags: golden::list(golden::get(magnet, "tags"))
                            .iter()
                            .map(golden::string)
                            .collect(),
                        score: golden::int(golden::get(magnet, "score")),
                    })
                    .collect();
                assert_eq!(magnets, want, "{html:?}");
            }
            "video_actors" | "search_stars" => {
                let answer = html.clone();
                let bus = start_bus(move |_: &Seen| Reply::bytes(200, answer.clone())).await;
                let actors = if kind == "video_actors" {
                    bus.client.get_video_actors("ABC-123").await
                } else {
                    bus.client.search_stars("ABC-123").await
                }
                .unwrap_or_else(|error| panic!("{kind} {html:?}: {error}"));
                let want: Vec<JavBusActor> = golden::list(golden::get(case, "actors"))
                    .iter()
                    .map(|actor| {
                        let actor = golden::list(actor);
                        JavBusActor {
                            actor_id: golden::string(&actor[0]),
                            name: golden::string(&actor[1]),
                        }
                    })
                    .collect();
                assert_eq!(actors, want, "{kind} {html:?}");
            }
            other => panic!("unknown golden kind {other}"),
        }
    }
    assert_eq!(
        seen.into_iter().collect::<Vec<_>>(),
        [
            ("actor_page".to_owned(), 6),
            ("magnets".to_owned(), 5),
            ("search_stars".to_owned(), 2),
            ("video_actors".to_owned(), 3),
        ]
    );
}
