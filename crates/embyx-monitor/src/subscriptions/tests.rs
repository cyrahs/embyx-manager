//! Ported from `tests/test_monitor_subscriptions.py`, against a temporary PostgreSQL database,
//! plus the golden of `validate_feed_url`, `trim_cursor` and the derived record fields.

use chrono::{TimeDelta, TimeZone as _};
use embyx_core::golden;
use embyx_db::testdb::TestDatabase;

use super::*;

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 2, 12, 0, 0).single().expect("valid")
}

struct Fixture {
    test: TestDatabase,
    database: Database,
    repository: SubscriptionRepository,
}

impl Fixture {
    async fn new() -> Self {
        let test = TestDatabase::create().await;
        let database = test.database();
        let repository = SubscriptionRepository::new(database.clone());
        Self {
            test,
            database,
            repository,
        }
    }

    async fn finish(self) {
        self.database.close().await;
        self.test.drop_database().await;
    }
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

// py: tests/test_monitor_subscriptions.py::test_feed_urls_must_be_http_without_credentials cases=4
#[test]
fn feed_urls_must_be_http_without_credentials() {
    for url in [
        "https://rsshub.test/javbus/star/rwt",
        "http://rsshub:1200/x?limit=20",
        " https://sukebei.nyaa.si/?page=rss&q=a ",
        "ftp://x",
    ] {
        if url.starts_with("ftp") {
            let error = validate_feed_url(url).expect_err("refused");
            assert!(error.contains("absolute HTTP"), "{error}");
        } else {
            assert_eq!(validate_feed_url(url).as_deref(), Ok(url.trim()));
        }
        let error = validate_feed_url("https://user:pw@rsshub.test/x").expect_err("refused");
        assert!(error.contains("credentials"), "{error}");
    }
}

// py: tests/test_monitor_subscriptions.py::test_add_list_and_get_round_trip
#[tokio::test]
async fn add_list_and_get_round_trip() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;

    let created = repository
        .add_rss(
            "https://rsshub.test/javbus/star/rwt",
            "Actor",
            now(),
            Some("演员甲"),
            false,
        )
        .await
        .expect("add");

    assert_eq!(created.kind, SubscriptionKind::Rss);
    assert_eq!(created.feed_url(), "https://rsshub.test/javbus/star/rwt");
    assert_eq!(created.display_name(), "演员甲");
    assert!(created.enabled);
    assert!(created.cursor.is_empty());
    assert!(!created.seed_pending);
    assert_eq!(repository.list().await.expect("list"), std::slice::from_ref(&created));
    assert_eq!(repository.get(created.id).await.expect("get"), Some(created));
    fixture.finish().await;
}

// py: tests/test_monitor_subscriptions.py::test_the_same_feed_cannot_be_subscribed_twice
#[tokio::test]
async fn the_same_feed_cannot_be_subscribed_twice() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    repository
        .add_rss("https://rsshub.test/a", "Actor", now(), None, false)
        .await
        .expect("add");

    let error = repository
        .add_rss("https://rsshub.test/a", "Rank", now(), None, false)
        .await
        .expect_err("a duplicate");
    assert!(matches!(error, SubscriptionError::Exists(_)), "{error:?}");
    assert_eq!(error.to_string(), "already subscribed: https://rsshub.test/a");
    fixture.finish().await;
}

// py: tests/test_monitor_subscriptions.py::test_update_and_delete
#[tokio::test]
async fn update_and_delete() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    let created = repository
        .add_rss("https://rsshub.test/a", "Actor", now(), None, false)
        .await
        .expect("add");

    let disabled = repository
        .update(created.id, now() + TimeDelta::minutes(1), Some(false), None, None)
        .await
        .expect("update")
        .expect("a row");
    assert!(!disabled.enabled);
    assert_eq!(disabled.category, "Actor");

    let moved = repository
        .update(created.id, now(), None, Some("Rank"), None)
        .await
        .expect("update")
        .expect("a row");
    assert_eq!(moved.category, "Rank");
    assert!(!moved.enabled);

    assert_eq!(
        repository
            .update(created.id + 1000, now(), Some(true), None, None)
            .await
            .expect("update"),
        None
    );
    assert!(repository.delete(created.id).await.expect("delete"));
    assert!(!repository.delete(created.id).await.expect("delete"));
    fixture.finish().await;
}

// py: tests/test_monitor_subscriptions.py::test_record_poll_stores_the_cursor_and_settles_the_seed
#[tokio::test]
async fn record_poll_stores_the_cursor_and_settles_the_seed() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    let created = repository
        .add_rss("https://rsshub.test/a", "Actor", now(), None, true)
        .await
        .expect("add");

    repository
        .record_poll(created.id, now(), Some(&strings(&["k1", "k2", "k1"])), None)
        .await
        .expect("poll");
    let record = repository.get(created.id).await.expect("get").expect("a row");
    assert_eq!(record.cursor, ["k1", "k2"]);
    assert!(!record.seed_pending);
    assert_eq!(record.last_polled_at, Some(now()));
    assert_eq!(record.last_error, None);

    // A failed poll keeps the cursor so the items it missed are read next time.
    repository
        .record_poll(created.id, now() + TimeDelta::hours(1), None, Some("boom"))
        .await
        .expect("poll");
    let record = repository.get(created.id).await.expect("get").expect("a row");
    assert_eq!(record.cursor, ["k1", "k2"]);
    assert_eq!(record.last_error.as_deref(), Some("boom"));
    assert_eq!(record.last_polled_at, Some(now() + TimeDelta::hours(1)));
    fixture.finish().await;
}

// py: tests/test_monitor_subscriptions.py::test_talent_subscriptions_derive_their_feed_url_and_are_unique_per_talent
#[tokio::test]
async fn talent_subscriptions_derive_their_feed_url_and_are_unique_per_talent() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;

    let created = repository
        .add_talent(5022, "河北彩花", &strings(&["河北彩伽"]), "Actor", now(), true)
        .await
        .expect("add");

    assert_eq!(created.kind, SubscriptionKind::AvbaseTalent);
    assert_eq!(created.url, None);
    assert_eq!(created.feed_url(), "https://www.avbase.net/talents/5022/feed");
    assert_eq!(created.aliases, ["河北彩伽"]);
    assert!(created.seed_pending);
    let error = repository
        .add_talent(5022, "x", &[], "Rank", now(), false)
        .await
        .expect_err("a duplicate");
    assert!(matches!(error, SubscriptionError::Exists(_)), "{error:?}");
    assert_eq!(error.to_string(), "already subscribed: talent 5022");
    fixture.finish().await;
}

// py: tests/test_monitor_subscriptions.py::test_relinking_a_talent_moves_its_id_and_keeps_the_cursor
#[tokio::test]
async fn relinking_a_talent_moves_its_id_and_keeps_the_cursor() {
    let fixture = Fixture::new().await;
    let repository = &fixture.repository;
    let created = repository
        .add_talent(46505, "小花のん", &[], "Actor", now(), false)
        .await
        .expect("add");
    repository
        .record_poll(created.id, now(), Some(&strings(&["k1"])), Some("404"))
        .await
        .expect("poll");
    let other = repository
        .add_talent(5022, "河北彩花", &[], "Actor", now(), false)
        .await
        .expect("add");
    let rss = repository
        .add_rss("https://rsshub.test/a", "Actor", now(), None, false)
        .await
        .expect("add");

    let relinked = repository
        .relink_talent(created.id, 71334, &strings(&["別名"]), now() + TimeDelta::hours(1))
        .await
        .expect("relink")
        .expect("a row");

    assert_eq!(relinked.talent_id, Some(71334));
    assert_eq!(relinked.feed_url(), "https://www.avbase.net/talents/71334/feed");
    assert_eq!(relinked.aliases, ["別名"]);
    assert_eq!(relinked.cursor, ["k1"]);
    assert_eq!(relinked.updated_at, now() + TimeDelta::hours(1));
    let error = repository
        .relink_talent(created.id, i64::from(other.talent_id.unwrap_or(0)), &[], now())
        .await
        .expect_err("a duplicate");
    assert!(matches!(error, SubscriptionError::Exists(_)), "{error:?}");
    assert_eq!(
        repository.relink_talent(rss.id, 1, &[], now()).await.expect("relink"),
        None
    );
    fixture.finish().await;
}

/// A talent id beyond the `INTEGER` column is refused before the database, with asyncpg's
/// `DataError` text.
#[tokio::test]
async fn a_talent_id_beyond_32_bits_is_refused() {
    let fixture = Fixture::new().await;
    let error = fixture
        .repository
        .add_talent(1 << 31, "x", &[], "Actor", now(), false)
        .await
        .expect_err("refused");
    assert_eq!(
        error.to_string(),
        "invalid input for query argument $2: 2147483648 (value out of int32 range)"
    );
    assert_eq!(error.class_name(), "DataError");
    fixture.finish().await;
}

fn optional(value: &embyx_core::pyjson::Value) -> Option<String> {
    value.as_str().map(str::to_owned)
}

/// `validate_feed_url`, `trim_cursor` and the derived `feed_url` / `display_name` over the
/// golden's inputs: odd ports, IPv6 hosts, credentials, whitespace, non-ASCII hosts and paths.
#[test]
fn feed_urls_cursors_and_records_match_python() {
    let document = golden::load("monitor/subscriptions.json");
    let mut seen = 0;
    for case in golden::cases(&document) {
        if let Some(urls) = case.get("feed_urls") {
            for entry in golden::list(urls) {
                let url = golden::string(golden::get(entry, "url"));
                let expected = match entry.get("valid") {
                    Some(valid) => Ok(golden::string(valid)),
                    None => Err(golden::string(golden::get(entry, "error"))),
                };
                assert_eq!(validate_feed_url(&url), expected, "{url:?}");
                seen += 1;
            }
        }
        if let Some(cursors) = case.get("cursors") {
            for entry in golden::list(cursors) {
                let keys: Vec<String> = golden::list(golden::get(entry, "keys"))
                    .iter()
                    .map(golden::string)
                    .collect();
                let trimmed: Vec<String> = golden::list(golden::get(entry, "trimmed"))
                    .iter()
                    .map(golden::string)
                    .collect();
                assert_eq!(trim_cursor(&keys), trimmed, "{keys:?}");
                seen += 1;
            }
        }
        if let Some(records) = case.get("records") {
            for entry in golden::list(records) {
                let kind = SubscriptionKind::parse(&golden::string(golden::get(entry, "kind"))).expect("a kind");
                let talent_id = match golden::get(entry, "talent_id") {
                    embyx_core::pyjson::Value::Int(id) => Some(i32::try_from(*id).expect("small")),
                    _ => None,
                };
                let record = SubscriptionRecord {
                    id: 1,
                    kind,
                    category: "Actor".to_owned(),
                    enabled: true,
                    url: optional(golden::get(entry, "url")),
                    talent_id,
                    name: optional(golden::get(entry, "name")),
                    aliases: Vec::new(),
                    cursor: Vec::new(),
                    seed_pending: false,
                    last_polled_at: None,
                    last_error: None,
                    created_at: now(),
                    updated_at: now(),
                };
                assert_eq!(
                    record.feed_url(),
                    golden::string(golden::get(entry, "feed_url")),
                    "{entry:?}"
                );
                assert_eq!(
                    record.display_name(),
                    golden::string(golden::get(entry, "display_name")),
                    "{entry:?}"
                );
                seen += 1;
            }
        }
    }
    assert!(seen > 30, "{seen}");
}
