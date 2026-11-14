//! Ported from `tests/test_monitor_playlists_repository.py`, against a temporary PostgreSQL
//! database, plus the stored JSON bytes.

use chrono::{TimeDelta, TimeZone as _};
use embyx_db::testdb::TestDatabase;

use super::*;
use crate::acquisitions::{AcquisitionRepository, AcquisitionState, Sighting};

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 20, 12, 0, 0).single().expect("valid")
}

fn later() -> DateTime<Utc> {
    now() + TimeDelta::hours(1)
}

/// `ranked(key, kind, note, avids)`.
fn ranked(key: &str, kind: i64, note: &str, avids: &[&str]) -> RankedList {
    RankedList {
        key: key.to_owned(),
        kind,
        note: note.to_owned(),
        entries: avids
            .iter()
            .enumerate()
            .map(|(index, avid)| RankedEntry {
                rank: index + 1,
                avid: (*avid).to_owned(),
                title: format!("title {avid}"),
            })
            .collect(),
    }
}

fn avids(record: &PlaylistRecord) -> Vec<&str> {
    record.entries.iter().map(|entry| entry.avid.as_str()).collect()
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

// py: tests/test_monitor_playlists_repository.py::test_replace_lists_inserts_updates_and_marks_the_departed
#[tokio::test]
async fn replace_lists_inserts_updates_and_marks_the_departed() {
    let test = TestDatabase::create().await;
    let database = test.database();
    let repository = PlaylistRepository::new(database.clone());

    repository
        .replace_lists(
            &[
                ranked("k7", 7, "有码", &["A-001", "B-002"]),
                ranked("k5", 5, "JL", &["C-003"]),
            ],
            now(),
        )
        .await
        .expect("replace");
    repository.set_enabled("k7", false, now()).await.expect("disable");
    repository
        .replace_lists(&[ranked("k7", 7, "有码 v2", &["B-002", "A-001"])], later())
        .await
        .expect("replace");

    let records = repository.list().await.expect("list");
    let k7 = records.iter().find(|record| record.key == "k7").expect("k7");
    let k5 = records.iter().find(|record| record.key == "k5").expect("k5");
    assert_eq!(avids(k7), ["B-002", "A-001"]);
    assert_eq!(k7.name, "有码 v2");
    assert!(!k7.enabled);
    assert_eq!(k7.last_error, None);
    assert_eq!(k7.created_at, now());
    assert_eq!(k7.updated_at, later());
    assert_eq!(k5.last_error.as_deref(), Some("the source no longer publishes this list"));
    assert_eq!(avids(k5), ["C-003"]);
    assert!(k5.enabled);

    database.close().await;
    test.drop_database().await;
}

// py: tests/test_monitor_playlists_repository.py::test_sync_state_and_source_round_trip
#[tokio::test]
async fn sync_state_and_source_round_trip() {
    let test = TestDatabase::create().await;
    let database = test.database();
    let repository = PlaylistRepository::new(database.clone());
    repository
        .replace_lists(&[ranked("k2024", 2024, "2024", &["A-001", "B-002", "C-003"])], now())
        .await
        .expect("replace");

    assert_eq!(repository.source().await.expect("source"), None);
    repository.record_source("20260112", now()).await.expect("source");
    repository.record_source("20260201", later()).await.expect("source");
    let source = repository.source().await.expect("source").expect("stored");
    assert_eq!(
        (source.database_name.as_str(), source.fetched_at),
        ("20260201", later())
    );

    repository.record_error("k2024", now(), "boom").await.expect("error");
    let record = repository.get("k2024").await.expect("get").expect("stored");
    assert_eq!(record.last_error.as_deref(), Some("boom"));

    repository
        .record_sync(
            "k2024",
            later(),
            &strings(&["A-001", "C-003"]),
            &strings(&["B-002"]),
            Some("pl9"),
        )
        .await
        .expect("sync");
    let record = repository.get("k2024").await.expect("get").expect("stored");
    assert_eq!(record.present, ["A-001", "C-003"]);
    assert_eq!(record.missing, ["B-002"]);
    assert_eq!(record.emby_playlist_id.as_deref(), Some("pl9"));
    assert_eq!(record.last_synced_at, Some(later()));
    assert_eq!(record.last_error, None);

    assert_eq!(repository.get("missing").await.expect("get"), None);
    assert_eq!(repository.set_enabled("missing", true, now()).await.expect("enable"), None);

    database.close().await;
    test.drop_database().await;
}

// py: tests/test_monitor_playlists_repository.py::test_ledger_states_for_only_reports_tracked_avids
#[tokio::test]
async fn ledger_states_for_only_reports_tracked_avids() {
    let test = TestDatabase::create().await;
    let database = test.database();
    let ledger = AcquisitionRepository::new(database.clone());
    ledger
        .discover(
            "A-001",
            "manual",
            now(),
            Sighting {
                task_dir_path: Some("/115/x".to_owned()),
                ..Sighting::default()
            },
        )
        .await
        .expect("discover");

    let states = ledger.states_for(&strings(&["A-001", "B-002"])).await.expect("states");
    assert_eq!(
        states.into_iter().collect::<Vec<_>>(),
        [("A-001".to_owned(), AcquisitionState::Discovered)]
    );
    assert!(ledger.states_for(&[]).await.expect("states").is_empty());

    database.close().await;
    test.drop_database().await;
}

/// The JSON columns hold Python's bytes: the entries with non-ASCII titles as is, the code
/// lists with ASCII escapes (what `json.dumps` writes either way).
#[tokio::test]
async fn stored_json_bytes_match_python() {
    let test = TestDatabase::create().await;
    let database = test.database();
    let repository = PlaylistRepository::new(database.clone());
    let mut list = ranked("k4:最佳", 4, "最佳", &["A-001"]);
    list.entries[0].title = "新任 \"quoted\" \u{1F600}".to_owned();
    repository.replace_lists(&[list], now()).await.expect("replace");
    repository
        .record_sync("k4:最佳", now(), &strings(&["A-001", "é"]), &[], None)
        .await
        .expect("sync");
    let row: (String, String, String) =
        sqlx::query_as("SELECT entries_json, present_json, missing_json FROM playlists WHERE key = 'k4:最佳'")
            .fetch_one(test.pool())
            .await
            .expect("row");
    assert_eq!(row.0, "[[1, \"A-001\", \"新任 \\\"quoted\\\" \u{1F600}\"]]");
    assert_eq!(row.1, "[\"A-001\", \"\\u00e9\"]");
    assert_eq!(row.2, "[]");

    let error = repository
        .replace_lists(&[ranked("k9", 1 << 40, "big", &[])], now())
        .await
        .unwrap_err();
    assert_eq!(error.class_name(), "DataError");
    assert_eq!(
        error.to_string(),
        "invalid input for query argument $2: 1099511627776 (value out of int32 range)"
    );

    database.close().await;
    test.drop_database().await;
}
