//! Ported from `tests/test_playlists_api.py`: the playlist endpoints over an in-memory
//! repository and ledger, and a stand-in manual source for filling.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone as _, Utc};
use embyx_clients::jinjier::RankedEntry;
use embyx_core::config::models::{PlaylistsConfig, RssCategory, RssConfig};
use embyx_core::pyjson::{self, Value};
use embyx_monitor::acquisitions::{AcquisitionState, LedgerError};
use embyx_monitor::intake::SourceError;
use embyx_monitor::manual::{ManualEntry, ManualError, ManualOutcome, ManualSubmission, SubmitOptions};
use embyx_monitor::playlists::{PlaylistRecord, PlaylistSourceState};
use embyx_monitor::reports::Raised;
use embyx_monitor::support::{BoxFuture, StoreError};
use http::{Method, StatusCode};
use indexmap::IndexMap;
use tokio::sync::watch;

use super::*;
use crate::monitor_api::ManualIntake;
use crate::routing::{App, Layers};
use crate::testing::call;

/// `NOW` of the playlist sync tests.
fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 20, 12, 0, 0).single().expect("a time")
}

/// `FakeRepository`: the playlist table in memory.
struct FakeRepository {
    records: Mutex<IndexMap<String, PlaylistRecord>>,
    source: Option<PlaylistSourceState>,
}

impl FakeRepository {
    fn new(records: Vec<PlaylistRecord>, source: Option<PlaylistSourceState>) -> Arc<Self> {
        Arc::new(Self {
            records: Mutex::new(records.into_iter().map(|record| (record.key.clone(), record)).collect()),
            source,
        })
    }

    fn record(&self, key: &str) -> PlaylistRecord {
        self.records.lock().expect("records")[key].clone()
    }
}

impl PlaylistCatalog for FakeRepository {
    fn list(&self) -> BoxFuture<'_, Result<Vec<PlaylistRecord>, StoreError>> {
        let mut records: Vec<PlaylistRecord> = self.records.lock().expect("records").values().cloned().collect();
        records.sort_by(|a, b| (a.kind, &a.note, &a.key).cmp(&(b.kind, &b.note, &b.key)));
        Box::pin(std::future::ready(Ok(records)))
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<PlaylistRecord>, StoreError>> {
        let found = self.records.lock().expect("records").get(key).cloned();
        Box::pin(std::future::ready(Ok(found)))
    }

    fn source(&self) -> BoxFuture<'_, Result<Option<PlaylistSourceState>, StoreError>> {
        Box::pin(std::future::ready(Ok(self.source.clone())))
    }

    fn set_enabled<'a>(
        &'a self,
        key: &'a str,
        enabled: bool,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Option<PlaylistRecord>, StoreError>> {
        let mut records = self.records.lock().expect("records");
        let updated = records.get_mut(key).map(|record| {
            record.enabled = enabled;
            record.updated_at = now;
            record.clone()
        });
        Box::pin(std::future::ready(Ok(updated)))
    }
}

/// `record(key, avids, kind=, name=)` of the playlist sync tests.
fn record(key: &str, avids: &[&str], kind: i64, name: &str) -> PlaylistRecord {
    PlaylistRecord {
        key: key.to_owned(),
        kind,
        note: name.to_owned(),
        name: name.to_owned(),
        enabled: true,
        entries: avids
            .iter()
            .enumerate()
            .map(|(index, avid)| RankedEntry {
                rank: index + 1,
                avid: (*avid).to_owned(),
                title: avid.to_lowercase(),
            })
            .collect(),
        present: Vec::new(),
        missing: Vec::new(),
        emby_playlist_id: None,
        last_synced_at: None,
        last_error: None,
        created_at: now(),
        updated_at: now(),
    }
}

fn top250(key: &str, avids: &[&str]) -> PlaylistRecord {
    record(key, avids, 7, "JavDB 有码 TOP250")
}

/// `synced(key, avids, present)`: a record the sync has seen.
fn synced(key: &str, avids: &[&str], present: &[&str]) -> PlaylistRecord {
    PlaylistRecord {
        present: present.iter().map(|avid| (*avid).to_owned()).collect(),
        missing: avids
            .iter()
            .filter(|avid| !present.contains(avid))
            .map(|avid| (*avid).to_owned())
            .collect(),
        emby_playlist_id: Some("pl1".to_owned()),
        last_synced_at: Some(now()),
        ..top250(key, avids)
    }
}

/// `FakeLedger`: `states_for` answering from a table, recording what it was asked.
struct FakeLedger {
    states: BTreeMap<String, AcquisitionState>,
    asked: Mutex<Vec<Vec<String>>>,
}

impl TrackedStates for FakeLedger {
    fn states_for<'a>(
        &'a self,
        avids: &'a [String],
    ) -> BoxFuture<'a, Result<BTreeMap<String, AcquisitionState>, LedgerError>> {
        self.asked.lock().expect("asked").push(avids.to_vec());
        let found = self
            .states
            .iter()
            .filter(|(avid, _)| avids.contains(avid))
            .map(|(avid, state)| (avid.clone(), *state))
            .collect();
        Box::pin(std::future::ready(Ok(found)))
    }
}

fn bare_app(router: ApiRouter) -> App {
    App::new(
        router.into_routes(),
        None,
        Layers {
            max_request_bytes: None,
            api_no_store: false,
        },
    )
}

fn make_client(repository: &Arc<FakeRepository>, ledger: Option<Arc<FakeLedger>>) -> App {
    bare_app(create_playlists_router(
        Arc::clone(repository) as Arc<dyn PlaylistCatalog>,
        MutationAuth::new(None),
        ledger.map(|ledger| ledger as Arc<dyn TrackedStates>),
        None,
    ))
}

fn field<'a>(value: &'a Value, key: &str) -> &'a Value {
    value.get(key).unwrap_or_else(|| panic!("no {key} in {value:?}"))
}

fn json(text: &str) -> Value {
    pyjson::loads(text).expect("JSON")
}

fn items(value: &Value) -> Vec<Value> {
    value.as_list().expect("a list").to_vec()
}

// py: tests/test_playlists_api.py::test_list_reports_counts_and_source
#[tokio::test]
async fn list_reports_counts_and_source() {
    let repository = FakeRepository::new(
        vec![
            synced("k7", &["A-001", "B-002", "C-003"], &["B-002"]),
            record("k2024", &["D-004"], 2024, "2024"),
        ],
        Some(PlaylistSourceState {
            database_name: "20260112".to_owned(),
            fetched_at: now(),
        }),
    );

    let (_, body) = call(
        &make_client(&repository, None),
        Method::GET,
        "/api/playlists",
        &[],
        None,
    )
    .await;

    assert_eq!(
        field(&body, "source"),
        &json(r#"{"database_name": "20260112", "fetched_at": "2026-09-20T12:00:00Z"}"#)
    );
    let counts: Vec<Value> = items(field(&body, "items"))
        .iter()
        .map(|item| {
            Value::List(vec![
                field(item, "key").clone(),
                field(item, "total").clone(),
                field(item, "present").clone(),
                field(item, "missing").clone(),
            ])
        })
        .collect();
    assert_eq!(Value::List(counts), json(r#"[["k7", 3, 1, 2], ["k2024", 1, 0, 0]]"#));
    assert_eq!(
        field(&items(field(&body, "items"))[0], "emby_playlist_id"),
        &Value::from("pl1")
    );
    assert_eq!(field(&items(field(&body, "items"))[1], "last_synced_at"), &Value::Null);
}

// py: tests/test_playlists_api.py::test_missing_lists_the_gap_in_ranking_order_with_ledger_state
#[tokio::test]
async fn missing_lists_the_gap_in_ranking_order_with_ledger_state() {
    let repository = FakeRepository::new(vec![synced("k7", &["A-001", "B-002", "C-003"], &["B-002"])], None);
    let ledger = Arc::new(FakeLedger {
        states: BTreeMap::from([
            ("C-003".to_owned(), AcquisitionState::Downloading),
            ("B-002".to_owned(), AcquisitionState::Archived),
        ]),
        asked: Mutex::default(),
    });

    let (_, body) = call(
        &make_client(&repository, Some(Arc::clone(&ledger))),
        Method::GET,
        "/api/playlists/k7/missing",
        &[],
        None,
    )
    .await;

    assert_eq!(field(&body, "name"), &Value::from("JavDB 有码 TOP250"));
    assert_eq!(
        field(&body, "items"),
        &json(
            r#"[{"rank": 1, "avid": "A-001", "title": "a-001", "tracked": null},
                {"rank": 3, "avid": "C-003", "title": "c-003", "tracked": "downloading"}]"#
        )
    );
    assert_eq!(
        *ledger.asked.lock().expect("asked"),
        vec![vec!["A-001".to_owned(), "C-003".to_owned()]]
    );
}

// py: tests/test_playlists_api.py::test_missing_without_a_ledger_and_unknown_keys
#[tokio::test]
async fn missing_without_a_ledger_and_unknown_keys() {
    let client = make_client(&FakeRepository::new(vec![synced("k7", &["A-001"], &[])], None), None);

    let (_, body) = call(&client, Method::GET, "/api/playlists/k7/missing", &[], None).await;
    assert_eq!(field(&items(field(&body, "items"))[0], "tracked"), &Value::Null);
    assert_eq!(
        call(&client, Method::GET, "/api/playlists/nope/missing", &[], None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &client,
            Method::PATCH,
            "/api/playlists/nope",
            &[],
            Some(r#"{"enabled": false}"#)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

// py: tests/test_playlists_api.py::test_awards_keys_with_spaces_and_cjk_round_trip_through_the_path
#[tokio::test]
async fn awards_keys_with_spaces_and_cjk_round_trip_through_the_path() {
    let key = "k4:第四届JAV金鸡儿奖 最佳故事片 提名";
    let repository = FakeRepository::new(
        vec![record(key, &["A-001"], 4, "第四届JAV金鸡儿奖 最佳故事片 提名")],
        None,
    );
    let client = make_client(&repository, None);
    // What the test client sends: the path percent-encoded.
    let path = format!("/api/playlists/{}", embyx_core::pyurl::quote(key, "/:"));

    let (_, missing) = call(&client, Method::GET, &format!("{path}/missing"), &[], None).await;
    assert_eq!(field(&missing, "key"), &Value::from(key));
    let (_, patched) = call(&client, Method::PATCH, &path, &[], Some(r#"{"enabled": false}"#)).await;
    assert_eq!(field(&patched, "enabled"), &Value::Bool(false));
}

// py: tests/test_playlists_api.py::test_patch_flips_only_the_flag
#[tokio::test]
async fn patch_flips_only_the_flag() {
    let repository = FakeRepository::new(vec![synced("k7", &["A-001"], &["A-001"])], None);
    let client = make_client(&repository, None);

    let (_, body) = call(
        &client,
        Method::PATCH,
        "/api/playlists/k7",
        &[],
        Some(r#"{"enabled": false}"#),
    )
    .await;

    assert_eq!(field(&body, "enabled"), &Value::Bool(false));
    // The sync, not the flag, retires the playlist.
    assert_eq!(field(&body, "emby_playlist_id"), &Value::from("pl1"));
    assert_eq!(
        call(
            &client,
            Method::PATCH,
            "/api/playlists/k7",
            &[],
            Some(r#"{"enabled": true, "name": "x"}"#)
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert!(repository.record("k7").updated_at > now());
}

// -- filling ---------------------------------------------------------------------------------

/// `FakeManual` of the fill tests.
/// One `submit` call: inputs, directory, source, limit and whether it queued.
type SubmitCall = (Vec<String>, String, String, Option<usize>, bool);

#[derive(Default)]
struct FakeManual {
    outcomes: BTreeMap<String, ManualOutcome>,
    /// Raised by the batch itself, after the target was accepted.
    error: bool,
    /// Raised by the up-front target check.
    refuse: bool,
    /// When set, the batch reports its first entry and then waits for it to turn true.
    gate: Option<watch::Receiver<bool>>,
    calls: Mutex<Vec<SubmitCall>>,
}

impl ManualIntake for FakeManual {
    fn browse<'a>(
        &'a self,
        _path: &'a str,
    ) -> BoxFuture<'a, Result<embyx_monitor::manual::DirectoryListing, ManualError>> {
        unreachable!("a fill does not browse")
    }

    fn default_directory(&self) -> BoxFuture<'_, Result<Option<String>, ManualError>> {
        unreachable!("a fill does not ask for the default")
    }

    fn check_target<'a>(&'a self, task_dir_path: &'a str) -> BoxFuture<'a, Result<String, ManualError>> {
        let result = if self.refuse {
            Err(ManualError::DirectoryNotRouted)
        } else {
            Ok(task_dir_path.to_owned())
        };
        Box::pin(std::future::ready(result))
    }

    fn submit<'a>(
        &'a self,
        inputs: &'a [String],
        task_dir_path: &'a str,
        options: SubmitOptions,
    ) -> BoxFuture<'a, Result<ManualSubmission, ManualError>> {
        Box::pin(async move {
            self.calls.lock().expect("calls").push((
                inputs.to_vec(),
                task_dir_path.to_owned(),
                options.source.clone(),
                options.limit,
                options.queue,
            ));
            let mut entries = Vec::new();
            for (index, avid) in inputs.iter().enumerate() {
                if index == 1
                    && let Some(gate) = &self.gate
                {
                    let mut gate = gate.clone();
                    gate.wait_for(|open| *open).await.expect("the gate");
                }
                if self.error {
                    return Err(ManualError::Cloud(SourceError::Raised(Raised::new(
                        "RuntimeError",
                        "boom",
                    ))));
                }
                let entry = ManualEntry {
                    text: avid.clone(),
                    avid: Some(avid.clone()),
                    outcome: self.outcomes.get(avid).copied().unwrap_or(ManualOutcome::Queued),
                    archived_paths: Vec::new(),
                };
                if let Some(on_entry) = &options.on_entry {
                    on_entry(&entry);
                }
                entries.push(entry);
            }
            Ok(ManualSubmission {
                task_dir_path: task_dir_path.to_owned(),
                entries,
            })
        })
    }
}

fn make_fill_client(repository: &Arc<FakeRepository>, manual: &Arc<FakeManual>, task_dir: Option<&str>) -> App {
    let task_dir = task_dir.map(str::to_owned);
    let fill = PlaylistFillApi {
        manual: Arc::clone(manual) as Arc<dyn ManualIntake>,
        task_dir: Arc::new(move || match &task_dir {
            Some(dir) => (Some(dir.clone()), None),
            None => (None, Some(NO_FILL_DIRECTORY.to_owned())),
        }),
    };
    bare_app(create_playlists_router(
        Arc::clone(repository) as Arc<dyn PlaylistCatalog>,
        MutationAuth::new(None),
        None,
        Some(fill),
    ))
}

const RANK: Option<&str> = Some("/115/embyx_in/rank");

/// `finished_fill(client, key)`: the fill's view once its background task has ended.
async fn finished_fill(client: &App, key: &str) -> Value {
    for _ in 0..200 {
        let (_, body) = call(client, Method::GET, &format!("/api/playlists/{key}/fill"), &[], None).await;
        if field(&body, "state") != &Value::from("running") {
            return body;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the fill never finished");
}

// py: tests/test_playlists_api.py::test_resolve_fill_dir_prefers_the_section_then_the_ranking_category
#[test]
fn resolve_fill_dir_prefers_the_section_then_the_ranking_category() {
    let category = |label: &str, dir: &str| RssCategory {
        label: label.to_owned(),
        task_dir_path: dir.to_owned(),
    };
    let rss = RssConfig {
        categories: vec![
            category("Actor", "/115/embyx_in/clt"),
            category("Rank", "/115/embyx_in/rank"),
        ],
        ..RssConfig::default()
    };
    let section = |dir: &str| PlaylistsConfig {
        task_dir_path: dir.to_owned(),
        ..PlaylistsConfig::default()
    };
    assert_eq!(
        resolve_fill_dir(&section("/115/lists"), &rss),
        (Some("/115/lists".to_owned()), None)
    );
    assert_eq!(
        resolve_fill_dir(&PlaylistsConfig::default(), &rss),
        (Some("/115/embyx_in/rank".to_owned()), None)
    );
    let chinese = RssConfig {
        categories: vec![category("榜单", "/115/bang")],
        ..RssConfig::default()
    };
    assert_eq!(
        resolve_fill_dir(&PlaylistsConfig::default(), &chinese),
        (Some("/115/bang".to_owned()), None)
    );
    assert_eq!(
        resolve_fill_dir(&PlaylistsConfig::default(), &RssConfig::default()),
        (None, Some(NO_FILL_DIRECTORY.to_owned()))
    );
}

// py: tests/test_playlists_api.py::test_list_reports_where_a_fill_would_go
#[tokio::test]
async fn list_reports_where_a_fill_would_go() {
    let repository = FakeRepository::new(vec![synced("k7", &["A-001"], &[])], None);
    let manual = Arc::new(FakeManual::default());
    let list = |client: App| async move { call(&client, Method::GET, "/api/playlists", &[], None).await.1 };

    let body = list(make_fill_client(&repository, &manual, RANK)).await;
    assert_eq!(field(&body, "fill_task_dir"), &Value::from("/115/embyx_in/rank"));

    let body = list(make_fill_client(&repository, &manual, None)).await;
    assert_eq!(field(&body, "fill_task_dir"), &Value::Null);
    assert_eq!(field(&body, "fill_reason"), &Value::from(NO_FILL_DIRECTORY));

    let unmounted = list(make_client(&repository, None)).await;
    assert_eq!(
        (field(&unmounted, "fill_task_dir"), field(&unmounted, "fill_reason")),
        (&Value::Null, &Value::from("filling is not available"))
    );
}

// py: tests/test_playlists_api.py::test_fill_queues_the_whole_gap_in_the_background_under_the_playlist_source
#[tokio::test]
async fn fill_queues_the_whole_gap_in_the_background_under_the_playlist_source() {
    let repository = FakeRepository::new(
        vec![synced("k7", &["A-001", "B-002", "C-003", "D-004"], &["B-002"])],
        None,
    );
    let manual = Arc::new(FakeManual {
        outcomes: BTreeMap::from([
            ("C-003".to_owned(), ManualOutcome::AlreadyTracked),
            ("D-004".to_owned(), ManualOutcome::AlreadyInLibrary),
        ]),
        ..FakeManual::default()
    });
    let client = make_fill_client(&repository, &manual, RANK);

    let (status, started) = call(&client, Method::POST, "/api/playlists/k7/fill", &[], None).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(field(&started, "total"), &Value::Int(3));
    let body = finished_fill(&client, "k7").await;
    let (_, listed) = call(&client, Method::GET, "/api/playlists", &[], None).await;

    assert_eq!(
        *manual.calls.lock().expect("calls"),
        vec![(
            vec!["A-001".to_owned(), "C-003".to_owned(), "D-004".to_owned()],
            "/115/embyx_in/rank".to_owned(),
            "playlist:k7".to_owned(),
            None,
            true
        )]
    );
    assert_eq!(field(&body, "state"), &Value::from("done"));
    assert_eq!(field(&body, "task_dir_path"), &Value::from("/115/embyx_in/rank"));
    assert_eq!(
        (field(&body, "done"), field(&body, "total")),
        (&Value::Int(3), &Value::Int(3))
    );
    let handled: Vec<Value> = items(field(&body, "items"))
        .iter()
        .map(|item| Value::List(vec![field(item, "avid").clone(), field(item, "outcome").clone()]))
        .collect();
    assert_eq!(
        Value::List(handled),
        json(r#"[["A-001", "queued"], ["C-003", "already_tracked"], ["D-004", "already_in_library"]]"#)
    );
    assert_eq!(
        field(&body, "counts"),
        &json(r#"{"queued": 1, "already_tracked": 1, "already_in_library": 1}"#)
    );
    assert_ne!(field(&body, "finished_at"), &Value::Null);
    assert_eq!(field(&listed, "fills"), &Value::List(vec![body]));
}

// py: tests/test_playlists_api.py::test_a_running_fill_reports_progress_and_is_not_started_twice
#[tokio::test]
async fn a_running_fill_reports_progress_and_is_not_started_twice() {
    let repository = FakeRepository::new(vec![synced("k7", &["A-001", "B-002", "C-003"], &[])], None);
    let (open, gate) = watch::channel(false);
    let manual = Arc::new(FakeManual {
        gate: Some(gate),
        ..FakeManual::default()
    });
    let client = make_fill_client(&repository, &manual, RANK);

    call(&client, Method::POST, "/api/playlists/k7/fill", &[], None).await;
    let mut progress = Value::Null;
    for _ in 0..200 {
        progress = call(&client, Method::GET, "/api/playlists/k7/fill", &[], None).await.1;
        if field(&progress, "done") != &Value::Int(0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (_, again) = call(&client, Method::POST, "/api/playlists/k7/fill", &[], None).await;
    open.send(true).expect("open the gate");
    let body = finished_fill(&client, "k7").await;

    assert_eq!(
        (
            field(&progress, "state"),
            field(&progress, "done"),
            field(&progress, "total")
        ),
        (&Value::from("running"), &Value::Int(1), &Value::Int(3))
    );
    assert_eq!(field(&progress, "finished_at"), &Value::Null);
    assert_eq!(field(&again, "state"), &Value::from("running"));
    assert_eq!(manual.calls.lock().expect("calls").len(), 1);
    assert_eq!(
        (field(&body, "state"), field(&body, "done")),
        (&Value::from("done"), &Value::Int(3))
    );
}

// py: tests/test_playlists_api.py::test_a_fill_that_breaks_midway_keeps_what_it_queued
#[tokio::test]
async fn a_fill_that_breaks_midway_keeps_what_it_queued() {
    let repository = FakeRepository::new(vec![synced("k7", &["A-001"], &[])], None);
    let manual = Arc::new(FakeManual {
        error: true,
        ..FakeManual::default()
    });
    let client = make_fill_client(&repository, &manual, RANK);

    call(&client, Method::POST, "/api/playlists/k7/fill", &[], None).await;
    let body = finished_fill(&client, "k7").await;

    assert_eq!(
        (field(&body, "state"), field(&body, "error")),
        (&Value::from("failed"), &Value::from("fill_failed"))
    );
}

// py: tests/test_playlists_api.py::test_fill_refusals
#[tokio::test]
async fn fill_refusals() {
    let repository = FakeRepository::new(vec![synced("k7", &["A-001"], &[])], None);
    let fresh = || Arc::new(FakeManual::default());
    let post = |client: App, path: &'static str| async move { call(&client, Method::POST, path, &[], None).await };

    assert_eq!(
        post(make_fill_client(&repository, &fresh(), None), "/api/playlists/k7/fill")
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        post(
            make_fill_client(&repository, &fresh(), RANK),
            "/api/playlists/nope/fill"
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let refusing = Arc::new(FakeManual {
        refuse: true,
        ..FakeManual::default()
    });
    let (status, unrouted) = post(make_fill_client(&repository, &refusing, RANK), "/api/playlists/k7/fill").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(unrouted, json(r#"{"error": {"code": "directory_not_routed"}}"#));
    let client = make_fill_client(&repository, &fresh(), RANK);
    assert_eq!(
        call(&client, Method::GET, "/api/playlists/k7/fill", &[], None).await.0,
        StatusCode::NOT_FOUND
    );
    // Without the fill dependency the route does not exist at all.
    let status = post(make_client(&repository, None), "/api/playlists/k7/fill").await.0;
    assert!(matches!(status, StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED));
}

// py: tests/test_playlists_api.py::test_fill_of_a_complete_list_submits_nothing
#[tokio::test]
async fn fill_of_a_complete_list_submits_nothing() {
    let repository = FakeRepository::new(vec![synced("k7", &["A-001"], &["A-001"])], None);
    let manual = Arc::new(FakeManual::default());
    let client = make_fill_client(&repository, &manual, RANK);

    call(&client, Method::POST, "/api/playlists/k7/fill", &[], None).await;
    let body = finished_fill(&client, "k7").await;

    assert!(manual.calls.lock().expect("calls")[0].0.is_empty());
    assert_eq!(field(&body, "items"), &Value::List(Vec::new()));
    assert_eq!(field(&body, "counts"), &json("{}"));
    assert_eq!(field(&body, "state"), &Value::from("done"));
}
