//! Ported from `tests/test_monitor_playlist_sync.py`: source refresh, order-preserving sync,
//! retirement, failure isolation, against the Python tests' in-memory fakes.

use std::sync::Mutex;

use chrono::TimeZone as _;
use embyx_clients::jinjier::RankedEntry;
use embyx_clients::jinjier::RankedList;
use embyx_clients::jinjier::fixtures::{ROWS, make_database};
use embyx_core::avid::AvidParser;
use indexmap::IndexMap;

use super::*;
use crate::playlists::PlaylistSourceState;
use crate::support::fixed_clock;

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 20, 12, 0, 0).single().expect("valid")
}

/// `FakeRepository`: the playlist table in memory, with the same write surface as the real one.
#[derive(Default)]
struct FakeRepository {
    records: Mutex<IndexMap<String, PlaylistRecord>>,
    stored_source: Mutex<Option<PlaylistSourceState>>,
    replaced: Mutex<Vec<Vec<String>>>,
}

impl FakeRepository {
    fn new(records: Vec<PlaylistRecord>, source: Option<PlaylistSourceState>) -> Arc<Self> {
        Arc::new(Self {
            records: Mutex::new(records.into_iter().map(|record| (record.key.clone(), record)).collect()),
            stored_source: Mutex::new(source),
            replaced: Mutex::default(),
        })
    }

    fn record(&self, key: &str) -> PlaylistRecord {
        self.records.lock().expect("records")[key].clone()
    }

    fn keys(&self) -> HashSet<String> {
        self.records.lock().expect("records").keys().cloned().collect()
    }

    fn source_state(&self) -> Option<PlaylistSourceState> {
        self.stored_source.lock().expect("source").clone()
    }

    fn update(&self, key: &str, change: impl FnOnce(&mut PlaylistRecord)) {
        let mut records = self.records.lock().expect("records");
        change(records.get_mut(key).expect("a stored record"));
    }
}

impl PlaylistStore for FakeRepository {
    fn list(&self) -> BoxFuture<'_, Result<Vec<PlaylistRecord>, StoreError>> {
        let mut records: Vec<PlaylistRecord> = self.records.lock().expect("records").values().cloned().collect();
        records.sort_by(|a, b| (a.kind, &a.note, &a.key).cmp(&(b.kind, &b.note, &b.key)));
        Box::pin(async move { Ok(records) })
    }

    fn source(&self) -> BoxFuture<'_, Result<Option<PlaylistSourceState>, StoreError>> {
        let source = self.source_state();
        Box::pin(async move { Ok(source) })
    }

    fn record_source<'a>(
        &'a self,
        database_name: &'a str,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        *self.stored_source.lock().expect("source") = Some(PlaylistSourceState {
            database_name: database_name.to_owned(),
            fetched_at: now,
        });
        Box::pin(async { Ok(()) })
    }

    fn replace_lists<'a>(
        &'a self,
        lists: &'a [RankedList],
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.replaced
            .lock()
            .expect("replaced")
            .push(lists.iter().map(|ranked| ranked.key.clone()).collect());
        let mut records = self.records.lock().expect("records");
        for ranked in lists {
            let current = records.get(&ranked.key).cloned();
            records.insert(
                ranked.key.clone(),
                PlaylistRecord {
                    key: ranked.key.clone(),
                    kind: ranked.kind,
                    note: ranked.note.clone(),
                    name: ranked.name().to_owned(),
                    enabled: current.as_ref().is_none_or(|current| current.enabled),
                    entries: ranked.entries.clone(),
                    present: current
                        .as_ref()
                        .map(|current| current.present.clone())
                        .unwrap_or_default(),
                    missing: current
                        .as_ref()
                        .map(|current| current.missing.clone())
                        .unwrap_or_default(),
                    emby_playlist_id: current.as_ref().and_then(|current| current.emby_playlist_id.clone()),
                    last_synced_at: current.as_ref().and_then(|current| current.last_synced_at),
                    last_error: None,
                    created_at: current.as_ref().map_or(now, |current| current.created_at),
                    updated_at: now,
                },
            );
        }
        Box::pin(async { Ok(()) })
    }

    fn record_sync<'a>(
        &'a self,
        key: &'a str,
        now: DateTime<Utc>,
        present: &'a [String],
        missing: &'a [String],
        emby_playlist_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.update(key, |record| {
            record.present = present.to_vec();
            record.missing = missing.to_vec();
            record.emby_playlist_id = emby_playlist_id.map(str::to_owned);
            record.last_synced_at = Some(now);
            record.last_error = None;
            record.updated_at = now;
        });
        Box::pin(async { Ok(()) })
    }

    fn record_error<'a>(
        &'a self,
        key: &'a str,
        now: DateTime<Utc>,
        error: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.update(key, |record| {
            record.last_error = Some(error.to_owned());
            record.updated_at = now;
        });
        Box::pin(async { Ok(()) })
    }
}

/// `FakeSource`.
struct FakeSource {
    name: Option<String>,
    database: Vec<u8>,
    downloads: Mutex<usize>,
    broken_download: bool,
}

impl FakeSource {
    fn new(name: Option<&str>) -> Arc<Self> {
        Arc::new(Self::build(name))
    }

    fn build(name: Option<&str>) -> Self {
        Self {
            name: name.map(str::to_owned),
            database: make_database(ROWS),
            downloads: Mutex::new(0),
            broken_download: false,
        }
    }

    fn downloads(&self) -> usize {
        *self.downloads.lock().expect("downloads")
    }
}

impl RankingSource for FakeSource {
    fn discover_database_name(&self) -> BoxFuture<'_, Result<String, JinjierError>> {
        let name = self.name.clone().ok_or_else(|| JinjierError("page changed".to_owned()));
        Box::pin(async move { name })
    }

    fn download_database<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<u8>, JinjierError>> {
        *self.downloads.lock().expect("downloads") += 1;
        let result = if self.broken_download {
            Err(JinjierError(format!("{name}: HTTP 503")))
        } else {
            Ok(self.database.clone())
        };
        Box::pin(async move { result })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    Create(String, Vec<String>),
    Add(String, Vec<String>),
    Remove(String, Vec<String>),
    Delete(String),
}

#[derive(Default)]
struct FakeEmbyState {
    playlists: IndexMap<String, Vec<(String, String)>>,
    names: HashMap<String, String>,
    calls: Vec<Call>,
    next: usize,
}

impl FakeEmbyState {
    fn entry(&mut self) -> String {
        self.next += 1;
        format!("e{}", self.next)
    }
}

/// `FakeEmby`: the movie index plus playlists that keep insertion order, like the server.
struct FakeEmby {
    index: Vec<(String, String)>,
    fail_on: HashSet<String>,
    state: Mutex<FakeEmbyState>,
}

impl FakeEmby {
    fn new(index: &[(&str, &str)]) -> Self {
        Self {
            index: index
                .iter()
                .map(|(avid, item)| ((*avid).to_owned(), (*item).to_owned()))
                .collect(),
            fail_on: HashSet::new(),
            state: Mutex::default(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FakeEmbyState> {
        self.state.lock().expect("emby")
    }

    fn create(&self, name: &str, item_ids: &[String]) -> Result<String, EmbyError> {
        if self.fail_on.contains(name) {
            return Err(EmbyError::Server(format!("POST /Playlists: HTTP 500 for {name}")));
        }
        let mut state = self.lock();
        state.next += 1;
        let playlist_id = format!("pl{}", state.next);
        state.names.insert(playlist_id.clone(), name.to_owned());
        let entries = item_ids.iter().map(|item| (state.entry(), item.clone())).collect();
        state.playlists.insert(playlist_id.clone(), entries);
        state.calls.push(Call::Create(name.to_owned(), item_ids.to_vec()));
        Ok(playlist_id)
    }

    fn items(&self, playlist_id: &str) -> Vec<String> {
        self.lock().playlists[playlist_id]
            .iter()
            .map(|(_, item)| item.clone())
            .collect()
    }

    fn name(&self, playlist_id: &str) -> String {
        self.lock().names[playlist_id].clone()
    }

    fn calls(&self) -> Vec<Call> {
        self.lock().calls.clone()
    }

    fn clear_calls(&self) {
        self.lock().calls.clear();
    }

    fn playlist_count(&self) -> usize {
        self.lock().playlists.len()
    }
}

impl PlaylistServer for FakeEmby {
    fn movie_index<'a>(
        &'a self,
        _avid_of: &'a (dyn Fn(&str) -> String + Sync),
    ) -> BoxFuture<'a, Result<Vec<(String, String)>, EmbyError>> {
        let index = self.index.clone();
        Box::pin(async move { Ok(index) })
    }

    fn list_playlists(&self) -> BoxFuture<'_, Result<Vec<EmbyPlaylist>, EmbyError>> {
        let state = self.lock();
        let playlists = state
            .playlists
            .keys()
            .map(|playlist_id| EmbyPlaylist {
                playlist_id: playlist_id.clone(),
                name: state.names[playlist_id].clone(),
            })
            .collect();
        Box::pin(async move { Ok(playlists) })
    }

    fn create_playlist<'a>(
        &'a self,
        name: &'a str,
        item_ids: &'a [String],
    ) -> BoxFuture<'a, Result<String, EmbyError>> {
        let result = self.create(name, item_ids);
        Box::pin(async move { result })
    }

    fn playlist_entries<'a>(
        &'a self,
        playlist_id: &'a str,
    ) -> BoxFuture<'a, Result<Vec<EmbyPlaylistEntry>, EmbyError>> {
        let entries = self.lock().playlists[playlist_id]
            .iter()
            .map(|(entry_id, item_id)| EmbyPlaylistEntry {
                entry_id: entry_id.clone(),
                item_id: item_id.clone(),
            })
            .collect();
        Box::pin(async move { Ok(entries) })
    }

    fn add_entries<'a>(&'a self, playlist_id: &'a str, item_ids: &'a [String]) -> BoxFuture<'a, Result<(), EmbyError>> {
        let mut state = self.lock();
        let added: Vec<(String, String)> = item_ids.iter().map(|item| (state.entry(), item.clone())).collect();
        state.playlists[playlist_id].extend(added);
        state.calls.push(Call::Add(playlist_id.to_owned(), item_ids.to_vec()));
        Box::pin(async { Ok(()) })
    }

    fn remove_entries<'a>(
        &'a self,
        playlist_id: &'a str,
        entry_ids: &'a [String],
    ) -> BoxFuture<'a, Result<(), EmbyError>> {
        let mut state = self.lock();
        state.playlists[playlist_id].retain(|(entry, _)| !entry_ids.contains(entry));
        state
            .calls
            .push(Call::Remove(playlist_id.to_owned(), entry_ids.to_vec()));
        Box::pin(async { Ok(()) })
    }

    fn delete_playlist<'a>(&'a self, playlist_id: &'a str) -> BoxFuture<'a, Result<(), EmbyError>> {
        let mut state = self.lock();
        state.playlists.shift_remove(playlist_id);
        state.names.remove(playlist_id);
        state.calls.push(Call::Delete(playlist_id.to_owned()));
        Box::pin(async { Ok(()) })
    }
}

/// `record(key, avids, enabled=True, emby_playlist_id=None, kind=7, name='JavDB 有码 TOP250')`.
fn record(key: &str, avids: &[&str], enabled: bool, emby_playlist_id: Option<&str>) -> PlaylistRecord {
    record_named(key, avids, enabled, emby_playlist_id, 7, "JavDB 有码 TOP250")
}

fn record_named(
    key: &str,
    avids: &[&str],
    enabled: bool,
    emby_playlist_id: Option<&str>,
    kind: i64,
    name: &str,
) -> PlaylistRecord {
    PlaylistRecord {
        key: key.to_owned(),
        kind,
        note: name.to_owned(),
        name: name.to_owned(),
        enabled,
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
        emby_playlist_id: emby_playlist_id.map(str::to_owned),
        last_synced_at: None,
        last_error: None,
        created_at: now(),
        updated_at: now(),
    }
}

fn source_state(name: &str) -> PlaylistSourceState {
    PlaylistSourceState {
        database_name: name.to_owned(),
        fetched_at: now(),
    }
}

fn make_pipeline(
    repository: &Arc<FakeRepository>,
    source: &Arc<FakeSource>,
    emby: &Arc<FakeEmby>,
) -> PlaylistSyncPipeline {
    let parser = AvidParser::default();
    PlaylistSyncPipeline::new(
        Arc::clone(repository) as Arc<dyn PlaylistStore>,
        Arc::clone(source) as Arc<dyn RankingSource>,
        Arc::clone(emby) as Arc<dyn PlaylistServer>,
        Arc::new(move |title: &str| parser.get_avid(title)),
    )
    .with_clock(fixed_clock(now()))
}

fn ctx() -> RunContext {
    RunContext::new("test-playlists")
}

fn stat(run: &RunContext, counter: &str) -> Option<i64> {
    run.stats().get(counter)
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

// -- source refresh -------------------------------------------------------------

// py: tests/test_monitor_playlist_sync.py::test_first_run_downloads_and_stores_the_lists_then_creates_playlists
#[tokio::test]
async fn first_run_downloads_and_stores_the_lists_then_creates_playlists() {
    let repository = FakeRepository::new(Vec::new(), None);
    let source = FakeSource::new(Some("20260112"));
    let emby = Arc::new(FakeEmby::new(&[
        ("SSNI-497", "10"),
        ("ABP-984", "30"),
        ("MIAB-317", "40"),
        ("MOON-018", "50"),
    ]));
    let run = ctx();

    make_pipeline(&repository, &source, &emby).run(&run).await.expect("run");

    assert_eq!(source.downloads(), 1);
    assert_eq!(repository.source_state(), Some(source_state("20260112")));
    assert_eq!(
        repository.keys(),
        [
            "k5",
            "k7",
            "k2024",
            "k4:第四届JAV金鸡儿奖 最佳故事片 提名",
            "k4:第四届JAV金鸡儿奖 最佳故事片 获奖",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    );
    let top = repository.record("k7");
    // IPX-811 sits between the two present titles in the ranking; the playlist keeps their order.
    assert_eq!(top.present, ["SSNI-497", "ABP-984"]);
    assert_eq!(top.missing, ["IPX-811"]);
    let top_id = top.emby_playlist_id.clone().expect("a playlist");
    assert_eq!(emby.items(&top_id), ["10", "30"]);
    assert_eq!(emby.name(&top_id), "JavDB 有码 TOP250");
    assert_eq!(top.last_synced_at, Some(now()));
    // JavLibrary's two titles are both absent: counted, but no empty playlist.
    assert_eq!(repository.record("k5").missing, ["START-257", "SHKD-724"]);
    assert_eq!(repository.record("k5").emby_playlist_id, None);
    assert_eq!(stat(&run, "source_updated"), Some(1));
    assert_eq!(stat(&run, "lists_created"), Some(3));
    assert_eq!(stat(&run, "lists_synced"), Some(5));
    assert_eq!(stat(&run, "missing_total"), Some(1 + 2 + 2 + 1 + 1));
}

// py: tests/test_monitor_playlist_sync.py::test_same_database_name_skips_the_download
#[tokio::test]
async fn same_database_name_skips_the_download() {
    let repository = FakeRepository::new(
        vec![record("k7", &["SSNI-497"], true, None)],
        Some(source_state("20260112")),
    );
    let source = FakeSource::new(Some("20260112"));
    let emby = Arc::new(FakeEmby::new(&[("SSNI-497", "10")]));
    let run = ctx();

    make_pipeline(&repository, &source, &emby).run(&run).await.expect("run");

    assert_eq!(source.downloads(), 0);
    assert_eq!(stat(&run, "source_updated"), None);
    let playlist_id = repository.record("k7").emby_playlist_id.expect("a playlist");
    assert_eq!(emby.items(&playlist_id), ["10"]);
}

// py: tests/test_monitor_playlist_sync.py::test_new_database_name_replaces_entries_but_keeps_the_operators_flag
#[tokio::test]
async fn new_database_name_replaces_entries_but_keeps_the_operators_flag() {
    let old = record("k7", &["OLD-001"], false, None);
    let repository = FakeRepository::new(vec![old], Some(source_state("20250101")));
    let source = FakeSource::new(Some("20260112"));
    let emby = Arc::new(FakeEmby::new(&[("SSNI-497", "10")]));

    make_pipeline(&repository, &source, &emby)
        .run(&ctx())
        .await
        .expect("run");

    assert_eq!(source.downloads(), 1);
    let k7 = repository.record("k7");
    assert!(!k7.enabled);
    assert_eq!(
        k7.entries.iter().map(|entry| entry.avid.as_str()).collect::<Vec<_>>(),
        ["SSNI-497", "IPX-811", "ABP-984"]
    );
    // Disabled: statistics kept current, no playlist made.
    assert_eq!(k7.present, ["SSNI-497"]);
    assert_eq!(emby.playlist_count(), 0);
    assert_eq!(repository.replaced.lock().expect("replaced").len(), 1);
}

// py: tests/test_monitor_playlist_sync.py::test_source_outage_keeps_syncing_the_stored_lists
#[tokio::test]
async fn source_outage_keeps_syncing_the_stored_lists() {
    let repository = FakeRepository::new(
        vec![record("k7", &["SSNI-497"], true, None)],
        Some(source_state("20260112")),
    );
    let emby = Arc::new(FakeEmby::new(&[("SSNI-497", "10")]));
    let run = ctx();

    make_pipeline(&repository, &FakeSource::new(None), &emby)
        .run(&run)
        .await
        .expect("run");

    assert!(
        run.log_tail()
            .iter()
            .any(|line| line.contains("ranking source unavailable"))
    );
    assert!(run.errors().is_empty());
    let playlist_id = repository.record("k7").emby_playlist_id.expect("a playlist");
    assert_eq!(emby.items(&playlist_id), ["10"]);

    let mut broken = FakeSource::build(Some("20270101"));
    broken.broken_download = true;
    let run = ctx();
    make_pipeline(&repository, &Arc::new(broken), &emby)
        .run(&run)
        .await
        .expect("run");
    assert!(run.log_tail().iter().any(|line| line.contains("could not be read")));
    assert_eq!(repository.source_state().expect("stored").database_name, "20260112");
    assert_eq!(stat(&run, "lists_unchanged"), Some(1));
}

// py: tests/test_monitor_playlist_sync.py::test_nothing_stored_and_no_source_is_a_failed_run
#[tokio::test]
async fn nothing_stored_and_no_source_is_a_failed_run() {
    let error = make_pipeline(
        &FakeRepository::new(Vec::new(), None),
        &FakeSource::new(None),
        &Arc::new(FakeEmby::new(&[])),
    )
    .run(&ctx())
    .await
    .unwrap_err();
    assert_eq!(error.class_name(), "RuntimeError");
    assert!(error.to_string().contains("no ranking lists are stored"));
}

// -- syncing one list --------------------------------------------------------------

// py: tests/test_monitor_playlist_sync.py::test_playlist_in_the_right_order_is_left_alone
#[tokio::test]
async fn playlist_in_the_right_order_is_left_alone() {
    let emby = Arc::new(FakeEmby::new(&[("A-001", "1"), ("B-002", "2")]));
    let playlist_id = emby.create("JavDB 有码 TOP250", &strings(&["1", "2"])).expect("create");
    emby.clear_calls();
    let repository = FakeRepository::new(
        vec![record("k7", &["A-001", "B-002"], true, Some(&playlist_id))],
        Some(source_state("x")),
    );
    let run = ctx();

    make_pipeline(&repository, &FakeSource::new(Some("x")), &emby)
        .run(&run)
        .await
        .expect("run");

    assert_eq!(emby.calls(), []);
    assert_eq!(stat(&run, "lists_unchanged"), Some(1));
}

// py: tests/test_monitor_playlist_sync.py::test_changed_order_or_membership_rebuilds_the_playlist_in_one_pass
#[tokio::test]
async fn changed_order_or_membership_rebuilds_the_playlist_in_one_pass() {
    let emby = Arc::new(FakeEmby::new(&[("A-001", "1"), ("B-002", "2"), ("C-003", "3")]));
    let playlist_id = emby.create("JavDB 有码 TOP250", &strings(&["2", "1"])).expect("create");
    let old_entries: Vec<String> = emby.lock().playlists[&playlist_id]
        .iter()
        .map(|(entry, _)| entry.clone())
        .collect();
    emby.clear_calls();
    let repository = FakeRepository::new(
        vec![record("k7", &["A-001", "B-002", "C-003"], true, Some(&playlist_id))],
        Some(source_state("x")),
    );
    let run = ctx();

    make_pipeline(&repository, &FakeSource::new(Some("x")), &emby)
        .run(&run)
        .await
        .expect("run");

    assert_eq!(
        emby.calls(),
        [
            Call::Remove(playlist_id.clone(), old_entries),
            Call::Add(playlist_id.clone(), strings(&["1", "2", "3"])),
        ]
    );
    assert_eq!(emby.items(&playlist_id), ["1", "2", "3"]);
    assert_eq!(stat(&run, "lists_rebuilt"), Some(1));
}

// py: tests/test_monitor_playlist_sync.py::test_playlist_deleted_on_the_server_is_recreated
#[tokio::test]
async fn playlist_deleted_on_the_server_is_recreated() {
    let emby = Arc::new(FakeEmby::new(&[("A-001", "1")]));
    let repository = FakeRepository::new(
        vec![record("k7", &["A-001"], true, Some("gone"))],
        Some(source_state("x")),
    );
    let run = ctx();

    make_pipeline(&repository, &FakeSource::new(Some("x")), &emby)
        .run(&run)
        .await
        .expect("run");

    let new_id = repository.record("k7").emby_playlist_id.expect("a playlist");
    assert_ne!(new_id, "gone");
    assert_eq!(emby.items(&new_id), ["1"]);
    assert_eq!(stat(&run, "lists_created"), Some(1));
}

// py: tests/test_monitor_playlist_sync.py::test_disabled_list_has_its_playlist_removed_and_gap_still_counted
#[tokio::test]
async fn disabled_list_has_its_playlist_removed_and_gap_still_counted() {
    let emby = Arc::new(FakeEmby::new(&[("A-001", "1")]));
    let playlist_id = emby.create("JavDB 有码 TOP250", &strings(&["1"])).expect("create");
    let repository = FakeRepository::new(
        vec![record("k7", &["A-001", "B-002"], false, Some(&playlist_id))],
        Some(source_state("x")),
    );
    let run = ctx();

    make_pipeline(&repository, &FakeSource::new(Some("x")), &emby)
        .run(&run)
        .await
        .expect("run");

    assert_eq!(emby.playlist_count(), 0);
    let k7 = repository.record("k7");
    assert_eq!(k7.emby_playlist_id, None);
    assert_eq!(k7.present, ["A-001"]);
    assert_eq!(k7.missing, ["B-002"]);
    assert_eq!(stat(&run, "lists_removed"), Some(1));
    assert_eq!(stat(&run, "lists_synced"), None);

    // Already gone (or never made): nothing to delete, no error.
    let run = ctx();
    make_pipeline(&repository, &FakeSource::new(Some("x")), &emby)
        .run(&run)
        .await
        .expect("run");
    assert_eq!(stat(&run, "lists_removed"), None);
}

// py: tests/test_monitor_playlist_sync.py::test_one_list_failing_on_emby_is_recorded_and_the_rest_continue
#[tokio::test]
async fn one_list_failing_on_emby_is_recorded_and_the_rest_continue() {
    let mut fake = FakeEmby::new(&[("A-001", "1"), ("B-002", "2")]);
    fake.fail_on.insert("Broken".to_owned());
    let emby = Arc::new(fake);
    let repository = FakeRepository::new(
        vec![
            record_named("k7", &["A-001"], true, None, 7, "Broken"),
            record_named("k2024", &["B-002"], true, None, 2024, "JavDB 2024 TOP250"),
        ],
        Some(source_state("x")),
    );
    let run = ctx();

    make_pipeline(&repository, &FakeSource::new(Some("x")), &emby)
        .run(&run)
        .await
        .expect("run");

    let k7 = repository.record("k7");
    assert_eq!(k7.last_error.as_deref(), Some("POST /Playlists: HTTP 500 for Broken"));
    assert_eq!(k7.emby_playlist_id, None);
    let playlist_id = repository.record("k2024").emby_playlist_id.expect("a playlist");
    assert_eq!(emby.items(&playlist_id), ["2"]);
    assert_eq!(
        run.stats().iter().collect::<Vec<_>>(),
        [
            ("lists", 2),
            ("library_titles", 2),
            ("lists_failed", 1),
            ("lists_created", 1),
            ("lists_synced", 1),
            ("missing_total", 0),
        ]
    );
    assert_eq!(run.errors().len(), 1);
}

/// An error out of one list that is not an `EmbyError` (a malformed answer the client lets
/// through) ends the run, as Python's `except EmbyError` lets it propagate.
#[tokio::test]
async fn an_error_that_is_not_an_emby_error_ends_the_run() {
    struct Malformed(FakeEmby);
    impl PlaylistServer for Malformed {
        fn movie_index<'a>(
            &'a self,
            avid_of: &'a (dyn Fn(&str) -> String + Sync),
        ) -> BoxFuture<'a, Result<Vec<(String, String)>, EmbyError>> {
            self.0.movie_index(avid_of)
        }
        fn list_playlists(&self) -> BoxFuture<'_, Result<Vec<EmbyPlaylist>, EmbyError>> {
            self.0.list_playlists()
        }
        fn create_playlist<'a>(&'a self, _: &'a str, _: &'a [String]) -> BoxFuture<'a, Result<String, EmbyError>> {
            Box::pin(async {
                Err(EmbyError::Raised(embyx_clients::pyvalue::Raised {
                    class: "TypeError",
                    message: "string indices must be integers, not 'str'".to_owned(),
                }))
            })
        }
        fn playlist_entries<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Vec<EmbyPlaylistEntry>, EmbyError>> {
            self.0.playlist_entries(id)
        }
        fn add_entries<'a>(&'a self, id: &'a str, items: &'a [String]) -> BoxFuture<'a, Result<(), EmbyError>> {
            self.0.add_entries(id, items)
        }
        fn remove_entries<'a>(&'a self, id: &'a str, entries: &'a [String]) -> BoxFuture<'a, Result<(), EmbyError>> {
            self.0.remove_entries(id, entries)
        }
        fn delete_playlist<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), EmbyError>> {
            self.0.delete_playlist(id)
        }
    }
    let repository = FakeRepository::new(
        vec![
            record_named("k7", &["A-001"], true, None, 7, "First"),
            record_named("k2024", &["A-001"], true, None, 2024, "Second"),
        ],
        Some(source_state("x")),
    );
    let emby: Arc<dyn PlaylistServer> = Arc::new(Malformed(FakeEmby::new(&[("A-001", "1")])));
    let parser = AvidParser::default();
    let pipeline = PlaylistSyncPipeline::new(
        Arc::clone(&repository) as Arc<dyn PlaylistStore>,
        FakeSource::new(Some("x")) as Arc<dyn RankingSource>,
        emby,
        Arc::new(move |title: &str| parser.get_avid(title)),
    )
    .with_clock(fixed_clock(now()));
    let run = ctx();

    let error = pipeline.run(&run).await.unwrap_err();

    assert_eq!(error.class_name(), "TypeError");
    assert_eq!(repository.record("k7").last_error, None);
    assert_eq!(repository.record("k2024").last_synced_at, None);
    assert_eq!(stat(&run, "lists_failed"), None);
}
