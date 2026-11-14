//! Ported from `tests/test_monitor_rss.py`: the poller against the fake ledger, the fake
//! subscription registry, scripted feeds, magnet sources and CloudDrive.

use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
use std::sync::Mutex;

use chrono::{TimeDelta, Utc};
use embyx_clients::clouddrive::client::code_by_name;
use embyx_clients::clouddrive::proto::FileOperationResult;
use embyx_clients::clouddrive::{CloudDriveError, RpcError};
use embyx_clients::http::HttpxError;
use embyx_clients::javbus::JavBusMagnet;
use embyx_clients::net::StatusError;
use embyx_clients::net::retry::RecordingSleeper;
use embyx_core::config::models::ArchiveConfig;

use super::*;
use crate::acquisitions::AttemptState;
use crate::intake::SourceError;
use crate::testing::{FakeLedger, FakeSubscriptions, SubscriptionSpec, feed_url, make_subscription};

const HASH_A: &str = "C12FE1C06BBA254A9DC9F519B335AA7C1367A88A";
const HASH_B: &str = "D23FE1C06BBA254A9DC9F519B335AA7C1367A88B";
const HASH_C: &str = "E34FE1C06BBA254A9DC9F519B335AA7C1367A88C";
const TASK_DIR: &str = "/115/task";
const RANK_DIR: &str = "/115/embyx_in/rank";

fn magnet_a() -> String {
    format!("magnet:?xt=urn:btih:{HASH_A}&dn=ABC-123")
}

fn magnet_b() -> String {
    format!("magnet:?xt=urn:btih:{HASH_B}&dn=ABC-123")
}

fn make_ctx() -> RunContext {
    RunContext::new("test-rss")
}

fn category(label: &str, task_dir: &str) -> RssCategory {
    RssCategory {
        label: label.to_owned(),
        task_dir_path: task_dir.to_owned(),
    }
}

fn actor_then_rank() -> Vec<RssCategory> {
    vec![category("Actor", TASK_DIR), category("Rank", RANK_DIR)]
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

// -- feeds ----------------------------------------------------------------------

/// `make_item(item_id, title, magnet_html='', link=None)`.
#[derive(Debug, Clone)]
struct Item {
    id: String,
    title: String,
    content: String,
    link: Option<String>,
}

fn item(id: &str, title: &str) -> Item {
    Item {
        id: id.to_owned(),
        title: title.to_owned(),
        content: String::new(),
        link: None,
    }
}

fn item_with(id: &str, title: &str, content: &str) -> Item {
    Item {
        content: content.to_owned(),
        ..item(id, title)
    }
}

/// `xml.sax.saxutils.escape`.
fn escape(text: &str) -> String {
    text.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

/// `feed_xml(items)`: an RSS 2.0 body holding the given items, guid first so the key is the id.
fn feed_xml(items: &[Item]) -> Vec<u8> {
    let mut entries = String::new();
    for item in items {
        entries.push_str("<item>");
        write!(entries, "<guid>{}</guid>", escape(&item.id)).expect("a String write");
        write!(entries, "<title>{}</title>", escape(&item.title)).expect("a String write");
        if let Some(link) = item.link.as_deref().filter(|link| !link.is_empty()) {
            write!(entries, "<link>{}</link>", escape(link)).expect("a String write");
        }
        if !item.content.is_empty() {
            write!(entries, "<description><![CDATA[{}]]></description>", item.content).expect("a String write");
        }
        entries.push_str("</item>");
    }
    format!(r#"<?xml version="1.0"?><rss version="2.0"><channel><title>test</title>{entries}</channel></rss>"#)
        .into_bytes()
}

/// `magnet_table(info_hash)`.
fn magnet_table(info_hash: &str) -> String {
    format!(
        r#"
    <table><tbody><tr>
      <td><a href="magnet:?xt=urn:btih:{info_hash}&dn=x">x</a></td>
      <td>2 GiB</td>
    </tr></tbody></table>
    "#
    )
}

fn runtime_error(message: &str) -> Raised {
    Raised::new("RuntimeError", message)
}

/// `not_found(url)`: `httpx.HTTPStatusError('404 Not Found', ...)` for a GET of `url`.
fn not_found(url: &str) -> FetchError {
    FetchError::Feed(FeedFetchError::Http(HttpxError::Status(StatusError {
        status: 404,
        reason: "Not Found".to_owned(),
        url: url.to_owned(),
        location: None,
    })))
}

type FetchQueue = VecDeque<Result<Vec<u8>, FetchError>>;
type FetchFn = Box<dyn Fn(&str) -> Result<Vec<u8>, FetchError> + Send + Sync>;

/// `deps.fetch`: an `AsyncMock` answering from a function or a list of side effects.
struct FakeFetch {
    answer: FetchFn,
    queued: Mutex<Option<FetchQueue>>,
    calls: Mutex<Vec<String>>,
}

impl FakeFetch {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("calls").clone()
    }
}

impl FeedFetcher for FakeFetch {
    fn fetch<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<Vec<u8>, FetchError>> {
        self.calls.lock().expect("calls").push(url.to_owned());
        let result = match self.queued.lock().expect("queued").as_mut() {
            Some(queue) => queue
                .pop_front()
                .unwrap_or_else(|| Err(FetchError::Raised(Raised::new("StopAsyncIteration", "")))),
            None => (self.answer)(url),
        };
        Box::pin(std::future::ready(result))
    }
}

// -- magnet sources and CloudDrive -------------------------------------------------

struct FakeSukebei {
    answers: HashMap<String, String>,
    calls: Mutex<Vec<String>>,
}

impl SukebeiMagnets for FakeSukebei {
    fn get_magnet<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Option<String>, SourceError>> {
        self.calls.lock().expect("calls").push(avid.to_owned());
        Box::pin(std::future::ready(Ok(self.answers.get(avid).cloned())))
    }
}

struct FakeJavBus {
    answers: HashMap<String, Vec<JavBusMagnet>>,
}

impl JavBusMagnets for FakeJavBus {
    fn get_magnets<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Vec<JavBusMagnet>, SourceError>> {
        Box::pin(std::future::ready(Ok(self
            .answers
            .get(avid)
            .cloned()
            .unwrap_or_default())))
    }
}

fn javbus_magnet(magnet: &str, size_int: i128) -> JavBusMagnet {
    JavBusMagnet {
        magnet: magnet.to_owned(),
        size: String::new(),
        size_int,
        tags: Vec::new(),
        score: 0,
    }
}

struct FakeCloud {
    queued: Mutex<VecDeque<Result<FileOperationResult, SourceError>>>,
    fallback: Result<FileOperationResult, SourceError>,
    calls: Mutex<Vec<(Vec<String>, String)>>,
}

impl FakeCloud {
    fn calls(&self) -> Vec<(Vec<String>, String)> {
        self.calls.lock().expect("calls").clone()
    }
}

impl OfflineFiles for FakeCloud {
    fn add_offline_files<'a>(
        &'a self,
        urls: &'a [String],
        dst_dir: &'a str,
    ) -> BoxFuture<'a, Result<FileOperationResult, SourceError>> {
        self.calls
            .lock()
            .expect("calls")
            .push((urls.to_vec(), dst_dir.to_owned()));
        let next = self.queued.lock().expect("queued").pop_front();
        Box::pin(std::future::ready(next.unwrap_or_else(|| self.fallback.clone())))
    }
}

fn success() -> FileOperationResult {
    FileOperationResult {
        success: true,
        ..FileOperationResult::default()
    }
}

// -- catalog lookups ------------------------------------------------------------------

/// `cast_lookup_stub(casts)` (or an `AsyncMock` raising `error`).
struct StubCast {
    casts: HashMap<String, CreditedWork>,
    error: Option<Raised>,
    calls: Mutex<Vec<String>>,
}

impl StubCast {
    fn new(casts: &[(&str, CreditedWork)]) -> Arc<Self> {
        Arc::new(Self {
            casts: casts
                .iter()
                .map(|(avid, cast)| ((*avid).to_owned(), cast.clone()))
                .collect(),
            error: None,
            calls: Mutex::default(),
        })
    }

    fn failing(error: Raised) -> Arc<Self> {
        Arc::new(Self {
            casts: HashMap::new(),
            error: Some(error),
            calls: Mutex::default(),
        })
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("calls").clone()
    }
}

impl CastLookup for StubCast {
    fn lookup<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Option<CreditedWork>, LookupError>> {
        self.calls.lock().expect("calls").push(avid.to_owned());
        let result = match &self.error {
            Some(error) => Err(Box::new(error.clone()) as LookupError),
            None => Ok(self.casts.get(avid).cloned()),
        };
        Box::pin(std::future::ready(result))
    }
}

fn credited(talents: &[i128]) -> CreditedWork {
    CreditedWork {
        talent_ids: talents.iter().copied().collect(),
        release_date: None,
    }
}

/// The `talent_lookup` of `moved_talent_pipeline`: a fixed answer, or an exception.
struct StubTalent {
    listed: Result<Option<AvbaseTalent>, Raised>,
    calls: Mutex<Vec<String>>,
}

impl TalentLookup for StubTalent {
    fn lookup<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Option<AvbaseTalent>, LookupError>> {
        self.calls.lock().expect("calls").push(name.to_owned());
        let result = self.listed.clone().map_err(|error| Box::new(error) as LookupError);
        Box::pin(std::future::ready(result))
    }
}

fn talent(talent_id: i128, name: &str, aliases: &[&str], total_works: i128) -> AvbaseTalent {
    AvbaseTalent {
        talent_id,
        name: name.to_owned(),
        aliases: strings(aliases),
        total_works,
    }
}

// -- the pipeline -----------------------------------------------------------------------

/// `make_pipeline(...)`'s keyword arguments.
#[derive(Default)]
struct Setup {
    items: Vec<Item>,
    items_by_label: Option<Vec<(&'static str, Vec<Item>)>>,
    categories: Option<Vec<RssCategory>>,
    subscriptions: Option<FakeSubscriptions>,
    sukebei: Vec<(&'static str, String)>,
    javbus: Vec<(&'static str, Vec<JavBusMagnet>)>,
    ledger: Option<FakeLedger>,
    add_sequence: Vec<Result<FileOperationResult, SourceError>>,
    add_error: Option<SourceError>,
    fetch_sequence: Option<Vec<Result<Vec<u8>, FetchError>>>,
    fetch_fn: Option<FetchFn>,
    archive_config: Option<ArchiveConfig>,
    cast_lookup: Option<Arc<StubCast>>,
    talent_lookup: Option<Arc<StubTalent>>,
}

/// `deps`: the fakes the pipeline was built from.
struct Deps {
    ledger: Arc<FakeLedger>,
    subscriptions: Arc<FakeSubscriptions>,
    fetch: Arc<FakeFetch>,
    sukebei: Arc<FakeSukebei>,
    cloud: Arc<FakeCloud>,
}

impl Deps {
    fn state(&self, avid: &str) -> AcquisitionState {
        self.ledger.state().states[avid]
    }

    fn source(&self, avid: &str) -> String {
        self.ledger.state().sources[avid].clone()
    }

    fn task_dir(&self, avid: &str) -> Option<String> {
        self.ledger.state().task_dirs[avid].clone()
    }

    fn has_state(&self, avid: &str) -> bool {
        self.ledger.state().states.contains_key(avid)
    }

    fn no_states(&self) -> bool {
        self.ledger.state().states.is_empty()
    }
}

fn make_pipeline(setup: Setup) -> (RssPipeline, Deps) {
    let categories = setup.categories.unwrap_or_else(|| vec![category("Actor", TASK_DIR)]);
    // One subscription per label, numbered in label order, each serving its feed. A bare item
    // list belongs to whichever category is configured.
    let feeds: Vec<(String, Vec<Item>)> = match setup.items_by_label {
        Some(by_label) => by_label
            .into_iter()
            .map(|(label, items)| (label.to_owned(), items))
            .collect(),
        None => vec![(
            categories
                .first()
                .map_or_else(|| "Actor".to_owned(), |first| first.label.clone()),
            setup.items,
        )],
    };
    let subscriptions = Arc::new(setup.subscriptions.unwrap_or_else(|| {
        FakeSubscriptions::new(
            feeds
                .iter()
                .enumerate()
                .map(|(index, (label, _))| {
                    make_subscription(
                        i64::try_from(index + 1).expect("small"),
                        SubscriptionSpec {
                            category: label.clone(),
                            ..SubscriptionSpec::default()
                        },
                    )
                })
                .collect(),
        )
    }));
    let bodies: HashMap<String, Vec<u8>> = feeds
        .iter()
        .map(|(label, items)| (feed_url(label), feed_xml(items)))
        .collect();
    let fetch = Arc::new(FakeFetch {
        answer: setup.fetch_fn.unwrap_or_else(|| {
            Box::new(move |url: &str| {
                bodies
                    .get(url)
                    .cloned()
                    .ok_or_else(|| FetchError::Raised(Raised::new("KeyError", format!("'{url}'"))))
            })
        }),
        queued: Mutex::new(setup.fetch_sequence.map(VecDeque::from)),
        calls: Mutex::default(),
    });
    let sukebei = Arc::new(FakeSukebei {
        answers: setup
            .sukebei
            .into_iter()
            .map(|(avid, magnet)| (avid.to_owned(), magnet))
            .collect(),
        calls: Mutex::default(),
    });
    let javbus = Arc::new(FakeJavBus {
        answers: setup
            .javbus
            .into_iter()
            .map(|(avid, magnets)| (avid.to_owned(), magnets))
            .collect(),
    });
    let cloud = Arc::new(FakeCloud {
        queued: Mutex::new(setup.add_sequence.into()),
        fallback: setup.add_error.map_or_else(|| Ok(success()), Err),
        calls: Mutex::default(),
    });
    let ledger = Arc::new(setup.ledger.unwrap_or_default());
    let config = RssConfig {
        enabled: true,
        categories,
        ..RssConfig::default()
    };
    let pipeline = RssPipeline::new(RssParts {
        config,
        avid_parser: AvidParser::default(),
        subscriptions: Arc::clone(&subscriptions) as Arc<dyn SubscriptionStore>,
        fetch: Arc::clone(&fetch) as Arc<dyn FeedFetcher>,
        cloud: Arc::clone(&cloud) as Arc<dyn OfflineFiles>,
        sukebei: Arc::clone(&sukebei) as Arc<dyn SukebeiMagnets>,
        javbus: javbus as Arc<dyn JavBusMagnets>,
        ledger: Arc::clone(&ledger) as Arc<dyn crate::acquisitions::Ledger>,
        // An unconfigured archive has no routes, so the library check passes everything
        // through without touching the file system.
        archiver: ArchivePipeline::new(setup.archive_config.unwrap_or_default(), AvidParser::default()),
        on_submitted: None,
        cast_lookup: setup.cast_lookup.map(|lookup| lookup as Arc<dyn CastLookup>),
        talent_lookup: setup.talent_lookup.map(|lookup| lookup as Arc<dyn TalentLookup>),
    })
    .expect("a valid cooldown")
    .with_sleeper(RecordingSleeper::new().sleeper());
    (
        pipeline,
        Deps {
            ledger,
            subscriptions,
            fetch,
            sukebei,
            cloud,
        },
    )
}

fn stat(ctx: &RunContext, counter: &str) -> Option<i64> {
    ctx.stats().get(counter)
}

fn submitted(magnet: String, dir: &str) -> Vec<(Vec<String>, String)> {
    vec![(vec![magnet], dir.to_owned())]
}

// py: tests/test_monitor_rss.py::test_discovery_records_the_avid_and_submits_the_first_magnet
#[tokio::test]
async fn discovery_records_the_avid_and_submits_the_first_magnet() {
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123 release")],
        sukebei: vec![("ABC-123", magnet_a())],
        ..Setup::default()
    });

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    assert_eq!(deps.cloud.calls(), submitted(magnet_a(), "/115/task"));
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Downloading);
    assert_eq!(deps.ledger.attempt_states("ABC-123"), [AttemptState::Submitted]);
    assert_eq!(stat(&ctx, "magnets_added"), Some(1));
}

// py: tests/test_monitor_rss.py::test_items_enter_the_cursor_as_soon_as_they_are_seen
#[tokio::test]
async fn items_enter_the_cursor_as_soon_as_they_are_seen() {
    // The cursor is not a retry queue: an item is remembered whatever became of its AVID, and
    // the ledger's schedule drives the retry.
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123"), item("item-2", "ABC-123")],
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    let record = deps.subscriptions.record(1);
    assert_eq!(record.cursor, ["item-1", "item-2"]);
    assert_eq!(record.last_error, None);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::ResolveFailed);
}

// py: tests/test_monitor_rss.py::test_every_source_contributes_candidates_deduplicated_by_hash
#[tokio::test]
async fn every_source_contributes_candidates_deduplicated_by_hash() {
    let html = magnet_table(HASH_B);
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item_with("item-1", "ABC-123", &html)],
        sukebei: vec![("ABC-123", magnet_a())],
        javbus: vec![(
            "ABC-123",
            vec![
                // The same torrent sukebei returned, advertised in base32.
                javbus_magnet("magnet:?xt=urn:btih:YEX6DQDLXISUVHOJ6UM3GNNKPQJWPKEK", 9),
                javbus_magnet(&format!("magnet:?xt=urn:btih:{HASH_C}"), 5),
            ],
        )],
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    // The item's magnet comes back re-labelled with the AVID as its display name.
    assert_eq!(
        deps.ledger.magnets("ABC-123"),
        [magnet_a(), magnet_b(), format!("magnet:?xt=urn:btih:{HASH_C}")]
    );
}

// py: tests/test_monitor_rss.py::test_candidates_are_capped
#[tokio::test]
async fn candidates_are_capped() {
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123")],
        javbus: vec![(
            "ABC-123",
            (1..12)
                .map(|index: i128| javbus_magnet(&format!("magnet:?xt=urn:btih:{index:040X}"), index))
                .collect(),
        )],
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    assert_eq!(deps.ledger.magnets("ABC-123").len(), 5);
}

// py: tests/test_monitor_rss.py::test_magnets_without_a_usable_hash_are_not_recorded
#[tokio::test]
async fn magnets_without_a_usable_hash_are_not_recorded() {
    // CloudDrive reports tasks by info hash; one we cannot compute is untrackable.
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123")],
        sukebei: vec![("ABC-123", "magnet:?dn=ABC-123".to_owned())],
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    assert!(deps.ledger.magnets("ABC-123").is_empty());
    assert_eq!(deps.state("ABC-123"), AcquisitionState::ResolveFailed);
}

// py: tests/test_monitor_rss.py::test_unresolvable_avid_gets_a_cooldown_instead_of_a_retry_loop
#[tokio::test]
async fn unresolvable_avid_gets_a_cooldown_instead_of_a_retry_loop() {
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123")],
        ..Setup::default()
    });

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    assert_eq!(deps.state("ABC-123"), AcquisitionState::ResolveFailed);
    assert!(deps.ledger.state().next_action_at["ABC-123"].is_some());
    assert_eq!(stat(&ctx, "magnets_failed"), Some(1));
}

// py: tests/test_monitor_rss.py::test_avids_the_ledger_already_owns_are_skipped cases=3
#[tokio::test]
async fn avids_the_ledger_already_owns_are_skipped() {
    for state in [
        AcquisitionState::Downloading,
        AcquisitionState::Archived,
        AcquisitionState::Ignored,
    ] {
        let (pipeline, deps) = make_pipeline(Setup {
            items: vec![item("item-1", "ABC-123")],
            sukebei: vec![("ABC-123", magnet_a())],
            ledger: Some(FakeLedger::with_known(&[("ABC-123", state)])),
            ..Setup::default()
        });

        let ctx = make_ctx();
        pipeline.run(&ctx).await.expect("run");

        assert!(deps.cloud.calls().is_empty(), "{state:?}");
        assert!(deps.sukebei.calls.lock().expect("calls").is_empty(), "{state:?}");
        assert_eq!(stat(&ctx, "skipped_known"), Some(1), "{state:?}");
        // The item still enters the cursor: re-reading it would only re-derive the same AVID.
        assert_eq!(deps.subscriptions.record(1).cursor, ["item-1"], "{state:?}");
    }
}

// py: tests/test_monitor_rss.py::test_a_failed_submission_falls_through_to_the_next_candidate
#[tokio::test]
async fn a_failed_submission_falls_through_to_the_next_candidate() {
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123")],
        sukebei: vec![("ABC-123", magnet_a())],
        javbus: vec![("ABC-123", vec![javbus_magnet(&magnet_b(), 1)])],
        add_sequence: vec![Err(runtime_error("offline queue rejected it").into()), Ok(success())],
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    assert_eq!(
        deps.ledger.attempt_states("ABC-123"),
        [AttemptState::Error, AttemptState::Submitted]
    );
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Downloading);
}

// py: tests/test_monitor_rss.py::test_all_candidates_failing_leaves_the_avid_undownloaded
#[tokio::test]
async fn all_candidates_failing_leaves_the_avid_undownloaded() {
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123")],
        sukebei: vec![("ABC-123", magnet_a())],
        add_error: Some(runtime_error("offline queue rejected it").into()),
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    assert_eq!(deps.ledger.attempt_states("ABC-123"), [AttemptState::Error]);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Discovered);
}

// py: tests/test_monitor_rss.py::test_duplicate_offline_task_counts_as_submitted
#[tokio::test]
async fn duplicate_offline_task_counts_as_submitted() {
    let error: SourceError = CloudDriveError::Rpc(RpcError {
        code: code_by_name("UNKNOWN").expect("a code"),
        details: "任务已存在".to_owned(),
        debug: "UNKNOWN:任务已存在".to_owned(),
        streaming: false,
    })
    .into();
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123")],
        sukebei: vec![("ABC-123", magnet_a())],
        add_error: Some(error),
        ..Setup::default()
    });

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    // CloudDrive is already working on this hash, which is what we track.
    assert_eq!(deps.ledger.attempt_states("ABC-123"), [AttemptState::Submitted]);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Downloading);
    assert_eq!(stat(&ctx, "duplicates"), Some(1));
}

// py: tests/test_monitor_rss.py::test_empty_feed_does_nothing
#[tokio::test]
async fn empty_feed_does_nothing() {
    let (pipeline, deps) = make_pipeline(Setup::default());

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    assert!(deps.cloud.calls().is_empty());
    assert_eq!(stat(&ctx, "items"), Some(0));
    assert_eq!(deps.subscriptions.record(1).last_error, None);
}

// py: tests/test_monitor_rss.py::test_unparseable_titles_are_reported_once_and_not_re_read
#[tokio::test]
async fn unparseable_titles_are_reported_once_and_not_re_read() {
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "!!!")],
        ..Setup::default()
    });

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    assert_eq!(deps.subscriptions.record(1).cursor, ["item-1"]);
    assert_eq!(stat(&ctx, "unique_avids"), Some(0));
    assert!(ctx.log_tail().iter().any(|line| line.contains("Failed to get avid")));
}

// py: tests/test_monitor_rss.py::test_each_category_is_recorded_under_its_own_source
#[tokio::test]
async fn each_category_is_recorded_under_its_own_source() {
    let (pipeline, deps) = make_pipeline(Setup {
        items_by_label: Some(vec![
            ("Actor", vec![item("item-1", "ABC-123")]),
            ("Rank", vec![item("item-2", "DEF-456")]),
        ]),
        categories: Some(vec![category("Actor", TASK_DIR), category("Rank", TASK_DIR)]),
        sukebei: vec![("ABC-123", magnet_a()), ("DEF-456", magnet_b())],
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    let sources: Vec<(String, String)> = deps.ledger.state().sources.clone().into_iter().collect();
    assert_eq!(
        sources,
        [
            ("ABC-123".to_owned(), "rss:Actor".to_owned()),
            ("DEF-456".to_owned(), "rss:Rank".to_owned())
        ]
    );
    assert_eq!(deps.fetch.calls(), [feed_url("Actor"), feed_url("Rank")]);
}

// py: tests/test_monitor_rss.py::test_a_category_downloads_into_its_own_directory
#[tokio::test]
async fn a_category_downloads_into_its_own_directory() {
    let (pipeline, deps) = make_pipeline(Setup {
        items_by_label: Some(vec![
            ("Actor", vec![item("item-1", "ABC-123")]),
            ("Rank", vec![item("item-2", "DEF-456")]),
        ]),
        categories: Some(vec![category("Actor", TASK_DIR), category("Rank", RANK_DIR)]),
        sukebei: vec![("ABC-123", magnet_a()), ("DEF-456", magnet_b())],
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    let submitted: BTreeMap<String, String> = deps
        .cloud
        .calls()
        .into_iter()
        .map(|(urls, dir)| (urls[0].clone(), dir))
        .collect();
    assert_eq!(
        submitted,
        BTreeMap::from([(magnet_a(), TASK_DIR.to_owned()), (magnet_b(), RANK_DIR.to_owned())])
    );
    // Each AVID carries its category's directory, so a later retry follows it.
    let task_dirs: Vec<(String, Option<String>)> = deps.ledger.state().task_dirs.clone().into_iter().collect();
    assert_eq!(
        task_dirs,
        [
            ("ABC-123".to_owned(), Some(TASK_DIR.to_owned())),
            ("DEF-456".to_owned(), Some(RANK_DIR.to_owned()))
        ]
    );
}

/// `FakeLedger(known={avid: RESOLVE_FAILED})` owned by `source` in `task_dir`, due `next`.
fn parked(avid: &str, source: &str, task_dir: Option<&str>, next: Option<DateTime<Utc>>) -> FakeLedger {
    let ledger = FakeLedger::with_known(&[(avid, AcquisitionState::ResolveFailed)]);
    {
        let mut state = ledger.state();
        state.sources.insert(avid.to_owned(), source.to_owned());
        if let Some(task_dir) = task_dir {
            state.task_dirs.insert(avid.to_owned(), Some(task_dir.to_owned()));
        }
        state.next_action_at.insert(avid.to_owned(), next);
    }
    ledger
}

// py: tests/test_monitor_rss.py::test_a_chart_sighting_files_a_tracked_actors_work_under_the_actor
#[tokio::test]
async fn a_chart_sighting_files_a_tracked_actors_work_under_the_actor() {
    // Categories rank in configuration order: the actor's row keeps its directory, the
    // chart's magnet still helps.
    let ledger = parked(
        "ABC-123",
        "rss:Actor",
        Some(TASK_DIR),
        Some(Utc::now() + TimeDelta::days(1)),
    );
    let (pipeline, deps) = make_pipeline(Setup {
        items_by_label: Some(vec![
            ("Actor", vec![]),
            ("Rank", vec![item_with("item-1", "ABC-123", &magnet_table(HASH_B))]),
        ]),
        categories: Some(vec![category("Actor", TASK_DIR), category("Rank", RANK_DIR)]),
        ledger: Some(ledger),
        ..Setup::default()
    });

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    assert_eq!(deps.cloud.calls(), submitted(magnet_b(), TASK_DIR));
    assert_eq!(deps.task_dir("ABC-123").as_deref(), Some(TASK_DIR));
    assert_eq!(deps.source("ABC-123"), "rss:Actor");
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Downloading);
    assert_eq!(stat(&ctx, "filed_by_owner"), Some(1));
}

// py: tests/test_monitor_rss.py::test_an_actor_sighting_takes_a_charts_row_into_the_actors_directory
#[tokio::test]
async fn an_actor_sighting_takes_a_charts_row_into_the_actors_directory() {
    let ledger = parked("ABC-123", "rss:Rank", Some(RANK_DIR), None);
    let (pipeline, deps) = make_pipeline(Setup {
        items_by_label: Some(vec![("Actor", vec![item("item-1", "ABC-123")]), ("Rank", vec![])]),
        categories: Some(vec![category("Actor", TASK_DIR), category("Rank", RANK_DIR)]),
        ledger: Some(ledger),
        sukebei: vec![("ABC-123", magnet_a())],
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    assert_eq!(deps.cloud.calls(), submitted(magnet_a(), TASK_DIR));
    assert_eq!(deps.task_dir("ABC-123").as_deref(), Some(TASK_DIR));
    assert_eq!(deps.source("ABC-123"), "rss:Actor");
}

/// `actor_and_rank_subscriptions(talent_id=45338, category='Actor', enabled=True)`.
fn actor_and_rank_subscriptions(talent_id: i32, category: &str, enabled: bool) -> FakeSubscriptions {
    FakeSubscriptions::new(vec![
        make_subscription(
            1,
            SubscriptionSpec {
                category: category.to_owned(),
                name: Some("香水じゅん".to_owned()),
                talent_id: Some(talent_id),
                enabled,
                ..SubscriptionSpec::default()
            },
        ),
        make_subscription(
            2,
            SubscriptionSpec {
                category: "Rank".to_owned(),
                url: Some(feed_url("Rank")),
                ..SubscriptionSpec::default()
            },
        ),
    ])
}

fn default_subscriptions() -> FakeSubscriptions {
    actor_and_rank_subscriptions(45338, "Actor", true)
}

// py: tests/test_monitor_rss.py::test_a_chart_files_a_subscribed_talents_unseen_work_under_the_talent
#[tokio::test]
async fn a_chart_files_a_subscribed_talents_unseen_work_under_the_talent() {
    // The talent's feed listed the work before its subscription was seeded, so only the chart
    // sights it.
    let release = NaiveDate::from_ymd_opt(2026, 9, 18).expect("valid");
    let lookup = StubCast::new(&[(
        "HMN-911",
        CreditedWork {
            talent_ids: BTreeSet::from([45338]),
            release_date: Some(release),
        },
    )]);
    let (pipeline, deps) = make_pipeline(Setup {
        items_by_label: Some(vec![("Rank", vec![item("item-1", "HMN-911")])]),
        categories: Some(actor_then_rank()),
        subscriptions: Some(default_subscriptions()),
        sukebei: vec![("HMN-911", magnet_a())],
        cast_lookup: Some(lookup),
        ..Setup::default()
    });

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    assert_eq!(deps.cloud.calls(), submitted(magnet_a(), TASK_DIR));
    assert_eq!(deps.source("HMN-911"), "rss:Actor");
    assert_eq!(deps.task_dir("HMN-911").as_deref(), Some(TASK_DIR));
    assert_eq!(deps.ledger.state().release_dates["HMN-911"], Some(release));
    assert_eq!(stat(&ctx, "filed_by_cast"), Some(1));
}

// py: tests/test_monitor_rss.py::test_a_chart_takes_over_its_own_cooled_row_when_the_cast_is_subscribed
#[tokio::test]
async fn a_chart_takes_over_its_own_cooled_row_when_the_cast_is_subscribed() {
    let ledger = parked("HMN-911", "rss:Rank", Some(RANK_DIR), None);
    let (pipeline, deps) = make_pipeline(Setup {
        items_by_label: Some(vec![("Rank", vec![item("item-1", "HMN-911")])]),
        categories: Some(actor_then_rank()),
        subscriptions: Some(default_subscriptions()),
        ledger: Some(ledger),
        sukebei: vec![("HMN-911", magnet_a())],
        cast_lookup: Some(StubCast::new(&[("HMN-911", credited(&[45338]))])),
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    assert_eq!(deps.cloud.calls(), submitted(magnet_a(), TASK_DIR));
    assert_eq!(deps.source("HMN-911"), "rss:Actor");
    assert_eq!(deps.task_dir("HMN-911").as_deref(), Some(TASK_DIR));
}

// py: tests/test_monitor_rss.py::test_a_chart_keeps_a_work_nobody_subscribed_is_credited_on cases=3
#[tokio::test]
async fn a_chart_keeps_a_work_nobody_subscribed_is_credited_on() {
    let cases: [(&str, Vec<(&str, CreditedWork)>); 3] = [
        ("not-in-catalog", vec![]),
        ("nobody-subscribed", vec![("HMN-911", credited(&[99]))]),
        ("no-cast", vec![("HMN-911", credited(&[]))]),
    ];
    for (id, casts) in cases {
        let (pipeline, deps) = make_pipeline(Setup {
            items_by_label: Some(vec![("Rank", vec![item("item-1", "HMN-911")])]),
            categories: Some(actor_then_rank()),
            subscriptions: Some(default_subscriptions()),
            sukebei: vec![("HMN-911", magnet_a())],
            cast_lookup: Some(StubCast::new(&casts)),
            ..Setup::default()
        });

        let ctx = make_ctx();
        pipeline.run(&ctx).await.expect("run");

        assert_eq!(deps.cloud.calls(), submitted(magnet_a(), RANK_DIR), "{id}");
        assert_eq!(deps.source("HMN-911"), "rss:Rank", "{id}");
        assert_eq!(stat(&ctx, "filed_by_cast"), None, "{id}");
    }
}

// py: tests/test_monitor_rss.py::test_only_an_enabled_subscription_in_a_category_ahead_claims_a_work cases=2
#[tokio::test]
async fn only_an_enabled_subscription_in_a_category_ahead_claims_a_work() {
    for (id, subscriptions) in [
        ("disabled", actor_and_rank_subscriptions(45338, "Actor", false)),
        ("same-category", actor_and_rank_subscriptions(45338, "Rank", true)),
    ] {
        let (pipeline, deps) = make_pipeline(Setup {
            items_by_label: Some(vec![("Rank", vec![item("item-1", "HMN-911")])]),
            categories: Some(actor_then_rank()),
            subscriptions: Some(subscriptions),
            sukebei: vec![("HMN-911", magnet_a())],
            cast_lookup: Some(StubCast::new(&[("HMN-911", credited(&[45338]))])),
            ..Setup::default()
        });

        pipeline.run(&make_ctx()).await.expect("run");

        assert_eq!(deps.cloud.calls(), submitted(magnet_a(), RANK_DIR), "{id}");
        assert_eq!(deps.source("HMN-911"), "rss:Rank", "{id}");
    }
}

// py: tests/test_monitor_rss.py::test_an_unreadable_catalog_files_the_chart_sighting_as_itself
#[tokio::test]
async fn an_unreadable_catalog_files_the_chart_sighting_as_itself() {
    let (pipeline, deps) = make_pipeline(Setup {
        items_by_label: Some(vec![("Rank", vec![item("item-1", "HMN-911")])]),
        categories: Some(actor_then_rank()),
        subscriptions: Some(default_subscriptions()),
        sukebei: vec![("HMN-911", magnet_a())],
        cast_lookup: Some(StubCast::failing(runtime_error("avbase down"))),
        ..Setup::default()
    });

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    assert_eq!(deps.cloud.calls(), submitted(magnet_a(), RANK_DIR));
    assert_eq!(deps.source("HMN-911"), "rss:Rank");
    assert_eq!(deps.state("HMN-911"), AcquisitionState::Downloading);
    assert!(
        ctx.errors()
            .contains(&"ERROR: Failed to look up the cast of HMN-911: RuntimeError('avbase down')".to_owned())
    );
}

// py: tests/test_monitor_rss.py::test_the_catalog_is_not_asked_about_works_the_ledger_already_settled
#[tokio::test]
async fn the_catalog_is_not_asked_about_works_the_ledger_already_settled() {
    // A row past discovery, or one a category ahead owns, needs no lookup: the sighting cannot
    // move it.
    let ledger = FakeLedger::with_known(&[
        ("ABC-123", AcquisitionState::Downloading),
        ("DEF-456", AcquisitionState::ResolveFailed),
    ]);
    {
        let mut state = ledger.state();
        state.sources.insert("ABC-123".to_owned(), "rss:Rank".to_owned());
        state.sources.insert("DEF-456".to_owned(), "rss:Actor".to_owned());
        state.task_dirs.insert("DEF-456".to_owned(), Some(TASK_DIR.to_owned()));
        state.next_action_at.insert("DEF-456".to_owned(), None);
    }
    let lookup = StubCast::new(&[]);
    let (pipeline, deps) = make_pipeline(Setup {
        items_by_label: Some(vec![
            ("Actor", vec![item("item-0", "GHI-789")]),
            ("Rank", vec![item("item-1", "ABC-123"), item("item-2", "DEF-456")]),
        ]),
        categories: Some(actor_then_rank()),
        ledger: Some(ledger),
        sukebei: vec![("DEF-456", magnet_b()), ("GHI-789", magnet_a())],
        cast_lookup: Some(Arc::clone(&lookup)),
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    // The first category has nobody to yield to, so its own sightings never consult the
    // catalog either.
    assert!(lookup.calls().is_empty());
    assert_eq!(deps.task_dir("DEF-456").as_deref(), Some(TASK_DIR));
}

// py: tests/test_monitor_rss.py::test_the_catalog_is_asked_once_per_work_per_run
#[tokio::test]
async fn the_catalog_is_asked_once_per_work_per_run() {
    let lookup = StubCast::new(&[("HMN-911", credited(&[45338]))]);
    let subscriptions = FakeSubscriptions::new(vec![
        make_subscription(
            1,
            SubscriptionSpec {
                name: Some("香水じゅん".to_owned()),
                talent_id: Some(45338),
                ..SubscriptionSpec::default()
            },
        ),
        make_subscription(
            2,
            SubscriptionSpec {
                category: "Rank".to_owned(),
                url: Some(feed_url("Rank")),
                name: Some("most wanted".to_owned()),
                ..SubscriptionSpec::default()
            },
        ),
        make_subscription(
            3,
            SubscriptionSpec {
                category: "Rank".to_owned(),
                url: Some(feed_url("Rank2")),
                name: Some("best rated".to_owned()),
                ..SubscriptionSpec::default()
            },
        ),
    ]);
    let (pipeline, deps) = make_pipeline(Setup {
        items_by_label: Some(vec![
            ("Rank", vec![item("item-1", "HMN-911")]),
            ("Rank2", vec![item("item-2", "HMN-911")]),
        ]),
        categories: Some(actor_then_rank()),
        subscriptions: Some(subscriptions),
        sukebei: vec![("HMN-911", magnet_a())],
        cast_lookup: Some(Arc::clone(&lookup)),
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    assert_eq!(lookup.calls(), ["HMN-911"]);
    assert_eq!(deps.cloud.calls(), submitted(magnet_a(), TASK_DIR));
}

// py: tests/test_monitor_rss.py::test_no_categories_ingests_nothing
#[tokio::test]
async fn no_categories_ingests_nothing() {
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123")],
        categories: Some(Vec::new()),
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    assert!(deps.fetch.calls().is_empty());
    assert!(deps.no_states());
    // The subscription is reported rather than guessed a directory for.
    assert_eq!(
        deps.subscriptions.record(1).last_error.as_deref(),
        Some("category Actor is not configured")
    );
}

// py: tests/test_monitor_rss.py::test_one_failing_category_does_not_cost_the_others_their_pass
#[tokio::test]
async fn one_failing_category_does_not_cost_the_others_their_pass() {
    let (pipeline, deps) = make_pipeline(Setup {
        items_by_label: Some(vec![("Actor", vec![]), ("Rank", vec![])]),
        fetch_sequence: Some(vec![
            Err(FetchError::Raised(runtime_error("rsshub is down"))),
            Ok(feed_xml(&[item("item-2", "DEF-456")])),
        ]),
        categories: Some(vec![category("Actor", TASK_DIR), category("Rank", TASK_DIR)]),
        sukebei: vec![("DEF-456", magnet_b())],
        ..Setup::default()
    });

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    assert_eq!(deps.state("DEF-456"), AcquisitionState::Downloading);
    assert_eq!(stat(&ctx, "subscriptions_failed"), Some(1));
    assert_eq!(
        deps.subscriptions.record(1).last_error.as_deref(),
        Some("RuntimeError: rsshub is down")
    );
    assert_eq!(deps.subscriptions.record(2).last_error, None);
}

// py: tests/test_monitor_rss.py::test_stats_cover_every_category_in_the_run
#[tokio::test]
async fn stats_cover_every_category_in_the_run() {
    // Counters accumulate across categories; a later one must not overwrite the tally of the
    // one before it.
    let (pipeline, _deps) = make_pipeline(Setup {
        items_by_label: Some(vec![
            ("Actor", vec![item("item-1", "ABC-123")]),
            ("Rank", vec![item("item-2", "DEF-456"), item("item-3", "GHI-789")]),
        ]),
        categories: Some(vec![category("Actor", TASK_DIR), category("Rank", TASK_DIR)]),
        sukebei: vec![("ABC-123", magnet_a()), ("DEF-456", magnet_b())],
        ..Setup::default()
    });

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    assert_eq!(stat(&ctx, "items"), Some(3));
    assert_eq!(stat(&ctx, "unique_avids"), Some(3));
    assert_eq!(stat(&ctx, "magnets_found"), Some(2));
    assert_eq!(stat(&ctx, "magnets_added"), Some(2));
}

// -- the library check ---------------------------------------------------------

/// `library_config(tmp_path)`: routes mirroring production, a priority actor inbox and a rank
/// inbox.
fn library_config(root: &std::path::Path) -> ArchiveConfig {
    ArchiveConfig {
        src_dir: crate::fs::path_text(&root.join("mnt").join("115").join("embyx_in")),
        dst_dir: crate::fs::path_text(&root.join("library")),
        mapping: vec![("rank".to_owned(), "rank".to_owned())],
        priority_mapping: vec![("clt".to_owned(), "actor/clt".to_owned())],
        ..ArchiveConfig::default()
    }
}

fn write_library_copy(root: &std::path::Path, relative: &str) -> std::path::PathBuf {
    let path = root.join("library").join(relative);
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("parents");
    std::fs::write(&path, b"v").expect("write");
    path
}

// py: tests/test_monitor_rss.py::test_an_avid_the_library_holds_is_settled_without_a_download
#[tokio::test]
async fn an_avid_the_library_holds_is_settled_without_a_download() {
    let tmp = tempfile::tempdir().expect("a temporary directory");
    let existing = write_library_copy(tmp.path(), "actor/clt/ABC/ABC-123.mp4");
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123 release")],
        categories: Some(vec![category("Rank", "/115/embyx_in/rank")]),
        sukebei: vec![("ABC-123", magnet_a())],
        archive_config: Some(library_config(tmp.path())),
        ..Setup::default()
    });

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    assert!(deps.cloud.calls().is_empty());
    assert!(deps.sukebei.calls.lock().expect("calls").is_empty());
    assert_eq!(deps.subscriptions.record(1).cursor, ["item-1"]);
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Archived);
    assert_eq!(
        deps.ledger.state().notes["ABC-123"].as_deref(),
        Some("already in library")
    );
    assert_eq!(
        deps.ledger.state().archived_paths["ABC-123"],
        ["actor/clt/ABC/ABC-123.mp4"]
    );
    // A normal category leaves the priority copy where it is.
    assert!(existing.exists());
    assert_eq!(stat(&ctx, "already_in_library"), Some(1));
}

// py: tests/test_monitor_rss.py::test_a_priority_category_pulls_the_copy_out_of_a_normal_route
#[tokio::test(flavor = "multi_thread")]
async fn a_priority_category_pulls_the_copy_out_of_a_normal_route() {
    let tmp = tempfile::tempdir().expect("a temporary directory");
    let existing = write_library_copy(tmp.path(), "rank/ABC/ABC-123.mp4");
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123 release")],
        categories: Some(vec![category("Actor", "/115/embyx_in/clt")]),
        sukebei: vec![("ABC-123", magnet_a())],
        archive_config: Some(library_config(tmp.path())),
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    assert!(deps.cloud.calls().is_empty());
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Archived);
    assert_eq!(
        deps.ledger.state().archived_paths["ABC-123"],
        ["actor/clt/ABC/ABC-123.mp4"]
    );
    assert!(!existing.exists());
    assert!(tmp.path().join("library/actor/clt/ABC/ABC-123.mp4").exists());
}

// py: tests/test_monitor_rss.py::test_an_absent_avid_still_downloads_when_routes_are_configured
#[tokio::test]
async fn an_absent_avid_still_downloads_when_routes_are_configured() {
    let tmp = tempfile::tempdir().expect("a temporary directory");
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123 release")],
        categories: Some(vec![category("Rank", "/115/embyx_in/rank")]),
        sukebei: vec![("ABC-123", magnet_a())],
        archive_config: Some(library_config(tmp.path())),
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    assert_eq!(deps.cloud.calls(), submitted(magnet_a(), "/115/embyx_in/rank"));
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Downloading);
}

// py: tests/test_monitor_rss.py::test_an_item_carrying_a_magnet_wakes_a_parked_avid
#[tokio::test]
async fn an_item_carrying_a_magnet_wakes_a_parked_avid() {
    // The magnet is evidence the wait is over: the cooldown no longer applies.
    let ledger = FakeLedger::with_known(&[("ABC-123", AcquisitionState::ResolveFailed)]);
    ledger
        .state()
        .next_action_at
        .insert("ABC-123".to_owned(), Some(Utc::now() + TimeDelta::days(1)));
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item_with("item-1", "ABC-123", &magnet_table(HASH_B))],
        ledger: Some(ledger),
        ..Setup::default()
    });

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    assert_eq!(deps.cloud.calls(), submitted(magnet_b(), TASK_DIR));
    assert_eq!(deps.state("ABC-123"), AcquisitionState::Downloading);
    assert_eq!(stat(&ctx, "skipped_known").unwrap_or(0), 0);
}

// py: tests/test_monitor_rss.py::test_an_item_without_a_magnet_leaves_a_parked_avid_cooling
#[tokio::test]
async fn an_item_without_a_magnet_leaves_a_parked_avid_cooling() {
    let ledger = FakeLedger::with_known(&[("ABC-123", AcquisitionState::ResolveFailed)]);
    let due = Utc::now() + TimeDelta::days(1);
    ledger.state().next_action_at.insert("ABC-123".to_owned(), Some(due));
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123")],
        ledger: Some(ledger),
        sukebei: vec![("ABC-123", magnet_a())],
        ..Setup::default()
    });

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    assert_eq!(stat(&ctx, "skipped_known"), Some(1));
    assert!(deps.cloud.calls().is_empty());
    assert_eq!(deps.ledger.state().next_action_at["ABC-123"], Some(due));
}

// -- subscriptions and cursors ------------------------------------------------

// py: tests/test_monitor_rss.py::test_items_seen_last_poll_are_not_read_again
#[tokio::test]
async fn items_seen_last_poll_are_not_read_again() {
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123")],
        sukebei: vec![("ABC-123", magnet_a())],
        ..Setup::default()
    });
    pipeline.run(&make_ctx()).await.expect("run");
    // Pretend the download failed and the row is waiting: a re-read would try again.
    {
        let mut state = deps.ledger.state();
        state
            .states
            .insert("ABC-123".to_owned(), AcquisitionState::ResolveFailed);
        state.next_action_at.insert("ABC-123".to_owned(), None);
    }

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    assert_eq!(stat(&ctx, "new_items"), Some(0));
    assert_eq!(stat(&ctx, "unique_avids").unwrap_or(0), 0);
    assert_eq!(deps.cloud.calls().len(), 1);
}

// py: tests/test_monitor_rss.py::test_a_pending_seed_records_the_feed_without_ingesting_it
#[tokio::test]
async fn a_pending_seed_records_the_feed_without_ingesting_it() {
    let subscriptions = FakeSubscriptions::new(vec![make_subscription(
        1,
        SubscriptionSpec {
            seed_pending: true,
            ..SubscriptionSpec::default()
        },
    )]);
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123")],
        subscriptions: Some(subscriptions),
        sukebei: vec![("ABC-123", magnet_a())],
        ..Setup::default()
    });

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    assert_eq!(stat(&ctx, "subscriptions_seeded"), Some(1));
    assert!(deps.no_states());
    assert_eq!(deps.fetch.calls(), [feed_url("Actor")]);
    let record = deps.subscriptions.record(1);
    assert_eq!(record.cursor, ["item-1"]);
    assert!(!record.seed_pending);
}

// py: tests/test_monitor_rss.py::test_a_disabled_subscription_is_not_polled
#[tokio::test]
async fn a_disabled_subscription_is_not_polled() {
    let subscriptions = FakeSubscriptions::new(vec![make_subscription(
        1,
        SubscriptionSpec {
            enabled: false,
            ..SubscriptionSpec::default()
        },
    )]);
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123")],
        subscriptions: Some(subscriptions),
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    assert!(deps.fetch.calls().is_empty());
    assert!(deps.no_states());
}

// py: tests/test_monitor_rss.py::test_the_avid_falls_back_to_the_item_link_with_the_catalog_prefix_stripped
#[tokio::test]
async fn the_avid_falls_back_to_the_item_link_with_the_catalog_prefix_stripped() {
    // An AVBase talent feed: the title is the work's title, the ID only in the link.
    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![Item {
            link: Some("https://www.avbase.net/works/moodyz:MIZD-555".to_owned()),
            ..item("https://www.avbase.net/works/MIZD-555", "乳首ギンギンのけ反りギュンッ")
        }],
        sukebei: vec![("MIZD-555", magnet_a())],
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    assert_eq!(deps.state("MIZD-555"), AcquisitionState::Downloading);
}

// py: tests/test_monitor_rss.py::test_a_feed_that_cannot_be_parsed_is_reported_on_the_subscription
#[tokio::test]
async fn a_feed_that_cannot_be_parsed_is_reported_on_the_subscription() {
    let (pipeline, deps) = make_pipeline(Setup {
        fetch_sequence: Some(vec![Ok(b"<html><body>not a feed</body></html>".to_vec())]),
        ..Setup::default()
    });

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    assert_eq!(stat(&ctx, "subscriptions_failed"), Some(1));
    let record = deps.subscriptions.record(1);
    let error = record.last_error.expect("an error");
    assert!(error.starts_with("FeedParseError"), "{error}");
    assert!(record.cursor.is_empty());
}

fn avbase_feed(talent_id: i128) -> String {
    format!("https://www.avbase.net/talents/{talent_id}/feed")
}

/// `moved_talent_pipeline(listed=, subscriptions=None, live=None)`: one talent subscription
/// (#1, talent 46505) whose feed is gone; `live` lists the feeds that still answer.
fn moved_talent_pipeline(
    listed: Result<Option<AvbaseTalent>, Raised>,
    subscriptions: Option<FakeSubscriptions>,
    live: &[(i128, Vec<Item>)],
) -> (RssPipeline, Deps, Arc<StubTalent>) {
    let feeds: HashMap<String, Vec<u8>> = live
        .iter()
        .map(|(talent_id, items)| (avbase_feed(*talent_id), feed_xml(items)))
        .collect();
    let lookup = Arc::new(StubTalent {
        listed,
        calls: Mutex::default(),
    });
    let subscriptions = subscriptions.unwrap_or_else(|| {
        FakeSubscriptions::new(vec![make_subscription(
            1,
            SubscriptionSpec {
                name: Some("小花のん".to_owned()),
                talent_id: Some(46505),
                aliases: strings(&["こはなのん"]),
                cursor: strings(&["https://www.avbase.net/works/OLD-001"]),
                ..SubscriptionSpec::default()
            },
        )])
    });
    let (pipeline, deps) = make_pipeline(Setup {
        subscriptions: Some(subscriptions),
        fetch_fn: Some(Box::new(move |url: &str| {
            feeds.get(url).cloned().ok_or_else(|| not_found(url))
        })),
        talent_lookup: Some(Arc::clone(&lookup)),
        ..Setup::default()
    });
    (pipeline, deps, lookup)
}

// py: tests/test_monitor_rss.py::test_a_talent_whose_feed_is_gone_follows_avbase_to_its_new_id
#[tokio::test]
async fn a_talent_whose_feed_is_gone_follows_avbase_to_its_new_id() {
    let (pipeline, deps, _) = moved_talent_pipeline(
        Ok(Some(talent(71334, "小花のん", &[], 993))),
        None,
        &[(
            71334,
            vec![
                item("https://www.avbase.net/works/OLD-001", "OLD-001"),
                item("https://www.avbase.net/works/NEW-002", "NEW-002"),
            ],
        )],
    );

    let ctx = make_ctx();
    pipeline.run(&ctx).await.expect("run");

    let record = deps.subscriptions.record(1);
    assert_eq!(record.talent_id, Some(71334));
    assert_eq!(record.feed_url(), avbase_feed(71334));
    // Re-created talents can come back without aliases; the known ones are kept.
    assert_eq!(record.aliases, ["こはなのん"]);
    assert_eq!(record.last_error, None);
    // Items are keyed by work URL, so the cursor carries over and only the new work is read.
    assert_eq!(
        record.cursor,
        [
            "https://www.avbase.net/works/OLD-001",
            "https://www.avbase.net/works/NEW-002"
        ]
    );
    assert_eq!(stat(&ctx, "subscriptions_relinked"), Some(1));
    assert_eq!(stat(&ctx, "new_items"), Some(1));
    assert!(deps.has_state("NEW-002"));
    assert!(!deps.has_state("OLD-001"));
}

// py: tests/test_monitor_rss.py::test_a_relinked_talent_takes_the_new_aliases_when_avbase_lists_them
#[tokio::test]
async fn a_relinked_talent_takes_the_new_aliases_when_avbase_lists_them() {
    let (pipeline, deps, _) = moved_talent_pipeline(
        Ok(Some(talent(71334, "小花のん", &["別名"], 993))),
        None,
        &[(71334, vec![])],
    );

    pipeline.run(&make_ctx()).await.expect("run");

    assert_eq!(deps.subscriptions.record(1).aliases, ["別名"]);
}

// py: tests/test_monitor_rss.py::test_a_gone_feed_stays_failed_when_avbase_offers_no_new_id cases=4
#[tokio::test]
async fn a_gone_feed_stays_failed_when_avbase_offers_no_new_id() {
    let cases: [(&str, Result<Option<AvbaseTalent>, Raised>); 4] = [
        ("not-listed", Ok(None)),
        ("same-id", Ok(Some(talent(46505, "小花のん", &[], 1)))),
        ("someone-else", Ok(Some(talent(99999, "別人", &["他人"], 1)))),
        ("catalog-down", Err(runtime_error("AVBase is down"))),
    ];
    for (id, listed) in cases {
        let (pipeline, deps, _) = moved_talent_pipeline(listed, None, &[(99999, vec![])]);

        let ctx = make_ctx();
        pipeline.run(&ctx).await.expect("run");

        let record = deps.subscriptions.record(1);
        assert_eq!(record.talent_id, Some(46505), "{id}");
        let error = record.last_error.expect("an error");
        assert!(error.contains("404"), "{id}: {error}");
        assert_eq!(stat(&ctx, "subscriptions_failed"), Some(1), "{id}");
        assert_eq!(stat(&ctx, "subscriptions_relinked"), None, "{id}");
    }
}

// py: tests/test_monitor_rss.py::test_a_new_id_another_subscription_follows_is_reported_not_taken
#[tokio::test]
async fn a_new_id_another_subscription_follows_is_reported_not_taken() {
    let subscriptions = FakeSubscriptions::new(vec![
        make_subscription(
            1,
            SubscriptionSpec {
                name: Some("小花のん".to_owned()),
                talent_id: Some(46505),
                ..SubscriptionSpec::default()
            },
        ),
        make_subscription(
            2,
            SubscriptionSpec {
                name: Some("小花のん".to_owned()),
                talent_id: Some(71334),
                ..SubscriptionSpec::default()
            },
        ),
    ]);
    let (pipeline, deps, _) = moved_talent_pipeline(
        Ok(Some(talent(71334, "小花のん", &[], 993))),
        Some(subscriptions),
        &[(71334, vec![])],
    );

    pipeline.run(&make_ctx()).await.expect("run");

    let record = deps.subscriptions.record(1);
    assert_eq!(record.talent_id, Some(46505));
    let error = record.last_error.expect("an error");
    assert!(error.contains("already follows"), "{error}");
    assert_eq!(deps.subscriptions.record(2).last_error, None);
}

// py: tests/test_monitor_rss.py::test_a_gone_rss_feed_is_not_looked_up_as_a_talent
#[tokio::test]
async fn a_gone_rss_feed_is_not_looked_up_as_a_talent() {
    let lookup = Arc::new(StubTalent {
        listed: Ok(None),
        calls: Mutex::default(),
    });
    let (pipeline, deps) = make_pipeline(Setup {
        fetch_fn: Some(Box::new(|_url: &str| Err(not_found(&feed_url("Actor"))))),
        talent_lookup: Some(Arc::clone(&lookup)),
        ..Setup::default()
    });

    pipeline.run(&make_ctx()).await.expect("run");

    assert!(lookup.calls.lock().expect("calls").is_empty());
    assert!(
        deps.subscriptions
            .record(1)
            .last_error
            .unwrap_or_default()
            .contains("404")
    );
}

/// `_describe` keeps 500 characters of `Class: message`, or the class alone for an empty
/// message; a stop request is not a subscription failure but ends the run.
#[tokio::test]
async fn errors_are_described_as_python_does_and_a_stop_ends_the_run() {
    let long = "x".repeat(600);
    assert_eq!(describe(&runtime_error("")), "RuntimeError");
    let described = describe(&runtime_error(&long));
    assert_eq!(described.chars().count(), 500);
    assert!(described.starts_with("RuntimeError: xxx"));

    let (pipeline, deps) = make_pipeline(Setup {
        items: vec![item("item-1", "ABC-123")],
        ..Setup::default()
    });
    let ctx = make_ctx();
    ctx.request_stop();
    let error = pipeline.run(&ctx).await.unwrap_err();
    assert_eq!(error.class_name(), "RunCancelledError");
    assert!(deps.fetch.calls().is_empty());
    assert!(deps.subscriptions.state().polls.is_empty());
}

mod routing;
