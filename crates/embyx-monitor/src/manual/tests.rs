//! Ported from `tests/test_monitor_manual.py`: browsing, the remembered directory and
//! submissions through the shared intake, against the in-memory ledger and a CloudDrive fake.
//! The replacement tests (`replace/tests.rs`) build on the same fakes, as the Python ones do.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use embyx_clients::clouddrive::CloudFile;
use embyx_clients::clouddrive::proto::FileOperationResult;
use embyx_clients::javbus::JavBusMagnet;
use embyx_core::avid::AvidParser;
use embyx_core::config::models::ArchiveConfig;

use super::*;
use crate::acquisitions::{AcquisitionState, AttemptState};
use crate::archive::ArchivePipeline;
use crate::fs::path_text;
use crate::intake::{IntakeParts, JavBusMagnets, OfflineFiles, SukebeiMagnets};
use crate::reports::Raised;
use crate::support::BoxFuture;
use crate::testing::FakeLedger;

pub(crate) const HASH_A: &str = "C12FE1C06BBA254A9DC9F519B335AA7C1367A88A";
pub(crate) const MAGNET_A: &str = "magnet:?xt=urn:btih:C12FE1C06BBA254A9DC9F519B335AA7C1367A88A&dn=ABC-123";

// The deployment these mirror: an inbox route and a rank route nested in it, both mounted
// locally under src_dir, plus a directory with no route at all.
pub(crate) const INBOX: &str = "/115/embyx_in";
pub(crate) const RANK_DIR: &str = "/115/embyx_in/rank";
const UNROUTED_DIR: &str = "/115/misc";

/// `make_archiver(tmp_path)`: the mount exposes the cloud tree unchanged, which is what lets a
/// route root be matched to a CloudDrive API path by suffix.
pub(crate) fn make_archiver(root: &Path) -> ArchivePipeline {
    let config = ArchiveConfig {
        enabled: true,
        src_dir: path_text(&root.join("mount/115")),
        dst_dir: path_text(&root.join("library")),
        mapping: vec![
            ("embyx_in".to_owned(), "embyx".to_owned()),
            ("embyx_in/rank".to_owned(), "embyx/rank".to_owned()),
        ],
        ..ArchiveConfig::default()
    };
    ArchivePipeline::new(config, AvidParser::default())
}

/// `FakeCloud`: CloudDrive listings for browsing, plus the offline submissions made.
#[derive(Debug, Default)]
pub(crate) struct FakeCloud {
    tree: BTreeMap<String, Vec<CloudFile>>,
    pub(crate) submitted: Mutex<Vec<(Vec<String>, String)>>,
    /// `AsyncMock(return_value=SimpleNamespace(success=False))`.
    pub(crate) refuses: bool,
}

impl FakeCloud {
    pub(crate) fn new(tree: &[(&str, Vec<CloudFile>)]) -> Self {
        Self {
            tree: tree
                .iter()
                .map(|(path, entries)| ((*path).to_owned(), entries.clone()))
                .collect(),
            ..Self::default()
        }
    }

    /// The default tree: the inbox, the rank directory and the unrouted one, all empty.
    pub(crate) fn standard() -> Self {
        Self::new(&[(INBOX, Vec::new()), (RANK_DIR, Vec::new()), (UNROUTED_DIR, Vec::new())])
    }

    pub(crate) fn submitted(&self) -> Vec<(Vec<String>, String)> {
        self.submitted.lock().expect("submitted").clone()
    }
}

impl CloudListing for FakeCloud {
    fn list_directory<'a>(&'a self, api_dir: &'a str) -> BoxFuture<'a, Result<Vec<CloudFile>, SourceError>> {
        let answer = self
            .tree
            .get(api_dir)
            .cloned()
            .ok_or_else(|| SourceError::Raised(Raised::new("FileNotFoundError", api_dir)));
        Box::pin(std::future::ready(answer))
    }
}

impl OfflineFiles for FakeCloud {
    fn add_offline_files<'a>(
        &'a self,
        urls: &'a [String],
        dst_dir: &'a str,
    ) -> BoxFuture<'a, Result<FileOperationResult, SourceError>> {
        self.submitted
            .lock()
            .expect("submitted")
            .push((urls.to_vec(), dst_dir.to_owned()));
        let result = FileOperationResult {
            success: !self.refuses,
            ..FileOperationResult::default()
        };
        Box::pin(std::future::ready(Ok(result)))
    }
}

/// `entry(name, parent, is_directory=)`.
pub(crate) fn entry(name: &str, parent: &str, is_directory: bool) -> CloudFile {
    CloudFile {
        id: String::new(),
        name: name.to_owned(),
        full_path: format!("{}/{name}", parent.trim_end_matches('/')),
        size: 0,
        is_directory,
        write_time: None,
        hashes: BTreeMap::new(),
    }
}

/// `SimpleNamespace(get_magnet=AsyncMock(return_value=magnet))`.
#[derive(Debug, Default)]
pub(crate) struct StubSukebei {
    magnet: Option<String>,
    pub(crate) calls: Mutex<Vec<String>>,
}

impl SukebeiMagnets for StubSukebei {
    fn get_magnet<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Option<String>, SourceError>> {
        self.calls.lock().expect("calls").push(avid.to_owned());
        Box::pin(std::future::ready(Ok(self.magnet.clone())))
    }
}

/// `SimpleNamespace(get_magnets=AsyncMock(return_value=[]))`.
pub(crate) struct NoJavBus;

impl JavBusMagnets for NoJavBus {
    fn get_magnets<'a>(&'a self, _avid: &'a str) -> BoxFuture<'a, Result<Vec<JavBusMagnet>, SourceError>> {
        Box::pin(std::future::ready(Ok(Vec::new())))
    }
}

/// `AcquisitionIntake(ledger=, sukebei=, javbus=, cloud=, failed_cooldown_seconds=3600)`.
pub(crate) fn make_intake(
    ledger: &Arc<FakeLedger>,
    cloud: &Arc<FakeCloud>,
    magnet: Option<&str>,
) -> (Arc<AcquisitionIntake>, Arc<StubSukebei>) {
    let sukebei = Arc::new(StubSukebei {
        magnet: magnet.map(str::to_owned),
        calls: Mutex::new(Vec::new()),
    });
    let intake = AcquisitionIntake::new(IntakeParts {
        ledger: Arc::clone(ledger) as Arc<dyn Ledger>,
        sukebei: Arc::clone(&sukebei) as Arc<dyn SukebeiMagnets>,
        javbus: Arc::new(NoJavBus),
        cloud: Arc::clone(cloud) as Arc<dyn OfflineFiles>,
        failed_cooldown_seconds: 3600,
        on_submitted: None,
        release_date_lookup: None,
    })
    .expect("a valid cooldown");
    (Arc::new(intake), sukebei)
}

/// What `make_source` sets (its keyword arguments).
struct Setup {
    ledger: Option<FakeLedger>,
    cloud: Option<FakeCloud>,
    magnet: Option<&'static str>,
    configured_dirs: Vec<&'static str>,
    with_cloud: bool,
    on_queued: Option<OnQueued>,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            ledger: None,
            cloud: None,
            magnet: Some(MAGNET_A),
            configured_dirs: vec![INBOX],
            with_cloud: true,
            on_queued: None,
        }
    }
}

struct Deps {
    ledger: Arc<FakeLedger>,
    cloud: Arc<FakeCloud>,
    sukebei: Arc<StubSukebei>,
}

impl Deps {
    fn states(&self) -> Vec<(String, AcquisitionState)> {
        self.ledger
            .state()
            .states
            .iter()
            .map(|(avid, state)| (avid.clone(), *state))
            .collect()
    }
}

fn make_source(root: &Path, setup: Setup) -> (ManualIntakeSource, Deps) {
    let ledger = Arc::new(setup.ledger.unwrap_or_default());
    let cloud = Arc::new(setup.cloud.unwrap_or_else(FakeCloud::standard));
    let (intake, sukebei) = make_intake(&ledger, &cloud, setup.magnet);
    let root = root.to_path_buf();
    let with_cloud = setup.with_cloud;
    let configured: Vec<String> = setup.configured_dirs.iter().map(|dir| (*dir).to_owned()).collect();
    let listing = Arc::clone(&cloud);
    let source = ManualIntakeSource {
        ledger: Arc::clone(&ledger) as Arc<dyn Ledger>,
        intake_factory: Arc::new(move || with_cloud.then(|| Arc::clone(&intake))),
        cloud_factory: Arc::new(move || with_cloud.then(|| Arc::clone(&listing) as Arc<dyn CloudListing>)),
        archiver_factory: Arc::new(move || make_archiver(&root)),
        parser_factory: Arc::new(AvidParser::default),
        configured_dirs: Arc::new(move || configured.clone()),
        on_queued: setup.on_queued,
        logger: "test-manual".to_owned(),
    };
    (source, Deps { ledger, cloud, sukebei })
}

/// What an `on_entry` callback heard: the AVID and the outcome.
type Heard = (Option<String>, ManualOutcome);

fn lines(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

fn hold(root: &Path, relative: &str) {
    let file = root.join("library").join(relative);
    std::fs::create_dir_all(file.parent().expect("a parent")).expect("parents");
    std::fs::write(file, b"0").expect("write");
}

// -- browsing -----------------------------------------------------------------

// py: tests/test_monitor_manual.py::test_browse_marks_which_directories_can_be_submitted_to
#[tokio::test]
async fn browse_marks_which_directories_can_be_submitted_to() {
    // Routed and configured are separate facts: only the first decides submittability.
    let dir = tempfile::tempdir().expect("tempdir");
    let cloud = FakeCloud::new(&[(
        "/115",
        vec![
            entry("embyx_in", "/115", true),
            entry("misc", "/115", true),
            entry("note.txt", "/115", false),
        ],
    )]);
    let (source, _) = make_source(
        dir.path(),
        Setup {
            cloud: Some(cloud),
            ..Setup::default()
        },
    );

    let listing = source.browse("/115").await.expect("browse");

    assert_eq!(listing.path, "/115");
    assert_eq!(listing.parent.as_deref(), Some("/"));
    let seen: Vec<(String, bool, bool)> = listing
        .entries
        .iter()
        .map(|item| (item.name.clone(), item.configured, item.routed))
        .collect();
    assert_eq!(
        seen,
        vec![("embyx_in".to_owned(), true, true), ("misc".to_owned(), false, false)]
    );
}

// py: tests/test_monitor_manual.py::test_browse_finds_a_route_for_a_directory_no_source_declares
#[tokio::test]
async fn browse_finds_a_route_for_a_directory_no_source_declares() {
    // The rank route exists in the archive tables without being an input source's own.
    let dir = tempfile::tempdir().expect("tempdir");
    let cloud = FakeCloud::new(&[(INBOX, vec![entry("rank", INBOX, true)])]);
    let (source, _) = make_source(
        dir.path(),
        Setup {
            cloud: Some(cloud),
            ..Setup::default()
        },
    );

    let listing = source.browse(INBOX).await.expect("browse");

    let seen: Vec<(String, bool, bool)> = listing
        .entries
        .iter()
        .map(|item| (item.name.clone(), item.configured, item.routed))
        .collect();
    assert_eq!(seen, vec![("rank".to_owned(), false, true)]);
}

// py: tests/test_monitor_manual.py::test_browse_reports_the_root_and_a_missing_directory
#[tokio::test]
async fn browse_reports_the_root_and_a_missing_directory() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (source, _) = make_source(
        dir.path(),
        Setup {
            cloud: Some(FakeCloud::new(&[("/", Vec::new())])),
            ..Setup::default()
        },
    );

    let listing = source.browse("/").await.expect("browse");

    assert_eq!(listing.parent, None);
    assert!(matches!(
        source.browse("/nope").await,
        Err(ManualError::DirectoryNotFound)
    ));
    assert!(matches!(
        source.browse("relative/path").await,
        Err(ManualError::DirectoryNotFound)
    ));
}

// py: tests/test_monitor_manual.py::test_browse_without_clouddrive
#[tokio::test]
async fn browse_without_clouddrive() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (source, _) = make_source(
        dir.path(),
        Setup {
            with_cloud: false,
            ..Setup::default()
        },
    );

    assert!(matches!(source.browse("/").await, Err(ManualError::CloudUnavailable)));
}

// -- the remembered directory -------------------------------------------------

// py: tests/test_monitor_manual.py::test_default_directory_remembers_the_last_manual_submission
#[tokio::test]
async fn default_directory_remembers_the_last_manual_submission() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (source, deps) = make_source(dir.path(), Setup::default());

    // Nothing submitted yet: a configured one.
    assert_eq!(
        source.default_directory().await.expect("default").as_deref(),
        Some(INBOX)
    );

    source
        .submit(&lines(&["ABC-123"]), RANK_DIR, SubmitOptions::default())
        .await
        .expect("submit");

    assert_eq!(deps.ledger.state().task_dirs["ABC-123"].as_deref(), Some(RANK_DIR));
    assert_eq!(
        source.default_directory().await.expect("default").as_deref(),
        Some(RANK_DIR)
    );
}

// py: tests/test_monitor_manual.py::test_default_directory_skips_a_configured_directory_without_a_route
#[tokio::test]
async fn default_directory_skips_a_configured_directory_without_a_route() {
    // Offering it would only produce a refusal when the operator submits.
    let dir = tempfile::tempdir().expect("tempdir");
    let (source, _) = make_source(
        dir.path(),
        Setup {
            configured_dirs: vec![UNROUTED_DIR],
            ..Setup::default()
        },
    );

    assert_eq!(source.default_directory().await.expect("default"), None);
}

// -- submitting ---------------------------------------------------------------

// py: tests/test_monitor_manual.py::test_submit_reads_dirty_names_and_pins_the_chosen_directory
#[tokio::test]
async fn submit_reads_dirty_names_and_pins_the_chosen_directory() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (source, deps) = make_source(dir.path(), Setup::default());

    let submission = source
        .submit(
            &lines(&["[JAV] ABC-123 1080p.mp4", "  def-456  ", ""]),
            RANK_DIR,
            SubmitOptions::default(),
        )
        .await
        .expect("submit");

    let seen: Vec<(Option<String>, ManualOutcome)> = submission
        .entries
        .iter()
        .map(|item| (item.avid.clone(), item.outcome))
        .collect();
    assert_eq!(
        seen,
        vec![
            (Some("ABC-123".to_owned()), ManualOutcome::Submitted),
            (Some("DEF-456".to_owned()), ManualOutcome::Submitted),
        ]
    );
    assert_eq!(submission.task_dir_path, RANK_DIR);
    let state = deps.ledger.state();
    let sources: Vec<(&str, &str)> = state.sources.iter().map(|(a, s)| (a.as_str(), s.as_str())).collect();
    assert_eq!(sources, vec![("ABC-123", "manual"), ("DEF-456", "manual")]);
    let task_dirs: Vec<(&str, Option<&str>)> = state
        .task_dirs
        .iter()
        .map(|(avid, dir)| (avid.as_str(), dir.as_deref()))
        .collect();
    assert_eq!(
        task_dirs,
        vec![("ABC-123", Some(RANK_DIR)), ("DEF-456", Some(RANK_DIR))]
    );
    drop(state);
    assert_eq!(
        deps.cloud.submitted(),
        vec![
            (vec![MAGNET_A.to_owned()], RANK_DIR.to_owned()),
            (vec![MAGNET_A.to_owned()], RANK_DIR.to_owned())
        ]
    );
}

// py: tests/test_monitor_manual.py::test_submit_refuses_a_directory_without_an_archive_route
#[tokio::test]
async fn submit_refuses_a_directory_without_an_archive_route() {
    // A download there could never be located, so no ledger row is written for it.
    let dir = tempfile::tempdir().expect("tempdir");
    let (source, deps) = make_source(dir.path(), Setup::default());

    let refused = source
        .submit(&lines(&["ABC-123"]), UNROUTED_DIR, SubmitOptions::default())
        .await;

    assert!(matches!(refused, Err(ManualError::DirectoryNotRouted)));
    assert!(deps.states().is_empty());
    assert!(deps.cloud.submitted().is_empty());
}

// py: tests/test_monitor_manual.py::test_submit_refuses_a_directory_clouddrive_does_not_have
#[tokio::test]
async fn submit_refuses_a_directory_clouddrive_does_not_have() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (source, deps) = make_source(
        dir.path(),
        Setup {
            cloud: Some(FakeCloud::new(&[(INBOX, Vec::new())])),
            ..Setup::default()
        },
    );

    let refused = source
        .submit(&lines(&["ABC-123"]), RANK_DIR, SubmitOptions::default())
        .await;

    assert!(matches!(refused, Err(ManualError::DirectoryNotFound)));
    assert!(deps.states().is_empty());
}

// py: tests/test_monitor_manual.py::test_submit_reports_a_line_no_avid_can_be_read_from
#[tokio::test]
async fn submit_reports_a_line_no_avid_can_be_read_from() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (source, deps) = make_source(dir.path(), Setup::default());

    let submission = source
        .submit(&lines(&["???", "ABC-123"]), INBOX, SubmitOptions::default())
        .await
        .expect("submit");

    let seen: Vec<(String, Option<String>, ManualOutcome)> = submission
        .entries
        .iter()
        .map(|item| (item.text.clone(), item.avid.clone(), item.outcome))
        .collect();
    assert_eq!(
        seen,
        vec![
            ("???".to_owned(), None, ManualOutcome::Unreadable),
            (
                "ABC-123".to_owned(),
                Some("ABC-123".to_owned()),
                ManualOutcome::Submitted
            ),
        ]
    );
    assert_eq!(
        deps.states(),
        vec![("ABC-123".to_owned(), AcquisitionState::Downloading)]
    );
}

// py: tests/test_monitor_manual.py::test_submit_reports_an_avid_the_ledger_already_owns
#[tokio::test]
async fn submit_reports_an_avid_the_ledger_already_owns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (source, deps) = make_source(
        dir.path(),
        Setup {
            ledger: Some(FakeLedger::with_known(&[("ABC-123", AcquisitionState::Downloading)])),
            ..Setup::default()
        },
    );

    let submission = source
        .submit(&lines(&["ABC-123", "ABC-123"]), INBOX, SubmitOptions::default())
        .await
        .expect("submit");

    let outcomes: Vec<ManualOutcome> = submission.entries.iter().map(|item| item.outcome).collect();
    assert_eq!(
        outcomes,
        vec![ManualOutcome::AlreadyTracked, ManualOutcome::AlreadyTracked]
    );
    assert!(deps.cloud.submitted().is_empty());
}

// py: tests/test_monitor_manual.py::test_submit_parks_an_avid_with_no_magnet
#[tokio::test]
async fn submit_parks_an_avid_with_no_magnet() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (source, deps) = make_source(
        dir.path(),
        Setup {
            magnet: None,
            ..Setup::default()
        },
    );

    let submission = source
        .submit(&lines(&["ABC-123"]), INBOX, SubmitOptions::default())
        .await
        .expect("submit");

    let outcomes: Vec<ManualOutcome> = submission.entries.iter().map(|item| item.outcome).collect();
    assert_eq!(outcomes, vec![ManualOutcome::NoMagnet]);
    assert_eq!(deps.ledger.state().states["ABC-123"], AcquisitionState::ResolveFailed);
}

// py: tests/test_monitor_manual.py::test_submit_reports_what_the_library_already_holds
#[tokio::test]
async fn submit_reports_what_the_library_already_holds() {
    // Nothing is recorded: the operator asked for a video they already have.
    let dir = tempfile::tempdir().expect("tempdir");
    hold(dir.path(), "embyx/ABC/ABC-123.mp4");
    let (source, deps) = make_source(dir.path(), Setup::default());

    let submission = source
        .submit(&lines(&["ABC-123"]), INBOX, SubmitOptions::default())
        .await
        .expect("submit");

    let seen: Vec<(ManualOutcome, Vec<String>)> = submission
        .entries
        .iter()
        .map(|item| (item.outcome, item.archived_paths.clone()))
        .collect();
    assert_eq!(
        seen,
        vec![(
            ManualOutcome::AlreadyInLibrary,
            vec!["embyx/ABC/ABC-123.mp4".to_owned()]
        )]
    );
    assert!(deps.states().is_empty());
    assert!(deps.cloud.submitted().is_empty());
}

// py: tests/test_monitor_manual.py::test_submit_reports_a_magnet_clouddrive_rejects
#[tokio::test]
async fn submit_reports_a_magnet_clouddrive_rejects() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cloud = FakeCloud::new(&[(INBOX, Vec::new())]);
    cloud.refuses = true;
    let (source, deps) = make_source(
        dir.path(),
        Setup {
            cloud: Some(cloud),
            ..Setup::default()
        },
    );

    let submission = source
        .submit(&lines(&["ABC-123"]), INBOX, SubmitOptions::default())
        .await
        .expect("submit");

    let outcomes: Vec<ManualOutcome> = submission.entries.iter().map(|item| item.outcome).collect();
    assert_eq!(outcomes, vec![ManualOutcome::SubmitFailed]);
    assert_eq!(deps.ledger.attempt_states("ABC-123"), vec![AttemptState::Error]);
}

// py: tests/test_monitor_manual.py::test_submit_refuses_more_inputs_than_it_will_look_up
#[tokio::test]
async fn submit_refuses_more_inputs_than_it_will_look_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (source, deps) = make_source(dir.path(), Setup::default());
    let inputs: Vec<String> = (0..=MAX_MANUAL_INPUTS).map(|index| format!("ABC-{index:03}")).collect();

    let refused = source.submit(&inputs, INBOX, SubmitOptions::default()).await;

    assert!(matches!(refused, Err(ManualError::TooManyInputs)));
    assert!(deps.states().is_empty());
}

// py: tests/test_monitor_manual.py::test_submit_without_clouddrive
#[tokio::test]
async fn submit_without_clouddrive() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (source, _) = make_source(
        dir.path(),
        Setup {
            with_cloud: false,
            ..Setup::default()
        },
    );

    let refused = source
        .submit(&lines(&["ABC-123"]), INBOX, SubmitOptions::default())
        .await;

    assert!(matches!(refused, Err(ManualError::CloudUnavailable)));
}

// py: tests/test_monitor_manual.py::test_submit_takes_another_sources_label_and_lifts_the_cap
#[tokio::test]
async fn submit_takes_another_sources_label_and_lifts_the_cap() {
    // A playlist fill is the manual path under its own name, with no paste limit.
    let dir = tempfile::tempdir().expect("tempdir");
    let (source, deps) = make_source(dir.path(), Setup::default());
    let avids: Vec<String> = (1..=MAX_MANUAL_INPUTS + 1).map(|n| format!("PL-{n:03}")).collect();

    let refused = source.submit(&avids, RANK_DIR, SubmitOptions::default()).await;
    assert!(matches!(refused, Err(ManualError::TooManyInputs)));

    let submission = source
        .submit(
            &avids,
            RANK_DIR,
            SubmitOptions {
                source: "playlist:k7".to_owned(),
                limit: None,
                ..SubmitOptions::default()
            },
        )
        .await
        .expect("submit");

    assert_eq!(submission.entries.len(), MAX_MANUAL_INPUTS + 1);
    assert!(
        submission
            .entries
            .iter()
            .all(|entry| entry.outcome == ManualOutcome::Submitted)
    );
    assert_eq!(deps.ledger.state().sources["PL-001"], "playlist:k7");
    assert!(deps.cloud.submitted().iter().all(|(_, dst)| dst == RANK_DIR));
}

// py: tests/test_monitor_manual.py::test_a_queued_batch_only_records_and_wakes_the_tracker_once
#[tokio::test]
async fn a_queued_batch_only_records_and_wakes_the_tracker_once() {
    // A long list is recorded for the background pass instead of resolved inline.
    let dir = tempfile::tempdir().expect("tempdir");
    hold(dir.path(), "embyx/rank/ABC/ABC-123.mp4");
    let wakes = Arc::new(Mutex::new(0_usize));
    let counter = Arc::clone(&wakes);
    let (source, deps) = make_source(
        dir.path(),
        Setup {
            on_queued: Some(Arc::new(move || *counter.lock().expect("wakes") += 1)),
            ..Setup::default()
        },
    );
    let heard: Arc<Mutex<Vec<Heard>>> = Arc::new(Mutex::new(Vec::new()));
    let listener = Arc::clone(&heard);

    let submission = source
        .submit(
            &lines(&["ABC-123", "PL-001", "PL-002", "???"]),
            RANK_DIR,
            SubmitOptions {
                source: "playlist:k7".to_owned(),
                limit: None,
                queue: true,
                on_entry: Some(Arc::new(move |entry: &ManualEntry| {
                    listener
                        .lock()
                        .expect("heard")
                        .push((entry.avid.clone(), entry.outcome));
                })),
            },
        )
        .await
        .expect("submit");

    let heard = heard.lock().expect("heard").clone();
    assert_eq!(
        heard,
        vec![
            (Some("ABC-123".to_owned()), ManualOutcome::AlreadyInLibrary),
            (Some("PL-001".to_owned()), ManualOutcome::Queued),
            (Some("PL-002".to_owned()), ManualOutcome::Queued),
            (None, ManualOutcome::Unreadable),
        ]
    );
    let outcomes: Vec<ManualOutcome> = submission.entries.iter().map(|entry| entry.outcome).collect();
    assert_eq!(outcomes, heard.iter().map(|(_, outcome)| *outcome).collect::<Vec<_>>());
    assert_eq!(
        deps.states(),
        vec![
            ("PL-001".to_owned(), AcquisitionState::Discovered),
            ("PL-002".to_owned(), AcquisitionState::Discovered)
        ]
    );
    // Due for the next tracker pass.
    assert!(deps.ledger.state().next_action_at["PL-001"].is_some());
    assert_eq!(deps.ledger.state().task_dirs["PL-001"].as_deref(), Some(RANK_DIR));
    // No lookup ran inline.
    assert!(deps.sukebei.calls.lock().expect("calls").is_empty());
    assert!(deps.cloud.submitted().is_empty());
    assert_eq!(*wakes.lock().expect("wakes"), 1);
}

// py: tests/test_monitor_manual.py::test_an_inline_batch_does_not_wake_the_tracker
#[tokio::test]
async fn an_inline_batch_does_not_wake_the_tracker() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wakes = Arc::new(Mutex::new(0_usize));
    let counter = Arc::clone(&wakes);
    let (source, _) = make_source(
        dir.path(),
        Setup {
            magnet: None,
            on_queued: Some(Arc::new(move || *counter.lock().expect("wakes") += 1)),
            ..Setup::default()
        },
    );

    source
        .submit(&lines(&["ABC-123"]), INBOX, SubmitOptions::default())
        .await
        .expect("submit");

    assert_eq!(*wakes.lock().expect("wakes"), 0);
}

// py: tests/test_monitor_manual.py::test_check_target_refuses_what_submit_would
#[tokio::test]
async fn check_target_refuses_what_submit_would() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (source, _) = make_source(dir.path(), Setup::default());

    assert_eq!(
        source.check_target(&format!("{RANK_DIR}/")).await.expect("routed"),
        RANK_DIR
    );
    assert!(matches!(
        source.check_target(UNROUTED_DIR).await,
        Err(ManualError::DirectoryNotRouted)
    ));
    let (unmade, _) = make_source(
        dir.path(),
        Setup {
            cloud: Some(FakeCloud::new(&[(INBOX, Vec::new())])),
            ..Setup::default()
        },
    );
    assert!(matches!(
        unmade.check_target(RANK_DIR).await,
        Err(ManualError::DirectoryNotFound)
    ));
    let (offline, _) = make_source(
        dir.path(),
        Setup {
            with_cloud: false,
            ..Setup::default()
        },
    );
    assert!(matches!(
        offline.check_target(INBOX).await,
        Err(ManualError::CloudUnavailable)
    ));
}

// -- Rust-only ----------------------------------------------------------------

#[test]
fn parents_and_refusal_codes_follow_python() {
    assert_eq!(parent_of("/"), None);
    assert_eq!(parent_of("/115"), Some("/".to_owned()));
    assert_eq!(parent_of("/115/embyx_in"), Some("/115".to_owned()));
    assert_eq!(ManualError::TooManyInputs.code(), Some("too_many_inputs"));
    assert_eq!(ManualError::DirectoryNotRouted.class_name(), "DirectoryNotRoutedError");
    assert!(matches!(normalize_dir("  "), Err(ManualError::DirectoryNotFound)));
    assert!(matches!(normalize_dir("/a/../b"), Err(ManualError::DirectoryNotFound)));
    assert_eq!(normalize_dir("/115//embyx_in/").expect("normalized"), "/115/embyx_in");
}

#[tokio::test]
async fn a_listing_failure_other_than_a_missing_directory_is_not_a_refusal() {
    struct Broken;
    impl CloudListing for Broken {
        fn list_directory<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Vec<CloudFile>, SourceError>> {
            Box::pin(std::future::ready(Err(SourceError::Raised(Raised::new(
                "RuntimeError",
                "CloudDrive is down",
            )))))
        }
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut source, deps) = make_source(dir.path(), Setup::default());
    source.cloud_factory = Arc::new(|| Some(Arc::new(Broken) as Arc<dyn CloudListing>));

    let browsed = source.browse("/115").await.expect_err("down");
    let submitted = source
        .submit(&lines(&["ABC-123"]), INBOX, SubmitOptions::default())
        .await
        .expect_err("down");

    assert_eq!(browsed.class_name(), "RuntimeError");
    assert_eq!(submitted.class_name(), "RuntimeError");
    assert!(deps.states().is_empty());
    let _ = HASH_A;
}
