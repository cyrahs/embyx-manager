//! Ported from `tests/test_rss_category_routing.py`: one category's items, end to end, from the
//! feed to its own offline directory to the library. The unit tests cover each stage on its
//! own; these walk the seam between them, since every stage has to agree on a category's
//! directory for its downloads to stay apart all the way into the library.

use std::path::Path;

use embyx_clients::clouddrive::{CloudFile, OfflineStatus, OfflineTask};

use super::*;
use crate::cloud::{CloudListing, TrackerCloud};
use crate::reconcile::ReconcileScanner;
use crate::tracker::{AcquisitionTracker, SubmitMagnet, TrackerParts, TrackerSettings};

/// The deployment this mirrors: a shared inbox with the rank category nested in it.
const INBOX: &str = "/115/embyx_in";
const NESTED_RANK_DIR: &str = "/115/embyx_in/rank";

fn magnet_actor() -> String {
    format!("magnet:?xt=urn:btih:{HASH_A}")
}

fn magnet_rank() -> String {
    format!("magnet:?xt=urn:btih:{HASH_B}")
}

fn routing_ctx() -> RunContext {
    RunContext::new("test-routing")
}

/// `offline_task(name, info_hash, status)` of the tracker tests.
fn offline_task(name: &str, info_hash: &str, status: OfflineStatus) -> OfflineTask {
    OfflineTask {
        name: name.to_owned(),
        size: 1024,
        url: format!("magnet:?xt=urn:btih:{info_hash}"),
        status,
        info_hash: info_hash.to_owned(),
        file_id: "1".to_owned(),
        add_time: 0,
        progress: 10.0,
        peers: 3,
    }
}

/// `write_video(path)` of the tracker tests: ten bytes.
fn write_video(path: &Path) {
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("parent");
    std::fs::write(path, b"xxxxxxxxxx").expect("write");
}

fn nested_routes(mount: &Path, library: &Path) -> ArchiveConfig {
    ArchiveConfig {
        enabled: true,
        src_dir: mount.to_str().expect("UTF-8").to_owned(),
        dst_dir: library.to_str().expect("UTF-8").to_owned(),
        // The rank route nests inside the inbox route, mirroring the directories.
        mapping: vec![
            ("embyx_in".to_owned(), "embyx".to_owned()),
            ("embyx_in/rank".to_owned(), "embyx/rank".to_owned()),
        ],
        ..ArchiveConfig::default()
    }
}

/// `RecordingCloud`: offline tasks appear under whichever directory they were submitted to.
#[derive(Default)]
struct RecordingCloud {
    tasks_by_dir: Mutex<HashMap<String, Vec<OfflineTask>>>,
    submitted: Mutex<Vec<(String, String)>>,
    refreshed: Mutex<Vec<String>>,
}

impl OfflineFiles for RecordingCloud {
    fn add_offline_files<'a>(
        &'a self,
        urls: &'a [String],
        dst_dir: &'a str,
    ) -> BoxFuture<'a, Result<FileOperationResult, SourceError>> {
        let mut submitted = self.submitted.lock().expect("submitted");
        submitted.extend(urls.iter().map(|url| (url.clone(), dst_dir.to_owned())));
        Box::pin(std::future::ready(Ok(success())))
    }
}

impl CloudListing for RecordingCloud {
    fn list_directory<'a>(&'a self, api_dir: &'a str) -> BoxFuture<'a, Result<Vec<CloudFile>, SourceError>> {
        self.refreshed.lock().expect("refreshed").push(api_dir.to_owned());
        Box::pin(std::future::ready(Ok(Vec::new())))
    }
}

/// Python's `RecordingCloud` has no removal methods; the poll in this test archives its
/// downloads without reaching them, so these only answer.
impl TrackerCloud for RecordingCloud {
    fn list_offline_files<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Vec<OfflineTask>, SourceError>> {
        let tasks = self
            .tasks_by_dir
            .lock()
            .expect("tasks")
            .get(path)
            .cloned()
            .unwrap_or_default();
        Box::pin(std::future::ready(Ok(tasks)))
    }

    fn remove_offline_files<'a>(
        &'a self,
        _info_hashes: &'a [String],
        _path: &'a str,
        _delete_files: bool,
    ) -> BoxFuture<'a, Result<(), SourceError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn delete_files<'a>(&'a self, _api_paths: &'a [String]) -> BoxFuture<'a, Result<(), SourceError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn settle(&self) -> BoxFuture<'_, ()> {
        Box::pin(std::future::ready(()))
    }
}

// py: tests/test_rss_category_routing.py::test_a_category_stays_in_its_own_directory_from_feed_to_library
#[tokio::test]
#[expect(clippy::too_many_lines, reason = "feed, tracker and archive in one Python test")]
async fn a_category_stays_in_its_own_directory_from_feed_to_library() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mount = tmp.path().join("mount");
    let library = tmp.path().join("library");
    std::fs::create_dir_all(mount.join("embyx_in").join("rank")).expect("mkdir");
    let archive_config = nested_routes(&mount, &library);
    let ledger = Arc::new(FakeLedger::new());
    let cloud = Arc::new(RecordingCloud::default());

    let bodies: HashMap<String, Vec<u8>> = HashMap::from([
        (feed_url("Actor"), feed_xml(&[item("item-1", "ABC-123 release")])),
        // A javlibrary title: the id runs straight into the title.
        (feed_url("Rank"), feed_xml(&[item("item-2", "DEF-456中出し")])),
    ]);
    let rss = RssPipeline::new(RssParts {
        config: RssConfig {
            enabled: true,
            categories: vec![category("Actor", INBOX), category("Rank", NESTED_RANK_DIR)],
            ..RssConfig::default()
        },
        avid_parser: AvidParser::default(),
        subscriptions: Arc::new(FakeSubscriptions::new(vec![
            make_subscription(
                1,
                SubscriptionSpec {
                    category: "Actor".to_owned(),
                    ..SubscriptionSpec::default()
                },
            ),
            make_subscription(
                2,
                SubscriptionSpec {
                    category: "Rank".to_owned(),
                    ..SubscriptionSpec::default()
                },
            ),
        ])),
        fetch: Arc::new(FakeFetch {
            answer: Box::new(move |url| Ok(bodies[url].clone())),
            queued: Mutex::new(None),
            calls: Mutex::default(),
        }),
        cloud: Arc::clone(&cloud) as Arc<dyn OfflineFiles>,
        sukebei: Arc::new(FakeSukebei {
            answers: HashMap::from([
                ("ABC-123".to_owned(), magnet_actor()),
                ("DEF-456".to_owned(), magnet_rank()),
            ]),
            calls: Mutex::default(),
        }),
        javbus: Arc::new(FakeJavBus {
            answers: HashMap::new(),
        }),
        ledger: Arc::clone(&ledger) as Arc<dyn crate::acquisitions::Ledger>,
        archiver: ArchivePipeline::new(archive_config.clone(), AvidParser::default()),
        on_submitted: None,
        cast_lookup: None,
        talent_lookup: None,
    })
    .expect("pipeline");

    rss.run(&routing_ctx()).await.expect("run");

    // Each category's magnet went to its own directory.
    let submitted: HashMap<String, String> = cloud.submitted.lock().expect("submitted").iter().cloned().collect();
    assert_eq!(
        submitted,
        HashMap::from([
            (magnet_actor(), INBOX.to_owned()),
            (magnet_rank(), NESTED_RANK_DIR.to_owned())
        ])
    );

    // CloudDrive finishes both downloads, each under the directory it was queued in.
    *cloud.tasks_by_dir.lock().expect("tasks") = HashMap::from([
        (
            INBOX.to_owned(),
            vec![offline_task("ABC-123 release", HASH_A, OfflineStatus::Finished)],
        ),
        (
            NESTED_RANK_DIR.to_owned(),
            vec![offline_task("DEF-456 release", HASH_B, OfflineStatus::Finished)],
        ),
    ]);
    write_video(&mount.join("embyx_in").join("ABC-123 release").join("ABC-123.mp4"));
    write_video(
        &mount
            .join("embyx_in")
            .join("rank")
            .join("DEF-456 release")
            .join("DEF-456.mp4"),
    );

    let submit: SubmitMagnet = Arc::new(|_avid, _magnet| Box::pin(std::future::ready(Ok(true))));
    let tracker = AcquisitionTracker::new(TrackerParts {
        ledger: Arc::clone(&ledger) as Arc<dyn crate::acquisitions::Ledger>,
        cloud: Arc::clone(&cloud) as Arc<dyn TrackerCloud>,
        settings: TrackerSettings::from_config(&archive_config, vec![INBOX.to_owned(), NESTED_RANK_DIR.to_owned()], ""),
        archiver: ArchivePipeline::new(archive_config, AvidParser::default()),
        submit_magnet: submit,
        release_date_lookup: None,
    });
    tracker.poll(&routing_ctx()).await.expect("poll");

    assert!(library.join("embyx").join("ABC").join("ABC-123.mp4").exists());
    assert!(
        library
            .join("embyx")
            .join("rank")
            .join("DEF")
            .join("DEF-456.mp4")
            .exists()
    );
    let state = ledger.state();
    assert_eq!(state.states.get("ABC-123"), Some(&AcquisitionState::Archived));
    assert_eq!(state.states.get("DEF-456"), Some(&AcquisitionState::Archived));
    // Each AVID carries its category's directory, so a later retry follows it.
    let task_dirs: HashMap<String, Option<String>> = state.task_dirs.clone().into_iter().collect();
    assert_eq!(
        task_dirs,
        HashMap::from([
            ("ABC-123".to_owned(), Some(INBOX.to_owned())),
            ("DEF-456".to_owned(), Some(NESTED_RANK_DIR.to_owned())),
        ])
    );
}

// py: tests/test_rss_category_routing.py::test_the_fallback_scan_leaves_the_nested_category_directory_intact
#[tokio::test]
async fn the_fallback_scan_leaves_the_nested_category_directory_intact() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mount = tmp.path().join("mount");
    let library = tmp.path().join("library");
    std::fs::create_dir_all(mount.join("embyx_in").join("rank")).expect("mkdir");
    let archive_config = nested_routes(&mount, &library);
    // A download still running under the rank directory, plus a settled one.
    write_video(
        &mount
            .join("embyx_in")
            .join("rank")
            .join("DEF-456 release")
            .join("DEF-456.mp4"),
    );
    write_video(&mount.join("embyx_in").join("ABC-123 release").join("ABC-123.mp4"));

    let ledger = Arc::new(FakeLedger::new());
    let mut scanner = ReconcileScanner::new(
        Arc::clone(&ledger) as Arc<dyn crate::acquisitions::Ledger>,
        ArchivePipeline::new(archive_config.clone(), AvidParser::default()),
        archive_config,
    );
    scanner.run(&routing_ctx()).await.expect("first scan");
    scanner.run(&routing_ctx()).await.expect("second scan"); // a folder needs two identical sightings

    assert!(mount.join("embyx_in").join("rank").is_dir());
    assert!(library.join("embyx").join("ABC").join("ABC-123.mp4").exists());
    assert!(
        library
            .join("embyx")
            .join("rank")
            .join("DEF")
            .join("DEF-456.mp4")
            .exists()
    );
}
