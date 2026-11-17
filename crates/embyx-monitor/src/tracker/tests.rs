//! The tracker against the in-memory ledger, fake `CloudDrive`s and a temporary tree: finished,
//! junk, errored, stalled and vanished downloads, the attempt cap, nested routes, clearing
//! archived tasks, and replacements (换版).

use std::fs as stdfs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use chrono::{DateTime, NaiveDate, Utc};
use embyx_clients::clouddrive::CloudFile;
use embyx_core::avid::AvidParser;

use super::*;
use crate::acquisitions::{MagnetCandidate, Sighting};
use crate::cloud::CloudListing;
use crate::reports::Raised;
use crate::testing::{FakeLedger, now_stub};

const HASH_A: &str = "C12FE1C06BBA254A9DC9F519B335AA7C1367A88A";
const HASH_B: &str = "D23FE1C06BBA254A9DC9F519B335AA7C1367A88B";
/// The tracker tests' own `HASH_C`.
const HASH_C: &str = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC";

const TASK_DIR: &str = "/115/task";
const RANK_DIR: &str = "/115/task/rank";

fn make_ctx() -> RunContext {
    RunContext::new("test-tracker")
}

fn offline_task(name: &str, info_hash: &str, status: OfflineStatus, progress: f64) -> OfflineTask {
    OfflineTask {
        name: name.to_owned(),
        size: 1024,
        url: format!("magnet:?xt=urn:btih:{info_hash}"),
        status,
        info_hash: info_hash.to_owned(),
        file_id: "1".to_owned(),
        add_time: 0,
        progress,
        peers: 3,
    }
}

fn downloading(name: &str, info_hash: &str) -> OfflineTask {
    offline_task(name, info_hash, OfflineStatus::Downloading, 10.0)
}

fn finished(name: &str, info_hash: &str) -> OfflineTask {
    offline_task(name, info_hash, OfflineStatus::Finished, 10.0)
}

fn errored(name: &str, info_hash: &str) -> OfflineTask {
    offline_task(name, info_hash, OfflineStatus::Error, 10.0)
}

#[derive(Debug, Default)]
struct CloudLog {
    listed: Vec<String>,
    refreshed: Vec<String>,
    removed: Vec<(String, String, bool)>,
    /// Every `remove_offline_files` batch, when the test replaced the method.
    remove_calls: Vec<Vec<String>>,
    deleted: Vec<String>,
}

/// `FakeCloud`, and `MountedCloud` when `mount` is set: a CloudDrive whose API tree is the
/// test's temporary directory as the mount shows it.
#[derive(Debug, Default)]
struct FakeCloud {
    tasks_by_dir: Vec<(String, Vec<OfflineTask>)>,
    log: Mutex<CloudLog>,
    mount: Option<PathBuf>,
    /// `MountedCloud(deletes=)`: whether a deletion reaches the mount.
    deletes: bool,
    /// The replaced `remove_offline_files` of the batching test: the second call raises.
    failing_remove: bool,
}

impl FakeCloud {
    fn new(tasks: Vec<OfflineTask>) -> Self {
        Self::by_dir(vec![(TASK_DIR.to_owned(), tasks)])
    }

    fn by_dir(tasks_by_dir: Vec<(String, Vec<OfflineTask>)>) -> Self {
        Self {
            tasks_by_dir,
            deletes: true,
            ..Self::default()
        }
    }

    fn log(&self) -> std::sync::MutexGuard<'_, CloudLog> {
        self.log.lock().expect("log")
    }
}

impl CloudListing for FakeCloud {
    fn list_directory<'a>(&'a self, api_dir: &'a str) -> BoxFuture<'a, Result<Vec<CloudFile>, SourceError>> {
        self.log().refreshed.push(api_dir.to_owned());
        let mut files = Vec::new();
        if let Some(root) = &self.mount {
            let local = root.join(api_dir.trim_start_matches('/'));
            if local.is_dir() {
                let mut children: Vec<PathBuf> = stdfs::read_dir(&local)
                    .expect("listing")
                    .map(|entry| entry.expect("entry").path())
                    .collect();
                children.sort();
                for child in children {
                    let name = child.file_name().expect("a name").to_string_lossy().into_owned();
                    files.push(CloudFile {
                        id: String::new(),
                        full_path: format!("{}/{name}", api_dir.trim_end_matches('/')),
                        name,
                        size: 0,
                        is_directory: child.is_dir(),
                        write_time: None,
                        hashes: std::collections::BTreeMap::new(),
                    });
                }
            }
        }
        Box::pin(std::future::ready(Ok(files)))
    }
}

impl TrackerCloud for FakeCloud {
    fn list_offline_files<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Vec<OfflineTask>, SourceError>> {
        self.log().listed.push(path.to_owned());
        let tasks = self
            .tasks_by_dir
            .iter()
            .find(|(dir, _)| dir == path)
            .map(|(_, tasks)| tasks.clone())
            .unwrap_or_default();
        Box::pin(std::future::ready(Ok(tasks)))
    }

    fn remove_offline_files<'a>(
        &'a self,
        info_hashes: &'a [String],
        path: &'a str,
        delete_files: bool,
    ) -> BoxFuture<'a, Result<(), SourceError>> {
        let mut log = self.log();
        let result = if self.failing_remove {
            log.remove_calls.push(info_hashes.to_vec());
            if log.remove_calls.len() == 2 {
                Err(SourceError::Raised(Raised::new("RuntimeError", "CloudDrive is busy")))
            } else {
                Ok(())
            }
        } else {
            log.removed.extend(
                info_hashes
                    .iter()
                    .map(|info_hash| (info_hash.clone(), path.to_owned(), delete_files)),
            );
            Ok(())
        };
        Box::pin(std::future::ready(result))
    }

    fn delete_files<'a>(&'a self, api_paths: &'a [String]) -> BoxFuture<'a, Result<(), SourceError>> {
        self.log().deleted.extend(api_paths.iter().cloned());
        if self.deletes
            && let Some(root) = &self.mount
        {
            for path in api_paths {
                stdfs::remove_file(root.join(path.trim_start_matches('/'))).expect("unlink");
            }
        }
        Box::pin(std::future::ready(Ok(())))
    }

    fn settle(&self) -> BoxFuture<'_, ()> {
        Box::pin(std::future::ready(()))
    }
}

fn write_video(path: &Path, size: usize) {
    stdfs::create_dir_all(path.parent().expect("a parent")).expect("parent");
    stdfs::write(path, vec![b'x'; size]).expect("write");
}

type Submitted = Arc<Mutex<Vec<(String, String)>>>;

/// `submit(avid, magnet)`: records, answers `ok`.
fn recording_submit(ok: bool) -> (SubmitMagnet, Submitted) {
    let submitted: Submitted = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&submitted);
    let submit: SubmitMagnet = Arc::new(move |avid, magnet| {
        sink.lock().expect("submitted").push((avid, magnet));
        Box::pin(std::future::ready(Ok(ok)))
    });
    (submit, submitted)
}

/// `submit` that fails the test: nothing should retry.
fn no_submit() -> SubmitMagnet {
    Arc::new(|_avid, _magnet| -> BoxFuture<'static, Result<bool, LedgerError>> {
        panic!("no magnet should be submitted")
    })
}

struct Built {
    dir: tempfile::TempDir,
    tracker: AcquisitionTracker,
    ledger: Arc<FakeLedger>,
    cloud: Arc<FakeCloud>,
    submitted: Submitted,
}

impl Built {
    fn path(&self, relative: &str) -> PathBuf {
        self.dir.path().join(relative)
    }

    fn state(&self, avid: &str) -> Option<AcquisitionState> {
        self.ledger.state().states.get(avid).copied()
    }

    fn submitted(&self) -> Vec<(String, String)> {
        self.submitted.lock().expect("submitted").clone()
    }

    async fn poll(&self) -> RunContext {
        let ctx = make_ctx();
        self.tracker.poll(&ctx).await.expect("poll");
        ctx
    }
}

fn routes(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(source, dst)| ((*source).to_owned(), (*dst).to_owned()))
        .collect()
}

fn tracker_over(
    config: ArchiveConfig,
    cloud: Arc<FakeCloud>,
    task_dirs: &[&str],
    submit: SubmitMagnet,
) -> (AcquisitionTracker, Arc<FakeLedger>) {
    let ledger = Arc::new(FakeLedger::new());
    let settings = TrackerSettings::from_config(&config, task_dirs.iter().map(|dir| (*dir).to_owned()).collect(), "");
    let tracker = AcquisitionTracker::new(TrackerParts {
        ledger: Arc::clone(&ledger) as Arc<dyn Ledger>,
        cloud: cloud as Arc<dyn TrackerCloud>,
        archiver: ArchivePipeline::new(config, AvidParser::default()),
        settings,
        submit_magnet: submit,
        release_date_lookup: None,
    });
    (tracker, ledger)
}

/// `build(tmp_path, tasks, submit_ok=, max_attempts=, min_size_mb=)` in a directory made
/// beforehand, with the library mount prefix `build_replacing` sets.
fn build_in(
    dir: tempfile::TempDir,
    cloud: FakeCloud,
    submit_ok: bool,
    max_attempts: i64,
    min_size_mb: i64,
    mount_prefix: &str,
) -> Built {
    let local = dir.path().join("task");
    stdfs::create_dir_all(local.join("downloads")).expect("downloads");
    stdfs::create_dir_all(dir.path().join("library/sorted")).expect("library");
    let config = ArchiveConfig {
        enabled: true,
        src_dir: path_text(&local),
        dst_dir: path_text(&dir.path().join("library")),
        mapping: routes(&[("downloads", "sorted")]),
        min_size_mb,
        max_attempts,
        ..ArchiveConfig::default()
    };
    let cloud = Arc::new(cloud);
    let (submit, submitted) = recording_submit(submit_ok);
    let ledger = Arc::new(FakeLedger::new());
    let tracker = AcquisitionTracker::new(TrackerParts {
        ledger: Arc::clone(&ledger) as Arc<dyn Ledger>,
        cloud: Arc::clone(&cloud) as Arc<dyn TrackerCloud>,
        settings: TrackerSettings::from_config(&config, vec![TASK_DIR.to_owned()], mount_prefix),
        archiver: ArchivePipeline::new(config, AvidParser::default()),
        submit_magnet: submit,
        release_date_lookup: None,
    });
    Built {
        dir,
        tracker,
        ledger,
        cloud,
        submitted,
    }
}

fn build_with(cloud: FakeCloud, submit_ok: bool, max_attempts: i64, min_size_mb: i64) -> Built {
    build_in(
        tempfile::tempdir().expect("tempdir"),
        cloud,
        submit_ok,
        max_attempts,
        min_size_mb,
        "",
    )
}

fn build(tasks: Vec<OfflineTask>) -> Built {
    build_with(FakeCloud::new(tasks), true, 5, 0)
}

fn magnet(info_hash: &str) -> String {
    format!("magnet:?xt=urn:btih:{info_hash}")
}

fn candidates(hashes: &[&str]) -> Vec<MagnetCandidate> {
    hashes
        .iter()
        .map(|hash| MagnetCandidate {
            magnet: magnet(hash),
            info_hash: Some((*hash).to_owned()),
            source: "sukebei".to_owned(),
            size_hint: None,
        })
        .collect()
}

/// `seed(ledger, avid, hashes)`: discovered, candidates recorded, the first submitted, the
/// AVID downloading.
async fn seed(ledger: &FakeLedger, avid: &str, hashes: &[&str]) {
    ledger
        .discover(avid, "rss:Actor", now_stub(), Sighting::default())
        .await
        .expect("discover");
    ledger
        .add_attempts(avid, &candidates(hashes), now_stub())
        .await
        .expect("add");
    ledger.claim_next_pending(avid, now_stub()).await.expect("claim");
    ledger
        .state()
        .states
        .insert(avid.to_owned(), AcquisitionState::Downloading);
}

fn set_attempt(ledger: &FakeLedger, avid: &str, index: usize, change: impl FnOnce(&mut MagnetAttemptRecord)) {
    change(&mut ledger.state().attempts.get_mut(avid).expect("attempts")[index]);
}

// py: tests/test_monitor_tracker.py::test_finished_download_is_archived_and_the_avid_closed
#[tokio::test]
async fn finished_download_is_archived_and_the_avid_closed() {
    let built = build(vec![finished("ABC-123 release", HASH_A)]);
    seed(&built.ledger, "ABC-123", &[HASH_A]).await;
    write_video(&built.path("task/downloads/ABC-123 release/ABC-123.mp4"), 10);

    let ctx = built.poll().await;

    assert!(built.path("library/sorted/ABC/ABC-123.mp4").exists());
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Archived));
    assert_eq!(built.ledger.attempt_states("ABC-123"), vec![AttemptState::Archived]);
    assert_eq!(ctx.stats().get("archived"), Some(1));
    // The mount is asked for the finished folder, the task directory first: a persistent
    // cache (115) never re-reads it on its own.
    assert_eq!(
        built.cloud.log().refreshed,
        vec![TASK_DIR.to_owned(), format!("{TASK_DIR}/ABC-123 release")]
    );
    // The task record goes, its files kept: they are already in the library.
    assert_eq!(
        built.cloud.log().removed,
        vec![(HASH_A.to_owned(), TASK_DIR.to_owned(), false)]
    );
    assert_eq!(ctx.stats().get("archived_tasks_cleared"), Some(1));
}

// py: tests/test_monitor_tracker.py::test_finished_download_uses_expected_avid_when_file_has_single_zero_padding
#[tokio::test]
async fn finished_download_uses_expected_avid_when_file_has_single_zero_padding() {
    let built = build(vec![finished("vrkm-1846", HASH_A)]);
    seed(&built.ledger, "VRKM-1846", &[HASH_A]).await;
    let folder = built.path("task/downloads/vrkm-1846");
    write_video(&folder.join("masex.tv@vrkm01846_1_8k.mp4"), 10);
    write_video(&folder.join("masex.tv@vrkm01846_2_8k.mp4"), 10);

    built.poll().await;

    assert_eq!(built.state("VRKM-1846"), Some(AcquisitionState::Archived));
    assert_eq!(built.ledger.attempt_states("VRKM-1846"), vec![AttemptState::Archived]);
    assert_eq!(
        built.ledger.state().archived_paths["VRKM-1846"],
        vec![
            "sorted/VRKM/VRKM-1846-cd1.mp4".to_owned(),
            "sorted/VRKM/VRKM-1846-cd2.mp4".to_owned()
        ]
    );
    assert!(!folder.exists());
}

// py: tests/test_monitor_tracker.py::test_a_finished_download_is_filed_by_the_route_that_holds_it
#[tokio::test]
async fn a_finished_download_is_filed_by_the_route_that_holds_it() {
    // The task directory is one of the archive routes; here a priority one.
    let dir = tempfile::tempdir().expect("tempdir");
    let local = dir.path().join("task");
    stdfs::create_dir_all(local.join("vip")).expect("vip");
    let config = ArchiveConfig {
        enabled: true,
        src_dir: path_text(&local),
        dst_dir: path_text(&dir.path().join("library")),
        mapping: routes(&[("downloads", "sorted")]),
        priority_mapping: routes(&[("vip", "starred")]),
        ..ArchiveConfig::default()
    };
    let cloud = Arc::new(FakeCloud::new(vec![finished("ABC-123 release", HASH_A)]));
    let (tracker, ledger) = tracker_over(config, cloud, &[TASK_DIR], no_submit());
    seed(&ledger, "ABC-123", &[HASH_A]).await;
    write_video(&local.join("vip/ABC-123 release/ABC-123.mp4"), 10);

    tracker.poll(&make_ctx()).await.expect("poll");

    assert!(dir.path().join("library/starred/ABC/ABC-123.mp4").exists());
    assert_eq!(ledger.state().states["ABC-123"], AcquisitionState::Archived);
}

// py: tests/test_monitor_tracker.py::test_a_finished_task_not_yet_on_the_mount_is_left_for_the_next_poll
#[tokio::test]
async fn a_finished_task_not_yet_on_the_mount_is_left_for_the_next_poll() {
    let built = build(vec![finished("ABC-123 release", HASH_A)]);
    seed(&built.ledger, "ABC-123", &[HASH_A]).await;
    // No folder written: the mount has not caught up with CloudDrive yet.

    built.poll().await;

    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Downloading));
    assert_eq!(built.ledger.attempt_states("ABC-123"), vec![AttemptState::Finished]);
    assert!(built.submitted().is_empty());
}

// py: tests/test_monitor_tracker.py::test_a_download_of_nothing_but_ads_moves_to_the_next_magnet
#[tokio::test]
async fn a_download_of_nothing_but_ads_moves_to_the_next_magnet() {
    let built = build_with(FakeCloud::new(vec![finished("junk release", HASH_A)]), true, 5, 1);
    seed(&built.ledger, "ABC-123", &[HASH_A, HASH_B]).await;
    write_video(&built.path("task/downloads/junk release/advert.mp4"), 10);

    let ctx = built.poll().await;

    assert_eq!(
        built.ledger.attempt_states("ABC-123"),
        vec![AttemptState::Junk, AttemptState::Submitted]
    );
    let avids: Vec<String> = built.submitted().into_iter().map(|(avid, _)| avid).collect();
    assert_eq!(avids, vec!["ABC-123".to_owned()]);
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Downloading));
    assert_eq!(ctx.stats().get("junk"), Some(1));
    // The junk folder goes with the task: nothing will come back for it.
    assert_eq!(
        built.cloud.log().removed,
        vec![(HASH_A.to_owned(), TASK_DIR.to_owned(), true)]
    );
}

// py: tests/test_monitor_tracker.py::test_errored_task_moves_to_the_next_magnet
#[tokio::test]
async fn errored_task_moves_to_the_next_magnet() {
    let built = build(vec![errored("x", HASH_A)]);
    seed(&built.ledger, "ABC-123", &[HASH_A, HASH_B]).await;

    let ctx = built.poll().await;

    assert_eq!(
        built.ledger.attempt_states("ABC-123"),
        vec![AttemptState::Error, AttemptState::Submitted]
    );
    assert_eq!(built.submitted().len(), 1);
    assert_eq!(ctx.stats().get("retried"), Some(1));
    // The failed task is dropped at CloudDrive along with whatever it downloaded.
    assert_eq!(
        built.cloud.log().removed,
        vec![(HASH_A.to_owned(), TASK_DIR.to_owned(), true)]
    );
}

// py: tests/test_monitor_tracker.py::test_progress_is_recorded_while_the_download_moves
#[tokio::test]
async fn progress_is_recorded_while_the_download_moves() {
    let built = build(vec![offline_task("x", HASH_A, OfflineStatus::Downloading, 42.5)]);
    seed(&built.ledger, "ABC-123", &[HASH_A, HASH_B]).await;

    built.poll().await;

    let first = built.ledger.state().attempts["ABC-123"][0].clone();
    assert!((first.progress.expect("progress") - 42.5).abs() < 1e-9);
    assert_eq!(first.state, AttemptState::Downloading);
    assert!(built.submitted().is_empty());
}

// py: tests/test_monitor_tracker.py::test_a_download_stuck_past_the_timeout_moves_on
#[tokio::test]
async fn a_download_stuck_past_the_timeout_moves_on() {
    let built = build(vec![downloading("x", HASH_A)]);
    seed(&built.ledger, "ABC-123", &[HASH_A, HASH_B]).await;
    // Already downloading, stuck at the same percentage since well before the timeout.
    set_attempt(&built.ledger, "ABC-123", 0, |attempt| {
        attempt.state = AttemptState::Downloading;
        attempt.progress = Some(10.0);
        attempt.updated_at = now_stub() - TimeDelta::days(3);
    });

    built.poll().await;

    assert_eq!(
        built.ledger.attempt_states("ABC-123"),
        vec![AttemptState::Stalled, AttemptState::Submitted]
    );
    assert_eq!(built.submitted().len(), 1);
    // A stalled task left at CloudDrive could finish later and drop a duplicate folder; it
    // goes, files included.
    assert_eq!(
        built.cloud.log().removed,
        vec![(HASH_A.to_owned(), TASK_DIR.to_owned(), true)]
    );
}

// py: tests/test_monitor_tracker.py::test_running_out_of_magnets_marks_the_avid_exhausted
#[tokio::test]
async fn running_out_of_magnets_marks_the_avid_exhausted() {
    let built = build(vec![errored("x", HASH_A)]);
    seed(&built.ledger, "ABC-123", &[HASH_A]).await;

    let ctx = built.poll().await;

    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Exhausted));
    assert!(built.ledger.state().next_action_at["ABC-123"].is_some());
    assert!(built.submitted().is_empty());
    assert_eq!(ctx.stats().get("exhausted"), Some(1));
}

struct Lookup {
    asked: Mutex<Vec<String>>,
    answer: NaiveDate,
}

impl ReleaseDateLookup for Lookup {
    fn lookup<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Option<NaiveDate>> {
        self.asked.lock().expect("asked").push(avid.to_owned());
        Box::pin(std::future::ready(Some(self.answer)))
    }
}

// py: tests/test_monitor_tracker.py::test_exhausting_a_row_without_a_release_date_looks_it_up_first
#[tokio::test]
async fn exhausting_a_row_without_a_release_date_looks_it_up_first() {
    let mut built = build(vec![errored("x", HASH_A)]);
    seed(&built.ledger, "ABC-123", &[HASH_A]).await;
    let answer = Utc::now().date_naive() + TimeDelta::days(60);
    let lookup = Arc::new(Lookup {
        asked: Mutex::new(Vec::new()),
        answer,
    });
    built
        .tracker
        .set_release_date_lookup(Some(Arc::clone(&lookup) as Arc<dyn ReleaseDateLookup>));

    built.poll().await;

    assert_eq!(*lookup.asked.lock().expect("asked"), vec!["ABC-123".to_owned()]);
    assert_eq!(built.ledger.state().release_dates["ABC-123"], Some(answer));
    // Two months out: the weekly preheat rather than the stall timeout.
    let next: DateTime<Utc> = built.ledger.state().next_action_at["ABC-123"].expect("next action");
    let wait = next - Utc::now();
    assert!(TimeDelta::days(6) + TimeDelta::hours(23) < wait && wait <= TimeDelta::days(7));
}

// py: tests/test_monitor_tracker.py::test_the_attempt_cap_stops_the_retry_chain
#[tokio::test]
async fn the_attempt_cap_stops_the_retry_chain() {
    let built = build_with(FakeCloud::new(vec![errored("x", HASH_A)]), true, 1, 0);
    seed(&built.ledger, "ABC-123", &[HASH_A, HASH_B]).await;

    built.poll().await;

    assert!(built.submitted().is_empty());
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Exhausted));
}

// py: tests/test_monitor_tracker.py::test_a_folder_that_is_not_what_we_ordered_is_parked
#[tokio::test]
async fn a_folder_that_is_not_what_we_ordered_is_parked() {
    let built = build(vec![finished("surprise", HASH_A)]);
    seed(&built.ledger, "ABC-123", &[HASH_A, HASH_B]).await;
    write_video(&built.path("task/downloads/surprise/DEF-456.mp4"), 10);

    built.poll().await;

    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::NeedsAttention));
    assert_eq!(
        built.ledger.state().notes["ABC-123"].as_deref(),
        Some("expected ABC-123 but found DEF-456")
    );
    // The claim is released rather than consumed, so it can be retried once fixed.
    assert_eq!(
        built.ledger.attempt_states("ABC-123"),
        vec![AttemptState::Finished, AttemptState::Pending]
    );
    assert!(built.submitted().is_empty());
    assert!(built.path("task/downloads/surprise/DEF-456.mp4").exists());
}

// py: tests/test_monitor_tracker.py::test_tasks_outside_the_ledger_are_left_alone
#[tokio::test]
async fn tasks_outside_the_ledger_are_left_alone() {
    let built = build(vec![downloading("ours", HASH_A), finished("someone elses", HASH_B)]);
    seed(&built.ledger, "ABC-123", &[HASH_A]).await;
    write_video(&built.path("task/downloads/someone elses/ZZZ-999.mp4"), 10);

    built.poll().await;

    assert!(built.path("task/downloads/someone elses/ZZZ-999.mp4").exists());
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Downloading));
    assert!(built.submitted().is_empty());
}

// py: tests/test_monitor_tracker.py::test_a_task_that_vanished_but_landed_is_still_archived
#[tokio::test]
async fn a_task_that_vanished_but_landed_is_still_archived() {
    // An operator cleared the offline record; the download itself is on the mount.
    let built = build(Vec::new());
    seed(&built.ledger, "ABC-123", &[HASH_A]).await;
    write_video(&built.path("task/downloads/ABC-123 release/ABC-123.mp4"), 10);

    built.poll().await;

    assert!(built.path("library/sorted/ABC/ABC-123.mp4").exists());
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Archived));
}

// py: tests/test_monitor_tracker.py::test_a_task_that_vanished_without_landing_moves_on
#[tokio::test]
async fn a_task_that_vanished_without_landing_moves_on() {
    let built = build(Vec::new());
    seed(&built.ledger, "ABC-123", &[HASH_A, HASH_B]).await;

    built.poll().await;

    assert_eq!(
        built.ledger.attempt_states("ABC-123"),
        vec![AttemptState::Lost, AttemptState::Submitted]
    );
    assert_eq!(built.submitted().len(), 1);
    // CloudDrive no longer lists the task, so there is nothing to remove.
    assert!(built.cloud.log().removed.is_empty());
}

// py: tests/test_monitor_tracker.py::test_a_cancelled_task_of_an_ignored_avid_is_not_resubmitted
#[tokio::test]
async fn a_cancelled_task_of_an_ignored_avid_is_not_resubmitted() {
    // The sweep still books the attempt as lost, but must not resurrect the AVID with its next
    // candidate magnet.
    let built = build(Vec::new());
    seed(&built.ledger, "ABC-123", &[HASH_A, HASH_B]).await;
    built
        .ledger
        .state()
        .states
        .insert("ABC-123".to_owned(), AcquisitionState::Ignored);

    built.poll().await;

    assert_eq!(
        built.ledger.attempt_states("ABC-123"),
        vec![AttemptState::Lost, AttemptState::Pending]
    );
    assert!(built.submitted().is_empty());
}

// py: tests/test_monitor_tracker.py::test_an_avid_an_operator_parked_is_not_touched
#[tokio::test]
async fn an_avid_an_operator_parked_is_not_touched() {
    let built = build(vec![finished("ABC-123 release", HASH_A)]);
    seed(&built.ledger, "ABC-123", &[HASH_A]).await;
    built
        .ledger
        .state()
        .states
        .insert("ABC-123".to_owned(), AcquisitionState::NeedsAttention);
    write_video(&built.path("task/downloads/ABC-123 release/ABC-123.mp4"), 10);

    built.poll().await;

    assert!(!built.path("library/sorted/ABC/ABC-123.mp4").exists());
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::NeedsAttention));
}

/// `build_nested`: a category directory nested inside the shared inbox, each its own route.
fn build_nested(tasks_by_dir: Vec<(String, Vec<OfflineTask>)>) -> Built {
    let dir = tempfile::tempdir().expect("tempdir");
    let local = dir.path().join("task");
    stdfs::create_dir_all(local.join("downloads/rank")).expect("rank");
    let config = ArchiveConfig {
        enabled: true,
        src_dir: path_text(&local),
        dst_dir: path_text(&dir.path().join("library")),
        mapping: routes(&[("downloads", "sorted"), ("downloads/rank", "sorted/rank")]),
        ..ArchiveConfig::default()
    };
    let cloud = Arc::new(FakeCloud::by_dir(tasks_by_dir));
    let (tracker, ledger) = tracker_over(config, Arc::clone(&cloud), &[TASK_DIR, RANK_DIR], no_submit());
    Built {
        dir,
        tracker,
        ledger,
        cloud,
        submitted: Arc::new(Mutex::new(Vec::new())),
    }
}

// py: tests/test_monitor_tracker.py::test_every_configured_directory_is_polled
#[tokio::test]
async fn every_configured_directory_is_polled() {
    let built = build_nested(vec![
        (TASK_DIR.to_owned(), vec![finished("ABC-123 release", HASH_A)]),
        (RANK_DIR.to_owned(), vec![finished("DEF-456 release", HASH_B)]),
    ]);
    seed(&built.ledger, "ABC-123", &[HASH_A]).await;
    seed(&built.ledger, "DEF-456", &[HASH_B]).await;
    write_video(&built.path("task/downloads/ABC-123 release/ABC-123.mp4"), 10);
    write_video(&built.path("task/downloads/rank/DEF-456 release/DEF-456.mp4"), 10);

    let ctx = built.poll().await;

    assert_eq!(built.cloud.log().listed, vec![TASK_DIR.to_owned(), RANK_DIR.to_owned()]);
    assert_eq!(ctx.stats().get("offline_tasks"), Some(2));
    // Each download is filed by the route that holds it.
    assert!(built.path("library/sorted/ABC/ABC-123.mp4").exists());
    assert!(built.path("library/sorted/rank/DEF/DEF-456.mp4").exists());
}

// py: tests/test_monitor_tracker.py::test_the_mount_refresh_uses_the_directory_the_task_was_listed_under
#[tokio::test]
async fn the_mount_refresh_uses_the_directory_the_task_was_listed_under() {
    let built = build_nested(vec![(RANK_DIR.to_owned(), vec![finished("DEF-456 release", HASH_B)])]);
    seed(&built.ledger, "DEF-456", &[HASH_B]).await;
    write_video(&built.path("task/downloads/rank/DEF-456 release/DEF-456.mp4"), 10);

    built.poll().await;

    assert_eq!(
        built.cloud.log().refreshed,
        vec![RANK_DIR.to_owned(), format!("{RANK_DIR}/DEF-456 release")]
    );
}

// py: tests/test_monitor_tracker.py::test_a_hash_listed_in_two_directories_is_advanced_once
#[tokio::test]
async fn a_hash_listed_in_two_directories_is_advanced_once() {
    let task = finished("ABC-123 release", HASH_A);
    let built = build_nested(vec![
        (TASK_DIR.to_owned(), vec![task.clone()]),
        (RANK_DIR.to_owned(), vec![task]),
    ]);
    seed(&built.ledger, "ABC-123", &[HASH_A]).await;
    write_video(&built.path("task/downloads/ABC-123 release/ABC-123.mp4"), 10);

    let ctx = built.poll().await;

    assert_eq!(ctx.stats().get("duplicate_offline_tasks"), Some(1));
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Archived));
    assert_eq!(
        built.cloud.log().refreshed,
        vec![TASK_DIR.to_owned(), format!("{TASK_DIR}/ABC-123 release")]
    );
}

// py: tests/test_monitor_tracker.py::test_no_configured_directory_does_not_conclude_the_downloads_in_flight
#[tokio::test]
async fn no_configured_directory_does_not_conclude_the_downloads_in_flight() {
    // An empty directory list is no evidence, not evidence of everything vanishing.
    let dir = tempfile::tempdir().expect("tempdir");
    let config = ArchiveConfig {
        enabled: true,
        src_dir: path_text(&dir.path().join("task")),
        dst_dir: path_text(&dir.path().join("library")),
        mapping: routes(&[("downloads", "sorted")]),
        ..ArchiveConfig::default()
    };
    let cloud = Arc::new(FakeCloud::new(Vec::new()));
    let (submit, _) = recording_submit(true);
    let (tracker, ledger) = tracker_over(config, Arc::clone(&cloud), &[], submit);
    seed(&ledger, "ABC-123", &[HASH_A]).await;

    let ctx = make_ctx();
    tracker.poll(&ctx).await.expect("poll");

    assert!(cloud.log().listed.is_empty());
    assert_eq!(ledger.attempt_states("ABC-123"), vec![AttemptState::Submitted]);
    assert!(ctx.log_tail().iter().any(|line| line.contains("no offline directory")));
}

// py: tests/test_monitor_tracker.py::test_only_finished_tasks_the_ledger_archived_are_cleared
#[tokio::test]
async fn only_finished_tasks_the_ledger_archived_are_cleared() {
    // A task added by hand, or one not archived yet, stays listed.
    let built = build(vec![
        finished("archived earlier", HASH_A),
        finished("someone elses", HASH_B),
        finished("not on the mount yet", HASH_C),
    ]);
    seed(&built.ledger, "ABC-123", &[HASH_A]).await;
    set_attempt(&built.ledger, "ABC-123", 0, |attempt| {
        attempt.state = AttemptState::Archived;
    });
    built
        .ledger
        .state()
        .states
        .insert("ABC-123".to_owned(), AcquisitionState::Archived);
    seed(&built.ledger, "DEF-456", &[HASH_C]).await;

    built.poll().await;

    assert_eq!(
        built.cloud.log().removed,
        vec![(HASH_A.to_owned(), TASK_DIR.to_owned(), false)]
    );
    // Waits for the mount.
    assert_eq!(built.ledger.attempt_states("DEF-456"), vec![AttemptState::Finished]);
}

// py: tests/test_monitor_tracker.py::test_archived_tasks_are_cleared_in_batches_and_a_failure_is_shrugged_off
#[tokio::test]
async fn archived_tasks_are_cleared_in_batches_and_a_failure_is_shrugged_off() {
    let hashes: Vec<String> = (0..5).map(|index| format!("{index:040X}")).collect();
    let tasks = hashes
        .iter()
        .enumerate()
        .map(|(index, info_hash)| finished(&format!("done {index}"), info_hash))
        .collect();
    let mut cloud = FakeCloud::new(tasks);
    cloud.failing_remove = true;
    let mut built = build_with(cloud, true, 5, 0);
    built.tracker.set_clear_batch(2);
    for (index, info_hash) in hashes.iter().enumerate() {
        let avid = format!("ABC-{index:03}");
        seed(&built.ledger, &avid, &[info_hash.as_str()]).await;
        set_attempt(&built.ledger, &avid, 0, |attempt| {
            attempt.state = AttemptState::Archived;
        });
    }

    let ctx = built.poll().await;

    assert_eq!(
        built.cloud.log().remove_calls,
        vec![hashes[0..2].to_vec(), hashes[2..4].to_vec(), hashes[4..5].to_vec()]
    );
    assert_eq!(ctx.stats().get("archived_tasks_cleared"), Some(3));
}

// -- replacements (换版) --------------------------------------------------------

/// `build_replacing(tmp_path, tasks, deletes=, mount_prefix=)`: the mounted fake over the
/// temporary directory, the library mount prefix set.
fn build_replacing(tasks: Vec<OfflineTask>, deletes: bool, mount_prefix: Option<&str>) -> Built {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cloud = FakeCloud::new(tasks);
    cloud.mount = Some(dir.path().to_path_buf());
    cloud.deletes = deletes;
    let prefix = mount_prefix.map_or_else(|| path_text(dir.path()), str::to_owned);
    build_in(dir, cloud, true, 5, 0, &prefix)
}

/// `seed_replacement`: an archived title whose first download (`HASH_C`) produced the copy
/// being replaced.
async fn seed_replacement(ledger: &FakeLedger, avid: &str, old_paths: &[&str], hashes: &[&str]) {
    seed(ledger, avid, &[HASH_C]).await;
    set_attempt(ledger, avid, 0, |attempt| attempt.state = AttemptState::Archived);
    ledger
        .state()
        .states
        .insert(avid.to_owned(), AcquisitionState::Archived);
    let old_paths: Vec<String> = old_paths.iter().map(|path| (*path).to_owned()).collect();
    ledger
        .begin_replacement(avid, "manual", TASK_DIR, &old_paths, now_stub())
        .await
        .expect("begin");
    ledger
        .add_attempts(avid, &candidates(hashes), now_stub())
        .await
        .expect("add");
    ledger.claim_next_pending(avid, now_stub()).await.expect("claim");
    ledger
        .state()
        .states
        .insert(avid.to_owned(), AcquisitionState::Downloading);
}

// py: tests/test_monitor_tracker.py::test_a_replacement_deletes_the_old_copy_before_filing_the_new_one
#[tokio::test]
async fn a_replacement_deletes_the_old_copy_before_filing_the_new_one() {
    let built = build_replacing(vec![finished("ABC-123 proper", HASH_A)], true, None);
    let old = built.path("library/sorted/ABC/ABC-123.mp4");
    write_video(&old, 3);
    seed_replacement(&built.ledger, "ABC-123", &["sorted/ABC/ABC-123.mp4"], &[HASH_A]).await;
    write_video(&built.path("task/downloads/ABC-123 proper/ABC-123.mp4"), 20);

    let ctx = built.poll().await;

    assert_eq!(
        built.cloud.log().deleted,
        vec!["/library/sorted/ABC/ABC-123.mp4".to_owned()]
    );
    assert_eq!(stdfs::read(&old).expect("new copy"), vec![b'x'; 20]);
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Archived));
    assert_eq!(
        built.ledger.attempt_states("ABC-123"),
        vec![AttemptState::Archived, AttemptState::Archived]
    );
    // The replacement is over once the new copy is in.
    assert!(!built.ledger.state().replace_paths.contains_key("ABC-123"));
    assert_eq!(ctx.stats().get("replaced_copies_deleted"), Some(1));
}

/// Rust only (D81): an old copy whose name is not UTF-8 was recorded lossily (U+FFFD), as
/// Python records it with surrogate escapes. Python finds it again, cannot have CloudDrive list
/// or delete it, and reports it still on the mount every poll; so does Rust, instead of filing
/// the new copy beside it.
#[cfg(unix)]
#[tokio::test]
async fn a_replaced_copy_whose_name_is_not_utf8_is_never_deleted_or_doubled() {
    use std::os::unix::ffi::OsStrExt as _;

    let built = build_replacing(vec![finished("ABC-123 proper", HASH_A)], true, None);
    let brand = built.path("library/sorted/ABC");
    let old = brand.join(std::ffi::OsStr::from_bytes(b"ABC-123-\xff.mp4"));
    write_video(&old, 3);
    seed_replacement(
        &built.ledger,
        "ABC-123",
        &["sorted/ABC/ABC-123-\u{FFFD}.mp4"],
        &[HASH_A],
    )
    .await;
    let new = built.path("task/downloads/ABC-123 proper/ABC-123.mp4");
    write_video(&new, 20);

    for _ in 0..2 {
        let ctx = built.poll().await;
        assert!(built.cloud.log().deleted.is_empty());
        assert_eq!(ctx.stats().get("replaced_copies_deleted"), None);
        assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Downloading));
        assert_eq!(
            built.ledger.attempt_states("ABC-123"),
            vec![AttemptState::Archived, AttemptState::Finished]
        );
    }
    assert!(old.exists());
    assert!(new.exists());
    assert!(!brand.join("ABC-123.mp4").exists());
    assert_eq!(
        built.ledger.state().replace_paths["ABC-123"],
        vec!["sorted/ABC/ABC-123-\u{FFFD}.mp4".to_owned()]
    );
}

// py: tests/test_monitor_tracker.py::test_a_replacement_into_parts_drops_the_single_old_file
#[tokio::test]
async fn a_replacement_into_parts_drops_the_single_old_file() {
    let built = build_replacing(vec![finished("ABC-123 parts", HASH_A)], true, None);
    let old = built.path("library/sorted/ABC/ABC-123.mp4");
    write_video(&old, 10);
    seed_replacement(&built.ledger, "ABC-123", &["sorted/ABC/ABC-123.mp4"], &[HASH_A]).await;
    let folder = built.path("task/downloads/ABC-123 parts");
    write_video(&folder.join("ABC-123-1.mp4"), 10);
    write_video(&folder.join("ABC-123-2.mp4"), 10);

    built.poll().await;

    assert!(!old.exists());
    let mut names: Vec<String> = stdfs::read_dir(old.parent().expect("brand"))
        .expect("listing")
        .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(names, vec!["ABC-123-cd1.mp4".to_owned(), "ABC-123-cd2.mp4".to_owned()]);
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Archived));
}

// py: tests/test_monitor_tracker.py::test_a_replacement_whose_download_is_the_wrong_title_keeps_the_old_copy
#[tokio::test]
async fn a_replacement_whose_download_is_the_wrong_title_keeps_the_old_copy() {
    let built = build_replacing(vec![finished("ABC-123 again", HASH_A)], true, None);
    let old = built.path("library/sorted/ABC/ABC-123.mp4");
    write_video(&old, 10);
    seed_replacement(&built.ledger, "ABC-123", &["sorted/ABC/ABC-123.mp4"], &[HASH_A]).await;
    write_video(&built.path("task/downloads/ABC-123 again/XYZ-999.mp4"), 10);

    built.poll().await;

    assert!(built.cloud.log().deleted.is_empty());
    assert!(old.exists());
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::NeedsAttention));
    assert_eq!(
        built.ledger.state().replace_paths["ABC-123"],
        vec!["sorted/ABC/ABC-123.mp4".to_owned()]
    );
}

// py: tests/test_monitor_tracker.py::test_a_replacement_waits_while_the_mount_still_shows_the_old_copy
#[tokio::test]
async fn a_replacement_waits_while_the_mount_still_shows_the_old_copy() {
    let built = build_replacing(vec![finished("ABC-123 proper", HASH_A)], false, None);
    let old = built.path("library/sorted/ABC/ABC-123.mp4");
    write_video(&old, 3);
    seed_replacement(&built.ledger, "ABC-123", &["sorted/ABC/ABC-123.mp4"], &[HASH_A]).await;
    let folder = built.path("task/downloads/ABC-123 proper");
    write_video(&folder.join("ABC-123.mp4"), 20);

    built.poll().await;

    // Released for the next poll, nothing filed over the old copy.
    assert_eq!(
        built.cloud.log().deleted,
        vec!["/library/sorted/ABC/ABC-123.mp4".to_owned()]
    );
    assert_eq!(stdfs::read(&old).expect("old copy"), b"xxx".to_vec());
    assert!(folder.join("ABC-123.mp4").exists());
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Downloading));
    assert_eq!(
        built.ledger.attempt_states("ABC-123").last(),
        Some(&AttemptState::Finished)
    );

    // The mount catches up; the next pass files it.
    stdfs::remove_file(&old).expect("unlink");
    built.poll().await;
    assert_eq!(stdfs::read(&old).expect("new copy"), vec![b'x'; 20]);
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Archived));
}

// py: tests/test_monitor_tracker.py::test_a_replacement_never_deletes_a_recorded_path_that_is_not_the_title
#[tokio::test]
async fn a_replacement_never_deletes_a_recorded_path_that_is_not_the_title() {
    let built = build_replacing(vec![finished("ABC-123 proper", HASH_A)], true, None);
    let other = built.path("library/sorted/ABC/ABC-1234.mp4");
    write_video(&other, 10);
    seed_replacement(
        &built.ledger,
        "ABC-123",
        &["sorted/ABC/ABC-1234.mp4", "../task/x.mp4"],
        &[HASH_A],
    )
    .await;
    write_video(&built.path("task/downloads/ABC-123 proper/ABC-123.mp4"), 10);

    built.poll().await;

    assert!(built.cloud.log().deleted.is_empty());
    assert!(other.exists());
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Archived));
}

// py: tests/test_monitor_tracker.py::test_a_replacement_without_a_mount_prefix_is_parked
#[tokio::test]
async fn a_replacement_without_a_mount_prefix_is_parked() {
    let built = build_replacing(vec![finished("ABC-123 proper", HASH_A)], true, Some(""));
    let old = built.path("library/sorted/ABC/ABC-123.mp4");
    write_video(&old, 10);
    seed_replacement(&built.ledger, "ABC-123", &["sorted/ABC/ABC-123.mp4"], &[HASH_A]).await;
    write_video(&built.path("task/downloads/ABC-123 proper/ABC-123.mp4"), 10);

    built.poll().await;

    assert!(built.cloud.log().deleted.is_empty());
    assert!(old.exists());
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::NeedsAttention));
}

// py: tests/test_monitor_tracker.py::test_a_replacement_counts_only_its_own_attempts_against_the_cap
#[tokio::test]
async fn a_replacement_counts_only_its_own_attempts_against_the_cap() {
    let mut built = build_replacing(vec![errored("ABC-123 bad", HASH_A)], true, None);
    built.tracker.settings_mut().max_attempts = 2;
    seed_replacement(&built.ledger, "ABC-123", &["sorted/ABC/ABC-123.mp4"], &[HASH_A, HASH_B]).await;

    built.poll().await;

    // The archived download before the replacement is the old copy's history: with it
    // counted, this first failure would already exhaust a cap of two.
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Downloading));
    assert_eq!(built.submitted(), vec![("ABC-123".to_owned(), magnet(HASH_B))]);
}

// -- Rust-only: helpers, failures and cancellation -------------------------------

#[test]
fn api_paths_are_taken_relative_to_the_mount_by_parts() {
    assert_eq!(
        api_path("/mnt/cd2/115/library/A/x.mp4", "/mnt/cd2"),
        Some("/115/library/A/x.mp4".to_owned())
    );
    assert_eq!(api_path("/mnt/cd2", "/mnt/cd2/"), Some("/".to_owned()));
    // `relative_to` compares parts, not text.
    assert_eq!(api_path("/mnt/cd20/x.mp4", "/mnt/cd2"), None);
    assert_eq!(api_path("/elsewhere/x.mp4", "/mnt/cd2"), None);
    assert_eq!(api_parent("/library/sorted/ABC/x.mp4"), "/library/sorted/ABC");
    assert_eq!(api_parent("/x.mp4"), "/");
}

#[test]
fn stall_reasons_read_as_python_formats_them() {
    assert_eq!(progress_text(None), "0");
    assert_eq!(progress_text(Some(-0.0)), "0");
    assert_eq!(progress_text(Some(42.5)), "42");
    assert_eq!(progress_text(Some(43.5)), "44");
    assert_eq!(progress_text(Some(99.6)), "100");
    assert_eq!(progress_text(Some(f64::NAN)), "nan");
    assert_eq!(py_days(TimeDelta::hours(71)), 2);
    assert_eq!(py_days(TimeDelta::hours(-1)), -1);
    assert_eq!(
        py_timedelta_hours(i64::MAX).expect_err("too large").0,
        "Python int too large to convert to C int"
    );
    assert_eq!(
        py_timedelta_hours(24_000_000_000).expect_err("too large").0,
        "days=1000000000; must have magnitude <= 999999999"
    );
}

#[tokio::test]
async fn a_listing_failure_ends_the_poll_before_anything_is_concluded() {
    struct Refusing;
    impl CloudListing for Refusing {
        fn list_directory<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Vec<CloudFile>, SourceError>> {
            Box::pin(std::future::ready(Ok(Vec::new())))
        }
    }
    impl TrackerCloud for Refusing {
        fn list_offline_files<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Vec<OfflineTask>, SourceError>> {
            Box::pin(std::future::ready(Err(SourceError::Raised(Raised::new(
                "RuntimeError",
                "listing outran the deadline",
            )))))
        }
        fn remove_offline_files<'a>(
            &'a self,
            _: &'a [String],
            _: &'a str,
            _: bool,
        ) -> BoxFuture<'a, Result<(), SourceError>> {
            panic!("nothing is removed")
        }
        fn delete_files<'a>(&'a self, _: &'a [String]) -> BoxFuture<'a, Result<(), SourceError>> {
            panic!("nothing is deleted")
        }
        fn settle(&self) -> BoxFuture<'_, ()> {
            Box::pin(std::future::ready(()))
        }
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let config = ArchiveConfig {
        enabled: true,
        src_dir: path_text(&dir.path().join("task")),
        dst_dir: path_text(&dir.path().join("library")),
        mapping: routes(&[("downloads", "sorted")]),
        ..ArchiveConfig::default()
    };
    let ledger = Arc::new(FakeLedger::new());
    let tracker = AcquisitionTracker::new(TrackerParts {
        ledger: Arc::clone(&ledger) as Arc<dyn Ledger>,
        cloud: Arc::new(Refusing),
        settings: TrackerSettings::from_config(&config, vec![TASK_DIR.to_owned()], ""),
        archiver: ArchivePipeline::new(config, AvidParser::default()),
        submit_magnet: no_submit(),
        release_date_lookup: None,
    });
    seed(&ledger, "ABC-123", &[HASH_A]).await;

    let error = tracker.poll(&make_ctx()).await.expect_err("the listing failed");

    assert_eq!(error.class_name(), "RuntimeError");
    // No listing is no evidence: the in-flight attempt is not swept as lost.
    assert_eq!(ledger.attempt_states("ABC-123"), vec![AttemptState::Submitted]);
}

#[tokio::test]
async fn a_stop_request_between_attempts_leaves_the_rest_for_the_next_poll() {
    let built = build(vec![finished("ABC-123 release", HASH_A), errored("x", HASH_B)]);
    seed(&built.ledger, "ABC-123", &[HASH_A]).await;
    seed(&built.ledger, "DEF-456", &[HASH_B, HASH_C]).await;
    write_video(&built.path("task/downloads/ABC-123 release/ABC-123.mp4"), 10);
    let ctx = make_ctx();
    ctx.request_stop();

    let error = built.tracker.poll(&ctx).await.expect_err("stopped");

    assert_eq!(error.class_name(), "RunCancelledError");
    assert_eq!(built.ledger.attempt_states("ABC-123"), vec![AttemptState::Submitted]);
    assert_eq!(
        built.ledger.attempt_states("DEF-456"),
        vec![AttemptState::Submitted, AttemptState::Pending]
    );
    assert!(built.cloud.log().listed.is_empty());
}
