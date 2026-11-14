//! The reconcile scan: settling, the tracker's lock, reporting once, idempotence and nested
//! routes, on a temporary tree with the in-memory ledger.

use std::fs as stdfs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use embyx_core::avid::AvidParser;

use super::*;
use crate::testing::{FakeLedger, now_stub};

fn make_ctx() -> RunContext {
    RunContext::new("test-reconcile")
}

fn write_video(path: &Path, size: usize) {
    stdfs::create_dir_all(path.parent().expect("a parent")).expect("parent");
    stdfs::write(path, vec![b'x'; size]).expect("write");
}

struct Built {
    _dir: tempfile::TempDir,
    root: PathBuf,
    scanner: ReconcileScanner,
    ledger: Arc<FakeLedger>,
}

impl Built {
    fn path(&self, relative: &str) -> PathBuf {
        self.root.join(relative)
    }

    fn state(&self, avid: &str) -> Option<AcquisitionState> {
        self.ledger.state().states.get(avid).copied()
    }
}

/// `build(tmp_path, **overrides)`.
fn build(mapping: &[(&str, &str)]) -> Built {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_path_buf();
    let config = ArchiveConfig {
        enabled: true,
        src_dir: fs::path_text(&root.join("task")),
        dst_dir: fs::path_text(&root.join("library")),
        mapping: mapping
            .iter()
            .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
            .collect(),
        min_size_mb: 0,
        ..ArchiveConfig::default()
    };
    stdfs::create_dir_all(root.join("task/intake")).expect("intake");
    stdfs::create_dir_all(root.join("library/sorted")).expect("sorted");
    let archiver = ArchivePipeline::new(config.clone(), AvidParser::default());
    let ledger = Arc::new(FakeLedger::new());
    let scanner = ReconcileScanner::new(Arc::clone(&ledger) as Arc<dyn Ledger>, archiver, config);
    Built {
        _dir: dir,
        root,
        scanner,
        ledger,
    }
}

fn build_default() -> Built {
    build(&[("intake", "sorted")])
}

// py: tests/test_monitor_reconcile.py::test_a_folder_is_archived_only_once_it_has_settled
#[tokio::test]
async fn a_folder_is_archived_only_once_it_has_settled() {
    let mut built = build_default();
    let folder = built.path("task/intake/ABC-123 release");
    write_video(&folder.join("ABC-123.mp4"), 10);

    built.scanner.run(&make_ctx()).await.expect("run");
    // First sighting only measures it; nothing is moved yet.
    assert!(folder.exists());
    assert!(!built.path("library/sorted/ABC/ABC-123.mp4").exists());

    built.scanner.run(&make_ctx()).await.expect("run");

    assert!(built.path("library/sorted/ABC/ABC-123.mp4").exists());
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Archived));
    assert_eq!(
        built.ledger.state().sources.get("ABC-123").map(String::as_str),
        Some(AcquisitionSource::Reconcile.as_str())
    );
}

// py: tests/test_monitor_reconcile.py::test_a_growing_folder_is_left_for_the_next_pass
#[tokio::test]
async fn a_growing_folder_is_left_for_the_next_pass() {
    let mut built = build_default();
    let folder = built.path("task/intake/ABC-123 release");
    write_video(&folder.join("ABC-123.mp4"), 10);

    built.scanner.run(&make_ctx()).await.expect("run");
    write_video(&folder.join("ABC-123.mp4"), 999);
    built.scanner.run(&make_ctx()).await.expect("run");

    assert!(folder.exists());
    assert!(!built.path("library/sorted/ABC/ABC-123.mp4").exists());

    built.scanner.run(&make_ctx()).await.expect("run");
    assert!(built.path("library/sorted/ABC/ABC-123.mp4").exists());
}

// py: tests/test_monitor_reconcile.py::test_folders_the_tracker_owns_are_left_alone
#[tokio::test]
async fn folders_the_tracker_owns_are_left_alone() {
    let mut built = build_default();
    built
        .ledger
        .discover("ABC-123", "rss:Actor", now_stub(), Sighting::default())
        .await
        .expect("discover");
    built
        .ledger
        .state()
        .states
        .insert("ABC-123".to_owned(), AcquisitionState::Downloading);
    let folder = built.path("task/intake/ABC-123 release");
    write_video(&folder.join("ABC-123.mp4"), 10);

    built.scanner.run(&make_ctx()).await.expect("run");
    built.scanner.run(&make_ctx()).await.expect("run");

    assert!(folder.exists());
    assert!(!built.path("library/sorted/ABC/ABC-123.mp4").exists());
}

// py: tests/test_monitor_reconcile.py::test_a_folder_with_two_avids_is_reported_once_and_left_intact
#[tokio::test]
async fn a_folder_with_two_avids_is_reported_once_and_left_intact() {
    let mut built = build_default();
    let folder = built.path("task/intake/mixed");
    write_video(&folder.join("ABC-123.mp4"), 10);
    write_video(&folder.join("DEF-456.mp4"), 10);

    built.scanner.run(&make_ctx()).await.expect("run");
    let second = make_ctx();
    built.scanner.run(&second).await.expect("run");
    let third = make_ctx();
    built.scanner.run(&third).await.expect("run");

    // Two AVIDs in one folder means there is no key to park it under, so the scanner
    // remembers it instead; either way the warning fires once, not forever.
    assert_eq!(second.stats().get("needs_attention"), Some(1));
    assert_eq!(third.stats().get("needs_attention"), None);
    assert!(folder.join("ABC-123.mp4").exists());
}

// py: tests/test_monitor_reconcile.py::test_repeated_runs_are_idempotent
#[tokio::test]
async fn repeated_runs_are_idempotent() {
    let mut built = build_default();
    write_video(&built.path("task/intake/ABC-123 release/ABC-123.mp4"), 10);

    for _ in 0..4 {
        built.scanner.run(&make_ctx()).await.expect("run");
    }

    assert!(built.path("library/sorted/ABC/ABC-123.mp4").exists());
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Archived));
}

// py: tests/test_monitor_reconcile.py::test_a_missing_route_source_is_not_a_failure
#[tokio::test]
async fn a_missing_route_source_is_not_a_failure() {
    let mut built = build(&[("intake", "sorted"), ("gone", "elsewhere")]);

    let ctx = make_ctx();
    built.scanner.run(&ctx).await.expect("run");

    assert_eq!(ctx.stats().get("items_failed"), None);
}

// py: tests/test_monitor_reconcile.py::test_an_archived_avid_is_not_downgraded_by_a_later_scan
#[tokio::test]
async fn an_archived_avid_is_not_downgraded_by_a_later_scan() {
    let mut built = build_default();
    built
        .ledger
        .discover("ABC-123", "rss:Actor", now_stub(), Sighting::default())
        .await
        .expect("discover");
    built
        .ledger
        .state()
        .states
        .insert("ABC-123".to_owned(), AcquisitionState::Archived);
    let folder = built.path("task/intake/ABC-123 leftover");
    write_video(&folder.join("ABC-123.mp4"), 10);

    built.scanner.run(&make_ctx()).await.expect("run");
    built.scanner.run(&make_ctx()).await.expect("run");

    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Archived));
}

// py: tests/test_monitor_reconcile.py::test_a_nested_route_is_left_to_its_own_scan
#[tokio::test]
async fn a_nested_route_is_left_to_its_own_scan() {
    // The scan must not measure or archive another route's source directory.
    let mut built = build(&[("intake", "sorted"), ("intake/rank", "sorted/rank")]);
    let nested = built.path("task/intake/rank");
    write_video(&nested.join("DEF-456 release/DEF-456.mp4"), 10);

    built.scanner.run(&make_ctx()).await.expect("run");
    built.scanner.run(&make_ctx()).await.expect("run");

    assert!(nested.is_dir());
    assert!(built.path("library/sorted/rank/DEF/DEF-456.mp4").exists());
    assert_eq!(built.state("DEF-456"), Some(AcquisitionState::Archived));
}
