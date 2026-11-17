//! The move-in sweep: staged videos filed and recorded, sets, held AVIDs, unrouted or
//! unconfigured staging trees, unreadable names and idempotence.

use std::fs as stdfs;

use embyx_core::config::models::ArchiveConfig;

use super::*;
use crate::acquisitions::AcquisitionState;
use crate::testing::FakeLedger;

fn make_ctx() -> RunContext {
    RunContext::new("test-move-in")
}

fn write_video(path: &Path) {
    stdfs::create_dir_all(path.parent().expect("a parent")).expect("parent");
    stdfs::write(path, vec![b'x'; 10]).expect("write");
}

struct Built {
    _dir: tempfile::TempDir,
    root: PathBuf,
    sweeper: MoveInSweeper,
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

/// `build(tmp_path, task_dir_path=)`.
fn build(task_dir_path: &str) -> Built {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_path_buf();
    let config = ArchiveConfig {
        enabled: true,
        src_dir: fs::path_text(&root.join("task")),
        dst_dir: fs::path_text(&root.join("library")),
        mapping: vec![("intake".to_owned(), "sorted".to_owned())],
        min_size_mb: 0,
        ..ArchiveConfig::default()
    };
    stdfs::create_dir_all(root.join("task/intake")).expect("intake");
    stdfs::create_dir_all(root.join("library/sorted")).expect("sorted");
    stdfs::create_dir_all(root.join("movein")).expect("movein");
    let archiver = ArchivePipeline::new(config, AvidParser::default());
    let ledger = Arc::new(FakeLedger::new());
    let sweeper = MoveInSweeper::new(
        Arc::clone(&ledger) as Arc<dyn Ledger>,
        archiver,
        AvidParser::default(),
        root.join("movein"),
        task_dir_path.to_owned(),
    );
    Built {
        _dir: dir,
        root,
        sweeper,
        ledger,
    }
}

// py: tests/test_monitor_move_in.py::test_staged_videos_are_archived_and_recorded
#[tokio::test]
async fn staged_videos_are_archived_and_recorded() {
    let mut built = build("/task/intake");
    write_video(&built.path("movein/ABC/ABC-123.mp4"));
    write_video(&built.path("movein/DEF-456.mp4"));

    built.sweeper.run(&make_ctx()).await.expect("run");

    assert!(built.path("library/sorted/ABC/ABC-123.mp4").exists());
    assert!(built.path("library/sorted/DEF/DEF-456.mp4").exists());
    assert!(!built.path("movein/ABC/ABC-123.mp4").exists());
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Archived));
    assert_eq!(built.state("DEF-456"), Some(AcquisitionState::Archived));
    assert_eq!(
        built.ledger.state().sources.get("ABC-123").map(String::as_str),
        Some(AcquisitionSource::FillActor.as_str())
    );
    // The emptied brand directory stages nothing and is deliberately left behind.
    assert!(built.path("movein/ABC").is_dir());
}

// py: tests/test_monitor_move_in.py::test_a_multi_part_set_travels_together
#[tokio::test]
async fn a_multi_part_set_travels_together() {
    let mut built = build("/task/intake");
    write_video(&built.path("movein/ABC/ABC-123-cd1.mp4"));
    write_video(&built.path("movein/ABC/ABC-123-cd2.mp4"));

    built.sweeper.run(&make_ctx()).await.expect("run");

    assert!(built.path("library/sorted/ABC/ABC-123-cd1.mp4").exists());
    assert!(built.path("library/sorted/ABC/ABC-123-cd2.mp4").exists());
}

// py: tests/test_monitor_move_in.py::test_a_held_avid_is_parked_and_its_file_left
#[tokio::test]
async fn a_held_avid_is_parked_and_its_file_left() {
    let mut built = build("/task/intake");
    write_video(&built.path("library/sorted/ABC/ABC-123.mp4"));
    write_video(&built.path("movein/ABC/ABC-123.mp4"));

    let first = make_ctx();
    built.sweeper.run(&first).await.expect("run");
    let second = make_ctx();
    built.sweeper.run(&second).await.expect("run");

    assert!(built.path("movein/ABC/ABC-123.mp4").exists());
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::NeedsAttention));
    // The warning fires when the AVID is parked, not on every later pass.
    assert_eq!(first.stats().get("needs_attention"), Some(1));
    assert_eq!(second.stats().get("needs_attention"), None);
}

// py: tests/test_monitor_move_in.py::test_an_unrouted_staging_tree_moves_nothing
#[tokio::test]
async fn an_unrouted_staging_tree_moves_nothing() {
    let mut built = build("/somewhere/else");
    write_video(&built.path("movein/ABC/ABC-123.mp4"));

    built.sweeper.run(&make_ctx()).await.expect("run");

    assert!(built.path("movein/ABC/ABC-123.mp4").exists());
    assert!(built.ledger.state().states.is_empty());
}

// py: tests/test_monitor_move_in.py::test_a_missing_task_dir_moves_nothing
#[tokio::test]
async fn a_missing_task_dir_moves_nothing() {
    let mut built = build("");
    write_video(&built.path("movein/ABC-123.mp4"));

    built.sweeper.run(&make_ctx()).await.expect("run");

    assert!(built.path("movein/ABC-123.mp4").exists());
    assert!(built.ledger.state().states.is_empty());
}

// py: tests/test_monitor_move_in.py::test_an_unreadable_file_is_reported_once_and_left
#[tokio::test]
async fn an_unreadable_file_is_reported_once_and_left() {
    let mut built = build("/task/intake");
    write_video(&built.path("movein/holiday footage.mp4"));

    let first = make_ctx();
    built.sweeper.run(&first).await.expect("run");
    let second = make_ctx();
    built.sweeper.run(&second).await.expect("run");

    assert!(built.path("movein/holiday footage.mp4").exists());
    assert_eq!(first.stats().get("needs_attention"), Some(1));
    assert_eq!(second.stats().get("needs_attention"), None);
}

// py: tests/test_monitor_move_in.py::test_repeated_runs_are_idempotent
#[tokio::test]
async fn repeated_runs_are_idempotent() {
    let mut built = build("/task/intake");
    write_video(&built.path("movein/ABC/ABC-123.mp4"));

    for _ in 0..3 {
        built.sweeper.run(&make_ctx()).await.expect("run");
    }

    assert!(built.path("library/sorted/ABC/ABC-123.mp4").exists());
    assert_eq!(built.state("ABC-123"), Some(AcquisitionState::Archived));
}
