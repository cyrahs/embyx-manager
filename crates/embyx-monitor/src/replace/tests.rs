//! Ported from `tests/test_monitor_replace.py`: what a replacement (换版) plans and what reaches
//! CloudDrive, against the in-memory ledger and the manual tests' CloudDrive fake.

use std::path::Path;
use std::sync::{Arc, Mutex};

use embyx_core::avid::AvidParser;

use super::*;
use crate::manual::tests::{FakeCloud, HASH_A, INBOX, MAGNET_A, RANK_DIR, make_archiver, make_intake};
use crate::testing::{FakeLedger, now_stub};

const HASH_B: &str = "D23FE1C06BBA254A9DC9F519B335AA7C1367A88B";

fn magnet_b() -> String {
    format!("magnet:?xt=urn:btih:{HASH_B}")
}

struct Deps {
    ledger: Arc<FakeLedger>,
    cloud: Arc<FakeCloud>,
    submitted: Arc<Mutex<Vec<(String, String)>>>,
}

/// `make_replacer(tmp_path, ledger=, magnet=)`, with the default configured directories
/// (the inbox and the rank one) and a submit that succeeds.
fn make_replacer(root: &Path, ledger: Option<FakeLedger>, magnet: Option<&str>) -> (ReplacementSource, Deps) {
    let ledger = Arc::new(ledger.unwrap_or_default());
    // `FakeCloud()`: an empty tree, as Python's replacer tests use.
    let cloud = Arc::new(FakeCloud::default());
    let (intake, _) = make_intake(&ledger, &cloud, magnet);
    let submitted: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let recorder = Arc::clone(&submitted);
    let submit: SubmitMagnet = Arc::new(move |avid, link| {
        recorder.lock().expect("submitted").push((avid, link));
        Box::pin(std::future::ready(Ok(true)))
    });
    let root = root.to_path_buf();
    let source = ReplacementSource::new(
        Arc::clone(&ledger) as Arc<dyn Ledger>,
        Arc::new(move || Some(Arc::clone(&intake))),
        Arc::new(move || make_archiver(&root)),
        Arc::new(AvidParser::default),
        Arc::new(|| vec![INBOX.to_owned(), RANK_DIR.to_owned()]),
        submit,
    )
    .with_logger("test-replace");
    (
        source,
        Deps {
            ledger,
            cloud,
            submitted,
        },
    )
}

/// `hold(tmp_path, *relative)`.
fn hold(root: &Path, relative: &str) {
    let file = root.join("library").join(relative);
    std::fs::create_dir_all(file.parent().expect("a parent")).expect("parents");
    std::fs::write(file, b"old").expect("write");
}

fn candidate(magnet: &str, info_hash: &str, source: &str) -> MagnetCandidate {
    MagnetCandidate {
        magnet: magnet.to_owned(),
        info_hash: Some(info_hash.to_owned()),
        source: source.to_owned(),
        size_hint: None,
    }
}

fn code(result: Result<impl fmt::Debug, ReplaceError>) -> Option<&'static str> {
    result.expect_err("a refusal").code()
}

// py: tests/test_monitor_replace.py::test_plan_names_the_copy_to_delete_and_where_the_new_one_comes_from
#[tokio::test]
async fn plan_names_the_copy_to_delete_and_where_the_new_one_comes_from() {
    let tmp = tempfile::tempdir().expect("tempdir");
    hold(tmp.path(), "embyx/rank/IPZ/IPZ-661.mp4");
    let (source, _) = make_replacer(tmp.path(), None, Some(MAGNET_A));

    let plan = source.plan("ipz-661").await.expect("a plan");

    assert_eq!(plan.avid, "IPZ-661");
    assert_eq!(plan.holdings, vec!["embyx/rank/IPZ/IPZ-661.mp4".to_owned()]);
    assert_eq!(plan.state, None);
    // The rank route feeds the directory the old copy sits in.
    assert_eq!(plan.task_dir_path.as_deref(), Some(RANK_DIR));
    assert_eq!(plan.blocked, None);
}

// py: tests/test_monitor_replace.py::test_plan_reports_what_blocks_a_replacement
#[tokio::test]
async fn plan_reports_what_blocks_a_replacement() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let ledger = FakeLedger::with_known(&[("ABC-123", AcquisitionState::Downloading)]);
    hold(tmp.path(), "embyx/ABC/ABC-123.mp4");
    let (source, _) = make_replacer(tmp.path(), Some(ledger), Some(MAGNET_A));

    assert_eq!(
        source.plan("ABC-123").await.expect("plan").blocked,
        Some("acquisition_active")
    );
    assert_eq!(
        source.plan("XYZ-001").await.expect("plan").blocked,
        Some("not_in_library")
    );
    assert_eq!(code(source.plan("hello world").await), Some("unreadable_avid"));
}

// py: tests/test_monitor_replace.py::test_start_resolves_a_fresh_magnet_and_keeps_the_old_copy_until_filing
#[tokio::test]
async fn start_resolves_a_fresh_magnet_and_keeps_the_old_copy_until_filing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    hold(tmp.path(), "embyx/ABC/ABC-123.mp4");
    let (source, deps) = make_replacer(tmp.path(), None, Some(MAGNET_A));

    let result = source.start("ABC-123", None).await.expect("started");

    assert_eq!(result.outcome, ReplaceOutcome::Submitted);
    assert_eq!(result.record.state, AcquisitionState::Downloading);
    assert_eq!(result.record.replace_paths, vec!["embyx/ABC/ABC-123.mp4".to_owned()]);
    assert_eq!(
        deps.cloud.submitted(),
        vec![(vec![MAGNET_A.to_owned()], INBOX.to_owned())]
    );
    assert!(tmp.path().join("library/embyx/ABC/ABC-123.mp4").exists());
}

// py: tests/test_monitor_replace.py::test_start_submits_the_pasted_magnet_ahead_of_older_candidates
#[tokio::test]
async fn start_submits_the_pasted_magnet_ahead_of_older_candidates() {
    let tmp = tempfile::tempdir().expect("tempdir");
    hold(tmp.path(), "embyx/ABC/ABC-123.mp4");
    let ledger = FakeLedger::with_known(&[("ABC-123", AcquisitionState::Archived)]);
    ledger
        .add_attempts("ABC-123", &[candidate(MAGNET_A, HASH_A, "javbus")], now_stub())
        .await
        .expect("attempts");
    let (source, deps) = make_replacer(tmp.path(), Some(ledger), Some(MAGNET_A));

    let result = source.start("ABC-123", Some(&magnet_b())).await.expect("started");

    assert_eq!(result.outcome, ReplaceOutcome::Submitted);
    assert_eq!(
        *deps.submitted.lock().expect("submitted"),
        vec![("ABC-123".to_owned(), magnet_b())]
    );
    assert_eq!(
        deps.ledger.attempt_states("ABC-123"),
        vec![AttemptState::Pending, AttemptState::Submitted]
    );
    assert_eq!(deps.ledger.state().states["ABC-123"], AcquisitionState::Downloading);
}

// py: tests/test_monitor_replace.py::test_start_refuses_a_magnet_already_tried_before_writing_anything
#[tokio::test]
async fn start_refuses_a_magnet_already_tried_before_writing_anything() {
    let tmp = tempfile::tempdir().expect("tempdir");
    hold(tmp.path(), "embyx/ABC/ABC-123.mp4");
    let ledger = FakeLedger::with_known(&[("ABC-123", AcquisitionState::Archived)]);
    ledger
        .add_attempts("ABC-123", &[candidate(MAGNET_A, HASH_A, "sukebei")], now_stub())
        .await
        .expect("attempts");
    let (source, deps) = make_replacer(tmp.path(), Some(ledger), Some(MAGNET_A));

    for (magnet, expected) in [(MAGNET_A, "magnet_already_tried"), ("not a magnet", "unusable_magnet")] {
        assert_eq!(code(source.start("ABC-123", Some(magnet)).await), Some(expected));
    }
    let state = deps.ledger.state();
    assert_eq!(state.states["ABC-123"], AcquisitionState::Archived);
    assert!(state.replace_paths.is_empty());
}

// py: tests/test_monitor_replace.py::test_start_without_a_magnet_parks_the_replacement_for_later
#[tokio::test]
async fn start_without_a_magnet_parks_the_replacement_for_later() {
    let tmp = tempfile::tempdir().expect("tempdir");
    hold(tmp.path(), "embyx/ABC/ABC-123.mp4");
    let (source, deps) = make_replacer(tmp.path(), None, None);

    let result = source.start("ABC-123", None).await.expect("started");

    assert_eq!(result.outcome, ReplaceOutcome::NoMagnet);
    assert_eq!(result.record.state, AcquisitionState::ResolveFailed);
    assert!(result.record.next_action_at.is_some());
    // Still a replacement: a magnet pasted later finishes the job.
    assert_eq!(result.record.replace_paths, vec!["embyx/ABC/ABC-123.mp4".to_owned()]);
    assert!(deps.cloud.submitted().is_empty());
}

// py: tests/test_monitor_replace.py::test_start_refuses_a_title_the_tracker_is_working_on
#[tokio::test]
async fn start_refuses_a_title_the_tracker_is_working_on() {
    let tmp = tempfile::tempdir().expect("tempdir");
    hold(tmp.path(), "embyx/ABC/ABC-123.mp4");
    let ledger = FakeLedger::with_known(&[("ABC-123", AcquisitionState::Discovered)]);
    let (source, _) = make_replacer(tmp.path(), Some(ledger), Some(MAGNET_A));

    assert_eq!(code(source.start("ABC-123", None).await), Some("acquisition_active"));
}
