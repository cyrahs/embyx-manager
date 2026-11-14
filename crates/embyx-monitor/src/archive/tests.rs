//! The archive pipeline: the helpers, one folder's outcomes, priority routes, the full scan and
//! the library check, each on a temporary tree.

use std::fs as stdfs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use embyx_core::pyjson::Value;

use super::*;
use crate::fs::recording::{Fault, RecordingFs};
use crate::reports::Raised;

struct Tree {
    _dir: tempfile::TempDir,
    root: PathBuf,
}

impl Tree {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().to_path_buf();
        Self { _dir: dir, root }
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.root.join(relative)
    }
}

fn make_ctx() -> RunContext {
    RunContext::new("test-archive")
}

fn pairs(values: &[(&str, &str)]) -> Vec<(String, String)> {
    values.iter().map(|(a, b)| ((*a).to_owned(), (*b).to_owned())).collect()
}

fn base_config(tree: &Tree) -> ArchiveConfig {
    ArchiveConfig {
        enabled: true,
        src_dir: path_text(&tree.path("task")),
        dst_dir: path_text(&tree.path("library")),
        mapping: pairs(&[("intake", "sorted")]),
        min_size_mb: 0,
        brand_mapping: Vec::new(),
        ..ArchiveConfig::default()
    }
}

/// `make_pipeline(tmp_path, **overrides)`.
fn make_pipeline_with(tree: &Tree, fs: Option<Arc<dyn Fs>>, edit: impl FnOnce(&mut ArchiveConfig)) -> ArchivePipeline {
    let mut config = base_config(tree);
    edit(&mut config);
    stdfs::create_dir_all(tree.path("task/intake")).expect("intake");
    stdfs::create_dir_all(tree.path("library/sorted")).expect("sorted");
    match fs {
        Some(fs) => ArchivePipeline::with_fs(config, AvidParser::default(), fs),
        None => ArchivePipeline::new(config, AvidParser::default()),
    }
}

fn make_pipeline(tree: &Tree) -> ArchivePipeline {
    make_pipeline_with(tree, None, |_| {})
}

/// `make_priority_pipeline`: a priority route vip -> starred beside intake -> sorted.
fn make_priority_pipeline_with(tree: &Tree, edit: impl FnOnce(&mut ArchiveConfig)) -> ArchivePipeline {
    let pipeline = make_pipeline_with(tree, None, |config| {
        config.priority_mapping = pairs(&[("vip", "starred")]);
        edit(config);
    });
    stdfs::create_dir_all(tree.path("task/vip")).expect("vip");
    stdfs::create_dir_all(tree.path("library/starred")).expect("starred");
    pipeline
}

fn make_priority_pipeline(tree: &Tree) -> ArchivePipeline {
    make_priority_pipeline_with(tree, |_| {})
}

fn write_video_sized(path: &Path, size: usize) -> PathBuf {
    stdfs::create_dir_all(path.parent().expect("a parent")).expect("parent");
    stdfs::write(path, vec![b'x'; size]).expect("write");
    path.to_path_buf()
}

fn write_video(path: &Path) -> PathBuf {
    write_video_sized(path, 10)
}

fn archive(pipeline: &ArchivePipeline, folder: &Path) -> ArchiveResult {
    pipeline.archive_folder(folder, "sorted", &make_ctx(), false, "")
}

fn archive_expecting(pipeline: &ArchivePipeline, folder: &Path, expected_avid: &str) -> ArchiveResult {
    pipeline.archive_folder(folder, "sorted", &make_ctx(), false, expected_avid)
}

fn size_of(path: &Path) -> u64 {
    stdfs::metadata(path).expect("stat").len()
}

fn paths(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn videos(names: &[&str]) -> Vec<PathBuf> {
    names.iter().map(PathBuf::from).collect()
}

// -- helpers ------------------------------------------------------------------

// py: tests/test_monitor_archive.py::test_multi_part_video_check
#[test]
fn multi_part_video_check_reads_numbered_and_lettered_sets() {
    assert!(multi_part_video_check(&videos(&["a-1.mp4", "a-2.mp4"])));
    assert!(multi_part_video_check(&videos(&["a-A.mp4", "a-B.mp4"])));
    assert!(!multi_part_video_check(&videos(&["a-1.mp4", "b-extra.mp4"])));
    // A lone video is simply not a multi-part set; callers no longer guard the call.
    assert!(!multi_part_video_check(&videos(&["a.mp4"])));
    assert!(!multi_part_video_check(&[]));
}

// py: tests/test_monitor_archive.py::test_is_4k_video
#[test]
fn is_4k_video_reads_the_resolution_markers() {
    assert!(is_4k_video(Path::new("ABC-123 4k.mp4")));
    assert!(is_4k_video(Path::new("ABC-123-4K.mp4")));
    assert!(is_4k_video(Path::new("ABC-123-2160p.mp4")));
    assert!(!is_4k_video(Path::new("ABC-1234k.mp4")));
    assert!(!is_4k_video(Path::new("ABC-123.mp4")));
}

// py: tests/test_monitor_archive.py::test_normalize_copy_suffix
#[test]
fn normalize_copy_suffix_drops_the_copy_counter() {
    assert_eq!(normalize_copy_suffix("ABC-123 (1)"), "ABC-123");
    assert_eq!(normalize_copy_suffix("ABC-123"), "ABC-123");
}

/// Any exception, or the cancellation `_isolate` lets through.
#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error("{}", .0.message)]
    Raised(Raised),
    #[error(transparent)]
    Cancelled(RunCancelledError),
}

impl PyException for Failure {
    fn class_name(&self) -> &str {
        match self {
            Self::Raised(error) => error.class_name(),
            Self::Cancelled(error) => error.class_name(),
        }
    }

    fn py_repr(&self) -> String {
        match self {
            Self::Raised(error) => error.py_repr(),
            Self::Cancelled(error) => error.py_repr(),
        }
    }
}

impl Isolated for Failure {
    fn cancellation(&self) -> Option<RunCancelledError> {
        match self {
            Self::Raised(_) => None,
            Self::Cancelled(error) => Some(*error),
        }
    }
}

// py: tests/test_monitor_archive.py::test_isolate_contains_a_failure_and_records_it
#[test]
fn isolate_contains_a_failure_and_records_it() {
    let ctx = make_ctx();
    let failed: Result<(), Failure> = Err(Failure::Raised(Raised::new("RuntimeError", "nope")));
    assert_eq!(isolate(&ctx, "boom for thing", failed), Ok(None));
    assert_eq!(ctx.stats().get("items_failed"), Some(1));
}

// py: tests/test_monitor_archive.py::test_isolate_lets_cancellation_through
#[test]
fn isolate_lets_cancellation_through() {
    let ctx = make_ctx();
    let cancelled: Result<(), Failure> = Err(Failure::Cancelled(RunCancelledError));
    assert_eq!(isolate(&ctx, "boom", cancelled), Err(RunCancelledError));
    assert_eq!(ctx.stats().get("items_failed"), None);
}

// -- archiving one folder -----------------------------------------------------

// py: tests/test_monitor_archive.py::test_folder_is_archived_under_its_brand_and_then_deleted
#[test]
fn folder_is_archived_under_its_brand_and_then_deleted() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/ABC-123 release");
    write_video(&folder.join("ABC-123.mp4"));

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    assert_eq!(result.avid, "ABC-123");
    assert_eq!(result.archived_paths, paths(&["sorted/ABC/ABC-123.mp4"]));
    assert!(pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4").exists());
    assert!(!folder.exists());
}

// py: tests/test_monitor_archive.py::test_video_is_renamed_after_its_avid
#[test]
fn video_is_renamed_after_its_avid() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/release");
    write_video(&folder.join("[somesite] abc-123 1080p.mp4"));

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    assert!(pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_nested_video_is_found_rather_than_deleted_with_the_folder
#[test]
fn nested_video_is_found_rather_than_deleted_with_the_folder() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/ABC-123 release");
    write_video(&folder.join("inner/ABC-123.mp4"));

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    assert!(pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4").exists());
    assert!(!folder.exists());
}

// py: tests/test_monitor_archive.py::test_multi_part_set_becomes_cd_parts
#[test]
fn multi_part_set_becomes_cd_parts() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/set");
    write_video(&folder.join("ABC-123-1.mp4"));
    write_video(&folder.join("ABC-123-2.mp4"));

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    assert!(pipeline.dst_dir.join("sorted/ABC/ABC-123-cd1.mp4").exists());
    assert!(pipeline.dst_dir.join("sorted/ABC/ABC-123-cd2.mp4").exists());
    assert_eq!(
        result.archived_paths,
        paths(&["sorted/ABC/ABC-123-cd1.mp4", "sorted/ABC/ABC-123-cd2.mp4"])
    );
}

// py: tests/test_monitor_archive.py::test_high_resolution_cut_wins_over_the_smaller_one
#[test]
fn high_resolution_cut_wins_over_the_smaller_one() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/dual");
    write_video(&folder.join("ABC-123.mp4"));
    write_video(&folder.join("ABC-123 4k.mp4"));

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    assert_eq!(result.archived_paths, paths(&["sorted/ABC/ABC-123.mp4"]));
    assert!(!folder.exists());
}

// -- untangling several videos in one folder ----------------------------------
// The naming shapes below are taken verbatim from the reconcile backlog of 2026-08-14, where
// every entry was parked as "several unrelated videos in one folder"; sizes mirror the real
// files in MB.

// py: tests/test_monitor_archive.py::test_unnumbered_first_part_leads_its_numbered_siblings
#[test]
fn unnumbered_first_part_leads_its_numbered_siblings() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/[44x.me]rvg-080");
    write_video_sized(&folder.join("[44x.me]rvg-080.mp4"), 5486);
    write_video_sized(&folder.join("[44x.me]rvg-080-2.mp4"), 6165);
    write_video_sized(&folder.join("[44x.me]rvg-080-3.mp4"), 5825);
    write_video_sized(&folder.join("[44x.me]rvg-080-4.mp4"), 5824);

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    assert_eq!(
        result.archived_paths,
        paths(&[
            "sorted/RVG/RVG-080-cd1.mp4",
            "sorted/RVG/RVG-080-cd2.mp4",
            "sorted/RVG/RVG-080-cd3.mp4",
            "sorted/RVG/RVG-080-cd4.mp4",
        ])
    );
    // The unnumbered file is the first part, although it sorts last by name.
    assert_eq!(size_of(&pipeline.dst_dir.join("sorted/RVG/RVG-080-cd1.mp4")), 5486);
    assert_eq!(size_of(&pipeline.dst_dir.join("sorted/RVG/RVG-080-cd2.mp4")), 6165);
    assert!(!folder.exists());
}

// py: tests/test_monitor_archive.py::test_letter_parts_become_cd_parts
#[test]
fn letter_parts_become_cd_parts() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/PPT-057");
    write_video_sized(&folder.join("PPT-057A_HD.mp4"), 4712);
    write_video_sized(&folder.join("PPT-057B_HD.mp4"), 5865);
    write_video_sized(&folder.join("PPT-057C_HD.mp4"), 3764);

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    assert_eq!(
        result.archived_paths,
        paths(&[
            "sorted/PPT/PPT-057-cd1.mp4",
            "sorted/PPT/PPT-057-cd2.mp4",
            "sorted/PPT/PPT-057-cd3.mp4",
        ])
    );
}

// py: tests/test_monitor_archive.py::test_lowercase_letter_parts_become_cd_parts
#[test]
fn lowercase_letter_parts_become_cd_parts() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/dvdms-353");
    write_video_sized(&folder.join("dvdms-353a.mp4"), 1752);
    write_video_sized(&folder.join("dvdms-353b.mp4"), 1567);

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    assert_eq!(
        result.archived_paths,
        paths(&["sorted/DVDMS/DVDMS-353-cd1.mp4", "sorted/DVDMS/DVDMS-353-cd2.mp4"])
    );
}

// py: tests/test_monitor_archive.py::test_letter_parts_with_a_quality_marker_and_odd_container
#[test]
fn letter_parts_with_a_quality_marker_and_odd_container() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/[HD]XV923-WMV");
    write_video_sized(&folder.join("xv923A.HD.wmv"), 2650);
    write_video_sized(&folder.join("xv923B.HD.wmv"), 2508);

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    assert_eq!(
        result.archived_paths,
        paths(&["sorted/XV/XV-923-cd1.wmv", "sorted/XV/XV-923-cd2.wmv"])
    );
}

// py: tests/test_monitor_archive.py::test_mixed_index_separators_are_still_one_set
#[test]
fn mixed_index_separators_are_still_one_set() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/TBW-19");
    write_video_sized(&folder.join("TBW-19-1.mp4"), 465);
    write_video_sized(&folder.join("TBW-19_02.mp4"), 912);

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    // The id is spelled the catalog way on the way in: TBW-19 files land as TBW-019.
    assert_eq!(
        result.archived_paths,
        paths(&["sorted/TBW/TBW-019-cd1.mp4", "sorted/TBW/TBW-019-cd2.mp4"])
    );
    assert_eq!(size_of(&pipeline.dst_dir.join("sorted/TBW/TBW-019-cd1.mp4")), 465);
}

// py: tests/test_monitor_archive.py::test_phone_rip_is_dropped_for_the_main_video
#[test]
fn phone_rip_is_dropped_for_the_main_video() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/hjd2048.com-1129nhdtb201-h264");
    write_video_sized(&folder.join("hjd2048.com-1129nhdtb201-h264.mp4"), 6527);
    write_video_sized(&folder.join("nhdtb201-5.mp4"), 986);

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    assert_eq!(result.archived_paths, paths(&["sorted/NHDTB/NHDTB-201.mp4"]));
    assert_eq!(size_of(&pipeline.dst_dir.join("sorted/NHDTB/NHDTB-201.mp4")), 6527);
    assert!(!folder.exists());
}

// py: tests/test_monitor_archive.py::test_phone_rip_is_dropped_when_the_main_video_carries_a_tag
#[test]
fn phone_rip_is_dropped_when_the_main_video_carries_a_tag() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/OKAX449");
    write_video_sized(&folder.join("okax449-h264.mp4"), 7196);
    write_video_sized(&folder.join("okax449-5.mp4"), 1072);

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    assert_eq!(result.archived_paths, paths(&["sorted/OKAX/OKAX-449.mp4"]));
}

// py: tests/test_monitor_archive.py::test_oversized_phone_rip_still_needs_attention
#[test]
fn oversized_phone_rip_still_needs_attention() {
    // A "-5" file above half the main file's size is not confidently a rip.
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/OKAX449");
    write_video_sized(&folder.join("okax449-h264.mp4"), 1000);
    write_video_sized(&folder.join("okax449-5.mp4"), 800);

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::NeedsAttention);
    assert_eq!(result.reason, "several unrelated videos in one folder");
    assert!(folder.join("okax449-h264.mp4").exists());
    assert!(folder.join("okax449-5.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_vr_set_keeps_the_high_resolution_cut_of_each_part
#[test]
fn vr_set_keeps_the_high_resolution_cut_of_each_part() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/urvrsp-430");
    write_video_sized(&folder.join("4k2.com@urvrsp00430_1_8k.mp4"), 3467);
    write_video_sized(&folder.join("4k2.com@urvrsp00430_1_hq.mp4"), 1446);
    write_video_sized(&folder.join("4k2.com@urvrsp00430_2_8k.mp4"), 4067);
    write_video_sized(&folder.join("4k2.com@urvrsp00430_2_hq.mp4"), 4018);

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    assert_eq!(
        result.archived_paths,
        paths(&["sorted/URVRSP/URVRSP-430-cd1.mp4", "sorted/URVRSP/URVRSP-430-cd2.mp4"])
    );
    assert_eq!(
        size_of(&pipeline.dst_dir.join("sorted/URVRSP/URVRSP-430-cd1.mp4")),
        3467
    );
    assert_eq!(
        size_of(&pipeline.dst_dir.join("sorted/URVRSP/URVRSP-430-cd2.mp4")),
        4067
    );
}

// py: tests/test_monitor_archive.py::test_same_avid_videos_that_read_as_nothing_still_need_attention
#[test]
fn same_avid_videos_that_read_as_nothing_still_need_attention() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/mystery");
    write_video(&folder.join("ABC-123.mp4"));
    write_video(&folder.join("ABC-123 making of.mp4"));

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::NeedsAttention);
    assert_eq!(result.reason, "several unrelated videos in one folder");
    assert!(folder.join("ABC-123.mp4").exists());
    assert!(folder.join("ABC-123 making of.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_duplicate_copies_are_dropped
#[test]
fn duplicate_copies_are_dropped() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/dupes");
    write_video(&folder.join("ABC-123.mp4"));
    write_video(&folder.join("ABC-123 (1).mp4"));

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    assert!(pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_folder_without_a_qualifying_video_is_junk
#[test]
fn folder_without_a_qualifying_video_is_junk() {
    let tree = Tree::new();
    let pipeline = make_pipeline_with(&tree, None, |config| config.min_size_mb = 1);
    let folder = pipeline.src_dir.join("intake/ads");
    write_video_sized(&folder.join("ABC-123.mp4"), 100);

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Junk);
    // Junk is reported, not deleted here: the caller decides, because for a tracked download
    // it also means "try the next magnet".
    assert!(folder.exists());
}

// py: tests/test_monitor_archive.py::test_several_unrelated_videos_need_attention
#[test]
fn several_unrelated_videos_need_attention() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/mixed");
    write_video(&folder.join("ABC-123.mp4"));
    write_video(&folder.join("DEF-456.mp4"));

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::NeedsAttention);
    assert!(result.reason.contains("multiple avids"), "{}", result.reason);
    // Nothing is moved or deleted while the identity is in doubt.
    assert!(folder.join("ABC-123.mp4").exists());
    assert!(folder.join("DEF-456.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_unreadable_avid_needs_attention
#[test]
fn unreadable_avid_needs_attention() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/whatever");
    write_video(&folder.join("movie.mp4"));

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::NeedsAttention);
    assert!(folder.exists());
}

// py: tests/test_monitor_archive.py::test_folder_name_supplies_the_avid_when_the_file_name_does_not
#[test]
fn folder_name_supplies_the_avid_when_the_file_name_does_not() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/ABC-123");
    write_video(&folder.join("movie.mp4"));

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    assert!(pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_expected_avid_mismatch_is_reported_and_nothing_moves
#[test]
fn expected_avid_mismatch_is_reported_and_nothing_moves() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/surprise");
    write_video(&folder.join("DEF-456.mp4"));

    let result = archive_expecting(&pipeline, &folder, "ABC-123");

    assert_eq!(result.outcome, Outcome::NeedsAttention);
    assert_eq!(result.reason, "expected ABC-123 but found DEF-456");
    assert!(folder.join("DEF-456.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_expected_avid_match_archives_normally
#[test]
fn expected_avid_match_archives_normally() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/download");
    write_video(&folder.join("ABC-123.mp4"));

    let result = archive_expecting(&pipeline, &folder, "ABC-123");

    assert_eq!(result.outcome, Outcome::Archived);
    assert!(pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_expected_avid_accepts_single_zero_padding_and_names_from_expected
#[test]
fn expected_avid_accepts_single_zero_padding_and_names_from_expected() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/vrkm-1846");
    write_video(&folder.join("masex.tv@vrkm01846_1_8k.mp4"));
    write_video(&folder.join("masex.tv@vrkm01846_2_8k.mp4"));

    let result = archive_expecting(&pipeline, &folder, "VRKM-1846");

    assert_eq!(result.outcome, Outcome::Archived);
    assert_eq!(result.avid, "VRKM-1846");
    assert_eq!(
        result.archived_paths,
        paths(&["sorted/VRKM/VRKM-1846-cd1.mp4", "sorted/VRKM/VRKM-1846-cd2.mp4"])
    );
    assert!(!folder.exists());
}

// py: tests/test_monitor_archive.py::test_expected_avid_collapses_mixed_single_zero_padding
#[test]
fn expected_avid_collapses_mixed_single_zero_padding() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/download");
    write_video(&folder.join("VRKM-1846_1.mp4"));
    write_video(&folder.join("VRKM-01846_2.mp4"));

    let result = archive_expecting(&pipeline, &folder, "VRKM-1846");

    assert_eq!(result.outcome, Outcome::Archived);
    assert_eq!(
        result.archived_paths,
        paths(&["sorted/VRKM/VRKM-1846-cd1.mp4", "sorted/VRKM/VRKM-1846-cd2.mp4"])
    );
}

// py: tests/test_monitor_archive.py::test_expected_avid_matches_the_file_across_padding_widths
#[test]
fn expected_avid_matches_the_file_across_padding_widths() {
    // A JavBus-padded file name and a catalog-spelled expectation are one id.
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/download");
    write_video(&folder.join("ABC-0123.mp4"));

    let result = archive_expecting(&pipeline, &folder, "ABC-123");

    assert_eq!(result.outcome, Outcome::Archived);
    assert_eq!(result.archived_paths, paths(&["sorted/ABC/ABC-123.mp4"]));
}

// py: tests/test_monitor_archive.py::test_existing_destination_is_left_alone
#[test]
fn existing_destination_is_left_alone() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    write_video_sized(&pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4"), 999);
    let folder = pipeline.src_dir.join("intake/again");
    write_video(&folder.join("ABC-123.mp4"));

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::NeedsAttention);
    assert_eq!(size_of(&pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4")), 999);
    assert!(folder.exists());
}

// py: tests/test_monitor_archive.py::test_brand_mapping_overrides_the_route_destination
#[test]
fn brand_mapping_overrides_the_route_destination() {
    let tree = Tree::new();
    let pipeline = make_pipeline_with(&tree, None, |config| {
        config.brand_mapping = vec![("special".to_owned(), vec!["ABC".to_owned()])];
    });
    let folder = pipeline.src_dir.join("intake/release");
    write_video(&folder.join("ABC-123.mp4"));

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::Archived);
    assert!(pipeline.dst_dir.join("special/ABC/ABC-123.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_failure_while_archiving_is_reported_not_raised
#[test]
fn failure_while_archiving_is_reported_not_raised() {
    let tree = Tree::new();
    let (fs, records) = RecordingFs::collecting();
    let folder = tree.path("task/intake/release");
    let video = write_video(&folder.join("ABC-123.mp4"));
    // `Path.rename` raising OSError('mount went away') for the one rename this makes.
    fs.add_fault(Fault::message("rename", &video, "mount went away"));
    let pipeline = make_pipeline_with(&tree, Some(Arc::new(fs)), |_| {});

    let ctx = make_ctx();
    let result = pipeline.archive_folder(&folder, "sorted", &ctx, false, "");

    assert_eq!(result.outcome, Outcome::Failed);
    assert_eq!(result.reason, "mount went away");
    assert_eq!(ctx.stats().get("items_failed"), Some(1));
    assert!(folder.exists());
    assert!(video.exists());
    let renames = records
        .lock()
        .expect("records")
        .iter()
        .filter(|record| record.get("method").and_then(Value::as_str) == Some("rename"))
        .count();
    assert_eq!(renames, 1);
}

// -- priority routes ----------------------------------------------------------

// py: tests/test_monitor_archive.py::test_priority_route_promotes_a_copy_out_of_the_normal_destination
#[test]
fn priority_route_promotes_a_copy_out_of_the_normal_destination() {
    let tree = Tree::new();
    let pipeline = make_priority_pipeline(&tree);
    write_video(&pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4"));
    let folder = pipeline.src_dir.join("vip/release");
    write_video(&folder.join("ABC-123-4k.mp4"));

    let ctx = make_ctx();
    let result = pipeline.archive_folder(&folder, "starred", &ctx, true, "");

    assert!(pipeline.dst_dir.join("starred/ABC/ABC-123.mp4").exists());
    assert!(!pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4").exists());
    assert_eq!(ctx.stats().get("duplicates_promoted"), Some(1));
    // The promoted copy now occupies the destination, so the newly arrived one is left for a
    // human rather than overwriting what was just rescued.
    assert_eq!(result.outcome, Outcome::NeedsAttention);
    assert!(folder.exists());
}

// py: tests/test_monitor_archive.py::test_priority_promotion_moves_every_part_of_a_set
#[test]
fn priority_promotion_moves_every_part_of_a_set() {
    let tree = Tree::new();
    let pipeline = make_priority_pipeline(&tree);
    write_video(&pipeline.dst_dir.join("sorted/ABC/ABC-123-cd1.mp4"));
    write_video(&pipeline.dst_dir.join("sorted/ABC/ABC-123-cd2.mp4"));
    let folder = pipeline.src_dir.join("vip/release");
    write_video(&folder.join("DEF-456.mp4"));

    pipeline.archive_folder(&folder, "starred", &make_ctx(), true, "");
    // Promotion is keyed on the avid being archived, so a different avid moves nothing.
    assert!(pipeline.dst_dir.join("sorted/ABC/ABC-123-cd1.mp4").exists());

    let other = pipeline.src_dir.join("vip/abc");
    write_video(&other.join("ABC-123.mp4"));
    pipeline.archive_folder(&other, "starred", &make_ctx(), true, "");

    assert!(pipeline.dst_dir.join("starred/ABC/ABC-123-cd1.mp4").exists());
    assert!(pipeline.dst_dir.join("starred/ABC/ABC-123-cd2.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_promotion_does_not_claim_a_longer_avid
#[test]
fn promotion_does_not_claim_a_longer_avid() {
    let tree = Tree::new();
    let pipeline = make_priority_pipeline(&tree);
    write_video(&pipeline.dst_dir.join("sorted/ABC/ABC-1234.mp4"));
    let folder = pipeline.src_dir.join("vip/release");
    write_video(&folder.join("ABC-123.mp4"));

    pipeline.archive_folder(&folder, "starred", &make_ctx(), true, "");

    assert!(pipeline.dst_dir.join("sorted/ABC/ABC-1234.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_promotion_keeps_both_when_the_target_exists
#[test]
fn promotion_keeps_both_when_the_target_exists() {
    let tree = Tree::new();
    let pipeline = make_priority_pipeline(&tree);
    write_video_sized(&pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4"), 11);
    write_video_sized(&pipeline.dst_dir.join("starred/ABC/ABC-123.mp4"), 22);
    let folder = pipeline.src_dir.join("vip/release");
    write_video(&folder.join("ABC-123.mp4"));

    let result = pipeline.archive_folder(&folder, "starred", &make_ctx(), true, "");

    assert_eq!(result.outcome, Outcome::NeedsAttention);
    assert_eq!(size_of(&pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4")), 11);
    assert_eq!(size_of(&pipeline.dst_dir.join("starred/ABC/ABC-123.mp4")), 22);
}

// py: tests/test_monitor_archive.py::test_normal_route_skips_an_avid_the_priority_route_holds
#[test]
fn normal_route_skips_an_avid_the_priority_route_holds() {
    let tree = Tree::new();
    let pipeline = make_priority_pipeline(&tree);
    write_video(&pipeline.dst_dir.join("starred/ABC/ABC-123.mp4"));
    let folder = pipeline.src_dir.join("intake/release");
    write_video(&folder.join("ABC-123.mp4"));

    let result = pipeline.archive_folder(&folder, "sorted", &make_ctx(), false, "");

    assert_eq!(result.outcome, Outcome::NeedsAttention);
    assert!(!pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4").exists());
    assert!(folder.exists());
}

// py: tests/test_monitor_archive.py::test_brand_mapped_brands_ignore_the_priority_guards
#[test]
fn brand_mapped_brands_ignore_the_priority_guards() {
    let tree = Tree::new();
    let pipeline = make_priority_pipeline_with(&tree, |config| {
        config.brand_mapping = vec![("special".to_owned(), vec!["ABC".to_owned()])];
    });
    write_video(&pipeline.dst_dir.join("starred/ABC/ABC-123.mp4"));
    let folder = pipeline.src_dir.join("intake/release");
    write_video(&folder.join("ABC-123.mp4"));

    let result = pipeline.archive_folder(&folder, "sorted", &make_ctx(), false, "");

    assert_eq!(result.outcome, Outcome::Archived);
    assert!(pipeline.dst_dir.join("special/ABC/ABC-123.mp4").exists());
}

// -- the full scan ------------------------------------------------------------

// py: tests/test_monitor_archive.py::test_run_archives_every_route
#[test]
fn run_archives_every_route() {
    let tree = Tree::new();
    let pipeline = make_pipeline_with(&tree, None, |config| {
        config.mapping = pairs(&[("intake", "sorted"), ("extra", "other")]);
    });
    write_video(&pipeline.src_dir.join("intake/one/ABC-123.mp4"));
    write_video(&pipeline.src_dir.join("extra/two/DEF-456.mp4"));

    pipeline.run(&make_ctx()).expect("run");

    assert!(pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4").exists());
    assert!(pipeline.dst_dir.join("other/DEF/DEF-456.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_run_takes_priority_routes_first
#[test]
fn run_takes_priority_routes_first() {
    let tree = Tree::new();
    let pipeline = make_priority_pipeline(&tree);
    write_video(&pipeline.src_dir.join("intake/normal/ABC-123.mp4"));
    write_video(&pipeline.src_dir.join("vip/better/ABC-123.mp4"));

    pipeline.run(&make_ctx()).expect("run");

    // The priority copy lands first, so the normal one finds the avid taken.
    assert!(pipeline.dst_dir.join("starred/ABC/ABC-123.mp4").exists());
    assert!(!pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_run_files_loose_videos_left_in_a_route
#[test]
fn run_files_loose_videos_left_in_a_route() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    write_video(&pipeline.src_dir.join("intake/ABC-123.mp4"));

    pipeline.run(&make_ctx()).expect("run");

    assert!(pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_run_treats_a_missing_route_source_as_nothing_to_do
#[test]
fn run_treats_a_missing_route_source_as_nothing_to_do() {
    let tree = Tree::new();
    let pipeline = make_pipeline_with(&tree, None, |config| {
        config.mapping = pairs(&[("intake", "sorted"), ("gone", "elsewhere")]);
    });

    let ctx = make_ctx();
    pipeline.run(&ctx).expect("run");

    assert_eq!(ctx.stats().get("items_failed"), None);
}

// py: tests/test_monitor_archive.py::test_run_continues_after_one_folder_fails
#[test]
fn run_continues_after_one_folder_fails() {
    let tree = Tree::new();
    let (fs, _records) = RecordingFs::collecting();
    // `Path.rglob` raising OSError('mount went away') below aaa-bad.
    fs.add_fault(Fault::message(
        "rglob",
        tree.path("task/intake/aaa-bad"),
        "mount went away",
    ));
    let pipeline = make_pipeline_with(&tree, Some(Arc::new(fs)), |_| {});
    write_video(&pipeline.src_dir.join("intake/aaa-bad/ABC-123.mp4"));
    write_video(&pipeline.src_dir.join("intake/zzz-good/DEF-456.mp4"));

    let ctx = make_ctx();
    pipeline.run(&ctx).expect("run");

    assert_eq!(ctx.stats().get("items_failed"), Some(1));
    assert!(pipeline.dst_dir.join("sorted/DEF/DEF-456.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_run_stops_when_cancelled
#[test]
fn run_stops_when_cancelled() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    write_video(&pipeline.src_dir.join("intake/one/ABC-123.mp4"));
    let ctx = make_ctx();
    ctx.request_stop();

    assert_eq!(pipeline.run(&ctx), Err(ArchiveError::Cancelled(RunCancelledError)));
    assert!(pipeline.src_dir.join("intake/one/ABC-123.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_padded_content_id_archives_under_the_same_avid_rss_would_record
#[test]
fn padded_content_id_archives_under_the_same_avid_rss_would_record() {
    // RSS derived the AVID without un-padding while the archiver un-padded it, so the same
    // release could be recorded as ABC-00123 and filed as ABC-123. Both now read it through
    // one parser.
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let parser = AvidParser::default();
    let folder = pipeline.src_dir.join("intake/release");
    write_video(&folder.join("abc00123.mp4"));

    let result = archive(&pipeline, &folder);

    assert_eq!(parser.get_avid("[sukebei] abc00123 1080p"), "ABC-123");
    assert_eq!(result.avid, "ABC-123");
    assert!(pipeline.dst_dir.join("sorted/ABC/ABC-123.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_a_nested_route_is_not_archived_by_the_route_that_contains_it
#[test]
fn a_nested_route_is_not_archived_by_the_route_that_contains_it() {
    // A category downloading into a subdirectory of the shared inbox. Without the guard the
    // outer scan treats the inner route as one download: it would file a single video out of
    // everything below it and then delete the directory, taking the other downloads with it.
    let tree = Tree::new();
    let pipeline = make_pipeline_with(&tree, None, |config| {
        config.mapping = pairs(&[("intake", "sorted"), ("intake/rank", "sorted/rank")]);
    });
    let nested = tree.path("task/intake/rank");
    write_video(&nested.join("DEF-456 release/DEF-456.mp4"));
    write_video(&tree.path("task/intake/ABC-123 release/ABC-123.mp4"));

    pipeline.run(&make_ctx()).expect("run");

    assert!(tree.path("library/sorted/ABC/ABC-123.mp4").exists());
    // The nested route files its own downloads, into its own destination.
    assert!(tree.path("library/sorted/rank/DEF/DEF-456.mp4").exists());
    assert!(nested.is_dir());
}

// py: tests/test_monitor_archive.py::test_a_nested_route_is_skipped_even_when_it_is_deeper
#[test]
fn a_nested_route_is_skipped_even_when_it_is_deeper() {
    let tree = Tree::new();
    let pipeline = make_pipeline_with(&tree, None, |config| {
        config.mapping = pairs(&[("intake", "sorted"), ("intake/rank/new", "sorted/rank")]);
    });
    stdfs::create_dir_all(tree.path("task/intake/rank/new")).expect("mkdir");
    write_video(&tree.path("task/intake/rank/new/DEF-456 release/DEF-456.mp4"));

    pipeline.run(&make_ctx()).expect("run");

    // 'rank' holds a route without being one, so it is left alone too.
    assert!(tree.path("task/intake/rank/new").is_dir());
    assert!(tree.path("library/sorted/rank/DEF/DEF-456.mp4").exists());
}

// -- the library check ---------------------------------------------------------

// py: tests/test_monitor_archive.py::test_route_for_task_dir_matches_by_path_suffix
#[test]
fn route_for_task_dir_matches_by_path_suffix() {
    let tree = Tree::new();
    let pipeline = make_priority_pipeline(&tree);

    assert_eq!(
        pipeline.route_for_task_dir("/task/intake"),
        Some(("sorted".to_owned(), false))
    );
    assert_eq!(
        pipeline.route_for_task_dir("/task/vip"),
        Some(("starred".to_owned(), true))
    );
    assert_eq!(pipeline.route_for_task_dir("/task/unknown"), None);
    // The suffix must match in order, not merely share segments.
    assert_eq!(pipeline.route_for_task_dir("/intake/task"), None);
}

// py: tests/test_monitor_archive.py::test_library_holdings_reports_every_copy
#[test]
fn library_holdings_reports_every_copy() {
    let tree = Tree::new();
    let pipeline = make_priority_pipeline(&tree);
    write_video(&tree.path("library/sorted/ABC/ABC-123.mp4"));
    write_video(&tree.path("library/sorted/ABC/ABC-123-cd2.mp4"));

    let held = pipeline
        .library_holdings("ABC-123", &make_ctx(), Some("/task/intake"))
        .expect("holdings");

    assert_eq!(held, paths(&["sorted/ABC/ABC-123-cd2.mp4", "sorted/ABC/ABC-123.mp4"]));
}

// py: tests/test_monitor_archive.py::test_library_holdings_is_empty_for_an_absent_avid
#[test]
fn library_holdings_is_empty_for_an_absent_avid() {
    let tree = Tree::new();
    let pipeline = make_priority_pipeline(&tree);
    // A longer AVID sharing the prefix must not count as a copy.
    write_video(&tree.path("library/sorted/ABC/ABC-1234.mp4"));

    let held = pipeline
        .library_holdings("ABC-123", &make_ctx(), Some("/task/intake"))
        .expect("holdings");

    assert!(held.is_empty(), "{held:?}");
}

// py: tests/test_monitor_archive.py::test_a_priority_task_dir_promotes_the_normal_copy
#[test]
fn a_priority_task_dir_promotes_the_normal_copy() {
    let tree = Tree::new();
    let pipeline = make_priority_pipeline(&tree);
    let copy = write_video(&tree.path("library/sorted/ABC/ABC-123.mp4"));

    let held = pipeline
        .library_holdings("ABC-123", &make_ctx(), Some("/task/vip"))
        .expect("holdings");

    assert_eq!(held, paths(&["starred/ABC/ABC-123.mp4"]));
    assert!(!copy.exists());
    assert!(tree.path("library/starred/ABC/ABC-123.mp4").exists());
}

// py: tests/test_monitor_archive.py::test_a_normal_task_dir_leaves_the_priority_copy_where_it_is
#[test]
fn a_normal_task_dir_leaves_the_priority_copy_where_it_is() {
    let tree = Tree::new();
    let pipeline = make_priority_pipeline(&tree);
    let copy = write_video(&tree.path("library/starred/ABC/ABC-123.mp4"));

    let held = pipeline
        .library_holdings("ABC-123", &make_ctx(), Some("/task/intake"))
        .expect("holdings");

    assert_eq!(held, paths(&["starred/ABC/ABC-123.mp4"]));
    assert!(copy.exists());
}

// py: tests/test_monitor_archive.py::test_an_unmatched_task_dir_still_finds_the_copy
#[test]
fn an_unmatched_task_dir_still_finds_the_copy() {
    // No route to promote into, but the one-copy answer is route-independent.
    let tree = Tree::new();
    let pipeline = make_priority_pipeline(&tree);
    write_video(&tree.path("library/sorted/ABC/ABC-123.mp4"));

    let held = pipeline
        .library_holdings("ABC-123", &make_ctx(), Some("/elsewhere/entirely"))
        .expect("holdings");

    assert_eq!(held, paths(&["sorted/ABC/ABC-123.mp4"]));
}

#[test]
fn videos_with_names_that_are_not_utf8_need_attention_instead_of_being_compared() {
    // Lossy text would read these two as one name and its `(1)` copy, and delete the second
    // (D72); Python keeps the bytes apart and finds two unrelated videos. Either way nothing
    // may be deleted.
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let folder = pipeline.src_dir.join("intake/bytes");
    let first = write_video(&folder.join(OsStr::from_bytes(b"\xfeABC-123.mp4")));
    let second = write_video(&folder.join(OsStr::from_bytes(b"\xffABC-123 (1).mp4")));

    let result = archive(&pipeline, &folder);

    assert_eq!(result.outcome, Outcome::NeedsAttention);
    assert_eq!(result.reason, "a video name is not UTF-8");
    assert!(first.exists());
    assert!(second.exists());
}

#[test]
fn promotion_keeps_a_file_name_that_is_not_utf8() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let tree = Tree::new();
    let pipeline = make_priority_pipeline(&tree);
    let name = OsStr::from_bytes(b"ABC-123-\xff.mp4");
    write_video(&tree.path("library/sorted/ABC").join(name));

    pipeline
        .library_holdings("ABC-123", &make_ctx(), Some("/task/vip"))
        .expect("holdings");

    assert!(tree.path("library/starred/ABC").join(name).exists());
}
