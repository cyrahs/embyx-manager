//! Multi-part detection over a fake mapping tree, and the routes a title re-enters through.
//!
//! Ported from `tests/test_merge_detect.py`; the two tests of the titles endpoint are WP14's
//! (`merge/api.py`).

use std::fs;
use std::path::Path;

use embyx_core::config::models::ArchiveConfig;
use embyx_merge::detect::{
    SourceBasis, TitleProblem, resolve_source, scan_multipart, source_for_library_dir, source_for_task_dir,
    whole_copies,
};

const LIBRARY: &str = "/mnt/cd2/115/embyx";

fn routes(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(source, destination)| ((*source).to_owned(), (*destination).to_owned()))
        .collect()
}

fn archive() -> ArchiveConfig {
    ArchiveConfig {
        src_dir: "/mnt/cd2/115/embyx_in".to_owned(),
        dst_dir: LIBRARY.to_owned(),
        mapping: routes(&[
            ("hh", "type/hh"),
            ("rank", "rank"),
            ("rest", "rest"),
            ("special", "type/special"),
        ]),
        priority_mapping: routes(&[("vr", "type/vr"), ("clt", "actor/clt")]),
        ..ArchiveConfig::default()
    }
}

fn strm(root: &Path, directory: &str, name: &str, target: &str) {
    let path = root.join(directory).join(name);
    fs::create_dir_all(path.parent().expect("has a parent")).expect("mkdir");
    fs::write(path, format!("{target}\n")).expect("write strm");
}

fn parts(root: &Path, directory: &str, avid: &str, count: i32, library_dir: &str, skip: &[i32]) {
    let brand = avid.split('-').next().expect("has a brand");
    for index in 1..=count {
        if skip.contains(&index) {
            continue;
        }
        strm(
            root,
            directory,
            &format!("{avid}-cd{index}.strm"),
            &format!("{LIBRARY}/{library_dir}/{brand}/{avid}-cd{index}.mp4"),
        );
    }
}

// py: tests/test_merge_detect.py::test_scan_groups_parts_by_title_and_orders_by_part_count
#[test]
fn scan_groups_parts_by_title_and_orders_by_part_count() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    parts(root, "type/vr/SQTEVR/SQTEVR-009", "SQTEVR-009", 20, "type/vr", &[]);
    parts(root, "rank/ABP/ABP-123", "ABP-123", 3, "rank", &[]);
    strm(
        root,
        "rank/ABP/ABP-456",
        "ABP-456.strm",
        &format!("{LIBRARY}/rank/ABP/ABP-456.mp4"),
    );
    strm(
        root,
        "rank/ABP/ABP-789",
        "ABP-789-cd1.strm",
        &format!("{LIBRARY}/rank/ABP/ABP-789-cd1.mp4"),
    );

    let titles = scan_multipart(root, LIBRARY);

    let summary: Vec<(&str, usize)> = titles
        .iter()
        .map(|title| (title.avid.as_str(), title.part_count()))
        .collect();
    assert_eq!(summary, [("SQTEVR-009", 20), ("ABP-123", 3)]);
    let (big, small) = (&titles[0], &titles[1]);
    assert_eq!(
        big.parts.iter().map(|part| part.index).collect::<Vec<_>>(),
        (1..=20).collect::<Vec<_>>()
    );
    assert_eq!(
        big.parts[9].target,
        format!("{LIBRARY}/type/vr/SQTEVR/SQTEVR-009-cd10.mp4")
    );
    assert_eq!(big.directory, "type/vr/SQTEVR/SQTEVR-009");
    assert_eq!(
        (big.library_dir.as_deref(), big.brand.as_deref()),
        (Some("type/vr"), Some("SQTEVR"))
    );
    assert!(!big.stackable());
    assert!(big.complete());
    assert!(small.stackable());
}

// py: tests/test_merge_detect.py::test_scan_flags_a_title_the_library_also_holds_whole
#[test]
fn scan_flags_a_title_the_library_also_holds_whole() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    parts(root, "type/vr/VRKM/VRKM-1429", "VRKM-01429", 9, "type/vr", &[]);
    strm(
        root,
        "type/vr/VRKM/VRKM-1429",
        "VRKM-1429.strm",
        &format!("{LIBRARY}/type/vr/VRKM/VRKM-1429.mp4"),
    );
    parts(root, "type/vr/VRKM/VRKM-1430", "VRKM-1430", 9, "type/vr", &[]);
    strm(
        root,
        "type/vr/VRKM/VRKM-1430",
        "VRKM-1430-5.strm",
        &format!("{LIBRARY}/type/vr/VRKM/VRKM-1430-5.mp4"),
    );

    let mut problems: Vec<(String, Option<TitleProblem>)> = scan_multipart(root, LIBRARY)
        .into_iter()
        .map(|title| (title.avid, title.problem))
        .collect();
    problems.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(
        problems,
        [
            ("VRKM-01429".to_owned(), Some(TitleProblem::WholeCopy)),
            ("VRKM-1430".to_owned(), None)
        ]
    );
}

// py: tests/test_merge_detect.py::test_whole_copies_match_the_id_however_it_is_padded
#[test]
fn whole_copies_match_the_id_however_it_is_padded() {
    let names = [
        "AVOPVR-117-cd1.mp4",
        "AVOPVR-117.mp4",
        "AVOPVR-117.nfo",
        "AVOPVR-1170.mp4",
        "avopvr-0117.mkv",
    ];

    assert_eq!(whole_copies("AVOPVR-117", names), ["AVOPVR-117.mp4", "avopvr-0117.mkv"]);
    assert_eq!(
        whole_copies("SAVR-01135", ["SAVR-1135.mp4", "SAVR-1135-cd2.mp4"]),
        ["SAVR-1135.mp4"]
    );
    assert!(whole_copies("SAVR-01135", ["SAVR-01135-cd1.mp4"]).is_empty());
}

// py: tests/test_merge_detect.py::test_scan_reports_missing_parts
#[test]
fn scan_reports_missing_parts() {
    let tmp = tempfile::tempdir().expect("tempdir");
    parts(tmp.path(), "rest/XYZ/XYZ-001", "XYZ-001", 12, "rest", &[1, 7]);

    let titles = scan_multipart(tmp.path(), LIBRARY);

    assert_eq!(titles.len(), 1);
    let title = &titles[0];
    assert_eq!(title.missing, [1, 7]);
    assert!(!title.complete());
    assert!(!title.stackable());
}

// py: tests/test_merge_detect.py::test_scan_flags_targets_outside_the_library_and_scattered_parts
#[test]
fn scan_flags_targets_outside_the_library_and_scattered_parts() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    strm(root, "a/OUT-001", "OUT-001-cd1.strm", "/elsewhere/OUT/OUT-001-cd1.mp4");
    strm(root, "a/OUT-001", "OUT-001-cd2.strm", "/elsewhere/OUT/OUT-001-cd2.mp4");
    strm(
        root,
        "b/SPL-001",
        "SPL-001-cd1.strm",
        &format!("{LIBRARY}/rank/SPL/SPL-001-cd1.mp4"),
    );
    strm(
        root,
        "b/SPL-001",
        "SPL-001-cd2.strm",
        &format!("{LIBRARY}/rest/SPL/SPL-001-cd2.mp4"),
    );
    strm(root, "c/BAD-001", "BAD-001-cd1.strm", "not a path");
    strm(
        root,
        "c/BAD-001",
        "BAD-001-cd2.strm",
        &format!("{LIBRARY}/rank/BAD/BAD-001-cd2.mp4"),
    );

    let mut problems: Vec<(String, Option<TitleProblem>)> = scan_multipart(root, LIBRARY)
        .into_iter()
        .map(|title| (title.avid, title.problem))
        .collect();
    problems.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(
        problems,
        [
            ("BAD-001".to_owned(), Some(TitleProblem::UnreadableStrm)),
            ("OUT-001".to_owned(), Some(TitleProblem::OutsideLibrary)),
            ("SPL-001".to_owned(), Some(TitleProblem::ScatteredParts)),
        ]
    );
}

// py: tests/test_merge_detect.py::test_library_dir_names_the_route_when_exactly_one_feeds_it
#[test]
fn library_dir_names_the_route_when_exactly_one_feeds_it() {
    let archive = archive();
    assert_eq!(source_for_library_dir(&archive, Some("type/vr")).as_deref(), Some("vr"));
    assert_eq!(
        source_for_library_dir(&archive, Some("actor/clt")).as_deref(),
        Some("clt")
    );
    assert_eq!(source_for_library_dir(&archive, Some("rank")).as_deref(), Some("rank"));
    assert_eq!(source_for_library_dir(&archive, Some("type/unknown")), None);
    let mut shared = archive.clone();
    shared.mapping.push(("hh2".to_owned(), "type/hh".to_owned()));
    assert_eq!(source_for_library_dir(&shared, Some("type/hh")), None);
}

// py: tests/test_merge_detect.py::test_task_dir_matches_a_route_by_path_suffix
#[test]
fn task_dir_matches_a_route_by_path_suffix() {
    let archive = archive();
    assert_eq!(
        source_for_task_dir(&archive, Some("/115/embyx_in/clt")).as_deref(),
        Some("clt")
    );
    assert_eq!(
        source_for_task_dir(&archive, Some("/115/embyx_in/vr/")).as_deref(),
        Some("vr")
    );
    assert_eq!(source_for_task_dir(&archive, Some("/115/other")), None);
    assert_eq!(source_for_task_dir(&archive, None), None);
}

// py: tests/test_merge_detect.py::test_resolve_source_prefers_the_library_and_falls_back_to_the_ledger
#[test]
fn resolve_source_prefers_the_library_and_falls_back_to_the_ledger() {
    let tmp = tempfile::tempdir().expect("tempdir");
    parts(tmp.path(), "x/ABC-001", "ABC-001", 2, "type/vr", &[]);
    parts(tmp.path(), "y/DEF-001", "DEF-001", 2, "type/brand-only", &[]);
    let titles = scan_multipart(tmp.path(), LIBRARY);
    let by_avid = |avid: &str| titles.iter().find(|title| title.avid == avid).expect("scanned");
    let archive = archive();

    let library = resolve_source(&archive, by_avid("ABC-001"), Some("/115/embyx_in/clt"));
    let ledger = resolve_source(&archive, by_avid("DEF-001"), Some("/115/embyx_in/clt"));
    let unknown = resolve_source(&archive, by_avid("DEF-001"), None);

    assert_eq!(
        (library.source.as_deref(), library.basis),
        (Some("vr"), Some(SourceBasis::Library))
    );
    assert_eq!(
        (ledger.source.as_deref(), ledger.basis),
        (Some("clt"), Some(SourceBasis::Ledger))
    );
    assert_eq!((unknown.source, unknown.basis), (None, None));
}
