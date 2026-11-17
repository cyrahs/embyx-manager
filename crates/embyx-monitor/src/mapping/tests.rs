//! The mapping pipeline on temporary trees (`tests/test_monitor_mapping.py`), the name rules
//! and file-system calls against `goldens/monitor/mapping_*.json`, and names that are not
//! UTF-8.

use std::ffi::OsString;
use std::fs as stdfs;
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use embyx_core::golden;

use super::*;
use crate::fs::recording::{Fault, RecordingFs};

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
}

fn make_ctx() -> RunContext {
    RunContext::new("test-mapping")
}

/// `make_pipeline(tmp_path)`, optionally over a recording file system.
fn make_pipeline_with(tree: &Tree, fs: Option<Arc<dyn Fs>>) -> MappingPipeline {
    let config = MappingConfig {
        enabled: true,
        src_dir: path_text(&tree.root.join("flat")),
        dst_dir: path_text(&tree.root.join("emby")),
        ..MappingConfig::default()
    };
    let _ = stdfs::create_dir(tree.root.join("flat"));
    match fs {
        Some(fs) => MappingPipeline::with_fs(config, AvidParser::default(), fs),
        None => MappingPipeline::new(config, AvidParser::default()),
    }
}

fn make_pipeline(tree: &Tree) -> MappingPipeline {
    make_pipeline_with(tree, None)
}

fn recording() -> Arc<RecordingFs> {
    Arc::new(RecordingFs::collecting().0)
}

fn write_strm(path: &Path, content: &str) -> PathBuf {
    stdfs::create_dir_all(path.parent().expect("parent")).expect("parents");
    stdfs::write(path, content).expect("write");
    path.to_path_buf()
}

fn strm(path: &Path) -> PathBuf {
    write_strm(path, "/cloud/video.mp4")
}

fn touch(directory: &Path, names: &[&str]) {
    stdfs::create_dir_all(directory).expect("directory");
    for name in names {
        stdfs::write(directory.join(name), "x").expect("write");
    }
}

fn names(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = stdfs::read_dir(directory)
        .expect("list")
        .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

fn any_strm(root: &Path) -> bool {
    !crate::fs::glob_recursive(&LocalFs, root, &strm_glob, 1).is_empty()
}

// py: tests/test_monitor_mapping.py::test_full_sync_maps_strm_into_per_title_directory
#[test]
fn full_sync_maps_strm_into_per_title_directory() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    strm(&pipeline.src_dir.join("brand/ABC/ABC-123.strm"));

    let ctx = make_ctx();
    pipeline.run_full(&ctx).expect("run_full");

    let mapped = pipeline.dst_dir.join("brand/ABC/ABC-123/ABC-123.strm");
    assert_eq!(stdfs::read_to_string(mapped).expect("read"), "/cloud/video.mp4");
    assert_eq!(ctx.stats().get("files_updated"), Some(1));
}

// py: tests/test_monitor_mapping.py::test_full_sync_skips_unchanged_files
#[test]
fn full_sync_skips_unchanged_files() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    strm(&pipeline.src_dir.join("ABC-123.strm"));
    pipeline.run_full(&make_ctx()).expect("first run");

    let ctx = make_ctx();
    pipeline.run_full(&ctx).expect("second run");

    assert_eq!(ctx.stats().get("files_updated"), None);
    assert_eq!(ctx.stats().get("files_skipped"), Some(1));
}

// py: tests/test_monitor_mapping.py::test_full_sync_deletes_strays_and_empty_dirs
#[test]
fn full_sync_deletes_strays_and_empty_dirs() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let source = strm(&pipeline.src_dir.join("brand/ABC-123.strm"));
    pipeline.run_full(&make_ctx()).expect("first run");
    stdfs::remove_file(source).expect("remove");

    let ctx = make_ctx();
    pipeline.run_full(&ctx).expect("second run");

    assert!(!pipeline.dst_dir.join("brand").exists());
    assert_eq!(ctx.stats().get("files_deleted"), Some(1));
    assert!(ctx.stats().get("dirs_deleted").expect("dirs_deleted") >= 1);
}

// py: tests/test_monitor_mapping.py::test_full_sync_overwrites_when_source_newer
#[test]
fn full_sync_overwrites_when_source_newer() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let source = write_strm(&pipeline.src_dir.join("ABC-123.strm"), "old");
    pipeline.run_full(&make_ctx()).expect("first run");
    std::thread::sleep(Duration::from_millis(10));
    stdfs::write(&source, "new").expect("write");
    let future = SystemTime::now() + Duration::from_secs(5);
    stdfs::File::options()
        .write(true)
        .open(&source)
        .expect("open")
        .set_times(stdfs::FileTimes::new().set_accessed(future).set_modified(future))
        .expect("utime");

    pipeline.run_full(&make_ctx()).expect("second run");

    let mapped = pipeline.dst_dir.join("ABC-123/ABC-123.strm");
    assert_eq!(stdfs::read_to_string(mapped).expect("read"), "new");
}

// py: tests/test_monitor_mapping.py::test_incremental_update_and_delete
#[test]
fn incremental_update_and_delete() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let kept = strm(&pipeline.src_dir.join("brand/ABC-123.strm"));
    let removed = strm(&pipeline.src_dir.join("brand/DEF-456.strm"));
    pipeline.run_full(&make_ctx()).expect("run_full");
    stdfs::remove_file(&removed).expect("remove");

    let ctx = make_ctx();
    let (failed_changed, failed_deleted) = pipeline
        .run_incremental(&ctx, &[kept], &[removed])
        .expect("run_incremental");

    assert!(failed_changed.is_empty());
    assert!(failed_deleted.is_empty());
    assert!(pipeline.dst_dir.join("brand/ABC-123/ABC-123.strm").exists());
    assert!(!pipeline.dst_dir.join("brand/DEF-456").exists());
}

// py: tests/test_monitor_mapping.py::test_incremental_reports_failed_paths
#[test]
fn incremental_reports_failed_paths() {
    // Python patches `update_one` to raise `OSError('disk detached')`; here its first call
    // (the source's `stat`) raises it.
    let tree = Tree::new();
    let fs = recording();
    let pipeline = make_pipeline_with(&tree, Some(Arc::clone(&fs) as Arc<dyn Fs>));
    let changed = strm(&pipeline.src_dir.join("ABC-123.strm"));
    fs.add_fault(Fault::message("stat", &changed, "disk detached"));

    let ctx = make_ctx();
    let (failed_changed, failed_deleted) = pipeline
        .run_incremental(&ctx, std::slice::from_ref(&changed), &[])
        .expect("run_incremental");

    assert_eq!(failed_changed, std::slice::from_ref(&changed));
    assert!(failed_deleted.is_empty());
    assert_eq!(
        ctx.errors(),
        [format!(
            "ERROR: Incremental update failed for {}: OSError('disk detached')",
            path_text(&changed)
        )]
    );
}

// py: tests/test_monitor_mapping.py::test_tokyo_hot_separated_id_maps_to_canonical_directory
#[test]
fn tokyo_hot_separated_id_maps_to_canonical_directory() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    strm(&pipeline.src_dir.join("rest/N/N-0893.strm"));

    let ctx = make_ctx();
    pipeline.run_full(&ctx).expect("run_full");

    assert!(pipeline.dst_dir.join("rest/N/N0893/N-0893.strm").exists());
    assert!(!ctx.log_tail().iter().any(|line| line.contains("failed to get avid")));
}

// py: tests/test_monitor_mapping.py::test_non_avid_strm_is_skipped_with_warning
#[test]
fn non_avid_strm_is_skipped_with_warning() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    strm(&pipeline.src_dir.join("!!!.strm"));

    let ctx = make_ctx();
    pipeline.run_full(&ctx).expect("run_full");

    assert!(!any_strm(&pipeline.dst_dir));
    assert!(ctx.log_tail().iter().any(|line| line.contains("failed to get avid")));
}

// py: tests/test_monitor_mapping.py::test_full_sync_skips_dir_that_is_not_empty_on_disk
#[test]
fn full_sync_skips_dir_that_is_not_empty_on_disk() {
    // A stale NFS directory cache lists a directory as empty while the server still holds its
    // .strm; rmtree then fails with ENOTEMPTY. That one directory must be skipped, not abort
    // the run, and the other empty dirs still go.
    let tree = Tree::new();
    let fs = recording();
    let pipeline = make_pipeline_with(&tree, Some(Arc::clone(&fs) as Arc<dyn Fs>));
    let stale = strm(&pipeline.src_dir.join("stale/STALE-001.strm"));
    let gone = strm(&pipeline.src_dir.join("gone/GONE-001.strm"));
    pipeline.run_full(&make_ctx()).expect("first run");
    stdfs::remove_file(stale).expect("remove");
    stdfs::remove_file(gone).expect("remove");
    let stale_dir = pipeline.dst_dir.join("stale/STALE-001");
    fs.add_fault(Fault::errno("rmtree", &stale_dir, ENOTEMPTY));

    let ctx = make_ctx();
    pipeline.run_full(&ctx).expect("second run");

    assert!(stale_dir.exists());
    assert!(!pipeline.dst_dir.join("gone").exists());
    assert_eq!(ctx.stats().get("dirs_skipped"), Some(1));
    assert!(ctx.stats().get("dirs_deleted").expect("dirs_deleted") >= 1);
    assert!(ctx.errors().is_empty());
    assert!(
        ctx.log_tail()
            .iter()
            .any(|line| line.contains("not empty on disk") && line.contains("STALE-001"))
    );
}

// py: tests/test_monitor_mapping.py::test_full_sync_reports_other_delete_errors_and_continues
#[test]
fn full_sync_reports_other_delete_errors_and_continues() {
    let tree = Tree::new();
    let fs = recording();
    let pipeline = make_pipeline_with(&tree, Some(Arc::clone(&fs) as Arc<dyn Fs>));
    stdfs::remove_file(strm(&pipeline.src_dir.join("a/AAA-001.strm"))).expect("remove");
    strm(&pipeline.src_dir.join("b/BBB-001.strm"));
    pipeline.run_full(&make_ctx()).expect("first run");
    stdfs::remove_file(pipeline.src_dir.join("b/BBB-001.strm")).expect("remove");
    let locked = pipeline.dst_dir.join("locked/LOCK-001");
    stdfs::create_dir_all(&locked).expect("mkdir");
    fs.add_fault(Fault::errno("rmtree", &locked, 13));

    let ctx = make_ctx();
    pipeline.run_full(&ctx).expect("second run");

    assert!(!pipeline.dst_dir.join("b").exists());
    assert_eq!(ctx.stats().get("dirs_skipped"), Some(1));
    let errors = ctx.errors();
    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("LOCK-001"));
    assert_eq!(
        errors[0],
        "ERROR: failed to delete empty directory locked/LOCK-001: PermissionError(13, 'Permission denied')"
    );
}

// py: tests/test_monitor_mapping.py::test_incremental_delete_stops_at_dir_that_is_not_empty_on_disk
#[test]
fn incremental_delete_stops_at_dir_that_is_not_empty_on_disk() {
    let tree = Tree::new();
    let fs = recording();
    let pipeline = make_pipeline_with(&tree, Some(Arc::clone(&fs) as Arc<dyn Fs>));
    let source = strm(&pipeline.src_dir.join("brand/ABC-123.strm"));
    pipeline.run_full(&make_ctx()).expect("run_full");
    stdfs::remove_file(&source).expect("remove");
    let title_dir = pipeline.dst_dir.join("brand/ABC-123");
    fs.add_fault(Fault::errno("rmdir", &title_dir, ENOTEMPTY));

    let ctx = make_ctx();
    let (_, failed_deleted) = pipeline.run_incremental(&ctx, &[], &[source]).expect("run_incremental");

    assert!(failed_deleted.is_empty());
    assert!(!title_dir.join("ABC-123.strm").exists());
    assert!(title_dir.exists());
    assert_eq!(ctx.stats().get("files_deleted"), Some(1));
    assert_eq!(ctx.stats().get("dirs_deleted"), None);
    assert!(ctx.errors().is_empty());
}

// py: tests/test_monitor_mapping.py::test_full_sync_deletes_metadata_of_parts_merged_into_one_file
#[test]
fn full_sync_deletes_metadata_of_parts_merged_into_one_file() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    strm(&pipeline.src_dir.join("clt/SQTEVR/SQTEVR-009.strm"));
    let title = pipeline.dst_dir.join("clt/SQTEVR/SQTEVR-009");
    touch(
        &title,
        &[
            "SQTEVR-009.nfo",
            "SQTEVR-009-mediainfo.json",
            "poster.jpg",
            "fanart.jpg",
        ],
    );
    touch(
        &title,
        &[
            "SQTEVR-009-cd1.nfo",
            "SQTEVR-009-cd1-poster.jpg",
            "SQTEVR-009-cd10-landscape.jpg",
        ],
    );

    let ctx = make_ctx();
    pipeline.run_full(&ctx).expect("run_full");

    assert_eq!(
        names(&title),
        [
            "SQTEVR-009-mediainfo.json",
            "SQTEVR-009.nfo",
            "SQTEVR-009.strm",
            "fanart.jpg",
            "poster.jpg"
        ]
    );
    assert_eq!(ctx.stats().get("sidecars_deleted"), Some(3));
}

// py: tests/test_monitor_mapping.py::test_full_sync_keeps_metadata_a_live_strm_claims
#[test]
fn full_sync_keeps_metadata_a_live_strm_claims() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    strm(&pipeline.src_dir.join("ABC-123-cd1-4K.strm"));
    strm(&pipeline.src_dir.join("ABC-123-cd2.strm"));
    let title = pipeline.dst_dir.join("ABC-123");
    touch(
        &title,
        &["ABC-123-cd1-4K.nfo", "ABC-123-cd2-poster.jpg", "ABC-123-cd3.nfo"],
    );

    pipeline.run_full(&make_ctx()).expect("run_full");

    assert!(title.join("ABC-123-cd1-4K.nfo").exists());
    assert!(title.join("ABC-123-cd2-poster.jpg").exists());
    assert!(!title.join("ABC-123-cd3.nfo").exists());
}

// py: tests/test_monitor_mapping.py::test_incremental_delete_takes_the_strms_own_metadata_only
#[test]
fn incremental_delete_takes_the_strms_own_metadata_only() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let base = strm(&pipeline.src_dir.join("ABC-123.strm"));
    let part = strm(&pipeline.src_dir.join("ABC-123-cd1.strm"));
    strm(&pipeline.src_dir.join("ABC-123-cd2.strm"));
    pipeline.run_full(&make_ctx()).expect("run_full");
    let title = pipeline.dst_dir.join("ABC-123");
    touch(
        &title,
        &[
            "ABC-123.nfo",
            "ABC-123-poster.jpg",
            "ABC-123-cd1.nfo",
            "ABC-123-cd1-poster.jpg",
        ],
    );
    touch(&title, &["ABC-123-cd2-poster.jpg", "fanart.jpg"]);

    stdfs::remove_file(&part).expect("remove");
    pipeline
        .run_incremental(&make_ctx(), &[], &[part])
        .expect("first batch");
    assert!(!title.join("ABC-123-cd1.nfo").exists());
    assert!(!title.join("ABC-123-cd1-poster.jpg").exists());
    assert!(title.join("ABC-123.nfo").exists());

    stdfs::remove_file(&base).expect("remove");
    pipeline
        .run_incremental(&make_ctx(), &[], &[base])
        .expect("second batch");
    assert_eq!(
        names(&title),
        ["ABC-123-cd2-poster.jpg", "ABC-123-cd2.strm", "fanart.jpg"]
    );
}

// py: tests/test_monitor_mapping.py::test_full_sync_moves_a_title_out_of_a_directory_its_id_no_longer_maps_to
#[test]
fn full_sync_moves_a_title_out_of_a_directory_its_id_no_longer_maps_to() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    strm(&pipeline.src_dir.join("vr/VRKM/VRKM-01468-cd1.strm"));
    strm(&pipeline.src_dir.join("vr/VRKM/VRKM-01468-cd2.strm"));
    let old = pipeline.dst_dir.join("vr/VRKM/VRKM-01468");
    touch(
        &old,
        &[
            "VRKM-01468-cd1.strm",
            "VRKM-01468-cd2.strm",
            "VRKM-01468-cd1.nfo",
            "VRKM-01468-cd1-poster.jpg",
        ],
    );

    let ctx = make_ctx();
    pipeline.run_full(&ctx).expect("run_full");

    assert!(!old.exists());
    let current = pipeline.dst_dir.join("vr/VRKM/VRKM-1468");
    assert_eq!(names(&current), ["VRKM-01468-cd1.strm", "VRKM-01468-cd2.strm"]);
    assert_eq!(ctx.stats().get("files_deleted"), Some(2));
}

// py: tests/test_monitor_mapping.py::test_full_sync_keeps_a_strm_whose_id_no_longer_reads
#[test]
fn full_sync_keeps_a_strm_whose_id_no_longer_reads() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    strm(&pipeline.src_dir.join("misc/holiday.strm"));
    let kept = pipeline.dst_dir.join("misc/HOLIDAY/holiday.strm");
    touch(kept.parent().expect("parent"), &["holiday.strm"]);

    pipeline.run_full(&make_ctx()).expect("run_full");

    assert!(kept.exists());
}

// -- name rules and the file system against the Python baseline ---------------------------

fn bytes_of(value: &embyx_core::pyjson::Value) -> Option<Vec<u8>> {
    match value {
        embyx_core::pyjson::Value::Null => None,
        other => Some(hex::decode(golden::string(other)).expect("hex")),
    }
}

#[test]
fn name_rules_match_python() {
    let document = golden::load("monitor/mapping_names.json");
    let case = &golden::cases(&document)[0];
    for entry in golden::list(golden::get(case, "names")) {
        let name = bytes_of(golden::get(entry, "name")).expect("a name");
        let path = Path::new("/m").join(OsStr::from_bytes(&name));
        let shown = String::from_utf8_lossy(&name).into_owned();
        assert_eq!(
            is_strm(&path),
            *golden::get(entry, "is_strm") == embyx_core::pyjson::Value::Bool(true),
            "{shown:?}"
        );
        assert_eq!(
            Some(stem(&path).as_bytes().to_vec()),
            bytes_of(golden::get(entry, "stem")),
            "{shown:?}"
        );
        assert_eq!(
            part_stem(&name).map(<[u8]>::to_vec),
            bytes_of(golden::get(entry, "part_stem")),
            "{shown:?}"
        );
    }
    for entry in golden::list(golden::get(case, "owners")) {
        let name = bytes_of(golden::get(entry, "name")).expect("a name");
        let stems: Vec<OsString> = golden::list(golden::get(entry, "stems"))
            .iter()
            .map(|stem| OsString::from_vec(bytes_of(stem).expect("a stem")))
            .collect();
        let candidates: Vec<&OsString> = stems.iter().collect();
        assert_eq!(
            owner(&name, &candidates).map(<[u8]>::to_vec),
            bytes_of(golden::get(entry, "owner")),
            "{:?}",
            String::from_utf8_lossy(&name)
        );
    }
    for entry in golden::list(golden::get(case, "orders")) {
        let paths: Vec<PathBuf> = golden::list(golden::get(entry, "paths"))
            .iter()
            .map(|path| PathBuf::from(OsString::from_vec(bytes_of(path).expect("a path"))))
            .collect();
        let expected: Vec<Vec<u8>> = golden::list(golden::get(entry, "sorted"))
            .iter()
            .map(|path| bytes_of(path).expect("a path"))
            .collect();
        let sorted: Vec<Vec<u8>> = py_sorted_set(&paths)
            .iter()
            .map(|path| path.as_os_str().as_bytes().to_vec())
            .collect();
        assert_eq!(sorted, expected);
    }
}

fn relative_list(root: &Path, paths: &[PathBuf]) -> Vec<String> {
    paths
        .iter()
        .map(|path| path_text(path.strip_prefix(root).expect("under the root")))
        .collect()
}

fn texts(value: &embyx_core::pyjson::Value) -> Vec<String> {
    golden::list(value).iter().map(golden::string).collect()
}

/// `{type, message}` of an error with the temporary directory written as `{tmp}`, or
/// `{result}`.
fn outcome<T: Into<embyx_core::pyjson::Value>>(result: Result<T, PyOsError>, tmp: &Path) -> embyx_core::pyjson::Value {
    use embyx_core::pyjson::{Dict, Value};
    let mut value = Dict::new();
    match result {
        Ok(result) => value.insert("result", result.into()),
        Err(error) => {
            let shown = |text: String| text.replace(&path_text(tmp), "{tmp}");
            value.insert("type", Value::from(error.class_name()));
            value.insert("message", Value::Str(shown(error.to_string())));
            value.insert("repr", Value::Str(shown(error.py_repr())));
        }
    }
    Value::Dict(value)
}

#[test]
#[expect(clippy::too_many_lines, reason = "one golden walk through every file system call")]
fn file_system_calls_match_python() {
    use embyx_core::pyjson::Value;
    let document = golden::load("monitor/mapping_fs.json");
    let case = &golden::cases(&document)[0];
    let tree = Tree::new();
    let tmp = tree.root.clone();
    let root = tmp.join("tree");
    for relative in texts(golden::get(case, "tree")) {
        let path = root.join(relative);
        stdfs::create_dir_all(path.parent().expect("parent")).expect("parents");
        stdfs::write(path, "x").expect("write");
    }
    stdfs::create_dir_all(root.join("empty/deeper")).expect("mkdir");
    std::os::unix::fs::symlink(root.join("z"), root.join("link")).expect("symlink");
    let fs = recording();
    fs.sort_listings();
    assert_eq!(
        relative_list(&root, &fs.glob_recursive(&root, &strm_glob)),
        texts(golden::get(case, "glob_strm"))
    );
    assert_eq!(
        relative_list(&root, &fs.glob_recursive(&root, &|_| true)),
        texts(golden::get(case, "glob_all"))
    );
    assert_eq!(
        relative_list(&root, &fs.glob_recursive(&root.join("link"), &strm_glob)),
        texts(golden::get(case, "glob_strm_in_link"))
    );

    let local = LocalFs;
    let src = tmp.join("src.strm");
    stdfs::write(&src, "/cloud/video.mp4").expect("write");
    stdfs::set_permissions(&src, stdfs::Permissions::from_mode(0o640)).expect("chmod");
    let at = |ns: u64| SystemTime::UNIX_EPOCH + Duration::from_nanos(ns);
    stdfs::File::options()
        .write(true)
        .open(&src)
        .expect("open")
        .set_times(
            stdfs::FileTimes::new()
                .set_accessed(at(1_700_000_000_123_456_789))
                .set_modified(at(1_600_000_000_987_654_321)),
        )
        .expect("utime");
    let dst = tmp.join("out/dst.strm");
    stdfs::create_dir(tmp.join("out")).expect("mkdir");
    local.copy2(&src, &dst).expect("copy2");
    let into_dir = tmp.join("into");
    stdfs::create_dir(&into_dir).expect("mkdir");
    local.copy2(&src, &into_dir).expect("copy2 into a directory");
    let same_size = tmp.join("same.strm");
    stdfs::write(&same_size, "/cloud/video.mp5").expect("write");
    stdfs::create_dir(tmp.join("full")).expect("mkdir");
    stdfs::write(tmp.join("full/x"), "x").expect("write");
    stdfs::create_dir(tmp.join("exists")).expect("mkdir");
    stdfs::write(tmp.join("a-file"), "x").expect("write");

    let copied = stdfs::metadata(&dst).expect("stat");
    let copy = golden::get(case, "copy2");
    assert_eq!(
        i128::from(copied.mtime()) * 1_000_000_000 + i128::from(copied.mtime_nsec()),
        golden::int(golden::get(copy, "mtime_ns"))
    );
    assert_eq!(
        format!("0o{:o}", copied.mode() & 0o7777),
        golden::string(golden::get(copy, "mode"))
    );
    assert_eq!(
        stdfs::read_to_string(&dst).expect("read"),
        golden::string(golden::get(copy, "content"))
    );
    assert_eq!(names(&into_dir), texts(golden::get(copy, "into_dir")));
    assert_eq!(
        Value::Bool(
            local.stat(&src, true).expect("stat").mtime.to_bits()
                == local.stat(&dst, true).expect("stat").mtime.to_bits(),
        ),
        *golden::get(copy, "st_mtime_equal")
    );

    let errors = golden::get(case, "errors");
    let unit = |result: Result<(), PyOsError>| result.map(|()| Value::Null);
    let cases: Vec<(&str, Value)> = vec![
        (
            "copy_missing_source",
            outcome(
                unit(local.copy2(&tmp.join("missing.strm"), &tmp.join("out/x.strm"))),
                &tmp,
            ),
        ),
        (
            "copy_missing_directory",
            outcome(unit(local.copy2(&src, &tmp.join("nowhere/x.strm"))), &tmp),
        ),
        (
            "copy_source_is_directory",
            outcome(unit(local.copy2(&tmp.join("full"), &tmp.join("out/y.strm"))), &tmp),
        ),
        ("copy_same_file", outcome(unit(local.copy2(&src, &src)), &tmp)),
        (
            "mkdir_existing",
            outcome(unit(local.mkdir_parents_new(&tmp.join("exists"))), &tmp),
        ),
        (
            "mkdir_under_file",
            outcome(unit(local.mkdir_parents_new(&tmp.join("a-file/x/y"))), &tmp),
        ),
        (
            "mkdir_new",
            outcome(unit(local.mkdir_parents_new(&tmp.join("new/one/two"))), &tmp),
        ),
        ("rmdir_not_empty", outcome(unit(local.rmdir(&tmp.join("full"))), &tmp)),
        ("rmdir_missing", outcome(unit(local.rmdir(&tmp.join("gone"))), &tmp)),
        ("cmp_equal", outcome(local.cmp_files(&src, &dst), &tmp)),
        (
            "cmp_same_size_differs",
            outcome(local.cmp_files(&src, &same_size), &tmp),
        ),
        ("cmp_directory", outcome(local.cmp_files(&src, &tmp.join("full")), &tmp)),
        (
            "cmp_missing",
            outcome(local.cmp_files(&src, &tmp.join("missing.strm")), &tmp),
        ),
    ];
    for (name, got) in cases {
        assert_eq!(&got, golden::get(errors, name), "{name}");
    }
}

// -- names that are not UTF-8 ---------------------------------------------------------------

/// Two sidecars whose names differ only in bytes that are not UTF-8 belong to two different
/// `.strm` files: deleting one title's `.strm` takes its own metadata and leaves the other's,
/// which a lossy decode (both read as `U+FFFD`) would have confused.
#[test]
fn sidecars_are_matched_on_bytes_not_on_a_lossy_decode() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let title = pipeline.dst_dir.join("ABC-123");
    let name = |bytes: &[u8]| PathBuf::from(OsString::from_vec(bytes.to_vec()));
    let first = pipeline.src_dir.join(name(b"ABC-123 \xff.strm"));
    let second = pipeline.src_dir.join(name(b"ABC-123 \xfe.strm"));
    strm(&first);
    strm(&second);
    pipeline.run_full(&make_ctx()).expect("run_full");
    for sidecar in [
        &b"ABC-123 \xff.nfo"[..],
        b"ABC-123 \xfe.nfo",
        b"ABC-123 \xff-poster.jpg",
    ] {
        stdfs::write(title.join(name(sidecar)), "x").expect("write");
    }

    stdfs::remove_file(&first).expect("remove");
    let ctx = make_ctx();
    let (_, failed) = pipeline
        .run_incremental(&ctx, &[], std::slice::from_ref(&first))
        .expect("run_incremental");

    assert!(failed.is_empty());
    let mut left: Vec<Vec<u8>> = stdfs::read_dir(&title)
        .expect("list")
        .map(|entry| entry.expect("entry").file_name().into_vec())
        .collect();
    left.sort();
    assert_eq!(left, [b"ABC-123 \xfe.nfo".to_vec(), b"ABC-123 \xfe.strm".to_vec()]);
    assert_eq!(ctx.stats().get("sidecars_deleted"), Some(2));
    // A full run over the tree as it now stands changes nothing.
    let again = make_ctx();
    pipeline.run_full(&again).expect("run_full");
    assert_eq!(again.stats().get("files_skipped"), Some(1));
    assert_eq!(again.stats().get("files_deleted"), None);
    assert_eq!(again.stats().get("sidecars_deleted"), None);
}

/// A stored `.strm` is the source's bytes, whatever they are.
#[test]
fn the_mirrored_strm_holds_the_source_bytes() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let source = pipeline.src_dir.join("ABC-123.strm");
    stdfs::write(&source, b"/cloud/\xff\x00video.mp4\r\n").expect("write");

    pipeline.run_full(&make_ctx()).expect("run_full");

    assert_eq!(
        stdfs::read(pipeline.dst_dir.join("ABC-123/ABC-123.strm")).expect("read"),
        b"/cloud/\xff\x00video.mp4\r\n"
    );
}

/// D90: a source rewritten with other bytes of the same size and the same modification time is
/// copied once; with no `filecmp` cache the next full run compares the mirror equal and leaves
/// it alone (Python's cache keeps the first `False` and copies it on every full run).
#[test]
fn a_same_size_same_mtime_rewrite_is_copied_once() {
    let tree = Tree::new();
    let pipeline = make_pipeline(&tree);
    let source = write_strm(&pipeline.src_dir.join("ABC-123.strm"), "aaaa");
    pipeline.run_full(&make_ctx()).expect("first run");
    let mapped = pipeline.dst_dir.join("ABC-123/ABC-123.strm");
    let modified = stdfs::metadata(&source).expect("stat").modified().expect("mtime");
    stdfs::write(&source, "bbbb").expect("write");
    stdfs::File::options()
        .write(true)
        .open(&source)
        .expect("open")
        .set_times(stdfs::FileTimes::new().set_modified(modified))
        .expect("utime");

    let ctx = make_ctx();
    pipeline.run_full(&ctx).expect("second run");
    assert_eq!(ctx.stats().get("files_updated"), Some(1));
    assert_eq!(stdfs::read_to_string(&mapped).expect("read"), "bbbb");

    let ctx = make_ctx();
    pipeline.run_full(&ctx).expect("third run");
    assert_eq!(ctx.stats().get("files_updated"), None);
    assert_eq!(ctx.stats().get("files_skipped"), Some(1));
}
