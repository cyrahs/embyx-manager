use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::recording::{CRASH, CrashPoint, Fault, RecordingMoveFs};
use super::{EEXIST, LocalMoveFs, MoveFs, Strict, exists, is_dir, is_file, is_symlink, mkdir_exist_ok, realpath};

/// The tree of the `realpath` cases: `dir/file`, `link -> dir`, `dangling -> missing`, the
/// loop `a -> b -> a`, `dir/up -> ../dir/file`, `abs -> <root>/dir`.
fn tree() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    std::fs::create_dir(root.join("dir")).unwrap();
    std::fs::write(root.join("dir/file"), b"").unwrap();
    let link = |target: &Path, name: &str| std::os::unix::fs::symlink(target, root.join(name)).unwrap();
    link(Path::new("dir"), "link");
    link(Path::new("missing"), "dangling");
    link(Path::new("b"), "a");
    link(Path::new("a"), "b");
    link(Path::new("../dir/file"), "dir/up");
    link(&root.join("dir"), "abs");
    (dir, root)
}

/// `os.path.realpath(case, strict=...)` on `CPython` 3.13.18 over [`tree`] (run in the tree's
/// directory): the result below the root, or the `OSError` subclass and errno.
#[test]
fn realpath_walks_the_parts_as_python_does() {
    let (_dir, root) = tree();
    let cases: [(&str, &str, &str); 12] = [
        ("link/file", "/dir/file", "/dir/file"),
        ("dangling", "/missing", "FileNotFoundError 2"),
        ("dangling/x", "/missing/x", "FileNotFoundError 2"),
        ("a", "/a", "OSError 40"),
        ("a/x", "/a/x", "OSError 40"),
        ("link/../dir", "/dir", "/dir"),
        ("dir/up", "/dir/file", "/dir/file"),
        ("abs/file", "/dir/file", "/dir/file"),
        ("dir/file/x", "/dir/file/x", "NotADirectoryError 20"),
        ("dir//./file", "/dir/file", "/dir/file"),
        ("nope/../dir", "/dir", "FileNotFoundError 2"),
        ("link/up/..", "/dir", "NotADirectoryError 20"),
    ];
    let show = |result: Result<PathBuf, super::PyOsError>| match result {
        Ok(path) => {
            let bytes = path.as_os_str().as_bytes();
            String::from_utf8(bytes[root.as_os_str().len()..].to_vec()).unwrap()
        }
        Err(error) => format!(
            "{} {}",
            embyx_monitor::reports::PyException::class_name(&error),
            error.errno.unwrap()
        ),
    };
    for (case, lenient, strict) in cases {
        let mut path = root.as_os_str().to_owned();
        path.push("/");
        path.push(case);
        let path = PathBuf::from(path);
        assert_eq!(show(realpath(&LocalMoveFs, &path, Strict::No)), lenient, "{case}");
        assert_eq!(
            show(realpath(&LocalMoveFs, &path, Strict::Yes)),
            strict,
            "{case} strict"
        );
    }
}

#[test]
fn pathlib_predicates_read_only_missing_paths_as_no() {
    let (_dir, root) = tree();
    let fs = &LocalMoveFs;
    assert!(is_dir(fs, &root.join("link")).unwrap());
    assert!(is_symlink(fs, &root.join("link")).unwrap());
    assert!(is_file(fs, &root.join("dir/file")).unwrap());
    assert!(!is_file(fs, &root.join("dir")).unwrap());
    assert!(!exists(fs, &root.join("dangling")).unwrap());
    assert!(is_symlink(fs, &root.join("dangling")).unwrap());
    // ENOTDIR and ELOOP are "no", as in pathlib.
    assert!(!exists(fs, &root.join("dir/file/x")).unwrap());
    assert!(!exists(fs, &root.join("a")).unwrap());

    // Any other error is raised.
    let recording = RecordingMoveFs::default();
    recording.add_fault(Fault::errno("stat", Some(root.join("dir")), 5));
    let error = is_dir(&recording, &root.join("dir")).unwrap_err();
    assert_eq!(error.errno, Some(5));
    assert_eq!(
        error.to_string(),
        format!("[Errno 5] Input/output error: '{}'", root.join("dir").display())
    );
}

#[test]
fn rename_noreplace_and_mkdir_exist_ok() {
    let (_dir, root) = tree();
    let fs = &LocalMoveFs;
    std::fs::write(root.join("other"), b"x").unwrap();
    let error = fs
        .rename_noreplace(&root.join("other"), &root.join("dir/file"))
        .unwrap_err();
    assert_eq!(error.errno, Some(EEXIST));
    assert!(root.join("other").exists());
    fs.rename_noreplace(&root.join("other"), &root.join("moved")).unwrap();
    assert!(!root.join("other").exists());

    mkdir_exist_ok(fs, &root.join("dir"), 0o777).unwrap();
    mkdir_exist_ok(fs, &root.join("new"), 0o777).unwrap();
    assert!(root.join("new").is_dir());
    // A file in the way is still `FileExistsError`.
    assert_eq!(
        mkdir_exist_ok(fs, &root.join("moved"), 0o777).unwrap_err().errno,
        Some(EEXIST)
    );
}

#[test]
fn recording_fs_records_faults_and_crashes_mutations() {
    let (_dir, root) = tree();
    let (fs, records) = RecordingMoveFs::collecting();
    fs.add_fault(Fault::errno("link", None, 1).times(1));

    assert_eq!(
        fs.link(&root.join("dir/file"), &root.join("l1")).unwrap_err().errno,
        Some(1)
    );
    fs.link(&root.join("dir/file"), &root.join("l1")).unwrap();
    fs.set_paused(true);
    fs.unlink(&root.join("l1")).unwrap();
    fs.set_paused(false);

    let records = records.lock().unwrap().clone();
    let texts: Vec<String> = records
        .iter()
        .map(|record| embyx_core::pyjson::dumps(record, &embyx_core::pyjson::DumpOptions::python_default()).unwrap())
        .collect();
    let file = root.join("dir/file").display().to_string();
    let l1 = root.join("l1").display().to_string();
    assert_eq!(
        texts,
        [
            format!(
                r#"{{"fake": "fs", "method": "link", "request": {{"src": "{file}", "dst": "{l1}"}}, "error": {{"type": "PermissionError", "message": "[Errno 1] Operation not permitted: '{file}' -> '{l1}'"}}}}"#
            ),
            format!(
                r#"{{"fake": "fs", "method": "link", "request": {{"src": "{file}", "dst": "{l1}"}}, "response": null}}"#
            ),
        ]
    );

    fs.set_crash(Some(CrashPoint {
        op: "unlink".to_owned(),
        path: None,
        nth: 2,
    }));
    std::fs::write(root.join("x1"), b"").unwrap();
    std::fs::write(root.join("x2"), b"").unwrap();
    fs.unlink(&root.join("x1")).unwrap();
    let fs = Arc::new(fs);
    let crashing = Arc::clone(&fs);
    let x2 = root.join("x2");
    let panic = std::thread::spawn(move || crashing.unlink(&x2)).join().unwrap_err();
    assert_eq!(panic.downcast_ref::<&str>(), Some(&CRASH));
    assert!(root.join("x2").exists());
    assert!(!fs.crash_armed());
}
