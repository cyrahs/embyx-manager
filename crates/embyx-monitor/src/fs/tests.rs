//! `pathlib` and `shutil` as `CPython` 3.13 behaves on a temporary tree: the expected texts
//! were read off Python 3.13 running the same calls.

use std::fs as stdfs;
use std::os::unix::fs::symlink;

use super::*;

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

    fn file(&self, relative: &str) -> PathBuf {
        let path = self.path(relative);
        stdfs::create_dir_all(path.parent().expect("a parent")).expect("parent");
        stdfs::write(&path, b"x").expect("write");
        path
    }
}

fn texts(error: &PyOsError) -> (String, String, String) {
    (error.class_name().to_owned(), error.to_string(), error.py_repr())
}

fn owned(class: &str, text: &str, repr: &str) -> (String, String, String) {
    (class.to_owned(), text.to_owned(), repr.to_owned())
}

#[test]
fn os_errors_render_as_python_writes_them() {
    let error = PyOsError::errno(ENOENT, Some(Path::new("nope")), Some(Path::new("other")));
    assert_eq!(
        texts(&error),
        owned(
            "FileNotFoundError",
            "[Errno 2] No such file or directory: 'nope' -> 'other'",
            "FileNotFoundError(2, 'No such file or directory')"
        )
    );
    let error = PyOsError::errno(17, Some(Path::new("file.mp4")), None);
    assert_eq!(
        texts(&error),
        owned(
            "FileExistsError",
            "[Errno 17] File exists: 'file.mp4'",
            "FileExistsError(17, 'File exists')"
        )
    );
    let error = PyOsError::errno(5, None, None);
    assert_eq!(
        texts(&error),
        owned(
            "OSError",
            "[Errno 5] Input/output error",
            "OSError(5, 'Input/output error')"
        )
    );
    let error = PyOsError::message("mount went away");
    assert_eq!(
        texts(&error),
        owned("OSError", "mount went away", "OSError('mount went away')")
    );
    let error = PyOsError::message("Cannot call rmtree on a symbolic link").with_path_object(Path::new("dirlink"));
    assert_eq!(
        texts(&error),
        owned(
            "OSError",
            "[Errno None] None: PosixPath('dirlink')",
            "OSError('Cannot call rmtree on a symbolic link')"
        )
    );
}

#[test]
fn pathlib_reads_only_the_missing_errors_as_no() {
    let tree = Tree::new();
    let file = tree.file("file.mp4");
    assert!(is_file(&LocalFs, &file).expect("is_file"));
    assert!(is_video(&LocalFs, &file).expect("is_video"));
    assert!(!is_dir(&LocalFs, &file.join("x")).expect("ENOTDIR reads as no"));
    assert!(!exists(&LocalFs, &tree.path("missing")).expect("ENOENT reads as no"));
    symlink(tree.path("nowhere"), tree.path("dangling")).expect("symlink");
    assert!(is_symlink(&LocalFs, &tree.path("dangling")).expect("is_symlink"));
    assert!(!exists(&LocalFs, &tree.path("dangling")).expect("exists"));
    assert!(PyOsError::errno(ELOOP, None, None).is_ignored_by_pathlib());
    assert!(PyOsError::errno(EBADF, None, None).is_ignored_by_pathlib());
    assert!(!PyOsError::errno(5, None, None).is_ignored_by_pathlib());
    assert!(!PyOsError::message("mount went away").is_ignored_by_pathlib());
}

#[test]
fn rglob_lists_a_linked_directory_without_entering_it() {
    let tree = Tree::new();
    tree.file("t/a.mp4");
    tree.file("t/sub/b.mp4");
    tree.file("target/keep.mp4");
    symlink(tree.path("target"), tree.path("t/link")).expect("symlink");

    let mut found: Vec<String> = rglob(&LocalFs, &tree.path("t"))
        .expect("rglob")
        .iter()
        .map(|path| path_text(path.strip_prefix(&tree.root).expect("below")))
        .collect();
    found.sort();

    assert_eq!(found, ["t/a.mp4", "t/link", "t/sub", "t/sub/b.mp4"]);
    // A directory that cannot be listed contributes nothing; `iterdir` raises instead.
    assert!(rglob(&LocalFs, &tree.path("missing")).expect("rglob").is_empty());
    let error = iterdir(&LocalFs, &tree.path("missing")).expect_err("iterdir raises");
    assert_eq!(error.errno, Some(ENOENT));
}

#[test]
fn mkdir_parents_creates_the_chain_and_accepts_an_existing_directory() {
    let tree = Tree::new();
    let deep = tree.path("a/b/c");
    mkdir_parents(&LocalFs, &deep).expect("mkdir");
    assert!(deep.is_dir());
    mkdir_parents(&LocalFs, &deep).expect("exist_ok");

    let file = tree.file("file.mp4");
    let error = mkdir_parents(&LocalFs, &file).expect_err("a file is in the way");
    assert_eq!(
        error.to_string(),
        format!("[Errno 17] File exists: '{}'", path_text(&file))
    );
    let below = file.join("x/y");
    let error = mkdir_parents(&LocalFs, &below).expect_err("a file is a parent");
    assert_eq!(
        error.to_string(),
        format!("[Errno 20] Not a directory: '{}'", path_text(&below))
    );
}

#[test]
fn rmtree_removes_a_tree_but_not_what_a_link_in_it_points_at() {
    let tree = Tree::new();
    tree.file("t/a.mp4");
    tree.file("t/sub/b.mp4");
    let kept = tree.file("target/keep.mp4");
    symlink(tree.path("target"), tree.path("t/link")).expect("symlink");

    LocalFs.rmtree(&tree.path("t")).expect("rmtree");

    assert!(!tree.path("t").exists());
    assert!(kept.exists());
}

#[test]
fn rmtree_refuses_links_and_files_naming_the_path_object() {
    let tree = Tree::new();
    tree.file("target/keep.mp4");
    let file = tree.file("file.mp4");
    symlink(tree.path("target"), tree.path("dirlink")).expect("symlink");
    symlink(tree.path("nowhere"), tree.path("dangling")).expect("symlink");
    let text = |relative: &str| path_text(&tree.path(relative));

    let error = LocalFs.rmtree(&tree.path("missing")).expect_err("missing");
    assert_eq!(
        texts(&error),
        owned(
            "FileNotFoundError",
            &format!("[Errno 2] No such file or directory: PosixPath('{}')", text("missing")),
            "FileNotFoundError(2, 'No such file or directory')"
        )
    );
    let error = LocalFs.rmtree(&tree.path("dirlink")).expect_err("a link");
    assert_eq!(
        texts(&error),
        owned(
            "OSError",
            &format!("[Errno None] None: PosixPath('{}')", text("dirlink")),
            "OSError('Cannot call rmtree on a symbolic link')"
        )
    );
    let error = LocalFs.rmtree(&file).expect_err("a file");
    assert_eq!(
        texts(&error),
        owned(
            "NotADirectoryError",
            &format!("[Errno 20] Not a directory: PosixPath('{}')", text("file.mp4")),
            "NotADirectoryError(20, 'Not a directory')"
        )
    );
    let error = LocalFs.rmtree(&tree.path("dangling")).expect_err("a dangling link");
    assert_eq!(
        error.to_string(),
        format!("[Errno 2] No such file or directory: PosixPath('{}')", text("dangling"))
    );
    assert!(tree.path("target/keep.mp4").exists());
    assert!(file.exists());
}

#[test]
fn an_error_inside_rmtree_names_the_walked_directory_and_a_vanished_entry_is_skipped() {
    let top = Path::new("/srv/task/folder");
    let below = Path::new("/srv/task/folder/inner");
    let failed = || Err(PyOsError::errno(13, None, None));
    // `err.filename = path`: the `Path` object at the top, the joined `str` below it.
    let error = named(failed(), top, top, true).expect_err("raised");
    assert_eq!(
        error.to_string(),
        "[Errno 13] Permission denied: PosixPath('/srv/task/folder')"
    );
    let error = named(failed(), top, below, false).expect_err("raised");
    assert_eq!(
        error.to_string(),
        "[Errno 13] Permission denied: '/srv/task/folder/inner'"
    );
    // `FileNotFoundError` is reported only at the top.
    let vanished = || Err(PyOsError::errno(ENOENT, None, None));
    assert!(named(vanished(), top, top, true).is_err());
    assert_eq!(named(vanished(), top, below, false), Ok(()));
}

#[test]
fn rmtree_lists_a_directory_through_its_open_descriptor() {
    // `os.scandir(topfd)` reads a duplicate of the descriptor instead of opening the directory
    // again, so an empty directory without search permission is still listed and removed (the
    // permission bits only bite when the tests run as a regular user, as CI's do).
    let tree = Tree::new();
    tree.file("t/a.mp4");
    let empty = tree.path("t/empty-readonly");
    stdfs::create_dir_all(&empty).expect("mkdir");
    let mut permissions = stdfs::metadata(&empty).expect("stat").permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o444);
    stdfs::set_permissions(&empty, permissions).expect("chmod");

    LocalFs.rmtree(&tree.path("t")).expect("rmtree");

    assert!(!tree.path("t").exists());
}
