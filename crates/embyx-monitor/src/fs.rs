//! The file system as the archive pipelines see it through `pathlib` and `shutil`.
//!
//! Python origin: the `pathlib.Path` and `shutil.rmtree` calls of `monitor/archive.py`,
//! `reconcile.py` and `move_in.py`, as `CPython` 3.13 implements them. The pipelines move and
//! delete library files, so the details that decide what happens on a failing mount are kept:
//!
//! - `is_dir()`, `is_file()`, `exists()` and `is_symlink()` answer "no" only for `ENOENT`,
//!   `ENOTDIR`, `EBADF` and `ELOOP`; any other error (a FUSE mount gone, `EIO`) is raised.
//! - `rglob('*')` yields every entry below a directory without descending into symbolic links
//!   and silently skips a directory it cannot list; `iterdir()` raises.
//! - `mkdir(parents=True, exist_ok=True)` and `shutil.rmtree` follow the 3.13 algorithms
//!   ([`mkdir_parents`], [`LocalFs::rmtree`]), including "Cannot call rmtree on a symbolic link".
//! - Errors are [`PyOsError`]: `str()` and `repr()` as Python writes them, because an outcome's
//!   reason is `str(exc)` and a run-log line carries `repr(exc)`.
//!
//! Every call goes through the [`Fs`] trait so tests and scenarios can record mutations and
//! inject failures ([`recording`], behind `test-support`); [`LocalFs`] is the real one.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use embyx_core::pytext::py_repr;

use crate::reports::PyException;

mod copy;
#[cfg(any(test, feature = "test-support"))]
pub mod recording;

/// `ENOENT`.
pub const ENOENT: i32 = 2;
/// `EBADF`.
pub const EBADF: i32 = 9;
/// `ENOTDIR`.
pub const ENOTDIR: i32 = 20;
/// `ELOOP`.
pub const ELOOP: i32 = 40;

/// `pathlib._abc._IGNORED_ERRNOS`: the errors `is_dir()` and friends read as "no".
const IGNORED_ERRNOS: [i32; 4] = [ENOENT, ENOTDIR, EBADF, ELOOP];

/// An `OSError` as Python raises it: the subclass for its errno, `str()` and `repr()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PyOsError {
    /// `exc.errno`; `None` for `OSError(message)`.
    pub errno: Option<i32>,
    /// `exc.strerror` when there is an errno, otherwise the single argument of
    /// `OSError(message)` (one field keeps the error small).
    text: Option<String>,
    /// `exc.filename` and `exc.filename2`.
    pub filename: Option<Filename>,
    pub filename2: Option<Filename>,
    /// The class when it is not the errno's (`shutil.SameFileError`, a `FileNotFoundError`
    /// built from a message).
    class: Option<&'static str>,
}

/// An `OSError`'s file name: the `str` an `os` call stores, or the `Path` object
/// `shutil.rmtree` stores for the top of the tree (`err.filename = path`), which `str(exc)`
/// writes as `PosixPath('...')`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filename {
    Str(String),
    PosixPath(String),
}

impl Filename {
    fn of(path: &Path) -> Self {
        Self::Str(path_text(path))
    }

    /// `repr(filename)`.
    fn py_repr(&self) -> String {
        match self {
            Self::Str(text) => py_repr(text),
            Self::PosixPath(text) => format!("PosixPath({})", py_repr(text)),
        }
    }
}

impl PyOsError {
    /// `OSError(errno, os.strerror(errno), filename[, None, filename2])`, what an `os` call raises.
    pub fn errno(code: i32, filename: Option<&Path>, filename2: Option<&Path>) -> Self {
        Self {
            errno: Some(code),
            text: Some(strerror(code)),
            filename: filename.map(Filename::of),
            filename2: filename2.map(Filename::of),
            class: None,
        }
    }

    /// `OSError(message)`.
    pub fn message(message: impl Into<String>) -> Self {
        Self {
            errno: None,
            text: Some(message.into()),
            filename: None,
            filename2: None,
            class: None,
        }
    }

    /// `Class(message)` for an `OSError` subclass Python raises with one argument
    /// (`FileNotFoundError('Directory does not exist: ...')`, `shutil.SameFileError`).
    pub fn classed(class: &'static str, message: impl Into<String>) -> Self {
        let mut built = Self::message(message);
        built.class = Some(class);
        built
    }

    /// The error an `os` call raises for this I/O error. An error without an errno (a path
    /// with a NUL byte, which pathlib reads as `False` or `ValueError`; D70) keeps its text as
    /// the message.
    pub fn from_io(error: &io::Error, filename: Option<&Path>, filename2: Option<&Path>) -> Self {
        if let Some(code) = error.raw_os_error() {
            return Self::errno(code, filename, filename2);
        }
        let mut built = Self::message(error.to_string());
        built.filename = filename.map(Filename::of);
        built.filename2 = filename2.map(Filename::of);
        built
    }

    /// `err.filename = path` with the `Path` object `shutil.rmtree` was given, as it sets it
    /// for an error at the top of the tree.
    #[must_use]
    pub fn with_path_object(mut self, path: &Path) -> Self {
        self.filename = Some(Filename::PosixPath(path_text(path)));
        self
    }

    /// `exc.strerror`.
    pub fn strerror(&self) -> Option<&str> {
        self.errno.and(self.text.as_deref())
    }

    /// The single argument of `OSError(message)`.
    fn argument(&self) -> Option<&str> {
        if self.errno.is_some() {
            None
        } else {
            self.text.as_deref()
        }
    }

    /// Whether `pathlib` reads this error as "the path is not there".
    pub fn is_ignored_by_pathlib(&self) -> bool {
        self.errno.is_some_and(|code| IGNORED_ERRNOS.contains(&code))
    }
}

impl fmt::Display for PyOsError {
    /// `OSError.__str__`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let errno = self.errno.map_or_else(|| "None".to_owned(), |code| code.to_string());
        let strerror = self.strerror().unwrap_or("None");
        match (&self.filename, &self.filename2) {
            (Some(filename), Some(filename2)) => write!(
                f,
                "[Errno {errno}] {strerror}: {} -> {}",
                filename.py_repr(),
                filename2.py_repr()
            ),
            (Some(filename), None) => write!(f, "[Errno {errno}] {strerror}: {}", filename.py_repr()),
            (None, _) => match (self.errno, self.strerror()) {
                (Some(_), Some(_)) => write!(f, "[Errno {errno}] {strerror}"),
                _ => f.write_str(self.argument().unwrap_or("")),
            },
        }
    }
}

impl std::error::Error for PyOsError {}

impl PyException for PyOsError {
    /// The `OSError` subclass Python picks for the errno (Linux numbers).
    fn class_name(&self) -> &'static str {
        if let Some(class) = self.class {
            return class;
        }
        match self.errno {
            Some(11 | 114 | 115) => "BlockingIOError",
            Some(10) => "ChildProcessError",
            Some(32 | 108) => "BrokenPipeError",
            Some(103) => "ConnectionAbortedError",
            Some(111) => "ConnectionRefusedError",
            Some(104) => "ConnectionResetError",
            Some(17) => "FileExistsError",
            Some(ENOENT) => "FileNotFoundError",
            Some(21) => "IsADirectoryError",
            Some(ENOTDIR) => "NotADirectoryError",
            Some(4) => "InterruptedError",
            Some(1 | 13) => "PermissionError",
            Some(3) => "ProcessLookupError",
            Some(110) => "TimeoutError",
            _ => "OSError",
        }
    }

    /// `repr(exc)`: the arguments without the file names.
    fn py_repr(&self) -> String {
        match (self.argument(), self.errno, self.strerror()) {
            (Some(message), _, _) => format!("{}({})", self.class_name(), py_repr(message)),
            (None, Some(code), Some(strerror)) => format!("{}({code}, {})", self.class_name(), py_repr(strerror)),
            _ => format!("{}()", self.class_name()),
        }
    }
}

/// `os.strerror(code)`.
fn strerror(code: i32) -> String {
    let text = io::Error::from_raw_os_error(code).to_string();
    text.strip_suffix(&format!(" (os error {code})"))
        .unwrap_or(&text)
        .to_owned()
}

/// `str(path)` (lossy for a name that is not UTF-8; DIVERGENCES.md D72).
pub fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// `st_mtime`: `(double)sec + (double)nsec * 1e-9`, the float `CPython` builds.
#[expect(clippy::cast_precision_loss, reason = "CPython's own double arithmetic")]
fn py_mtime(metadata: &std::fs::Metadata) -> f64 {
    use std::os::unix::fs::MetadataExt as _;
    metadata.mtime() as f64 + metadata.mtime_nsec() as f64 * 1e-9
}

/// What `os.stat` tells the pipelines: the kind of entry and its size.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FileStat {
    pub kind: FileKind,
    pub size: u64,
    /// `st_mtime`: seconds as a float, `sec + nsec * 1e-9` as `CPython` computes it.
    pub mtime: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Directory,
    File,
    Symlink,
    Other,
}

/// One `os.scandir` entry: its path and `entry.is_dir(follow_symlinks=False)` (an error
/// there reads as `False`, as `glob` treats it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanEntry {
    pub path: PathBuf,
    pub is_dir: bool,
}

/// The calls the pipelines make. The first group are the `os` primitives; the second the
/// `pathlib` operations built on them ([`iterdir`], [`rglob`], [`mkdir_parents`]), which a
/// fake overrides to record or fail them as the Python scenarios patch `Path`.
pub trait Fs: Send + Sync + fmt::Debug {
    /// `os.stat(path, follow_symlinks=...)` (`Path.stat()`, `Path.lstat()`).
    fn stat(&self, path: &Path, follow_symlinks: bool) -> Result<FileStat, PyOsError>;
    /// `os.scandir(path)`, every entry.
    fn scandir(&self, path: &Path) -> Result<Vec<ScanEntry>, PyOsError>;
    /// `os.mkdir(path)`.
    fn mkdir(&self, path: &Path) -> Result<(), PyOsError>;
    /// `Path.rename(target)`: `os.rename`, which replaces an existing file.
    fn rename(&self, src: &Path, dst: &Path) -> Result<(), PyOsError>;
    /// `Path.unlink()`.
    fn unlink(&self, path: &Path) -> Result<(), PyOsError>;
    /// `shutil.rmtree(path)`.
    fn rmtree(&self, path: &Path) -> Result<(), PyOsError>;
    /// `Path.rmdir()`: `os.rmdir`.
    fn rmdir(&self, path: &Path) -> Result<(), PyOsError>;
    /// `shutil.copy2(src, dst)`: the bytes, then `copystat` (times, extended attributes, mode).
    fn copy2(&self, src: &Path, dst: &Path) -> Result<(), PyOsError>;
    /// `filecmp.cmp(a, b, shallow=False)`: both regular files of one size and equal bytes
    /// (its own `os.stat` calls, not `Path.stat`).
    fn cmp_files(&self, a: &Path, b: &Path) -> Result<bool, PyOsError>;

    /// `list(Path.iterdir())`, unsorted.
    fn iterdir(&self, path: &Path) -> Result<Vec<PathBuf>, PyOsError> {
        iterdir(self, path)
    }

    /// `list(Path.rglob('*'))`, unsorted.
    fn rglob(&self, path: &Path) -> Result<Vec<PathBuf>, PyOsError> {
        rglob(self, path)
    }

    /// `Path.mkdir(parents=True, exist_ok=True)`.
    fn mkdir_parents(&self, path: &Path) -> Result<(), PyOsError> {
        mkdir_parents(self, path)
    }

    /// `Path.mkdir(parents=True)` (`exist_ok=False`): an existing path is an error.
    fn mkdir_parents_new(&self, path: &Path) -> Result<(), PyOsError> {
        mkdir_parents_new(self, path)
    }

    /// `list(Path.glob('**/<pattern>'))` in 3.13 (`recurse_symlinks=False`), where `matches`
    /// tests an entry's name against `<pattern>`. See [`glob_recursive`].
    fn glob_recursive(&self, path: &Path, matches: &dyn Fn(&OsStr) -> bool) -> Vec<PathBuf> {
        glob_recursive(self, path, matches, usize::MAX)
    }
}

/// `Path.iterdir()`: `os.scandir` of the path, raised as is.
pub fn iterdir<F: Fs + ?Sized>(fs: &F, path: &Path) -> Result<Vec<PathBuf>, PyOsError> {
    Ok(fs.scandir(path)?.into_iter().map(|entry| entry.path).collect())
}

/// `Path.rglob('*')` in 3.13 (`recurse_symlinks=False`): the children of the path and of
/// every directory below it, symbolic links listed but not entered; a directory that cannot
/// be listed contributes nothing.
pub fn rglob<F: Fs + ?Sized>(fs: &F, path: &Path) -> Result<Vec<PathBuf>, PyOsError> {
    let mut found = Vec::new();
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = fs.scandir(&directory) else {
            continue;
        };
        for entry in entries {
            if entry.is_dir {
                pending.push(entry.path.clone());
            }
            found.push(entry.path);
        }
    }
    Ok(found)
}

/// `Path.mkdir(parents=True, exist_ok=True)` in 3.13: `os.mkdir`; on `FileNotFoundError` the
/// parent first, then this one again; any other error is fine only if the path is a
/// directory now (an `is_dir()` that raises raises instead).
pub fn mkdir_parents<F: Fs + ?Sized>(fs: &F, path: &Path) -> Result<(), PyOsError> {
    match fs.mkdir(path) {
        Ok(()) => Ok(()),
        Err(error) if error.errno == Some(ENOENT) => {
            let parent = parent_of(path);
            if parent == path {
                return Err(error);
            }
            mkdir_parents(fs, &parent)?;
            match fs.mkdir(path) {
                Ok(()) => Ok(()),
                // The retry is `self.mkdir(mode, parents=False, exist_ok=exist_ok)`, which
                // raises `FileNotFoundError` as it is.
                Err(error) if error.errno == Some(ENOENT) => Err(error),
                Err(error) => exists_as_dir(fs, path, error),
            }
        }
        Err(error) => exists_as_dir(fs, path, error),
    }
}

/// `Path.mkdir(parents=True, exist_ok=False)` in 3.13: `os.mkdir`; on `FileNotFoundError`
/// the parents with `exist_ok=True`, then this one again, any error raised; any other error
/// raised as is.
pub fn mkdir_parents_new<F: Fs + ?Sized>(fs: &F, path: &Path) -> Result<(), PyOsError> {
    match fs.mkdir(path) {
        Err(error) if error.errno == Some(ENOENT) => {
            let parent = parent_of(path);
            if parent == path {
                return Err(error);
            }
            mkdir_parents(fs, &parent)?;
            fs.mkdir(path)
        }
        other => other,
    }
}

/// `Path.glob('**/<pattern>')` in 3.13 with `recurse_symlinks=False` (`glob._Globber`'s
/// `recursive_selector` over a `wildcard_selector`), at most `limit` paths: the matching
/// children of the path, then a walk that pops the last pending directory, and for each
/// sub-directory it lists (not entering symbolic links) yields that directory's matching
/// children before queueing it. A directory that cannot be listed contributes nothing; the
/// glob itself never fails. Python lists each directory twice (once to match its children,
/// once to walk it) and walks lazily, so a directory created while the walk runs can still be
/// found; here the listing taken for the match is kept for the walk and the paths are collected
/// before the caller acts on them. That yields the same paths in the same order as long as the
/// tree does not change under the walk (it does only when a mirror lies inside its own source,
/// where Python nests copies of copies and this copies once per run; neither is supported).
pub fn glob_recursive<F: Fs + ?Sized>(
    fs: &F,
    path: &Path,
    matches: &dyn Fn(&OsStr) -> bool,
    limit: usize,
) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let take = |entries: &[ScanEntry], found: &mut Vec<PathBuf>| {
        for entry in entries {
            if found.len() == limit {
                return;
            }
            if entry.path.file_name().is_some_and(matches) {
                found.push(entry.path.clone());
            }
        }
    };
    let listing = fs.scandir(path).unwrap_or_default();
    take(&listing, &mut found);
    let mut stack = vec![listing];
    while let Some(entries) = stack.pop() {
        for entry in entries.iter().filter(|entry| entry.is_dir) {
            if found.len() == limit {
                return found;
            }
            let children = fs.scandir(&entry.path).unwrap_or_default();
            take(&children, &mut found);
            stack.push(children);
        }
    }
    found
}

fn exists_as_dir<F: Fs + ?Sized>(fs: &F, path: &Path, error: PyOsError) -> Result<(), PyOsError> {
    if is_dir(fs, path)? { Ok(()) } else { Err(error) }
}

/// `Path.parent`: `.` for a relative name, `/` for the root.
fn parent_of(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        Some(_) => PathBuf::from("."),
        None => path.to_path_buf(),
    }
}

fn kind_is<F: Fs + ?Sized>(fs: &F, path: &Path, follow_symlinks: bool, kind: FileKind) -> Result<bool, PyOsError> {
    match fs.stat(path, follow_symlinks) {
        Ok(stat) => Ok(stat.kind == kind),
        Err(error) if error.is_ignored_by_pathlib() => Ok(false),
        Err(error) => Err(error),
    }
}

/// `Path.is_dir()`.
pub fn is_dir<F: Fs + ?Sized>(fs: &F, path: &Path) -> Result<bool, PyOsError> {
    kind_is(fs, path, true, FileKind::Directory)
}

/// `Path.is_file()`.
pub fn is_file<F: Fs + ?Sized>(fs: &F, path: &Path) -> Result<bool, PyOsError> {
    kind_is(fs, path, true, FileKind::File)
}

/// `Path.is_symlink()`.
pub fn is_symlink<F: Fs + ?Sized>(fs: &F, path: &Path) -> Result<bool, PyOsError> {
    kind_is(fs, path, false, FileKind::Symlink)
}

/// `Path.exists()`.
pub fn exists<F: Fs + ?Sized>(fs: &F, path: &Path) -> Result<bool, PyOsError> {
    match fs.stat(path, true) {
        Ok(_) => Ok(true),
        Err(error) if error.is_ignored_by_pathlib() => Ok(false),
        Err(error) => Err(error),
    }
}

/// `Path.stat().st_size`.
pub fn size<F: Fs + ?Sized>(fs: &F, path: &Path) -> Result<u64, PyOsError> {
    Ok(fs.stat(path, true)?.size)
}

/// `core.media.is_video(path)`: a regular file (links followed) with a video suffix, the
/// file test first, so a failing `stat` raises whatever the name.
pub fn is_video<F: Fs + ?Sized>(fs: &F, path: &Path) -> Result<bool, PyOsError> {
    Ok(is_file(fs, path)? && embyx_core::media::has_video_suffix(path))
}

/// The real file system.
#[derive(Debug, Clone, Copy, Default)]
pub struct LocalFs;

impl Fs for LocalFs {
    fn stat(&self, path: &Path, follow_symlinks: bool) -> Result<FileStat, PyOsError> {
        let metadata = if follow_symlinks {
            std::fs::metadata(path)
        } else {
            std::fs::symlink_metadata(path)
        }
        .map_err(|error| PyOsError::from_io(&error, Some(path), None))?;
        let file_type = metadata.file_type();
        let kind = if file_type.is_symlink() {
            FileKind::Symlink
        } else if file_type.is_dir() {
            FileKind::Directory
        } else if file_type.is_file() {
            FileKind::File
        } else {
            FileKind::Other
        };
        Ok(FileStat {
            kind,
            size: metadata.len(),
            mtime: py_mtime(&metadata),
        })
    }

    fn scandir(&self, path: &Path) -> Result<Vec<ScanEntry>, PyOsError> {
        let fail = |error: io::Error| PyOsError::from_io(&error, Some(path), None);
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(path).map_err(fail)? {
            let entry = entry.map_err(fail)?;
            let is_dir = entry.file_type().is_ok_and(|kind| kind.is_dir());
            entries.push(ScanEntry {
                path: path.join(entry.file_name()),
                is_dir,
            });
        }
        Ok(entries)
    }

    fn mkdir(&self, path: &Path) -> Result<(), PyOsError> {
        std::fs::create_dir(path).map_err(|error| PyOsError::from_io(&error, Some(path), None))
    }

    fn rename(&self, src: &Path, dst: &Path) -> Result<(), PyOsError> {
        std::fs::rename(src, dst).map_err(|error| PyOsError::from_io(&error, Some(src), Some(dst)))
    }

    fn unlink(&self, path: &Path) -> Result<(), PyOsError> {
        std::fs::remove_file(path).map_err(|error| PyOsError::from_io(&error, Some(path), None))
    }

    fn rmdir(&self, path: &Path) -> Result<(), PyOsError> {
        std::fs::remove_dir(path).map_err(|error| PyOsError::from_io(&error, Some(path), None))
    }

    fn copy2(&self, src: &Path, dst: &Path) -> Result<(), PyOsError> {
        copy::copy2(src, dst)
    }

    fn cmp_files(&self, a: &Path, b: &Path) -> Result<bool, PyOsError> {
        copy::cmp_files(a, b)
    }

    /// `shutil.rmtree` (3.13, `_rmtree_safe_fd`): every directory is opened and walked
    /// through file descriptors, so a directory swapped for a symbolic link mid-walk is refused
    /// rather than followed. The top must be a real directory ("Cannot call rmtree on a
    /// symbolic link", `NotADirectoryError` for a file). Inside, files are unlinked in listing
    /// order as each directory is read, sub-directories are walked after it, last listed
    /// first, and each directory is removed once emptied; an entry that vanished meanwhile is
    /// skipped; the first other error stops the walk and is raised.
    fn rmtree(&self, path: &Path) -> Result<(), PyOsError> {
        let mut stack = vec![Removal::Walk {
            parent: None,
            name: path.as_os_str().to_owned(),
            path: path.to_path_buf(),
        }];
        while let Some(step) = stack.pop() {
            rmtree_step(path, step, &mut stack)?;
        }
        Ok(())
    }
}

/// One item of [`LocalFs::rmtree`]'s stack, `(func, dirfd, path, orig_entry)` in
/// `_rmtree_safe_fd`. `parent` is the open directory the name is relative to (`None` for the
/// top, which is opened by its path); `path` is what an error names.
enum Removal {
    Walk {
        parent: Option<Rc<OwnedFd>>,
        name: OsString,
        path: PathBuf,
    },
    /// `os.close(topfd)`: dropping the last reference closes it.
    Close(Rc<OwnedFd>),
    Rmdir {
        parent: Option<Rc<OwnedFd>>,
        name: OsString,
        path: PathBuf,
    },
}

/// The `OSError` an `os` call raises for this errno, unnamed.
fn errno_error(errno: rustix::io::Errno) -> PyOsError {
    PyOsError::errno(errno.raw_os_error(), None, None)
}

/// `_rmtree_safe_fd(stack, onexc)`: one step. `err.filename = path`: the `Path` object for
/// the top, the joined `str` below it; `FileNotFoundError` below the top is ignored.
fn rmtree_step(top: &Path, step: Removal, stack: &mut Vec<Removal>) -> Result<(), PyOsError> {
    use rustix::fs::{AtFlags, CWD, Dir, FileType, Mode, OFlags, fstat, openat, statat, unlinkat};
    use rustix::io::Errno;

    let (parent, name, path) = match step {
        Removal::Close(fd) => {
            drop(fd);
            return Ok(());
        }
        Removal::Rmdir { parent, name, path } => {
            let removed = match &parent {
                None => std::fs::remove_dir(&path).map_err(|error| PyOsError::from_io(&error, None, None)),
                Some(dir) => unlinkat(&**dir, name.as_os_str(), AtFlags::REMOVEDIR).map_err(errno_error),
            };
            return named(removed, top, &path, parent.is_none());
        }
        Removal::Walk { parent, name, path } => (parent, name, path),
    };
    let is_top = parent.is_none();
    let walked = (|| -> Result<(), PyOsError> {
        let dir_fd = parent.as_deref().map_or(CWD, AsFd::as_fd);
        // `os.lstat(name, dir_fd=)` for the top, the entry's cached `stat(follow_symlinks=False)`
        // below it.
        let original = statat(dir_fd, name.as_os_str(), AtFlags::SYMLINK_NOFOLLOW).map_err(errno_error)?;
        let opened = openat(
            dir_fd,
            name.as_os_str(),
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(errno_error)?;
        let opened = Rc::new(opened);
        let current = fstat(&*opened).map_err(errno_error);
        let same = current
            .as_ref()
            .is_ok_and(|current| current.st_dev == original.st_dev && current.st_ino == original.st_ino);
        if same {
            stack.push(Removal::Rmdir {
                parent: parent.clone(),
                name: name.clone(),
                path: path.clone(),
            });
        }
        stack.push(Removal::Close(Rc::clone(&opened)));
        current?;
        if !same {
            // Symlinks to directories are forbidden, see GH-46010.
            return Err(PyOsError::message("Cannot call rmtree on a symbolic link"));
        }
        let mut entries = Vec::new();
        // `os.scandir(topfd)` is `fdopendir(dup(topfd))`: no second open of the directory, so a
        // directory without search permission is still listed.
        let listing = rustix::io::dup(&*opened).map_err(errno_error)?;
        for entry in Dir::new(listing).map_err(errno_error)? {
            let entry = entry.map_err(errno_error)?;
            let entry_name = OsStr::from_bytes(entry.file_name().to_bytes()).to_owned();
            if entry_name == "." || entry_name == ".." {
                continue;
            }
            entries.push((entry_name, entry.file_type()));
        }
        for (entry_name, kind) in entries {
            let full = path.join(&entry_name);
            // `entry.is_dir(follow_symlinks=False)`: the listing's type, `lstat` when unknown.
            let is_dir = match kind {
                FileType::Unknown => match statat(&*opened, entry_name.as_os_str(), AtFlags::SYMLINK_NOFOLLOW) {
                    Ok(stat) => FileType::from_raw_mode(stat.st_mode) == FileType::Directory,
                    Err(Errno::NOENT) => continue,
                    Err(_) => false,
                },
                kind => kind == FileType::Directory,
            };
            if is_dir {
                stack.push(Removal::Walk {
                    parent: Some(Rc::clone(&opened)),
                    name: entry_name,
                    path: full,
                });
                continue;
            }
            match unlinkat(&*opened, entry_name.as_os_str(), AtFlags::empty()) {
                Ok(()) | Err(Errno::NOENT) => {}
                // `onexc` re-raises inside the walk's own `try`, so the error ends up naming
                // the directory being walked, not the entry.
                Err(errno) => return Err(errno_error(errno)),
            }
        }
        Ok(())
    })();
    named(walked, top, &path, is_top)
}

/// `except FileNotFoundError` / `except OSError` around a step: `err.filename = path` (the
/// `Path` object at the top, the joined `str` below it), and a vanished entry below the top
/// is no error.
fn named(result: Result<(), PyOsError>, top: &Path, path: &Path, is_top: bool) -> Result<(), PyOsError> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if is_top => Err(error.with_path_object(top)),
        Err(error) if error.errno == Some(ENOENT) => Ok(()),
        Err(mut error) => {
            error.filename = Some(Filename::of(path));
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests;
