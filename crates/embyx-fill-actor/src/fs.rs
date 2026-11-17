//! The file system as the fill actor's scans and local moves see it.
//!
//! Python origin: the `pathlib`, `os` and `ctypes` calls of `src/embyx_manager/fill_actor/service.py`
//! (`_rename_no_replace`, `os.link`, `Path.stat`, `Path.resolve`, ...), as `CPython` 3.13
//! implements them. A local move only ever renames without replacing, links without
//! following, and decides by inode identity, so those primitives are what [`MoveFs`] offers;
//! WP6's `embyx_monitor::fs::Fs` has no inodes, links or `renameat2` (DIVERGENCES.md D120).
//!
//! - `is_dir()`, `is_file()`, `exists()` and `is_symlink()` answer "no" only for `ENOENT`,
//!   `ENOTDIR`, `EBADF` and `ELOOP` (`pathlib._abc._ignore_error`); any other error is raised.
//! - [`realpath`] is `os.path.realpath(path, strict=...)` of 3.13.18 step for step, on bytes.
//! - Errors are WP6's [`PyOsError`], with the file names Python's call puts in them.
//!
//! Every call goes through [`MoveFs`] so tests and scenarios can record mutations, inject
//! failures, block a call or crash in front of one ([`recording`], behind `test-support`);
//! [`LocalMoveFs`] is the real one.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::path::{Component, Path, PathBuf};

pub use embyx_monitor::fs::PyOsError;
use embyx_monitor::fs::{EBADF, ELOOP, ENOENT, ENOTDIR};

#[cfg(any(test, feature = "test-support"))]
pub mod recording;

/// `EEXIST`.
pub const EEXIST: i32 = 17;
/// `EXDEV`.
pub const EXDEV: i32 = 18;
/// `EPERM`.
pub const EPERM: i32 = 1;
/// `EINVAL`.
pub const EINVAL: i32 = 22;
/// `ENOSYS`.
pub const ENOSYS: i32 = 38;
/// `EOPNOTSUPP` (Linux: the same number as `ENOTSUP`).
pub const EOPNOTSUPP: i32 = 95;

/// The kind of entry `stat` found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Directory,
    File,
    Symlink,
    Other,
}

/// What `os.stat` tells the fill actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stat {
    pub kind: FileKind,
    /// `st_dev`, `st_ino`, `st_size`, `st_mtime_ns`, `st_ctime_ns` as Python's integers.
    pub device: i64,
    pub inode: i64,
    pub size: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub nlink: u64,
}

/// The calls the fill actor makes. Each is one `os` call with Python's error.
pub trait MoveFs: Send + Sync + fmt::Debug {
    /// `os.stat(path, follow_symlinks=...)`.
    fn stat(&self, path: &Path, follow_symlinks: bool) -> Result<Stat, PyOsError>;
    /// `os.readlink(path)`.
    fn readlink(&self, path: &Path) -> Result<OsString, PyOsError>;
    /// `list(Path.iterdir())`: `os.listdir` order.
    fn iterdir(&self, path: &Path) -> Result<Vec<PathBuf>, PyOsError>;
    /// `os.mkdir(path, mode)`.
    fn mkdir(&self, path: &Path, mode: u32) -> Result<(), PyOsError>;
    /// `_rename_no_replace(source, destination)`: `renameat2(..., RENAME_NOREPLACE)`;
    /// `FileExistsError(EEXIST, strerror, destination)` when the destination exists, else
    /// `OSError(errno, strerror, source, destination)`.
    fn rename_noreplace(&self, source: &Path, destination: &Path) -> Result<(), PyOsError>;
    /// `os.link(source, destination, follow_symlinks=False)`.
    fn link(&self, source: &Path, destination: &Path) -> Result<(), PyOsError>;
    /// `Path.rename(target)`: `os.rename`, which replaces an existing file.
    fn rename(&self, source: &Path, destination: &Path) -> Result<(), PyOsError>;
    /// `Path.unlink()`.
    fn unlink(&self, path: &Path) -> Result<(), PyOsError>;
    /// `Path.read_bytes()`, read no further than `limit` bytes (the `.strm` mapping, whose
    /// size is refused past its limit either way).
    fn read_limited(&self, path: &Path, limit: usize) -> Result<Vec<u8>, PyOsError>;
    /// `os.getcwd()`.
    fn getcwd(&self) -> Result<PathBuf, PyOsError>;
}

// ---------- pathlib on MoveFs ----------

/// `pathlib._abc._ignore_error`: the errors `is_dir()` and friends read as "no".
fn ignored(error: &PyOsError) -> bool {
    error
        .errno
        .is_some_and(|code| [ENOENT, ENOTDIR, EBADF, ELOOP].contains(&code))
}

fn kind_is(fs: &dyn MoveFs, path: &Path, follow_symlinks: bool, kind: FileKind) -> Result<bool, PyOsError> {
    match fs.stat(path, follow_symlinks) {
        Ok(stat) => Ok(stat.kind == kind),
        Err(error) if ignored(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

/// `Path.is_dir()`.
pub fn is_dir(fs: &dyn MoveFs, path: &Path) -> Result<bool, PyOsError> {
    kind_is(fs, path, true, FileKind::Directory)
}

/// `Path.is_file()`.
pub fn is_file(fs: &dyn MoveFs, path: &Path) -> Result<bool, PyOsError> {
    kind_is(fs, path, true, FileKind::File)
}

/// `Path.is_symlink()`.
pub fn is_symlink(fs: &dyn MoveFs, path: &Path) -> Result<bool, PyOsError> {
    kind_is(fs, path, false, FileKind::Symlink)
}

/// `Path.exists()`.
pub fn exists(fs: &dyn MoveFs, path: &Path) -> Result<bool, PyOsError> {
    match fs.stat(path, true) {
        Ok(_) => Ok(true),
        Err(error) if ignored(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

/// `Path.mkdir(mode=mode, exist_ok=True)` (no parents): `FileNotFoundError` is raised as it
/// is, any other error only when the path is not a directory now.
pub fn mkdir_exist_ok(fs: &dyn MoveFs, path: &Path, mode: u32) -> Result<(), PyOsError> {
    match fs.mkdir(path, mode) {
        Ok(()) => Ok(()),
        Err(error) if error.errno == Some(ENOENT) => Err(error),
        Err(error) => {
            if is_dir(fs, path)? {
                Ok(())
            } else {
                Err(error)
            }
        }
    }
}

/// `Path.absolute()`: the path itself when absolute, else joined to `os.getcwd()`; nothing is
/// normalised or resolved.
pub fn absolute(fs: &dyn MoveFs, path: &Path) -> Result<PathBuf, PyOsError> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(fs.getcwd()?.join(path))
}

/// `PurePath.relative_to(other)` (`walk_up=False`) as a yes/no: `other` is the path itself or
/// one of its parents, part by part (pathlib's parts: the anchor, then every name; `.` and
/// empty names dropped, `..` kept).
pub fn is_relative_to(path: &Path, other: &Path) -> bool {
    let parts = |path: &Path| -> Vec<OsString> {
        path.components()
            .filter(|component| !matches!(component, Component::CurDir))
            .map(|component| component.as_os_str().to_owned())
            .collect()
    };
    let (mine, theirs) = (parts(path), parts(other));
    mine.starts_with(&theirs)
}

/// The part names of `path.relative_to(root)` (`None` where pathlib raises `ValueError`).
pub fn relative_parts(path: &Path, root: &Path) -> Option<Vec<OsString>> {
    let parts = |path: &Path| -> Vec<OsString> {
        path.components()
            .filter(|component| !matches!(component, Component::CurDir))
            .map(|component| component.as_os_str().to_owned())
            .collect()
    };
    let (mine, theirs) = (parts(path), parts(root));
    mine.starts_with(&theirs).then(|| mine[theirs.len()..].to_vec())
}

/// How strict [`realpath`] is (`os.path.realpath(path, strict=...)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strict {
    /// `strict=False`: every `OSError` is ignored.
    No,
    /// `strict=True`: none is.
    Yes,
}

/// What one `lstat` (and `readlink`) of [`realpath`] found.
enum Found {
    NotLink,
    /// A link met before: its resolved target, or `None` while it is unresolved.
    Seen(Option<Vec<u8>>),
    Link(Vec<u8>),
}

/// `os.path.realpath(path, strict=...)` as `CPython` 3.13.18 walks it: one part at a time,
/// `lstat` each, follow a link by `readlink` (a link already resolved reuses its target, a link
/// met again while unresolved is a loop), `..` drops the last part of what is resolved so far.
/// With `strict=False` a part whose `lstat` or `readlink` fails is taken as it is.
pub fn realpath(fs: &dyn MoveFs, path: &Path, strict: Strict) -> Result<PathBuf, PyOsError> {
    /// One item of the stack of parts still to resolve (`rest`).
    enum Part {
        Name(Vec<u8>),
        /// `None`, then the link's path: the link's target is resolved now.
        Resolved(Vec<u8>),
    }
    let filename = path.as_os_str().as_bytes();
    let mut rest: Vec<Part> = filename
        .split(|&byte| byte == b'/')
        .rev()
        .map(|name| Part::Name(name.to_vec()))
        .collect();
    let mut part_count = rest.len();
    let mut resolved: Vec<u8> = if filename.starts_with(b"/") {
        b"/".to_vec()
    } else {
        fs.getcwd()?.into_os_string().into_vec()
    };
    let mut seen: std::collections::HashMap<Vec<u8>, Option<Vec<u8>>> = std::collections::HashMap::new();
    while part_count > 0 {
        let Some(part) = rest.pop() else { break };
        let name = match part {
            Part::Resolved(link) => {
                seen.insert(link, Some(resolved.clone()));
                continue;
            }
            Part::Name(name) => name,
        };
        part_count -= 1;
        if name.is_empty() || name == b"." {
            continue;
        }
        if name == b".." {
            let cut = resolved.iter().rposition(|&byte| byte == b'/').unwrap_or(0);
            resolved.truncate(cut);
            if resolved.is_empty() {
                resolved.push(b'/');
            }
            continue;
        }
        let mut newpath = resolved.clone();
        if newpath != b"/" {
            newpath.push(b'/');
        }
        newpath.extend_from_slice(&name);
        let new = PathBuf::from(OsString::from_vec(newpath.clone()));
        let step = (|| -> Result<Found, PyOsError> {
            let stat = fs.stat(&new, false)?;
            if stat.kind != FileKind::Symlink {
                if strict == Strict::Yes && part_count > 0 && stat.kind != FileKind::Directory {
                    return Err(PyOsError::errno(ENOTDIR, Some(&new), None));
                }
                return Ok(Found::NotLink);
            }
            if let Some(cached) = seen.get(&newpath) {
                if cached.is_none() && strict == Strict::Yes {
                    // A symlink loop: `os.stat` raises `ELOOP`.
                    fs.stat(&new, true)?;
                }
                return Ok(Found::Seen(cached.clone()));
            }
            Ok(Found::Link(fs.readlink(&new)?.into_vec()))
        })();
        match step {
            Ok(Found::NotLink | Found::Seen(None)) => resolved = newpath,
            Ok(Found::Seen(Some(cached))) => resolved = cached,
            Ok(Found::Link(target)) => {
                if target.starts_with(b"/") {
                    resolved = b"/".to_vec();
                }
                // Marked as seen but not yet resolved; the marker records the target once
                // its parts are resolved.
                seen.insert(newpath.clone(), None);
                rest.push(Part::Resolved(newpath));
                let parts: Vec<&[u8]> = target.split(|&byte| byte == b'/').collect();
                part_count += parts.len();
                rest.extend(parts.into_iter().rev().map(|name| Part::Name(name.to_vec())));
            }
            // An error occurred and was ignored.
            Err(_) if strict == Strict::No => resolved = newpath,
            Err(error) => return Err(error),
        }
    }
    Ok(PathBuf::from(OsString::from_vec(resolved)))
}

/// `Path.name` as bytes (`""` for `/`).
pub fn name(path: &Path) -> &OsStr {
    path.file_name().unwrap_or_else(|| OsStr::new(""))
}

// ---------- the real file system ----------

/// The real file system (`rustix` system calls, so `renameat2` and `linkat` are the ones
/// Python's `ctypes` and `os.link` call).
#[derive(Debug, Clone, Copy, Default)]
pub struct LocalMoveFs;

fn errno(error: rustix::io::Errno, filename: Option<&Path>, filename2: Option<&Path>) -> PyOsError {
    PyOsError::errno(error.raw_os_error(), filename, filename2)
}

fn nanoseconds(seconds: i64, nanos: i64) -> i64 {
    seconds.saturating_mul(1_000_000_000).saturating_add(nanos)
}

/// A `st_*_nsec` field, whose type differs between architectures (always below 10^9).
fn subsecond(value: impl TryInto<i64>) -> i64 {
    value.try_into().unwrap_or(0)
}

impl MoveFs for LocalMoveFs {
    fn stat(&self, path: &Path, follow_symlinks: bool) -> Result<Stat, PyOsError> {
        use rustix::fs::{AtFlags, CWD, FileType, statat};
        let flags = if follow_symlinks {
            AtFlags::empty()
        } else {
            AtFlags::SYMLINK_NOFOLLOW
        };
        let stat = statat(CWD, path, flags).map_err(|error| errno(error, Some(path), None))?;
        let kind = match FileType::from_raw_mode(stat.st_mode) {
            FileType::Directory => FileKind::Directory,
            FileType::RegularFile => FileKind::File,
            FileType::Symlink => FileKind::Symlink,
            _ => FileKind::Other,
        };
        Ok(Stat {
            kind,
            device: stat.st_dev.cast_signed(),
            inode: stat.st_ino.cast_signed(),
            size: stat.st_size,
            mtime_ns: nanoseconds(stat.st_mtime, subsecond(stat.st_mtime_nsec)),
            ctime_ns: nanoseconds(stat.st_ctime, subsecond(stat.st_ctime_nsec)),
            nlink: stat.st_nlink,
        })
    }

    fn readlink(&self, path: &Path) -> Result<OsString, PyOsError> {
        std::fs::read_link(path)
            .map(PathBuf::into_os_string)
            .map_err(|error| PyOsError::from_io(&error, Some(path), None))
    }

    fn iterdir(&self, path: &Path) -> Result<Vec<PathBuf>, PyOsError> {
        let fail = |error: std::io::Error| PyOsError::from_io(&error, Some(path), None);
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(path).map_err(fail)? {
            entries.push(path.join(entry.map_err(fail)?.file_name()));
        }
        Ok(entries)
    }

    fn mkdir(&self, path: &Path, mode: u32) -> Result<(), PyOsError> {
        rustix::fs::mkdir(path, rustix::fs::Mode::from_raw_mode(mode)).map_err(|error| errno(error, Some(path), None))
    }

    fn rename_noreplace(&self, source: &Path, destination: &Path) -> Result<(), PyOsError> {
        use rustix::fs::{CWD, RenameFlags, renameat_with};
        match renameat_with(CWD, source, CWD, destination, RenameFlags::NOREPLACE) {
            Ok(()) => Ok(()),
            Err(rustix::io::Errno::EXIST) => Err(PyOsError::errno(EEXIST, Some(destination), None)),
            Err(error) => Err(errno(error, Some(source), Some(destination))),
        }
    }

    fn link(&self, source: &Path, destination: &Path) -> Result<(), PyOsError> {
        use rustix::fs::{AtFlags, CWD, linkat};
        linkat(CWD, source, CWD, destination, AtFlags::empty())
            .map_err(|error| errno(error, Some(source), Some(destination)))
    }

    fn rename(&self, source: &Path, destination: &Path) -> Result<(), PyOsError> {
        std::fs::rename(source, destination)
            .map_err(|error| PyOsError::from_io(&error, Some(source), Some(destination)))
    }

    fn unlink(&self, path: &Path) -> Result<(), PyOsError> {
        std::fs::remove_file(path).map_err(|error| PyOsError::from_io(&error, Some(path), None))
    }

    fn read_limited(&self, path: &Path, limit: usize) -> Result<Vec<u8>, PyOsError> {
        use std::io::Read as _;
        let fail = |error: std::io::Error| PyOsError::from_io(&error, Some(path), None);
        let file = std::fs::File::open(path).map_err(fail)?;
        let mut payload = Vec::new();
        file.take(limit as u64).read_to_end(&mut payload).map_err(fail)?;
        Ok(payload)
    }

    fn getcwd(&self) -> Result<PathBuf, PyOsError> {
        std::env::current_dir().map_err(|error| PyOsError::from_io(&error, None, None))
    }
}

#[cfg(test)]
mod tests;
