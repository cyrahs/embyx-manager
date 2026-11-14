//! `shutil.copy2` and `filecmp.cmp(shallow=False)` as Python 3.13 runs them on Linux, for the
//! `.strm` mapping (`monitor/mapping.py`).
//!
//! `copy2(src, dst)`: a destination that is a directory gets the source's name inside it;
//! the same file twice is `SameFileError`, a named pipe `SpecialFileError`; the bytes are
//! copied (`open(src, 'rb')`, `open(dst, 'wb')`), then `copystat`: access and modification
//! times to the nanosecond, the extended attributes (`ENOTSUP`, `ENODATA` and `EINVAL` on
//! listing, `EPERM`, `ENOTSUP`, `ENODATA`, `EINVAL` and `EACCES` on copying one are ignored),
//! then the permission bits. Each error names the file Python's call names.

use std::fs::{File, Metadata, OpenOptions};
use std::io::{self, Read as _};
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use embyx_core::pytext::py_repr;

use super::{PyOsError, path_text};

/// `errno` numbers `_copyxattr` ignores.
const ENODATA: i32 = 61;
const ENOTSUP: i32 = 95;
const EINVAL: i32 = 22;
const EPERM: i32 = 1;
const EACCES: i32 = 13;
const EISDIR: i32 = 21;
/// `S_IFIFO`.
const FIFO: u32 = 0o010_000;
const FORMAT_BITS: u32 = 0o170_000;
const REGULAR: u32 = 0o100_000;

fn os_error(error: &io::Error, filename: &Path) -> PyOsError {
    PyOsError::from_io(error, Some(filename), None)
}

fn errno_of(error: &rustix::io::Errno) -> i32 {
    error.raw_os_error()
}

/// `shutil.copy2(src, dst)` with `Path` arguments.
pub(super) fn copy2(src: &Path, dst: &Path) -> Result<(), PyOsError> {
    // `os.path.isdir(dst)`: any error reads as "no". The joined name is a `str` from here on.
    let (target, joined) = if std::fs::metadata(dst).is_ok_and(|metadata| metadata.is_dir()) {
        (dst.join(src.file_name().unwrap_or_default()), true)
    } else {
        (dst.to_path_buf(), false)
    };
    copyfile(src, &target, joined)?;
    copystat(src, &target)
}

/// `_samefile(src, dst)`: `os.path.samefile`, any error reading as "no".
fn same_file(src: &Path, dst: &Path) -> bool {
    match (std::fs::metadata(src), std::fs::metadata(dst)) {
        (Ok(a), Ok(b)) => a.dev() == b.dev() && a.ino() == b.ino(),
        _ => false,
    }
}

fn copyfile(src: &Path, dst: &PathBuf, joined: bool) -> Result<(), PyOsError> {
    if same_file(src, dst) {
        let shown = |path: &Path, is_str: bool| {
            let text = py_repr(&path_text(path));
            if is_str { text } else { format!("PosixPath({text})") }
        };
        return Err(PyOsError::classed(
            "SameFileError",
            format!("{} and {} are the same file", shown(src, false), shown(dst, joined)),
        ));
    }
    for path in [src, dst.as_path()] {
        if let Ok(metadata) = std::fs::metadata(path)
            && metadata.mode() & FORMAT_BITS == FIFO
        {
            return Err(PyOsError::classed(
                "SpecialFileError",
                format!("`{}` is a named pipe", path_text(path)),
            ));
        }
    }
    let mut source = open_for_reading(src)?;
    let mut target = match OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o666)
        .open(dst)
    {
        Ok(file) => file,
        // Issue 43219: a less confusing error for a destination in a missing directory.
        Err(error) if error.raw_os_error() == Some(EISDIR) && !dst.exists() => {
            return Err(PyOsError::classed(
                "FileNotFoundError",
                format!("Directory does not exist: {}", path_text(dst)),
            ));
        }
        Err(error) => return Err(os_error(&error, dst)),
    };
    // `_fastcopy_sendfile`: its errors carry both names.
    io::copy(&mut source, &mut target).map_err(|error| PyOsError::from_io(&error, Some(src), Some(dst)))?;
    Ok(())
}

/// `open(path, 'rb')`: a directory opens at the OS level and `FileIO` refuses it with
/// `IsADirectoryError`.
fn open_for_reading(path: &Path) -> Result<File, PyOsError> {
    let file = File::open(path).map_err(|error| os_error(&error, path))?;
    let metadata = file.metadata().map_err(|error| os_error(&error, path))?;
    if metadata.is_dir() {
        return Err(PyOsError::errno(EISDIR, Some(path), None));
    }
    Ok(file)
}

/// `shutil.copystat(src, dst)`, symbolic links followed.
fn copystat(src: &Path, dst: &Path) -> Result<(), PyOsError> {
    let metadata = std::fs::metadata(src).map_err(|error| os_error(&error, src))?;
    set_times(dst, &metadata)?;
    copy_xattrs(src, dst)?;
    std::fs::set_permissions(dst, std::fs::Permissions::from_mode(metadata.mode() & 0o7777))
        .map_err(|error| os_error(&error, dst))
}

/// `os.utime(dst, ns=(st.st_atime_ns, st.st_mtime_ns))`.
fn set_times(dst: &Path, metadata: &Metadata) -> Result<(), PyOsError> {
    use rustix::fs::{AtFlags, CWD, Timestamps, utimensat};
    let times = Timestamps {
        last_access: rustix::fs::Timespec {
            tv_sec: metadata.atime(),
            tv_nsec: metadata.atime_nsec(),
        },
        last_modification: rustix::fs::Timespec {
            tv_sec: metadata.mtime(),
            tv_nsec: metadata.mtime_nsec(),
        },
    };
    utimensat(CWD, dst, &times, AtFlags::empty()).map_err(|error| PyOsError::errno(errno_of(&error), Some(dst), None))
}

/// `os.listxattr(path)`: the names, growing the buffer as the size changes under it.
fn list_xattrs(path: &Path) -> Result<Vec<Vec<u8>>, rustix::io::Errno> {
    loop {
        let size = rustix::fs::listxattr_query(path)?;
        let mut buffer = vec![0_u8; size];
        match rustix::fs::listxattr(path, &mut buffer[..]) {
            Ok(length) => {
                buffer.truncate(length);
                return Ok(buffer
                    .split(|byte| *byte == 0)
                    .filter(|name| !name.is_empty())
                    .map(<[u8]>::to_vec)
                    .collect());
            }
            Err(rustix::io::Errno::RANGE) => {}
            Err(error) => return Err(error),
        }
    }
}

/// `os.getxattr(path, name)`.
fn get_xattr(path: &Path, name: &[u8]) -> Result<Vec<u8>, rustix::io::Errno> {
    use std::os::unix::ffi::OsStrExt as _;
    let name = std::ffi::OsStr::from_bytes(name);
    loop {
        let size = rustix::fs::getxattr_query(path, name)?;
        let mut buffer = vec![0_u8; size];
        match rustix::fs::getxattr(path, name, &mut buffer[..]) {
            Ok(length) => {
                buffer.truncate(length);
                return Ok(buffer);
            }
            Err(rustix::io::Errno::RANGE) => {}
            Err(error) => return Err(error),
        }
    }
}

/// `_copyxattr(src, dst)`.
fn copy_xattrs(src: &Path, dst: &Path) -> Result<(), PyOsError> {
    use std::os::unix::ffi::OsStrExt as _;
    let names = match list_xattrs(src) {
        Ok(names) => names,
        Err(error) if [ENOTSUP, ENODATA, EINVAL].contains(&errno_of(&error)) => return Ok(()),
        Err(error) => return Err(PyOsError::errno(errno_of(&error), Some(src), None)),
    };
    for name in names {
        let copied = get_xattr(src, &name).map_err(|error| (error, src)).and_then(|value| {
            rustix::fs::setxattr(
                dst,
                std::ffi::OsStr::from_bytes(&name),
                &value,
                rustix::fs::XattrFlags::empty(),
            )
            .map_err(|error| (error, dst))
        });
        match copied {
            Ok(()) => {}
            Err((error, _)) if [EPERM, ENOTSUP, ENODATA, EINVAL, EACCES].contains(&errno_of(&error)) => {}
            Err((error, path)) => return Err(PyOsError::errno(errno_of(&error), Some(path), None)),
        }
    }
    Ok(())
}

/// `filecmp.cmp(a, b, shallow=False)`: `False` unless both are regular files (links
/// followed) of one size; then the bytes, read in 8 KiB blocks until they differ. The module's
/// result cache is not kept (DIVERGENCES.md D90).
pub(super) fn cmp_files(a: &Path, b: &Path) -> Result<bool, PyOsError> {
    let first = std::fs::metadata(a).map_err(|error| os_error(&error, a))?;
    let second = std::fs::metadata(b).map_err(|error| os_error(&error, b))?;
    if first.mode() & FORMAT_BITS != REGULAR || second.mode() & FORMAT_BITS != REGULAR {
        return Ok(false);
    }
    if first.len() != second.len() {
        return Ok(false);
    }
    let mut left = File::open(a).map_err(|error| os_error(&error, a))?;
    let mut right = File::open(b).map_err(|error| os_error(&error, b))?;
    let (mut one, mut two) = ([0_u8; 8192], [0_u8; 8192]);
    loop {
        let read_one = read_block(&mut left, &mut one)?;
        let read_two = read_block(&mut right, &mut two)?;
        if one[..read_one] != two[..read_two] {
            return Ok(false);
        }
        if read_one == 0 {
            return Ok(true);
        }
    }
}

/// `fp.read(8192)`: up to a full block, short only at the end of the file.
fn read_block(file: &mut File, buffer: &mut [u8]) -> Result<usize, PyOsError> {
    let mut filled = 0;
    while filled < buffer.len() {
        match file.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(count) => filled += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(PyOsError::from_io(&error, None, None)),
        }
    }
    Ok(filled)
}
