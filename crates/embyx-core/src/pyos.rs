//! An `OSError` as Python raises and prints it: the subclass for the errno, `str(exc)` as
//! `[Errno N] strerror: 'filename'` (with ` -> 'filename2'` for a two-path call), and
//! `exc.strerror`.
//!
//! Python origin: `CPython` 3.13 `Objects/exceptions.c` (`OSError_str`, the errno-to-subclass
//! map). One copy for the crates that report I/O errors as plain text: the Kubernetes client
//! (the service-account token and the Job template) and the merge service. The monitor's and
//! the fill actor's file systems keep their own structured error
//! (`embyx_monitor::fs::PyOsError`), which also carries the errno and both file names for
//! `repr()`, and takes its subclass from [`errno_class`].

use std::fmt::Write as _;
use std::io;
use std::path::Path;

use crate::pytext;

/// An `OSError` reduced to what a caller reports: its class and `str()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PyOsError {
    pub class: &'static str,
    pub message: String,
}

impl PyOsError {
    /// The error Python raises for `error` on `path`.
    pub fn from_io(error: &io::Error, path: Option<&Path>) -> Self {
        Self {
            class: class_name(error),
            message: os_error_text(error, path, None),
        }
    }
}

impl std::fmt::Display for PyOsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// The `OSError` subclass Python raises for an I/O error. The class follows the kind even
/// without an errno: `tempfile` wraps the OS error in its own (`io::Error::new(kind, ..)`),
/// where Python still raises the subclass.
pub fn class_name(error: &io::Error) -> &'static str {
    if let Some(code) = error.raw_os_error() {
        return errno_class(code);
    }
    // An error that lost its errno (wrapped by a library) keeps the subclass of its kind.
    match error.kind() {
        io::ErrorKind::NotFound => "FileNotFoundError",
        io::ErrorKind::AlreadyExists => "FileExistsError",
        io::ErrorKind::PermissionDenied => "PermissionError",
        io::ErrorKind::IsADirectory => "IsADirectoryError",
        io::ErrorKind::NotADirectory => "NotADirectoryError",
        _ => "OSError",
    }
}

/// The `OSError` subclass `CPython` picks for an errno (`Objects/exceptions.c`'s map, Linux
/// numbers); `OSError` itself for any other.
pub fn errno_class(errno: i32) -> &'static str {
    match errno {
        11 | 114 | 115 => "BlockingIOError",
        10 => "ChildProcessError",
        32 | 108 => "BrokenPipeError",
        103 => "ConnectionAbortedError",
        111 => "ConnectionRefusedError",
        104 => "ConnectionResetError",
        17 => "FileExistsError",
        2 => "FileNotFoundError",
        21 => "IsADirectoryError",
        20 => "NotADirectoryError",
        4 => "InterruptedError",
        1 | 13 => "PermissionError",
        3 => "ProcessLookupError",
        110 => "TimeoutError",
        _ => "OSError",
    }
}

/// `str(path)` quoted as Python's `repr` quotes it.
pub fn repr_path(path: &Path) -> String {
    pytext::py_repr(&path.to_string_lossy())
}

/// `str(OSError)`: `[Errno N] strerror: 'filename'`, with ` -> 'filename2'` for a rename.
/// An error without an errno prints as it is.
pub fn os_error_text(error: &io::Error, filename: Option<&Path>, filename2: Option<&Path>) -> String {
    let Some(code) = error.raw_os_error() else {
        return error.to_string();
    };
    let mut out = format!("[Errno {code}] {}", errno_text(code));
    if let Some(filename) = filename {
        let _ = write!(out, ": {}", repr_path(filename));
        if let Some(filename2) = filename2 {
            let _ = write!(out, " -> {}", repr_path(filename2));
        }
    }
    out
}

/// `OSError.strerror`, or the whole text when the error has no errno.
pub fn strerror(error: &io::Error) -> String {
    match error.raw_os_error() {
        Some(code) => errno_text(code),
        None => error.to_string(),
    }
}

/// `os.strerror(code)`: the C library's text without Rust's ` (os error N)`.
fn errno_text(code: i32) -> String {
    let text = io::Error::from_raw_os_error(code).to_string();
    text.strip_suffix(&format!(" (os error {code})"))
        .unwrap_or(&text)
        .to_owned()
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::path::Path;

    use super::{PyOsError, os_error_text, strerror};

    #[test]
    fn os_errors_read_like_python() {
        let error = PyOsError::from_io(&io::Error::from_raw_os_error(2), Some(Path::new("/x/y.jpg")));
        assert_eq!(error.class, "FileNotFoundError");
        assert_eq!(error.message, "[Errno 2] No such file or directory: '/x/y.jpg'");
        let error = PyOsError::from_io(&io::Error::from_raw_os_error(13), None);
        assert_eq!(error.class, "PermissionError");
        assert_eq!(error.message, "[Errno 13] Permission denied");
        let error = PyOsError::from_io(&io::Error::from_raw_os_error(28), Some(Path::new("it's")));
        assert_eq!(error.class, "OSError");
        assert_eq!(error.message, "[Errno 28] No space left on device: \"it's\"");
    }

    #[test]
    fn a_wrapped_os_error_keeps_its_subclass() {
        // What `tempfile::tempdir()` returns for an unwritable TMPDIR: the OS error wrapped
        // in its own, so there is no raw errno.
        let inner = io::Error::from_raw_os_error(13);
        let wrapped = io::Error::new(inner.kind(), format!("{inner} at path \"/tmp/x\""));
        assert_eq!(wrapped.raw_os_error(), None);
        let error = PyOsError::from_io(&wrapped, None);
        assert_eq!(error.class, "PermissionError");
        assert_eq!(error.message, wrapped.to_string());
        let error = PyOsError::from_io(&io::Error::other("boom"), None);
        assert_eq!(error.class, "OSError");
    }

    #[test]
    fn an_errno_picks_cpythons_subclass() {
        // `type(OSError(code, 'x')).__name__` on Python 3.13 (Linux).
        for (code, class) in [
            (1, "PermissionError"),
            (2, "FileNotFoundError"),
            (3, "ProcessLookupError"),
            (4, "InterruptedError"),
            (10, "ChildProcessError"),
            (11, "BlockingIOError"),
            (13, "PermissionError"),
            (17, "FileExistsError"),
            (20, "NotADirectoryError"),
            (21, "IsADirectoryError"),
            (28, "OSError"),
            (32, "BrokenPipeError"),
            (103, "ConnectionAbortedError"),
            (104, "ConnectionResetError"),
            (108, "BrokenPipeError"),
            (110, "TimeoutError"),
            (111, "ConnectionRefusedError"),
            (114, "BlockingIOError"),
            (115, "BlockingIOError"),
        ] {
            assert_eq!(super::errno_class(code), class, "{code}");
            assert_eq!(
                PyOsError::from_io(&io::Error::from_raw_os_error(code), None).class,
                class,
                "{code}"
            );
        }
    }

    #[test]
    fn a_two_path_error_names_both_and_strerror_is_the_bare_text() {
        let error = io::Error::from_raw_os_error(18);
        assert_eq!(
            os_error_text(&error, Some(Path::new("/a")), Some(Path::new("/b"))),
            "[Errno 18] Invalid cross-device link: '/a' -> '/b'"
        );
        assert_eq!(strerror(&error), "Invalid cross-device link");
        assert_eq!(strerror(&io::Error::other("boom")), "boom");
    }
}
