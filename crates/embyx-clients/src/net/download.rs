//! Response bodies streamed to disk.
//!
//! Python origin: Kemono's `_attempt_download` (`tempfile.mkstemp` beside the
//! destination, chunks written and hashed with sha256, `tmp_path.replace(dst_path)` once
//! complete, the temporary file removed on any failure) and Jandan's
//! `dst_path.write_bytes(response.content)`. Every download goes through the first
//! shape here, so memory stays flat whatever the file size and a crash never leaves a
//! partial file under the final name (`docs/rewrite/PLAN.md`, memory rules).
//!
//! Copied from fav (`fav-net/src/download.rs`); see docs/CONVENTIONS.md, "Code copied from fav". `D` numbers
//! marked fav's are fav's own DIVERGENCES.md entries.

use std::io;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::net::client::{Response, TransportError};

/// How the temporary file is named and created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TempOptions<'a> {
    /// `mkstemp(prefix=...)`; eight random characters and `suffix` follow.
    pub prefix: &'a str,
    pub suffix: &'a str,
    /// Permission bits before the umask: `0o600` for `mkstemp`, `0o666` for an
    /// ordinary `open(..., 'wb')`.
    pub mode: u32,
}

/// Why a body did not reach the disk.
#[derive(Debug, thiserror::Error)]
pub enum DownloadError {
    /// Reading the body failed.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// Creating, writing or renaming the file failed.
    #[error("{}", PyOsError::from_io(.error, .path.as_deref()))]
    Io { error: io::Error, path: Option<PathBuf> },
}

impl DownloadError {
    fn io(error: io::Error, path: Option<&Path>) -> Self {
        Self::Io {
            error,
            path: path.map(Path::to_path_buf),
        }
    }

    /// The Python exception class this failure is.
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Transport(error) => error.class.name(),
            Self::Io { error, path } => PyOsError::from_io(error, path.as_deref()).class,
        }
    }
}

/// A complete body in a temporary file, not yet under its final name. Dropping it
/// removes the file.
#[derive(Debug)]
pub struct Downloaded {
    path: tempfile::TempPath,
    sha256: String,
    len: u64,
}

impl Downloaded {
    /// The lowercase hex sha256 of the body.
    pub fn sha256_hex(&self) -> &str {
        &self.sha256
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn temp_path(&self) -> &Path {
        &self.path
    }

    /// `tmp_path.replace(dst)`: move the file into place, replacing what is there.
    pub async fn persist(self, dst: &Path) -> Result<(), DownloadError> {
        let dst = dst.to_path_buf();
        let path = self.path;
        tokio::task::spawn_blocking(move || {
            path.persist(&dst)
                .map_err(|error| DownloadError::io(error.error, Some(&dst)))
        })
        .await
        .map_err(|error| DownloadError::io(io::Error::other(error), None))?
    }
}

/// Stream `response`'s body into a new temporary file in `dir`, hashing it on the way.
/// On any failure the temporary file is removed.
pub async fn to_temp_file(
    response: &mut Response,
    dir: &Path,
    options: &TempOptions<'_>,
) -> Result<Downloaded, DownloadError> {
    let builder_dir = dir.to_path_buf();
    let prefix = options.prefix.to_owned();
    let suffix = options.suffix.to_owned();
    let mode = options.mode;
    let named = tokio::task::spawn_blocking(move || {
        let mut builder = tempfile::Builder::new();
        builder.prefix(&prefix).suffix(&suffix).rand_bytes(8);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            builder.permissions(std::fs::Permissions::from_mode(mode));
        }
        builder
            .tempfile_in(&builder_dir)
            .map_err(|error| DownloadError::io(error, Some(&builder_dir)))
    })
    .await
    .map_err(|error| DownloadError::io(io::Error::other(error), None))??;
    let (file, path) = named.into_parts();
    let mut file = tokio::fs::File::from_std(file);
    let mut digest = Sha256::new();
    let mut len = 0_u64;
    while let Some(chunk) = response.chunk().await? {
        file.write_all(&chunk)
            .await
            .map_err(|error| DownloadError::io(error, Some(&path)))?;
        digest.update(&chunk);
        len += chunk.len() as u64;
    }
    file.flush()
        .await
        .map_err(|error| DownloadError::io(error, Some(&path)))?;
    drop(file);
    Ok(Downloaded {
        path,
        sha256: hex(&digest.finalize()),
        len,
    })
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// An `OSError` as Python would raise it: the subclass for the errno, and
/// `[Errno N] strerror: 'path'` as its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PyOsError {
    pub class: &'static str,
    pub message: String,
}

impl PyOsError {
    pub fn from_io(error: &io::Error, path: Option<&Path>) -> Self {
        // The class follows the kind even without an errno: `tempfile` wraps the OS error in
        // its own (`io::Error::new(kind, PathError)`), where Python still raises the subclass.
        let class = match error.kind() {
            io::ErrorKind::NotFound => "FileNotFoundError",
            io::ErrorKind::AlreadyExists => "FileExistsError",
            io::ErrorKind::PermissionDenied => "PermissionError",
            io::ErrorKind::IsADirectory => "IsADirectoryError",
            io::ErrorKind::NotADirectory => "NotADirectoryError",
            _ => "OSError",
        };
        let Some(code) = error.raw_os_error() else {
            return Self {
                class,
                message: error.to_string(),
            };
        };
        let text = io::Error::from_raw_os_error(code).to_string();
        let strerror = text
            .strip_suffix(&format!(" (os error {code})"))
            .unwrap_or(&text)
            .to_owned();
        let message = match path {
            Some(path) => format!("[Errno {code}] {strerror}: {}", py_repr_path(path)),
            None => format!("[Errno {code}] {strerror}"),
        };
        Self { class, message }
    }
}

impl std::fmt::Display for PyOsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// The `OSError` subclass Python raises for an I/O error.
pub fn io_class_name(error: &io::Error) -> &'static str {
    PyOsError::from_io(error, None).class
}

/// `str(OSError)` without the file name: `[Errno 2] No such file or directory`.
pub fn py_os_error_text(error: &io::Error) -> String {
    PyOsError::from_io(error, None).message
}

/// `shutil.move(src, dst)`: a rename, or copy (with times) and delete across file systems.
pub fn move_file(src: &Path, dst: &Path) -> io::Result<()> {
    match std::fs::rename(src, dst) {
        Ok(()) => Ok(()),
        Err(error) if error.raw_os_error() == Some(18) => {
            // EXDEV: another file system. `copy2` keeps the times; the copy goes to a
            // `.part` first so a crash never leaves a truncated file at `dst` (fav's D52).
            let mut part = dst.as_os_str().to_owned();
            part.push(".part");
            let part = std::path::PathBuf::from(part);
            std::fs::copy(src, &part)?;
            let metadata = std::fs::metadata(src)?;
            let times = std::fs::FileTimes::new()
                .set_accessed(metadata.accessed()?)
                .set_modified(metadata.modified()?);
            std::fs::File::options().write(true).open(&part)?.set_times(times)?;
            std::fs::rename(&part, dst)?;
            std::fs::remove_file(src)
        }
        Err(error) => Err(error),
    }
}

/// `repr(str(path))` for the common case of a printable path.
fn py_repr_path(path: &Path) -> String {
    let text = path.to_string_lossy();
    if text.contains('\'') && !text.contains('"') {
        format!("\"{text}\"")
    } else {
        format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::path::Path;

    use super::PyOsError;

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
}
