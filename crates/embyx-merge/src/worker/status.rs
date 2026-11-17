//! The worker's status file: the app polls it, and it is replaced atomically so a reader never
//! sees half a write.
//!
//! Python origin: `StatusFile` and `read_status` of `src/embyx_manager/merge/worker.py`. The
//! bytes are Python's: `json.dumps(fields, ensure_ascii=False)` with the fields in the order
//! they were first set and `updated_at` (`datetime.now(UTC).isoformat()`) last.

use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, FixedOffset, Utc};
use embyx_core::pyjson::{self, Dict, DumpOptions, Value};

use super::WorkerError;
use crate::py;

/// `STATUS_INTERVAL_SECONDS`: an update that is not forced is written at most this often.
pub const STATUS_INTERVAL_SECONDS: f64 = 5.0;

/// A monotonic clock in seconds (`time.monotonic`).
pub type Clock = Box<dyn FnMut() -> f64 + Send>;
/// The wall clock for `updated_at` (`datetime.now(UTC)`).
pub type Now = Box<dyn FnMut() -> DateTime<FixedOffset> + Send>;
/// Called with every text written, for tests and the cross-check.
pub type Observer = Box<dyn FnMut(&str) + Send>;

/// `StatusFile`.
pub struct StatusFile {
    path: PathBuf,
    clock: Clock,
    now: Now,
    last_write: f64,
    fields: Dict,
    observer: Option<Observer>,
}

impl std::fmt::Debug for StatusFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatusFile")
            .field("path", &self.path)
            .field("last_write", &self.last_write)
            .field("fields", &self.fields)
            .finish_non_exhaustive()
    }
}

/// The real monotonic clock, in seconds since the first call.
pub fn monotonic() -> Clock {
    let start = std::time::Instant::now();
    Box::new(move || start.elapsed().as_secs_f64())
}

/// The real wall clock in UTC.
pub fn utc_now() -> Now {
    Box::new(|| Utc::now().fixed_offset())
}

impl StatusFile {
    /// A status file at `path` on the real clocks; nothing is written until the first update.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self::with_clocks(path, monotonic(), utc_now())
    }

    pub fn with_clocks(path: impl Into<PathBuf>, clock: Clock, now: Now) -> Self {
        let mut fields = Dict::new();
        fields.insert("state", Value::Str("running".to_owned()));
        Self {
            path: path.into(),
            clock,
            now,
            last_write: f64::NEG_INFINITY,
            fields,
            observer: None,
        }
    }

    /// Hand every written text to `observer` as well.
    #[must_use]
    pub fn observed(mut self, observer: Observer) -> Self {
        self.observer = Some(observer);
        self
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The fields as they stand, written or not.
    pub fn fields(&self) -> &Dict {
        &self.fields
    }

    /// `update(force=True, **fields)`: a forced update is always written.
    pub fn update<K: AsRef<str>>(&mut self, fields: impl IntoIterator<Item = (K, Value)>) -> Result<(), WorkerError> {
        self.write(true, fields)
    }

    /// `update(force=False, **fields)`: written only once the interval has passed.
    pub fn update_lazily<K: AsRef<str>>(
        &mut self,
        fields: impl IntoIterator<Item = (K, Value)>,
    ) -> Result<(), WorkerError> {
        self.write(false, fields)
    }

    fn write<K: AsRef<str>>(
        &mut self,
        force: bool,
        fields: impl IntoIterator<Item = (K, Value)>,
    ) -> Result<(), WorkerError> {
        for (name, value) in fields {
            self.fields.insert(name.as_ref(), value);
        }
        let now = (self.clock)();
        if !force && now - self.last_write < STATUS_INTERVAL_SECONDS {
            return Ok(());
        }
        self.last_write = now;
        let parent = parent(&self.path);
        mkdir_parents(&parent)?;
        let mut payload = self.fields.clone();
        payload.insert("updated_at", Value::Str(pyjson::py_isoformat_datetime(&(self.now)())));
        let text = pyjson::dumps(&Value::Dict(payload), &DumpOptions::plain())
            .map_err(|error| WorkerError::Uncaught(py::PyException::new("TypeError", error.to_string())))?;
        let name = self
            .path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        let temporary = self.path.with_file_name(format!(".{name}.tmp"));
        fs::write(&temporary, text.as_bytes()).map_err(|error| WorkerError::os(&error, &temporary))?;
        fs::rename(&temporary, &self.path).map_err(|error| WorkerError::os2(&error, &temporary, &self.path))?;
        if let Some(observer) = &mut self.observer {
            observer(&text);
        }
        Ok(())
    }
}

/// `Path.parent`, where the parent of a bare name is `.`.
pub(crate) fn parent(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// `Path.mkdir(parents=True, exist_ok=True)` as pathlib does it, so a failure names the
/// directory Python's would.
pub(crate) fn mkdir_parents(path: &Path) -> Result<(), WorkerError> {
    match fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let up = parent(path);
            if up == path || path.parent().is_none() {
                return Err(WorkerError::os(&error, path));
            }
            mkdir_parents(&up)?;
            match fs::create_dir(path) {
                Ok(()) => Ok(()),
                Err(_) if path.is_dir() => Ok(()),
                Err(error) => Err(WorkerError::os(&error, path)),
            }
        }
        Err(_) if path.is_dir() => Ok(()),
        Err(error) => Err(WorkerError::os(&error, path)),
    }
}

/// `read_status`: the last status a worker wrote, or `None` when there is none or it cannot
/// be read.
pub fn read_status(path: &Path) -> Option<Dict> {
    crate::transcode::read_json(path)
}
