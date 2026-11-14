//! The file-system fake of the scenarios and tests: `RecordingFs` in
//! `tools/scenarios/operations/archive.py`, rule for rule.
//!
//! It wraps a real file system and records each mutation the pipelines make (`rename`,
//! `unlink`, the outermost `mkdir`, `rmtree`) as an `fs` call record; it fails a call the
//! spec names with the `OSError` the spec asks for (before the call runs, so a faulted call
//! changes nothing), and it asks the current run to stop right after a named mutation. Reads
//! (`stat`, `iterdir`, `rglob`) can be faulted but are not recorded.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use embyx_core::pyjson::{Dict, Value};

use super::{FileStat, Fs, LocalFs, PyOsError, ScanEntry, path_text};
use crate::reports::{PyException, RunContext};

/// Where call records go (`ctx.record` on the Python side).
pub type Sink = Arc<dyn Fn(Value) + Send + Sync>;

/// What a faulted call raises.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultError {
    /// `OSError(errno, os.strerror(errno), path[, None, path2])`.
    Errno(i32),
    /// `OSError(message)`.
    Message(String),
}

/// `args.faults[]`: `op` on exactly `path` fails, `times` times (`None`: always).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fault {
    pub op: String,
    pub path: PathBuf,
    pub error: FaultError,
    pub times: Option<u32>,
}

impl Fault {
    pub fn errno(op: &str, path: impl Into<PathBuf>, code: i32) -> Self {
        Self {
            op: op.to_owned(),
            path: path.into(),
            error: FaultError::Errno(code),
            times: None,
        }
    }

    pub fn message(op: &str, path: impl Into<PathBuf>, message: &str) -> Self {
        Self {
            op: op.to_owned(),
            path: path.into(),
            error: FaultError::Message(message.to_owned()),
            times: None,
        }
    }

    #[must_use]
    pub fn times(mut self, times: u32) -> Self {
        self.times = Some(times);
        self
    }
}

#[derive(Debug, Default)]
struct State {
    faults: Vec<Fault>,
    stops: Vec<(String, PathBuf)>,
    run: Option<Arc<RunContext>>,
}

/// The recording, failing file system.
pub struct RecordingFs {
    inner: Arc<dyn Fs>,
    sink: Sink,
    state: Mutex<State>,
}

impl std::fmt::Debug for RecordingFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordingFs")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl RecordingFs {
    /// Over the real file system.
    pub fn new(sink: Sink) -> Self {
        Self::over(Arc::new(LocalFs), sink)
    }

    pub fn over(inner: Arc<dyn Fs>, sink: Sink) -> Self {
        Self {
            inner,
            sink,
            state: Mutex::new(State::default()),
        }
    }

    /// A fake that keeps its records in a list ([`RecordingFs::records`] reads it back).
    pub fn collecting() -> (Self, Arc<Mutex<Vec<Value>>>) {
        let records = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::clone(&records);
        let sink: Sink = Arc::new(move |record| writer.lock().expect("records").push(record));
        (Self::new(sink), records)
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn add_fault(&self, fault: Fault) {
        self.state().faults.push(fault);
    }

    /// `args.stops[]`: ask the current run to stop right after `op` on `path`.
    pub fn add_stop(&self, op: &str, path: impl Into<PathBuf>) {
        self.state().stops.push((op.to_owned(), path.into()));
    }

    /// The run a stop is sent to (`fs.run_ctx`).
    pub fn set_run(&self, run: Option<Arc<RunContext>>) {
        self.state().run = run;
    }

    fn fault(&self, op: &str, path: &Path, path2: Option<&Path>) -> Option<PyOsError> {
        let mut state = self.state();
        let wanted = path_text(path);
        let fault = state
            .faults
            .iter_mut()
            .find(|fault| fault.op == op && path_text(&fault.path) == wanted && fault.times != Some(0))?;
        if let Some(times) = fault.times.as_mut() {
            *times -= 1;
        }
        Some(match &fault.error {
            FaultError::Message(message) => PyOsError::message(message.clone()),
            FaultError::Errno(code) => PyOsError::errno(*code, Some(path), path2),
        })
    }

    fn after(&self, op: &str, path: &Path) {
        let run = {
            let state = self.state();
            let wanted = path_text(path);
            let stops = state
                .stops
                .iter()
                .any(|(stop_op, stop_path)| stop_op == op && path_text(stop_path) == wanted);
            if stops { state.run.clone() } else { None }
        };
        if let Some(run) = run {
            run.request_stop();
        }
    }

    fn mutation(
        &self,
        op: &str,
        request: Dict,
        path: &Path,
        path2: Option<&Path>,
        call: impl FnOnce() -> Result<(), PyOsError>,
    ) -> Result<(), PyOsError> {
        let outcome = match self.fault(op, path, path2) {
            Some(error) => Err(error),
            None => call(),
        };
        let mut record = Dict::new();
        record.insert("fake", Value::from("fs"));
        record.insert("method", Value::from(op));
        record.insert("request", Value::Dict(request));
        match &outcome {
            Ok(()) => record.insert("response", Value::Null),
            Err(error) => {
                let mut detail = Dict::new();
                detail.insert("type", Value::from(error.class_name()));
                detail.insert("message", Value::Str(error.to_string()));
                record.insert("error", Value::Dict(detail));
            }
        }
        (self.sink)(Value::Dict(record));
        self.after(op, path);
        outcome
    }
}

fn path_request(path: &Path) -> Dict {
    let mut request = Dict::new();
    request.insert("path", Value::Str(path_text(path)));
    request
}

impl Fs for RecordingFs {
    fn stat(&self, path: &Path, follow_symlinks: bool) -> Result<FileStat, PyOsError> {
        if let Some(error) = self.fault("stat", path, None) {
            return Err(error);
        }
        self.inner.stat(path, follow_symlinks)
    }

    fn scandir(&self, path: &Path) -> Result<Vec<ScanEntry>, PyOsError> {
        self.inner.scandir(path)
    }

    fn mkdir(&self, path: &Path) -> Result<(), PyOsError> {
        self.inner.mkdir(path)
    }

    fn rename(&self, src: &Path, dst: &Path) -> Result<(), PyOsError> {
        let mut request = Dict::new();
        request.insert("src", Value::Str(path_text(src)));
        request.insert("dst", Value::Str(path_text(dst)));
        self.mutation("rename", request, src, Some(dst), || self.inner.rename(src, dst))
    }

    fn unlink(&self, path: &Path) -> Result<(), PyOsError> {
        self.mutation("unlink", path_request(path), path, None, || self.inner.unlink(path))
    }

    fn rmtree(&self, path: &Path) -> Result<(), PyOsError> {
        self.mutation("rmtree", path_request(path), path, None, || self.inner.rmtree(path))
    }

    fn iterdir(&self, path: &Path) -> Result<Vec<PathBuf>, PyOsError> {
        if let Some(error) = self.fault("iterdir", path, None) {
            return Err(error);
        }
        super::iterdir(self, path)
    }

    fn rglob(&self, path: &Path) -> Result<Vec<PathBuf>, PyOsError> {
        if let Some(error) = self.fault("rglob", path, None) {
            return Err(error);
        }
        super::rglob(self, path)
    }

    /// Recorded once, as the outermost `Path.mkdir`; the `os.mkdir` calls inside are not.
    fn mkdir_parents(&self, path: &Path) -> Result<(), PyOsError> {
        self.mutation("mkdir", path_request(path), path, None, || {
            super::mkdir_parents(self, path)
        })
    }
}
