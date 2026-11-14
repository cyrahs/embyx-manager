//! The file-system fake of the fill-actor tests and scenarios: `RecordingFs` in
//! `tools/scenarios/operations/fill_actor_service.py`, rule for rule.
//!
//! It wraps a real file system and
//!
//! - records each mutation (`rename_noreplace`, `link`, `rename`, `unlink`, `mkdir`) as an `fs`
//!   call record `{fake, method, request, response | error}`;
//! - fails a call the spec names with the `OSError` it asks for, before the call runs, so a
//!   faulted call changes nothing (`stat`, `iterdir` and `read` can be faulted too and are not
//!   recorded);
//! - crashes in front of a named mutation: the call panics with [`CRASH`] before it runs, which
//!   ends the service's work there the way a process that dies ends it (the scenarios' crash
//!   points between two steps of a move);
//! - runs a test's hooks before and after each mutation (a test standing in for Python's
//!   monkeypatched `_rename_no_replace`, `os.link` or `os.rename`).

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use embyx_core::pyjson::{Dict, Value};
use embyx_monitor::fs::path_text;
use embyx_monitor::reports::PyException as _;

use super::{LocalMoveFs, MoveFs, PyOsError, Stat};

/// The panic message of a crash point.
pub const CRASH: &str = "scenario crash";

/// Where call records go (`ctx.record` on the Python side).
pub type Sink = Arc<dyn Fn(Value) + Send + Sync>;

/// A hook around one mutation: `(op, path, path2)`.
pub type Hook = Arc<dyn Fn(&str, &Path, Option<&Path>) + Send + Sync>;

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
    /// `None`: any path.
    pub path: Option<PathBuf>,
    pub error: FaultError,
    pub times: Option<u32>,
}

impl Fault {
    pub fn errno(op: &str, path: Option<PathBuf>, code: i32) -> Self {
        Self {
            op: op.to_owned(),
            path,
            error: FaultError::Errno(code),
            times: None,
        }
    }

    pub fn message(op: &str, path: Option<PathBuf>, message: &str) -> Self {
        Self {
            op: op.to_owned(),
            path,
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

/// `args.crash` for the file system: panic in front of the `nth` (from 1) call of `op` on
/// `path` (`None`: any path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrashPoint {
    pub op: String,
    pub path: Option<PathBuf>,
    pub nth: u32,
}

#[derive(Default)]
struct State {
    faults: Vec<Fault>,
    crash: Option<CrashPoint>,
    before: Option<Hook>,
    after: Option<Hook>,
    paused: bool,
}

/// The recording, failing, crashing file system.
pub struct RecordingMoveFs {
    inner: Arc<dyn MoveFs>,
    sink: Option<Sink>,
    state: Mutex<State>,
}

impl std::fmt::Debug for RecordingMoveFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordingMoveFs")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl Default for RecordingMoveFs {
    fn default() -> Self {
        Self::new(None)
    }
}

impl RecordingMoveFs {
    /// Over the real file system; records go to `sink` (none: not recorded).
    pub fn new(sink: Option<Sink>) -> Self {
        Self {
            inner: Arc::new(LocalMoveFs),
            sink,
            state: Mutex::new(State::default()),
        }
    }

    /// A fake that keeps its records in a list.
    pub fn collecting() -> (Self, Arc<Mutex<Vec<Value>>>) {
        let records = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::clone(&records);
        let sink: Sink = Arc::new(move |record| writer.lock().unwrap_or_else(PoisonError::into_inner).push(record));
        (Self::new(Some(sink)), records)
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn add_fault(&self, fault: Fault) {
        self.state().faults.push(fault);
    }

    pub fn clear_faults(&self) {
        self.state().faults.clear();
    }

    /// Crash in front of a mutation (replacing the one set before); `None` disarms it.
    pub fn set_crash(&self, crash: Option<CrashPoint>) {
        self.state().crash = crash;
    }

    /// Whether the crash point is still armed.
    pub fn crash_armed(&self) -> bool {
        self.state().crash.is_some()
    }

    pub fn set_before(&self, hook: Option<Hook>) {
        self.state().before = hook;
    }

    pub fn set_after(&self, hook: Option<Hook>) {
        self.state().after = hook;
    }

    /// While paused the fake neither records, faults nor crashes (steps that shape the tree).
    pub fn set_paused(&self, paused: bool) {
        self.state().paused = paused;
    }

    fn fault(&self, op: &str, path: &Path, path2: Option<&Path>) -> Option<PyOsError> {
        let mut state = self.state();
        if state.paused {
            return None;
        }
        let wanted = path_text(path);
        let fault = state.faults.iter_mut().find(|fault| {
            fault.op == op && fault.path.as_ref().is_none_or(|path| path_text(path) == wanted) && fault.times != Some(0)
        })?;
        if let Some(times) = fault.times.as_mut() {
            *times -= 1;
        }
        Some(match &fault.error {
            FaultError::Message(message) => PyOsError::message(message.clone()),
            FaultError::Errno(code) => PyOsError::errno(*code, Some(path), path2),
        })
    }

    fn crash_here(&self, op: &str, path: &Path) -> bool {
        let mut state = self.state();
        if state.paused {
            return false;
        }
        let wanted = path_text(path);
        let Some(crash) = state.crash.as_mut() else {
            return false;
        };
        if crash.op != op || crash.path.as_ref().is_some_and(|path| path_text(path) != wanted) {
            return false;
        }
        if crash.nth > 1 {
            crash.nth -= 1;
            return false;
        }
        state.crash = None;
        true
    }

    fn mutation(
        &self,
        op: &str,
        request: Dict,
        path: &Path,
        path2: Option<&Path>,
        call: impl FnOnce() -> Result<(), PyOsError>,
    ) -> Result<(), PyOsError> {
        if self.crash_here(op, path) {
            std::panic::panic_any(CRASH);
        }
        let (before, after, paused) = {
            let state = self.state();
            (state.before.clone(), state.after.clone(), state.paused)
        };
        if let Some(hook) = before.as_ref().filter(|_| !paused) {
            hook(op, path, path2);
        }
        let outcome = match self.fault(op, path, path2) {
            Some(error) => Err(error),
            None => call(),
        };
        if !paused && let Some(sink) = &self.sink {
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
            sink(Value::Dict(record));
        }
        if let Some(hook) = after.as_ref().filter(|_| !paused) {
            hook(op, path, path2);
        }
        outcome
    }
}

fn one(path: &Path) -> Dict {
    let mut request = Dict::new();
    request.insert("path", Value::Str(path_text(path)));
    request
}

fn two(source: &Path, destination: &Path) -> Dict {
    let mut request = Dict::new();
    request.insert("src", Value::Str(path_text(source)));
    request.insert("dst", Value::Str(path_text(destination)));
    request
}

impl MoveFs for RecordingMoveFs {
    fn stat(&self, path: &Path, follow_symlinks: bool) -> Result<Stat, PyOsError> {
        if let Some(error) = self.fault("stat", path, None) {
            return Err(error);
        }
        self.inner.stat(path, follow_symlinks)
    }

    fn readlink(&self, path: &Path) -> Result<OsString, PyOsError> {
        self.inner.readlink(path)
    }

    fn iterdir(&self, path: &Path) -> Result<Vec<PathBuf>, PyOsError> {
        if let Some(error) = self.fault("iterdir", path, None) {
            return Err(error);
        }
        self.inner.iterdir(path)
    }

    fn mkdir(&self, path: &Path, mode: u32) -> Result<(), PyOsError> {
        self.mutation("mkdir", one(path), path, None, || self.inner.mkdir(path, mode))
    }

    fn rename_noreplace(&self, source: &Path, destination: &Path) -> Result<(), PyOsError> {
        self.mutation(
            "rename_noreplace",
            two(source, destination),
            source,
            Some(destination),
            || self.inner.rename_noreplace(source, destination),
        )
    }

    fn link(&self, source: &Path, destination: &Path) -> Result<(), PyOsError> {
        self.mutation("link", two(source, destination), source, Some(destination), || {
            self.inner.link(source, destination)
        })
    }

    fn rename(&self, source: &Path, destination: &Path) -> Result<(), PyOsError> {
        self.mutation("rename", two(source, destination), source, Some(destination), || {
            self.inner.rename(source, destination)
        })
    }

    fn unlink(&self, path: &Path) -> Result<(), PyOsError> {
        self.mutation("unlink", one(path), path, None, || self.inner.unlink(path))
    }

    fn read_limited(&self, path: &Path, limit: usize) -> Result<Vec<u8>, PyOsError> {
        if let Some(error) = self.fault("read", path, None) {
            return Err(error);
        }
        self.inner.read_limited(path, limit)
    }

    fn getcwd(&self) -> Result<PathBuf, PyOsError> {
        self.inner.getcwd()
    }
}
