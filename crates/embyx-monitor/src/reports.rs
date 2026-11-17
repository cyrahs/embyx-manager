//! Structured run reporting shared by the monitor pipelines.
//!
//! Python origin: `src/embyx_manager/monitor/reports.py`. Each pipeline run gets a
//! [`RunContext`]: a cancellation token, a counter map, and a bounded log collector whose
//! tail is persisted with the run (`pipeline_runs.log_json`) so the dashboard can show what
//! happened. The persisted lines keep Python's wording byte for byte: callers render their
//! message with Python's `%` arguments already applied, and an exception adds its `repr()`
//! through [`PyException`].

use std::collections::VecDeque;
use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use embyx_core::pyjson::{Dict, Value};
use embyx_core::pytext::py_repr;

/// `MAX_LOG_LINES`: lines kept in the tail.
pub const MAX_LOG_LINES: usize = 200;
/// `MAX_ERRORS`: error lines kept for the run's `errors_json`.
pub const MAX_ERRORS: usize = 50;

/// A Python `StrEnum`: the value, the parse that raises `ValueError` for anything else.
macro_rules! str_enum {
    ($(#[$meta:meta])* $name:ident { $($(#[$vmeta:meta])* $variant:ident = $value:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name {
            $($(#[$vmeta])* $variant),+
        }

        impl $name {
            /// Every member, in declaration order (`list(Enum)`).
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// `.value`.
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }

            /// `Enum(value)`; `None` where Python raises `ValueError`.
            pub fn parse(value: &str) -> Option<Self> {
                match value {
                    $($value => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

pub(crate) use str_enum;

str_enum! {
    /// `PipelineName`.
    PipelineName { Rss = "rss", Archive = "archive", Mapping = "mapping", Playlists = "playlists" }
}

str_enum! {
    /// `RunTrigger`.
    RunTrigger { Scheduled = "scheduled", Manual = "manual", Watchdog = "watchdog", Startup = "startup" }
}

str_enum! {
    /// `RunState`.
    RunState { Running = "running", Completed = "completed", Failed = "failed", Cancelled = "cancelled" }
}

/// `RunCancelledError`: raised inside a pipeline when its stop token is set (no arguments, so
/// its text is empty).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("")]
pub struct RunCancelledError;

impl PyException for RunCancelledError {
    fn class_name(&self) -> &'static str {
        "RunCancelledError"
    }

    fn py_repr(&self) -> String {
        "RunCancelledError()".to_owned()
    }
}

/// A Rust error standing for a Python exception: what `type(exc).__name__` and `repr(exc)`
/// would say. Run logs carry the `repr` ([`RunContext::exception`]).
pub trait PyException: fmt::Display {
    /// `type(exc).__name__`.
    fn class_name(&self) -> &str;

    /// `repr(exc)`. The default is an exception built from one message, whose `str()` is the
    /// `Display` text: `ClassName('message')`.
    fn py_repr(&self) -> String {
        format!("{}({})", self.class_name(), py_repr(&self.to_string()))
    }
}

/// Any Python exception by class and message, for errors that only exist as text (and for
/// tests, which raise `RuntimeError('...')` from their fakes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Raised {
    pub class: String,
    pub message: String,
}

impl Raised {
    pub fn new(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            class: class.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for Raised {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Raised {}

impl PyException for Raised {
    fn class_name(&self) -> &str {
        &self.class
    }
}

/// The run's counters, `dict[str, int]` in insertion order (the order `json.dumps` writes).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Counters(Vec<(String, i64)>);

impl Counters {
    pub fn new() -> Self {
        Self::default()
    }

    /// `stats[counter]`.
    pub fn get(&self, counter: &str) -> Option<i64> {
        self.0.iter().find(|(name, _)| name == counter).map(|(_, value)| *value)
    }

    /// `stats[counter] = value` (a new counter goes last, an existing one keeps its place).
    pub fn set(&mut self, counter: &str, value: i64) {
        match self.0.iter_mut().find(|(name, _)| name == counter) {
            Some(entry) => entry.1 = value,
            None => self.0.push((counter.to_owned(), value)),
        }
    }

    /// `stats[counter] = stats.get(counter, 0) + amount`.
    pub fn add(&mut self, counter: &str, amount: i64) {
        let value = self.get(counter).unwrap_or(0) + amount;
        self.set(counter, value);
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, i64)> {
        self.0.iter().map(|(name, value)| (name.as_str(), *value))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The counters as the JSON value `json.dumps(stats)` writes.
    pub fn to_value(&self) -> Value {
        let mut dict = Dict::new();
        for (name, value) in &self.0 {
            dict.insert(name.as_str(), Value::Int(i128::from(*value)));
        }
        Value::Dict(dict)
    }
}

impl<const N: usize> From<[(&str, i64); N]> for Counters {
    fn from(entries: [(&str, i64); N]) -> Self {
        let mut counters = Self::new();
        for (name, value) in entries {
            counters.set(name, value);
        }
        counters
    }
}

#[derive(Debug, Default)]
struct Collected {
    lines: VecDeque<String>,
    errors: Vec<String>,
    stats: Counters,
}

/// `RunContext`: one run's stop token, counters and log collector. Shared by reference between
/// the tasks of a run; every method takes `&self`.
#[derive(Debug)]
pub struct RunContext {
    /// The `logging.Logger` name the lines also go to (as a `logger` field of the event).
    logger: String,
    collected: Mutex<Collected>,
    stop: AtomicBool,
}

impl RunContext {
    /// `RunContext(logger=logging.getLogger(name))`.
    pub fn new(logger: impl Into<String>) -> Self {
        Self {
            logger: logger.into(),
            collected: Mutex::new(Collected::default()),
            stop: AtomicBool::new(false),
        }
    }

    fn collected(&self) -> std::sync::MutexGuard<'_, Collected> {
        self.collected.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    // -- cancellation ----------------------------------------------------

    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    pub fn stop_requested(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    pub fn check_cancelled(&self) -> Result<(), RunCancelledError> {
        if self.stop_requested() {
            Err(RunCancelledError)
        } else {
            Ok(())
        }
    }

    // -- counters ---------------------------------------------------------

    pub fn add(&self, counter: &str, amount: i64) {
        self.collected().stats.add(counter, amount);
    }

    pub fn set(&self, counter: &str, value: i64) {
        self.collected().stats.set(counter, value);
    }

    /// `ctx.stats` (a copy).
    pub fn stats(&self) -> Counters {
        self.collected().stats.clone()
    }

    // -- logging ----------------------------------------------------------
    //
    // `message` is the line with Python's `%` arguments applied (`message % args`), or the
    // bare message where Python passes no arguments.

    pub fn info(&self, message: &str) {
        tracing::info!(logger = %self.logger, "{message}");
        self.record(message.to_owned());
    }

    pub fn warning(&self, message: &str) {
        tracing::warn!(logger = %self.logger, "{message}");
        self.record(format!("WARNING: {message}"));
    }

    pub fn error(&self, message: &str) {
        tracing::error!(logger = %self.logger, "{message}");
        let rendered = self.record(format!("ERROR: {message}"));
        self.push_error(rendered);
    }

    /// `ctx.exception(message)` inside an `except` block: the line carries `: repr(exc)`.
    /// `error` is `None` where Python calls it outside one (`sys.exc_info()` is empty).
    pub fn exception(&self, message: &str, error: Option<&dyn PyException>) {
        let detail = if let Some(error) = error {
            tracing::error!(logger = %self.logger, error = %error, "{message}");
            format!(": {}", error.py_repr())
        } else {
            tracing::error!(logger = %self.logger, "{message}");
            String::new()
        };
        let rendered = self.record(format!("ERROR: {message}{detail}"));
        self.push_error(rendered);
    }

    fn record(&self, line: String) -> String {
        let mut collected = self.collected();
        if collected.lines.len() == MAX_LOG_LINES {
            collected.lines.pop_front();
        }
        collected.lines.push_back(line.clone());
        line
    }

    fn push_error(&self, rendered: String) {
        let mut collected = self.collected();
        if collected.errors.len() < MAX_ERRORS {
            collected.errors.push(rendered);
        }
    }

    pub fn log_tail(&self) -> Vec<String> {
        self.collected().lines.iter().cloned().collect()
    }

    pub fn errors(&self) -> Vec<String> {
        self.collected().errors.clone()
    }
}

#[cfg(test)]
mod tests;
