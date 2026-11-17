//! The terminal: events on lines of their own, the status on one line that keeps being
//! rewritten, and Ctrl+C counted instead of ending the worker.
//!
//! Python origin: `Console` and `StopSignal` of `src/embyx_manager/merge/home_worker.py`.
//! The console's words stay Chinese, as Python's (PLAN.md, code rules).

use std::fmt::Write as _;
use std::io::{IsTerminal as _, Write as _};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// Where the worker reports: `log(message)` and `log(line, status=True)`.
pub trait Log: Send + Sync {
    /// An event, on a line of its own with the time in front.
    fn event(&self, message: &str);
    /// The status line.
    fn status(&self, line: &str);
}

/// `Console`: standard output, rewriting the status line in place on a terminal.
#[derive(Debug)]
pub struct Console {
    tty: bool,
    status: Mutex<String>,
}

impl Default for Console {
    fn default() -> Self {
        Self::new()
    }
}

impl Console {
    pub fn new() -> Self {
        Self {
            tty: std::io::stdout().is_terminal(),
            status: Mutex::new(String::new()),
        }
    }

    /// The first `columns - 1` characters (`message[: columns - 1]`).
    fn fit(message: &str) -> String {
        let columns = terminal_columns();
        message.chars().take(columns.saturating_sub(1)).collect()
    }
}

/// `shutil.get_terminal_size().columns`: `COLUMNS` when set to a positive number, else the
/// terminal's width, else 80.
fn terminal_columns() -> usize {
    if let Some(columns) = std::env::var("COLUMNS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|columns| *columns > 0)
    {
        return columns;
    }
    terminal_size::terminal_size_of(std::io::stdout()).map_or(80, |(terminal_size::Width(width), _)| usize::from(width))
}

impl Log for Console {
    fn event(&self, message: &str) {
        let status = self.status.lock().unwrap_or_else(PoisonError::into_inner);
        let stamp = chrono::Local::now().format("%H:%M:%S");
        let mut text = String::new();
        if self.tty {
            text.push_str("\r\x1b[K");
        }
        let _ = writeln!(text, "{stamp} {message}");
        if self.tty && !status.is_empty() {
            text.push_str(&Self::fit(&status));
        }
        let mut out = std::io::stdout().lock();
        let _ = out.write_all(text.as_bytes());
        let _ = out.flush();
    }

    fn status(&self, line: &str) {
        let mut status = self.status.lock().unwrap_or_else(PoisonError::into_inner);
        line.clone_into(&mut status);
        let text = if self.tty {
            format!("\r\x1b[K{}", Self::fit(line))
        } else {
            format!("{line}\n")
        };
        let mut out = std::io::stdout().lock();
        let _ = out.write_all(text.as_bytes());
        let _ = out.flush();
    }
}

/// `StopSignal`: counts Ctrl+C presses (and Ctrl+Break on Windows) instead of letting the
/// first one end the worker.
#[derive(Clone)]
pub struct StopSignal {
    presses: Arc<AtomicUsize>,
    log: Arc<dyn Log>,
}

impl std::fmt::Debug for StopSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StopSignal")
            .field("presses", &self.presses())
            .finish_non_exhaustive()
    }
}

impl StopSignal {
    pub fn new(log: Arc<dyn Log>) -> Self {
        Self {
            presses: Arc::new(AtomicUsize::new(0)),
            log,
        }
    }

    /// `install()`: the console's interrupt now counts here.
    pub fn install(&self) -> Result<(), ctrlc::Error> {
        let this = self.clone();
        ctrlc::set_handler(move || this.press())
    }

    /// `_handle`: one press.
    pub fn press(&self) {
        let presses = self.presses.fetch_add(1, Ordering::SeqCst) + 1;
        if presses == 1 {
            self.log
                .event("收到 Ctrl+C：写完当前小块、传完队列后退出（再按一次立即退出）");
        } else {
            self.log
                .event("再次收到 Ctrl+C：立即退出，下次启动从集群收到的最后一块接着做");
        }
    }

    /// `stop()`: the presses so far.
    pub fn presses(&self) -> usize {
        self.presses.load(Ordering::SeqCst)
    }
}

/// A log that keeps what it is told, for tests.
#[derive(Debug, Default)]
pub struct MemoryLog {
    lines: Mutex<Vec<(bool, String)>>,
}

impl MemoryLog {
    /// The events so far, without the status lines.
    pub fn events(&self) -> Vec<String> {
        self.lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(status, _)| !status)
            .map(|(_, line)| line.clone())
            .collect()
    }

    /// Everything so far: `(is_status, line)`.
    pub fn lines(&self) -> Vec<(bool, String)> {
        self.lines.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

impl Log for MemoryLog {
    fn event(&self, message: &str) {
        self.lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((false, message.to_owned()));
    }

    fn status(&self, line: &str) {
        self.lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((true, line.to_owned()));
    }
}
