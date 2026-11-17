//! Running ffprobe and ffmpeg: the calls the worker makes, behind a trait so the goldens can
//! replay Python's runs with canned answers, and the real implementation on
//! `std::process`.
//!
//! Python origin: the `subprocess.run(..., capture_output=True)` and
//! `subprocess.Popen(..., stdout=PIPE, stderr=PIPE, text=True, errors='replace')` calls of
//! `src/embyx_manager/merge/worker.py`.

use std::io::{self, BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, PoisonError};

/// What `subprocess.run(argv, capture_output=True)` returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Captured {
    /// Python's `returncode`: the exit status, or minus the signal that ended the process.
    pub returncode: i64,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// A started process: its two output pipes and a handle to stop or reap it.
pub struct Spawned {
    pub stdout: Box<dyn Read + Send>,
    pub stderr: Box<dyn Read + Send>,
    pub process: Arc<dyn Process>,
}

impl std::fmt::Debug for Spawned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spawned").finish_non_exhaustive()
    }
}

/// A running process another thread may kill while this one waits for it.
pub trait Process: Send + Sync {
    /// `Popen.kill()`; a process that already ended is left alone.
    fn kill(&self);
    /// `Popen.poll()`: the return code once it has ended.
    fn try_wait(&self) -> io::Result<Option<i64>>;
}

/// How the worker starts ffprobe and ffmpeg.
pub trait Exec: Send + Sync {
    /// `subprocess.run(argv, capture_output=True, check=False)`.
    fn run(&self, argv: &[String]) -> io::Result<Captured>;
    /// `subprocess.Popen(argv, stdout=PIPE, stderr=PIPE)`.
    fn spawn(&self, argv: &[String]) -> io::Result<Spawned>;
}

/// The real thing: processes started from `PATH` (or the path given), stdin inherited as
/// Python's `subprocess` leaves it.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemExec;

fn command(argv: &[String]) -> io::Result<Command> {
    let (program, rest) = argv
        .split_first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty command line"))?;
    let mut command = Command::new(program);
    command.args(rest).stdin(Stdio::inherit());
    Ok(command)
}

/// Python's `returncode` for an exit status.
fn returncode(status: std::process::ExitStatus) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        if let Some(signal) = status.signal() {
            return -i64::from(signal);
        }
    }
    status.code().map_or(-1, i64::from)
}

impl Exec for SystemExec {
    fn run(&self, argv: &[String]) -> io::Result<Captured> {
        let output = command(argv)?.output()?;
        Ok(Captured {
            returncode: returncode(output.status),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }

    fn spawn(&self, argv: &[String]) -> io::Result<Spawned> {
        let mut child = command(argv)?.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()?;
        let stdout = child.stdout.take().ok_or_else(|| io::Error::other("no stdout pipe"))?;
        let stderr = child.stderr.take().ok_or_else(|| io::Error::other("no stderr pipe"))?;
        Ok(Spawned {
            stdout: Box::new(stdout),
            stderr: Box::new(stderr),
            process: Arc::new(SharedChild(Mutex::new(child))),
        })
    }
}

/// A child process behind a lock that is held only for an instant, so a kill from the stall
/// watch never waits behind a blocking wait.
struct SharedChild(Mutex<Child>);

impl Process for SharedChild {
    fn kill(&self) {
        let mut child = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if matches!(child.try_wait(), Ok(None)) {
            let _ = child.kill();
        }
    }

    fn try_wait(&self) -> io::Result<Option<i64>> {
        let mut child = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(child.try_wait()?.map(returncode))
    }
}

/// Lines of a text pipe as Python's universal-newline reading gives them: a line ends at
/// `\n`, `\r\n` or a lone `\r`, and bytes that are not UTF-8 become U+FFFD
/// (`errors='replace'`). The line ending is not included.
pub(crate) struct Lines<R> {
    reader: BufReader<R>,
}

impl<R: Read> Lines<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
        }
    }

    pub(crate) fn next_line(&mut self) -> io::Result<Option<String>> {
        let mut line = Vec::new();
        loop {
            let buffer = self.reader.fill_buf()?;
            if buffer.is_empty() {
                return Ok((!line.is_empty()).then(|| String::from_utf8_lossy(&line).into_owned()));
            }
            if let Some(end) = buffer.iter().position(|&byte| byte == b'\n' || byte == b'\r') {
                let carriage = buffer[end] == b'\r';
                line.extend_from_slice(&buffer[..end]);
                self.reader.consume(end + 1);
                if carriage && self.reader.fill_buf()?.first() == Some(&b'\n') {
                    self.reader.consume(1);
                }
                return Ok(Some(String::from_utf8_lossy(&line).into_owned()));
            }
            let length = buffer.len();
            line.extend_from_slice(buffer);
            self.reader.consume(length);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Lines;

    #[test]
    fn lines_end_where_python_ends_them() {
        let mut lines = Lines::new(&b"a\r\nb\rc\nd\xffe\n\nlast"[..]);
        let mut seen = Vec::new();
        while let Some(line) = lines.next_line().expect("reads") {
            seen.push(line);
        }
        assert_eq!(seen, ["a", "b", "c", "d\u{fffd}e", "", "last"]);
    }
}
