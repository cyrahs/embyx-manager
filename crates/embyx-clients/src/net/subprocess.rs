//! Running external programs with both pipes captured or drained line by line.
//!
//! Python origin (in fav, where this was written): `subprocess.run(command, text=True,
//! capture_output=True, check=False)` and `asyncio.create_subprocess_exec(*command,
//! stdout=PIPE, stderr=PIPE)` with both pipes drained line by line. Callers take a
//! [`Runner`] so tests replace the programs with scripted outcomes; [`SystemRunner`] is the
//! real one. Whether embyx-manager's ffmpeg and ffprobe calls (`merge/`) use it is WP13's
//! and WP15's call.
//!
//! Differences from Python, all at the edges (DIVERGENCES.md D3, fav's D61): output that is not
//! UTF-8 is decoded with replacement characters where `text=True` raises
//! `UnicodeDecodeError`; a line longer than asyncio's 64 KiB stream limit is read whole
//! where asyncio raises `ValueError`; standard input is `/dev/null` instead of the
//! worker's; and a child is killed when its run is dropped (the worker stopping), where
//! Python leaves it running.
//!
//! Copied from fav (`fav-net/src/subprocess.rs`); see docs/CONVENTIONS.md, "Code copied from fav". `D` numbers
//! marked fav's are fav's own DIVERGENCES.md entries.

use std::fmt;
use std::future::Future;
use std::io;
use std::os::unix::process::ExitStatusExt as _;
use std::pin::Pin;
use std::process::{ExitStatus, Stdio};

use tokio::io::{AsyncBufReadExt as _, AsyncRead, BufReader};
use tokio::process::Command;

/// A boxed future, as the runner trait returns them.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What `subprocess.run(..., text=True, capture_output=True)` returns.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Completed {
    /// `returncode`: the exit code, or minus the signal that ended the process.
    pub returncode: i32,
    pub stdout: String,
    pub stderr: String,
}

/// Which pipe a line came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
}

/// Where [`Runner::run_lines`] delivers each line, in the order the lines were read.
pub type LineSink<'a> = &'a mut (dyn FnMut(Stream, &str) + Send);

/// Starts external programs. `argv[0]` is looked up on `PATH`, as `subprocess` does.
pub trait Runner: Send + Sync + fmt::Debug {
    /// Run to completion and capture both pipes as text (universal newlines).
    fn run(&self, argv: Vec<String>) -> BoxFuture<'_, io::Result<Completed>>;

    /// Run to completion, handing every line of either pipe to `sink` as it arrives, with
    /// the line ending (and any trailing whitespace) removed. Returns the exit code.
    fn run_lines<'a>(&'a self, argv: Vec<String>, sink: LineSink<'a>) -> BoxFuture<'a, io::Result<i32>>;
}

/// The runner that starts real processes.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemRunner;

fn command(argv: &[String]) -> io::Result<Command> {
    let (program, arguments) = argv
        .split_first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty command"))?;
    let mut command = Command::new(program);
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    Ok(command)
}

/// `Popen.returncode`: the exit code, or `-signal` for a process a signal ended.
pub fn returncode(status: ExitStatus) -> i32 {
    status
        .code()
        .or_else(|| status.signal().map(|signal| -signal))
        .unwrap_or(-1)
}

/// Bytes read with `text=True`: decoded as UTF-8 (`errors='replace'` here, see the module
/// docs) with `\r\n` and `\r` turned into `\n`.
pub fn decode_text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).replace("\r\n", "\n").replace('\r', "\n")
}

/// `raw_line.decode(errors='replace').rstrip()`.
pub fn decode_line(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .trim_end_matches(embyx_core::pytext::is_py_space)
        .to_owned()
}

impl Runner for SystemRunner {
    fn run(&self, argv: Vec<String>) -> BoxFuture<'_, io::Result<Completed>> {
        Box::pin(async move {
            let output = command(&argv)?.output().await?;
            Ok(Completed {
                returncode: returncode(output.status),
                stdout: decode_text(&output.stdout),
                stderr: decode_text(&output.stderr),
            })
        })
    }

    fn run_lines<'a>(&'a self, argv: Vec<String>, sink: LineSink<'a>) -> BoxFuture<'a, io::Result<i32>> {
        Box::pin(async move {
            let mut child = command(&argv)?.spawn()?;
            let stdout = child
                .stdout
                .take()
                .ok_or_else(|| io::Error::other("stdout is not a pipe"))?;
            let stderr = child
                .stderr
                .take()
                .ok_or_else(|| io::Error::other("stderr is not a pipe"))?;
            drain(stdout, stderr, sink).await?;
            Ok(returncode(child.wait().await?))
        })
    }
}

/// Read both pipes to their end, line by line, as `asyncio.gather(drain_stdout(),
/// drain_stderr())` does.
async fn drain(stdout: impl AsyncRead + Unpin, stderr: impl AsyncRead + Unpin, sink: LineSink<'_>) -> io::Result<()> {
    let mut out = BufReader::new(stdout);
    let mut err = BufReader::new(stderr);
    let (mut out_line, mut err_line) = (Vec::new(), Vec::new());
    let (mut out_open, mut err_open) = (true, true);
    while out_open || err_open {
        tokio::select! {
            read = out.read_until(b'\n', &mut out_line), if out_open => {
                // `read_until` keeps a partial line in the buffer when the other branch wins the
                // race, so a last line without a newline can be there when the end is reached.
                let ended = read? == 0;
                if !out_line.is_empty() {
                    sink(Stream::Stdout, &decode_line(&out_line));
                    out_line.clear();
                }
                out_open = !ended;
            }
            read = err.read_until(b'\n', &mut err_line), if err_open => {
                let ended = read? == 0;
                if !err_line.is_empty() {
                    sink(Stream::Stderr, &decode_line(&err_line));
                    err_line.clear();
                }
                err_open = !ended;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncRead, ReadBuf};

    use super::{Runner, Stream, SystemRunner, decode_line, decode_text, drain};

    /// The order of a race between the pipes, scripted: stdout gives `tail` without a newline
    /// and then waits; only then does stderr give a whole line, which wins the race and drops
    /// stdout's unfinished read; stdout ends after that.
    #[derive(Default)]
    struct Race {
        tail_given: bool,
        line_given: bool,
        stderr_waker: Option<std::task::Waker>,
    }

    struct Pipe {
        race: std::sync::Arc<std::sync::Mutex<Race>>,
        stderr: bool,
        done: bool,
    }

    impl AsyncRead for Pipe {
        fn poll_read(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let race = std::sync::Arc::clone(&self.race);
            let mut race = race.lock().expect("race");
            if self.done {
                return Poll::Ready(Ok(()));
            }
            if self.stderr {
                if !race.tail_given {
                    race.stderr_waker = Some(context.waker().clone());
                    return Poll::Pending;
                }
                buf.put_slice(b"line\n");
                race.line_given = true;
                self.done = true;
            } else if !race.tail_given {
                buf.put_slice(b"tail");
                race.tail_given = true;
                if let Some(waker) = race.stderr_waker.take() {
                    waker.wake();
                }
            } else if !race.line_given {
                return Poll::Pending;
            } else {
                self.done = true;
            }
            Poll::Ready(Ok(()))
        }
    }

    fn sh(script: &str) -> Vec<String> {
        vec!["sh".to_owned(), "-c".to_owned(), script.to_owned()]
    }

    #[test]
    fn text_mode_translates_newlines() {
        assert_eq!(decode_text(b"a\r\nb\rc\n"), "a\nb\nc\n");
        assert_eq!(decode_text(b"bad \xff byte"), "bad \u{fffd} byte");
    }

    #[test]
    fn lines_lose_trailing_whitespace_only() {
        assert_eq!(decode_line(b"  [info] x \t\r\n"), "  [info] x");
        assert_eq!(decode_line(b"\n"), "");
    }

    #[tokio::test]
    async fn run_captures_both_pipes_and_the_exit_code() {
        let completed = SystemRunner
            .run(sh("printf 'out\\r\\n'; printf 'err' >&2; exit 3"))
            .await
            .expect("sh runs");
        assert_eq!(completed.returncode, 3);
        assert_eq!(completed.stdout, "out\n");
        assert_eq!(completed.stderr, "err");
    }

    #[tokio::test]
    async fn a_signal_is_a_negative_returncode() {
        let completed = SystemRunner.run(sh("kill -TERM $$")).await.expect("sh runs");
        assert_eq!(completed.returncode, -15);
    }

    #[tokio::test]
    async fn a_missing_program_is_not_found() {
        let error = SystemRunner
            .run(vec!["embyx-no-such-program".to_owned()])
            .await
            .expect_err("nothing to run");
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn a_last_line_cut_off_by_the_other_pipe_is_kept() {
        let mut lines = Vec::new();
        let mut sink = |stream: Stream, line: &str| lines.push((stream, line.to_owned()));
        let race = std::sync::Arc::default();
        let stdout = Pipe {
            race: std::sync::Arc::clone(&race),
            stderr: false,
            done: false,
        };
        let stderr = Pipe {
            race,
            stderr: true,
            done: false,
        };
        drain(stdout, stderr, &mut sink).await.expect("drained");
        lines.sort_unstable_by_key(|(stream, _)| *stream == Stream::Stdout);
        assert_eq!(
            lines,
            [(Stream::Stderr, "line".to_owned()), (Stream::Stdout, "tail".to_owned())]
        );
    }

    #[tokio::test]
    async fn run_lines_delivers_each_line_of_either_pipe() {
        let mut lines = Vec::new();
        let mut sink = |stream: Stream, line: &str| lines.push((stream, line.to_owned()));
        let code = SystemRunner
            .run_lines(sh("echo one; echo two >&2; printf 'three'; exit 4"), &mut sink)
            .await
            .expect("sh runs");
        assert_eq!(code, 4);
        let mut stdout: Vec<_> = lines
            .iter()
            .filter(|(s, _)| *s == Stream::Stdout)
            .map(|(_, l)| l.as_str())
            .collect();
        let stderr: Vec<_> = lines
            .iter()
            .filter(|(s, _)| *s == Stream::Stderr)
            .map(|(_, l)| l.as_str())
            .collect();
        stdout.sort_unstable();
        assert_eq!(stdout, ["one", "three"]);
        assert_eq!(stderr, ["two"]);
    }
}
