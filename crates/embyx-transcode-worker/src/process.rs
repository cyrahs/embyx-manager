//! Starting, pausing and stopping ffmpeg and ffprobe, and the machine's host name: the one
//! module of the workspace that may use `unsafe` (`tools/check_tree.sh`).
//!
//! Python origin: `_process_flags`, `suspend`, `resume` and the `subprocess.run` /
//! `subprocess.Popen` calls of `src/embyx_manager/merge/home_worker.py`, and
//! `socket.gethostname()`.
//!
//! Every child gets a process group of its own, so a Ctrl+C at the console reaches only
//! the worker, which decides what to do (`CREATE_NEW_PROCESS_GROUP` on Windows). On Unix
//! Python uses `start_new_session=True` (`setsid`); this port uses `process_group(0)`
//! (`setpgid`), which keeps the terminal's signals away just the same (D166). Pausing is
//! `SIGSTOP` / `SIGCONT` on Unix and `NtSuspendProcess` / `NtResumeProcess` on Windows, as
//! Python does through `ctypes`.

use std::ffi::OsString;
use std::io::{self, Read};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

/// What `subprocess.run(argv, capture_output=True, text=True)` returns. The text is decoded
/// as UTF-8 with replacement characters (D167).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Captured {
    /// Python's `returncode`: the exit status, or minus the signal that ended the process.
    pub returncode: i64,
    pub stdout: String,
    pub stderr: String,
}

/// Why `subprocess.run` raised.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// `subprocess.TimeoutExpired`: the child was killed.
    #[error("timed out after {0:?}")]
    Timeout(Duration),
    /// `OSError` starting it (a missing executable).
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// A started ffmpeg: its output pipes and the controls the worker uses.
pub trait Running: Send + Sync {
    /// The standard output, once (`Popen.stdout`).
    fn take_stdout(&self) -> Option<Box<dyn Read + Send>>;
    /// The standard error, once (`Popen.stderr`).
    fn take_stderr(&self) -> Option<Box<dyn Read + Send>>;
    /// `Popen.poll()`.
    fn poll(&self) -> io::Result<Option<i64>>;
    /// `Popen.kill()`; a process that already ended is left alone.
    fn kill(&self) -> io::Result<()>;
    /// `suspend(process)`.
    fn suspend(&self) -> io::Result<()>;
    /// `resume(process)`.
    fn resume(&self) -> io::Result<()>;
    /// The process id.
    fn id(&self) -> u32;
}

/// How the worker starts ffmpeg and ffprobe; tests and goldens replace it.
pub trait Runner: Send + Sync {
    /// `subprocess.run(argv, capture_output=True, text=True, check=False, timeout=...)`.
    fn run(&self, argv: &[OsString], timeout: Option<Duration>) -> Result<Captured, RunError>;
    /// `subprocess.Popen(argv, stdout=PIPE, stderr=PIPE, text=True, errors='replace')`.
    fn spawn(&self, argv: &[OsString]) -> io::Result<Arc<dyn Running>>;
}

/// The real processes.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemRunner;

/// A `Command` for `argv` in a process group of its own, stdin inherited as Python leaves it.
fn command(argv: &[OsString]) -> io::Result<Command> {
    let (program, rest) = argv
        .split_first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty command line"))?;
    let mut command = Command::new(program);
    command.args(rest).stdin(Stdio::inherit());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        command.process_group(0);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt as _;
        command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NEW_PROCESS_GROUP);
    }
    Ok(command)
}

/// Python's `returncode` for an exit status.
pub(crate) fn returncode(status: ExitStatus) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        if let Some(signal) = status.signal() {
            return -i64::from(signal);
        }
    }
    status.code().map_or(-1, i64::from)
}

fn read_all(mut pipe: impl Read + Send + 'static) -> thread::JoinHandle<Vec<u8>> {
    thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = pipe.read_to_end(&mut bytes);
        bytes
    })
}

impl Runner for SystemRunner {
    fn run(&self, argv: &[OsString], timeout: Option<Duration>) -> Result<Captured, RunError> {
        let mut child = command(argv)?.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()?;
        let stdout = read_all(child.stdout.take().ok_or_else(|| io::Error::other("no stdout pipe"))?);
        let stderr = read_all(child.stderr.take().ok_or_else(|| io::Error::other("no stderr pipe"))?);
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout.join();
                let _ = stderr.join();
                return Err(RunError::Timeout(timeout.unwrap_or_default()));
            }
            thread::sleep(Duration::from_millis(20));
        };
        let text = |handle: thread::JoinHandle<Vec<u8>>| {
            String::from_utf8_lossy(&handle.join().unwrap_or_default()).into_owned()
        };
        Ok(Captured {
            returncode: returncode(status),
            stdout: text(stdout),
            stderr: text(stderr),
        })
    }

    fn spawn(&self, argv: &[OsString]) -> io::Result<Arc<dyn Running>> {
        let mut child = command(argv)?.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()?;
        let stdout = child.stdout.take().map(|pipe| Box::new(pipe) as Box<dyn Read + Send>);
        let stderr = child.stderr.take().map(|pipe| Box::new(pipe) as Box<dyn Read + Send>);
        Ok(Arc::new(SystemChild {
            child: Mutex::new(child),
            stdout: Mutex::new(stdout),
            stderr: Mutex::new(stderr),
        }))
    }
}

/// A child behind a lock held only for an instant, so a kill from another thread never
/// waits behind a blocking wait.
struct SystemChild {
    child: Mutex<Child>,
    stdout: Mutex<Option<Box<dyn Read + Send>>>,
    stderr: Mutex<Option<Box<dyn Read + Send>>>,
}

impl Running for SystemChild {
    fn take_stdout(&self) -> Option<Box<dyn Read + Send>> {
        self.stdout.lock().unwrap_or_else(PoisonError::into_inner).take()
    }

    fn take_stderr(&self) -> Option<Box<dyn Read + Send>> {
        self.stderr.lock().unwrap_or_else(PoisonError::into_inner).take()
    }

    fn poll(&self) -> io::Result<Option<i64>> {
        let mut child = self.child.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(child.try_wait()?.map(returncode))
    }

    fn kill(&self) -> io::Result<()> {
        let mut child = self.child.lock().unwrap_or_else(PoisonError::into_inner);
        if matches!(child.try_wait(), Ok(None)) {
            child.kill()?;
        }
        Ok(())
    }

    fn suspend(&self) -> io::Result<()> {
        let child = self.child.lock().unwrap_or_else(PoisonError::into_inner);
        platform::suspend(&child)
    }

    fn resume(&self) -> io::Result<()> {
        let child = self.child.lock().unwrap_or_else(PoisonError::into_inner);
        platform::resume(&child)
    }

    fn id(&self) -> u32 {
        self.child.lock().unwrap_or_else(PoisonError::into_inner).id()
    }
}

/// `socket.gethostname()`.
pub fn hostname() -> String {
    platform::hostname()
}

#[cfg(unix)]
mod platform {
    use std::io;
    use std::process::Child;

    use nix::sys::signal::{self, Signal};
    use nix::unistd::Pid;

    fn send(child: &Child, signal: Signal) -> io::Result<()> {
        let pid = i32::try_from(child.id()).map_err(io::Error::other)?;
        signal::kill(Pid::from_raw(pid), signal).map_err(io::Error::from)
    }

    pub(super) fn suspend(child: &Child) -> io::Result<()> {
        send(child, Signal::SIGSTOP)
    }

    pub(super) fn resume(child: &Child) -> io::Result<()> {
        send(child, Signal::SIGCONT)
    }

    pub(super) fn hostname() -> String {
        nix::unistd::gethostname()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    }
}

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "ntdll's suspend/resume and the host name have no safe binding; confined to this module"
)]
mod platform {
    use std::io;
    use std::os::windows::io::AsRawHandle as _;
    use std::process::Child;

    use windows_sys::Win32::Foundation::{HANDLE, NTSTATUS};
    use windows_sys::Win32::System::SystemInformation::{ComputerNamePhysicalDnsHostname, GetComputerNameExW};

    // Undocumented ntdll exports (what Python's `ctypes.windll.ntdll` calls), declared the
    // way windows-sys declares its own imports.
    windows_link::link!("ntdll.dll" "system" fn NtSuspendProcess(process: HANDLE) -> NTSTATUS);
    windows_link::link!("ntdll.dll" "system" fn NtResumeProcess(process: HANDLE) -> NTSTATUS);

    fn status(code: NTSTATUS) -> io::Result<()> {
        if code < 0 {
            Err(io::Error::other(format!("NTSTATUS {code:#010x}")))
        } else {
            Ok(())
        }
    }

    pub(super) fn suspend(child: &Child) -> io::Result<()> {
        // SAFETY: the handle belongs to `child`, which the caller holds locked and alive.
        status(unsafe { NtSuspendProcess(child.as_raw_handle()) })
    }

    pub(super) fn resume(child: &Child) -> io::Result<()> {
        // SAFETY: as in `suspend`.
        status(unsafe { NtResumeProcess(child.as_raw_handle()) })
    }

    pub(super) fn hostname() -> String {
        let mut size: u32 = 0;
        // SAFETY: a null buffer with size 0 asks for the size needed.
        unsafe { GetComputerNameExW(ComputerNamePhysicalDnsHostname, std::ptr::null_mut(), &raw mut size) };
        let mut buffer = vec![0_u16; size as usize + 1];
        let mut length = u32::try_from(buffer.len()).unwrap_or(u32::MAX);
        // SAFETY: `buffer` holds `length` UTF-16 units.
        let ok = unsafe { GetComputerNameExW(ComputerNamePhysicalDnsHostname, buffer.as_mut_ptr(), &raw mut length) };
        if ok == 0 {
            return String::new();
        }
        buffer.truncate(length as usize);
        String::from_utf16_lossy(&buffer)
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::time::Duration;

    use super::{RunError, Runner as _, SystemRunner};

    fn argv(items: &[&str]) -> Vec<OsString> {
        items.iter().map(OsString::from).collect()
    }

    #[test]
    fn a_run_past_its_timeout_is_killed() {
        #[cfg(unix)]
        let command = argv(&["sleep", "30"]);
        #[cfg(windows)]
        let command = argv(&["ping", "-n", "30", "127.0.0.1"]);
        let started = std::time::Instant::now();
        let error = SystemRunner
            .run(&command, Some(Duration::from_millis(300)))
            .expect_err("times out");
        assert!(matches!(error, RunError::Timeout(_)), "{error:?}");
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn a_missing_program_is_an_os_error() {
        let error = SystemRunner
            .run(&argv(&["embyx-no-such-program-here"]), None)
            .expect_err("missing");
        assert!(matches!(error, RunError::Io(_)), "{error:?}");
    }

    #[test]
    fn the_host_name_is_known() {
        assert!(!super::hostname().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn a_spawned_child_has_its_own_group_and_can_be_paused() {
        use nix::unistd::{Pid, getpgid, getpgrp};
        let child = SystemRunner.spawn(&argv(&["sleep", "30"])).expect("spawn");
        let pid = Pid::from_raw(i32::try_from(child.id()).expect("pid"));
        assert_eq!(getpgid(Some(pid)).expect("getpgid"), pid);
        assert_ne!(getpgid(Some(pid)).expect("getpgid"), getpgrp());
        child.suspend().expect("suspend");
        #[cfg(target_os = "linux")]
        assert_eq!(wait_for_state(child.id(), 'T'), 'T');
        child.resume().expect("resume");
        #[cfg(target_os = "linux")]
        assert_eq!(wait_for_state(child.id(), 'S'), 'S');
        assert_eq!(child.poll().expect("poll"), None);
        child.kill().expect("kill");
        let mut code = None;
        for _ in 0..250 {
            code = child.poll().expect("poll");
            if code.is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(code, Some(-9));
    }

    /// The state letter of `/proc/<pid>/stat` (`T` stopped, `S` sleeping), once it is `want`
    /// or after a second.
    #[cfg(target_os = "linux")]
    fn wait_for_state(pid: u32, want: char) -> char {
        let state = || {
            let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).expect("stat");
            stat.rsplit_once(") ")
                .expect("stat format")
                .1
                .chars()
                .next()
                .expect("state")
        };
        for _ in 0..50 {
            if state() == want {
                return want;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        state()
    }

    #[cfg(windows)]
    #[test]
    fn a_spawned_child_can_be_suspended_resumed_and_killed() {
        let child = SystemRunner
            .spawn(&argv(&["ping", "-n", "30", "127.0.0.1"]))
            .expect("spawn");
        child.suspend().expect("suspend");
        assert_eq!(child.poll().expect("poll"), None);
        child.resume().expect("resume");
        assert_eq!(child.poll().expect("poll"), None);
        child.kill().expect("kill");
        let mut code = None;
        for _ in 0..250 {
            code = child.poll().expect("poll");
            if code.is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(code, Some(1));
    }
}
