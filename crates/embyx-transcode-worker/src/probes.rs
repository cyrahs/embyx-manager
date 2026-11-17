//! API-coverage probe for process control in the home worker
//! (docs/rewrite/API-COVERAGE.md, "nix" and "windows-sys").
//!
//! `merge/home_worker.py` starts ffmpeg in its own process group (`start_new_session=True`
//! on Unix, `CREATE_NEW_PROCESS_GROUP` on Windows) so a Ctrl+C at the console reaches only
//! the worker; pauses and resumes it (`SIGSTOP`/`SIGCONT`, `NtSuspendProcess` /
//! `NtResumeProcess` from ntdll through ctypes); and counts Ctrl+C (and Ctrl+Break on
//! Windows) presses instead of dying on the first. What the Rust side gives for each. The
//! Windows half runs in the Windows CI job. No application code is ported here.

#[cfg(unix)]
mod unix {
    use std::os::unix::process::CommandExt as _;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    use nix::sys::signal::{self, Signal};
    use nix::unistd::{Pid, getpgid, getpgrp};

    /// The state letter of `/proc/<pid>/stat` (`T` stopped, `S` sleeping).
    #[cfg(target_os = "linux")]
    fn state(pid: u32) -> char {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).expect("stat");
        let after_name = stat.rsplit_once(") ").expect("stat format").1;
        after_name.chars().next().expect("state")
    }

    #[cfg(target_os = "linux")]
    fn wait_for_state(pid: u32, want: char) -> char {
        for _ in 0..50 {
            let have = state(pid);
            if have == want {
                return have;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        state(pid)
    }

    #[test]
    fn a_child_in_its_own_group_can_be_stopped_and_continued() {
        // std has no stable setsid; `process_group(0)` (setpgid) gives the child its own
        // process group, which is what keeps a terminal's Ctrl+C away from it. setsid also
        // detaches it from the controlling terminal; ffmpeg runs with `-nostdin`, so the
        // port uses the process group (a divergence candidate for WP15).
        let mut child = Command::new("sleep")
            .arg("30")
            .stdin(Stdio::null())
            .process_group(0)
            .spawn()
            .expect("spawn sleep");
        let pid = Pid::from_raw(i32::try_from(child.id()).expect("pid"));
        assert_eq!(getpgid(Some(pid)).expect("getpgid"), pid);
        assert_ne!(getpgid(Some(pid)).expect("getpgid"), getpgrp());

        signal::kill(pid, Signal::SIGSTOP).expect("SIGSTOP");
        #[cfg(target_os = "linux")]
        assert_eq!(wait_for_state(child.id(), 'T'), 'T');
        signal::kill(pid, Signal::SIGCONT).expect("SIGCONT");
        #[cfg(target_os = "linux")]
        assert_eq!(wait_for_state(child.id(), 'S'), 'S');

        child.kill().expect("kill");
        child.wait().expect("wait");
    }

    #[test]
    fn ctrl_c_presses_can_be_counted() {
        // The worker counts SIGINT instead of dying on the first one (`StopSignal`).
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let mut interrupts =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).expect("handler");
            for _ in 0..2 {
                signal::raise(Signal::SIGINT).expect("raise");
                tokio::time::timeout(Duration::from_secs(5), interrupts.recv())
                    .await
                    .expect("SIGINT delivered")
                    .expect("stream open");
            }
        });
    }
}

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "ntdll has no safe binding; WP15's process module is the one place for this"
)]
mod windows {
    use std::os::windows::io::AsRawHandle as _;
    use std::os::windows::process::CommandExt as _;
    use std::process::{Command, Stdio};

    use windows_sys::Win32::Foundation::{HANDLE, NTSTATUS, STATUS_SUCCESS};
    use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, GetExitCodeProcess};

    // Not in windows-sys (undocumented ntdll exports); declared the way windows-sys declares
    // its own imports.
    windows_link::link!("ntdll.dll" "system" fn NtSuspendProcess(process: HANDLE) -> NTSTATUS);
    windows_link::link!("ntdll.dll" "system" fn NtResumeProcess(process: HANDLE) -> NTSTATUS);

    /// `STILL_ACTIVE`, what `GetExitCodeProcess` reports for a running process.
    const STILL_ACTIVE: u32 = 259;

    fn exit_code(handle: HANDLE) -> u32 {
        let mut code = 0;
        // SAFETY: `handle` is the live process handle owned by a `Child`.
        let ok = unsafe { GetExitCodeProcess(handle, &raw mut code) };
        assert_ne!(ok, 0, "GetExitCodeProcess");
        code
    }

    #[test]
    fn a_child_in_a_new_group_can_be_suspended_and_resumed() {
        let mut child = Command::new("ping")
            .args(["-n", "30", "127.0.0.1"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .creation_flags(CREATE_NEW_PROCESS_GROUP)
            .spawn()
            .expect("spawn ping");
        let handle: HANDLE = child.as_raw_handle();
        // SAFETY: `handle` is the live process handle owned by `child`.
        assert_eq!(unsafe { NtSuspendProcess(handle) }, STATUS_SUCCESS);
        assert_eq!(exit_code(handle), STILL_ACTIVE);
        // SAFETY: as above.
        assert_eq!(unsafe { NtResumeProcess(handle) }, STATUS_SUCCESS);
        assert_eq!(exit_code(handle), STILL_ACTIVE);
        child.kill().expect("kill");
        child.wait().expect("wait");
    }

    #[test]
    fn ctrl_c_and_ctrl_break_handlers_install() {
        // `StopSignal.install` handles SIGINT and SIGBREAK; tokio's console handlers are the
        // Rust counterparts. Raising a console event in CI would reach the test runner too,
        // so only installation is checked here.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let _ctrl_c = tokio::signal::windows::ctrl_c().expect("ctrl_c");
            let _ctrl_break = tokio::signal::windows::ctrl_break().expect("ctrl_break");
        });
    }
}
