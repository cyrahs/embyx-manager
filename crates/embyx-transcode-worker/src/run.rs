//! `embyx-manager transcode-worker`: the worker's main loop.
//!
//! Python origin: `WAIT_REASONS`, `idle_report`, `check_tools`, `main` and `_sleep` of
//! `src/embyx_manager/merge/home_worker.py`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use embyx_core::pyjson::{Dict, Value};

use crate::config::{self, Args, WorkerConfig};
use crate::console::{Console, Log, StopSignal};
use crate::process::{Runner, SystemRunner};
use crate::py;
use crate::server::{CallError, Cluster, HttpServer};
use crate::task::{Assignment, Outcome, RunError, RunParts, TaskRun, Timing, VERSION};

/// `WAIT_REASONS`: why the cluster has nothing to hand out, as the console says it.
pub fn wait_reason(wait: &str) -> Option<&'static str> {
    Some(match wait {
        "idle" => "没有等转码的作品",
        "busy" => "集群在合并别的作品，等它完成",
        "probing" => "集群正在读取分盘信息",
        "assembling" => "这部已全部转完，集群在拼接，等它完成",
        "unavailable" => "集群这边暂时不能合并",
        _ => return None,
    })
}

/// `idle_report(config)`: what a claim sends.
pub fn idle_report(config: &WorkerConfig) -> Value {
    let mut report = Dict::new();
    report.insert("name", Value::Str(config.name.clone()));
    report.insert("version", Value::Str(VERSION.to_owned()));
    report.insert("task_id", Value::Null);
    report.insert("state", Value::Str("waiting".to_owned()));
    report.insert("parts", Value::List(Vec::new()));
    Value::Dict(report)
}

/// `shutil.which(tool)`: the executable `tool` names, looked up on `PATH` unless it holds a
/// directory part.
pub fn which(tool: &str) -> Option<PathBuf> {
    let candidate = Path::new(tool);
    let has_directory = candidate.components().count() > 1 || candidate.is_absolute();
    if has_directory {
        return executable(candidate);
    }
    let mut directories: Vec<PathBuf> = Vec::new();
    if cfg!(windows) && std::env::var_os("NoDefaultCurrentDirectoryInExePath").is_none() {
        directories.push(PathBuf::from("."));
    }
    if let Some(path) = std::env::var_os("PATH") {
        directories.extend(std::env::split_paths(&path));
    }
    directories
        .into_iter()
        .find_map(|directory| executable(&directory.join(tool)))
}

/// The file `path` names when it is an executable file; on Windows with each of `PATHEXT`'s
/// extensions too, as `shutil.which` tries them.
fn executable(path: &Path) -> Option<PathBuf> {
    #[cfg(windows)]
    {
        let extensions =
            std::env::var("PATHEXT").unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD;.VBS;.JS;.WS;.MSC".to_owned());
        let extensions: Vec<String> = extensions
            .split(';')
            .filter(|ext| !ext.is_empty())
            .map(str::to_lowercase)
            .collect();
        let name = path.to_string_lossy().to_lowercase();
        let mut candidates = Vec::new();
        if extensions.iter().any(|ext| name.ends_with(ext.as_str())) {
            candidates.push(path.to_path_buf());
        }
        for ext in &extensions {
            let mut with_ext = path.as_os_str().to_owned();
            with_ext.push(ext);
            candidates.push(PathBuf::from(with_ext));
        }
        candidates.into_iter().find(|candidate| candidate.is_file())
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let metadata = std::fs::metadata(path).ok()?;
        (metadata.is_file() && metadata.permissions().mode() & 0o111 != 0).then(|| path.to_path_buf())
    }
}

/// `check_tools(config)`: the complaint when ffmpeg or ffprobe cannot be found.
pub fn check_tools(config: &WorkerConfig) -> Option<String> {
    for tool in [&config.ffmpeg, &config.ffprobe] {
        if which(tool).is_none() && !Path::new(tool).is_file() {
            return Some(format!(
                "找不到 {tool}，请先安装 ffmpeg（winget install Gyan.FFmpeg）或用 --ffmpeg 指定路径"
            ));
        }
    }
    None
}

/// What the loop works with; `main` builds the real ones.
#[derive(Clone)]
pub struct Worker {
    pub config: WorkerConfig,
    pub cluster: Arc<dyn Cluster>,
    pub runner: Arc<dyn Runner>,
    pub log: Arc<dyn Log>,
    pub timing: Timing,
    /// The Ctrl+C presses so far (`stop()`).
    pub stop: Arc<dyn Fn() -> usize + Send + Sync>,
    /// Replaces `_sleep` (the goldens record the waits instead of sitting them out).
    pub sleep: Option<Arc<dyn Fn(std::time::Duration) + Send + Sync>>,
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("config", &self.config)
            .field("timing", &self.timing)
            .finish_non_exhaustive()
    }
}

/// Why the loop ended without returning 0: an error Python does not catch, after which it
/// prints `已退出` and its traceback.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error(transparent)]
    Run(#[from] RunError),
    #[error("{}: {}", .0.class, .0.message)]
    Py(#[from] py::PyError),
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

impl Worker {
    /// `_sleep(seconds, stop)`: sleep in quarter seconds until the time is up or Ctrl+C.
    fn sleep(&self, duration: std::time::Duration) {
        if let Some(sleep) = &self.sleep {
            sleep(duration);
            return;
        }
        let deadline = Instant::now() + duration;
        while Instant::now() < deadline && (self.stop)() == 0 {
            thread::sleep(self.timing.tick.min(deadline.saturating_duration_since(Instant::now())));
        }
    }

    /// The `try` block of `main`: claim, wait or work, until Ctrl+C.
    pub fn work(&self) -> Result<(), WorkerError> {
        let log = &self.log;
        let mut failures = 0_usize;
        let mut last_wait = String::new();
        while (self.stop)() == 0 {
            let answer = match self.cluster.claim(&idle_report(&self.config)) {
                Ok(answer) => answer,
                Err(error @ (CallError::Server(_) | CallError::Transport(_) | CallError::Protocol(_))) => {
                    failures += 1;
                    let delay = self.timing.retry_delay(failures);
                    log.event(&format!(
                        "连不上集群（{error}），{} 秒后重试",
                        py::fixed(delay.as_secs_f64(), 0)
                    ));
                    self.sleep(delay);
                    continue;
                }
                Err(CallError::Io(error)) => return Err(WorkerError::Io(error)),
            };
            failures = 0;
            let task = match answer.get("task") {
                None | Some(Value::Null) => {
                    let wait = answer.get("wait").map_or_else(|| "None".to_owned(), py::str_of);
                    let reason = wait_reason(&wait).map_or_else(|| wait.clone(), str::to_owned);
                    if reason != last_wait {
                        log.event(&format!("等待：{reason}"));
                        last_wait = reason;
                    }
                    let probing = matches!(answer.get("wait"), Some(Value::Str(wait)) if wait == "probing");
                    self.sleep(if probing {
                        self.timing.short_poll
                    } else {
                        self.timing.idle_poll
                    });
                    continue;
                }
                Some(Value::Dict(task)) => Assignment::from_dict(task)?,
                Some(other) => {
                    return Err(WorkerError::Py(py::PyError::new(
                        "TypeError",
                        format!("'{}' object is not subscriptable", py::type_name(other)),
                    )));
                }
            };
            last_wait.clear();
            let keep = task.id.to_string();
            for entry in std::fs::read_dir(&self.config.cache_dir)? {
                let entry = entry?;
                if entry.path().is_dir() && entry.file_name().to_string_lossy() != keep {
                    let _ = std::fs::remove_dir_all(entry.path());
                }
            }
            let left = task.parts.iter().filter(|part| !part.done).count();
            log.event(&format!(
                "开始 {}：{} 盘，还剩 {left} 盘，视频码率 {} Mbps",
                task.avid,
                task.parts.len(),
                py::fixed(task.video_mbps, 2)
            ));
            let avid = task.avid.clone();
            let run = TaskRun::new(
                RunParts {
                    config: self.config.clone(),
                    cluster: Arc::clone(&self.cluster),
                    runner: Arc::clone(&self.runner),
                    log: Arc::clone(&self.log),
                    timing: self.timing.clone(),
                },
                task,
            )?;
            let stop = Arc::clone(&self.stop);
            match run.run(&move || stop())? {
                Outcome::Done => log.event(&format!("{avid} 全部分盘已传到集群，接下来由集群拼接、上传 115")),
                Outcome::Resync => self.sleep(self.timing.short_poll),
                Outcome::Stopped | Outcome::Dropped => {}
            }
        }
        Ok(())
    }

    /// `main` from the banner on: the loop, then `已退出` whatever happened.
    pub fn serve(&self) -> Result<(), WorkerError> {
        std::fs::create_dir_all(&self.config.cache_dir)?;
        self.log.event(&format!(
            "transcode-worker {VERSION} 连接 {}，缓存 {}，上传上限 {} Mbps",
            self.config.server,
            self.config.cache_dir.display(),
            py::general(self.config.upload_mbps)
        ));
        let result = self.work();
        self.log.event("已退出");
        result
    }
}

/// `main(args)`: the command's exit status.
pub fn main(args: &Args) -> i32 {
    let config = match config::load_config(args) {
        Ok(config) => config,
        Err(error) => {
            report_error(&error.to_string());
            return 1;
        }
    };
    let console: Arc<dyn Log> = Arc::new(Console::new());
    if let Some(problem) = check_tools(&config) {
        console.event(&problem);
        return 2;
    }
    let stop = StopSignal::new(Arc::clone(&console));
    if let Err(error) = stop.install() {
        report_error(&format!("cannot catch Ctrl+C: {error}"));
        return 1;
    }
    let worker = Worker {
        cluster: Arc::new(HttpServer::new(&config)),
        runner: Arc::new(SystemRunner),
        log: console,
        timing: Timing::default(),
        stop: Arc::new(move || stop.presses()),
        sleep: None,
        config,
    };
    match worker.serve() {
        Ok(()) => 0,
        Err(error) => {
            report_error(&error.to_string());
            1
        }
    }
}

/// What Python leaves on stderr when it stops: a `SystemExit` message, or the last line of a
/// traceback.
fn report_error(message: &str) {
    use std::io::Write as _;
    let mut err = std::io::stderr().lock();
    let _ = writeln!(err, "{message}");
}
