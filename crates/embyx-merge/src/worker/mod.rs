//! The merge Job's body: concatenate one title's parts into a single file.
//!
//! Python origin: `src/embyx_manager/merge/worker.py` and the `merge-worker` command of
//! `__main__.py`.
//!
//! Runs as `embyx-manager merge-worker` inside a Kubernetes Job of its own, so a long merge
//! neither starves the web app nor dies with it. It reads the parts through the CloudDrive
//! mount, writes the merged file next to the uploads CloudDrive will pick up, and reports
//! through a small JSON status file on the same volume: the app polls that file, and the Job
//! carries no database access or secrets.
//!
//! The parts are checked before anything is written: they must exist, fit on the volume with
//! a reserve to spare, and carry the same streams, since `-c copy` cannot reconcile different
//! codecs or resolutions. Parts that carry the same streams in a different order are first
//! remuxed, video first, into a staging directory next to the output, as the concat demuxer
//! pairs streams by index. An MP4 output's index stays in memory until ffmpeg ends, so a title
//! with more packets than the Job's memory limit can index is refused up front rather than
//! killed hours in.
//!
//! A merge is a lossless stream copy unless the parts add up to more than 115 accepts in one
//! file: then the video is re-encoded to AV1 (SVT-AV1) at a bitrate worked out from the
//! duration, so the result fits, while audio is still copied. Only MP4 and Matroska outputs are
//! re-encoded, as MOV cannot carry AV1. The result is checked after: its duration must match
//! the parts' sum, and a re-encode must have come in under the limit. Only then is it renamed
//! into place, so the app never sees a half-written file under the final name.
//!
//! Everything the merge Job contract fixes is Python's to the byte (PLAN.md, cross-version
//! contract 4): every ffmpeg and ffprobe argument list, the concat scripts, the status file's
//! fields and their order, the exit codes (a refusal exits 0). `goldens/merge/` holds Python's
//! runs and the tests replay them.

pub mod exec;
pub mod status;

use std::collections::{BTreeSet, VecDeque};
use std::fs;
use std::io::{self, Read as _, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use embyx_core::pyjson::{self, Dict, Value};
use embyx_core::{pypath, pytext};
use sha1::{Digest as _, Sha1};

pub use self::exec::{Captured, Exec, Process, Spawned, SystemExec};
pub use self::status::{StatusFile, read_status};
use crate::py::{self, PyException};
use crate::transcode::{audio_path, audio_status_path, chunk_path, concat_script, read_json};

/// `MUXERS`: the containers a merge can write, by file suffix; the parts' own container is kept.
pub const MUXERS: [(&str, &str); 7] = [
    (".mp4", "mp4"),
    (".m4v", "mp4"),
    (".mov", "mov"),
    (".mkv", "matroska"),
    (".ts", "mpegts"),
    (".avi", "avi"),
    (".wmv", "asf"),
];
/// `SIGNATURE_KEYS`: stream properties that must agree across parts for a stream copy to play
/// through.
pub const SIGNATURE_KEYS: [&str; 6] = ["codec_type", "codec_name", "width", "height", "sample_rate", "channels"];
/// How far the merged duration may drift from the parts' sum: container rounding per part.
pub const DURATION_TOLERANCE_SECONDS: f64 = 2.0;
pub const DURATION_TOLERANCE_SHARE: f64 = 0.002;
pub const HASH_CHUNK_BYTES: usize = 8 * 1024 * 1024;
pub const GIB: i128 = 1024 * 1024 * 1024;
/// ffmpeg's stderr is kept to this many trailing lines for the failure report.
pub const STDERR_TAIL_LINES: usize = 20;
/// ffmpeg is stopped once neither its output nor its position has moved for this long.
pub const STALL_SECONDS: f64 = 15.0 * 60.0;
/// The largest single file 115 accepts ("单文件不能大于115GB"); a bigger merge could never be
/// uploaded.
pub const MAX_UPLOAD_BYTES: i128 = 115 * 1_000_000_000;
/// What a re-encode aims for: this share of 115's limit, leaving room for the container and
/// for the encoder overshooting its average bitrate.
pub const TRANSCODE_BUDGET_SHARE: f64 = 0.9;
/// And no more than this share of the parts' own video bitrate (VMAF 93.1 on the 4K60 VR
/// test clip with the image's SVT-AV1, as x265 "faster" gave at 60%).
pub const TRANSCODE_SOURCE_SHARE: f64 = 0.4;
/// The share the home worker's NVENC AV1 gets instead (50% kept every frame above VMAF 89).
pub const HOME_SOURCE_SHARE: f64 = 0.5;
/// NVENC's VBR came out 9-14% above its target, so the home path aims lower under 115's limit.
pub const HOME_BUDGET_SHARE: f64 = 0.8;
/// Assumed for an audio stream whose container does not state its bitrate.
pub const FALLBACK_AUDIO_BITS: i128 = 320_000;
/// The muxers a re-encoded AV1 stream may go into (MOV cannot carry AV1).
pub const TRANSCODE_MUXERS: [&str; 2] = ["mp4", "matroska"];
/// SVT-AV1's speed preset and level of parallelism (lp=2 peaked at 1.8 GiB on the 4K clip with
/// the image's SVT-AV1 2.3; lp=3 and lp=4 were too much for the Job's 3 GiB).
pub const TRANSCODE_PRESET: &str = "8";
pub const TRANSCODE_THREADS: i64 = 2;
/// SVT-AV1's lookahead; 2.3 raises 0 to the least the preset allows.
pub const TRANSCODE_LOOKAHEAD: i64 = 0;
/// SVT-AV1's and the decoder's memory for a frame of `TRANSCODE_REFERENCE_PIXELS`, on top of
/// the muxer's index; scaled by frame size, never below the floor.
pub const TRANSCODE_MEMORY_BYTES: i128 = 2 * GIB;
pub const TRANSCODE_REFERENCE_PIXELS: i128 = 4096 * 2048;
pub const TRANSCODE_MEMORY_FLOOR_BYTES: i128 = 512 * 1024 * 1024;
/// The muxers that hold an entry per packet in memory until they write the index at the end.
pub const INDEXED_MUXERS: [&str; 2] = ["mp4", "mov"];
/// Memory per packet held by those muxers, and ffmpeg's and the worker's own baseline.
pub const INDEX_BYTES_PER_PACKET: i128 = 80;
pub const INDEX_BASELINE_BYTES: i128 = 192 * 1024 * 1024;
/// The Job container's memory limit under cgroup v2: a byte count, or `max`.
pub const CGROUP_MEMORY_MAX: &str = "/sys/fs/cgroup/memory.max";

/// Why a merge stopped: Python's exception classes, with the text they carry.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WorkerError {
    /// `MergeRefusedError`: the parts can never be merged, however often the Job runs. The
    /// check did its job, so the Job still ends cleanly (exit 0).
    #[error("{0}")]
    Refused(String),
    /// `MergeWorkerError` or `OSError`: worded for the operator; the Job fails (exit 1).
    #[error("{0}")]
    Failed(String),
    /// An exception the Python worker does not catch: no status update, exit 1.
    #[error("{}: {}", .0.class, .0.message)]
    Uncaught(PyException),
}

impl WorkerError {
    pub(crate) fn os(error: &io::Error, path: &Path) -> Self {
        Self::Failed(py::os_error(error, Some(path), None))
    }

    pub(crate) fn os2(error: &io::Error, path: &Path, other: &Path) -> Self {
        Self::Failed(py::os_error(error, Some(path), Some(other)))
    }

    fn os_program(error: &io::Error, program: &str) -> Self {
        Self::Failed(py::os_error(error, Some(Path::new(program)), None))
    }
}

impl From<PyException> for WorkerError {
    fn from(error: PyException) -> Self {
        Self::Uncaught(error)
    }
}

type Fields = Vec<(String, Value)>;

fn text(value: &str) -> Value {
    Value::Str(value.to_owned())
}

fn float(value: f64) -> Value {
    Value::Float(value)
}

fn int(value: i128) -> Value {
    Value::Int(value)
}

/// `str(path)`.
fn show(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// `path.name`.
fn name(path: &Path) -> String {
    pypath::name(&show(path)).to_owned()
}

/// `path.suffix`.
fn suffix(path: &Path) -> String {
    pypath::suffix(&show(path)).to_owned()
}

/// `Path(text)`: pathlib's spelling of a path given on the command line.
pub fn py_path(text: &str) -> PathBuf {
    PathBuf::from(pypath::PyPath::new(text).as_str())
}

fn with_name(path: &Path, new_name: &str) -> PathBuf {
    path.with_file_name(new_name)
}

/// `partial_path`: where the merge is written until it has been checked.
pub fn partial_path(output: &Path) -> PathBuf {
    with_name(output, &format!(".{}.part", name(output)))
}

/// `staging_dir`: where parts remuxed into stream order are kept while the merge runs.
pub fn staging_dir(output: &Path) -> PathBuf {
    with_name(output, &format!(".{}.parts", name(output)))
}

/// `MUXERS[suffix]`.
pub fn muxer(suffix: &str) -> Option<&'static str> {
    MUXERS
        .iter()
        .find(|(known, _)| *known == suffix)
        .map(|(_, muxer)| *muxer)
}

/// `memory_limit`: the container's memory limit in bytes, or `None` when it has none or it
/// cannot be read.
pub fn memory_limit(path: &Path) -> Result<Option<i128>, WorkerError> {
    let Ok(bytes) = fs::read(path) else {
        return Ok(None);
    };
    // `read_text(encoding='utf-8')` raises `UnicodeDecodeError`, which is no `OSError`.
    let text = std::str::from_utf8(&bytes).map_err(|_| {
        PyException::new(
            "UnicodeDecodeError",
            "'utf-8' codec can't decode the cgroup memory limit",
        )
    })?;
    let value = pytext::py_strip(text);
    if !pytext::py_isdigit(value) {
        return Ok(None);
    }
    pytext::py_int_decimal(value).map(Some).ok_or_else(|| {
        WorkerError::Uncaught(PyException::new(
            "ValueError",
            format!("invalid literal for int() with base 10: {}", pytext::py_repr(value)),
        ))
    })
}

/// `Probe`: what ffprobe says about one file.
#[derive(Debug, Clone, PartialEq)]
pub struct Probe {
    pub duration: f64,
    /// The video and audio streams' properties (`SIGNATURE_KEYS`), in the file's order.
    pub signature: Vec<Vec<Value>>,
    /// Every stream's type in index order, which the concat demuxer pairs streams by.
    pub layout: Vec<String>,
    /// Video and audio packets, when the container counts them (MP4 does, Matroska does not).
    pub packets: Option<i128>,
    /// The audio streams' bitrate in bits per second, as the container states it or assumed.
    pub audio_bits: i128,
}

fn is_video(stream: &[Value]) -> bool {
    stream.first().and_then(Value::as_str) == Some("video")
}

/// Python's tuple `==` over two signatures. Tuples compare their items by identity first, and
/// `json.loads` gives every `NaN` the same float object, so a `NaN` equals a `NaN` here.
fn same_signature(a: &[Vec<Value>], b: &[Vec<Value>]) -> bool {
    let same = |x: &Value, y: &Value| match (x, y) {
        (Value::Float(x), Value::Float(y)) if x.is_nan() && y.is_nan() => true,
        _ => py::eq(x, y),
    };
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(x, y)| x.len() == y.len() && x.iter().zip(y).all(|(x, y)| same(x, y)))
}

impl Probe {
    /// `by_kind`: the signature with video before audio, which a remux with
    /// `-map 0:v -map 0:a` yields.
    pub fn by_kind(&self) -> Vec<Vec<Value>> {
        let mut streams = self.signature.clone();
        streams.sort_by_key(|stream| !is_video(stream));
        streams
    }

    /// `in_order`: whether the file holds nothing but its video streams followed by its audio
    /// streams.
    pub fn in_order(&self) -> bool {
        let kinds = self.by_kind();
        self.layout.len() == kinds.len()
            && self
                .layout
                .iter()
                .zip(&kinds)
                .all(|(layout, stream)| py::eq(&Value::Str(layout.clone()), &stream[0]))
    }
}

/// `concat_list`: an ffmpeg concat-demuxer script naming each part; quotes are escaped the
/// demuxer's way.
pub fn concat_list(parts: &[PathBuf]) -> String {
    let lines: Vec<String> = parts
        .iter()
        .map(|part| format!("file '{}'", show(part).replace('\'', "'\\''")))
        .collect();
    lines.join("\n") + "\n"
}

/// `transcode_bitrate`: the video bitrate, in bits per second, that brings a re-encode under
/// `max_upload_bytes`: the lower of what the budget leaves after the copied audio and `share`
/// of the parts' own video bitrate.
pub fn transcode_bitrate(
    total_bytes: i128,
    duration: f64,
    audio_bits: i128,
    share: f64,
    budget_share: f64,
    max_upload_bytes: i128,
) -> Result<i128, WorkerError> {
    if duration == 0.0 {
        return Err(PyException::new("ZeroDivisionError", "float division by zero").into());
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "Python converts the ints to floats the same way"
    )]
    let (max, total, audio) = (max_upload_bytes as f64, (total_bytes * 8) as f64, audio_bits as f64);
    let budget = max * budget_share * 8.0 / duration - audio;
    let video = total / duration - audio;
    // `max(video, 0) * share`: the int 0 when 0 is larger, so the product is 0.0.
    let source = if 0.0 > video { 0.0 } else { video * share };
    let bits = py::int_of_float(py::min(budget, source))?;
    if bits <= 0 {
        return Err(WorkerError::Refused(format!(
            "the audio alone leaves no room for video under {} GB",
            py::floordiv(max_upload_bytes, 1_000_000_000)
        )));
    }
    Ok(bits)
}

/// `transcode_memory`: the encoder's and decoder's memory, in bytes, for a re-encode of
/// `result`'s video.
pub fn transcode_memory(result: &Probe) -> i128 {
    let dimension = |value: &Value| match value {
        Value::Int(number) => Some(*number),
        Value::Bool(flag) => Some(i128::from(*flag)),
        _ => None,
    };
    let mut pixels = 0;
    for stream in &result.signature {
        if let (true, Some(width), Some(height)) = (is_video(stream), dimension(&stream[2]), dimension(&stream[3])) {
            pixels = pixels.max(width * height);
        }
    }
    if pixels == 0 {
        return TRANSCODE_MEMORY_BYTES;
    }
    TRANSCODE_MEMORY_FLOOR_BYTES.max(py::floordiv(
        TRANSCODE_MEMORY_BYTES * pixels,
        TRANSCODE_REFERENCE_PIXELS,
    ))
}

/// `_check_memory`.
fn check_memory(probes: &[Probe], muxer: &str, limit: Option<i128>, transcode: bool) -> Result<(), WorkerError> {
    let Some(limit) = limit else {
        return Ok(());
    };
    if !INDEXED_MUXERS.contains(&muxer) || probes.iter().any(|result| result.packets.is_none()) {
        return Ok(());
    }
    let packets: i128 = probes.iter().map(|result| result.packets.unwrap_or(0)).sum();
    let mut needed = packets * INDEX_BYTES_PER_PACKET + INDEX_BASELINE_BYTES;
    if transcode {
        needed += transcode_memory(&probes[0]);
    }
    if needed > limit {
        return Err(WorkerError::Refused(format!(
            "the parts hold {} packets, whose {muxer} index needs about {} GiB of memory, more than the Job is \
             allowed ({} GiB)",
            py::thousands(packets),
            py::fixed(py::truediv(needed, GIB), 1),
            py::fixed(py::truediv(limit, GIB), 1),
        )));
    }
    Ok(())
}

/// The free space of the file system holding a directory (`shutil.disk_usage(path).free`).
pub type FreeSpace = dyn Fn(&Path) -> io::Result<u64> + Send + Sync;

/// `shutil.disk_usage(path).free`: blocks available to an unprivileged user.
pub fn disk_free(path: &Path) -> io::Result<u64> {
    #[cfg(unix)]
    {
        let stat = rustix::fs::statvfs(path).map_err(io::Error::from)?;
        Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the merge worker runs in the Linux merge Job only",
        ))
    }
}

/// The worker's settings: the module constants Python's tests patch, and the programs.
#[derive(Debug, Clone)]
pub struct Settings {
    pub ffmpeg: String,
    pub ffprobe: String,
    /// `MAX_UPLOAD_BYTES`.
    pub max_upload_bytes: i128,
    /// `TRANSCODE_PRESET`.
    pub transcode_preset: String,
    /// `STALL_SECONDS`.
    pub stall_seconds: f64,
    /// Stands in for `transcode_bitrate` (a test patches it to force an oversized encode).
    pub video_bits: Option<i128>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            ffmpeg: "ffmpeg".to_owned(),
            ffprobe: "ffprobe".to_owned(),
            max_upload_bytes: MAX_UPLOAD_BYTES,
            transcode_preset: TRANSCODE_PRESET.to_owned(),
            stall_seconds: STALL_SECONDS,
            video_bits: None,
        }
    }
}

/// The merge worker with what it runs on: settings, the way processes start, the free-space
/// check.
pub struct Worker {
    pub settings: Settings,
    exec: Arc<dyn Exec>,
    free_space: Arc<FreeSpace>,
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl Default for Worker {
    fn default() -> Self {
        Self::new(Settings::default())
    }
}

impl Worker {
    /// The real worker: processes from `PATH`, the file system's own free space.
    pub fn new(settings: Settings) -> Self {
        Self::with(settings, Arc::new(SystemExec), Arc::new(disk_free))
    }

    pub fn with(settings: Settings, exec: Arc<dyn Exec>, free_space: Arc<FreeSpace>) -> Self {
        Self {
            settings,
            exec,
            free_space,
        }
    }

    /// `probe`: ffprobe's view of `path`.
    pub fn probe(&self, path: &Path) -> Result<Probe, WorkerError> {
        let ffprobe = &self.settings.ffprobe;
        let argv: Vec<String> = [
            ffprobe.as_str(),
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
        ]
        .iter()
        .map(|&arg| arg.to_owned())
        .chain([show(path)])
        .collect();
        let completed = self
            .exec
            .run(&argv)
            .map_err(|error| WorkerError::os_program(&error, ffprobe))?;
        if completed.returncode != 0 {
            let stderr = String::from_utf8_lossy(&completed.stderr);
            let lines = py::splitlines(pytext::py_strip(&stderr));
            return Err(WorkerError::Failed(format!(
                "ffprobe cannot read {}: {}",
                name(path),
                lines.last().copied().unwrap_or("no output")
            )));
        }
        let no_duration = || WorkerError::Failed(format!("ffprobe reports no duration for {}", name(path)));
        let info = parse_json_bytes(&completed.stdout).ok_or_else(no_duration)?;
        let duration = subscript(&info, "format")
            .and_then(|format| subscript(format, "duration"))
            .and_then(py::float)
            .map_err(|_| no_duration())?;
        // `info.get('streams', [])`; info is a dict, or the subscript above had failed.
        let every = streams(info.get("streams"))?;
        let media: Vec<&Dict> = every
            .iter()
            .filter(|stream| {
                matches!(
                    stream.get("codec_type").and_then(Value::as_str),
                    Some("video" | "audio")
                )
            })
            .collect();
        let signature: Vec<Vec<Value>> = media
            .iter()
            .map(|stream| {
                SIGNATURE_KEYS
                    .iter()
                    .map(|key| stream.get(key).cloned().unwrap_or(Value::Null))
                    .collect()
            })
            .collect();
        if !signature.iter().any(|stream| is_video(stream)) {
            return Err(WorkerError::Refused(format!("{} has no video stream", name(path))));
        }
        let field = |stream: &Dict, key: &str| stream.get(key).map_or_else(String::new, py::str_of);
        let counts: Vec<String> = media.iter().map(|stream| field(stream, "nb_frames")).collect();
        let audio_rates: Vec<String> = media
            .iter()
            .filter(|stream| stream.get("codec_type").and_then(Value::as_str) == Some("audio"))
            .map(|stream| field(stream, "bit_rate"))
            .collect();
        let packets = if counts.iter().all(|count| pytext::py_isdigit(count)) {
            Some(
                counts
                    .iter()
                    .map(|count| int_of_digits(count))
                    .sum::<Result<i128, WorkerError>>()?,
            )
        } else {
            None
        };
        let mut audio_bits = 0;
        for rate in &audio_rates {
            audio_bits += if pytext::py_isdigit(rate) {
                int_of_digits(rate)?
            } else {
                FALLBACK_AUDIO_BITS
            };
        }
        Ok(Probe {
            duration,
            signature,
            layout: every
                .iter()
                .map(|stream| stream.get("codec_type").map_or_else(|| "None".to_owned(), py::str_of))
                .collect(),
            packets,
            audio_bits,
        })
    }

    fn check_space(&self, output: &Path, needed: i128, reserve_bytes: i128) -> Result<(), WorkerError> {
        let parent = status::parent(output);
        let free = (self.free_space)(&parent).map_err(|error| WorkerError::os(&error, &parent))?;
        // A leftover from an interrupted attempt is about to be overwritten.
        let reclaimable = match fs::metadata(partial_path(output)) {
            Ok(meta) => i128::from(meta.len()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
            Err(error) => return Err(WorkerError::os(&error, &partial_path(output))),
        };
        let available = i128::from(free) + reclaimable;
        if available < needed + reserve_bytes {
            return Err(WorkerError::Failed(format!(
                "not enough space in {}: {} GiB free, {} GiB needed plus a {} GiB reserve",
                show(&parent),
                py::fixed(py::truediv(available, GIB), 1),
                py::fixed(py::truediv(needed, GIB), 1),
                py::fixed(py::truediv(reserve_bytes, GIB), 0),
            )));
        }
        Ok(())
    }

    fn check_parts(&self, parts: &[PathBuf], output: &Path) -> Result<i128, WorkerError> {
        if parts.len() < 2 {
            return Err(WorkerError::Refused("a merge needs at least two parts".to_owned()));
        }
        let output_suffix = suffix(output).to_lowercase();
        let suffixes: BTreeSet<String> = parts.iter().map(|part| suffix(part).to_lowercase()).collect();
        if suffixes.len() != 1 || !suffixes.contains(&output_suffix) {
            let listed: Vec<String> = suffixes.iter().map(|found| pytext::py_repr(found)).collect();
            return Err(WorkerError::Refused(format!(
                "parts and output must share one container, got [{}]",
                listed.join(", ")
            )));
        }
        let Some(muxer) = muxer(&output_suffix) else {
            return Err(WorkerError::Refused(format!(
                "cannot write a {} container",
                suffix(output)
            )));
        };
        let mut total = 0;
        for part in parts {
            match fs::metadata(part) {
                Ok(meta) => total += i128::from(meta.len()),
                Err(error) => {
                    return Err(WorkerError::Failed(format!(
                        "cannot read {}: {}",
                        show(part),
                        py::strerror(&error)
                    )));
                }
            }
        }
        if total > self.settings.max_upload_bytes && !TRANSCODE_MUXERS.contains(&muxer) {
            return Err(WorkerError::Refused(format!(
                "the parts add up to {} GB, more than the {} GB 115 accepts in one file, and a {} merge cannot be \
                 re-encoded to fit",
                py::fixed(py::truediv(total, 1_000_000_000), 1),
                py::floordiv(self.settings.max_upload_bytes, 1_000_000_000),
                suffix(output),
            )));
        }
        Ok(total)
    }

    /// `_run_ffmpeg`: run ffmpeg, report its progress, stop it once it stalls, and turn a
    /// failure into the last lines it wrote to stderr.
    pub fn run_ffmpeg(&self, command: &[String], duration: f64, status: &mut StatusFile) -> Result<(), WorkerError> {
        let program = command.first().map_or("", String::as_str);
        let spawned = self
            .exec
            .spawn(command)
            .map_err(|error| WorkerError::os_program(&error, program))?;
        let Spawned {
            stdout,
            stderr,
            process,
        } = spawned;
        let tail = Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_TAIL_LINES)));
        // stderr has to be read while ffmpeg runs: a damaged part can make the demuxer log an
        // error every few bytes, and once the pipe's 64 KiB fill up ffmpeg stops mid-write.
        let reader = {
            let tail = Arc::clone(&tail);
            let spawned = std::thread::Builder::new()
                .name("ffmpeg-stderr".to_owned())
                .spawn(move || keep_tail(stderr, &tail));
            match spawned {
                Ok(reader) => reader,
                Err(error) => {
                    // `threading.Thread.start` raises inside `with Popen`, whose exit waits.
                    stop(process.as_ref());
                    return Err(cannot_start_thread(&error));
                }
            }
        };
        let stall_seconds = self.settings.stall_seconds;
        let mut watch = match StallWatch::start(Arc::clone(&process), stall_seconds) {
            Ok(watch) => watch,
            Err(error) => {
                stop(process.as_ref());
                let _ = reader.join();
                return Err(cannot_start_thread(&error));
            }
        };
        let outcome = follow_progress(stdout, duration, status, &watch, process.as_ref());
        watch.stop();
        if !matches!(process.try_wait(), Ok(Some(_))) {
            process.kill();
        }
        // `Popen.__exit__` waits for the process.
        let waited = wait(process.as_ref());
        let _ = reader.join();
        let returncode = outcome?;
        let returncode = returncode.or(waited).unwrap_or(-1);
        let tail: Vec<String> = tail
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .cloned()
            .collect();
        if watch.fired() {
            let span = if stall_seconds >= 60.0 {
                format!("{} minutes", py::fixed(stall_seconds / 60.0, 0))
            } else {
                format!("{} seconds", py::fixed(stall_seconds, 0))
            };
            let mut message = format!("ffmpeg made no progress for {span} and was stopped");
            if !tail.is_empty() {
                message.push_str(": ");
                message.push_str(&tail.join(" | "));
            }
            return Err(WorkerError::Failed(message));
        }
        if returncode != 0 {
            let detail = if tail.is_empty() {
                format!("exit {returncode}")
            } else {
                tail.join(" | ")
            };
            return Err(WorkerError::Failed(format!("ffmpeg failed: {detail}")));
        }
        Ok(())
    }

    fn ffmpeg_command(&self, middle: &[&str], target: &Path) -> Vec<String> {
        let head = [
            self.settings.ffmpeg.as_str(),
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "error",
            "-y",
        ];
        head.iter()
            .chain(middle)
            .chain(["-progress", "pipe:1", "-nostats"].iter())
            .map(|&arg| arg.to_owned())
            .chain([show(target)])
            .collect()
    }

    /// `merge`: merge `parts` into `output`; returns the final status fields.
    #[allow(
        clippy::too_many_lines,
        reason = "one step after the other, as merge() reads in Python"
    )]
    pub fn merge(
        &self,
        parts: &[PathBuf],
        output: &Path,
        status: &mut StatusFile,
        reserve_bytes: i128,
        workdir: &Path,
        memory_limit: Option<i128>,
    ) -> Result<Dict, WorkerError> {
        status.update([("phase", text("checking")), ("progress", float(0.0))])?;
        let total_bytes = self.check_parts(parts, output)?;
        let max = self.settings.max_upload_bytes;
        let transcode = total_bytes > max;
        // A re-encode writes at most 115's limit, not the parts' size.
        self.check_space(output, if transcode { max } else { total_bytes }, reserve_bytes)?;
        status.update([("total_bytes", int(total_bytes))])?;
        let muxer = muxer(&suffix(output).to_lowercase()).unwrap_or_default();

        let probes = self.probe_all(parts, status)?;
        let first = &probes[0];
        check_memory(&probes, muxer, memory_limit, transcode)?;
        let expected = py::sum(probes.iter().map(|result| result.duration));
        let video_bits = if transcode {
            Some(match self.settings.video_bits {
                Some(bits) => bits,
                None => transcode_bitrate(
                    total_bytes,
                    expected,
                    first.audio_bits,
                    TRANSCODE_SOURCE_SHARE,
                    TRANSCODE_BUDGET_SHARE,
                    max,
                )?,
            })
        } else {
            None
        };

        // Same streams in another order: put every part that strays video first.
        let mut stray = Vec::new();
        if probes.iter().any(|result| result.layout != first.layout) {
            stray = probes
                .iter()
                .enumerate()
                .filter(|(_, result)| !result.in_order())
                .map(|(index, _)| index)
                .collect();
            let written = if transcode { max } else { total_bytes };
            let mut extra = 0;
            for &index in &stray {
                extra += i128::from(
                    fs::metadata(&parts[index])
                        .map_err(|error| WorkerError::os(&error, &parts[index]))?
                        .len(),
                );
            }
            self.check_space(output, written + extra, reserve_bytes)?;
        }
        let staging = staging_dir(output);
        let target = partial_path(output);
        let mut inputs = parts.to_vec();
        let concatenated = (|| {
            for &index in &stray {
                status.update([("phase", text("reordering")), ("progress", float(0.0))])?;
                mkdir_exist_ok(&staging)?;
                inputs[index] = staging.join(name(&parts[index]));
                let source = show(&parts[index]);
                let command = self.ffmpeg_command(
                    &["-i", &source, "-map", "0:v", "-map", "0:a?", "-c", "copy", "-f", muxer],
                    &inputs[index],
                );
                self.run_ffmpeg(&command, probes[index].duration, status)?;
            }
            self.concat(&inputs, &target, muxer, expected, status, workdir, video_bits)
        })();
        let _ = fs::remove_dir_all(&staging);
        concatenated?;
        self.finish(&target, output, expected, status)
    }

    /// `_concat`: concatenate `parts` by stream copy, or re-encode the video to AV1 at
    /// `video_bits`.
    #[allow(clippy::too_many_arguments, reason = "Python's signature")]
    fn concat(
        &self,
        parts: &[PathBuf],
        target: &Path,
        muxer: &str,
        expected: f64,
        status: &mut StatusFile,
        workdir: &Path,
        video_bits: Option<i128>,
    ) -> Result<(), WorkerError> {
        let bits;
        let svt;
        let codecs: Vec<&str> = if let Some(video_bits) = video_bits {
            status.update([
                ("phase", text("transcoding")),
                ("progress", float(0.0)),
                ("duration", float(expected)),
                ("video_bitrate", int(video_bits)),
            ])?;
            bits = video_bits.to_string();
            svt = format!("lp={TRANSCODE_THREADS}:lookahead={TRANSCODE_LOOKAHEAD}");
            vec![
                "-c:v",
                "libsvtav1",
                "-preset",
                &self.settings.transcode_preset,
                "-b:v",
                &bits,
                "-svtav1-params",
                &svt,
                "-c:a",
                "copy",
            ]
        } else {
            status.update([
                ("phase", text("merging")),
                ("progress", float(0.0)),
                ("duration", float(expected)),
            ])?;
            vec!["-c", "copy"]
        };
        let script = pypath::PyPath::new(&show(workdir)).join("concat.txt");
        fs::write(&script, concat_list(parts)).map_err(|error| WorkerError::os(&error, &script))?;
        let script_text = show(&script);
        let mut middle = vec![
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            &script_text,
            "-map",
            "0:v",
            "-map",
            "0:a?",
        ];
        middle.extend(codecs);
        middle.extend(["-f", muxer]);
        let command = self.ffmpeg_command(&middle, target);
        self.run_ffmpeg(&command, expected, status)
    }

    /// `_probe_all`: probe every part and check they carry the same streams, as a stream copy
    /// needs.
    fn probe_all(&self, parts: &[PathBuf], status: &mut StatusFile) -> Result<Vec<Probe>, WorkerError> {
        status.update([("phase", text("probing"))])?;
        let mut probes = Vec::with_capacity(parts.len());
        for (index, part) in parts.iter().enumerate() {
            probes.push(self.probe(part)?);
            #[allow(clippy::cast_possible_wrap, reason = "a handful of parts")]
            status.update_lazily([("progress", float(py::truediv(index as i128 + 1, parts.len() as i128)))])?;
        }
        let first = probes[0].by_kind();
        for (part, result) in parts.iter().zip(&probes) {
            if !same_signature(&result.by_kind(), &first) {
                return Err(WorkerError::Refused(format!(
                    "{} carries different streams than {}; a stream copy would break",
                    name(part),
                    name(&parts[0])
                )));
            }
        }
        Ok(probes)
    }

    /// `prepare_home`: probe the parts for the home worker, then copy their audio into
    /// `transcode_dir`.
    ///
    /// The status says each part's duration and the video bitrate as soon as the probe is
    /// done, which is all the worker waits for; the audio copy, a full read of every part, runs
    /// while the worker encodes. Audio already copied by an earlier run of the Job is kept.
    pub fn prepare_home(
        &self,
        parts: &[PathBuf],
        output: &Path,
        status: &mut StatusFile,
        transcode_dir: &Path,
        reserve_bytes: i128,
        memory_limit: Option<i128>,
    ) -> Result<Dict, WorkerError> {
        status.update([("phase", text("checking")), ("progress", float(0.0))])?;
        let total_bytes = self.check_parts(parts, output)?;
        let muxer = muxer(&suffix(output).to_lowercase()).unwrap_or_default();
        if !TRANSCODE_MUXERS.contains(&muxer) {
            return Err(WorkerError::Refused(format!(
                "a {} merge cannot carry AV1",
                suffix(output)
            )));
        }
        let probes = self.probe_all(parts, status)?;
        check_memory(&probes, muxer, memory_limit, false)?;
        let expected = py::sum(probes.iter().map(|result| result.duration));
        let first = &probes[0];
        let video_bits = transcode_bitrate(
            total_bytes,
            expected,
            first.audio_bits,
            HOME_SOURCE_SHARE,
            HOME_BUDGET_SHARE,
            self.settings.max_upload_bytes,
        )?;
        #[allow(clippy::cast_precision_loss, reason = "int * float in Python")]
        let estimate = py::int_of_float(video_bits as f64 * expected / 8.0)?
            + py::int_of_float(first.audio_bits as f64 * expected / 8.0)?;
        // The chunks and the assembled file sit on the volume together for a while.
        self.check_space(output, 2 * estimate, reserve_bytes)?;
        let has_audio = first.signature.iter().any(|stream| stream[0].as_str() == Some("audio"));
        let mut listed = Vec::with_capacity(parts.len());
        for (part, result) in parts.iter().zip(&probes) {
            let size = fs::metadata(part).map_err(|error| WorkerError::os(&error, part))?.len();
            let mut entry = Dict::new();
            entry.insert("duration", float(result.duration));
            entry.insert("size", int(i128::from(size)));
            listed.push(Value::Dict(entry));
        }
        status.update([
            ("phase", text("audio")),
            ("progress", float(0.0)),
            ("total_bytes", int(total_bytes)),
            ("duration", float(expected)),
            ("video_bitrate", int(video_bits)),
            ("estimated_bytes", int(estimate)),
            ("has_audio", Value::Bool(has_audio)),
            ("parts", Value::List(listed)),
            ("audio_done", int(0)),
        ])?;
        #[allow(clippy::cast_possible_wrap, reason = "a handful of parts")]
        for (index, part) in parts.iter().enumerate() {
            if !has_audio {
                break;
            }
            let target = audio_path(transcode_dir, index as i128);
            if !target.is_file() {
                status::mkdir_parents(&status::parent(&target))?;
                let temporary = with_name(&target, &format!(".{}.part", name(&target)));
                status.update([("part", int(index as i128))])?;
                let source = show(part);
                let command = self.ffmpeg_command(
                    &[
                        "-i", &source, "-map", "0:a", "-vn", "-sn", "-dn", "-c", "copy", "-f", "matroska",
                    ],
                    &temporary,
                );
                self.run_ffmpeg(&command, probes[index].duration, status)?;
                fs::rename(&temporary, &target).map_err(|error| WorkerError::os2(&error, &temporary, &target))?;
            }
            status.update([
                ("audio_done", int(index as i128 + 1)),
                ("progress", float(py::truediv(index as i128 + 1, parts.len() as i128))),
            ])?;
        }
        let mut result = Dict::new();
        #[allow(clippy::cast_possible_wrap, reason = "a handful of parts")]
        result.insert("audio_done", int(parts.len() as i128));
        Ok(result)
    }

    /// `assemble_home`: put the home worker's video chunks and the copied audio together into
    /// `output`.
    ///
    /// `manifest.json` in `transcode_dir` (written by the app) lists each part's chunks and
    /// durations. Every part starts where it started in the originals: the last chunk of a part
    /// is stretched or cut to the part's duration, and its audio given the same, which is how
    /// the concat demuxer lines up a lossless merge.
    pub fn assemble_home(
        &self,
        output: &Path,
        status: &mut StatusFile,
        transcode_dir: &Path,
        reserve_bytes: i128,
        workdir: &Path,
    ) -> Result<Dict, WorkerError> {
        status.update([("phase", text("checking")), ("progress", float(0.0))])?;
        let manifest = read_json(&transcode_dir.join("manifest.json"));
        let prepared = read_json(&audio_status_path(transcode_dir));
        let (Some(manifest), Some(prepared)) = (manifest, prepared) else {
            return Err(WorkerError::Failed(
                "the transcode manifest or the copied audio is missing".to_owned(),
            ));
        };
        if prepared.get("state").and_then(Value::as_str) != Some("done") {
            return Err(WorkerError::Failed(
                "the transcode manifest or the copied audio is missing".to_owned(),
            ));
        }
        let prepared = Value::Dict(prepared);
        let manifest = Value::Dict(manifest);
        let mut durations = Vec::new();
        for entry in iterate(subscript(&prepared, "parts")?)? {
            durations.push(py::float(subscript(&entry, "duration")?)?);
        }
        let has_audio = prepared.get("has_audio").is_some_and(Value::is_truthy);
        let manifest_parts = subscript(&manifest, "parts")?;
        let listed = py_len(manifest_parts)?;
        if listed != durations.len() {
            return Err(WorkerError::Failed(format!(
                "the manifest lists {listed} parts, the originals {}",
                durations.len()
            )));
        }
        status.update([("phase", text("probing"))])?;
        let (video, audio, needed) =
            self.assembly_inputs(manifest_parts, &durations, transcode_dir, has_audio, status)?;
        self.check_space(output, needed, reserve_bytes)?;
        let expected = py::sum(durations.iter().copied());
        let workdir = pypath::PyPath::new(&show(workdir));
        let video_script = workdir.join("video.txt");
        fs::write(&video_script, concat_script(&video)).map_err(|error| WorkerError::os(&error, &video_script))?;
        let video_text = show(&video_script);
        let mut middle: Vec<&str> = vec!["-f", "concat", "-safe", "0", "-i", &video_text];
        let audio_script = workdir.join("audio.txt");
        let audio_text = show(&audio_script);
        if has_audio {
            fs::write(&audio_script, concat_script(&audio)).map_err(|error| WorkerError::os(&error, &audio_script))?;
            middle.extend(["-f", "concat", "-safe", "0", "-i", &audio_text]);
        }
        middle.extend(["-map", "0:v"]);
        if has_audio {
            middle.extend(["-map", "1:a"]);
        }
        let target = partial_path(output);
        status.update([
            ("phase", text("merging")),
            ("progress", float(0.0)),
            ("duration", float(expected)),
        ])?;
        let output_muxer = muxer(&suffix(output).to_lowercase()).ok_or_else(|| {
            WorkerError::Uncaught(PyException::new(
                "KeyError",
                pytext::py_repr(&suffix(output).to_lowercase()),
            ))
        })?;
        middle.extend(["-c", "copy", "-f", output_muxer]);
        let command = self.ffmpeg_command(&middle, &target);
        self.run_ffmpeg(&command, expected, status)?;
        self.finish(&target, output, expected, status)
    }

    /// `_assembly_inputs`: the video chunks and audio tracks with the durations they take, and
    /// their bytes.
    #[allow(clippy::type_complexity, reason = "Python's tuple")]
    fn assembly_inputs(
        &self,
        manifest_parts: &Value,
        durations: &[f64],
        transcode_dir: &Path,
        has_audio: bool,
        status: &mut StatusFile,
    ) -> Result<(Vec<(PathBuf, f64)>, Vec<(PathBuf, f64)>, i128), WorkerError> {
        let mut video = Vec::new();
        let mut audio = Vec::new();
        let mut needed = 0;
        let mut signature: Option<Vec<Vec<Value>>> = None;
        for part in iterate(manifest_parts)? {
            let index = py::int(subscript(&part, "index")?)?;
            let chunks = subscript(&part, "chunks")?;
            if !chunks.is_truthy() {
                return Err(WorkerError::Failed(format!("cd{} has no video chunks", index + 1)));
            }
            let chunks = iterate(chunks)?;
            let mut paths = Vec::with_capacity(chunks.len());
            for chunk in &chunks {
                paths.push(chunk_path(transcode_dir, index, py::int(subscript(chunk, "index")?)?));
            }
            let mut lengths = Vec::with_capacity(chunks.len());
            for chunk in &chunks {
                lengths.push(py::float(subscript(chunk, "duration")?)?);
            }
            let part_duration = *py_index(durations, index)?;
            // The part's last chunk ends where the original part ends.
            let rest = part_duration - py::sum(lengths[..lengths.len() - 1].iter().copied());
            if rest > 0.0 {
                let last = lengths.len() - 1;
                lengths[last] = rest;
            }
            let tracks = if has_audio {
                vec![audio_path(transcode_dir, index)]
            } else {
                Vec::new()
            };
            for path in paths.iter().chain(&tracks) {
                match fs::metadata(path) {
                    Ok(meta) => needed += i128::from(meta.len()),
                    Err(_) => {
                        return Err(WorkerError::Failed(format!(
                            "{} of cd{} is missing",
                            name(path),
                            index + 1
                        )));
                    }
                }
            }
            video.extend(paths.iter().cloned().zip(lengths));
            audio.extend(tracks.into_iter().map(|track| (track, part_duration)));
            // The parts must agree on the encoded stream, as a stream copy needs.
            let found = self.probe(&paths[0])?.signature;
            if let Some(previous) = &signature
                && !same_signature(&found, previous)
            {
                return Err(WorkerError::Failed(format!(
                    "the video chunks of cd{} differ from those of the first part",
                    index + 1
                )));
            }
            signature = Some(found);
            #[allow(clippy::cast_possible_wrap, reason = "a handful of parts")]
            status.update_lazily([(
                "progress",
                float(py::truediv(index + 1, durations.len().max(1) as i128)),
            )])?;
        }
        Ok((video, audio, needed))
    }

    /// `_finish`: check the written merge, hash it and move it into place.
    fn finish(
        &self,
        target: &Path,
        output: &Path,
        expected: f64,
        status: &mut StatusFile,
    ) -> Result<Dict, WorkerError> {
        status.update([("phase", text("verifying")), ("progress", float(1.0))])?;
        let merged = self.probe(target)?;
        let tolerance = DURATION_TOLERANCE_SECONDS.max(expected * DURATION_TOLERANCE_SHARE);
        if (merged.duration - expected).abs() > tolerance {
            return Err(WorkerError::Failed(format!(
                "merged duration {}s does not match the parts ({}s)",
                py::fixed(merged.duration, 1),
                py::fixed(expected, 1)
            )));
        }
        let size = i128::from(
            fs::metadata(target)
                .map_err(|error| WorkerError::os(&error, target))?
                .len(),
        );
        let max = self.settings.max_upload_bytes;
        if size > max {
            return Err(WorkerError::Refused(format!(
                "the re-encoded file came to {} GB, still more than the {} GB 115 accepts in one file",
                py::fixed(py::truediv(size, 1_000_000_000), 1),
                py::floordiv(max, 1_000_000_000)
            )));
        }
        status.update([
            ("phase", text("hashing")),
            ("progress", float(0.0)),
            ("size", int(size)),
        ])?;
        let digest = sha1_of(target, |done| {
            let progress = if size == 0 { 1.0 } else { py::truediv(done, size) };
            status.update_lazily([("progress", float(progress))])
        })?;
        fs::rename(target, output).map_err(|error| WorkerError::os2(&error, target, output))?;
        let mut result = Dict::new();
        result.insert("size", int(size));
        result.insert("sha1", Value::Str(digest));
        result.insert("duration", float(merged.duration));
        Ok(result)
    }
}

/// `sha1_of`: the upper-case SHA-1 of a file, read in 8 MiB blocks (115 identifies files by
/// SHA-1, so that is what is compared).
pub fn sha1_of(
    path: &Path,
    mut on_progress: impl FnMut(i128) -> Result<(), WorkerError>,
) -> Result<String, WorkerError> {
    let mut handle = fs::File::open(path).map_err(|error| WorkerError::os(&error, path))?;
    let mut digest = Sha1::new();
    let mut buffer = vec![0_u8; HASH_CHUNK_BYTES];
    let mut done: i128 = 0;
    loop {
        // `read(n)` on a buffered file fills the block unless the file ends.
        let mut filled = 0;
        while filled < buffer.len() {
            match handle.read(&mut buffer[filled..]) {
                Ok(0) => break,
                Ok(count) => filled += count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(WorkerError::Failed(py::os_error(&error, None, None))),
            }
        }
        if filled == 0 {
            break;
        }
        digest.update(&buffer[..filled]);
        done += i128::try_from(filled).unwrap_or(i128::MAX);
        on_progress(done)?;
    }
    Ok(hex_upper(&digest.finalize()))
}

fn hex_upper(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02X}");
            out
        })
}

/// `Path.mkdir(exist_ok=True)` without parents.
fn mkdir_exist_ok(path: &Path) -> Result<(), WorkerError> {
    match fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(_) if path.is_dir() => Ok(()),
        Err(error) => Err(WorkerError::os(&error, path)),
    }
}

/// `json.loads(bytes)`: the encoding detected as Python does, then parsed.
fn parse_json_bytes(bytes: &[u8]) -> Option<Value> {
    let decoded = match pyjson::detect_json_encoding(bytes) {
        pyjson::JsonEncoding::Utf8(raw) => std::str::from_utf8(raw).ok()?.to_owned(),
        pyjson::JsonEncoding::Decoded(text) => text?,
    };
    pyjson::loads(&decoded).ok()
}

/// `value[key]` for a string key, with Python's errors.
fn subscript<'a>(value: &'a Value, key: &str) -> Result<&'a Value, PyException> {
    match value {
        Value::Dict(dict) => dict
            .get(key)
            .ok_or_else(|| PyException::new("KeyError", pytext::py_repr(key))),
        Value::List(_) => Err(PyException::new(
            "TypeError",
            "list indices must be integers or slices, not str",
        )),
        Value::Str(_) => Err(PyException::new(
            "TypeError",
            "string indices must be integers, not 'str'",
        )),
        other => Err(PyException::new(
            "TypeError",
            format!("'{}' object is not subscriptable", py::type_name(other)),
        )),
    }
}

/// `len(value)`.
fn py_len(value: &Value) -> Result<usize, PyException> {
    match value {
        Value::Str(text) => Ok(text.chars().count()),
        Value::List(items) => Ok(items.len()),
        Value::Dict(dict) => Ok(dict.len()),
        other => Err(PyException::new(
            "TypeError",
            format!("object of type '{}' has no len()", py::type_name(other)),
        )),
    }
}

/// `for item in value`: a list's items, a dict's keys, a string's characters.
fn iterate(value: &Value) -> Result<Vec<Value>, PyException> {
    match value {
        Value::List(items) => Ok(items.clone()),
        Value::Dict(dict) => Ok(dict
            .iter()
            .map(|(key, _)| key.as_str().map_or(Value::Null, |key| Value::Str(key.to_owned())))
            .collect()),
        Value::Str(text) => Ok(text.chars().map(|ch| Value::Str(ch.to_string())).collect()),
        other => Err(PyException::new(
            "TypeError",
            format!("'{}' object is not iterable", py::type_name(other)),
        )),
    }
}

/// `items[index]` with Python's negative indexes.
fn py_index<T>(items: &[T], index: i128) -> Result<&T, PyException> {
    let length = i128::try_from(items.len()).unwrap_or(i128::MAX);
    let position = if index < 0 { index + length } else { index };
    usize::try_from(position)
        .ok()
        .and_then(|position| items.get(position))
        .ok_or_else(|| PyException::new("IndexError", "list index out of range"))
}

/// `int(text)` for a string `isdigit()` accepted: superscripts pass that check and fail here.
fn int_of_digits(text: &str) -> Result<i128, WorkerError> {
    pytext::py_int_decimal(text).ok_or_else(|| {
        WorkerError::Uncaught(PyException::new(
            "ValueError",
            format!("invalid literal for int() with base 10: {}", pytext::py_repr(text)),
        ))
    })
}

/// `info.get('streams', [])` iterated: each stream must answer `.get`, and its `codec_type`
/// is looked up in a set, which an unhashable value cannot be.
fn streams(value: Option<&Value>) -> Result<Vec<Dict>, WorkerError> {
    let items = match value {
        None => Vec::new(),
        Some(value) => iterate(value)?,
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let Value::Dict(stream) = item else {
            return Err(PyException::new(
                "AttributeError",
                format!("'{}' object has no attribute 'get'", py::type_name(&item)),
            )
            .into());
        };
        if let Some(kind @ (Value::List(_) | Value::Dict(_))) = stream.get("codec_type") {
            return Err(PyException::new("TypeError", format!("unhashable type: '{}'", py::type_name(kind))).into());
        }
        out.push(stream);
    }
    Ok(out)
}

/// `_keep_tail`: read `stream` to its end, keeping only its last non-blank lines.
fn keep_tail(stream: Box<dyn io::Read + Send>, tail: &Mutex<VecDeque<String>>) {
    let mut lines = exec::Lines::new(stream);
    while let Ok(Some(line)) = lines.next_line() {
        if pytext::py_strip(&line).is_empty() {
            continue;
        }
        let kept = line.trim_end_matches(pytext::is_py_space).to_owned();
        let mut tail = tail.lock().unwrap_or_else(PoisonError::into_inner);
        if tail.len() == STDERR_TAIL_LINES {
            tail.pop_front();
        }
        tail.push_back(kept);
    }
}

/// Read ffmpeg's `-progress` output to its end and report it; the return code once it has
/// ended (`None` when it could not be read).
fn follow_progress(
    stdout: Box<dyn io::Read + Send>,
    duration: f64,
    status: &mut StatusFile,
    watch: &StallWatch,
    process: &dyn Process,
) -> Result<Option<i64>, WorkerError> {
    let mut lines = exec::Lines::new(stdout);
    // ffmpeg keeps printing progress blocks while stuck, so only a change counts as progress.
    let mut position = (String::new(), String::new());
    while let Some(line) = lines
        .next_line()
        .map_err(|error| WorkerError::Failed(py::os_error(&error, None, None)))?
    {
        let line = pytext::py_strip(&line);
        let (key, value) = line.split_once('=').unwrap_or((line, ""));
        if key == "out_time_us" && pytext::py_isdigit(value) {
            if value != position.0 {
                value.clone_into(&mut position.0);
                watch.advanced();
            }
            if duration > 0.0 {
                let micros = int_of_digits(value)?;
                let progress = py::min(py::truediv(micros, 1_000_000) / duration, 1.0);
                status.update_lazily([("progress", float(progress))])?;
            }
        } else if key == "total_size" && pytext::py_isdigit(value) {
            if value != position.1 {
                value.clone_into(&mut position.1);
                watch.advanced();
            }
            status.update_lazily([("written_bytes", int(int_of_digits(value)?))])?;
        }
    }
    Ok(wait(process))
}

/// `Popen.wait()`, polling so the stall watch can still kill a process that closed its output.
/// Kill a process that is still running and reap it.
fn stop(process: &dyn Process) {
    if !matches!(process.try_wait(), Ok(Some(_))) {
        process.kill();
    }
    let _ = wait(process);
}

/// `RuntimeError: can't start new thread`, which Python's worker does not catch.
fn cannot_start_thread(_error: &io::Error) -> WorkerError {
    WorkerError::Uncaught(PyException::new("RuntimeError", "can't start new thread"))
}

fn wait(process: &dyn Process) -> Option<i64> {
    loop {
        match process.try_wait() {
            Ok(Some(code)) => return Some(code),
            Ok(None) => std::thread::sleep(Duration::from_millis(10)),
            Err(_) => return None,
        }
    }
}

/// `_StallWatch`: kills the process once `advanced` has not been called for `stall_seconds`.
struct StallWatch {
    last: Arc<Mutex<Instant>>,
    fired: Arc<AtomicBool>,
    done: Option<mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl StallWatch {
    fn start(process: Arc<dyn Process>, stall_seconds: f64) -> io::Result<Self> {
        let last = Arc::new(Mutex::new(Instant::now()));
        let fired = Arc::new(AtomicBool::new(false));
        let (done, stopped) = mpsc::channel::<()>();
        let interval = Duration::from_secs_f64((stall_seconds / 10.0).clamp(0.05, 30.0));
        let thread = {
            let last = Arc::clone(&last);
            let fired = Arc::clone(&fired);
            std::thread::Builder::new()
                .name("ffmpeg-stall-watch".to_owned())
                .spawn(move || {
                    // The sender is dropped to stop: the wait then ends with `Disconnected`.
                    while let Err(mpsc::RecvTimeoutError::Timeout) = stopped.recv_timeout(interval) {
                        let since = last.lock().unwrap_or_else(PoisonError::into_inner).elapsed();
                        if since.as_secs_f64() > stall_seconds {
                            fired.store(true, Ordering::SeqCst);
                            process.kill();
                            return;
                        }
                    }
                })?
        };
        Ok(Self {
            last,
            fired,
            done: Some(done),
            thread: Some(thread),
        })
    }

    fn advanced(&self) {
        *self.last.lock().unwrap_or_else(PoisonError::into_inner) = Instant::now();
    }

    /// Stop watching and wait for the watch to end, so `fired` is final.
    fn stop(&mut self) {
        self.done.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }
}

impl Drop for StallWatch {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The `merge-worker` command's arguments, as `__main__.py` passes them to `worker.run`.
#[derive(Debug, Clone, PartialEq)]
pub struct Command {
    pub parts: Vec<String>,
    pub output: String,
    pub status: String,
    pub reserve_gib: f64,
    pub home_prepare: Option<String>,
    pub home_assemble: Option<String>,
}

/// What `run` reads from its surroundings: `TMPDIR` and the cgroup's memory limit.
#[derive(Debug, Clone)]
pub struct Environment {
    /// `os.environ.get('TMPDIR', '/tmp')`.
    pub tmpdir: String,
    /// `CGROUP_MEMORY_MAX`.
    pub cgroup_memory_max: PathBuf,
}

impl Environment {
    pub fn from_process() -> Self {
        Self {
            tmpdir: std::env::var_os("TMPDIR")
                .map_or_else(|| "/tmp".to_owned(), |value| value.to_string_lossy().into_owned()),
            cgroup_memory_max: PathBuf::from(CGROUP_MEMORY_MAX),
        }
    }
}

/// `run`: the `merge-worker` subcommand; returns the exit code.
///
/// Exits 0 once the merged file is in place, and also when the parts were refused as
/// unmergeable: the status file then says `failed` with `refused` set, which is a finished
/// answer rather than a broken Job. `home_prepare` instead probes the parts and copies their
/// audio for the home transcode worker into that directory; `home_assemble` merges what the
/// worker and that run left there. An error Python does not catch exits 1 without touching the
/// status file, as Python's traceback does.
pub fn run(
    worker: &Worker,
    command: &Command,
    status: &mut StatusFile,
    environment: &Environment,
    stderr: &mut dyn Write,
) -> i32 {
    let output = py_path(&command.output);
    let workdir = py_path(&environment.tmpdir);
    let outcome = (|| {
        #[allow(clippy::cast_possible_truncation, reason = "int(reserve_gib * GIB) truncates")]
        #[allow(clippy::cast_precision_loss, reason = "float * int in Python")]
        let reserve_bytes = py::int_of_float(command.reserve_gib * GIB as f64)?;
        let parts: Vec<PathBuf> = command.parts.iter().map(|part| py_path(part)).collect();
        if let Some(directory) = &command.home_prepare {
            let limit = memory_limit(&environment.cgroup_memory_max)?;
            worker.prepare_home(&parts, &output, status, &py_path(directory), reserve_bytes, limit)
        } else if let Some(directory) = &command.home_assemble {
            worker.assemble_home(&output, status, &py_path(directory), reserve_bytes, &workdir)
        } else {
            let limit = memory_limit(&environment.cgroup_memory_max)?;
            worker.merge(&parts, &output, status, reserve_bytes, &workdir, limit)
        }
    })();
    let handled = match outcome {
        Ok(result) => {
            let mut fields: Fields = vec![
                ("state".to_owned(), text("done")),
                ("phase".to_owned(), text("done")),
                ("progress".to_owned(), float(1.0)),
            ];
            for (key, value) in result.iter() {
                if let Some(key) = key.as_str() {
                    fields.push((key.to_owned(), value.clone()));
                }
            }
            status.update(fields).map(|()| 0)
        }
        Err(WorkerError::Refused(message)) => remove_partial(&output).and_then(|()| {
            status.update([
                ("state", text("failed")),
                ("refused", Value::Bool(true)),
                ("error", Value::Str(message.clone())),
            ])?;
            let _ = writeln!(stderr, "merge refused: {message}");
            Ok(0)
        }),
        Err(WorkerError::Failed(message)) => remove_partial(&output).and_then(|()| {
            status.update([("state", text("failed")), ("error", Value::Str(message.clone()))])?;
            // Also in the Job's log, for a look at a failed Job without the status file.
            let _ = writeln!(stderr, "merge failed: {message}");
            Ok(1)
        }),
        Err(error @ WorkerError::Uncaught(_)) => Err(error),
    };
    match handled {
        Ok(code) => code,
        Err(error) => {
            // Python ends with a traceback whose last line names the exception.
            let line = match &error {
                WorkerError::Uncaught(exception) => format!("{}: {}", exception.class, exception.message),
                WorkerError::Refused(message) => format!("MergeRefusedError: {message}"),
                WorkerError::Failed(message) => format!("OSError: {message}"),
            };
            let _ = writeln!(stderr, "Traceback (most recent call last):\n{line}");
            1
        }
    }
}

/// `partial_path(output).unlink(missing_ok=True)`.
fn remove_partial(output: &Path) -> Result<(), WorkerError> {
    let partial = partial_path(output);
    match fs::remove_file(&partial) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(WorkerError::os(&error, &partial)),
    }
}
