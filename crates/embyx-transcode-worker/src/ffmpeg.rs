//! The ffmpeg and ffprobe command lines the worker builds, and what it reads back.
//!
//! Python origin: `uses_nvenc`, `decoder_args`, `encoder_args`, `encode_command`,
//! `first_pts`, `count_frames` and `probe_fps` of `src/embyx_manager/merge/home_worker.py`.
//! Every argument list is pinned by `goldens/merge/home_worker.json` (cross-version contract
//! 5: the encoder settings the cluster sends turn into the same ffmpeg arguments in both
//! builds).

use std::ffi::OsString;
use std::path::Path;
use std::time::Duration;

use embyx_core::pyjson::{self, Dict, Value};
use embyx_core::pytext;

use crate::config::WorkerConfig;
use crate::process::{Captured, RunError, Runner};
use crate::py::{self, PyError};

/// `probe_fps`'s `timeout=600`.
pub const PROBE_TIMEOUT: Duration = Duration::from_mins(10);

fn os(items: impl IntoIterator<Item = impl Into<OsString>>) -> Vec<OsString> {
    items.into_iter().map(Into::into).collect()
}

/// `uses_nvenc(encoder)`.
pub fn uses_nvenc(encoder: &Dict) -> bool {
    encoder
        .get("codec")
        .map_or_else(String::new, py::str_of)
        .ends_with("_nvenc")
}

/// `decoder_args(encoder)`: decode on the GPU when encoding there, so frames never leave it.
pub fn decoder_args(encoder: &Dict) -> Vec<OsString> {
    if uses_nvenc(encoder) {
        os(["-hwaccel", "cuda", "-hwaccel_output_format", "cuda"])
    } else {
        Vec::new()
    }
}

/// Python's `iter(value)` for the `extra` list: a list's items, a string's characters, a
/// dict's keys.
fn items(value: &Value) -> Result<Vec<Value>, PyError> {
    match value {
        Value::List(items) => Ok(items.clone()),
        Value::Str(text) => Ok(text.chars().map(|ch| Value::Str(ch.to_string())).collect()),
        Value::Dict(dict) => Ok(dict
            .iter()
            .map(|(key, _)| Value::Str(key.as_str().unwrap_or_default().to_owned()))
            .collect()),
        other => Err(PyError::new(
            "TypeError",
            format!("'{}' object is not iterable", py::type_name(other)),
        )),
    }
}

fn get_or<'a>(encoder: &'a Dict, key: &str, default: &'a Value) -> &'a Value {
    encoder.get(key).unwrap_or(default)
}

/// `encoder_args(encoder, video_bits, fps)`: the video filter and encoder options.
pub fn encoder_args(encoder: &Dict, video_bits: i128, fps: f64) -> Result<Vec<OsString>, PyError> {
    let codec = py::str_of(encoder.get("codec").ok_or_else(|| PyError::key("codec"))?);
    let gop_seconds = py::float(get_or(encoder, "gop_seconds", &Value::Int(5)))?;
    let gop = py::round(gop_seconds * fps)?.max(1);
    let extra = || -> Result<Vec<String>, PyError> {
        Ok(items(get_or(encoder, "extra", &Value::List(Vec::new())))?
            .iter()
            .map(py::str_of)
            .collect())
    };
    if !uses_nvenc(encoder) {
        let mut args = vec![
            "-c:v".to_owned(),
            codec,
            "-b:v".to_owned(),
            video_bits.to_string(),
            "-g".to_owned(),
            gop.to_string(),
        ];
        args.extend(extra()?);
        return Ok(os(args));
    }
    let mut args: Vec<String> = Vec::new();
    if py::int(get_or(encoder, "bit_depth", &Value::Int(8)))? == 10 {
        args.extend(["-vf".to_owned(), "scale_cuda=format=p010le".to_owned()]);
    }
    args.extend([
        "-c:v".to_owned(),
        codec,
        "-preset".to_owned(),
        py::str_of(get_or(encoder, "preset", &Value::Str("p7".to_owned()))),
        "-tune".to_owned(),
        py::str_of(get_or(encoder, "tune", &Value::Str("hq".to_owned()))),
        "-rc".to_owned(),
        "vbr".to_owned(),
        "-b:v".to_owned(),
        video_bits.to_string(),
        "-maxrate".to_owned(),
        (2 * video_bits).to_string(),
        "-bufsize".to_owned(),
        (2 * video_bits).to_string(),
    ]);
    if let Some(multipass) = encoder.get("multipass").filter(|value| py::truthy(value)) {
        args.extend(["-multipass".to_owned(), py::str_of(multipass)]);
    }
    if let Some(lookahead) = encoder.get("lookahead").filter(|value| py::truthy(value)) {
        args.extend(["-rc-lookahead".to_owned(), py::str_of(lookahead)]);
    }
    if encoder.get("spatial_aq").is_some_and(py::truthy) {
        args.extend(["-spatial-aq".to_owned(), "1".to_owned()]);
    }
    args.extend([
        "-g".to_owned(),
        gop.to_string(),
        "-forced-idr".to_owned(),
        "1".to_owned(),
    ]);
    args.extend(extra()?);
    Ok(os(args))
}

/// `['-headers', f'Authorization: Bearer {token}\r\n']` when there is a token.
fn token_headers(config: &WorkerConfig) -> Vec<OsString> {
    if config.token.is_empty() {
        Vec::new()
    } else {
        os([
            "-headers".to_owned(),
            format!("Authorization: Bearer {}\r\n", config.token),
        ])
    }
}

/// What one ffmpeg run reads and writes (`encode_command`'s keyword arguments).
#[derive(Debug, Clone)]
pub struct EncodeRun<'a> {
    pub url: &'a str,
    pub encoder: &'a Dict,
    pub video_bits: i128,
    pub fps: f64,
    pub seek: f64,
    pub chunk_seconds: i128,
    pub first_index: i128,
    pub directory: &'a Path,
    pub listing: &'a Path,
}

/// `encode_command(config, ...)`: ffmpeg reading `url` from `seek` on, writing chunks
/// `first_index` onwards into `directory`.
pub fn encode_command(config: &WorkerConfig, run: &EncodeRun<'_>) -> Result<Vec<OsString>, PyError> {
    let network = if run.url.starts_with("http") {
        os([
            "-reconnect",
            "1",
            "-reconnect_on_network_error",
            "1",
            "-reconnect_delay_max",
            "60",
        ])
    } else {
        Vec::new()
    };
    let mut argv: Vec<OsString> = os([
        config.ffmpeg.as_str(),
        "-hide_banner",
        "-nostdin",
        "-loglevel",
        "error",
        "-y",
    ]);
    argv.extend(decoder_args(run.encoder));
    argv.extend(token_headers(config));
    argv.extend(network);
    if run.seek > 0.0 {
        argv.extend(os(["-ss".to_owned(), py::fixed(run.seek, 6)]));
    }
    argv.extend(os(["-i", run.url, "-map", "0:v:0", "-an", "-sn", "-dn"]));
    argv.extend(encoder_args(run.encoder, run.video_bits, run.fps)?);
    argv.extend(os([
        "-force_key_frames".to_owned(),
        format!("expr:gte(t,n_forced*{})", run.chunk_seconds),
        "-f".to_owned(),
        "segment".to_owned(),
        "-segment_time".to_owned(),
        run.chunk_seconds.to_string(),
        "-segment_format".to_owned(),
        "matroska".to_owned(),
        "-reset_timestamps".to_owned(),
        "1".to_owned(),
        "-segment_start_number".to_owned(),
        run.first_index.to_string(),
        "-segment_list".to_owned(),
    ]));
    argv.push(run.listing.as_os_str().to_owned());
    argv.extend(os(["-segment_list_type", "csv", "-progress", "pipe:1", "-nostats"]));
    argv.push(run.directory.join("chunk-%05d.mkv").into_os_string());
    Ok(argv)
}

/// Why reading a chunk's or a part's facts with ffprobe failed.
#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    /// `RuntimeError`: ffprobe failed or said nothing usable.
    #[error("{0}")]
    Runtime(String),
    /// `ValueError` (a frame count `int()` refuses, an answer that is not JSON).
    #[error("{0}")]
    Value(String),
    /// `subprocess.TimeoutExpired`.
    #[error("{0}")]
    Timeout(String),
    /// `OSError` starting ffprobe.
    #[error("{0}")]
    Os(std::io::Error),
}

/// `completed.stderr.strip()[-200:]`.
fn stderr_tail(captured: &Captured) -> String {
    let stripped = pytext::py_strip(&captured.stderr);
    let count = stripped.chars().count();
    stripped.chars().skip(count.saturating_sub(200)).collect()
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Python's `str.splitlines()`.
pub(crate) fn splitlines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        let Some(at) = rest.find(|ch: char| {
            matches!(
                ch,
                '\n' | '\r' | '\u{0b}' | '\u{0c}' | '\u{1c}' | '\u{1d}' | '\u{1e}' | '\u{85}' | '\u{2028}' | '\u{2029}'
            )
        }) else {
            lines.push(rest);
            break;
        };
        lines.push(&rest[..at]);
        let width = if rest[at..].starts_with("\r\n") {
            2
        } else {
            rest[at..].chars().next().map_or(1, char::len_utf8)
        };
        rest = &rest[at + width..];
    }
    lines
}

fn run(runner: &dyn Runner, argv: &[OsString], timeout: Option<Duration>) -> Result<Captured, ProbeError> {
    runner.run(argv, timeout).map_err(|error| match error {
        RunError::Io(error) => ProbeError::Os(error),
        RunError::Timeout(after) => ProbeError::Timeout(format!(
            "Command '{}' timed out after {} seconds",
            argv_repr(argv),
            after.as_secs()
        )),
    })
}

/// `repr(argv)` with the bearer token left out: Python prints it whole (D168).
fn argv_repr(argv: &[OsString]) -> String {
    let items: Vec<String> = argv
        .iter()
        .map(|item| {
            let text = item.to_string_lossy();
            if text.starts_with("Authorization: Bearer ") {
                pytext::py_repr("Authorization: Bearer <redacted>\r\n")
            } else {
                pytext::py_repr(&text)
            }
        })
        .collect();
    format!("[{}]", items.join(", "))
}

/// `first_pts(path, ffprobe)`: the earliest presentation time among a chunk's first packets.
///
/// The muxer shifts the whole run by the encoder's reorder delay, so the times in the segment
/// list are off by exactly this much from the first chunk on.
pub fn first_pts(runner: &dyn Runner, path: &Path, ffprobe: &str) -> Result<f64, ProbeError> {
    let argv = first_pts_command(path, ffprobe);
    let completed = run(runner, &argv, None)?;
    let times: Vec<f64> = splitlines(&completed.stdout)
        .into_iter()
        .filter_map(|line| py::float_from_str(pytext::py_strip(line).trim_end_matches(',')))
        .collect();
    let Some(first) = times.first().copied() else {
        return Err(ProbeError::Runtime(format!(
            "ffprobe cannot read the timestamps of {}: {}",
            file_name(path),
            stderr_tail(&completed)
        )));
    };
    if completed.returncode != 0 {
        return Err(ProbeError::Runtime(format!(
            "ffprobe cannot read the timestamps of {}: {}",
            file_name(path),
            stderr_tail(&completed)
        )));
    }
    Ok(times.into_iter().skip(1).fold(first, py::min))
}

/// The argument list of `first_pts`.
pub fn first_pts_command(path: &Path, ffprobe: &str) -> Vec<OsString> {
    let mut argv = os([
        ffprobe,
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-read_intervals",
        "%+#32",
        "-show_entries",
        "packet=pts_time",
        "-of",
        "csv=p=0",
    ]);
    argv.push(path.as_os_str().to_owned());
    argv
}

/// `count_frames(path, ffprobe)`.
pub fn count_frames(runner: &dyn Runner, path: &Path, ffprobe: &str) -> Result<i128, ProbeError> {
    let argv = count_frames_command(path, ffprobe);
    let completed = run(runner, &argv, None)?;
    let stripped = pytext::py_strip(&completed.stdout);
    let value = stripped.split(',').next().unwrap_or_default();
    if completed.returncode != 0 || !pytext::py_isdigit(value) {
        return Err(ProbeError::Runtime(format!(
            "ffprobe cannot count the frames of {}: {}",
            file_name(path),
            stderr_tail(&completed)
        )));
    }
    pytext::py_int(value).map_err(ProbeError::Value)
}

/// The argument list of `count_frames`.
pub fn count_frames_command(path: &Path, ffprobe: &str) -> Vec<OsString> {
    let mut argv = os([
        ffprobe,
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-count_packets",
        "-show_entries",
        "stream=nb_read_packets",
        "-of",
        "csv=p=0",
    ]);
    argv.push(path.as_os_str().to_owned());
    argv
}

/// The argument list of `probe_fps`.
pub fn probe_fps_command(url: &str, config: &WorkerConfig) -> Vec<OsString> {
    let mut argv = os([config.ffprobe.as_str(), "-v", "error"]);
    argv.extend(token_headers(config));
    argv.extend(os([
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=avg_frame_rate,r_frame_rate",
        "-of",
        "json",
        url,
    ]));
    argv
}

/// `probe_fps(url, config)`: the part's frame rate.
pub fn probe_fps(runner: &dyn Runner, url: &str, config: &WorkerConfig) -> Result<f64, ProbeError> {
    let argv = probe_fps_command(url, config);
    let completed = run(runner, &argv, Some(PROBE_TIMEOUT))?;
    let text = if completed.stdout.is_empty() {
        "{}"
    } else {
        completed.stdout.as_str()
    };
    let document = pyjson::loads(text).map_err(|error| ProbeError::Value(error.python_message(text)))?;
    let Value::Dict(document) = document else {
        return Err(ProbeError::Value(format!(
            "'{}' object has no attribute 'get'",
            py::type_name(&document)
        )));
    };
    let first = match document.get("streams") {
        Some(Value::List(streams)) if !streams.is_empty() => streams[0].clone(),
        Some(streams) if py::truthy(streams) => {
            return Err(ProbeError::Value(format!(
                "'{}' object is not subscriptable as a list of streams",
                py::type_name(streams)
            )));
        }
        _ => Value::Dict(Dict::new()),
    };
    let Value::Dict(first) = first else {
        return Err(ProbeError::Value(format!(
            "'{}' object has no attribute 'get'",
            py::type_name(&first)
        )));
    };
    for key in ["avg_frame_rate", "r_frame_rate"] {
        let text = first.get(key).map_or_else(|| "0/0".to_owned(), py::str_of);
        let (numerator, denominator) = text.split_once('/').unwrap_or((&text, ""));
        let denominator = if denominator.is_empty() { "1" } else { denominator };
        let (Some(numerator), Some(denominator)) = (py::float_from_str(numerator), py::float_from_str(denominator))
        else {
            continue;
        };
        if denominator == 0.0 {
            continue;
        }
        let rate = numerator / denominator;
        if rate > 0.0 {
            return Ok(rate);
        }
    }
    Err(ProbeError::Runtime(format!(
        "ffprobe found no frame rate: {}",
        stderr_tail(&completed)
    )))
}
