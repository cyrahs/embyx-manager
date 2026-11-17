//! The paths and files of a home transcode that the merge worker reads: the chunks the home
//! worker uploaded, the audio the prepare run copied, the concat scripts that line them up.
//!
//! Python origin: the helpers of `src/embyx_manager/merge/transcode.py` that `worker.py`
//! imports (`chunk_path`, `audio_path`, `audio_status_path`, `concat_script`, `read_json`),
//! plus `transcode_dir`. The rest of that module (worker presence, the chunk repository, the
//! audio Job's name) is WP14's.

use std::path::{Path, PathBuf};

use embyx_core::pyjson::{self, Dict, Value};

/// `transcode_dir`: where one task's chunks, copied audio and audio Job status live.
pub fn transcode_dir(work_dir: &Path, task_id: i64) -> PathBuf {
    work_dir.join(".transcode").join(task_id.to_string())
}

/// `chunk_path`: `cd01/chunk-00000.mkv` under the task's directory.
pub fn chunk_path(directory: &Path, part_index: i128, chunk_index: i128) -> PathBuf {
    directory
        .join(format!("cd{:02}", part_index + 1))
        .join(format!("chunk-{chunk_index:05}.mkv"))
}

/// `audio_path`: `audio/cd01.mka` under the task's directory.
pub fn audio_path(directory: &Path, part_index: i128) -> PathBuf {
    directory.join("audio").join(format!("cd{:02}.mka", part_index + 1))
}

/// `audio_status_path`: the prepare run's status file.
pub fn audio_status_path(directory: &Path) -> PathBuf {
    directory.join("audio.json")
}

/// `concat_script`: a concat-demuxer script whose files each take exactly the given duration.
///
/// The demuxer starts each file where the previous one's stated duration ends, so the video
/// chunks and the copied audio line up part by part as the originals did, however long each
/// stream of a part actually runs.
pub fn concat_script(entries: &[(PathBuf, f64)]) -> String {
    let mut lines = Vec::with_capacity(entries.len() * 2);
    for (path, duration) in entries {
        let quoted = path.to_string_lossy().replace('\'', "'\\''");
        lines.push(format!("file '{quoted}'"));
        lines.push(format!("duration {}", crate::py::fixed(*duration, 6)));
    }
    lines.join("\n") + "\n"
}

/// `read_json`: a JSON object from a file, or `None` when it is missing, unreadable, not JSON
/// or not an object.
pub fn read_json(path: &Path) -> Option<Dict> {
    let bytes = std::fs::read(path).ok()?;
    let text = std::str::from_utf8(&bytes).ok()?;
    match pyjson::loads(text).ok()? {
        Value::Dict(dict) => Some(dict),
        _ => None,
    }
}

/// `WORKER_TIMEOUT`: how long after its last report the worker counts as gone.
pub const WORKER_TIMEOUT_SECONDS: i64 = 90;
/// `CHUNK_SECONDS`: the length the worker cuts the video into; each chunk starts on a keyframe.
pub const CHUNK_SECONDS: i64 = 60;
/// `MAX_CHUNK_BYTES`: a chunk never comes near this; anything bigger is refused unread.
pub const MAX_CHUNK_BYTES: i64 = 2 * 1024 * 1024 * 1024;
/// `CHUNK_GAP_SECONDS`: how far a chunk may start from where the previous one ended.
pub const CHUNK_GAP_SECONDS: f64 = 0.25;
/// `PART_DURATION_TOLERANCE_SECONDS`: how far the chunks of a finished part may fall short of
/// or run past the part.
pub const PART_DURATION_TOLERANCE_SECONDS: f64 = 2.0;

/// `ENCODER`: what the worker hands NVENC, sent with every task so the worker needs no update
/// when they change (home worker protocol, cross-version contract 5).
pub fn encoder() -> Dict {
    let mut encoder = Dict::new();
    encoder.insert("codec", Value::from("av1_nvenc"));
    encoder.insert("preset", Value::from("p7"));
    encoder.insert("tune", Value::from("uhq"));
    encoder.insert("bit_depth", Value::Int(10));
    encoder.insert("multipass", Value::from("fullres"));
    encoder.insert("lookahead", Value::Int(32));
    encoder.insert("spatial_aq", Value::Bool(true));
    encoder.insert("gop_seconds", Value::Int(5));
    encoder
}

/// `Chunk`: one piece of encoded video the worker delivered.
#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    pub part_index: i32,
    pub chunk_index: i32,
    /// Where the chunk starts in its part, in seconds.
    pub start: f64,
    pub duration: f64,
    pub frames: i32,
    pub bytes: i64,
    pub sha256: String,
}

impl Chunk {
    /// `Chunk.end`.
    pub fn end(&self) -> f64 {
        self.start + self.duration
    }
}

/// `audio_job_name(task_id, avid)`: the merge Job's name with `embyx-audio-` in front.
#[cfg(feature = "db")]
pub fn audio_job_name(task_id: i64, avid: &str) -> String {
    use embyx_clients::kube::{MAX_JOB_NAME, job_name};
    let name = job_name(task_id, avid).replacen("embyx-merge-", "embyx-audio-", 1);
    // `[:MAX_JOB_NAME]` counts characters; the name is ASCII.
    let cut: String = name.chars().take(MAX_JOB_NAME).collect();
    cut.trim_end_matches('-').to_owned()
}

#[cfg(feature = "db")]
pub use self::bookkeeping::*;

#[cfg(feature = "db")]
mod bookkeeping;
