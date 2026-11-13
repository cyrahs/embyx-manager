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
