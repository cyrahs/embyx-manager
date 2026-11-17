//! Video file recognition by suffix.
//!
//! Python origin: `src/embyx_manager/core/media.py`.

use std::path::Path;

use crate::pypath;

/// `VIDEO_SUFFIXES`.
pub const VIDEO_SUFFIXES: [&str; 9] = [".mp4", ".mkv", ".avi", ".wmv", ".mov", ".flv", ".m4v", ".ts", ".rmvb"];

/// `has_video_suffix(path)`: `path.suffix.lower()` is one of [`VIDEO_SUFFIXES`].
pub fn has_video_suffix(path: &Path) -> bool {
    let text = path.to_string_lossy();
    let suffix = pypath::suffix(&text).to_lowercase();
    VIDEO_SUFFIXES.contains(&suffix.as_str())
}

/// `is_video(path)`: a regular file (symbolic links followed) with a video suffix.
pub fn is_video(path: &Path) -> bool {
    path.is_file() && has_video_suffix(path)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{VIDEO_SUFFIXES, has_video_suffix, is_video};
    use crate::golden;
    use crate::pyjson::Value;

    #[test]
    fn suffixes_match_the_python_baseline() {
        let document = golden::load("magnet/media.json");
        let cases = golden::cases(&document);
        let mut suffixes: Vec<String> = golden::list(golden::get(&cases[0], "suffixes"))
            .iter()
            .map(golden::string)
            .collect();
        suffixes.sort();
        let mut ours: Vec<String> = VIDEO_SUFFIXES.iter().map(|&suffix| suffix.to_owned()).collect();
        ours.sort();
        assert_eq!(ours, suffixes);
        for case in &cases[1..] {
            let name = golden::string(golden::get(case, "name"));
            let expected = matches!(golden::get(case, "has_video_suffix"), Value::Bool(true));
            assert_eq!(has_video_suffix(Path::new(&name)), expected, "{name:?}");
        }
    }

    #[test]
    fn is_video_needs_a_regular_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("a.MP4");
        std::fs::write(&file, b"x").expect("write");
        let folder = dir.path().join("b.mkv");
        std::fs::create_dir(&folder).expect("mkdir");
        let text = dir.path().join("c.txt");
        std::fs::write(&text, b"x").expect("write");
        assert!(is_video(&file));
        assert!(!is_video(&folder));
        assert!(!is_video(&text));
        assert!(!is_video(&dir.path().join("missing.mp4")));
    }
}
