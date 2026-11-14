//! The `.strm` watcher: file-system events under the mapping source, as the scheduler's
//! mapping loop consumes them.
//!
//! Python origin: `_StrmEventHandler` and `MonitorScheduler._on_fs_event` /
//! `_sync_observer` in `monitor/scheduler.py`, over watchdog 6.0's recursive inotify
//! observer. watchdog reports created, modified, deleted and moved paths; the handler keeps
//! those that are not directories and whose suffix is `.strm` (any case), a move counting as
//! the source deleted and the destination changed. watchdog also synthesizes one event per
//! file inside a directory that is moved into the tree (`generate_sub_created_events`) or
//! within it (`generate_sub_moved_events`), and for the files of a directory created before
//! its own watch was added (`_recursive_simulate`). `notify` reports only the directory, so
//! [`translate`] walks such a directory itself (API-COVERAGE, "notify 8").

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};

use notify::EventKind;
use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
use tokio::time::Instant;

/// The paths waiting for an incremental mapping run, and when the last event came.
#[derive(Debug, Default)]
pub(crate) struct WatchState {
    pub(crate) changed: BTreeSet<PathBuf>,
    pub(crate) deleted: BTreeSet<PathBuf>,
    /// `_fs_event_count`.
    pub(crate) count: u64,
    /// `_last_fs_event`; `None` before the first event (Python's `0.0`, long ago).
    pub(crate) last: Option<Instant>,
}

impl WatchState {
    /// `_on_fs_event(path, deleted)`.
    pub(crate) fn record(&mut self, path: PathBuf, deleted: bool) {
        if deleted {
            self.changed.remove(&path);
            self.deleted.insert(path);
        } else {
            self.deleted.remove(&path);
            self.changed.insert(path);
        }
        self.count += 1;
        self.last = Some(Instant::now());
    }
}

/// `Path(path).suffix.lower() == '.strm'`: the last dot of the name, not a leading one.
/// Only ASCII letters lower-case to `s`, `t`, `r` and `m`, so an ASCII-insensitive compare
/// of the bytes is exact (and keeps names that are not UTF-8).
pub(crate) fn is_strm(path: &Path) -> bool {
    let Some(name) = path.file_name().map(OsStr::as_bytes) else {
        return false;
    };
    match name.iter().rposition(|&byte| byte == b'.') {
        Some(dot) if dot > 0 => name[dot..].eq_ignore_ascii_case(b".strm"),
        _ => false,
    }
}

/// `os.path.isdir` without following a final link, which is what inotify's `IN_ISDIR`
/// says about the entry an event names.
fn is_directory(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_dir())
}

/// The files under `root`, as `os.walk(root)` lists them (links to directories are not
/// entered and, being directories to `os.walk`, not listed as files).
fn walk_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        let mut entries: Vec<_> = entries.filter_map(Result::ok).collect();
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            let is_link = entry.file_type().is_ok_and(|kind| kind.is_symlink());
            if path.is_dir() {
                if !is_link {
                    pending.push(path);
                }
            } else {
                files.push(path);
            }
        }
    }
    files
}

/// One forwarded event: the path and whether it is gone.
pub(crate) type Forwarded = (PathBuf, bool);

/// `_forward(path, is_directory=, deleted=)` for one path.
fn forward(out: &mut Vec<Forwarded>, path: &Path, is_directory: bool, deleted: bool) {
    if is_directory || path.as_os_str().is_empty() || !is_strm(path) {
        return;
    }
    out.push((path.to_path_buf(), deleted));
}

/// The events of a directory that arrived (moved in, or created with files already in it):
/// one "created" per file below it.
fn arrived_directory(out: &mut Vec<Forwarded>, directory: &Path) {
    for file in walk_files(directory) {
        forward(out, &file, false, false);
    }
}

/// The events of a directory moved within the tree: per file below its new place, the old
/// path deleted and the new one changed.
fn moved_directory(out: &mut Vec<Forwarded>, from: &Path, to: &Path) {
    for file in walk_files(to) {
        if let Ok(relative) = file.strip_prefix(to) {
            forward(out, &from.join(relative), false, true);
        }
        forward(out, &file, false, false);
    }
}

/// What the handler forwards for one `notify` event.
pub(crate) fn translate(kind: EventKind, paths: &[PathBuf]) -> Vec<Forwarded> {
    let mut out = Vec::new();
    match kind {
        EventKind::Create(CreateKind::Folder) => {
            for path in paths {
                arrived_directory(&mut out, path);
            }
        }
        EventKind::Create(_) => {
            for path in paths {
                if is_directory(path) {
                    arrived_directory(&mut out, path);
                } else {
                    forward(&mut out, path, false, false);
                }
            }
        }
        EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => {
            if let [from, to] = paths {
                if is_directory(to) {
                    moved_directory(&mut out, from, to);
                } else {
                    forward(&mut out, from, false, true);
                    forward(&mut out, to, false, false);
                }
            }
        }
        // The first half of a move: the entry is gone from here (moved out, or the "from"
        // that a `Both` repeats).
        EventKind::Modify(ModifyKind::Name(RenameMode::From)) => {
            for path in paths {
                forward(&mut out, path, false, true);
            }
        }
        // Moved in (or the "to" that a `Both` repeats): a directory brings its files.
        EventKind::Modify(ModifyKind::Name(_)) => {
            for path in paths {
                if is_directory(path) {
                    arrived_directory(&mut out, path);
                } else if path.exists() {
                    forward(&mut out, path, false, false);
                }
            }
        }
        EventKind::Modify(_) => {
            for path in paths {
                forward(&mut out, path, is_directory(path), false);
            }
        }
        EventKind::Remove(RemoveKind::Folder) | EventKind::Access(_) | EventKind::Any | EventKind::Other => {}
        EventKind::Remove(_) => {
            for path in paths {
                forward(&mut out, path, false, true);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strm_suffix_is_the_last_dot_of_the_name_in_any_case() {
        assert!(is_strm(Path::new("/x/ABC-123.strm")));
        assert!(is_strm(Path::new("/x/ABC-123.STRM")));
        assert!(is_strm(Path::new("/x/a.b.Strm")));
        assert!(!is_strm(Path::new("/x/.strm")));
        assert!(!is_strm(Path::new("/x/ABC-123.strm.part")));
        assert!(!is_strm(Path::new("/x/ABC-123.nfo")));
        assert!(!is_strm(Path::new("")));
    }

    #[test]
    fn a_directory_moved_within_the_tree_moves_each_file() {
        let root = tempfile::tempdir().expect("tempdir");
        let to = root.path().join("new");
        std::fs::create_dir_all(to.join("deep")).expect("mkdir");
        std::fs::write(to.join("A-1.strm"), "x").expect("write");
        std::fs::write(to.join("deep").join("B-2.strm"), "x").expect("write");
        std::fs::write(to.join("A-1.nfo"), "x").expect("write");
        let from = root.path().join("old");

        let events = translate(
            EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
            &[from.clone(), to.clone()],
        );

        assert_eq!(
            events,
            vec![
                (from.join("A-1.strm"), true),
                (to.join("A-1.strm"), false),
                (from.join("deep").join("B-2.strm"), true),
                (to.join("deep").join("B-2.strm"), false),
            ]
        );
    }

    #[test]
    fn a_directory_moved_in_reports_its_files_and_a_removed_one_nothing() {
        let root = tempfile::tempdir().expect("tempdir");
        let arrived = root.path().join("ABC-127");
        std::fs::create_dir(&arrived).expect("mkdir");
        std::fs::write(arrived.join("ABC-127.strm"), "x").expect("write");

        let events = translate(
            EventKind::Modify(ModifyKind::Name(RenameMode::To)),
            std::slice::from_ref(&arrived),
        );
        assert_eq!(events, vec![(arrived.join("ABC-127.strm"), false)]);

        let created = translate(EventKind::Create(CreateKind::Folder), std::slice::from_ref(&arrived));
        assert_eq!(created, vec![(arrived.join("ABC-127.strm"), false)]);

        assert!(translate(EventKind::Remove(RemoveKind::Folder), &[arrived]).is_empty());
    }

    #[test]
    fn files_are_created_modified_moved_and_deleted() {
        let root = tempfile::tempdir().expect("tempdir");
        let file = root.path().join("ABC-123.strm");
        std::fs::write(&file, "x").expect("write");
        let other = root.path().join("ABC-124.strm");

        assert_eq!(
            translate(EventKind::Create(CreateKind::File), std::slice::from_ref(&file)),
            vec![(file.clone(), false)]
        );
        assert_eq!(
            translate(
                EventKind::Modify(ModifyKind::Data(notify::event::DataChange::Any)),
                std::slice::from_ref(&file)
            ),
            vec![(file.clone(), false)]
        );
        assert_eq!(
            translate(
                EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
                &[other.clone(), file.clone()]
            ),
            vec![(other.clone(), true), (file.clone(), false)]
        );
        assert_eq!(
            translate(
                EventKind::Modify(ModifyKind::Name(RenameMode::From)),
                std::slice::from_ref(&other)
            ),
            vec![(other.clone(), true)]
        );
        assert_eq!(
            translate(EventKind::Remove(RemoveKind::File), std::slice::from_ref(&other)),
            vec![(other, true)]
        );
        // A modified directory, and a file that is not a .strm, are not forwarded.
        assert!(
            translate(
                EventKind::Modify(ModifyKind::Metadata(notify::event::MetadataKind::Any)),
                &[root.path().to_path_buf()]
            )
            .is_empty()
        );
        assert!(translate(EventKind::Create(CreateKind::File), &[root.path().join("x.nfo")]).is_empty());
    }

    #[test]
    fn the_watch_state_keeps_the_latest_verdict_per_path() {
        let mut state = WatchState::default();
        state.record(PathBuf::from("/a.strm"), false);
        state.record(PathBuf::from("/a.strm"), true);
        state.record(PathBuf::from("/b.strm"), true);
        state.record(PathBuf::from("/b.strm"), false);
        assert_eq!(state.changed, BTreeSet::from([PathBuf::from("/b.strm")]));
        assert_eq!(state.deleted, BTreeSet::from([PathBuf::from("/a.strm")]));
        assert_eq!(state.count, 4);
        assert!(state.last.is_some());
    }
}
