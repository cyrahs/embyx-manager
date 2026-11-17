//! Local moves on the file system: `_apply_one`, the hard-link fallback, the journal
//! reconciliation and the private quarantine.
//!
//! Python origin: the synchronous methods of `FillActorService` in
//! `src/embyx_manager/fill_actor/service.py` that run on the mutation executor
//! (`_apply_one`, `_apply_hardlink_fallback`, `_rollback_unexpected_link`,
//! `_prepare_destination_parent`, `_reconcile_filesystem`, `_finish_quarantined_reconcile`,
//! `_move_to_quarantine`, `_restore_quarantine_no_replace`, ...), in their order, with their
//! error codes. Every call goes through [`MoveFs`]. A function returns `Err` where Python's
//! raises out of the method (an `OSError` from a check that pathlib does not swallow); the
//! caller turns that into `move_failed` as `_run_move`'s `except Exception` does.

use std::path::{Path, PathBuf};

use sha2::{Digest as _, Sha256};

use crate::fs::{self, EEXIST, EINVAL, ENOSYS, EOPNOTSUPP, EPERM, EXDEV, MoveFs, PyOsError, Strict};
use crate::models::{MoveResult, MoveState};
use crate::records::{CandidateRecord, FileFingerprint, MoveJournalState};

type Result<T> = std::result::Result<T, PyOsError>;

/// `_RENAME_NOREPLACE_UNSUPPORTED`: NFS and friends refuse `renameat2` flags with these.
const RENAME_NOREPLACE_UNSUPPORTED: [i32; 3] = [EINVAL, ENOSYS, EOPNOTSUPP];

fn unsupported(error: &PyOsError) -> bool {
    error
        .errno
        .is_some_and(|code| RENAME_NOREPLACE_UNSUPPORTED.contains(&code))
}

/// `except FileExistsError`.
fn file_exists(error: &PyOsError) -> bool {
    error.errno == Some(EEXIST)
}

/// The file system and the move-in root one local move works against.
pub(crate) struct Local<'a> {
    pub fs: &'a dyn MoveFs,
    pub move_in: &'a Path,
}

/// `MoveResult(candidate_id=..., video_id=..., file_name=record.source.name, ...)`.
pub(crate) fn move_result(record: &CandidateRecord, state: MoveState, error_code: Option<&str>) -> MoveResult {
    MoveResult {
        candidate_id: record.candidate_id.clone(),
        video_id: record.video_id.clone(),
        // A name that is not UTF-8 never reaches a stored plan (the scan refuses it as
        // Python's `model_dump_json` does), so the lossy text is never seen.
        file_name: fs::name(&record.source).to_string_lossy().into_owned(),
        state,
        error_code: error_code.map(str::to_owned),
    }
}

#[allow(clippy::unnecessary_wraps, reason = "the move steps return it as their result")]
fn outcome(record: &CandidateRecord, state: MoveState, code: &str) -> Result<MoveResult> {
    Ok(move_result(record, state, Some(code)))
}

#[allow(clippy::unnecessary_wraps, reason = "the move steps return it as their result")]
fn moved(record: &CandidateRecord) -> Result<MoveResult> {
    Ok(move_result(record, MoveState::Moved, None))
}

/// `_fingerprint(path)`: `path.stat()` as a fingerprint.
pub(crate) fn fingerprint(fs: &dyn MoveFs, path: &Path) -> Result<FileFingerprint> {
    let stat = fs.stat(path, true)?;
    Ok(FileFingerprint {
        device: stat.device,
        inode: stat.inode,
        size: stat.size,
        mtime_ns: stat.mtime_ns,
        ctime_ns: stat.ctime_ns,
    })
}

/// `_matches_linked_identity(path, fingerprint)`: same device, inode, size and mtime (a hard
/// link changes the ctime).
fn matches_linked_identity(fs: &dyn MoveFs, path: &Path, expected: &FileFingerprint) -> Result<bool> {
    let stat = fs.stat(path, true)?;
    Ok(stat.device == expected.device
        && stat.inode == expected.inode
        && stat.size == expected.size
        && stat.mtime_ns == expected.mtime_ns)
}

/// `_matches_fingerprint_if_present(path, fingerprint)`.
fn matches_fingerprint_if_present(fs: &dyn MoveFs, path: &Path, expected: &FileFingerprint) -> bool {
    (|| -> Result<bool> { Ok(!fs::is_symlink(fs, path)? && fingerprint(fs, path)? == *expected) })().unwrap_or(false)
}

/// `_matches_linked_fingerprint_if_present(path, fingerprint)`.
fn matches_linked_fingerprint_if_present(fs: &dyn MoveFs, path: &Path, expected: &FileFingerprint) -> bool {
    (|| -> Result<bool> { Ok(!fs::is_symlink(fs, path)? && matches_linked_identity(fs, path, expected)?) })()
        .unwrap_or(false)
}

/// `_same_inode(first, second)`: both `stat` and agree on device and inode.
fn same_inode(fs: &dyn MoveFs, first: &Path, second: &Path) -> bool {
    match (fs.stat(first, true), fs.stat(second, true)) {
        (Ok(first), Ok(second)) => first.device == second.device && first.inode == second.inode,
        _ => false,
    }
}

/// `_is_safe_segment(value)`: one plain path segment.
pub(crate) fn is_safe_segment(value: &str) -> bool {
    !(value.is_empty() || value == "." || value == ".." || value.contains(['/', '\\', '\0']))
}

/// `_is_within(path, root)`: `path.resolve(strict=False).relative_to(root.resolve(strict=True))`,
/// `False` where that raises `FileNotFoundError` or `ValueError`; any other `OSError` is raised.
pub(crate) fn is_within(fs: &dyn MoveFs, path: &Path, root: &Path) -> Result<bool> {
    let resolved = fs::realpath(fs, path, Strict::No)?;
    let root = match fs::realpath(fs, root, Strict::Yes) {
        Ok(root) => root,
        Err(error) if error.errno == Some(embyx_monitor::fs::ENOENT) => return Ok(false),
        Err(error) => return Err(error),
    };
    Ok(fs::is_relative_to(&resolved, &root))
}

/// `_has_symlink_component(path, root)`: the root or any part between it and the path is a
/// symbolic link (or the path is not under the root at all).
pub(crate) fn has_symlink_component(fs: &dyn MoveFs, path: &Path, root: &Path) -> Result<bool> {
    let Some(relative) = fs::relative_parts(&fs::absolute(fs, path)?, &fs::absolute(fs, root)?) else {
        return Ok(true);
    };
    let mut current = root.to_path_buf();
    if fs::is_symlink(fs, &current)? {
        return Ok(true);
    }
    for part in relative {
        current.push(part);
        if fs::is_symlink(fs, &current)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `_reconcile_quarantine_path(record)`: the private name a move parks a link under while it
/// is resolved, unique per candidate.
pub(crate) fn quarantine_path(move_in: &Path, candidate_id: &str) -> PathBuf {
    let digest = hex::encode(Sha256::digest(candidate_id.as_bytes()));
    move_in.join(format!(".embyx-reconcile-{digest}"))
}

impl Local<'_> {
    fn quarantine(&self, record: &CandidateRecord) -> PathBuf {
        quarantine_path(self.move_in, &record.candidate_id)
    }

    /// `_apply_one(record)`.
    pub(crate) fn apply_one(&self, record: &CandidateRecord) -> Result<MoveResult> {
        let fs = self.fs;
        if has_symlink_component(fs, &record.source, &record.source_root)? {
            return outcome(record, MoveState::InvalidPath, "source_symlink");
        }
        if has_symlink_component(fs, &record.destination, self.move_in)? {
            return outcome(record, MoveState::InvalidPath, "destination_symlink");
        }
        if !fs::is_dir(fs, self.move_in)? {
            return outcome(record, MoveState::InvalidPath, "destination_root_missing");
        }
        if !is_within(fs, &record.source, &record.source_root)? || !is_within(fs, &record.destination, self.move_in)? {
            return outcome(record, MoveState::InvalidPath, "path_outside_root");
        }
        if !fs::is_file(fs, &record.source)? {
            return outcome(record, MoveState::Stale, "source_missing");
        }
        if fingerprint(fs, &record.source)? != record.fingerprint {
            return outcome(record, MoveState::Stale, "source_changed");
        }
        if fs::exists(fs, &record.destination)? {
            return outcome(record, MoveState::Conflict, "destination_exists");
        }
        if !self.prepare_destination_parent(&record.destination)? {
            return outcome(record, MoveState::InvalidPath, "destination_parent_unavailable");
        }
        if has_symlink_component(fs, &record.destination, self.move_in)? {
            return outcome(record, MoveState::InvalidPath, "destination_symlink");
        }
        match fs.rename_noreplace(&record.source, &record.destination) {
            Ok(()) => {}
            Err(error) if file_exists(&error) => return outcome(record, MoveState::Conflict, "destination_exists"),
            Err(error) if unsupported(&error) => return self.apply_hardlink_fallback(record),
            Err(error) => {
                let code = if error.errno == Some(EXDEV) {
                    "cross_device_move"
                } else {
                    "move_failed"
                };
                return outcome(record, MoveState::Failed, code);
            }
        }
        let destination_matches = (|| -> Result<bool> {
            Ok(!fs::is_symlink(fs, &record.destination)?
                && matches_linked_identity(fs, &record.destination, &record.fingerprint)?)
        })()
        .unwrap_or(false);
        if !destination_matches {
            if fs.rename_noreplace(&record.destination, &record.source).is_err() {
                return outcome(record, MoveState::Failed, "move_rollback_failed");
            }
            return outcome(record, MoveState::Stale, "source_changed_during_move");
        }
        moved(record)
    }

    /// `_apply_hardlink_fallback(record, base)`: move with a no-overwrite hard link when NFS
    /// rejects `renameat2` flags.
    fn apply_hardlink_fallback(&self, record: &CandidateRecord) -> Result<MoveResult> {
        let fs = self.fs;
        let quarantine = self.quarantine(record);
        if fs::exists(fs, &quarantine)? {
            return outcome(record, MoveState::Failed, "move_rollback_failed");
        }
        if let Err(error) = fs.link(&record.source, &record.destination) {
            if file_exists(&error) {
                return outcome(record, MoveState::Conflict, "destination_exists");
            }
            let code = match error.errno {
                Some(EXDEV) => "cross_device_move",
                Some(EPERM | EOPNOTSUPP | ENOSYS) => "move_unsupported",
                _ => "move_failed",
            };
            return outcome(record, MoveState::Failed, code);
        }
        let destination_matches = matches_linked_fingerprint_if_present(fs, &record.destination, &record.fingerprint);
        let source_matches = matches_linked_fingerprint_if_present(fs, &record.source, &record.fingerprint);
        if !destination_matches {
            return self.rollback_unexpected_link(record, &quarantine);
        }
        if !fs::exists(fs, &record.source)? || !source_matches {
            return moved(record);
        }
        if self.move_to_quarantine(&record.source, &quarantine).is_err() {
            return outcome(record, MoveState::Failed, "move_rollback_failed");
        }
        self.finish_quarantined_reconcile(record, &quarantine)
    }

    /// `_rollback_unexpected_link(record, quarantine, base)`.
    fn rollback_unexpected_link(&self, record: &CandidateRecord, quarantine: &Path) -> Result<MoveResult> {
        let fs = self.fs;
        if self.move_to_quarantine(&record.destination, quarantine).is_err() {
            return outcome(record, MoveState::Failed, "move_rollback_failed");
        }
        if fs::exists(fs, &record.source)? && same_inode(fs, quarantine, &record.source) {
            if fs.unlink(quarantine).is_err() {
                return outcome(record, MoveState::Failed, "move_rollback_failed");
            }
            return outcome(record, MoveState::Stale, "source_changed_during_move");
        }
        if self
            .restore_quarantine_no_replace(quarantine, &record.destination)
            .is_err()
        {
            return outcome(record, MoveState::Failed, "move_rollback_failed");
        }
        outcome(record, MoveState::Conflict, "destination_changed_during_move")
    }

    /// `_prepare_destination_parent(destination)`: the move-in root itself, or a brand
    /// directory right below it, created if missing.
    fn prepare_destination_parent(&self, destination: &Path) -> Result<bool> {
        let fs = self.fs;
        let parent = destination.parent().unwrap_or(destination);
        if parent == self.move_in {
            return Ok(true);
        }
        if !is_within(fs, parent, self.move_in)? || parent.parent().unwrap_or(parent) != self.move_in {
            return Ok(false);
        }
        if fs::mkdir_exist_ok(fs, parent, 0o755).is_err() {
            return Ok(false);
        }
        Ok(fs::is_dir(fs, parent)? && !fs::is_symlink(fs, parent)?)
    }

    /// `_reconcile_filesystem(record, state)`: finish or undo a move whose journal stopped at
    /// `state`, from what the two paths (and the quarantine) hold now.
    pub(crate) fn reconcile_filesystem(&self, record: &CandidateRecord, state: MoveJournalState) -> Result<MoveResult> {
        let fs = self.fs;
        if has_symlink_component(fs, &record.source, &record.source_root)?
            || has_symlink_component(fs, &record.destination, self.move_in)?
        {
            return outcome(record, MoveState::InvalidPath, "reconcile_symlink");
        }
        if !is_within(fs, &record.source, &record.source_root)? || !is_within(fs, &record.destination, self.move_in)? {
            return outcome(record, MoveState::InvalidPath, "path_outside_root");
        }
        let source_matches = matches_fingerprint_if_present(fs, &record.source, &record.fingerprint);
        let source_matches_linked = matches_linked_fingerprint_if_present(fs, &record.source, &record.fingerprint);
        let destination_matches = matches_linked_fingerprint_if_present(fs, &record.destination, &record.fingerprint);
        let source_exists = fs::exists(fs, &record.source)?;
        let destination_exists = fs::exists(fs, &record.destination)?;
        let quarantine = self.quarantine(record);

        if fs::exists(fs, &quarantine)? {
            return self.finish_quarantined_reconcile(record, &quarantine);
        }
        if source_matches_linked && destination_matches && same_inode(fs, &record.source, &record.destination) {
            if self.move_to_quarantine(&record.source, &quarantine).is_err() {
                return outcome(record, MoveState::Failed, "reconcile_quarantine_failed");
            }
            return self.finish_quarantined_reconcile(record, &quarantine);
        }
        if destination_matches && (!source_exists || !source_matches_linked) {
            return moved(record);
        }
        if source_matches && !destination_exists && state == MoveJournalState::Prepared {
            return self.apply_one(record);
        }
        if state == MoveJournalState::Prepared && destination_exists && !destination_matches {
            if source_exists {
                return outcome(record, MoveState::Failed, "reconcile_rollback_conflict");
            }
            return match fs.rename_noreplace(&record.destination, &record.source) {
                Ok(()) => outcome(record, MoveState::Stale, "source_changed_during_move"),
                Err(error) if file_exists(&error) => outcome(record, MoveState::Failed, "reconcile_rollback_conflict"),
                Err(_) => outcome(record, MoveState::Failed, "reconcile_rollback_failed"),
            };
        }
        if source_exists && !source_matches_linked {
            return outcome(record, MoveState::Stale, "source_changed");
        }
        if destination_exists {
            return outcome(record, MoveState::Conflict, "reconcile_destination_conflict");
        }
        if matches!(state, MoveJournalState::Linked | MoveJournalState::SourceRemoved) {
            return outcome(record, MoveState::Failed, "reconcile_destination_missing");
        }
        outcome(record, MoveState::Stale, "source_missing")
    }

    /// `_finish_quarantined_reconcile(record, quarantine, base)`.
    fn finish_quarantined_reconcile(&self, record: &CandidateRecord, quarantine: &Path) -> Result<MoveResult> {
        let fs = self.fs;
        let quarantine_matches = matches_linked_fingerprint_if_present(fs, quarantine, &record.fingerprint);
        let destination_matches = matches_linked_fingerprint_if_present(fs, &record.destination, &record.fingerprint);
        if !quarantine_matches {
            if !destination_matches {
                return outcome(record, MoveState::Failed, "reconcile_quarantine_conflict");
            }
            if self.restore_quarantine_no_replace(quarantine, &record.source).is_err() {
                return outcome(record, MoveState::Failed, "reconcile_quarantine_conflict");
            }
            return moved(record);
        }
        if destination_matches && same_inode(fs, quarantine, &record.destination) {
            if fs.unlink(quarantine).is_err() {
                return outcome(record, MoveState::Failed, "reconcile_quarantine_cleanup_failed");
            }
            return moved(record);
        }
        if self.restore_quarantine_no_replace(quarantine, &record.source).is_err() {
            return outcome(record, MoveState::Failed, "reconcile_quarantine_conflict");
        }
        outcome(record, MoveState::Stale, "source_changed_during_reconcile")
    }

    /// `_move_to_quarantine(source, quarantine)`: a no-replace rename, or where the file system
    /// refuses the flag, a plain rename after checking the quarantine is free.
    fn move_to_quarantine(&self, source: &Path, quarantine: &Path) -> Result<()> {
        let fs = self.fs;
        match fs.rename_noreplace(source, quarantine) {
            Ok(()) => Ok(()),
            Err(error) if !unsupported(&error) => Err(error),
            Err(_) => {
                if fs::exists(fs, quarantine)? {
                    return Err(PyOsError::errno(EEXIST, Some(quarantine), None));
                }
                fs.rename(source, quarantine)
            }
        }
    }

    /// `_restore_quarantine_no_replace(quarantine, destination)`.
    fn restore_quarantine_no_replace(&self, quarantine: &Path, destination: &Path) -> Result<()> {
        let fs = self.fs;
        if fs::exists(fs, destination)? {
            if same_inode(fs, quarantine, destination) {
                return fs.unlink(quarantine);
            }
            return Err(PyOsError::errno(EEXIST, Some(destination), None));
        }
        match fs.rename_noreplace(quarantine, destination) {
            Ok(()) => return Ok(()),
            Err(error) if !unsupported(&error) => return Err(error),
            Err(_) => {}
        }
        fs.link(quarantine, destination)?;
        if !same_inode(fs, quarantine, destination) {
            return Err(PyOsError::message("restored quarantine identity mismatch"));
        }
        fs.unlink(quarantine)
    }
}

/// `_find_matching_files(root, brand, video_id, stop_requested)`'s candidates: the files of
/// `root/brand` named `<video_id>[-cdN].<suffix>`, sorted by name as Python sorts `str`.
pub(crate) fn find_matching_files(
    fs: &dyn MoveFs,
    root: &Path,
    brand: &str,
    video_id: &str,
    stop: &dyn Fn() -> bool,
) -> std::result::Result<Vec<PathBuf>, ScanError> {
    if stop() {
        return Ok(Vec::new());
    }
    if !fs::is_dir(fs, root)? || fs::is_symlink(fs, root)? {
        return Err(ScanError::Os(PyOsError::message("scan root unavailable")));
    }
    if stop() {
        return Ok(Vec::new());
    }
    let brand_path = root.join(brand);
    if !fs::is_dir(fs, &brand_path)? {
        return Ok(Vec::new());
    }
    if has_symlink_component(fs, &brand_path, root)? || !is_within(fs, &brand_path, root)? {
        return Err(ScanError::Value("unsafe brand path"));
    }
    let pattern = video_file_pattern(video_id);
    let mut matches = Vec::new();
    for path in fs.iterdir(&brand_path)? {
        if stop() {
            return Ok(Vec::new());
        }
        if !fs::is_symlink(fs, &path)?
            && fs::is_file(fs, &path)?
            && pattern.is_match(&crate::pystr::match_text(fs::name(&path)))
        {
            matches.push(path);
        }
    }
    matches.sort_by_cached_key(|path| crate::pystr::sort_key(fs::name(path)));
    Ok(matches)
}

/// `re.compile(rf'^{re.escape(video_id)}(?:-cd\d{{1,2}})?\..+', re.IGNORECASE)`, matched with
/// `fullmatch`. An undecodable byte in a name only ever meets `.+`, so matching the lossy text
/// decides as Python's surrogates do ([`crate::pystr::match_text`]).
pub(crate) fn video_file_pattern(video_id: &str) -> regex::Regex {
    let pattern = format!(r"^(?:{})(?:-cd\d{{1,2}})?\..+$", regex::escape(video_id));
    embyx_core::pyre::compile(&embyx_core::pyre::python_ignore_case(&pattern, true), true, false)
}

/// What a scan raised: the video becomes `scan_failed`, whatever it was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScanError {
    Os(PyOsError),
    Value(&'static str),
    /// A CloudDrive listing that failed for another reason than a missing directory.
    Cloud(crate::cloud_moves::MoverError),
}

impl From<PyOsError> for ScanError {
    fn from(error: PyOsError) -> Self {
        Self::Os(error)
    }
}
