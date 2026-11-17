//! Filing fill-actor moves into the library.
//!
//! Python origin: `src/embyx_manager/monitor/move_in.py`. Fill Actor's apply step moves a
//! chosen video out of an additional library into the staging tree under
//! `fill_actor.move_in_root`, optionally inside a brand subdirectory. Every file below it is
//! one an operator asked to move, so this sweep files each one into the library through the
//! archive pipeline and records the outcome in the acquisition ledger.
//!
//! The library destination is the route fill actor's offline directory feeds
//! (`fill_actor.task_dir_path`); a staging tree that is not routed yet is reported, not
//! guessed at. Only files are touched: the sweep never deletes a directory.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use embyx_core::avid::AvidParser;
use indexmap::IndexMap;

use crate::acquisitions::{AcquisitionSource, Ledger, LedgerError};
use crate::archive::{ArchivePipeline, Outcome, blocking};
use crate::fs::{self, Fs, PyOsError};
use crate::reconcile::{record_archived, record_attention};
use crate::reports::{PyException, RunCancelledError, RunContext};
use crate::support::{Clock, system_clock};

/// What ended a sweep: a stop request, a file-system error while listing the staging tree,
/// or the ledger. A failure to file one AVID is contained and counted instead.
#[derive(Debug, thiserror::Error)]
pub enum MoveInError {
    #[error(transparent)]
    Cancelled(#[from] RunCancelledError),
    #[error(transparent)]
    Os(#[from] PyOsError),
    #[error(transparent)]
    Ledger(#[from] LedgerError),
}

impl PyException for MoveInError {
    fn class_name(&self) -> &str {
        match self {
            Self::Cancelled(error) => error.class_name(),
            Self::Os(error) => error.class_name(),
            Self::Ledger(error) => error.class_name(),
        }
    }

    fn py_repr(&self) -> String {
        match self {
            Self::Cancelled(error) => error.py_repr(),
            Self::Os(error) => error.py_repr(),
            Self::Ledger(error) => error.py_repr(),
        }
    }
}

/// `MoveInSweeper`.
pub struct MoveInSweeper {
    ledger: Arc<dyn Ledger>,
    archiver: ArchivePipeline,
    avid: AvidParser,
    move_in_root: PathBuf,
    task_dir_path: String,
    clock: Clock,
    /// Files already reported as unreadable, so the warning fires once per process.
    reported: HashSet<PathBuf>,
}

impl std::fmt::Debug for MoveInSweeper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MoveInSweeper")
            .field("archiver", &self.archiver)
            .field("move_in_root", &self.move_in_root)
            .field("task_dir_path", &self.task_dir_path)
            .field("reported", &self.reported)
            .finish_non_exhaustive()
    }
}

impl MoveInSweeper {
    pub fn new(
        ledger: Arc<dyn Ledger>,
        archiver: ArchivePipeline,
        avid_parser: AvidParser,
        move_in_root: PathBuf,
        task_dir_path: String,
    ) -> Self {
        Self {
            ledger,
            archiver,
            avid: avid_parser,
            move_in_root,
            task_dir_path,
            clock: system_clock(),
            reported: HashSet::new(),
        }
    }

    /// The clock `datetime.now(UTC)` reads.
    #[must_use]
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// `rebind(archiver=, avid_parser=, move_in_root=, task_dir_path=)`: point the sweeper at
    /// the current configuration between runs; what it reported is kept.
    pub fn rebind(
        &mut self,
        archiver: ArchivePipeline,
        avid_parser: AvidParser,
        move_in_root: PathBuf,
        task_dir_path: String,
    ) {
        self.archiver = archiver;
        self.avid = avid_parser;
        self.move_in_root = move_in_root;
        self.task_dir_path = task_dir_path;
    }

    /// `run(ctx)`: one sweep of the staging tree.
    pub async fn run(&mut self, ctx: &RunContext) -> Result<(), MoveInError> {
        if self.task_dir_path.is_empty() {
            ctx.info("skipping the move-in sweep, fill_actor.task_dir_path is not configured");
            return Ok(());
        }
        let Some((dst_subdir, priority)) = self.archiver.route_for_task_dir(&self.task_dir_path) else {
            ctx.warning(&format!(
                "skipping the move-in sweep, no archive route covers {}",
                self.task_dir_path
            ));
            return Ok(());
        };
        let fs = Arc::clone(self.archiver.fs());
        let staged = blocking(|| staged_videos(&*fs, &self.move_in_root))?;
        if staged.is_empty() {
            return Ok(());
        }
        let mut by_avid: IndexMap<String, Vec<PathBuf>> = IndexMap::new();
        for video in staged {
            let name = file_name(&video);
            let avid = self.avid.get_avid(&name);
            if avid.is_empty() {
                if self.reported.insert(video) {
                    ctx.warning(&format!(
                        "failed to read an avid for the staged file {name}, leaving it"
                    ));
                    ctx.add("needs_attention", 1);
                }
                continue;
            }
            by_avid.entry(avid).or_default().push(video);
        }
        for (avid, group) in by_avid {
            ctx.check_cancelled()?;
            self.file_group(&avid, group, &dst_subdir, ctx, priority).await?;
        }
        Ok(())
    }

    async fn file_group(
        &self,
        avid: &str,
        videos: Vec<PathBuf>,
        dst_subdir: &str,
        ctx: &RunContext,
        priority: bool,
    ) -> Result<(), LedgerError> {
        let filed = blocking(|| {
            self.archiver
                .archive_loose_group(avid, videos, dst_subdir, ctx, priority)
        });
        let result = match filed {
            Ok(result) => result,
            Err(error) => {
                ctx.exception(&format!("failed to archive the staged files of {avid}"), Some(&error));
                ctx.add("items_failed", 1);
                return Ok(());
            }
        };
        let now = (self.clock)();
        if result.outcome == Outcome::Archived {
            record_archived(
                &*self.ledger,
                AcquisitionSource::FillActor,
                avid,
                result.archived_paths,
                now,
            )
            .await?;
            ctx.add("move_in_archived", 1);
            return Ok(());
        }
        record_attention(
            &*self.ledger,
            AcquisitionSource::FillActor,
            avid,
            &result.reason,
            now,
            ctx,
        )
        .await
    }
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// `_staged_videos`: every video in the staging tree, the root and its brand directories;
/// symbolic links are skipped, and a root that is a link stages nothing.
fn staged_videos(fs: &dyn Fs, root: &Path) -> Result<Vec<PathBuf>, PyOsError> {
    if !fs::is_dir(fs, root)? || fs::is_symlink(fs, root)? {
        return Ok(Vec::new());
    }
    let mut entries = fs.iterdir(root)?;
    entries.sort();
    let mut videos = Vec::new();
    for entry in entries {
        if fs::is_symlink(fs, &entry)? {
            continue;
        }
        if fs::is_video(fs, &entry)? {
            videos.push(entry);
        } else if fs::is_dir(fs, &entry)? {
            let mut children = fs.iterdir(&entry)?;
            children.sort();
            for child in children {
                if !fs::is_symlink(fs, &child)? && fs::is_video(fs, &child)? {
                    videos.push(child);
                }
            }
        }
    }
    Ok(videos)
}

#[cfg(test)]
mod tests;
