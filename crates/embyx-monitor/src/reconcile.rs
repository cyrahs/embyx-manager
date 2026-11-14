//! The scan that catches what the tracker never saw.
//!
//! Python origin: `src/embyx_manager/monitor/reconcile.py`. The tracker handles everything
//! the pipeline downloaded; this scan is for the rest: folders dropped in by hand, downloads
//! that predate the ledger, anything left behind when an offline record was cleared before it
//! could be archived. It walks the configured routes, files what it can, and writes what it
//! finds into the ledger.
//!
//! Two rules keep it from fighting the tracker or itself: an AVID the ledger shows as active
//! belongs to the tracker and its folder is left alone; a folder is only archived once it has
//! been seen twice at the same size. A folder it cannot identify becomes a `needs_attention` row
//! rather than a warning logged every run forever.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use embyx_core::config::models::ArchiveConfig;

use crate::acquisitions::{AcquisitionSource, AcquisitionState, Ledger, LedgerError, Sighting, TransitionFields};
use crate::archive::{ArchivePipeline, Outcome, blocking};
use crate::fs::{self, PyOsError};
use crate::reports::{PyException, RunCancelledError, RunContext};
use crate::support::{Clock, system_clock};

/// What ended a pass: a stop request, a file-system error outside a folder's own filing, or
/// the ledger.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error(transparent)]
    Cancelled(#[from] RunCancelledError),
    #[error(transparent)]
    Os(#[from] PyOsError),
    #[error(transparent)]
    Ledger(#[from] LedgerError),
}

impl PyException for ReconcileError {
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

/// `_Sighting`: what a folder looked like on a previous pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FolderSighting {
    size: u64,
    entries: u64,
}

/// `ReconcileScanner`. It outlives a run: settling a folder takes two passes to observe.
pub struct ReconcileScanner {
    ledger: std::sync::Arc<dyn Ledger>,
    archiver: ArchivePipeline,
    config: ArchiveConfig,
    clock: Clock,
    /// Folder -> what it looked like last pass, so growth is visible.
    seen: HashMap<PathBuf, FolderSighting>,
    /// Folders reported as needing attention that have no AVID to park them under; the
    /// warning fires once per process.
    reported: HashSet<PathBuf>,
}

impl std::fmt::Debug for ReconcileScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReconcileScanner")
            .field("archiver", &self.archiver)
            .field("seen", &self.seen)
            .field("reported", &self.reported)
            .finish_non_exhaustive()
    }
}

impl ReconcileScanner {
    pub fn new(ledger: std::sync::Arc<dyn Ledger>, archiver: ArchivePipeline, config: ArchiveConfig) -> Self {
        Self {
            ledger,
            archiver,
            config,
            clock: system_clock(),
            seen: HashMap::new(),
            reported: HashSet::new(),
        }
    }

    /// The clock `datetime.now(UTC)` reads.
    #[must_use]
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// `rebind(archiver=, config=)`: point the scanner at the current configuration between
    /// runs; what it has seen is kept.
    pub fn rebind(&mut self, archiver: ArchivePipeline, config: ArchiveConfig) {
        self.archiver = archiver;
        self.config = config;
    }

    /// `run(ctx)`: one pass over every route, priority routes first.
    pub async fn run(&mut self, ctx: &RunContext) -> Result<(), ReconcileError> {
        let active = self.ledger.active_avids().await?;
        for (src, dst, priority) in ArchivePipeline::routes(&self.config) {
            ctx.check_cancelled()?;
            let root = self.archiver.route_root(&src);
            let fs = std::sync::Arc::clone(self.archiver.fs());
            if !blocking(|| fs::is_dir(&*fs, &root))? {
                // Nothing to do is not a failure; the route may not be mounted yet.
                ctx.info(&format!(
                    "skipping {}, its source directory does not exist",
                    fs::path_text(&root)
                ));
                continue;
            }
            self.scan_route(&root, &dst, &active, ctx, priority).await?;
        }
        Ok(())
    }

    async fn scan_route(
        &mut self,
        root: &Path,
        dst_subdir: &str,
        active: &std::collections::BTreeSet<String>,
        ctx: &RunContext,
        priority: bool,
    ) -> Result<(), ReconcileError> {
        let fs = std::sync::Arc::clone(self.archiver.fs());
        let folders = blocking(|| -> Result<Vec<PathBuf>, PyOsError> {
            let mut folders = Vec::new();
            for entry in fs.iterdir(root)? {
                if fs::is_dir(&*fs, &entry)? {
                    folders.push(entry);
                }
            }
            folders.sort();
            Ok(folders)
        })?;
        for folder in folders {
            ctx.check_cancelled()?;
            let name = folder_name(&folder);
            if self.archiver.covers_other_route(&folder, root) {
                // Another route's source: it is scanned as a route of its own, and measuring it
                // here would walk a live download directory.
                ctx.info(&format!("skipping {name}, it is another route of its own"));
                continue;
            }
            let avid = self.archiver.avid_of(&name);
            if !avid.is_empty() && active.contains(&avid) {
                // The tracker owns this download; the ledger is the lock.
                ctx.info(&format!("leaving {name} to the tracker"));
                continue;
            }
            if !self.is_settled(&folder, ctx)? {
                continue;
            }
            self.reconcile_folder(&folder, dst_subdir, ctx, priority).await?;
        }
        Ok(())
    }

    /// `_is_settled`: true once a folder has looked the same for two consecutive passes.
    fn is_settled(&mut self, folder: &Path, ctx: &RunContext) -> Result<bool, PyOsError> {
        let fs = std::sync::Arc::clone(self.archiver.fs());
        let sighting = blocking(|| measure(&*fs, folder))?;
        let previous = self.seen.insert(folder.to_path_buf(), sighting);
        if previous == Some(sighting) {
            return Ok(true);
        }
        ctx.info(&format!(
            "{} is still changing, leaving it for the next pass",
            folder_name(folder)
        ));
        Ok(false)
    }

    async fn reconcile_folder(
        &mut self,
        folder: &Path,
        dst_subdir: &str,
        ctx: &RunContext,
        priority: bool,
    ) -> Result<(), ReconcileError> {
        let result = blocking(|| self.archiver.archive_folder(folder, dst_subdir, ctx, priority, ""));
        let now = (self.clock)();
        if result.outcome == Outcome::Archived {
            self.seen.remove(folder);
            self.record_archived(&result.avid, result.archived_paths, now).await?;
            ctx.add("reconciled", 1);
            return Ok(());
        }
        if result.outcome != Outcome::NeedsAttention {
            return Ok(());
        }
        if !result.avid.is_empty() {
            self.record_attention(&result.avid, &result.reason, now, ctx).await?;
        } else if self.reported.insert(folder.to_path_buf()) {
            ctx.warning(&format!("{} needs attention: {}", folder_name(folder), result.reason));
            ctx.add("needs_attention", 1);
        }
        Ok(())
    }

    async fn record_archived(
        &self,
        avid: &str,
        archived_paths: Vec<String>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), LedgerError> {
        record_archived(&*self.ledger, AcquisitionSource::Reconcile, avid, archived_paths, now).await
    }

    async fn record_attention(
        &self,
        avid: &str,
        reason: &str,
        now: chrono::DateTime<chrono::Utc>,
        ctx: &RunContext,
    ) -> Result<(), LedgerError> {
        record_attention(&*self.ledger, AcquisitionSource::Reconcile, avid, reason, now, ctx).await
    }
}

/// `folder.name`.
fn folder_name(folder: &Path) -> String {
    folder
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// `_measure`: total size and count of the files below a folder (links to files counted).
fn measure(fs: &dyn fs::Fs, folder: &Path) -> Result<FolderSighting, PyOsError> {
    let mut size = 0_u64;
    let mut entries = 0_u64;
    for path in fs.rglob(folder)? {
        if fs::is_file(fs, &path)? {
            size += fs::size(fs, &path)?;
            entries += 1;
        }
    }
    Ok(FolderSighting { size, entries })
}

/// `_record_archived` (reconcile and move-in): discover the AVID if the ledger never saw it,
/// then mark it archived with its library paths; an archived row is left as it is.
pub(crate) async fn record_archived(
    ledger: &dyn Ledger,
    source: AcquisitionSource,
    avid: &str,
    archived_paths: Vec<String>,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), LedgerError> {
    let mut record = ledger.get(avid).await?;
    if record.is_none() {
        ledger.discover(avid, source.as_str(), now, Sighting::default()).await?;
        record = ledger.get(avid).await?;
    }
    let Some(record) = record.filter(|record| record.state != AcquisitionState::Archived) else {
        return Ok(());
    };
    ledger
        .transition(
            avid,
            record.state,
            AcquisitionState::Archived,
            now,
            TransitionFields::archived_paths(archived_paths),
        )
        .await?;
    Ok(())
}

/// `_record_attention` (reconcile and move-in): park the AVID as `needs_attention`; log it
/// only when the row moves there, so a parked row is not reported every pass.
pub(crate) async fn record_attention(
    ledger: &dyn Ledger,
    source: AcquisitionSource,
    avid: &str,
    reason: &str,
    now: chrono::DateTime<chrono::Utc>,
    ctx: &RunContext,
) -> Result<(), LedgerError> {
    let mut record = ledger.get(avid).await?;
    if record
        .as_ref()
        .is_some_and(|record| record.state == AcquisitionState::NeedsAttention)
    {
        return Ok(());
    }
    if record.is_none() {
        ledger.discover(avid, source.as_str(), now, Sighting::default()).await?;
        record = ledger.get(avid).await?;
    }
    let Some(record) = record.filter(|record| record.state != AcquisitionState::Archived) else {
        return Ok(());
    };
    if ledger
        .transition(
            avid,
            record.state,
            AcquisitionState::NeedsAttention,
            now,
            TransitionFields::note(reason),
        )
        .await?
    {
        ctx.warning(&format!("{avid} needs attention: {reason}"));
        ctx.add("needs_attention", 1);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
