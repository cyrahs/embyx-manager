//! Driving each acquisition to a conclusion against CloudDrive's task list.
//!
//! Python origin: `src/embyx_manager/monitor/tracker.py`. Every pass reads every offline
//! task CloudDrive knows about under the configured directories and joins it to the ledger
//! on the info hash recorded when the magnet was submitted. From there each attempt goes
//! somewhere: progress is refreshed, a finished download is archived, and anything that
//! errored, stopped moving, or turned out to hold no usable video falls through to the AVID's
//! next magnet.
//!
//! Concurrency is handled by claiming a finished attempt with a compare-and-set, so two
//! replicas can poll the same task list and only one archives the folder. Abandoning an
//! attempt removes its offline task at CloudDrive with its data; a task whose attempt archived
//! is cleared with its files kept.
//!
//! A replacement (换版) is an acquisition of a title the library already holds. Its download
//! is filed like any other, except that once the folder is known to be fileable the copy being
//! replaced is deleted first, through the CloudDrive API into the cloud's recycle bin, and only
//! once the mount no longer shows it is the new copy filed.
//!
//! CloudDrive calls go through [`TrackerCloud`]; on [`AsyncCloudDrive`] each runs to its end in
//! a task of its own, so a poll whose future is dropped never abandons a deletion it sent.
//! Whoever cancels a poll and then acts on the ledger awaits [`AcquisitionTracker::settle`]
//! (build the tracker on [`AsyncCloudDrive::scoped`]). File-system work runs through the
//! archiver's [`crate::fs::Fs`] in [`blocking`].
//!
//! [`AsyncCloudDrive`]: embyx_clients::clouddrive::AsyncCloudDrive
//! [`AsyncCloudDrive::scoped`]: embyx_clients::clouddrive::AsyncCloudDrive::scoped

use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::TimeDelta;
use embyx_clients::clouddrive::{OfflineStatus, OfflineTask};
use embyx_core::config::models::ArchiveConfig;
use embyx_core::pypath::{self, PyPath};
use indexmap::IndexMap;

use crate::acquisitions::{AcquisitionState, AttemptState, Ledger, LedgerError, MagnetAttemptRecord, TransitionFields};
use crate::archive::{ArchivePipeline, ArchiveResult, Inspection, Outcome, blocking};
use crate::cadence::next_resolve_at;
use crate::cloud::{TrackerCloud, is_file_not_found};
use crate::fs::{self, PyOsError, path_text};
use crate::intake::SourceError;
use crate::release_dates::{ReleaseDateLookup, ensure_release_date};
use crate::reports::{PyException, RunCancelledError, RunContext};
use crate::support::{BoxFuture, Clock, OverflowError, py_timedelta_seconds, system_clock};

/// `CLEAR_BATCH`: how many archived tasks one `RemoveOfflineFiles` call clears.
pub const CLEAR_BATCH: usize = 100;

/// `submit_magnet(avid, magnet)`: adds one offline task; true when accepted.
pub type SubmitMagnet = Arc<dyn Fn(String, String) -> BoxFuture<'static, bool> + Send + Sync>;

/// `TrackerRoute`: one archive route, resolved to the local directory it covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackerRoute {
    pub root: PathBuf,
    pub dst: String,
    pub priority: bool,
}

/// `TrackerSettings`: where finished downloads live and how patient the tracker is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackerSettings {
    /// The CloudDrive API paths the offline tasks are queued under, default first.
    pub task_dir_paths: Vec<String>,
    /// The archive routes, priority routes first, resolved against `src_dir`.
    pub routes: Vec<TrackerRoute>,
    pub stall_timeout_hours: i64,
    pub max_attempts: i64,
    /// How long CloudDrive is given to register a freshly submitted magnet.
    pub submit_grace_seconds: i64,
    /// Where the CloudDrive mount the library lives on is mounted locally; empty leaves
    /// replacements parked.
    pub library_mount_prefix: String,
}

impl TrackerSettings {
    /// `TrackerSettings.from_config(config, task_dir_paths=, library_mount_prefix=)`.
    pub fn from_config(config: &ArchiveConfig, task_dir_paths: Vec<String>, library_mount_prefix: &str) -> Self {
        let src_dir = PyPath::new(&config.src_dir);
        let routes = config
            .priority_mapping
            .iter()
            .map(|(source, dst)| (source, dst, true))
            .chain(config.mapping.iter().map(|(source, dst)| (source, dst, false)))
            .map(|(source, dst, priority)| TrackerRoute {
                root: PathBuf::from(pypath::join(src_dir.as_str(), source)),
                dst: dst.clone(),
                priority,
            })
            .collect();
        Self {
            task_dir_paths,
            routes,
            stall_timeout_hours: config.stall_timeout_hours,
            max_attempts: config.max_attempts,
            submit_grace_seconds: 300,
            library_mount_prefix: library_mount_prefix.to_owned(),
        }
    }
}

/// What ended a poll: a stop request, the ledger, a CloudDrive listing, the file system, or a
/// `timedelta` out of range.
#[derive(Debug, thiserror::Error)]
pub enum TrackerError {
    #[error(transparent)]
    Cancelled(#[from] RunCancelledError),
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error(transparent)]
    Cloud(#[from] SourceError),
    #[error(transparent)]
    Os(#[from] PyOsError),
    #[error(transparent)]
    Overflow(#[from] OverflowError),
}

impl PyException for TrackerError {
    fn class_name(&self) -> &str {
        match self {
            Self::Cancelled(error) => error.class_name(),
            Self::Ledger(error) => error.class_name(),
            Self::Cloud(error) => error.class_name(),
            Self::Os(error) => error.class_name(),
            Self::Overflow(_) => "OverflowError",
        }
    }

    fn py_repr(&self) -> String {
        match self {
            Self::Cancelled(error) => error.py_repr(),
            Self::Ledger(error) => error.py_repr(),
            Self::Cloud(error) => error.py_repr(),
            Self::Os(error) => error.py_repr(),
            Self::Overflow(error) => format!("OverflowError({})", embyx_core::pytext::py_repr(&error.0)),
        }
    }
}

/// `timedelta(hours=hours)`.
fn py_timedelta_hours(hours: i64) -> Result<TimeDelta, OverflowError> {
    match hours.checked_mul(3600) {
        Some(seconds) => py_timedelta_seconds(seconds),
        None => Err(OverflowError("Python int too large to convert to C int".to_owned())),
    }
}

/// `delta.days`: whole days, rounded down.
fn py_days(delta: TimeDelta) -> i64 {
    match delta.num_microseconds() {
        Some(micros) => micros.div_euclid(86_400_000_000),
        None => delta.num_seconds().div_euclid(86_400),
    }
}

/// `f'{progress or 0:.0f}'`.
fn progress_text(progress: Option<f64>) -> String {
    match progress {
        // `-0.0` is falsy too, and the literal pattern matches it.
        None | Some(0.0) => "0".to_owned(),
        Some(value) if value.is_nan() => "nan".to_owned(),
        Some(value) if value.is_infinite() => if value > 0.0 { "inf" } else { "-inf" }.to_owned(),
        Some(value) => format!("{value:.0}"),
    }
}

/// `PurePosixPath(text).parts`: the anchor and the names.
fn posix_parts(text: &str) -> Vec<String> {
    let normalized = PyPath::new(text);
    let normalized = normalized.as_str();
    let (anchor, rest) = if let Some(rest) = normalized.strip_prefix("//") {
        (Some("//"), rest)
    } else if let Some(rest) = normalized.strip_prefix('/') {
        (Some("/"), rest)
    } else {
        (None, normalized)
    };
    anchor
        .into_iter()
        .chain(rest.split('/').filter(|part| !part.is_empty() && *part != "."))
        .map(str::to_owned)
        .collect()
}

/// Why `_api_path` gives no API path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NoApiPath {
    /// `relative_to` raised: the path is not under the mount.
    Outside,
    /// The local name is not UTF-8, so no CloudDrive path can be named for it without a lossy
    /// decode (DIVERGENCES.md D81).
    NotUtf8,
}

/// `_api_path(path, mount_prefix)`: the CloudDrive API path of a file seen through the mount.
fn api_path(path: &Path, mount_prefix: &str) -> Result<String, NoApiPath> {
    let text = path.to_str().ok_or(NoApiPath::NotUtf8)?;
    let parts = posix_parts(text);
    let prefix = posix_parts(mount_prefix);
    if prefix.is_empty() || parts.len() < prefix.len() || parts[..prefix.len()] != prefix[..] {
        return Err(NoApiPath::Outside);
    }
    Ok(format!("/{}", parts[prefix.len()..].join("/")))
}

/// `str(PurePosixPath(path).parent)` of an absolute API path.
fn api_parent(path: &str) -> String {
    let normalized = PyPath::new(path);
    let normalized = normalized.as_str();
    match normalized.rfind('/') {
        Some(0) => "/".to_owned(),
        Some(index) if normalized.starts_with("//") && index == 1 => "//".to_owned(),
        Some(index) => normalized[..index].to_owned(),
        None => ".".to_owned(),
    }
}

/// `folder.name`, read for its AVID (lossy where the name is not UTF-8, as D72 reads names).
fn name_of(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn failed(outcome: Outcome, avid: &str, reason: String) -> ArchiveResult {
    ArchiveResult {
        outcome,
        avid: avid.to_owned(),
        archived_paths: Vec::new(),
        reason,
    }
}

/// The constructor's arguments (`AcquisitionTracker(ledger=, cloud=, archiver=, ...)`).
pub struct TrackerParts {
    pub ledger: Arc<dyn Ledger>,
    pub cloud: Arc<dyn TrackerCloud>,
    pub archiver: ArchivePipeline,
    pub settings: TrackerSettings,
    pub submit_magnet: SubmitMagnet,
    pub release_date_lookup: Option<Arc<dyn ReleaseDateLookup>>,
}

impl fmt::Debug for TrackerParts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrackerParts")
            .field("ledger", &self.ledger)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

/// `AcquisitionTracker`.
pub struct AcquisitionTracker {
    ledger: Arc<dyn Ledger>,
    cloud: Arc<dyn TrackerCloud>,
    archiver: ArchivePipeline,
    settings: TrackerSettings,
    submit: SubmitMagnet,
    release_date_lookup: Option<Arc<dyn ReleaseDateLookup>>,
    clock: Clock,
    clear_batch: usize,
}

impl fmt::Debug for AcquisitionTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AcquisitionTracker")
            .field("ledger", &self.ledger)
            .field("archiver", &self.archiver)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl AcquisitionTracker {
    pub fn new(parts: TrackerParts) -> Self {
        Self {
            ledger: parts.ledger,
            cloud: parts.cloud,
            archiver: parts.archiver,
            settings: parts.settings,
            submit: parts.submit_magnet,
            release_date_lookup: parts.release_date_lookup,
            clock: system_clock(),
            clear_batch: CLEAR_BATCH,
        }
    }

    /// The clock `datetime.now(UTC)` stands for.
    #[must_use]
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// `CLEAR_BATCH`, which the Python tests patch.
    pub fn set_clear_batch(&mut self, clear_batch: usize) {
        self.clear_batch = clear_batch.max(1);
    }

    pub fn settings(&self) -> &TrackerSettings {
        &self.settings
    }

    pub fn settings_mut(&mut self) -> &mut TrackerSettings {
        &mut self.settings
    }

    pub fn set_release_date_lookup(&mut self, lookup: Option<Arc<dyn ReleaseDateLookup>>) {
        self.release_date_lookup = lookup;
    }

    /// Wait for every CloudDrive call a poll started, also one whose poll was dropped.
    pub async fn settle(&self) {
        self.cloud.settle().await;
    }

    /// `poll(ctx)`: one pass over the offline task lists of every configured directory.
    pub async fn poll(&self, ctx: &RunContext) -> Result<(), TrackerError> {
        if self.settings.task_dir_paths.is_empty() {
            // No directory to read means no evidence either way, which is not the same as
            // every task having vanished; sweeping here would give up on everything in flight.
            ctx.warning("no offline directory is configured, skipping the poll");
            return Ok(());
        }
        // Each task is kept with the directory it was listed under, the only way back to its
        // path once the listings are merged.
        let mut by_hash: IndexMap<String, (OfflineTask, String)> = IndexMap::new();
        let mut total: i64 = 0;
        for task_dir in &self.settings.task_dir_paths {
            ctx.check_cancelled()?;
            let tasks = self.cloud.list_offline_files(task_dir).await?;
            total += i64::try_from(tasks.len()).unwrap_or(i64::MAX);
            for task in tasks {
                if task.info_hash.is_empty() {
                    continue;
                }
                if by_hash.contains_key(&task.info_hash) {
                    // CloudDrive rejects a magnet already queued elsewhere, so this is rare; the
                    // first directory listing it wins either way.
                    ctx.add("duplicate_offline_tasks", 1);
                    continue;
                }
                by_hash.insert(task.info_hash.clone(), (task, task_dir.clone()));
            }
        }
        ctx.set("offline_tasks", total);
        let hashes: Vec<String> = by_hash.keys().cloned().collect();
        let attempts = self.ledger.attempts_by_info_hash(&hashes).await?;

        for (info_hash, attempt) in &attempts {
            ctx.check_cancelled()?;
            let Some((task, task_dir)) = by_hash.get(info_hash) else {
                continue;
            };
            self.advance(attempt, task, task_dir, ctx).await?;
        }

        let live: HashSet<&str> = by_hash.keys().map(String::as_str).collect();
        self.sweep_lost(&live, ctx).await?;
        self.clear_archived(&by_hash, ctx).await?;
        Ok(())
    }

    /// `_clear_archived`: drop the finished tasks whose attempt archived, keeping their files.
    /// Best-effort: the next pass tries again.
    async fn clear_archived(
        &self,
        by_hash: &IndexMap<String, (OfflineTask, String)>,
        ctx: &RunContext,
    ) -> Result<(), TrackerError> {
        let finished: IndexMap<&str, &str> = by_hash
            .iter()
            .filter(|(_, (task, _))| task.status == OfflineStatus::Finished)
            .map(|(info_hash, (_, task_dir))| (info_hash.as_str(), task_dir.as_str()))
            .collect();
        let wanted: Vec<String> = finished.keys().map(|hash| (*hash).to_owned()).collect();
        let archived = self.ledger.archived_info_hashes(&wanted).await?;
        let mut by_dir: IndexMap<&str, Vec<String>> = IndexMap::new();
        // `sorted(archived)`: a BTreeSet iterates in that order.
        for info_hash in &archived {
            if let Some(task_dir) = finished.get(info_hash.as_str()) {
                by_dir.entry(task_dir).or_default().push(info_hash.clone());
            }
        }
        for (task_dir, hashes) in by_dir {
            for batch in hashes.chunks(self.clear_batch) {
                match self.cloud.remove_offline_files(batch, task_dir, false).await {
                    Err(error) => ctx.exception(
                        &format!(
                            "failed to clear {} archived offline tasks under {task_dir}",
                            batch.len()
                        ),
                        Some(&error),
                    ),
                    Ok(()) => ctx.add("archived_tasks_cleared", i64::try_from(batch.len()).unwrap_or(i64::MAX)),
                }
            }
        }
        Ok(())
    }

    // -- one attempt ---------------------------------------------------------

    async fn advance(
        &self,
        attempt: &MagnetAttemptRecord,
        task: &OfflineTask,
        task_dir: &str,
        ctx: &RunContext,
    ) -> Result<(), TrackerError> {
        match task.status {
            OfflineStatus::Finished => self.archive_finished(attempt, task, task_dir, ctx).await,
            OfflineStatus::Error => {
                if self
                    .give_up(
                        attempt,
                        AttemptState::Error,
                        "CloudDrive reported the download failed",
                        ctx,
                    )
                    .await?
                {
                    self.remove_task(&task.info_hash, task_dir, ctx).await;
                }
                Ok(())
            }
            _ if matches!(attempt.state, AttemptState::Submitted | AttemptState::Downloading) => {
                self.refresh_progress(attempt, task, task_dir, ctx).await
            }
            _ => Ok(()),
        }
    }

    async fn refresh_progress(
        &self,
        attempt: &MagnetAttemptRecord,
        task: &OfflineTask,
        task_dir: &str,
        ctx: &RunContext,
    ) -> Result<(), TrackerError> {
        let now = (self.clock)();
        let moved = self
            .ledger
            .record_progress(
                &attempt.avid,
                attempt.attempt_no,
                AttemptState::Downloading,
                Some(task.progress),
                now,
            )
            .await?;
        if moved {
            return Ok(());
        }
        // Nothing was written, so updated_at still marks the last real progress.
        let stalled_for = now - attempt.updated_at;
        if stalled_for >= py_timedelta_hours(self.settings.stall_timeout_hours)? {
            let reason = format!(
                "no progress for {}d at {}%",
                py_days(stalled_for),
                progress_text(attempt.progress)
            );
            if self.give_up(attempt, AttemptState::Stalled, &reason, ctx).await? {
                self.remove_task(&task.info_hash, task_dir, ctx).await;
            }
        }
        Ok(())
    }

    async fn archive_finished(
        &self,
        attempt: &MagnetAttemptRecord,
        task: &OfflineTask,
        task_dir: &str,
        ctx: &RunContext,
    ) -> Result<(), TrackerError> {
        let now = (self.clock)();
        let record = self.ledger.get(&attempt.avid).await?;
        if record.is_none_or(|record| record.state != AcquisitionState::Downloading) {
            // Someone is holding this AVID: an operator marked it, or it is done.
            return Ok(());
        }
        if attempt.state != AttemptState::Finished
            && !self
                .ledger
                .transition_attempt(
                    &attempt.avid,
                    attempt.attempt_no,
                    attempt.state,
                    AttemptState::Finished,
                    now,
                    None,
                )
                .await?
        {
            return Ok(());
        }
        if !self
            .ledger
            .transition_attempt(
                &attempt.avid,
                attempt.attempt_no,
                AttemptState::Finished,
                AttemptState::Archiving,
                now,
                None,
            )
            .await?
        {
            // Another replica claimed it first.
            return Ok(());
        }

        self.refresh_mount(task, task_dir, ctx).await;
        let located = blocking(|| self.locate(&task.name, &attempt.avid))?;
        let Some((folder, route)) = located else {
            // Release the claim: the mount may simply not show the folder yet.
            self.ledger
                .transition_attempt(
                    &attempt.avid,
                    attempt.attempt_no,
                    AttemptState::Archiving,
                    AttemptState::Finished,
                    (self.clock)(),
                    None,
                )
                .await?;
            ctx.warning(&format!(
                "finished task {} is not under any archive route yet",
                task.name
            ));
            return Ok(());
        };
        let result = self.file(attempt, &folder, &route, ctx).await?;
        self.settle_attempt(attempt, &result, ctx).await?;
        if result.outcome == Outcome::Junk {
            // The archiver only reported the folder; the task and its files are still at
            // CloudDrive and nothing will come back for them.
            self.remove_task(&task.info_hash, task_dir, ctx).await;
        }
        Ok(())
    }

    /// `_file`: archive a claimed download, clearing the copy it replaces out of the way first.
    async fn file(
        &self,
        attempt: &MagnetAttemptRecord,
        folder: &Path,
        route: &TrackerRoute,
        ctx: &RunContext,
    ) -> Result<ArchiveResult, TrackerError> {
        let record = self.ledger.get(&attempt.avid).await?;
        let replacing = record.map(|record| record.replace_paths).unwrap_or_default();
        if replacing.is_empty() {
            return Ok(blocking(|| {
                self.archiver
                    .archive_folder(folder, &route.dst, ctx, route.priority, &attempt.avid)
            }));
        }
        // The old copy goes only once the new download is known to be fileable: a folder that
        // turns out to be junk or the wrong title leaves it alone.
        let selection = match blocking(|| self.archiver.inspect_folder(folder, ctx, &attempt.avid)) {
            Inspection::Concluded(result) => return Ok(result),
            Inspection::Selected(selection) => selection,
        };
        match self.remove_replaced(&attempt.avid, &replacing, ctx).await {
            Err(error) => {
                // The claim must still be released.
                ctx.exception(
                    &format!("{}: failed to delete the replaced copy", attempt.avid),
                    Some(&error),
                );
                return Ok(failed(Outcome::Failed, &attempt.avid, error.to_string()));
            }
            Ok(Some(problem)) => return Ok(problem),
            Ok(None) => {}
        }
        Ok(blocking(|| {
            self.archiver
                .file_selection(folder, &selection, &route.dst, ctx, route.priority)
        }))
    }

    /// `_remove_replaced`: delete the copy a replacement replaces; `None` once the library no
    /// longer shows it.
    ///
    /// Deletion goes through the CloudDrive API so the files land in the cloud's recycle bin.
    /// A file already gone counts as deleted, which makes a retry after a half-finished pass
    /// safe. Until the mount stops showing the old files the new ones could collide with them,
    /// so that wait is a passing failure and the next poll looks again.
    async fn remove_replaced(
        &self,
        avid: &str,
        replacing: &[String],
        ctx: &RunContext,
    ) -> Result<Option<ArchiveResult>, TrackerError> {
        let prefix = &self.settings.library_mount_prefix;
        if prefix.is_empty() {
            return Ok(Some(failed(
                Outcome::NeedsAttention,
                avid,
                "a replacement needs merge.cloud_mount_prefix to delete the old copy".to_owned(),
            )));
        }
        let present = blocking(|| self.archiver.replaced_copies(avid, replacing))?;
        if present.is_empty() {
            return Ok(None);
        }
        let mut api_paths: Vec<String> = Vec::new();
        for path in &present {
            match api_path(path, prefix) {
                Ok(api) => api_paths.push(api),
                Err(NoApiPath::Outside) => {
                    return Ok(Some(failed(
                        Outcome::NeedsAttention,
                        avid,
                        format!("{} is outside the CloudDrive mount {prefix}", path_text(path)),
                    )));
                }
                Err(NoApiPath::NotUtf8) => {
                    return Ok(Some(failed(
                        Outcome::NeedsAttention,
                        avid,
                        format!("{} is not a UTF-8 name", path_text(path)),
                    )));
                }
            }
        }
        let listed = self.cloud_listed(&api_paths).await?;
        if !listed.is_empty() {
            ctx.info(&format!("{avid}: deleting the replaced copy {}", listed.join(", ")));
            self.cloud.delete_files(&listed).await?;
            ctx.add(
                "replaced_copies_deleted",
                i64::try_from(listed.len()).unwrap_or(i64::MAX),
            );
        }
        // Re-reading each directory is what makes the mount drop the deleted files.
        self.cloud_listed(&api_paths).await?;
        if !blocking(|| self.archiver.replaced_copies(avid, replacing))?.is_empty() {
            return Ok(Some(failed(
                Outcome::Failed,
                avid,
                "the replaced copy is still on the mount; filing on the next poll".to_owned(),
            )));
        }
        Ok(None)
    }

    /// `_cloud_listed`: which of these files CloudDrive lists, from one fresh listing per
    /// directory.
    async fn cloud_listed(&self, api_paths: &[String]) -> Result<Vec<String>, SourceError> {
        let directories: BTreeSet<String> = api_paths.iter().map(|path| api_parent(path)).collect();
        let mut listed: HashSet<String> = HashSet::new();
        for directory in &directories {
            let entries = match self.cloud.list_directory(directory).await {
                Ok(entries) => entries,
                Err(error) if is_file_not_found(&error) => continue,
                Err(error) => return Err(error),
            };
            listed.extend(
                entries
                    .into_iter()
                    .filter(|entry| !entry.is_directory)
                    .map(|entry| entry.full_path),
            );
        }
        Ok(api_paths
            .iter()
            .filter(|path| listed.contains(*path))
            .cloned()
            .collect())
    }

    /// `_settle`: write the archive's outcome to the ledger.
    async fn settle_attempt(
        &self,
        attempt: &MagnetAttemptRecord,
        result: &ArchiveResult,
        ctx: &RunContext,
    ) -> Result<(), TrackerError> {
        let now = (self.clock)();
        let (avid, attempt_no) = (attempt.avid.as_str(), attempt.attempt_no);
        match result.outcome {
            Outcome::Archived => {
                self.ledger
                    .transition_attempt(
                        avid,
                        attempt_no,
                        AttemptState::Archiving,
                        AttemptState::Archived,
                        now,
                        None,
                    )
                    .await?;
                self.ledger
                    .transition(
                        avid,
                        AcquisitionState::Downloading,
                        AcquisitionState::Archived,
                        now,
                        TransitionFields::archived_paths(result.archived_paths.clone()),
                    )
                    .await?;
                ctx.add("archived", 1);
            }
            Outcome::Junk => {
                ctx.warning(&format!("{avid} finished with no usable video, trying the next magnet"));
                self.ledger
                    .transition_attempt(avid, attempt_no, AttemptState::Archiving, AttemptState::Junk, now, None)
                    .await?;
                ctx.add("junk", 1);
                self.try_next(avid, ctx).await?;
            }
            Outcome::NeedsAttention | Outcome::Failed => {
                // Release the claim so the work is retried, and park the AVID when a human has
                // to look at it.
                self.ledger
                    .transition_attempt(
                        avid,
                        attempt_no,
                        AttemptState::Archiving,
                        AttemptState::Finished,
                        now,
                        None,
                    )
                    .await?;
                if result.outcome == Outcome::NeedsAttention {
                    self.ledger
                        .transition(
                            avid,
                            AcquisitionState::Downloading,
                            AcquisitionState::NeedsAttention,
                            now,
                            TransitionFields::note(result.reason.clone()),
                        )
                        .await?;
                    ctx.add("needs_attention", 1);
                }
            }
        }
        Ok(())
    }

    /// `_give_up`: conclude the attempt and move to the next magnet; true when this call did
    /// it. The transition is the claim: false means someone else concluded it first.
    async fn give_up(
        &self,
        attempt: &MagnetAttemptRecord,
        state: AttemptState,
        reason: &str,
        ctx: &RunContext,
    ) -> Result<bool, TrackerError> {
        let now = (self.clock)();
        if !self
            .ledger
            .transition_attempt(
                &attempt.avid,
                attempt.attempt_no,
                attempt.state,
                state,
                now,
                Some(reason.to_owned()),
            )
            .await?
        {
            return Ok(false);
        }
        ctx.warning(&format!("{}: {reason}", attempt.avid));
        ctx.add(state.as_str(), 1);
        self.try_next(&attempt.avid, ctx).await?;
        Ok(true)
    }

    /// `_remove_task`: drop an abandoned offline task at CloudDrive, downloaded data included.
    /// Best-effort: a failure never blocks the switch to the next magnet.
    async fn remove_task(&self, info_hash: &str, task_dir: &str, ctx: &RunContext) {
        let hashes = [info_hash.to_owned()];
        match self.cloud.remove_offline_files(&hashes, task_dir, true).await {
            Err(error) => ctx.exception(
                &format!("failed to remove the abandoned offline task {info_hash} under {task_dir}"),
                Some(&error),
            ),
            Ok(()) => ctx.add("tasks_removed", 1),
        }
    }

    /// `_try_next`: submit this AVID's next magnet, or mark it out of options. Python recurses
    /// after a refused submission; this loops.
    async fn try_next(&self, avid: &str, ctx: &RunContext) -> Result<(), TrackerError> {
        loop {
            let Some(record) = self.ledger.get(avid).await? else {
                return Ok(());
            };
            if record.state != AcquisitionState::Downloading {
                // Nobody's to retry: an operator ignored or parked it, or it is archived. The
                // remaining magnets stay put until a deliberate retry hands it back.
                return Ok(());
            }
            let attempts = self.ledger.attempts_for(avid).await?;
            // The cap counts magnets actually submitted; a replacement counts only its own.
            let floor = record.replace_after_attempt.unwrap_or(0);
            let tried = attempts
                .iter()
                .filter(|attempt| attempt.state != AttemptState::Pending && attempt.attempt_no > floor)
                .count();
            if i64::try_from(tried).unwrap_or(i64::MAX) >= self.settings.max_attempts {
                return self.exhaust(avid, ctx).await;
            }
            let now = (self.clock)();
            let Some(claimed) = self.ledger.claim_next_pending(avid, now).await? else {
                return self.exhaust(avid, ctx).await;
            };
            if (self.submit)(avid.to_owned(), claimed.magnet.clone()).await {
                ctx.info(&format!("{avid}: trying magnet {}", claimed.attempt_no));
                ctx.add("retried", 1);
                return Ok(());
            }
            self.ledger
                .transition_attempt(
                    avid,
                    claimed.attempt_no,
                    AttemptState::Submitted,
                    AttemptState::Error,
                    now,
                    Some("failed to add the offline task".to_owned()),
                )
                .await?;
        }
    }

    async fn exhaust(&self, avid: &str, ctx: &RunContext) -> Result<(), TrackerError> {
        let Some(record) = self.ledger.get(avid).await? else {
            return Ok(());
        };
        if record.state != AcquisitionState::Downloading {
            return Ok(());
        }
        let release_date =
            ensure_release_date(self.ledger.as_ref(), &record, self.release_date_lookup.as_deref()).await?;
        let now = (self.clock)();
        // The resolver may find new magnets once the release has aged; the schedule says when
        // to look, the stall timeout only when no release date is known.
        let fallback = py_timedelta_hours(self.settings.stall_timeout_hours)?;
        let next_action_at = next_resolve_at(now, release_date, fallback)?;
        self.ledger
            .transition(
                avid,
                AcquisitionState::Downloading,
                AcquisitionState::Exhausted,
                now,
                TransitionFields {
                    note: Some("every magnet was tried".to_owned()),
                    next_action_at: Some(next_action_at),
                    archived_paths: None,
                },
            )
            .await?;
        ctx.warning(&format!("{avid}: every magnet was tried"));
        ctx.add("exhausted", 1);
        Ok(())
    }

    // -- tasks that vanished --------------------------------------------------

    /// `_sweep_lost`: attempts CloudDrive no longer lists, i.e. records cleared behind our back.
    async fn sweep_lost(&self, live_hashes: &HashSet<&str>, ctx: &RunContext) -> Result<(), TrackerError> {
        let grace = py_timedelta_seconds(self.settings.submit_grace_seconds)?;
        let now = (self.clock)();
        for attempt in self.ledger.in_flight_attempts().await? {
            ctx.check_cancelled()?;
            let Some(info_hash) = &attempt.info_hash else {
                continue;
            };
            if live_hashes.contains(info_hash.as_str()) {
                continue;
            }
            if let Some(submitted_at) = attempt.submitted_at
                && now - submitted_at < grace
            {
                continue;
            }
            let located = blocking(|| self.find_folder_for(&attempt.avid))?;
            if let Some((folder, route)) = located {
                // The download did land; only its bookkeeping went away.
                self.archive_orphaned(&attempt, &folder, &route, ctx).await?;
                continue;
            }
            self.give_up(
                &attempt,
                AttemptState::Lost,
                "CloudDrive no longer lists this task",
                ctx,
            )
            .await?;
        }
        Ok(())
    }

    // -- locating a download in the routes ------------------------------------

    /// `_locate`: the downloaded folder and the route holding it; the exact task name first,
    /// the AVID second so a renamed folder is still found.
    fn locate(&self, name: &str, avid: &str) -> Result<Option<(PathBuf, TrackerRoute)>, PyOsError> {
        let fs = self.archiver.fs();
        for route in &self.settings.routes {
            let candidate = PathBuf::from(pypath::join(&path_text(&route.root), name));
            if fs::is_dir(&**fs, &candidate)? {
                return Ok(Some((candidate, route.clone())));
            }
        }
        self.find_folder_for(avid)
    }

    /// `_find_folder_for`: the route folder holding this AVID, matched by the AVID its name
    /// reads as (a vanished task takes its folder name with it).
    fn find_folder_for(&self, avid: &str) -> Result<Option<(PathBuf, TrackerRoute)>, PyOsError> {
        let fs = self.archiver.fs();
        for route in &self.settings.routes {
            if !fs::is_dir(&**fs, &route.root)? {
                continue;
            }
            let mut folders = fs.iterdir(&route.root)?;
            folders.sort();
            for folder in folders {
                if fs::is_dir(&**fs, &folder)? && self.archiver.avid_of(&name_of(&folder)) == avid {
                    return Ok(Some((folder, route.clone())));
                }
            }
        }
        Ok(None)
    }

    async fn archive_orphaned(
        &self,
        attempt: &MagnetAttemptRecord,
        folder: &Path,
        route: &TrackerRoute,
        ctx: &RunContext,
    ) -> Result<(), TrackerError> {
        let now = (self.clock)();
        if !self
            .ledger
            .transition_attempt(
                &attempt.avid,
                attempt.attempt_no,
                attempt.state,
                AttemptState::Finished,
                now,
                None,
            )
            .await?
        {
            return Ok(());
        }
        if !self
            .ledger
            .transition_attempt(
                &attempt.avid,
                attempt.attempt_no,
                AttemptState::Finished,
                AttemptState::Archiving,
                now,
                None,
            )
            .await?
        {
            return Ok(());
        }
        let result = self.file(attempt, folder, route, ctx).await?;
        self.settle_attempt(attempt, &result, ctx).await
    }

    /// `_refresh_mount`: make CloudDrive re-read the finished task's directory, parent first
    /// (under a persistent directory cache its listing never expires on its own).
    async fn refresh_mount(&self, task: &OfflineTask, task_dir: &str, ctx: &RunContext) {
        let path = format!("{task_dir}/{}", task.name);
        let refreshed = async {
            self.cloud.list_directory(task_dir).await?;
            self.cloud.list_directory(&path).await?;
            Ok::<(), SourceError>(())
        }
        .await;
        match refreshed {
            Ok(()) => {}
            Err(error) if is_file_not_found(&error) => {
                ctx.warning(&format!("finished task {} is not on the mount yet", task.name));
            }
            Err(error) => ctx.exception(&format!("failed to refresh {path}"), Some(&error)),
        }
    }
}

#[cfg(test)]
mod tests;
