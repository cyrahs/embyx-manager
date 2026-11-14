//! What the merge page and auto-merge share: the scanned titles, the queueing of one, and the
//! state of automatic merging.
//!
//! Python origin: the parts of `src/embyx_manager/merge/api.py` that are not HTTP:
//! `MergeCatalog`, `TitleScanCache`, `current_title`, `enqueue`, `MergeRequest`, `AutoState`,
//! `SkippedTitle` and `AutoStatus`. The routes themselves are `embyx-api`'s.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use embyx_core::config::models::{ArchiveConfig, MappingConfig};
use embyx_core::pypath;

use crate::detect::{MultipartTitle, resolve_source, route_sources, scan_multipart};
use crate::ports::{BoxFuture, Clock, system_clock};
use crate::service::{Section, StepError};
use crate::tasks::{MergeTask, MergeTaskRepository, NewTask, TaskError};
use crate::worker::muxer;

/// `task_dirs_for(avids)`: the acquisition ledger's offline directory per title.
pub type TaskDirsFor =
    Arc<dyn Fn(Vec<String>) -> BoxFuture<'static, Result<HashMap<String, String>, StepError>> + Send + Sync>;

/// `MergeCatalog`: what the titles endpoint reads, the two config sections and the ledger's
/// offline directories.
#[derive(Clone)]
pub struct MergeCatalog {
    pub archive: Section<ArchiveConfig>,
    pub mapping: Section<MappingConfig>,
    pub task_dirs_for: TaskDirsFor,
}

impl std::fmt::Debug for MergeCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergeCatalog").finish_non_exhaustive()
    }
}

impl MergeCatalog {
    pub fn archive(&self) -> ArchiveConfig {
        (self.archive)()
    }

    pub fn mapping(&self) -> MappingConfig {
        (self.mapping)()
    }
}

#[derive(Default)]
struct Scan {
    key: Option<(String, String)>,
    titles: Arc<Vec<MultipartTitle>>,
    scanned_at: Option<DateTime<Utc>>,
}

/// `TitleScanCache`: the last full scan of the mapping tree. Walking every strm over the
/// network share takes the better part of a minute, so the page reads this copy until someone
/// asks for a fresh one. Clones share the scan.
#[derive(Clone)]
pub struct TitleScanCache {
    scan: Arc<tokio::sync::Mutex<Scan>>,
    clock: Clock,
}

impl std::fmt::Debug for TitleScanCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TitleScanCache").finish_non_exhaustive()
    }
}

impl Default for TitleScanCache {
    fn default() -> Self {
        Self::new(system_clock())
    }
}

/// `asyncio.to_thread(scan_multipart, root, library_root=...)`.
async fn scan_in_thread(root: PathBuf, library_root: String) -> Result<Vec<MultipartTitle>, StepError> {
    tokio::task::spawn_blocking(move || scan_multipart(&root, &library_root))
        .await
        .map_err(|error| StepError::runtime(error.to_string()))
}

impl TitleScanCache {
    pub fn new(clock: Clock) -> Self {
        Self {
            scan: Arc::new(tokio::sync::Mutex::new(Scan::default())),
            clock,
        }
    }

    /// `titles(root, library_root, refresh=...)`: the scan and when it was made.
    pub async fn titles(
        &self,
        root: &Path,
        library_root: &str,
        refresh: bool,
    ) -> Result<(Arc<Vec<MultipartTitle>>, DateTime<Utc>), StepError> {
        let requested_at = (self.clock)();
        let key = (root.to_string_lossy().into_owned(), library_root.to_owned());
        let mut scan = self.scan.lock().await;
        // A refresh that waited on another one already has a scan newer than itself.
        let stale = match scan.scanned_at {
            None => true,
            Some(scanned_at) => scan.key.as_ref() != Some(&key) || (refresh && scanned_at < requested_at),
        };
        if stale {
            scan.titles = Arc::new(scan_in_thread(root.to_path_buf(), library_root.to_owned()).await?);
            scan.key = Some(key);
            scan.scanned_at = Some((self.clock)());
        }
        let scanned_at = scan.scanned_at.unwrap_or(requested_at);
        Ok((Arc::clone(&scan.titles), scanned_at))
    }
}

/// `_scan`: the archive config and every multi-part title; `None` when the directories are
/// not configured.
pub async fn scan(
    catalog: &MergeCatalog,
    cache: &TitleScanCache,
    refresh: bool,
) -> Result<(ArchiveConfig, Option<(Arc<Vec<MultipartTitle>>, DateTime<Utc>)>), StepError> {
    let archive = catalog.archive();
    let mapping = catalog.mapping();
    if mapping.dst_dir.is_empty() || archive.dst_dir.is_empty() {
        return Ok((archive, None));
    }
    let scanned = cache
        .titles(&crate::service::py_path(&mapping.dst_dir), &archive.dst_dir, refresh)
        .await?;
    Ok((archive, Some(scanned)))
}

/// Aborts the task it holds when dropped, so cancelling the warm-up cancels its scan.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The text of a panic, as `str(exc)` would give it.
fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|text| (*text).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "the scan panicked".to_owned())
}

/// `warm_title_cache`: scan once at startup so the first visit after a deploy does not wait on
/// the walk. Python logs any exception (`except Exception`); a section getter that panics is
/// this build's raising one, so the scan runs as a task of its own and a panic is logged too.
pub async fn warm_title_cache(catalog: &MergeCatalog, cache: &TitleScanCache) {
    let (catalog, cache) = (catalog.clone(), cache.clone());
    let mut task = AbortOnDrop(tokio::spawn(
        async move { scan(&catalog, &cache, false).await.map(drop) },
    ));
    let error = match (&mut task.0).await {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error.to_string(),
        Err(join) if join.is_panic() => panic_text(join.into_panic().as_ref()),
        Err(join) => join.to_string(),
    };
    tracing::error!(
        logger = "embyx_manager.merge.api",
        error = %error,
        "could not scan the mapping tree for multi-part titles"
    );
}

/// `str.upper()`.
fn upper(text: &str) -> String {
    text.to_uppercase()
}

/// `current_title`: the title as its directory holds it now, found through the last scan.
pub async fn current_title(
    catalog: &MergeCatalog,
    cache: &TitleScanCache,
    avid: &str,
) -> Result<Option<MultipartTitle>, StepError> {
    let wanted = upper(embyx_core::pytext::py_strip(avid));
    let mut found = None;
    for refresh in [false, true] {
        let (_archive, scanned) = scan(catalog, cache, refresh).await?;
        let Some((titles, _)) = scanned else {
            return Ok(None);
        };
        found = titles.iter().find(|title| upper(&title.avid) == wanted).cloned();
        if found.is_some() {
            break;
        }
    }
    let Some(found) = found else {
        return Ok(None);
    };
    let root = crate::service::py_path(&catalog.mapping().dst_dir);
    let archive = catalog.archive();
    let rescanned = scan_in_thread(root.join(&found.directory), archive.dst_dir).await?;
    Ok(rescanned.into_iter().find(|title| upper(&title.avid) == wanted))
}

/// `MergeRequest`: what someone asks to merge. The field limits are the API's to check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeRequest {
    pub avid: String,
    /// The intake route to re-enter through; required when the title's own cannot be told.
    pub source: Option<String>,
}

/// Why `enqueue` did not queue the title.
#[derive(Debug, Clone, PartialEq)]
pub enum EnqueueError {
    /// `ApiError(status, code)`.
    Refused { status: u16, code: &'static str },
    /// Anything else Python lets through.
    Error(StepError),
}

impl std::fmt::Display for EnqueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused { code, .. } => f.write_str(code),
            Self::Error(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for EnqueueError {}

impl From<StepError> for EnqueueError {
    fn from(error: StepError) -> Self {
        Self::Error(error)
    }
}

fn refuse(status: u16, code: &'static str) -> EnqueueError {
    EnqueueError::Refused { status, code }
}

/// `enqueue`: queue one scanned title, after checking it can be merged and re-enter
/// somewhere.
pub async fn enqueue(
    catalog: &MergeCatalog,
    repository: &MergeTaskRepository,
    archive: &ArchiveConfig,
    title: Option<&MultipartTitle>,
    request: &MergeRequest,
) -> Result<MergeTask, EnqueueError> {
    let Some(title) = title else {
        return Err(refuse(404, "merge_title_not_found"));
    };
    let (Some(library_dir), Some(brand)) = (title.library_dir.as_ref(), title.brand.as_ref()) else {
        return Err(refuse(422, "merge_title_incomplete"));
    };
    if !title.complete() {
        return Err(refuse(422, "merge_title_incomplete"));
    }
    let mut suffixes: Vec<String> = title
        .parts
        .iter()
        .map(|part| pypath::suffix(&part.target).to_lowercase())
        .collect();
    suffixes.sort();
    suffixes.dedup();
    // `next(iter(suffixes))` of a set: with more than one the title is refused anyway.
    let suffix = suffixes
        .first()
        .cloned()
        .ok_or_else(|| EnqueueError::Error(StepError::raised("StopIteration", "")))?;
    if suffixes.len() != 1 || muxer(&suffix).is_none() {
        return Err(refuse(422, "merge_container_unsupported"));
    }
    let source = if let Some(source) = &request.source {
        Some(source.clone())
    } else {
        let ledger = (catalog.task_dirs_for)(vec![title.avid.clone()]).await?;
        resolve_source(archive, title, ledger.get(&title.avid).map(String::as_str)).source
    };
    let Some(source) = source else {
        return Err(refuse(422, "merge_source_required"));
    };
    if !route_sources(archive).contains(&source) {
        return Err(refuse(422, "merge_source_unknown"));
    }
    let created = repository
        .create(&NewTask {
            avid: title.avid.clone(),
            source,
            library_dir: library_dir.clone(),
            brand: brand.clone(),
            parts: title.parts.iter().map(|part| part.target.clone()).collect(),
            merged_name: format!("{}{suffix}", title.avid),
        })
        .await;
    match created {
        Ok(task) => Ok(task),
        Err(TaskError::Conflict(_)) => Err(refuse(409, "merge_task_exists")),
        Err(error) => Err(EnqueueError::Error(error.into())),
    }
}

/// `AutoState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoState {
    Off,
    /// Merging cannot run in this deployment, or the library is not configured.
    Unavailable,
    /// A task is under way; the next title waits for it to be filed.
    Busy,
    /// A task failed; nothing more is queued until it is retried or removed.
    Paused,
    /// Nothing left that fits; the next round looks again.
    Idle,
}

impl AutoState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Unavailable => "unavailable",
            Self::Busy => "busy",
            Self::Paused => "paused",
            Self::Idle => "idle",
        }
    }
}

/// `SkippedTitle`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedTitle {
    pub avid: String,
    /// The parts' total size, when it could be read.
    pub size: Option<i128>,
    /// `too_big`, `parts_missing`, `size_unknown` or the enqueue refusal's error code.
    pub reason: String,
}

/// `AutoStatus`: where automatic merging stands (`merge.auto`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoStatus {
    pub state: AutoState,
    /// What a merge may take on the work volume right now: free space minus the reserve.
    pub room: Option<i128>,
    /// The first titles skipped in the last round; `skipped_count` counts them all.
    pub skipped: Vec<SkippedTitle>,
    pub skipped_count: usize,
    pub checked_at: Option<DateTime<Utc>>,
}

impl AutoStatus {
    /// `AutoStatus(state=state)`.
    pub fn of(state: AutoState) -> Self {
        Self {
            state,
            room: None,
            skipped: Vec::new(),
            skipped_count: 0,
            checked_at: None,
        }
    }
}
