//! Merging titles one after another without anyone clicking 合并.
//!
//! Python origin: `src/embyx_manager/merge/auto.py`. With `merge.auto_enabled` on, the next
//! title is queued only while no task is open, so each title is merged, uploaded, checked and
//! filed before the next one starts. A failed task is left for someone to retry or remove and
//! its title is skipped; with `merge.auto_pause_on_failure` on, a failure pauses this instead.
//! A title whose task was removed, cancelled or already merged is never picked again, nor one
//! the library already holds whole. A title waiting on the home transcode worker while the
//! worker is away does not count as under way. A title is queued only when its parts fit on
//! the work volume with the reserve to spare and room left for what home transcodes are yet
//! to write.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};
use embyx_core::config::models::MergeConfig;
use embyx_core::pypath;
use tokio_util::sync::CancellationToken;

use crate::catalog::{
    AutoState, AutoStatus, EnqueueError, MergeCatalog, MergeRequest, SkippedTitle, TitleScanCache, current_title,
    enqueue,
};
use crate::detect::{MAX_STACKED_PARTS, MultipartTitle, whole_copies};
use crate::ports::{Clock, system_clock};
use crate::service::{CloudSource, MergeService, Section, StepError, api_path, pure_parent};
use crate::tasks::{MergeState, MergeTask, MergeTaskRepository};

const LOGGER: &str = "embyx_manager.merge.auto";

/// `MergeRequest.avid`'s `max_length`.
const MAX_AVID_CHARS: usize = 64;

/// pydantic's `ValidationError` for an AVID longer than [`MAX_AVID_CHARS`]: the input's
/// `repr` cut to its first 25 and last 24 characters when longer than 50.
fn too_long(avid: &str) -> StepError {
    let repr: Vec<char> = embyx_core::pytext::py_repr(avid).chars().collect();
    let shown: String = if repr.len() > 50 {
        let head: String = repr[..25].iter().collect();
        let tail: String = repr[repr.len() - 24..].iter().collect();
        format!("{head}...{tail}")
    } else {
        repr.into_iter().collect()
    };
    StepError::from(embyx_clients::pyvalue::Raised::new(
        "ValidationError",
        format!(
            "1 validation error for MergeRequest\navid\n  String should have at most {MAX_AVID_CHARS} characters \
             [type=string_too_long, input_value={shown}, input_type=str]\n    \
             For further information visit https://errors.pydantic.dev/2.13/v/string_too_long"
        ),
    ))
}

/// `POLL_SECONDS`.
pub const POLL_SECONDS: f64 = 60.0;
/// `RESCAN_INTERVAL`: scan the library again at least this often, for titles that arrived
/// since.
pub fn rescan_interval() -> Duration {
    Duration::hours(24)
}
/// `SKIPPED_SHOWN`: how many skipped titles the status names; the rest are only counted.
pub const SKIPPED_SHOWN: usize = 20;
/// `GIB`.
pub const GIB: i128 = 1 << 30;
/// `WHOLE_COPY`: the skip reason of a title the library already holds whole.
pub const WHOLE_COPY: &str = "whole_copy";

/// `free_space(path)`: the bytes free on the work volume.
pub type FreeSpace = Arc<dyn Fn(&Path) -> std::io::Result<i128> + Send + Sync>;

/// `free_bytes(path)`: `shutil.disk_usage(path).free`.
pub fn free_bytes(path: &Path) -> std::io::Result<i128> {
    crate::worker::disk_free(path).map(i128::from)
}

/// What an [`AutoMerger`] is built from (`AutoMerger(...)`'s keyword arguments).
#[expect(missing_debug_implementations, reason = "closures")]
pub struct AutoParts {
    pub repository: MergeTaskRepository,
    pub catalog: MergeCatalog,
    pub cache: TitleScanCache,
    pub service: MergeService,
    pub merge_config: Section<MergeConfig>,
    pub cloud: CloudSource,
    pub free_space: FreeSpace,
    pub poll_seconds: f64,
    pub clock: Clock,
}

impl AutoParts {
    /// The parts with Python's defaults: free space from the disk, 60 s polls, the wall clock.
    pub fn new(
        repository: MergeTaskRepository,
        catalog: MergeCatalog,
        cache: TitleScanCache,
        service: MergeService,
        merge_config: Section<MergeConfig>,
        cloud: CloudSource,
    ) -> Self {
        Self {
            repository,
            catalog,
            cache,
            service,
            merge_config,
            cloud,
            free_space: Arc::new(free_bytes),
            poll_seconds: POLL_SECONDS,
            clock: system_clock(),
        }
    }
}

#[derive(Default)]
struct Refused {
    /// Titles refused for a reason only a new scan can change.
    titles: HashMap<String, SkippedTitle>,
    /// The scan they came from.
    scan: Option<DateTime<Utc>>,
}

struct Inner {
    repository: MergeTaskRepository,
    catalog: MergeCatalog,
    cache: TitleScanCache,
    service: MergeService,
    merge_config: Section<MergeConfig>,
    cloud: CloudSource,
    free_space: FreeSpace,
    poll: StdDuration,
    clock: Clock,
    /// Part sizes never change, so each title's total is read from CloudDrive once.
    sizes: Mutex<HashMap<Vec<String>, i128>>,
    refused: Mutex<Refused>,
    status: Mutex<AutoStatus>,
    running: Mutex<Option<(tokio::task::JoinHandle<()>, CancellationToken)>>,
}

/// `AutoMerger`. Clones share it.
#[derive(Clone)]
pub struct AutoMerger {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for AutoMerger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutoMerger")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

fn eligible(title: &MultipartTitle, config: &MergeConfig, settled: &BTreeSet<String>) -> bool {
    if !title.complete() || settled.contains(&title.avid) {
        return false;
    }
    config.auto_include_stackable || title.part_count() > MAX_STACKED_PARTS
}

impl AutoMerger {
    pub fn new(parts: AutoParts) -> Self {
        Self {
            inner: Arc::new(Inner {
                repository: parts.repository,
                catalog: parts.catalog,
                cache: parts.cache,
                service: parts.service,
                merge_config: parts.merge_config,
                cloud: parts.cloud,
                free_space: parts.free_space,
                poll: StdDuration::from_secs_f64(parts.poll_seconds.max(0.0)),
                clock: parts.clock,
                sizes: Mutex::new(HashMap::new()),
                refused: Mutex::new(Refused::default()),
                status: Mutex::new(AutoStatus::of(AutoState::Off)),
                running: Mutex::new(None),
            }),
        }
    }

    /// `status()`.
    pub fn status(&self) -> AutoStatus {
        self.inner.status.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    fn set_status(&self, status: AutoStatus) {
        *self.inner.status.lock().unwrap_or_else(PoisonError::into_inner) = status;
    }

    /// `start()`.
    pub fn start(&self) {
        let token = CancellationToken::new();
        let merger = self.clone();
        let stop = token.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = stop.cancelled() => return,
                    outcome = merger.step() => {
                        if let Err(error) = outcome {
                            tracing::error!(logger = LOGGER, error = %error, "automatic merge round failed");
                        }
                    }
                }
                tokio::select! {
                    biased;
                    () = stop.cancelled() => return,
                    () = tokio::time::sleep(merger.inner.poll) => {}
                }
            }
        });
        *self.inner.running.lock().unwrap_or_else(PoisonError::into_inner) = Some((handle, token));
    }

    /// `aclose()`.
    pub async fn aclose(&self) {
        let running = self.inner.running.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some((handle, token)) = running {
            token.cancel();
            let _ = handle.await;
        }
    }

    /// `step()`: queue the next title that fits, when nothing else is under way.
    pub async fn step(&self) -> Result<(), StepError> {
        let inner = &self.inner;
        let config = (inner.merge_config)();
        let open_tasks = inner.repository.open_tasks().await?;
        if let Some(blocked) = self.blocked(&config, &open_tasks) {
            self.set_status(AutoStatus::of(blocked));
            return Ok(());
        }
        let Some((titles, scanned_at)) = self.titles().await? else {
            self.set_status(AutoStatus::of(AutoState::Unavailable));
            return Ok(());
        };
        {
            let mut refused = inner.refused.lock().unwrap_or_else(PoisonError::into_inner);
            if refused.scan != Some(scanned_at) {
                *refused = Refused {
                    titles: HashMap::new(),
                    scan: Some(scanned_at),
                };
            }
        }
        let work_dir = crate::service::py_path(&config.work_dir);
        let free = (inner.free_space)(&work_dir).map_err(|error| crate::service::os_error(&error, &work_dir))?;
        let room = free
            - i128::from(config.free_space_reserve_gib) * GIB
            // What a home transcode is yet to write.
            - inner.service.reserved_bytes(&open_tasks);
        let mut settled = inner.repository.cancelled_avids().await?;
        settled.extend(inner.repository.merged_avids().await?);
        let failed: BTreeSet<&str> = open_tasks
            .iter()
            .filter(|task| task.state == MergeState::Failed)
            .map(|task| task.avid.as_str())
            .collect();
        let mut skipped: Vec<SkippedTitle> = Vec::new();
        for title in titles.iter() {
            if !eligible(title, &config, &settled) {
                continue;
            }
            if failed.contains(title.avid.as_str()) {
                skipped.push(SkippedTitle {
                    avid: title.avid.clone(),
                    size: None,
                    reason: "failed".to_owned(),
                });
                continue;
            }
            if self.try_queue(title, &config, room, &mut skipped).await? {
                self.set_status(self.report(AutoState::Busy, room, skipped));
                return Ok(());
            }
        }
        self.set_status(self.report(AutoState::Idle, room, skipped));
        Ok(())
    }

    /// `_blocked`: why no title may be queued now, or `None` when one may.
    fn blocked(&self, config: &MergeConfig, open_tasks: &[MergeTask]) -> Option<AutoState> {
        if !config.auto_enabled {
            return Some(AutoState::Off);
        }
        if self.inner.service.unavailable().is_some() {
            return Some(AutoState::Unavailable);
        }
        let failed = open_tasks.iter().any(|task| task.state == MergeState::Failed);
        if failed && config.auto_pause_on_failure {
            return Some(AutoState::Paused);
        }
        // A title waiting on the home transcode worker while it is away holds up nothing.
        let under_way = open_tasks
            .iter()
            .any(|task| task.state != MergeState::Failed && !self.inner.service.parked(task));
        under_way.then_some(AutoState::Busy)
    }

    /// `_try_queue`: queue `title` when it fits and can be merged; otherwise note why in
    /// `skipped`.
    async fn try_queue(
        &self,
        title: &MultipartTitle,
        config: &MergeConfig,
        room: i128,
        skipped: &mut Vec<SkippedTitle>,
    ) -> Result<bool, StepError> {
        let inner = &self.inner;
        let known = inner
            .refused
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .titles
            .get(&title.avid)
            .cloned();
        if let Some(known) = known {
            skipped.push(known);
            return Ok(false);
        }
        let (size, reason) = self.size(title, config).await;
        let Some(size) = size.filter(|size| *size <= room) else {
            skipped.push(SkippedTitle {
                avid: title.avid.clone(),
                size,
                reason: reason.unwrap_or("too_big").to_owned(),
            });
            return Ok(false);
        };
        if let Some(reason) = self.unlisted(title, config).await {
            let targets: Vec<String> = title.parts.iter().map(|part| part.target.clone()).collect();
            inner
                .sizes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&targets);
            let refused = SkippedTitle {
                avid: title.avid.clone(),
                size: None,
                reason: reason.to_owned(),
            };
            if reason == WHOLE_COPY {
                self.remember_refusal(&refused);
            }
            skipped.push(refused);
            return Ok(false);
        }
        let current = current_title(&inner.catalog, &inner.cache, &title.avid).await?;
        // `MergeRequest(avid=title.avid)` is validated where it is built: an AVID over 64
        // characters (a strm stem has no limit) raises, and the round ends there each time.
        if title.avid.chars().count() > MAX_AVID_CHARS {
            return Err(too_long(&title.avid));
        }
        let request = MergeRequest {
            avid: title.avid.clone(),
            source: None,
        };
        let archive = inner.catalog.archive();
        let task = match enqueue(&inner.catalog, &inner.repository, &archive, current.as_ref(), &request).await {
            Ok(task) => task,
            Err(EnqueueError::Refused { code, .. }) => {
                let refused = SkippedTitle {
                    avid: title.avid.clone(),
                    size: Some(size),
                    reason: code.to_owned(),
                };
                self.remember_refusal(&refused);
                skipped.push(refused);
                return Ok(false);
            }
            Err(EnqueueError::Error(error)) => return Err(error),
        };
        tracing::info!(
            logger = LOGGER,
            "queued {} ({} parts, {size} bytes) for automatic merging",
            task.avid,
            title.part_count()
        );
        inner.service.wake();
        Ok(true)
    }

    fn remember_refusal(&self, refused: &SkippedTitle) {
        self.inner
            .refused
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .titles
            .insert(refused.avid.clone(), refused.clone());
    }

    /// `_report`.
    fn report(&self, state: AutoState, room: i128, skipped: Vec<SkippedTitle>) -> AutoStatus {
        let skipped_count = skipped.len();
        AutoStatus {
            state,
            room: Some(room.max(0)),
            skipped: skipped.into_iter().take(SKIPPED_SHOWN).collect(),
            skipped_count,
            checked_at: Some((self.inner.clock)()),
        }
    }

    /// `_titles`: the last scan, scanned again when a title was filed since or it has grown
    /// old.
    async fn titles(&self) -> Result<Option<(Arc<Vec<MultipartTitle>>, DateTime<Utc>)>, StepError> {
        let inner = &self.inner;
        let archive = inner.catalog.archive();
        let mapping = inner.catalog.mapping();
        if mapping.dst_dir.is_empty() || archive.dst_dir.is_empty() {
            return Ok(None);
        }
        let root = crate::service::py_path(&mapping.dst_dir);
        let (titles, scanned_at) = inner.cache.titles(&root, &archive.dst_dir, false).await?;
        let last_done = inner.repository.last_done_at().await?;
        let stale = (inner.clock)() - scanned_at > rescan_interval();
        if stale || last_done.is_some_and(|last_done| last_done > scanned_at) {
            return Ok(Some(inner.cache.titles(&root, &archive.dst_dir, true).await?));
        }
        Ok(Some((titles, scanned_at)))
    }

    /// `_size`: the parts' total size as CloudDrive lists them, or why it is unknown.
    async fn size(&self, title: &MultipartTitle, config: &MergeConfig) -> (Option<i128>, Option<&'static str>) {
        let targets: Vec<String> = title.parts.iter().map(|part| part.target.clone()).collect();
        if let Some(size) = self
            .inner
            .sizes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&targets)
            .copied()
        {
            return (Some(size), None);
        }
        let Some(cloud) = (self.inner.cloud)() else {
            return (None, Some("size_unknown"));
        };
        let directory = pure_parent(targets.first().map_or("", String::as_str));
        let listed = match api_path(&directory, &config.cloud_mount_prefix) {
            Ok(api_dir) => cloud.list_directory(&api_dir, false).await.map_err(StepError::from),
            Err(error) => Err(error),
        };
        let listing = match listed {
            Ok(listing) => listing,
            Err(error) => {
                tracing::error!(
                    logger = LOGGER,
                    error = %error,
                    "could not list {directory} for automatic merging"
                );
                return (None, Some("size_unknown"));
            }
        };
        let mut sizes: HashMap<&str, i128> = HashMap::new();
        for entry in listing.iter().filter(|entry| !entry.is_directory) {
            sizes.insert(entry.name.as_str(), i128::from(entry.size));
        }
        let mut total: i128 = 0;
        for target in &targets {
            let Some(size) = sizes.get(pypath::name(target)) else {
                return (None, Some("parts_missing"));
            };
            total += size;
        }
        self.inner
            .sizes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(targets, total);
        (Some(total), None)
    }

    /// `_unlisted`: why CloudDrive, asked afresh, does not list every part alone, or `None`
    /// when it does. Sizes come from a cache, but parts leave once a title is merged, and the
    /// mapping tree may not show yet a whole copy filed since the scan.
    async fn unlisted(&self, title: &MultipartTitle, config: &MergeConfig) -> Option<&'static str> {
        let Some(cloud) = (self.inner.cloud)() else {
            return Some("size_unknown");
        };
        let first = title.parts.first().map_or("", |part| part.target.as_str());
        let directory = pure_parent(first);
        let listed = match api_path(&directory, &config.cloud_mount_prefix) {
            Ok(api_dir) => cloud.list_directory(&api_dir, true).await.map_err(StepError::from),
            Err(error) => Err(error),
        };
        let listing = match listed {
            Ok(listing) => listing,
            Err(error) => {
                tracing::error!(
                    logger = LOGGER,
                    error = %error,
                    "could not list {directory} for automatic merging"
                );
                return Some("size_unknown");
            }
        };
        let names: BTreeSet<&str> = listing
            .iter()
            .filter(|entry| !entry.is_directory)
            .map(|entry| entry.name.as_str())
            .collect();
        if !title
            .parts
            .iter()
            .all(|part| names.contains(pypath::name(&part.target)))
        {
            return Some("parts_missing");
        }
        if whole_copies(&title.avid, names.iter().copied()).is_empty() {
            None
        } else {
            Some(WHOLE_COPY)
        }
    }
}

#[cfg(test)]
mod tests {
    /// Read off Python: `MergeRequest(avid='A' * 70)`.
    #[test]
    fn an_avid_over_64_characters_is_pydantics_validation_error() {
        let error = super::too_long(&"A".repeat(70));
        assert_eq!(error.class_name(), "ValidationError");
        assert_eq!(
            error.to_string(),
            "1 validation error for MergeRequest\navid\n  String should have at most 64 characters \
             [type=string_too_long, input_value='AAAAAAAAAAAAAAAAAAAAAAAA...AAAAAAAAAAAAAAAAAAAAAAA', \
             input_type=str]\n    For further information visit \
             https://errors.pydantic.dev/2.13/v/string_too_long"
        );
    }
}
