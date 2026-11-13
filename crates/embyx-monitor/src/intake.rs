//! Shared acquisition intake: one AVID -> magnet candidates -> first offline task.
//!
//! Python origin: `src/embyx_manager/monitor/intake.py`. Every input source (the RSS
//! categories, fill actor, playlists, manual) funnels newly wanted AVIDs through this
//! component. It records the AVID in the acquisition ledger, resolves a ranked list of magnet
//! candidates, and submits the first one to CloudDrive; the tracker owns the download from
//! there. An AVID with no usable magnet is parked in `resolve_failed` until the resolve
//! schedule ([`crate::cadence`]) says to look again, and the tracker's retry pass hands it back
//! here at that time.
//!
//! Sources name the offline directory each submission is queued under, pinned on the
//! acquisition at discovery. The run-log lines keep Python's wording: they are stored in
//! `pipeline_runs.log_json` by the pipeline that owns the [`RunContext`].

use std::fmt::{self, Write as _};
use std::sync::Arc;

use chrono::{NaiveDate, TimeDelta};
use embyx_clients::clouddrive::proto::FileOperationResult;
use embyx_clients::clouddrive::{AsyncCloudDrive, CloudDriveError};
use embyx_clients::javbus::{JavBusClient, JavBusError, JavBusMagnet};
use embyx_clients::net::retry::{Sleeper, tokio_sleeper};
use embyx_clients::sukebei::{SukebeiClient, SukebeiError};
use embyx_clients::{http::HttpxError, tenacity};
use embyx_core::magnet::extract_info_hash;
use embyx_core::pytext::py_repr;

use crate::acquisitions::{
    AcquisitionRecord, AcquisitionState, AttemptState, Ledger, LedgerError, MagnetCandidate, Sighting, TransitionFields,
};
use crate::cadence::next_resolve_at;
use crate::release_dates::{ReleaseDateLookup, ensure_release_date};
use crate::reports::{PyException, Raised, RunContext, str_enum};
use crate::support::{BoxFuture, Clock, OverflowError, py_timedelta_seconds, system_clock};

/// `MAX_CANDIDATES`: magnets kept per resolve, best first.
pub const MAX_CANDIDATES: usize = 5;

/// `PARKABLE_STATES`: states a resolve pass that submits nothing parks from: a fresh
/// discovery, or a cooled-down row that a sighting woke for another look.
pub const PARKABLE_STATES: &[AcquisitionState] = &[
    AcquisitionState::Discovered,
    AcquisitionState::ResolveFailed,
    AcquisitionState::Exhausted,
];

str_enum! {
    /// `IntakeOutcome`: what became of one AVID handed to the intake.
    IntakeOutcome {
        Submitted = "submitted",
        Queued = "queued",
        AlreadyTracked = "already_tracked",
        NoMagnet = "no_magnet",
        SubmitFailed = "submit_failed",
    }
}

/// An error from a magnet source or from CloudDrive, by the Python exception it stands for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceError {
    Sukebei(SukebeiError),
    JavBus(JavBusError),
    CloudDrive(CloudDriveError),
    /// Any other exception (a test fake's `RuntimeError`, for one).
    Raised(Raised),
}

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sukebei(error) => error.fmt(f),
            Self::JavBus(error) => error.fmt(f),
            Self::CloudDrive(error) => error.fmt(f),
            Self::Raised(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for SourceError {}

impl From<SukebeiError> for SourceError {
    fn from(error: SukebeiError) -> Self {
        Self::Sukebei(error)
    }
}

impl From<JavBusError> for SourceError {
    fn from(error: JavBusError) -> Self {
        Self::JavBus(error)
    }
}

impl From<CloudDriveError> for SourceError {
    fn from(error: CloudDriveError) -> Self {
        Self::CloudDrive(error)
    }
}

impl From<Raised> for SourceError {
    fn from(error: Raised) -> Self {
        Self::Raised(error)
    }
}

/// The `OSError` family: what `CLOUD_RETRY` retries besides gRPC and httpx failures.
const OS_ERRORS: &[&str] = &[
    "OSError",
    "IOError",
    "EnvironmentError",
    "BlockingIOError",
    "ChildProcessError",
    "ConnectionError",
    "BrokenPipeError",
    "ConnectionAbortedError",
    "ConnectionRefusedError",
    "ConnectionResetError",
    "FileExistsError",
    "FileNotFoundError",
    "InterruptedError",
    "IsADirectoryError",
    "NotADirectoryError",
    "PermissionError",
    "ProcessLookupError",
    "TimeoutError",
];

impl SourceError {
    /// The status of an `httpx.HTTPStatusError`.
    pub fn http_status(&self) -> Option<u16> {
        match self {
            Self::Sukebei(SukebeiError::Http(error)) | Self::JavBus(JavBusError::Http(error)) => error.status(),
            _ => None,
        }
    }

    /// `exc.details()` of a `grpc.RpcError`.
    pub fn rpc_details(&self) -> Option<&str> {
        match self {
            Self::CloudDrive(error) => error.details(),
            _ => None,
        }
    }

    /// `retry_if_exception_type((grpc.RpcError, httpx.HTTPError, httpx.TimeoutException, OSError))`.
    pub fn is_cloud_retryable(&self) -> bool {
        match self {
            Self::CloudDrive(error) => matches!(
                error,
                CloudDriveError::Rpc(_) | CloudDriveError::FileNotFound { .. } | CloudDriveError::NotADirectory { .. }
            ),
            Self::Sukebei(SukebeiError::Http(error)) | Self::JavBus(JavBusError::Http(error)) => error.is_http_error(),
            Self::Sukebei(SukebeiError::Raised { class, .. }) => OS_ERRORS.contains(class),
            Self::JavBus(_) => false,
            Self::Raised(raised) => OS_ERRORS.contains(&raised.class.as_str()),
        }
    }
}

/// `repr()` of an httpx exception: built from its message.
fn httpx_repr(error: &HttpxError) -> String {
    format!("{}({})", error.class_name(), py_repr(&error.to_string()))
}

impl PyException for SourceError {
    fn class_name(&self) -> &str {
        match self {
            Self::Sukebei(error) => error.class_name(),
            Self::JavBus(error) => error.class_name(),
            Self::CloudDrive(error) => error.class_name(),
            Self::Raised(error) => &error.class,
        }
    }

    fn py_repr(&self) -> String {
        match self {
            Self::Sukebei(SukebeiError::Http(error)) | Self::JavBus(JavBusError::Http(error)) => httpx_repr(error),
            // grpcio's RPC errors have `__repr__ = __str__`.
            Self::CloudDrive(CloudDriveError::Rpc(error)) => error.to_string(),
            // `OSError(errno, strerror, filename)` keeps two arguments in its repr.
            Self::CloudDrive(CloudDriveError::FileNotFound { message, .. }) => {
                format!("FileNotFoundError(2, {})", py_repr(message))
            }
            Self::CloudDrive(CloudDriveError::NotADirectory { message, .. }) => {
                format!("NotADirectoryError(20, {})", py_repr(message))
            }
            _ => format!("{}({})", self.class_name(), py_repr(&self.to_string())),
        }
    }
}

/// What the intake asks sukebei: `get_magnet(avid)`, the best magnet or `None`.
pub trait SukebeiMagnets: Send + Sync {
    fn get_magnet<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Option<String>, SourceError>>;
}

/// What the intake asks JavBus: `get_magnets(avid)`, already ranked by the client.
pub trait JavBusMagnets: Send + Sync {
    fn get_magnets<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Vec<JavBusMagnet>, SourceError>>;
}

/// What the intake asks CloudDrive: `add_offline_files(urls, dst_dir)`.
pub trait OfflineFiles: Send + Sync {
    fn add_offline_files<'a>(
        &'a self,
        urls: &'a [String],
        dst_dir: &'a str,
    ) -> BoxFuture<'a, Result<FileOperationResult, SourceError>>;
}

impl SukebeiMagnets for SukebeiClient {
    fn get_magnet<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Option<String>, SourceError>> {
        Box::pin(async move { Ok(SukebeiClient::get_magnet(self, avid).await?) })
    }
}

impl JavBusMagnets for JavBusClient {
    fn get_magnets<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Vec<JavBusMagnet>, SourceError>> {
        Box::pin(async move { Ok(JavBusClient::get_magnets(self, avid).await?) })
    }
}

impl OfflineFiles for AsyncCloudDrive {
    fn add_offline_files<'a>(
        &'a self,
        urls: &'a [String],
        dst_dir: &'a str,
    ) -> BoxFuture<'a, Result<FileOperationResult, SourceError>> {
        // The wrapper awaits a sent mutation to its end even when this future is dropped
        // (cross-version contract 6).
        Box::pin(async move { Ok(AsyncCloudDrive::add_offline_files(self, urls, dst_dir).await?) })
    }
}

/// Text-format escaping as `str(message)` writes strings (`CEscape(text, as_utf8=True)`).
fn text_format_escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '\t' => escaped.push_str("\\t"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '"' => escaped.push_str("\\\""),
            '\'' => escaped.push_str("\\'"),
            '\\' => escaped.push_str("\\\\"),
            ch if (ch as u32) < 32 || ch as u32 == 127 => {
                let _ = write!(escaped, "\\{:03o}", ch as u32);
            }
            ch => escaped.push(ch),
        }
    }
    escaped
}

/// `str(result)` of a `FileOperationResult`: protobuf's text format, fields at their default
/// left out, one line per field.
pub fn operation_result_text(result: &FileOperationResult) -> String {
    let mut text = String::new();
    if result.success {
        text.push_str("success: true\n");
    }
    if !result.error_message.is_empty() {
        let _ = writeln!(text, "errorMessage: \"{}\"", text_format_escape(&result.error_message));
    }
    for path in &result.result_file_paths {
        let _ = writeln!(text, "resultFilePaths: \"{}\"", text_format_escape(path));
    }
    text
}

/// What an intake call fails with: the ledger, or the resolve schedule's date arithmetic.
#[derive(Debug, thiserror::Error)]
pub enum IntakeError {
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error(transparent)]
    Overflow(#[from] OverflowError),
}

impl PyException for IntakeError {
    fn class_name(&self) -> &str {
        match self {
            Self::Ledger(error) => error.class_name(),
            Self::Overflow(_) => "OverflowError",
        }
    }
}

/// `on_submitted()`: called whenever a magnet lands at CloudDrive.
pub type OnSubmitted = Arc<dyn Fn() + Send + Sync>;

/// How one `_add_magnet` call ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AddOutcome {
    Success,
    Duplicate,
    Failed,
}

/// The constructor's arguments (`AcquisitionIntake(ledger=, sukebei=, javbus=, cloud=, ...)`).
pub struct IntakeParts {
    pub ledger: Arc<dyn Ledger>,
    pub sukebei: Arc<dyn SukebeiMagnets>,
    pub javbus: Arc<dyn JavBusMagnets>,
    pub cloud: Arc<dyn OfflineFiles>,
    pub failed_cooldown_seconds: i64,
    pub on_submitted: Option<OnSubmitted>,
    /// Fills in the release date of a row parked without one.
    pub release_date_lookup: Option<Arc<dyn ReleaseDateLookup>>,
}

impl fmt::Debug for IntakeParts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IntakeParts")
            .field("failed_cooldown_seconds", &self.failed_cooldown_seconds)
            .finish_non_exhaustive()
    }
}

/// `AcquisitionIntake`.
pub struct AcquisitionIntake {
    ledger: Arc<dyn Ledger>,
    sukebei: Arc<dyn SukebeiMagnets>,
    javbus: Arc<dyn JavBusMagnets>,
    cloud: Arc<dyn OfflineFiles>,
    failed_cooldown: TimeDelta,
    on_submitted: Option<OnSubmitted>,
    release_date_lookup: Option<Arc<dyn ReleaseDateLookup>>,
    clock: Clock,
    sleeper: Sleeper,
}

impl fmt::Debug for AcquisitionIntake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AcquisitionIntake")
            .field("ledger", &self.ledger)
            .field("failed_cooldown", &self.failed_cooldown)
            .finish_non_exhaustive()
    }
}

impl AcquisitionIntake {
    /// `AcquisitionIntake(...)`: `OverflowError` for a cooldown beyond `timedelta`'s range,
    /// as Python's constructor raises. The wall clock and real sleeps.
    pub fn new(parts: IntakeParts) -> Result<Self, OverflowError> {
        Ok(Self {
            failed_cooldown: py_timedelta_seconds(parts.failed_cooldown_seconds)?,
            ledger: parts.ledger,
            sukebei: parts.sukebei,
            javbus: parts.javbus,
            cloud: parts.cloud,
            on_submitted: parts.on_submitted,
            release_date_lookup: parts.release_date_lookup,
            clock: system_clock(),
            sleeper: tokio_sleeper(),
        })
    }

    /// The clock `datetime.now(UTC)` stands for.
    #[must_use]
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// The sleeper `CLOUD_RETRY`'s waits go through.
    #[must_use]
    pub fn with_sleeper(mut self, sleeper: Sleeper) -> Self {
        self.sleeper = sleeper;
        self
    }

    /// `enqueue(avid, source=, task_dir_path=, ctx=, item_magnet=None, release_date=None)`: run
    /// the whole intake for one AVID and report how far it got.
    ///
    /// A sighting that brings its own magnet wakes an AVID still cooling down from an empty
    /// pass: the magnet is the evidence the wait is over.
    pub async fn enqueue(
        &self,
        avid: &str,
        source: &str,
        task_dir_path: &str,
        ctx: &RunContext,
        item_magnet: Option<&str>,
        release_date: Option<NaiveDate>,
    ) -> Result<IntakeOutcome, IntakeError> {
        let sighting = Sighting {
            task_dir_path: Some(task_dir_path.to_owned()),
            release_date,
            wake: item_magnet.is_some(),
            ..Sighting::default()
        };
        if !self.ledger.discover(avid, source, (self.clock)(), sighting).await? {
            return Ok(IntakeOutcome::AlreadyTracked);
        }
        let candidates = match self.resolve(avid, ctx, item_magnet).await {
            Ok(candidates) => candidates,
            Err(error) => {
                ctx.exception(&format!("Failed to get magnets for {avid}"), Some(&error));
                Vec::new()
            }
        };
        if candidates.is_empty() {
            ctx.warning(&format!("Failed to get any magnet for {avid}"));
            self.park_unresolved(avid).await?;
            return Ok(IntakeOutcome::NoMagnet);
        }
        if self.record_and_submit(avid, &candidates, task_dir_path, ctx).await? {
            Ok(IntakeOutcome::Submitted)
        } else {
            Ok(IntakeOutcome::SubmitFailed)
        }
    }

    /// `queue(...)`: record one AVID for the background pass instead of resolving inline.
    ///
    /// The row lands in `discovered` with its action timer already due, so the next tracker
    /// pass resolves magnets and submits it (the fill-actor scan's path).
    pub async fn queue(
        &self,
        avid: &str,
        source: &str,
        task_dir_path: &str,
        ctx: &RunContext,
        release_date: Option<NaiveDate>,
    ) -> Result<IntakeOutcome, IntakeError> {
        let now = (self.clock)();
        let sighting = Sighting {
            task_dir_path: Some(task_dir_path.to_owned()),
            next_action_at: Some(now),
            release_date,
            ..Sighting::default()
        };
        if !self.ledger.discover(avid, source, now, sighting).await? {
            return Ok(IntakeOutcome::AlreadyTracked);
        }
        ctx.add("queued", 1);
        Ok(IntakeOutcome::Queued)
    }

    // -- cooldown retries -----------------------------------------------------

    /// `retry_due(ctx=, fallback_task_dir=None, limit=50)`: re-resolve the acquisitions whose
    /// cooldown has expired; the number processed.
    pub async fn retry_due(
        &self,
        ctx: &RunContext,
        fallback_task_dir: Option<&str>,
        limit: i64,
    ) -> Result<usize, IntakeError> {
        let records = self.ledger.due_for_retry((self.clock)(), limit).await?;
        for record in &records {
            let outcome = self.retry(record, ctx, fallback_task_dir).await?;
            ctx.add(&format!("retry_{outcome}"), 1);
        }
        if !records.is_empty() {
            ctx.set("retry_due", i64::try_from(records.len()).unwrap_or(i64::MAX));
            ctx.info(&format!("Retried {} cooled-down avids", records.len()));
        }
        Ok(records.len())
    }

    /// `retry(record, ctx=, fallback_task_dir=None)`: one resolve-and-submit pass for a record
    /// picked off the action timer.
    ///
    /// A pass that submits nothing re-arms the timer from the record's current state. The
    /// submission goes to the directory pinned on the record, else `fallback_task_dir`.
    pub async fn retry(
        &self,
        record: &AcquisitionRecord,
        ctx: &RunContext,
        fallback_task_dir: Option<&str>,
    ) -> Result<IntakeOutcome, IntakeError> {
        // `record.task_dir_path or fallback_task_dir`: an empty path counts as none.
        let task_dir = record
            .task_dir_path
            .as_deref()
            .filter(|path| !path.is_empty())
            .or(fallback_task_dir);
        let Some(task_dir) = task_dir else {
            ctx.warning(&format!("No offline directory to retry {}", record.avid));
            self.repark(record, "no offline directory configured").await?;
            return Ok(IntakeOutcome::SubmitFailed);
        };
        let candidates = match self.resolve(&record.avid, ctx, None).await {
            Ok(candidates) => candidates,
            Err(error) => {
                ctx.exception(&format!("Failed to get magnets for {}", record.avid), Some(&error));
                Vec::new()
            }
        };
        let mut outcome = IntakeOutcome::NoMagnet;
        if !candidates.is_empty() {
            if self.record_and_submit(&record.avid, &candidates, task_dir, ctx).await? {
                return Ok(IntakeOutcome::Submitted);
            }
            outcome = IntakeOutcome::SubmitFailed;
        }
        ctx.warning(&format!("Retry submitted nothing for {}", record.avid));
        let note = if outcome == IntakeOutcome::NoMagnet {
            "no magnet found"
        } else {
            "no magnet accepted"
        };
        self.repark(record, note).await?;
        Ok(outcome)
    }

    async fn repark(&self, record: &AcquisitionRecord, note: &str) -> Result<(), IntakeError> {
        // The first empty pass is when the release date starts to matter, so a row recorded
        // without one (a feed sighting) is completed here.
        let release_date =
            ensure_release_date(self.ledger.as_ref(), record, self.release_date_lookup.as_deref()).await?;
        let now = (self.clock)();
        let next_action_at = next_resolve_at(now, release_date, self.failed_cooldown)?;
        self.ledger
            .transition(
                &record.avid,
                record.state,
                AcquisitionState::ResolveFailed,
                now,
                TransitionFields {
                    note: Some(note.to_owned()),
                    next_action_at: Some(next_action_at),
                    archived_paths: None,
                },
            )
            .await?;
        Ok(())
    }

    // -- magnet resolution ---------------------------------------------------

    /// `resolve(avid, ctx=, item_magnet=None)`: every magnet worth trying for one AVID, best
    /// first.
    ///
    /// Sukebei leads; the source's own magnet and JavBus fill in behind it. Only tracked
    /// hashes are kept. A sukebei failure is the caller's to report; a JavBus failure is
    /// logged here (a 404 only means the work is not released yet).
    pub async fn resolve(
        &self,
        avid: &str,
        ctx: &RunContext,
        item_magnet: Option<&str>,
    ) -> Result<Vec<MagnetCandidate>, SourceError> {
        let mut candidates: Vec<MagnetCandidate> = Vec::new();
        let mut collect = |magnet: Option<&str>, source: &str, size_hint: Option<i128>| {
            let Some(magnet) = magnet.filter(|magnet| magnet.to_lowercase().starts_with("magnet:")) else {
                return;
            };
            let Some(info_hash) = extract_info_hash(magnet) else {
                ctx.warning(&format!("Skipping magnet without a usable info hash for {avid}"));
                return;
            };
            if candidates
                .iter()
                .any(|known| known.info_hash.as_deref() == Some(info_hash.as_str()))
            {
                return;
            }
            candidates.push(MagnetCandidate {
                magnet: magnet.to_owned(),
                info_hash: Some(info_hash),
                source: source.to_owned(),
                size_hint,
            });
        };
        let sukebei = self.sukebei.get_magnet(avid).await?;
        collect(sukebei.as_deref(), "sukebei", None);
        collect(item_magnet, "rss_item", None);
        match self.javbus.get_magnets(avid).await {
            Ok(magnets) => {
                // Already ranked by the client: quality tags first, then size.
                for magnet in magnets {
                    collect(Some(&magnet.magnet), "javbus", Some(magnet.size_int));
                }
            }
            Err(error) if error.http_status() == Some(404) => {
                // JavBus lists a work only once it is released; nothing failed.
                ctx.info(&format!("JavBus does not list {avid} yet"));
            }
            Err(error) => {
                ctx.exception(&format!("Failed to get magnets from javbus for {avid}"), Some(&error));
            }
        }
        candidates.truncate(MAX_CANDIDATES);
        Ok(candidates)
    }

    /// Park an AVID with no usable magnet; the schedule sets its next try. The row is a fresh
    /// discovery or a cooled-down one a sighting woke, so the CAS starts from its state now.
    pub async fn park_unresolved(&self, avid: &str) -> Result<(), IntakeError> {
        let Some(record) = self.ledger.get(avid).await? else {
            return Ok(());
        };
        if !PARKABLE_STATES.contains(&record.state) {
            return Ok(());
        }
        self.repark(&record, "no magnet found").await
    }

    // -- CloudDrive offline tasks ---------------------------------------------

    /// Record the candidates and submit the first; `false` when none went in.
    pub async fn record_and_submit(
        &self,
        avid: &str,
        candidates: &[MagnetCandidate],
        task_dir: &str,
        ctx: &RunContext,
    ) -> Result<bool, IntakeError> {
        let added = self.ledger.add_attempts(avid, candidates, (self.clock)()).await?;
        ctx.add("candidates_recorded", i64::try_from(added).unwrap_or(i64::MAX));
        self.submit_next(avid, task_dir, ctx).await
    }

    /// Submit this AVID's next untried magnet; `false` when none is left.
    async fn submit_next(&self, avid: &str, task_dir: &str, ctx: &RunContext) -> Result<bool, IntakeError> {
        loop {
            let now = (self.clock)();
            let Some(attempt) = self.ledger.claim_next_pending(avid, now).await? else {
                return Ok(false);
            };
            if self.add_magnet(avid, &attempt.magnet, task_dir, ctx).await == AddOutcome::Failed {
                self.ledger
                    .transition_attempt(
                        avid,
                        attempt.attempt_no,
                        AttemptState::Submitted,
                        AttemptState::Error,
                        now,
                        Some("failed to add the offline task".to_owned()),
                    )
                    .await?;
                continue;
            }
            if let Some(on_submitted) = &self.on_submitted {
                on_submitted();
            }
            if let Some(record) = self.ledger.get(avid).await?
                && record.state != AcquisitionState::Downloading
            {
                self.ledger
                    .transition(
                        avid,
                        record.state,
                        AcquisitionState::Downloading,
                        now,
                        TransitionFields::default(),
                    )
                    .await?;
            }
            return Ok(true);
        }
    }

    async fn add_magnet(&self, avid: &str, link: &str, task_dir: &str, ctx: &RunContext) -> AddOutcome {
        let urls = [link.to_owned()];
        let added = tenacity::retry(&self.sleeper, SourceError::is_cloud_retryable, || {
            self.cloud.add_offline_files(&urls, task_dir)
        })
        .await;
        match added {
            Err(error) => {
                if let Some(details) = error.rpc_details() {
                    if details.contains("任务已存在") {
                        // Already queued at CloudDrive: the hash is what we track, so this
                        // attempt is live either way.
                        ctx.warning(&format!("Duplicate magnet for {avid}"));
                        ctx.add("duplicates", 1);
                        return AddOutcome::Duplicate;
                    }
                    ctx.exception(&format!("Failed to add magnet for {avid}: {details}"), Some(&error));
                } else {
                    ctx.exception(&format!("Failed to add magnet for {avid}"), Some(&error));
                }
                ctx.add("add_failed", 1);
                AddOutcome::Failed
            }
            Ok(result) if result.success => {
                ctx.info(&format!("Added magnet for {avid}"));
                ctx.add("magnets_added", 1);
                AddOutcome::Success
            }
            Ok(result) => {
                ctx.error(&format!(
                    "Failed to add magnet for {avid}: {}",
                    operation_result_text(&result)
                ));
                ctx.add("add_failed", 1);
                AddOutcome::Failed
            }
        }
    }
}

#[cfg(test)]
mod tests;
