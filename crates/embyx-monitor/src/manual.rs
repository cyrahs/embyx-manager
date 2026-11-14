//! The manual intake source: an operator's own list of AVIDs.
//!
//! Python origin: `src/embyx_manager/monitor/manual.py`. The third input source, beside the
//! RSS categories and fill actor. Pasted lines take the same road as every other source: the
//! shared [`AcquisitionIntake`] records each one in the ledger, resolves magnet candidates,
//! submits the first, and the tracker owns the download from there.
//!
//! A manual submission names its own offline directory per batch, browsed from CloudDrive and
//! defaulting to whichever directory the last manual batch used. The directory must have an
//! archive route, or the finished download would sit where the tracker never looks:
//! [`ManualIntakeSource::submit`] refuses the batch rather than writing rows nothing could
//! ever conclude.

use std::fmt;
use std::sync::Arc;

use embyx_core::config::normalize::normalize_absolute_path;
use embyx_core::pytext::py_strip;

use crate::acquisitions::{AcquisitionSource, Ledger, LedgerError};
use crate::archive::{ArchivePipeline, blocking};
use crate::cloud::{CloudListing, is_file_not_found, is_value_error};
use crate::intake::{AcquisitionIntake, IntakeError, IntakeOutcome, SourceError};
use crate::replace::{ArchiverFactory, ConfiguredDirs, IntakeFactory, ParserFactory};
use crate::reports::{PyException, RunContext, str_enum};

/// `LOGGER`'s name.
pub const LOGGER: &str = "embyx-manager.manual";

/// `MAX_MANUAL_INPUTS`: how many lines one submission may carry. Each costs magnet lookups at
/// two external sites, so a runaway paste is refused rather than served slowly.
pub const MAX_MANUAL_INPUTS: usize = 100;

str_enum! {
    /// `ManualOutcome`: what became of one line of the operator's input.
    ManualOutcome {
        Submitted = "submitted",
        AlreadyTracked = "already_tracked",
        AlreadyInLibrary = "already_in_library",
        NoMagnet = "no_magnet",
        SubmitFailed = "submit_failed",
        /// Recorded for the tracker's background pass, which resolves and submits it.
        Queued = "queued",
        /// No AVID could be read from the line at all.
        Unreadable = "unreadable",
    }
}

impl From<IntakeOutcome> for ManualOutcome {
    /// `_OUTCOMES`: the shared intake's outcomes in the manual source's vocabulary.
    fn from(outcome: IntakeOutcome) -> Self {
        match outcome {
            IntakeOutcome::Submitted => Self::Submitted,
            IntakeOutcome::AlreadyTracked => Self::AlreadyTracked,
            IntakeOutcome::NoMagnet => Self::NoMagnet,
            IntakeOutcome::SubmitFailed => Self::SubmitFailed,
            IntakeOutcome::Queued => Self::Queued,
        }
    }
}

/// `ManualEntry`: one input line and how far it got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualEntry {
    pub text: String,
    pub avid: Option<String>,
    pub outcome: ManualOutcome,
    /// Where the library already holds it, for `AlreadyInLibrary`.
    pub archived_paths: Vec<String>,
}

/// `ManualSubmission`: one batch's outcome, with the directory it actually went to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualSubmission {
    pub task_dir_path: String,
    pub entries: Vec<ManualEntry>,
}

/// `OfflineDirectory`: one CloudDrive directory offered as a submission target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OfflineDirectory {
    pub path: String,
    pub name: String,
    /// An input source's own directory, so the tracker polls it unconditionally.
    pub configured: bool,
    /// Has an archive route, which is what makes it submittable at all.
    pub routed: bool,
}

/// `DirectoryListing`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryListing {
    pub path: String,
    /// The parent directory, or `None` at the CloudDrive root.
    pub parent: Option<String>,
    pub entries: Vec<OfflineDirectory>,
}

/// What the manual source fails with: a `ManualIntakeError` subclass (its code is the API's
/// answer), or an error Python lets escape.
#[derive(Debug, thiserror::Error)]
pub enum ManualError {
    /// `CloudUnavailableError`.
    #[error("clouddrive_not_configured")]
    CloudUnavailable,
    /// `DirectoryNotFoundError`.
    #[error("directory_not_found")]
    DirectoryNotFound,
    /// `DirectoryNotRoutedError`: a download there could never be filed.
    #[error("directory_not_routed")]
    DirectoryNotRouted,
    /// `TooManyInputsError`.
    #[error("too_many_inputs")]
    TooManyInputs,
    #[error(transparent)]
    Cloud(#[from] SourceError),
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error(transparent)]
    Intake(#[from] IntakeError),
}

impl ManualError {
    /// `exc.code` of a `ManualIntakeError`.
    pub fn code(&self) -> Option<&'static str> {
        match self {
            Self::CloudUnavailable => Some("clouddrive_not_configured"),
            Self::DirectoryNotFound => Some("directory_not_found"),
            Self::DirectoryNotRouted => Some("directory_not_routed"),
            Self::TooManyInputs => Some("too_many_inputs"),
            Self::Cloud(_) | Self::Ledger(_) | Self::Intake(_) => None,
        }
    }
}

impl PyException for ManualError {
    fn class_name(&self) -> &str {
        match self {
            Self::CloudUnavailable => "CloudUnavailableError",
            Self::DirectoryNotFound => "DirectoryNotFoundError",
            Self::DirectoryNotRouted => "DirectoryNotRoutedError",
            Self::TooManyInputs => "TooManyInputsError",
            Self::Cloud(error) => error.class_name(),
            Self::Ledger(error) => error.class_name(),
            Self::Intake(error) => error.class_name(),
        }
    }
}

/// `cloud_factory()`: the CloudDrive client, or `None` while it is not configured.
pub type CloudFactory = Arc<dyn Fn() -> Option<Arc<dyn CloudListing>> + Send + Sync>;
/// `on_queued()`: wakes the tracker once a queued batch has rows for its background pass.
pub type OnQueued = Arc<dyn Fn() + Send + Sync>;

/// `on_entry(entry)`: hears each line's outcome as it lands.
pub type OnEntry = Arc<dyn Fn(&ManualEntry) + Send + Sync>;

/// The keyword arguments of `submit()` besides the lines and the directory.
#[derive(Clone)]
pub struct SubmitOptions {
    /// The ledger's source label (`manual`, or a playlist's own).
    pub source: String,
    /// The paste limit; `None` lifts it.
    pub limit: Option<usize>,
    /// Record each AVID for the tracker's background pass instead of resolving inline.
    pub queue: bool,
    /// Hears each line's outcome as it lands.
    pub on_entry: Option<OnEntry>,
}

impl Default for SubmitOptions {
    fn default() -> Self {
        Self {
            source: AcquisitionSource::Manual.as_str().to_owned(),
            limit: Some(MAX_MANUAL_INPUTS),
            queue: false,
            on_entry: None,
        }
    }
}

impl fmt::Debug for SubmitOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubmitOptions")
            .field("source", &self.source)
            .field("limit", &self.limit)
            .field("queue", &self.queue)
            .finish_non_exhaustive()
    }
}

/// `ManualIntakeSource`: everything the manual source needs, resolved from live configuration
/// (CloudDrive, the archive routes and the AVID rules can change between two submissions).
#[derive(Clone)]
pub struct ManualIntakeSource {
    pub ledger: Arc<dyn Ledger>,
    pub intake_factory: IntakeFactory,
    pub cloud_factory: CloudFactory,
    pub archiver_factory: ArchiverFactory,
    pub parser_factory: ParserFactory,
    /// The input sources' own offline directories, marked as such when browsing.
    pub configured_dirs: ConfiguredDirs,
    pub on_queued: Option<OnQueued>,
    pub logger: String,
}

impl fmt::Debug for ManualIntakeSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManualIntakeSource")
            .field("ledger", &self.ledger)
            .field("logger", &self.logger)
            .finish_non_exhaustive()
    }
}

impl ManualIntakeSource {
    // -- picking a directory ---------------------------------------------------

    /// `browse(path)`: the subdirectories of one CloudDrive directory, annotated for picking.
    pub async fn browse(&self, path: &str) -> Result<DirectoryListing, ManualError> {
        let Some(cloud) = (self.cloud_factory)() else {
            return Err(ManualError::CloudUnavailable);
        };
        let directory = normalize_dir(path)?;
        let files = match cloud.list_directory(&directory).await {
            Ok(files) => files,
            // A missing directory, or an API path CloudDrive will not accept at all.
            Err(error) if is_file_not_found(&error) || is_value_error(&error) => {
                return Err(ManualError::DirectoryNotFound);
            }
            Err(error) => return Err(error.into()),
        };
        let configured = (self.configured_dirs)();
        let archiver = (self.archiver_factory)();
        let mut entries: Vec<OfflineDirectory> = files
            .into_iter()
            .filter(|file| file.is_directory)
            .map(|file| OfflineDirectory {
                configured: configured.contains(&file.full_path),
                routed: archiver.route_for_task_dir(&file.full_path).is_some(),
                path: file.full_path,
                name: file.name,
            })
            .collect();
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(DirectoryListing {
            parent: parent_of(&directory),
            path: directory,
            entries,
        })
    }

    /// `default_directory()`: the last manual submission's directory, else a configured one
    /// that has a route; `None` leaves the operator to browse.
    pub async fn default_directory(&self) -> Result<Option<String>, ManualError> {
        let remembered = self.ledger.latest_task_dir(AcquisitionSource::Manual.as_str()).await?;
        if let Some(remembered) = remembered.filter(|directory| !directory.is_empty()) {
            return Ok(Some(remembered));
        }
        let archiver = (self.archiver_factory)();
        Ok((self.configured_dirs)()
            .into_iter()
            .find(|directory| archiver.route_for_task_dir(directory).is_some()))
    }

    // -- submitting ------------------------------------------------------------

    /// `check_target(task_dir_path)`: the normalized directory a batch would go to, or the
    /// refusal, while the operator still waits for an answer.
    pub async fn check_target(&self, task_dir_path: &str) -> Result<String, ManualError> {
        let (task_dir, _intake, _archiver) = self.prepare(task_dir_path).await?;
        Ok(task_dir)
    }

    /// `submit(inputs, task_dir_path=, source=, limit=, queue=, on_entry=)`: run every input
    /// line through the shared intake; report each one's fate.
    pub async fn submit(
        &self,
        inputs: &[String],
        task_dir_path: &str,
        options: SubmitOptions,
    ) -> Result<ManualSubmission, ManualError> {
        let lines: Vec<&str> = inputs
            .iter()
            .map(|line| py_strip(line))
            .filter(|line| !line.is_empty())
            .collect();
        if options.limit.is_some_and(|limit| lines.len() > limit) {
            return Err(ManualError::TooManyInputs);
        }
        let (task_dir, intake, archiver) = self.prepare(task_dir_path).await?;

        let parser = (self.parser_factory)();
        let ctx = RunContext::new(self.logger.clone());
        let mut entries: Vec<ManualEntry> = Vec::new();
        for line in &lines {
            let avid = parser.get_avid(line);
            let entry = if avid.is_empty() {
                ctx.warning(&format!("Failed to read an avid from {line}"));
                ManualEntry {
                    text: (*line).to_owned(),
                    avid: None,
                    outcome: ManualOutcome::Unreadable,
                    archived_paths: Vec::new(),
                }
            } else {
                self.submit_one(line, avid, &task_dir, &archiver, &intake, &ctx, &options)
                    .await?
            };
            if let Some(on_entry) = &options.on_entry {
                on_entry(&entry);
            }
            entries.push(entry);
        }
        ctx.info(&format!(
            "{} submission: {} lines into {task_dir}",
            options.source,
            lines.len()
        ));
        if let Some(on_queued) = &self.on_queued
            && entries.iter().any(|entry| entry.outcome == ManualOutcome::Queued)
        {
            on_queued();
        }
        Ok(ManualSubmission {
            task_dir_path: task_dir,
            entries,
        })
    }

    async fn prepare(
        &self,
        task_dir_path: &str,
    ) -> Result<(String, Arc<AcquisitionIntake>, ArchivePipeline), ManualError> {
        let intake = (self.intake_factory)();
        let cloud = (self.cloud_factory)();
        let (Some(intake), Some(cloud)) = (intake, cloud) else {
            return Err(ManualError::CloudUnavailable);
        };
        let task_dir = normalize_dir(task_dir_path)?;
        let archiver = (self.archiver_factory)();
        if archiver.route_for_task_dir(&task_dir).is_none() {
            return Err(ManualError::DirectoryNotRouted);
        }
        match cloud.list_directory(&task_dir).await {
            Ok(_) => {}
            Err(error) if is_file_not_found(&error) => return Err(ManualError::DirectoryNotFound),
            Err(error) => return Err(error.into()),
        }
        Ok((task_dir, intake, archiver))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one call site, all of it per-line state, as in Python"
    )]
    async fn submit_one(
        &self,
        line: &str,
        avid: String,
        task_dir: &str,
        archiver: &ArchivePipeline,
        intake: &AcquisitionIntake,
        ctx: &RunContext,
        options: &SubmitOptions,
    ) -> Result<ManualEntry, ManualError> {
        let held = Self::library_holdings(&avid, task_dir, archiver, ctx);
        if let Some(first) = held.first() {
            // Nothing is recorded: the operator asked for something they already have, and a
            // ledger row would only claim an acquisition that never happened.
            ctx.info(&format!("{avid} is already in the library at {first}"));
            return Ok(ManualEntry {
                text: line.to_owned(),
                avid: Some(avid),
                outcome: ManualOutcome::AlreadyInLibrary,
                archived_paths: held,
            });
        }
        let outcome = if options.queue {
            intake.queue(&avid, &options.source, task_dir, ctx, None).await?
        } else {
            intake
                .enqueue(&avid, &options.source, task_dir, ctx, None, None)
                .await?
        };
        Ok(ManualEntry {
            text: line.to_owned(),
            avid: Some(avid),
            outcome: outcome.into(),
            archived_paths: Vec::new(),
        })
    }

    /// `_library_holdings`: where the library already holds this AVID. An unreadable library
    /// is not a held one: the submission proceeds and the archive-time check has the final
    /// word.
    fn library_holdings(avid: &str, task_dir: &str, archiver: &ArchivePipeline, ctx: &RunContext) -> Vec<String> {
        match blocking(|| archiver.library_holdings(avid, ctx, Some(task_dir))) {
            Ok(held) => held,
            Err(error) => {
                ctx.exception(&format!("Failed to check the library for {avid}"), Some(&error));
                Vec::new()
            }
        }
    }
}

/// `_normalize_dir`: an absolute CloudDrive API directory, or `DirectoryNotFoundError`.
fn normalize_dir(path: &str) -> Result<String, ManualError> {
    match normalize_absolute_path("directory", path) {
        Ok(normalized) if !normalized.is_empty() => Ok(normalized),
        _ => Err(ManualError::DirectoryNotFound),
    }
}

/// `_parent_of`.
fn parent_of(directory: &str) -> Option<String> {
    if directory == "/" {
        return None;
    }
    let parent = directory.rsplit_once('/').map_or(directory, |(parent, _)| parent);
    Some(if parent.is_empty() {
        "/".to_owned()
    } else {
        parent.to_owned()
    })
}

#[cfg(test)]
pub(crate) mod tests;
