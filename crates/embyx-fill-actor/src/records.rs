//! The fill actor's private records: plans with their candidates, jobs with leases and
//! progress, the move journal and the CloudDrive move operations.
//!
//! Python origin: the dataclasses, enums and transition tables of
//! `src/embyx_manager/fill_actor/persistence.py`. The in-memory repository in that module is
//! not ported (PLAN.md decision 1; DIVERGENCES.md D110): the PostgreSQL repository
//! ([`crate::postgres_repository`]) is the only one. Each record's `__post_init__` check is
//! [`validate`](CandidateRecord::validate) here, with Python's `ValueError` text
//! (`goldens/fill_actor/records.json`); the repository runs it wherever Python constructs a
//! record, so a row neither build could have written is refused the same way.

use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;

use chrono::{DateTime, Timelike as _, Utc};

use crate::cloud_moves::CloudFileMetadata;
use crate::models::{ApplyResult, FillActorPlan, str_enum};

/// `SHA256_HEX_LENGTH`.
pub const SHA256_HEX_LENGTH: usize = 64;
/// `JOB_CANCELLED_ERROR_CODE`.
pub const JOB_CANCELLED_ERROR_CODE: &str = "job_cancelled";

str_enum! {
    /// `JobState`.
    JobState {
        Queued = "queued",
        Running = "running",
        Completed = "completed",
        PartialFailed = "partial_failed",
        Failed = "failed",
    }
}

str_enum! {
    /// `CancelJobOutcome`.
    CancelJobOutcome {
        Cancelled = "cancelled",
        AlreadyCancelled = "already_cancelled",
        AlreadyTerminal = "already_terminal",
        NotCancellable = "not_cancellable",
        NotFound = "not_found",
    }
}

str_enum! {
    /// `EnqueueApplyJobOutcome`.
    EnqueueApplyJobOutcome {
        Enqueued = "enqueued",
        Existing = "existing",
        Conflict = "conflict",
        Full = "full",
    }
}

str_enum! {
    /// `JobOperation`.
    JobOperation {
        CreatePlan = "create_plan",
        Apply = "apply",
    }
}

str_enum! {
    /// `JobStage`.
    JobStage {
        Queued = "queued",
        ActorCatalog = "actor_catalog",
        LibraryScan = "library_scan",
        /// Legacy: job rows persisted before scans submitted to the download tracker.
        MagnetLookup = "magnet_lookup",
        Submitting = "submitting",
        Persisting = "persisting",
        Done = "done",
        Unknown = "unknown",
    }
}

str_enum! {
    /// `JobProgressUnit`.
    JobProgressUnit {
        Actors = "actors",
        Pages = "pages",
        Videos = "videos",
        /// Legacy: job rows persisted before scans submitted to the download tracker.
        Magnets = "magnets",
        Steps = "steps",
        Items = "items",
    }
}

str_enum! {
    /// `MoveJournalState`.
    MoveJournalState {
        Prepared = "prepared",
        Linked = "linked",
        SourceRemoved = "source_removed",
        Reconciled = "reconciled",
    }
}

str_enum! {
    /// `CandidateKind`.
    CandidateKind {
        LocalFile = "local_file",
        CloudStrm = "cloud_strm",
    }
}

str_enum! {
    /// `CloudMoveOperationState`.
    CloudMoveOperationState {
        Prepared = "prepared",
        Submitting = "submitting",
        Verifying = "verifying",
        Unknown = "unknown",
        Succeeded = "succeeded",
        Conflict = "conflict",
        Failed = "failed",
    }
}

impl CloudMoveOperationState {
    /// `.terminal`.
    pub fn terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Conflict | Self::Failed)
    }

    /// The states that must carry an error code.
    fn needs_error_code(self) -> bool {
        matches!(self, Self::Unknown | Self::Conflict | Self::Failed)
    }
}

/// A record failed its `__post_init__` check: Python's `ValueError` and its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordError(pub &'static str);

impl fmt::Display for RecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for RecordError {}

/// `FileFingerprint`: what a local candidate looked like when the plan was made.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileFingerprint {
    pub device: i64,
    pub inode: i64,
    pub size: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
}

/// `CandidateRecord`: the private half of a plan's move candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateRecord {
    pub candidate_id: String,
    pub video_id: String,
    pub source: PathBuf,
    pub source_root: PathBuf,
    pub destination: PathBuf,
    pub fingerprint: FileFingerprint,
    pub kind: CandidateKind,
    pub mapping_sha256: Option<String>,
    pub cloud_source_path: Option<String>,
    pub cloud_destination_dir: Option<String>,
    pub cloud_file: Option<CloudFileMetadata>,
}

impl CandidateRecord {
    /// A local candidate (`kind=LOCAL_FILE`, no CloudDrive metadata).
    pub fn local(
        candidate_id: impl Into<String>,
        video_id: impl Into<String>,
        source: impl Into<PathBuf>,
        source_root: impl Into<PathBuf>,
        destination: impl Into<PathBuf>,
        fingerprint: FileFingerprint,
    ) -> Self {
        Self {
            candidate_id: candidate_id.into(),
            video_id: video_id.into(),
            source: source.into(),
            source_root: source_root.into(),
            destination: destination.into(),
            fingerprint,
            kind: CandidateKind::LocalFile,
            mapping_sha256: None,
            cloud_source_path: None,
            cloud_destination_dir: None,
            cloud_file: None,
        }
    }

    /// `__post_init__`: a local candidate carries no CloudDrive metadata, a CloudDrive one
    /// carries all of it, consistent with itself.
    pub fn validate(&self) -> Result<(), RecordError> {
        let cloud_values = [
            self.mapping_sha256.is_some(),
            self.cloud_source_path.is_some(),
            self.cloud_destination_dir.is_some(),
            self.cloud_file.is_some(),
        ];
        match self.kind {
            CandidateKind::LocalFile if cloud_values.iter().any(|present| *present) => {
                Err(RecordError("local candidates must not contain CloudDrive metadata"))
            }
            CandidateKind::LocalFile => Ok(()),
            CandidateKind::CloudStrm => {
                if !cloud_values.iter().all(|present| *present) {
                    return Err(RecordError("CloudDrive candidates require mapping and remote metadata"));
                }
                if let Some(file) = &self.cloud_file
                    && Some(&file.path) != self.cloud_source_path.as_ref()
                {
                    return Err(RecordError(
                        "CloudDrive candidate metadata path must match its source path",
                    ));
                }
                if self
                    .mapping_sha256
                    .as_ref()
                    .is_none_or(|digest| digest.chars().count() != SHA256_HEX_LENGTH)
                {
                    return Err(RecordError("CloudDrive candidate mapping digest is invalid"));
                }
                Ok(())
            }
        }
    }
}

/// `PlanRecord`: a plan's public half and its private candidates.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanRecord {
    pub public: FillActorPlan,
    pub candidates: Vec<CandidateRecord>,
    /// fill-actor config version the plan was scanned under; 0 for pre-migration rows.
    pub config_version: i32,
}

impl PlanRecord {
    /// `__post_init__`: candidate ids unique, and the private records are exactly the public
    /// plan's candidates (by id and video).
    pub fn validate(&self) -> Result<(), RecordError> {
        let mut seen = HashSet::new();
        if !self
            .candidates
            .iter()
            .all(|candidate| seen.insert(candidate.candidate_id.as_str()))
        {
            return Err(RecordError("candidate ids must be unique within a plan"));
        }
        // Python builds two dicts (the last of a repeated public id wins) and compares them.
        let mut public = std::collections::HashMap::new();
        for video in &self.public.videos {
            for candidate in &video.move_candidates {
                public.insert(candidate.candidate_id.as_str(), candidate.video_id.as_str());
            }
        }
        let private: std::collections::HashMap<&str, &str> = self
            .candidates
            .iter()
            .map(|candidate| (candidate.candidate_id.as_str(), candidate.video_id.as_str()))
            .collect();
        if private != public {
            return Err(RecordError(
                "private candidate records must match public plan candidates",
            ));
        }
        Ok(())
    }

    /// `candidate(candidate_id)`.
    pub fn candidate(&self, candidate_id: &str) -> Option<&CandidateRecord> {
        self.candidates
            .iter()
            .find(|candidate| candidate.candidate_id == candidate_id)
    }
}

fn check_counts(completed: i64, total: Option<i64>) -> Result<(), RecordError> {
    if completed < 0 {
        return Err(RecordError("job progress completed must not be negative"));
    }
    if total.is_some_and(|total| total < 0) {
        return Err(RecordError("job progress total must not be negative"));
    }
    if total.is_some_and(|total| completed > total) {
        return Err(RecordError("job progress completed must not exceed total"));
    }
    Ok(())
}

/// `JobProgress`: where a job stands, as the dashboard shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobProgress {
    pub stage: JobStage,
    pub completed: i64,
    pub total: Option<i64>,
    pub unit: JobProgressUnit,
    pub current: Option<String>,
    pub stage_started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl JobProgress {
    /// `__post_init__`.
    pub fn validate(&self) -> Result<(), RecordError> {
        check_counts(self.completed, self.total)
    }

    /// The progress `JobRecord.__post_init__` gives a job created without one: queued, the
    /// actors as the total when there are any, items otherwise.
    pub fn initial(actor_count: usize, created_at: DateTime<Utc>, updated_at: DateTime<Utc>) -> Self {
        Self {
            stage: JobStage::Queued,
            completed: 0,
            total: (actor_count > 0).then(|| i64::try_from(actor_count).unwrap_or(i64::MAX)),
            unit: if actor_count > 0 {
                JobProgressUnit::Actors
            } else {
                JobProgressUnit::Items
            },
            current: None,
            stage_started_at: created_at,
            updated_at,
        }
    }
}

/// `JobProgressEvent`: a progress report from inside a job, before it is timed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobProgressEvent {
    pub stage: JobStage,
    pub completed: i64,
    pub total: Option<i64>,
    pub unit: JobProgressUnit,
    pub current: Option<String>,
}

impl JobProgressEvent {
    /// `__post_init__`.
    pub fn validate(&self) -> Result<(), RecordError> {
        check_counts(self.completed, self.total)
    }
}

/// `JobRecord`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRecord {
    pub job_id: String,
    pub operation: JobOperation,
    pub state: JobState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub plan_id: Option<String>,
    pub error_code: Option<String>,
    pub owner_id: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub actor_ids: Vec<String>,
    pub progress: JobProgress,
}

impl JobRecord {
    /// `JobRecord(job_id=, operation=, state=, created_at=, updated_at=, plan_id=,
    /// actor_ids=)` with the other fields at their defaults and the default progress.
    pub fn new(
        job_id: impl Into<String>,
        operation: JobOperation,
        state: JobState,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        plan_id: Option<String>,
        actor_ids: Vec<String>,
    ) -> Self {
        let created_at = normalize_datetime(created_at);
        let updated_at = normalize_datetime(updated_at);
        Self {
            job_id: job_id.into(),
            operation,
            state,
            created_at,
            updated_at,
            plan_id,
            error_code: None,
            owner_id: None,
            lease_expires_at: None,
            progress: JobProgress::initial(actor_ids.len(), created_at, updated_at),
            actor_ids,
        }
    }
}

/// `ApplyJobRecord`: an apply job with the request it runs and, when done, its result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyJobRecord {
    pub job: JobRecord,
    pub revision: String,
    pub candidate_ids: Vec<String>,
    pub result: Option<ApplyResult>,
}

impl ApplyJobRecord {
    /// `__post_init__`.
    pub fn validate(&self) -> Result<(), RecordError> {
        if self.job.operation != JobOperation::Apply {
            return Err(RecordError("apply job records require the apply operation"));
        }
        if self.revision.is_empty() {
            return Err(RecordError("apply job revision must not be empty"));
        }
        let mut seen = HashSet::new();
        if !self.candidate_ids.iter().all(|id| seen.insert(id.as_str())) {
            return Err(RecordError("apply job candidate ids must be unique"));
        }
        if let Some(result) = &self.result {
            if self.job.state != JobState::Completed {
                return Err(RecordError("apply results require a completed job"));
            }
            if result.revision != self.revision {
                return Err(RecordError("apply result revision must match its job"));
            }
            if self
                .job
                .plan_id
                .as_ref()
                .is_some_and(|plan_id| &result.plan_id != plan_id)
            {
                return Err(RecordError("apply result plan id must match its job"));
            }
        }
        Ok(())
    }
}

/// `EnqueueApplyJobResult`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueApplyJobResult {
    pub outcome: EnqueueApplyJobOutcome,
    pub record: Option<ApplyJobRecord>,
}

/// `CancelJobResult`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelJobResult {
    pub outcome: CancelJobOutcome,
    pub job: Option<JobRecord>,
    pub previous_state: Option<JobState>,
    pub previous_owner_id: Option<String>,
}

/// `MoveJournalRecord`: how far a local move got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveJournalRecord {
    pub plan_id: String,
    pub candidate_id: String,
    pub state: MoveJournalState,
    pub updated_at: DateTime<Utc>,
}

/// `CloudMoveOperationRecord`: how far a CloudDrive move got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudMoveOperationRecord {
    pub plan_id: String,
    pub candidate_id: String,
    pub attempt_id: String,
    pub source_path: String,
    pub destination_dir: String,
    pub state: CloudMoveOperationState,
    pub updated_at: DateTime<Utc>,
    pub error_code: Option<String>,
}

impl CloudMoveOperationRecord {
    /// `__post_init__`: identifiers and paths present; an error code exactly in the
    /// unknown, conflict and failed states.
    pub fn validate(&self) -> Result<(), RecordError> {
        if self.attempt_id.is_empty() || self.source_path.is_empty() || self.destination_dir.is_empty() {
            return Err(RecordError(
                "CloudDrive operation identifiers and paths must not be empty",
            ));
        }
        match (self.state.needs_error_code(), self.error_code.is_some()) {
            (true, false) => Err(RecordError(
                "non-successful CloudDrive operations require an error code",
            )),
            (false, true) => Err(RecordError(
                "successful or in-progress CloudDrive operations must not carry an error code",
            )),
            _ => Ok(()),
        }
    }
}

/// `InvalidMoveJournalTransitionError` / `InvalidCloudMoveTransitionError` (both
/// `ValueError`s).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionError {
    Journal {
        current: Option<MoveJournalState>,
        requested: MoveJournalState,
    },
    CloudMove {
        current: Option<CloudMoveOperationState>,
        requested: CloudMoveOperationState,
    },
}

impl TransitionError {
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Journal { .. } => "InvalidMoveJournalTransitionError",
            Self::CloudMove { .. } => "InvalidCloudMoveTransitionError",
        }
    }
}

impl fmt::Display for TransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Journal { current, requested } => write!(
                f,
                "invalid move journal transition: {} -> {}",
                current.map_or("none", MoveJournalState::as_str),
                requested.as_str()
            ),
            Self::CloudMove { current, requested } => write!(
                f,
                "invalid CloudDrive move transition: {} -> {}",
                current.map_or("none", CloudMoveOperationState::as_str),
                requested.as_str()
            ),
        }
    }
}

impl std::error::Error for TransitionError {}

/// `_ALLOWED_JOURNAL_TRANSITIONS`.
fn journal_allows(current: Option<MoveJournalState>, requested: MoveJournalState) -> bool {
    use MoveJournalState::{Linked, Prepared, Reconciled, SourceRemoved};
    match current {
        None => requested == Prepared,
        Some(Prepared) => matches!(requested, Prepared | Linked | Reconciled),
        Some(Linked) => matches!(requested, Linked | SourceRemoved | Reconciled),
        Some(SourceRemoved) => matches!(requested, SourceRemoved | Reconciled),
        Some(Reconciled) => requested == Reconciled,
    }
}

/// `validate_journal_transition(current, requested)`.
pub fn validate_journal_transition(
    current: Option<MoveJournalState>,
    requested: MoveJournalState,
) -> Result<(), TransitionError> {
    if journal_allows(current, requested) {
        Ok(())
    } else {
        Err(TransitionError::Journal { current, requested })
    }
}

/// `_ALLOWED_CLOUD_MOVE_TRANSITIONS`.
fn cloud_move_allows(current: Option<CloudMoveOperationState>, requested: CloudMoveOperationState) -> bool {
    use CloudMoveOperationState::{Conflict, Failed, Prepared, Submitting, Succeeded, Unknown, Verifying};
    match current {
        None => requested == Prepared,
        Some(Prepared) => matches!(requested, Prepared | Submitting | Conflict | Failed),
        Some(Submitting) => matches!(requested, Submitting | Verifying | Unknown),
        Some(Verifying) => matches!(requested, Verifying | Unknown | Succeeded | Conflict | Failed),
        Some(Unknown) => matches!(requested, Unknown | Succeeded | Conflict | Failed),
        Some(state @ (Succeeded | Conflict | Failed)) => requested == state,
    }
}

/// `validate_cloud_move_transition(current, requested)`.
pub fn validate_cloud_move_transition(
    current: Option<CloudMoveOperationState>,
    requested: CloudMoveOperationState,
) -> Result<(), TransitionError> {
    if cloud_move_allows(current, requested) {
        Ok(())
    } else {
        Err(TransitionError::CloudMove { current, requested })
    }
}

/// `normalize_datetime(value)`: the instant in UTC, at a Python datetime's resolution
/// (microseconds; the database keeps no more either).
pub fn normalize_datetime(value: DateTime<Utc>) -> DateTime<Utc> {
    let nanos = value.nanosecond();
    value.with_nanosecond(nanos - nanos % 1000).unwrap_or(value)
}
