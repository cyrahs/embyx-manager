//! Replacing a title the library holds with another copy of it (换版).
//!
//! Python origin: `src/embyx_manager/monitor/replace.py`. A replacement hands a filed title
//! back to the acquisition pipeline: the row returns to `discovered` with the library paths of
//! the copy it holds, a fresh magnet goes to CloudDrive, and the tracker deletes the old copy
//! only once the new download is about to be filed ([`crate::tracker`]). Until then the title
//! stays watchable, and a replacement that never finds a working magnet leaves it as it was.
//!
//! The magnet is the operator's when they paste one; otherwise the same resolvers as every
//! other source are asked. Hashes already on the row are never tried again.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use embyx_core::avid::AvidParser;
use embyx_core::magnet::extract_info_hash;
use embyx_core::pypath::PyPath;
use embyx_core::pytext::py_strip;

use crate::acquisitions::{
    AcquisitionRecord, AcquisitionSource, AcquisitionState, AttemptState, Ledger, LedgerError, MagnetCandidate,
    REPLACEABLE_STATES, TransitionFields,
};
use crate::archive::{ArchivePipeline, blocking};
use crate::fs::PyOsError;
use crate::intake::{AcquisitionIntake, IntakeError};
use crate::reports::{PyException, RunContext, str_enum};
use crate::support::{Clock, system_clock};
use crate::tracker::SubmitMagnet;

/// `LOGGER`'s name.
pub const LOGGER: &str = "embyx-manager.replace";

str_enum! {
    /// `ReplaceOutcome`: how far a replacement got before the request returned.
    ReplaceOutcome {
        /// A magnet is at CloudDrive; the tracker takes it from here.
        Submitted = "submitted",
        /// No magnet worth trying: the resolve schedule looks again, or one can be pasted.
        NoMagnet = "no_magnet",
        /// CloudDrive refused the magnet; the row waits for another like `NoMagnet`.
        SubmitFailed = "submit_failed",
    }
}

/// What `plan` and `start` fail with: `ReplaceError(code)`, the refusal the API answers with,
/// or an error Python lets escape (a file system, ledger or intake failure).
#[derive(Debug, thiserror::Error)]
pub enum ReplaceError {
    /// `ReplaceError(code)`.
    #[error("{0}")]
    Refused(&'static str),
    #[error(transparent)]
    Os(#[from] PyOsError),
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error(transparent)]
    Intake(#[from] IntakeError),
}

impl ReplaceError {
    /// `exc.code` of a `ReplaceError`.
    pub fn code(&self) -> Option<&'static str> {
        match self {
            Self::Refused(code) => Some(code),
            _ => None,
        }
    }
}

impl PyException for ReplaceError {
    fn class_name(&self) -> &str {
        match self {
            Self::Refused(_) => "ReplaceError",
            Self::Os(error) => error.class_name(),
            Self::Ledger(error) => error.class_name(),
            Self::Intake(error) => error.class_name(),
        }
    }
}

/// `ReplacePlan`: what a replacement of one title would do, for the operator to confirm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplacePlan {
    pub avid: String,
    /// Library paths of the copy that would be deleted once the new one is in.
    pub holdings: Vec<String>,
    pub state: Option<AcquisitionState>,
    /// The offline directory the new download would be queued under.
    pub task_dir_path: Option<String>,
    /// Why it cannot start now (an error code), or `None` when it can.
    pub blocked: Option<&'static str>,
}

/// `ReplaceResult`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaceResult {
    pub plan: ReplacePlan,
    pub outcome: ReplaceOutcome,
    pub record: AcquisitionRecord,
}

/// `intake_factory()`: the shared intake, or `None` while CloudDrive is not configured.
pub type IntakeFactory = Arc<dyn Fn() -> Option<Arc<AcquisitionIntake>> + Send + Sync>;
/// `archiver_factory()`.
pub type ArchiverFactory = Arc<dyn Fn() -> ArchivePipeline + Send + Sync>;
/// `parser_factory()`.
pub type ParserFactory = Arc<dyn Fn() -> AvidParser + Send + Sync>;
/// `configured_dirs()`: the input sources' offline directories.
pub type ConfiguredDirs = Arc<dyn Fn() -> Vec<String> + Send + Sync>;

/// `ReplacementSource`: everything a replacement needs, resolved from live configuration.
///
/// `submit_magnet(avid, magnet)` is the dashboard's own submit, which queues under the
/// directory pinned on the row; `configured_dirs` are the fallback when the row has none.
#[derive(Clone)]
pub struct ReplacementSource {
    pub ledger: Arc<dyn Ledger>,
    pub intake_factory: IntakeFactory,
    pub archiver_factory: ArchiverFactory,
    pub parser_factory: ParserFactory,
    pub configured_dirs: ConfiguredDirs,
    pub submit_magnet: SubmitMagnet,
    pub logger: String,
    pub clock: Clock,
}

impl fmt::Debug for ReplacementSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplacementSource")
            .field("ledger", &self.ledger)
            .field("logger", &self.logger)
            .finish_non_exhaustive()
    }
}

impl ReplacementSource {
    /// `ReplacementSource(...)` with the default logger and the wall clock.
    pub fn new(
        ledger: Arc<dyn Ledger>,
        intake_factory: IntakeFactory,
        archiver_factory: ArchiverFactory,
        parser_factory: ParserFactory,
        configured_dirs: ConfiguredDirs,
        submit_magnet: SubmitMagnet,
    ) -> Self {
        Self {
            ledger,
            intake_factory,
            archiver_factory,
            parser_factory,
            configured_dirs,
            submit_magnet,
            logger: LOGGER.to_owned(),
            clock: system_clock(),
        }
    }

    #[must_use]
    pub fn with_logger(mut self, logger: impl Into<String>) -> Self {
        self.logger = logger.into();
        self
    }

    /// The clock `datetime.now(UTC)` stands for.
    #[must_use]
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// `plan(text)`: read the AVID from `text` and say what replacing it would involve.
    pub async fn plan(&self, text: &str) -> Result<ReplacePlan, ReplaceError> {
        let avid = (self.parser_factory)().get_avid(py_strip(text));
        if avid.is_empty() {
            return Err(ReplaceError::Refused("unreadable_avid"));
        }
        let archiver = (self.archiver_factory)();
        let ctx = RunContext::new(self.logger.clone());
        // No task directory is passed: that would promote copies between routes, and a
        // preview must not move anything.
        let holdings = blocking(|| archiver.library_holdings(&avid, &ctx, None))?;
        let record = self.ledger.get(&avid).await?;
        let state = record.as_ref().map(|record| record.state);
        let task_dir = self.task_dir_for(record.as_ref(), &holdings, &archiver);
        let blocked = if holdings.is_empty() {
            Some("not_in_library")
        } else if state.is_some_and(|state| !REPLACEABLE_STATES.contains(&state)) {
            Some("acquisition_active")
        } else if task_dir.is_none() {
            Some("no_task_dir")
        } else if (self.intake_factory)().is_none() {
            Some("clouddrive_not_configured")
        } else {
            None
        };
        Ok(ReplacePlan {
            avid,
            holdings,
            state,
            task_dir_path: task_dir,
            blocked,
        })
    }

    /// `start(text, magnet=None)`: begin replacing one title; the old copy stays until the new
    /// one is filed.
    pub async fn start(&self, text: &str, magnet: Option<&str>) -> Result<ReplaceResult, ReplaceError> {
        let plan = self.plan(text).await?;
        if let Some(blocked) = plan.blocked {
            return Err(ReplaceError::Refused(blocked));
        }
        let intake = (self.intake_factory)();
        let (Some(intake), Some(task_dir)) = (intake, plan.task_dir_path.clone()) else {
            return Err(ReplaceError::Refused("clouddrive_not_configured"));
        };
        let candidate = self.operator_candidate(&plan.avid, magnet).await?;

        let now = (self.clock)();
        let Some(record) = self
            .ledger
            .begin_replacement(
                &plan.avid,
                AcquisitionSource::Manual.as_str(),
                &task_dir,
                &plan.holdings,
                now,
            )
            .await?
        else {
            return Err(ReplaceError::Refused("acquisition_changed"));
        };
        let ctx = RunContext::new(self.logger.clone());
        ctx.info(&format!("replacing {}, now at {}", plan.avid, plan.holdings.join(", ")));
        let outcome = match candidate {
            Some(candidate) => self.submit_operator_magnet(&plan.avid, candidate).await?,
            None => self.submit_resolved(&plan.avid, &task_dir, &intake, &ctx).await?,
        };
        if outcome != ReplaceOutcome::Submitted {
            // Parked like any acquisition without a magnet: the resolve schedule tries again,
            // and the dashboard takes a pasted one at any time.
            intake.park_unresolved(&plan.avid).await?;
        }
        let current = self.ledger.get(&plan.avid).await?;
        Ok(ReplaceResult {
            plan,
            outcome,
            record: current.unwrap_or(record),
        })
    }

    /// `_operator_candidate`: the pasted magnet as a candidate, refused before anything is
    /// written.
    async fn operator_candidate(
        &self,
        avid: &str,
        magnet: Option<&str>,
    ) -> Result<Option<MagnetCandidate>, ReplaceError> {
        let Some(magnet) = magnet.map(py_strip).filter(|magnet| !magnet.is_empty()) else {
            return Ok(None);
        };
        let info_hash = extract_info_hash(magnet);
        let Some(info_hash) = info_hash.filter(|_| magnet.to_lowercase().starts_with("magnet:")) else {
            return Err(ReplaceError::Refused("unusable_magnet"));
        };
        let tried: HashSet<Option<String>> = self
            .ledger
            .attempts_for(avid)
            .await?
            .into_iter()
            .map(|attempt| attempt.info_hash)
            .collect();
        if tried.contains(&Some(info_hash.clone())) {
            return Err(ReplaceError::Refused("magnet_already_tried"));
        }
        Ok(Some(MagnetCandidate {
            magnet: magnet.to_owned(),
            info_hash: Some(info_hash),
            source: "manual".to_owned(),
            size_hint: None,
        }))
    }

    /// `_submit_operator_magnet`: submit exactly the pasted magnet, ahead of any older
    /// candidate still pending.
    async fn submit_operator_magnet(
        &self,
        avid: &str,
        candidate: MagnetCandidate,
    ) -> Result<ReplaceOutcome, ReplaceError> {
        let now = (self.clock)();
        if self.ledger.add_attempts(avid, &[candidate], now).await? == 0 {
            return Ok(ReplaceOutcome::SubmitFailed);
        }
        let attempts = self.ledger.attempts_for(avid).await?;
        let Some(attempt_no) = attempts.last().map(|attempt| attempt.attempt_no) else {
            return Ok(ReplaceOutcome::SubmitFailed);
        };
        let attempt = match self.ledger.claim_attempt(avid, attempt_no, now).await {
            Ok(attempt) => attempt,
            Err(LedgerError::MagnetInUse(_)) => return Err(ReplaceError::Refused("magnet_in_use")),
            Err(error) => return Err(error.into()),
        };
        let Some(attempt) = attempt else {
            return Ok(ReplaceOutcome::SubmitFailed);
        };
        if !(self.submit_magnet)(avid.to_owned(), attempt.magnet.clone()).await? {
            self.ledger
                .transition_attempt(
                    avid,
                    attempt.attempt_no,
                    AttemptState::Submitted,
                    AttemptState::Error,
                    (self.clock)(),
                    Some("failed to add the offline task".to_owned()),
                )
                .await?;
            return Ok(ReplaceOutcome::SubmitFailed);
        }
        self.ledger
            .transition(
                avid,
                AcquisitionState::Discovered,
                AcquisitionState::Downloading,
                (self.clock)(),
                TransitionFields::default(),
            )
            .await?;
        Ok(ReplaceOutcome::Submitted)
    }

    /// `_submit_resolved`: ask the resolvers for fresh candidates and submit the first untried
    /// one.
    async fn submit_resolved(
        &self,
        avid: &str,
        task_dir: &str,
        intake: &AcquisitionIntake,
        ctx: &RunContext,
    ) -> Result<ReplaceOutcome, ReplaceError> {
        let candidates = match intake.resolve(avid, ctx, None).await {
            Ok(candidates) => candidates,
            Err(error) => {
                ctx.exception(&format!("Failed to get magnets for {avid}"), Some(&error));
                Vec::new()
            }
        };
        // Even with nothing new, a candidate collected earlier and never tried may still be
        // pending on the row; record_and_submit takes that too.
        if intake.record_and_submit(avid, &candidates, task_dir, ctx).await? {
            return Ok(ReplaceOutcome::Submitted);
        }
        Ok(if candidates.is_empty() {
            ReplaceOutcome::NoMagnet
        } else {
            ReplaceOutcome::SubmitFailed
        })
    }

    /// `_task_dir_for`: where the new download is queued, beside the old one when that is
    /// known: the row's own directory, else a source directory whose route feeds the library
    /// directory the old copy sits in, else any routed source directory.
    fn task_dir_for(
        &self,
        record: Option<&AcquisitionRecord>,
        holdings: &[String],
        archiver: &ArchivePipeline,
    ) -> Option<String> {
        if let Some(task_dir) = record
            .and_then(|record| record.task_dir_path.as_deref())
            .filter(|task_dir| !task_dir.is_empty() && archiver.route_for_task_dir(task_dir).is_some())
        {
            return Some(task_dir.to_owned());
        }
        // A library path is {route destination}/{brand}/{file}.
        let held_in: HashSet<String> = holdings.iter().map(|path| posix_parent(&posix_parent(path))).collect();
        let routed: Vec<(String, String)> = (self.configured_dirs)()
            .into_iter()
            .filter_map(|directory| {
                archiver
                    .route_for_task_dir(&directory)
                    .map(|(dst, _priority)| (directory, dst))
            })
            .collect();
        for (directory, dst) in &routed {
            if held_in.contains(PyPath::new(dst).as_str()) {
                return Some(directory.clone());
            }
        }
        routed.into_iter().next().map(|(directory, _)| directory)
    }
}

/// `str(PurePosixPath(path).parent)`.
fn posix_parent(path: &str) -> String {
    let normalized = PyPath::new(path);
    let normalized = normalized.as_str();
    if normalized == "/" || normalized == "//" || normalized == "." {
        return normalized.to_owned();
    }
    match normalized.rfind('/') {
        None => ".".to_owned(),
        Some(0) => "/".to_owned(),
        Some(1) if normalized.starts_with("//") => "//".to_owned(),
        Some(index) => normalized[..index].to_owned(),
    }
}

#[cfg(test)]
mod tests;
