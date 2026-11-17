//! CloudDrive moves of `.strm`-mapped videos: the scan's candidates, the move with its
//! operation record, and the recovery of an operation a crash or a restart left open.
//!
//! Python origin: `fill_actor/service.py` (`_create_cloud_video_plan`, `_run_cloud_move`,
//! `_recover_cloud_move`, `_observe_cloud_move`, `_complete_cloud_move`,
//! `_advance_cloud_operation` and their helpers). A move only ever concludes from what the
//! listings show: `MoveFile` takes a path, not a file id, so its answer alone proves nothing.
//!
//! Each execution (one candidate's move, one operation's recovery) runs its CloudDrive calls on
//! a [`CloudFileMover::scoped`] mover and awaits `settle()` before it lets go of the mutation
//! lock, also when the execution's future is dropped (D116, contract 6): the lock is never
//! free while a `MoveFile` it guarded may still land.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use embyx_core::pypath;
use embyx_core::pytext::casefold;

use super::local::{self, ScanError};
use super::{
    CLOUD_MOVE_OBSERVATION_GRACE, CLOUD_VERIFY_ATTEMPTS, FillActorPaths, FillActorRuntime, FillActorService, HeldLock,
    LOGGER, ServiceError, VideoScan, posix_parent, strings, video_plan,
};
use crate::cloud_moves::{CloudFileMetadata, CloudFileMover, CloudMovePaths, CloudMoveResponse, MAX_STRM_BYTES};
use crate::fs;
use crate::models::{MoveCandidate, MoveResult, MoveState, VideoState};
use crate::records::{CandidateKind, CandidateRecord, CloudMoveOperationRecord, CloudMoveOperationState};

/// The listings one scan has read, by API directory (`None`: the directory is not there).
pub(super) type DirectoryCache = HashMap<String, Option<Vec<CloudFileMetadata>>>;

/// `str(CLOUD_MOVE_OBSERVATION_GRACE)`, as the log line shows it.
const GRACE_TEXT: &str = "0:15:00";

/// The mutation lock and the scoped mover of one execution. [`Self::release`] waits for the
/// mover's calls and then lets go of the lock; dropped instead (the execution's future was
/// dropped), it leaves a task that does the same.
struct CloudLease {
    mover: Arc<dyn CloudFileMover>,
    lock: HeldLock,
    released: bool,
}

impl CloudLease {
    fn new(mover: Arc<dyn CloudFileMover>, lock: HeldLock) -> Self {
        Self {
            mover,
            lock,
            released: false,
        }
    }

    async fn release(mut self) {
        self.mover.settle().await;
        self.released = true;
        drop(self.lock.take());
    }
}

impl Drop for CloudLease {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        let (mover, lock) = (Arc::clone(&self.mover), self.lock.take());
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                mover.settle().await;
                drop(lock);
            });
        }
    }
}

/// `_cloud_result_base(record)` with a state and an error code.
fn cloud_result(record: &CandidateRecord, state: MoveState, error_code: Option<&str>) -> MoveResult {
    MoveResult {
        candidate_id: record.candidate_id.clone(),
        video_id: record.video_id.clone(),
        file_name: record.cloud_file.as_ref().map_or_else(
            || fs::name(&record.source).to_string_lossy().into_owned(),
            |file| file.name.clone(),
        ),
        state,
        error_code: error_code.map(str::to_owned),
    }
}

/// `_find_cloud_file(api_path, listing)`.
fn find_cloud_file<'a>(api_path: &str, listing: Option<&'a [CloudFileMetadata]>) -> Option<&'a CloudFileMetadata> {
    listing?.iter().find(|file| file.path == api_path)
}

/// `_matches_video_file_name(file_name, video_id)`.
fn matches_video_file_name(file_name: &str, video_id: &str) -> bool {
    local::video_file_pattern(video_id).is_match(file_name)
}

/// `dict.fromkeys(values)`: the values in their order, each once.
fn unique(values: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    values.into_iter().filter(|value| seen.insert(value.clone())).collect()
}

/// `tuple(sorted(set(values)))`.
fn sorted_set(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values.dedup();
    values
}

/// `_terminal_cloud_result(record, operation)`.
fn terminal_cloud_result(record: &CandidateRecord, operation: &CloudMoveOperationRecord) -> MoveResult {
    let (state, error_code) = match operation.state {
        CloudMoveOperationState::Succeeded => (MoveState::Moved, None),
        CloudMoveOperationState::Conflict => (MoveState::Conflict, operation.error_code.as_deref()),
        _ => (MoveState::Failed, operation.error_code.as_deref()),
    };
    cloud_result(record, state, error_code)
}

impl FillActorService {
    // ---------- scan ----------

    /// `_create_cloud_video_plan(...)`: the CloudDrive files the `.strm` mappings point at,
    /// each a candidate unless it is gone, renamed or already at its destination.
    #[allow(clippy::too_many_arguments, reason = "Python's keyword arguments")]
    #[allow(clippy::too_many_lines, reason = "Python's method, step for step")]
    pub(super) async fn create_cloud_video_plan(
        &self,
        runtime: &FillActorRuntime,
        paths: &FillActorPaths,
        video_id: &str,
        membership: &[String],
        brand: &str,
        additional: &[(usize, PathBuf)],
        cache: &mut DirectoryCache,
    ) -> Result<VideoScan, ScanError> {
        let Some(cloud_paths) = runtime.cloud_move_paths.as_ref() else {
            return Err(ScanError::Value("CloudDrive paths are not configured"));
        };
        let mut candidates = Vec::new();
        let mut records = Vec::new();
        let mut existing_files = Vec::new();
        let mut warnings: Vec<String> = Vec::new();
        let destination_dir = cloud_paths
            .destination_directory(brand)
            .map_err(|_| ScanError::Value("CloudDrive destination brand is invalid"))?;
        let destination_listing = self.list_cloud_directory_cached(&destination_dir, cache).await?;

        for (source_index, mapping_path) in additional {
            let mapping_name = fs::name(mapping_path).to_string_lossy();
            if casefold(pypath::suffix(&mapping_name)) != ".strm" {
                warnings.push("cloud_mapping_not_strm".to_owned());
                continue;
            }
            let Ok(parsed) = parse_mapping(self.fs(), cloud_paths, mapping_path, *source_index) else {
                warnings.push("invalid_strm_target".to_owned());
                continue;
            };
            let cloud_file = self.cloud_file_cached(&parsed.api_path, cache).await?;
            let destination_path = pypath::join(&destination_dir, pypath::name(&parsed.api_path));
            let destination_file = find_cloud_file(&destination_path, destination_listing.as_deref());
            let Some(cloud_file) = cloud_file else {
                match destination_file {
                    Some(file) => existing_files.push(file.name.clone()),
                    None => warnings.push("cloud_source_missing".to_owned()),
                }
                continue;
            };
            if !matches_video_file_name(&cloud_file.name, video_id) {
                warnings.push("cloud_source_name_mismatch".to_owned());
                continue;
            }

            let candidate_id = self.token();
            let destination_root = if runtime.move_in_by_brand {
                paths.move_in_path.join(brand)
            } else {
                paths.move_in_path.clone()
            };
            let source_root = paths.additional_brand_paths[*source_index].clone();
            records.push(CandidateRecord {
                candidate_id: candidate_id.clone(),
                video_id: video_id.to_owned(),
                source: mapping_path.clone(),
                source_root: source_root.clone(),
                destination: destination_root.join(fs::name(mapping_path)),
                fingerprint: local::fingerprint(self.fs(), mapping_path)?,
                kind: CandidateKind::CloudStrm,
                mapping_sha256: Some(parsed.mapping_sha256),
                cloud_source_path: Some(parsed.api_path),
                cloud_destination_dir: Some(destination_dir.clone()),
                cloud_file: Some(cloud_file.clone()),
            });
            candidates.push(MoveCandidate {
                candidate_id,
                video_id: video_id.to_owned(),
                file_name: cloud_file.name.clone(),
                source_label: embyx_monitor::fs::path_text(&source_root),
                destination_conflict: destination_file.is_some(),
            });
        }

        if !candidates.is_empty() {
            let mut plan = video_plan(video_id, membership, VideoState::AdditionalFound);
            plan.existing_files = sorted_set(existing_files);
            plan.move_candidates = candidates;
            plan.warnings = unique(warnings);
            return Ok(VideoScan {
                plan,
                records,
                needs_acquisition: false,
                unencodable: Vec::new(),
            });
        }
        if !existing_files.is_empty() {
            let mut plan = video_plan(video_id, membership, VideoState::Exists);
            plan.existing_files = sorted_set(existing_files);
            plan.warnings = strings(&["mapping_convergence_pending"]);
            return Ok(VideoScan::only(plan, false));
        }
        let mut plan = video_plan(video_id, membership, VideoState::ScanFailed);
        plan.warnings = unique(warnings);
        if plan.warnings.is_empty() {
            plan.warnings = strings(&["cloud_scan_failed"]);
        }
        Ok(VideoScan::only(plan, false))
    }

    /// `_list_cloud_directory_cached(api_directory, cache)`: on the service's own mover (a
    /// scan sends no mutation).
    async fn list_cloud_directory_cached(
        &self,
        api_directory: &str,
        cache: &mut DirectoryCache,
    ) -> Result<Option<Vec<CloudFileMetadata>>, ScanError> {
        if let Some(listing) = cache.get(api_directory) {
            return Ok(listing.clone());
        }
        let Some(mover) = &self.inner.mover else {
            return Err(ScanError::Value("CloudDrive mover is not configured"));
        };
        let listing = match mover.list_directory(api_directory).await {
            Ok(listing) => Some(listing),
            Err(error) if error.is_file_not_found() => None,
            Err(error) => return Err(ScanError::Cloud(error)),
        };
        cache.insert(api_directory.to_owned(), listing.clone());
        Ok(listing)
    }

    /// `_cloud_file_cached(api_path, cache)`.
    async fn cloud_file_cached(
        &self,
        api_path: &str,
        cache: &mut DirectoryCache,
    ) -> Result<Option<CloudFileMetadata>, ScanError> {
        let listing = self.list_cloud_directory_cached(&posix_parent(api_path), cache).await?;
        Ok(find_cloud_file(api_path, listing.as_deref()).cloned())
    }

    // ---------- move ----------

    /// `_run_cloud_move(plan_id, record)`.
    pub(super) async fn run_cloud_move(
        &self,
        plan_id: &str,
        record: &CandidateRecord,
    ) -> Result<MoveResult, ServiceError> {
        let runtime = self.runtime();
        let (Some(mover), Some(cloud_paths), Some(cloud_file)) = (
            self.inner.mover.clone(),
            runtime.cloud_move_paths.clone(),
            record.cloud_file.clone(),
        ) else {
            return Ok(cloud_result(
                record,
                MoveState::Failed,
                Some("cloud_move_not_configured"),
            ));
        };
        if !self.roots_ready().await? {
            return Ok(cloud_result(record, MoveState::Failed, Some("root_unavailable")));
        }
        let lease = CloudLease::new(mover.scoped(), self.acquire_lock().await?);
        let result = self
            .cloud_move_locked(plan_id, record, &*lease.mover, &cloud_paths, &cloud_file)
            .await;
        lease.release().await;
        result
    }

    /// The part of `_run_cloud_move` that runs under the mutation lock.
    #[allow(clippy::too_many_lines, reason = "Python's method, step for step")]
    async fn cloud_move_locked(
        &self,
        plan_id: &str,
        record: &CandidateRecord,
        mover: &dyn CloudFileMover,
        cloud_paths: &CloudMovePaths,
        cloud_file: &CloudFileMetadata,
    ) -> Result<MoveResult, ServiceError> {
        let repository = &self.inner.repository;
        if let Some(cached) = repository.get_move_result(plan_id, &record.candidate_id).await? {
            return Ok(cached);
        }
        if let Some(operation) = repository
            .get_cloud_move_operation(plan_id, &record.candidate_id)
            .await?
        {
            return self.recover_cloud_move(plan_id, record, operation, mover).await;
        }

        let stale = |state: MoveState, code: &str| cloud_result(record, state, Some(code));
        if let Some(mapping_error) = self.validate_cloud_mapping(record, cloud_paths)? {
            let result = stale(MoveState::Stale, mapping_error);
            repository.save_move_result(plan_id, &result).await?;
            return Ok(result);
        }

        let preflight = async {
            let source = stat_cloud_file(mover, record.cloud_source_path.as_deref()).await?;
            let listing = list_cloud_directory(mover, record.cloud_destination_dir.as_deref()).await?;
            Ok::<_, crate::cloud_moves::MoverError>((source, listing))
        };
        let (source, mut destination_listing) = match preflight.await {
            Ok(found) => found,
            Err(error) => {
                tracing::error!(
                    logger = LOGGER,
                    error = format!("{}: {error}", error.class_name()),
                    "CloudDrive preflight failed for {}",
                    record.cloud_source_path.as_deref().unwrap_or("None")
                );
                return Ok(stale(MoveState::Failed, "cloud_preflight_failed"));
            }
        };
        let Some(source) = source else {
            let result = stale(MoveState::Stale, "cloud_source_missing");
            repository.save_move_result(plan_id, &result).await?;
            return Ok(result);
        };
        if !cloud_file.matches_identity(&source) {
            let result = stale(MoveState::Stale, "cloud_source_changed");
            repository.save_move_result(plan_id, &result).await?;
            return Ok(result);
        }
        if destination_listing.is_none() {
            let Some((parent, brand, _directory)) = self.cloud_destination_components(record, cloud_paths) else {
                let result = stale(MoveState::InvalidPath, "cloud_destination_changed");
                repository.save_move_result(plan_id, &result).await?;
                return Ok(result);
            };
            destination_listing = match mover.ensure_directory(&parent, &brand).await {
                Ok(true) => list_cloud_directory(mover, record.cloud_destination_dir.as_deref())
                    .await
                    .unwrap_or(None),
                Ok(false) | Err(_) => None,
            };
            if destination_listing.is_none() {
                let result = stale(MoveState::InvalidPath, "cloud_destination_missing");
                repository.save_move_result(plan_id, &result).await?;
                return Ok(result);
            }
        }
        let destination_dir = record.cloud_destination_dir.clone().unwrap_or_default();
        let source_path = record.cloud_source_path.clone().unwrap_or_default();
        let destination_path = pypath::join(&destination_dir, &cloud_file.name);
        if find_cloud_file(&destination_path, destination_listing.as_deref()).is_some() {
            let result = stale(MoveState::Conflict, "cloud_destination_exists");
            repository.save_move_result(plan_id, &result).await?;
            return Ok(result);
        }

        // MoveFile accepts only a path, not an expected file id. Recheck immediately before
        // journaling/submission to minimize the unavoidable remote TOCTOU window.
        let recheck = async {
            let source = stat_cloud_file(mover, Some(&source_path)).await?;
            let destination = stat_cloud_file(mover, Some(&destination_path)).await?;
            Ok::<_, crate::cloud_moves::MoverError>((source, destination))
        };
        let (final_source, final_destination) = match recheck.await {
            Ok(found) => found,
            Err(error) => {
                tracing::error!(
                    logger = LOGGER,
                    error = format!("{}: {error}", error.class_name()),
                    "CloudDrive preflight recheck failed for {source_path}"
                );
                return Ok(stale(MoveState::Failed, "cloud_preflight_failed"));
            }
        };
        if final_source.is_none_or(|source| !cloud_file.matches_identity(&source)) {
            let result = stale(MoveState::Stale, "cloud_source_changed");
            repository.save_move_result(plan_id, &result).await?;
            return Ok(result);
        }
        if final_destination.is_some() {
            let result = stale(MoveState::Conflict, "cloud_destination_exists");
            repository.save_move_result(plan_id, &result).await?;
            return Ok(result);
        }

        let operation = CloudMoveOperationRecord {
            plan_id: plan_id.to_owned(),
            candidate_id: record.candidate_id.clone(),
            attempt_id: self.token(),
            source_path: source_path.clone(),
            destination_dir: destination_dir.clone(),
            state: CloudMoveOperationState::Prepared,
            updated_at: self.now(),
            error_code: None,
        };
        match repository.save_cloud_move_operation(&operation).await {
            Ok(()) => {}
            Err(error) if error.is_value_error() => {
                return Ok(stale(MoveState::Failed, "cloud_move_in_progress"));
            }
            Err(error) => return Err(error.into()),
        }
        let operation = self
            .advance_cloud_operation(&operation, CloudMoveOperationState::Submitting, None)
            .await?;
        let response = match mover.move_file(&source_path, &destination_dir).await {
            Ok(response) => response,
            Err(error) => {
                tracing::error!(
                    logger = LOGGER,
                    error = format!("{}: {error}", error.class_name()),
                    "CloudDrive MoveFile failed for {source_path} -> {destination_dir}"
                );
                let operation = self
                    .advance_cloud_operation(
                        &operation,
                        CloudMoveOperationState::Unknown,
                        Some("cloud_move_transport_unknown"),
                    )
                    .await?;
                return self
                    .observe_cloud_move(plan_id, record, &operation, mover, None, true)
                    .await;
            }
        };
        if !response.success {
            tracing::error!(
                logger = LOGGER,
                "CloudDrive rejected moving {source_path} to {destination_dir}: {}",
                response.error_message.as_deref().unwrap_or("no error message")
            );
        }
        let operation = self
            .advance_cloud_operation(&operation, CloudMoveOperationState::Verifying, None)
            .await?;
        for attempt in 0..CLOUD_VERIFY_ATTEMPTS {
            let result = self
                .observe_cloud_move(
                    plan_id,
                    record,
                    &operation,
                    mover,
                    Some(&response),
                    attempt == CLOUD_VERIFY_ATTEMPTS - 1,
                )
                .await?;
            if result.error_code.as_deref() != Some("cloud_move_verifying") {
                return Ok(result);
            }
            (self.inner.sleeper)(0.5).await;
        }
        Ok(stale(MoveState::Failed, "cloud_move_status_unknown"))
    }

    /// `_recover_cloud_move(plan_id, record, operation)`.
    async fn recover_cloud_move(
        &self,
        plan_id: &str,
        record: &CandidateRecord,
        operation: CloudMoveOperationRecord,
        mover: &dyn CloudFileMover,
    ) -> Result<MoveResult, ServiceError> {
        if operation.state.terminal() {
            let result = terminal_cloud_result(record, &operation);
            self.inner.repository.finalize_cloud_move(&operation, &result).await?;
            return Ok(result);
        }
        if operation.state == CloudMoveOperationState::Prepared {
            return self
                .complete_cloud_move(
                    plan_id,
                    record,
                    &operation,
                    CloudMoveOperationState::Failed,
                    MoveState::Failed,
                    Some("cloud_move_not_submitted"),
                )
                .await;
        }
        let operation = if operation.state == CloudMoveOperationState::Submitting {
            self.advance_cloud_operation(
                &operation,
                CloudMoveOperationState::Unknown,
                Some("cloud_move_interrupted"),
            )
            .await?
        } else {
            operation
        };
        self.observe_cloud_move(plan_id, record, &operation, mover, None, true)
            .await
    }

    /// `_observe_cloud_move(plan_id, record, operation, response=..., final=...)`: conclude
    /// from the listings, or leave the operation under observation.
    #[allow(clippy::too_many_lines, reason = "Python's method, step for step")]
    async fn observe_cloud_move(
        &self,
        plan_id: &str,
        record: &CandidateRecord,
        operation: &CloudMoveOperationRecord,
        mover: &dyn CloudFileMover,
        response: Option<&CloudMoveResponse>,
        last: bool,
    ) -> Result<MoveResult, ServiceError> {
        let Some(cloud_file) = &record.cloud_file else {
            return Ok(cloud_result(
                record,
                MoveState::Failed,
                Some("cloud_candidate_incomplete"),
            ));
        };
        let destination_path = pypath::join(&operation.destination_dir, &cloud_file.name);
        let observed = async {
            let source = stat_cloud_file(mover, Some(&operation.source_path)).await?;
            let destination = stat_cloud_file(mover, Some(&destination_path)).await?;
            Ok::<_, crate::cloud_moves::MoverError>((source, destination))
        };
        let (source, destination) = match observed.await {
            Ok(found) => found,
            Err(error) => {
                tracing::error!(
                    logger = LOGGER,
                    error = format!("{}: {error}", error.class_name()),
                    "CloudDrive observation failed for {}",
                    operation.source_path
                );
                if operation.state != CloudMoveOperationState::Unknown {
                    self.advance_cloud_operation(
                        operation,
                        CloudMoveOperationState::Unknown,
                        Some("cloud_observation_failed"),
                    )
                    .await?;
                }
                return Ok(cloud_result(
                    record,
                    MoveState::Failed,
                    Some("cloud_move_status_unknown"),
                ));
            }
        };

        let source_matches = source.as_ref().is_some_and(|file| cloud_file.matches_identity(file));
        let destination_matches = destination
            .as_ref()
            .is_some_and(|file| cloud_file.matches_identity(file));
        let complete = |state: CloudMoveOperationState, move_state: MoveState, code: Option<&'static str>| {
            self.complete_cloud_move(plan_id, record, operation, state, move_state, code)
        };
        if source.is_none() && destination_matches {
            return complete(CloudMoveOperationState::Succeeded, MoveState::Moved, None).await;
        }
        if source_matches && destination.is_some() && !destination_matches {
            return complete(
                CloudMoveOperationState::Conflict,
                MoveState::Conflict,
                Some("cloud_destination_exists"),
            )
            .await;
        }
        if source.is_some() && !source_matches {
            return complete(
                CloudMoveOperationState::Failed,
                MoveState::Stale,
                Some("cloud_source_changed"),
            )
            .await;
        }
        if response.is_some_and(|response| !response.success) && source_matches && destination.is_none() {
            return complete(
                CloudMoveOperationState::Failed,
                MoveState::Failed,
                Some("cloud_move_rejected"),
            )
            .await;
        }
        if last
            && response.is_none()
            && source_matches
            && destination.is_none()
            && self.now() - operation.updated_at >= CLOUD_MOVE_OBSERVATION_GRACE
        {
            // The source sat untouched and the destination never appeared for the whole
            // grace period: the submission demonstrably never took effect. A response in hand
            // means this is the submitting call itself, which must stay observation-only;
            // without one this is a later recovery pass, and concluding here frees the source
            // for a fresh attempt from a new scan.
            tracing::warn!(
                logger = LOGGER,
                "concluding the CloudDrive move of {} never happened after {GRACE_TEXT} of observation",
                operation.source_path
            );
            return complete(
                CloudMoveOperationState::Failed,
                MoveState::Failed,
                Some("cloud_move_not_observed"),
            )
            .await;
        }
        if !last {
            return Ok(cloud_result(record, MoveState::Failed, Some("cloud_move_verifying")));
        }
        if operation.state != CloudMoveOperationState::Unknown {
            self.advance_cloud_operation(
                operation,
                CloudMoveOperationState::Unknown,
                Some("cloud_move_status_unknown"),
            )
            .await?;
        }
        tracing::warn!(
            logger = LOGGER,
            "CloudDrive move of {} remains unverified; leaving the operation under observation",
            operation.source_path
        );
        Ok(cloud_result(
            record,
            MoveState::Failed,
            Some("cloud_move_status_unknown"),
        ))
    }

    /// `_complete_cloud_move(...)`: the operation's last state and the candidate's result, in
    /// one transaction.
    async fn complete_cloud_move(
        &self,
        plan_id: &str,
        record: &CandidateRecord,
        operation: &CloudMoveOperationRecord,
        operation_state: CloudMoveOperationState,
        move_state: MoveState,
        error_code: Option<&str>,
    ) -> Result<MoveResult, ServiceError> {
        if operation.plan_id != plan_id {
            return Err(ServiceError::Raised {
                class: "ValueError".to_owned(),
                message: "CloudDrive operation plan mismatch".to_owned(),
            });
        }
        let operation = CloudMoveOperationRecord {
            state: operation_state,
            updated_at: self.now(),
            error_code: error_code
                .filter(|_| operation_state != CloudMoveOperationState::Succeeded)
                .map(str::to_owned),
            ..operation.clone()
        };
        let result = cloud_result(record, move_state, error_code);
        self.inner.repository.finalize_cloud_move(&operation, &result).await?;
        Ok(result)
    }

    /// `_advance_cloud_operation(operation, state, error_code=...)`.
    async fn advance_cloud_operation(
        &self,
        operation: &CloudMoveOperationRecord,
        state: CloudMoveOperationState,
        error_code: Option<&str>,
    ) -> Result<CloudMoveOperationRecord, ServiceError> {
        let updated = CloudMoveOperationRecord {
            state,
            updated_at: self.now(),
            error_code: error_code.map(str::to_owned),
            ..operation.clone()
        };
        self.inner.repository.save_cloud_move_operation(&updated).await?;
        Ok(updated)
    }

    /// `_validate_cloud_mapping(record)`: why the `.strm` file no longer maps what the plan
    /// saw, if it does not. An `OSError` of the symlink and containment checks is raised.
    fn validate_cloud_mapping(
        &self,
        record: &CandidateRecord,
        cloud_paths: &CloudMovePaths,
    ) -> Result<Option<&'static str>, ServiceError> {
        if record.mapping_sha256.is_none() || record.cloud_source_path.is_none() {
            return Ok(Some("cloud_candidate_incomplete"));
        }
        let fs = self.fs();
        if local::has_symlink_component(fs, &record.source, &record.source_root)? {
            return Ok(Some("source_symlink"));
        }
        if !local::is_within(fs, &record.source, &record.source_root)? || !fs::is_file(fs, &record.source)? {
            return Ok(Some("source_missing"));
        }
        match local::fingerprint(fs, &record.source) {
            Err(_) => return Ok(Some("invalid_strm_target")),
            Ok(fingerprint) if fingerprint != record.fingerprint => return Ok(Some("source_changed")),
            Ok(_) => {}
        }
        let runtime = self.runtime();
        let Some(paths) = runtime.paths.as_ref() else {
            return Err(ServiceError::fill_actor(
                crate::errors::FillActorErrorKind::NotConfigured,
                "",
            ));
        };
        let Some(source_index) = paths
            .additional_brand_paths
            .iter()
            .position(|root| *root == record.source_root)
        else {
            return Ok(Some("invalid_strm_target"));
        };
        let Ok(parsed) = parse_mapping(fs, cloud_paths, &record.source, source_index) else {
            return Ok(Some("invalid_strm_target"));
        };
        if record.mapping_sha256.as_deref() != Some(parsed.mapping_sha256.as_str())
            || record.cloud_source_path.as_deref() != Some(parsed.api_path.as_str())
        {
            return Ok(Some("strm_target_changed"));
        }
        if self.cloud_destination_components(record, cloud_paths).is_none() {
            return Ok(Some("cloud_destination_changed"));
        }
        Ok(None)
    }

    /// `_cloud_destination_components(record)`: `(move-in API root, brand, directory)` while
    /// the candidate's brand still resolves to the directory the plan chose.
    fn cloud_destination_components(
        &self,
        record: &CandidateRecord,
        cloud_paths: &CloudMovePaths,
    ) -> Option<(String, String, String)> {
        let directory = record.cloud_destination_dir.as_ref()?;
        let brand = self
            .inner
            .brands
            .resolve_brand(&record.video_id)
            .filter(|brand| local::is_safe_segment(brand))?;
        // A safe segment is always a valid brand directory.
        let expected = cloud_paths.destination_directory(&brand).ok()?;
        (expected == *directory).then(|| (cloud_paths.move_in_api_root.as_str().to_owned(), brand, expected))
    }

    // ---------- recovery ----------

    /// The cloud half of `reconcile_moves()`: every operation left open, each under the
    /// mutation lock on a scoped mover of its own.
    pub(super) async fn reconcile_cloud_moves(&self) -> Result<Vec<MoveResult>, ServiceError> {
        let Some(mover) = self.inner.mover.clone() else {
            return Ok(Vec::new());
        };
        let repository = &self.inner.repository;
        let mut results = Vec::new();
        for operation in repository.list_unresolved_cloud_moves().await? {
            let Some(record) = repository
                .get_candidate(&operation.plan_id, &operation.candidate_id)
                .await?
                .filter(|record| record.kind == CandidateKind::CloudStrm)
            else {
                continue;
            };
            let lease = CloudLease::new(mover.scoped(), self.acquire_lock().await?);
            let outcome = async {
                let current = repository
                    .get_cloud_move_operation(&operation.plan_id, &operation.candidate_id)
                    .await?;
                match current.filter(|current| !current.state.terminal()) {
                    None => Ok(None),
                    Some(current) => self
                        .recover_cloud_move(&operation.plan_id, &record, current, &*lease.mover)
                        .await
                        .map(Some),
                }
            }
            .await;
            lease.release().await;
            if let Some(result) = outcome? {
                results.push(result);
            }
        }
        Ok(results)
    }
}

/// `parse_mapping(mapping_path, source_index=...)`, the mapping read through the service's
/// file system.
fn parse_mapping(
    fs: &dyn fs::MoveFs,
    cloud_paths: &CloudMovePaths,
    mapping_path: &Path,
    source_index: usize,
) -> Result<crate::cloud_moves::ParsedStrmTarget, crate::cloud_moves::CheckError> {
    cloud_paths.parse_mapping_from(
        || fs.read_limited(mapping_path, MAX_STRM_BYTES + 1),
        isize::try_from(source_index).unwrap_or(isize::MAX),
    )
}

/// `_list_cloud_directory(api_directory)`: `None` for a directory that is not there.
async fn list_cloud_directory(
    mover: &dyn CloudFileMover,
    api_directory: Option<&str>,
) -> Result<Option<Vec<CloudFileMetadata>>, crate::cloud_moves::MoverError> {
    let Some(api_directory) = api_directory else {
        return Ok(None);
    };
    match mover.list_directory(api_directory).await {
        Ok(listing) => Ok(Some(listing)),
        Err(error) if error.is_file_not_found() => Ok(None),
        Err(error) => Err(error),
    }
}

/// `_stat_cloud_file(api_path)`: the file as its directory lists it.
async fn stat_cloud_file(
    mover: &dyn CloudFileMover,
    api_path: Option<&str>,
) -> Result<Option<CloudFileMetadata>, crate::cloud_moves::MoverError> {
    let Some(api_path) = api_path else {
        return Ok(None);
    };
    let listing = list_cloud_directory(mover, Some(&posix_parent(api_path))).await?;
    Ok(find_cloud_file(api_path, listing.as_deref()).cloned())
}
