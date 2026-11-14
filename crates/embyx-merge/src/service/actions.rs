//! The operator's actions on a task: cancel, retry, pause the home worker, send a home
//! transcode back to the merge Job's own encoder.
//!
//! Python origin: the "operator actions" section and `set_paused` / `use_cpu` of
//! `MergeService` in `src/embyx_manager/merge/service.py`. Each runs under the tick lock, so a
//! tick never sees a task half way through one. CloudDrive is reached through a
//! [`MergeCloud::scoped`] handle that is settled before the action answers (cross-version
//! contract 6).

use std::sync::Arc;

use super::{
    LOGGER, MergeService, StepError, copy_task, is_home_retry_state, rmtree_ignore_errors, suppress_kube,
    unlink_missing_ok,
};
use crate::ports::MergeCloud;
use crate::tasks::{MergeState, MergeTask, TaskField};
use crate::transcode::{audio_job_name, audio_status_path, transcode_dir};
use crate::worker::{partial_path, read_status, staging_dir};
use embyx_clients::clouddrive::CopyStatus;

/// Why an action was not done.
#[derive(Debug, Clone, PartialEq)]
pub enum ActionError {
    /// `MergeActionError(code)`: the task's state does not allow it; `code` is the API's.
    Refused(&'static str),
    /// Anything else Python lets through to the server's error handler.
    Error(StepError),
}

impl std::fmt::Display for ActionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(code) => f.write_str(code),
            Self::Error(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ActionError {}

impl<E: Into<StepError>> From<E> for ActionError {
    fn from(error: E) -> Self {
        Self::Error(error.into())
    }
}

/// A scoped CloudDrive handle, settled when the action is over however it ends.
struct Scoped(Option<Arc<dyn MergeCloud>>);

impl Scoped {
    async fn settle(&self) {
        if let Some(cloud) = &self.0 {
            cloud.settle().await;
        }
    }
}

impl MergeService {
    async fn task_or_refuse(&self, task_id: i64) -> Result<MergeTask, ActionError> {
        self.repository()
            .get(task_id)
            .await?
            .ok_or(ActionError::Refused("merge_task_not_found"))
    }

    /// `cancel(task_id)`.
    pub async fn cancel(&self, task_id: i64) -> Result<MergeTask, ActionError> {
        let cloud = Scoped((self.inner.cloud)().map(|cloud| cloud.scoped()));
        let outcome = {
            let _guard = self.lock().await;
            self.cancel_locked(task_id, cloud.0.as_ref()).await
        };
        cloud.settle().await;
        outcome
    }

    async fn cancel_locked(&self, task_id: i64, cloud: Option<&Arc<dyn MergeCloud>>) -> Result<MergeTask, ActionError> {
        let task = self.task_or_refuse(task_id).await?;
        if !task.state.is_cancellable() {
            return Err(ActionError::Refused("merge_task_not_cancellable"));
        }
        if task.state == MergeState::Failed && task.failed_state == Some(MergeState::Replacing) {
            // The originals may already be gone; the merged file must not be dropped.
            return Err(ActionError::Refused("merge_task_not_cancellable"));
        }
        let paths = self.paths(&task)?;
        if task.state == MergeState::Failed && task.failed_state == Some(MergeState::Archiving) {
            self.discard_duplicate(&task, &paths, cloud).await?;
        }
        if let (Some(job), Some(kube)) = (task.job_name.as_deref().filter(|job| !job.is_empty()), self.kube()) {
            suppress_kube(kube.delete_job(job).await)?;
        }
        if let Some(cloud) = cloud
            && task.merged_sha1.as_deref().is_some_and(|sha1| !sha1.is_empty())
        {
            // `with suppress(Exception)`, twice.
            if let Ok(Some(copy)) = copy_task(cloud.as_ref(), &paths).await
                && copy.status != CopyStatus::Completed
            {
                let _ = cloud.cancel_copy_task(&copy.source_path, &paths.staging_dir).await;
            }
            let _ = cloud.delete_files(std::slice::from_ref(&paths.staging_dir)).await;
        }
        for path in [paths.output.clone(), partial_path(&paths.output), paths.status.clone()] {
            unlink_missing_ok(&path)?;
        }
        rmtree_ignore_errors(&staging_dir(&paths.output));
        if task.transcoder.as_deref() == Some("home") {
            self.discard_transcode(&task).await?;
        }
        self.repository()
            .transition(
                task.id,
                task.state,
                MergeState::Cancelled,
                vec![TaskField::FinishedAt(Some(self.now())), TaskField::Notice(None)],
            )
            .await?
            .ok_or(ActionError::Refused("merge_task_changed"))
    }

    /// `_discard_duplicate`: delete a merged copy the archive refused because the library
    /// holds the title whole. The parts are gone by now, so the merged file may be dropped
    /// only when that whole copy is there to take its place.
    async fn discard_duplicate(
        &self,
        task: &MergeTask,
        paths: &super::MergePaths,
        cloud: Option<&Arc<dyn MergeCloud>>,
    ) -> Result<(), ActionError> {
        let whole = match cloud {
            None => None,
            Some(_) => match self.whole_copy(cloud, task, paths).await {
                Ok(whole) => whole,
                Err(error) => {
                    tracing::error!(
                        logger = LOGGER,
                        error = %error,
                        "could not look for a whole copy of {}",
                        task.avid
                    );
                    return Err(ActionError::Refused("merge_discard_failed"));
                }
            },
        };
        let (Some(cloud), Some(_)) = (cloud, whole) else {
            return Err(ActionError::Refused("merge_task_not_cancellable"));
        };
        let listed = async {
            cloud.delete_files(std::slice::from_ref(&paths.target_dir)).await?;
            cloud.list_directory(&paths.intake_dir, true).await
        }
        .await;
        let intake = match listed {
            Ok(entries) => entries,
            Err(error) if error.is_file_not_found() => Vec::new(),
            Err(error) => {
                tracing::error!(
                    logger = LOGGER,
                    error = %error,
                    "could not delete the merged copy of {}",
                    task.avid
                );
                return Err(ActionError::Refused("merge_discard_failed"));
            }
        };
        if intake.iter().any(|entry| entry.full_path == paths.target_dir) {
            return Err(ActionError::Refused("merge_discard_failed"));
        }
        Ok(())
    }

    /// `_reset_prepare`: clear a failed audio Job and its report, so the next tick starts it
    /// again.
    async fn reset_prepare(&self, task: &MergeTask) -> Result<(), StepError> {
        let status = audio_status_path(&transcode_dir(&super::py_path(&self.work_dir()), task.id));
        let failed = read_status(&status)
            .unwrap_or_default()
            .get("state")
            .is_some_and(|state| super::pyops::is_str(state, "failed"));
        if failed {
            unlink_missing_ok(&status)?;
        }
        if let Some(kube) = self.kube() {
            suppress_kube(kube.delete_job(&audio_job_name(task.id, &task.avid)).await)?;
        }
        Ok(())
    }

    /// `retry(task_id)`: resume a failed task where it can safely go on.
    pub async fn retry(&self, task_id: i64) -> Result<MergeTask, ActionError> {
        let retried = {
            let _guard = self.lock().await;
            let task = self.task_or_refuse(task_id).await?;
            if task.state != MergeState::Failed {
                return Err(ActionError::Refused("merge_task_not_failed"));
            }
            let mut resume = match task.failed_state.unwrap_or(MergeState::Queued) {
                MergeState::Uploading | MergeState::Verifying => MergeState::Uploading,
                MergeState::Replacing => MergeState::Replacing,
                MergeState::Archiving => MergeState::Archiving,
                _ => MergeState::Queued,
            };
            if task.transcoder.as_deref() == Some("home") && is_home_retry_state(task.failed_state) {
                // The chunks are kept: a failed audio copy is redone, a failed assembly run
                // again.
                resume = MergeState::Transcoding;
                self.reset_prepare(&task).await?;
            }
            let mut fields = vec![
                TaskField::FailedState(None),
                TaskField::Error(None),
                TaskField::Notice(None),
            ];
            if matches!(resume, MergeState::Queued | MergeState::Transcoding) {
                // The failed Job outlives the task by its TTL and holds the name the new one is
                // started under, which would count as started.
                if let (Some(job), Some(kube)) = (task.job_name.as_deref().filter(|job| !job.is_empty()), self.kube()) {
                    suppress_kube(kube.delete_job(job).await)?;
                }
                fields.extend([
                    TaskField::JobName(None),
                    TaskField::Phase(None),
                    TaskField::Progress(None),
                ]);
            }
            if resume == MergeState::Uploading {
                fields.push(TaskField::UploadAttempts(0));
            }
            self.repository()
                .transition(task.id, MergeState::Failed, resume, fields)
                .await?
                .ok_or(ActionError::Refused("merge_task_changed"))?
        };
        self.wake();
        Ok(retried)
    }

    /// `set_paused(task_id, paused=...)`: ask the home worker to stop after its current chunk,
    /// or to go on.
    pub async fn set_paused(&self, task_id: i64, paused: bool) -> Result<MergeTask, ActionError> {
        let _guard = self.lock().await;
        let task = self.task_or_refuse(task_id).await?;
        if !matches!(task.state, MergeState::Queued | MergeState::Transcoding) || !self.home_bound(&task) {
            return Err(ActionError::Refused("merge_task_not_transcoding"));
        }
        self.repository()
            .update(task.id, vec![TaskField::TranscodePaused(paused)])
            .await?
            .ok_or(ActionError::Refused("merge_task_changed"))
    }

    /// `use_cpu(task_id)`: send a home transcode back to the merge Job's own encoder, dropping
    /// what the worker did.
    pub async fn use_cpu(&self, task_id: i64) -> Result<MergeTask, ActionError> {
        let moved = {
            let _guard = self.lock().await;
            let task = self.task_or_refuse(task_id).await?;
            let failed_home = task.state == MergeState::Failed
                && (task.failed_state == Some(MergeState::Queued) || is_home_retry_state(task.failed_state));
            let movable = matches!(task.state, MergeState::Queued | MergeState::Transcoding) || failed_home;
            if !movable || !self.home_bound(&task) {
                return Err(ActionError::Refused("merge_task_not_transcoding"));
            }
            self.discard_transcode(&task).await?;
            if let (Some(job), Some(kube)) = (task.job_name.as_deref().filter(|job| !job.is_empty()), self.kube()) {
                suppress_kube(kube.delete_job(job).await)?;
            }
            self.repository()
                .transition(
                    task.id,
                    task.state,
                    MergeState::Queued,
                    vec![
                        TaskField::Transcoder(Some("cpu".to_owned())),
                        TaskField::TranscodePaused(false),
                        TaskField::JobName(None),
                        TaskField::Phase(None),
                        TaskField::Progress(None),
                        TaskField::FailedState(None),
                        TaskField::Error(None),
                        TaskField::Notice(None),
                    ],
                )
                .await?
                .ok_or(ActionError::Refused("merge_task_changed"))?
        };
        self.wake();
        Ok(moved)
    }
}
