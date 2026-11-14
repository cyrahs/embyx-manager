//! What the merge service and auto-merge ask of CloudDrive, Kubernetes and the pipelines.
//!
//! Python passes `AsyncCloudDrive` and `KubeJobs` themselves and the tests pass duck-typed
//! fakes; here the calls are traits so a test can do the same. [`AsyncCloudDrive`] and
//! [`KubeJobs`] implement them. Every CloudDrive call runs in the wrapper's own task
//! (cross-version contract 6): a mutation that was sent completes even when the caller's
//! future is dropped, and the service, which drops a tick at shutdown, awaits
//! [`MergeCloud::settle`] on the [`AsyncCloudDrive::scoped`] handle the tick used.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use embyx_clients::clouddrive::aio::{EnsureResult, MoveResult, OperationResult};
use embyx_clients::clouddrive::{AsyncCloudDrive, CloudDriveError, CloudFile, CopyTask, UploadState};
use embyx_clients::kube::{KubeJobs, KubeJobsError};
use embyx_core::pyjson::Value;

/// A boxed, sendable future.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The wall clock, injected so tests and scenarios can pin it (`datetime.now(UTC)`).
pub type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// The wall clock.
pub fn system_clock() -> Clock {
    Arc::new(Utc::now)
}

/// The CloudDrive calls of the merge service and auto-merge (`AsyncCloudDrive`).
pub trait MergeCloud: Send + Sync {
    /// `list_directory(api_dir, force_refresh=...)`.
    fn list_directory<'a>(
        &'a self,
        api_dir: &'a str,
        force_refresh: bool,
    ) -> BoxFuture<'a, Result<Vec<CloudFile>, CloudDriveError>>;
    /// `stat_file(api_path)`.
    fn stat_file<'a>(&'a self, api_path: &'a str) -> BoxFuture<'a, Result<Option<CloudFile>, CloudDriveError>>;
    /// `copy_tasks()`.
    fn copy_tasks(&self) -> BoxFuture<'_, Result<Vec<CopyTask>, CloudDriveError>>;
    /// `copy_file(source, destination)`.
    fn copy_file<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<OperationResult, CloudDriveError>>;
    /// `upload_state(dest_path)`.
    fn upload_state<'a>(&'a self, dest_path: &'a str) -> BoxFuture<'a, Result<Option<UploadState>, CloudDriveError>>;
    /// `restart_copy_task(source_path, dest_path)`.
    fn restart_copy_task<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<(), CloudDriveError>>;
    /// `cancel_copy_task(source_path, dest_path)`.
    fn cancel_copy_task<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<(), CloudDriveError>>;
    /// `delete_files(api_paths)`.
    fn delete_files<'a>(&'a self, paths: &'a [String]) -> BoxFuture<'a, Result<OperationResult, CloudDriveError>>;
    /// `ensure_directory(parent, name)`.
    fn ensure_directory<'a>(
        &'a self,
        parent: &'a str,
        name: &'a str,
    ) -> BoxFuture<'a, Result<EnsureResult, CloudDriveError>>;
    /// `move_file(source, destination_dir)`.
    fn move_file<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<MoveResult, CloudDriveError>>;
    /// Wait for every call this handle started, also those whose caller was dropped (what a
    /// cancelled Python caller waits for before it re-raises). Immediate for fakes and for a
    /// handle that is not scoped.
    fn settle(&self) -> BoxFuture<'_, ()>;
    /// The same client with a scope of its own ([`AsyncCloudDrive::scoped`]); fakes answer
    /// themselves.
    fn scoped(&self) -> Arc<dyn MergeCloud>;
}

impl MergeCloud for AsyncCloudDrive {
    fn list_directory<'a>(
        &'a self,
        api_dir: &'a str,
        force_refresh: bool,
    ) -> BoxFuture<'a, Result<Vec<CloudFile>, CloudDriveError>> {
        Box::pin(AsyncCloudDrive::list_directory(self, api_dir, force_refresh))
    }

    fn stat_file<'a>(&'a self, api_path: &'a str) -> BoxFuture<'a, Result<Option<CloudFile>, CloudDriveError>> {
        Box::pin(AsyncCloudDrive::stat_file(self, api_path))
    }

    fn copy_tasks(&self) -> BoxFuture<'_, Result<Vec<CopyTask>, CloudDriveError>> {
        Box::pin(AsyncCloudDrive::copy_tasks(self))
    }

    fn copy_file<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<OperationResult, CloudDriveError>> {
        Box::pin(AsyncCloudDrive::copy_file(self, source, destination))
    }

    fn upload_state<'a>(&'a self, dest_path: &'a str) -> BoxFuture<'a, Result<Option<UploadState>, CloudDriveError>> {
        Box::pin(AsyncCloudDrive::upload_state(self, dest_path))
    }

    fn restart_copy_task<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<(), CloudDriveError>> {
        Box::pin(AsyncCloudDrive::restart_copy_task(self, source, destination))
    }

    fn cancel_copy_task<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<(), CloudDriveError>> {
        Box::pin(AsyncCloudDrive::cancel_copy_task(self, source, destination))
    }

    fn delete_files<'a>(&'a self, paths: &'a [String]) -> BoxFuture<'a, Result<OperationResult, CloudDriveError>> {
        Box::pin(AsyncCloudDrive::delete_files(self, paths))
    }

    fn ensure_directory<'a>(
        &'a self,
        parent: &'a str,
        name: &'a str,
    ) -> BoxFuture<'a, Result<EnsureResult, CloudDriveError>> {
        Box::pin(AsyncCloudDrive::ensure_directory(self, parent, name))
    }

    fn move_file<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<MoveResult, CloudDriveError>> {
        Box::pin(AsyncCloudDrive::move_file(self, source, destination))
    }

    fn settle(&self) -> BoxFuture<'_, ()> {
        Box::pin(AsyncCloudDrive::settle(self))
    }

    fn scoped(&self) -> Arc<dyn MergeCloud> {
        Arc::new(AsyncCloudDrive::scoped(self))
    }
}

/// The Job calls of the merge service (`KubeJobs`).
pub trait MergeKube: Send + Sync {
    /// `create_job(name=..., args=..., task_id=...)`.
    fn create_job<'a>(
        &'a self,
        name: &'a str,
        args: &'a [String],
        task_id: i64,
    ) -> BoxFuture<'a, Result<(), KubeJobsError>>;
    /// `get_job(name)`.
    fn get_job<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Option<Value>, KubeJobsError>>;
    /// `delete_job(name)`.
    fn delete_job<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<(), KubeJobsError>>;
    /// `log_tail(name)` with Python's default of 20 lines.
    fn log_tail<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<String, KubeJobsError>>;
    /// `aclose()`.
    fn aclose(&self) -> BoxFuture<'_, ()>;
}

/// `log_tail`'s default line count.
pub const LOG_TAIL_LINES: u32 = 20;

impl MergeKube for KubeJobs {
    fn create_job<'a>(
        &'a self,
        name: &'a str,
        args: &'a [String],
        task_id: i64,
    ) -> BoxFuture<'a, Result<(), KubeJobsError>> {
        Box::pin(KubeJobs::create_job(self, name, args, task_id))
    }

    fn get_job<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Option<Value>, KubeJobsError>> {
        Box::pin(KubeJobs::get_job(self, name))
    }

    fn delete_job<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<(), KubeJobsError>> {
        Box::pin(KubeJobs::delete_job(self, name))
    }

    fn log_tail<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<String, KubeJobsError>> {
        Box::pin(KubeJobs::log_tail(self, name, LOG_TAIL_LINES))
    }

    fn aclose(&self) -> BoxFuture<'_, ()> {
        // httpx's `aclose()` closes the pool; reqwest's pool goes with the last clone.
        Box::pin(async {})
    }
}

/// A pipeline the service asks for a run after a step (`PipelineName.ARCHIVE` / `MAPPING`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pipeline {
    Archive,
    Mapping,
}

impl Pipeline {
    /// `PipelineName(...).value`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Archive => "archive",
            Self::Mapping => "mapping",
        }
    }
}

/// `trigger(pipeline)`: start a run now; the application logs and swallows a busy or
/// unconfigured pipeline, so nothing comes back.
pub type Trigger = Arc<dyn Fn(Pipeline) -> BoxFuture<'static, ()> + Send + Sync>;
