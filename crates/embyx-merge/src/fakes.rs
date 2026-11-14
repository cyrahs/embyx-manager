//! In-memory CloudDrive and Job API for the service's and auto-merge's tests.
//!
//! Python origin: `FakeCloud` and `FakeKube` of `tests/test_merge_service.py` (and their
//! copies in `tests/test_merge_auto.py` and `tests/test_merge_transcode.py`). Clones share
//! their state, so a test keeps one and hands the service another.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use embyx_clients::clouddrive::aio::{EnsureResult, MoveResult, OperationResult};
use embyx_clients::clouddrive::{CloudDriveError, CloudFile, CopyStatus, CopyTask, UploadState};
use embyx_clients::kube::KubeJobsError;
use embyx_core::pyjson::{Dict, Value};
use embyx_core::pypath;

use crate::ports::{BoxFuture, MergeCloud, MergeKube};
use crate::service::pure_parent;

/// One file CloudDrive lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeFile {
    pub size: i64,
    /// Hash kind (as text) to hash.
    pub hashes: BTreeMap<String, String>,
}

/// What [`FakeCloud`] holds.
#[derive(Debug, Clone, Default)]
pub struct CloudState {
    pub files: BTreeMap<String, FakeFile>,
    pub directories: BTreeSet<String>,
    pub copies: Vec<CopyTask>,
    pub restarted: Vec<(String, String)>,
    pub cancelled: Vec<(String, String)>,
    pub deleted: Vec<String>,
    /// Why the upload queue gave up on a destination path.
    pub fatal: BTreeMap<String, String>,
    /// Bytes the upload queue reports sent, by destination path.
    pub sent: BTreeMap<String, u64>,
    /// Directories whose listing fails with an RPC error.
    pub broken: BTreeSet<String>,
    /// How many listings were asked for, by directory and whether they were fresh.
    pub listings: Vec<(String, bool)>,
}

/// CloudDrive's API paths as a map; copies finish when a test says so.
#[derive(Debug, Clone, Default)]
pub struct FakeCloud {
    state: Arc<Mutex<CloudState>>,
}

impl FakeCloud {
    /// The directories `tests/test_merge_service.py`'s `FakeCloud` starts with.
    pub fn new(directories: &[&str]) -> Self {
        let cloud = Self::default();
        cloud.state().directories = directories.iter().map(|path| (*path).to_owned()).collect();
        cloud
    }

    pub fn state(&self) -> MutexGuard<'_, CloudState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `add_file(path, size, sha1='')`.
    pub fn add_file(&self, path: &str, size: i64, sha1: &str) {
        let mut hashes = BTreeMap::new();
        if !sha1.is_empty() {
            hashes.insert("2".to_owned(), sha1.to_owned());
        }
        self.state().files.insert(path.to_owned(), FakeFile { size, hashes });
    }

    pub fn has_file(&self, path: &str) -> bool {
        self.state().files.contains_key(path)
    }

    fn entry(path: &str, file: &FakeFile) -> CloudFile {
        CloudFile {
            id: String::new(),
            name: pypath::name(path).to_owned(),
            full_path: path.to_owned(),
            size: file.size,
            is_directory: false,
            write_time: None,
            hashes: file.hashes.clone(),
        }
    }
}

fn ok() -> OperationResult {
    OperationResult {
        success: true,
        error_message: String::new(),
    }
}

impl MergeCloud for FakeCloud {
    fn list_directory<'a>(
        &'a self,
        api_dir: &'a str,
        force_refresh: bool,
    ) -> BoxFuture<'a, Result<Vec<CloudFile>, CloudDriveError>> {
        Box::pin(async move {
            let mut state = self.state();
            state.listings.push((api_dir.to_owned(), force_refresh));
            if state.broken.contains(api_dir) {
                return Err(CloudDriveError::Closed("CloudDrive is unreachable"));
            }
            if !state.directories.contains(api_dir) {
                return Err(CloudDriveError::FileNotFound {
                    message: "No such file or directory".to_owned(),
                    path: api_dir.to_owned(),
                });
            }
            let mut entries: Vec<CloudFile> = state
                .files
                .iter()
                .filter(|(path, _)| pure_parent(path) == api_dir)
                .map(|(path, file)| Self::entry(path, file))
                .collect();
            entries.extend(
                state
                    .directories
                    .iter()
                    .filter(|path| path.as_str() != "/" && pure_parent(path) == api_dir)
                    .map(|path| CloudFile {
                        id: String::new(),
                        name: pypath::name(path).to_owned(),
                        full_path: path.clone(),
                        size: 0,
                        is_directory: true,
                        write_time: None,
                        hashes: BTreeMap::new(),
                    }),
            );
            Ok(entries)
        })
    }

    fn stat_file<'a>(&'a self, api_path: &'a str) -> BoxFuture<'a, Result<Option<CloudFile>, CloudDriveError>> {
        Box::pin(async move { Ok(self.state().files.get(api_path).map(|file| Self::entry(api_path, file))) })
    }

    fn copy_tasks(&self) -> BoxFuture<'_, Result<Vec<CopyTask>, CloudDriveError>> {
        Box::pin(async move { Ok(self.state().copies.clone()) })
    }

    fn copy_file<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<OperationResult, CloudDriveError>> {
        Box::pin(async move {
            self.state().copies.push(CopyTask {
                source_path: source.to_owned(),
                dest_path: destination.to_owned(),
                status: CopyStatus::Pending,
                total_bytes: 0,
                uploaded_bytes: 0,
                failed_files: 0,
                paused: false,
                errors: Vec::new(),
            });
            Ok(ok())
        })
    }

    fn upload_state<'a>(&'a self, dest_path: &'a str) -> BoxFuture<'a, Result<Option<UploadState>, CloudDriveError>> {
        Box::pin(async move {
            let state = self.state();
            if let Some(error) = state.fatal.get(dest_path) {
                return Ok(Some(UploadState {
                    status: "FatalError".to_owned(),
                    transferred: 0,
                    error: error.clone(),
                }));
            }
            Ok(state.sent.get(dest_path).map(|sent| UploadState {
                status: "Transfer".to_owned(),
                transferred: *sent,
                error: String::new(),
            }))
        })
    }

    fn restart_copy_task<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<(), CloudDriveError>> {
        Box::pin(async move {
            self.state().restarted.push((source.to_owned(), destination.to_owned()));
            Ok(())
        })
    }

    fn cancel_copy_task<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<(), CloudDriveError>> {
        Box::pin(async move {
            let mut state = self.state();
            state.cancelled.push((source.to_owned(), destination.to_owned()));
            state.copies.retain(|copy| copy.dest_path != destination);
            Ok(())
        })
    }

    fn delete_files<'a>(&'a self, paths: &'a [String]) -> BoxFuture<'a, Result<OperationResult, CloudDriveError>> {
        Box::pin(async move {
            let mut state = self.state();
            for path in paths {
                state.deleted.push(path.clone());
                state.files.remove(path);
                state.directories.remove(path);
            }
            Ok(ok())
        })
    }

    fn ensure_directory<'a>(
        &'a self,
        parent: &'a str,
        name: &'a str,
    ) -> BoxFuture<'a, Result<EnsureResult, CloudDriveError>> {
        Box::pin(async move {
            let path = format!("{}/{name}", parent.trim_end_matches('/'));
            let created = self.state().directories.insert(path.clone());
            Ok(EnsureResult {
                success: true,
                created,
                path,
            })
        })
    }

    fn move_file<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<MoveResult, CloudDriveError>> {
        Box::pin(async move {
            let mut state = self.state();
            let target = format!("{destination}/{}", pypath::name(source));
            // `self.files.pop(source)`: Python's fake raises `KeyError` for a source it does not
            // hold, so a test that moves the wrong path fails here as it does there.
            let file = state
                .files
                .remove(source)
                .unwrap_or_else(|| panic!("KeyError: {source:?} is not in the fake CloudDrive"));
            state.files.insert(target.clone(), file);
            Ok(MoveResult {
                success: true,
                error_message: String::new(),
                result_file_paths: vec![target],
            })
        })
    }

    fn settle(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn scoped(&self) -> Arc<dyn MergeCloud> {
        Arc::new(self.clone())
    }
}

/// What [`FakeKube`] holds.
#[derive(Debug, Clone, Default)]
pub struct KubeState {
    /// Each Job's `status` object, by name.
    pub jobs: BTreeMap<String, Dict>,
    pub created: Vec<(String, Vec<String>)>,
    pub deleted: Vec<String>,
    /// What `log_tail` answers.
    pub log: String,
}

/// The Job API; a Job's status changes when a test says so.
#[derive(Debug, Clone)]
pub struct FakeKube {
    state: Arc<Mutex<KubeState>>,
}

impl Default for FakeKube {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(KubeState {
                log: "ffmpeg: boom".to_owned(),
                ..KubeState::default()
            })),
        }
    }
}

impl FakeKube {
    pub fn state(&self) -> MutexGuard<'_, KubeState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Set a Job's status to one condition (`Complete` or `Failed`) with an optional reason
    /// message.
    pub fn set_condition(&self, name: &str, kind: &str, message: Option<&str>) {
        let mut condition = Dict::new();
        condition.insert("type", Value::from(kind));
        condition.insert("status", Value::from("True"));
        if let Some(message) = message {
            condition.insert("reason", Value::from("DeadlineExceeded"));
            condition.insert("message", Value::from(message));
        }
        let mut status = Dict::new();
        status.insert("conditions", Value::List(vec![Value::Dict(condition)]));
        self.state().jobs.insert(name.to_owned(), status);
    }

    /// The Job as `get_job` answers it: `{'status': {...}}`.
    fn job(status: &Dict) -> Value {
        let mut job = Dict::new();
        job.insert("status", Value::Dict(status.clone()));
        Value::Dict(job)
    }
}

impl MergeKube for FakeKube {
    fn create_job<'a>(
        &'a self,
        name: &'a str,
        args: &'a [String],
        _task_id: i64,
    ) -> BoxFuture<'a, Result<(), KubeJobsError>> {
        Box::pin(async move {
            let mut state = self.state();
            state.created.push((name.to_owned(), args.to_vec()));
            // As the API answers 409 for a name in use, an existing Job is kept.
            state.jobs.entry(name.to_owned()).or_default();
            Ok(())
        })
    }

    fn get_job<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Option<Value>, KubeJobsError>> {
        Box::pin(async move { Ok(self.state().jobs.get(name).map(Self::job)) })
    }

    fn delete_job<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<(), KubeJobsError>> {
        Box::pin(async move {
            let mut state = self.state();
            state.deleted.push(name.to_owned());
            state.jobs.remove(name);
            Ok(())
        })
    }

    fn log_tail<'a>(&'a self, _name: &'a str) -> BoxFuture<'a, Result<String, KubeJobsError>> {
        Box::pin(async move { Ok(self.state().log.clone()) })
    }

    fn aclose(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}
