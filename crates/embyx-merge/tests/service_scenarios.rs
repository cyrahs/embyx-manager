//! The merge service scenarios (`tools/scenarios/specs/merge/`, operation `merge.service`): the
//! Rust port of `tools/scenarios/operations/merge_service.py`. Each spec's steps run through
//! the ported service, auto-merge and home transcode bookkeeping over the seeded work volume,
//! the tasks on PostgreSQL, the CloudDrive fake and the fake Kubernetes API's Jobs model, and
//! must render the bytes the Python baseline committed under `goldens/scenarios/merge/`.
//!
//! The Python operation pins `datetime` in the merge modules; here one shared clock is handed
//! to every repository, the presence, the title cache, the service and auto-merge.
//! `cancel_after` aborts the step's task once that CloudDrive call answered, as `task.cancel()`
//! cancels the Python one right after the call returned.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use embyx_clients::clouddrive::aio::{EnsureResult, MoveResult, OperationResult};
use embyx_clients::clouddrive::fake::FakeCloudDrive;
use embyx_clients::clouddrive::{AsyncCloudDrive, ClientOptions, CloudDriveError, CloudFile, CopyTask, UploadState};
use embyx_clients::fakehttp::FakeHttp;
use embyx_clients::kube::{KubeJobs, KubeOptions};
use embyx_clients::scenario::ScenarioFakes;
use embyx_core::config::lax::Mode;
use embyx_core::config::models::{ArchiveConfig, CloudDriveConfig, MappingConfig, MergeConfig};
use embyx_core::pyjson::{self, Dict, DumpOptions, Value, py_isoformat_datetime};
use embyx_core::scenario::{self, Parts, Recorder, Seeded, Spec};
use embyx_db::database::Database;
use embyx_db::testdb::TestDatabase;
use embyx_merge::auto::{AutoMerger, AutoParts};
use embyx_merge::catalog::{AutoStatus, MergeCatalog, TaskDirsFor, TitleScanCache};
use embyx_merge::ports::{BoxFuture, Clock, MergeCloud, MergeKube, Pipeline, Trigger};
use embyx_merge::service::{ActionError, MergeService, ServiceParts, StepError, WorkerCallError};
use embyx_merge::tasks::{MergeTask, MergeTaskRepository, NewTask};
use embyx_merge::transcode::{Chunk, TranscodeRepository, WorkerPresence, WorkerReport};
use sha2::{Digest as _, Sha256};
use tokio::task::AbortHandle;

use common::task;

/// `merge_service.TOKEN`.
const TOKEN: &str = "scenario-token";
/// `auto.GIB`.
const GIB: i128 = 1 << 30;

fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------- reading the spec ----------

fn text<'a>(args: &'a Value, key: &str) -> &'a str {
    args.get(key)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("{key:?} is a string in {args:?}"))
}

fn int(value: &Value) -> i64 {
    match value {
        Value::Int(int) => i64::try_from(*int).expect("small"),
        other => panic!("expected an int, got {other:?}"),
    }
}

fn int_arg(args: &Value, key: &str) -> i64 {
    int(args.get(key).unwrap_or_else(|| panic!("{key:?} in {args:?}")))
}

/// `float(value)` of an int or a float.
fn float(value: &Value) -> f64 {
    match value {
        #[expect(clippy::cast_precision_loss, reason = "float(int)")]
        Value::Int(int) => *int as f64,
        Value::Float(float) => *float,
        other => panic!("expected a number, got {other:?}"),
    }
}

fn empty() -> Value {
    Value::Dict(Dict::new())
}

fn error_value(class: &str, message: String) -> Value {
    let mut error = Dict::new();
    error.insert("type", Value::from(class));
    error.insert("message", Value::Str(message));
    Value::Dict(error)
}

/// `model.model_validate({**model().model_dump(), **fields})`.
fn model<S: embyx_core::config::Section>(fields: Option<&Value>, extra: &[(&str, Value)]) -> S {
    let Value::Dict(mut merged) = S::default().dump() else {
        panic!("a model dumps to an object");
    };
    if let Some(Value::Dict(given)) = fields {
        for (key, value) in given.iter() {
            merged.insert(key.as_str().expect("a field name"), value.clone());
        }
    }
    for (key, value) in extra {
        merged.insert(*key, value.clone());
    }
    S::validate(&Value::Dict(merged), Mode::Python).expect("a valid section")
}

/// pydantic's JSON of an aware UTC datetime: `Z` for the offset.
fn pydantic_datetime(value: DateTime<Utc>) -> Value {
    let text = py_isoformat_datetime(&value.fixed_offset());
    Value::Str(
        text.strip_suffix("+00:00")
            .map_or(text.clone(), |base| format!("{base}Z")),
    )
}

/// `AutoMerger.status().model_dump(mode='json')`.
fn auto_status(status: &AutoStatus) -> Value {
    let mut fields = Dict::new();
    fields.insert("state", Value::from(status.state.as_str()));
    fields.insert("room", status.room.map_or(Value::Null, Value::Int));
    fields.insert(
        "skipped",
        Value::List(
            status
                .skipped
                .iter()
                .map(|title| {
                    let mut skipped = Dict::new();
                    skipped.insert("avid", Value::Str(title.avid.clone()));
                    skipped.insert("size", title.size.map_or(Value::Null, Value::Int));
                    skipped.insert("reason", Value::Str(title.reason.clone()));
                    Value::Dict(skipped)
                })
                .collect(),
        ),
    );
    fields.insert(
        "skipped_count",
        Value::Int(i128::try_from(status.skipped_count).expect("a count")),
    );
    fields.insert("checked_at", status.checked_at.map_or(Value::Null, pydantic_datetime));
    Value::Dict(fields)
}

// ---------- the CloudDrive wrapper ----------

/// What `_Cloud` does at the current step.
#[derive(Default)]
struct CloudControl {
    /// `cancel_after = "<method>"`.
    cancel_after: Mutex<Option<String>>,
    /// The step's task.
    step: Mutex<Option<AbortHandle>>,
}

impl CloudControl {
    async fn answered(&self, method: &str) {
        let step = {
            let mut cancel_after = lock(&self.cancel_after);
            if cancel_after.as_deref() != Some(method) {
                return;
            }
            *cancel_after = None;
            lock(&self.step).clone()
        };
        if let Some(step) = step {
            step.abort();
            tokio::task::yield_now().await;
        }
    }
}

/// `_Cloud`: `AsyncCloudDrive` that cancels the running step once `cancel_after` answered.
/// Its scoped handles share the control.
struct ScenarioCloud {
    inner: Arc<dyn MergeCloud>,
    control: Arc<CloudControl>,
}

impl ScenarioCloud {
    async fn after<T>(&self, method: &str, answer: T) -> T {
        self.control.answered(method).await;
        answer
    }
}

impl MergeCloud for ScenarioCloud {
    fn list_directory<'a>(
        &'a self,
        api_dir: &'a str,
        force_refresh: bool,
    ) -> BoxFuture<'a, Result<Vec<CloudFile>, CloudDriveError>> {
        Box::pin(async move {
            let answer = self.inner.list_directory(api_dir, force_refresh).await;
            self.after("list_directory", answer).await
        })
    }

    fn stat_file<'a>(&'a self, api_path: &'a str) -> BoxFuture<'a, Result<Option<CloudFile>, CloudDriveError>> {
        Box::pin(async move {
            let answer = self.inner.stat_file(api_path).await;
            self.after("stat_file", answer).await
        })
    }

    fn copy_tasks(&self) -> BoxFuture<'_, Result<Vec<CopyTask>, CloudDriveError>> {
        Box::pin(async move {
            let answer = self.inner.copy_tasks().await;
            self.after("copy_tasks", answer).await
        })
    }

    fn copy_file<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<OperationResult, CloudDriveError>> {
        Box::pin(async move {
            let answer = self.inner.copy_file(source, destination).await;
            self.after("copy_file", answer).await
        })
    }

    fn upload_state<'a>(&'a self, dest_path: &'a str) -> BoxFuture<'a, Result<Option<UploadState>, CloudDriveError>> {
        Box::pin(async move {
            let answer = self.inner.upload_state(dest_path).await;
            self.after("upload_state", answer).await
        })
    }

    fn restart_copy_task<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<(), CloudDriveError>> {
        Box::pin(async move {
            let answer = self.inner.restart_copy_task(source, destination).await;
            self.after("restart_copy_task", answer).await
        })
    }

    fn cancel_copy_task<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<(), CloudDriveError>> {
        Box::pin(async move {
            let answer = self.inner.cancel_copy_task(source, destination).await;
            self.after("cancel_copy_task", answer).await
        })
    }

    fn delete_files<'a>(&'a self, paths: &'a [String]) -> BoxFuture<'a, Result<OperationResult, CloudDriveError>> {
        Box::pin(async move {
            let answer = self.inner.delete_files(paths).await;
            self.after("delete_files", answer).await
        })
    }

    fn ensure_directory<'a>(
        &'a self,
        parent: &'a str,
        name: &'a str,
    ) -> BoxFuture<'a, Result<EnsureResult, CloudDriveError>> {
        Box::pin(async move {
            let answer = self.inner.ensure_directory(parent, name).await;
            self.after("ensure_directory", answer).await
        })
    }

    fn move_file<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
    ) -> BoxFuture<'a, Result<MoveResult, CloudDriveError>> {
        Box::pin(async move {
            let answer = self.inner.move_file(source, destination).await;
            self.after("move_file", answer).await
        })
    }

    fn settle(&self) -> BoxFuture<'_, ()> {
        self.inner.settle()
    }

    fn scoped(&self) -> Arc<dyn MergeCloud> {
        Arc::new(Self {
            inner: self.inner.scoped(),
            control: Arc::clone(&self.control),
        })
    }
}

// ---------- the world ----------

/// What stays across a `restart`: the database, the roots, the fakes and the clock.
struct Ground {
    spec: Spec,
    args: Value,
    seeded: Seeded,
    test: TestDatabase,
    fakes: ScenarioFakes,
    now: Arc<Mutex<DateTime<Utc>>>,
    free_gib: Arc<Mutex<i64>>,
    control: Arc<CloudControl>,
}

/// `_World`: the service, auto-merge and their clients; built again by `restart`.
struct World {
    database: Database,
    repository: MergeTaskRepository,
    cloud: AsyncCloudDrive,
    service: MergeService,
    auto: AutoMerger,
    triggered: Arc<Mutex<Vec<Pipeline>>>,
}

fn path_text(path: &std::path::Path) -> String {
    path.to_str().expect("a UTF-8 path").to_owned()
}

impl Ground {
    fn clock(&self) -> Clock {
        let now = Arc::clone(&self.now);
        Arc::new(move || *lock(&now))
    }

    #[expect(clippy::too_many_lines, reason = "one world, built as `_World.__init__` builds it")]
    fn build(&self) -> World {
        let args = &self.args;
        // `_World.__init__` reads `free_gib` again: a `free_space` step lasts until a restart.
        *lock(&self.free_gib) = args.get("free_gib").map_or(100, int);
        let work = path_text(self.seeded.root("work"));
        let merge: MergeConfig = model(args.get("merge"), &[("work_dir", Value::Str(work))]);
        let archive: ArchiveConfig = model(args.get("archive"), &[]);
        let secrets = self.seeded.root("secrets");
        let template = args.get("job_template").cloned().unwrap_or_else(job_template);
        std::fs::write(
            secrets.join("job.json"),
            pyjson::dumps(&template, &DumpOptions::python_default()).expect("dumps"),
        )
        .expect("write the template");
        std::fs::write(secrets.join("token"), format!("{TOKEN}\n")).expect("write the token");

        let clock = self.clock();
        let database = self.test.database();
        let repository = MergeTaskRepository::with_clock(database.clone(), Arc::clone(&clock));
        let address = self.fakes.clouddrive.address();
        let cloud = AsyncCloudDrive::connect(&ClientOptions::new(address.clone(), TOKEN, false));
        let wrapped: Arc<dyn MergeCloud> = Arc::new(ScenarioCloud {
            inner: Arc::new(cloud.clone()),
            control: Arc::clone(&self.control),
        });
        let kube = KubeJobs::new(&KubeOptions {
            api_url: self.fakes.kubernetes.url.clone(),
            namespace: args
                .get("namespace")
                .and_then(Value::as_str)
                .unwrap_or("media")
                .to_owned(),
            pod_name: args
                .get("pod_name")
                .and_then(Value::as_str)
                .unwrap_or("embyx-manager-0")
                .to_owned(),
            template_path: secrets.join("job.json"),
            token_path: secrets.join("token"),
            ca_path: None,
            proxy: None,
            timeout: Duration::from_secs(30),
        })
        .expect("a Job client");

        let triggered = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&triggered);
        let trigger: Trigger = Arc::new(move |pipeline| {
            lock(&seen).push(pipeline);
            Box::pin(async {})
        });
        let sizes: HashMap<String, i64> = match args.get("part_sizes") {
            Some(Value::Dict(sizes)) => sizes
                .iter()
                .map(|(part, size)| (part.as_str().expect("a part").to_owned(), int(size)))
                .collect(),
            _ => HashMap::new(),
        };
        let merge_section = merge.clone();
        let archive_section = archive.clone();
        let clouddrive = CloudDriveConfig {
            address,
            api_token: TOKEN.to_owned(),
            ..CloudDriveConfig::default()
        };
        let source = Arc::clone(&wrapped);
        let mut parts = ServiceParts::new(
            repository.clone(),
            Arc::new(move || Some(Arc::clone(&source))),
            Some(Arc::new(kube) as Arc<dyn MergeKube>),
            Arc::new(move || merge_section.clone()),
            Arc::new(move || archive_section.clone()),
            Arc::new(move || clouddrive.clone()),
            trigger,
        );
        parts.transcodes = Some(TranscodeRepository::with_clock(database.clone(), Arc::clone(&clock)));
        parts.presence = Some(WorkerPresence::new(Arc::clone(&clock)));
        parts.sizes = Arc::new(move |parts: &[String]| {
            let mut total = 0;
            for part in parts {
                let Some(size) = sizes.get(part) else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("[Errno 2] No such file or directory: '{part}'"),
                    ));
                };
                total += size;
            }
            Ok(total)
        });
        parts.clock = Arc::clone(&clock);
        let service = MergeService::new(parts);

        let ledger: HashMap<String, String> = match args.get("ledger") {
            Some(Value::Dict(ledger)) => ledger
                .iter()
                .map(|(avid, dir)| {
                    (
                        avid.as_str().expect("an avid").to_owned(),
                        dir.as_str().expect("a directory").to_owned(),
                    )
                })
                .collect(),
            _ => HashMap::new(),
        };
        let task_dirs_for: TaskDirsFor = Arc::new(move |avids: Vec<String>| {
            let found = avids
                .into_iter()
                .filter_map(|avid| ledger.get(&avid).map(|dir| (avid, dir.clone())))
                .collect();
            Box::pin(async move { Ok(found) })
        });
        let mapping = MappingConfig {
            src_dir: "/remote".to_owned(),
            dst_dir: path_text(self.seeded.root("mapping")),
            ..MappingConfig::default()
        };
        let catalog_archive = archive;
        let auto_merge = merge;
        let source = Arc::clone(&wrapped);
        let mut auto_parts = AutoParts::new(
            repository.clone(),
            MergeCatalog {
                archive: Arc::new(move || catalog_archive.clone()),
                mapping: Arc::new(move || mapping.clone()),
                task_dirs_for,
            },
            TitleScanCache::new(Arc::clone(&clock)),
            service.clone(),
            Arc::new(move || auto_merge.clone()),
            Arc::new(move || Some(Arc::clone(&source))),
        );
        let free_gib = Arc::clone(&self.free_gib);
        auto_parts.free_space = Arc::new(move |_path| Ok(i128::from(*lock(&free_gib)) * GIB));
        auto_parts.clock = clock;
        World {
            database,
            repository,
            cloud,
            service,
            auto: AutoMerger::new(auto_parts),
            triggered,
        }
    }
}

impl World {
    async fn close(self) {
        self.service.aclose().await;
        self.cloud.close().await;
        self.database.close().await;
    }

    async fn tasks(&self) -> Value {
        let mut listed = self.repository.listing().await.expect("the listing");
        listed.sort_by_key(|task| task.id);
        Value::List(listed.iter().map(|found| task(Some(found))).collect())
    }
}

/// `JOB_TEMPLATE`.
fn job_template() -> Value {
    pyjson::loads(
        r#"{"apiVersion": "batch/v1", "kind": "Job", "metadata": {"labels": {"app": "embyx-merge"}},
            "spec": {"template": {"spec": {"containers": [{"name": "merge", "command": ["embyx-manager", "merge-worker"]}]}}}}"#,
    )
    .expect("the template")
}

// ---------- the steps ----------

/// `_error`.
enum Failure {
    Step(StepError),
    Action(ActionError),
    Worker(WorkerCallError),
    Missing(i64),
}

impl From<StepError> for Failure {
    fn from(error: StepError) -> Self {
        Self::Step(error)
    }
}

impl From<ActionError> for Failure {
    fn from(error: ActionError) -> Self {
        Self::Action(error)
    }
}

impl From<WorkerCallError> for Failure {
    fn from(error: WorkerCallError) -> Self {
        Self::Worker(error)
    }
}

impl From<embyx_merge::tasks::TaskError> for Failure {
    fn from(error: embyx_merge::tasks::TaskError) -> Self {
        Self::Step(error.into())
    }
}

fn step_error(error: &StepError) -> Value {
    error_value(error.class_name(), error.to_string())
}

impl Failure {
    fn report(&self) -> Value {
        match self {
            Self::Step(error)
            | Self::Action(ActionError::Error(error))
            | Self::Worker(WorkerCallError::Error(error)) => step_error(error),
            Self::Action(ActionError::Refused(code)) => {
                let mut fields = Dict::new();
                fields.insert("type", Value::from("MergeActionError"));
                fields.insert("code", Value::from(*code));
                Value::Dict(fields)
            }
            Self::Worker(WorkerCallError::Refused(refused)) => {
                let mut fields = Dict::new();
                fields.insert("type", Value::from("ChunkRefusedError"));
                fields.insert("code", Value::from(refused.code));
                fields.insert("status", Value::Int(refused.status.into()));
                fields.insert("details", Value::Dict(refused.details.clone()));
                Value::Dict(fields)
            }
            Self::Missing(id) => error_value("KeyError", format!("'no task {id}'")),
        }
    }
}

/// `_report`.
fn report(now: DateTime<Utc>, args: &Value) -> WorkerReport {
    let field = |key: &str, default: &str| args.get(key).and_then(Value::as_str).unwrap_or(default).to_owned();
    WorkerReport {
        name: field("name", ""),
        version: field("version", ""),
        task_id: args.get("task_id").filter(|id| !matches!(id, Value::Null)).map(int),
        state: field("state", "idle"),
        ..WorkerReport::at(now)
    }
}

fn part_index(args: &Value) -> i32 {
    i32::try_from(int_arg(args, "part_index")).expect("a part index")
}

/// `_chunk`: what the transcode API's PUT does with a chunk whose bytes arrived whole.
async fn chunk(service: &MergeService, args: &Value) -> Result<Value, Failure> {
    let task = service
        .transcoding_task(int_arg(args, "task_id"), int_arg(args, "part_index"))
        .await?;
    let staging = service.staging_chunk(&task, int_arg(args, "part_index"), int_arg(args, "chunk_index"));
    let data = text(args, "text").as_bytes().to_vec();
    std::fs::create_dir_all(staging.parent().expect("a parent")).expect("mkdir");
    std::fs::write(&staging, &data).expect("write the chunk");
    let sha256 = args
        .get("sha256")
        .and_then(Value::as_str)
        .map_or_else(|| hex(&Sha256::digest(&data)), str::to_owned);
    let chunk = Chunk {
        part_index: part_index(args),
        chunk_index: i32::try_from(int_arg(args, "chunk_index")).expect("a chunk index"),
        start: float(args.get("start").expect("start")),
        duration: float(args.get("duration").expect("duration")),
        frames: args
            .get("frames")
            .map_or(1, |frames| i32::try_from(int(frames)).expect("frames")),
        bytes: i64::try_from(data.len()).expect("small"),
        sha256,
    };
    let stored = service.accept_chunk(&task, &chunk, &staging).await?;
    let mut fields = Dict::new();
    fields.insert("stored", Value::Bool(stored));
    Ok(Value::Dict(fields))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

async fn task_of(repository: &MergeTaskRepository, id: i64) -> Result<MergeTask, Failure> {
    repository.get(id).await?.ok_or(Failure::Missing(id))
}

/// The service steps of `_run`, in the step's own task.
#[derive(Clone)]
struct Actors {
    repository: MergeTaskRepository,
    service: MergeService,
    auto: AutoMerger,
    triggered: Arc<Mutex<Vec<Pipeline>>>,
    now: Arc<Mutex<DateTime<Utc>>>,
}

impl Actors {
    fn now(&self) -> DateTime<Utc> {
        *lock(&self.now)
    }

    async fn run(self, op: String, args: Value) -> Result<Value, Failure> {
        let service = &self.service;
        Ok(match op.as_str() {
            "create" => {
                let new = NewTask {
                    avid: text(&args, "avid").to_owned(),
                    source: text(&args, "source").to_owned(),
                    library_dir: text(&args, "library_dir").to_owned(),
                    brand: text(&args, "brand").to_owned(),
                    parts: args
                        .get("parts")
                        .and_then(Value::as_list)
                        .expect("parts")
                        .iter()
                        .map(|part| part.as_str().expect("a part").to_owned())
                        .collect(),
                    merged_name: text(&args, "merged_name").to_owned(),
                };
                task(Some(&self.repository.create(&new).await?))
            }
            "tick" => {
                service.tick().await?;
                let triggered = std::mem::take(&mut *lock(&self.triggered));
                let mut fields = Dict::new();
                fields.insert(
                    "triggered",
                    Value::List(
                        triggered
                            .into_iter()
                            .map(|pipeline| Value::from(pipeline.as_str()))
                            .collect(),
                    ),
                );
                Value::Dict(fields)
            }
            "cancel" => task(Some(&service.cancel(int_arg(&args, "id")).await?)),
            "retry" => task(Some(&service.retry(int_arg(&args, "id")).await?)),
            "use_cpu" => task(Some(&service.use_cpu(int_arg(&args, "id")).await?)),
            "set_paused" => task(Some(
                &service
                    .set_paused(
                        int_arg(&args, "id"),
                        matches!(args.get("paused"), Some(Value::Bool(true))),
                    )
                    .await?,
            )),
            "claim" => {
                let worker = report(self.now(), &args);
                service.claim(worker).await?
            }
            "heartbeat" => {
                let worker = report(self.now(), &args);
                service.heartbeat(worker).await?
            }
            "chunk" => chunk(service, &args).await?,
            "finish_part" => {
                let found = service
                    .transcoding_task(int_arg(&args, "task_id"), int_arg(&args, "part_index"))
                    .await?;
                service.finish_part(&found, part_index(&args)).await?;
                let mut fields = Dict::new();
                fields.insert("finished", Value::Bool(true));
                Value::Dict(fields)
            }
            "transcode_view" => {
                let found = task_of(&self.repository, int_arg(&args, "id")).await?;
                service.transcode_view(&found).await?.unwrap_or(Value::Null)
            }
            "home" => {
                let tasks = self.repository.open_tasks().await?;
                let ids = |keep: &dyn Fn(&MergeTask) -> bool| {
                    Value::List(
                        tasks
                            .iter()
                            .filter(|found| keep(found))
                            .map(|found| Value::Int(found.id.into()))
                            .collect(),
                    )
                };
                let mut fields = Dict::new();
                fields.insert("home_bound", ids(&|found| service.home_bound(found)));
                fields.insert("parked", ids(&|found| service.parked(found)));
                fields.insert("reserved_bytes", Value::Int(service.reserved_bytes(&tasks)));
                fields.insert("online", Value::Bool(service.presence().online()));
                Value::Dict(fields)
            }
            "auto" => {
                self.auto.step().await?;
                auto_status(&self.auto.status())
            }
            other => panic!("unknown step {other:?}"),
        })
    }
}

/// The steps that change the world without a call of the service; `None` for a service step.
fn change(ground: &Ground, op: &str, args: &Value) -> Option<Value> {
    Some(match op {
        "write" => {
            let path = ground.seeded.resolve(text(args, "path"));
            std::fs::create_dir_all(path.parent().expect("a parent")).expect("mkdir");
            let written = match args.get("json") {
                Some(json) => pyjson::dumps(json, &DumpOptions::python_default()).expect("dumps"),
                None => text(args, "text").to_owned(),
            };
            std::fs::write(&path, written).expect("write");
            Value::Null
        }
        "remove" => {
            std::fs::remove_file(ground.seeded.resolve(text(args, "path"))).expect("remove");
            Value::Null
        }
        "advance" => {
            let mut now = lock(&ground.now);
            *now += TimeDelta::seconds(int_arg(args, "seconds"));
            Value::Str(py_isoformat_datetime(&now.fixed_offset()))
        }
        "job_status" => {
            ground
                .fakes
                .kubernetes
                .set_job_status(text(args, "name"), args.get("status").expect("a status"));
            Value::Null
        }
        "cloud" => {
            ground
                .fakes
                .clouddrive
                .fake
                .edit(text(args, "edit"), &args.get("args").cloned().unwrap_or_else(empty));
            Value::Null
        }
        "free_space" => {
            *lock(&ground.free_gib) = int_arg(args, "gib");
            Value::Null
        }
        _ => return None,
    })
}

/// `run_steps`: the reports of `steps`, each with the listing after it.
async fn run_steps(ground: &Ground, steps: &[Value]) -> Vec<Value> {
    let mut world = ground.build();
    let mut reported = Vec::new();
    for step in steps {
        let op = text(step, "op").to_owned();
        let args = step.get("args").cloned().unwrap_or_else(empty);
        let mut entry = Dict::new();
        entry.insert("op", Value::from(op.as_str()));
        if op == "restart" {
            world.close().await;
            world = ground.build();
            entry.insert("result", Value::Null);
        } else if let Some(result) = change(ground, &op, &args) {
            entry.insert("result", result);
        } else {
            *lock(&ground.control.cancel_after) = args.get("cancel_after").and_then(Value::as_str).map(str::to_owned);
            let actors = Actors {
                repository: world.repository.clone(),
                service: world.service.clone(),
                auto: world.auto.clone(),
                triggered: Arc::clone(&world.triggered),
                now: Arc::clone(&ground.now),
            };
            let running = tokio::spawn(actors.run(op.clone(), args.clone()));
            *lock(&ground.control.step) = Some(running.abort_handle());
            match running.await {
                Ok(Ok(result)) => entry.insert("result", result),
                Ok(Err(failure)) => entry.insert("error", failure.report()),
                Err(error) if error.is_cancelled() => {
                    entry.insert("error", error_value("CancelledError", String::new()));
                }
                Err(error) => std::panic::resume_unwind(error.into_panic()),
            }
            *lock(&ground.control.step) = None;
            *lock(&ground.control.cancel_after) = None;
        }
        entry.insert("tasks", world.tasks().await);
        reported.push(Value::Dict(entry));
    }
    world.close().await;
    reported
}

/// How a scenario's steps are run: by the Rust build alone, or split at its `restart` steps
/// with the segments run by the two builds in turn.
#[derive(Clone, Copy)]
enum Run {
    Rust,
    Alternating { python_first: bool },
}

/// `[from, to)` of each segment: a segment starts at a `restart` step.
fn segments(steps: &[Value]) -> Vec<(usize, usize)> {
    let mut starts = vec![0];
    starts.extend(
        steps
            .iter()
            .enumerate()
            .filter(|(index, step)| *index > 0 && text(step, "op") == "restart")
            .map(|(index, _)| index),
    );
    let mut ends: Vec<usize> = starts[1..].to_vec();
    ends.push(steps.len());
    starts.into_iter().zip(ends).collect()
}

/// The fakes started from the states another build left (or the spec's seeds): an HTTP fake's
/// `routes_left` are its routes again.
async fn fakes_from(states: &Dict, recorder: &Recorder) -> ScenarioFakes {
    let state = |name: &str| states.get(name).cloned().unwrap_or_else(empty);
    let http_seed = |name: &str| match state(name) {
        Value::Dict(dumped) if dumped.contains_key("routes_left") => {
            let mut seed = Dict::new();
            for (key, value) in dumped.iter() {
                let key = key.as_str().expect("a key");
                seed.insert(if key == "routes_left" { "routes" } else { key }, value.clone());
            }
            Value::Dict(seed)
        }
        other => other,
    };
    ScenarioFakes {
        clouddrive: FakeCloudDrive::new(&state("clouddrive"), recorder.clone())
            .serve()
            .await,
        kubernetes: FakeHttp::start("kubernetes", &http_seed("kubernetes"), recorder.clone()).await,
        emby: FakeHttp::start("emby", &http_seed("emby"), recorder.clone()).await,
    }
}

fn dict_of(value: Value) -> Dict {
    match value {
        Value::Dict(dict) => dict,
        other => panic!("not an object: {other:?}"),
    }
}

/// The test database's DSN for the Python half: the server of `EMBYX_MANAGER_TEST_DATABASE_URL`
/// with the database's own name, as `runner.create_database` builds it.
fn python_dsn(test: &TestDatabase) -> String {
    let admin = std::env::var("EMBYX_MANAGER_TEST_DATABASE_URL").expect("the test database URL");
    let (head, query) = admin
        .split_once('?')
        .map_or((admin.as_str(), None), |(head, query)| (head, Some(query)));
    let base = head.rsplit_once('/').map_or(head, |(base, _)| base);
    let mut dsn = format!("{base}/{}", test.name());
    if let Some(query) = query {
        dsn.push('?');
        dsn.push_str(query);
    }
    dsn
}

/// One segment on the Python baseline (`tools/scenarios/segment.py`).
fn python_segment(ground: &Ground, name: &str, (from, to): (usize, usize), now: DateTime<Utc>, states: &Dict) -> Dict {
    let mut roots = Dict::new();
    for root in ["work", "mapping", "secrets"] {
        roots.insert(root, Value::Str(path_text(ground.seeded.root(root))));
    }
    let mut handoff = Dict::new();
    handoff.insert("spec", Value::from(name));
    handoff.insert("from", Value::Int(i128::try_from(from).expect("small")));
    handoff.insert("to", Value::Int(i128::try_from(to).expect("small")));
    handoff.insert("dsn", Value::Str(python_dsn(&ground.test)));
    handoff.insert("roots", Value::Dict(roots));
    handoff.insert("now", Value::Str(py_isoformat_datetime(&now.fixed_offset())));
    handoff.insert("fakes", Value::Dict(states.clone()));
    let input = pyjson::dumps(&Value::Dict(handoff), &DumpOptions::plain()).expect("JSON");
    let repository = scenario::repo_root();
    let mut child = std::process::Command::new("uv")
        .args(["run", "--locked", "python"])
        .arg(repository.join("tools/scenarios/segment.py"))
        .current_dir(&repository)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("uv runs (it must be installed)");
    std::io::Write::write_all(&mut child.stdin.take().expect("stdin"), input.as_bytes()).expect("send the handoff");
    let output = child.wait_with_output().expect("the segment ends");
    let stdout = String::from_utf8(output.stdout).expect("utf-8");
    assert!(
        output.status.success(),
        "the Python segment {from}..{to} of {name} failed: {}{stdout}",
        String::from_utf8_lossy(&output.stderr)
    );
    dict_of(pyjson::loads(stdout.trim_end()).expect("the segment prints JSON"))
}

async fn scenario_steps(name: &str, run: Run) {
    let spec = Spec::load(name);
    assert_eq!(spec.operation(), "merge.service");
    let seeded = Seeded::new(&spec);
    let test = TestDatabase::create().await;
    embyx_db::scenario::seed(test.pool(), &spec).await;
    let recorder = Recorder::new();
    let args = spec.section("args");
    let start = DateTime::parse_from_rfc3339(spec.clock())
        .expect("an RFC 3339 clock")
        .to_utc();
    let mut states = Dict::new();
    for fake in ["clouddrive", "kubernetes", "emby"] {
        states.insert(fake, spec.section(fake));
    }
    let mut ground = Ground {
        args: args.clone(),
        seeded,
        test,
        fakes: fakes_from(&states, &recorder).await,
        now: Arc::new(Mutex::new(start)),
        free_gib: Arc::new(Mutex::new(100)),
        control: Arc::new(CloudControl::default()),
        spec,
    };
    let steps = args.get("steps").and_then(Value::as_list).expect("steps").to_vec();
    let plan = match run {
        Run::Rust => vec![((0, steps.len()), false)],
        Run::Alternating { python_first } => {
            let split = segments(&steps);
            assert!(split.len() > 1, "{name} has no restart to split it at");
            split
                .into_iter()
                .enumerate()
                .map(|(index, bounds)| (bounds, (index % 2 == 0) == python_first))
                .collect()
        }
    };
    let mut reported = Vec::new();
    for (index, ((from, to), python)) in plan.into_iter().enumerate() {
        if python {
            let now = *lock(&ground.now);
            let mut segment = python_segment(&ground, name, (from, to), now, &states);
            let Some(Value::List(done)) = segment.remove("steps") else {
                panic!("the segment reports its steps");
            };
            reported.extend(done);
            let clock = segment.get("clock").and_then(Value::as_str).expect("the clock");
            *lock(&ground.now) = DateTime::parse_from_rfc3339(clock).expect("an RFC 3339 clock").to_utc();
            for call in segment.get("calls").and_then(Value::as_list).expect("the calls") {
                recorder.record(call.clone());
            }
            states = dict_of(segment.remove("fakes").expect("the fakes"));
        } else {
            if index > 0 {
                ground.fakes = fakes_from(&states, &recorder).await;
            }
            reported.extend(run_steps(&ground, &steps[from..to]).await);
            states = dict_of(ground.fakes.dump());
        }
    }
    let clock = *lock(&ground.now);

    let Ground {
        spec,
        seeded,
        test,
        fakes,
        ..
    } = ground;
    drop(fakes);
    let rows = embyx_db::scenario::dump_rows(test.pool(), &spec).await;
    let mut result = Dict::new();
    result.insert("steps", Value::List(reported));
    result.insert("clock", Value::Str(py_isoformat_datetime(&clock.fixed_offset())));
    scenario::assert_transcript(Parts {
        spec: &spec,
        seeded: &seeded,
        outcome: scenario::result(Value::Dict(result)),
        calls: recorder.calls(),
        rows,
        fakes: Value::Dict(states),
    });
    test.drop_database().await;
}

async fn service_steps(name: &str) {
    scenario_steps(name, Run::Rust).await;
}

/// The resume matrix: `name`'s segments run by the two builds in turn, Python first and then
/// Rust first, must come to the single build's transcript.
async fn resume_matrix(name: &str) {
    scenario_steps(name, Run::Alternating { python_first: true }).await;
    scenario_steps(name, Run::Alternating { python_first: false }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn walk_matches_the_python_transcript() {
    service_steps("merge/walk").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_failures_match_the_python_transcript() {
    service_steps("merge/merge_failures").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_matches_the_python_transcript() {
    service_steps("merge/upload").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_matches_the_python_transcript() {
    service_steps("merge/cancel").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replace_archive_matches_the_python_transcript() {
    service_steps("merge/replace_archive").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn home_matches_the_python_transcript() {
    service_steps("merge/home").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_matches_the_python_transcript() {
    service_steps("merge/auto").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over115_cpu_matches_the_python_transcript() {
    service_steps("merge/over115_cpu").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn home_muxers_match_the_python_transcript() {
    service_steps("merge/home_muxers").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn walk_resumes_across_the_builds() {
    resume_matrix("merge/walk").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_resumes_across_the_builds() {
    resume_matrix("merge/upload").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn home_resumes_across_the_builds() {
    resume_matrix("merge/home").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_resumes_across_the_builds() {
    resume_matrix("merge/cancel").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replace_archive_resumes_across_the_builds() {
    resume_matrix("merge/replace_archive").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_failures_resumes_across_the_builds() {
    resume_matrix("merge/merge_failures").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_resumes_across_the_builds() {
    resume_matrix("merge/auto").await;
}
