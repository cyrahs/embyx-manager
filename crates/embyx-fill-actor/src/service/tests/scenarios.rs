//! The service scenarios (`tools/scenarios/specs/fill_actor/`, operation `fill_actor.service`):
//! the Rust port of `tools/scenarios/operations/fill_actor_service.py`. Each spec's steps run
//! through the ported service over the seeded tree, the repository on PostgreSQL and, in
//! CloudDrive mode, the mover on the CloudDrive fake, and must render the bytes the Python
//! baseline committed under `goldens/scenarios/fill_actor/`.
//!
//! The Python operation's crash (`Crash`, a `BaseException` nothing in the service catches) is
//! a panic with [`CRASH`] here, in front of a file-system mutation ([`RecordingMoveFs`]), a
//! repository write ([`ScenarioRepository`]) or a mover call ([`ControlledMover`]); the step
//! runs in a task of its own, so the panic ends that task as the exception ends the Python
//! step. `cancel_after` aborts the step's task, as `task.cancel()` cancels the Python one.
//! Equal transcripts, rows included, after every crash mean each build leaves the journal and
//! the disk as the other does, so either build resumes what the other left.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Once, PoisonError};

use chrono::{DateTime, TimeDelta, Utc};
use embyx_clients::clouddrive::{AsyncCloudDrive, ClientOptions};
use embyx_clients::scenario::ScenarioFakes;
use embyx_core::pyjson::{self, Dict, Value};
use embyx_core::scenario::{self, Parts, Recorder, Seeded, Spec};
use embyx_db::database::Database;
use embyx_db::testdb::TestDatabase;
use embyx_monitor::fs::path_text;
use tokio::task::AbortHandle;

use super::{FakeCatalog, FakeGateway, tokens};
use crate::adapters::CloudDriveFileMover;
use crate::cloud_moves::{BoxFuture, CloudFileMetadata, CloudFileMover, CloudMovePaths, CloudMoveResponse, MoverError};
use crate::fs::recording::{CRASH, CrashPoint, Fault, FaultError, Hook, RecordingMoveFs, Sink};
use crate::models::{FillActorPlan, MoveResult};
use crate::ports::BrandResolver;
use crate::postgres_repository::{PostgresFillActorRepository, RepositoryError};
use crate::records::{CandidateRecord, CloudMoveOperationRecord, MoveJournalRecord, MoveJournalState, PlanRecord};
use crate::service::store::FillActorRepository;
use crate::service::{
    FillActorPaths, FillActorRuntime, FillActorService, ServiceError, ServiceOptions, ServiceParts, static_runtime,
};

/// `operations/intake.py` `TOKEN`.
const TOKEN: &str = "scenario-token";

type Shared<T> = Arc<Mutex<T>>;

fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Crash points panic on purpose: keep their reports out of the test output.
fn quiet_crashes() {
    static QUIET: Once = Once::new();
    QUIET.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if info.payload().downcast_ref::<&str>() != Some(&CRASH) {
                default(info);
            }
        }));
    });
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

fn texts(value: &Value) -> Vec<String> {
    value
        .as_list()
        .expect("a list")
        .iter()
        .map(|item| item.as_str().expect("a string").to_owned())
        .collect()
}

fn only_op(op: &str) -> Dict {
    let mut entry = Dict::new();
    entry.insert("op", Value::from(op));
    entry
}

fn error_value(class: &str, message: String) -> Value {
    let mut error = Dict::new();
    error.insert("type", Value::from(class));
    error.insert("message", Value::Str(message));
    Value::Dict(error)
}

/// `json.loads(model.model_dump_json())`.
fn json(text: &str) -> Value {
    pyjson::loads(text).expect("the model's JSON")
}

// ---------- the repository ----------

/// `RecordingRepository`: `PostgresFillActorRepository` with its writes recorded once they
/// returned, and a crash point (`{repository = <method>, state?}`) in front of them.
struct ScenarioRepository {
    inner: Arc<PostgresFillActorRepository>,
    recorder: Recorder,
    crash: Shared<Option<Value>>,
}

impl ScenarioRepository {
    fn check(&self, method: &str, state: &str) {
        let mut crash = lock(&self.crash);
        let Some(point) = crash.as_ref() else {
            return;
        };
        if point.get("repository").and_then(Value::as_str) != Some(method) {
            return;
        }
        if point.get("state").is_some_and(|wanted| wanted.as_str() != Some(state)) {
            return;
        }
        *crash = None;
        drop(crash);
        std::panic::panic_any(CRASH);
    }

    fn call<'a>(
        &'a self,
        method: &'static str,
        state: &'static str,
        request: Dict,
        call: BoxFuture<'a, Result<(), RepositoryError>>,
    ) -> BoxFuture<'a, Result<(), RepositoryError>> {
        Box::pin(async move {
            self.check(method, state);
            let result = call.await;
            let mut record = Dict::new();
            record.insert("fake", Value::from("repository"));
            record.insert("method", Value::from(method));
            record.insert("request", Value::Dict(request));
            match &result {
                Ok(()) => record.insert("response", Value::Null),
                Err(error) => record.insert("error", error_value(error.class_name(), error.to_string())),
            }
            self.recorder.record(Value::Dict(record));
            result
        })
    }
}

fn optional(value: Option<&String>) -> Value {
    value.map_or(Value::Null, |value| Value::Str(value.clone()))
}

/// `_operation(operation)`.
fn operation_request(operation: &CloudMoveOperationRecord) -> Dict {
    let mut request = Dict::new();
    request.insert("plan_id", Value::Str(operation.plan_id.clone()));
    request.insert("candidate_id", Value::Str(operation.candidate_id.clone()));
    request.insert("attempt_id", Value::Str(operation.attempt_id.clone()));
    request.insert("state", Value::from(operation.state.as_str()));
    request.insert("error_code", optional(operation.error_code.as_ref()));
    request
}

impl FillActorRepository for ScenarioRepository {
    fn save_plan<'a>(&'a self, record: &'a PlanRecord) -> BoxFuture<'a, Result<(), RepositoryError>> {
        FillActorRepository::save_plan(&*self.inner, record)
    }

    fn get_plan<'a>(&'a self, plan_id: &'a str) -> BoxFuture<'a, Result<Option<PlanRecord>, RepositoryError>> {
        FillActorRepository::get_plan(&*self.inner, plan_id)
    }

    fn get_candidate<'a>(
        &'a self,
        plan_id: &'a str,
        candidate_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<CandidateRecord>, RepositoryError>> {
        FillActorRepository::get_candidate(&*self.inner, plan_id, candidate_id)
    }

    fn save_move_result<'a>(
        &'a self,
        plan_id: &'a str,
        result: &'a MoveResult,
    ) -> BoxFuture<'a, Result<(), RepositoryError>> {
        let mut request = Dict::new();
        request.insert("plan_id", Value::from(plan_id));
        request.insert("candidate_id", Value::Str(result.candidate_id.clone()));
        request.insert("state", Value::from(result.state.as_str()));
        request.insert("error_code", optional(result.error_code.as_ref()));
        self.call(
            "save_move_result",
            result.state.as_str(),
            request,
            FillActorRepository::save_move_result(&*self.inner, plan_id, result),
        )
    }

    fn get_move_result<'a>(
        &'a self,
        plan_id: &'a str,
        candidate_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<MoveResult>, RepositoryError>> {
        FillActorRepository::get_move_result(&*self.inner, plan_id, candidate_id)
    }

    fn save_move_journal<'a>(&'a self, record: &'a MoveJournalRecord) -> BoxFuture<'a, Result<(), RepositoryError>> {
        let mut request = Dict::new();
        request.insert("plan_id", Value::Str(record.plan_id.clone()));
        request.insert("candidate_id", Value::Str(record.candidate_id.clone()));
        request.insert("state", Value::from(record.state.as_str()));
        self.call(
            "save_move_journal",
            record.state.as_str(),
            request,
            FillActorRepository::save_move_journal(&*self.inner, record),
        )
    }

    fn get_move_journal<'a>(
        &'a self,
        plan_id: &'a str,
        candidate_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<MoveJournalRecord>, RepositoryError>> {
        FillActorRepository::get_move_journal(&*self.inner, plan_id, candidate_id)
    }

    fn list_unreconciled_moves(&self) -> BoxFuture<'_, Result<Vec<MoveJournalRecord>, RepositoryError>> {
        FillActorRepository::list_unreconciled_moves(&*self.inner)
    }

    fn save_cloud_move_operation<'a>(
        &'a self,
        record: &'a CloudMoveOperationRecord,
    ) -> BoxFuture<'a, Result<(), RepositoryError>> {
        self.call(
            "save_cloud_move_operation",
            record.state.as_str(),
            operation_request(record),
            FillActorRepository::save_cloud_move_operation(&*self.inner, record),
        )
    }

    fn get_cloud_move_operation<'a>(
        &'a self,
        plan_id: &'a str,
        candidate_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<CloudMoveOperationRecord>, RepositoryError>> {
        FillActorRepository::get_cloud_move_operation(&*self.inner, plan_id, candidate_id)
    }

    fn list_unresolved_cloud_moves(&self) -> BoxFuture<'_, Result<Vec<CloudMoveOperationRecord>, RepositoryError>> {
        FillActorRepository::list_unresolved_cloud_moves(&*self.inner)
    }

    fn finalize_cloud_move<'a>(
        &'a self,
        operation: &'a CloudMoveOperationRecord,
        result: &'a MoveResult,
    ) -> BoxFuture<'a, Result<(), RepositoryError>> {
        let mut request = operation_request(operation);
        request.insert("result_state", Value::from(result.state.as_str()));
        request.insert("result_error_code", optional(result.error_code.as_ref()));
        self.call(
            "finalize_cloud_move",
            operation.state.as_str(),
            request,
            FillActorRepository::finalize_cloud_move(&*self.inner, operation, result),
        )
    }
}

// ---------- the mover ----------

/// What `ControlledMover` does at the current step.
#[derive(Default)]
struct MoverControl {
    /// `crash = {mover = <method>}`.
    crash: Mutex<Option<String>>,
    /// `cancel_after = {mover = <method>}`.
    cancel_after: Mutex<Option<String>>,
    /// The step's task.
    step: Mutex<Option<AbortHandle>>,
}

impl MoverControl {
    fn before(&self, method: &str) {
        let mut crash = lock(&self.crash);
        if crash.as_deref() == Some(method) {
            *crash = None;
            drop(crash);
            std::panic::panic_any(CRASH);
        }
    }

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

/// `ControlledMover`: the CloudDrive mover with a crash point in front of a call and a cancel
/// of the step once a call answered. Its scoped movers share the control.
struct ControlledMover {
    inner: Arc<dyn CloudFileMover>,
    control: Arc<MoverControl>,
}

impl CloudFileMover for ControlledMover {
    fn list_directory<'a>(
        &'a self,
        api_directory: &'a str,
    ) -> BoxFuture<'a, Result<Vec<CloudFileMetadata>, MoverError>> {
        Box::pin(async move {
            self.control.before("list_directory");
            let result = self.inner.list_directory(api_directory).await;
            self.control.answered("list_directory").await;
            result
        })
    }

    fn ensure_directory<'a>(
        &'a self,
        parent_api_directory: &'a str,
        folder_name: &'a str,
    ) -> BoxFuture<'a, Result<bool, MoverError>> {
        Box::pin(async move {
            self.control.before("ensure_directory");
            let result = self.inner.ensure_directory(parent_api_directory, folder_name).await;
            self.control.answered("ensure_directory").await;
            result
        })
    }

    fn move_file<'a>(
        &'a self,
        source_api_path: &'a str,
        destination_api_directory: &'a str,
    ) -> BoxFuture<'a, Result<CloudMoveResponse, MoverError>> {
        Box::pin(async move {
            self.control.before("move_file");
            let result = self.inner.move_file(source_api_path, destination_api_directory).await;
            self.control.answered("move_file").await;
            result
        })
    }

    fn scoped(&self) -> Arc<dyn CloudFileMover> {
        Arc::new(Self {
            inner: self.inner.scoped(),
            control: Arc::clone(&self.control),
        })
    }

    fn settle(&self) -> BoxFuture<'_, ()> {
        self.inner.settle()
    }
}

// ---------- the world ----------

/// `_Brands`: a brand per video, `args.brand` (default `ABC`) for the rest.
struct ScenarioBrands {
    values: HashMap<String, String>,
    default: Option<String>,
}

impl BrandResolver for ScenarioBrands {
    fn resolve_brand(&self, video_id: &str) -> Option<String> {
        self.values.get(video_id).cloned().or_else(|| self.default.clone())
    }
}

/// The file system's control at the current step: `cancel_after = {fs = <op>, path?}`.
#[derive(Default)]
struct FsControl {
    cancel_after: Mutex<Option<(String, Option<PathBuf>)>>,
    step: Mutex<Option<AbortHandle>>,
}

fn faults(seeded: &Seeded, values: &Value) -> Vec<Fault> {
    values
        .as_list()
        .unwrap_or_default()
        .iter()
        .map(|fault| Fault {
            op: text(fault, "op").to_owned(),
            path: Some(seeded.resolve(text(fault, "path"))),
            error: match fault.get("errno") {
                Some(code) => FaultError::Errno(i32::try_from(int(code)).expect("an errno")),
                None => FaultError::Message(text(fault, "message").to_owned()),
            },
            times: fault
                .get("times")
                .map(|times| u32::try_from(int(times)).expect("a count")),
        })
        .collect()
}

/// `fs.set_faults(...)`: replace the file system's faults.
fn set_faults(fs: &RecordingMoveFs, seeded: &Seeded, values: &Value) {
    fs.clear_faults();
    for fault in faults(seeded, values) {
        fs.add_fault(fault);
    }
}

/// `_shape`: the unrecorded steps that change the tree between two steps.
fn shape(seeded: &Seeded, op: &str, args: &Value) -> bool {
    match op {
        "write" => {
            let path = seeded.resolve(text(args, "path"));
            std::fs::create_dir_all(path.parent().expect("a parent")).expect("parents");
            std::fs::write(&path, text(args, "text")).expect("write");
        }
        "remove" => std::fs::remove_file(seeded.resolve(text(args, "path"))).expect("remove"),
        "link" => {
            std::fs::hard_link(seeded.resolve(text(args, "target")), seeded.resolve(text(args, "path"))).expect("link");
        }
        "rename" => {
            std::fs::rename(seeded.resolve(text(args, "src")), seeded.resolve(text(args, "dst"))).expect("rename");
        }
        _ => return false,
    }
    true
}

/// `operations/fill_actor.py` `_report` of a candidate record, the fingerprint cut to its size
/// (device, inode and times differ from run to run).
fn report_candidate(record: &CandidateRecord) -> Value {
    let path = |path: &Path| Value::from(path.to_str().expect("UTF-8 paths in the scenarios"));
    let optional = |value: Option<&String>| value.map_or(Value::Null, |value| Value::from(value.as_str()));
    let mut fingerprint = Dict::new();
    fingerprint.insert("size", Value::Int(record.fingerprint.size.into()));
    let cloud_file = record.cloud_file.as_ref().map_or(Value::Null, |file| {
        let mut entry = Dict::new();
        entry.insert("path", Value::from(file.path.as_str()));
        entry.insert("file_id", Value::from(file.file_id.as_str()));
        entry.insert("name", Value::from(file.name.as_str()));
        entry.insert("size", Value::Int(file.size));
        entry.insert("write_time", Value::Int(file.write_time));
        let hashes = file
            .hashes
            .iter()
            .map(|(kind, hash)| Value::List(vec![Value::from(kind.as_str()), Value::from(hash.as_str())]))
            .collect();
        entry.insert("hashes", Value::List(hashes));
        Value::Dict(entry)
    });
    let mut entry = Dict::new();
    entry.insert("candidate_id", Value::from(record.candidate_id.as_str()));
    entry.insert("video_id", Value::from(record.video_id.as_str()));
    entry.insert("source", path(&record.source));
    entry.insert("source_root", path(&record.source_root));
    entry.insert("destination", path(&record.destination));
    entry.insert("fingerprint", Value::Dict(fingerprint));
    entry.insert("kind", Value::from(record.kind.as_str()));
    entry.insert("mapping_sha256", optional(record.mapping_sha256.as_ref()));
    entry.insert("cloud_source_path", optional(record.cloud_source_path.as_ref()));
    entry.insert("cloud_destination_dir", optional(record.cloud_destination_dir.as_ref()));
    entry.insert("cloud_file", cloud_file);
    Value::Dict(entry)
}

/// One step's outcome: its value and the plan the next steps use.
type StepOutcome = Result<(Value, Option<Arc<FillActorPlan>>), Value>;

async fn run_step(
    service: FillActorService,
    repository: Arc<ScenarioRepository>,
    clock: Shared<DateTime<Utc>>,
    op: String,
    args: Value,
    plan: Option<Arc<FillActorPlan>>,
) -> StepOutcome {
    let service_error = |error: ServiceError| error_value(error.class_name(), error.to_string());
    match op.as_str() {
        "scan" => {
            let actors = args.get("actors").map_or_else(|| vec!["actor".to_owned()], texts);
            let created = service
                .create_plan(&actors, None, None, None)
                .await
                .map_err(service_error)?;
            Ok((json(&created.dump_json()), Some(Arc::new(created))))
        }
        "apply" => {
            let plan = plan.expect("a scan before apply");
            let files = args.get("files").map(texts);
            let candidates: Vec<String> = plan
                .videos
                .iter()
                .flat_map(|video| &video.move_candidates)
                .filter(|candidate| files.as_ref().is_none_or(|files| files.contains(&candidate.file_name)))
                .map(|candidate| candidate.candidate_id.clone())
                .collect();
            let result = service
                .apply(&plan.plan_id, &plan.revision, &candidates, None, None)
                .await
                .map_err(service_error)?;
            Ok((json(&result.dump_json()), Some(plan)))
        }
        "reconcile" => {
            let results = service.reconcile_moves().await.map_err(service_error)?;
            Ok((
                Value::List(results.iter().map(|result| json(&result.dump_json())).collect()),
                plan,
            ))
        }
        "candidates" => {
            let current = plan.as_ref().expect("a scan before candidates");
            let mut reported = Vec::new();
            for candidate in current.videos.iter().flat_map(|video| &video.move_candidates) {
                let record = repository
                    .get_candidate(&current.plan_id, &candidate.candidate_id)
                    .await
                    .map_err(|error| error_value(error.class_name(), error.to_string()))?;
                reported.push(record.as_ref().map_or(Value::Null, report_candidate));
            }
            Ok((Value::List(reported), plan))
        }
        "journal" => {
            let current = plan.as_ref().expect("a scan before journal");
            let file = text(&args, "file");
            let candidate = current
                .videos
                .iter()
                .flat_map(|video| &video.move_candidates)
                .find(|candidate| candidate.file_name == file)
                .expect("a candidate of the plan");
            for state in texts(args.get("states").expect("states")) {
                let record = MoveJournalRecord {
                    plan_id: current.plan_id.clone(),
                    candidate_id: candidate.candidate_id.clone(),
                    state: MoveJournalState::parse(&state).expect("a journal state"),
                    updated_at: *lock(&clock),
                };
                repository
                    .save_move_journal(&record)
                    .await
                    .map_err(|error| error_value(error.class_name(), error.to_string()))?;
            }
            Ok((Value::Null, plan))
        }
        other => panic!("unknown step {other:?}"),
    }
}

/// What a scenario builds a service from; `start()` is a process start.
struct World {
    seeded: Seeded,
    args: Value,
    test: TestDatabase,
    databases: Mutex<Vec<Database>>,
    recorder: Recorder,
    fs: Arc<RecordingMoveFs>,
    runtime: FillActorRuntime,
    clock: Shared<DateTime<Utc>>,
    tokens: crate::service::TokenFactory,
    mover: Option<Arc<ControlledMover>>,
    mover_control: Arc<MoverControl>,
    repository_crash: Shared<Option<Value>>,
}

impl World {
    fn start(&self) -> (FillActorService, Arc<ScenarioRepository>) {
        let database = self.test.database();
        lock(&self.databases).push(database.clone());
        let repository = Arc::new(ScenarioRepository {
            inner: Arc::new(PostgresFillActorRepository::new(database)),
            recorder: self.recorder.clone(),
            crash: Arc::clone(&self.repository_crash),
        });
        let catalog = self
            .args
            .get("catalog")
            .and_then(Value::as_dict)
            .map(|values| {
                values
                    .iter()
                    .map(|(actor, videos)| {
                        let actor = match actor {
                            embyx_core::pyjson::Key::Str(actor) => actor.clone(),
                            other => panic!("an actor name, not {other:?}"),
                        };
                        (actor, Ok(texts(videos)))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let brands = ScenarioBrands {
            values: self
                .args
                .get("brands")
                .and_then(Value::as_dict)
                .map(|values| {
                    values
                        .iter()
                        .map(|(video, brand)| {
                            let video = match video {
                                embyx_core::pyjson::Key::Str(video) => video.clone(),
                                other => panic!("a video id, not {other:?}"),
                            };
                            (video, brand.as_str().expect("a brand").to_owned())
                        })
                        .collect()
                })
                .unwrap_or_default(),
            default: Some(
                self.args
                    .get("brand")
                    .and_then(Value::as_str)
                    .unwrap_or("ABC")
                    .to_owned(),
            ),
        };
        let reading = Arc::clone(&self.clock);
        let options = ServiceOptions {
            clock: Some(Arc::new(move || *lock(&reading))),
            token_factory: Some(Arc::clone(&self.tokens)),
            cloud_file_mover: self
                .mover
                .as_ref()
                .map(|mover| Arc::clone(mover) as Arc<dyn CloudFileMover>),
            fs: Arc::clone(&self.fs) as Arc<dyn crate::fs::MoveFs>,
            // The verification rounds' `asyncio.sleep(0.5)`, without the wait.
            sleeper: Arc::new(|_| Box::pin(async {})),
            ..ServiceOptions::default()
        };
        let service = FillActorService::new(
            ServiceParts {
                runtime: static_runtime(self.runtime.clone()),
                actor_catalog: Arc::new(FakeCatalog(catalog)),
                acquisition_gateway: Arc::new(FakeGateway::default()),
                brand_resolver: Arc::new(brands),
                repository: Arc::clone(&repository) as Arc<dyn FillActorRepository>,
            },
            options,
        )
        .expect("valid options");
        (service, repository)
    }
}

fn resolve_all(seeded: &Seeded, values: Option<&Value>, default: &[&str]) -> Vec<PathBuf> {
    values.map_or_else(
        || default.iter().map(|path| seeded.resolve(path)).collect(),
        |values| texts(values).iter().map(|path| seeded.resolve(path)).collect(),
    )
}

/// `service_steps`.
#[allow(
    clippy::too_many_lines,
    reason = "the Python operation's step loop, one arm per kind of step, kept in its order"
)]
async fn service_steps(name: &str) {
    quiet_crashes();
    let spec = Spec::load(name);
    assert_eq!(spec.operation(), "fill_actor.service");
    let seeded = Seeded::new(&spec);
    let test = TestDatabase::create().await;
    embyx_db::scenario::seed(test.pool(), &spec).await;
    let recorder = Recorder::new();
    let fakes = ScenarioFakes::start(&spec, &recorder).await;
    let args = spec.section("args");

    let library = args.get("library").cloned().unwrap_or_else(|| Value::Dict(Dict::new()));
    let paths = FillActorPaths::from_iterable(
        seeded.resolve(library.get("actor").and_then(Value::as_str).unwrap_or("actor")),
        resolve_all(&seeded, library.get("additional"), &["additional"]),
        seeded.resolve(library.get("move_in").and_then(Value::as_str).unwrap_or("movein")),
    );
    let cloud_args = args.get("cloud").cloned();
    let cloud_paths = cloud_args.as_ref().map(|cloud| {
        CloudMovePaths::from_values(
            text(cloud, "strm_mount_prefix"),
            &texts(cloud.get("source_api_roots").expect("source roots")),
            text(cloud, "move_in_api_root"),
        )
        .expect("valid CloudDrive paths")
    });
    let runtime = FillActorRuntime {
        version: 0,
        paths: Some(paths),
        root_sentinel: args.get("root_sentinel").and_then(Value::as_str).map(str::to_owned),
        move_in_by_brand: matches!(args.get("move_in_by_brand"), Some(Value::Bool(true))),
        apply_enabled: !matches!(args.get("apply_enabled"), Some(Value::Bool(false))),
        cloud_move_paths: cloud_paths,
    };

    let writer = recorder.clone();
    let sink: Sink = Arc::new(move |call| writer.record(call));
    let fs = Arc::new(RecordingMoveFs::new(Some(sink)));
    set_faults(&fs, &seeded, args.get("faults").unwrap_or(&Value::List(Vec::new())));
    let fs_control = Arc::new(FsControl::default());
    let hook_control = Arc::clone(&fs_control);
    let after: Hook = Arc::new(move |op: &str, path: &Path, _path2: Option<&Path>| {
        let step = {
            let mut cancel_after = lock(&hook_control.cancel_after);
            match cancel_after.as_ref() {
                Some((wanted, wanted_path))
                    if wanted == op
                        && wanted_path
                            .as_ref()
                            .is_none_or(|wanted| path_text(wanted) == path_text(path)) => {}
                _ => return,
            }
            *cancel_after = None;
            lock(&hook_control.step).clone()
        };
        if let Some(step) = step {
            step.abort();
        }
    });
    fs.set_after(Some(after));

    let cloud = cloud_args
        .as_ref()
        .map(|_| AsyncCloudDrive::connect(&ClientOptions::new(fakes.clouddrive.address(), TOKEN, false)));
    let mover_control = Arc::new(MoverControl::default());
    let mover = cloud.as_ref().map(|cloud| {
        let cloud = cloud.clone();
        Arc::new(ControlledMover {
            inner: Arc::new(CloudDriveFileMover::new(Arc::new(move || Some(cloud.clone())))),
            control: Arc::clone(&mover_control),
        })
    });
    let start = DateTime::parse_from_rfc3339(spec.clock())
        .expect("an RFC 3339 clock")
        .to_utc();
    let world = World {
        seeded,
        args: args.clone(),
        test,
        databases: Mutex::new(Vec::new()),
        recorder: recorder.clone(),
        fs: Arc::clone(&fs),
        runtime,
        clock: Arc::new(Mutex::new(start)),
        tokens: tokens(),
        mover,
        mover_control: Arc::clone(&mover_control),
        repository_crash: Arc::new(Mutex::new(None)),
    };

    let (mut service, mut repository) = world.start();
    let mut plan: Option<Arc<FillActorPlan>> = None;
    let mut reported = Vec::new();
    for step in args.get("steps").and_then(Value::as_list).expect("steps") {
        let op = text(step, "op").to_owned();
        let step_args = step.get("args").cloned().unwrap_or_else(|| Value::Dict(Dict::new()));
        if let Some(seconds) = step_args.get("advance_seconds") {
            *lock(&world.clock) += TimeDelta::seconds(int(seconds));
        }
        let mut entry = only_op(&op);
        if shape(&world.seeded, &op, &step_args) {
            reported.push(Value::Dict(entry));
            continue;
        }
        match op.as_str() {
            "restart" => {
                (service, repository) = world.start();
                reported.push(Value::Dict(entry));
                continue;
            }
            "faults" => {
                set_faults(&world.fs, &world.seeded, step_args.get("faults").expect("faults"));
                reported.push(Value::Dict(entry));
                continue;
            }
            "cloud_move" => {
                // Someone else moves a CloudDrive file (a recorded `MoveFile`, as any other).
                cloud
                    .as_ref()
                    .expect("cloud_move needs args.cloud")
                    .move_file(text(&step_args, "src"), text(&step_args, "dst"))
                    .await
                    .expect("the fake answers");
                reported.push(Value::Dict(entry));
                continue;
            }
            _ => {}
        }
        let crash = step_args.get("crash");
        world
            .fs
            .set_crash(crash.filter(|crash| crash.get("fs").is_some()).map(|crash| {
                CrashPoint {
                    op: text(crash, "fs").to_owned(),
                    path: crash
                        .get("path")
                        .map(|path| world.seeded.resolve(path.as_str().expect("a path"))),
                    nth: crash
                        .get("nth")
                        .map_or(1, |nth| u32::try_from(int(nth)).expect("a count")),
                }
            }));
        *lock(&world.repository_crash) = crash.filter(|crash| crash.get("repository").is_some()).cloned();
        *lock(&world.mover_control.crash) = crash
            .and_then(|crash| crash.get("mover"))
            .map(|method| method.as_str().expect("a mover method").to_owned());
        let cancel_after = step_args.get("cancel_after");
        *lock(&fs_control.cancel_after) = cancel_after.and_then(|cancel| cancel.get("fs")).map(|op| {
            (
                op.as_str().expect("an fs op").to_owned(),
                cancel_after
                    .and_then(|cancel| cancel.get("path"))
                    .map(|path| world.seeded.resolve(path.as_str().expect("a path"))),
            )
        });
        *lock(&world.mover_control.cancel_after) = cancel_after
            .and_then(|cancel| cancel.get("mover"))
            .map(|method| method.as_str().expect("a mover method").to_owned());
        let task = tokio::spawn(run_step(
            service.clone(),
            Arc::clone(&repository),
            Arc::clone(&world.clock),
            op.clone(),
            step_args.clone(),
            plan.clone(),
        ));
        *lock(&fs_control.step) = Some(task.abort_handle());
        *lock(&world.mover_control.step) = Some(task.abort_handle());
        match task.await {
            Ok(Ok((value, next))) => {
                entry.insert("result", value);
                plan = next;
            }
            Ok(Err(error)) => entry.insert("error", error),
            Err(error) if error.is_cancelled() => entry.insert("error", error_value("CancelledError", String::new())),
            Err(error) => {
                let payload = error.into_panic();
                let message = payload
                    .downcast_ref::<&str>()
                    .map(|text| (*text).to_owned())
                    .or_else(|| payload.downcast_ref::<String>().cloned());
                if message.as_deref() != Some(CRASH) {
                    std::panic::resume_unwind(payload);
                }
                entry.insert("crashed", Value::Bool(true));
            }
        }
        *lock(&fs_control.step) = None;
        *lock(&world.mover_control.step) = None;
        world.fs.set_crash(None);
        *lock(&world.repository_crash) = None;
        *lock(&world.mover_control.crash) = None;
        *lock(&world.mover_control.cancel_after) = None;
        // `_settle`: wait for the moves the service still runs.
        service.aclose().await;
        reported.push(Value::Dict(entry));
    }
    drop((service, repository));
    if let Some(cloud) = &cloud {
        cloud.close().await;
    }
    let databases: Vec<Database> = lock(&world.databases).drain(..).collect();
    for database in databases {
        database.close().await;
    }
    let rows = embyx_db::scenario::dump_rows(world.test.pool(), &spec).await;
    let fakes_dump = fakes.dump();
    drop(fakes);
    let mut result = Dict::new();
    result.insert("steps", Value::List(reported));
    scenario::assert_transcript(Parts {
        spec: &spec,
        seeded: &world.seeded,
        outcome: scenario::result(Value::Dict(result)),
        calls: recorder.calls(),
        rows,
        fakes: fakes_dump,
    });
    let World { test, .. } = world;
    test.drop_database().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_local_matches_the_python_transcript() {
    service_steps("fill_actor/apply_local").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_rename_matches_the_python_transcript() {
    service_steps("fill_actor/crash_rename").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_hardlink_matches_the_python_transcript() {
    service_steps("fill_actor/crash_hardlink").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_matches_the_python_transcript() {
    service_steps("fill_actor/cancel").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_matches_the_python_transcript() {
    service_steps("fill_actor/recovery").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offline_matches_the_python_transcript() {
    service_steps("fill_actor/offline").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cloud_apply_matches_the_python_transcript() {
    service_steps("fill_actor/cloud_apply").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cloud_crash_matches_the_python_transcript() {
    service_steps("fill_actor/cloud_crash").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_reconcile_matches_the_python_transcript() {
    service_steps("fill_actor/crash_reconcile").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cloud_recovery_matches_the_python_transcript() {
    service_steps("fill_actor/cloud_recovery").await;
}
