//! The tracker, replacement (换版) and manual intake scenarios (`tools/scenarios/specs/{tracker,
//! replace,manual}/`): the Rust port of `tools/scenarios/operations/tracker.py` builds the
//! tracker, the replacement source and the manual source over one ledger in PostgreSQL, one
//! `AsyncCloudDrive` talking to the Rust fake and the seeded mount seen through
//! [`RecordingFs`], runs each spec's steps, and must render the bytes the Python baseline
//! committed under `goldens/scenarios/`.
//!
//! A step's `cancel_after` cancels it the moment that RPC has answered: the Python operation
//! cancels the step's task there, and here the step's future is dropped at the same point.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use embyx_clients::clouddrive::proto::FileOperationResult;
use embyx_clients::clouddrive::{AsyncCloudDrive, ClientOptions, CloudFile, OfflineTask};
use embyx_clients::javbus::JavBusMagnet;
use embyx_clients::net::retry::RecordingSleeper;
use embyx_clients::scenario::ScenarioFakes;
use embyx_core::avid::AvidParser;
use embyx_core::pyjson::{Dict, Value};
use embyx_core::scenario::{self, Parts, Recorder, Seeded, Spec};
use embyx_db::testdb::TestDatabase;
use tokio::sync::Notify;

use super::{AcquisitionTracker, SubmitMagnet, TrackerParts, TrackerSettings};
use crate::acquisitions::{AcquisitionRepository, Ledger};
use crate::archive::ArchivePipeline;
use crate::archive::scenarios::{
    ScenarioClock, Step, begin_step, config, flag, only_op, recording_fs, steps_of, strings, text,
};
use crate::cloud::{CloudListing, TrackerCloud};
use crate::fs::{Fs, path_text};
use crate::intake::{AcquisitionIntake, IntakeParts, JavBusMagnets, OfflineFiles, SourceError, SukebeiMagnets};
use crate::manual::{DirectoryListing, ManualIntakeSource, ManualSubmission, SubmitOptions};
use crate::replace::{ReplacePlan, ReplacementSource};
use crate::reports::{PyException, RunContext};
use crate::support::BoxFuture;

/// `operations/intake.py` `TOKEN`.
const TOKEN: &str = "scenario-token";

/// `_scrub`: grpcio's `debug_error_string` names the peer and the time.
fn scrub(text: &str) -> String {
    const OPEN: &str = "debug_error_string = \"";
    const CLOSE: &str = "\"\n>";
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find(OPEN) {
        let after = &rest[start + OPEN.len()..];
        let Some(end) = after.find(CLOSE) else { break };
        out.push_str(&rest[..start]);
        out.push_str("debug_error_string = \"<scrubbed>\"\n>");
        rest = &after[end + CLOSE.len()..];
    }
    out.push_str(rest);
    out
}

fn scrubbed_error(error: &dyn PyException) -> Value {
    let mut value = Dict::new();
    value.insert("type", Value::from(error.class_name()));
    value.insert("message", Value::Str(scrub(&error.to_string())));
    Value::Dict(value)
}

/// `_Sukebei`: `args.sukebei[avid]`, or no magnet.
struct StubSukebei(Value);

impl SukebeiMagnets for StubSukebei {
    fn get_magnet<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Option<String>, SourceError>> {
        let answer = self.0.get(avid).and_then(Value::as_str).map(str::to_owned);
        Box::pin(std::future::ready(Ok(answer)))
    }
}

/// `_JavBus({})`: nothing.
struct NoJavBus;

impl JavBusMagnets for NoJavBus {
    fn get_magnets<'a>(&'a self, _avid: &'a str) -> BoxFuture<'a, Result<Vec<JavBusMagnet>, SourceError>> {
        Box::pin(std::future::ready(Ok(Vec::new())))
    }
}

/// `_Cloud`: the `AsyncCloudDrive`, with the mount following API deletes and a cancel switch.
struct ScenarioCloud {
    inner: Arc<AsyncCloudDrive>,
    mount: PathBuf,
    names: Mutex<HashMap<(String, String), String>>,
    keeps_deleted: AtomicBool,
    cancel_after: Mutex<Option<String>>,
    cancelled: Notify,
}

impl ScenarioCloud {
    /// `_answered(method)`: past this point the step is cancelled and never resumes.
    async fn answered(&self, method: &str) {
        let hit = {
            let mut cancel = self.cancel_after.lock().expect("cancel switch");
            if cancel.as_deref() == Some(method) {
                *cancel = None;
                true
            } else {
                false
            }
        };
        if hit {
            self.cancelled.notify_one();
            std::future::pending::<()>().await;
        }
    }

    /// `_drop(api_path)`: the mount stops showing what the API deleted (not recorded).
    fn drop_local(&self, api_path: &str) {
        if self.keeps_deleted.load(Ordering::SeqCst) {
            return;
        }
        let local = self.mount.join(api_path.trim_start_matches('/'));
        if local.is_dir() {
            std::fs::remove_dir_all(&local).expect("rmtree");
        } else if local.exists() {
            std::fs::remove_file(&local).expect("unlink");
        }
    }
}

impl CloudListing for ScenarioCloud {
    fn list_directory<'a>(&'a self, api_dir: &'a str) -> BoxFuture<'a, Result<Vec<CloudFile>, SourceError>> {
        CloudListing::list_directory(&*self.inner, api_dir)
    }
}

impl TrackerCloud for ScenarioCloud {
    fn list_offline_files<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Vec<OfflineTask>, SourceError>> {
        Box::pin(async move {
            let tasks = TrackerCloud::list_offline_files(&*self.inner, path).await?;
            let mut names = self.names.lock().expect("names");
            for task in &tasks {
                names.insert((path.to_owned(), task.info_hash.clone()), task.name.clone());
            }
            Ok(tasks)
        })
    }

    fn remove_offline_files<'a>(
        &'a self,
        info_hashes: &'a [String],
        path: &'a str,
        delete_files: bool,
    ) -> BoxFuture<'a, Result<(), SourceError>> {
        Box::pin(async move {
            TrackerCloud::remove_offline_files(&*self.inner, info_hashes, path, delete_files).await?;
            if delete_files {
                for info_hash in info_hashes {
                    let name = self
                        .names
                        .lock()
                        .expect("names")
                        .get(&(path.to_owned(), info_hash.clone()))
                        .cloned();
                    if let Some(name) = name.filter(|name| !name.is_empty()) {
                        self.drop_local(&embyx_core::pypath::join(path, &name));
                    }
                }
            }
            self.answered("RemoveOfflineFiles").await;
            Ok(())
        })
    }

    fn delete_files<'a>(&'a self, api_paths: &'a [String]) -> BoxFuture<'a, Result<(), SourceError>> {
        Box::pin(async move {
            let result = self.inner.delete_files(api_paths).await?;
            if result.success {
                for path in api_paths {
                    self.drop_local(path);
                }
            }
            self.answered("DeleteFiles").await;
            Ok(())
        })
    }

    fn settle(&self) -> BoxFuture<'_, ()> {
        TrackerCloud::settle(&*self.inner)
    }
}

impl OfflineFiles for ScenarioCloud {
    fn add_offline_files<'a>(
        &'a self,
        urls: &'a [String],
        dst_dir: &'a str,
    ) -> BoxFuture<'a, Result<FileOperationResult, SourceError>> {
        Box::pin(async move {
            let result = OfflineFiles::add_offline_files(&*self.inner, urls, dst_dir).await?;
            self.answered("AddOfflineFiles").await;
            Ok(result)
        })
    }
}

fn plan_value(plan: &ReplacePlan) -> Value {
    let mut value = Dict::new();
    value.insert("avid", Value::Str(plan.avid.clone()));
    value.insert("holdings", strings(plan.holdings.iter().cloned()));
    value.insert(
        "state",
        plan.state.map_or(Value::Null, |state| Value::from(state.as_str())),
    );
    value.insert(
        "task_dir_path",
        plan.task_dir_path.clone().map_or(Value::Null, Value::Str),
    );
    value.insert("blocked", plan.blocked.map_or(Value::Null, Value::from));
    Value::Dict(value)
}

fn submission_value(submission: &ManualSubmission) -> Value {
    let entries = submission
        .entries
        .iter()
        .map(|entry| {
            let mut value = Dict::new();
            value.insert("text", Value::Str(entry.text.clone()));
            value.insert("avid", entry.avid.clone().map_or(Value::Null, Value::Str));
            value.insert("outcome", Value::from(entry.outcome.as_str()));
            value.insert("archived_paths", strings(entry.archived_paths.iter().cloned()));
            Value::Dict(value)
        })
        .collect();
    let mut value = Dict::new();
    value.insert("task_dir_path", Value::Str(submission.task_dir_path.clone()));
    value.insert("entries", Value::List(entries));
    Value::Dict(value)
}

fn listing_value(listing: &DirectoryListing) -> Value {
    let entries = listing
        .entries
        .iter()
        .map(|entry| {
            let mut value = Dict::new();
            value.insert("path", Value::Str(entry.path.clone()));
            value.insert("name", Value::Str(entry.name.clone()));
            value.insert("configured", Value::Bool(entry.configured));
            value.insert("routed", Value::Bool(entry.routed));
            Value::Dict(value)
        })
        .collect();
    let mut value = Dict::new();
    value.insert("path", Value::Str(listing.path.clone()));
    value.insert("parent", listing.parent.clone().map_or(Value::Null, Value::Str));
    value.insert("entries", Value::List(entries));
    Value::Dict(value)
}

/// The world `steps` builds.
struct World {
    tracker: AcquisitionTracker,
    replacer: ReplacementSource,
    manual: ManualIntakeSource,
}

impl World {
    async fn run_step(&self, op: &str, step: &Value, run: &RunContext) -> Result<Value, Value> {
        match op {
            "poll" => self
                .tracker
                .poll(run)
                .await
                .map(|()| Value::Null)
                .map_err(|error| scrubbed_error(&error)),
            "plan" => self
                .replacer
                .plan(text(step, "text"))
                .await
                .map(|plan| plan_value(&plan))
                .map_err(|error| scrubbed_error(&error)),
            "start" => self
                .replacer
                .start(text(step, "text"), step.get("magnet").and_then(Value::as_str))
                .await
                .map(|result| {
                    let mut value = Dict::new();
                    value.insert("outcome", Value::from(result.outcome.as_str()));
                    value.insert("state", Value::from(result.record.state.as_str()));
                    Value::Dict(value)
                })
                .map_err(|error| scrubbed_error(&error)),
            "browse" => self
                .manual
                .browse(text(step, "path"))
                .await
                .map(|listing| listing_value(&listing))
                .map_err(|error| scrubbed_error(&error)),
            "default_directory" => self
                .manual
                .default_directory()
                .await
                .map(|found| found.map_or(Value::Null, Value::Str))
                .map_err(|error| scrubbed_error(&error)),
            "check_target" => self
                .manual
                .check_target(text(step, "task_dir"))
                .await
                .map(Value::Str)
                .map_err(|error| scrubbed_error(&error)),
            "submit" => {
                let lines: Vec<String> = step
                    .get("lines")
                    .and_then(Value::as_list)
                    .expect("lines")
                    .iter()
                    .map(|line| line.as_str().expect("a line").to_owned())
                    .collect();
                let options = SubmitOptions {
                    queue: flag(step, "queue"),
                    ..SubmitOptions::default()
                };
                self.manual
                    .submit(&lines, text(step, "task_dir"), options)
                    .await
                    .map(|submission| submission_value(&submission))
                    .map_err(|error| scrubbed_error(&error))
            }
            other => panic!("unknown step {other:?}"),
        }
    }
}

/// `steps`: the tracker, the replacement source and the manual source over one world.
#[allow(
    clippy::too_many_lines,
    reason = "the world is built in the Python operation's order, then its step loop"
)]
async fn tracker_steps(name: &str) {
    let spec = Spec::load(name);
    let seeded = Seeded::new(&spec);
    let database = TestDatabase::create().await;
    embyx_db::scenario::seed(database.pool(), &spec).await;
    let recorder = Recorder::new();
    let fakes = ScenarioFakes::start(&spec, &recorder).await;
    let args = spec.section("args");
    let task_dirs: Vec<String> = args
        .get("task_dirs")
        .and_then(Value::as_list)
        .expect("task_dirs")
        .iter()
        .map(|dir| dir.as_str().expect("a directory").to_owned())
        .collect();
    // `resolve(ctx.roots, "mount")` is the root itself, with no trailing separator.
    let mount = PathBuf::from(path_text(&seeded.resolve(text(&args, "mount"))).trim_end_matches('/'));
    let archive_config = config(&seeded, args.get("config").expect("a config"));
    let fs = recording_fs(&seeded, &args, &recorder);
    let clock = ScenarioClock::new(&spec);
    let inner = Arc::new(AsyncCloudDrive::connect(&ClientOptions::new(
        fakes.clouddrive.address(),
        TOKEN,
        false,
    )));
    let cloud = Arc::new(ScenarioCloud {
        inner: Arc::clone(&inner),
        mount: mount.clone(),
        names: Mutex::new(HashMap::new()),
        keeps_deleted: AtomicBool::new(false),
        cancel_after: Mutex::new(None),
        cancelled: Notify::new(),
    });
    let ledger: Arc<dyn Ledger> = Arc::new(AcquisitionRepository::new(database.database()));
    let sleeper = RecordingSleeper::new();
    let intake = Arc::new(
        AcquisitionIntake::new(IntakeParts {
            ledger: Arc::clone(&ledger),
            sukebei: Arc::new(StubSukebei(
                args.get("sukebei").cloned().unwrap_or(Value::Dict(Dict::new())),
            )),
            javbus: Arc::new(NoJavBus),
            cloud: Arc::clone(&cloud) as Arc<dyn OfflineFiles>,
            failed_cooldown_seconds: args
                .get("failed_cooldown_seconds")
                .map_or(3600, crate::archive::scenarios::int),
            on_submitted: None,
            release_date_lookup: None,
        })
        .expect("a valid cooldown")
        .with_clock(clock.clock())
        .with_sleeper(sleeper.sleeper()),
    );
    let dyn_fs: Arc<dyn Fs> = Arc::clone(&fs) as Arc<dyn Fs>;
    let archiver = {
        let archive_config = archive_config.clone();
        move || ArchivePipeline::with_fs(archive_config.clone(), AvidParser::default(), Arc::clone(&dyn_fs))
    };
    let submit: SubmitMagnet = {
        let ledger = Arc::clone(&ledger);
        let cloud = Arc::clone(&cloud);
        let first = task_dirs[0].clone();
        Arc::new(move |avid: String, magnet: String| {
            let ledger = Arc::clone(&ledger);
            let cloud = Arc::clone(&cloud);
            let first = first.clone();
            Box::pin(async move {
                let record = ledger.get(&avid).await?;
                let task_dir = record.and_then(|record| record.task_dir_path).unwrap_or(first);
                Ok(
                    match OfflineFiles::add_offline_files(&*cloud, &[magnet], &task_dir).await {
                        Ok(result) => result.success,
                        Err(SourceError::CloudDrive(error)) => {
                            error.details().is_some_and(|details| details.contains("任务已存在"))
                        }
                        Err(_) => false,
                    },
                )
            })
        })
    };
    let tracker = AcquisitionTracker::new(TrackerParts {
        ledger: Arc::clone(&ledger),
        cloud: Arc::clone(&cloud) as Arc<dyn TrackerCloud>,
        archiver: archiver(),
        settings: TrackerSettings::from_config(&archive_config, task_dirs.clone(), &path_text(&mount)),
        submit_magnet: Arc::clone(&submit),
        release_date_lookup: None,
    })
    .with_clock(clock.clock());
    let configured: Arc<dyn Fn() -> Vec<String> + Send + Sync> = {
        let task_dirs = task_dirs.clone();
        Arc::new(move || task_dirs.clone())
    };
    let intake_factory = {
        let intake = Arc::clone(&intake);
        Arc::new(move || Some(Arc::clone(&intake)))
    };
    let archiver = Arc::new(archiver);
    let replacer = ReplacementSource::new(
        Arc::clone(&ledger),
        intake_factory.clone(),
        {
            let archiver = Arc::clone(&archiver);
            Arc::new(move || archiver())
        },
        Arc::new(AvidParser::default),
        Arc::clone(&configured),
        submit,
    )
    .with_clock(clock.clock());
    let manual = ManualIntakeSource {
        ledger: Arc::clone(&ledger),
        intake_factory,
        cloud_factory: {
            let cloud = Arc::clone(&cloud);
            Arc::new(move || Some(Arc::clone(&cloud) as Arc<dyn CloudListing>))
        },
        archiver_factory: {
            let archiver = Arc::clone(&archiver);
            Arc::new(move || archiver())
        },
        parser_factory: Arc::new(AvidParser::default),
        configured_dirs: configured,
        on_queued: None,
        logger: crate::manual::LOGGER.to_owned(),
    };
    let world = World {
        tracker,
        replacer,
        manual,
    };

    let mut reported = Vec::new();
    for (op, step) in steps_of(&spec) {
        let run = match begin_step(&seeded, &fs, &clock, &op, &step) {
            Step::Shaped => {
                reported.push(only_op(&op));
                continue;
            }
            Step::Run(run) => run,
        };
        cloud
            .keeps_deleted
            .store(flag(&step, "mount_keeps_deleted"), Ordering::SeqCst);
        *cloud.cancel_after.lock().expect("cancel switch") =
            step.get("cancel_after").and_then(Value::as_str).map(str::to_owned);
        let outcome = {
            let running = world.run_step(&op, &step, &run);
            tokio::pin!(running);
            tokio::select! {
                biased;
                () = cloud.cancelled.notified() => {
                    let mut error = Dict::new();
                    error.insert("type", Value::from("CancelledError"));
                    error.insert("message", Value::from(""));
                    Err(Value::Dict(error))
                }
                outcome = &mut running => outcome,
            }
        };
        let mut entry = Dict::new();
        entry.insert("op", Value::from(op.as_str()));
        match outcome {
            Ok(result) => entry.insert("result", result),
            Err(error) => entry.insert("error", error),
        }
        if op == "poll" {
            entry.insert("stats", run.stats().to_value());
            entry.insert("log", strings(run.log_tail().iter().map(|line| scrub(line))));
            entry.insert("errors", strings(run.errors().iter().map(|line| scrub(line))));
        }
        reported.push(Value::Dict(entry));
    }
    inner.close().await;
    let rows = embyx_db::scenario::dump_rows(database.pool(), &spec).await;
    let fakes_dump = fakes.dump();
    drop(fakes);
    let mut result = Dict::new();
    result.insert("steps", Value::List(reported));
    scenario::assert_transcript(Parts {
        spec: &spec,
        seeded: &seeded,
        outcome: scenario::result(Value::Dict(result)),
        calls: recorder.calls(),
        rows,
        fakes: fakes_dump,
    });
    database.drop_database().await;
}

#[tokio::test]
async fn tracker_outcomes_match_the_python_transcript() {
    tracker_steps("tracker/outcomes").await;
}

#[tokio::test]
async fn tracker_failures_match_the_python_transcript() {
    tracker_steps("tracker/failures").await;
}

#[tokio::test]
async fn tracker_cancel_matches_the_python_transcript() {
    tracker_steps("tracker/cancel").await;
}

#[tokio::test]
async fn replace_filing_matches_the_python_transcript() {
    tracker_steps("replace/filing").await;
}

#[tokio::test]
async fn replace_cancel_matches_the_python_transcript() {
    tracker_steps("replace/cancel").await;
}

#[tokio::test]
async fn replace_failures_match_the_python_transcript() {
    tracker_steps("replace/failures").await;
}

#[tokio::test]
async fn replace_outside_matches_the_python_transcript() {
    tracker_steps("replace/outside").await;
}

#[tokio::test]
async fn replace_start_matches_the_python_transcript() {
    tracker_steps("replace/start").await;
}

#[tokio::test]
async fn manual_submit_matches_the_python_transcript() {
    tracker_steps("manual/submit").await;
}

/// Rust only: the cancel-safety `replace/cancel` cannot show (Python's thread finishes a sent
/// call on its own). The poll is dropped while CloudDrive still holds the old copy's
/// `DeleteFiles`; the call still completes, and `settle()` on the tracker's scoped handle
/// returns only once it has.
#[tokio::test]
async fn a_dropped_poll_leaves_its_delete_running_and_settle_waits_for_it() {
    use std::time::Duration;

    let spec = Spec::load("replace/cancel");
    let seeded = Seeded::new(&spec);
    let database = TestDatabase::create().await;
    embyx_db::scenario::seed(database.pool(), &spec).await;
    let recorder = Recorder::new();
    let fakes = ScenarioFakes::start(&spec, &recorder).await;
    let hold = fakes.clouddrive.fake.hold("DeleteFiles");
    let args = spec.section("args");
    let archive_config = config(&seeded, args.get("config").expect("a config"));
    let mount = seeded.resolve("mount");
    let client = AsyncCloudDrive::connect(&ClientOptions::new(fakes.clouddrive.address(), TOKEN, false));
    let ledger: Arc<dyn Ledger> = Arc::new(AcquisitionRepository::new(database.database()));
    let submit: SubmitMagnet = Arc::new(|_, _| Box::pin(std::future::ready(Ok(false))));
    let tracker = AcquisitionTracker::new(TrackerParts {
        ledger: Arc::clone(&ledger),
        cloud: Arc::new(client.scoped()),
        archiver: ArchivePipeline::new(archive_config.clone(), AvidParser::default()),
        settings: TrackerSettings::from_config(&archive_config, vec!["/115/task/inbox".to_owned()], &path_text(&mount)),
        submit_magnet: submit,
        release_date_lookup: None,
    });
    let run = RunContext::new("test-tracker");
    {
        let poll = tracker.poll(&run);
        tokio::pin!(poll);
        tokio::select! {
            () = hold.arrived() => {}
            outcome = &mut poll => panic!("the poll ended before deleting: {outcome:?}"),
        }
        // Dropped here, with DeleteFiles held at CloudDrive.
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), tracker.settle())
            .await
            .is_err(),
        "settle waits for the held delete"
    );
    hold.release();
    tokio::time::timeout(Duration::from_secs(5), tracker.settle())
        .await
        .expect("the delete ends");
    let deleted = recorder
        .calls()
        .iter()
        .filter(|call| call.get("method") == Some(&Value::from("DeleteFiles")))
        .filter(|call| call.get("response").is_some())
        .count();
    assert_eq!(deleted, 1);
    let dump = fakes.clouddrive.fake.dump();
    let files = dump.get("files").and_then(Value::as_list).expect("files");
    assert!(
        files
            .iter()
            .all(|file| file.get("path") != Some(&Value::from("/library/sorted/RPX/RPX-311.mp4")))
    );
    client.close().await;
    drop(fakes);
    database.drop_database().await;
}

#[test]
fn scrub_replaces_only_the_debug_string() {
    let text = "<_InactiveRpcError of RPC that terminated with:\n\tstatus = StatusCode.INTERNAL\n\tdetails = \"x\"\n\tdebug_error_string = \"INTERNAL:x {a:\"b\"}\"\n>: tail";
    assert_eq!(
        scrub(text),
        "<_InactiveRpcError of RPC that terminated with:\n\tstatus = StatusCode.INTERNAL\n\tdetails = \"x\"\n\tdebug_error_string = \"<scrubbed>\"\n>: tail"
    );
    assert_eq!(scrub("plain"), "plain");
}
