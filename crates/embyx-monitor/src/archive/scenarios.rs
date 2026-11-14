//! The archive, reconcile and move-in scenarios (`tools/scenarios/specs/{archive,reconcile,
//! move_in}/`): the Rust port of `tools/scenarios/operations/archive.py` runs each spec's
//! steps through the ported pipelines over the seeded tree, through [`RecordingFs`] (the
//! Python side's `RecordingFs`), with the ledger in PostgreSQL where the spec has one, and
//! must render the bytes the Python baseline committed under `goldens/scenarios/`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeDelta, Utc};
use embyx_core::avid::AvidParser;
use embyx_core::config::Section as _;
use embyx_core::config::lax::Mode;
use embyx_core::config::models::ArchiveConfig;
use embyx_core::pyjson::{Dict, Value};
use embyx_core::scenario::{self, Parts, Recorder, Seeded, Spec};
use embyx_db::testdb::TestDatabase;

use super::{ArchivePipeline, ArchiveResult, Inspection, VideoSet};
use crate::acquisitions::{AcquisitionRepository, Ledger};
use crate::fs::recording::{Fault, FaultError, RecordingFs, Sink};
use crate::fs::{Fs, path_text};
use crate::move_in::MoveInSweeper;
use crate::reconcile::ReconcileScanner;
use crate::reports::{PyException, RunContext};
use crate::support::Clock;

fn text<'a>(args: &'a Value, key: &str) -> &'a str {
    args.get(key)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("step argument {key:?} is a string"))
}

fn flag(args: &Value, key: &str) -> bool {
    matches!(args.get(key), Some(Value::Bool(true)))
}

fn int(value: &Value) -> i64 {
    match value {
        Value::Int(int) => i64::try_from(*int).expect("small"),
        other => panic!("expected an int, got {other:?}"),
    }
}

/// `_config(ctx, values)`: the roots resolved, `enabled` defaulting to true, validated as
/// `ArchiveConfig(**values)`.
fn config(seeded: &Seeded, values: &Value) -> ArchiveConfig {
    let mut values = values.as_dict().expect("a config table").clone();
    for key in ["src_dir", "dst_dir"] {
        let resolved = path_text(&seeded.resolve(values.get(key).and_then(Value::as_str).expect("a directory")));
        values.insert(key, Value::Str(resolved));
    }
    if values.get("enabled").is_none() {
        values.insert("enabled", Value::Bool(true));
    }
    ArchiveConfig::validate(&Value::Dict(values), Mode::Python).expect("a valid archive config")
}

/// `RecordingFs(ctx, args)`: the spec's faults and stops, records to the scenario recorder.
fn recording_fs(seeded: &Seeded, args: &Value, recorder: &Recorder) -> Arc<RecordingFs> {
    let writer = recorder.clone();
    let sink: Sink = Arc::new(move |call| writer.record(call));
    let fs = RecordingFs::new(sink);
    for fault in args.get("faults").and_then(Value::as_list).unwrap_or_default() {
        let error = match fault.get("errno") {
            Some(code) => FaultError::Errno(i32::try_from(int(code)).expect("an errno")),
            None => FaultError::Message(text(fault, "message").to_owned()),
        };
        fs.add_fault(Fault {
            op: text(fault, "op").to_owned(),
            path: seeded.resolve(text(fault, "path")),
            error,
            times: fault
                .get("times")
                .map(|times| u32::try_from(int(times)).expect("a count")),
        });
    }
    for stop in args.get("stops").and_then(Value::as_list).unwrap_or_default() {
        fs.add_stop(text(stop, "op"), seeded.resolve(text(stop, "path")));
    }
    Arc::new(fs)
}

fn result_value(result: &ArchiveResult) -> Value {
    let mut value = Dict::new();
    value.insert("outcome", Value::from(result.outcome.as_str()));
    value.insert("avid", Value::Str(result.avid.clone()));
    value.insert(
        "archived_paths",
        Value::List(result.archived_paths.iter().cloned().map(Value::Str).collect()),
    );
    value.insert("reason", Value::Str(result.reason.clone()));
    Value::Dict(value)
}

fn strings(paths: impl IntoIterator<Item = String>) -> Value {
    Value::List(paths.into_iter().map(Value::Str).collect())
}

fn error_value(error: &dyn PyException) -> Value {
    let mut value = Dict::new();
    value.insert("type", Value::from(error.class_name()));
    value.insert("message", Value::Str(error.to_string()));
    Value::Dict(value)
}

/// `_shape_tree`: the unrecorded `write` and `remove` steps; false for any other step.
fn shape_tree(seeded: &Seeded, op: &str, args: &Value) -> bool {
    match op {
        "write" => {
            let path = seeded.resolve(text(args, "path"));
            std::fs::create_dir_all(path.parent().expect("a parent")).expect("parents");
            std::fs::write(
                &path,
                vec![b'x'; usize::try_from(int(args.get("size").expect("a size"))).expect("size")],
            )
            .expect("write");
            true
        }
        "remove" => {
            std::fs::remove_file(seeded.resolve(text(args, "path"))).expect("remove");
            true
        }
        _ => false,
    }
}

/// One `_run_steps` entry for a step that ran: its result or error, then the run's
/// counters, log tail and errors.
fn entry(op: &str, outcome: Result<Value, Value>, run: &RunContext) -> Value {
    let mut entry = Dict::new();
    entry.insert("op", Value::from(op));
    match outcome {
        Ok(result) => entry.insert("result", result),
        Err(error) => entry.insert("error", error),
    }
    entry.insert("stats", run.stats().to_value());
    entry.insert("log", strings(run.log_tail()));
    entry.insert("errors", strings(run.errors()));
    Value::Dict(entry)
}

fn only_op(op: &str) -> Value {
    let mut entry = Dict::new();
    entry.insert("op", Value::from(op));
    Value::Dict(entry)
}

/// The scenario's clock, moved on by a step's `advance_seconds`.
struct ScenarioClock(Arc<Mutex<DateTime<Utc>>>);

impl ScenarioClock {
    fn new(spec: &Spec) -> Self {
        let start = DateTime::parse_from_rfc3339(spec.clock())
            .expect("an RFC 3339 clock")
            .with_timezone(&Utc);
        Self(Arc::new(Mutex::new(start)))
    }

    fn clock(&self) -> Clock {
        let reading = Arc::clone(&self.0);
        Arc::new(move || *reading.lock().expect("clock"))
    }

    fn advance(&self, args: &Value) {
        if let Some(seconds) = args.get("advance_seconds") {
            *self.0.lock().expect("clock") += TimeDelta::seconds(int(seconds));
        }
    }
}

/// Prepares one step: moves the clock, shapes the tree, or hands out the step's run.
enum Step {
    Shaped,
    Run(Arc<RunContext>),
}

fn begin_step(seeded: &Seeded, fs: &RecordingFs, clock: &ScenarioClock, op: &str, args: &Value) -> Step {
    clock.advance(args);
    if shape_tree(seeded, op, args) {
        return Step::Shaped;
    }
    let run = Arc::new(RunContext::new("scenario-archive"));
    if flag(args, "stop_before") {
        run.request_stop();
    }
    fs.set_run(Some(Arc::clone(&run)));
    Step::Run(run)
}

fn steps_of(spec: &Spec) -> Vec<(String, Value)> {
    spec.section("args")
        .get("steps")
        .and_then(Value::as_list)
        .expect("steps")
        .iter()
        .map(|step| {
            (
                text(step, "op").to_owned(),
                step.get("args").cloned().unwrap_or_else(|| Value::Dict(Dict::new())),
            )
        })
        .collect()
}

fn finish(spec: &Spec, seeded: &Seeded, reported: Vec<Value>, recorder: &Recorder, rows: Value) {
    let mut result = Dict::new();
    result.insert("steps", Value::List(reported));
    scenario::assert_transcript(Parts {
        spec,
        seeded,
        outcome: scenario::result(Value::Dict(result)),
        calls: recorder.calls(),
        rows,
        fakes: embyx_db::scenario::unseeded_fakes(spec),
    });
}

/// `archive_steps`: `ArchivePipeline` step by step.
#[allow(
    clippy::too_many_lines,
    reason = "one arm per operation of `archive.steps`, in the Python operation's order"
)]
fn archive_steps(name: &str) {
    let spec = Spec::load(name);
    assert!(spec.get("database").is_none(), "archive.steps runs without a database");
    let seeded = Seeded::new(&spec);
    let recorder = Recorder::new();
    let args = spec.section("args");
    let fs = recording_fs(&seeded, &args, &recorder);
    let clock = ScenarioClock::new(&spec);
    let dyn_fs: Arc<dyn Fs> = Arc::clone(&fs) as Arc<dyn Fs>;
    let build =
        |values: &Value| ArchivePipeline::with_fs(config(&seeded, values), AvidParser::default(), Arc::clone(&dyn_fs));
    let mut pipeline = build(args.get("config").expect("a config"));
    let mut selections: Vec<VideoSet> = Vec::new();
    let mut reported = Vec::new();
    for (op, step) in steps_of(&spec) {
        let run = match begin_step(&seeded, &fs, &clock, &op, &step) {
            Step::Shaped => {
                reported.push(only_op(&op));
                continue;
            }
            Step::Run(run) => run,
        };
        let outcome: Result<Value, Value> = match op.as_str() {
            "configure" => {
                pipeline = build(step.get("config").expect("a config"));
                Ok(Value::Null)
            }
            "archive_folder" => Ok(result_value(&pipeline.archive_folder(
                &seeded.resolve(text(&step, "folder")),
                text(&step, "dst"),
                &run,
                flag(&step, "priority"),
                step.get("expected_avid").and_then(Value::as_str).unwrap_or(""),
            ))),
            "archive_loose_group" => {
                let videos: Vec<PathBuf> = step
                    .get("videos")
                    .and_then(Value::as_list)
                    .expect("videos")
                    .iter()
                    .map(|video| seeded.resolve(video.as_str().expect("a path")))
                    .collect();
                pipeline
                    .archive_loose_group(
                        text(&step, "avid"),
                        videos,
                        text(&step, "dst"),
                        &run,
                        flag(&step, "priority"),
                    )
                    .map(|result| result_value(&result))
                    .map_err(|error| error_value(&error))
            }
            "inspect_folder" => match pipeline.inspect_folder(
                &seeded.resolve(text(&step, "folder")),
                &run,
                step.get("expected_avid").and_then(Value::as_str).unwrap_or(""),
            ) {
                Inspection::Concluded(result) => Ok(result_value(&result)),
                Inspection::Selected(selection) => {
                    let mut value = Dict::new();
                    value.insert("avid", Value::Str(selection.avid.clone()));
                    value.insert("videos", strings(selection.videos.iter().map(|video| path_text(video))));
                    selections.push(selection);
                    Ok(Value::Dict(value))
                }
            },
            "file_selection" => {
                let selection = selections.pop().expect("an inspected selection");
                Ok(result_value(&pipeline.file_selection(
                    &seeded.resolve(text(&step, "folder")),
                    &selection,
                    text(&step, "dst"),
                    &run,
                    flag(&step, "priority"),
                )))
            }
            "run" => pipeline
                .run(&run)
                .map(|()| Value::Null)
                .map_err(|error| error_value(&error)),
            "library_holdings" => pipeline
                .library_holdings(text(&step, "avid"), &run, step.get("task_dir").and_then(Value::as_str))
                .map(strings)
                .map_err(|error| error_value(&error)),
            "replaced_copies" => {
                let paths: Vec<String> = step
                    .get("paths")
                    .and_then(Value::as_list)
                    .expect("paths")
                    .iter()
                    .map(|path| path.as_str().expect("a path").to_owned())
                    .collect();
                pipeline
                    .replaced_copies(text(&step, "avid"), &paths)
                    .map(|found| strings(found.iter().map(|path| path_text(path))))
                    .map_err(|error| error_value(&error))
            }
            "route_for_task_dir" => Ok(match pipeline.route_for_task_dir(text(&step, "task_dir")) {
                None => Value::Null,
                Some((dst, priority)) => Value::List(vec![Value::Str(dst), Value::Bool(priority)]),
            }),
            other => panic!("unknown step {other:?}"),
        };
        reported.push(entry(&op, outcome, &run));
    }
    finish(&spec, &seeded, reported, &recorder, Value::Dict(Dict::new()));
}

/// The ledger on a scenario database (`_ledger`).
async fn ledger(spec: &Spec) -> (TestDatabase, Arc<dyn Ledger>) {
    let database = TestDatabase::create().await;
    embyx_db::scenario::seed(database.pool(), spec).await;
    let ledger: Arc<dyn Ledger> = Arc::new(AcquisitionRepository::new(database.database()));
    (database, ledger)
}

/// `reconcile_steps`: `ReconcileScanner` passes, the ledger in PostgreSQL.
async fn reconcile_steps(name: &str) {
    let spec = Spec::load(name);
    let seeded = Seeded::new(&spec);
    let (database, ledger) = ledger(&spec).await;
    let recorder = Recorder::new();
    let args = spec.section("args");
    let fs = recording_fs(&seeded, &args, &recorder);
    let clock = ScenarioClock::new(&spec);
    let dyn_fs: Arc<dyn Fs> = Arc::clone(&fs) as Arc<dyn Fs>;
    let build = |values: &Value| {
        let config = config(&seeded, values);
        (
            ArchivePipeline::with_fs(config.clone(), AvidParser::default(), Arc::clone(&dyn_fs)),
            config,
        )
    };
    let (archiver, archive_config) = build(args.get("config").expect("a config"));
    let mut scanner = ReconcileScanner::new(ledger, archiver, archive_config).with_clock(clock.clock());
    let mut reported = Vec::new();
    for (op, step) in steps_of(&spec) {
        let run = match begin_step(&seeded, &fs, &clock, &op, &step) {
            Step::Shaped => {
                reported.push(only_op(&op));
                continue;
            }
            Step::Run(run) => run,
        };
        let outcome = match op.as_str() {
            "run" => scanner
                .run(&run)
                .await
                .map(|()| Value::Null)
                .map_err(|error| error_value(&error)),
            "rebind" => {
                let (archiver, config) = build(step.get("config").expect("a config"));
                scanner.rebind(archiver, config);
                Ok(Value::Null)
            }
            other => panic!("unknown step {other:?}"),
        };
        reported.push(entry(&op, outcome, &run));
    }
    let rows = embyx_db::scenario::dump_rows(database.pool(), &spec).await;
    finish(&spec, &seeded, reported, &recorder, rows);
    database.drop_database().await;
}

/// `move_in_steps`: `MoveInSweeper` passes, the ledger in PostgreSQL.
async fn move_in_steps(name: &str) {
    let spec = Spec::load(name);
    let seeded = Seeded::new(&spec);
    let (database, ledger) = ledger(&spec).await;
    let recorder = Recorder::new();
    let args = spec.section("args");
    let fs = recording_fs(&seeded, &args, &recorder);
    let clock = ScenarioClock::new(&spec);
    let archiver = ArchivePipeline::with_fs(
        config(&seeded, args.get("config").expect("a config")),
        AvidParser::default(),
        Arc::clone(&fs) as Arc<dyn Fs>,
    );
    let mut sweeper = MoveInSweeper::new(
        ledger,
        archiver.clone(),
        AvidParser::default(),
        seeded.resolve(text(&args, "move_in_root")),
        text(&args, "task_dir_path").to_owned(),
    )
    .with_clock(clock.clock());
    let mut reported = Vec::new();
    for (op, step) in steps_of(&spec) {
        let run = match begin_step(&seeded, &fs, &clock, &op, &step) {
            Step::Shaped => {
                reported.push(only_op(&op));
                continue;
            }
            Step::Run(run) => run,
        };
        let outcome = match op.as_str() {
            "run" => sweeper
                .run(&run)
                .await
                .map(|()| Value::Null)
                .map_err(|error| error_value(&error)),
            "rebind" => {
                let root = step
                    .get("move_in_root")
                    .and_then(Value::as_str)
                    .unwrap_or_else(|| text(&args, "move_in_root"));
                sweeper.rebind(
                    archiver.clone(),
                    AvidParser::default(),
                    seeded.resolve(root),
                    text(&step, "task_dir_path").to_owned(),
                );
                Ok(Value::Null)
            }
            other => panic!("unknown step {other:?}"),
        };
        reported.push(entry(&op, outcome, &run));
    }
    let rows = embyx_db::scenario::dump_rows(database.pool(), &spec).await;
    finish(&spec, &seeded, reported, &recorder, rows);
    database.drop_database().await;
}

#[test]
fn archive_outcomes_match_the_python_transcript() {
    archive_steps("archive/outcomes");
}

#[test]
fn archive_collisions_match_the_python_transcript() {
    archive_steps("archive/collisions");
}

#[test]
fn archive_priority_matches_the_python_transcript() {
    archive_steps("archive/priority");
}

#[test]
fn archive_run_matches_the_python_transcript() {
    archive_steps("archive/run");
}

#[test]
fn archive_failures_match_the_python_transcript() {
    archive_steps("archive/failures");
}

#[test]
fn archive_cancel_matches_the_python_transcript() {
    archive_steps("archive/cancel");
}

#[tokio::test]
async fn reconcile_passes_match_the_python_transcript() {
    reconcile_steps("reconcile/passes").await;
}

#[tokio::test]
async fn reconcile_failures_match_the_python_transcript() {
    reconcile_steps("reconcile/failures").await;
}

#[tokio::test]
async fn move_in_passes_match_the_python_transcript() {
    move_in_steps("move_in/passes").await;
}

#[tokio::test]
async fn move_in_unrouted_matches_the_python_transcript() {
    move_in_steps("move_in/unrouted").await;
}
