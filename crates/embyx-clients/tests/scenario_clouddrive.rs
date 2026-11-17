//! The CloudDrive scenarios (`tools/scenarios/specs/clouddrive/`): the Rust port of
//! `tools/scenarios/operations/clouddrive.py` runs each spec's steps through
//! `AsyncCloudDrive` against the Rust fake and must render the bytes the Python baseline
//! committed under `goldens/scenarios/clouddrive/`.

use std::future::Future;
use std::pin::pin;
use std::task::Poll;

use embyx_clients::clouddrive::client::code_name;
use embyx_clients::clouddrive::fake::to_json;
use embyx_clients::clouddrive::{
    AsyncCloudDrive, ClientOptions, CloudDriveError, CloudFile, CopyTask, OfflineTask, UploadState,
};
use embyx_clients::scenario::ScenarioFakes;
use embyx_core::pyjson::{Dict, Value};
use embyx_core::scenario::{self, Parts, Recorder, Seeded, Spec};
use embyx_db::testdb::TestDatabase;

/// `operations/clouddrive.py` `TOKEN`.
const TOKEN: &str = "scenario-token";

fn text<'a>(args: &'a Value, key: &str) -> &'a str {
    args.get(key)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("step argument {key:?} is a string"))
}

fn texts(args: &Value, key: &str) -> Vec<String> {
    args.get(key)
        .and_then(Value::as_list)
        .unwrap_or_else(|| panic!("step argument {key:?} is a list"))
        .iter()
        .map(|item| item.as_str().expect("a string").to_owned())
        .collect()
}

fn flag(args: &Value, key: &str) -> Option<bool> {
    args.get(key).map(|value| match value {
        Value::Bool(flag) => *flag,
        other => panic!("step argument {key:?} is a bool, not {other:?}"),
    })
}

fn list(items: Vec<Value>) -> Value {
    Value::List(items)
}

/// `plain(UploadState)`.
fn upload_state(state: &UploadState) -> Value {
    let mut dict = Dict::new();
    dict.insert("status", Value::from(state.status.as_str()));
    dict.insert("transferred", Value::Int(i128::from(state.transferred)));
    dict.insert("error", Value::from(state.error.as_str()));
    dict.insert("fatal", Value::from(state.fatal()));
    Value::Dict(dict)
}

/// `error_of`: an RPC error by code and details, anything else by its text.
fn error_of(error: &CloudDriveError) -> Value {
    let mut dict = Dict::new();
    dict.insert("type", Value::from(error.class_name()));
    if let Some(rpc) = error.as_rpc() {
        dict.insert("code", Value::from(code_name(rpc.code)));
        dict.insert("details", Value::from(rpc.details.as_str()));
    } else {
        dict.insert("message", Value::Str(error.to_string()));
    }
    Value::Dict(dict)
}

/// `_run`: one step, its value as `plain()` reports it.
async fn run(cloud: &AsyncCloudDrive, op: &str, args: &Value) -> Result<Value, CloudDriveError> {
    Ok(match op {
        "check" => {
            let check = cloud.check().await?;
            let mut dict = Dict::new();
            dict.insert("reachable", Value::Bool(check.reachable));
            dict.insert("authenticated", Value::Bool(check.authenticated));
            dict.insert("version", Value::Str(check.version));
            Value::Dict(dict)
        }
        "list_directory" => {
            let force_refresh = flag(args, "force_refresh").unwrap_or(true);
            let files = cloud.list_directory(text(args, "path"), force_refresh).await?;
            list(files.iter().map(CloudFile::to_value).collect())
        }
        "stat_file" => cloud
            .stat_file(text(args, "path"))
            .await?
            .map_or(Value::Null, |file| file.to_value()),
        "ensure_directory" => cloud
            .ensure_directory(text(args, "parent"), text(args, "name"))
            .await?
            .to_value(),
        "move_file" => cloud
            .move_file(text(args, "source"), text(args, "destination"))
            .await?
            .to_value(),
        "copy_file" => cloud
            .copy_file(text(args, "source"), text(args, "destination"))
            .await?
            .to_value(),
        "copy_tasks" => list(cloud.copy_tasks().await?.iter().map(CopyTask::to_value).collect()),
        "upload_state" => cloud
            .upload_state(text(args, "path"))
            .await?
            .as_ref()
            .map_or(Value::Null, upload_state),
        "restart_copy_task" => {
            cloud
                .restart_copy_task(text(args, "source"), text(args, "destination"))
                .await?;
            Value::Null
        }
        "cancel_copy_task" => {
            cloud
                .cancel_copy_task(text(args, "source"), text(args, "destination"))
                .await?;
            Value::Null
        }
        "delete_files" => cloud.delete_files(&texts(args, "paths")).await?.to_value(),
        "add_offline_files" => to_json(
            &cloud
                .add_offline_files(&texts(args, "urls"), text(args, "folder"))
                .await?,
            "clouddrive.FileOperationResult",
        ),
        "list_offline_files" => list(
            cloud
                .list_offline_files(text(args, "path"))
                .await?
                .iter()
                .map(OfflineTask::to_value)
                .collect(),
        ),
        "remove_offline_files" => to_json(
            &cloud
                .remove_offline_files(
                    &texts(args, "hashes"),
                    text(args, "path"),
                    flag(args, "delete_files").expect("delete_files"),
                )
                .await?,
            "clouddrive.FileOperationResult",
        ),
        // The wrapper has no rename; the scenario calls the client, as Python does.
        "rename_file" => to_json(
            &cloud
                .client()
                .rename_file(text(args, "path"), text(args, "name"))
                .await
                .map_err(CloudDriveError::from)?,
            "clouddrive.FileOperationResult",
        ),
        other => panic!("unknown step {other:?}"),
    })
}

/// `steps`.
async fn steps(cloud: &AsyncCloudDrive, args: &Value) -> Value {
    let mut reported = Vec::new();
    for step in args.get("steps").and_then(Value::as_list).unwrap_or_default() {
        let op = text(step, "op");
        let mut entry = Dict::new();
        entry.insert("op", Value::from(op));
        let step_args = step.get("args").cloned().unwrap_or_else(|| Value::Dict(Dict::new()));
        match run(cloud, op, &step_args).await {
            Ok(value) => entry.insert("result", value),
            Err(error) => entry.insert("error", error_of(&error)),
        }
        reported.push(Value::Dict(entry));
    }
    let mut result = Dict::new();
    result.insert("steps", Value::List(reported));
    Value::Dict(result)
}

/// `cancelled_move`: start the move, let it take one step (the wrapper hands the RPC to a
/// task of its own), then drop the caller, as `task.cancel()` does in Python.
async fn cancelled_move(cloud: &AsyncCloudDrive, args: &Value) -> Value {
    let source = text(args, "source").to_owned();
    let destination = text(args, "destination").to_owned();
    let outcome = {
        let mut call = pin!(cloud.move_file(&source, &destination));
        let first = std::future::poll_fn(|context| Poll::Ready(call.as_mut().poll(context))).await;
        if first.is_ready() { "finished" } else { "cancelled" }
    };
    let mut result = Dict::new();
    result.insert("caller", Value::from(outcome));
    Value::Dict(result)
}

async fn run_scenario(name: &str) {
    let spec = Spec::load(name);
    let seeded = Seeded::new(&spec);
    let database = TestDatabase::create().await;
    embyx_db::scenario::seed(database.pool(), &spec).await;
    let recorder = Recorder::new();
    let fakes = ScenarioFakes::start(&spec, &recorder).await;
    let cloud = AsyncCloudDrive::connect(&ClientOptions::new(fakes.clouddrive.address(), TOKEN, false));
    let args = spec.section("args");
    let result = match spec.operation() {
        "clouddrive.steps" => steps(&cloud, &args).await,
        "clouddrive.cancelled_move" => cancelled_move(&cloud, &args).await,
        other => panic!("{name}: not a CloudDrive operation: {other}"),
    };
    // `finally: await cloud.aclose()`: also waits for a move whose caller is gone.
    cloud.close().await;
    let rows = embyx_db::scenario::dump_rows(database.pool(), &spec).await;
    let fakes_dump = fakes.dump();
    drop(fakes);
    scenario::assert_transcript(Parts {
        spec: &spec,
        seeded: &seeded,
        outcome: scenario::result(result),
        calls: recorder.calls(),
        rows,
        fakes: fakes_dump,
    });
    database.drop_database().await;
}

#[tokio::test]
async fn clouddrive_files_matches_the_python_transcript() {
    run_scenario("clouddrive/files").await;
}

#[tokio::test]
async fn clouddrive_offline_matches_the_python_transcript() {
    run_scenario("clouddrive/offline").await;
}

#[tokio::test]
async fn clouddrive_copies_matches_the_python_transcript() {
    run_scenario("clouddrive/copies").await;
}

#[tokio::test]
async fn clouddrive_errors_matches_the_python_transcript() {
    run_scenario("clouddrive/errors").await;
}

#[tokio::test]
async fn clouddrive_cancelled_move_matches_the_python_transcript() {
    run_scenario("clouddrive/cancelled_move").await;
}
