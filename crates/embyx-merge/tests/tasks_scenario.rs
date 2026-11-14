//! The `merge/tasks` scenario (`tools/scenarios/specs/merge/tasks.toml`): the Rust port of
//! `tools/scenarios/operations/merge.py` on `MergeTaskRepository`, compared with the Python
//! transcript, rows and `parts_json` bytes included.

use chrono::{DateTime, FixedOffset};
use embyx_core::pyjson::{Dict, Value};
use embyx_core::scenario::{self, Parts, Seeded, Spec};
use embyx_db::scenario::{dump_rows, seed, unseeded_fakes};
use embyx_db::testdb::TestDatabase;
use embyx_merge::tasks::{MergeState, MergeTaskRepository, NewTask, TaskError, TaskField};

mod common;

use common::{isoformat, task};

fn text(value: &Value) -> String {
    value.as_str().expect("a string").to_owned()
}

fn get<'a>(args: &'a Value, key: &str) -> &'a Value {
    match args {
        Value::Dict(fields) => fields.get(key).unwrap_or_else(|| panic!("no {key} in {args:?}")),
        other => panic!("not an object: {other:?}"),
    }
}

fn optional_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        other => Some(text(other)),
    }
}

/// `merge._fields`: the step's fields as the repository takes them.
fn fields(now: DateTime<FixedOffset>, args: &Value) -> Vec<TaskField> {
    let Some(Value::Dict(given)) = (match args {
        Value::Dict(entries) => entries.get("fields"),
        _ => None,
    }) else {
        return Vec::new();
    };
    given
        .iter()
        .map(|(name, value)| match name.as_str().expect("a field name") {
            "failed_state" => {
                TaskField::FailedState(optional_string(value).map(|state| MergeState::parse(&state).expect("a state")))
            }
            "job_name" => TaskField::JobName(optional_string(value)),
            "phase" => TaskField::Phase(optional_string(value)),
            "progress" => TaskField::Progress(match value {
                Value::Null => None,
                Value::Float(number) => Some(*number),
                other => panic!("progress {other:?}"),
            }),
            "merged_bytes" => TaskField::MergedBytes(int(value)),
            "merged_sha1" => TaskField::MergedSha1(optional_string(value)),
            "uploaded_bytes" => TaskField::UploadedBytes(int(value)),
            "upload_attempts" => TaskField::UploadAttempts(i32::try_from(int(value).expect("a count")).expect("small")),
            "error" => TaskField::Error(optional_string(value)),
            "notice" => TaskField::Notice(optional_string(value)),
            "finished_at" => TaskField::FinishedAt(optional_string(value).map(|text| {
                if text == "now" {
                    now.to_utc()
                } else {
                    DateTime::parse_from_rfc3339(&text).expect("ISO 8601").to_utc()
                }
            })),
            "transcoder" => TaskField::Transcoder(optional_string(value)),
            "transcode_paused" => TaskField::TranscodePaused(matches!(value, Value::Bool(true))),
            other => panic!("not a field the repository takes: {other}"),
        })
        .collect()
}

fn int(value: &Value) -> Option<i64> {
    match value {
        Value::Null => None,
        Value::Int(number) => Some(i64::try_from(*number).expect("fits i64")),
        other => panic!("not an integer: {other:?}"),
    }
}

fn id(args: &Value) -> i64 {
    int(get(args, "id")).expect("an id")
}

fn state(args: &Value, key: &str) -> MergeState {
    MergeState::parse(&text(get(args, key))).expect("a state")
}

fn failure(error: &TaskError) -> Value {
    let mut fields = Dict::new();
    let class = match error {
        TaskError::Conflict(_) => "MergeTaskConflictError",
        other => panic!("unexpected error: {other}"),
    };
    fields.insert("type", Value::Str(class.to_owned()));
    fields.insert("message", Value::Str(error.to_string()));
    Value::Dict(fields)
}

fn avids(found: impl IntoIterator<Item = String>) -> Value {
    Value::List(found.into_iter().map(Value::Str).collect())
}

/// `merge._run`.
async fn run_step(
    repository: &MergeTaskRepository,
    now: DateTime<FixedOffset>,
    op: &str,
    args: &Value,
) -> Result<Value, TaskError> {
    Ok(match op {
        "create" => {
            let new = NewTask {
                avid: text(get(args, "avid")),
                source: text(get(args, "source")),
                library_dir: text(get(args, "library_dir")),
                brand: text(get(args, "brand")),
                parts: match get(args, "parts") {
                    Value::List(parts) => parts.iter().map(text).collect(),
                    other => panic!("parts {other:?}"),
                },
                merged_name: text(get(args, "merged_name")),
            };
            task(Some(&repository.create(&new).await?))
        }
        "get" => task(repository.get(id(args)).await?.as_ref()),
        "update" => task(repository.update(id(args), fields(now, args)).await?.as_ref()),
        "transition" => task(
            repository
                .transition(
                    id(args),
                    state(args, "expected"),
                    state(args, "state"),
                    fields(now, args),
                )
                .await?
                .as_ref(),
        ),
        "open_tasks" => Value::List(
            repository
                .open_tasks()
                .await?
                .iter()
                .map(|found| task(Some(found)))
                .collect(),
        ),
        "listing" => Value::List(
            repository
                .listing()
                .await?
                .iter()
                .map(|found| task(Some(found)))
                .collect(),
        ),
        "cancelled_avids" => avids(repository.cancelled_avids().await?),
        "merged_avids" => avids(repository.merged_avids().await?),
        "last_done_at" => repository.last_done_at().await?.map_or(Value::Null, isoformat),
        other => panic!("unknown step {other:?}"),
    })
}

#[tokio::test]
async fn merge_tasks_match_the_python_transcript() {
    let spec = Spec::load("merge/tasks");
    let test = TestDatabase::create().await;
    seed(test.pool(), &spec).await;
    let now = DateTime::parse_from_rfc3339(spec.clock()).expect("clock");
    let repository = MergeTaskRepository::new(test.database());
    let mut reported = Vec::new();
    let Value::List(steps) = get(&spec.section("args"), "steps").clone() else {
        panic!("steps is a list");
    };
    for step in &steps {
        let op = text(get(step, "op"));
        let args = match step {
            Value::Dict(fields) => fields.get("args").cloned().unwrap_or(Value::Dict(Dict::new())),
            other => panic!("a step is an object: {other:?}"),
        };
        let mut entry = Dict::new();
        entry.insert("op", Value::Str(op.clone()));
        match run_step(&repository, now, &op, &args).await {
            Ok(result) => entry.insert("result", result),
            Err(error) => entry.insert("error", failure(&error)),
        }
        reported.push(Value::Dict(entry));
    }
    repository.database().close().await;
    let seeded = Seeded::new(&spec);
    let rows = dump_rows(test.pool(), &spec).await;
    let fakes = unseeded_fakes(&spec);
    scenario::assert_transcript(Parts {
        spec: &spec,
        seeded: &seeded,
        outcome: scenario::result(Value::List(reported)),
        calls: Vec::new(),
        rows,
        fakes,
    });
    test.drop_database().await;
}
