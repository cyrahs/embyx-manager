//! The `fill_actor/repository` scenario (`tools/scenarios/specs/fill_actor/repository.toml`):
//! the Rust port of `tools/scenarios/operations/fill_actor.py` on
//! `PostgresFillActorRepository`, compared with the Python transcript, rows included. The
//! spec seeds rows as the Python build wrote them, so this also reads (and carries on from)
//! the other build's rows; equal rows mean each build can resume the other's.

use chrono::{DateTime, Utc};
use embyx_core::pyjson::{Dict, Value, py_isoformat_datetime};
use embyx_core::scenario::{self, Parts, Seeded, Spec};
use embyx_db::scenario::{dump_rows, seed, unseeded_fakes};
use embyx_db::testdb::TestDatabase;
use embyx_fill_actor::cloud_moves::CloudFileMetadata;
use embyx_fill_actor::models::{ApplyResult, FillActorPlan, MoveResult};
use embyx_fill_actor::postgres_repository::PostgresFillActorRepository;
use embyx_fill_actor::records::{
    ApplyJobRecord, CancelJobResult, CandidateKind, CandidateRecord, CloudMoveOperationRecord, CloudMoveOperationState,
    EnqueueApplyJobResult, FileFingerprint, JobOperation, JobProgress, JobProgressUnit, JobRecord, JobStage, JobState,
    MoveJournalRecord, MoveJournalState, PlanRecord,
};

/// A step's failure as the transcript reports it.
struct Failure {
    class: String,
    message: String,
}

impl<E: std::fmt::Display + ClassName> From<E> for Failure {
    fn from(error: E) -> Self {
        Self {
            class: error.class().to_owned(),
            message: error.to_string(),
        }
    }
}

trait ClassName {
    fn class(&self) -> &str;
}

impl ClassName for embyx_fill_actor::postgres_repository::RepositoryError {
    fn class(&self) -> &str {
        self.class_name()
    }
}

impl ClassName for embyx_core::config::lax::ValidationError {
    fn class(&self) -> &str {
        Self::CLASS
    }
}

impl ClassName for embyx_fill_actor::cloud_moves::CheckError {
    fn class(&self) -> &str {
        self.class_name()
    }
}

// ---------- reading the arguments ----------

fn get<'a>(args: &'a Value, key: &str) -> &'a Value {
    args.get(key).unwrap_or_else(|| panic!("no {key} in {args:?}"))
}

fn text(value: &Value) -> String {
    value
        .as_str()
        .unwrap_or_else(|| panic!("not a string: {value:?}"))
        .to_owned()
}

fn optional_text(args: &Value, key: &str) -> Option<String> {
    match args.get(key) {
        None | Some(Value::Null) => None,
        Some(value) => Some(text(value)),
    }
}

fn int(value: &Value) -> i64 {
    match value {
        Value::Int(number) => i64::try_from(*number).expect("fits i64"),
        other => panic!("not an integer: {other:?}"),
    }
}

/// `datetime.fromisoformat(text)` for the aware texts of the spec.
fn time(value: &Value) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(&text(value)).expect("ISO 8601").to_utc()
}

fn texts(value: &Value) -> Vec<String> {
    match value {
        Value::List(items) => items.iter().map(text).collect(),
        other => panic!("not a list: {other:?}"),
    }
}

fn candidate(args: &Value) -> Result<CandidateRecord, Failure> {
    let fingerprint = get(args, "fingerprint");
    let mut record = CandidateRecord::local(
        text(get(args, "candidate_id")),
        text(get(args, "video_id")),
        text(get(args, "source")),
        text(get(args, "source_root")),
        text(get(args, "destination")),
        FileFingerprint {
            device: int(get(fingerprint, "device")),
            inode: int(get(fingerprint, "inode")),
            size: int(get(fingerprint, "size")),
            mtime_ns: int(get(fingerprint, "mtime_ns")),
            ctime_ns: int(get(fingerprint, "ctime_ns")),
        },
    );
    record.kind = CandidateKind::parse(&optional_text(args, "kind").unwrap_or_else(|| "local_file".to_owned()))
        .expect("a candidate kind");
    record.mapping_sha256 = optional_text(args, "mapping_sha256");
    record.cloud_source_path = optional_text(args, "cloud_source_path");
    record.cloud_destination_dir = optional_text(args, "cloud_destination_dir");
    record.cloud_file = match args.get("cloud_file") {
        None | Some(Value::Null) => None,
        Some(Value::Dict(mapping)) => Some(CloudFileMetadata::from_mapping(mapping)?),
        Some(other) => panic!("cloud_file {other:?}"),
    };
    Ok(record)
}

fn plan(args: &Value) -> Result<PlanRecord, Failure> {
    let candidates = match get(args, "candidates") {
        Value::List(items) => items.iter().map(candidate).collect::<Result<Vec<_>, _>>()?,
        other => panic!("candidates {other:?}"),
    };
    Ok(PlanRecord {
        public: FillActorPlan::validate_json(&text(get(args, "public")))?,
        candidates,
        config_version: args
            .get("config_version")
            .map_or(0, |value| i32::try_from(int(value)).expect("i32")),
    })
}

fn progress(args: &Value) -> JobProgress {
    JobProgress {
        stage: JobStage::parse(&text(get(args, "stage"))).expect("a stage"),
        completed: int(get(args, "completed")),
        total: match get(args, "total") {
            Value::Null => None,
            value => Some(int(value)),
        },
        unit: JobProgressUnit::parse(&text(get(args, "unit"))).expect("a unit"),
        current: optional_text(args, "current"),
        stage_started_at: time(get(args, "stage_started_at")),
        updated_at: time(get(args, "updated_at")),
    }
}

fn job(args: &Value) -> JobRecord {
    let mut record = JobRecord::new(
        text(get(args, "job_id")),
        JobOperation::parse(&text(get(args, "operation"))).expect("an operation"),
        JobState::parse(&text(get(args, "state"))).expect("a state"),
        time(get(args, "created_at")),
        time(get(args, "updated_at")),
        optional_text(args, "plan_id"),
        args.get("actor_ids").map(texts).unwrap_or_default(),
    );
    record.error_code = optional_text(args, "error_code");
    record.owner_id = optional_text(args, "owner_id");
    record.lease_expires_at = args.get("lease_expires_at").map(time);
    if let Some(given) = args.get("progress") {
        record.progress = progress(given);
    }
    record
}

fn apply_result(args: &Value, key: &str) -> Result<Option<ApplyResult>, Failure> {
    Ok(match args.get(key) {
        None | Some(Value::Null) => None,
        Some(value) => Some(ApplyResult::validate_json(&text(value))?),
    })
}

fn apply_job(args: &Value) -> Result<ApplyJobRecord, Failure> {
    Ok(ApplyJobRecord {
        job: job(get(args, "job")),
        revision: text(get(args, "revision")),
        candidate_ids: texts(get(args, "candidate_ids")),
        result: apply_result(args, "result")?,
    })
}

fn journal(args: &Value) -> MoveJournalRecord {
    MoveJournalRecord {
        plan_id: text(get(args, "plan_id")),
        candidate_id: text(get(args, "candidate_id")),
        state: MoveJournalState::parse(&text(get(args, "state"))).expect("a journal state"),
        updated_at: time(get(args, "updated_at")),
    }
}

fn operation(args: &Value) -> CloudMoveOperationRecord {
    CloudMoveOperationRecord {
        plan_id: text(get(args, "plan_id")),
        candidate_id: text(get(args, "candidate_id")),
        attempt_id: text(get(args, "attempt_id")),
        source_path: text(get(args, "source_path")),
        destination_dir: text(get(args, "destination_dir")),
        state: CloudMoveOperationState::parse(&text(get(args, "state"))).expect("a cloud move state"),
        updated_at: time(get(args, "updated_at")),
        error_code: optional_text(args, "error_code"),
    }
}

fn move_result(args: &Value) -> Result<MoveResult, Failure> {
    Ok(MoveResult::validate_json(&text(get(args, "result")))?)
}

// ---------- reporting (`fill_actor._report`) ----------

fn object(entries: Vec<(&str, Value)>) -> Value {
    let mut dict = Dict::new();
    for (key, value) in entries {
        dict.insert(key, value);
    }
    Value::Dict(dict)
}

fn iso(value: DateTime<Utc>) -> Value {
    Value::Str(py_isoformat_datetime(&value.fixed_offset()))
}

fn option<T>(value: Option<T>, report: impl FnOnce(T) -> Value) -> Value {
    value.map_or(Value::Null, report)
}

fn string(value: &str) -> Value {
    Value::from(value)
}

fn strings(values: &[String]) -> Value {
    Value::List(values.iter().map(|value| string(value)).collect())
}

fn report_cloud_file(value: &CloudFileMetadata) -> Value {
    object(vec![
        ("path", string(&value.path)),
        ("file_id", string(&value.file_id)),
        ("name", string(&value.name)),
        ("size", Value::Int(value.size)),
        ("write_time", Value::Int(value.write_time)),
        (
            "hashes",
            Value::List(
                value
                    .hashes
                    .iter()
                    .map(|(kind, hash)| Value::List(vec![string(kind), string(hash)]))
                    .collect(),
            ),
        ),
    ])
}

fn report_candidate(value: &CandidateRecord) -> Value {
    let path = |path: &std::path::Path| string(path.to_str().expect("UTF-8"));
    let fingerprint = &value.fingerprint;
    object(vec![
        ("candidate_id", string(&value.candidate_id)),
        ("video_id", string(&value.video_id)),
        ("source", path(&value.source)),
        ("source_root", path(&value.source_root)),
        ("destination", path(&value.destination)),
        (
            "fingerprint",
            object(vec![
                ("device", Value::Int(fingerprint.device.into())),
                ("inode", Value::Int(fingerprint.inode.into())),
                ("size", Value::Int(fingerprint.size.into())),
                ("mtime_ns", Value::Int(fingerprint.mtime_ns.into())),
                ("ctime_ns", Value::Int(fingerprint.ctime_ns.into())),
            ]),
        ),
        ("kind", string(value.kind.as_str())),
        ("mapping_sha256", option(value.mapping_sha256.as_deref(), string)),
        ("cloud_source_path", option(value.cloud_source_path.as_deref(), string)),
        (
            "cloud_destination_dir",
            option(value.cloud_destination_dir.as_deref(), string),
        ),
        ("cloud_file", option(value.cloud_file.as_ref(), report_cloud_file)),
    ])
}

fn report_plan(value: &PlanRecord) -> Value {
    object(vec![
        ("public", Value::Str(value.public.dump_json())),
        (
            "candidates",
            Value::List(value.candidates.iter().map(report_candidate).collect()),
        ),
        ("config_version", Value::Int(value.config_version.into())),
    ])
}

fn report_progress(value: &JobProgress) -> Value {
    object(vec![
        ("stage", string(value.stage.as_str())),
        ("completed", Value::Int(value.completed.into())),
        ("total", option(value.total, |total| Value::Int(total.into()))),
        ("unit", string(value.unit.as_str())),
        ("current", option(value.current.as_deref(), string)),
        ("stage_started_at", iso(value.stage_started_at)),
        ("updated_at", iso(value.updated_at)),
    ])
}

fn report_job(value: &JobRecord) -> Value {
    object(vec![
        ("job_id", string(&value.job_id)),
        ("operation", string(value.operation.as_str())),
        ("state", string(value.state.as_str())),
        ("created_at", iso(value.created_at)),
        ("updated_at", iso(value.updated_at)),
        ("plan_id", option(value.plan_id.as_deref(), string)),
        ("error_code", option(value.error_code.as_deref(), string)),
        ("owner_id", option(value.owner_id.as_deref(), string)),
        ("lease_expires_at", option(value.lease_expires_at, iso)),
        ("actor_ids", strings(&value.actor_ids)),
        ("progress", report_progress(&value.progress)),
    ])
}

fn report_apply_job(value: &ApplyJobRecord) -> Value {
    object(vec![
        ("job", report_job(&value.job)),
        ("revision", string(&value.revision)),
        ("candidate_ids", strings(&value.candidate_ids)),
        (
            "result",
            option(value.result.as_ref(), |result| Value::Str(result.dump_json())),
        ),
    ])
}

fn report_enqueued(value: &EnqueueApplyJobResult) -> Value {
    object(vec![
        ("outcome", string(value.outcome.as_str())),
        ("record", option(value.record.as_ref(), report_apply_job)),
    ])
}

fn report_cancelled(value: &CancelJobResult) -> Value {
    object(vec![
        ("outcome", string(value.outcome.as_str())),
        ("job", option(value.job.as_ref(), report_job)),
        (
            "previous_state",
            option(value.previous_state, |state| string(state.as_str())),
        ),
        ("previous_owner_id", option(value.previous_owner_id.as_deref(), string)),
    ])
}

fn report_journal(value: &MoveJournalRecord) -> Value {
    object(vec![
        ("plan_id", string(&value.plan_id)),
        ("candidate_id", string(&value.candidate_id)),
        ("state", string(value.state.as_str())),
        ("updated_at", iso(value.updated_at)),
    ])
}

fn report_operation(value: &CloudMoveOperationRecord) -> Value {
    object(vec![
        ("plan_id", string(&value.plan_id)),
        ("candidate_id", string(&value.candidate_id)),
        ("attempt_id", string(&value.attempt_id)),
        ("source_path", string(&value.source_path)),
        ("destination_dir", string(&value.destination_dir)),
        ("state", string(value.state.as_str())),
        ("updated_at", iso(value.updated_at)),
        ("error_code", option(value.error_code.as_deref(), string)),
    ])
}

fn report_move_result(value: &MoveResult) -> Value {
    Value::Str(value.dump_json())
}

fn list<T>(values: &[T], report: impl Fn(&T) -> Value) -> Value {
    Value::List(values.iter().map(report).collect())
}

fn int_value(value: usize) -> Value {
    Value::Int(i128::try_from(value).expect("fits"))
}

// ---------- the steps (`fill_actor._run`) ----------

#[allow(
    clippy::too_many_lines,
    reason = "one arm per repository method, as the Python operation has"
)]
async fn run_step(repository: &PostgresFillActorRepository, op: &str, args: &Value) -> Result<Value, Failure> {
    let id = |key: &str| text(get(args, key));
    Ok(match op {
        "save_plan" => {
            repository.save_plan(&plan(get(args, "plan"))?).await?;
            Value::Null
        }
        "get_plan" => option(repository.get_plan(&id("plan_id")).await?.as_ref(), report_plan),
        "get_candidate" => option(
            repository
                .get_candidate(&id("plan_id"), &id("candidate_id"))
                .await?
                .as_ref(),
            report_candidate,
        ),
        "delete_plan" => Value::Bool(repository.delete_plan(&id("plan_id")).await?),
        "purge_expired_plans" => int_value(repository.purge_expired_plans(time(get(args, "now"))).await?),
        "save_move_result" => {
            repository.save_move_result(&id("plan_id"), &move_result(args)?).await?;
            Value::Null
        }
        "get_move_result" => option(
            repository
                .get_move_result(&id("plan_id"), &id("candidate_id"))
                .await?
                .as_ref(),
            report_move_result,
        ),
        "list_move_results" => list(&repository.list_move_results(&id("plan_id")).await?, report_move_result),
        "save_job" => {
            repository.save_job(&job(get(args, "job"))).await?;
            Value::Null
        }
        "enqueue_job" => Value::Bool(
            repository
                .enqueue_job(&job(get(args, "job")), int(get(args, "max_active")))
                .await?,
        ),
        "enqueue_apply_job" => report_enqueued(
            &repository
                .enqueue_apply_job(&apply_job(get(args, "record"))?, int(get(args, "max_active")))
                .await?,
        ),
        "get_apply_job" => option(
            repository.get_apply_job(&id("job_id")).await?.as_ref(),
            report_apply_job,
        ),
        "claim_next_job" => option(
            repository
                .claim_next_job(
                    &id("owner_id"),
                    time(get(args, "now")),
                    time(get(args, "lease_expires_at")),
                )
                .await?
                .as_ref(),
            report_job,
        ),
        "renew_owned_job_lease" => Value::Bool(
            repository
                .renew_owned_job_lease(
                    &id("job_id"),
                    &id("owner_id"),
                    time(get(args, "now")),
                    time(get(args, "lease_expires_at")),
                )
                .await?,
        ),
        "update_owned_job_progress" => Value::Bool(
            repository
                .update_owned_job_progress(
                    &id("job_id"),
                    &id("owner_id"),
                    &progress(get(args, "progress")),
                    time(get(args, "now")),
                )
                .await?,
        ),
        "finish_owned_job" => {
            let result = apply_result(args, "apply_result")?;
            Value::Bool(
                repository
                    .finish_owned_job(
                        &id("job_id"),
                        &id("owner_id"),
                        JobState::parse(&id("state")).expect("a state"),
                        optional_text(args, "error_code").as_deref(),
                        time(get(args, "now")),
                        &progress(get(args, "progress")),
                        result.as_ref(),
                    )
                    .await?,
            )
        }
        "cancel_job" => report_cancelled(&repository.cancel_job(&id("job_id"), time(get(args, "now"))).await?),
        "fail_expired_jobs" => int_value(
            repository
                .fail_expired_jobs(time(get(args, "now")), &id("error_code"))
                .await?,
        ),
        "get_job" => option(repository.get_job(&id("job_id")).await?.as_ref(), report_job),
        "list_jobs" => {
            let states: Option<Vec<JobState>> = args.get("states").map(|states| {
                texts(states)
                    .iter()
                    .map(|state| JobState::parse(state).expect("a state"))
                    .collect()
            });
            list(&repository.list_jobs(states.as_deref()).await?, report_job)
        }
        "save_move_journal" => {
            repository.save_move_journal(&journal(get(args, "journal"))).await?;
            Value::Null
        }
        "get_move_journal" => option(
            repository
                .get_move_journal(&id("plan_id"), &id("candidate_id"))
                .await?
                .as_ref(),
            report_journal,
        ),
        "list_unreconciled_moves" => list(&repository.list_unreconciled_moves().await?, report_journal),
        "save_cloud_move_operation" => {
            repository
                .save_cloud_move_operation(&operation(get(args, "operation")))
                .await?;
            Value::Null
        }
        "get_cloud_move_operation" => option(
            repository
                .get_cloud_move_operation(&id("plan_id"), &id("candidate_id"))
                .await?
                .as_ref(),
            report_operation,
        ),
        "list_unresolved_cloud_moves" => list(&repository.list_unresolved_cloud_moves().await?, report_operation),
        "finalize_cloud_move" => {
            repository
                .finalize_cloud_move(&operation(get(args, "operation")), &move_result(args)?)
                .await?;
            Value::Null
        }
        other => panic!("unknown step {other:?}"),
    })
}

#[tokio::test]
async fn fill_actor_repository_matches_the_python_transcript() {
    let spec = Spec::load("fill_actor/repository");
    let test = TestDatabase::create().await;
    seed(test.pool(), &spec).await;
    let repository = PostgresFillActorRepository::new(test.database());
    let mut reported = Vec::new();
    let Value::List(steps) = get(&spec.section("args"), "steps").clone() else {
        panic!("steps is a list");
    };
    for step in &steps {
        let op = text(get(step, "op"));
        let args = step.get("args").cloned().unwrap_or(Value::Dict(Dict::new()));
        let mut entry = Dict::new();
        entry.insert("op", Value::Str(op.clone()));
        match run_step(&repository, &op, &args).await {
            Ok(result) => entry.insert("result", result),
            Err(failure) => entry.insert(
                "error",
                object(vec![
                    ("type", Value::Str(failure.class)),
                    ("message", Value::Str(failure.message)),
                ]),
            ),
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
