//! The fill actor's models, records and cloud-move checks against Python's answers:
//! `goldens/fill_actor/` (made by `tools/goldens/areas/fill_actor.py` from the baseline).
//!
//! `models.json` pins `model_validate_json` and `model_dump_json` byte for byte (the stored
//! plan and results, contract 2); `cloud_files.json` and `cloud_paths.json` pin the CloudDrive
//! snapshot, the move response and the `.strm` mapping; `records.json` pins the records'
//! `__post_init__` checks, the journal transition tables, every enum value and error code.

use std::collections::BTreeMap;
use std::path::PathBuf;

use chrono::{DateTime, Duration, SecondsFormat, TimeZone as _, Utc};
use embyx_core::golden;
use embyx_core::pyjson::{Dict, Value};
use embyx_fill_actor::cloud_moves::{CheckError, CloudFileMetadata, CloudMovePaths, CloudMoveResponse};
use embyx_fill_actor::errors::FillActorErrorKind;
use embyx_fill_actor::models::{
    ActorPlan, ApplyResult, ApplyState, FillActorPlan, MoveCandidate, MoveResult, MoveState, VideoPlan, VideoState,
};
use embyx_fill_actor::ports::AcquisitionOutcome;
use embyx_fill_actor::postgres_repository::cloud_file_to_json;
use embyx_fill_actor::pydatetime::{PyDateTime, lax_datetime};
use embyx_fill_actor::records::{
    ApplyJobRecord, CancelJobOutcome, CandidateKind, CandidateRecord, CloudMoveOperationRecord,
    CloudMoveOperationState, EnqueueApplyJobOutcome, FileFingerprint, JOB_CANCELLED_ERROR_CODE, JobOperation,
    JobProgress, JobProgressEvent, JobProgressUnit, JobRecord, JobStage, JobState, MoveJournalState, PlanRecord,
    RecordError, normalize_datetime, validate_cloud_move_transition, validate_journal_transition,
};

fn text(value: &Value) -> String {
    golden::string(value)
}

fn dict(value: &Value) -> &Dict {
    value
        .as_dict()
        .unwrap_or_else(|| panic!("expected an object, got {value:?}"))
}

fn error_value(class: &str, message: &str) -> Value {
    let mut error = Dict::new();
    error.insert("type", Value::from(class));
    error.insert("message", Value::from(message));
    Value::Dict(error)
}

// ---------- models.json ----------

/// `Model.model_validate_json(input).model_dump_json()`, or the error list.
fn model_round_trip(model: &str, input: &str) -> Result<String, Vec<Value>> {
    let dumped = match model {
        "FillActorPlan" => FillActorPlan::validate_json(input).map(|plan| plan.dump_json()),
        "ActorPlan" => ActorPlan::validate_json(input).map(|actor| dump(&actor.to_value())),
        "MoveCandidate" => MoveCandidate::validate_json(input).map(|candidate| dump(&candidate.to_value())),
        "VideoPlan" => VideoPlan::validate_json(input).map(|video| dump(&video.to_value())),
        "MoveResult" => MoveResult::validate_json(input).map(|result| result.dump_json()),
        "ApplyResult" => ApplyResult::validate_json(input).map(|result| result.dump_json()),
        other => panic!("no model {other}"),
    };
    dumped.map_err(|error| {
        error
            .errors
            .iter()
            .map(|detail| {
                let mut entry = Dict::new();
                entry.insert("type", Value::from(detail.kind));
                entry.insert("loc", Value::List(detail.loc.clone()));
                entry.insert("msg", Value::from(detail.msg.as_str()));
                Value::Dict(entry)
            })
            .collect()
    })
}

fn dump(value: &Value) -> String {
    embyx_core::config::dump::pydantic_json(value)
}

/// The one input pydantic accepts and the Rust models refuse: an integer past `i64`
/// (DIVERGENCES.md D114).
fn beyond_i64(input: &str) -> bool {
    input.contains("1000000000000000000000000000000")
}

#[test]
fn models_validate_and_dump_as_pydantic_does() {
    let document = golden::load("fill_actor/models.json");
    let mut checked = 0;
    for case in golden::cases(&document) {
        let model = text(golden::get(case, "model"));
        let input = text(golden::get(case, "input"));
        let got = model_round_trip(&model, &input);
        if beyond_i64(&input) {
            let errors = got.expect_err("an integer past i64 is refused");
            assert_eq!(
                errors[0].get("type"),
                Some(&Value::from("int_parsing_size")),
                "{model} {input}"
            );
            continue;
        }
        match (case.get("dump"), case.get("errors")) {
            (Some(expected), None) => assert_eq!(got, Ok(text(expected)), "{model} {input}"),
            (None, Some(expected)) => assert_eq!(got, Err(golden::list(expected).to_vec()), "{model} {input}"),
            _ => panic!("a model case has a dump or errors"),
        }
        checked += 1;
    }
    assert!(checked > 90, "{checked} model cases");
}

// ---------- cloud_files.json ----------

fn metadata_result(value: &CloudFileMetadata) -> Value {
    let mut out = Dict::new();
    out.insert("path", Value::from(value.path.as_str()));
    out.insert("file_id", Value::from(value.file_id.as_str()));
    out.insert("name", Value::from(value.name.as_str()));
    out.insert("size", Value::Int(value.size));
    out.insert("write_time", Value::Int(value.write_time));
    out.insert(
        "hashes",
        Value::List(
            value
                .hashes
                .iter()
                .map(|(kind, hash)| Value::List(vec![Value::from(kind.as_str()), Value::from(hash.as_str())]))
                .collect(),
        ),
    );
    out.insert("stored", Value::from(cloud_file_to_json(value)));
    Value::Dict(out)
}

fn check_error(error: &CheckError) -> Value {
    error_value(error.class_name(), &error.to_string())
}

fn outcome(result: Result<Value, Value>) -> (&'static str, Value) {
    match result {
        Ok(value) => ("result", value),
        Err(error) => ("error", error),
    }
}

#[test]
fn cloud_files_and_move_responses_match_python() {
    let document = golden::load("fill_actor/cloud_files.json");
    for case in golden::cases(&document) {
        match text(golden::get(case, "kind")).as_str() {
            "metadata" => {
                let input = golden::get(case, "input");
                let got = CloudFileMetadata::from_mapping(dict(input))
                    .map(|value| metadata_result(&value))
                    .map_err(|error| check_error(&error));
                let (key, value) = outcome(got);
                assert_eq!(case.get(key), Some(&value), "{input:?}");
                // What is stored reads back as the same snapshot.
                if key == "result" {
                    let stored = text(golden::get(&value, "stored"));
                    let Value::Dict(mapping) = embyx_core::pyjson::loads(&stored).unwrap() else {
                        panic!("stored is an object");
                    };
                    let again = CloudFileMetadata::from_mapping(&mapping).unwrap();
                    assert_eq!(metadata_result(&again), value);
                }
            }
            "identity" => {
                let left = CloudFileMetadata::from_mapping(dict(golden::get(case, "left"))).unwrap();
                let right = CloudFileMetadata::from_mapping(dict(golden::get(case, "right"))).unwrap();
                assert_eq!(
                    Value::Bool(left.matches_identity(&right)),
                    *golden::get(case, "matches"),
                    "{case:?}"
                );
            }
            "response" => {
                let input = golden::get(case, "input");
                let got = CloudMoveResponse::from_mapping(dict(input))
                    .map(|response| {
                        let mut out = Dict::new();
                        out.insert("success", Value::Bool(response.success));
                        out.insert("error_message", response.error_message.map_or(Value::Null, Value::from));
                        out.insert(
                            "result_paths",
                            Value::List(response.result_paths.into_iter().map(Value::from).collect()),
                        );
                        Value::Dict(out)
                    })
                    .map_err(|error| check_error(&error));
                let (key, value) = outcome(got);
                assert_eq!(case.get(key), Some(&value), "{input:?}");
            }
            other => panic!("unknown kind {other}"),
        }
    }
}

// ---------- cloud_paths.json ----------

fn strings(value: &Value) -> Vec<String> {
    golden::list(value).iter().map(text).collect()
}

fn source_index(value: &Value) -> isize {
    isize::try_from(golden::int(value)).unwrap()
}

#[test]
fn cloud_paths_and_strm_mappings_match_python() {
    let document = golden::load("fill_actor/cloud_paths.json");
    let roots: Vec<String> = ["a", "b", "c", "d"]
        .iter()
        .map(|root| format!("/cloud/library/source-{root}"))
        .collect();
    let paths = CloudMovePaths::from_values("/mounted-cloud", &roots, "/cloud/library/destination").unwrap();
    let tmp = tempfile::tempdir().unwrap();
    for case in golden::cases(&document) {
        let (key, value) = match text(golden::get(case, "kind")).as_str() {
            "from_values" => {
                let input = golden::get(case, "input");
                outcome(
                    CloudMovePaths::from_values(
                        &text(golden::get(input, "strm_mount_prefix")),
                        &strings(golden::get(input, "source_api_roots")),
                        &text(golden::get(input, "move_in_api_root")),
                    )
                    .map(|paths| {
                        let mut out = Dict::new();
                        out.insert("strm_mount_prefix", Value::from(paths.strm_mount_prefix.as_str()));
                        out.insert(
                            "source_api_roots",
                            Value::List(
                                paths
                                    .source_api_roots
                                    .iter()
                                    .map(|root| Value::from(root.as_str()))
                                    .collect(),
                            ),
                        );
                        out.insert("move_in_api_root", Value::from(paths.move_in_api_root.as_str()));
                        Value::Dict(out)
                    })
                    .map_err(|error| check_error(&error)),
                )
            }
            "parse_mapping" => {
                let label = text(golden::get(case, "label"));
                let mapping: PathBuf = tmp.path().join(format!("{label}.strm"));
                if label != "missing_file" {
                    std::fs::write(&mapping, hex::decode(text(golden::get(case, "hex"))).unwrap()).unwrap();
                }
                outcome(
                    paths
                        .parse_mapping(&mapping, source_index(golden::get(case, "source_index")))
                        .map(|parsed| {
                            let mut out = Dict::new();
                            out.insert("mapping_sha256", Value::from(parsed.mapping_sha256.as_str()));
                            out.insert("mounted_path", Value::from(parsed.mounted_path.as_str()));
                            out.insert("api_path", Value::from(parsed.api_path.as_str()));
                            Value::Dict(out)
                        })
                        .map_err(|error| check_error(&error)),
                )
            }
            "destination" => outcome(
                paths
                    .destination_directory(&text(golden::get(case, "brand")))
                    .map(Value::from)
                    .map_err(|error| check_error(&error)),
            ),
            other => panic!("unknown kind {other}"),
        };
        assert_eq!(case.get(key), Some(&value), "{case:?}");
    }
}

// ---------- records.json ----------

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 13, 10, 0, 0).unwrap()
}

fn candidate() -> CandidateRecord {
    CandidateRecord {
        kind: CandidateKind::CloudStrm,
        mapping_sha256: Some("a".repeat(64)),
        cloud_source_path: Some("/cloud/ABC/ABC-001.mp4".to_owned()),
        cloud_destination_dir: Some("/cloud/dest/ABC".to_owned()),
        cloud_file: Some(CloudFileMetadata {
            path: "/cloud/ABC/ABC-001.mp4".to_owned(),
            file_id: "f".to_owned(),
            name: "ABC-001.mp4".to_owned(),
            size: 1,
            write_time: 2,
            hashes: Vec::new(),
        }),
        ..local()
    }
}

fn local() -> CandidateRecord {
    CandidateRecord::local(
        "c-1",
        "ABC-001",
        "/mapping/ABC-001.strm",
        "/mapping",
        "/move-in/ABC/ABC-001.strm",
        FileFingerprint {
            device: 1,
            inode: 2,
            size: 3,
            mtime_ns: 4,
            ctime_ns: 5,
        },
    )
}

fn plan(candidates: &[(&str, &str)], records: &[(&str, &str)]) -> PlanRecord {
    let videos = candidates
        .iter()
        .map(|(candidate_id, video_id)| VideoPlan {
            video_id: (*video_id).to_owned(),
            actor_ids: Vec::new(),
            state: VideoState::AdditionalFound,
            release_date: None,
            existing_files: Vec::new(),
            move_candidates: vec![MoveCandidate {
                candidate_id: (*candidate_id).to_owned(),
                video_id: (*video_id).to_owned(),
                file_name: "f.mp4".to_owned(),
                source_label: "s".to_owned(),
                destination_conflict: false,
            }],
            warnings: Vec::new(),
        })
        .collect();
    PlanRecord {
        public: FillActorPlan {
            plan_id: "p".to_owned(),
            revision: "r".to_owned(),
            created_at: PyDateTime::utc(now()),
            expires_at: PyDateTime::utc(now()),
            actors: Vec::new(),
            videos,
        },
        candidates: records
            .iter()
            .map(|(candidate_id, video_id)| CandidateRecord {
                candidate_id: (*candidate_id).to_owned(),
                video_id: (*video_id).to_owned(),
                ..local()
            })
            .collect(),
        config_version: 0,
    }
}

fn job() -> JobRecord {
    JobRecord::new(
        "j",
        JobOperation::Apply,
        JobState::Queued,
        now(),
        now() + Duration::seconds(1),
        None,
        Vec::new(),
    )
}

fn apply_result(plan_id: &str, revision: &str) -> ApplyResult {
    ApplyResult {
        plan_id: plan_id.to_owned(),
        revision: revision.to_owned(),
        state: ApplyState::Succeeded,
        results: Vec::new(),
    }
}

fn apply_job(job: JobRecord, revision: &str, candidate_ids: &[&str], result: Option<ApplyResult>) -> ApplyJobRecord {
    ApplyJobRecord {
        job,
        revision: revision.to_owned(),
        candidate_ids: candidate_ids.iter().map(|id| (*id).to_owned()).collect(),
        result,
    }
}

fn operation(state: CloudMoveOperationState, error_code: Option<&str>) -> CloudMoveOperationRecord {
    CloudMoveOperationRecord {
        plan_id: "p".to_owned(),
        candidate_id: "c".to_owned(),
        attempt_id: "a".to_owned(),
        source_path: "/s".to_owned(),
        destination_dir: "/d".to_owned(),
        state,
        updated_at: now(),
        error_code: error_code.map(str::to_owned),
    }
}

fn iso(value: DateTime<Utc>) -> Value {
    Value::from(value.to_rfc3339_opts(SecondsFormat::AutoSi, false))
}

/// `_progress(job.progress)`.
fn describe_progress(progress: &JobProgress) -> Value {
    let mut out = Dict::new();
    out.insert("stage", Value::from(progress.stage.as_str()));
    out.insert("completed", Value::Int(i128::from(progress.completed)));
    out.insert(
        "total",
        progress
            .total
            .map_or(Value::Null, |total| Value::Int(i128::from(total))),
    );
    out.insert("unit", Value::from(progress.unit.as_str()));
    out.insert("current", progress.current.clone().map_or(Value::Null, Value::from));
    out.insert("stage_started_at", iso(progress.stage_started_at));
    out.insert("updated_at", iso(progress.updated_at));
    Value::Dict(out)
}

/// Every labelled constructor case: `Ok(description)` or the `ValueError` text.
#[allow(
    clippy::too_many_lines,
    reason = "one entry per golden label, in the generator's order"
)]
fn constructors() -> BTreeMap<String, Result<Value, RecordError>> {
    let ok = |result: Result<(), RecordError>| result.map(|()| Value::from("ok"));
    let mut cases: BTreeMap<String, Result<Value, RecordError>> = BTreeMap::new();
    let mut add = |label: &str, result: Result<Value, RecordError>| {
        cases.insert(label.to_owned(), result);
    };
    let cloud_file = candidate().cloud_file;
    add("candidate_cloud", ok(candidate().validate()));
    add("candidate_local", ok(local().validate()));
    add(
        "candidate_local_with_digest",
        ok(CandidateRecord {
            mapping_sha256: Some("a".repeat(64)),
            ..local()
        }
        .validate()),
    );
    add(
        "candidate_local_with_file",
        ok(CandidateRecord {
            cloud_file: cloud_file.clone(),
            ..local()
        }
        .validate()),
    );
    add(
        "candidate_cloud_without_file",
        ok(CandidateRecord {
            cloud_file: None,
            ..candidate()
        }
        .validate()),
    );
    add(
        "candidate_cloud_without_digest",
        ok(CandidateRecord {
            mapping_sha256: None,
            ..candidate()
        }
        .validate()),
    );
    add(
        "candidate_cloud_without_source",
        ok(CandidateRecord {
            cloud_source_path: None,
            ..candidate()
        }
        .validate()),
    );
    add(
        "candidate_cloud_without_destination",
        ok(CandidateRecord {
            cloud_destination_dir: None,
            ..candidate()
        }
        .validate()),
    );
    add(
        "candidate_cloud_path_mismatch",
        ok(CandidateRecord {
            cloud_source_path: Some("/cloud/other.mp4".to_owned()),
            ..candidate()
        }
        .validate()),
    );
    for (label, digest) in [
        ("candidate_cloud_short_digest", "a".repeat(63)),
        ("candidate_cloud_long_digest", "a".repeat(65)),
        ("candidate_cloud_non_hex_digest", "z".repeat(64)),
    ] {
        add(
            label,
            ok(CandidateRecord {
                mapping_sha256: Some(digest),
                ..candidate()
            }
            .validate()),
        );
    }
    add(
        "plan_matching",
        ok(plan(&[("c-1", "V-1"), ("c-2", "V-2")], &[("c-2", "V-2"), ("c-1", "V-1")]).validate()),
    );
    add("plan_empty", ok(plan(&[], &[]).validate()));
    add("plan_missing_record", ok(plan(&[("c-1", "V-1")], &[]).validate()));
    add("plan_extra_record", ok(plan(&[], &[("c-1", "V-1")]).validate()));
    add(
        "plan_video_mismatch",
        ok(plan(&[("c-1", "V-1")], &[("c-1", "V-2")]).validate()),
    );
    add(
        "plan_duplicate_records",
        ok(plan(&[("c-1", "V-1")], &[("c-1", "V-1"), ("c-1", "V-1")]).validate()),
    );
    add(
        "plan_duplicate_public",
        ok(plan(&[("c-1", "V-1"), ("c-1", "V-1")], &[("c-1", "V-1")]).validate()),
    );
    for (completed, total) in [
        (0, None),
        (3, None),
        (0, Some(0)),
        (2, Some(3)),
        (3, Some(3)),
        (4, Some(3)),
        (-1, None),
        (0, Some(-1)),
        (-1, Some(-2)),
    ] {
        let total_text = total.map_or_else(|| "None".to_owned(), |total: i64| total.to_string());
        let progress = JobProgress {
            stage: JobStage::LibraryScan,
            completed,
            total,
            unit: JobProgressUnit::Videos,
            current: None,
            stage_started_at: now(),
            updated_at: now(),
        };
        add(&format!("progress_{completed}_{total_text}"), ok(progress.validate()));
        let event = JobProgressEvent {
            stage: JobStage::LibraryScan,
            completed,
            total,
            unit: JobProgressUnit::Videos,
            current: None,
        };
        add(
            &format!("progress_event_{completed}_{total_text}"),
            ok(event.validate()),
        );
    }
    add("job_no_actors", Ok(describe_progress(&job().progress)));
    let with_actors = JobRecord::new(
        "j",
        JobOperation::Apply,
        JobState::Queued,
        now(),
        now() + Duration::seconds(1),
        None,
        ["a", "b", "a"].iter().map(|id| (*id).to_owned()).collect(),
    );
    add("job_actors", Ok(describe_progress(&with_actors.progress)));
    let explicit = JobProgress {
        stage: JobStage::Done,
        completed: 1,
        total: Some(1),
        unit: JobProgressUnit::Steps,
        current: Some("x".to_owned()),
        stage_started_at: now() + Duration::hours(1),
        updated_at: now() + Duration::hours(2),
    };
    add(
        "job_explicit_progress",
        explicit.validate().map(|()| describe_progress(&explicit)),
    );
    let completed_job = JobRecord {
        state: JobState::Completed,
        plan_id: Some("p".to_owned()),
        ..job()
    };
    add("apply_job", ok(apply_job(job(), "r", &["c-1", "c-2"], None).validate()));
    add(
        "apply_job_empty_candidates",
        ok(apply_job(job(), "r", &[], None).validate()),
    );
    add(
        "apply_job_create_plan",
        ok(apply_job(
            JobRecord {
                operation: JobOperation::CreatePlan,
                ..job()
            },
            "r",
            &[],
            None,
        )
        .validate()),
    );
    add(
        "apply_job_empty_revision",
        ok(apply_job(job(), "", &[], None).validate()),
    );
    add(
        "apply_job_duplicate_candidates",
        ok(apply_job(job(), "r", &["c", "c"], None).validate()),
    );
    add(
        "apply_job_result",
        ok(apply_job(completed_job.clone(), "r", &[], Some(apply_result("p", "r"))).validate()),
    );
    add(
        "apply_job_result_without_plan",
        ok(apply_job(
            JobRecord {
                state: JobState::Completed,
                ..job()
            },
            "r",
            &[],
            Some(apply_result("other", "r")),
        )
        .validate()),
    );
    add(
        "apply_job_result_not_completed",
        ok(apply_job(
            JobRecord {
                state: JobState::Failed,
                ..job()
            },
            "r",
            &[],
            Some(apply_result("p", "r")),
        )
        .validate()),
    );
    add(
        "apply_job_result_revision",
        ok(apply_job(completed_job.clone(), "r", &[], Some(apply_result("p", "x"))).validate()),
    );
    add(
        "apply_job_result_plan",
        ok(apply_job(completed_job, "r", &[], Some(apply_result("other", "r"))).validate()),
    );
    for state in CloudMoveOperationState::ALL {
        for error_code in [None, Some("cloud_move_failed"), Some("")] {
            let label = format!("operation_{}_{}", state.as_str(), error_code.unwrap_or("None"));
            add(&label, ok(operation(*state, error_code).validate()));
        }
    }
    for field in ["attempt_id", "source_path", "destination_dir"] {
        let mut record = operation(CloudMoveOperationState::Failed, None);
        match field {
            "attempt_id" => record.attempt_id.clear(),
            "source_path" => record.source_path.clear(),
            _ => record.destination_dir.clear(),
        }
        add(&format!("operation_empty_{field}"), ok(record.validate()));
    }
    cases
}

fn enum_values<T: Copy>(all: &[T], value: impl Fn(T) -> &'static str) -> Vec<Value> {
    all.iter().map(|member| Value::from(value(*member))).collect()
}

fn values() -> BTreeMap<&'static str, Vec<Value>> {
    let mut out = BTreeMap::new();
    out.insert("VideoState", enum_values(VideoState::ALL, VideoState::as_str));
    out.insert("MoveState", enum_values(MoveState::ALL, MoveState::as_str));
    out.insert("ApplyState", enum_values(ApplyState::ALL, ApplyState::as_str));
    out.insert("JobState", enum_values(JobState::ALL, JobState::as_str));
    out.insert(
        "CancelJobOutcome",
        enum_values(CancelJobOutcome::ALL, CancelJobOutcome::as_str),
    );
    out.insert(
        "EnqueueApplyJobOutcome",
        enum_values(EnqueueApplyJobOutcome::ALL, EnqueueApplyJobOutcome::as_str),
    );
    out.insert("JobOperation", enum_values(JobOperation::ALL, JobOperation::as_str));
    out.insert("JobStage", enum_values(JobStage::ALL, JobStage::as_str));
    out.insert(
        "JobProgressUnit",
        enum_values(JobProgressUnit::ALL, JobProgressUnit::as_str),
    );
    out.insert(
        "MoveJournalState",
        enum_values(MoveJournalState::ALL, MoveJournalState::as_str),
    );
    out.insert("CandidateKind", enum_values(CandidateKind::ALL, CandidateKind::as_str));
    out.insert(
        "CloudMoveOperationState",
        enum_values(CloudMoveOperationState::ALL, CloudMoveOperationState::as_str),
    );
    out.insert(
        "AcquisitionOutcome",
        enum_values(AcquisitionOutcome::ALL, AcquisitionOutcome::as_str),
    );
    out.insert(
        "CloudMoveOperationState.terminal",
        CloudMoveOperationState::ALL
            .iter()
            .filter(|state| state.terminal())
            .map(|state| Value::from(state.as_str()))
            .collect(),
    );
    out.insert(
        "error_codes",
        FillActorErrorKind::ALL
            .iter()
            .map(|kind| Value::List(vec![Value::from(kind.class_name()), Value::from(kind.code())]))
            .collect(),
    );
    out.insert("JOB_CANCELLED_ERROR_CODE", vec![Value::from(JOB_CANCELLED_ERROR_CODE)]);
    out
}

fn journal_state(value: &Value) -> Option<MoveJournalState> {
    value
        .as_str()
        .map(|text| MoveJournalState::parse(text).expect("a journal state"))
}

fn cloud_state(value: &Value) -> Option<CloudMoveOperationState> {
    value
        .as_str()
        .map(|text| CloudMoveOperationState::parse(text).expect("a cloud move state"))
}

#[test]
fn records_match_python() {
    let document = golden::load("fill_actor/records.json");
    let constructors = constructors();
    let values = values();
    let mut seen = 0;
    for section in golden::cases(&document) {
        let cases = golden::list(golden::get(section, "cases"));
        match text(golden::get(section, "kind")).as_str() {
            "constructors" => {
                for case in cases {
                    let label = text(golden::get(case, "label"));
                    let got = constructors
                        .get(&label)
                        .unwrap_or_else(|| panic!("no Rust case {label}"))
                        .clone()
                        .map_err(|error| error_value("ValueError", error.0));
                    let (key, value) = outcome(got);
                    assert_eq!(case.get(key), Some(&value), "{label}");
                    seen += 1;
                }
                assert_eq!(seen, constructors.len(), "every Rust case has a golden");
            }
            "transitions" => {
                for case in cases {
                    let current = golden::get(case, "current");
                    let requested = golden::get(case, "requested");
                    let got = match text(golden::get(case, "table")).as_str() {
                        "journal" => {
                            validate_journal_transition(journal_state(current), journal_state(requested).unwrap())
                        }
                        _ => validate_cloud_move_transition(cloud_state(current), cloud_state(requested).unwrap()),
                    };
                    assert_eq!(Value::Bool(got.is_ok()), *golden::get(case, "allowed"), "{case:?}");
                    if let Err(error) = got {
                        assert_eq!(
                            golden::get(case, "error"),
                            &error_value(error.class_name(), &error.to_string()),
                            "{case:?}"
                        );
                    }
                }
            }
            "values" => {
                for case in cases {
                    let name = text(golden::get(case, "enum"));
                    let expected = golden::list(golden::get(case, "values"));
                    assert_eq!(values.get(name.as_str()).map(Vec::as_slice), Some(expected), "{name}");
                }
                assert_eq!(cases.len(), values.len());
            }
            "normalize_datetime" => {
                for case in cases {
                    let input = lax_datetime(golden::get(case, "input")).expect("a datetime");
                    assert_eq!(
                        iso(normalize_datetime(input.to_utc())),
                        *golden::get(case, "normalized"),
                        "{case:?}"
                    );
                }
            }
            other => panic!("unknown kind {other}"),
        }
    }
}
