//! The fill actor's public models: the plan a scan produces and the results of applying it.
//!
//! Python origin: `src/embyx_manager/fill_actor/models.py` (frozen pydantic models). The
//! repository stores a plan as `model_dump_json()` in `fill_actor_plans.public_json` and a
//! move or apply result in `result_json`; [`FillActorPlan::dump_json`] and friends write the
//! same bytes (cross-version contract 2, `goldens/fill_actor/models.json`), and
//! `validate_json` reads them back with pydantic's lax JSON rules and errors
//! (`extra='ignore'`, the default).

use chrono::NaiveDate;
use embyx_core::config::dump::pydantic_json;
use embyx_core::config::lax::{self, ErrorDetail, Mode, Recorded, Refusal, ValidationError, Validator};
use embyx_core::pyjson::{Dict, Value};

use crate::pydatetime::{PyDateTime, date_text, lax_date, lax_datetime};

/// A `StrEnum` of the models and records: `.value`, `Enum(value)`, every member in order.
macro_rules! str_enum {
    ($(#[$meta:meta])* $name:ident { $($(#[$vmeta:meta])* $variant:ident = $value:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name {
            $($(#[$vmeta])* $variant),+
        }

        impl $name {
            /// Every member, in declaration order (`list(Enum)`).
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// `.value`.
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }

            /// `Enum(value)`; `None` where Python raises `ValueError`.
            pub fn parse(value: &str) -> Option<Self> {
                match value {
                    $($value => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

pub(crate) use str_enum;

str_enum! {
    /// `VideoState`.
    VideoState {
        Exists = "exists",
        AdditionalFound = "additional_found",
        /// Legacy: plans persisted before scans submitted to the download tracker.
        MagnetFound = "magnet_found",
        /// Recorded in the acquisition ledger; the tracker resolves and submits in the
        /// background, so the dashboard is where its progress shows up.
        Queued = "queued",
        /// Legacy: plans persisted while scans still resolved and submitted inline.
        Submitted = "submitted",
        AlreadyTracked = "already_tracked",
        SubmitFailed = "submit_failed",
        /// Legacy companion of `Submitted`: no magnet found during the inline resolve.
        Missing = "missing",
        InvalidVideoId = "invalid_video_id",
        ScanFailed = "scan_failed",
    }
}

str_enum! {
    /// `MoveState`.
    MoveState {
        Moved = "moved",
        Stale = "stale",
        Conflict = "conflict",
        InvalidPath = "invalid_path",
        Failed = "failed",
    }
}

str_enum! {
    /// `ApplyState`.
    ApplyState {
        Succeeded = "succeeded",
        PartialFailed = "partial_failed",
        Failed = "failed",
    }
}

/// `ActorPlan`.
#[derive(Debug, Clone, PartialEq)]
pub struct ActorPlan {
    pub actor_id: String,
    /// `Field(ge=0)`.
    pub scraped_count: i64,
    pub video_ids: Vec<String>,
    pub error_code: Option<String>,
    /// The catalog's identity for the actor; absent on plans from before the catalog knew
    /// names.
    pub actor_name: Option<String>,
    pub talent_id: Option<i64>,
    pub aliases: Vec<String>,
    /// Redacted, truncated diagnostic text; optional so plans stored before it existed
    /// still load.
    pub error_message: Option<String>,
}

/// `MoveCandidate`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveCandidate {
    pub candidate_id: String,
    pub video_id: String,
    pub file_name: String,
    pub source_label: String,
    pub destination_conflict: bool,
}

/// `VideoPlan`.
#[derive(Debug, Clone, PartialEq)]
pub struct VideoPlan {
    pub video_id: String,
    pub actor_ids: Vec<String>,
    pub state: VideoState,
    /// When the catalog dated the release; anchors the ledger's resolve schedule.
    pub release_date: Option<NaiveDate>,
    pub existing_files: Vec<String>,
    pub move_candidates: Vec<MoveCandidate>,
    pub warnings: Vec<String>,
}

/// `FillActorPlan`.
#[derive(Debug, Clone, PartialEq)]
pub struct FillActorPlan {
    pub plan_id: String,
    pub revision: String,
    pub created_at: PyDateTime,
    pub expires_at: PyDateTime,
    pub actors: Vec<ActorPlan>,
    pub videos: Vec<VideoPlan>,
}

/// `MoveResult`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveResult {
    pub candidate_id: String,
    pub video_id: String,
    pub file_name: String,
    pub state: MoveState,
    pub error_code: Option<String>,
}

/// `ApplyResult`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyResult {
    pub plan_id: String,
    pub revision: String,
    pub state: ApplyState,
    pub results: Vec<MoveResult>,
}

// ---------- model_dump ----------

fn object(entries: Vec<(&str, Value)>) -> Value {
    let mut dict = Dict::new();
    for (key, value) in entries {
        dict.insert(key, value);
    }
    Value::Dict(dict)
}

fn text(value: &str) -> Value {
    Value::Str(value.to_owned())
}

fn optional_text(value: Option<&String>) -> Value {
    value.map_or(Value::Null, |text| Value::Str(text.clone()))
}

fn texts(values: &[String]) -> Value {
    Value::List(values.iter().map(|value| Value::Str(value.clone())).collect())
}

impl ActorPlan {
    /// `model_dump(mode='json')`, fields in declaration order.
    pub fn to_value(&self) -> Value {
        object(vec![
            ("actor_id", text(&self.actor_id)),
            ("scraped_count", Value::from(self.scraped_count)),
            ("video_ids", texts(&self.video_ids)),
            ("error_code", optional_text(self.error_code.as_ref())),
            ("actor_name", optional_text(self.actor_name.as_ref())),
            ("talent_id", self.talent_id.map_or(Value::Null, Value::from)),
            ("aliases", texts(&self.aliases)),
            ("error_message", optional_text(self.error_message.as_ref())),
        ])
    }
}

impl MoveCandidate {
    pub fn to_value(&self) -> Value {
        object(vec![
            ("candidate_id", text(&self.candidate_id)),
            ("video_id", text(&self.video_id)),
            ("file_name", text(&self.file_name)),
            ("source_label", text(&self.source_label)),
            ("destination_conflict", Value::Bool(self.destination_conflict)),
        ])
    }
}

impl VideoPlan {
    pub fn to_value(&self) -> Value {
        object(vec![
            ("video_id", text(&self.video_id)),
            ("actor_ids", texts(&self.actor_ids)),
            ("state", text(self.state.as_str())),
            (
                "release_date",
                self.release_date
                    .map_or(Value::Null, |date| Value::Str(date_text(date))),
            ),
            ("existing_files", texts(&self.existing_files)),
            (
                "move_candidates",
                Value::List(self.move_candidates.iter().map(MoveCandidate::to_value).collect()),
            ),
            ("warnings", texts(&self.warnings)),
        ])
    }
}

impl FillActorPlan {
    pub fn to_value(&self) -> Value {
        object(vec![
            ("plan_id", text(&self.plan_id)),
            ("revision", text(&self.revision)),
            ("created_at", self.created_at.to_value()),
            ("expires_at", self.expires_at.to_value()),
            (
                "actors",
                Value::List(self.actors.iter().map(ActorPlan::to_value).collect()),
            ),
            (
                "videos",
                Value::List(self.videos.iter().map(VideoPlan::to_value).collect()),
            ),
        ])
    }

    /// `model_dump_json()`: the bytes of `fill_actor_plans.public_json`.
    pub fn dump_json(&self) -> String {
        pydantic_json(&self.to_value())
    }

    /// `FillActorPlan.model_validate_json(text)`.
    pub fn validate_json(text: &str) -> Result<Self, ValidationError> {
        validate_json(text, "FillActorPlan", plan_fields)
    }
}

impl MoveResult {
    pub fn to_value(&self) -> Value {
        object(vec![
            ("candidate_id", text(&self.candidate_id)),
            ("video_id", text(&self.video_id)),
            ("file_name", text(&self.file_name)),
            ("state", text(self.state.as_str())),
            ("error_code", optional_text(self.error_code.as_ref())),
        ])
    }

    /// `model_dump_json()`: the bytes of `fill_actor_move_results.result_json`.
    pub fn dump_json(&self) -> String {
        pydantic_json(&self.to_value())
    }

    /// `MoveResult.model_validate_json(text)`.
    pub fn validate_json(text: &str) -> Result<Self, ValidationError> {
        validate_json(text, "MoveResult", move_result_fields)
    }
}

impl ApplyResult {
    pub fn to_value(&self) -> Value {
        object(vec![
            ("plan_id", text(&self.plan_id)),
            ("revision", text(&self.revision)),
            ("state", text(self.state.as_str())),
            (
                "results",
                Value::List(self.results.iter().map(MoveResult::to_value).collect()),
            ),
        ])
    }

    /// `model_dump_json()`: the bytes of `fill_actor_apply_jobs.result_json`.
    pub fn dump_json(&self) -> String {
        pydantic_json(&self.to_value())
    }

    /// `ApplyResult.model_validate_json(text)`.
    pub fn validate_json(text: &str) -> Result<Self, ValidationError> {
        validate_json(text, "ApplyResult", apply_result_fields)
    }
}

impl ActorPlan {
    /// `ActorPlan.model_validate_json(text)`.
    pub fn validate_json(text: &str) -> Result<Self, ValidationError> {
        validate_json(text, "ActorPlan", actor_fields)
    }
}

impl MoveCandidate {
    /// `MoveCandidate.model_validate_json(text)`.
    pub fn validate_json(text: &str) -> Result<Self, ValidationError> {
        validate_json(text, "MoveCandidate", candidate_fields)
    }
}

impl VideoPlan {
    /// `VideoPlan.model_validate_json(text)`.
    pub fn validate_json(text: &str) -> Result<Self, ValidationError> {
        validate_json(text, "VideoPlan", video_fields)
    }
}

// ---------- model_validate_json ----------

/// A model's field list and its field-by-field conversion.
struct ModelSpec<T> {
    name: &'static str,
    fields: &'static [&'static str],
    convert: fn(&mut Validator<'_>) -> Option<T>,
}

fn validate_json<T>(text: &str, title: &'static str, spec: fn() -> ModelSpec<T>) -> Result<T, ValidationError> {
    let fail = |errors| ValidationError { title, errors };
    let value = lax::parse_json(text).map_err(|error| fail(vec![error]))?;
    validate_value(&value, &spec(), Vec::new()).map_err(fail)
}

fn validate_value<T>(value: &Value, spec: &ModelSpec<T>, prefix: Vec<Value>) -> Result<T, Vec<ErrorDetail>> {
    let mut validator =
        Validator::new(value, Mode::Json, spec.name, spec.fields, prefix).map_err(|error| vec![error])?;
    let converted = (spec.convert)(&mut validator);
    validator.finish_ignoring_extras()?;
    Ok(converted.expect("every field converted when no error was recorded"))
}

fn at(field: &str) -> Vec<Value> {
    vec![Value::Str(field.to_owned())]
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "the shape of a field check, for fields without one"
)]
fn no_check<T>(value: T) -> Result<T, String> {
    Ok(value)
}

fn string(v: &mut Validator<'_>, field: &str, value: &Value) -> Result<String, Recorded> {
    v.scalar_at(&at(field), value, lax::lax_str)
}

fn optional_string(v: &mut Validator<'_>, field: &str, value: &Value) -> Result<Option<String>, Recorded> {
    match value {
        Value::Null => Ok(None),
        other => string(v, field, other).map(Some),
    }
}

fn strings(v: &mut Validator<'_>, field: &str, value: &Value) -> Result<Vec<String>, Recorded> {
    v.tuple(&at(field), value, |v, loc, item| v.scalar_at(&loc, item, lax::lax_str))
}

fn str_enum_value<E: Copy>(
    v: &mut Validator<'_>,
    field: &str,
    value: &Value,
    all: &[E],
    as_str: fn(E) -> &'static str,
) -> Result<E, Recorded> {
    v.scalar_at(&at(field), value, |value| {
        value
            .as_str()
            .and_then(|text| all.iter().copied().find(|member| as_str(*member) == text))
            .ok_or_else(|| enum_refusal(all.iter().map(|member| as_str(*member))))
    })
}

/// pydantic's `enum` error: `Input should be 'a', 'b' or 'c'`.
fn enum_refusal<'a>(values: impl Iterator<Item = &'a str>) -> Refusal {
    let quoted: Vec<String> = values.map(|value| format!("'{value}'")).collect();
    let msg = match quoted.split_last() {
        Some((last, rest)) if !rest.is_empty() => format!("Input should be {} or {last}", rest.join(", ")),
        Some((last, _)) => format!("Input should be {last}"),
        None => "Input should be".to_owned(),
    };
    Refusal { kind: "enum", msg }
}

fn nested<T>(v: &mut Validator<'_>, loc: &[Value], value: &Value, spec: fn() -> ModelSpec<T>) -> Result<T, Recorded> {
    let prefix = v.nested_prefix(loc);
    validate_value(value, &spec(), prefix).map_err(|errors| {
        for error in errors {
            v.push_detail(error);
        }
        Recorded
    })
}

fn nested_list<T>(
    v: &mut Validator<'_>,
    field: &str,
    value: &Value,
    spec: fn() -> ModelSpec<T>,
) -> Result<Vec<T>, Recorded> {
    v.tuple(&at(field), value, |v, loc, item| nested(v, &loc, item, spec))
}

fn actor_fields() -> ModelSpec<ActorPlan> {
    ModelSpec {
        name: "ActorPlan",
        fields: &[
            "actor_id",
            "scraped_count",
            "video_ids",
            "error_code",
            "actor_name",
            "talent_id",
            "aliases",
            "error_message",
        ],
        convert: |v| {
            let actor_id = v.required("actor_id", |v, x| string(v, "actor_id", x), no_check);
            let scraped_count = v.required(
                "scraped_count",
                |v, x| {
                    v.scalar_at(&at("scraped_count"), x, |value| {
                        let count = lax::lax_int(value)?;
                        if count < 0 {
                            return Err(Refusal {
                                kind: "greater_than_equal",
                                msg: "Input should be greater than or equal to 0".to_owned(),
                            });
                        }
                        Ok(count)
                    })
                },
                no_check,
            );
            let video_ids = v.field("video_ids", Vec::new, |v, x| strings(v, "video_ids", x), no_check);
            let error_code = v.field(
                "error_code",
                || None,
                |v, x| optional_string(v, "error_code", x),
                no_check,
            );
            let actor_name = v.field(
                "actor_name",
                || None,
                |v, x| optional_string(v, "actor_name", x),
                no_check,
            );
            let talent_id = v.field(
                "talent_id",
                || None,
                |v, x| match x {
                    Value::Null => Ok(None),
                    other => v.scalar_at(&at("talent_id"), other, lax::lax_int).map(Some),
                },
                no_check,
            );
            let aliases = v.field("aliases", Vec::new, |v, x| strings(v, "aliases", x), no_check);
            let error_message = v.field(
                "error_message",
                || None,
                |v, x| optional_string(v, "error_message", x),
                no_check,
            );
            Some(ActorPlan {
                actor_id: actor_id?,
                scraped_count: scraped_count?,
                video_ids: video_ids?,
                error_code: error_code?,
                actor_name: actor_name?,
                talent_id: talent_id?,
                aliases: aliases?,
                error_message: error_message?,
            })
        },
    }
}

fn candidate_fields() -> ModelSpec<MoveCandidate> {
    ModelSpec {
        name: "MoveCandidate",
        fields: &[
            "candidate_id",
            "video_id",
            "file_name",
            "source_label",
            "destination_conflict",
        ],
        convert: |v| {
            let candidate_id = v.required("candidate_id", |v, x| string(v, "candidate_id", x), no_check);
            let video_id = v.required("video_id", |v, x| string(v, "video_id", x), no_check);
            let file_name = v.required("file_name", |v, x| string(v, "file_name", x), no_check);
            let source_label = v.required("source_label", |v, x| string(v, "source_label", x), no_check);
            let destination_conflict = v.required(
                "destination_conflict",
                |v, x| v.scalar_at(&at("destination_conflict"), x, |value| lax::lax_bool(value, Mode::Json)),
                no_check,
            );
            Some(MoveCandidate {
                candidate_id: candidate_id?,
                video_id: video_id?,
                file_name: file_name?,
                source_label: source_label?,
                destination_conflict: destination_conflict?,
            })
        },
    }
}

fn video_fields() -> ModelSpec<VideoPlan> {
    ModelSpec {
        name: "VideoPlan",
        fields: &[
            "video_id",
            "actor_ids",
            "state",
            "release_date",
            "existing_files",
            "move_candidates",
            "warnings",
        ],
        convert: |v| {
            let video_id = v.required("video_id", |v, x| string(v, "video_id", x), no_check);
            let actor_ids = v.required("actor_ids", |v, x| strings(v, "actor_ids", x), no_check);
            let state = v.required(
                "state",
                |v, x| str_enum_value(v, "state", x, VideoState::ALL, VideoState::as_str),
                no_check,
            );
            let release_date = v.field(
                "release_date",
                || None,
                |v, x| match x {
                    Value::Null => Ok(None),
                    other => v.scalar_at(&at("release_date"), other, lax_date).map(Some),
                },
                no_check,
            );
            let existing_files = v.field(
                "existing_files",
                Vec::new,
                |v, x| strings(v, "existing_files", x),
                no_check,
            );
            let move_candidates = v.field(
                "move_candidates",
                Vec::new,
                |v, x| nested_list(v, "move_candidates", x, candidate_fields),
                no_check,
            );
            let warnings = v.field("warnings", Vec::new, |v, x| strings(v, "warnings", x), no_check);
            Some(VideoPlan {
                video_id: video_id?,
                actor_ids: actor_ids?,
                state: state?,
                release_date: release_date?,
                existing_files: existing_files?,
                move_candidates: move_candidates?,
                warnings: warnings?,
            })
        },
    }
}

fn plan_fields() -> ModelSpec<FillActorPlan> {
    ModelSpec {
        name: "FillActorPlan",
        fields: &["plan_id", "revision", "created_at", "expires_at", "actors", "videos"],
        convert: |v| {
            let plan_id = v.required("plan_id", |v, x| string(v, "plan_id", x), no_check);
            let revision = v.required("revision", |v, x| string(v, "revision", x), no_check);
            let created_at = v.required(
                "created_at",
                |v, x| v.scalar_at(&at("created_at"), x, lax_datetime),
                no_check,
            );
            let expires_at = v.required(
                "expires_at",
                |v, x| v.scalar_at(&at("expires_at"), x, lax_datetime),
                no_check,
            );
            let actors = v.required("actors", |v, x| nested_list(v, "actors", x, actor_fields), no_check);
            let videos = v.required("videos", |v, x| nested_list(v, "videos", x, video_fields), no_check);
            Some(FillActorPlan {
                plan_id: plan_id?,
                revision: revision?,
                created_at: created_at?,
                expires_at: expires_at?,
                actors: actors?,
                videos: videos?,
            })
        },
    }
}

fn move_result_fields() -> ModelSpec<MoveResult> {
    ModelSpec {
        name: "MoveResult",
        fields: &["candidate_id", "video_id", "file_name", "state", "error_code"],
        convert: |v| {
            let candidate_id = v.required("candidate_id", |v, x| string(v, "candidate_id", x), no_check);
            let video_id = v.required("video_id", |v, x| string(v, "video_id", x), no_check);
            let file_name = v.required("file_name", |v, x| string(v, "file_name", x), no_check);
            let state = v.required(
                "state",
                |v, x| str_enum_value(v, "state", x, MoveState::ALL, MoveState::as_str),
                no_check,
            );
            let error_code = v.field(
                "error_code",
                || None,
                |v, x| optional_string(v, "error_code", x),
                no_check,
            );
            Some(MoveResult {
                candidate_id: candidate_id?,
                video_id: video_id?,
                file_name: file_name?,
                state: state?,
                error_code: error_code?,
            })
        },
    }
}

fn apply_result_fields() -> ModelSpec<ApplyResult> {
    ModelSpec {
        name: "ApplyResult",
        fields: &["plan_id", "revision", "state", "results"],
        convert: |v| {
            let plan_id = v.required("plan_id", |v, x| string(v, "plan_id", x), no_check);
            let revision = v.required("revision", |v, x| string(v, "revision", x), no_check);
            let state = v.required(
                "state",
                |v, x| str_enum_value(v, "state", x, ApplyState::ALL, ApplyState::as_str),
                no_check,
            );
            let results = v.required(
                "results",
                |v, x| nested_list(v, "results", x, move_result_fields),
                no_check,
            );
            Some(ApplyResult {
                plan_id: plan_id?,
                revision: revision?,
                state: state?,
                results: results?,
            })
        },
    }
}
