//! What the merge scenarios share: a task as `operations/merge.py` `_task` reports it.

#![allow(dead_code, unreachable_pub, reason = "each test binary uses a part")]

use chrono::{DateTime, Utc};
use embyx_core::pyjson::{Dict, Value, py_isoformat_datetime};
use embyx_merge::tasks::MergeTask;

pub fn isoformat(value: DateTime<Utc>) -> Value {
    Value::Str(py_isoformat_datetime(&value.fixed_offset()))
}

pub fn optional_text(value: Option<&String>) -> Value {
    value.map_or(Value::Null, |text| Value::Str(text.clone()))
}

/// `merge._task`: every field but the wall-clock timestamps, with how they relate. The
/// fields are listed as `dataclasses.fields` lists them; the transcript sorts the keys.
pub fn task(task: Option<&MergeTask>) -> Value {
    let Some(task) = task else {
        return Value::Null;
    };
    let mut fields = Dict::new();
    fields.insert("id", Value::Int(task.id.into()));
    fields.insert("avid", Value::Str(task.avid.clone()));
    fields.insert("source", Value::Str(task.source.clone()));
    fields.insert("library_dir", Value::Str(task.library_dir.clone()));
    fields.insert("brand", Value::Str(task.brand.clone()));
    fields.insert(
        "parts",
        Value::List(task.parts.iter().cloned().map(Value::Str).collect()),
    );
    fields.insert("merged_name", Value::Str(task.merged_name.clone()));
    fields.insert("state", Value::Str(task.state.as_str().to_owned()));
    fields.insert(
        "failed_state",
        task.failed_state
            .map_or(Value::Null, |state| Value::Str(state.as_str().to_owned())),
    );
    fields.insert("job_name", optional_text(task.job_name.as_ref()));
    fields.insert("phase", optional_text(task.phase.as_ref()));
    fields.insert("progress", task.progress.map_or(Value::Null, Value::Float));
    fields.insert(
        "merged_bytes",
        task.merged_bytes.map_or(Value::Null, |bytes| Value::Int(bytes.into())),
    );
    fields.insert("merged_sha1", optional_text(task.merged_sha1.as_ref()));
    fields.insert(
        "uploaded_bytes",
        task.uploaded_bytes
            .map_or(Value::Null, |bytes| Value::Int(bytes.into())),
    );
    fields.insert("upload_attempts", Value::Int(task.upload_attempts.into()));
    fields.insert("error", optional_text(task.error.as_ref()));
    fields.insert("notice", optional_text(task.notice.as_ref()));
    fields.insert("finished_at", task.finished_at.map_or(Value::Null, isoformat));
    fields.insert("transcoder", optional_text(task.transcoder.as_ref()));
    fields.insert("transcode_paused", Value::Bool(task.transcode_paused));
    fields.insert(
        "updated_at_is_created_at",
        Value::Bool(task.updated_at == task.created_at),
    );
    fields.insert(
        "state_changed_at_is_updated_at",
        Value::Bool(task.state_changed_at == task.updated_at),
    );
    Value::Dict(fields)
}
