//! Stored merge tasks: one row per title the merge tab was asked to merge.
//!
//! Python origin: `src/embyx_manager/merge/tasks.py`. Every statement is Python's, so a row
//! written by either build reads the same in the other (PLAN.md, cross-version contract 3):
//! the state names, `parts_json` as `json.dumps(parts, ensure_ascii=False)`, and the
//! compare-and-set of `transition`.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use embyx_core::pyjson::{self, DumpOptions, Value};
use embyx_db::database::{Database, DatabaseError};
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{Arguments as _, AssertSqlSafe, Row as _};

use crate::ports::{Clock, system_clock};

/// `MergeState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MergeState {
    /// Waiting for the merge Job slot; one title merges at a time.
    Queued,
    /// A worker outside the cluster re-encodes the video while a Job copies the parts' audio.
    Transcoding,
    /// The Job is concatenating the parts onto the work volume.
    Merging,
    /// CloudDrive is copying the merged file into the 115 staging directory.
    Uploading,
    /// Comparing the uploaded file's size and SHA-1 with the merged file's.
    Verifying,
    /// Deleting the original parts and moving the merged file into the intake route.
    Replacing,
    /// Waiting for the archive pipeline to file the merged title.
    Archiving,
    Done,
    Failed,
    Cancelled,
}

impl MergeState {
    pub const ALL: [Self; 10] = [
        Self::Queued,
        Self::Transcoding,
        Self::Merging,
        Self::Uploading,
        Self::Verifying,
        Self::Replacing,
        Self::Archiving,
        Self::Done,
        Self::Failed,
        Self::Cancelled,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Transcoding => "transcoding",
            Self::Merging => "merging",
            Self::Uploading => "uploading",
            Self::Verifying => "verifying",
            Self::Replacing => "replacing",
            Self::Archiving => "archiving",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// `MergeState(value)`.
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|state| state.as_str() == value)
    }

    /// `FINISHED_STATES`.
    pub fn is_finished(self) -> bool {
        matches!(self, Self::Done | Self::Cancelled)
    }

    /// `CANCELLABLE_STATES`: the states a task can be cancelled in; past them the originals may
    /// be gone.
    pub fn is_cancellable(self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Transcoding | Self::Merging | Self::Uploading | Self::Verifying | Self::Failed
        )
    }
}

/// `RECENT_FINISHED_LIMIT`.
pub const RECENT_FINISHED_LIMIT: i64 = 20;

/// A column a state handler may change besides `state` (`_UPDATABLE`), with its new value.
#[derive(Debug, Clone, PartialEq)]
pub enum TaskField {
    FailedState(Option<MergeState>),
    JobName(Option<String>),
    Phase(Option<String>),
    Progress(Option<f64>),
    MergedBytes(Option<i64>),
    MergedSha1(Option<String>),
    UploadedBytes(Option<i64>),
    UploadAttempts(i32),
    Error(Option<String>),
    Notice(Option<String>),
    FinishedAt(Option<DateTime<Utc>>),
    Transcoder(Option<String>),
    TranscodePaused(bool),
}

impl TaskField {
    pub fn column(&self) -> &'static str {
        match self {
            Self::FailedState(_) => "failed_state",
            Self::JobName(_) => "job_name",
            Self::Phase(_) => "phase",
            Self::Progress(_) => "progress",
            Self::MergedBytes(_) => "merged_bytes",
            Self::MergedSha1(_) => "merged_sha1",
            Self::UploadedBytes(_) => "uploaded_bytes",
            Self::UploadAttempts(_) => "upload_attempts",
            Self::Error(_) => "error",
            Self::Notice(_) => "notice",
            Self::FinishedAt(_) => "finished_at",
            Self::Transcoder(_) => "transcoder",
            Self::TranscodePaused(_) => "transcode_paused",
        }
    }

    fn bind(self, arguments: &mut PgArguments) -> Result<(), sqlx::error::BoxDynError> {
        match self {
            Self::FailedState(state) => arguments.add(state.map(MergeState::as_str)),
            Self::JobName(value)
            | Self::Phase(value)
            | Self::MergedSha1(value)
            | Self::Error(value)
            | Self::Notice(value)
            | Self::Transcoder(value) => arguments.add(value),
            Self::Progress(value) => arguments.add(value),
            Self::MergedBytes(value) | Self::UploadedBytes(value) => arguments.add(value),
            Self::UploadAttempts(value) => arguments.add(value),
            Self::FinishedAt(value) => arguments.add(value),
            Self::TranscodePaused(value) => arguments.add(value),
        }
    }
}

/// `MergeTask`.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeTask {
    pub id: i64,
    pub avid: String,
    pub source: String,
    pub library_dir: String,
    pub brand: String,
    /// Each part's file on the CloudDrive mount, in part order.
    pub parts: Vec<String>,
    /// The merged file's name, e.g. `ABC-123.mp4`.
    pub merged_name: String,
    pub state: MergeState,
    pub failed_state: Option<MergeState>,
    pub job_name: Option<String>,
    pub phase: Option<String>,
    pub progress: Option<f64>,
    pub merged_bytes: Option<i64>,
    pub merged_sha1: Option<String>,
    pub uploaded_bytes: Option<i64>,
    pub upload_attempts: i32,
    /// Why the task failed.
    pub error: Option<String>,
    /// A passing problem the task is waiting out, e.g. CloudDrive unreachable.
    pub notice: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state_changed_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    /// Who re-encodes a merge too big for 115: `None` until decided, `home` or `cpu`.
    pub transcoder: Option<String>,
    /// Someone asked the home worker to stop after its current chunk.
    pub transcode_paused: bool,
}

/// Why a repository call failed.
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    /// `MergeTaskConflictError`: the title already has an unfinished merge task.
    #[error("{0}")]
    Conflict(String),
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    /// A stored value Python could not have read back either (`MergeState(...)`,
    /// `json.loads` of `parts_json`).
    #[error("{0}")]
    Corrupt(String),
}

/// `MergeTaskRepository`.
#[derive(Clone)]
pub struct MergeTaskRepository {
    database: Database,
    /// `datetime.now(UTC)`, injected so scenarios can pin it.
    clock: Clock,
}

impl std::fmt::Debug for MergeTaskRepository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergeTaskRepository").finish_non_exhaustive()
    }
}

/// The fields of a new task (`create`'s keyword arguments).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTask {
    pub avid: String,
    pub source: String,
    pub library_dir: String,
    pub brand: String,
    pub parts: Vec<String>,
    pub merged_name: String,
}

fn state_of(value: &str) -> Result<MergeState, TaskError> {
    MergeState::parse(value).ok_or_else(|| TaskError::Corrupt(format!("'{value}' is not a valid MergeState")))
}

fn from_row(row: &PgRow) -> Result<MergeTask, TaskError> {
    let parts_json: String = row.try_get("parts_json")?;
    let parts = match pyjson::loads(&parts_json).map_err(|error| TaskError::Corrupt(error.to_string()))? {
        Value::List(items) => items
            .into_iter()
            .map(|item| match item {
                Value::Str(text) => Ok(text),
                other => Err(TaskError::Corrupt(format!("a part is not a string: {other:?}"))),
            })
            .collect::<Result<Vec<_>, _>>()?,
        other => return Err(TaskError::Corrupt(format!("parts_json is not a list: {other:?}"))),
    };
    let failed_state: Option<String> = row.try_get("failed_state")?;
    Ok(MergeTask {
        id: row.try_get("id")?,
        avid: row.try_get("avid")?,
        source: row.try_get("source")?,
        library_dir: row.try_get("library_dir")?,
        brand: row.try_get("brand")?,
        parts,
        merged_name: row.try_get("merged_name")?,
        state: state_of(&row.try_get::<String, _>("state")?)?,
        // `MergeState(row['failed_state']) if row['failed_state'] else None`: '' is None too.
        failed_state: match failed_state.as_deref() {
            None | Some("") => None,
            Some(value) => Some(state_of(value)?),
        },
        job_name: row.try_get("job_name")?,
        phase: row.try_get("phase")?,
        progress: row.try_get("progress")?,
        merged_bytes: row.try_get("merged_bytes")?,
        merged_sha1: row.try_get("merged_sha1")?,
        uploaded_bytes: row.try_get("uploaded_bytes")?,
        upload_attempts: row.try_get("upload_attempts")?,
        error: row.try_get("error")?,
        notice: row.try_get("notice")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        state_changed_at: row.try_get("state_changed_at")?,
        finished_at: row.try_get("finished_at")?,
        transcoder: row.try_get("transcoder")?,
        transcode_paused: row.try_get("transcode_paused")?,
    })
}

impl MergeTaskRepository {
    pub fn new(database: Database) -> Self {
        Self::with_clock(database, system_clock())
    }

    /// The repository with the time it stamps rows with taken from `clock`.
    pub fn with_clock(database: Database, clock: Clock) -> Self {
        Self { database, clock }
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    /// `create`: a new task in `queued`; `TaskError::Conflict` when the title already has an
    /// unfinished one.
    pub async fn create(&self, task: &NewTask) -> Result<MergeTask, TaskError> {
        let pool = self.database.get_pool().await?;
        let parts = Value::List(task.parts.iter().map(|part| Value::Str(part.clone())).collect());
        let parts_json =
            pyjson::dumps(&parts, &DumpOptions::plain()).map_err(|error| TaskError::Corrupt(error.to_string()))?;
        let row = sqlx::query(
            "
                INSERT INTO merge_tasks (
                    avid, source, library_dir, brand, parts_json, merged_name, state,
                    created_at, updated_at, state_changed_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'queued', $7, $7, $7)
                RETURNING *
                ",
        )
        .bind(&task.avid)
        .bind(&task.source)
        .bind(&task.library_dir)
        .bind(&task.brand)
        .bind(parts_json)
        .bind(&task.merged_name)
        .bind((self.clock)())
        .fetch_one(&pool)
        .await
        .map_err(|error| match &error {
            sqlx::Error::Database(database) if database.code().as_deref() == Some("23505") => {
                TaskError::Conflict(task.avid.clone())
            }
            _ => TaskError::Sqlx(error),
        })?;
        from_row(&row)
    }

    /// `get`.
    pub async fn get(&self, task_id: i64) -> Result<Option<MergeTask>, TaskError> {
        let pool = self.database.get_pool().await?;
        let row = sqlx::query("SELECT * FROM merge_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_optional(&pool)
            .await?;
        row.as_ref().map(from_row).transpose()
    }

    /// `open_tasks`: every task not yet done or cancelled, oldest first.
    pub async fn open_tasks(&self) -> Result<Vec<MergeTask>, TaskError> {
        let pool = self.database.get_pool().await?;
        let rows = sqlx::query("SELECT * FROM merge_tasks WHERE state NOT IN ('done', 'cancelled') ORDER BY id")
            .fetch_all(&pool)
            .await?;
        rows.iter().map(from_row).collect()
    }

    /// `listing`: open tasks plus the most recently finished ones, newest first.
    pub async fn listing(&self) -> Result<Vec<MergeTask>, TaskError> {
        let pool = self.database.get_pool().await?;
        let rows = sqlx::query(
            "
            (SELECT * FROM merge_tasks WHERE state NOT IN ('done', 'cancelled'))
            UNION ALL
            (
                SELECT * FROM merge_tasks WHERE state IN ('done', 'cancelled')
                ORDER BY finished_at DESC NULLS LAST, id DESC LIMIT $1
            )
            ORDER BY id DESC
            ",
        )
        .bind(RECENT_FINISHED_LIMIT)
        .fetch_all(&pool)
        .await?;
        rows.iter().map(from_row).collect()
    }

    async fn avids(&self, sql: &'static str) -> Result<BTreeSet<String>, TaskError> {
        let pool = self.database.get_pool().await?;
        let rows: Vec<(String,)> = sqlx::query_as(sql).fetch_all(&pool).await?;
        Ok(rows.into_iter().map(|(avid,)| avid).collect())
    }

    /// `cancelled_avids`: titles someone cancelled or removed a task for; automatic merging
    /// leaves them be.
    pub async fn cancelled_avids(&self) -> Result<BTreeSet<String>, TaskError> {
        self.avids("SELECT DISTINCT avid FROM merge_tasks WHERE state = 'cancelled'")
            .await
    }

    /// `merged_avids`: titles a task already merged; their parts may linger in the scan until
    /// mapping catches up.
    pub async fn merged_avids(&self) -> Result<BTreeSet<String>, TaskError> {
        self.avids("SELECT DISTINCT avid FROM merge_tasks WHERE state = 'done'")
            .await
    }

    /// `last_done_at`: when the latest task finished merging its title into the library.
    pub async fn last_done_at(&self) -> Result<Option<DateTime<Utc>>, TaskError> {
        let pool = self.database.get_pool().await?;
        let value: Option<DateTime<Utc>> =
            sqlx::query_scalar("SELECT max(finished_at) FROM merge_tasks WHERE state = 'done'")
                .fetch_one(&pool)
                .await?;
        Ok(value)
    }

    /// `update`: change fields without moving the task to another state.
    pub async fn update(&self, task_id: i64, fields: Vec<TaskField>) -> Result<Option<MergeTask>, TaskError> {
        self.write(task_id, None, None, fields).await
    }

    /// `transition`: move a task from `expected` to `state`; `None` when it was no longer in
    /// `expected`.
    pub async fn transition(
        &self,
        task_id: i64,
        expected: MergeState,
        state: MergeState,
        fields: Vec<TaskField>,
    ) -> Result<Option<MergeTask>, TaskError> {
        self.write(task_id, Some(expected), Some(state), fields).await
    }

    async fn write(
        &self,
        task_id: i64,
        expected: Option<MergeState>,
        state: Option<MergeState>,
        fields: Vec<TaskField>,
    ) -> Result<Option<MergeTask>, TaskError> {
        let mut arguments = PgArguments::default();
        let bind_error = |error: sqlx::error::BoxDynError| TaskError::Sqlx(sqlx::Error::Encode(error));
        arguments.add(task_id).map_err(bind_error)?;
        arguments.add((self.clock)()).map_err(bind_error)?;
        let mut count = 2;
        let mut assignments = vec!["updated_at = $2".to_owned()];
        if let Some(state) = state {
            arguments.add(state.as_str()).map_err(bind_error)?;
            count += 1;
            assignments.push(format!("state = ${count}"));
            assignments.push("state_changed_at = $2".to_owned());
        }
        for field in fields {
            let column = field.column();
            field.bind(&mut arguments).map_err(bind_error)?;
            count += 1;
            assignments.push(format!("{column} = ${count}"));
        }
        let mut condition = String::new();
        if let Some(expected) = expected {
            arguments.add(expected.as_str()).map_err(bind_error)?;
            count += 1;
            condition = format!(" AND state = ${count}");
        }
        let pool = self.database.get_pool().await?;
        // Column names come from `TaskField::column`, values are bound.
        let sql = format!(
            "UPDATE merge_tasks SET {} WHERE id = $1{condition} RETURNING *",
            assignments.join(", ")
        );
        let row = sqlx::query_with(AssertSqlSafe(sql), arguments)
            .fetch_optional(&pool)
            .await?;
        row.as_ref().map(from_row).transpose()
    }
}
