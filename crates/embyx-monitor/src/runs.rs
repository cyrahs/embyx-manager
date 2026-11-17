//! Persistence for pipeline run history.
//!
//! Python origin: `src/embyx_manager/monitor/runs.py`. `stats_json`, `errors_json` and
//! `log_json` hold `json.dumps(...)` of the run's counters, error lines and log tail, in
//! Python's default form (ASCII-only, `", "` and `": "`), so the dashboard reads the same bytes
//! whichever build wrote the run.

use chrono::{DateTime, Utc};
use embyx_core::pyjson::{self, DumpOptions, Value};
use embyx_db::database::Database;
use sqlx::Row as _;
use sqlx::postgres::PgRow;

use crate::acquisitions::load_string_list;
use crate::reports::{Counters, PipelineName, RunState, RunTrigger};
use crate::support::{Clock, StoreError, system_clock, uuid4_hex};

/// `PipelineRunRecord`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineRunRecord {
    pub run_id: String,
    pub pipeline: PipelineName,
    pub trigger: RunTrigger,
    pub state: RunState,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub stats: Counters,
    pub errors: Vec<String>,
    pub log_tail: Vec<String>,
}

/// `json.dumps(value)`.
fn dumps(value: &Value) -> String {
    pyjson::dumps(value, &DumpOptions::python_default()).expect("strings and integers always dump")
}

fn dump_lines(lines: &[String]) -> String {
    dumps(&Value::List(
        lines.iter().map(|line| Value::Str(line.clone())).collect(),
    ))
}

/// `PipelineRunRepository`.
#[derive(Clone)]
pub struct PipelineRunRepository {
    database: Database,
    clock: Clock,
}

impl std::fmt::Debug for PipelineRunRepository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineRunRepository")
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

impl PipelineRunRepository {
    pub fn new(database: Database) -> Self {
        Self::with_clock(database, system_clock())
    }

    /// With the clock `datetime.now(UTC)` stands for.
    pub fn with_clock(database: Database, clock: Clock) -> Self {
        Self { database, clock }
    }

    /// `start_run(pipeline, trigger)`: a new `running` row; its id is `uuid4().hex`.
    pub async fn start_run(&self, pipeline: PipelineName, trigger: RunTrigger) -> Result<String, StoreError> {
        let run_id = uuid4_hex();
        let pool = self.database.get_pool().await?;
        sqlx::query(
            "
            INSERT INTO pipeline_runs (run_id, pipeline, trigger, state, started_at)
            VALUES ($1, $2, $3, 'running', $4)
            ",
        )
        .bind(&run_id)
        .bind(pipeline.as_str())
        .bind(trigger.as_str())
        .bind((self.clock)())
        .execute(&pool)
        .await?;
        Ok(run_id)
    }

    /// `finish_run(run_id, state=, stats=, errors=, log_tail=)`.
    pub async fn finish_run(
        &self,
        run_id: &str,
        state: RunState,
        counters: &Counters,
        errors: &[String],
        log_tail: &[String],
    ) -> Result<(), StoreError> {
        let pool = self.database.get_pool().await?;
        sqlx::query(
            "
            UPDATE pipeline_runs
            SET state = $1, finished_at = $2, stats_json = $3, errors_json = $4, log_json = $5
            WHERE run_id = $6
            ",
        )
        .bind(state.as_str())
        .bind((self.clock)())
        .bind(dumps(&counters.to_value()))
        .bind(dump_lines(errors))
        .bind(dump_lines(log_tail))
        .bind(run_id)
        .execute(&pool)
        .await?;
        Ok(())
    }

    pub async fn get_run(&self, run_id: &str) -> Result<Option<PipelineRunRecord>, StoreError> {
        let pool = self.database.get_pool().await?;
        let row = sqlx::query("SELECT * FROM pipeline_runs WHERE run_id = $1")
            .bind(run_id)
            .fetch_optional(&pool)
            .await?;
        row.as_ref().map(run_from_row).transpose()
    }

    /// `list_runs(pipeline=None, limit=20, offset=0)`: newest first.
    pub async fn list_runs(
        &self,
        pipeline: Option<PipelineName>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<PipelineRunRecord>, StoreError> {
        let pool = self.database.get_pool().await?;
        let rows = match pipeline {
            None => {
                sqlx::query("SELECT * FROM pipeline_runs ORDER BY started_at DESC, run_id LIMIT $1 OFFSET $2")
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(&pool)
                    .await?
            }
            Some(pipeline) => {
                sqlx::query(
                    "
                    SELECT * FROM pipeline_runs WHERE pipeline = $1
                    ORDER BY started_at DESC, run_id LIMIT $2 OFFSET $3
                    ",
                )
                .bind(pipeline.as_str())
                .bind(limit)
                .bind(offset)
                .fetch_all(&pool)
                .await?
            }
        };
        rows.iter().map(run_from_row).collect()
    }

    pub async fn latest_run(&self, pipeline: PipelineName) -> Result<Option<PipelineRunRecord>, StoreError> {
        Ok(self.list_runs(Some(pipeline), 1, 0).await?.into_iter().next())
    }

    /// Mark runs left in `running` by a dead process as failed (startup recovery).
    pub async fn fail_stale_running(&self, error: &str) -> Result<u64, StoreError> {
        let pool = self.database.get_pool().await?;
        let done = sqlx::query(
            "
            UPDATE pipeline_runs
            SET state = 'failed', finished_at = $1,
                errors_json = $2
            WHERE state = 'running'
            ",
        )
        .bind((self.clock)())
        .bind(dump_lines(&[error.to_owned()]))
        .execute(&pool)
        .await?;
        Ok(done.rows_affected())
    }

    /// `prune(keep_per_pipeline=500)`: delete all but the newest runs of each pipeline.
    pub async fn prune(&self, keep_per_pipeline: i64) -> Result<u64, StoreError> {
        let pool = self.database.get_pool().await?;
        let done = sqlx::query(
            "
            DELETE FROM pipeline_runs WHERE run_id IN (
                SELECT run_id FROM (
                    SELECT run_id, ROW_NUMBER() OVER (
                        PARTITION BY pipeline ORDER BY started_at DESC
                    ) AS position
                    FROM pipeline_runs
                ) ranked WHERE ranked.position > $1
            )
            ",
        )
        .bind(keep_per_pipeline)
        .execute(&pool)
        .await?;
        Ok(done.rows_affected())
    }
}

/// `json.loads(row['stats_json'])` as the counters it holds.
fn load_counters(text: &str) -> Result<Counters, StoreError> {
    let value = pyjson::loads(text).map_err(|error| StoreError::Decode {
        class: "JSONDecodeError",
        message: error.python_message(text),
    })?;
    let mut counters = Counters::new();
    let Value::Dict(dict) = value else {
        return Err(StoreError::Decode {
            class: "TypeError",
            message: "stats_json does not hold a JSON object".to_owned(),
        });
    };
    for (key, value) in dict.iter() {
        let (Some(name), Value::Int(number)) = (key.as_str(), value) else {
            return Err(StoreError::Decode {
                class: "TypeError",
                message: "stats_json holds a counter that is not an integer".to_owned(),
            });
        };
        let number = i64::try_from(*number).map_err(|_| StoreError::Decode {
            class: "OverflowError",
            message: "stats_json holds a counter beyond 64 bits".to_owned(),
        })?;
        counters.set(name, number);
    }
    Ok(counters)
}

fn parse_or<T>(value: &str, parse: fn(&str) -> Option<T>, name: &str) -> Result<T, StoreError> {
    parse(value).ok_or_else(|| StoreError::invalid_enum(value, name))
}

/// `_run_from_row(row)`.
fn run_from_row(row: &PgRow) -> Result<PipelineRunRecord, StoreError> {
    Ok(PipelineRunRecord {
        run_id: row.try_get("run_id")?,
        pipeline: parse_or(row.try_get("pipeline")?, PipelineName::parse, "PipelineName")?,
        trigger: parse_or(row.try_get("trigger")?, RunTrigger::parse, "RunTrigger")?,
        state: parse_or(row.try_get("state")?, RunState::parse, "RunState")?,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
        stats: load_counters(row.try_get("stats_json")?)?,
        errors: load_string_list(row.try_get("errors_json")?, "errors_json")?,
        log_tail: load_string_list(row.try_get("log_json")?, "log_json")?,
    })
}

#[cfg(test)]
mod tests;
