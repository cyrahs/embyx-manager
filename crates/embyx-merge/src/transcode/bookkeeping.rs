//! The home worker's presence and the chunks and parts it delivered.
//!
//! Python origin: `WorkerReport`, `WorkerPresence` and `TranscodeRepository` of
//! `src/embyx_manager/merge/transcode.py`. The statements are Python's, so a chunk recorded by
//! either build reads the same in the other (cross-version contract 3).

use std::sync::{Arc, Mutex, PoisonError};

use chrono::{DateTime, Duration, Utc};
use embyx_db::database::Database;
use sqlx::Row as _;

use super::{Chunk, WORKER_TIMEOUT_SECONDS};
use crate::ports::{Clock, system_clock};
use crate::tasks::TaskError;

/// One part the worker is encoding (`PartProgress` as `model_dump()` gives it).
#[derive(Debug, Clone, PartialEq)]
pub struct PartProgress {
    pub index: i64,
    pub fps: f64,
    /// Seconds into the part the encoder has reached.
    pub position: f64,
}

/// `WorkerReport`: what the worker last said about itself; kept in memory.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerReport {
    pub seen_at: DateTime<Utc>,
    pub name: String,
    pub version: String,
    pub task_id: Option<i64>,
    /// `encoding`, `paused`, `stopping`, `waiting` or `idle`.
    pub state: String,
    pub parts: Vec<PartProgress>,
    pub upload_bps: f64,
    pub queue_chunks: i64,
    pub queue_bytes: i64,
}

impl WorkerReport {
    /// `WorkerReport(seen_at=...)` with every other field at its default.
    pub fn at(seen_at: DateTime<Utc>) -> Self {
        Self {
            seen_at,
            name: String::new(),
            version: String::new(),
            task_id: None,
            state: "idle".to_owned(),
            parts: Vec::new(),
            upload_bps: 0.0,
            queue_chunks: 0,
            queue_bytes: 0,
        }
    }
}

/// `WorkerPresence`: whether the home worker is there; a single worker is expected. Clones
/// share the last report.
#[derive(Clone)]
pub struct WorkerPresence {
    timeout: Duration,
    last: Arc<Mutex<Option<WorkerReport>>>,
    clock: Clock,
}

impl std::fmt::Debug for WorkerPresence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerPresence")
            .field("timeout", &self.timeout)
            .field("last", &self.last())
            .finish_non_exhaustive()
    }
}

impl Default for WorkerPresence {
    fn default() -> Self {
        Self::new(system_clock())
    }
}

impl WorkerPresence {
    /// `WorkerPresence()` reading `now` from `clock`.
    pub fn new(clock: Clock) -> Self {
        Self {
            timeout: Duration::seconds(WORKER_TIMEOUT_SECONDS),
            last: Arc::new(Mutex::new(None)),
            clock,
        }
    }

    pub fn report(&self, report: WorkerReport) {
        *self.last.lock().unwrap_or_else(PoisonError::into_inner) = Some(report);
    }

    pub fn last(&self) -> Option<WorkerReport> {
        self.last.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// `online()`: the last report is at most 90 s old.
    pub fn online(&self) -> bool {
        self.online_at((self.clock)())
    }

    /// `online(now)`.
    pub fn online_at(&self, now: DateTime<Utc>) -> bool {
        self.last().is_some_and(|last| now - last.seen_at <= self.timeout)
    }

    /// `view()`: the last report with whether the worker is online; `None` before it ever
    /// called.
    pub fn view(&self) -> Option<(WorkerReport, bool)> {
        let last = self.last()?;
        let online = self.online();
        Some((last, online))
    }
}

/// `TranscodeRepository`: the chunks the worker delivered and the parts it finished, per task.
#[derive(Clone)]
pub struct TranscodeRepository {
    database: Database,
    clock: Clock,
}

impl std::fmt::Debug for TranscodeRepository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TranscodeRepository").finish_non_exhaustive()
    }
}

impl TranscodeRepository {
    pub fn new(database: Database) -> Self {
        Self::with_clock(database, system_clock())
    }

    /// The repository with the time it stamps rows with taken from `clock`.
    pub fn with_clock(database: Database, clock: Clock) -> Self {
        Self { database, clock }
    }

    /// `chunks(task_id)`: in part and chunk order.
    pub async fn chunks(&self, task_id: i64) -> Result<Vec<Chunk>, TaskError> {
        let pool = self.database.get_pool().await?;
        let rows =
            sqlx::query("SELECT * FROM merge_transcode_chunks WHERE task_id = $1 ORDER BY part_index, chunk_index")
                .bind(task_id)
                .fetch_all(&pool)
                .await?;
        rows.iter()
            .map(|row| {
                Ok(Chunk {
                    part_index: row.try_get("part_index")?,
                    chunk_index: row.try_get("chunk_index")?,
                    start: row.try_get("start_seconds")?,
                    duration: row.try_get("duration_seconds")?,
                    frames: row.try_get("frames")?,
                    bytes: row.try_get("bytes")?,
                    sha256: row.try_get("sha256")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(TaskError::from)
    }

    /// `add_chunk(task_id, chunk)`: `false` when the part already has one under its index.
    pub async fn add_chunk(&self, task_id: i64, chunk: &Chunk) -> Result<bool, TaskError> {
        let pool = self.database.get_pool().await?;
        let result = sqlx::query(
            "
            INSERT INTO merge_transcode_chunks (
                task_id, part_index, chunk_index, start_seconds, duration_seconds, frames, bytes, sha256, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT DO NOTHING
            ",
        )
        .bind(task_id)
        .bind(chunk.part_index)
        .bind(chunk.chunk_index)
        .bind(chunk.start)
        .bind(chunk.duration)
        .bind(chunk.frames)
        .bind(chunk.bytes)
        .bind(&chunk.sha256)
        .bind((self.clock)())
        .execute(&pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// `finished_parts(task_id)`.
    pub async fn finished_parts(&self, task_id: i64) -> Result<Vec<i32>, TaskError> {
        let pool = self.database.get_pool().await?;
        let rows: Vec<(i32,)> = sqlx::query_as("SELECT part_index FROM merge_transcode_parts WHERE task_id = $1")
            .bind(task_id)
            .fetch_all(&pool)
            .await?;
        let mut parts: Vec<i32> = rows.into_iter().map(|(index,)| index).collect();
        // A frozenset: only membership and the count are read.
        parts.sort_unstable();
        parts.dedup();
        Ok(parts)
    }

    /// `finish_part(task_id, part_index)`.
    pub async fn finish_part(&self, task_id: i64, part_index: i32) -> Result<(), TaskError> {
        let pool = self.database.get_pool().await?;
        sqlx::query(
            "
            INSERT INTO merge_transcode_parts (task_id, part_index, finished_at) VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            ",
        )
        .bind(task_id)
        .bind(part_index)
        .bind((self.clock)())
        .execute(&pool)
        .await?;
        Ok(())
    }

    /// `clear(task_id)`: forget everything the worker delivered for the task, in one
    /// transaction.
    pub async fn clear(&self, task_id: i64) -> Result<(), TaskError> {
        let pool = self.database.get_pool().await?;
        let mut transaction = pool.begin().await?;
        sqlx::query("DELETE FROM merge_transcode_chunks WHERE task_id = $1")
            .bind(task_id)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DELETE FROM merge_transcode_parts WHERE task_id = $1")
            .bind(task_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }
}
