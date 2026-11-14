//! The fill actor's PostgreSQL repository: plans, candidates, results, jobs with leases,
//! the move journal and CloudDrive move operations.
//!
//! Python origin: `src/embyx_manager/fill_actor/postgres_repository.py`. Every statement,
//! row lock, compare-and-set and transaction boundary is Python's, so either build resumes
//! rows the other wrote (cross-version contract 3): read-modify-write paths lock rows with
//! `FOR UPDATE`, queue admission takes the transaction-scoped enqueue advisory lock so the
//! active-job cap holds across processes, and a lease is honoured only while it has not
//! expired. Stored JSON is written byte for byte as Python writes it (contract 2): the
//! public plan and results with pydantic's `model_dump_json()`, the id lists with
//! `json.dumps`, the CloudDrive snapshot in the canonical sorted compact form
//! (`goldens/scenarios/fill_actor/repository.json`).

use std::fmt;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use embyx_core::config::lax::ValidationError;
use embyx_core::pyjson::{self, DumpOptions, Value};
use embyx_core::pypath::PyPath;
use embyx_core::pytext::py_repr;
use embyx_db::database::{ADVISORY_NAMESPACE, Database, DatabaseError, ENQUEUE_LOCK_KEY};
use embyx_monitor::support::StoreError;
use sqlx::postgres::{PgQueryResult, PgRow};
use sqlx::{PgConnection, Row as _};

use crate::cloud_moves::{CheckError, CloudFileMetadata};
use crate::models::{ApplyResult, FillActorPlan, MoveResult};
use crate::records::{
    ApplyJobRecord, CancelJobOutcome, CancelJobResult, CandidateKind, CandidateRecord, CloudMoveOperationRecord,
    CloudMoveOperationState, EnqueueApplyJobOutcome, EnqueueApplyJobResult, FileFingerprint, JOB_CANCELLED_ERROR_CODE,
    JobOperation, JobProgress, JobProgressUnit, JobRecord, JobStage, JobState, MoveJournalRecord, MoveJournalState,
    PlanRecord, RecordError, TransitionError, normalize_datetime, validate_cloud_move_transition,
    validate_journal_transition,
};

/// Why a repository call failed, by the Python exception it stands for.
#[derive(Debug)]
pub enum RepositoryError {
    /// A database failure (asyncpg's classes, DIVERGENCES.md D61), a stored value that does
    /// not decode, or an argument the column cannot hold (`DataError`).
    Store(StoreError),
    /// `KeyError((plan_id, candidate_id))`: no such candidate.
    MissingCandidate { plan_id: String, candidate_id: String },
    /// `ValueError(message)` raised by the repository itself.
    Value(&'static str),
    /// A record's `__post_init__` refused it (`ValueError`).
    Record(RecordError),
    /// `InvalidMoveJournalTransitionError` / `InvalidCloudMoveTransitionError`.
    Transition(TransitionError),
    /// A stored model that does not validate (pydantic's `ValidationError`).
    Validation(ValidationError),
    /// A stored CloudDrive snapshot that `CloudFileMetadata.from_mapping` refuses.
    Check(CheckError),
    /// `RuntimeError(message)`: a compare-and-set the repository's own lock should make
    /// impossible did not hold.
    Runtime(&'static str),
}

impl RepositoryError {
    /// `type(exc).__name__`.
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Store(error) => error.class_name(),
            Self::MissingCandidate { .. } => "KeyError",
            Self::Value(_) | Self::Record(_) => "ValueError",
            Self::Transition(error) => error.class_name(),
            Self::Validation(_) => ValidationError::CLASS,
            Self::Check(error) => error.class_name(),
            Self::Runtime(_) => "RuntimeError",
        }
    }

    /// `isinstance(exc, ValueError)`: what the service catches around a save
    /// (`ValidationError` is one too).
    pub fn is_value_error(&self) -> bool {
        match self {
            Self::Value(_) | Self::Record(_) | Self::Transition(_) | Self::Validation(_) => true,
            Self::Check(error) => error.is_value_error(),
            Self::Store(StoreError::Decode { class, .. }) => *class != "TypeError",
            Self::Store(_) | Self::MissingCandidate { .. } | Self::Runtime(_) => false,
        }
    }
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => error.fmt(f),
            Self::MissingCandidate { plan_id, candidate_id } => {
                write!(f, "({}, {})", py_repr(plan_id), py_repr(candidate_id))
            }
            Self::Value(message) | Self::Runtime(message) => f.write_str(message),
            Self::Record(error) => error.fmt(f),
            Self::Transition(error) => error.fmt(f),
            Self::Validation(error) => error.fmt(f),
            Self::Check(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for RepositoryError {}

impl From<sqlx::Error> for RepositoryError {
    fn from(error: sqlx::Error) -> Self {
        Self::Store(StoreError::from(error))
    }
}

impl From<DatabaseError> for RepositoryError {
    fn from(error: DatabaseError) -> Self {
        Self::Store(StoreError::Database(error))
    }
}

impl From<RecordError> for RepositoryError {
    fn from(error: RecordError) -> Self {
        Self::Record(error)
    }
}

impl From<TransitionError> for RepositoryError {
    fn from(error: TransitionError) -> Self {
        Self::Transition(error)
    }
}

impl From<ValidationError> for RepositoryError {
    fn from(error: ValidationError) -> Self {
        Self::Validation(error)
    }
}

impl From<CheckError> for RepositoryError {
    fn from(error: CheckError) -> Self {
        Self::Check(error)
    }
}

type Result<T, E = RepositoryError> = std::result::Result<T, E>;

/// `PostgresFillActorRepository`.
#[derive(Debug, Clone)]
pub struct PostgresFillActorRepository {
    database: Database,
}

const APPLY_JOB_COLUMNS: &str = "fill_actor_jobs.*,
                   fill_actor_apply_jobs.revision,
                   fill_actor_apply_jobs.candidate_ids_json,
                   fill_actor_apply_jobs.result_json";

impl PostgresFillActorRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    async fn pool(&self) -> Result<sqlx::PgPool> {
        Ok(self.database.get_pool().await?)
    }

    /// `save_plan(record)`: the plan and its candidates in one transaction; candidates the
    /// plan no longer has are deleted.
    pub async fn save_plan(&self, record: &PlanRecord) -> Result<()> {
        // Python's records refuse themselves when built: each candidate, then the plan.
        for candidate in &record.candidates {
            candidate.validate()?;
        }
        record.validate()?;
        let public = &record.public;
        let rows = record
            .candidates
            .iter()
            .map(CandidateRow::from_record)
            .collect::<Result<Vec<_>>>()?;
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        sqlx::query(
            "
                INSERT INTO fill_actor_plans (
                    plan_id, revision, public_json, created_at, expires_at, config_version
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (plan_id) DO UPDATE SET
                    revision = excluded.revision,
                    public_json = excluded.public_json,
                    created_at = excluded.created_at,
                    expires_at = excluded.expires_at,
                    config_version = excluded.config_version
                ",
        )
        .bind(&public.plan_id)
        .bind(&public.revision)
        .bind(public.dump_json())
        .bind(normalize_datetime(public.created_at.to_utc()))
        .bind(normalize_datetime(public.expires_at.to_utc()))
        .bind(record.config_version)
        .execute(&mut *transaction)
        .await?;
        let candidate_ids: Vec<&str> = record
            .candidates
            .iter()
            .map(|candidate| candidate.candidate_id.as_str())
            .collect();
        sqlx::query("DELETE FROM fill_actor_candidates WHERE plan_id = $1 AND candidate_id != ALL($2::text[])")
            .bind(&public.plan_id)
            .bind(&candidate_ids)
            .execute(&mut *transaction)
            .await?;
        for (candidate, row) in record.candidates.iter().zip(rows) {
            let fingerprint = candidate.fingerprint;
            sqlx::query(
                "
                    INSERT INTO fill_actor_candidates (
                        plan_id, candidate_id, video_id, source_path, source_root, destination_path,
                        fingerprint_device, fingerprint_inode, fingerprint_size,
                        fingerprint_mtime_ns, fingerprint_ctime_ns, candidate_kind,
                        mapping_sha256, cloud_source_path, cloud_destination_dir, cloud_file_json
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                    ON CONFLICT (plan_id, candidate_id) DO UPDATE SET
                        video_id = excluded.video_id,
                        source_path = excluded.source_path,
                        source_root = excluded.source_root,
                        destination_path = excluded.destination_path,
                        fingerprint_device = excluded.fingerprint_device,
                        fingerprint_inode = excluded.fingerprint_inode,
                        fingerprint_size = excluded.fingerprint_size,
                        fingerprint_mtime_ns = excluded.fingerprint_mtime_ns,
                        fingerprint_ctime_ns = excluded.fingerprint_ctime_ns,
                        candidate_kind = excluded.candidate_kind,
                        mapping_sha256 = excluded.mapping_sha256,
                        cloud_source_path = excluded.cloud_source_path,
                        cloud_destination_dir = excluded.cloud_destination_dir,
                        cloud_file_json = excluded.cloud_file_json
                    ",
            )
            .bind(&public.plan_id)
            .bind(&candidate.candidate_id)
            .bind(&candidate.video_id)
            .bind(row.source)
            .bind(row.source_root)
            .bind(row.destination)
            .bind(fingerprint.device)
            .bind(fingerprint.inode)
            .bind(fingerprint.size)
            .bind(fingerprint.mtime_ns)
            .bind(fingerprint.ctime_ns)
            .bind(candidate.kind.as_str())
            .bind(&candidate.mapping_sha256)
            .bind(&candidate.cloud_source_path)
            .bind(&candidate.cloud_destination_dir)
            .bind(candidate.cloud_file.as_ref().map(cloud_file_to_json))
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// `get_plan(plan_id)`.
    pub async fn get_plan(&self, plan_id: &str) -> Result<Option<PlanRecord>> {
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        let row = sqlx::query("SELECT public_json, config_version FROM fill_actor_plans WHERE plan_id = $1")
            .bind(plan_id)
            .fetch_optional(&mut *transaction)
            .await?;
        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(None);
        };
        let candidate_rows =
            sqlx::query("SELECT * FROM fill_actor_candidates WHERE plan_id = $1 ORDER BY candidate_id")
                .bind(plan_id)
                .fetch_all(&mut *transaction)
                .await?;
        transaction.commit().await?;
        let public_json: String = row.try_get("public_json")?;
        let record = PlanRecord {
            public: FillActorPlan::validate_json(&public_json)?,
            candidates: candidate_rows
                .iter()
                .map(candidate_from_row)
                .collect::<Result<Vec<_>>>()?,
            config_version: row.try_get("config_version")?,
        };
        record.validate()?;
        Ok(Some(record))
    }

    /// `get_candidate(plan_id, candidate_id)`.
    pub async fn get_candidate(&self, plan_id: &str, candidate_id: &str) -> Result<Option<CandidateRecord>> {
        let pool = self.pool().await?;
        let row = sqlx::query("SELECT * FROM fill_actor_candidates WHERE plan_id = $1 AND candidate_id = $2")
            .bind(plan_id)
            .bind(candidate_id)
            .fetch_optional(&pool)
            .await?;
        row.as_ref().map(candidate_from_row).transpose()
    }

    /// `delete_plan(plan_id)`: refused (`false`) while a local move is unreconciled, a
    /// CloudDrive move unresolved or an apply job queued or running; jobs that named the
    /// plan keep their rows without it.
    pub async fn delete_plan(&self, plan_id: &str) -> Result<bool> {
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        let blocked: Option<bool> = sqlx::query_scalar(
            "
                SELECT EXISTS (
                    SELECT 1 FROM fill_actor_move_journal
                    WHERE plan_id = $1 AND state != 'reconciled'
                ) OR EXISTS (
                    SELECT 1 FROM fill_actor_cloud_move_operations
                    WHERE plan_id = $1 AND state NOT IN ('succeeded', 'conflict', 'failed')
                ) OR EXISTS (
                    SELECT 1 FROM fill_actor_jobs
                    WHERE plan_id = $1 AND operation = 'apply' AND state IN ('queued', 'running')
                )
                ",
        )
        .bind(plan_id)
        .fetch_one(&mut *transaction)
        .await?;
        if blocked == Some(true) {
            transaction.commit().await?;
            return Ok(false);
        }
        sqlx::query("UPDATE fill_actor_jobs SET plan_id = NULL WHERE plan_id = $1")
            .bind(plan_id)
            .execute(&mut *transaction)
            .await?;
        let status = sqlx::query("DELETE FROM fill_actor_plans WHERE plan_id = $1")
            .bind(plan_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(status.rows_affected() > 0)
    }

    /// `purge_expired_plans(now)`: delete every expired plan nothing still depends on; how
    /// many.
    pub async fn purge_expired_plans(&self, now: DateTime<Utc>) -> Result<usize> {
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        let expired: Vec<String> = sqlx::query_scalar(
            "
                SELECT plan_id FROM fill_actor_plans
                WHERE expires_at <= $1
                  AND NOT EXISTS (
                      SELECT 1 FROM fill_actor_move_journal
                      WHERE fill_actor_move_journal.plan_id = fill_actor_plans.plan_id
                        AND fill_actor_move_journal.state != 'reconciled'
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM fill_actor_cloud_move_operations
                      WHERE fill_actor_cloud_move_operations.plan_id = fill_actor_plans.plan_id
                        AND fill_actor_cloud_move_operations.state NOT IN ('succeeded', 'conflict', 'failed')
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM fill_actor_jobs
                      WHERE fill_actor_jobs.plan_id = fill_actor_plans.plan_id
                        AND fill_actor_jobs.operation = 'apply'
                        AND fill_actor_jobs.state IN ('queued', 'running')
                  )
                FOR UPDATE
                ",
        )
        .bind(normalize_datetime(now))
        .fetch_all(&mut *transaction)
        .await?;
        if expired.is_empty() {
            transaction.commit().await?;
            return Ok(0);
        }
        sqlx::query("UPDATE fill_actor_jobs SET plan_id = NULL WHERE plan_id = ANY($1::text[])")
            .bind(&expired)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DELETE FROM fill_actor_plans WHERE plan_id = ANY($1::text[])")
            .bind(&expired)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(expired.len())
    }

    /// `save_move_result(plan_id, result)`.
    pub async fn save_move_result(&self, plan_id: &str, result: &MoveResult) -> Result<()> {
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        insert_move_result(&mut transaction, plan_id, result).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// `get_move_result(plan_id, candidate_id)`.
    pub async fn get_move_result(&self, plan_id: &str, candidate_id: &str) -> Result<Option<MoveResult>> {
        let pool = self.pool().await?;
        let value: Option<String> = sqlx::query_scalar(
            "SELECT result_json FROM fill_actor_move_results WHERE plan_id = $1 AND candidate_id = $2",
        )
        .bind(plan_id)
        .bind(candidate_id)
        .fetch_optional(&pool)
        .await?;
        Ok(value.map(|text| MoveResult::validate_json(&text)).transpose()?)
    }

    /// `list_move_results(plan_id)`, by candidate id.
    pub async fn list_move_results(&self, plan_id: &str) -> Result<Vec<MoveResult>> {
        let pool = self.pool().await?;
        let values: Vec<String> = sqlx::query_scalar(
            "SELECT result_json FROM fill_actor_move_results WHERE plan_id = $1 ORDER BY candidate_id",
        )
        .bind(plan_id)
        .fetch_all(&pool)
        .await?;
        Ok(values
            .iter()
            .map(|text| MoveResult::validate_json(text))
            .collect::<Result<Vec<_>, _>>()?)
    }

    /// `save_job(record)`: insert or replace the job row.
    pub async fn save_job(&self, record: &JobRecord) -> Result<()> {
        let pool = self.pool().await?;
        let mut connection = pool.acquire().await?;
        execute_save_job(&mut connection, record).await
    }

    /// `enqueue_job(record, max_active=)`: insert the job unless the queue is full or the id
    /// is taken.
    pub async fn enqueue_job(&self, record: &JobRecord, max_active: i64) -> Result<bool> {
        record.progress.validate()?;
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        lock_enqueue(&mut transaction).await?;
        let active = count_active(&mut transaction).await?;
        let exists: Option<i32> = sqlx::query_scalar("SELECT 1 FROM fill_actor_jobs WHERE job_id = $1")
            .bind(&record.job_id)
            .fetch_optional(&mut *transaction)
            .await?;
        if active >= max_active || exists.is_some() {
            transaction.commit().await?;
            return Ok(false);
        }
        execute_save_job(&mut transaction, record).await?;
        transaction.commit().await?;
        Ok(true)
    }

    /// `enqueue_apply_job(record, max_active=)`: an apply request is idempotent by job id;
    /// the same id with another plan, revision or selection is a conflict.
    pub async fn enqueue_apply_job(&self, record: &ApplyJobRecord, max_active: i64) -> Result<EnqueueApplyJobResult> {
        // Built in Python's order: the progress, then the apply record.
        record.job.progress.validate()?;
        record.validate()?;
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        lock_enqueue(&mut transaction).await?;
        let existing = sqlx::query(sqlx::AssertSqlSafe(format!(
            "
                SELECT {APPLY_JOB_COLUMNS}
                FROM fill_actor_jobs
                LEFT JOIN fill_actor_apply_jobs ON fill_actor_apply_jobs.job_id = fill_actor_jobs.job_id
                WHERE fill_actor_jobs.job_id = $1
                "
        )))
        .bind(&record.job.job_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(existing) = existing {
            let revision: Option<String> = existing.try_get("revision")?;
            let plan_id: Option<String> = existing.try_get("plan_id")?;
            let same = match &revision {
                Some(revision) => {
                    plan_id == record.job.plan_id
                        && *revision == record.revision
                        && id_list(&existing, "candidate_ids_json")? == record.candidate_ids
                }
                None => false,
            };
            let result = if same {
                EnqueueApplyJobResult {
                    outcome: EnqueueApplyJobOutcome::Existing,
                    record: Some(apply_job_from_row(&existing)?),
                }
            } else {
                EnqueueApplyJobResult {
                    outcome: EnqueueApplyJobOutcome::Conflict,
                    record: None,
                }
            };
            transaction.commit().await?;
            return Ok(result);
        }
        let active = count_active(&mut transaction).await?;
        if active >= max_active {
            transaction.commit().await?;
            return Ok(EnqueueApplyJobResult {
                outcome: EnqueueApplyJobOutcome::Full,
                record: None,
            });
        }
        execute_save_job(&mut transaction, &record.job).await?;
        sqlx::query(
            "
                INSERT INTO fill_actor_apply_jobs (job_id, revision, candidate_ids_json, result_json)
                VALUES ($1, $2, $3, $4)
                ",
        )
        .bind(&record.job.job_id)
        .bind(&record.revision)
        .bind(json_id_list(&record.candidate_ids))
        .bind(record.result.as_ref().map(ApplyResult::dump_json))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(EnqueueApplyJobResult {
            outcome: EnqueueApplyJobOutcome::Enqueued,
            record: Some(record.clone()),
        })
    }

    /// `get_apply_job(job_id)`.
    pub async fn get_apply_job(&self, job_id: &str) -> Result<Option<ApplyJobRecord>> {
        let pool = self.pool().await?;
        let row = fetch_apply_job(&pool, job_id).await?;
        row.as_ref().map(apply_job_from_row).transpose()
    }

    /// `claim_next_job(owner_id=, now=, lease_expires_at=)`: the oldest queued job, taken
    /// with a lease; jobs another process is claiming are skipped.
    pub async fn claim_next_job(
        &self,
        owner_id: &str,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<JobRecord>> {
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        let row = sqlx::query(
            "
                SELECT * FROM fill_actor_jobs WHERE state = 'queued'
                ORDER BY created_at, job_id LIMIT 1
                FOR UPDATE SKIP LOCKED
                ",
        )
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(None);
        };
        let job_id: String = row.try_get("job_id")?;
        let (now, lease_expires_at) = (normalize_datetime(now), normalize_datetime(lease_expires_at));
        sqlx::query(
            "
                UPDATE fill_actor_jobs
                SET state = 'running', updated_at = $1, owner_id = $2, lease_expires_at = $3
                WHERE job_id = $4 AND state = 'queued'
                ",
        )
        .bind(now)
        .bind(owner_id)
        .bind(lease_expires_at)
        .bind(&job_id)
        .execute(&mut *transaction)
        .await?;
        let record = JobRecord {
            job_id,
            operation: operation_of(&row)?,
            state: JobState::Running,
            created_at: row.try_get("created_at")?,
            updated_at: now,
            plan_id: row.try_get("plan_id")?,
            error_code: row.try_get("error_code")?,
            owner_id: Some(owner_id.to_owned()),
            lease_expires_at: Some(lease_expires_at),
            actor_ids: id_list(&row, "actor_ids_json")?,
            progress: progress_from_row(&row)?,
        };
        transaction.commit().await?;
        Ok(Some(record))
    }

    /// `renew_owned_job_lease(...)`: extend the lease while it is still held.
    pub async fn renew_owned_job_lease(
        &self,
        job_id: &str,
        owner_id: &str,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<bool> {
        let pool = self.pool().await?;
        let status = sqlx::query(
            "
            UPDATE fill_actor_jobs SET updated_at = $1, lease_expires_at = $2
            WHERE job_id = $3 AND owner_id = $4 AND state = 'running'
              AND lease_expires_at IS NOT NULL AND lease_expires_at > $1
            ",
        )
        .bind(normalize_datetime(now))
        .bind(normalize_datetime(lease_expires_at))
        .bind(job_id)
        .bind(owner_id)
        .execute(&pool)
        .await?;
        Ok(status.rows_affected() == 1)
    }

    /// `update_owned_job_progress(...)`: record progress while the lease is still held;
    /// the lease and `updated_at` are left alone.
    pub async fn update_owned_job_progress(
        &self,
        job_id: &str,
        owner_id: &str,
        progress: &JobProgress,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        progress.validate()?;
        let values = ProgressValues::new(progress, 1)?;
        let pool = self.pool().await?;
        let status = values
            .bind(sqlx::query(
                "
            UPDATE fill_actor_jobs SET
                progress_stage = $1, progress_completed = $2, progress_total = $3, progress_unit = $4,
                progress_current = $5, progress_stage_started_at = $6, progress_updated_at = $7
            WHERE job_id = $8 AND owner_id = $9 AND state = 'running'
              AND lease_expires_at IS NOT NULL AND lease_expires_at > $10
            ",
            ))
            .bind(job_id)
            .bind(owner_id)
            .bind(normalize_datetime(now))
            .execute(&pool)
            .await?;
        Ok(status.rows_affected() == 1)
    }

    /// `finish_owned_job(...)`: the terminal state, if the lease is still held; an apply
    /// job's result is stored in the same transaction, and a result its job cannot carry
    /// undoes the finish.
    #[allow(clippy::too_many_arguments, reason = "Python's keyword arguments, one for one")]
    pub async fn finish_owned_job(
        &self,
        job_id: &str,
        owner_id: &str,
        state: JobState,
        error_code: Option<&str>,
        now: DateTime<Utc>,
        progress: &JobProgress,
        apply_result: Option<&ApplyResult>,
    ) -> Result<bool> {
        progress.validate()?;
        let values = ProgressValues::new(progress, 4)?;
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        let now = normalize_datetime(now);
        let query = sqlx::query(
            "
                UPDATE fill_actor_jobs SET
                    state = $1, updated_at = $2, error_code = $3, owner_id = NULL, lease_expires_at = NULL,
                    progress_stage = $4, progress_completed = $5, progress_total = $6, progress_unit = $7,
                    progress_current = $8, progress_stage_started_at = $9, progress_updated_at = $10
                WHERE job_id = $11 AND owner_id = $12 AND state = 'running'
                  AND lease_expires_at IS NOT NULL AND lease_expires_at > $2
                ",
        )
        .bind(state.as_str())
        .bind(now)
        .bind(error_code);
        let status = values
            .bind(query)
            .bind(job_id)
            .bind(owner_id)
            .execute(&mut *transaction)
            .await?;
        if status.rows_affected() != 1 {
            transaction.commit().await?;
            return Ok(false);
        }
        if let Some(apply_result) = apply_result {
            // Any error from here on drops the transaction, which rolls the finish back.
            let Some(apply_row) = fetch_apply_job(&mut *transaction, job_id).await? else {
                return Err(RepositoryError::Value("apply result requires an apply job"));
            };
            ApplyJobRecord {
                job: job_from_row(&apply_row)?,
                revision: apply_row.try_get("revision")?,
                candidate_ids: id_list(&apply_row, "candidate_ids_json")?,
                result: Some(apply_result.clone()),
            }
            .validate()?;
            let result_status = sqlx::query("UPDATE fill_actor_apply_jobs SET result_json = $1 WHERE job_id = $2")
                .bind(apply_result.dump_json())
                .bind(job_id)
                .execute(&mut *transaction)
                .await?;
            if result_status.rows_affected() != 1 {
                return Err(RepositoryError::Value("apply result update was not atomic"));
            }
        }
        transaction.commit().await?;
        Ok(true)
    }

    /// `cancel_job(job_id=, now=)`: fail a queued or running scan job as cancelled. Apply
    /// jobs cannot be cancelled; a job already over says how it ended.
    pub async fn cancel_job(&self, job_id: &str, now: DateTime<Utc>) -> Result<CancelJobResult> {
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        let row = sqlx::query("SELECT * FROM fill_actor_jobs WHERE job_id = $1 FOR UPDATE")
            .bind(job_id)
            .fetch_optional(&mut *transaction)
            .await?;
        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(CancelJobResult {
                outcome: CancelJobOutcome::NotFound,
                job: None,
                previous_state: None,
                previous_owner_id: None,
            });
        };
        let current = job_from_row(&row)?;
        let unchanged = |outcome| CancelJobResult {
            outcome,
            previous_state: Some(current.state),
            previous_owner_id: current.owner_id.clone(),
            job: Some(current.clone()),
        };
        let refused = if current.operation == JobOperation::Apply {
            Some(unchanged(CancelJobOutcome::NotCancellable))
        } else if current.state == JobState::Failed && current.error_code.as_deref() == Some(JOB_CANCELLED_ERROR_CODE) {
            Some(unchanged(CancelJobOutcome::AlreadyCancelled))
        } else if !matches!(current.state, JobState::Queued | JobState::Running) {
            Some(unchanged(CancelJobOutcome::AlreadyTerminal))
        } else {
            None
        };
        if let Some(result) = refused {
            transaction.commit().await?;
            return Ok(result);
        }
        let now = normalize_datetime(now);
        let progress = terminal_progress(&current, now);
        let values = ProgressValues::new(&progress, 3)?;
        let query = sqlx::query(
            "
                UPDATE fill_actor_jobs SET
                    state = 'failed', updated_at = $1, error_code = $2, owner_id = NULL, lease_expires_at = NULL,
                    progress_stage = $3, progress_completed = $4, progress_total = $5, progress_unit = $6,
                    progress_current = $7, progress_stage_started_at = $8, progress_updated_at = $9
                WHERE job_id = $10 AND state IN ('queued', 'running')
                ",
        )
        .bind(now)
        .bind(JOB_CANCELLED_ERROR_CODE);
        let status: PgQueryResult = values.bind(query).bind(job_id).execute(&mut *transaction).await?;
        if status.rows_affected() != 1 {
            // FOR UPDATE serializes competing writers; Python raises RuntimeError here.
            return Err(RepositoryError::Runtime("cancelled job transition was not atomic"));
        }
        let cancelled = sqlx::query("SELECT * FROM fill_actor_jobs WHERE job_id = $1")
            .bind(job_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(RepositoryError::Runtime("cancelled job disappeared"))?;
        let job = job_from_row(&cancelled)?;
        transaction.commit().await?;
        Ok(CancelJobResult {
            outcome: CancelJobOutcome::Cancelled,
            job: Some(job),
            previous_state: Some(current.state),
            previous_owner_id: current.owner_id,
        })
    }

    /// `fail_expired_jobs(now=, error_code=)`: fail every running job whose lease is gone;
    /// how many.
    pub async fn fail_expired_jobs(&self, now: DateTime<Utc>, error_code: &str) -> Result<usize> {
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        let rows: Vec<String> = sqlx::query_scalar(
            "
                UPDATE fill_actor_jobs
                SET state = 'failed', updated_at = $1, error_code = $2, owner_id = NULL, lease_expires_at = NULL,
                    progress_stage = 'done',
                    progress_current = CASE WHEN operation = 'apply' THEN NULL ELSE progress_current END,
                    progress_stage_started_at = $1, progress_updated_at = $1
                WHERE state = 'running' AND (lease_expires_at IS NULL OR lease_expires_at <= $1)
                RETURNING job_id
                ",
        )
        .bind(normalize_datetime(now))
        .bind(error_code)
        .fetch_all(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(rows.len())
    }

    /// `get_job(job_id)`.
    pub async fn get_job(&self, job_id: &str) -> Result<Option<JobRecord>> {
        let pool = self.pool().await?;
        let row = sqlx::query("SELECT * FROM fill_actor_jobs WHERE job_id = $1")
            .bind(job_id)
            .fetch_optional(&pool)
            .await?;
        row.as_ref().map(job_from_row).transpose()
    }

    /// `list_jobs(states=None)`: oldest first.
    pub async fn list_jobs(&self, states: Option<&[JobState]>) -> Result<Vec<JobRecord>> {
        let pool = self.pool().await?;
        let rows = match states {
            None => {
                sqlx::query("SELECT * FROM fill_actor_jobs ORDER BY created_at, job_id")
                    .fetch_all(&pool)
                    .await?
            }
            Some(states) => {
                let values: Vec<&str> = states.iter().map(|state| state.as_str()).collect();
                sqlx::query("SELECT * FROM fill_actor_jobs WHERE state = ANY($1::text[]) ORDER BY created_at, job_id")
                    .bind(&values)
                    .fetch_all(&pool)
                    .await?
            }
        };
        rows.iter().map(job_from_row).collect()
    }

    /// `save_move_journal(record)`: advance a local move's journal entry, only along the
    /// allowed transitions.
    pub async fn save_move_journal(&self, record: &MoveJournalRecord) -> Result<()> {
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        let candidate: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM fill_actor_candidates WHERE plan_id = $1 AND candidate_id = $2 FOR UPDATE",
        )
        .bind(&record.plan_id)
        .bind(&record.candidate_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if candidate.is_none() {
            return Err(missing(&record.plan_id, &record.candidate_id));
        }
        let current_value: Option<String> = sqlx::query_scalar(
            "SELECT state FROM fill_actor_move_journal WHERE plan_id = $1 AND candidate_id = $2 FOR UPDATE",
        )
        .bind(&record.plan_id)
        .bind(&record.candidate_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let current = current_value
            .map(|value| parse_enum(&value, "MoveJournalState", MoveJournalState::parse))
            .transpose()?;
        validate_journal_transition(current, record.state)?;
        sqlx::query(
            "
                INSERT INTO fill_actor_move_journal (plan_id, candidate_id, state, updated_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (plan_id, candidate_id) DO UPDATE SET
                    state = excluded.state,
                    updated_at = excluded.updated_at
                ",
        )
        .bind(&record.plan_id)
        .bind(&record.candidate_id)
        .bind(record.state.as_str())
        .bind(normalize_datetime(record.updated_at))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// `get_move_journal(plan_id, candidate_id)`.
    pub async fn get_move_journal(&self, plan_id: &str, candidate_id: &str) -> Result<Option<MoveJournalRecord>> {
        let pool = self.pool().await?;
        let row = sqlx::query("SELECT * FROM fill_actor_move_journal WHERE plan_id = $1 AND candidate_id = $2")
            .bind(plan_id)
            .bind(candidate_id)
            .fetch_optional(&pool)
            .await?;
        row.as_ref().map(journal_from_row).transpose()
    }

    /// `list_unreconciled_moves()`: the local moves start-up must finish, oldest first.
    pub async fn list_unreconciled_moves(&self) -> Result<Vec<MoveJournalRecord>> {
        let pool = self.pool().await?;
        let rows = sqlx::query(
            "
            SELECT * FROM fill_actor_move_journal
            WHERE state != 'reconciled'
            ORDER BY updated_at, plan_id, candidate_id
            ",
        )
        .fetch_all(&pool)
        .await?;
        rows.iter().map(journal_from_row).collect()
    }

    /// `save_cloud_move_operation(record)`: advance a CloudDrive move that is not over yet
    /// (a terminal state goes through [`Self::finalize_cloud_move`] with its result).
    pub async fn save_cloud_move_operation(&self, record: &CloudMoveOperationRecord) -> Result<()> {
        // `__post_init__` runs before the repository sees the record.
        record.validate()?;
        if record.state.terminal() {
            return Err(RepositoryError::Value(
                "terminal CloudDrive operations must be finalized with their result",
            ));
        }
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        execute_save_cloud_move_operation(&mut transaction, record).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// `get_cloud_move_operation(plan_id, candidate_id)`.
    pub async fn get_cloud_move_operation(
        &self,
        plan_id: &str,
        candidate_id: &str,
    ) -> Result<Option<CloudMoveOperationRecord>> {
        let pool = self.pool().await?;
        let row =
            sqlx::query("SELECT * FROM fill_actor_cloud_move_operations WHERE plan_id = $1 AND candidate_id = $2")
                .bind(plan_id)
                .bind(candidate_id)
                .fetch_optional(&pool)
                .await?;
        row.as_ref().map(cloud_move_operation_from_row).transpose()
    }

    /// `list_unresolved_cloud_moves()`: the CloudDrive moves start-up must resolve, oldest
    /// first.
    pub async fn list_unresolved_cloud_moves(&self) -> Result<Vec<CloudMoveOperationRecord>> {
        let pool = self.pool().await?;
        let rows = sqlx::query(
            "
            SELECT * FROM fill_actor_cloud_move_operations
            WHERE state NOT IN ('succeeded', 'conflict', 'failed')
            ORDER BY updated_at, plan_id, candidate_id
            ",
        )
        .fetch_all(&pool)
        .await?;
        rows.iter().map(cloud_move_operation_from_row).collect()
    }

    /// `finalize_cloud_move(operation, result)`: the terminal state and the move's result,
    /// in one transaction.
    pub async fn finalize_cloud_move(&self, operation: &CloudMoveOperationRecord, result: &MoveResult) -> Result<()> {
        operation.validate()?;
        if !operation.state.terminal() {
            return Err(RepositoryError::Value(
                "finalized CloudDrive operation must be terminal",
            ));
        }
        if operation.candidate_id != result.candidate_id {
            return Err(RepositoryError::Value("CloudDrive result must match its operation"));
        }
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        execute_save_cloud_move_operation(&mut transaction, operation).await?;
        insert_move_result(&mut transaction, &operation.plan_id, result).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// `health_check()`: the readiness probe (`embyx_db::health`, shared with the shell).
    pub async fn health_check(&self) -> Result<bool> {
        Ok(embyx_db::health::health_check(&self.database).await?)
    }
}

// ---------- shared statements ----------

fn missing(plan_id: &str, candidate_id: &str) -> RepositoryError {
    RepositoryError::MissingCandidate {
        plan_id: plan_id.to_owned(),
        candidate_id: candidate_id.to_owned(),
    }
}

async fn lock_enqueue(connection: &mut PgConnection) -> Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind(ADVISORY_NAMESPACE)
        .bind(ENQUEUE_LOCK_KEY)
        .execute(&mut *connection)
        .await?;
    Ok(())
}

async fn count_active(connection: &mut PgConnection) -> Result<i64> {
    let active: Option<i64> =
        sqlx::query_scalar("SELECT COUNT(*) FROM fill_actor_jobs WHERE state IN ('queued', 'running')")
            .fetch_one(&mut *connection)
            .await?;
    Ok(active.unwrap_or(0))
}

async fn fetch_apply_job<'e, E>(executor: E, job_id: &str) -> Result<Option<PgRow>>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    Ok(sqlx::query(sqlx::AssertSqlSafe(format!(
        "
            SELECT {APPLY_JOB_COLUMNS}
            FROM fill_actor_apply_jobs
            JOIN fill_actor_jobs ON fill_actor_jobs.job_id = fill_actor_apply_jobs.job_id
            WHERE fill_actor_apply_jobs.job_id = $1
            "
    )))
    .bind(job_id)
    .fetch_optional(executor)
    .await?)
}

/// `_insert_move_result`: the candidate must exist and be for the result's video.
async fn insert_move_result(connection: &mut PgConnection, plan_id: &str, result: &MoveResult) -> Result<()> {
    let video_id: Option<String> =
        sqlx::query_scalar("SELECT video_id FROM fill_actor_candidates WHERE plan_id = $1 AND candidate_id = $2")
            .bind(plan_id)
            .bind(&result.candidate_id)
            .fetch_optional(&mut *connection)
            .await?;
    let Some(video_id) = video_id else {
        return Err(missing(plan_id, &result.candidate_id));
    };
    if video_id != result.video_id {
        return Err(RepositoryError::Value("move result video id does not match candidate"));
    }
    sqlx::query(
        "
            INSERT INTO fill_actor_move_results (plan_id, candidate_id, result_json)
            VALUES ($1, $2, $3)
            ON CONFLICT (plan_id, candidate_id) DO UPDATE SET result_json = excluded.result_json
            ",
    )
    .bind(plan_id)
    .bind(&result.candidate_id)
    .bind(result.dump_json())
    .execute(&mut *connection)
    .await?;
    Ok(())
}

/// `_execute_save_cloud_move_operation`: the candidate must be the CloudDrive candidate the
/// operation names, the transition allowed and the attempt id unchanged; at most one
/// unresolved operation per CloudDrive source (a unique index).
async fn execute_save_cloud_move_operation(
    connection: &mut PgConnection,
    record: &CloudMoveOperationRecord,
) -> Result<()> {
    let candidate = sqlx::query(
        "
            SELECT candidate_kind, cloud_source_path, cloud_destination_dir
            FROM fill_actor_candidates WHERE plan_id = $1 AND candidate_id = $2
            FOR UPDATE
            ",
    )
    .bind(&record.plan_id)
    .bind(&record.candidate_id)
    .fetch_optional(&mut *connection)
    .await?;
    let Some(candidate) = candidate else {
        return Err(missing(&record.plan_id, &record.candidate_id));
    };
    let kind: String = candidate.try_get("candidate_kind")?;
    if kind != CandidateKind::CloudStrm.as_str() {
        return Err(RepositoryError::Value(
            "CloudDrive operations require a CloudDrive candidate",
        ));
    }
    let source: Option<String> = candidate.try_get("cloud_source_path")?;
    let destination: Option<String> = candidate.try_get("cloud_destination_dir")?;
    if source.as_deref() != Some(record.source_path.as_str())
        || destination.as_deref() != Some(record.destination_dir.as_str())
    {
        return Err(RepositoryError::Value(
            "CloudDrive operation paths must match its candidate",
        ));
    }
    let current_row = sqlx::query(
        "
            SELECT state, attempt_id FROM fill_actor_cloud_move_operations
            WHERE plan_id = $1 AND candidate_id = $2
            FOR UPDATE
            ",
    )
    .bind(&record.plan_id)
    .bind(&record.candidate_id)
    .fetch_optional(&mut *connection)
    .await?;
    let current = match &current_row {
        Some(row) => Some(parse_enum(
            &row.try_get::<String, _>("state")?,
            "CloudMoveOperationState",
            CloudMoveOperationState::parse,
        )?),
        None => None,
    };
    validate_cloud_move_transition(current, record.state)?;
    if let Some(row) = &current_row
        && row.try_get::<String, _>("attempt_id")? != record.attempt_id
    {
        return Err(RepositoryError::Value("CloudDrive operation attempt id cannot change"));
    }
    let inserted = sqlx::query(
        "
                INSERT INTO fill_actor_cloud_move_operations (
                    plan_id, candidate_id, attempt_id, source_path, destination_dir,
                    state, updated_at, error_code
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (plan_id, candidate_id) DO UPDATE SET
                    state = excluded.state,
                    updated_at = excluded.updated_at,
                    error_code = excluded.error_code
                ",
    )
    .bind(&record.plan_id)
    .bind(&record.candidate_id)
    .bind(&record.attempt_id)
    .bind(&record.source_path)
    .bind(&record.destination_dir)
    .bind(record.state.as_str())
    .bind(normalize_datetime(record.updated_at))
    .bind(&record.error_code)
    .execute(&mut *connection)
    .await;
    match inserted {
        Ok(_) => Ok(()),
        Err(error) if StoreError::is_unique_violation(&error) => Err(RepositoryError::Value(
            "CloudDrive source already has an unresolved operation",
        )),
        Err(error) => Err(error.into()),
    }
}

/// `_execute_save_job`: insert or replace every column of the job row.
async fn execute_save_job(connection: &mut PgConnection, record: &JobRecord) -> Result<()> {
    record.progress.validate()?;
    let values = ProgressValues::new(&record.progress, 11)?;
    let query = sqlx::query(
        "
            INSERT INTO fill_actor_jobs (
                job_id, operation, state, created_at, updated_at, plan_id, error_code,
                owner_id, lease_expires_at, actor_ids_json,
                progress_stage, progress_completed, progress_total, progress_unit, progress_current,
                progress_stage_started_at, progress_updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            ON CONFLICT (job_id) DO UPDATE SET
                operation = excluded.operation,
                state = excluded.state,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                plan_id = excluded.plan_id,
                error_code = excluded.error_code,
                owner_id = excluded.owner_id,
                lease_expires_at = excluded.lease_expires_at,
                actor_ids_json = excluded.actor_ids_json,
                progress_stage = excluded.progress_stage,
                progress_completed = excluded.progress_completed,
                progress_total = excluded.progress_total,
                progress_unit = excluded.progress_unit,
                progress_current = excluded.progress_current,
                progress_stage_started_at = excluded.progress_stage_started_at,
                progress_updated_at = excluded.progress_updated_at
            ",
    )
    .bind(&record.job_id)
    .bind(record.operation.as_str())
    .bind(record.state.as_str())
    .bind(normalize_datetime(record.created_at))
    .bind(normalize_datetime(record.updated_at))
    .bind(&record.plan_id)
    .bind(&record.error_code)
    .bind(&record.owner_id)
    .bind(record.lease_expires_at.map(normalize_datetime))
    .bind(json_id_list(&record.actor_ids));
    values.bind(query).execute(&mut *connection).await?;
    Ok(())
}

/// `_progress_values(progress)`: the seven progress columns. `completed` and `total` are
/// `INTEGER` columns; a value outside 32 bits is asyncpg's `DataError` for the argument at
/// its position.
struct ProgressValues {
    stage: &'static str,
    completed: i32,
    total: Option<i32>,
    unit: &'static str,
    current: Option<String>,
    stage_started_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl ProgressValues {
    /// `stage_position`: the stage's argument number (`$N`) in the statement.
    fn new(progress: &JobProgress, stage_position: usize) -> Result<Self> {
        let int32 = |value: i64, position: usize| {
            i32::try_from(value).map_err(|_| {
                RepositoryError::Store(StoreError::Argument(format!(
                    "invalid input for query argument ${position}: {value} (value out of int32 range)"
                )))
            })
        };
        Ok(Self {
            stage: progress.stage.as_str(),
            completed: int32(progress.completed, stage_position + 1)?,
            total: progress
                .total
                .map(|total| int32(total, stage_position + 2))
                .transpose()?,
            unit: progress.unit.as_str(),
            current: progress.current.clone(),
            stage_started_at: normalize_datetime(progress.stage_started_at),
            updated_at: normalize_datetime(progress.updated_at),
        })
    }

    fn bind(
        self,
        query: sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>,
    ) -> sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments> {
        query
            .bind(self.stage)
            .bind(self.completed)
            .bind(self.total)
            .bind(self.unit)
            .bind(self.current)
            .bind(self.stage_started_at)
            .bind(self.updated_at)
    }
}

/// `_terminal_progress(record, now)`: the job's progress marked done; an apply job's current
/// item is cleared.
fn terminal_progress(record: &JobRecord, now: DateTime<Utc>) -> JobProgress {
    let progress = &record.progress;
    JobProgress {
        stage: JobStage::Done,
        completed: progress.completed,
        total: progress.total,
        unit: progress.unit,
        current: if record.operation == JobOperation::Apply {
            None
        } else {
            progress.current.clone()
        },
        stage_started_at: now,
        updated_at: now,
    }
}

// ---------- rows ----------

/// `str(path)` as pathlib writes it, for the three path columns.
struct CandidateRow {
    source: String,
    source_root: String,
    destination: String,
}

/// `str(path)` bound as query argument `$position`. A path that is not UTF-8 is Python's
/// surrogate-escaped `str`, which asyncpg cannot encode: its `DataError` names the argument,
/// the value's `repr` (cut at 40 characters) and the `UnicodeEncodeError` (DIVERGENCES.md
/// D113).
fn path_text(path: &Path, position: usize) -> Result<String> {
    if let Some(text) = path.to_str() {
        return Ok(PyPath::new(text).as_str().to_owned());
    }
    Err(RepositoryError::Store(StoreError::Argument(unencodable_path(
        path, position,
    ))))
}

/// One code point of a surrogate-escaped `str`: a character, or an undecodable byte
/// (`U+DC80`..`U+DCFF`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum PyChar {
    Char(char),
    Escaped(u8),
}

/// `bytes.decode('utf-8', 'surrogateescape')`.
fn surrogateescape(mut bytes: &[u8]) -> Vec<PyChar> {
    let mut chars = Vec::with_capacity(bytes.len());
    while !bytes.is_empty() {
        match std::str::from_utf8(bytes) {
            Ok(text) => {
                chars.extend(text.chars().map(PyChar::Char));
                break;
            }
            Err(error) => {
                let (valid, rest) = bytes.split_at(error.valid_up_to());
                // `valid_up_to` bytes are UTF-8 by the error's own word.
                chars.extend(String::from_utf8_lossy(valid).chars().map(PyChar::Char));
                let bad = error.error_len().unwrap_or(rest.len());
                chars.extend(rest[..bad].iter().map(|&byte| PyChar::Escaped(byte)));
                bytes = &rest[bad..];
            }
        }
    }
    chars
}

/// `str(Path(text))` on the bytes: `/` and `.` are ASCII, so pathlib's normalisation does
/// not depend on the decoding.
fn normalized_path_bytes(bytes: &[u8]) -> Vec<u8> {
    let root: &[u8] = if bytes.starts_with(b"//") && !bytes.starts_with(b"///") {
        b"//"
    } else if bytes.starts_with(b"/") {
        b"/"
    } else {
        b""
    };
    let parts: Vec<&[u8]> = bytes
        .split(|&byte| byte == b'/')
        .filter(|part| !part.is_empty() && *part != b".")
        .collect();
    let mut out = root.to_vec();
    out.extend(parts.join(&b'/'));
    if out.is_empty() {
        out.push(b'.');
    }
    out
}

/// `repr(text)` of a surrogate-escaped `str`.
fn surrogate_repr(chars: &[PyChar]) -> String {
    use std::fmt::Write as _;
    let text: String = chars
        .iter()
        .filter_map(|ch| match ch {
            PyChar::Char(ch) => Some(*ch),
            PyChar::Escaped(_) => None,
        })
        .collect();
    let quoted = py_repr(&text);
    let quote = quoted.chars().next().unwrap_or('\'');
    let mut out = String::with_capacity(quoted.len() + chars.len() * 6);
    out.push(quote);
    for ch in chars {
        match ch {
            PyChar::Escaped(byte) => {
                let _ = write!(out, "\\u{:04x}", 0xdc00 | u32::from(*byte));
            }
            PyChar::Char(ch) if *ch == quote => {
                out.push('\\');
                out.push(*ch);
            }
            PyChar::Char(ch) => {
                // `py_repr` of one character, without its quotes; the quote is handled above.
                let one = py_repr(&ch.to_string());
                out.push_str(&one[1..one.len() - 1]);
            }
        }
    }
    out.push(quote);
    out
}

/// asyncpg's `DataError` text for a path argument that is not UTF-8.
fn unencodable_path(path: &Path, position: usize) -> String {
    use std::os::unix::ffi::OsStrExt as _;
    let chars = surrogateescape(&normalized_path_bytes(path.as_os_str().as_bytes()));
    let mut repr = surrogate_repr(&chars);
    if repr.chars().count() > 40 {
        repr = repr.chars().take(40).collect::<String>() + "...";
    }
    // The UTF-8 encoder reports the first run of consecutive surrogates.
    let start = chars
        .iter()
        .position(|ch| matches!(ch, PyChar::Escaped(_)))
        .unwrap_or_default();
    let run = chars[start..]
        .iter()
        .take_while(|ch| matches!(ch, PyChar::Escaped(_)))
        .count();
    let what = match chars.get(start) {
        Some(PyChar::Escaped(byte)) if run == 1 => {
            format!("character '\\u{:04x}' in position {start}", 0xdc00 | u32::from(*byte))
        }
        _ => format!("characters in position {start}-{}", start + run.max(1) - 1),
    };
    format!(
        "invalid input for query argument ${position}: {repr} ('utf-8' codec can't encode {what}: surrogates not allowed)"
    )
}

impl CandidateRow {
    fn from_record(record: &CandidateRecord) -> Result<Self> {
        Ok(Self {
            source: path_text(&record.source, 4)?,
            source_root: path_text(&record.source_root, 5)?,
            destination: path_text(&record.destination, 6)?,
        })
    }
}

/// `Enum(value)`: `ValueError("'x' is not a valid Enum")` for a value it does not have.
fn parse_enum<E>(value: &str, name: &str, parse: fn(&str) -> Option<E>) -> Result<E> {
    parse(value).ok_or_else(|| {
        RepositoryError::Store(StoreError::Decode {
            class: "ValueError",
            message: format!("{} is not a valid {name}", py_repr(value)),
        })
    })
}

fn operation_of(row: &PgRow) -> Result<JobOperation> {
    parse_enum(
        &row.try_get::<String, _>("operation")?,
        "JobOperation",
        JobOperation::parse,
    )
}

/// `json.dumps(ids)`: a list of strings in Python's default form (`["a", "b"]`).
fn json_id_list(ids: &[String]) -> String {
    let value = Value::List(ids.iter().map(|id| Value::Str(id.clone())).collect());
    pyjson::dumps(&value, &DumpOptions::python_default()).expect("a list of strings always dumps")
}

/// `tuple(json.loads(row[column]))` for the id lists. A stored value that is not a list of
/// strings is refused rather than taken apart (DIVERGENCES.md D62, D112).
fn id_list(row: &PgRow, column: &str) -> Result<Vec<String>> {
    let text: String = row.try_get(column)?;
    let decode = |class: &'static str, message: String| RepositoryError::Store(StoreError::Decode { class, message });
    let value = pyjson::loads(&text).map_err(|error| decode("JSONDecodeError", error.python_message(&text)))?;
    let Value::List(items) = value else {
        return Err(decode("TypeError", format!("{column} is not a JSON list")));
    };
    items
        .into_iter()
        .map(|item| match item {
            Value::Str(text) => Ok(text),
            _ => Err(decode(
                "TypeError",
                format!("{column} holds a value that is not a string"),
            )),
        })
        .collect()
}

/// `_candidate_from_row(row)`.
fn candidate_from_row(row: &PgRow) -> Result<CandidateRecord> {
    let cloud_file_json: Option<String> = row.try_get("cloud_file_json")?;
    let record = CandidateRecord {
        candidate_id: row.try_get("candidate_id")?,
        video_id: row.try_get("video_id")?,
        source: PathBuf::from(row.try_get::<String, _>("source_path")?),
        source_root: PathBuf::from(row.try_get::<String, _>("source_root")?),
        destination: PathBuf::from(row.try_get::<String, _>("destination_path")?),
        fingerprint: FileFingerprint {
            device: row.try_get("fingerprint_device")?,
            inode: row.try_get("fingerprint_inode")?,
            size: row.try_get("fingerprint_size")?,
            mtime_ns: row.try_get("fingerprint_mtime_ns")?,
            ctime_ns: row.try_get("fingerprint_ctime_ns")?,
        },
        kind: parse_enum(
            &row.try_get::<String, _>("candidate_kind")?,
            "CandidateKind",
            CandidateKind::parse,
        )?,
        mapping_sha256: row.try_get("mapping_sha256")?,
        cloud_source_path: row.try_get("cloud_source_path")?,
        cloud_destination_dir: row.try_get("cloud_destination_dir")?,
        cloud_file: cloud_file_json.as_deref().map(cloud_file_from_json).transpose()?,
    };
    record.validate()?;
    Ok(record)
}

/// `_journal_from_row(row)`.
fn journal_from_row(row: &PgRow) -> Result<MoveJournalRecord> {
    Ok(MoveJournalRecord {
        plan_id: row.try_get("plan_id")?,
        candidate_id: row.try_get("candidate_id")?,
        state: parse_enum(
            &row.try_get::<String, _>("state")?,
            "MoveJournalState",
            MoveJournalState::parse,
        )?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// `_cloud_move_operation_from_row(row)`.
fn cloud_move_operation_from_row(row: &PgRow) -> Result<CloudMoveOperationRecord> {
    let record = CloudMoveOperationRecord {
        plan_id: row.try_get("plan_id")?,
        candidate_id: row.try_get("candidate_id")?,
        attempt_id: row.try_get("attempt_id")?,
        source_path: row.try_get("source_path")?,
        destination_dir: row.try_get("destination_dir")?,
        state: parse_enum(
            &row.try_get::<String, _>("state")?,
            "CloudMoveOperationState",
            CloudMoveOperationState::parse,
        )?,
        updated_at: row.try_get("updated_at")?,
        error_code: row.try_get("error_code")?,
    };
    record.validate()?;
    Ok(record)
}

/// `_cloud_file_to_json(value)`: `json.dumps(..., separators=(',', ':'), sort_keys=True)`.
pub fn cloud_file_to_json(value: &CloudFileMetadata) -> String {
    pyjson::dumps(&Value::Dict(value.to_mapping()), &DumpOptions::sorted_compact_ascii())
        .expect("a CloudDrive snapshot always dumps")
}

/// `_cloud_file_from_json(value)`.
fn cloud_file_from_json(text: &str) -> Result<CloudFileMetadata> {
    let decode = |class: &'static str, message: String| RepositoryError::Store(StoreError::Decode { class, message });
    match pyjson::loads(text).map_err(|error| decode("JSONDecodeError", error.python_message(text)))? {
        Value::Dict(dict) => Ok(CloudFileMetadata::from_mapping(&dict)?),
        _ => Err(decode("TypeError", "invalid stored CloudDrive metadata".to_owned())),
    }
}

/// `_job_from_row(row)`.
fn job_from_row(row: &PgRow) -> Result<JobRecord> {
    Ok(JobRecord {
        job_id: row.try_get("job_id")?,
        operation: operation_of(row)?,
        state: parse_enum(&row.try_get::<String, _>("state")?, "JobState", JobState::parse)?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        plan_id: row.try_get("plan_id")?,
        error_code: row.try_get("error_code")?,
        owner_id: row.try_get("owner_id")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        actor_ids: id_list(row, "actor_ids_json")?,
        progress: progress_from_row(row)?,
    })
}

/// `_apply_job_from_row(row)`.
fn apply_job_from_row(row: &PgRow) -> Result<ApplyJobRecord> {
    let result_json: Option<String> = row.try_get("result_json")?;
    let record = ApplyJobRecord {
        job: job_from_row(row)?,
        revision: row.try_get("revision")?,
        candidate_ids: id_list(row, "candidate_ids_json")?,
        result: result_json.as_deref().map(ApplyResult::validate_json).transpose()?,
    };
    record.validate()?;
    Ok(record)
}

/// `_progress_from_row(row)`.
fn progress_from_row(row: &PgRow) -> Result<JobProgress> {
    let progress = JobProgress {
        stage: parse_enum(
            &row.try_get::<String, _>("progress_stage")?,
            "JobStage",
            JobStage::parse,
        )?,
        completed: i64::from(row.try_get::<i32, _>("progress_completed")?),
        total: row.try_get::<Option<i32>, _>("progress_total")?.map(i64::from),
        unit: parse_enum(
            &row.try_get::<String, _>("progress_unit")?,
            "JobProgressUnit",
            JobProgressUnit::parse,
        )?,
        current: row.try_get("progress_current")?,
        stage_started_at: row.try_get("progress_stage_started_at")?,
        updated_at: row.try_get("progress_updated_at")?,
    };
    progress.validate()?;
    Ok(progress)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt as _;
    use std::path::Path;

    use super::{RepositoryError, path_text};

    /// asyncpg's `DataError` text for each path, from `str(Path(os.fsdecode(raw)))` bound as
    /// the argument (`repr` cut at 40 characters, the codec error's first run of escapes).
    #[test]
    fn a_path_that_is_not_utf8_is_asyncpgs_data_error() {
        let cases: [(&[u8], usize, &str); 6] = [
            (
                b"/additional/\xff.mp4",
                4,
                r"invalid input for query argument $4: '/additional/\udcff.mp4' ('utf-8' codec can't encode character '\udcff' in position 12: surrogates not allowed)",
            ),
            (
                b"/a/\xff\xfe\xc3/x",
                5,
                r"invalid input for query argument $5: '/a/\udcff\udcfe\udcc3/x' ('utf-8' codec can't encode characters in position 3-5: surrogates not allowed)",
            ),
            (
                b"/very/long/directory/name/that/goes/on/\xff/and/on.mp4",
                6,
                r"invalid input for query argument $6: '/very/long/directory/name/that/goes/on/... ('utf-8' codec can't encode character '\udcff' in position 39: surrogates not allowed)",
            ),
            (
                b"/it's/\xff",
                4,
                r#"invalid input for query argument $4: "/it's/\udcff" ('utf-8' codec can't encode character '\udcff' in position 6: surrogates not allowed)"#,
            ),
            (
                b"//./a//b/\xe2\x82/c\xff",
                4,
                r"invalid input for query argument $4: '//a/b/\udce2\udc82/c\udcff' ('utf-8' codec can't encode characters in position 6-7: surrogates not allowed)",
            ),
            (
                b"/tab\t/\xe9t\xc3\xa9/\xff",
                6,
                "invalid input for query argument $6: '/tab\\t/\\udce9t\u{e9}/\\udcff' ('utf-8' codec can't encode character '\\udce9' in position 6: surrogates not allowed)",
            ),
        ];
        for (raw, position, expected) in cases {
            let error = path_text(Path::new(OsStr::from_bytes(raw)), position).unwrap_err();
            assert_eq!(error.class_name(), "DataError");
            assert!(!error.is_value_error());
            assert!(matches!(error, RepositoryError::Store(_)));
            assert_eq!(error.to_string(), expected, "{raw:?}");
        }
        assert_eq!(path_text(Path::new("//./a//b/"), 4).unwrap(), "//a/b");
    }
}
