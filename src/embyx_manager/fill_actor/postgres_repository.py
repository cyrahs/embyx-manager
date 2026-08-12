"""PostgreSQL implementation of the fill-actor repository contract.

Mirrors the semantics of :class:`MemoryFillActorRepository` (the executable
specification) using asyncpg. Read-modify-write paths take row locks
(``FOR UPDATE``); queue admission is serialized with a transaction-scoped
advisory lock so the active-job cap holds across processes.
"""

import asyncio
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import asyncpg

from embyx_manager.fill_actor.cloud_moves import CloudFileMetadata
from embyx_manager.fill_actor.models import ApplyResult, FillActorPlan, MoveResult
from embyx_manager.fill_actor.persistence import (
    JOB_CANCELLED_ERROR_CODE,
    ApplyJobRecord,
    CancelJobOutcome,
    CancelJobResult,
    CandidateKind,
    CandidateRecord,
    CloudMoveOperationRecord,
    CloudMoveOperationState,
    EnqueueApplyJobOutcome,
    EnqueueApplyJobResult,
    FileFingerprint,
    JobFeedErrorCode,
    JobFeedRecord,
    JobFeedState,
    JobOperation,
    JobProgress,
    JobProgressUnit,
    JobRecord,
    JobStage,
    JobState,
    MoveJournalRecord,
    MoveJournalState,
    PlanRecord,
    normalize_datetime,
    validate_cloud_move_transition,
    validate_journal_transition,
)

CURRENT_SCHEMA_VERSION = 1

# Advisory-lock key space for embyx-manager; low word selects the resource.
_ADVISORY_NAMESPACE = 0x454D4258  # 'EMBX'
ENQUEUE_LOCK_KEY = 1

_MIGRATIONS: dict[int, tuple[str, ...]] = {
    1: (
        """
        CREATE TABLE fill_actor_plans (
            plan_id TEXT PRIMARY KEY,
            revision TEXT NOT NULL,
            public_json TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL
        )
        """,
        'CREATE INDEX fill_actor_plans_expires_at_idx ON fill_actor_plans (expires_at)',
        """
        CREATE TABLE fill_actor_candidates (
            plan_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            source_path TEXT NOT NULL,
            source_root TEXT NOT NULL,
            destination_path TEXT NOT NULL,
            fingerprint_device BIGINT NOT NULL,
            fingerprint_inode BIGINT NOT NULL,
            fingerprint_size BIGINT NOT NULL,
            fingerprint_mtime_ns BIGINT NOT NULL,
            fingerprint_ctime_ns BIGINT NOT NULL,
            candidate_kind TEXT NOT NULL DEFAULT 'local_file'
                CHECK (candidate_kind IN ('local_file', 'cloud_strm')),
            mapping_sha256 TEXT,
            cloud_source_path TEXT,
            cloud_destination_dir TEXT,
            cloud_file_json TEXT,
            PRIMARY KEY (plan_id, candidate_id),
            FOREIGN KEY (plan_id) REFERENCES fill_actor_plans (plan_id) ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE fill_actor_move_results (
            plan_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            result_json TEXT NOT NULL,
            PRIMARY KEY (plan_id, candidate_id),
            FOREIGN KEY (plan_id, candidate_id)
                REFERENCES fill_actor_candidates (plan_id, candidate_id) ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE fill_actor_jobs (
            job_id TEXT PRIMARY KEY,
            operation TEXT NOT NULL CHECK (operation IN ('create_plan', 'apply')),
            state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'completed', 'partial_failed', 'failed')),
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            plan_id TEXT,
            error_code TEXT,
            owner_id TEXT,
            lease_expires_at TIMESTAMPTZ,
            actor_ids_json TEXT NOT NULL DEFAULT '[]',
            progress_stage TEXT NOT NULL DEFAULT 'queued' CHECK (progress_stage IN (
                'queued', 'actor_catalog', 'library_scan', 'magnet_lookup', 'persisting', 'done', 'unknown'
            )),
            progress_completed INTEGER NOT NULL DEFAULT 0 CHECK (progress_completed >= 0),
            progress_total INTEGER CHECK (progress_total IS NULL OR progress_total >= 0),
            progress_unit TEXT NOT NULL DEFAULT 'items'
                CHECK (progress_unit IN ('actors', 'pages', 'videos', 'magnets', 'steps', 'items')),
            progress_current TEXT,
            progress_stage_started_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01T00:00:00+00:00',
            progress_updated_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01T00:00:00+00:00'
        )
        """,
        'CREATE INDEX fill_actor_jobs_plan_id_idx ON fill_actor_jobs (plan_id)',
        'CREATE INDEX fill_actor_jobs_lease_idx ON fill_actor_jobs (state, lease_expires_at)',
        "CREATE INDEX fill_actor_jobs_queue_idx ON fill_actor_jobs (created_at, job_id) WHERE state = 'queued'",
        """
        CREATE TABLE fill_actor_job_feeds (
            job_id TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('queued', 'warming', 'ready', 'failed')),
            attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            updated_at TIMESTAMPTZ NOT NULL,
            error_code TEXT CHECK (error_code IS NULL OR error_code IN (
                'rsshub_timeout', 'rsshub_network_error', 'rsshub_http_error',
                'rsshub_invalid_feed', 'rsshub_not_ready', 'rsshub_cancelled'
            )),
            freshrss_add_url TEXT,
            CHECK (
                (state = 'failed' AND error_code IS NOT NULL)
                OR (state != 'failed' AND error_code IS NULL)
            ),
            PRIMARY KEY (job_id, actor_id),
            FOREIGN KEY (job_id) REFERENCES fill_actor_jobs (job_id) ON DELETE CASCADE
        )
        """,
        'CREATE INDEX fill_actor_job_feeds_state_idx ON fill_actor_job_feeds (state, updated_at)',
        """
        CREATE TABLE fill_actor_move_journal (
            plan_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('prepared', 'linked', 'source_removed', 'reconciled')),
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (plan_id, candidate_id),
            FOREIGN KEY (plan_id, candidate_id)
                REFERENCES fill_actor_candidates (plan_id, candidate_id) ON DELETE CASCADE
        )
        """,
        'CREATE INDEX fill_actor_move_journal_state_idx ON fill_actor_move_journal (state, updated_at)',
        """
        CREATE TABLE fill_actor_cloud_move_operations (
            plan_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            attempt_id TEXT NOT NULL,
            source_path TEXT NOT NULL,
            destination_dir TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN (
                'prepared', 'submitting', 'verifying', 'unknown', 'succeeded', 'conflict', 'failed'
            )),
            updated_at TIMESTAMPTZ NOT NULL,
            error_code TEXT,
            CHECK (
                (state IN ('unknown', 'conflict', 'failed') AND error_code IS NOT NULL)
                OR (state NOT IN ('unknown', 'conflict', 'failed') AND error_code IS NULL)
            ),
            PRIMARY KEY (plan_id, candidate_id),
            FOREIGN KEY (plan_id, candidate_id)
                REFERENCES fill_actor_candidates (plan_id, candidate_id) ON DELETE CASCADE
        )
        """,
        """
        CREATE INDEX fill_actor_cloud_move_operations_state_idx
        ON fill_actor_cloud_move_operations (state, updated_at)
        """,
        """
        CREATE UNIQUE INDEX fill_actor_cloud_move_operations_active_source_idx
        ON fill_actor_cloud_move_operations (source_path)
        WHERE state IN ('prepared', 'submitting', 'verifying', 'unknown')
        """,
        """
        CREATE TABLE fill_actor_apply_jobs (
            job_id TEXT PRIMARY KEY,
            revision TEXT NOT NULL CHECK (length(revision) > 0),
            candidate_ids_json TEXT NOT NULL,
            result_json TEXT,
            FOREIGN KEY (job_id) REFERENCES fill_actor_jobs (job_id) ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE health_probe (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            checked_at TIMESTAMPTZ NOT NULL
        )
        """,
    ),
}


class UnsupportedSchemaVersionError(RuntimeError):
    def __init__(self, version: int) -> None:
        super().__init__(f'unsupported fill-actor database schema version: {version}')


class PostgresFillActorRepository:
    def __init__(self, dsn: str, *, min_pool_size: int = 1, max_pool_size: int = 10) -> None:
        self._dsn = dsn
        self._min_pool_size = min_pool_size
        self._max_pool_size = max_pool_size
        self._pool: asyncpg.Pool | None = None
        self._pool_lock = asyncio.Lock()

    async def aclose(self) -> None:
        async with self._pool_lock:
            if self._pool is not None:
                await self._pool.close()
                self._pool = None

    async def get_pool(self) -> asyncpg.Pool:
        if self._pool is not None:
            return self._pool
        async with self._pool_lock:
            if self._pool is None:
                pool = await asyncpg.create_pool(
                    self._dsn,
                    min_size=self._min_pool_size,
                    max_size=self._max_pool_size,
                )
                try:
                    await self._migrate(pool)
                except BaseException:
                    await pool.close()
                    raise
                self._pool = pool
        return self._pool

    async def _migrate(self, pool: asyncpg.Pool) -> None:
        async with pool.acquire() as connection, connection.transaction():
            # Serialize concurrent process start-up against one another.
            await connection.execute('SELECT pg_advisory_xact_lock($1, $2)', _ADVISORY_NAMESPACE, 0)
            await connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL
                )
                """,
            )
            row = await connection.fetchrow('SELECT MAX(version) AS version FROM schema_migrations')
            version = row['version'] if row is not None and row['version'] is not None else 0
            if version > CURRENT_SCHEMA_VERSION:
                raise UnsupportedSchemaVersionError(version)
            for pending in range(version + 1, CURRENT_SCHEMA_VERSION + 1):
                for statement in _MIGRATIONS[pending]:
                    await connection.execute(statement)
                await connection.execute(
                    'INSERT INTO schema_migrations (version, applied_at) VALUES ($1, $2)',
                    pending,
                    datetime.now(UTC),
                )

    async def save_plan(self, record: PlanRecord) -> None:
        public = record.public
        pool = await self.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                INSERT INTO fill_actor_plans (plan_id, revision, public_json, created_at, expires_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (plan_id) DO UPDATE SET
                    revision = excluded.revision,
                    public_json = excluded.public_json,
                    created_at = excluded.created_at,
                    expires_at = excluded.expires_at
                """,
                public.plan_id,
                public.revision,
                public.model_dump_json(),
                normalize_datetime(public.created_at),
                normalize_datetime(public.expires_at),
            )
            await connection.execute(
                'DELETE FROM fill_actor_candidates WHERE plan_id = $1 AND candidate_id != ALL($2::text[])',
                public.plan_id,
                [candidate.candidate_id for candidate in record.candidates],
            )
            for candidate in record.candidates:
                fingerprint = candidate.fingerprint
                await connection.execute(
                    """
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
                    """,
                    public.plan_id,
                    candidate.candidate_id,
                    candidate.video_id,
                    str(candidate.source),
                    str(candidate.source_root),
                    str(candidate.destination),
                    fingerprint.device,
                    fingerprint.inode,
                    fingerprint.size,
                    fingerprint.mtime_ns,
                    fingerprint.ctime_ns,
                    candidate.kind.value,
                    candidate.mapping_sha256,
                    candidate.cloud_source_path,
                    candidate.cloud_destination_dir,
                    _cloud_file_to_json(candidate.cloud_file),
                )

    async def get_plan(self, plan_id: str) -> PlanRecord | None:
        pool = await self.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                'SELECT public_json FROM fill_actor_plans WHERE plan_id = $1',
                plan_id,
            )
            if row is None:
                return None
            candidate_rows = await connection.fetch(
                'SELECT * FROM fill_actor_candidates WHERE plan_id = $1 ORDER BY candidate_id',
                plan_id,
            )
        return PlanRecord(
            public=FillActorPlan.model_validate_json(row['public_json']),
            candidates=tuple(_candidate_from_row(candidate_row) for candidate_row in candidate_rows),
        )

    async def get_candidate(self, plan_id: str, candidate_id: str) -> CandidateRecord | None:
        pool = await self.get_pool()
        row = await pool.fetchrow(
            'SELECT * FROM fill_actor_candidates WHERE plan_id = $1 AND candidate_id = $2',
            plan_id,
            candidate_id,
        )
        return _candidate_from_row(row) if row is not None else None

    async def delete_plan(self, plan_id: str) -> bool:
        pool = await self.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            blocked = await connection.fetchval(
                """
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
                """,
                plan_id,
            )
            if blocked:
                return False
            await connection.execute('UPDATE fill_actor_jobs SET plan_id = NULL WHERE plan_id = $1', plan_id)
            status = await connection.execute('DELETE FROM fill_actor_plans WHERE plan_id = $1', plan_id)
            return _row_count(status) > 0

    async def purge_expired_plans(self, now: datetime) -> int:
        pool = await self.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            expired_rows = await connection.fetch(
                """
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
                """,
                normalize_datetime(now),
            )
            if not expired_rows:
                return 0
            plan_ids = [row['plan_id'] for row in expired_rows]
            await connection.execute(
                'UPDATE fill_actor_jobs SET plan_id = NULL WHERE plan_id = ANY($1::text[])',
                plan_ids,
            )
            await connection.execute(
                'DELETE FROM fill_actor_plans WHERE plan_id = ANY($1::text[])',
                plan_ids,
            )
            return len(plan_ids)

    async def save_move_result(self, plan_id: str, result: MoveResult) -> None:
        pool = await self.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            await self._insert_move_result(connection, plan_id, result)

    @staticmethod
    async def _insert_move_result(connection: asyncpg.Connection, plan_id: str, result: MoveResult) -> None:
        video_id = await connection.fetchval(
            'SELECT video_id FROM fill_actor_candidates WHERE plan_id = $1 AND candidate_id = $2',
            plan_id,
            result.candidate_id,
        )
        if video_id is None:
            raise KeyError((plan_id, result.candidate_id))
        if video_id != result.video_id:
            msg = 'move result video id does not match candidate'
            raise ValueError(msg)
        await connection.execute(
            """
            INSERT INTO fill_actor_move_results (plan_id, candidate_id, result_json)
            VALUES ($1, $2, $3)
            ON CONFLICT (plan_id, candidate_id) DO UPDATE SET result_json = excluded.result_json
            """,
            plan_id,
            result.candidate_id,
            result.model_dump_json(),
        )

    async def get_move_result(self, plan_id: str, candidate_id: str) -> MoveResult | None:
        pool = await self.get_pool()
        value = await pool.fetchval(
            'SELECT result_json FROM fill_actor_move_results WHERE plan_id = $1 AND candidate_id = $2',
            plan_id,
            candidate_id,
        )
        return MoveResult.model_validate_json(value) if value is not None else None

    async def list_move_results(self, plan_id: str) -> tuple[MoveResult, ...]:
        pool = await self.get_pool()
        rows = await pool.fetch(
            'SELECT result_json FROM fill_actor_move_results WHERE plan_id = $1 ORDER BY candidate_id',
            plan_id,
        )
        return tuple(MoveResult.model_validate_json(row['result_json']) for row in rows)

    async def save_job(self, record: JobRecord) -> None:
        pool = await self.get_pool()
        async with pool.acquire() as connection:
            await self._execute_save_job(connection, record)

    async def enqueue_job(
        self,
        record: JobRecord,
        *,
        max_active: int,
        feeds: Sequence[JobFeedRecord] = (),
    ) -> bool:
        feed_keys = [(feed.job_id, feed.actor_id) for feed in feeds]
        if any(feed.job_id != record.job_id for feed in feeds) or len(feed_keys) != len(set(feed_keys)):
            msg = 'job feeds must be unique and belong to the enqueued job'
            raise ValueError(msg)
        pool = await self.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute(
                'SELECT pg_advisory_xact_lock($1, $2)',
                _ADVISORY_NAMESPACE,
                ENQUEUE_LOCK_KEY,
            )
            active = await connection.fetchval(
                "SELECT COUNT(*) FROM fill_actor_jobs WHERE state IN ('queued', 'running')",
            )
            exists = await connection.fetchval(
                'SELECT 1 FROM fill_actor_jobs WHERE job_id = $1',
                record.job_id,
            )
            if active >= max_active or exists is not None:
                return False
            await self._execute_save_job(connection, record)
            for feed in feeds:
                await connection.execute(
                    """
                    INSERT INTO fill_actor_job_feeds (
                        job_id, actor_id, state, attempts, updated_at, error_code, freshrss_add_url
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                    feed.job_id,
                    feed.actor_id,
                    feed.state.value,
                    feed.attempts,
                    normalize_datetime(feed.updated_at),
                    feed.error_code.value if feed.error_code is not None else None,
                    feed.freshrss_add_url,
                )
            return True

    async def enqueue_apply_job(
        self,
        record: ApplyJobRecord,
        *,
        max_active: int,
    ) -> EnqueueApplyJobResult:
        pool = await self.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute(
                'SELECT pg_advisory_xact_lock($1, $2)',
                _ADVISORY_NAMESPACE,
                ENQUEUE_LOCK_KEY,
            )
            existing_row = await connection.fetchrow(
                """
                SELECT fill_actor_jobs.*,
                       fill_actor_apply_jobs.revision,
                       fill_actor_apply_jobs.candidate_ids_json,
                       fill_actor_apply_jobs.result_json
                FROM fill_actor_jobs
                LEFT JOIN fill_actor_apply_jobs ON fill_actor_apply_jobs.job_id = fill_actor_jobs.job_id
                WHERE fill_actor_jobs.job_id = $1
                """,
                record.job.job_id,
            )
            if existing_row is not None:
                if (
                    existing_row['revision'] is not None
                    and existing_row['plan_id'] == record.job.plan_id
                    and existing_row['revision'] == record.revision
                    and tuple(json.loads(existing_row['candidate_ids_json'])) == record.candidate_ids
                ):
                    return EnqueueApplyJobResult(
                        EnqueueApplyJobOutcome.EXISTING,
                        _apply_job_from_row(existing_row),
                    )
                return EnqueueApplyJobResult(EnqueueApplyJobOutcome.CONFLICT, None)

            active = await connection.fetchval(
                "SELECT COUNT(*) FROM fill_actor_jobs WHERE state IN ('queued', 'running')",
            )
            if active >= max_active:
                return EnqueueApplyJobResult(EnqueueApplyJobOutcome.FULL, None)
            await self._execute_save_job(connection, record.job)
            await connection.execute(
                """
                INSERT INTO fill_actor_apply_jobs (job_id, revision, candidate_ids_json, result_json)
                VALUES ($1, $2, $3, $4)
                """,
                record.job.job_id,
                record.revision,
                json.dumps(record.candidate_ids),
                record.result.model_dump_json() if record.result is not None else None,
            )
            return EnqueueApplyJobResult(EnqueueApplyJobOutcome.ENQUEUED, record)

    async def get_apply_job(self, job_id: str) -> ApplyJobRecord | None:
        pool = await self.get_pool()
        row = await pool.fetchrow(
            """
            SELECT fill_actor_jobs.*,
                   fill_actor_apply_jobs.revision,
                   fill_actor_apply_jobs.candidate_ids_json,
                   fill_actor_apply_jobs.result_json
            FROM fill_actor_apply_jobs
            JOIN fill_actor_jobs ON fill_actor_jobs.job_id = fill_actor_apply_jobs.job_id
            WHERE fill_actor_apply_jobs.job_id = $1
            """,
            job_id,
        )
        return _apply_job_from_row(row) if row is not None else None

    async def claim_next_job(
        self,
        *,
        owner_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> JobRecord | None:
        pool = await self.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """
                SELECT * FROM fill_actor_jobs WHERE state = 'queued'
                ORDER BY created_at, job_id LIMIT 1
                FOR UPDATE SKIP LOCKED
                """,
            )
            if row is None:
                return None
            await connection.execute(
                """
                UPDATE fill_actor_jobs
                SET state = 'running', updated_at = $1, owner_id = $2, lease_expires_at = $3
                WHERE job_id = $4 AND state = 'queued'
                """,
                normalize_datetime(now),
                owner_id,
                normalize_datetime(lease_expires_at),
                row['job_id'],
            )
            return JobRecord(
                job_id=row['job_id'],
                operation=JobOperation(row['operation']),
                state=JobState.RUNNING,
                created_at=row['created_at'],
                updated_at=normalize_datetime(now),
                plan_id=row['plan_id'],
                error_code=row['error_code'],
                owner_id=owner_id,
                lease_expires_at=normalize_datetime(lease_expires_at),
                actor_ids=tuple(json.loads(row['actor_ids_json'])),
                progress=_progress_from_row(row),
            )

    async def renew_owned_job_lease(
        self,
        *,
        job_id: str,
        owner_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> bool:
        pool = await self.get_pool()
        status = await pool.execute(
            """
            UPDATE fill_actor_jobs SET updated_at = $1, lease_expires_at = $2
            WHERE job_id = $3 AND owner_id = $4 AND state = 'running'
              AND lease_expires_at IS NOT NULL AND lease_expires_at > $1
            """,
            normalize_datetime(now),
            normalize_datetime(lease_expires_at),
            job_id,
            owner_id,
        )
        return _row_count(status) == 1

    async def update_owned_job_progress(
        self,
        *,
        job_id: str,
        owner_id: str,
        progress: JobProgress,
        now: datetime,
    ) -> bool:
        pool = await self.get_pool()
        status = await pool.execute(
            """
            UPDATE fill_actor_jobs SET
                progress_stage = $1, progress_completed = $2, progress_total = $3, progress_unit = $4,
                progress_current = $5, progress_stage_started_at = $6, progress_updated_at = $7
            WHERE job_id = $8 AND owner_id = $9 AND state = 'running'
              AND lease_expires_at IS NOT NULL AND lease_expires_at > $10
            """,
            *_progress_values(progress),
            job_id,
            owner_id,
            normalize_datetime(now),
        )
        return _row_count(status) == 1

    async def finish_owned_job(  # noqa: PLR0913
        self,
        *,
        job_id: str,
        owner_id: str,
        state: JobState,
        error_code: str | None,
        now: datetime,
        progress: JobProgress,
        apply_result: ApplyResult | None = None,
    ) -> bool:
        pool = await self.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            status = await connection.execute(
                """
                UPDATE fill_actor_jobs SET
                    state = $1, updated_at = $2, error_code = $3, owner_id = NULL, lease_expires_at = NULL,
                    progress_stage = $4, progress_completed = $5, progress_total = $6, progress_unit = $7,
                    progress_current = $8, progress_stage_started_at = $9, progress_updated_at = $10
                WHERE job_id = $11 AND owner_id = $12 AND state = 'running'
                  AND lease_expires_at IS NOT NULL AND lease_expires_at > $2
                """,
                state.value,
                normalize_datetime(now),
                error_code,
                *_progress_values(progress),
                job_id,
                owner_id,
            )
            if _row_count(status) != 1:
                return False
            if apply_result is not None:
                apply_row = await connection.fetchrow(
                    """
                    SELECT fill_actor_jobs.*,
                           fill_actor_apply_jobs.revision,
                           fill_actor_apply_jobs.candidate_ids_json,
                           fill_actor_apply_jobs.result_json
                    FROM fill_actor_apply_jobs
                    JOIN fill_actor_jobs ON fill_actor_jobs.job_id = fill_actor_apply_jobs.job_id
                    WHERE fill_actor_apply_jobs.job_id = $1
                    """,
                    job_id,
                )
                if apply_row is None:
                    msg = 'apply result requires an apply job'
                    raise ValueError(msg)
                ApplyJobRecord(
                    job=_job_from_row(apply_row),
                    revision=apply_row['revision'],
                    candidate_ids=tuple(json.loads(apply_row['candidate_ids_json'])),
                    result=apply_result,
                )
                result_status = await connection.execute(
                    'UPDATE fill_actor_apply_jobs SET result_json = $1 WHERE job_id = $2',
                    apply_result.model_dump_json(),
                    job_id,
                )
                if _row_count(result_status) != 1:
                    msg = 'apply result update was not atomic'
                    raise ValueError(msg)
            return True

    async def cancel_job(self, *, job_id: str, now: datetime) -> CancelJobResult:
        pool = await self.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                'SELECT * FROM fill_actor_jobs WHERE job_id = $1 FOR UPDATE',
                job_id,
            )
            if row is None:
                return CancelJobResult(CancelJobOutcome.NOT_FOUND, None)

            current = _job_from_row(row)
            if current.operation is JobOperation.APPLY:
                return CancelJobResult(
                    CancelJobOutcome.NOT_CANCELLABLE,
                    current,
                    previous_state=current.state,
                    previous_owner_id=current.owner_id,
                )
            if current.state is JobState.FAILED and current.error_code == JOB_CANCELLED_ERROR_CODE:
                return CancelJobResult(
                    CancelJobOutcome.ALREADY_CANCELLED,
                    current,
                    previous_state=current.state,
                    previous_owner_id=current.owner_id,
                )
            if current.state not in {JobState.QUEUED, JobState.RUNNING}:
                return CancelJobResult(
                    CancelJobOutcome.ALREADY_TERMINAL,
                    current,
                    previous_state=current.state,
                    previous_owner_id=current.owner_id,
                )

            progress = _terminal_progress(current, normalize_datetime(now))
            status = await connection.execute(
                """
                UPDATE fill_actor_jobs SET
                    state = 'failed', updated_at = $1, error_code = $2, owner_id = NULL, lease_expires_at = NULL,
                    progress_stage = $3, progress_completed = $4, progress_total = $5, progress_unit = $6,
                    progress_current = $7, progress_stage_started_at = $8, progress_updated_at = $9
                WHERE job_id = $10 AND state IN ('queued', 'running')
                """,
                normalize_datetime(now),
                JOB_CANCELLED_ERROR_CODE,
                *_progress_values(progress),
                job_id,
            )
            if _row_count(status) != 1:  # pragma: no cover - FOR UPDATE serializes competing writers
                msg = 'cancelled job transition was not atomic'
                raise RuntimeError(msg)
            await connection.execute(
                """
                UPDATE fill_actor_job_feeds
                SET state = 'failed', updated_at = $1, error_code = $2
                WHERE job_id = $3 AND state IN ('queued', 'warming')
                """,
                normalize_datetime(now),
                JobFeedErrorCode.CANCELLED.value,
                job_id,
            )
            cancelled_row = await connection.fetchrow(
                'SELECT * FROM fill_actor_jobs WHERE job_id = $1',
                job_id,
            )
            if cancelled_row is None:  # pragma: no cover - same transaction retains the row
                msg = 'cancelled job disappeared'
                raise RuntimeError(msg)
            return CancelJobResult(
                CancelJobOutcome.CANCELLED,
                _job_from_row(cancelled_row),
                previous_state=current.state,
                previous_owner_id=current.owner_id,
            )

    async def fail_expired_jobs(self, *, now: datetime, error_code: str) -> int:
        pool = await self.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """
                UPDATE fill_actor_jobs
                SET state = 'failed', updated_at = $1, error_code = $2, owner_id = NULL, lease_expires_at = NULL,
                    progress_stage = 'done',
                    progress_current = CASE WHEN operation = 'apply' THEN NULL ELSE progress_current END,
                    progress_stage_started_at = $1, progress_updated_at = $1
                WHERE state = 'running' AND (lease_expires_at IS NULL OR lease_expires_at <= $1)
                RETURNING job_id
                """,
                normalize_datetime(now),
                error_code,
            )
            if not rows:
                return 0
            await connection.execute(
                """
                UPDATE fill_actor_job_feeds
                SET state = 'failed', updated_at = $1, error_code = $2
                WHERE job_id = ANY($3::text[]) AND state IN ('queued', 'warming')
                """,
                normalize_datetime(now),
                JobFeedErrorCode.CANCELLED.value,
                [row['job_id'] for row in rows],
            )
            return len(rows)

    async def get_job(self, job_id: str) -> JobRecord | None:
        pool = await self.get_pool()
        row = await pool.fetchrow('SELECT * FROM fill_actor_jobs WHERE job_id = $1', job_id)
        return _job_from_row(row) if row is not None else None

    async def list_jobs(self, states: Sequence[JobState] | None = None) -> tuple[JobRecord, ...]:
        pool = await self.get_pool()
        if states is None:
            rows = await pool.fetch('SELECT * FROM fill_actor_jobs ORDER BY created_at, job_id')
        else:
            rows = await pool.fetch(
                'SELECT * FROM fill_actor_jobs WHERE state = ANY($1::text[]) ORDER BY created_at, job_id',
                [state.value for state in states],
            )
        return tuple(_job_from_row(row) for row in rows)

    async def list_job_feeds(self, job_id: str) -> tuple[JobFeedRecord, ...]:
        pool = await self.get_pool()
        rows = await pool.fetch(
            'SELECT * FROM fill_actor_job_feeds WHERE job_id = $1 ORDER BY actor_id',
            job_id,
        )
        return tuple(_job_feed_from_row(row) for row in rows)

    async def update_owned_job_feed(  # noqa: PLR0913
        self,
        *,
        job_id: str,
        actor_id: str,
        owner_id: str,
        state: JobFeedState,
        attempts: int,
        error_code: JobFeedErrorCode | None,
        now: datetime,
    ) -> bool:
        record = JobFeedRecord(
            job_id=job_id,
            actor_id=actor_id,
            state=state,
            attempts=attempts,
            updated_at=now,
            error_code=error_code,
        )
        pool = await self.get_pool()
        status = await pool.execute(
            """
            UPDATE fill_actor_job_feeds SET
                state = $1, attempts = $2, updated_at = $3, error_code = $4
            WHERE job_id = $5 AND actor_id = $6
              AND state IN ('queued', 'warming')
              AND attempts <= $2
              AND EXISTS (
                  SELECT 1 FROM fill_actor_jobs
                  WHERE fill_actor_jobs.job_id = fill_actor_job_feeds.job_id
                    AND fill_actor_jobs.owner_id = $7
                    AND fill_actor_jobs.state = 'running'
                    AND fill_actor_jobs.lease_expires_at IS NOT NULL
                    AND fill_actor_jobs.lease_expires_at > $3
              )
            """,
            record.state.value,
            record.attempts,
            normalize_datetime(record.updated_at),
            record.error_code.value if record.error_code is not None else None,
            record.job_id,
            record.actor_id,
            owner_id,
        )
        return _row_count(status) == 1

    async def save_move_journal(self, record: MoveJournalRecord) -> None:
        pool = await self.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            candidate = await connection.fetchval(
                'SELECT 1 FROM fill_actor_candidates WHERE plan_id = $1 AND candidate_id = $2 FOR UPDATE',
                record.plan_id,
                record.candidate_id,
            )
            if candidate is None:
                raise KeyError((record.plan_id, record.candidate_id))
            current_value = await connection.fetchval(
                'SELECT state FROM fill_actor_move_journal WHERE plan_id = $1 AND candidate_id = $2 FOR UPDATE',
                record.plan_id,
                record.candidate_id,
            )
            current = MoveJournalState(current_value) if current_value is not None else None
            validate_journal_transition(current, record.state)
            await connection.execute(
                """
                INSERT INTO fill_actor_move_journal (plan_id, candidate_id, state, updated_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (plan_id, candidate_id) DO UPDATE SET
                    state = excluded.state,
                    updated_at = excluded.updated_at
                """,
                record.plan_id,
                record.candidate_id,
                record.state.value,
                normalize_datetime(record.updated_at),
            )

    async def get_move_journal(self, plan_id: str, candidate_id: str) -> MoveJournalRecord | None:
        pool = await self.get_pool()
        row = await pool.fetchrow(
            'SELECT * FROM fill_actor_move_journal WHERE plan_id = $1 AND candidate_id = $2',
            plan_id,
            candidate_id,
        )
        return _journal_from_row(row) if row is not None else None

    async def list_unreconciled_moves(self) -> tuple[MoveJournalRecord, ...]:
        pool = await self.get_pool()
        rows = await pool.fetch(
            """
            SELECT * FROM fill_actor_move_journal
            WHERE state != 'reconciled'
            ORDER BY updated_at, plan_id, candidate_id
            """,
        )
        return tuple(_journal_from_row(row) for row in rows)

    async def save_cloud_move_operation(self, record: CloudMoveOperationRecord) -> None:
        if record.state.terminal:
            msg = 'terminal CloudDrive operations must be finalized with their result'
            raise ValueError(msg)
        pool = await self.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            await self._execute_save_cloud_move_operation(connection, record)

    @staticmethod
    async def _execute_save_cloud_move_operation(
        connection: asyncpg.Connection,
        record: CloudMoveOperationRecord,
    ) -> None:
        candidate = await connection.fetchrow(
            """
            SELECT candidate_kind, cloud_source_path, cloud_destination_dir
            FROM fill_actor_candidates WHERE plan_id = $1 AND candidate_id = $2
            FOR UPDATE
            """,
            record.plan_id,
            record.candidate_id,
        )
        if candidate is None:
            raise KeyError((record.plan_id, record.candidate_id))
        if candidate['candidate_kind'] != CandidateKind.CLOUD_STRM.value:
            msg = 'CloudDrive operations require a CloudDrive candidate'
            raise ValueError(msg)
        if (
            candidate['cloud_source_path'] != record.source_path
            or candidate['cloud_destination_dir'] != record.destination_dir
        ):
            msg = 'CloudDrive operation paths must match its candidate'
            raise ValueError(msg)
        current_row = await connection.fetchrow(
            """
            SELECT state, attempt_id FROM fill_actor_cloud_move_operations
            WHERE plan_id = $1 AND candidate_id = $2
            FOR UPDATE
            """,
            record.plan_id,
            record.candidate_id,
        )
        current = CloudMoveOperationState(current_row['state']) if current_row is not None else None
        validate_cloud_move_transition(current, record.state)
        if current_row is not None and current_row['attempt_id'] != record.attempt_id:
            msg = 'CloudDrive operation attempt id cannot change'
            raise ValueError(msg)
        try:
            await connection.execute(
                """
                INSERT INTO fill_actor_cloud_move_operations (
                    plan_id, candidate_id, attempt_id, source_path, destination_dir,
                    state, updated_at, error_code
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (plan_id, candidate_id) DO UPDATE SET
                    state = excluded.state,
                    updated_at = excluded.updated_at,
                    error_code = excluded.error_code
                """,
                record.plan_id,
                record.candidate_id,
                record.attempt_id,
                record.source_path,
                record.destination_dir,
                record.state.value,
                normalize_datetime(record.updated_at),
                record.error_code,
            )
        except asyncpg.UniqueViolationError as exc:
            msg = 'CloudDrive source already has an unresolved operation'
            raise ValueError(msg) from exc

    async def get_cloud_move_operation(
        self,
        plan_id: str,
        candidate_id: str,
    ) -> CloudMoveOperationRecord | None:
        pool = await self.get_pool()
        row = await pool.fetchrow(
            'SELECT * FROM fill_actor_cloud_move_operations WHERE plan_id = $1 AND candidate_id = $2',
            plan_id,
            candidate_id,
        )
        return _cloud_move_operation_from_row(row) if row is not None else None

    async def list_unresolved_cloud_moves(self) -> tuple[CloudMoveOperationRecord, ...]:
        pool = await self.get_pool()
        rows = await pool.fetch(
            """
            SELECT * FROM fill_actor_cloud_move_operations
            WHERE state NOT IN ('succeeded', 'conflict', 'failed')
            ORDER BY updated_at, plan_id, candidate_id
            """,
        )
        return tuple(_cloud_move_operation_from_row(row) for row in rows)

    async def finalize_cloud_move(self, operation: CloudMoveOperationRecord, result: MoveResult) -> None:
        if not operation.state.terminal:
            msg = 'finalized CloudDrive operation must be terminal'
            raise ValueError(msg)
        if operation.candidate_id != result.candidate_id:
            msg = 'CloudDrive result must match its operation'
            raise ValueError(msg)
        pool = await self.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            await self._execute_save_cloud_move_operation(connection, operation)
            await self._insert_move_result(connection, operation.plan_id, result)

    async def health_check(self) -> bool:
        try:
            pool = await self.get_pool()
            async with pool.acquire() as connection:
                transaction = connection.transaction()
                await transaction.start()
                try:
                    version = await connection.fetchval('SELECT MAX(version) FROM schema_migrations')
                    await connection.execute(
                        """
                        INSERT INTO health_probe (id, checked_at) VALUES (1, $1)
                        ON CONFLICT (id) DO UPDATE SET checked_at = excluded.checked_at
                        """,
                        datetime.now(UTC),
                    )
                finally:
                    await transaction.rollback()
        except (OSError, asyncpg.PostgresError):
            return False
        return version == CURRENT_SCHEMA_VERSION

    @staticmethod
    async def _execute_save_job(connection: asyncpg.Connection, record: JobRecord) -> None:
        progress = record.progress
        if progress is None:  # pragma: no cover - JobRecord normalizes this invariant
            msg = 'job record progress is required'
            raise ValueError(msg)
        await connection.execute(
            """
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
            """,
            record.job_id,
            record.operation.value,
            record.state.value,
            normalize_datetime(record.created_at),
            normalize_datetime(record.updated_at),
            record.plan_id,
            record.error_code,
            record.owner_id,
            normalize_datetime(record.lease_expires_at) if record.lease_expires_at is not None else None,
            json.dumps(record.actor_ids),
            *_progress_values(progress),
        )


def _row_count(status: str) -> int:
    # asyncpg returns command tags such as 'UPDATE 1' / 'DELETE 0'.
    try:
        return int(status.rsplit(' ', 1)[-1])
    except ValueError:  # pragma: no cover - defensive against unexpected tags
        return 0


def _candidate_from_row(row: asyncpg.Record) -> CandidateRecord:
    cloud_file_json = row['cloud_file_json']
    return CandidateRecord(
        candidate_id=row['candidate_id'],
        video_id=row['video_id'],
        source=Path(row['source_path']),
        source_root=Path(row['source_root']),
        destination=Path(row['destination_path']),
        fingerprint=FileFingerprint(
            device=row['fingerprint_device'],
            inode=row['fingerprint_inode'],
            size=row['fingerprint_size'],
            mtime_ns=row['fingerprint_mtime_ns'],
            ctime_ns=row['fingerprint_ctime_ns'],
        ),
        kind=CandidateKind(row['candidate_kind']),
        mapping_sha256=row['mapping_sha256'],
        cloud_source_path=row['cloud_source_path'],
        cloud_destination_dir=row['cloud_destination_dir'],
        cloud_file=_cloud_file_from_json(cloud_file_json) if cloud_file_json is not None else None,
    )


def _journal_from_row(row: asyncpg.Record) -> MoveJournalRecord:
    return MoveJournalRecord(
        plan_id=row['plan_id'],
        candidate_id=row['candidate_id'],
        state=MoveJournalState(row['state']),
        updated_at=row['updated_at'],
    )


def _cloud_move_operation_from_row(row: asyncpg.Record) -> CloudMoveOperationRecord:
    return CloudMoveOperationRecord(
        plan_id=row['plan_id'],
        candidate_id=row['candidate_id'],
        attempt_id=row['attempt_id'],
        source_path=row['source_path'],
        destination_dir=row['destination_dir'],
        state=CloudMoveOperationState(row['state']),
        updated_at=row['updated_at'],
        error_code=row['error_code'],
    )


def _cloud_file_to_json(value: CloudFileMetadata | None) -> str | None:
    if value is None:
        return None
    return json.dumps(
        {
            'path': value.path,
            'id': value.file_id,
            'name': value.name,
            'size': value.size,
            'write_time': value.write_time,
            'hashes': dict(value.hashes),
        },
        separators=(',', ':'),
        sort_keys=True,
    )


def _cloud_file_from_json(value: str) -> CloudFileMetadata:
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        msg = 'invalid stored CloudDrive metadata'
        raise TypeError(msg)
    return CloudFileMetadata.from_mapping(decoded)


def _job_from_row(row: asyncpg.Record) -> JobRecord:
    return JobRecord(
        job_id=row['job_id'],
        operation=JobOperation(row['operation']),
        state=JobState(row['state']),
        created_at=row['created_at'],
        updated_at=row['updated_at'],
        plan_id=row['plan_id'],
        error_code=row['error_code'],
        owner_id=row['owner_id'],
        lease_expires_at=row['lease_expires_at'],
        actor_ids=tuple(json.loads(row['actor_ids_json'])),
        progress=_progress_from_row(row),
    )


def _apply_job_from_row(row: asyncpg.Record) -> ApplyJobRecord:
    result_json = row['result_json']
    return ApplyJobRecord(
        job=_job_from_row(row),
        revision=row['revision'],
        candidate_ids=tuple(json.loads(row['candidate_ids_json'])),
        result=ApplyResult.model_validate_json(result_json) if result_json is not None else None,
    )


def _job_feed_from_row(row: asyncpg.Record) -> JobFeedRecord:
    return JobFeedRecord(
        job_id=row['job_id'],
        actor_id=row['actor_id'],
        state=JobFeedState(row['state']),
        attempts=row['attempts'],
        updated_at=row['updated_at'],
        error_code=JobFeedErrorCode(row['error_code']) if row['error_code'] else None,
        freshrss_add_url=row['freshrss_add_url'],
    )


def _progress_from_row(row: asyncpg.Record) -> JobProgress:
    return JobProgress(
        stage=JobStage(row['progress_stage']),
        completed=row['progress_completed'],
        total=row['progress_total'],
        unit=JobProgressUnit(row['progress_unit']),
        current=row['progress_current'],
        stage_started_at=row['progress_stage_started_at'],
        updated_at=row['progress_updated_at'],
    )


def _terminal_progress(record: JobRecord, now: datetime) -> JobProgress:
    progress = record.progress
    if progress is None:  # pragma: no cover - JobRecord normalizes this invariant
        msg = 'job record progress is required'
        raise ValueError(msg)
    return JobProgress(
        stage=JobStage.DONE,
        completed=progress.completed,
        total=progress.total,
        unit=progress.unit,
        current=None if record.operation is JobOperation.APPLY else progress.current,
        stage_started_at=now,
        updated_at=now,
    )


def _progress_values(progress: JobProgress) -> tuple[object, ...]:
    return (
        progress.stage.value,
        progress.completed,
        progress.total,
        progress.unit.value,
        progress.current,
        normalize_datetime(progress.stage_started_at),
        normalize_datetime(progress.updated_at),
    )
