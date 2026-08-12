"""Application database: asyncpg pool ownership and explicit schema migrations.

All embyx-manager tables share one migration sequence recorded in
``schema_migrations``. Concurrent process start-up is serialized with an
advisory lock so exactly one process applies each pending migration.
"""

import asyncio
from datetime import UTC, datetime

import asyncpg

CURRENT_SCHEMA_VERSION = 3

# Advisory-lock key space for embyx-manager; low word selects the resource.
ADVISORY_NAMESPACE = 0x454D4258  # 'EMBX'
MIGRATION_LOCK_KEY = 0
ENQUEUE_LOCK_KEY = 1
MUTATION_LOCK_KEY = 2


class UnsupportedSchemaVersionError(RuntimeError):
    def __init__(self, version: int) -> None:
        super().__init__(f'unsupported embyx-manager database schema version: {version}')


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

_MIGRATIONS[2] = (
    """
    CREATE TABLE app_config (
        section TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
)

_MIGRATIONS[3] = (
    """
    CREATE TABLE pipeline_runs (
        run_id TEXT PRIMARY KEY,
        pipeline TEXT NOT NULL CHECK (pipeline IN ('rss', 'archive', 'mapping')),
        trigger TEXT NOT NULL CHECK (trigger IN ('scheduled', 'manual', 'watchdog', 'startup')),
        state TEXT NOT NULL CHECK (state IN ('running', 'completed', 'failed', 'cancelled')),
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        stats_json TEXT NOT NULL DEFAULT '{}',
        errors_json TEXT NOT NULL DEFAULT '[]',
        log_json TEXT NOT NULL DEFAULT '[]'
    )
    """,
    'CREATE INDEX pipeline_runs_pipeline_idx ON pipeline_runs (pipeline, started_at DESC)',
    """
    CREATE TABLE rss_failed_avids (
        avid TEXT PRIMARY KEY,
        failed_at TIMESTAMPTZ NOT NULL
    )
    """,
)


class Database:
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

    @staticmethod
    async def _migrate(pool: asyncpg.Pool) -> None:
        async with pool.acquire() as connection, connection.transaction():
            # Serialize concurrent process start-up against one another.
            await connection.execute('SELECT pg_advisory_xact_lock($1, $2)', ADVISORY_NAMESPACE, MIGRATION_LOCK_KEY)
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
