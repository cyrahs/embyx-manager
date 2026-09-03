"""Application database: asyncpg pool ownership and explicit schema migrations.

All embyx-manager tables share one migration sequence recorded in
``schema_migrations``. Concurrent process start-up is serialized with an
advisory lock so exactly one process applies each pending migration.
"""

import asyncio
from datetime import UTC, datetime

import asyncpg

CURRENT_SCHEMA_VERSION = 14

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

# Plans are bound to the fill-actor config version that produced their paths, so a
# path change can never be applied against a plan scanned under the previous one.
_MIGRATIONS[4] = ('ALTER TABLE fill_actor_plans ADD COLUMN config_version INTEGER NOT NULL DEFAULT 0',)

# The acquisition ledger: one row per AVID tracking discovery -> magnet -> offline
# download -> archive, with the per-magnet attempts that carry the CloudDrive
# infoHash the tracker joins on. It subsumes rss_failed_avids, whose cooldown is
# just an acquisition waiting for its next_action_at.
_MIGRATIONS[5] = (
    """
    CREATE TABLE archive_acquisitions (
        avid TEXT PRIMARY KEY,
        state TEXT NOT NULL CHECK (state IN (
            'discovered', 'downloading', 'archived',
            'resolve_failed', 'exhausted', 'needs_attention', 'ignored'
        )),
        source TEXT NOT NULL CHECK (source IN ('rss_actor', 'rss_rank', 'manual', 'reconcile')),
        note TEXT,
        archived_paths_json TEXT NOT NULL DEFAULT '[]',
        next_action_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    'CREATE INDEX archive_acquisitions_state_idx ON archive_acquisitions (state, next_action_at)',
    """
    CREATE TABLE archive_magnet_attempts (
        avid TEXT NOT NULL REFERENCES archive_acquisitions (avid) ON DELETE CASCADE,
        attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
        magnet TEXT NOT NULL,
        info_hash TEXT,
        magnet_source TEXT NOT NULL,
        size_hint BIGINT,
        state TEXT NOT NULL CHECK (state IN (
            'pending', 'submitted', 'downloading', 'finished',
            'archiving', 'archived', 'junk', 'error', 'stalled', 'lost'
        )),
        -- Double, not real: CloudDrive's percendDone is a double, and a float4
        -- column would round it so every poll looked like fresh progress and no
        -- download would ever be seen as stalled.
        progress DOUBLE PRECISION,
        error TEXT,
        submitted_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (avid, attempt_no)
    )
    """,
    'CREATE INDEX archive_magnet_attempts_hash_idx ON archive_magnet_attempts (info_hash)',
    # The tracker joins the CloudDrive task list to attempts by infoHash, so at most
    # one attempt may be live for a hash at a time. Concluded attempts keep their hash
    # for history and are free to repeat.
    """
    CREATE UNIQUE INDEX archive_magnet_attempts_live_hash_idx
    ON archive_magnet_attempts (info_hash)
    WHERE info_hash IS NOT NULL AND state IN ('submitted', 'downloading', 'finished', 'archiving')
    """,
    # Carry the RSS cooldown over at the shipped default (rss.failed_avid_cooldown_seconds
    # = 86400); a deployment that tuned it only shifts these rows' first retry.
    """
    INSERT INTO archive_acquisitions (avid, state, source, next_action_at, created_at, updated_at)
    SELECT avid, 'resolve_failed', 'rss_actor', failed_at + make_interval(secs => 86400), failed_at, failed_at
    FROM rss_failed_avids
    ON CONFLICT (avid) DO NOTHING
    """,
    'DROP TABLE rss_failed_avids',
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


# cloud_name / cloud_account_id only ever addressed the finished-offline-task
# cleanup, which the ledger made unnecessary. The section model forbids unknown
# keys and falls back to defaults when validation fails, so the stored rows have
# to lose the keys or the whole CloudDrive section would read as unconfigured.
_MIGRATIONS[6] = (
    """
    UPDATE app_config
    SET value_json = (value_json::jsonb - 'cloud_name' - 'cloud_account_id')::text
    WHERE section = 'clouddrive'
    """,
)

# The tracker's own directory settings duplicated what was already configured:
# the offline task directory is clouddrive.task_dir_path, and its local view is
# one of the archive routes, which also names the destination. Same forbid-and-
# default story as migration 6: the keys must go or the section reads as unset.
_MIGRATIONS[7] = (
    """
    UPDATE app_config
    SET value_json = (value_json::jsonb - 'task_dir_local' - 'task_dst' - 'task_priority')::text
    WHERE section = 'archive'
    """,
)

# RSS categories become a list, each naming its own offline directory, so a feed
# category can download somewhere other than the shared inbox and be filed by an
# archive route of its own. The single clouddrive.task_dir_path goes away with
# it: every category says where its downloads go, so there is nothing left for a
# shared default to mean.
#
# Everything below inherits that one directory, which is where these deployments
# were already downloading, so the upgrade changes no behaviour: the two fixed
# labels become the first two categories pointing at it, and the acquisitions in
# flight keep the directory they were submitted to.
_MIGRATIONS[8] = (
    # The directory an AVID's magnets were submitted to, pinned at discovery so a
    # retry still goes where its siblings went after its category is edited.
    'ALTER TABLE archive_acquisitions ADD COLUMN task_dir_path TEXT',
    """
    UPDATE archive_acquisitions
    SET task_dir_path = (
        SELECT NULLIF(trim(value_json::jsonb ->> 'task_dir_path'), '')
        FROM app_config WHERE section = 'clouddrive'
    )
    """,
    # The labels are rewritten defensively because the old fields had no
    # validators. A stored blank becomes the shipped default rather than an empty
    # label, and a deployment that pointed both fields at one category collapses
    # to a single entry; either would fail the new validators, and a section that
    # fails validation reverts to defaults (see config/store.py), which here would
    # silently leave RSS with no categories at all.
    """
    UPDATE app_config
    SET value_json = (
        (value_json::jsonb - 'actor_label' - 'rank_label')
        || jsonb_build_object('categories', COALESCE((
            SELECT jsonb_agg(jsonb_build_object('label', label, 'task_dir_path', inbox.path) ORDER BY ord)
            FROM (
                SELECT DISTINCT ON (label) label, ord
                FROM unnest(ARRAY[
                    COALESCE(NULLIF(trim(value_json::jsonb ->> 'actor_label'), ''), 'Actor'),
                    COALESCE(NULLIF(trim(value_json::jsonb ->> 'rank_label'), ''), 'Rank')
                ]) WITH ORDINALITY AS entries(label, ord)
                ORDER BY label, ord
            ) unique_labels
            -- Both categories inherit the one directory. A deployment that never
            -- configured it has nowhere to point them: the join drops every row
            -- and it gets no categories, which the settings page asks for, rather
            -- than categories that would fail validation.
            CROSS JOIN (
                SELECT NULLIF(trim(value_json::jsonb ->> 'task_dir_path'), '') AS path
                FROM app_config WHERE section = 'clouddrive'
            ) inbox
            WHERE inbox.path IS NOT NULL
        ), '[]'::jsonb))
    )::text
    WHERE section = 'rss'
    """,
    # Now that the categories carry it, the single shared directory has no reader
    # left. Same forbid-and-default story as migrations 6 and 7: the key has to go
    # or the whole CloudDrive section would read as unconfigured.
    """
    UPDATE app_config
    SET value_json = (value_json::jsonb - 'task_dir_path')::text
    WHERE section = 'clouddrive'
    """,
    # Sources are now free text: RSS records the category it came from as
    # 'rss:<label>'. Rows written before this keep their legacy values.
    'ALTER TABLE archive_acquisitions DROP CONSTRAINT archive_acquisitions_source_check',
)

# The tracker now bursts fast checks right after a magnet is submitted, so the
# polling interval only paces downloads that are genuinely waiting and its
# default grew from 5 to 30 minutes. The store persists whole sections, so a
# deployment that ever saved settings holds the old default explicitly; exactly
# that value follows the default, while any other stored value was a choice and
# stays.
_MIGRATIONS[9] = (
    """
    UPDATE app_config
    SET value_json = (value_json::jsonb || jsonb_build_object('tracker_interval_seconds', 1800))::text
    WHERE section = 'archive'
      AND value_json::jsonb ->> 'tracker_interval_seconds' = '300'
    """,
)

# Fill actor became an acquisition intake source: its scans hand missing AVIDs
# to the ledger instead of surfacing magnets, and its job's magnet_lookup stage
# became a submitting stage. magnet_lookup stays valid so pre-upgrade job rows
# still satisfy the constraint. The acquisition source needs nothing: migration
# 8 made the column free text.
_MIGRATIONS[10] = (
    'ALTER TABLE fill_actor_jobs DROP CONSTRAINT fill_actor_jobs_progress_stage_check',
    """
    ALTER TABLE fill_actor_jobs ADD CONSTRAINT fill_actor_jobs_progress_stage_check
    CHECK (progress_stage IN (
        'queued', 'actor_catalog', 'library_scan', 'magnet_lookup', 'submitting', 'persisting', 'done', 'unknown'
    ))
    """,
)

# The resolve schedule anchors on the release date: a source that knows it
# (a catalog scan or feed) records it at discovery, and empty passes are then
# spaced by how far the release is instead of by the fixed cooldown. Existing
# rows keep NULL and stay on the fixed cooldown.
_MIGRATIONS[11] = ('ALTER TABLE archive_acquisitions ADD COLUMN release_date DATE',)

# The backend polls feeds itself instead of reading FreshRSS: subscriptions
# live here, each filed under one of the configured RSS categories. A plain
# feed URL is one kind; an AVBase talent (feed URL derived from its id) is the
# other. The cursor remembers which items a poll has seen, and seed_pending
# marks a subscription whose first poll should only record them.
_MIGRATIONS[12] = (
    """
    CREATE TABLE feed_subscriptions (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        kind TEXT NOT NULL CHECK (kind IN ('rss', 'avbase_talent')),
        category TEXT NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        url TEXT,
        talent_id INTEGER,
        name TEXT,
        aliases_json TEXT NOT NULL DEFAULT '[]',
        cursor_json TEXT NOT NULL DEFAULT '[]',
        seed_pending BOOLEAN NOT NULL DEFAULT FALSE,
        last_polled_at TIMESTAMPTZ,
        last_error TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        CHECK ((kind = 'rss') = (url IS NOT NULL)),
        CHECK ((kind = 'avbase_talent') = (talent_id IS NOT NULL))
    )
    """,
    'CREATE UNIQUE INDEX feed_subscriptions_url_idx ON feed_subscriptions (url) WHERE url IS NOT NULL',
    'CREATE UNIQUE INDEX feed_subscriptions_talent_idx ON feed_subscriptions (talent_id) WHERE talent_id IS NOT NULL',
)
