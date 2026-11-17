//! The 19 schema migrations, statement for statement.
//!
//! Python origin: `_MIGRATIONS` in `src/embyx_manager/db.py`. Each statement is the Python
//! string byte for byte (indentation included), so both builds run the same SQL in the same
//! order and leave the same schema (cross-version contract 1); `goldens/db/migrations.json`
//! holds them to the baseline. The comments are Python's, kept with the statements they
//! explain.

/// `CURRENT_SCHEMA_VERSION`: the newest schema this build knows. A database at a higher
/// version is refused.
pub const CURRENT_SCHEMA_VERSION: i32 = 19;

/// `_MIGRATIONS[version]`, for versions 1 to [`CURRENT_SCHEMA_VERSION`].
pub fn statements(version: i32) -> &'static [&'static str] {
    match version {
        1 => V1,
        2 => V2,
        3 => V3,
        4 => V4,
        5 => V5,
        6 => V6,
        7 => V7,
        8 => V8,
        9 => V9,
        10 => V10,
        11 => V11,
        12 => V12,
        13 => V13,
        14 => V14,
        15 => V15,
        16 => V16,
        17 => V17,
        18 => V18,
        19 => V19,
        _ => &[],
    }
}

const V1: &[&str] = &[
    r"
        CREATE TABLE fill_actor_plans (
            plan_id TEXT PRIMARY KEY,
            revision TEXT NOT NULL,
            public_json TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL
        )
        ",
    r"CREATE INDEX fill_actor_plans_expires_at_idx ON fill_actor_plans (expires_at)",
    r"
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
        ",
    r"
        CREATE TABLE fill_actor_move_results (
            plan_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            result_json TEXT NOT NULL,
            PRIMARY KEY (plan_id, candidate_id),
            FOREIGN KEY (plan_id, candidate_id)
                REFERENCES fill_actor_candidates (plan_id, candidate_id) ON DELETE CASCADE
        )
        ",
    r"
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
        ",
    r"CREATE INDEX fill_actor_jobs_plan_id_idx ON fill_actor_jobs (plan_id)",
    r"CREATE INDEX fill_actor_jobs_lease_idx ON fill_actor_jobs (state, lease_expires_at)",
    r"CREATE INDEX fill_actor_jobs_queue_idx ON fill_actor_jobs (created_at, job_id) WHERE state = 'queued'",
    r"
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
        ",
    r"CREATE INDEX fill_actor_job_feeds_state_idx ON fill_actor_job_feeds (state, updated_at)",
    r"
        CREATE TABLE fill_actor_move_journal (
            plan_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('prepared', 'linked', 'source_removed', 'reconciled')),
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (plan_id, candidate_id),
            FOREIGN KEY (plan_id, candidate_id)
                REFERENCES fill_actor_candidates (plan_id, candidate_id) ON DELETE CASCADE
        )
        ",
    r"CREATE INDEX fill_actor_move_journal_state_idx ON fill_actor_move_journal (state, updated_at)",
    r"
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
        ",
    r"
        CREATE INDEX fill_actor_cloud_move_operations_state_idx
        ON fill_actor_cloud_move_operations (state, updated_at)
        ",
    r"
        CREATE UNIQUE INDEX fill_actor_cloud_move_operations_active_source_idx
        ON fill_actor_cloud_move_operations (source_path)
        WHERE state IN ('prepared', 'submitting', 'verifying', 'unknown')
        ",
    r"
        CREATE TABLE fill_actor_apply_jobs (
            job_id TEXT PRIMARY KEY,
            revision TEXT NOT NULL CHECK (length(revision) > 0),
            candidate_ids_json TEXT NOT NULL,
            result_json TEXT,
            FOREIGN KEY (job_id) REFERENCES fill_actor_jobs (job_id) ON DELETE CASCADE
        )
        ",
    r"
        CREATE TABLE health_probe (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            checked_at TIMESTAMPTZ NOT NULL
        )
        ",
];

const V2: &[&str] = &[r"
    CREATE TABLE app_config (
        section TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        updated_at TIMESTAMPTZ NOT NULL
    )
    "];

const V3: &[&str] = &[
    r"
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
    ",
    r"CREATE INDEX pipeline_runs_pipeline_idx ON pipeline_runs (pipeline, started_at DESC)",
    r"
    CREATE TABLE rss_failed_avids (
        avid TEXT PRIMARY KEY,
        failed_at TIMESTAMPTZ NOT NULL
    )
    ",
];

// Plans are bound to the fill-actor config version that produced their paths, so a
// path change can never be applied against a plan scanned under the previous one.
const V4: &[&str] = &[r"ALTER TABLE fill_actor_plans ADD COLUMN config_version INTEGER NOT NULL DEFAULT 0"];

// The acquisition ledger: one row per AVID tracking discovery -> magnet -> offline
// download -> archive, with the per-magnet attempts that carry the CloudDrive
// infoHash the tracker joins on. It subsumes rss_failed_avids, whose cooldown is
// just an acquisition waiting for its next_action_at.
const V5: &[&str] = &[
    r"
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
    ",
    r"CREATE INDEX archive_acquisitions_state_idx ON archive_acquisitions (state, next_action_at)",
    r"
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
    ",
    r"CREATE INDEX archive_magnet_attempts_hash_idx ON archive_magnet_attempts (info_hash)",
    // The tracker joins the CloudDrive task list to attempts by infoHash, so at most
    // one attempt may be live for a hash at a time. Concluded attempts keep their hash
    // for history and are free to repeat.
    r"
    CREATE UNIQUE INDEX archive_magnet_attempts_live_hash_idx
    ON archive_magnet_attempts (info_hash)
    WHERE info_hash IS NOT NULL AND state IN ('submitted', 'downloading', 'finished', 'archiving')
    ",
    // Carry the RSS cooldown over at the shipped default (rss.failed_avid_cooldown_seconds
    // = 86400); a deployment that tuned it only shifts these rows' first retry.
    r"
    INSERT INTO archive_acquisitions (avid, state, source, next_action_at, created_at, updated_at)
    SELECT avid, 'resolve_failed', 'rss_actor', failed_at + make_interval(secs => 86400), failed_at, failed_at
    FROM rss_failed_avids
    ON CONFLICT (avid) DO NOTHING
    ",
    r"DROP TABLE rss_failed_avids",
];

// cloud_name / cloud_account_id only ever addressed the finished-offline-task
// cleanup, which the ledger made unnecessary. The section model forbids unknown
// keys and falls back to defaults when validation fails, so the stored rows have
// to lose the keys or the whole CloudDrive section would read as unconfigured.
const V6: &[&str] = &[r"
    UPDATE app_config
    SET value_json = (value_json::jsonb - 'cloud_name' - 'cloud_account_id')::text
    WHERE section = 'clouddrive'
    "];

// The tracker's own directory settings duplicated what was already configured:
// the offline task directory is clouddrive.task_dir_path, and its local view is
// one of the archive routes, which also names the destination. Same forbid-and-
// default story as migration 6: the keys must go or the section reads as unset.
const V7: &[&str] = &[r"
    UPDATE app_config
    SET value_json = (value_json::jsonb - 'task_dir_local' - 'task_dst' - 'task_priority')::text
    WHERE section = 'archive'
    "];

// RSS categories become a list, each naming its own offline directory, so a feed
// category can download somewhere other than the shared inbox and be filed by an
// archive route of its own. The single clouddrive.task_dir_path goes away with
// it: every category says where its downloads go, so there is nothing left for a
// shared default to mean.
//
// Everything below inherits that one directory, which is where these deployments
// were already downloading, so the upgrade changes no behaviour: the two fixed
// labels become the first two categories pointing at it, and the acquisitions in
// flight keep the directory they were submitted to.
const V8: &[&str] = &[
    // The directory an AVID's magnets were submitted to, pinned at discovery so a
    // retry still goes where its siblings went after its category is edited.
    r"ALTER TABLE archive_acquisitions ADD COLUMN task_dir_path TEXT",
    r"
    UPDATE archive_acquisitions
    SET task_dir_path = (
        SELECT NULLIF(trim(value_json::jsonb ->> 'task_dir_path'), '')
        FROM app_config WHERE section = 'clouddrive'
    )
    ",
    // The labels are rewritten defensively because the old fields had no
    // validators. A stored blank becomes the shipped default rather than an empty
    // label, and a deployment that pointed both fields at one category collapses
    // to a single entry; either would fail the new validators, and a section that
    // fails validation reverts to defaults (see config/store.py), which here would
    // silently leave RSS with no categories at all.
    r"
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
    ",
    // Now that the categories carry it, the single shared directory has no reader
    // left. Same forbid-and-default story as migrations 6 and 7: the key has to go
    // or the whole CloudDrive section would read as unconfigured.
    r"
    UPDATE app_config
    SET value_json = (value_json::jsonb - 'task_dir_path')::text
    WHERE section = 'clouddrive'
    ",
    // Sources are now free text: RSS records the category it came from as
    // 'rss:<label>'. Rows written before this keep their legacy values.
    r"ALTER TABLE archive_acquisitions DROP CONSTRAINT archive_acquisitions_source_check",
];

// The tracker now bursts fast checks right after a magnet is submitted, so the
// polling interval only paces downloads that are genuinely waiting and its
// default grew from 5 to 30 minutes. The store persists whole sections, so a
// deployment that ever saved settings holds the old default explicitly; exactly
// that value follows the default, while any other stored value was a choice and
// stays.
const V9: &[&str] = &[r"
    UPDATE app_config
    SET value_json = (value_json::jsonb || jsonb_build_object('tracker_interval_seconds', 1800))::text
    WHERE section = 'archive'
      AND value_json::jsonb ->> 'tracker_interval_seconds' = '300'
    "];

// Fill actor became an acquisition intake source: its scans hand missing AVIDs
// to the ledger instead of surfacing magnets, and its job's magnet_lookup stage
// became a submitting stage. magnet_lookup stays valid so pre-upgrade job rows
// still satisfy the constraint. The acquisition source needs nothing: migration
// 8 made the column free text.
const V10: &[&str] = &[
    r"ALTER TABLE fill_actor_jobs DROP CONSTRAINT fill_actor_jobs_progress_stage_check",
    r"
    ALTER TABLE fill_actor_jobs ADD CONSTRAINT fill_actor_jobs_progress_stage_check
    CHECK (progress_stage IN (
        'queued', 'actor_catalog', 'library_scan', 'magnet_lookup', 'submitting', 'persisting', 'done', 'unknown'
    ))
    ",
];

// The resolve schedule anchors on the release date: a source that knows it
// (a catalog scan or feed) records it at discovery, and empty passes are then
// spaced by how far the release is instead of by the fixed cooldown. Existing
// rows keep NULL and stay on the fixed cooldown.
const V11: &[&str] = &[r"ALTER TABLE archive_acquisitions ADD COLUMN release_date DATE"];

// The backend polls feeds itself instead of reading FreshRSS: subscriptions
// live here, each filed under one of the configured RSS categories. A plain
// feed URL is one kind; an AVBase talent (feed URL derived from its id) is the
// other. The cursor remembers which items a poll has seen, and seed_pending
// marks a subscription whose first poll should only record them.
const V12: &[&str] = &[
    r"
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
    ",
    r"CREATE UNIQUE INDEX feed_subscriptions_url_idx ON feed_subscriptions (url) WHERE url IS NOT NULL",
    r"CREATE UNIQUE INDEX feed_subscriptions_talent_idx ON feed_subscriptions (talent_id) WHERE talent_id IS NOT NULL",
];

// The AVID parser now spells BRAND-NUMBER with the catalog's three-digit
// padding (HTTM-0066 -> HTTM-066, TBW-19 -> TBW-019), so keys recorded under
// the old spelling would never match a new sighting or a scan. Rename them,
// letting the attempts follow through the foreign key; a key whose padded form
// is already taken (or that two old keys would collapse into) is left alone,
// since the two rows may carry different history. Brands whose numbers are not
// catalog ids keep their digits as they are.
const V13: &[&str] = &[
    r"ALTER TABLE archive_magnet_attempts DROP CONSTRAINT archive_magnet_attempts_avid_fkey",
    r"
    ALTER TABLE archive_magnet_attempts ADD CONSTRAINT archive_magnet_attempts_avid_fkey
        FOREIGN KEY (avid) REFERENCES archive_acquisitions (avid) ON UPDATE CASCADE ON DELETE CASCADE
    ",
    r"
    WITH candidates AS (
        SELECT
            avid,
            split_part(avid, '-', 1) || '-' || CASE
                WHEN length(ltrim(split_part(avid, '-', 2), '0')) >= 3 THEN ltrim(split_part(avid, '-', 2), '0')
                ELSE lpad(ltrim(split_part(avid, '-', 2), '0'), 3, '0')
            END AS normalized
        FROM archive_acquisitions
        WHERE avid ~ '^[A-Z0-9]*[A-Z][A-Z0-9]*-[0-9]+$'
          AND split_part(avid, '-', 1) NOT IN (
              'FC2', 'HEYZO', 'HEYDOUGA', 'GETCHU', 'GYUTTO', 'MKBD', 'MKD',
              'S2M', 'S2MBD', 'MK3D2DBD', 'CW3D2BD', 'CW3D2DBD'
          )
    )
    UPDATE archive_acquisitions AS row
    SET avid = candidates.normalized
    FROM candidates
    WHERE row.avid = candidates.avid
      AND candidates.normalized <> candidates.avid
      AND NOT EXISTS (SELECT 1 FROM archive_acquisitions taken WHERE taken.avid = candidates.normalized)
      AND (SELECT count(*) FROM candidates same WHERE same.normalized = candidates.normalized) = 1
    ",
];

// The backend no longer reads FreshRSS or warms RSSHub feeds for fill actor:
// the per-job feed table and the two config sections have nothing to describe.
const V14: &[&str] = &[
    r"DROP TABLE IF EXISTS fill_actor_job_feeds",
    r"DELETE FROM app_config WHERE section IN ('freshrss', 'feeds')",
];

// Ranking lists mirrored as Emby playlists. One row per list the source
// publishes; entries are the ranked codes after the library's exclusions, and
// present/missing are what the last sync found in Emby. enabled is the
// operator's choice and survives every re-download. The single-row source table
// remembers which database file the entries came from.
const V15: &[&str] = &[
    r"
    CREATE TABLE playlists (
        key TEXT PRIMARY KEY,
        kind INTEGER NOT NULL,
        note TEXT NOT NULL,
        name TEXT NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        entries_json TEXT NOT NULL DEFAULT '[]',
        present_json TEXT NOT NULL DEFAULT '[]',
        missing_json TEXT NOT NULL DEFAULT '[]',
        emby_playlist_id TEXT,
        last_synced_at TIMESTAMPTZ,
        last_error TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    ",
    r"
    CREATE TABLE playlist_source (
        id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
        database_name TEXT NOT NULL,
        fetched_at TIMESTAMPTZ NOT NULL
    )
    ",
];

// pipeline_runs was created with the three original pipelines spelled into its
// check constraint, so the first playlists run was refused at the database.
// The constraint now lists every pipeline name; adding a pipeline means adding
// it here as well as to PipelineName (tests/test_monitor_runs.py checks both).
const V16: &[&str] = &[
    r"ALTER TABLE pipeline_runs DROP CONSTRAINT pipeline_runs_pipeline_check",
    r"
    ALTER TABLE pipeline_runs ADD CONSTRAINT pipeline_runs_pipeline_check
    CHECK (pipeline IN ('rss', 'archive', 'mapping', 'playlists'))
    ",
];

// Multi-part titles merged into one file by the merge tab. A row walks from
// queued through the merge Job, the CloudDrive upload and its check, to the
// swap in the library; failed_state remembers where a failure happened so a
// retry resumes there. One title has at most one unfinished row.
const V17: &[&str] = &[
    r"
    CREATE TABLE merge_tasks (
        id BIGSERIAL PRIMARY KEY,
        avid TEXT NOT NULL,
        source TEXT NOT NULL,
        library_dir TEXT NOT NULL,
        brand TEXT NOT NULL,
        parts_json TEXT NOT NULL,
        merged_name TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN (
            'queued', 'merging', 'uploading', 'verifying', 'replacing', 'archiving',
            'done', 'failed', 'cancelled'
        )),
        failed_state TEXT,
        job_name TEXT,
        phase TEXT,
        progress DOUBLE PRECISION,
        merged_bytes BIGINT,
        merged_sha1 TEXT,
        uploaded_bytes BIGINT,
        upload_attempts INTEGER NOT NULL DEFAULT 0,
        error TEXT,
        notice TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        state_changed_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ
    )
    ",
    r"
    CREATE UNIQUE INDEX merge_tasks_one_open_per_avid ON merge_tasks (avid)
    WHERE state NOT IN ('done', 'cancelled')
    ",
];

// A title the library holds can be fetched again to replace the copy it holds
// (换版): the row goes back to discovered with the library paths of the copy
// being replaced, which the tracker deletes just before it files the new one.
// replace_after_attempt is the highest attempt number at that moment, so the
// attempt cap counts only the replacement's own downloads.
const V18: &[&str] = &[
    r"ALTER TABLE archive_acquisitions ADD COLUMN replace_paths_json TEXT",
    r"ALTER TABLE archive_acquisitions ADD COLUMN replace_after_attempt INTEGER",
];

// A merge too big for 115 can have its video re-encoded by a worker outside the
// cluster (the home GPU) before the merge Job assembles it: the task waits in
// transcoding while the worker uploads the video in chunks, one row each, and a
// Job copies the parts' audio. transcoder says who re-encodes: NULL lets the app
// decide, 'home' once the worker took the task, 'cpu' once someone sent it back
// to the merge Job's own encoder.
const V19: &[&str] = &[
    r"ALTER TABLE merge_tasks DROP CONSTRAINT merge_tasks_state_check",
    r"
    ALTER TABLE merge_tasks ADD CONSTRAINT merge_tasks_state_check CHECK (state IN (
        'queued', 'transcoding', 'merging', 'uploading', 'verifying', 'replacing', 'archiving',
        'done', 'failed', 'cancelled'
    ))
    ",
    r"ALTER TABLE merge_tasks ADD COLUMN transcoder TEXT CHECK (transcoder IN ('home', 'cpu'))",
    r"ALTER TABLE merge_tasks ADD COLUMN transcode_paused BOOLEAN NOT NULL DEFAULT FALSE",
    r"
    CREATE TABLE merge_transcode_chunks (
        task_id BIGINT NOT NULL REFERENCES merge_tasks (id) ON DELETE CASCADE,
        part_index INTEGER NOT NULL,
        chunk_index INTEGER NOT NULL,
        start_seconds DOUBLE PRECISION NOT NULL,
        duration_seconds DOUBLE PRECISION NOT NULL,
        frames INTEGER NOT NULL,
        bytes BIGINT NOT NULL,
        sha256 TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (task_id, part_index, chunk_index)
    )
    ",
    r"
    CREATE TABLE merge_transcode_parts (
        task_id BIGINT NOT NULL REFERENCES merge_tasks (id) ON DELETE CASCADE,
        part_index INTEGER NOT NULL,
        finished_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (task_id, part_index)
    )
    ",
];

#[cfg(test)]
mod tests;
