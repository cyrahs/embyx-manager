"""Migration 8: the two fixed RSS labels become categories that own their directory.

A stored section that fails validation reverts to defaults rather than raising
(see config/store.py), which for RSS now means no categories at all. These tests
run the migration's own statements against rows shaped the way deployments could
have left them and check the result still validates.
"""

import json
from datetime import UTC, datetime

from embyx_manager.config.models import CloudDriveConfig, RssConfig
from embyx_manager.db import _MIGRATIONS
from tests.conftest import make_database, postgres_test_dsn

# The statements that rewrite stored configuration, in the order they run.
BACKFILL_ACQUISITIONS = _MIGRATIONS[8][1]
RESHAPE_RSS_SECTION = _MIGRATIONS[8][2]
STRIP_CLOUDDRIVE_DIR = _MIGRATIONS[8][3]
NOW = datetime(2026, 8, 14, tzinfo=UTC)
INBOX = '/115/embyx_in'


async def _store_section(pool, section: str, values: dict) -> None:
    await pool.execute(
        """
        INSERT INTO app_config (section, value_json, version, updated_at)
        VALUES ($1, $2, 1, $3)
        ON CONFLICT (section) DO UPDATE SET value_json = excluded.value_json
        """,
        section,
        json.dumps(values),
        NOW,
    )


async def _migrate(rss: dict, clouddrive: dict | None = None):
    """Store a pre-migration pair of sections, rewrite them, and read both back."""
    postgres_test_dsn()
    pool = await make_database().get_pool()
    await _store_section(pool, 'rss', rss)
    if clouddrive is not None:
        await _store_section(pool, 'clouddrive', clouddrive)
    for statement in (RESHAPE_RSS_SECTION, STRIP_CLOUDDRIVE_DIR):
        await pool.execute(statement)
    stored_rss = await pool.fetchval("SELECT value_json FROM app_config WHERE section = 'rss'")
    stored_cloud = await pool.fetchval("SELECT value_json FROM app_config WHERE section = 'clouddrive'")
    return (
        RssConfig.model_validate_json(stored_rss),
        None if stored_cloud is None else CloudDriveConfig.model_validate_json(stored_cloud),
    )


async def test_both_labels_become_categories_on_the_old_shared_directory() -> None:
    rss, clouddrive = await _migrate(
        {
            'enabled': True,
            'interval_seconds': 900,
            'actor_label': 'Actor',
            'rank_label': 'Rank',
            'failed_avid_cooldown_seconds': 3600,
        },
        {'address': 'clouddrive:19798', 'api_token': 'x', 'task_dir_path': INBOX},
    )

    # Both keep downloading where the single directory used to send them, so the
    # upgrade changes no behaviour.
    assert [(category.label, category.task_dir_path) for category in rss.categories] == [
        ('Actor', INBOX),
        ('Rank', INBOX),
    ]
    assert rss.enabled is True
    assert rss.interval_seconds == 900
    assert rss.failed_avid_cooldown_seconds == 3600
    # The shared directory has no reader left, and the section forbids the key.
    assert clouddrive is not None
    assert not hasattr(clouddrive, 'task_dir_path')
    assert clouddrive.address == 'clouddrive:19798'


async def test_renamed_labels_are_carried_over() -> None:
    rss, _ = await _migrate(
        {'enabled': True, 'actor_label': '演员', 'rank_label': '排行'},
        {'task_dir_path': INBOX},
    )

    assert [category.label for category in rss.categories] == ['演员', '排行']


async def test_a_blank_label_falls_back_to_the_shipped_default() -> None:
    # The old fields had no validators, so a blank could be stored; an empty
    # label would fail the new model and take the whole section with it.
    rss, _ = await _migrate(
        {'enabled': True, 'actor_label': '  ', 'rank_label': 'Rank'},
        {'task_dir_path': INBOX},
    )

    assert [category.label for category in rss.categories] == ['Actor', 'Rank']


async def test_a_missing_label_falls_back_to_the_shipped_default() -> None:
    rss, _ = await _migrate({'enabled': True, 'rank_label': 'Rank'}, {'task_dir_path': INBOX})

    assert [category.label for category in rss.categories] == ['Actor', 'Rank']


async def test_both_labels_pointing_at_one_category_collapse_to_one_entry() -> None:
    rss, _ = await _migrate(
        {'enabled': True, 'actor_label': 'Feed', 'rank_label': 'Feed'},
        {'task_dir_path': INBOX},
    )

    assert [category.label for category in rss.categories] == ['Feed']


async def test_no_stored_directory_leaves_no_categories() -> None:
    # Nowhere to point them. Categories that fail validation would disable RSS
    # silently; none at all is reported by the readiness gate instead.
    rss, _ = await _migrate(
        {'enabled': True, 'actor_label': 'Actor', 'rank_label': 'Rank'},
        {'address': 'clouddrive:19798', 'task_dir_path': ''},
    )

    assert rss.categories == ()
    assert rss.enabled is True


async def test_a_section_that_was_never_written_is_left_alone() -> None:
    postgres_test_dsn()
    pool = await make_database().get_pool()

    for statement in (RESHAPE_RSS_SECTION, STRIP_CLOUDDRIVE_DIR):
        await pool.execute(statement)

    assert await pool.fetchval('SELECT COUNT(*) FROM app_config') == 0


async def test_acquisitions_keep_the_directory_they_were_submitted_to() -> None:
    postgres_test_dsn()
    pool = await make_database().get_pool()
    await _store_section(pool, 'clouddrive', {'task_dir_path': INBOX})
    await pool.execute(
        """
        INSERT INTO archive_acquisitions (avid, state, source, created_at, updated_at)
        VALUES ('ABC-123', 'downloading', 'rss_actor', $1, $1)
        """,
        NOW,
    )

    await pool.execute(BACKFILL_ACQUISITIONS)

    assert await pool.fetchval("SELECT task_dir_path FROM archive_acquisitions WHERE avid = 'ABC-123'") == INBOX
