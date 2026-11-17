"""The level 2 masking export (tools/level2/mask.py), on synthetic data only."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from mask import MASKED, Masker, _walk, check, export, load, secret_key

# Synthetic stand-ins: nothing here is or resembles a real credential.
CLOUD_TOKEN = 'synthetic-clouddrive-token-0001'  # noqa: S105
EMBY_KEY = 'synthetic-emby-key-0002'
API_TOKEN = 'synthetic-api-token-0003'  # noqa: S105
FEED_KEY = 'synthetic-feed-key-0004'


def test_secret_names_are_recognised_and_ordinary_keys_are_not() -> None:
    for name in ('api_token', 'api_key', 'apikey', 'token', 'passkey', 'X-Auth', 'cookie', 'password', 'sig',
                 'accessToken', 'ClientSecret', 'privateKey'):  # fmt: skip
        assert secret_key(name), name
    for name in ('key', 'playlist_key', 'playlistKey', 'brand_mapping', 'monkey', 'keyword', 'tokens_used_count',
                 'avid', 'title'):  # fmt: skip
        assert not secret_key(name), name


def test_text_masks_literals_url_credentials_parameters_and_auth_headers() -> None:
    masker = Masker({API_TOKEN}, {})
    text = (
        f'GET https://user:pw@rsshub.example/feed?key={FEED_KEY}&limit=5 failed; '
        f'Authorization: Bearer abcdefgh12345678; sent {API_TOKEN}'
    )
    masked = masker.text(text)
    assert FEED_KEY not in masked
    assert 'user:pw' not in masked
    assert 'abcdefgh12345678' not in masked
    assert API_TOKEN not in masked
    assert '&limit=5' in masked
    assert masker.counts == {'auth_header': 1, 'literal': 1, 'url_param': 1, 'url_userinfo': 1}


def test_nested_and_encoded_urls_header_fields_and_camel_case_keys_are_masked_and_checked() -> None:
    masker = Masker(set(), {})
    texts = [
        f'magnet:?xt=urn:btih:00&dn=X&tr=https://t.example/announce?passkey={FEED_KEY}',
        f'magnet:?xt=urn:btih:00&tr=https%3A%2F%2Ft.example%2Fa%3Fpasskey%3D{FEED_KEY}%26x%3D1',
        f'https://h.example/x?api%5Fkey={FEED_KEY}',
        f'MediaBrowser Client="x", Token="{EMBY_KEY}"',
        f'X-Emby-Token: {EMBY_KEY}',
        f'Authorization: token {EMBY_KEY}',
        f'postgresql://user:pa@{CLOUD_TOKEN}@db.example/x',
    ]
    for text in texts:
        assert _walk(text, set()), text
        masked = masker.text(text)
        assert not any(secret in masked for secret in (FEED_KEY, EMBY_KEY, CLOUD_TOKEN)), masked
        assert _walk(masked, set()) == [], masked
    assert masker.text('https://h.example/p?limit=5&sort=asc') == 'https://h.example/p?limit=5&sort=asc'
    value = {'accessToken': EMBY_KEY, 'api_tokens': [EMBY_KEY], 'cookies': {'session': EMBY_KEY}, 'playlistKey': 'pl1',
             'token_enabled': True, 'auth': None}  # fmt: skip
    assert len(_walk(value, set())) == 3
    assert masker.json_value(value) == {
        'accessToken': MASKED, 'api_tokens': MASKED, 'cookies': MASKED, 'playlistKey': 'pl1',
        'token_enabled': True, 'auth': None,
    }  # fmt: skip


def test_config_secrets_are_masked_by_model_and_by_name_and_learned_as_literals() -> None:
    masker = Masker(set(), {'clouddrive': frozenset({'api_token'})})
    rows = [
        {'section': 'clouddrive', 'value_json': json.dumps({'address': 'cd:19798', 'api_token': CLOUD_TOKEN})},
        {'section': 'emby', 'value_json': json.dumps({'address': 'http://emby:8096', 'api_key': EMBY_KEY})},
        {'section': 'archive', 'value_json': json.dumps({'brand_mapping': {'x': ['ABC']}, 'mapping': {}})},
    ]
    masker.collect_config(rows)
    out = [masker.row('app_config', row) for row in rows]
    assert json.loads(out[0]['value_json']) == {'address': 'cd:19798', 'api_token': MASKED}
    assert json.loads(out[1]['value_json']) == {'address': 'http://emby:8096', 'api_key': MASKED}
    assert json.loads(out[2]['value_json']) == {'brand_mapping': {'x': ['ABC']}, 'mapping': {}}
    # A secret learned from the config is masked wherever else it appears.
    assert masker.row('pipeline_runs', {'log_json': json.dumps([f'emby said {EMBY_KEY}'])}) == {
        'log_json': json.dumps([f'emby said {MASKED}'])
    }


def test_check_reports_what_is_left_without_printing_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('EMBYX_MANAGER_API_TOKEN', API_TOKEN)
    (tmp_path / 'app_config.jsonl').write_text(
        json.dumps({'section': 'emby', 'value_json': json.dumps({'api_key': EMBY_KEY})}) + '\n'
        + json.dumps({'section': 'x', 'value_json': json.dumps({'api_key': MASKED, 'url': f'https://h/?token={API_TOKEN}'})})
        + '\n'
    )  # fmt: skip
    problems = check(tmp_path, ['EMBYX_MANAGER_API_TOKEN'])
    assert problems == [
        "app_config:1: secret-named key 'api_key' is not masked",
        'app_config:2: a secret from the environment',
        "app_config:2: URL parameter 'token'",
    ]
    assert not any(EMBY_KEY in problem or API_TOKEN in problem for problem in problems)


@pytest.mark.skipif(not os.environ.get('EMBYX_MANAGER_TEST_DATABASE_URL'), reason='needs a disposable PostgreSQL')
def test_export_check_and_load_round_trip_a_synthetic_database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncpg  # noqa: PLC0415

    from embyx_manager.db import Database  # noqa: PLC0415

    admin = os.environ['EMBYX_MANAGER_TEST_DATABASE_URL']
    source, copy = f'embyx_l2_src_{os.getpid()}', f'embyx_l2_copy_{os.getpid()}'
    monkeypatch.setenv('EMBYX_MANAGER_API_TOKEN', API_TOKEN)
    now = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)

    async def scenario() -> None:
        connection = await asyncpg.connect(admin)
        await connection.execute(f'DROP DATABASE IF EXISTS "{source}" WITH (FORCE)')
        await connection.execute(f'CREATE DATABASE "{source}"')
        await connection.close()
        dsn = admin.rsplit('/', 1)[0] + f'/{source}'
        database = Database(dsn)
        await database.get_pool()
        await database.aclose()
        connection = await asyncpg.connect(dsn)
        try:
            await connection.execute(
                'INSERT INTO app_config (section, value_json, version, updated_at) VALUES ($1, $2, 1, $3), ($4, $5, 1, $3)',
                'clouddrive', json.dumps({'address': '127.0.0.1:19798', 'api_token': CLOUD_TOKEN, 'secure': False}),
                now, 'emby', json.dumps({'address': 'http://emby.invalid:8096', 'api_key': EMBY_KEY}),
            )  # fmt: skip
            await connection.execute(
                "INSERT INTO feed_subscriptions (kind, category, url, created_at, updated_at) VALUES ('rss', 'c', $1, $2, $2)",
                f'https://rsshub.invalid/javbus/star/x?key={FEED_KEY}', now,
            )  # fmt: skip
            await connection.execute(
                "INSERT INTO pipeline_runs (run_id, pipeline, trigger, state, started_at, log_json) "
                "VALUES ('r1', 'rss', 'manual', 'completed', $1, $2)",
                now, json.dumps([f'polled https://rsshub.invalid/x?key={FEED_KEY}', f'token {API_TOKEN} refused']),
            )  # fmt: skip
            await connection.execute(
                "INSERT INTO archive_acquisitions (avid, state, source, created_at, updated_at, release_date) "
                "VALUES ('ABC-123', 'archived', 'manual', $1, $1, '2026-01-02')",
                now,
            )  # fmt: skip
        finally:
            await connection.close()

        monkeypatch.setenv('EMBYX_L2_TEST_DSN', dsn)
        manifest = await export(dsn, tmp_path / 'export', ['EMBYX_MANAGER_API_TOKEN'])
        assert manifest['schema_version'] == 19
        assert manifest['tables']['app_config'] == 2
        assert manifest['masked']['url_param'] == 2
        written = ''.join(path.read_text() for path in (tmp_path / 'export').iterdir())
        for secret in (CLOUD_TOKEN, EMBY_KEY, API_TOKEN, FEED_KEY):
            assert secret not in written
        assert check(tmp_path / 'export', ['EMBYX_MANAGER_API_TOKEN']) == []

        copy_dsn = await load(tmp_path / 'export', admin, copy)
        connection = await asyncpg.connect(copy_dsn)
        try:
            config = json.loads(await connection.fetchval("SELECT value_json FROM app_config WHERE section = 'emby'"))
            assert config == {'address': 'http://emby.invalid:8096', 'api_key': MASKED}
            row = await connection.fetchrow('SELECT * FROM archive_acquisitions')
            assert (row['avid'], row['created_at'], str(row['release_date'])) == ('ABC-123', now, '2026-01-02')
            # The identity sequence continues after the loaded rows.
            next_id = await connection.fetchval(
                'INSERT INTO feed_subscriptions (kind, category, url, created_at, updated_at) '
                "VALUES ('rss', 'c', 'https://x.invalid/', now(), now()) RETURNING id"
            )
            assert next_id == 2
        finally:
            await connection.close()
            connection = await asyncpg.connect(admin)
            for name in (source, copy):
                await connection.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
            await connection.close()

    asyncio.run(scenario())
