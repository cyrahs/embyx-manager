import asyncio
from pathlib import Path

import asyncpg
from fastapi.testclient import TestClient

from embyx_manager import bootstrap
from embyx_manager.bootstrap import build_app
from embyx_manager.settings import Settings
from tests.conftest import postgres_test_dsn


def test_bootstrap_wires_repository_api_and_shutdown(tmp_path: Path) -> None:
    dsn = postgres_test_dsn()
    actor = tmp_path / 'actor'
    additional = tmp_path / 'additional'
    move_in = tmp_path / 'move-in'
    for path in (actor, additional, move_in):
        path.mkdir()
        (path / '.embyx-root').write_text('ready', encoding='utf-8')
    settings = Settings(
        database_url=dsn,
        actor_brand_path=actor,
        additional_brand_paths=(additional,),
        move_in_path=move_in,
        move_in_by_brand=True,
    )

    app = build_app(settings)
    try:
        with TestClient(app) as client:
            health = client.get('/api/health').json()
            assert health['status'] == 'ok'
            assert health['apply_enabled'] is False
            response = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
            assert response.status_code == 202
    finally:
        _reset_public_schema(dsn)


def _reset_public_schema(dsn: str) -> None:
    async def reset() -> None:
        connection = await asyncpg.connect(dsn)
        try:
            await connection.execute('DROP SCHEMA public CASCADE')
            await connection.execute('CREATE SCHEMA public')
        finally:
            await connection.close()

    asyncio.run(reset())


def test_bootstrap_passes_feed_integration_urls_to_warmer_and_api(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, dict] = {}
    warmer = object()
    jobs = object()
    app = object()

    def make_warmer(**kwargs):
        captured['warmer'] = kwargs
        return warmer

    def make_jobs(**kwargs):
        captured['jobs'] = kwargs
        return jobs

    def make_app(**kwargs):
        captured['api'] = kwargs
        return app

    monkeypatch.setattr(bootstrap, 'RSSHubFeedWarmer', make_warmer)
    monkeypatch.setattr(bootstrap, 'FillActorJobManager', make_jobs)
    monkeypatch.setattr(bootstrap, 'create_app', make_app)

    settings = Settings(
        database_url='postgresql://unused.invalid/db',
        actor_brand_path=tmp_path / 'actor',
        additional_brand_paths=(tmp_path / 'additional',),
        move_in_path=tmp_path / 'move-in',
        apply_enabled=True,
    )

    assert build_app(settings) is app
    # URLs now resolve from the live config store; with defaults they are None.
    assert callable(captured['warmer']['rsshub_url'])
    assert captured['warmer']['rsshub_url']() is None
    assert callable(captured['warmer']['freshrss_url'])
    assert captured['jobs']['feed_warmer'] is warmer
    assert captured['jobs']['service'].apply_enabled is True
    assert captured['api']['jobs'] is jobs
    assert callable(captured['api']['freshrss_url'])
    assert captured['api']['freshrss_url']() is None
    assert callable(captured['api']['freshrss_rsshub_url'])
