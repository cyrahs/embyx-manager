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
    app = build_app(Settings(database_url=dsn))
    try:
        with TestClient(app) as client:
            # A fresh database has no library roots, which is "not configured" rather
            # than an error: the app is healthy and every other feature keeps working.
            unconfigured = client.get('/api/health').json()
            assert unconfigured['status'] == 'ok'
            assert unconfigured['fill_actor']['configured'] is False
            refused = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
            assert refused.status_code == 503

            saved = client.put(
                '/api/config/fill_actor',
                json={
                    'values': {
                        'actor_root': str(actor),
                        'additional_roots': [str(additional)],
                        'move_in_root': str(move_in),
                        'move_in_by_brand': True,
                    },
                },
            )
            assert saved.status_code == 200

            # Saving takes effect without a restart.
            health = client.get('/api/health').json()
            assert health['status'] == 'ok'
            assert health['fill_actor']['configured'] is True
            assert health['fill_actor']['apply_enabled'] is False
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


def test_bootstrap_passes_feed_integration_urls_to_warmer_and_api(monkeypatch) -> None:
    captured: dict[str, dict] = {}
    warmer = object()
    jobs = object()
    fill_actor_router = object()
    app = object()

    def make_warmer(**kwargs):
        captured['warmer'] = kwargs
        return warmer

    def make_jobs(**kwargs):
        captured['jobs'] = kwargs
        return jobs

    def make_fill_actor_router(**kwargs):
        captured['fill_actor'] = kwargs
        return fill_actor_router

    def make_app(**kwargs):
        captured['api'] = kwargs
        return app

    monkeypatch.setattr(bootstrap, 'RSSHubFeedWarmer', make_warmer)
    monkeypatch.setattr(bootstrap, 'FillActorJobManager', make_jobs)
    monkeypatch.setattr(bootstrap, 'create_fill_actor_router', make_fill_actor_router)
    monkeypatch.setattr(bootstrap, 'create_app', make_app)

    settings = Settings(database_url='postgresql://unused.invalid/db')

    assert build_app(settings) is app
    # URLs now resolve from the live config store; with defaults they are None.
    assert callable(captured['warmer']['rsshub_url'])
    assert captured['warmer']['rsshub_url']() is None
    assert callable(captured['warmer']['freshrss_url'])
    assert captured['jobs']['feed_warmer'] is warmer
    # Paths and the move switch come from the config store, so a freshly built service
    # is unconfigured until that section is loaded.
    assert captured['jobs']['service'].configured is False
    assert captured['jobs']['service'].apply_enabled is False
    assert captured['fill_actor']['jobs'] is jobs
    assert callable(captured['fill_actor']['freshrss_url'])
    assert captured['fill_actor']['freshrss_url']() is None
    # Fill Actor is mounted like every other feature, not baked into the app root.
    assert fill_actor_router in captured['api']['routers']
    assert set(captured['api']['feature_health']) == {'fill_actor'}
    assert 'service' not in captured['api']
    assert 'jobs' not in captured['api']
    assert callable(captured['fill_actor']['freshrss_rsshub_url'])
