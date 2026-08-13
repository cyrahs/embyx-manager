from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from embyx_manager.config import CloudDriveConfig, ConfigStore, ConfigVersionConflictError, FeedsConfig
from embyx_manager.config import api as config_api
from embyx_manager.config.api import create_config_router
from embyx_manager.config.store import masked_values, secret_flags
from embyx_manager.db import Database
from embyx_manager.errors import ApiError
from tests.conftest import make_database, postgres_test_dsn, reset_public_schema


def make_store() -> ConfigStore:
    return ConfigStore(make_database())


async def test_store_defaults_and_update_round_trip() -> None:
    postgres_test_dsn()
    store = make_store()
    await store.load()

    clouddrive = store.get(CloudDriveConfig)
    assert clouddrive.address == ''
    assert store.get_with_version('clouddrive')[1] == 0

    updated, version = await store.update(
        'clouddrive',
        {'address': 'cd.internal:19798', 'api_token': 'secret-token', 'secure': False},
    )
    assert version == 1
    assert updated.address == 'cd.internal:19798'
    assert store.get(CloudDriveConfig).api_token == 'secret-token'


async def test_store_keeps_secret_when_empty_value_submitted() -> None:
    postgres_test_dsn()
    store = make_store()
    await store.load()
    await store.update('clouddrive', {'address': 'cd.internal:19798', 'api_token': 'secret-token'})

    updated, version = await store.update('clouddrive', {'address': 'cd2.internal:19798', 'api_token': ''})

    assert version == 2
    assert updated.address == 'cd2.internal:19798'
    assert updated.api_token == 'secret-token'


async def test_store_version_conflict() -> None:
    postgres_test_dsn()
    store = make_store()
    await store.load()
    await store.update('freshrss', {'url': 'https://rss.example.test'})

    with pytest.raises(ConfigVersionConflictError):
        await store.update('freshrss', {'url': 'https://other.example.test'}, expected_version=0)


async def test_store_refresh_converges_second_replica() -> None:
    postgres_test_dsn()
    writer = make_store()
    await writer.load()
    await writer.update('feeds', {'rsshub_url': 'http://rsshub.internal.test'})

    replica = make_store()
    await replica.load()
    assert replica.get(FeedsConfig).rsshub_url == 'http://rsshub.internal.test'


async def test_store_rejects_invalid_values() -> None:
    postgres_test_dsn()
    store = make_store()
    await store.load()

    with pytest.raises(ValueError, match='absolute HTTP'):
        await store.update('feeds', {'rsshub_url': 'ftp://bad.example'})


def test_masked_values_blank_secrets() -> None:
    config = CloudDriveConfig(address='cd.internal:19798', api_token='secret-token')

    assert masked_values(config)['api_token'] == ''
    assert masked_values(config)['address'] == 'cd.internal:19798'
    assert secret_flags(config) == {'api_token': True}
    assert secret_flags(CloudDriveConfig()) == {'api_token': False}


async def _noop_auth() -> None:
    return None


def make_config_client() -> TestClient:
    """App whose store/database live entirely in the TestClient's loop."""
    database = Database(postgres_test_dsn())
    store = ConfigStore(database)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await store.load()
        try:
            yield
        finally:
            await database.aclose()

    app = FastAPI(lifespan=lifespan)
    app.include_router(create_config_router(store, mutation_auth=_noop_auth))

    @app.exception_handler(ApiError)
    async def handle(_request, exc):

        return JSONResponse({'error': {'code': exc.code}}, status_code=exc.status_code)

    return TestClient(app)


def test_config_api_round_trip_masks_secrets() -> None:
    postgres_test_dsn()
    try:
        with make_config_client() as client:
            response = client.put(
                '/api/config/clouddrive',
                json={'values': {'address': 'cd.internal:19798', 'api_token': 'secret-token'}, 'version': 0},
            )
            assert response.status_code == 200
            body = response.json()
            assert body['values']['api_token'] == ''
            assert body['secrets'] == {'api_token': True}
            assert body['version'] == 1

            listing = client.get('/api/config').json()
            clouddrive = next(item for item in listing if item['section'] == 'clouddrive')
            assert clouddrive['values']['address'] == 'cd.internal:19798'
            assert clouddrive['values']['api_token'] == ''

            conflict = client.put(
                '/api/config/clouddrive',
                json={'values': {'address': 'cd2.internal:19798'}, 'version': 0},
            )
            assert conflict.status_code == 409
            assert conflict.json()['error']['code'] == 'config_version_conflict'

            invalid = client.put('/api/config/feeds', json={'values': {'rsshub_url': 'not-a-url'}})
            assert invalid.status_code == 422

            unknown = client.get('/api/config/nope')
            assert unknown.status_code == 404
    finally:
        reset_public_schema()


def test_clouddrive_test_endpoint_uses_form_values_and_stored_secret(monkeypatch) -> None:
    postgres_test_dsn()
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

        def get_system_info(self) -> object:
            return SimpleNamespace(cloudAppVersion='1.0')

        def get_sub_files(self, path, *, force_refresh=False):  # noqa: ARG002
            captured.setdefault('listed', []).append(path)  # type: ignore[union-attr]
            return []

        def close(self) -> None:
            captured['closed'] = True

    monkeypatch.setattr(config_api, 'CloudDriveClient', FakeClient)

    try:
        with make_config_client() as client:
            client.put(
                '/api/config/clouddrive',
                json={'values': {'address': 'cd.internal:19798', 'api_token': 'stored-secret'}},
            )
            response = client.post(
                '/api/config/clouddrive/test',
                json={'values': {'address': 'override.internal:19798', 'task_dir_path': '/downloads'}},
            )
    finally:
        reset_public_schema()

    body = response.json()
    assert response.status_code == 200
    assert body['ok'] is True
    assert captured['address'] == 'override.internal:19798'
    assert captured['api_token'] == 'stored-secret'
    assert '/downloads' in captured['listed']
    assert captured['closed'] is True


def test_clouddrive_test_endpoint_requires_connection_values() -> None:
    postgres_test_dsn()
    try:
        with make_config_client() as client:
            response = client.post('/api/config/clouddrive/test', json={})
    finally:
        reset_public_schema()

    assert response.status_code == 200
    assert response.json()['ok'] is False
    assert 'required' in response.json()['detail']


def test_freshrss_test_endpoint_reports_auth_failure(monkeypatch) -> None:
    postgres_test_dsn()

    class FakeFreshRSS:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

        async def fetch_token(self) -> str:
            request = httpx.Request('GET', 'https://rss.example.test/token')
            response = httpx.Response(401, request=request)
            message = 'unauthorized'
            raise httpx.HTTPStatusError(message, request=request, response=response)

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(config_api, 'FreshRSSClient', FakeFreshRSS)

    try:
        with make_config_client() as client:
            client.put(
                '/api/config/freshrss',
                json={'values': {'url': 'https://rss.example.test', 'api_key': 'stored-key'}},
            )
            response = client.post('/api/config/freshrss/test', json={})
    finally:
        reset_public_schema()

    assert response.status_code == 200
    assert response.json()['ok'] is False
    assert 'authentication failed' in response.json()['detail']
