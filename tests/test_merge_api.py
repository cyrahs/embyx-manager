"""Merge task endpoints over a fake repository and service."""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from embyx_manager.config.models import MappingConfig
from embyx_manager.errors import ApiError
from embyx_manager.merge.api import (
    AutoState,
    AutoStatus,
    MergeCatalog,
    MergeTasksApi,
    SkippedTitle,
    TitleScanCache,
    create_merge_router,
    warm_title_cache,
)
from embyx_manager.merge.service import MergeActionError
from embyx_manager.merge.tasks import MergeState, MergeTask, MergeTaskConflictError
from tests.test_merge_detect import ARCHIVE, parts, strm

NOW = datetime(2026, 10, 2, 20, 0, tzinfo=UTC)


def task(task_id: int, avid: str, state: MergeState = MergeState.QUEUED, **fields) -> MergeTask:
    values = {
        'id': task_id,
        'avid': avid,
        'source': 'vr',
        'library_dir': 'type/vr',
        'brand': avid.split('-', 1)[0],
        'parts': ('/a-cd1.mp4', '/a-cd2.mp4'),
        'merged_name': f'{avid}.mp4',
        'state': state,
        'failed_state': None,
        'job_name': None,
        'phase': None,
        'progress': None,
        'merged_bytes': None,
        'merged_sha1': None,
        'uploaded_bytes': None,
        'upload_attempts': 0,
        'error': None,
        'notice': None,
        'created_at': NOW,
        'updated_at': NOW,
        'state_changed_at': NOW,
        'finished_at': None,
    }
    values.update(fields)
    return MergeTask(**values)


class FakeRepository:
    def __init__(self) -> None:
        self.tasks: list[MergeTask] = []

    async def create(self, **fields) -> MergeTask:
        if any(existing.avid == fields['avid'] for existing in self.tasks):
            raise MergeTaskConflictError(fields['avid'])
        created = task(len(self.tasks) + 1, fields['avid'], source=fields['source'], parts=fields['parts'])
        created = MergeTask(**{**created.__dict__, 'merged_name': fields['merged_name']})
        self.tasks.append(created)
        return created

    async def listing(self) -> tuple[MergeTask, ...]:
        return tuple(reversed(self.tasks))


class FakeService:
    def __init__(self, reason: str | None = None) -> None:
        self.reason = reason
        self.woken = 0

    def unavailable(self) -> str | None:
        return self.reason

    def wake(self) -> None:
        self.woken += 1

    async def cancel(self, task_id: int) -> MergeTask:
        if task_id != 1:
            code = 'merge_task_not_found'
            raise MergeActionError(code)
        return task(1, 'SQTEVR-009', MergeState.CANCELLED)

    async def retry(self, task_id: int) -> MergeTask:
        del task_id
        code = 'merge_task_not_failed'
        raise MergeActionError(code)


async def allow() -> None:
    return None


def make_client(
    tmp_path: Path,
    repository: FakeRepository,
    service: FakeService,
    task_dirs: dict[str, str] | None = None,
    cache: TitleScanCache | None = None,
    auto_status: Callable[[], AutoStatus] | None = None,
) -> TestClient:
    async def task_dirs_for(avids) -> dict[str, str]:
        return {avid: path for avid, path in (task_dirs or {}).items() if avid in avids}

    app = FastAPI()

    @app.exception_handler(ApiError)
    async def handle(_request, exc: ApiError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={'error': {'code': exc.code}})

    app.include_router(
        create_merge_router(
            MergeCatalog(
                archive=lambda: ARCHIVE,
                mapping=lambda: MappingConfig(src_dir='/remote', dst_dir=str(tmp_path)),
                task_dirs_for=task_dirs_for,
            ),
            tasks=MergeTasksApi(repository=repository, service=service, auto_status=auto_status),  # type: ignore[arg-type]
            mutation_auth=allow,
            cache=cache,
        ),
    )
    return TestClient(app)


def test_create_queues_a_title_with_its_parts_and_route(tmp_path: Path) -> None:
    parts(tmp_path, 'type/vr/SQTEVR/SQTEVR-009', 'SQTEVR-009', 12, 'type/vr')
    repository, service = FakeRepository(), FakeService()
    client = make_client(tmp_path, repository, service)

    response = client.post('/api/merge/tasks', json={'avid': 'sqtevr-009'})

    assert response.status_code == 201
    body = response.json()
    assert (body['avid'], body['source'], body['part_count'], body['state']) == ('SQTEVR-009', 'vr', 12, 'queued')
    assert (body['cancellable'], body['retryable']) == (True, False)
    (created,) = repository.tasks
    assert created.parts[0] == '/mnt/cd2/115/embyx/type/vr/SQTEVR/SQTEVR-009-cd1.mp4'
    assert created.parts[-1].endswith('-cd12.mp4')
    assert created.merged_name == 'SQTEVR-009.mp4'
    assert service.woken == 1

    assert client.post('/api/merge/tasks', json={'avid': 'SQTEVR-009'}).json() == {
        'error': {'code': 'merge_task_exists'},
    }


def test_create_needs_a_route_when_the_library_cannot_tell(tmp_path: Path) -> None:
    parts(tmp_path, 'type/x/DEF/DEF-001', 'DEF-001', 2, 'type/x')
    client = make_client(tmp_path, FakeRepository(), FakeService())

    assert client.post('/api/merge/tasks', json={'avid': 'DEF-001'}).json() == {
        'error': {'code': 'merge_source_required'}
    }
    assert client.post('/api/merge/tasks', json={'avid': 'DEF-001', 'source': 'nope'}).json() == {
        'error': {'code': 'merge_source_unknown'},
    }
    created = client.post('/api/merge/tasks', json={'avid': 'DEF-001', 'source': 'rank'})
    assert (created.status_code, created.json()['source']) == (201, 'rank')


def test_create_falls_back_to_the_ledger_route(tmp_path: Path) -> None:
    parts(tmp_path, 'type/x/DEF/DEF-001', 'DEF-001', 2, 'type/x')
    client = make_client(tmp_path, FakeRepository(), FakeService(), {'DEF-001': '/115/embyx_in/special'})

    assert client.post('/api/merge/tasks', json={'avid': 'DEF-001'}).json()['source'] == 'special'


def test_create_refuses_what_cannot_be_merged(tmp_path: Path) -> None:
    parts(tmp_path, 'rest/XYZ/XYZ-001', 'XYZ-001', 4, 'rest', skip=(2,))
    strm(tmp_path, 'rank/MIX/MIX-001', 'MIX-001-cd1.strm', '/mnt/cd2/115/embyx/rank/MIX/MIX-001-cd1.mp4')
    strm(tmp_path, 'rank/MIX/MIX-001', 'MIX-001-cd2.strm', '/mnt/cd2/115/embyx/rank/MIX/MIX-001-cd2.mkv')
    client = make_client(tmp_path, FakeRepository(), FakeService())

    assert client.post('/api/merge/tasks', json={'avid': 'XYZ-001'}).json() == {
        'error': {'code': 'merge_title_incomplete'}
    }
    assert client.post('/api/merge/tasks', json={'avid': 'MIX-001'}).json() == {
        'error': {'code': 'merge_container_unsupported'},
    }
    missing = client.post('/api/merge/tasks', json={'avid': 'NONE-001'})
    assert (missing.status_code, missing.json()) == (404, {'error': {'code': 'merge_title_not_found'}})


def test_create_is_refused_while_merging_is_unavailable(tmp_path: Path) -> None:
    parts(tmp_path, 'type/vr/SQTEVR/SQTEVR-009', 'SQTEVR-009', 12, 'type/vr')
    client = make_client(tmp_path, FakeRepository(), FakeService('the merge Job template is not mounted'))

    response = client.post('/api/merge/tasks', json={'avid': 'SQTEVR-009'})

    assert (response.status_code, response.json()) == (409, {'error': {'code': 'merge_unavailable'}})
    assert client.get('/api/merge/tasks').json() == {
        'items': [],
        'unavailable': 'the merge Job template is not mounted',
        'auto': None,
    }


def test_cancel_and_retry_report_refusals_as_codes(tmp_path: Path) -> None:
    client = make_client(tmp_path, FakeRepository(), FakeService())

    assert client.post('/api/merge/tasks/1/cancel').json()['state'] == 'cancelled'
    missing = client.post('/api/merge/tasks/9/cancel')
    assert (missing.status_code, missing.json()) == (404, {'error': {'code': 'merge_task_not_found'}})
    refused = client.post('/api/merge/tasks/1/retry')
    assert (refused.status_code, refused.json()) == (409, {'error': {'code': 'merge_task_not_failed'}})


def test_a_task_that_failed_while_replacing_offers_retry_only(tmp_path: Path) -> None:
    repository = FakeRepository()
    repository.tasks.append(task(1, 'SQTEVR-009', MergeState.FAILED, failed_state=MergeState.REPLACING, error='x'))
    client = make_client(tmp_path, repository, FakeService())

    (item,) = client.get('/api/merge/tasks').json()['items']

    assert (item['state'], item['failed_state'], item['cancellable'], item['retryable']) == (
        'failed',
        'replacing',
        False,
        True,
    )


def test_titles_are_scanned_once_until_a_refresh_is_asked_for(tmp_path: Path) -> None:
    parts(tmp_path, 'type/vr/SQTEVR/SQTEVR-009', 'SQTEVR-009', 12, 'type/vr')
    client = make_client(tmp_path, FakeRepository(), FakeService())

    first = client.get('/api/merge/titles').json()
    parts(tmp_path, 'rank/ABP/ABP-123', 'ABP-123', 3, 'rank')
    cached = client.get('/api/merge/titles').json()
    refreshed = client.get('/api/merge/titles', params={'refresh': 'true'}).json()

    assert [item['avid'] for item in cached['items']] == ['SQTEVR-009']
    assert cached['scanned_at'] == first['scanned_at']
    assert [item['avid'] for item in refreshed['items']] == ['SQTEVR-009', 'ABP-123']
    assert refreshed['scanned_at'] > first['scanned_at']


def test_create_reads_the_title_directory_as_it_is_now(tmp_path: Path) -> None:
    parts(tmp_path, 'type/vr/SQTEVR/SQTEVR-009', 'SQTEVR-009', 12, 'type/vr')
    repository = FakeRepository()
    client = make_client(tmp_path, repository, FakeService())
    client.get('/api/merge/titles')

    parts(tmp_path, 'type/vr/SQTEVR/SQTEVR-009', 'SQTEVR-009', 13, 'type/vr')
    parts(tmp_path, 'rank/ABP/ABP-123', 'ABP-123', 3, 'rank')

    assert client.post('/api/merge/tasks', json={'avid': 'SQTEVR-009'}).json()['part_count'] == 13
    assert client.post('/api/merge/tasks', json={'avid': 'ABP-123'}).status_code == 201
    assert [task.avid for task in repository.tasks] == ['SQTEVR-009', 'ABP-123']


def test_create_refuses_a_title_whose_parts_left_since_the_scan(tmp_path: Path) -> None:
    parts(tmp_path, 'type/vr/SQTEVR/SQTEVR-009', 'SQTEVR-009', 12, 'type/vr')
    client = make_client(tmp_path, FakeRepository(), FakeService())
    client.get('/api/merge/titles')

    for strm_file in (tmp_path / 'type/vr/SQTEVR/SQTEVR-009').glob('*.strm'):
        strm_file.unlink()

    missing = client.post('/api/merge/tasks', json={'avid': 'SQTEVR-009'})
    assert (missing.status_code, missing.json()) == (404, {'error': {'code': 'merge_title_not_found'}})


def test_a_warmed_cache_answers_the_first_visit(tmp_path: Path) -> None:
    parts(tmp_path, 'type/vr/SQTEVR/SQTEVR-009', 'SQTEVR-009', 12, 'type/vr')
    catalog = MergeCatalog(
        archive=lambda: ARCHIVE,
        mapping=lambda: MappingConfig(src_dir='/remote', dst_dir=str(tmp_path)),
        task_dirs_for=_no_task_dirs,
    )
    cache = TitleScanCache()
    asyncio.run(warm_title_cache(catalog, cache))
    parts(tmp_path, 'rank/ABP/ABP-123', 'ABP-123', 3, 'rank')

    client = make_client(tmp_path, FakeRepository(), FakeService(), cache=cache)

    assert [item['avid'] for item in client.get('/api/merge/titles').json()['items']] == ['SQTEVR-009']


def test_warming_logs_instead_of_raising(caplog: pytest.LogCaptureFixture) -> None:
    def broken() -> MappingConfig:
        msg = 'config store not loaded'
        raise RuntimeError(msg)

    catalog = MergeCatalog(archive=lambda: ARCHIVE, mapping=broken, task_dirs_for=_no_task_dirs)

    asyncio.run(warm_title_cache(catalog, TitleScanCache()))

    assert 'could not scan the mapping tree' in caplog.text


async def _no_task_dirs(avids) -> dict[str, str]:
    del avids
    return {}


def test_task_listing_reports_automatic_merging(tmp_path: Path) -> None:
    status = AutoStatus(
        state=AutoState.IDLE,
        room=10,
        skipped=[SkippedTitle(avid='SQTEVR-009', size=20, reason='too_big')],
        skipped_count=1,
        checked_at=NOW,
    )
    client = make_client(tmp_path, FakeRepository(), FakeService(), auto_status=lambda: status)

    auto = client.get('/api/merge/tasks').json()['auto']

    assert auto['state'] == 'idle'
    assert auto['skipped'] == [{'avid': 'SQTEVR-009', 'size': 20, 'reason': 'too_big'}]
