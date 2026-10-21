"""Playlist endpoints over an in-memory repository and ledger."""

import asyncio
import time

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from embyx_manager.config.models import PlaylistsConfig, RssCategory, RssConfig
from embyx_manager.errors import ApiError
from embyx_manager.monitor.acquisitions import AcquisitionState
from embyx_manager.monitor.manual import DirectoryNotRoutedError, ManualEntry, ManualOutcome, ManualSubmission
from embyx_manager.monitor.playlists import PlaylistSourceState
from embyx_manager.monitor.playlists_api import (
    NO_FILL_DIRECTORY,
    PlaylistFillApi,
    create_playlists_router,
    resolve_fill_dir,
)
from tests.test_monitor_playlist_sync import NOW, FakeRepository, record


class FakeLedger:
    def __init__(self, states: dict[str, AcquisitionState]) -> None:
        self.states = states
        self.asked: list[list[str]] = []

    async def states_for(self, avids) -> dict[str, AcquisitionState]:
        self.asked.append(list(avids))
        return {avid: state for avid, state in self.states.items() if avid in avids}


async def allow() -> None:
    return None


def make_client(repository: FakeRepository, ledger: FakeLedger | None = None) -> TestClient:
    app = FastAPI()

    @app.exception_handler(ApiError)
    async def handle(_request, exc: ApiError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={'error': {'code': exc.code}})

    app.include_router(create_playlists_router(repository, mutation_auth=allow, ledger=ledger))  # type: ignore[arg-type]
    return TestClient(app)


def synced(key: str, avids: list[str], present: list[str], **kwargs) -> object:
    base = record(key, avids, **kwargs)
    return type(base)(
        **{
            **base.__dict__,
            'present': tuple(present),
            'missing': tuple(a for a in avids if a not in present),
            'emby_playlist_id': 'pl1',
            'last_synced_at': NOW,
        },
    )


def test_list_reports_counts_and_source() -> None:
    repository = FakeRepository(
        [synced('k7', ['A-001', 'B-002', 'C-003'], ['B-002']), record('k2024', ['D-004'], kind=2024, name='2024')],
        PlaylistSourceState('20260112', NOW),
    )

    body = make_client(repository).get('/api/playlists').json()

    assert body['source'] == {'database_name': '20260112', 'fetched_at': '2026-09-20T12:00:00Z'}
    assert [(item['key'], item['total'], item['present'], item['missing']) for item in body['items']] == [
        ('k7', 3, 1, 2),
        ('k2024', 1, 0, 0),
    ]
    assert body['items'][0]['emby_playlist_id'] == 'pl1'
    assert body['items'][1]['last_synced_at'] is None


def test_missing_lists_the_gap_in_ranking_order_with_ledger_state() -> None:
    repository = FakeRepository([synced('k7', ['A-001', 'B-002', 'C-003'], ['B-002'])])
    ledger = FakeLedger({'C-003': AcquisitionState.DOWNLOADING, 'B-002': AcquisitionState.ARCHIVED})

    body = make_client(repository, ledger).get('/api/playlists/k7/missing').json()

    assert body['name'] == 'JavDB 有码 TOP250'
    assert body['items'] == [
        {'rank': 1, 'avid': 'A-001', 'title': 'a-001', 'tracked': None},
        {'rank': 3, 'avid': 'C-003', 'title': 'c-003', 'tracked': 'downloading'},
    ]
    assert ledger.asked == [['A-001', 'C-003']]


def test_missing_without_a_ledger_and_unknown_keys() -> None:
    client = make_client(FakeRepository([synced('k7', ['A-001'], [])]))

    assert client.get('/api/playlists/k7/missing').json()['items'][0]['tracked'] is None
    assert client.get('/api/playlists/nope/missing').status_code == 404
    assert client.patch('/api/playlists/nope', json={'enabled': False}).status_code == 404


def test_awards_keys_with_spaces_and_cjk_round_trip_through_the_path() -> None:
    key = 'k4:第四届JAV金鸡儿奖 最佳故事片 提名'
    repository = FakeRepository([record(key, ['A-001'], kind=4, name='第四届JAV金鸡儿奖 最佳故事片 提名')])
    client = make_client(repository)

    assert client.get(f'/api/playlists/{key}/missing').json()['key'] == key
    assert client.patch(f'/api/playlists/{key}', json={'enabled': False}).json()['enabled'] is False


def test_patch_flips_only_the_flag() -> None:
    repository = FakeRepository([synced('k7', ['A-001'], ['A-001'])])
    client = make_client(repository)

    body = client.patch('/api/playlists/k7', json={'enabled': False}).json()

    assert body['enabled'] is False
    assert body['emby_playlist_id'] == 'pl1'  # the sync, not the flag, retires the playlist
    assert client.patch('/api/playlists/k7', json={'enabled': True, 'name': 'x'}).status_code == 422
    assert repository.records['k7'].updated_at > NOW


# -- filling -----------------------------------------------------------------------


class FakeManual:
    def __init__(
        self,
        outcomes: dict[str, ManualOutcome] | None = None,
        error: Exception | None = None,
        refuse: Exception | None = None,
        gate: asyncio.Event | None = None,
    ) -> None:
        self.outcomes = outcomes or {}
        #: Raised by the batch itself, after the target was accepted.
        self.error = error
        #: Raised by the up-front target check.
        self.refuse = refuse
        #: When set, the batch reports its first entry and then waits on it.
        self.gate = gate
        self.calls: list[dict] = []

    async def check_target(self, task_dir_path: str) -> str:
        if self.refuse is not None:
            raise self.refuse
        return task_dir_path

    async def submit(self, inputs, *, task_dir_path, source, limit, queue, on_entry) -> ManualSubmission:
        self.calls.append(
            {'inputs': list(inputs), 'task_dir_path': task_dir_path, 'source': source, 'limit': limit, 'queue': queue},
        )
        entries = []
        for index, avid in enumerate(inputs):
            if index == 1 and self.gate is not None:
                await self.gate.wait()
            if self.error is not None:
                raise self.error
            entry = ManualEntry(text=avid, avid=avid, outcome=self.outcomes.get(avid, ManualOutcome.QUEUED))
            entries.append(entry)
            on_entry(entry)
        return ManualSubmission(task_dir_path=task_dir_path, entries=tuple(entries))


def make_fill_client(repository: FakeRepository, manual: FakeManual, task_dir: str | None = '/115/embyx_in/rank'):
    app = FastAPI()

    @app.exception_handler(ApiError)
    async def handle(_request, exc: ApiError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={'error': {'code': exc.code}})

    fill = PlaylistFillApi(
        manual=manual,  # type: ignore[arg-type]
        task_dir=lambda: (task_dir, None) if task_dir else (None, NO_FILL_DIRECTORY),
    )
    app.include_router(create_playlists_router(repository, mutation_auth=allow, fill=fill))  # type: ignore[arg-type]
    return TestClient(app)


def finished_fill(client: TestClient, key: str) -> dict:
    """The fill's view once its background task has ended."""
    for _ in range(200):
        body = client.get(f'/api/playlists/{key}/fill').json()
        if body['state'] != 'running':
            return body
        time.sleep(0.01)
    msg = 'the fill never finished'
    raise AssertionError(msg)


def test_resolve_fill_dir_prefers_the_section_then_the_ranking_category() -> None:
    rss = RssConfig(
        categories=(
            RssCategory(label='Actor', task_dir_path='/115/embyx_in/clt'),
            RssCategory(label='Rank', task_dir_path='/115/embyx_in/rank'),
        ),
    )
    assert resolve_fill_dir(PlaylistsConfig(task_dir_path='/115/lists'), rss) == ('/115/lists', None)
    assert resolve_fill_dir(PlaylistsConfig(), rss) == ('/115/embyx_in/rank', None)
    chinese = RssConfig(categories=(RssCategory(label='榜单', task_dir_path='/115/bang'),))
    assert resolve_fill_dir(PlaylistsConfig(), chinese) == ('/115/bang', None)
    assert resolve_fill_dir(PlaylistsConfig(), RssConfig()) == (None, NO_FILL_DIRECTORY)


def test_list_reports_where_a_fill_would_go() -> None:
    repository = FakeRepository([synced('k7', ['A-001'], [])])
    assert (
        make_fill_client(repository, FakeManual()).get('/api/playlists').json()['fill_task_dir'] == '/115/embyx_in/rank'
    )

    body = make_fill_client(repository, FakeManual(), task_dir=None).get('/api/playlists').json()
    assert body['fill_task_dir'] is None
    assert body['fill_reason'] == NO_FILL_DIRECTORY

    unmounted = make_client(repository).get('/api/playlists').json()
    assert (unmounted['fill_task_dir'], unmounted['fill_reason']) == (None, 'filling is not available')


def test_fill_queues_the_whole_gap_in_the_background_under_the_playlist_source() -> None:
    repository = FakeRepository([synced('k7', ['A-001', 'B-002', 'C-003', 'D-004'], ['B-002'])])
    manual = FakeManual({'C-003': ManualOutcome.ALREADY_TRACKED, 'D-004': ManualOutcome.ALREADY_IN_LIBRARY})

    with make_fill_client(repository, manual) as client:
        started = client.post('/api/playlists/k7/fill')
        assert started.status_code == 202
        assert started.json()['total'] == 3
        body = finished_fill(client, 'k7')
        listed = client.get('/api/playlists').json()['fills']

    assert manual.calls == [
        {
            'inputs': ['A-001', 'C-003', 'D-004'],
            'task_dir_path': '/115/embyx_in/rank',
            'source': 'playlist:k7',
            'limit': None,
            'queue': True,
        },
    ]
    assert body['state'] == 'done'
    assert body['task_dir_path'] == '/115/embyx_in/rank'
    assert (body['done'], body['total']) == (3, 3)
    assert [(item['avid'], item['outcome']) for item in body['items']] == [
        ('A-001', 'queued'),
        ('C-003', 'already_tracked'),
        ('D-004', 'already_in_library'),
    ]
    assert body['counts'] == {'queued': 1, 'already_tracked': 1, 'already_in_library': 1}
    assert body['finished_at'] is not None
    assert listed == [body]


def test_a_running_fill_reports_progress_and_is_not_started_twice() -> None:
    repository = FakeRepository([synced('k7', ['A-001', 'B-002', 'C-003'], [])])
    gate = asyncio.Event()
    manual = FakeManual(gate=gate)

    with make_fill_client(repository, manual) as client:
        client.post('/api/playlists/k7/fill')
        for _ in range(200):
            progress = client.get('/api/playlists/k7/fill').json()
            if progress['done']:
                break
            time.sleep(0.01)
        again = client.post('/api/playlists/k7/fill').json()
        client.portal.call(gate.set)  # type: ignore[union-attr]
        body = finished_fill(client, 'k7')

    assert (progress['state'], progress['done'], progress['total']) == ('running', 1, 3)
    assert progress['finished_at'] is None
    assert again['state'] == 'running'
    assert len(manual.calls) == 1
    assert (body['state'], body['done']) == ('done', 3)


def test_a_fill_that_breaks_midway_keeps_what_it_queued() -> None:
    repository = FakeRepository([synced('k7', ['A-001'], [])])

    with make_fill_client(repository, FakeManual(error=RuntimeError('boom'))) as client:
        client.post('/api/playlists/k7/fill')
        body = finished_fill(client, 'k7')

    assert (body['state'], body['error']) == ('failed', 'fill_failed')


def test_fill_refusals() -> None:
    repository = FakeRepository([synced('k7', ['A-001'], [])])

    assert make_fill_client(repository, FakeManual(), task_dir=None).post('/api/playlists/k7/fill').status_code == 422
    assert make_fill_client(repository, FakeManual()).post('/api/playlists/nope/fill').status_code == 404
    unrouted = make_fill_client(repository, FakeManual(refuse=DirectoryNotRoutedError())).post('/api/playlists/k7/fill')
    assert unrouted.status_code == 422
    assert unrouted.json()['error']['code'] == 'directory_not_routed'
    assert make_fill_client(repository, FakeManual()).get('/api/playlists/k7/fill').status_code == 404
    # Without the fill dependency the route does not exist at all.
    assert make_client(repository).post('/api/playlists/k7/fill').status_code in {404, 405}


def test_fill_of_a_complete_list_submits_nothing() -> None:
    repository = FakeRepository([synced('k7', ['A-001'], ['A-001'])])
    manual = FakeManual()

    with make_fill_client(repository, manual) as client:
        client.post('/api/playlists/k7/fill')
        body = finished_fill(client, 'k7')

    assert manual.calls[0]['inputs'] == []
    assert body['items'] == []
    assert body['counts'] == {}
    assert body['state'] == 'done'
