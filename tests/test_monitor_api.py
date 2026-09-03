from datetime import UTC, datetime

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from embyx_manager.errors import ApiError
from embyx_manager.monitor.acquisitions import (
    AcquisitionState,
    AttemptState,
    MagnetCandidate,
)
from embyx_manager.monitor.api import AcquisitionApi, SubscriptionsApi, create_monitor_router
from embyx_manager.monitor.manual import (
    DirectoryListing,
    DirectoryNotFoundError,
    DirectoryNotRoutedError,
    ManualEntry,
    ManualIntakeError,
    ManualOutcome,
    ManualSubmission,
    OfflineDirectory,
)
from embyx_manager.monitor.reports import PipelineName, RunState, RunTrigger
from embyx_manager.monitor.runs import PipelineRunRecord
from embyx_manager.monitor.scheduler import (
    PipelineBusyError,
    PipelineNotConfiguredError,
    PipelineStatus,
    TrackerState,
)
from tests.test_monitor_rss import HASH_A, HASH_B, FakeLedger, FakeSubscriptions, make_subscription, now_stub


def make_record(run_id: str, pipeline: PipelineName, state: RunState = RunState.COMPLETED) -> PipelineRunRecord:
    return PipelineRunRecord(
        run_id=run_id,
        pipeline=pipeline,
        trigger=RunTrigger.MANUAL,
        state=state,
        started_at=datetime(2026, 8, 12, 10, 0, tzinfo=UTC),
        finished_at=datetime(2026, 8, 12, 10, 5, tzinfo=UTC),
        stats={'items': 2},
        errors=('boom',) if state is RunState.FAILED else (),
        log_tail=('line-1',),
    )


class FakeScheduler:
    def __init__(self) -> None:
        self.triggered: list[PipelineName] = []
        self.busy = False
        self.unconfigured_reason: str | None = None
        self.running: set[PipelineName] = set()

    def tracker_state(self) -> TrackerState:
        return TrackerState()

    def status(self) -> tuple[PipelineStatus, ...]:
        return (
            PipelineStatus(
                pipeline=PipelineName.RSS,
                enabled=True,
                configured=True,
                reason=None,
                running_run_id=None,
                next_scheduled_at=datetime(2026, 8, 12, 11, 0, tzinfo=UTC),
            ),
            PipelineStatus(
                pipeline=PipelineName.ARCHIVE,
                enabled=False,
                configured=False,
                reason='archive source, destination, and mapping must be configured',
                running_run_id=None,
                next_scheduled_at=None,
            ),
        )

    async def trigger(self, pipeline: PipelineName) -> str:
        if self.unconfigured_reason is not None:
            raise PipelineNotConfiguredError(pipeline, self.unconfigured_reason)
        if self.busy:
            raise PipelineBusyError(pipeline)
        self.triggered.append(pipeline)
        return 'run-1'

    async def cancel_running(self, pipeline: PipelineName) -> bool:
        return pipeline in self.running


class FakeRuns:
    def __init__(self, records: list[PipelineRunRecord]) -> None:
        self.records = {record.run_id: record for record in records}
        self.ordered = records

    async def latest_run(self, pipeline: PipelineName) -> PipelineRunRecord | None:
        matches = [record for record in self.ordered if record.pipeline is pipeline]
        return matches[0] if matches else None

    async def list_runs(self, pipeline=None, *, limit=20, offset=0):
        selected = [record for record in self.ordered if pipeline is None or record.pipeline is pipeline]
        return tuple(selected[offset : offset + limit])

    async def get_run(self, run_id: str) -> PipelineRunRecord | None:
        return self.records.get(run_id)


async def _noop_auth() -> None:
    return None


def make_client(
    scheduler: FakeScheduler,
    runs: FakeRuns,
    subscriptions: SubscriptionsApi | None = None,
) -> TestClient:
    app = FastAPI()
    app.include_router(
        create_monitor_router(scheduler, runs, mutation_auth=_noop_auth, subscriptions=subscriptions),  # type: ignore[arg-type]
    )

    @app.exception_handler(ApiError)
    async def handle(_request, exc):
        return JSONResponse({'error': {'code': exc.code}}, status_code=exc.status_code)

    return TestClient(app)


def test_status_includes_latest_run() -> None:
    scheduler = FakeScheduler()
    runs = FakeRuns([make_record('run-1', PipelineName.RSS)])

    with make_client(scheduler, runs) as client:
        body = client.get('/api/monitor/status').json()

    rss = next(item for item in body if item['pipeline'] == 'rss')
    assert rss['enabled'] is True
    assert rss['last_run']['run_id'] == 'run-1'
    assert rss['last_run']['stats'] == {'items': 2}
    archive = next(item for item in body if item['pipeline'] == 'archive')
    assert archive['configured'] is False
    assert archive['last_run'] is None


def test_trigger_starts_the_pipeline() -> None:
    scheduler = FakeScheduler()
    runs = FakeRuns([])

    with make_client(scheduler, runs) as client:
        response = client.post('/api/monitor/rss/trigger')

    assert response.status_code == 202
    assert response.json() == {'run_id': 'run-1'}
    assert scheduler.triggered == [PipelineName.RSS]


def test_every_pipeline_can_be_triggered_by_hand() -> None:
    """The archive scan in particular: its cron is a fallback, not the only way in."""
    scheduler = FakeScheduler()
    runs = FakeRuns([])

    with make_client(scheduler, runs) as client:
        codes = [client.post(f'/api/monitor/{name}/trigger').status_code for name in ('rss', 'archive', 'mapping')]

    assert codes == [202, 202, 202]
    assert scheduler.triggered == [PipelineName.RSS, PipelineName.ARCHIVE, PipelineName.MAPPING]


def test_trigger_busy_and_unconfigured_and_unknown() -> None:
    scheduler = FakeScheduler()
    runs = FakeRuns([])

    with make_client(scheduler, runs) as client:
        scheduler.busy = True
        busy = client.post('/api/monitor/rss/trigger', json={})
        scheduler.busy = False
        scheduler.unconfigured_reason = 'CloudDrive is not configured'
        unconfigured = client.post('/api/monitor/rss/trigger', json={})
        unknown = client.post('/api/monitor/nope/trigger', json={})

    assert busy.status_code == 409
    assert busy.json()['error']['code'] == 'pipeline_busy'
    assert unconfigured.status_code == 422
    assert unknown.status_code == 404


def test_cancel_running_pipeline() -> None:
    scheduler = FakeScheduler()
    scheduler.running.add(PipelineName.MAPPING)
    runs = FakeRuns([])

    with make_client(scheduler, runs) as client:
        ok = client.post('/api/monitor/mapping/cancel')
        not_running = client.post('/api/monitor/rss/cancel')

    assert ok.status_code == 200
    assert ok.json() == {'cancelling': True}
    assert not_running.status_code == 409


def test_runs_listing_and_detail() -> None:
    scheduler = FakeScheduler()
    records = [
        make_record('run-2', PipelineName.RSS, RunState.FAILED),
        make_record('run-1', PipelineName.MAPPING),
    ]
    runs = FakeRuns(records)

    with make_client(scheduler, runs) as client:
        everything = client.get('/api/monitor/runs').json()
        rss_only = client.get('/api/monitor/runs', params={'pipeline': 'rss'}).json()
        detail = client.get('/api/monitor/runs/run-2').json()
        missing = client.get('/api/monitor/runs/none')

    assert [item['run_id'] for item in everything] == ['run-2', 'run-1']
    assert [item['run_id'] for item in rss_only] == ['run-2']
    assert detail['errors'] == ['boom']
    assert detail['log_tail'] == ['line-1']
    assert missing.status_code == 404


# -- acquisition ledger -------------------------------------------------------


def make_ledger_client(
    ledger: FakeLedger,
    *,
    submit_ok: bool = True,
    tracker_reason: str | None = None,
) -> tuple[TestClient, list[tuple[str, str]]]:
    submitted: list[tuple[str, str]] = []

    async def submit(avid: str, magnet: str) -> bool:
        submitted.append((avid, magnet))
        return submit_ok

    app = FastAPI()
    app.include_router(
        create_monitor_router(
            FakeScheduler(),  # type: ignore[arg-type]
            FakeRuns([]),  # type: ignore[arg-type]
            mutation_auth=_noop_auth,
            acquisitions=AcquisitionApi(
                ledger=ledger,  # type: ignore[arg-type]
                submit_magnet=submit,
                tracker_ready=lambda: tracker_reason,
            ),
        ),
    )

    @app.exception_handler(ApiError)
    async def handle(_request, exc):
        return JSONResponse({'error': {'code': exc.code}}, status_code=exc.status_code)

    return TestClient(app), submitted


async def seed_ledger(**states: AcquisitionState) -> FakeLedger:
    ledger = FakeLedger()
    for avid, state in states.items():
        real_avid = avid.replace('_', '-')
        await ledger.discover(real_avid, source='rss:Actor', now=NOW)
        await ledger.add_attempts(
            real_avid,
            [MagnetCandidate(magnet=f'magnet:?xt=urn:btih:{HASH_A}', info_hash=HASH_A, source='sukebei')],
            now=NOW,
        )
        ledger.states[real_avid] = state
    return ledger


async def test_acquisitions_list_filters_by_state_and_reports_counts() -> None:
    ledger = await seed_ledger(ABC_123=AcquisitionState.NEEDS_ATTENTION, DEF_456=AcquisitionState.DOWNLOADING)
    client, _ = make_ledger_client(ledger)

    everything = client.get('/api/monitor/acquisitions').json()
    assert {item['avid'] for item in everything['items']} == {'ABC-123', 'DEF-456'}
    assert everything['counts'] == {'needs_attention': 1, 'downloading': 1}

    parked = client.get('/api/monitor/acquisitions', params={'state': 'needs_attention'}).json()
    assert [item['avid'] for item in parked['items']] == ['ABC-123']


async def test_unknown_state_filter_is_rejected() -> None:
    client, _ = make_ledger_client(await seed_ledger())

    response = client.get('/api/monitor/acquisitions', params={'state': 'nonsense'})

    assert response.status_code == 400
    assert response.json()['error']['code'] == 'unknown_state'


async def test_acquisition_detail_lists_its_attempts() -> None:
    ledger = await seed_ledger(ABC_123=AcquisitionState.DOWNLOADING)
    client, _ = make_ledger_client(ledger)

    body = client.get('/api/monitor/acquisitions/ABC-123').json()

    assert body['avid'] == 'ABC-123'
    assert [attempt['attempt_no'] for attempt in body['attempts']] == [1]
    assert body['attempts'][0]['magnet_source'] == 'sukebei'
    assert client.get('/api/monitor/acquisitions/ZZZ-999').status_code == 404


async def test_retry_submits_the_next_magnet_and_resumes_downloading() -> None:
    ledger = await seed_ledger(ABC_123=AcquisitionState.EXHAUSTED)
    client, submitted = make_ledger_client(ledger)

    body = client.post('/api/monitor/acquisitions/ABC-123/retry').json()

    assert body['state'] == 'downloading'
    assert [avid for avid, _ in submitted] == ['ABC-123']


async def test_retry_without_a_magnet_left_is_a_conflict() -> None:
    ledger = await seed_ledger(ABC_123=AcquisitionState.EXHAUSTED)
    await ledger.claim_next_pending('ABC-123', now=NOW)
    client, submitted = make_ledger_client(ledger)

    response = client.post('/api/monitor/acquisitions/ABC-123/retry')

    assert response.status_code == 409
    assert response.json()['error']['code'] == 'no_magnet_left'
    assert submitted == []


async def test_a_rejected_offline_task_marks_the_attempt_and_reports_upstream() -> None:
    ledger = await seed_ledger(ABC_123=AcquisitionState.EXHAUSTED)
    client, _ = make_ledger_client(ledger, submit_ok=False)

    response = client.post('/api/monitor/acquisitions/ABC-123/retry')

    assert response.status_code == 502
    assert ledger.attempt_states('ABC-123') == [AttemptState.ERROR]


async def test_an_operator_magnet_is_recorded_and_submitted() -> None:
    ledger = await seed_ledger(ABC_123=AcquisitionState.NEEDS_ATTENTION)
    client, submitted = make_ledger_client(ledger)
    magnet = f'magnet:?xt=urn:btih:{HASH_B}&dn=ABC-123'

    body = client.post('/api/monitor/acquisitions/ABC-123/magnet', json={'magnet': magnet}).json()

    assert body['state'] == 'downloading'
    assert submitted == [('ABC-123', magnet)]
    assert ledger.magnets('ABC-123')[-1] == magnet


async def test_an_unusable_magnet_is_refused() -> None:
    ledger = await seed_ledger(ABC_123=AcquisitionState.NEEDS_ATTENTION)
    client, submitted = make_ledger_client(ledger)

    response = client.post('/api/monitor/acquisitions/ABC-123/magnet', json={'magnet': 'https://example.com/x'})

    assert response.status_code == 400
    assert response.json()['error']['code'] == 'unusable_magnet'
    assert submitted == []


async def test_a_magnet_already_tried_is_not_queued_twice() -> None:
    ledger = await seed_ledger(ABC_123=AcquisitionState.NEEDS_ATTENTION)
    client, submitted = make_ledger_client(ledger)

    response = client.post(
        '/api/monitor/acquisitions/ABC-123/magnet',
        json={'magnet': f'magnet:?xt=urn:btih:{HASH_A}'},
    )

    assert response.status_code == 409
    assert response.json()['error']['code'] == 'magnet_already_tried'
    assert submitted == []


async def test_ignoring_an_avid_takes_it_out_of_the_pipeline() -> None:
    ledger = await seed_ledger(ABC_123=AcquisitionState.EXHAUSTED)
    client, _ = make_ledger_client(ledger)

    body = client.post('/api/monitor/acquisitions/ABC-123/ignore').json()

    assert body['state'] == 'ignored'


async def test_an_archived_avid_cannot_be_retried_or_ignored() -> None:
    ledger = await seed_ledger(ABC_123=AcquisitionState.ARCHIVED)
    client, submitted = make_ledger_client(ledger)

    assert client.post('/api/monitor/acquisitions/ABC-123/retry').status_code == 409
    assert client.post('/api/monitor/acquisitions/ABC-123/ignore').status_code == 409
    assert submitted == []


async def test_resume_hands_a_parked_avid_back_to_the_tracker() -> None:
    ledger = await seed_ledger(ABC_123=AcquisitionState.NEEDS_ATTENTION)
    client, _ = make_ledger_client(ledger)

    body = client.post('/api/monitor/acquisitions/ABC-123/resume').json()

    assert body['state'] == 'downloading'


async def test_resume_only_applies_to_parked_avids() -> None:
    ledger = await seed_ledger(ABC_123=AcquisitionState.DOWNLOADING)
    client, _ = make_ledger_client(ledger)

    response = client.post('/api/monitor/acquisitions/ABC-123/resume')

    assert response.status_code == 409
    assert response.json()['error']['code'] == 'not_parked'


async def test_tracker_status_reports_why_it_is_not_running() -> None:
    client, _ = make_ledger_client(await seed_ledger(), tracker_reason='archive is disabled')

    body = client.get('/api/monitor/tracker').json()

    assert body['running'] is False
    assert body['reason'] == 'archive is disabled'


def test_ledger_routes_are_absent_without_a_ledger() -> None:
    client = make_client(FakeScheduler(), FakeRuns([]))

    assert client.get('/api/monitor/acquisitions').status_code == 404


# -- the manual input source ---------------------------------------------------


class FakeManual:
    """Stands in for ManualIntakeSource; its own behaviour is tested separately."""

    def __init__(self, error: ManualIntakeError | None = None) -> None:
        self.error = error
        self.browsed: list[str] = []
        self.submitted: list[tuple[list[str], str]] = []

    async def browse(self, path: str) -> DirectoryListing:
        if self.error is not None:
            raise self.error
        self.browsed.append(path)
        return DirectoryListing(
            path=path,
            parent='/',
            entries=(
                OfflineDirectory(path=f'{path}/embyx_in', name='embyx_in', configured=True, routed=True),
                OfflineDirectory(path=f'{path}/misc', name='misc', configured=False, routed=False),
            ),
        )

    async def default_directory(self) -> str | None:
        return '/115/embyx_in'

    async def submit(self, inputs: list[str], *, task_dir_path: str) -> ManualSubmission:
        if self.error is not None:
            raise self.error
        self.submitted.append((list(inputs), task_dir_path))
        return ManualSubmission(
            task_dir_path=task_dir_path,
            entries=(
                ManualEntry(text='abc-123', avid='ABC-123', outcome=ManualOutcome.SUBMITTED),
                ManualEntry(
                    text='def-456',
                    avid='DEF-456',
                    outcome=ManualOutcome.ALREADY_IN_LIBRARY,
                    archived_paths=('embyx/DEF/DEF-456.mp4',),
                ),
            ),
        )


def make_manual_client(manual: FakeManual) -> TestClient:
    app = FastAPI()
    app.include_router(
        create_monitor_router(
            FakeScheduler(),  # type: ignore[arg-type]
            FakeRuns([]),  # type: ignore[arg-type]
            mutation_auth=_noop_auth,
            acquisitions=AcquisitionApi(ledger=FakeLedger(), manual=manual),  # type: ignore[arg-type]
        ),
    )

    @app.exception_handler(ApiError)
    async def handle(_request, exc):
        return JSONResponse({'error': {'code': exc.code}}, status_code=exc.status_code)

    return TestClient(app)


def test_browsing_reports_the_directories_and_where_the_picker_opens() -> None:
    manual = FakeManual()
    client = make_manual_client(manual)

    body = client.get('/api/monitor/manual/directories', params={'path': '/115'}).json()

    assert manual.browsed == ['/115']
    assert body['parent'] == '/'
    assert body['default_path'] == '/115/embyx_in'
    assert [(item['name'], item['routed']) for item in body['entries']] == [('embyx_in', True), ('misc', False)]


def test_submitting_reports_the_outcome_of_every_line() -> None:
    manual = FakeManual()
    client = make_manual_client(manual)

    body = client.post(
        '/api/monitor/manual',
        json={'inputs': ['abc-123', 'def-456'], 'task_dir_path': '/115/embyx_in'},
    ).json()

    assert manual.submitted == [(['abc-123', 'def-456'], '/115/embyx_in')]
    assert body['task_dir_path'] == '/115/embyx_in'
    assert [(item['avid'], item['outcome']) for item in body['items']] == [
        ('ABC-123', 'submitted'),
        ('DEF-456', 'already_in_library'),
    ]
    assert body['items'][1]['archived_paths'] == ['embyx/DEF/DEF-456.mp4']


def test_a_directory_without_a_route_is_refused_with_its_reason() -> None:
    client = make_manual_client(FakeManual(DirectoryNotRoutedError()))

    response = client.post('/api/monitor/manual', json={'inputs': ['abc-123'], 'task_dir_path': '/115/misc'})

    assert response.status_code == 422
    assert response.json()['error']['code'] == 'directory_not_routed'


def test_a_missing_directory_is_a_404() -> None:
    client = make_manual_client(FakeManual(DirectoryNotFoundError()))

    assert client.get('/api/monitor/manual/directories', params={'path': '/nope'}).status_code == 404


def test_manual_routes_are_absent_without_the_source() -> None:
    client, _ = make_ledger_client(FakeLedger())

    assert client.get('/api/monitor/manual/directories').status_code == 404
    assert client.post('/api/monitor/manual', json={'inputs': [], 'task_dir_path': '/x'}).status_code == 404


NOW = now_stub()


# -- subscriptions ---------------------------------------------------------------


def subscriptions_api(
    records: list = (),  # type: ignore[assignment]
    *,
    categories: tuple[str, ...] = ('Actor', 'Rank'),
) -> SubscriptionsApi:
    return SubscriptionsApi(
        repository=FakeSubscriptions(list(records)),  # type: ignore[arg-type]
        categories=lambda: categories,
    )


def test_subscriptions_are_listed_with_the_configured_categories() -> None:
    api = subscriptions_api([make_subscription(1, url='https://rsshub.test/javbus/star/rwt', name='演员甲')])

    with make_client(FakeScheduler(), FakeRuns([]), subscriptions=api) as client:
        body = client.get('/api/monitor/subscriptions').json()

    assert body['categories'] == ['Actor', 'Rank']
    assert [(item['url'], item['feed_url'], item['name']) for item in body['items']] == [
        ('https://rsshub.test/javbus/star/rwt', 'https://rsshub.test/javbus/star/rwt', '演员甲'),
    ]


def test_subscription_routes_are_absent_without_the_registry() -> None:
    with make_client(FakeScheduler(), FakeRuns([])) as client:
        assert client.get('/api/monitor/subscriptions').status_code == 404


def test_creating_a_subscription_validates_the_url_and_category() -> None:
    api = subscriptions_api()

    with make_client(FakeScheduler(), FakeRuns([]), subscriptions=api) as client:
        created = client.post(
            '/api/monitor/subscriptions',
            json={'url': ' https://rsshub.test/javbus/star/rwt ', 'category': 'Actor'},
        )
        assert created.status_code == 201
        assert created.json()['url'] == 'https://rsshub.test/javbus/star/rwt'
        assert created.json()['category'] == 'Actor'
        assert created.json()['seed_pending'] is False

        bad_url = client.post('/api/monitor/subscriptions', json={'url': 'ftp://x/y', 'category': 'Actor'})
        assert bad_url.status_code == 422
        assert bad_url.json() == {'error': {'code': 'invalid_feed_url'}}

        bad_category = client.post(
            '/api/monitor/subscriptions', json={'url': 'https://rsshub.test/o', 'category': 'Nope'}
        )
        assert bad_category.json() == {'error': {'code': 'unknown_category'}}

        duplicate = client.post(
            '/api/monitor/subscriptions',
            json={'url': 'https://rsshub.test/javbus/star/rwt', 'category': 'Rank'},
        )
        assert duplicate.status_code == 409
        assert duplicate.json() == {'error': {'code': 'subscription_exists'}}


def test_a_subscription_can_be_disabled_moved_and_deleted() -> None:
    api = subscriptions_api([make_subscription(1)])

    with make_client(FakeScheduler(), FakeRuns([]), subscriptions=api) as client:
        disabled = client.patch('/api/monitor/subscriptions/1', json={'enabled': False}).json()
        assert disabled['enabled'] is False

        moved = client.patch('/api/monitor/subscriptions/1', json={'category': 'Rank'}).json()
        assert moved['category'] == 'Rank'
        assert moved['enabled'] is False

        assert client.patch('/api/monitor/subscriptions/1', json={'category': 'Nope'}).status_code == 422
        assert client.patch('/api/monitor/subscriptions/9', json={'enabled': True}).status_code == 404

        assert client.delete('/api/monitor/subscriptions/1').status_code == 204
        assert client.delete('/api/monitor/subscriptions/1').status_code == 404


def test_creating_a_talent_subscription_stores_its_names_and_seeds_the_first_poll() -> None:
    api = subscriptions_api()

    with make_client(FakeScheduler(), FakeRuns([]), subscriptions=api) as client:
        created = client.post(
            '/api/monitor/subscriptions',
            json={
                'kind': 'avbase_talent',
                'category': 'Actor',
                'talent_id': 5022,
                'name': '河北彩花',
                'aliases': ['河北彩伽', ' 河北彩花 ', ''],
                'seed': True,
            },
        )
        assert created.status_code == 201
        body = created.json()
        assert body['kind'] == 'avbase_talent'
        assert body['feed_url'] == 'https://www.avbase.net/talents/5022/feed'
        assert body['aliases'] == ['河北彩伽']
        assert body['seed_pending'] is True
        assert body['cursor_size'] == 0

        missing = client.post(
            '/api/monitor/subscriptions',
            json={'kind': 'avbase_talent', 'category': 'Actor', 'name': '河北彩花'},
        )
        assert missing.json() == {'error': {'code': 'invalid_talent'}}

        duplicate = client.post(
            '/api/monitor/subscriptions',
            json={'kind': 'avbase_talent', 'category': 'Rank', 'talent_id': 5022, 'name': '河北彩花'},
        )
        assert duplicate.status_code == 409

        unknown = client.post('/api/monitor/subscriptions', json={'kind': 'javbus', 'category': 'Actor'})
        assert unknown.json() == {'error': {'code': 'unknown_subscription_kind'}}


def test_a_feed_url_can_be_changed_but_not_into_another_subscription_or_onto_a_talent() -> None:
    api = subscriptions_api(
        [
            make_subscription(1, url='http://rsshub/javlibrary/rank'),
            make_subscription(2, url='https://rsshub.example/other'),
            make_subscription(3, name='石川澪', talent_id=46144),
        ],
    )

    with make_client(FakeScheduler(), FakeRuns([]), subscriptions=api) as client:
        moved = client.patch(
            '/api/monitor/subscriptions/1',
            json={'url': 'http://rsshub.rss.svc.cluster.local/javlibrary/rank'},
        )
        assert moved.status_code == 200
        assert moved.json()['url'] == 'http://rsshub.rss.svc.cluster.local/javlibrary/rank'

        taken = client.patch('/api/monitor/subscriptions/1', json={'url': 'https://rsshub.example/other'})
        assert taken.status_code == 409

        bad = client.patch('/api/monitor/subscriptions/1', json={'url': 'ftp://x'})
        assert bad.json() == {'error': {'code': 'invalid_feed_url'}}

        talent = client.patch('/api/monitor/subscriptions/3', json={'url': 'https://rsshub.example/x'})
        assert talent.json() == {'error': {'code': 'url_not_editable'}}
