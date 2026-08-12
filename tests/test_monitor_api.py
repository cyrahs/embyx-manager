from datetime import UTC, datetime

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from embyx_manager.config import api as config_api
from embyx_manager.monitor.api import create_monitor_router
from embyx_manager.monitor.reports import PipelineName, RunState, RunTrigger
from embyx_manager.monitor.runs import PipelineRunRecord
from embyx_manager.monitor.scheduler import (
    PipelineBusyError,
    PipelineNotConfiguredError,
    PipelineStatus,
)


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
        self.triggered: list[tuple[PipelineName, bool]] = []
        self.busy = False
        self.unconfigured_reason: str | None = None
        self.running: set[PipelineName] = set()

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

    async def trigger(self, pipeline: PipelineName, *, rank: bool = False) -> str:
        if self.unconfigured_reason is not None:
            raise PipelineNotConfiguredError(pipeline, self.unconfigured_reason)
        if self.busy:
            raise PipelineBusyError(pipeline)
        self.triggered.append((pipeline, rank))
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


def make_client(scheduler: FakeScheduler, runs: FakeRuns) -> TestClient:
    app = FastAPI()
    app.include_router(create_monitor_router(scheduler, runs, mutation_auth=_noop_auth))  # type: ignore[arg-type]

    @app.exception_handler(config_api.ConfigApiError)
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


def test_trigger_passes_rank_flag() -> None:
    scheduler = FakeScheduler()
    runs = FakeRuns([])

    with make_client(scheduler, runs) as client:
        response = client.post('/api/monitor/rss/trigger', json={'rank': True})

    assert response.status_code == 202
    assert response.json() == {'run_id': 'run-1'}
    assert scheduler.triggered == [(PipelineName.RSS, True)]


def test_trigger_busy_and_unconfigured_and_unknown() -> None:
    scheduler = FakeScheduler()
    runs = FakeRuns([])

    with make_client(scheduler, runs) as client:
        scheduler.busy = True
        busy = client.post('/api/monitor/rss/trigger', json={})
        scheduler.busy = False
        scheduler.unconfigured_reason = 'FreshRSS is not configured'
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
