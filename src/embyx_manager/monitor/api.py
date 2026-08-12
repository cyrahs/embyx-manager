"""Dashboard endpoints for the monitor pipelines."""

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict

from embyx_manager.config.api import ConfigApiError
from embyx_manager.monitor.reports import PipelineName
from embyx_manager.monitor.runs import PipelineRunRecord, PipelineRunRepository
from embyx_manager.monitor.scheduler import (
    MonitorScheduler,
    PipelineBusyError,
    PipelineNotConfiguredError,
)

MAX_RUNS_PAGE = 100


class RunSummaryView(BaseModel):
    run_id: str
    pipeline: str
    trigger: str
    state: str
    started_at: datetime
    finished_at: datetime | None
    stats: dict[str, int]
    error_count: int

    @classmethod
    def from_record(cls, record: PipelineRunRecord) -> 'RunSummaryView':
        return cls(
            run_id=record.run_id,
            pipeline=record.pipeline.value,
            trigger=record.trigger.value,
            state=record.state.value,
            started_at=record.started_at,
            finished_at=record.finished_at,
            stats=record.stats,
            error_count=len(record.errors),
        )


class RunDetailView(RunSummaryView):
    errors: tuple[str, ...]
    log_tail: tuple[str, ...]

    @classmethod
    def from_record(cls, record: PipelineRunRecord) -> 'RunDetailView':
        return cls(
            run_id=record.run_id,
            pipeline=record.pipeline.value,
            trigger=record.trigger.value,
            state=record.state.value,
            started_at=record.started_at,
            finished_at=record.finished_at,
            stats=record.stats,
            error_count=len(record.errors),
            errors=record.errors,
            log_tail=record.log_tail,
        )


class PipelineStatusView(BaseModel):
    pipeline: str
    enabled: bool
    configured: bool
    reason: str | None
    running_run_id: str | None
    next_scheduled_at: datetime | None
    last_run: RunSummaryView | None


class TriggerRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    rank: bool = False


class TriggerResponse(BaseModel):
    run_id: str


def _parse_pipeline(pipeline: str) -> PipelineName:
    try:
        return PipelineName(pipeline)
    except ValueError as exc:
        raise ConfigApiError(404, 'unknown_pipeline') from exc


def create_monitor_router(  # noqa: C901 - route registration
    scheduler: MonitorScheduler,
    runs: PipelineRunRepository,
    *,
    mutation_auth: Any,
) -> APIRouter:
    router = APIRouter(prefix='/api/monitor')

    @router.get('/status')
    async def status() -> list[PipelineStatusView]:
        views = []
        for pipeline_status in scheduler.status():
            latest = await runs.latest_run(pipeline_status.pipeline)
            views.append(
                PipelineStatusView(
                    pipeline=pipeline_status.pipeline.value,
                    enabled=pipeline_status.enabled,
                    configured=pipeline_status.configured,
                    reason=pipeline_status.reason,
                    running_run_id=pipeline_status.running_run_id,
                    next_scheduled_at=pipeline_status.next_scheduled_at,
                    last_run=RunSummaryView.from_record(latest) if latest is not None else None,
                ),
            )
        return views

    @router.post('/{pipeline}/trigger', dependencies=[Depends(mutation_auth)], status_code=202)
    async def trigger(pipeline: str, request: TriggerRequest | None = None) -> TriggerResponse:
        name = _parse_pipeline(pipeline)
        rank = bool(request.rank) if request is not None else False
        try:
            run_id = await scheduler.trigger(name, rank=rank)
        except PipelineBusyError as exc:
            raise ConfigApiError(409, 'pipeline_busy') from exc
        except PipelineNotConfiguredError as exc:
            raise ConfigApiError(422, 'pipeline_not_configured') from exc
        return TriggerResponse(run_id=run_id)

    @router.post('/{pipeline}/cancel', dependencies=[Depends(mutation_auth)])
    async def cancel(pipeline: str) -> dict[str, bool]:
        name = _parse_pipeline(pipeline)
        cancelled = await scheduler.cancel_running(name)
        if not cancelled:
            raise ConfigApiError(409, 'pipeline_not_running')
        return {'cancelling': True}

    @router.get('/runs')
    async def list_runs(
        pipeline: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[RunSummaryView]:
        name = _parse_pipeline(pipeline) if pipeline else None
        bounded_limit = max(1, min(limit, MAX_RUNS_PAGE))
        records = await runs.list_runs(name, limit=bounded_limit, offset=max(0, offset))
        return [RunSummaryView.from_record(record) for record in records]

    @router.get('/runs/{run_id}')
    async def get_run(run_id: str) -> RunDetailView:
        record = await runs.get_run(run_id)
        if record is None:
            raise ConfigApiError(404, 'unknown_run')
        return RunDetailView.from_record(record)

    return router
