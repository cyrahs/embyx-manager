"""Dashboard endpoints for the monitor pipelines."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict

from embyx_manager.core.magnet import extract_info_hash
from embyx_manager.errors import ApiError
from embyx_manager.monitor.acquisitions import (
    AcquisitionRecord,
    AcquisitionRepository,
    AcquisitionState,
    AttemptState,
    MagnetAttemptRecord,
    MagnetCandidate,
)
from embyx_manager.monitor.reports import PipelineName
from embyx_manager.monitor.runs import PipelineRunRecord, PipelineRunRepository
from embyx_manager.monitor.scheduler import (
    MonitorScheduler,
    PipelineBusyError,
    PipelineNotConfiguredError,
)

MAX_RUNS_PAGE = 100
MAX_ACQUISITIONS_PAGE = 200


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


class AttemptView(BaseModel):
    attempt_no: int
    magnet_source: str
    state: str
    progress: float | None
    error: str | None
    submitted_at: datetime | None
    updated_at: datetime
    info_hash: str | None

    @classmethod
    def from_record(cls, record: MagnetAttemptRecord) -> 'AttemptView':
        return cls(
            attempt_no=record.attempt_no,
            magnet_source=record.magnet_source,
            state=record.state.value,
            progress=record.progress,
            error=record.error,
            submitted_at=record.submitted_at,
            updated_at=record.updated_at,
            info_hash=record.info_hash,
        )


class AcquisitionView(BaseModel):
    avid: str
    state: str
    source: str
    note: str | None
    archived_paths: tuple[str, ...]
    next_action_at: datetime | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_record(cls, record: AcquisitionRecord) -> 'AcquisitionView':
        return cls(
            avid=record.avid,
            state=record.state.value,
            source=record.source,
            note=record.note,
            archived_paths=record.archived_paths,
            next_action_at=record.next_action_at,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )


class AcquisitionListView(BaseModel):
    """A page of acquisitions plus the counts the panel groups by."""

    items: list[AcquisitionView]
    counts: dict[str, int]


class AcquisitionDetailView(AcquisitionView):
    attempts: list[AttemptView]


class TrackerStatusView(BaseModel):
    running: bool
    reason: str | None
    last_polled_at: datetime | None
    last_error: str | None
    last_stats: dict[str, int]


class MagnetRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    magnet: str


class TriggerResponse(BaseModel):
    run_id: str


def _parse_pipeline(pipeline: str) -> PipelineName:
    try:
        return PipelineName(pipeline)
    except ValueError as exc:
        raise ApiError(404, 'unknown_pipeline') from exc


@dataclass(frozen=True)
class AcquisitionApi:
    """Everything the dashboard needs to show and act on the ledger.

    Optional as a unit: without CloudDrive there is nothing to submit magnets
    to, so the ledger routes are only mounted when it is supplied.
    """

    ledger: AcquisitionRepository
    submit_magnet: Callable[[str, str], Awaitable[bool]] | None = None
    tracker_ready: Callable[[], str | None] | None = None


def create_monitor_router(  # noqa: C901, PLR0915 - route registration
    scheduler: MonitorScheduler,
    runs: PipelineRunRepository,
    *,
    mutation_auth: Any,
    acquisitions: AcquisitionApi | None = None,
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
    async def trigger(pipeline: str) -> TriggerResponse:
        name = _parse_pipeline(pipeline)
        try:
            run_id = await scheduler.trigger(name)
        except PipelineBusyError as exc:
            raise ApiError(409, 'pipeline_busy') from exc
        except PipelineNotConfiguredError as exc:
            raise ApiError(422, 'pipeline_not_configured') from exc
        return TriggerResponse(run_id=run_id)

    @router.post('/{pipeline}/cancel', dependencies=[Depends(mutation_auth)])
    async def cancel(pipeline: str) -> dict[str, bool]:
        name = _parse_pipeline(pipeline)
        cancelled = await scheduler.cancel_running(name)
        if not cancelled:
            raise ApiError(409, 'pipeline_not_running')
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
            raise ApiError(404, 'unknown_run')
        return RunDetailView.from_record(record)

    if acquisitions is None:
        return router
    ledger = acquisitions.ledger
    submit_magnet = acquisitions.submit_magnet
    tracker_ready = acquisitions.tracker_ready

    async def _load(avid: str) -> AcquisitionRecord:
        record = await ledger.get(avid)
        if record is None:
            raise ApiError(404, 'unknown_acquisition')
        return record

    async def _move(record: AcquisitionRecord, target: AcquisitionState, **fields: Any) -> AcquisitionView:
        """Apply one CAS transition, turning a lost race into a 409."""
        moved = await ledger.transition(
            record.avid,
            expected=record.state,
            target=target,
            now=datetime.now(UTC),
            **fields,
        )
        if not moved:
            raise ApiError(409, 'acquisition_changed')
        return AcquisitionView.from_record(await _load(record.avid))

    async def _submit(avid: str, attempt_no: int | None = None) -> None:
        """Hand a magnet to CloudDrive: a chosen one, or the next untried one."""
        if submit_magnet is None:
            raise ApiError(422, 'clouddrive_not_configured')
        now = datetime.now(UTC)
        attempt = (
            await ledger.claim_attempt(avid, attempt_no, now=now)
            if attempt_no is not None
            else await ledger.claim_next_pending(avid, now=now)
        )
        if attempt is None:
            raise ApiError(409, 'no_magnet_left')
        if not await submit_magnet(avid, attempt.magnet):
            await ledger.transition_attempt(
                avid,
                attempt.attempt_no,
                expected=AttemptState.SUBMITTED,
                target=AttemptState.ERROR,
                now=datetime.now(UTC),
                error='failed to add the offline task',
            )
            raise ApiError(502, 'offline_task_rejected')

    @router.get('/acquisitions')
    async def list_acquisitions(state: str | None = None, limit: int = 50, offset: int = 0) -> AcquisitionListView:
        states = None
        if state:
            try:
                states = [AcquisitionState(value) for value in state.split(',') if value]
            except ValueError as exc:
                raise ApiError(400, 'unknown_state') from exc
        records = await ledger.list_acquisitions(
            states=states,
            limit=max(1, min(limit, MAX_ACQUISITIONS_PAGE)),
            offset=max(0, offset),
        )
        counts = await ledger.count_by_state()
        return AcquisitionListView(
            items=[AcquisitionView.from_record(record) for record in records],
            counts={key.value: value for key, value in counts.items()},
        )

    @router.get('/acquisitions/{avid}')
    async def get_acquisition(avid: str) -> AcquisitionDetailView:
        record = await _load(avid)
        attempts = await ledger.attempts_for(avid)
        return AcquisitionDetailView(
            **AcquisitionView.from_record(record).model_dump(),
            attempts=[AttemptView.from_record(attempt) for attempt in attempts],
        )

    @router.post('/acquisitions/{avid}/retry', dependencies=[Depends(mutation_auth)])
    async def retry_acquisition(avid: str) -> AcquisitionView:
        """Try this AVID's next magnet now, whatever it is waiting on."""
        record = await _load(avid)
        if record.state is AcquisitionState.ARCHIVED:
            raise ApiError(409, 'already_archived')
        await _submit(avid)
        if record.state is AcquisitionState.DOWNLOADING:
            return AcquisitionView.from_record(await _load(avid))
        return await _move(record, AcquisitionState.DOWNLOADING)

    @router.post('/acquisitions/{avid}/magnet', dependencies=[Depends(mutation_auth)])
    async def add_magnet(avid: str, request: MagnetRequest) -> AcquisitionView:
        """Add an operator-supplied magnet and submit it straight away."""
        record = await _load(avid)
        if record.state is AcquisitionState.ARCHIVED:
            raise ApiError(409, 'already_archived')
        info_hash = extract_info_hash(request.magnet)
        if not request.magnet.lower().startswith('magnet:') or info_hash is None:
            raise ApiError(400, 'unusable_magnet')
        added = await ledger.add_attempts(
            avid,
            [MagnetCandidate(magnet=request.magnet, info_hash=info_hash, source='manual')],
            now=datetime.now(UTC),
        )
        if not added:
            raise ApiError(409, 'magnet_already_tried')
        # Submit the magnet the operator chose, not whichever candidate happens to
        # be next in line.
        attempts = await ledger.attempts_for(avid)
        await _submit(avid, attempts[-1].attempt_no)
        if record.state is AcquisitionState.DOWNLOADING:
            return AcquisitionView.from_record(await _load(avid))
        return await _move(record, AcquisitionState.DOWNLOADING)

    @router.post('/acquisitions/{avid}/ignore', dependencies=[Depends(mutation_auth)])
    async def ignore_acquisition(avid: str) -> AcquisitionView:
        record = await _load(avid)
        if record.state is AcquisitionState.ARCHIVED:
            raise ApiError(409, 'already_archived')
        return await _move(record, AcquisitionState.IGNORED, note='ignored by an operator')

    @router.post('/acquisitions/{avid}/resume', dependencies=[Depends(mutation_auth)])
    async def resume_acquisition(avid: str) -> AcquisitionView:
        """Hand a parked AVID back to the tracker once its folder has been sorted out."""
        record = await _load(avid)
        if record.state is not AcquisitionState.NEEDS_ATTENTION:
            raise ApiError(409, 'not_parked')
        return await _move(record, AcquisitionState.DOWNLOADING)

    @router.get('/tracker')
    async def tracker_status() -> TrackerStatusView:
        reason = tracker_ready() if tracker_ready is not None else 'the tracker is not wired up'
        state = scheduler.tracker_state()
        return TrackerStatusView(
            running=reason is None,
            reason=reason,
            last_polled_at=state.last_polled_at,
            last_error=state.last_error,
            last_stats=state.last_stats,
        )

    return router
