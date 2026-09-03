"""Dashboard endpoints for the monitor pipelines."""

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from fastapi import APIRouter, Depends, Response
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
from embyx_manager.monitor.manual import (
    DirectoryListing,
    ManualEntry,
    ManualIntakeError,
    ManualIntakeSource,
)
from embyx_manager.monitor.reports import PipelineName
from embyx_manager.monitor.runs import PipelineRunRecord, PipelineRunRepository
from embyx_manager.monitor.scheduler import (
    MonitorScheduler,
    PipelineBusyError,
    PipelineNotConfiguredError,
)
from embyx_manager.monitor.subscriptions import (
    SubscriptionExistsError,
    SubscriptionKind,
    SubscriptionRecord,
    SubscriptionRepository,
    validate_feed_url,
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
    release_date: date | None
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
            release_date=record.release_date,
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


class OfflineDirectoryView(BaseModel):
    path: str
    name: str
    configured: bool
    routed: bool


class DirectoryListingView(BaseModel):
    path: str
    parent: str | None
    entries: list[OfflineDirectoryView]
    #: Where the picker opens: the last manual submission's directory.
    default_path: str | None

    @classmethod
    def from_listing(cls, listing: DirectoryListing, default_path: str | None) -> 'DirectoryListingView':
        return cls(
            path=listing.path,
            parent=listing.parent,
            entries=[
                OfflineDirectoryView(
                    path=entry.path,
                    name=entry.name,
                    configured=entry.configured,
                    routed=entry.routed,
                )
                for entry in listing.entries
            ],
            default_path=default_path,
        )


class ManualSubmitRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    #: One line per wanted video: an AVID, or any name one can be read from.
    inputs: list[str]
    #: The CloudDrive directory the offline tasks are queued under.
    task_dir_path: str


class ManualEntryView(BaseModel):
    text: str
    avid: str | None
    outcome: str
    archived_paths: tuple[str, ...]

    @classmethod
    def from_entry(cls, entry: ManualEntry) -> 'ManualEntryView':
        return cls(
            text=entry.text,
            avid=entry.avid,
            outcome=entry.outcome.value,
            archived_paths=entry.archived_paths,
        )


class ManualSubmitView(BaseModel):
    task_dir_path: str
    items: list[ManualEntryView]


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
    #: The manual input source; its routes are mounted only when it is supplied.
    manual: ManualIntakeSource | None = None


class SubscriptionView(BaseModel):
    id: int
    kind: str
    category: str
    enabled: bool
    url: str | None
    feed_url: str
    talent_id: int | None
    name: str | None
    aliases: tuple[str, ...]
    seed_pending: bool
    #: How many feed items the poller remembers having seen.
    cursor_size: int
    last_polled_at: datetime | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_record(cls, record: SubscriptionRecord) -> 'SubscriptionView':
        return cls(
            id=record.id,
            kind=record.kind.value,
            category=record.category,
            enabled=record.enabled,
            url=record.url,
            feed_url=record.feed_url,
            talent_id=record.talent_id,
            name=record.name,
            aliases=record.aliases,
            seed_pending=record.seed_pending,
            cursor_size=len(record.cursor),
            last_polled_at=record.last_polled_at,
            last_error=record.last_error,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )


class SubscriptionListView(BaseModel):
    items: list[SubscriptionView]
    #: The configured RSS categories a subscription may belong to.
    categories: list[str]


class CreateSubscriptionRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    kind: str = 'rss'
    category: str
    name: str | None = None
    #: rss: the feed URL.
    url: str | None = None
    #: avbase_talent: the talent id and every other name it is credited under.
    talent_id: int | None = None
    aliases: list[str] = []
    #: True makes the first poll record the feed's current items instead of
    #: ingesting them — for a subscription whose backlog was covered elsewhere.
    seed: bool = False


class UpdateSubscriptionRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    enabled: bool | None = None
    category: str | None = None
    #: A new feed URL; only an rss subscription has one to change.
    url: str | None = None


@dataclass(frozen=True)
class SubscriptionsApi:
    """The subscription registry behind the settings page's feed list.

    ``categories`` returns the configured RSS category labels, the only values
    a subscription may be filed under.
    """

    repository: SubscriptionRepository
    categories: Callable[[], Sequence[str]]


#: Which HTTP status each refused manual submission answers with.
_MANUAL_STATUS = {'directory_not_found': 404}


def create_monitor_router(  # noqa: C901, PLR0915 - route registration
    scheduler: MonitorScheduler,
    runs: PipelineRunRepository,
    *,
    mutation_auth: Any,
    acquisitions: AcquisitionApi | None = None,
    subscriptions: SubscriptionsApi | None = None,
) -> APIRouter:
    router = APIRouter(prefix='/api/monitor')
    if subscriptions is not None:
        _mount_subscription_routes(router, subscriptions, mutation_auth)

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

    manual = acquisitions.manual
    if manual is not None:

        @router.get('/manual/directories', dependencies=[Depends(mutation_auth)])
        async def browse_directories(path: str = '/') -> DirectoryListingView:
            """The CloudDrive directories under ``path``, for picking one to download into."""
            try:
                listing = await manual.browse(path)
                default_path = await manual.default_directory()
            except ManualIntakeError as exc:
                raise ApiError(_MANUAL_STATUS.get(exc.code, 422), exc.code) from exc
            return DirectoryListingView.from_listing(listing, default_path)

        @router.post('/manual', dependencies=[Depends(mutation_auth)])
        async def submit_manual(request: ManualSubmitRequest) -> ManualSubmitView:
            """Hand an operator's own list of AVIDs to the shared acquisition intake."""
            try:
                submission = await manual.submit(request.inputs, task_dir_path=request.task_dir_path)
            except ManualIntakeError as exc:
                raise ApiError(_MANUAL_STATUS.get(exc.code, 422), exc.code) from exc
            return ManualSubmitView(
                task_dir_path=submission.task_dir_path,
                items=[ManualEntryView.from_entry(entry) for entry in submission.entries],
            )

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


def _mount_subscription_routes(  # noqa: C901 - route registration
    router: APIRouter,
    api: SubscriptionsApi,
    mutation_auth: Any,
) -> None:
    repository = api.repository

    def labels() -> tuple[str, ...]:
        return tuple(api.categories())

    @router.get('/subscriptions')
    async def list_subscriptions() -> SubscriptionListView:
        records = await repository.list()
        return SubscriptionListView(
            items=[SubscriptionView.from_record(record) for record in records],
            categories=list(labels()),
        )

    @router.post('/subscriptions', dependencies=[Depends(mutation_auth)], status_code=201)
    async def create_subscription(request: CreateSubscriptionRequest) -> SubscriptionView:
        if request.category not in labels():
            raise ApiError(422, 'unknown_category')
        name = (request.name or '').strip() or None
        now = datetime.now(UTC)
        try:
            if request.kind == 'rss':
                try:
                    url = validate_feed_url(request.url or '')
                except ValueError as exc:
                    raise ApiError(422, 'invalid_feed_url') from exc
                record = await repository.add_rss(
                    url=url,
                    category=request.category,
                    name=name,
                    now=now,
                    seed_pending=request.seed,
                )
            elif request.kind == 'avbase_talent':
                if request.talent_id is None or request.talent_id <= 0 or name is None:
                    raise ApiError(422, 'invalid_talent')
                aliases = tuple(
                    dict.fromkeys(alias.strip() for alias in request.aliases if alias.strip() and alias.strip() != name)
                )
                record = await repository.add_talent(
                    talent_id=request.talent_id,
                    name=name,
                    aliases=aliases,
                    category=request.category,
                    now=now,
                    seed_pending=request.seed,
                )
            else:
                raise ApiError(422, 'unknown_subscription_kind')
        except SubscriptionExistsError as exc:
            raise ApiError(409, 'subscription_exists') from exc
        return SubscriptionView.from_record(record)

    @router.patch('/subscriptions/{subscription_id}', dependencies=[Depends(mutation_auth)])
    async def update_subscription(subscription_id: int, request: UpdateSubscriptionRequest) -> SubscriptionView:
        if request.category is not None and request.category not in labels():
            raise ApiError(422, 'unknown_category')
        url = None
        if request.url is not None:
            try:
                url = validate_feed_url(request.url)
            except ValueError as exc:
                raise ApiError(422, 'invalid_feed_url') from exc
            current = await repository.get(subscription_id)
            if current is None:
                raise ApiError(404, 'unknown_subscription')
            if current.kind is not SubscriptionKind.RSS:
                raise ApiError(422, 'url_not_editable')
        try:
            record = await repository.update(
                subscription_id,
                now=datetime.now(UTC),
                enabled=request.enabled,
                category=request.category,
                url=url,
            )
        except SubscriptionExistsError as exc:
            raise ApiError(409, 'subscription_exists') from exc
        if record is None:
            raise ApiError(404, 'unknown_subscription')
        return SubscriptionView.from_record(record)

    @router.delete('/subscriptions/{subscription_id}', dependencies=[Depends(mutation_auth)], status_code=204)
    async def delete_subscription(subscription_id: int) -> Response:
        if not await repository.delete(subscription_id):
            raise ApiError(404, 'unknown_subscription')
        return Response(status_code=204)
