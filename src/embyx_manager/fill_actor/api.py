"""Fill Actor's HTTP surface, owned by the feature rather than by the app root."""

import asyncio
import logging
import math
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.requests import Request

from embyx_manager.errors import ApiError
from embyx_manager.fill_actor.errors import (
    ApplyJobNotCancellableError,
    ApplyRequestConflictError,
    ExpiredPlanError,
    FillActorError,
    InvalidActorIdError,
    JobQueueFullError,
    LegacyPlanError,
    MoveDisabledError,
    RevisionMismatchError,
    TooManyActorsError,
    TooManyVideosError,
    UnknownApplyJobError,
    UnknownCandidateError,
    UnknownPlanError,
)
from embyx_manager.fill_actor.jobs import FillActorJobManager
from embyx_manager.fill_actor.models import ApplyResult, FillActorPlan
from embyx_manager.fill_actor.persistence import (
    CancelJobOutcome,
    FillActorRepository,
    JobOperation,
    JobProgress,
    JobRecord,
    JobStage,
    JobState,
)
from embyx_manager.fill_actor.service import FillActorService

LOGGER = logging.getLogger(__name__)

MAINTENANCE_INTERVAL_SECONDS = 5


class CreatePlanRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    actor_ids: list[str] = Field(min_length=1)


class ResolveAvidRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    avid: str = Field(min_length=1, max_length=128)


class AvidActorView(BaseModel):
    actor_id: str
    name: str


class AvidActorsView(BaseModel):
    avid: str
    actors: tuple[AvidActorView, ...]


class ApplyPlanRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    revision: str = Field(min_length=1, max_length=256)
    candidate_ids: list[str] = Field(default_factory=list, max_length=5_000)


class CreateApplyJobRequest(ApplyPlanRequest):
    request_id: str = Field(min_length=16, max_length=128, pattern=r'^[A-Za-z0-9_-]+$')


class JobProgressView(BaseModel):
    stage: str
    completed: int
    total: int | None
    unit: str
    current: str | None
    stage_started_at: datetime
    updated_at: datetime
    percent: float | None
    eta_seconds: int | None
    elapsed_seconds: int
    last_progress_seconds: int

    @classmethod
    def from_record(cls, progress: JobProgress, *, state: JobState, now: datetime) -> 'JobProgressView':
        elapsed_seconds = max(0, math.floor((now - progress.stage_started_at).total_seconds()))
        last_progress_seconds = max(0, math.floor((now - progress.updated_at).total_seconds()))
        if progress.total is None:
            percent = None
        elif progress.total == 0:
            percent = 100.0
        else:
            percent = round(min(progress.completed / progress.total * 100, 100.0), 2)

        if progress.stage is JobStage.DONE or state not in {JobState.QUEUED, JobState.RUNNING}:
            eta_seconds = 0
        elif progress.total is None or progress.completed == 0:
            eta_seconds = None
        elif progress.completed >= progress.total:
            eta_seconds = 0
        elif elapsed_seconds == 0:
            eta_seconds = None
        else:
            eta_seconds = math.ceil(elapsed_seconds / progress.completed * (progress.total - progress.completed))
        return cls(
            stage=progress.stage.value,
            completed=progress.completed,
            total=progress.total,
            unit=progress.unit.value,
            current=progress.current,
            stage_started_at=progress.stage_started_at,
            updated_at=progress.updated_at,
            percent=percent,
            eta_seconds=eta_seconds,
            elapsed_seconds=elapsed_seconds,
            last_progress_seconds=last_progress_seconds,
        )


class JobView(BaseModel):
    job_id: str
    plan_id: str | None
    operation: str
    state: str
    created_at: datetime
    updated_at: datetime
    error_code: str | None
    progress: JobProgressView

    @classmethod
    def from_record(cls, record: JobRecord) -> 'JobView':
        if record.progress is None:  # pragma: no cover - JobRecord normalizes this invariant
            msg = 'job progress is required'
            raise ValueError(msg)
        now = datetime.now(UTC)
        return cls(
            job_id=record.job_id,
            plan_id=record.plan_id,
            operation=record.operation.value,
            state=record.state.value,
            created_at=record.created_at,
            updated_at=record.updated_at,
            error_code=record.error_code,
            progress=JobProgressView.from_record(record.progress, state=record.state, now=now),
        )


class PlanEnvelope(BaseModel):
    job: JobView
    plan: FillActorPlan | None


class ApplyJobEnvelope(BaseModel):
    job: JobView
    result: ApplyResult | None


def create_fill_actor_router(  # noqa: C901, PLR0915 - one function per endpoint over shared wiring
    *,
    service: FillActorService,
    repository: FillActorRepository,
    jobs: FillActorJobManager,
    mutation_auth: Callable[..., Awaitable[None]],
    avid_actor_lookup: Callable[[str], Awaitable[Sequence[tuple[str, str]]]] | None = None,
) -> APIRouter:
    """Every Fill Actor endpoint, mounted by the app root like any other feature."""
    router = APIRouter(prefix='/api/fill-actor')

    async def require_scan_ready() -> None:
        if not await repository.health_check() or not await service.scan_ready():
            raise ApiError(503, 'not_ready')

    async def require_apply_ready() -> None:
        if not service.apply_enabled:
            raise MoveDisabledError
        if not await repository.health_check() or not await service.apply_ready():
            raise ApiError(503, 'not_ready')

    async def require_repository_ready() -> None:
        if not await repository.health_check():
            raise ApiError(503, 'not_ready')

    @router.post(
        '/avid-actors',
        dependencies=[Depends(mutation_auth)],
    )
    async def resolve_avid_actors(request: ResolveAvidRequest) -> AvidActorsView:
        avid = request.avid.strip().upper()
        if avid_actor_lookup is None:
            raise ApiError(503, 'avid_actor_lookup_unavailable')
        try:
            actors = await avid_actor_lookup(avid)
        except Exception as exc:
            LOGGER.exception('AVID actor lookup failed avid=%s', avid)
            raise ApiError(502, 'avid_actor_lookup_failed') from exc
        if not actors:
            raise ApiError(404, 'avid_actors_not_found')
        return AvidActorsView(
            avid=avid,
            actors=tuple(AvidActorView(actor_id=actor_id, name=name) for actor_id, name in actors),
        )

    @router.post(
        '/plans',
        status_code=202,
        response_model=None,
        dependencies=[Depends(mutation_auth)],
    )
    async def create_plan(request: CreatePlanRequest) -> PlanEnvelope | JSONResponse:
        actor_ids = service.validate_actor_ids(request.actor_ids)
        await require_scan_ready()
        job = await jobs.start_plan(actor_ids)
        return PlanEnvelope(job=JobView.from_record(job), plan=None)

    @router.get('/plans/{plan_id}')
    async def get_plan(plan_id: str) -> PlanEnvelope:
        job = await jobs.get_job(plan_id)
        if job is None:
            raise UnknownPlanError(plan_id)
        plan = await jobs.get_plan(plan_id)
        if plan is None and job.state in {JobState.COMPLETED, JobState.PARTIAL_FAILED}:
            raise UnknownPlanError(plan_id)
        if plan is None and job.plan_id is None and job.error_code is None:
            raise UnknownPlanError(plan_id)
        return PlanEnvelope(job=JobView.from_record(job), plan=plan)

    @router.post(
        '/plans/{plan_id}/cancel',
        dependencies=[Depends(mutation_auth), Depends(require_repository_ready)],
    )
    async def cancel_plan(plan_id: str) -> PlanEnvelope:
        result = await jobs.cancel_plan(plan_id)
        if result.outcome is CancelJobOutcome.NOT_FOUND or result.job is None:
            raise UnknownPlanError(plan_id)
        if result.outcome is CancelJobOutcome.ALREADY_TERMINAL:
            raise ApiError(409, 'plan_not_cancellable')
        return PlanEnvelope(job=JobView.from_record(result.job), plan=None)

    @router.post(
        '/plans/{plan_id}/apply-jobs',
        status_code=202,
        dependencies=[Depends(mutation_auth)],
    )
    async def create_apply_job(plan_id: str, request: CreateApplyJobRequest) -> ApplyJobEnvelope:
        normalized_candidate_ids = tuple(dict.fromkeys(request.candidate_ids))
        existing = await jobs.get_apply_job(request.request_id)
        if existing is not None:
            existing_plan_id = (
                existing.job.plan_id
                if existing.job.plan_id is not None
                else existing.result.plan_id
                if existing.result is not None
                else None
            )
            if (
                existing_plan_id == plan_id
                and existing.revision == request.revision
                and existing.candidate_ids == normalized_candidate_ids
            ):
                return ApplyJobEnvelope(job=JobView.from_record(existing.job), result=existing.result)
            raise ApplyRequestConflictError(request.request_id)

        await require_apply_ready()
        plan_job = await jobs.get_job(plan_id)
        if plan_job is None or plan_job.operation is not JobOperation.CREATE_PLAN:
            raise UnknownPlanError(plan_id)
        if plan_job.state not in {JobState.COMPLETED, JobState.PARTIAL_FAILED}:
            raise ApiError(409, 'plan_not_ready')
        if await jobs.get_plan(plan_id) is None:
            raise UnknownPlanError(plan_id)
        record = await jobs.start_apply(
            plan_id=plan_id,
            revision=request.revision,
            candidate_ids=request.candidate_ids,
            request_id=request.request_id,
        )
        return ApplyJobEnvelope(job=JobView.from_record(record.job), result=record.result)

    @router.get('/apply-jobs/{job_id}', dependencies=[Depends(mutation_auth)])
    async def get_apply_job(job_id: str) -> ApplyJobEnvelope:
        record = await jobs.get_apply_job(job_id)
        if record is None:
            raise UnknownApplyJobError(job_id)
        return ApplyJobEnvelope(job=JobView.from_record(record.job), result=record.result)

    @router.post(
        '/plans/{plan_id}/apply',
        dependencies=[Depends(mutation_auth), Depends(require_apply_ready)],
    )
    async def apply_plan(plan_id: str, request: ApplyPlanRequest) -> ApplyResult:
        job = await jobs.get_job(plan_id)
        if job is None:
            raise UnknownPlanError(plan_id)
        if job.state not in {JobState.COMPLETED, JobState.PARTIAL_FAILED}:
            raise ApiError(409, 'plan_not_ready')
        return await service.apply(
            plan_id=plan_id,
            revision=request.revision,
            candidate_ids=request.candidate_ids,
        )

    return router


def fill_actor_health(
    *,
    service: FillActorService,
    repository: FillActorRepository,
) -> Callable[[], Awaitable[dict[str, object]]]:
    """Fill Actor's readiness, reported beside — never as — the app's own health."""

    async def probe() -> dict[str, object]:
        database_ready = await repository.health_check()
        configured = service.configured
        roots_ready = await service.roots_ready()
        cloud_ready = await service.cloud_ready()
        legacy_journal_ready = await service.legacy_journal_ready()
        scan_ready = database_ready and roots_ready and cloud_ready
        return {
            # `configured` separates "nobody filled in the Settings card yet" from
            # "the configured roots are not mounted", which need different fixes.
            'configured': configured,
            'roots': roots_ready,
            'cloud': cloud_ready,
            'legacy_journal': legacy_journal_ready,
            'apply_enabled': service.apply_enabled,
            'scan_ready': scan_ready,
            'apply_ready': service.apply_enabled and scan_ready and legacy_journal_ready,
        }

    return probe


def fill_actor_lifespan(
    *,
    service: FillActorService,
    repository: FillActorRepository,
    jobs: FillActorJobManager,
) -> Callable[[], AsyncIterator[None]]:
    """Starts and stops the feature's job queue and move reconciliation."""

    @asynccontextmanager
    async def lifespan() -> AsyncIterator[None]:
        if not await repository.health_check():
            msg = 'fill-actor repository is unavailable'
            raise RuntimeError(msg)
        if await service.apply_ready():
            await service.reconcile_moves()
        await jobs.start()

        async def maintain() -> None:
            while True:
                try:
                    if await service.apply_ready():
                        await service.reconcile_moves()
                    await repository.purge_expired_plans(datetime.now(UTC))
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.exception('fill-actor maintenance iteration failed')
                await asyncio.sleep(MAINTENANCE_INTERVAL_SECONDS)

        maintenance = asyncio.create_task(maintain(), name='fill-actor-maintenance')
        try:
            yield
        finally:
            maintenance.cancel()
            with suppress(asyncio.CancelledError):
                await maintenance
            await jobs.aclose()
            await service.aclose()

    return lifespan


async def handle_fill_actor_error(_request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, FillActorError):  # pragma: no cover - registered for FillActorError only
        raise exc
    return JSONResponse({'error': {'code': exc.code}}, status_code=_service_error_status(exc))


def _service_error_status(exc: FillActorError) -> int:
    mappings: Sequence[tuple[type[FillActorError], int]] = (
        (InvalidActorIdError, 422),
        (TooManyActorsError, 422),
        (TooManyVideosError, 422),
        (ApplyRequestConflictError, 409),
        (ApplyJobNotCancellableError, 409),
        (MoveDisabledError, 503),
        (LegacyPlanError, 409),
        (UnknownPlanError, 404),
        (UnknownApplyJobError, 404),
        (ExpiredPlanError, 410),
        (RevisionMismatchError, 409),
        (UnknownCandidateError, 422),
        (JobQueueFullError, 429),
    )
    return next((status for error_type, status in mappings if isinstance(exc, error_type)), 500)
