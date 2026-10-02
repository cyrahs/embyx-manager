"""Merge endpoints: the multi-part titles the library holds, and the tasks merging them."""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field

from embyx_manager.config.models import ArchiveConfig, MappingConfig
from embyx_manager.errors import ApiError
from embyx_manager.merge.detect import (
    MultipartTitle,
    SourceBasis,
    TitleProblem,
    resolve_source,
    route_sources,
    scan_multipart,
)
from embyx_manager.merge.service import MergeActionError, MergeService
from embyx_manager.merge.tasks import (
    CANCELLABLE_STATES,
    MergeState,
    MergeTask,
    MergeTaskConflictError,
    MergeTaskRepository,
)
from embyx_manager.merge.worker import MUXERS

#: Which HTTP status each refused cancel or retry answers with; the rest are 409.
_ACTION_STATUS = {'merge_task_not_found': 404}


@dataclass(frozen=True)
class MergeCatalog:
    """What the titles endpoint reads: the two config sections and the ledger's offline directories."""

    archive: Callable[[], ArchiveConfig]
    mapping: Callable[[], MappingConfig]
    task_dirs_for: Callable[[Sequence[str]], Awaitable[dict[str, str]]]


class TitleView(BaseModel):
    avid: str
    directory: str
    part_count: int
    parts: list[int]
    missing: list[int]
    library_dir: str | None
    brand: str | None
    problem: TitleProblem | None
    #: The intake route the merged file re-enters through; None until someone picks one.
    source: str | None
    source_basis: SourceBasis | None
    #: Emby already stacks it (cd1-cd9, none missing).
    stackable: bool
    mergeable: bool


class TitlesView(BaseModel):
    items: list[TitleView]
    #: The intake routes a title can be sent back through.
    routes: list[str]
    scanned_at: datetime | None
    #: Why nothing could be scanned, when the needed directories are not configured.
    reason: str | None = None


@dataclass(frozen=True)
class MergeTasksApi:
    """What the task endpoints need; they are mounted only when this is supplied."""

    repository: MergeTaskRepository
    service: MergeService


class TaskView(BaseModel):
    id: int
    avid: str
    source: str
    library_dir: str
    part_count: int
    state: MergeState
    failed_state: MergeState | None
    phase: str | None
    progress: float | None
    merged_bytes: int | None
    uploaded_bytes: int | None
    upload_attempts: int
    error: str | None
    notice: str | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None
    cancellable: bool
    retryable: bool

    @classmethod
    def from_task(cls, task: MergeTask) -> 'TaskView':
        cancellable = task.state in CANCELLABLE_STATES and not (
            task.state == MergeState.FAILED and task.failed_state in {MergeState.REPLACING, MergeState.ARCHIVING}
        )
        return cls(
            id=task.id,
            avid=task.avid,
            source=task.source,
            library_dir=task.library_dir,
            part_count=len(task.parts),
            state=task.state,
            failed_state=task.failed_state,
            phase=task.phase,
            progress=task.progress,
            merged_bytes=task.merged_bytes,
            uploaded_bytes=task.uploaded_bytes,
            upload_attempts=task.upload_attempts,
            error=task.error,
            notice=task.notice,
            created_at=task.created_at,
            updated_at=task.updated_at,
            finished_at=task.finished_at,
            cancellable=cancellable,
            retryable=task.state == MergeState.FAILED,
        )


class TasksView(BaseModel):
    items: list[TaskView]
    #: Why merging cannot run in this deployment, when it cannot.
    unavailable: str | None


class MergeRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    avid: str = Field(min_length=1, max_length=64)
    #: The intake route to re-enter through; required when the title's own cannot be told.
    source: str | None = Field(default=None, max_length=64)


def create_merge_router(
    catalog: MergeCatalog,
    *,
    tasks: MergeTasksApi | None = None,
    mutation_auth: Any = None,
) -> APIRouter:
    router = APIRouter(prefix='/api/merge')

    @router.get('/titles')
    async def list_titles() -> TitlesView:
        archive, titles = await _scan(catalog)
        if titles is None:
            return TitlesView(
                items=[],
                routes=list(route_sources(archive)),
                scanned_at=None,
                reason='mapping.dst_dir and archive.dst_dir must be configured',
            )
        task_dirs = await catalog.task_dirs_for([title.avid for title in titles])
        return TitlesView(
            items=[_view(archive, title, task_dirs.get(title.avid)) for title in titles],
            routes=list(route_sources(archive)),
            scanned_at=datetime.now(UTC),
        )

    if tasks is not None:
        _add_task_routes(router, catalog, tasks, [Depends(mutation_auth)] if mutation_auth is not None else [])
    return router


async def _scan(catalog: MergeCatalog) -> tuple[ArchiveConfig, list[MultipartTitle] | None]:
    """The archive config and every multi-part title; None when the directories are not configured."""
    archive = catalog.archive()
    mapping = catalog.mapping()
    if not mapping.dst_dir or not archive.dst_dir:
        return archive, None
    titles = await asyncio.to_thread(scan_multipart, Path(mapping.dst_dir), library_root=archive.dst_dir)
    return archive, titles


def _add_task_routes(router: APIRouter, catalog: MergeCatalog, tasks: MergeTasksApi, dependencies: list[Any]) -> None:
    @router.get('/tasks')
    async def list_tasks() -> TasksView:
        return TasksView(
            items=[TaskView.from_task(task) for task in await tasks.repository.listing()],
            unavailable=tasks.service.unavailable(),
        )

    @router.post('/tasks', status_code=201, dependencies=dependencies)
    async def create_task(request: MergeRequest) -> TaskView:
        if tasks.service.unavailable() is not None:
            raise ApiError(409, 'merge_unavailable')
        archive, titles = await _scan(catalog)
        task = await _enqueue(catalog, tasks.repository, archive, titles or [], request)
        tasks.service.wake()
        return TaskView.from_task(task)

    @router.post('/tasks/{task_id}/cancel', dependencies=dependencies)
    async def cancel_task(task_id: int) -> TaskView:
        try:
            return TaskView.from_task(await tasks.service.cancel(task_id))
        except MergeActionError as exc:
            raise ApiError(_ACTION_STATUS.get(exc.code, 409), exc.code) from exc

    @router.post('/tasks/{task_id}/retry', dependencies=dependencies)
    async def retry_task(task_id: int) -> TaskView:
        try:
            return TaskView.from_task(await tasks.service.retry(task_id))
        except MergeActionError as exc:
            raise ApiError(_ACTION_STATUS.get(exc.code, 409), exc.code) from exc


def _view(archive: ArchiveConfig, title: MultipartTitle, ledger_task_dir: str | None) -> TitleView:
    route = resolve_source(archive, title, ledger_task_dir)
    return TitleView(
        avid=title.avid,
        directory=title.directory,
        part_count=title.part_count,
        parts=[part.index for part in title.parts],
        missing=list(title.missing),
        library_dir=title.library_dir,
        brand=title.brand,
        problem=title.problem,
        source=route.source,
        source_basis=route.basis,
        stackable=title.stackable,
        mergeable=title.complete,
    )


async def _enqueue(
    catalog: MergeCatalog,
    repository: MergeTaskRepository,
    archive: ArchiveConfig,
    titles: list[MultipartTitle],
    request: MergeRequest,
) -> MergeTask:
    """Queue one scanned title, after checking it can be merged and re-enter somewhere."""
    wanted = request.avid.strip().upper()
    title = next((title for title in titles if title.avid.upper() == wanted), None)
    if title is None:
        raise ApiError(404, 'merge_title_not_found')
    if not title.complete or title.library_dir is None or title.brand is None:
        raise ApiError(422, 'merge_title_incomplete')
    suffixes = {PurePosixPath(part.target).suffix.lower() for part in title.parts}
    suffix = next(iter(suffixes))
    if len(suffixes) != 1 or suffix not in MUXERS:
        raise ApiError(422, 'merge_container_unsupported')
    source = request.source
    if source is None:
        ledger = await catalog.task_dirs_for([title.avid])
        source = resolve_source(archive, title, ledger.get(title.avid)).source
    if source is None:
        raise ApiError(422, 'merge_source_required')
    if source not in route_sources(archive):
        raise ApiError(422, 'merge_source_unknown')
    try:
        return await repository.create(
            avid=title.avid,
            source=source,
            library_dir=title.library_dir,
            brand=title.brand,
            parts=tuple(part.target for part in title.parts),
            merged_name=f'{title.avid}{suffix}',
        )
    except MergeTaskConflictError as exc:
        raise ApiError(409, 'merge_task_exists') from exc
