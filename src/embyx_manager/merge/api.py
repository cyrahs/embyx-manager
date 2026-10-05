"""Merge endpoints: the multi-part titles the library holds, and the tasks merging them."""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
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

LOGGER = logging.getLogger(__name__)

#: Which HTTP status each refused cancel or retry answers with; the rest are 409.
_ACTION_STATUS = {'merge_task_not_found': 404}


@dataclass(frozen=True)
class MergeCatalog:
    """What the titles endpoint reads: the two config sections and the ledger's offline directories."""

    archive: Callable[[], ArchiveConfig]
    mapping: Callable[[], MappingConfig]
    task_dirs_for: Callable[[Sequence[str]], Awaitable[dict[str, str]]]


class TitleScanCache:
    """The last full scan of the mapping tree.

    Walking every strm over the network share takes the better part of a
    minute, so the page reads this copy until someone asks for a fresh one,
    and queueing a task re-reads only that title's directory.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._key: tuple[str, str] | None = None
        self._titles: list[MultipartTitle] = []
        self._scanned_at: datetime | None = None

    async def titles(self, root: Path, library_root: str, *, refresh: bool) -> tuple[list[MultipartTitle], datetime]:
        requested_at = datetime.now(UTC)
        key = (str(root), library_root)
        async with self._lock:
            # A refresh that waited on another one already has a scan newer than itself.
            scanned_at = self._scanned_at
            if scanned_at is None or self._key != key or (refresh and scanned_at < requested_at):
                self._titles = await asyncio.to_thread(scan_multipart, root, library_root=library_root)
                self._key = key
                scanned_at = self._scanned_at = datetime.now(UTC)
            return self._titles, scanned_at


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


class AutoState(StrEnum):
    OFF = 'off'
    #: Merging cannot run in this deployment, or the library is not configured.
    UNAVAILABLE = 'unavailable'
    #: A task is under way; the next title waits for it to be filed.
    BUSY = 'busy'
    #: A task failed; nothing more is queued until it is retried or removed.
    PAUSED = 'paused'
    #: Nothing left that fits; the next round looks again.
    IDLE = 'idle'


class SkippedTitle(BaseModel):
    avid: str
    #: The parts' total size, when it could be read.
    size: int | None
    #: ``too_big``, ``parts_missing``, ``size_unknown`` or the enqueue refusal's error code.
    reason: str


class AutoStatus(BaseModel):
    """Where automatic merging stands (``merge.auto``)."""

    state: AutoState
    #: What a merge may take on the work volume right now: free space minus the reserve.
    room: int | None = None
    #: The first titles skipped in the last round; ``skipped_count`` counts them all.
    skipped: list[SkippedTitle] = Field(default_factory=list)
    skipped_count: int = 0
    checked_at: datetime | None = None


@dataclass(frozen=True)
class MergeTasksApi:
    """What the task endpoints need; they are mounted only when this is supplied."""

    repository: MergeTaskRepository
    service: MergeService
    #: Where automatic merging stands; None where it is not wired in.
    auto_status: Callable[[], AutoStatus] | None = None


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
    auto: AutoStatus | None = None


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
    cache: TitleScanCache | None = None,
) -> APIRouter:
    router = APIRouter(prefix='/api/merge')
    cache = cache or TitleScanCache()

    @router.get('/titles')
    async def list_titles(refresh: bool = False) -> TitlesView:  # noqa: FBT001, FBT002 - a query flag
        archive, scanned = await _scan(catalog, cache, refresh=refresh)
        if scanned is None:
            return TitlesView(
                items=[],
                routes=list(route_sources(archive)),
                scanned_at=None,
                reason='mapping.dst_dir and archive.dst_dir must be configured',
            )
        titles, scanned_at = scanned
        task_dirs = await catalog.task_dirs_for([title.avid for title in titles])
        return TitlesView(
            items=[_view(archive, title, task_dirs.get(title.avid)) for title in titles],
            routes=list(route_sources(archive)),
            scanned_at=scanned_at,
        )

    if tasks is not None:
        dependencies = [Depends(mutation_auth)] if mutation_auth is not None else []
        _add_task_routes(router, catalog, cache, tasks, dependencies)
    return router


async def _scan(
    catalog: MergeCatalog, cache: TitleScanCache, *, refresh: bool
) -> tuple[ArchiveConfig, tuple[list[MultipartTitle], datetime] | None]:
    """The archive config and every multi-part title; None when the directories are not configured."""
    archive = catalog.archive()
    mapping = catalog.mapping()
    if not mapping.dst_dir or not archive.dst_dir:
        return archive, None
    return archive, await cache.titles(Path(mapping.dst_dir), archive.dst_dir, refresh=refresh)


async def warm_title_cache(catalog: MergeCatalog, cache: TitleScanCache) -> None:
    """Scan once at startup so the first visit after a deploy does not wait on the walk."""
    try:
        await _scan(catalog, cache, refresh=False)
    except Exception:
        LOGGER.exception('could not scan the mapping tree for multi-part titles')


async def current_title(catalog: MergeCatalog, cache: TitleScanCache, avid: str) -> MultipartTitle | None:
    """The title as its directory holds it now, found through the last scan."""
    wanted = avid.strip().upper()
    for refresh in (False, True):
        _archive, scanned = await _scan(catalog, cache, refresh=refresh)
        if scanned is None:
            return None
        found = next((title for title in scanned[0] if title.avid.upper() == wanted), None)
        if found is not None:
            break
    else:
        return None
    root = Path(catalog.mapping().dst_dir)
    archive = catalog.archive()
    rescanned = await asyncio.to_thread(scan_multipart, root / found.directory, library_root=archive.dst_dir)
    return next((title for title in rescanned if title.avid.upper() == wanted), None)


def _add_task_routes(
    router: APIRouter,
    catalog: MergeCatalog,
    cache: TitleScanCache,
    tasks: MergeTasksApi,
    dependencies: list[Any],
) -> None:
    @router.get('/tasks')
    async def list_tasks() -> TasksView:
        return TasksView(
            items=[TaskView.from_task(task) for task in await tasks.repository.listing()],
            unavailable=tasks.service.unavailable(),
            auto=tasks.auto_status() if tasks.auto_status is not None else None,
        )

    @router.post('/tasks', status_code=201, dependencies=dependencies)
    async def create_task(request: MergeRequest) -> TaskView:
        if tasks.service.unavailable() is not None:
            raise ApiError(409, 'merge_unavailable')
        title = await current_title(catalog, cache, request.avid)
        task = await enqueue(catalog, tasks.repository, catalog.archive(), title, request)
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


async def enqueue(
    catalog: MergeCatalog,
    repository: MergeTaskRepository,
    archive: ArchiveConfig,
    title: MultipartTitle | None,
    request: MergeRequest,
) -> MergeTask:
    """Queue one scanned title, after checking it can be merged and re-enter somewhere."""
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
