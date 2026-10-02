"""Merge endpoints: the multi-part titles the library holds and where each would re-enter."""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from fastapi import APIRouter
from pydantic import BaseModel

from embyx_manager.config.models import ArchiveConfig, MappingConfig
from embyx_manager.merge.detect import (
    MultipartTitle,
    SourceBasis,
    TitleProblem,
    resolve_source,
    route_sources,
    scan_multipart,
)


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


def create_merge_router(catalog: MergeCatalog) -> APIRouter:
    router = APIRouter(prefix='/api/merge')

    @router.get('/titles')
    async def list_titles() -> TitlesView:
        archive = catalog.archive()
        mapping = catalog.mapping()
        if not mapping.dst_dir or not archive.dst_dir:
            return TitlesView(
                items=[],
                routes=list(route_sources(archive)),
                scanned_at=None,
                reason='mapping.dst_dir and archive.dst_dir must be configured',
            )
        titles = await asyncio.to_thread(scan_multipart, Path(mapping.dst_dir), library_root=archive.dst_dir)
        task_dirs = await catalog.task_dirs_for([title.avid for title in titles])
        return TitlesView(
            items=[_view(archive, title, task_dirs.get(title.avid)) for title in titles],
            routes=list(route_sources(archive)),
            scanned_at=datetime.now(UTC),
        )

    return router


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
