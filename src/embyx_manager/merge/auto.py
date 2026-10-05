"""Merging titles one after another without anyone clicking 合并.

With ``merge.auto_enabled`` on, the next title is queued only while no task
is open, so each title is merged, uploaded, checked and filed before the next
one starts and merged files never pile up waiting for CloudDrive. A failed
task pauses this until someone retries or removes it; a title whose task was
removed or cancelled is never picked again.

A title is queued only when its parts fit on the work volume with the
reserve to spare. One that does not is skipped for now and looked at again
on the next round, once uploads have freed the space.
"""

import asyncio
import logging
import shutil
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

from embyx_manager.clients.clouddrive.aio import AsyncCloudDrive
from embyx_manager.config.models import MergeConfig
from embyx_manager.errors import ApiError
from embyx_manager.merge.api import (
    AutoState,
    AutoStatus,
    MergeCatalog,
    MergeRequest,
    SkippedTitle,
    TitleScanCache,
    current_title,
    enqueue,
)
from embyx_manager.merge.detect import MAX_STACKED_PARTS, MultipartTitle
from embyx_manager.merge.service import MergeService, api_path
from embyx_manager.merge.tasks import MergeState, MergeTaskRepository

LOGGER = logging.getLogger(__name__)

POLL_SECONDS = 60.0
#: Scan the library again at least this often, for titles that arrived since.
RESCAN_INTERVAL = timedelta(hours=24)
#: How many skipped titles the status names; the rest are only counted.
SKIPPED_SHOWN = 20
GIB = 1 << 30


def free_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


class AutoMerger:
    def __init__(  # noqa: PLR0913
        self,
        *,
        repository: MergeTaskRepository,
        catalog: MergeCatalog,
        cache: TitleScanCache,
        service: MergeService,
        merge_config: Callable[[], MergeConfig],
        cloud: Callable[[], AsyncCloudDrive | None],
        free_space: Callable[[Path], int] = free_bytes,
        poll_seconds: float = POLL_SECONDS,
    ) -> None:
        self._repository = repository
        self._catalog = catalog
        self._cache = cache
        self._service = service
        self._merge_config = merge_config
        self._cloud = cloud
        self._free_space = free_space
        self._poll_seconds = poll_seconds
        #: Part sizes never change, so each title's total is read from CloudDrive once.
        self._sizes: dict[tuple[str, ...], int] = {}
        #: Titles refused for a reason only a new scan can change, and the scan they came from.
        self._refused: dict[str, SkippedTitle] = {}
        self._refused_scan: datetime | None = None
        self._status = AutoStatus(state=AutoState.OFF)
        self._task: asyncio.Task[None] | None = None

    def status(self) -> AutoStatus:
        return self._status

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name='merge-auto-loop')

    async def aclose(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                await self.step()
            except Exception:
                LOGGER.exception('automatic merge round failed')
            await asyncio.sleep(self._poll_seconds)

    async def step(self) -> None:
        """Queue the next title that fits, when nothing else is under way."""
        config = self._merge_config()
        blocked = await self._blocked(config)
        if blocked is not None:
            self._status = AutoStatus(state=blocked)
            return
        scanned = await self._titles()
        if scanned is None:
            self._status = AutoStatus(state=AutoState.UNAVAILABLE)
            return
        titles, scanned_at = scanned
        if scanned_at != self._refused_scan:
            self._refused, self._refused_scan = {}, scanned_at
        room = self._free_space(Path(config.work_dir)) - config.free_space_reserve_gib * GIB
        cancelled = await self._repository.cancelled_avids()
        skipped: list[SkippedTitle] = []
        for title in titles:
            if _eligible(title, config, cancelled) and await self._try_queue(title, config, room, skipped):
                self._status = self._report(AutoState.BUSY, room, skipped)
                return
        self._status = self._report(AutoState.IDLE, room, skipped)

    async def _blocked(self, config: MergeConfig) -> AutoState | None:
        """Why no title may be queued now, or None when one may."""
        if not config.auto_enabled:
            return AutoState.OFF
        if self._service.unavailable() is not None:
            return AutoState.UNAVAILABLE
        open_tasks = await self._repository.open_tasks()
        if any(task.state == MergeState.FAILED for task in open_tasks):
            return AutoState.PAUSED
        return AutoState.BUSY if open_tasks else None

    async def _try_queue(
        self, title: MultipartTitle, config: MergeConfig, room: int, skipped: list[SkippedTitle]
    ) -> bool:
        """Queue ``title`` when it fits and can be merged; otherwise note why in ``skipped``."""
        if title.avid in self._refused:
            skipped.append(self._refused[title.avid])
            return False
        size, reason = await self._size(title, config)
        if size is None or size > room:
            skipped.append(SkippedTitle(avid=title.avid, size=size, reason=reason or 'too_big'))
            return False
        try:
            task = await enqueue(
                self._catalog,
                self._repository,
                self._catalog.archive(),
                await current_title(self._catalog, self._cache, title.avid),
                MergeRequest(avid=title.avid),
            )
        except ApiError as exc:
            refused = SkippedTitle(avid=title.avid, size=size, reason=exc.code)
            self._refused[title.avid] = refused
            skipped.append(refused)
            return False
        LOGGER.info('queued %s (%d parts, %d bytes) for automatic merging', task.avid, title.part_count, size)
        self._service.wake()
        return True

    @staticmethod
    def _report(state: AutoState, room: int, skipped: list[SkippedTitle]) -> AutoStatus:
        return AutoStatus(
            state=state,
            room=max(room, 0),
            skipped=skipped[:SKIPPED_SHOWN],
            skipped_count=len(skipped),
            checked_at=datetime.now(UTC),
        )

    async def _titles(self) -> tuple[list[MultipartTitle], datetime] | None:
        """The last scan, scanned again when a title was filed since or it has grown old."""
        archive = self._catalog.archive()
        mapping = self._catalog.mapping()
        if not mapping.dst_dir or not archive.dst_dir:
            return None
        root = Path(mapping.dst_dir)
        titles, scanned_at = await self._cache.titles(root, archive.dst_dir, refresh=False)
        last_done = await self._repository.last_done_at()
        stale = datetime.now(UTC) - scanned_at > RESCAN_INTERVAL
        if stale or (last_done is not None and last_done > scanned_at):
            return await self._cache.titles(root, archive.dst_dir, refresh=True)
        return titles, scanned_at

    async def _size(self, title: MultipartTitle, config: MergeConfig) -> tuple[int | None, str | None]:
        """The parts' total size as CloudDrive lists them, or why it is unknown."""
        targets = tuple(part.target for part in title.parts)
        if targets in self._sizes:
            return self._sizes[targets], None
        cloud = self._cloud()
        if cloud is None:
            return None, 'size_unknown'
        directory = str(PurePosixPath(targets[0]).parent)
        try:
            listing = await cloud.list_directory(api_path(directory, config.cloud_mount_prefix), force_refresh=False)
        except Exception:
            LOGGER.exception('could not list %s for automatic merging', directory)
            return None, 'size_unknown'
        sizes = {str(entry['name']): int(entry['size']) for entry in listing if not entry['is_directory']}
        names = [PurePosixPath(target).name for target in targets]
        if any(name not in sizes for name in names):
            return None, 'parts_missing'
        total = sum(sizes[name] for name in names)
        self._sizes[targets] = total
        return total, None


def _eligible(title: MultipartTitle, config: MergeConfig, cancelled: frozenset[str]) -> bool:
    if not title.complete or title.avid in cancelled:
        return False
    return config.auto_include_stackable or title.part_count > MAX_STACKED_PARTS
