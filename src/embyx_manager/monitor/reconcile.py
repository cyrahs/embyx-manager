"""The scan that catches what the tracker never saw.

The tracker handles everything the pipeline itself downloaded. This scan is for
the rest: folders dropped in by hand, downloads that predate the ledger, and
anything left behind when an offline record was cleared before it could be
archived. It walks the configured routes, files what it can, and writes what it
finds into the ledger so the result is visible in the same place as everything
else.

Two rules keep it from fighting the tracker or itself. An AVID the ledger shows
as active belongs to the tracker, and its folder is left alone. And a folder is
only archived once it has been seen twice at the same size: the download
directory is a CloudDrive mount fed by offline downloads that appear whole, but
a folder copied in by other means may still be filling up.

A folder it cannot identify becomes a needs_attention row rather than a warning
logged every run forever.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from embyx_manager.config.models import ArchiveConfig
from embyx_manager.monitor.acquisitions import AcquisitionRepository, AcquisitionSource, AcquisitionState
from embyx_manager.monitor.archive import ArchivePipeline, Outcome
from embyx_manager.monitor.reports import RunContext

LOGGER = logging.getLogger('embyx-manager.reconcile')


@dataclass(frozen=True)
class _Sighting:
    """What a folder looked like on a previous pass."""

    size: int
    entries: int


class ReconcileScanner:
    def __init__(self, *, ledger: AcquisitionRepository, archiver: ArchivePipeline, config: ArchiveConfig) -> None:
        self._ledger = ledger
        self._archiver = archiver
        self._config = config
        # Folder -> what it looked like last pass, so growth is visible.
        self._seen: dict[Path, _Sighting] = {}
        # Folders reported as needing attention that have no AVID to file them
        # under. The ledger is keyed by AVID, so a folder holding two of them
        # cannot be parked there; remembering it here at least keeps the warning
        # from repeating every pass. Restarting the process reports them once more.
        self._reported: set[Path] = set()

    def rebind(self, *, archiver: ArchivePipeline, config: ArchiveConfig) -> None:
        """Point the scanner at the current configuration between runs.

        The scanner outlives a single run because settling a folder takes two
        passes to observe; only its collaborators are refreshed.
        """
        self._archiver = archiver
        self._config = config

    async def run(self, ctx: RunContext) -> None:
        active = await self._ledger.active_avids()
        routes = [(src, dst, True) for src, dst in self._config.priority_mapping.items()]
        routes += [(src, dst, False) for src, dst in self._config.mapping.items()]
        for src, dst, priority in routes:
            ctx.check_cancelled()
            root = self._archiver.src_dir / src
            if not await asyncio.to_thread(root.is_dir):
                # Nothing to do is not a failure; the route may not be mounted yet.
                ctx.info('skipping %s, its source directory does not exist', root)
                continue
            await self._scan_route(root, dst, active, ctx, priority=priority)

    async def _scan_route(
        self,
        root: Path,
        dst_subdir: str,
        active: frozenset[str],
        ctx: RunContext,
        *,
        priority: bool,
    ) -> None:
        folders = await asyncio.to_thread(lambda: sorted(entry for entry in root.iterdir() if entry.is_dir()))
        for folder in folders:
            ctx.check_cancelled()
            avid = self._archiver.avid_of(folder.name)
            if avid and avid in active:
                # The tracker owns this download; the ledger is the lock.
                ctx.info('leaving %s to the tracker', folder.name)
                continue
            if not await self._is_settled(folder, ctx):
                continue
            await self._reconcile_folder(folder, dst_subdir, ctx, priority=priority)

    async def _is_settled(self, folder: Path, ctx: RunContext) -> bool:
        """True once a folder has looked the same for two consecutive passes."""
        sighting = await asyncio.to_thread(self._measure, folder)
        previous = self._seen.get(folder)
        self._seen[folder] = sighting
        if previous == sighting:
            return True
        ctx.info('%s is still changing, leaving it for the next pass', folder.name)
        return False

    @staticmethod
    def _measure(folder: Path) -> _Sighting:
        total = 0
        entries = 0
        for path in folder.rglob('*'):
            if path.is_file():
                total += path.stat().st_size
                entries += 1
        return _Sighting(size=total, entries=entries)

    async def _reconcile_folder(
        self,
        folder: Path,
        dst_subdir: str,
        ctx: RunContext,
        *,
        priority: bool,
    ) -> None:
        result = await asyncio.to_thread(
            self._archiver.archive_folder,
            folder,
            dst_subdir,
            ctx,
            priority=priority,
        )
        now = datetime.now(UTC)
        if result.outcome is Outcome.ARCHIVED:
            self._seen.pop(folder, None)
            await self._record_archived(result.avid, result.archived_paths, now)
            ctx.add('reconciled')
            return
        if result.outcome is not Outcome.NEEDS_ATTENTION:
            return
        if result.avid:
            await self._record_attention(result.avid, result.reason, now, ctx)
        elif folder not in self._reported:
            self._reported.add(folder)
            ctx.warning('%s needs attention: %s', folder.name, result.reason)
            ctx.add('needs_attention')

    async def _record_archived(self, avid: str, archived_paths: tuple[str, ...], now: datetime) -> None:
        record = await self._ledger.get(avid)
        if record is None:
            await self._ledger.discover(avid, source=AcquisitionSource.RECONCILE, now=now)
            record = await self._ledger.get(avid)
        if record is None or record.state is AcquisitionState.ARCHIVED:
            return
        await self._ledger.transition(
            avid,
            expected=record.state,
            target=AcquisitionState.ARCHIVED,
            now=now,
            archived_paths=archived_paths,
        )

    async def _record_attention(self, avid: str, reason: str, now: datetime, ctx: RunContext) -> None:
        """Park an unidentifiable folder in the ledger; log it only the first time."""
        record = await self._ledger.get(avid)
        if record is not None and record.state is AcquisitionState.NEEDS_ATTENTION:
            return
        if record is None:
            await self._ledger.discover(avid, source=AcquisitionSource.RECONCILE, now=now)
            record = await self._ledger.get(avid)
        if record is None or record.state is AcquisitionState.ARCHIVED:
            return
        if await self._ledger.transition(
            avid,
            expected=record.state,
            target=AcquisitionState.NEEDS_ATTENTION,
            now=now,
            note=reason,
        ):
            ctx.warning('%s needs attention: %s', avid, reason)
            ctx.add('needs_attention')
