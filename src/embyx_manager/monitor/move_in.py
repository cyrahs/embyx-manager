"""Filing fill-actor moves into the library.

Fill Actor's apply step moves a chosen video out of an additional library into
the staging tree under ``fill_actor.move_in_root``, optionally inside a brand
subdirectory. Nothing downloads into that tree: every file below it is one an
operator explicitly asked to move, so this sweep files each one into the
library through the same archive pipeline downloads go through, and records
the outcome in the acquisition ledger where every other intake shows up.

The library destination is the route fill actor's offline directory feeds
(``fill_actor.task_dir_path``): a moved file and a fill-actor download of the
same video belong in the same place. A deployment whose staging tree is not
routed yet is reported, not guessed at.

Only files are touched. The sweep never deletes a directory, so a mis-pointed
configuration cannot cost a tree; an emptied brand directory stays behind and
stages nothing.
"""

import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path

from embyx_manager.core.avid import AvidParser
from embyx_manager.core.media import is_video
from embyx_manager.monitor.acquisitions import AcquisitionRepository, AcquisitionSource, AcquisitionState
from embyx_manager.monitor.archive import ArchivePipeline, Outcome
from embyx_manager.monitor.reports import RunContext

LOGGER = logging.getLogger('embyx-manager.move-in')


class MoveInSweeper:
    def __init__(
        self,
        *,
        ledger: AcquisitionRepository,
        archiver: ArchivePipeline,
        avid_parser: AvidParser,
        move_in_root: Path,
        task_dir_path: str,
    ) -> None:
        self._ledger = ledger
        self._archiver = archiver
        self._avid = avid_parser
        self._move_in_root = move_in_root
        self._task_dir_path = task_dir_path
        # Files already reported as unreadable, so the warning fires once, not
        # every pass forever. Restarting the process reports them once more.
        self._reported: set[Path] = set()

    def rebind(
        self,
        *,
        archiver: ArchivePipeline,
        avid_parser: AvidParser,
        move_in_root: Path,
        task_dir_path: str,
    ) -> None:
        """Point the sweeper at the current configuration between runs."""
        self._archiver = archiver
        self._avid = avid_parser
        self._move_in_root = move_in_root
        self._task_dir_path = task_dir_path

    async def run(self, ctx: RunContext) -> None:
        if not self._task_dir_path:
            ctx.info('skipping the move-in sweep, fill_actor.task_dir_path is not configured')
            return
        route = self._archiver.route_for_task_dir(self._task_dir_path)
        if route is None:
            ctx.warning(
                'skipping the move-in sweep, no archive route covers %s',
                self._task_dir_path,
            )
            return
        dst_subdir, priority = route
        staged = await asyncio.to_thread(self._staged_videos)
        if not staged:
            return
        by_avid: dict[str, list[Path]] = {}
        for video in staged:
            avid = self._avid.get_avid(video.name)
            if not avid:
                if video not in self._reported:
                    self._reported.add(video)
                    ctx.warning('failed to read an avid for the staged file %s, leaving it', video.name)
                    ctx.add('needs_attention')
                continue
            by_avid.setdefault(avid, []).append(video)
        for avid, group in by_avid.items():
            ctx.check_cancelled()
            await self._file_group(avid, group, dst_subdir, ctx, priority=priority)

    def _staged_videos(self) -> list[Path]:
        """Every video in the staging tree: the root and its brand directories."""
        root = self._move_in_root
        if not root.is_dir() or root.is_symlink():
            return []
        videos: list[Path] = []
        for entry in sorted(root.iterdir()):
            if entry.is_symlink():
                continue
            if is_video(entry):
                videos.append(entry)
            elif entry.is_dir():
                videos.extend(child for child in sorted(entry.iterdir()) if not child.is_symlink() and is_video(child))
        return videos

    async def _file_group(
        self,
        avid: str,
        videos: list[Path],
        dst_subdir: str,
        ctx: RunContext,
        *,
        priority: bool,
    ) -> None:
        try:
            result = await asyncio.to_thread(
                self._archiver.archive_loose_group,
                avid,
                videos,
                dst_subdir,
                ctx,
                priority=priority,
            )
        except Exception:  # noqa: BLE001
            ctx.exception('failed to archive the staged files of %s', avid)
            ctx.add('items_failed')
            return
        now = datetime.now(UTC)
        if result.outcome is Outcome.ARCHIVED:
            await self._record_archived(avid, result.archived_paths, now)
            ctx.add('move_in_archived')
            return
        await self._record_attention(avid, result.reason, now, ctx)

    async def _record_archived(self, avid: str, archived_paths: tuple[str, ...], now: datetime) -> None:
        record = await self._ledger.get(avid)
        if record is None:
            await self._ledger.discover(avid, source=AcquisitionSource.FILL_ACTOR, now=now)
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
        """Park a staged AVID the library would not take; log it only the first time."""
        record = await self._ledger.get(avid)
        if record is not None and record.state is AcquisitionState.NEEDS_ATTENTION:
            return
        if record is None:
            await self._ledger.discover(avid, source=AcquisitionSource.FILL_ACTOR, now=now)
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
