"""Driving each acquisition to a conclusion against CloudDrive's task list.

The tracker is the loop that makes the ledger worth keeping. Every pass it reads
every offline task CloudDrive knows about and joins it to the ledger on the info
hash recorded when the magnet was submitted. From there each attempt goes
somewhere: progress is refreshed, a finished download is archived, and anything
that errored, stopped moving, or turned out to hold no usable video falls through
to the AVID's next magnet.

It is a service loop rather than a pipeline run: it polls every few minutes and
would bury the run history in noise. What it does is visible in the ledger
instead. Concurrency is handled the same way, by claiming a finished attempt with
a compare-and-set, so two replicas can poll the same task list and only one will
archive the folder.

Deliberately absent: clearing finished tasks from CloudDrive. That existed to
stop the old scanner reprocessing folders, and the ledger answers that now.
Leaving the records in place also keeps CloudDrive's own duplicate detection
working when the same magnet comes round again.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from embyx_manager.clients.clouddrive import AsyncCloudDrive
from embyx_manager.clients.clouddrive.aio import OfflineStatus, OfflineTask
from embyx_manager.config.models import ArchiveConfig
from embyx_manager.monitor.acquisitions import (
    AcquisitionRepository,
    AcquisitionState,
    AttemptState,
    MagnetAttemptRecord,
)
from embyx_manager.monitor.archive import ArchivePipeline, ArchiveResult, Outcome
from embyx_manager.monitor.reports import RunContext

LOGGER = logging.getLogger('embyx-manager.tracker')

#: ``submit_magnet(avid, magnet)`` adds one offline task; True when accepted.
SubmitMagnet = Callable[[str, str], Awaitable[bool]]


@dataclass(frozen=True)
class TrackerRoute:
    """One archive route, resolved to the local directory it covers."""

    root: Path
    dst: str
    priority: bool


@dataclass(frozen=True)
class TrackerSettings:
    """Where finished downloads live and how patient the tracker is.

    Offline tasks are queued under CloudDrive API paths: the default directory
    and any RSS category that downloads somewhere of its own. Each has a local
    view among the archive routes, so a finished download is located in the route
    tables and filed by whichever route holds it, and the tracker shares the
    scan's directory configuration instead of duplicating it.
    """

    #: The CloudDrive API paths the offline tasks are queued under, default first.
    task_dir_paths: tuple[str, ...]
    #: The archive routes, priority routes first, resolved against src_dir.
    routes: tuple[TrackerRoute, ...]
    stall_timeout_hours: int = 24
    max_attempts: int = 5
    #: How long CloudDrive is given to register a freshly submitted magnet. Until
    #: then its absence from the task list means nothing: a magnet submitted during
    #: this very poll cannot appear in the listing this poll already read.
    submit_grace_seconds: int = 300

    @classmethod
    def from_config(cls, config: ArchiveConfig, *, task_dir_paths: tuple[str, ...]) -> 'TrackerSettings':
        src_dir = Path(config.src_dir)
        routes = tuple(
            TrackerRoute(root=src_dir / source, dst=dst, priority=priority)
            for table, priority in ((config.priority_mapping, True), (config.mapping, False))
            for source, dst in table.items()
        )
        return cls(
            task_dir_paths=task_dir_paths,
            routes=routes,
            stall_timeout_hours=config.stall_timeout_hours,
            max_attempts=config.max_attempts,
        )


class AcquisitionTracker:
    def __init__(
        self,
        *,
        ledger: AcquisitionRepository,
        cloud: AsyncCloudDrive,
        archiver: ArchivePipeline,
        settings: TrackerSettings,
        submit_magnet: SubmitMagnet,
    ) -> None:
        """``submit_magnet(avid, magnet)`` adds one offline task; True when accepted."""
        self._ledger = ledger
        self._cloud = cloud
        self._archiver = archiver
        self._settings = settings
        self._submit = submit_magnet

    async def poll(self, ctx: RunContext) -> None:
        """One pass over the offline task lists of every configured directory."""
        if not self._settings.task_dir_paths:
            # No directory to read means no evidence either way, which is not the
            # same as every task having vanished; sweeping here would conclude the
            # opposite and give up on everything in flight.
            ctx.warning('no offline directory is configured, skipping the poll')
            return
        # Each task is kept with the directory it was listed under, which is the
        # only way back to its path once the listings are merged.
        by_hash: dict[str, tuple[OfflineTask, str]] = {}
        total = 0
        for task_dir in self._settings.task_dir_paths:
            ctx.check_cancelled()
            tasks = await self._cloud.list_offline_files(task_dir)
            total += len(tasks)
            for task in tasks:
                info_hash = str(task['info_hash']) if task['info_hash'] else ''
                if not info_hash:
                    continue
                if info_hash in by_hash:
                    # CloudDrive rejects a magnet already queued elsewhere, so this
                    # is rare; the first directory listing it wins either way.
                    ctx.add('duplicate_offline_tasks')
                    continue
                by_hash[info_hash] = (task, task_dir)
        ctx.set('offline_tasks', total)
        attempts = await self._ledger.attempts_by_info_hash(by_hash)

        for info_hash, attempt in attempts.items():
            ctx.check_cancelled()
            task, task_dir = by_hash[info_hash]
            await self._advance(attempt, task, task_dir, ctx)

        await self._sweep_lost(set(by_hash), ctx)

    # -- one attempt ---------------------------------------------------------

    async def _advance(
        self,
        attempt: MagnetAttemptRecord,
        task: OfflineTask,
        task_dir: str,
        ctx: RunContext,
    ) -> None:
        status = task['status']
        if status is OfflineStatus.FINISHED:
            await self._archive_finished(attempt, task, task_dir, ctx)
        elif status is OfflineStatus.ERROR:
            await self._give_up(attempt, AttemptState.ERROR, 'CloudDrive reported the download failed', ctx)
        elif attempt.state in {AttemptState.SUBMITTED, AttemptState.DOWNLOADING}:
            await self._refresh_progress(attempt, task, ctx)

    async def _refresh_progress(self, attempt: MagnetAttemptRecord, task: OfflineTask, ctx: RunContext) -> None:
        now = datetime.now(UTC)
        moved = await self._ledger.record_progress(
            attempt.avid,
            attempt.attempt_no,
            state=AttemptState.DOWNLOADING,
            progress=float(task['progress']),
            now=now,
        )
        if moved:
            return
        # Nothing was written, so updated_at still marks the last real progress.
        stalled_for = now - attempt.updated_at
        if stalled_for >= timedelta(hours=self._settings.stall_timeout_hours):
            await self._give_up(
                attempt,
                AttemptState.STALLED,
                f'no progress for {stalled_for.days}d at {attempt.progress or 0:.0f}%',
                ctx,
            )

    async def _archive_finished(
        self,
        attempt: MagnetAttemptRecord,
        task: OfflineTask,
        task_dir: str,
        ctx: RunContext,
    ) -> None:
        now = datetime.now(UTC)
        record = await self._ledger.get(attempt.avid)
        if record is None or record.state is not AcquisitionState.DOWNLOADING:
            # Someone is holding this AVID: an operator marked it, or it is done.
            return
        if attempt.state is not AttemptState.FINISHED and not await self._ledger.transition_attempt(
            attempt.avid,
            attempt.attempt_no,
            expected=attempt.state,
            target=AttemptState.FINISHED,
            now=now,
        ):
            return
        if not await self._ledger.transition_attempt(
            attempt.avid,
            attempt.attempt_no,
            expected=AttemptState.FINISHED,
            target=AttemptState.ARCHIVING,
            now=now,
        ):
            # Another replica claimed it first.
            return

        await self._refresh_mount(task, task_dir, ctx)
        located = await asyncio.to_thread(self._locate, str(task['name']), attempt.avid)
        if located is None:
            # Release the claim: the mount may simply not show the folder yet.
            await self._ledger.transition_attempt(
                attempt.avid,
                attempt.attempt_no,
                expected=AttemptState.ARCHIVING,
                target=AttemptState.FINISHED,
                now=datetime.now(UTC),
            )
            ctx.warning('finished task %s is not under any archive route yet', task['name'])
            return
        folder, route = located
        result = await asyncio.to_thread(
            self._archiver.archive_folder,
            folder,
            route.dst,
            ctx,
            priority=route.priority,
            expected_avid=attempt.avid,
        )
        await self._settle(attempt, result, ctx)

    async def _settle(self, attempt: MagnetAttemptRecord, result: ArchiveResult, ctx: RunContext) -> None:
        now = datetime.now(UTC)
        outcome = result.outcome
        if outcome is Outcome.ARCHIVED:
            await self._ledger.transition_attempt(
                attempt.avid,
                attempt.attempt_no,
                expected=AttemptState.ARCHIVING,
                target=AttemptState.ARCHIVED,
                now=now,
            )
            await self._ledger.transition(
                attempt.avid,
                expected=AcquisitionState.DOWNLOADING,
                target=AcquisitionState.ARCHIVED,
                now=now,
                archived_paths=result.archived_paths,
            )
            ctx.add('archived')
            return
        if outcome is Outcome.JUNK:
            ctx.warning('%s finished with no usable video, trying the next magnet', attempt.avid)
            await self._ledger.transition_attempt(
                attempt.avid,
                attempt.attempt_no,
                expected=AttemptState.ARCHIVING,
                target=AttemptState.JUNK,
                now=now,
            )
            ctx.add('junk')
            await self._try_next(attempt.avid, ctx)
            return
        # Needs attention or a transient failure: release the claim so the work is
        # retried, and park the AVID when a human has to look at it.
        await self._ledger.transition_attempt(
            attempt.avid,
            attempt.attempt_no,
            expected=AttemptState.ARCHIVING,
            target=AttemptState.FINISHED,
            now=now,
        )
        if outcome is Outcome.NEEDS_ATTENTION:
            await self._ledger.transition(
                attempt.avid,
                expected=AcquisitionState.DOWNLOADING,
                target=AcquisitionState.NEEDS_ATTENTION,
                now=now,
                note=result.reason,
            )
            ctx.add('needs_attention')

    async def _give_up(
        self,
        attempt: MagnetAttemptRecord,
        state: AttemptState,
        reason: str,
        ctx: RunContext,
    ) -> None:
        now = datetime.now(UTC)
        if not await self._ledger.transition_attempt(
            attempt.avid,
            attempt.attempt_no,
            expected=attempt.state,
            target=state,
            now=now,
            error=reason,
        ):
            return
        ctx.warning('%s: %s', attempt.avid, reason)
        ctx.add(state.value)
        await self._try_next(attempt.avid, ctx)

    async def _try_next(self, avid: str, ctx: RunContext) -> None:
        """Submit this AVID's next magnet, or mark it out of options."""
        record = await self._ledger.get(avid)
        if record is None or record.state is not AcquisitionState.DOWNLOADING:
            # The AVID is nobody's to retry: an operator ignored or parked it,
            # or it is already archived. An operator cancelling a duplicate's
            # offline task must not have the sweep resurrect it with the next
            # candidate; the remaining magnets stay put until a deliberate
            # retry hands the AVID back to the tracker.
            return
        attempts = await self._ledger.attempts_for(avid)
        # The cap counts magnets actually submitted, not candidates collected, so
        # a long candidate list still stops after max_attempts real downloads.
        tried = sum(1 for attempt in attempts if attempt.state is not AttemptState.PENDING)
        if tried >= self._settings.max_attempts:
            await self._exhaust(avid, ctx)
            return
        now = datetime.now(UTC)
        claimed = await self._ledger.claim_next_pending(avid, now=now)
        if claimed is None:
            await self._exhaust(avid, ctx)
            return
        if await self._submit(avid, claimed.magnet):
            ctx.info('%s: trying magnet %d', avid, claimed.attempt_no)
            ctx.add('retried')
            return
        await self._ledger.transition_attempt(
            avid,
            claimed.attempt_no,
            expected=AttemptState.SUBMITTED,
            target=AttemptState.ERROR,
            now=now,
            error='failed to add the offline task',
        )
        await self._try_next(avid, ctx)

    async def _exhaust(self, avid: str, ctx: RunContext) -> None:
        record = await self._ledger.get(avid)
        if record is None or record.state is not AcquisitionState.DOWNLOADING:
            return
        now = datetime.now(UTC)
        await self._ledger.transition(
            avid,
            expected=AcquisitionState.DOWNLOADING,
            target=AcquisitionState.EXHAUSTED,
            now=now,
            note='every magnet was tried',
            # The resolver may find new magnets once the release has aged.
            next_action_at=now + timedelta(hours=self._settings.stall_timeout_hours),
        )
        ctx.warning('%s: every magnet was tried', avid)
        ctx.add('exhausted')

    # -- tasks that vanished --------------------------------------------------

    async def _sweep_lost(self, live_hashes: set[str], ctx: RunContext) -> None:
        """Attempts CloudDrive no longer lists, i.e. records cleared behind our back."""
        grace = timedelta(seconds=self._settings.submit_grace_seconds)
        now = datetime.now(UTC)
        for attempt in await self._ledger.in_flight_attempts():
            ctx.check_cancelled()
            if attempt.info_hash is None or attempt.info_hash in live_hashes:
                continue
            if attempt.submitted_at is not None and now - attempt.submitted_at < grace:
                continue
            located = await asyncio.to_thread(self._find_folder_for, attempt.avid)
            if located is not None:
                # The download did land; only its bookkeeping went away.
                await self._archive_orphaned(attempt, *located, ctx=ctx)
                continue
            await self._give_up(attempt, AttemptState.LOST, 'CloudDrive no longer lists this task', ctx)

    # -- locating a download in the routes ------------------------------------

    def _locate(self, name: str, avid: str) -> tuple[Path, TrackerRoute] | None:
        """The downloaded folder and the archive route holding it.

        Every offline task lands in one of the task directories, each of which the
        operator lists among the archive routes; the exact task name is tried
        first, the AVID second so a renamed folder is still found. Searching all
        routes is unambiguous even when one route's source nests inside another's,
        because the folder exists under exactly one of them.
        """
        for route in self._settings.routes:
            candidate = route.root / name
            if candidate.is_dir():
                return candidate, route
        return self._find_folder_for(avid)

    def _find_folder_for(self, avid: str) -> tuple[Path, TrackerRoute] | None:
        """The route folder holding this AVID, if the download is on the mount.

        A vanished task takes its folder name with it, so the directory is
        matched by the AVID its name parses to rather than by name.
        """
        for route in self._settings.routes:
            if not route.root.is_dir():
                continue
            for folder in sorted(route.root.iterdir()):
                if folder.is_dir() and self._avid_of(folder) == avid:
                    return folder, route
        return None

    def _avid_of(self, folder: Path) -> str:
        return self._archiver.avid_of(folder.name)

    async def _archive_orphaned(
        self,
        attempt: MagnetAttemptRecord,
        folder: Path,
        route: TrackerRoute,
        *,
        ctx: RunContext,
    ) -> None:
        now = datetime.now(UTC)
        if not await self._ledger.transition_attempt(
            attempt.avid,
            attempt.attempt_no,
            expected=attempt.state,
            target=AttemptState.FINISHED,
            now=now,
        ):
            return
        if not await self._ledger.transition_attempt(
            attempt.avid,
            attempt.attempt_no,
            expected=AttemptState.FINISHED,
            target=AttemptState.ARCHIVING,
            now=now,
        ):
            return
        result = await asyncio.to_thread(
            self._archiver.archive_folder,
            folder,
            route.dst,
            ctx,
            priority=route.priority,
            expected_avid=attempt.avid,
        )
        await self._settle(attempt, result, ctx)

    async def _refresh_mount(self, task: OfflineTask, task_dir: str, ctx: RunContext) -> None:
        """Make CloudDrive re-read the finished task's directory before we walk it.

        The mount caches listings, so a just-finished download can look empty.
        Asking the API for it is the targeted version of what used to be a
        five-second sleep after every stage. ``task_dir`` is the directory the
        task was listed under, which is where its folder actually is.
        """
        path = f'{task_dir}/{task["name"]}'
        try:
            await self._cloud.list_directory(path)
        except FileNotFoundError:
            ctx.warning('finished task %s is not on the mount yet', task['name'])
        except Exception:  # noqa: BLE001
            ctx.exception('failed to refresh %s', path)
