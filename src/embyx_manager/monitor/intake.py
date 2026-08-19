"""Shared acquisition intake: one AVID -> magnet candidates -> first offline task.

Every input source (the RSS categories, fill actor) funnels newly wanted AVIDs
through this component. It records the AVID in the acquisition ledger, resolves
a ranked list of magnet candidates, and submits the first one to CloudDrive;
the tracker owns the download from there. An AVID with no usable magnet is
parked in RESOLVE_FAILED with a cooldown, and the tracker's retry pass hands it
back here once the cooldown expires.

Sources name the offline directory each submission is queued under — pinned on
the acquisition at discovery — so one source's downloads can be filed by an
archive route of its own.
"""

from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from enum import StrEnum

import grpc
import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from embyx_manager.clients.clouddrive import AsyncCloudDrive
from embyx_manager.clients.javbus import JavBusClient
from embyx_manager.clients.sukebei import SukebeiClient
from embyx_manager.core.magnet import extract_info_hash
from embyx_manager.monitor.acquisitions import (
    AcquisitionRecord,
    AcquisitionRepository,
    AcquisitionState,
    AttemptState,
    MagnetCandidate,
)
from embyx_manager.monitor.reports import RunContext

MAX_CANDIDATES = 5

CLOUD_RETRY = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((grpc.RpcError, httpx.HTTPError, httpx.TimeoutException, OSError)),
    reraise=True,
)


class IntakeOutcome(StrEnum):
    """What became of one AVID handed to the intake."""

    SUBMITTED = 'submitted'
    QUEUED = 'queued'
    ALREADY_TRACKED = 'already_tracked'
    NO_MAGNET = 'no_magnet'
    SUBMIT_FAILED = 'submit_failed'


class AcquisitionIntake:
    def __init__(  # noqa: PLR0913
        self,
        *,
        ledger: AcquisitionRepository,
        sukebei: SukebeiClient,
        javbus: JavBusClient,
        cloud: AsyncCloudDrive,
        failed_cooldown_seconds: int,
        on_submitted: Callable[[], None] | None = None,
    ) -> None:
        """``on_submitted()`` is called whenever a magnet lands at CloudDrive."""
        self._ledger = ledger
        self._sukebei = sukebei
        self._javbus = javbus
        self._cloud = cloud
        self._failed_cooldown = timedelta(seconds=failed_cooldown_seconds)
        self._on_submitted = on_submitted

    async def enqueue(
        self,
        avid: str,
        *,
        source: str,
        task_dir_path: str,
        ctx: RunContext,
        item_magnet: str | None = None,
    ) -> IntakeOutcome:
        """Run the whole intake for one AVID and report how far it got."""
        if not await self._ledger.discover(avid, source=source, now=datetime.now(UTC), task_dir_path=task_dir_path):
            return IntakeOutcome.ALREADY_TRACKED
        try:
            candidates = await self.resolve(avid, ctx=ctx, item_magnet=item_magnet)
        except Exception:  # noqa: BLE001
            ctx.exception('Failed to get magnets for %s', avid)
            candidates = []
        if not candidates:
            ctx.warning('Failed to get any magnet for %s', avid)
            await self.park_unresolved(avid)
            return IntakeOutcome.NO_MAGNET
        if await self.record_and_submit(avid, candidates, task_dir_path, ctx=ctx):
            return IntakeOutcome.SUBMITTED
        return IntakeOutcome.SUBMIT_FAILED

    async def queue(self, avid: str, *, source: str, task_dir_path: str, ctx: RunContext) -> IntakeOutcome:
        """Record one AVID for the background pass instead of resolving inline.

        The row lands in ``discovered`` with its action timer already due, so
        the next tracker pass resolves magnets and submits it — the caller
        returns immediately and progress shows up on the dashboard. This is the
        fill-actor scan's path: a scan queues its whole missing list in one
        burst of inserts rather than serializing magnet lookups.
        """
        now = datetime.now(UTC)
        accepted = await self._ledger.discover(
            avid,
            source=source,
            now=now,
            task_dir_path=task_dir_path,
            next_action_at=now,
        )
        if not accepted:
            return IntakeOutcome.ALREADY_TRACKED
        ctx.add('queued')
        return IntakeOutcome.QUEUED

    # -- cooldown retries -----------------------------------------------------

    async def retry_due(self, *, ctx: RunContext, fallback_task_dir: str | None = None, limit: int = 50) -> int:
        """Re-resolve the acquisitions whose cooldown has expired; count processed."""
        records = await self._ledger.due_for_retry(now=datetime.now(UTC), limit=limit)
        for record in records:
            outcome = await self.retry(record, ctx=ctx, fallback_task_dir=fallback_task_dir)
            ctx.add(f'retry_{outcome.value}')
        if records:
            ctx.set('retry_due', len(records))
            ctx.info('Retried %d cooled-down avids', len(records))
        return len(records)

    async def retry(
        self,
        record: AcquisitionRecord,
        *,
        ctx: RunContext,
        fallback_task_dir: str | None = None,
    ) -> IntakeOutcome:
        """One resolve-and-submit pass for a record picked off the action timer.

        The record is a parked retry or a queued discovery awaiting its first
        resolve. Either way a pass that submits nothing must re-arm the timer
        from the record's current state — leaving the expired ``next_action_at``
        in place would make every subsequent pass pick the same row up again.

        The submission goes to the directory pinned on the record; a record
        without one (created before directories were pinned, or by reconcile)
        falls back to ``fallback_task_dir``.
        """
        task_dir = record.task_dir_path or fallback_task_dir
        if task_dir is None:
            ctx.warning('No offline directory to retry %s in', record.avid)
            await self._repark(record, note='no offline directory configured')
            return IntakeOutcome.SUBMIT_FAILED
        try:
            candidates = await self.resolve(record.avid, ctx=ctx)
        except Exception:  # noqa: BLE001
            ctx.exception('Failed to get magnets for %s', record.avid)
            candidates = []
        outcome = IntakeOutcome.NO_MAGNET
        if candidates:
            if await self.record_and_submit(record.avid, candidates, task_dir, ctx=ctx):
                return IntakeOutcome.SUBMITTED
            outcome = IntakeOutcome.SUBMIT_FAILED
        ctx.warning('Retry submitted nothing for %s', record.avid)
        await self._repark(
            record,
            note='no magnet found' if outcome is IntakeOutcome.NO_MAGNET else 'no magnet accepted',
        )
        return outcome

    async def _repark(self, record: AcquisitionRecord, *, note: str) -> None:
        now = datetime.now(UTC)
        await self._ledger.transition(
            record.avid,
            expected=record.state,
            target=AcquisitionState.RESOLVE_FAILED,
            now=now,
            note=note,
            next_action_at=now + self._failed_cooldown,
        )

    # -- magnet resolution ---------------------------------------------------

    async def resolve(
        self,
        avid: str,
        *,
        ctx: RunContext,
        item_magnet: str | None = None,
    ) -> list[MagnetCandidate]:
        """Every magnet worth trying for one AVID, best first.

        Sukebei leads because most items are recent releases it indexes well; the
        source's own magnet (an RSS item's table) and javbus fill in behind it.
        Only tracked hashes are kept: a magnet CloudDrive cannot report back on is
        one the tracker could never conclude.
        """
        candidates: list[MagnetCandidate] = []
        seen: set[str] = set()

        def collect(magnet: str | None, source: str, size_hint: int | None = None) -> None:
            if not magnet or not magnet.lower().startswith('magnet:'):
                return
            info_hash = extract_info_hash(magnet)
            if info_hash is None:
                ctx.warning('Skipping magnet without a usable info hash for %s', avid)
                return
            if info_hash in seen:
                return
            seen.add(info_hash)
            candidates.append(MagnetCandidate(magnet=magnet, info_hash=info_hash, source=source, size_hint=size_hint))

        collect(await self._sukebei.get_magnet(avid), 'sukebei')
        collect(item_magnet, 'rss_item')
        try:
            magnets = await self._javbus.get_magnets(avid)
        except Exception:  # noqa: BLE001
            ctx.exception('Failed to get magnets from javbus for %s', avid)
        else:
            for magnet in sorted(magnets, key=lambda entry: entry['size_int'], reverse=True):
                collect(magnet['magnet'], 'javbus', magnet['size_int'])
        return candidates[:MAX_CANDIDATES]

    async def park_unresolved(self, avid: str) -> None:
        """Park an AVID with no usable magnet; the cooldown gates its next try."""
        now = datetime.now(UTC)
        await self._ledger.transition(
            avid,
            expected=AcquisitionState.DISCOVERED,
            target=AcquisitionState.RESOLVE_FAILED,
            now=now,
            note='no magnet found',
            next_action_at=now + self._failed_cooldown,
        )

    # -- CloudDrive offline tasks ---------------------------------------------

    async def record_and_submit(
        self,
        avid: str,
        candidates: Sequence[MagnetCandidate],
        task_dir: str,
        *,
        ctx: RunContext,
    ) -> bool:
        """Record the candidates and submit the first; False when none went in."""
        added = await self._ledger.add_attempts(avid, candidates, now=datetime.now(UTC))
        ctx.add('candidates_recorded', added)
        return await self._submit_next(avid, task_dir, ctx)

    async def _submit_next(self, avid: str, task_dir: str, ctx: RunContext) -> bool:
        """Submit this AVID's next untried magnet; False when none is left."""
        now = datetime.now(UTC)
        attempt = await self._ledger.claim_next_pending(avid, now=now)
        if attempt is None:
            return False
        outcome = await self._add_magnet(avid, attempt.magnet, task_dir, ctx)
        if outcome == 'failed':
            await self._ledger.transition_attempt(
                avid,
                attempt.attempt_no,
                expected=AttemptState.SUBMITTED,
                target=AttemptState.ERROR,
                now=now,
                error='failed to add the offline task',
            )
            return await self._submit_next(avid, task_dir, ctx)
        if self._on_submitted is not None:
            self._on_submitted()
        record = await self._ledger.get(avid)
        if record is not None and record.state is not AcquisitionState.DOWNLOADING:
            await self._ledger.transition(
                avid,
                expected=record.state,
                target=AcquisitionState.DOWNLOADING,
                now=now,
            )
        return True

    async def _add_magnet(self, avid: str, link: str, task_dir: str, ctx: RunContext) -> str:
        try:
            result = await self._add_offline_with_retry(link, task_dir)
        except grpc.RpcError as exc:
            if '任务已存在' in (exc.details() or ''):
                # Already queued at CloudDrive: the hash is what we track, so this
                # attempt is live either way.
                ctx.warning('Duplicate magnet for %s', avid)
                ctx.add('duplicates')
                return 'duplicate'
            ctx.exception('Failed to add magnet for %s: %s', avid, exc.details())
            ctx.add('add_failed')
            return 'failed'
        except Exception:  # noqa: BLE001
            ctx.exception('Failed to add magnet for %s', avid)
            ctx.add('add_failed')
            return 'failed'
        if getattr(result, 'success', False):
            ctx.info('Added magnet for %s', avid)
            ctx.add('magnets_added')
            return 'success'
        ctx.error('Failed to add magnet for %s: %s', avid, result)
        ctx.add('add_failed')
        return 'failed'

    @CLOUD_RETRY
    async def _add_offline_with_retry(self, link: str, task_dir: str) -> object:
        return await self._cloud.add_offline_files([link], task_dir)
