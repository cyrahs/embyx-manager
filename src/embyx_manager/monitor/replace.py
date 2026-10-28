"""Replacing a title the library holds with another copy of it (换版).

A title can be filed with the wrong video — a release mislabelled upstream, or a
cut not worth keeping — and the ledger then calls it done. A replacement hands
it back to the acquisition pipeline: the row returns to ``discovered`` with the
library paths of the copy it holds, a fresh magnet goes to CloudDrive, and the
tracker deletes the old copy only once the new download is about to be filed.
Until then the title stays watchable, and a replacement that never finds a
working magnet leaves it exactly as it was.

The magnet is the operator's when they paste one; otherwise the same
resolvers as every other source are asked. Hashes already on the row are never
tried again, so the download that produced the wrong copy cannot come back.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import PurePosixPath

from embyx_manager.core.avid import AvidParser
from embyx_manager.core.magnet import extract_info_hash
from embyx_manager.monitor.acquisitions import (
    REPLACEABLE_STATES,
    AcquisitionRecord,
    AcquisitionRepository,
    AcquisitionSource,
    AcquisitionState,
    AttemptState,
    MagnetCandidate,
    MagnetInUseError,
)
from embyx_manager.monitor.archive import ArchivePipeline
from embyx_manager.monitor.intake import AcquisitionIntake
from embyx_manager.monitor.reports import RunContext

LOGGER = logging.getLogger('embyx-manager.replace')


class ReplaceOutcome(StrEnum):
    """How far a replacement got before the request returned."""

    #: A magnet is at CloudDrive; the tracker takes it from here.
    SUBMITTED = 'submitted'
    #: No magnet worth trying: the resolve schedule looks again, or one can be pasted.
    NO_MAGNET = 'no_magnet'
    #: CloudDrive refused the magnet; the row waits for another like NO_MAGNET.
    SUBMIT_FAILED = 'submit_failed'


class ReplaceError(Exception):
    """A replacement that cannot be started, with the reason as its API code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class ReplacePlan:
    """What a replacement of one title would do, for the operator to confirm."""

    avid: str
    #: Library paths of the copy that would be deleted once the new one is in.
    holdings: tuple[str, ...]
    state: AcquisitionState | None
    #: The offline directory the new download would be queued under.
    task_dir_path: str | None
    #: Why it cannot start now (an error code), or None when it can.
    blocked: str | None


@dataclass(frozen=True)
class ReplaceResult:
    plan: ReplacePlan
    outcome: ReplaceOutcome
    record: AcquisitionRecord


@dataclass(frozen=True)
class ReplacementSource:
    """Everything a replacement needs, resolved from live configuration.

    ``submit_magnet(avid, magnet)`` is the dashboard's own submit, which queues
    under the directory pinned on the row; ``configured_dirs`` are the input
    sources' offline directories, the fallback when the row has none.
    """

    ledger: AcquisitionRepository
    intake_factory: Callable[[], AcquisitionIntake | None]
    archiver_factory: Callable[[], ArchivePipeline]
    parser_factory: Callable[[], AvidParser]
    configured_dirs: Callable[[], tuple[str, ...]]
    submit_magnet: Callable[[str, str], Awaitable[bool]]
    logger: logging.Logger = field(default=LOGGER)

    async def plan(self, text: str) -> ReplacePlan:
        """Read the AVID from ``text`` and say what replacing it would involve."""
        avid = self.parser_factory().get_avid(text.strip())
        if not avid:
            code = 'unreadable_avid'
            raise ReplaceError(code)
        archiver = self.archiver_factory()
        ctx = RunContext(logger=self.logger)
        # No task directory is passed: that would promote copies between
        # routes, and a preview must not move anything.
        holdings = await asyncio.to_thread(archiver.library_holdings, avid, ctx)
        record = await self.ledger.get(avid)
        state = record.state if record is not None else None
        task_dir = self._task_dir_for(record, holdings, archiver)
        blocked = None
        if not holdings:
            blocked = 'not_in_library'
        elif state is not None and state not in REPLACEABLE_STATES:
            blocked = 'acquisition_active'
        elif task_dir is None:
            blocked = 'no_task_dir'
        elif self.intake_factory() is None:
            blocked = 'clouddrive_not_configured'
        return ReplacePlan(avid=avid, holdings=holdings, state=state, task_dir_path=task_dir, blocked=blocked)

    async def start(self, text: str, *, magnet: str | None = None) -> ReplaceResult:
        """Begin replacing one title; the old copy stays until the new one is filed."""
        plan = await self.plan(text)
        if plan.blocked is not None:
            raise ReplaceError(plan.blocked)
        intake = self.intake_factory()
        if intake is None or plan.task_dir_path is None:
            code = 'clouddrive_not_configured'
            raise ReplaceError(code)
        candidate = await self._operator_candidate(plan.avid, magnet)

        now = datetime.now(UTC)
        record = await self.ledger.begin_replacement(
            plan.avid,
            source=AcquisitionSource.MANUAL,
            task_dir_path=plan.task_dir_path,
            replace_paths=plan.holdings,
            now=now,
        )
        if record is None:
            code = 'acquisition_changed'
            raise ReplaceError(code)
        ctx = RunContext(logger=self.logger)
        ctx.info('replacing %s, now at %s', plan.avid, ', '.join(plan.holdings))
        if candidate is not None:
            outcome = await self._submit_operator_magnet(plan.avid, candidate)
        else:
            outcome = await self._submit_resolved(plan.avid, plan.task_dir_path, intake, ctx)
        if outcome is not ReplaceOutcome.SUBMITTED:
            # Parked like any acquisition without a magnet: the resolve schedule
            # tries again, and the dashboard takes a pasted one at any time.
            await intake.park_unresolved(plan.avid)
        current = await self.ledger.get(plan.avid)
        return ReplaceResult(plan=plan, outcome=outcome, record=current or record)

    async def _operator_candidate(self, avid: str, magnet: str | None) -> MagnetCandidate | None:
        """The pasted magnet as a candidate, refused before anything is written."""
        if magnet is None or not magnet.strip():
            return None
        magnet = magnet.strip()
        info_hash = extract_info_hash(magnet)
        if not magnet.lower().startswith('magnet:') or info_hash is None:
            code = 'unusable_magnet'
            raise ReplaceError(code)
        tried = {attempt.info_hash for attempt in await self.ledger.attempts_for(avid)}
        if info_hash in tried:
            code = 'magnet_already_tried'
            raise ReplaceError(code)
        return MagnetCandidate(magnet=magnet, info_hash=info_hash, source='manual')

    async def _submit_operator_magnet(self, avid: str, candidate: MagnetCandidate) -> ReplaceOutcome:
        """Submit exactly the pasted magnet, ahead of any older candidate still pending."""
        now = datetime.now(UTC)
        if not await self.ledger.add_attempts(avid, [candidate], now=now):
            return ReplaceOutcome.SUBMIT_FAILED
        attempt_no = (await self.ledger.attempts_for(avid))[-1].attempt_no
        try:
            attempt = await self.ledger.claim_attempt(avid, attempt_no, now=now)
        except MagnetInUseError as exc:
            code = 'magnet_in_use'
            raise ReplaceError(code) from exc
        if attempt is None:
            return ReplaceOutcome.SUBMIT_FAILED
        if not await self.submit_magnet(avid, attempt.magnet):
            await self.ledger.transition_attempt(
                avid,
                attempt.attempt_no,
                expected=AttemptState.SUBMITTED,
                target=AttemptState.ERROR,
                now=datetime.now(UTC),
                error='failed to add the offline task',
            )
            return ReplaceOutcome.SUBMIT_FAILED
        await self.ledger.transition(
            avid,
            expected=AcquisitionState.DISCOVERED,
            target=AcquisitionState.DOWNLOADING,
            now=datetime.now(UTC),
        )
        return ReplaceOutcome.SUBMITTED

    async def _submit_resolved(
        self,
        avid: str,
        task_dir: str,
        intake: AcquisitionIntake,
        ctx: RunContext,
    ) -> ReplaceOutcome:
        """Ask the resolvers for fresh candidates and submit the first untried one."""
        try:
            candidates = await intake.resolve(avid, ctx=ctx)
        except Exception:  # noqa: BLE001
            ctx.exception('Failed to get magnets for %s', avid)
            candidates = []
        # Even with nothing new, a candidate collected earlier and never tried
        # may still be pending on the row; record_and_submit takes that too.
        if await intake.record_and_submit(avid, candidates, task_dir, ctx=ctx):
            return ReplaceOutcome.SUBMITTED
        return ReplaceOutcome.NO_MAGNET if not candidates else ReplaceOutcome.SUBMIT_FAILED

    def _task_dir_for(
        self,
        record: AcquisitionRecord | None,
        holdings: tuple[str, ...],
        archiver: ArchivePipeline,
    ) -> str | None:
        """Where the new download is queued: beside the old one when that is known.

        The row's own directory comes first. Without one (a title filed before
        the ledger, or by the reconcile scan), a source directory whose route
        feeds the library directory the old copy sits in keeps the title in
        the same place; failing that, any routed source directory will do.
        """
        if record is not None and record.task_dir_path and archiver.route_for_task_dir(record.task_dir_path):
            return record.task_dir_path
        # A library path is {route destination}/{brand}/{file}.
        held_in = {PurePosixPath(path).parent.parent for path in holdings}
        routed = [
            (directory, route)
            for directory in self.configured_dirs()
            if (route := archiver.route_for_task_dir(directory)) is not None
        ]
        for directory, (dst, _priority) in routed:
            if PurePosixPath(dst) in held_in:
                return directory
        return routed[0][0] if routed else None
