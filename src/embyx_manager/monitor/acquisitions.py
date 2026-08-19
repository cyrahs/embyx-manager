"""The acquisition ledger: one row per AVID, from discovery to archived.

The ledger replaces the download folder as the pipeline's memory. An AVID is
discovered (RSS, manual, or reconcile), gets a list of magnet candidates, and
submits them one at a time as CloudDrive offline tasks; the tracker joins the
offline task list to the attempts on ``info_hash`` and drives each attempt to a
conclusion, falling through to the next candidate whenever one fails, stalls, or
turns out to hold no usable video.

Every state change is a compare-and-set: the expected state is part of the WHERE
clause, so two replicas racing over the same row cannot both win, and no
in-process lock is needed. A CAS that matches nothing returns False (someone
else moved the row); an edge that is not in the state machine at all raises,
because that is a programming error rather than a race.
"""

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

import asyncpg

from embyx_manager.db import Database


class AcquisitionState(StrEnum):
    DISCOVERED = 'discovered'
    DOWNLOADING = 'downloading'
    ARCHIVED = 'archived'
    RESOLVE_FAILED = 'resolve_failed'
    EXHAUSTED = 'exhausted'
    NEEDS_ATTENTION = 'needs_attention'
    IGNORED = 'ignored'


class AcquisitionSource(StrEnum):
    MANUAL = 'manual'
    RECONCILE = 'reconcile'
    FILL_ACTOR = 'fill_actor'


#: RSS sources carry the category they came from, so the set is open-ended and
#: cannot be an enum. Rows written before categories existed hold the two fixed
#: values 'rss_actor' and 'rss_rank', which stay readable as plain strings.
RSS_SOURCE_PREFIX = 'rss:'


def rss_source(label: str) -> str:
    """The ledger source for items ingested from one FreshRSS category."""
    return f'{RSS_SOURCE_PREFIX}{label}'


class AttemptState(StrEnum):
    PENDING = 'pending'
    SUBMITTED = 'submitted'
    DOWNLOADING = 'downloading'
    FINISHED = 'finished'
    ARCHIVING = 'archiving'
    ARCHIVED = 'archived'
    JUNK = 'junk'
    ERROR = 'error'
    STALLED = 'stalled'
    LOST = 'lost'


#: States in which the tracker owns the AVID and nothing else may touch its folder.
ACTIVE_STATES = frozenset({AcquisitionState.DISCOVERED, AcquisitionState.DOWNLOADING})
#: States a retry timer can lift.
RETRYABLE_STATES = frozenset({AcquisitionState.RESOLVE_FAILED, AcquisitionState.EXHAUSTED})
#: States the background resolve pass picks up when their ``next_action_at``
#: arrives: cooled-down retries, plus discovered rows a source queued for a
#: first resolve instead of resolving inline (fill-actor scans).
DUE_STATES = RETRYABLE_STATES | {AcquisitionState.DISCOVERED}
#: Attempt states that are still in flight at CloudDrive.
IN_FLIGHT_ATTEMPT_STATES = frozenset({AttemptState.SUBMITTED, AttemptState.DOWNLOADING})
#: Attempt states an offline task can still be acted on in; a hash is unique across these.
LIVE_ATTEMPT_STATES = IN_FLIGHT_ATTEMPT_STATES | {AttemptState.FINISHED, AttemptState.ARCHIVING}

_ACQUISITION_EDGES: dict[AcquisitionState, frozenset[AcquisitionState]] = {
    AcquisitionState.DISCOVERED: frozenset(
        {
            AcquisitionState.DOWNLOADING,
            AcquisitionState.RESOLVE_FAILED,
            AcquisitionState.NEEDS_ATTENTION,
            AcquisitionState.ARCHIVED,
            AcquisitionState.IGNORED,
        }
    ),
    AcquisitionState.DOWNLOADING: frozenset(
        {
            AcquisitionState.DOWNLOADING,  # next candidate submitted for the same AVID
            AcquisitionState.ARCHIVED,
            AcquisitionState.EXHAUSTED,
            AcquisitionState.NEEDS_ATTENTION,
            AcquisitionState.IGNORED,
        }
    ),
    AcquisitionState.RESOLVE_FAILED: frozenset(
        {
            AcquisitionState.DOWNLOADING,
            AcquisitionState.RESOLVE_FAILED,  # another resolve pass came up empty
            AcquisitionState.EXHAUSTED,
            AcquisitionState.NEEDS_ATTENTION,
            AcquisitionState.ARCHIVED,
            AcquisitionState.IGNORED,
        }
    ),
    AcquisitionState.EXHAUSTED: frozenset(
        {
            AcquisitionState.DOWNLOADING,
            AcquisitionState.RESOLVE_FAILED,
            AcquisitionState.NEEDS_ATTENTION,
            AcquisitionState.ARCHIVED,
            AcquisitionState.IGNORED,
        }
    ),
    AcquisitionState.NEEDS_ATTENTION: frozenset(
        {
            AcquisitionState.DOWNLOADING,  # operator resolved it; the tracker resumes
            AcquisitionState.ARCHIVED,
            AcquisitionState.EXHAUSTED,
            AcquisitionState.IGNORED,
        }
    ),
    # Archived is final: the video is in the library and the folder is gone.
    AcquisitionState.ARCHIVED: frozenset(),
    # Ignored is an operator decision, reversible only by re-discovering the AVID.
    AcquisitionState.IGNORED: frozenset({AcquisitionState.DISCOVERED}),
}

_ATTEMPT_EDGES: dict[AttemptState, frozenset[AttemptState]] = {
    AttemptState.PENDING: frozenset({AttemptState.SUBMITTED, AttemptState.ERROR}),
    AttemptState.SUBMITTED: frozenset(
        {
            AttemptState.DOWNLOADING,
            AttemptState.FINISHED,
            AttemptState.ERROR,
            AttemptState.STALLED,
            AttemptState.LOST,
        }
    ),
    AttemptState.DOWNLOADING: frozenset(
        {
            AttemptState.DOWNLOADING,  # progress refresh
            AttemptState.FINISHED,
            AttemptState.ERROR,
            AttemptState.STALLED,
            AttemptState.LOST,
        }
    ),
    AttemptState.FINISHED: frozenset({AttemptState.ARCHIVING, AttemptState.LOST}),
    # Archiving is a claim: it is released back to finished when the archive run
    # cannot conclude (transient failure, or an outcome needing an operator).
    AttemptState.ARCHIVING: frozenset({AttemptState.ARCHIVED, AttemptState.JUNK, AttemptState.FINISHED}),
    AttemptState.ARCHIVED: frozenset(),
    AttemptState.JUNK: frozenset(),
    AttemptState.ERROR: frozenset(),
    AttemptState.STALLED: frozenset(),
    AttemptState.LOST: frozenset(),
}


class IllegalTransitionError(RuntimeError):
    """A transition that is not an edge of the state machine was requested."""

    def __init__(self, source: StrEnum, target: StrEnum) -> None:
        kind = type(source).__name__.removesuffix('State').lower()
        super().__init__(f'{kind}: {source.value} -> {target.value} is not a legal transition')


@dataclass(frozen=True)
class MagnetCandidate:
    """One magnet worth trying for an AVID, before it becomes an attempt row."""

    magnet: str
    info_hash: str | None
    source: str
    size_hint: int | None = None


@dataclass(frozen=True)
class AcquisitionRecord:
    avid: str
    state: AcquisitionState
    source: str
    note: str | None
    archived_paths: tuple[str, ...]
    next_action_at: datetime | None
    created_at: datetime
    updated_at: datetime
    #: The offline directory this AVID's magnets go to; None means the default.
    task_dir_path: str | None = None


@dataclass(frozen=True)
class MagnetAttemptRecord:
    avid: str
    attempt_no: int
    magnet: str
    info_hash: str | None
    magnet_source: str
    size_hint: int | None
    state: AttemptState
    progress: float | None
    error: str | None
    submitted_at: datetime | None
    updated_at: datetime


class AcquisitionRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    # -- discovery ---------------------------------------------------------

    async def discover(
        self,
        avid: str,
        *,
        source: str,
        now: datetime,
        task_dir_path: str | None = None,
        next_action_at: datetime | None = None,
    ) -> bool:
        """Record an AVID sighting; return whether it still needs magnets resolved.

        False means someone already owns this AVID: it is downloading, archived,
        ignored, waiting on an operator, or cooling down after a failed resolve.

        ``task_dir_path`` pins the offline directory this AVID's magnets go to;
        None leaves it on the default directory of the moment. ``next_action_at``
        hands the resolve to the background pass at that time instead of the
        caller doing it inline; on an accepted re-discovery it re-arms the timer
        the same way.
        """
        pool = await self._database.get_pool()
        inserted = await pool.fetchval(
            """
            INSERT INTO archive_acquisitions
                (avid, state, source, task_dir_path, next_action_at, created_at, updated_at)
            VALUES ($1, 'discovered', $2, $3, $4, $5, $5)
            ON CONFLICT (avid) DO NOTHING
            RETURNING avid
            """,
            avid,
            str(source),
            task_dir_path,
            next_action_at,
            now,
        )
        if inserted is not None:
            return True
        existing = await self.get(avid)
        if existing is None:  # deleted between the insert and the read
            return True
        if existing.state is AcquisitionState.DISCOVERED:
            accepted = True
        elif existing.state in RETRYABLE_STATES:
            accepted = existing.next_action_at is None or existing.next_action_at <= now
        else:
            return False
        if accepted and task_dir_path is not None and existing.task_dir_path != task_dir_path:
            # The retry follows whichever category just re-discovered it, so
            # repointing a category takes effect from its next pass.
            await pool.execute(
                'UPDATE archive_acquisitions SET task_dir_path = $2 WHERE avid = $1',
                avid,
                task_dir_path,
            )
        if accepted and next_action_at is not None:
            # A queueing source accepted an orphaned or cooled-down row: give it
            # a timer or the background pass would never look at it again.
            await pool.execute(
                'UPDATE archive_acquisitions SET next_action_at = $2 WHERE avid = $1',
                avid,
                next_action_at,
            )
        return accepted

    async def get(self, avid: str) -> AcquisitionRecord | None:
        pool = await self._database.get_pool()
        row = await pool.fetchrow('SELECT * FROM archive_acquisitions WHERE avid = $1', avid)
        return _acquisition_from_row(row) if row is not None else None

    # -- acquisition transitions -------------------------------------------

    async def transition(  # noqa: PLR0913 - one CAS entry point for every ledger move
        self,
        avid: str,
        *,
        expected: AcquisitionState,
        target: AcquisitionState,
        now: datetime,
        note: str | None = None,
        next_action_at: datetime | None = None,
        archived_paths: Sequence[str] | None = None,
    ) -> bool:
        """Compare-and-set one acquisition's state; False when the row moved on.

        ``note``/``next_action_at``/``archived_paths`` are written together with
        the state, so a row is never observed in the new state without them.
        """
        if target not in _ACQUISITION_EDGES[expected]:
            raise IllegalTransitionError(expected, target)
        pool = await self._database.get_pool()
        status = await pool.execute(
            """
            UPDATE archive_acquisitions
            SET state = $3, note = $4, next_action_at = $5,
                archived_paths_json = COALESCE($6, archived_paths_json), updated_at = $7
            WHERE avid = $1 AND state = $2
            """,
            avid,
            expected.value,
            target.value,
            note,
            next_action_at,
            None if archived_paths is None else json.dumps(list(archived_paths)),
            now,
        )
        return _row_count(status) == 1

    async def due_for_retry(self, *, now: datetime, limit: int = 50) -> tuple[AcquisitionRecord, ...]:
        """Acquisitions whose action timer has expired, oldest deadline first.

        Covers cooled-down retries and queued discoveries alike; discovered
        rows without a timer belong to a source that resolves inline and are
        left alone.
        """
        pool = await self._database.get_pool()
        rows = await pool.fetch(
            """
            SELECT * FROM archive_acquisitions
            WHERE state = ANY($1::text[]) AND next_action_at IS NOT NULL AND next_action_at <= $2
            ORDER BY next_action_at, avid
            LIMIT $3
            """,
            sorted(state.value for state in DUE_STATES),
            now,
            limit,
        )
        return tuple(_acquisition_from_row(row) for row in rows)

    async def active_avids(self) -> frozenset[str]:
        """AVIDs the tracker currently owns; reconcile must leave their folders alone."""
        pool = await self._database.get_pool()
        rows = await pool.fetch(
            'SELECT avid FROM archive_acquisitions WHERE state = ANY($1::text[])',
            sorted(state.value for state in ACTIVE_STATES),
        )
        return frozenset(row['avid'] for row in rows)

    async def active_task_dirs(self) -> tuple[str, ...]:
        """Offline directories holding an acquisition that is still in flight.

        The tracker polls the configured directories, but a source may pin a
        directory that is not one of them — the manual source lets an operator
        pick any routed directory. Adding these to the poll set is what keeps
        such a download visible: an unpolled task is missing from every listing,
        which the sweep reads as CloudDrive having dropped it. A directory
        leaves the set once nothing is in flight there.
        """
        pool = await self._database.get_pool()
        rows = await pool.fetch(
            """
            SELECT DISTINCT task_dir_path FROM archive_acquisitions
            WHERE state = ANY($1::text[]) AND task_dir_path IS NOT NULL
            ORDER BY task_dir_path
            """,
            sorted(state.value for state in ACTIVE_STATES),
        )
        return tuple(row['task_dir_path'] for row in rows)

    async def latest_task_dir(self, *, source: str) -> str | None:
        """The offline directory this source's newest acquisition was queued under.

        The manual source has no configured directory of its own, so its default
        is whichever one the operator used last. Reading it back from the ledger
        keeps that memory in step with what actually happened, and shared by
        every browser signed into the deployment.
        """
        pool = await self._database.get_pool()
        return await pool.fetchval(
            """
            SELECT task_dir_path FROM archive_acquisitions
            WHERE source = $1 AND task_dir_path IS NOT NULL
            ORDER BY created_at DESC, avid DESC
            LIMIT 1
            """,
            source,
        )

    # -- attempts ------------------------------------------------------------

    async def add_attempts(self, avid: str, candidates: Sequence[MagnetCandidate], *, now: datetime) -> int:
        """Append candidates as pending attempts, skipping hashes already tried.

        Numbering continues from the highest existing attempt, so the history of
        what was tried for an AVID is never rewritten. The parent acquisition row
        is locked first: locking the attempts themselves would protect nothing on
        the first call, when there are no attempt rows to lock yet.
        """
        if not candidates:
            return 0
        pool = await self._database.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute('SELECT 1 FROM archive_acquisitions WHERE avid = $1 FOR UPDATE', avid)
            rows = await connection.fetch(
                'SELECT attempt_no, info_hash FROM archive_magnet_attempts WHERE avid = $1',
                avid,
            )
            next_no = max((row['attempt_no'] for row in rows), default=0) + 1
            seen = {row['info_hash'] for row in rows if row['info_hash'] is not None}
            pending: list[tuple[Any, ...]] = []
            for candidate in candidates:
                if candidate.info_hash is not None and candidate.info_hash in seen:
                    continue
                if candidate.info_hash is not None:
                    seen.add(candidate.info_hash)
                pending.append(
                    (
                        avid,
                        next_no + len(pending),
                        candidate.magnet,
                        candidate.info_hash,
                        candidate.source,
                        candidate.size_hint,
                        now,
                    )
                )
            if not pending:
                return 0
            await connection.executemany(
                """
                INSERT INTO archive_magnet_attempts (
                    avid, attempt_no, magnet, info_hash, magnet_source, size_hint, state, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7)
                """,
                pending,
            )
            return len(pending)

    async def claim_next_pending(self, avid: str, *, now: datetime) -> MagnetAttemptRecord | None:
        """Atomically take the lowest pending attempt and mark it submitted.

        The claim happens before the CloudDrive call, so a crash mid-submit leaves
        the attempt submitted and the tracker reconciles it against the task list
        rather than double-submitting it.
        """
        pool = await self._database.get_pool()
        row = await pool.fetchrow(
            """
            UPDATE archive_magnet_attempts SET state = 'submitted', submitted_at = $2, updated_at = $2
            WHERE avid = $1 AND state = 'pending' AND attempt_no = (
                SELECT attempt_no FROM archive_magnet_attempts
                WHERE avid = $1 AND state = 'pending'
                ORDER BY attempt_no
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            """,
            avid,
            now,
        )
        return _attempt_from_row(row) if row is not None else None

    async def claim_attempt(self, avid: str, attempt_no: int, *, now: datetime) -> MagnetAttemptRecord | None:
        """Take one specific pending attempt, for a magnet an operator chose."""
        pool = await self._database.get_pool()
        row = await pool.fetchrow(
            """
            UPDATE archive_magnet_attempts SET state = 'submitted', submitted_at = $3, updated_at = $3
            WHERE avid = $1 AND attempt_no = $2 AND state = 'pending'
            RETURNING *
            """,
            avid,
            attempt_no,
            now,
        )
        return _attempt_from_row(row) if row is not None else None

    async def transition_attempt(  # noqa: PLR0913 - mirrors the acquisition CAS
        self,
        avid: str,
        attempt_no: int,
        *,
        expected: AttemptState,
        target: AttemptState,
        now: datetime,
        error: str | None = None,
    ) -> bool:
        if target not in _ATTEMPT_EDGES[expected]:
            raise IllegalTransitionError(expected, target)
        pool = await self._database.get_pool()
        status = await pool.execute(
            """
            UPDATE archive_magnet_attempts SET state = $4, error = $5, updated_at = $6
            WHERE avid = $1 AND attempt_no = $2 AND state = $3
            """,
            avid,
            attempt_no,
            expected.value,
            target.value,
            error,
            now,
        )
        return _row_count(status) == 1

    async def record_progress(
        self,
        avid: str,
        attempt_no: int,
        *,
        state: AttemptState,
        progress: float | None,
        now: datetime,
    ) -> bool:
        """Refresh an in-flight attempt, writing only when something actually moved.

        Leaving ``updated_at`` alone on an unchanged poll is what makes stall
        detection possible: for an in-flight attempt it means "last time this
        download made progress". Conclusions (finished, error, lost) go through
        :meth:`transition_attempt`, which always writes.
        """
        if state not in IN_FLIGHT_ATTEMPT_STATES:
            msg = f'record_progress only refreshes in-flight attempts, got {state.value}'
            raise ValueError(msg)
        pool = await self._database.get_pool()
        status = await pool.execute(
            """
            UPDATE archive_magnet_attempts SET state = $3, progress = $4, updated_at = $5
            WHERE avid = $1 AND attempt_no = $2
              AND state IN ('submitted', 'downloading')
              AND (state IS DISTINCT FROM $3 OR progress IS DISTINCT FROM $4)
            """,
            avid,
            attempt_no,
            state.value,
            progress,
            now,
        )
        return _row_count(status) == 1

    async def attempts_by_info_hash(self, info_hashes: Iterable[str]) -> dict[str, MagnetAttemptRecord]:
        """Look up live attempts by CloudDrive infoHash; the tracker's join key.

        Only live attempts are returned, and a unique index keeps at most one of
        those per hash. A task whose attempt already concluded simply drops out of
        the mapping, which is how the tracker knows to leave it alone.
        """
        hashes = sorted({info_hash for info_hash in info_hashes if info_hash})
        if not hashes:
            return {}
        pool = await self._database.get_pool()
        rows = await pool.fetch(
            """
            SELECT * FROM archive_magnet_attempts
            WHERE info_hash = ANY($1::text[]) AND state = ANY($2::text[])
            """,
            hashes,
            sorted(state.value for state in LIVE_ATTEMPT_STATES),
        )
        return {row['info_hash']: _attempt_from_row(row) for row in rows}

    async def in_flight_attempts(self) -> tuple[MagnetAttemptRecord, ...]:
        """Attempts CloudDrive should still be working on, oldest update first."""
        pool = await self._database.get_pool()
        rows = await pool.fetch(
            """
            SELECT * FROM archive_magnet_attempts
            WHERE state = ANY($1::text[])
            ORDER BY updated_at, avid, attempt_no
            """,
            sorted(state.value for state in IN_FLIGHT_ATTEMPT_STATES),
        )
        return tuple(_attempt_from_row(row) for row in rows)

    async def attempts_for(self, avid: str) -> tuple[MagnetAttemptRecord, ...]:
        pool = await self._database.get_pool()
        rows = await pool.fetch(
            'SELECT * FROM archive_magnet_attempts WHERE avid = $1 ORDER BY attempt_no',
            avid,
        )
        return tuple(_attempt_from_row(row) for row in rows)

    # -- listing -------------------------------------------------------------

    async def list_acquisitions(
        self,
        *,
        states: Sequence[AcquisitionState] | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[AcquisitionRecord, ...]:
        pool = await self._database.get_pool()
        if states is None:
            rows = await pool.fetch(
                'SELECT * FROM archive_acquisitions ORDER BY updated_at DESC, avid LIMIT $1 OFFSET $2',
                limit,
                offset,
            )
        else:
            rows = await pool.fetch(
                """
                SELECT * FROM archive_acquisitions WHERE state = ANY($1::text[])
                ORDER BY updated_at DESC, avid LIMIT $2 OFFSET $3
                """,
                sorted({state.value for state in states}),
                limit,
                offset,
            )
        return tuple(_acquisition_from_row(row) for row in rows)

    async def count_by_state(self) -> dict[AcquisitionState, int]:
        pool = await self._database.get_pool()
        rows = await pool.fetch('SELECT state, COUNT(*) AS total FROM archive_acquisitions GROUP BY state')
        return {AcquisitionState(row['state']): int(row['total']) for row in rows}


def _row_count(status: str) -> int:
    return int(status.rsplit(' ', 1)[-1])


def _acquisition_from_row(row: asyncpg.Record) -> AcquisitionRecord:
    return AcquisitionRecord(
        avid=row['avid'],
        state=AcquisitionState(row['state']),
        source=row['source'],
        note=row['note'],
        archived_paths=tuple(json.loads(row['archived_paths_json'])),
        next_action_at=row['next_action_at'],
        created_at=row['created_at'],
        updated_at=row['updated_at'],
        task_dir_path=row['task_dir_path'],
    )


def _attempt_from_row(row: asyncpg.Record) -> MagnetAttemptRecord:
    return MagnetAttemptRecord(
        avid=row['avid'],
        attempt_no=row['attempt_no'],
        magnet=row['magnet'],
        info_hash=row['info_hash'],
        magnet_source=row['magnet_source'],
        size_hint=row['size_hint'],
        state=AttemptState(row['state']),
        progress=row['progress'],
        error=row['error'],
        submitted_at=row['submitted_at'],
        updated_at=row['updated_at'],
    )
