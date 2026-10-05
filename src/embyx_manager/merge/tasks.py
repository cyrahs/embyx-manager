"""Stored merge tasks: one row per title the merge tab was asked to merge."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

import asyncpg

from embyx_manager.db import Database


class MergeState(StrEnum):
    #: Waiting for the merge Job slot; one title merges at a time.
    QUEUED = 'queued'
    #: The Job is concatenating the parts onto the work volume.
    MERGING = 'merging'
    #: CloudDrive is copying the merged file into the 115 staging directory.
    UPLOADING = 'uploading'
    #: Comparing the uploaded file's size and SHA-1 with the merged file's.
    VERIFYING = 'verifying'
    #: Deleting the original parts and moving the merged file into the intake route.
    REPLACING = 'replacing'
    #: Waiting for the archive pipeline to file the merged title.
    ARCHIVING = 'archiving'
    DONE = 'done'
    FAILED = 'failed'
    CANCELLED = 'cancelled'


FINISHED_STATES = frozenset({MergeState.DONE, MergeState.CANCELLED})
#: The states a task can be cancelled in; past them the originals may be gone.
CANCELLABLE_STATES = frozenset(
    {MergeState.QUEUED, MergeState.MERGING, MergeState.UPLOADING, MergeState.VERIFYING, MergeState.FAILED},
)
#: Columns a state handler may change besides ``state``.
_UPDATABLE = frozenset(
    {
        'failed_state',
        'job_name',
        'phase',
        'progress',
        'merged_bytes',
        'merged_sha1',
        'uploaded_bytes',
        'upload_attempts',
        'error',
        'notice',
        'finished_at',
    },
)
RECENT_FINISHED_LIMIT = 20


class MergeTaskConflictError(Exception):
    """The title already has an unfinished merge task."""


@dataclass(frozen=True)
class MergeTask:
    id: int
    avid: str
    source: str
    library_dir: str
    brand: str
    #: Each part's file on the CloudDrive mount, in part order.
    parts: tuple[str, ...]
    #: The merged file's name, e.g. ``ABC-123.mp4``.
    merged_name: str
    state: MergeState
    failed_state: MergeState | None
    job_name: str | None
    phase: str | None
    progress: float | None
    merged_bytes: int | None
    merged_sha1: str | None
    uploaded_bytes: int | None
    upload_attempts: int
    #: Why the task failed.
    error: str | None
    #: A passing problem the task is waiting out, e.g. CloudDrive unreachable.
    notice: str | None
    created_at: datetime
    updated_at: datetime
    state_changed_at: datetime
    finished_at: datetime | None


class MergeTaskRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def create(  # noqa: PLR0913
        self,
        *,
        avid: str,
        source: str,
        library_dir: str,
        brand: str,
        parts: tuple[str, ...],
        merged_name: str,
    ) -> MergeTask:
        pool = await self._database.get_pool()
        now = datetime.now(UTC)
        try:
            row = await pool.fetchrow(
                """
                INSERT INTO merge_tasks (
                    avid, source, library_dir, brand, parts_json, merged_name, state,
                    created_at, updated_at, state_changed_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'queued', $7, $7, $7)
                RETURNING *
                """,
                avid,
                source,
                library_dir,
                brand,
                json.dumps(list(parts), ensure_ascii=False),
                merged_name,
                now,
            )
        except asyncpg.UniqueViolationError as exc:
            raise MergeTaskConflictError(avid) from exc
        assert row is not None  # noqa: S101 - INSERT ... RETURNING yields the row
        return _from_row(row)

    async def get(self, task_id: int) -> MergeTask | None:
        pool = await self._database.get_pool()
        row = await pool.fetchrow('SELECT * FROM merge_tasks WHERE id = $1', task_id)
        return _from_row(row) if row is not None else None

    async def open_tasks(self) -> tuple[MergeTask, ...]:
        """Every task not yet done or cancelled, oldest first."""
        pool = await self._database.get_pool()
        rows = await pool.fetch(
            "SELECT * FROM merge_tasks WHERE state NOT IN ('done', 'cancelled') ORDER BY id",
        )
        return tuple(_from_row(row) for row in rows)

    async def listing(self) -> tuple[MergeTask, ...]:
        """Open tasks plus the most recently finished ones, newest first."""
        pool = await self._database.get_pool()
        rows = await pool.fetch(
            """
            (SELECT * FROM merge_tasks WHERE state NOT IN ('done', 'cancelled'))
            UNION ALL
            (
                SELECT * FROM merge_tasks WHERE state IN ('done', 'cancelled')
                ORDER BY finished_at DESC NULLS LAST, id DESC LIMIT $1
            )
            ORDER BY id DESC
            """,
            RECENT_FINISHED_LIMIT,
        )
        return tuple(_from_row(row) for row in rows)

    async def cancelled_avids(self) -> frozenset[str]:
        """Titles someone cancelled or removed a task for; automatic merging leaves them be."""
        pool = await self._database.get_pool()
        rows = await pool.fetch("SELECT DISTINCT avid FROM merge_tasks WHERE state = 'cancelled'")
        return frozenset(str(row['avid']) for row in rows)

    async def last_done_at(self) -> datetime | None:
        """When the latest task finished merging its title into the library."""
        pool = await self._database.get_pool()
        value = await pool.fetchval("SELECT max(finished_at) FROM merge_tasks WHERE state = 'done'")
        return value if isinstance(value, datetime) else None

    async def update(self, task_id: int, **fields: Any) -> MergeTask | None:
        """Change fields without moving the task to another state."""
        return await self._write(task_id, None, None, fields)

    async def transition(
        self,
        task_id: int,
        expected: MergeState,
        state: MergeState,
        **fields: Any,
    ) -> MergeTask | None:
        """Move a task from ``expected`` to ``state``; None when it was no longer in ``expected``."""
        return await self._write(task_id, expected, state, fields)

    async def _write(
        self,
        task_id: int,
        expected: MergeState | None,
        state: MergeState | None,
        fields: dict[str, Any],
    ) -> MergeTask | None:
        unknown = set(fields) - _UPDATABLE
        if unknown:
            msg = f'not updatable: {sorted(unknown)}'
            raise ValueError(msg)
        now = datetime.now(UTC)
        assignments = ['updated_at = $2']
        values: list[Any] = [task_id, now]
        if state is not None:
            values.append(state.value)
            assignments.append(f'state = ${len(values)}')
            assignments.append('state_changed_at = $2')
        for name, value in fields.items():
            values.append(value)
            assignments.append(f'{name} = ${len(values)}')
        condition = ''
        if expected is not None:
            values.append(expected.value)
            condition = f' AND state = ${len(values)}'
        pool = await self._database.get_pool()
        row = await pool.fetchrow(
            f'UPDATE merge_tasks SET {", ".join(assignments)} WHERE id = $1{condition} RETURNING *',  # noqa: S608 - column names come from _UPDATABLE
            *values,
        )
        return _from_row(row) if row is not None else None


def _from_row(row: asyncpg.Record) -> MergeTask:
    return MergeTask(
        id=row['id'],
        avid=row['avid'],
        source=row['source'],
        library_dir=row['library_dir'],
        brand=row['brand'],
        parts=tuple(json.loads(row['parts_json'])),
        merged_name=row['merged_name'],
        state=MergeState(row['state']),
        failed_state=MergeState(row['failed_state']) if row['failed_state'] else None,
        job_name=row['job_name'],
        phase=row['phase'],
        progress=row['progress'],
        merged_bytes=row['merged_bytes'],
        merged_sha1=row['merged_sha1'],
        uploaded_bytes=row['uploaded_bytes'],
        upload_attempts=row['upload_attempts'],
        error=row['error'],
        notice=row['notice'],
        created_at=row['created_at'],
        updated_at=row['updated_at'],
        state_changed_at=row['state_changed_at'],
        finished_at=row['finished_at'],
    )
