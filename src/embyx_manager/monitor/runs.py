"""Persistence for pipeline run history."""

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from embyx_manager.db import Database
from embyx_manager.monitor.reports import PipelineName, RunState, RunTrigger


@dataclass(frozen=True)
class PipelineRunRecord:
    run_id: str
    pipeline: PipelineName
    trigger: RunTrigger
    state: RunState
    started_at: datetime
    finished_at: datetime | None
    stats: dict[str, int]
    errors: tuple[str, ...]
    log_tail: tuple[str, ...]


class PipelineRunRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def start_run(self, pipeline: PipelineName, trigger: RunTrigger) -> str:
        run_id = uuid.uuid4().hex
        pool = await self._database.get_pool()
        await pool.execute(
            """
            INSERT INTO pipeline_runs (run_id, pipeline, trigger, state, started_at)
            VALUES ($1, $2, $3, 'running', $4)
            """,
            run_id,
            pipeline.value,
            trigger.value,
            datetime.now(UTC),
        )
        return run_id

    async def finish_run(
        self,
        run_id: str,
        *,
        state: RunState,
        stats: dict[str, int],
        errors: tuple[str, ...],
        log_tail: tuple[str, ...],
    ) -> None:
        pool = await self._database.get_pool()
        await pool.execute(
            """
            UPDATE pipeline_runs
            SET state = $1, finished_at = $2, stats_json = $3, errors_json = $4, log_json = $5
            WHERE run_id = $6
            """,
            state.value,
            datetime.now(UTC),
            json.dumps(stats),
            json.dumps(list(errors)),
            json.dumps(list(log_tail)),
            run_id,
        )

    async def get_run(self, run_id: str) -> PipelineRunRecord | None:
        pool = await self._database.get_pool()
        row = await pool.fetchrow('SELECT * FROM pipeline_runs WHERE run_id = $1', run_id)
        return _run_from_row(row) if row is not None else None

    async def list_runs(
        self,
        pipeline: PipelineName | None = None,
        *,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[PipelineRunRecord, ...]:
        pool = await self._database.get_pool()
        if pipeline is None:
            rows = await pool.fetch(
                'SELECT * FROM pipeline_runs ORDER BY started_at DESC, run_id LIMIT $1 OFFSET $2',
                limit,
                offset,
            )
        else:
            rows = await pool.fetch(
                """
                SELECT * FROM pipeline_runs WHERE pipeline = $1
                ORDER BY started_at DESC, run_id LIMIT $2 OFFSET $3
                """,
                pipeline.value,
                limit,
                offset,
            )
        return tuple(_run_from_row(row) for row in rows)

    async def latest_run(self, pipeline: PipelineName) -> PipelineRunRecord | None:
        runs = await self.list_runs(pipeline, limit=1)
        return runs[0] if runs else None

    async def fail_stale_running(self, *, error: str) -> int:
        """Mark runs left in 'running' by a dead process as failed (startup recovery)."""
        pool = await self._database.get_pool()
        status = await pool.execute(
            """
            UPDATE pipeline_runs
            SET state = 'failed', finished_at = $1,
                errors_json = $2
            WHERE state = 'running'
            """,
            datetime.now(UTC),
            json.dumps([error]),
        )
        return int(status.rsplit(' ', 1)[-1])

    async def prune(self, *, keep_per_pipeline: int = 500) -> int:
        pool = await self._database.get_pool()
        status = await pool.execute(
            """
            DELETE FROM pipeline_runs WHERE run_id IN (
                SELECT run_id FROM (
                    SELECT run_id, ROW_NUMBER() OVER (
                        PARTITION BY pipeline ORDER BY started_at DESC
                    ) AS position
                    FROM pipeline_runs
                ) ranked WHERE ranked.position > $1
            )
            """,
            keep_per_pipeline,
        )
        return int(status.rsplit(' ', 1)[-1])


def _run_from_row(row: object) -> PipelineRunRecord:
    return PipelineRunRecord(
        run_id=row['run_id'],  # type: ignore[index]
        pipeline=PipelineName(row['pipeline']),  # type: ignore[index]
        trigger=RunTrigger(row['trigger']),  # type: ignore[index]
        state=RunState(row['state']),  # type: ignore[index]
        started_at=row['started_at'],  # type: ignore[index]
        finished_at=row['finished_at'],  # type: ignore[index]
        stats=json.loads(row['stats_json']),  # type: ignore[index]
        errors=tuple(json.loads(row['errors_json'])),  # type: ignore[index]
        log_tail=tuple(json.loads(row['log_json'])),  # type: ignore[index]
    )
