"""The acquisition intake against the ledger in PostgreSQL and the fake CloudDrive (WP5).

`intake.steps` builds `AcquisitionIntake` over the baseline's `AcquisitionRepository`, the
baseline's `AsyncCloudDrive` (talking to the fake) and stub magnet sources: `args.sukebei`
maps an AVID to its magnet, `args.javbus` maps an AVID to a list of `[magnet, size]` or to an
HTTP status the stub raises. It then runs `args.steps` in order (`enqueue`, `queue`,
`retry_due`) and stores the run's counters, errors and log tail with
`PipelineRunRepository.finish_run`, so the dumped rows pin `archive_acquisitions`,
`acquisition_attempts` and `pipeline_runs` bytes.

The intake's and the run repository's clock is the scenario's `now`, moved on by a step's
`advance_seconds`; `CLOUD_RETRY`'s waits are skipped (the Rust test records them instead).
"""

from __future__ import annotations

import logging
from contextlib import ExitStack
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar
from unittest import mock

import httpx

from embyx_manager.clients.clouddrive import AsyncCloudDrive, CloudDriveClient
from embyx_manager.db import Database
from embyx_manager.monitor import intake as intake_module
from embyx_manager.monitor import runs as runs_module
from embyx_manager.monitor.acquisitions import AcquisitionRepository
from embyx_manager.monitor.intake import AcquisitionIntake
from embyx_manager.monitor.reports import PipelineName, RunContext, RunState, RunTrigger
from embyx_manager.monitor.runs import PipelineRunRepository

if TYPE_CHECKING:
    from runner import Context

TOKEN = 'scenario-token'  # noqa: S105 - the fake's token, not a secret


class _Clock(datetime):
    """`datetime` with `now()` pinned to the scenario's moment."""

    current: ClassVar[datetime]

    @classmethod
    def now(cls, tz: Any = None) -> datetime:  # noqa: ARG003 - the pinned moment is already UTC
        return cls.current


class _Sukebei:
    def __init__(self, answers: dict[str, str]) -> None:
        self._answers = answers

    async def get_magnet(self, avid: str) -> str | None:
        return self._answers.get(avid)


class _JavBus:
    def __init__(self, answers: dict[str, Any]) -> None:
        self._answers = answers

    async def get_magnets(self, avid: str) -> list[dict[str, Any]]:
        answer = self._answers.get(avid, [])
        if isinstance(answer, int):
            request = httpx.Request('GET', f'https://www.javbus.com/{avid}')
            httpx.Response(answer, request=request).raise_for_status()
        return [{'magnet': magnet, 'size': '', 'size_int': size, 'tags': [], 'score': size} for magnet, size in answer]


async def _no_wait(_seconds: float) -> None:
    return None


async def _step(intake: AcquisitionIntake, ctx: RunContext, op: str, args: dict[str, Any]) -> Any:
    release = args.get('release_date')
    release_date = date.fromisoformat(release) if release else None
    match op:
        case 'enqueue':
            return await intake.enqueue(
                args['avid'],
                source=args['source'],
                task_dir_path=args['task_dir'],
                ctx=ctx,
                item_magnet=args.get('item_magnet'),
                release_date=release_date,
            )
        case 'queue':
            return await intake.queue(
                args['avid'], source=args['source'], task_dir_path=args['task_dir'], ctx=ctx, release_date=release_date
            )
        case 'retry_due':
            return await intake.retry_due(ctx=ctx, fallback_task_dir=args.get('fallback_task_dir'))
    msg = f'unknown step {op!r}'
    raise ValueError(msg)


async def steps(ctx: Context) -> Any:
    _Clock.current = ctx.now.astimezone(UTC)
    database = Database(ctx.dsn)
    client = CloudDriveClient(address=ctx.clouddrive_address, api_token=TOKEN, secure=False)
    cloud = AsyncCloudDrive(client)
    retrying = AcquisitionIntake._add_offline_with_retry.retry  # type: ignore[attr-defined]  # noqa: SLF001
    with ExitStack() as stack:
        stack.enter_context(mock.patch.object(intake_module, 'datetime', _Clock))
        stack.enter_context(mock.patch.object(runs_module, 'datetime', _Clock))
        stack.enter_context(mock.patch.object(retrying, 'sleep', _no_wait))
        try:
            intake = AcquisitionIntake(
                ledger=AcquisitionRepository(database),
                sukebei=_Sukebei(ctx.args.get('sukebei', {})),  # type: ignore[arg-type]
                javbus=_JavBus(ctx.args.get('javbus', {})),  # type: ignore[arg-type]
                cloud=cloud,
                failed_cooldown_seconds=ctx.args['failed_cooldown_seconds'],
            )
            runs = PipelineRunRepository(database)
            run_id = await runs.start_run(PipelineName.RSS, RunTrigger.MANUAL)
            run = RunContext(logger=logging.getLogger('scenario-intake'))
            reported = []
            for step in ctx.args['steps']:
                step_args = step.get('args', {})
                _Clock.current += timedelta(seconds=step_args.get('advance_seconds', 0))
                reported.append({'op': step['op'], 'result': await _step(intake, run, step['op'], step_args)})
            await runs.finish_run(
                run_id, state=RunState.COMPLETED, stats=run.stats, errors=run.errors, log_tail=run.log_tail
            )
        finally:
            await cloud.aclose()
            await database.aclose()
    return {'steps': reported}


OPERATIONS = {'intake.steps': steps}
