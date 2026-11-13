"""The merge task repository (`merge/tasks.py`), the `merge_tasks` rows both builds write.

`merge.tasks` runs `args.steps` in order on a `MergeTaskRepository` over the migrated
database: `create`, `get`, `update`, `transition`, `open_tasks`, `listing`,
`cancelled_avids`, `merged_avids` and `last_done_at`. A field value of `finished_at` is the
scenario clock when it is `"now"`, otherwise an ISO 8601 text; `failed_state` is a state's
name. A task is reported with every field but its three timestamps, which come from the
wall clock, and with whether `updated_at` equals `created_at` and `state_changed_at`. An
error is `{type, message}` with `type(exc).__name__` and `str(exc)`.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import TYPE_CHECKING, Any

from embyx_manager.db import Database
from embyx_manager.merge.tasks import MergeState, MergeTask, MergeTaskRepository

if TYPE_CHECKING:
    from runner import Context

_WALL_CLOCK = ('created_at', 'updated_at', 'state_changed_at')


def _task(task: MergeTask | None) -> Any:
    if task is None:
        return None
    reported: dict[str, Any] = {}
    for field in dataclasses.fields(task):
        if field.name in _WALL_CLOCK:
            continue
        value = getattr(task, field.name)
        if isinstance(value, datetime):
            value = value.isoformat()
        elif isinstance(value, tuple):
            value = list(value)
        reported[field.name] = value
    reported['updated_at_is_created_at'] = task.updated_at == task.created_at
    reported['state_changed_at_is_updated_at'] = task.state_changed_at == task.updated_at
    return reported


def _fields(ctx: Context, fields: dict[str, Any]) -> dict[str, Any]:
    converted = dict(fields)
    if isinstance(converted.get('finished_at'), str):
        text = converted['finished_at']
        converted['finished_at'] = ctx.now if text == 'now' else datetime.fromisoformat(text)
    if isinstance(converted.get('failed_state'), str):
        converted['failed_state'] = MergeState(converted['failed_state'])
    return converted


async def _run(repository: MergeTaskRepository, ctx: Context, op: str, args: dict[str, Any]) -> Any:  # noqa: PLR0911 - one case per step
    match op:
        case 'create':
            return _task(
                await repository.create(
                    avid=args['avid'],
                    source=args['source'],
                    library_dir=args['library_dir'],
                    brand=args['brand'],
                    parts=tuple(args['parts']),
                    merged_name=args['merged_name'],
                )
            )
        case 'get':
            return _task(await repository.get(args['id']))
        case 'update':
            return _task(await repository.update(args['id'], **_fields(ctx, args.get('fields', {}))))
        case 'transition':
            return _task(
                await repository.transition(
                    args['id'],
                    MergeState(args['expected']),
                    MergeState(args['state']),
                    **_fields(ctx, args.get('fields', {})),
                )
            )
        case 'open_tasks':
            return [_task(task) for task in await repository.open_tasks()]
        case 'listing':
            return [_task(task) for task in await repository.listing()]
        case 'cancelled_avids':
            return sorted(await repository.cancelled_avids())
        case 'merged_avids':
            return sorted(await repository.merged_avids())
        case 'last_done_at':
            value = await repository.last_done_at()
            return value.isoformat() if value is not None else None
    msg = f'unknown step {op!r}'
    raise ValueError(msg)


async def tasks(ctx: Context) -> Any:
    database = Database(ctx.dsn)
    repository = MergeTaskRepository(database)
    reported = []
    try:
        for step in ctx.args['steps']:
            entry: dict[str, Any] = {'op': step['op']}
            try:
                entry['result'] = await _run(repository, ctx, step['op'], step.get('args', {}))
            except Exception as exc:  # noqa: BLE001 - each step's failure is part of the transcript
                entry['error'] = {'type': type(exc).__name__, 'message': str(exc)}
            reported.append(entry)
    finally:
        await database.aclose()
    return reported


OPERATIONS = {'merge.tasks': tasks}
