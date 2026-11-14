"""The fill-actor service (`fill_actor/service.py`) on a temporary library (WP11).

`fill_actor.service` builds the baseline's `FillActorService` over the scenario's roots, a
`PostgresFillActorRepository` on the scenario database and, with `args.cloud`, the
`CloudDriveFileMover` on `AsyncCloudDrive` against the CloudDrive fake, then runs
`args.steps` in order. Each step is `{op, args}` and is reported as `{op, result | error |
crashed}`:

- `scan` (`args.actors`): `create_plan`; the plan's `model_dump_json()` as JSON.
- `apply` (`args.files`: candidate file names of the last scan, default all): `apply`; the
  result's `model_dump_json()` as JSON.
- `reconcile`: `reconcile_moves()`, each result as JSON.
- `candidates`: the repository's record of each candidate of the last scan, as
  `operations/fill_actor.py` reports it, the fingerprint cut to its size (device, inode and
  times differ from run to run).
- `restart`: a new service on a new repository (a new connection pool), as a process
  restart; the file system, the clock and the token counter carry on.
- `journal` (`args.file`, `args.states`): the repository saves those journal states, in
  order, for that candidate of the last scan (a journal another build left behind).
- `write` (`path`, `text`), `remove` (`path`), `link` (`target`, `path`), `rename` (`src`,
  `dst`): shape the tree, unrecorded.
- `faults` (`args.faults`): replace the file system's faults.
- `cloud_move` (`src`, `dst`): someone else moves a CloudDrive file (`AsyncCloudDrive.move_file`,
  a recorded `MoveFile`).
- `advance_seconds` on any step moves the clock first.

The file system is the real one (the seeded tree) seen through a recording layer: each
mutation the service makes (`_rename_no_replace`, `os.link`, `os.rename`, `os.unlink`,
`os.mkdir`, the calls `Path.rename`, `Path.unlink` and `Path.mkdir` end in) is a call
`{fake: "fs", method, request, response | error}` in order. `faults` are `{op, path, errno |
message, times}` as in `operations/archive.py`. The repository's writes (`save_move_journal`,
`save_move_result`, `save_cloud_move_operation`, `finalize_cloud_move`) are calls of the fake
`repository`, recorded once they returned.

A step's `crash` stops the service in the middle, the way a process that dies stops it:
`{fs = <op>, path?, nth?}` in front of that file-system mutation, `{repository = <method>,
state?}` in front of that repository write (`state`: the journal or operation state it
writes), `{mover = <method>}` in front of that `CloudFileMover` call. It raises `Crash`, a
`BaseException` no `except Exception` in the service catches; the step reports `crashed`, and
the next step is normally `restart`. A step's `cancel_after` cancels the step's task right
after a call has returned: `{fs = <op>, path?}` or `{mover = <method>}`. After every step the
operation waits for the moves the service still runs (`aclose()`), so a cancelled apply's move
ends before the next step.

Plan, revision, candidate and attempt ids come from one counter (`token-1`, ...), the clock from
the scenario. The service runs without a mutation lock (one service at a time).
"""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import ExitStack, suppress
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from unittest import mock

import embyx_manager.fill_actor.service as service_module
from embyx_manager.adapters import CloudDriveFileMover
from embyx_manager.clients.clouddrive import AsyncCloudDrive, CloudDriveClient
from embyx_manager.db import Database
from embyx_manager.fill_actor.cloud_moves import CloudMovePaths
from embyx_manager.fill_actor.persistence import MoveJournalRecord, MoveJournalState
from embyx_manager.fill_actor.ports import AcquisitionOutcome, CatalogListing
from embyx_manager.fill_actor.postgres_repository import PostgresFillActorRepository
from embyx_manager.fill_actor.service import FillActorPaths, FillActorService, static_runtime
from operations.archive import resolve
from operations.fill_actor import _report
from operations.intake import TOKEN

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from runner import Context

_RENAME_NO_REPLACE = service_module._rename_no_replace  # noqa: SLF001
_LINK = os.link
_RENAME = os.rename
_UNLINK = os.unlink
_MKDIR = os.mkdir


class Crash(BaseException):
    """The process stops here: nothing in the service catches it."""


class RecordingFs:
    """The file-system fake: records mutations, injects faults, crashes, cancels."""

    def __init__(self, ctx: Context) -> None:
        self._ctx = ctx
        self._record = ctx.record
        self.faults: list[dict[str, Any]] = []
        self.crash: dict[str, Any] | None = None
        self.cancel_after: dict[str, Any] | None = None
        self.cancel: Callable[[], None] | None = None
        self.paused = False

    def set_faults(self, faults: list[dict[str, Any]]) -> None:
        self.faults = [{**fault, 'path': str(resolve(self._ctx.roots, fault['path']))} for fault in faults]

    def _path(self, spec: dict[str, Any]) -> str | None:
        return str(resolve(self._ctx.roots, spec['path'])) if 'path' in spec else None

    def _fault(self, op: str, path: object, path2: object = None) -> OSError | None:
        for fault in self.faults:
            if fault['op'] != op or fault['path'] != str(path):
                continue
            times = fault.get('times', -1)
            if times == 0:
                continue
            if times > 0:
                fault['times'] = times - 1
            if 'errno' not in fault:
                return OSError(fault['message'])
            code = fault['errno']
            if path2 is not None:
                return OSError(code, os.strerror(code), str(path), None, str(path2))
            return OSError(code, os.strerror(code), str(path))
        return None

    def _crash_here(self, op: str, path: object) -> bool:
        crash = self.crash
        if crash is None or crash.get('fs') != op:
            return False
        wanted = self._path(crash)
        if wanted is not None and wanted != str(path):
            return False
        nth = crash.get('nth', 1)
        if nth > 1:
            crash['nth'] = nth - 1
            return False
        self.crash = None
        return True

    def mutation(
        self, op: str, request: dict[str, str], call: Callable[[], Any], path: object, path2: object = None
    ) -> Any:
        if self.paused:
            return call()
        if self._crash_here(op, path):
            raise Crash
        error = self._fault(op, path, path2)
        record: dict[str, Any] = {'fake': 'fs', 'method': op, 'request': request}
        try:
            if error is not None:
                raise error
            result = call()
        except OSError as exc:
            self._record({**record, 'error': {'type': type(exc).__name__, 'message': str(exc)}})
            self._after(op, path)
            raise
        self._record({**record, 'response': None})
        self._after(op, path)
        return result

    def _after(self, op: str, path: object) -> None:
        cancel_after = self.cancel_after
        if cancel_after is None or cancel_after.get('fs') != op or self.cancel is None:
            return
        wanted = self._path(cancel_after)
        if wanted is not None and wanted != str(path):
            return
        self.cancel_after = None
        self.cancel()

    def patches(self) -> list[Any]:
        fs = self

        def two(source: object, destination: object) -> dict[str, str]:
            return {'src': os.fsdecode(source), 'dst': os.fsdecode(destination)}

        def rename_no_replace(source: Path, destination: Path) -> None:
            fs.mutation(
                'rename_noreplace',
                two(source, destination),
                lambda: _RENAME_NO_REPLACE(source, destination),
                source,
                destination,
            )

        def link(source: Any, destination: Any, **kwargs: Any) -> None:
            fs.mutation(
                'link', two(source, destination), lambda: _LINK(source, destination, **kwargs), source, destination
            )

        def rename(source: Any, destination: Any, **kwargs: Any) -> None:
            fs.mutation(
                'rename', two(source, destination), lambda: _RENAME(source, destination, **kwargs), source, destination
            )

        def unlink(path: Any, **kwargs: Any) -> None:
            fs.mutation('unlink', {'path': os.fsdecode(path)}, lambda: _UNLINK(path, **kwargs), path)

        def mkdir(path: Any, mode: int = 0o777, **kwargs: Any) -> None:
            fs.mutation('mkdir', {'path': os.fsdecode(path)}, lambda: _MKDIR(path, mode, **kwargs), path)

        return [
            mock.patch.object(service_module, '_rename_no_replace', rename_no_replace),
            mock.patch.object(os, 'link', link),
            mock.patch.object(os, 'rename', rename),
            mock.patch.object(os, 'unlink', unlink),
            mock.patch.object(os, 'mkdir', mkdir),
        ]


class RecordingRepository:
    """`PostgresFillActorRepository` with its writes recorded and a crash point in front of them."""

    def __init__(self, inner: PostgresFillActorRepository, record: Callable[[dict[str, Any]], None]) -> None:
        self._inner = inner
        self._record = record
        self.crash: dict[str, Any] | None = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def _check(self, method: str, state: Any) -> None:
        crash = self.crash
        if crash is None or crash.get('repository') != method:
            return
        if 'state' in crash and crash['state'] != state.value:
            return
        self.crash = None
        raise Crash

    async def _call(self, method: str, state: Any, request: dict[str, Any], call: Callable[[], Any]) -> Any:
        self._check(method, state)
        record: dict[str, Any] = {'fake': 'repository', 'method': method, 'request': request}
        try:
            result = await call()
        except Exception as exc:
            self._record({**record, 'error': {'type': type(exc).__name__, 'message': str(exc)}})
            raise
        self._record({**record, 'response': None})
        return result

    async def save_move_journal(self, record: MoveJournalRecord) -> None:
        request = {'plan_id': record.plan_id, 'candidate_id': record.candidate_id, 'state': record.state.value}
        await self._call('save_move_journal', record.state, request, lambda: self._inner.save_move_journal(record))

    async def save_move_result(self, plan_id: str, result: Any) -> None:
        request = {
            'plan_id': plan_id,
            'candidate_id': result.candidate_id,
            'state': result.state.value,
            'error_code': result.error_code,
        }
        await self._call(
            'save_move_result', result.state, request, lambda: self._inner.save_move_result(plan_id, result)
        )

    async def save_cloud_move_operation(self, operation: Any) -> None:
        request = _operation(operation)
        await self._call(
            'save_cloud_move_operation',
            operation.state,
            request,
            lambda: self._inner.save_cloud_move_operation(operation),
        )

    async def finalize_cloud_move(self, operation: Any, result: Any) -> None:
        request = {**_operation(operation), 'result_state': result.state.value, 'result_error_code': result.error_code}
        await self._call(
            'finalize_cloud_move',
            operation.state,
            request,
            lambda: self._inner.finalize_cloud_move(operation, result),
        )


def _operation(operation: Any) -> dict[str, Any]:
    return {
        'plan_id': operation.plan_id,
        'candidate_id': operation.candidate_id,
        'attempt_id': operation.attempt_id,
        'state': operation.state.value,
        'error_code': operation.error_code,
    }


class ControlledMover:
    """The mover, with `crash = {mover = <method>}` in front of a call and `cancel_after = {mover =
    <method>}` cancelling the step once the call returned."""

    def __init__(self, inner: CloudDriveFileMover) -> None:
        self._inner = inner
        self.crash: dict[str, Any] | None = None
        self.cancel_after: dict[str, Any] | None = None
        self.task: asyncio.Task[Any] | None = None

    def _before(self, method: str) -> None:
        if self.crash is not None and self.crash.get('mover') == method:
            self.crash = None
            raise Crash

    async def _answered(self, method: str) -> None:
        if self.cancel_after is not None and self.cancel_after.get('mover') == method and self.task is not None:
            self.cancel_after = None
            self.task.cancel()
            if asyncio.current_task() is self.task:
                # A future that never ends: the cancel lands here, right after the call (the
                # operation patches `asyncio.sleep` not to wait, so `sleep(0)` would not
                # suspend). A move's own task runs on; the step's `await` of it is cancelled.
                await asyncio.get_running_loop().create_future()

    async def list_directory(self, api_directory: str) -> Any:
        self._before('list_directory')
        result = await self._inner.list_directory(api_directory)
        await self._answered('list_directory')
        return result

    async def ensure_directory(self, parent_api_directory: str, folder_name: str) -> Any:
        self._before('ensure_directory')
        result = await self._inner.ensure_directory(parent_api_directory, folder_name)
        await self._answered('ensure_directory')
        return result

    async def move_file(self, source_api_path: str, destination_api_directory: str) -> Any:
        self._before('move_file')
        result = await self._inner.move_file(source_api_path, destination_api_directory)
        await self._answered('move_file')
        return result


class _Catalog:
    def __init__(self, values: dict[str, list[str]]) -> None:
        self._values = values

    async def list_videos(self, actor_ref: str) -> CatalogListing:
        return CatalogListing(actor_name=None, talent_id=None, aliases=(), video_ids=tuple(self._values[actor_ref]))


class _Gateway:
    async def queue_missing(self, _video_id: str, *, release_date: Any = None) -> AcquisitionOutcome:  # noqa: ARG002
        return AcquisitionOutcome.NO_MAGNET


class _Brands:
    def __init__(self, values: dict[str, str], default: str | None) -> None:
        self._values = values
        self._default = default

    def resolve_brand(self, video_id: str) -> str | None:
        return self._values.get(video_id, self._default)


class _World:
    """What outlives a restart: the clock, the token counter, the fakes."""

    def __init__(self, ctx: Context) -> None:
        self.now = ctx.now
        self.tokens = 0

    def clock(self) -> Any:
        return self.now

    def token(self) -> str:
        self.tokens += 1
        return f'token-{self.tokens}'


def _json(model: Any) -> Any:
    return json.loads(model.model_dump_json())


async def service_steps(ctx: Context) -> Any:  # noqa: C901, PLR0912, PLR0915 - one branch per step
    args = ctx.args
    library = args.get('library', {})
    paths = FillActorPaths.from_iterable(
        actor_brand_path=resolve(ctx.roots, library.get('actor', 'actor')),
        additional_brand_paths=tuple(resolve(ctx.roots, path) for path in library.get('additional', ['additional'])),
        move_in_path=resolve(ctx.roots, library.get('move_in', 'movein')),
    )
    cloud_args = args.get('cloud')
    cloud_paths = (
        CloudMovePaths.from_values(
            strm_mount_prefix=cloud_args['strm_mount_prefix'],
            source_api_roots=tuple(cloud_args['source_api_roots']),
            move_in_api_root=cloud_args['move_in_api_root'],
        )
        if cloud_args is not None
        else None
    )
    runtime = static_runtime(
        paths=paths,
        root_sentinel=args.get('root_sentinel'),
        move_in_by_brand=args.get('move_in_by_brand', False),
        apply_enabled=args.get('apply_enabled', True),
        cloud_move_paths=cloud_paths,
    )
    world = _World(ctx)
    fs = RecordingFs(ctx)
    fs.set_faults(args.get('faults', []))
    cloud: AsyncCloudDrive | None = None
    mover: ControlledMover | None = None
    if cloud_args is not None:
        cloud = AsyncCloudDrive(CloudDriveClient(address=ctx.clouddrive_address, api_token=TOKEN, secure=False))
        mover = ControlledMover(CloudDriveFileMover(get_cloud=lambda: cloud))
    databases: list[Database] = []

    def start() -> tuple[FillActorService, RecordingRepository]:
        database = Database(ctx.dsn)
        databases.append(database)
        repository = RecordingRepository(PostgresFillActorRepository(database), ctx.record)
        service = FillActorService(
            runtime=runtime,
            actor_catalog=_Catalog(args.get('catalog', {})),
            acquisition_gateway=_Gateway(),
            brand_resolver=_Brands(args.get('brands', {}), args.get('brand', 'ABC')),
            repository=repository,
            clock=world.clock,
            token_factory=world.token,
            cloud_file_mover=mover,
        )
        return service, repository

    async def no_wait(_seconds: float) -> None:
        return None

    service, repository = start()
    plan: Any = None
    reported: list[dict[str, Any]] = []
    loop = asyncio.get_running_loop()
    try:
        with ExitStack() as stack:
            for patch in fs.patches():
                stack.enter_context(patch)
            # The verification rounds' `asyncio.sleep(0.5)`, without the wait.
            stack.enter_context(mock.patch.object(service_module.asyncio, 'sleep', no_wait))
            for step in args['steps']:
                op = step['op']
                step_args = step.get('args', {})
                if 'advance_seconds' in step_args:
                    world.now += timedelta(seconds=step_args['advance_seconds'])
                entry: dict[str, Any] = {'op': op}
                fs.paused = True
                try:
                    shaped = _shape(ctx, op, step_args)
                finally:
                    fs.paused = False
                if shaped:
                    reported.append(entry)
                    continue
                if op == 'restart':
                    service, repository = start()
                    reported.append(entry)
                    continue
                if op == 'cloud_move':
                    # Someone else moves a CloudDrive file (a recorded MoveFile, as any other).
                    if cloud is None:
                        msg = 'cloud_move needs args.cloud'
                        raise ValueError(msg)
                    await cloud.move_file(step_args['src'], step_args['dst'])
                    reported.append(entry)
                    continue
                if op == 'faults':
                    fs.set_faults(step_args['faults'])
                    reported.append(entry)
                    continue
                crash = step_args.get('crash')
                fs.crash = dict(crash) if crash is not None and 'fs' in crash else None
                repository.crash = dict(crash) if crash is not None and 'repository' in crash else None
                cancel_after = step_args.get('cancel_after')
                fs.cancel_after = cancel_after if cancel_after is not None and 'fs' in cancel_after else None
                if mover is not None:
                    mover.crash = dict(crash) if crash is not None and 'mover' in crash else None
                    mover.cancel_after = cancel_after if cancel_after is not None and 'mover' in cancel_after else None
                task = asyncio.create_task(_run(service, repository, op, step_args, plan))
                fs.cancel = lambda task=task: loop.call_soon_threadsafe(task.cancel)
                if mover is not None:
                    mover.task = task
                try:
                    value, plan = await task
                    entry['result'] = value
                except Crash:
                    entry['crashed'] = True
                except asyncio.CancelledError:
                    entry['error'] = {'type': 'CancelledError', 'message': ''}
                except Exception as exc:  # noqa: BLE001 - a step's failure is part of the transcript
                    entry['error'] = {'type': type(exc).__name__, 'message': str(exc)}
                fs.cancel = None
                fs.crash = None
                repository.crash = None
                if mover is not None:
                    mover.crash = None
                    mover.cancel_after = None
                await _settle(service)
                reported.append(entry)
    finally:
        if cloud is not None:
            await cloud.aclose()
        for database in databases:
            await database.aclose()
    return {'steps': reported}


async def _settle(service: FillActorService) -> None:
    """`aclose()`, which waits for every move the service still runs; a crashed one is over."""
    with suppress(Crash):
        await service.aclose()


async def _run(
    service: FillActorService,
    repository: RecordingRepository,
    op: str,
    args: dict[str, Any],
    plan: Any,
) -> tuple[Any, Any]:
    if op == 'scan':
        created = await service.create_plan(args.get('actors', ['actor']))
        return _json(created), created
    if op == 'apply':
        files = args.get('files')
        candidates = [
            candidate.candidate_id
            for video in plan.videos
            for candidate in video.move_candidates
            if files is None or candidate.file_name in files
        ]
        result = await service.apply(plan_id=plan.plan_id, revision=plan.revision, candidate_ids=candidates)
        return _json(result), plan
    if op == 'reconcile':
        return [_json(result) for result in await service.reconcile_moves()], plan
    if op == 'candidates':
        reported = []
        for video in plan.videos:
            for candidate in video.move_candidates:
                record = _report(await repository.get_candidate(plan.plan_id, candidate.candidate_id))
                # Device, inode and times differ from run to run; the size is the file's.
                record['fingerprint'] = {'size': record['fingerprint']['size']}
                reported.append(record)
        return reported, plan
    if op == 'journal':
        candidate = next(
            candidate
            for video in plan.videos
            for candidate in video.move_candidates
            if candidate.file_name == args['file']
        )
        for state in args['states']:
            await repository.save_move_journal(
                MoveJournalRecord(
                    plan_id=plan.plan_id,
                    candidate_id=candidate.candidate_id,
                    state=MoveJournalState(state),
                    updated_at=service._now(),  # noqa: SLF001 - the scenario clock
                )
            )
        return None, plan
    msg = f'unknown step {op!r}'
    raise ValueError(msg)


def _shape(ctx: Context, op: str, args: dict[str, Any]) -> bool:
    """The unrecorded steps that change the tree between two steps; False for any other."""
    if op == 'write':
        path = resolve(ctx.roots, args['path'])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(args['text'], encoding='utf-8')
        return True
    if op == 'remove':
        resolve(ctx.roots, args['path']).unlink()
        return True
    if op == 'link':
        resolve(ctx.roots, args['path']).hardlink_to(resolve(ctx.roots, args['target']))
        return True
    if op == 'rename':
        resolve(ctx.roots, args['src']).rename(resolve(ctx.roots, args['dst']))
        return True
    return False


OPERATIONS = {'fill_actor.service': service_steps}
