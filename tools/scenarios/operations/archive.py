"""Archive, reconcile and move-in on a temporary tree (WP6).

The three operations build the baseline's `ArchivePipeline`, `ReconcileScanner` and
`MoveInSweeper` over the scenario's roots and run `args.steps` in order. Every step gets a
fresh `RunContext`; its result, counters, log tail and errors are reported, so the outcome
pins the run-log bytes as well as the decisions.

The file system is the real one (the seeded temporary tree), seen through `RecordingFs`: it
records each mutation the code makes (`Path.rename`, `Path.unlink`, the outermost
`Path.mkdir`, `shutil.rmtree`) as a `fs` call in the transcript, in order, and injects what
the spec asks for:

- `args.faults`: `{op, path, errno | message, times}`. `op` is one of the four mutations or a
  read (`rglob`, `iterdir`, `stat`); `path` is a spec path (`<root>/<relative>`) compared
  with the call's (first) path. The call raises `OSError(errno, strerror, path[, path2])`,
  or `OSError(message)`, instead of running; `times` limits how often (default: always).
  A faulted mutation is recorded with its error.
- `args.stops`: `{op, path}`: right after that mutation the step's `RunContext` is asked to
  stop, which is how a scenario cancels a run in the middle.

Steps that only shape the tree between two runs (`write`, `remove`) are not recorded.
`reconcile.steps` and `move_in.steps` use the baseline's `AcquisitionRepository` on the
scenario database, with the modules' clocks pinned to the scenario's `now` (moved on by a
step's `advance_seconds`).
"""

from __future__ import annotations

import logging
import os
import shutil
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar
from unittest import mock

from embyx_manager.config.models import ArchiveConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.db import Database
from embyx_manager.monitor import move_in as move_in_module
from embyx_manager.monitor import reconcile as reconcile_module
from embyx_manager.monitor.acquisitions import AcquisitionRepository
from embyx_manager.monitor.archive import ArchivePipeline, ArchiveResult
from embyx_manager.monitor.move_in import MoveInSweeper
from embyx_manager.monitor.reconcile import ReconcileScanner
from embyx_manager.monitor.reports import RunContext

if TYPE_CHECKING:
    from collections.abc import Callable

    from runner import Context

_PATH_RENAME = Path.rename
_PATH_UNLINK = Path.unlink
_PATH_MKDIR = Path.mkdir
_PATH_RGLOB = Path.rglob
_PATH_ITERDIR = Path.iterdir
_PATH_STAT = Path.stat
_RMTREE = shutil.rmtree


def resolve(roots: dict[str, Path], spec_path: str) -> Path:
    """`<root>/<relative>` as the runner's `seed_tree` reads it."""
    root, _, relative = spec_path.partition('/')
    return roots[root] / relative if relative else roots[root]


class _Clock(datetime):
    """`datetime` with `now()` pinned to the scenario's moment."""

    current: ClassVar[datetime]

    @classmethod
    def now(cls, tz: Any = None) -> datetime:  # noqa: ARG003 - the pinned moment is already UTC
        return cls.current


class RecordingFs:
    """The file-system fake: records mutations, injects faults, requests stops."""

    def __init__(self, ctx: Context, args: dict[str, Any]) -> None:
        self._record = ctx.record
        self._faults = [{**fault, 'path': str(resolve(ctx.roots, fault['path']))} for fault in args.get('faults', [])]
        self._stops = [(stop['op'], str(resolve(ctx.roots, stop['path']))) for stop in args.get('stops', [])]
        self.run_ctx: RunContext | None = None
        self.paused = False
        self._mkdir_depth = 0

    def _fault(self, op: str, path: object, path2: object = None) -> OSError | None:
        if self.paused:
            return None
        for fault in self._faults:
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

    def _mutation(
        self,
        op: str,
        request: dict[str, str],
        call: Callable[[], Any],
        path: object,
        path2: object = None,
    ) -> Any:
        if self.paused:
            return call()
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
        if (op, str(path)) in self._stops and self.run_ctx is not None:
            self.run_ctx.request_stop()

    def patches(self) -> list[Any]:
        fs = self

        def rename(self: Path, target: Any) -> Any:
            return fs._mutation(
                'rename', {'src': str(self), 'dst': str(target)}, lambda: _PATH_RENAME(self, target), self, target
            )

        def unlink(self: Path, missing_ok: bool = False) -> Any:  # noqa: FBT001, FBT002 - pathlib's signature
            return fs._mutation('unlink', {'path': str(self)}, lambda: _PATH_UNLINK(self, missing_ok), self)

        def mkdir(self: Path, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> Any:  # noqa: FBT001, FBT002
            # Only the outermost call: `parents=True` recurses through `Path.mkdir` itself.
            if fs._mkdir_depth:
                return _PATH_MKDIR(self, mode, parents, exist_ok)
            fs._mkdir_depth += 1
            try:
                return fs._mutation(
                    'mkdir', {'path': str(self)}, lambda: _PATH_MKDIR(self, mode, parents, exist_ok), self
                )
            finally:
                fs._mkdir_depth -= 1

        def rmtree(path: Any, *args: Any, **kwargs: Any) -> Any:
            return fs._mutation('rmtree', {'path': str(path)}, lambda: _RMTREE(path, *args, **kwargs), path)

        def rglob(self: Path, pattern: str, **kwargs: Any) -> Any:
            error = fs._fault('rglob', self)
            if error is not None:
                raise error
            return _PATH_RGLOB(self, pattern, **kwargs)

        def iterdir(self: Path) -> Any:
            error = fs._fault('iterdir', self)
            if error is not None:
                raise error
            return _PATH_ITERDIR(self)

        def stat(self: Path, *, follow_symlinks: bool = True) -> Any:
            error = fs._fault('stat', self)
            if error is not None:
                raise error
            return _PATH_STAT(self, follow_symlinks=follow_symlinks)

        return [
            mock.patch.object(Path, 'rename', rename),
            mock.patch.object(Path, 'unlink', unlink),
            mock.patch.object(Path, 'mkdir', mkdir),
            mock.patch.object(Path, 'rglob', rglob),
            mock.patch.object(Path, 'iterdir', iterdir),
            mock.patch.object(Path, 'stat', stat),
            mock.patch.object(shutil, 'rmtree', rmtree),
        ]


def _config(ctx: Context, values: dict[str, Any]) -> ArchiveConfig:
    values = dict(values)
    values['src_dir'] = str(resolve(ctx.roots, values['src_dir']))
    values['dst_dir'] = str(resolve(ctx.roots, values['dst_dir']))
    values.setdefault('enabled', True)
    return ArchiveConfig(**values)


def _result(result: ArchiveResult) -> dict[str, Any]:
    return {
        'outcome': result.outcome.value,
        'avid': result.avid,
        'archived_paths': list(result.archived_paths),
        'reason': result.reason,
    }


def _shape_tree(ctx: Context, op: str, args: dict[str, Any]) -> bool:
    """The unrecorded steps that change the tree between runs; False for any other step."""
    if op == 'write':
        path = resolve(ctx.roots, args['path'])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'x' * args['size'])
        return True
    if op == 'remove':
        resolve(ctx.roots, args['path']).unlink()
        return True
    return False


async def _run_steps(ctx: Context, fs: RecordingFs, run_step: Callable[[str, dict[str, Any], RunContext], Any]) -> Any:
    reported = []
    for step in ctx.args['steps']:
        op = step['op']
        step_args = step.get('args', {})
        if 'advance_seconds' in step_args:
            _Clock.current += timedelta(seconds=step_args['advance_seconds'])
        fs.paused = True
        try:
            shaped = _shape_tree(ctx, op, step_args)
        finally:
            fs.paused = False
        if shaped:
            reported.append({'op': op})
            continue
        run = RunContext(logger=logging.getLogger('scenario-archive'))
        if step_args.get('stop_before'):
            run.request_stop()
        fs.run_ctx = run
        entry: dict[str, Any] = {'op': op}
        try:
            entry['result'] = await run_step(op, step_args, run)
        except Exception as exc:  # noqa: BLE001 - a step's failure is part of the transcript
            entry['error'] = {'type': type(exc).__name__, 'message': str(exc)}
        entry['stats'] = dict(run.stats)
        entry['log'] = list(run.log_tail)
        entry['errors'] = list(run.errors)
        reported.append(entry)
    return {'steps': reported}


async def archive_steps(ctx: Context) -> Any:
    """`ArchivePipeline` step by step: folders, loose groups, the full scan, the library checks."""
    pipelines = [ArchivePipeline(config=_config(ctx, ctx.args['config']), avid_parser=AvidParser())]
    fs = RecordingFs(ctx, ctx.args)
    selections: list[Any] = []

    async def run_step(op: str, args: dict[str, Any], run: RunContext) -> Any:  # noqa: PLR0911 - one return per step kind
        pipeline = pipelines[-1]
        match op:
            case 'configure':
                pipelines.append(ArchivePipeline(config=_config(ctx, args['config']), avid_parser=AvidParser()))
                return None
            case 'archive_folder':
                return _result(
                    pipeline.archive_folder(
                        resolve(ctx.roots, args['folder']),
                        args['dst'],
                        run,
                        priority=args.get('priority', False),
                        expected_avid=args.get('expected_avid', ''),
                    )
                )
            case 'archive_loose_group':
                return _result(
                    pipeline.archive_loose_group(
                        args['avid'],
                        [resolve(ctx.roots, video) for video in args['videos']],
                        args['dst'],
                        run,
                        priority=args.get('priority', False),
                    )
                )
            case 'inspect_folder':
                selection = pipeline.inspect_folder(
                    resolve(ctx.roots, args['folder']), run, expected_avid=args.get('expected_avid', '')
                )
                if isinstance(selection, ArchiveResult):
                    return _result(selection)
                selections.append(selection)
                return {'avid': selection.avid, 'videos': [str(video) for video in selection.videos]}
            case 'file_selection':
                return _result(
                    pipeline.file_selection(
                        resolve(ctx.roots, args['folder']),
                        selections.pop(),
                        args['dst'],
                        run,
                        priority=args.get('priority', False),
                    )
                )
            case 'run':
                pipeline.run(run)
                return None
            case 'library_holdings':
                return list(pipeline.library_holdings(args['avid'], run, task_dir_path=args.get('task_dir')))
            case 'replaced_copies':
                return [str(path) for path in pipeline.replaced_copies(args['avid'], tuple(args['paths']))]
            case 'route_for_task_dir':
                route = pipeline.route_for_task_dir(args['task_dir'])
                return None if route is None else list(route)
        msg = f'unknown step {op!r}'
        raise ValueError(msg)

    with ExitStack() as stack:
        for patch in fs.patches():
            stack.enter_context(patch)
        return await _run_steps(ctx, fs, run_step)


async def _ledger(ctx: Context, stack: ExitStack) -> tuple[Database, AcquisitionRepository]:
    database = Database(ctx.dsn)
    # Migrations run before the file system is patched.
    await database.get_pool()
    _Clock.current = ctx.now.astimezone(UTC)
    stack.enter_context(mock.patch.object(reconcile_module, 'datetime', _Clock))
    stack.enter_context(mock.patch.object(move_in_module, 'datetime', _Clock))
    return database, AcquisitionRepository(database)


async def reconcile_steps(ctx: Context) -> Any:
    """`ReconcileScanner` passes over the scenario's routes, the ledger in PostgreSQL."""
    with ExitStack() as stack:
        database, ledger = await _ledger(ctx, stack)
        config = _config(ctx, ctx.args['config'])
        scanner = ReconcileScanner(
            ledger=ledger, archiver=ArchivePipeline(config=config, avid_parser=AvidParser()), config=config
        )
        fs = RecordingFs(ctx, ctx.args)

        async def run_step(op: str, args: dict[str, Any], run: RunContext) -> Any:
            if op == 'run':
                await scanner.run(run)
                return None
            if op == 'rebind':
                rebound = _config(ctx, args['config'])
                scanner.rebind(archiver=ArchivePipeline(config=rebound, avid_parser=AvidParser()), config=rebound)
                return None
            msg = f'unknown step {op!r}'
            raise ValueError(msg)

        try:
            for patch in fs.patches():
                stack.enter_context(patch)
            return await _run_steps(ctx, fs, run_step)
        finally:
            await database.aclose()


async def move_in_steps(ctx: Context) -> Any:
    """`MoveInSweeper` passes over the staging tree, the ledger in PostgreSQL."""
    with ExitStack() as stack:
        database, ledger = await _ledger(ctx, stack)
        archiver = ArchivePipeline(config=_config(ctx, ctx.args['config']), avid_parser=AvidParser())
        sweeper = MoveInSweeper(
            ledger=ledger,
            archiver=archiver,
            avid_parser=AvidParser(),
            move_in_root=resolve(ctx.roots, ctx.args['move_in_root']),
            task_dir_path=ctx.args['task_dir_path'],
        )
        fs = RecordingFs(ctx, ctx.args)

        async def run_step(op: str, args: dict[str, Any], run: RunContext) -> Any:
            if op == 'run':
                await sweeper.run(run)
                return None
            if op == 'rebind':
                sweeper.rebind(
                    archiver=archiver,
                    avid_parser=AvidParser(),
                    move_in_root=resolve(ctx.roots, args.get('move_in_root', ctx.args['move_in_root'])),
                    task_dir_path=args['task_dir_path'],
                )
                return None
            msg = f'unknown step {op!r}'
            raise ValueError(msg)

        try:
            for patch in fs.patches():
                stack.enter_context(patch)
            return await _run_steps(ctx, fs, run_step)
        finally:
            await database.aclose()


OPERATIONS = {
    'archive.steps': archive_steps,
    'reconcile.steps': reconcile_steps,
    'move_in.steps': move_in_steps,
}
