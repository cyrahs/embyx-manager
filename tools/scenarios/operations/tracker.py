"""The tracker, replacements (换版) and the manual intake over one world (WP7).

`tracker.steps` builds the baseline's `AcquisitionTracker`, `ReplacementSource` and
`ManualIntakeSource` over the same ledger (PostgreSQL), the same CloudDrive (the baseline's
`AsyncCloudDrive` talking to the fake) and the same archive routes, and runs `args.steps` in
order. A replacement and a manual batch both end at the tracker, which is why they share it.

The world:

- `args.mount` is the spec root standing for the CloudDrive mount: `args.config`'s `src_dir`
  and `dst_dir` lie under it, it is the tracker's `library_mount_prefix`, and the spec seeds
  what the mount shows twice, as `[[tree]]` entries under it and as `clouddrive.files` at the
  same API paths. Deleting through the API (`DeleteFiles`, and `RemoveOfflineFiles` with
  `deleteFiles`) also deletes the local copy, unless a step sets `mount_keeps_deleted`.
- `args.task_dirs` are the CloudDrive directories the tracker polls, default first; the
  replacement and manual sources take them as the configured directories.
- `args.sukebei` maps an AVID to the magnet the resolver finds; JavBus finds nothing.
- Magnets are submitted the way the application's `_submit_magnet_at_cloud` does: under the
  row's directory or the first configured one, an existing task counting as accepted.

Steps: `poll` (reports the run's counters, log tail and errors), `plan`, `start`, `browse`,
`default_directory`, `check_target`, `submit`, and the unrecorded `write`/`remove` that shape
the tree. A step's `cancel_after = "<RPC>"` cancels the step's task the moment that RPC has
answered through the scenario's CloudDrive, the way a shutdown cancels it in the middle; the
step reports `CancelledError`. `args.faults` and `args.stops` act on the file system as in
`operations/archive.py`, with a poll's run as the one a stop is requested on.

The ledger's live attempts are joined to the task listing in info-hash order (D80): the
baseline takes them in the order PostgreSQL returns the rows, which an update can change.
`debug_error_string` in a gRPC error's text is grpcio's (peer address, time) and is scrubbed.
"""

from __future__ import annotations

import asyncio
import logging
import posixpath
import re
import shutil
from contextlib import ExitStack
from datetime import UTC, timedelta
from typing import TYPE_CHECKING, Any
from unittest import mock

import grpc

from embyx_manager.clients.clouddrive import AsyncCloudDrive, CloudDriveClient
from embyx_manager.core.avid import AvidParser
from embyx_manager.db import Database
from embyx_manager.monitor import intake as intake_module
from embyx_manager.monitor import replace as replace_module
from embyx_manager.monitor import tracker as tracker_module
from embyx_manager.monitor.acquisitions import AcquisitionRepository
from embyx_manager.monitor.archive import ArchivePipeline
from embyx_manager.monitor.intake import AcquisitionIntake
from embyx_manager.monitor.manual import ManualIntakeSource
from embyx_manager.monitor.replace import ReplacementSource
from embyx_manager.monitor.reports import RunContext
from embyx_manager.monitor.tracker import AcquisitionTracker, TrackerSettings
from operations.archive import RecordingFs, _Clock, _config, _shape_tree, resolve
from operations.intake import TOKEN, _JavBus, _no_wait, _Sukebei

if TYPE_CHECKING:
    from pathlib import Path

    from runner import Context

_DEBUG_ERROR = re.compile(r'debug_error_string = ".*?"\n>', re.DOTALL)


def _scrub(text: str) -> str:
    return _DEBUG_ERROR.sub('debug_error_string = "<scrubbed>"\n>', text)


class _Ledger(AcquisitionRepository):
    """The baseline's repository, with the live attempts in info-hash order (D80)."""

    async def attempts_by_info_hash(self, info_hashes: Any) -> Any:
        found = await super().attempts_by_info_hash(info_hashes)
        return dict(sorted(found.items()))


class _Cloud:
    """The baseline's `AsyncCloudDrive`, with the mount following API deletes and a cancel switch."""

    def __init__(self, inner: AsyncCloudDrive, mount: Path, fs: RecordingFs) -> None:
        self._inner = inner
        self._mount = mount
        self._fs = fs
        self._names: dict[tuple[str, str], str] = {}
        self.keeps_deleted = False
        self.cancel_after: str | None = None
        self.task: asyncio.Task[Any] | None = None

    async def _answered(self, method: str) -> None:
        if self.cancel_after == method and self.task is not None:
            self.cancel_after = None
            self.task.cancel()
            await asyncio.sleep(0)

    def _drop(self, api_path: str) -> None:
        if self.keeps_deleted:
            return
        local = self._mount / api_path.lstrip('/')
        self._fs.paused = True
        try:
            if local.is_dir():
                shutil.rmtree(local)
            elif local.exists():
                local.unlink()
        finally:
            self._fs.paused = False

    async def list_directory(self, api_dir: str, *, force_refresh: bool = True) -> Any:
        return await self._inner.list_directory(api_dir, force_refresh=force_refresh)

    async def list_offline_files(self, path: str) -> Any:
        tasks = await self._inner.list_offline_files(path)
        for task in tasks:
            self._names[path, task['info_hash']] = task['name']
        return tasks

    async def add_offline_files(self, urls: list[str], dst_dir: str) -> Any:
        result = await self._inner.add_offline_files(urls, dst_dir)
        await self._answered('AddOfflineFiles')
        return result

    async def remove_offline_files(self, info_hashes: list[str], path: str, *, delete_files: bool) -> Any:
        result = await self._inner.remove_offline_files(info_hashes, path, delete_files=delete_files)
        if delete_files:
            for info_hash in info_hashes:
                name = self._names.get((path, info_hash))
                if name:
                    self._drop(posixpath.join(path, name))
        await self._answered('RemoveOfflineFiles')
        return result

    async def delete_files(self, api_paths: list[str]) -> Any:
        result = await self._inner.delete_files(api_paths)
        if result['success']:
            for path in api_paths:
                self._drop(path)
        await self._answered('DeleteFiles')
        return result


def _plan(plan: Any) -> dict[str, Any]:
    return {
        'avid': plan.avid,
        'holdings': list(plan.holdings),
        'state': None if plan.state is None else plan.state.value,
        'task_dir_path': plan.task_dir_path,
        'blocked': plan.blocked,
    }


def _submission(submission: Any) -> dict[str, Any]:
    return {
        'task_dir_path': submission.task_dir_path,
        'entries': [
            {
                'text': entry.text,
                'avid': entry.avid,
                'outcome': entry.outcome.value,
                'archived_paths': list(entry.archived_paths),
            }
            for entry in submission.entries
        ],
    }


def _listing(listing: Any) -> dict[str, Any]:
    return {
        'path': listing.path,
        'parent': listing.parent,
        'entries': [
            {'path': entry.path, 'name': entry.name, 'configured': entry.configured, 'routed': entry.routed}
            for entry in listing.entries
        ],
    }


async def steps(ctx: Context) -> Any:  # noqa: C901, PLR0915 - one world, one branch per step kind
    args = ctx.args
    task_dirs = tuple(args['task_dirs'])
    mount = resolve(ctx.roots, args['mount'])
    config = _config(ctx, args['config'])
    fs = RecordingFs(ctx, args)
    _Clock.current = ctx.now.astimezone(UTC)
    database = Database(ctx.dsn)
    client = CloudDriveClient(address=ctx.clouddrive_address, api_token=TOKEN, secure=False)
    inner = AsyncCloudDrive(client)
    cloud = _Cloud(inner, mount, fs)
    retrying = AcquisitionIntake._add_offline_with_retry.retry  # type: ignore[attr-defined]  # noqa: SLF001
    with ExitStack() as stack:
        for module in (tracker_module, replace_module, intake_module):
            stack.enter_context(mock.patch.object(module, 'datetime', _Clock))
        stack.enter_context(mock.patch.object(retrying, 'sleep', _no_wait))
        try:
            # Migrations run before the file system is patched.
            await database.get_pool()
            ledger = _Ledger(database)
            intake = AcquisitionIntake(
                ledger=ledger,
                sukebei=_Sukebei(args.get('sukebei', {})),  # type: ignore[arg-type]
                javbus=_JavBus({}),  # type: ignore[arg-type]
                cloud=cloud,  # type: ignore[arg-type]
                failed_cooldown_seconds=args.get('failed_cooldown_seconds', 3600),
            )

            def archiver() -> ArchivePipeline:
                return ArchivePipeline(config=config, avid_parser=AvidParser())

            async def submit_magnet(avid: str, magnet: str) -> bool:
                record = await ledger.get(avid)
                task_dir = record.task_dir_path if record is not None else None
                if task_dir is None:
                    task_dir = task_dirs[0]
                try:
                    result = await cloud.add_offline_files([magnet], task_dir)
                except grpc.RpcError as exc:
                    return '任务已存在' in (exc.details() or '')
                except Exception:  # noqa: BLE001 - the application logs it and reports a refusal
                    return False
                return bool(getattr(result, 'success', False))

            tracker = AcquisitionTracker(
                ledger=ledger,
                cloud=cloud,  # type: ignore[arg-type]
                archiver=archiver(),
                settings=TrackerSettings.from_config(config, task_dir_paths=task_dirs, library_mount_prefix=str(mount)),
                submit_magnet=submit_magnet,
            )
            replacer = ReplacementSource(
                ledger=ledger,
                intake_factory=lambda: intake,
                archiver_factory=archiver,
                parser_factory=AvidParser,
                configured_dirs=lambda: task_dirs,
                submit_magnet=submit_magnet,
            )
            manual = ManualIntakeSource(
                ledger=ledger,
                intake_factory=lambda: intake,
                cloud_factory=lambda: cloud,  # type: ignore[arg-type,return-value]
                archiver_factory=archiver,
                parser_factory=AvidParser,
                configured_dirs=lambda: task_dirs,
            )

            async def run_step(op: str, step: dict[str, Any], run: RunContext) -> Any:  # noqa: PLR0911
                match op:
                    case 'poll':
                        await tracker.poll(run)
                        return None
                    case 'plan':
                        return _plan(await replacer.plan(step['text']))
                    case 'start':
                        result = await replacer.start(step['text'], magnet=step.get('magnet'))
                        return {'outcome': result.outcome.value, 'state': result.record.state.value}
                    case 'browse':
                        return _listing(await manual.browse(step['path']))
                    case 'default_directory':
                        return await manual.default_directory()
                    case 'check_target':
                        return await manual.check_target(step['task_dir'])
                    case 'submit':
                        return _submission(
                            await manual.submit(step['lines'], task_dir_path=step['task_dir'], queue=step.get('queue', False))
                        )
                msg = f'unknown step {op!r}'
                raise ValueError(msg)

            for patch in fs.patches():
                stack.enter_context(patch)
            reported = []
            for step in args['steps']:
                op = step['op']
                step_args = step.get('args', {})
                _Clock.current += timedelta(seconds=step_args.get('advance_seconds', 0))
                fs.paused = True
                try:
                    shaped = _shape_tree(ctx, op, step_args)
                finally:
                    fs.paused = False
                if shaped:
                    reported.append({'op': op})
                    continue
                run = RunContext(logger=logging.getLogger('scenario-tracker'))
                if step_args.get('stop_before'):
                    run.request_stop()
                fs.run_ctx = run
                cloud.keeps_deleted = step_args.get('mount_keeps_deleted', False)
                cloud.cancel_after = step_args.get('cancel_after')
                task = asyncio.create_task(run_step(op, step_args, run))
                cloud.task = task
                entry: dict[str, Any] = {'op': op}
                try:
                    entry['result'] = await task
                except asyncio.CancelledError:
                    entry['error'] = {'type': 'CancelledError', 'message': ''}
                except Exception as exc:  # noqa: BLE001 - a step's failure is part of the transcript
                    entry['error'] = {'type': type(exc).__name__, 'message': _scrub(str(exc))}
                cloud.task = None
                if op == 'poll':
                    entry['stats'] = dict(run.stats)
                    entry['log'] = [_scrub(line) for line in run.log_tail]
                    entry['errors'] = [_scrub(line) for line in run.errors]
                reported.append(entry)
        finally:
            await inner.aclose()
            await database.aclose()
    return {'steps': reported}


OPERATIONS = {'tracker.steps': steps}
