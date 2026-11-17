"""The `.strm` mapping pipeline on a temporary tree (WP8).

`mapping.steps` builds the baseline's `MappingPipeline` over the scenario's roots
(`args.config`: `src_dir`, `dst_dir` as spec paths) and runs `args.steps` in order: `full`
(`run_full`), `incremental` (`run_incremental` with `changed` and `deleted` spec paths; the
result is the failed paths, each list sorted), `configure` (a new pipeline from `config`). Every step gets a fresh
`RunContext`; its result or error, counters, log tail and errors are reported.

The file system is the real one, seen through `operations.archive.RecordingFs` with two more
mutations recorded, `shutil.copy2` (`copy2`, `{src, dst}`) and `Path.rmdir` (`rmdir`), and
with every directory listed in name order (bytes): the pipeline walks with `Path.glob` and
`iterdir`, whose order is the file system's, so the scenario fixes it (`os.scandir` and the
globber's `scandir` are patched) to make the transcript the same on any machine. Faults and
stops work as in the archive scenarios (`args.faults`, `args.stops`).

Unrecorded steps shape the tree between runs: `write` (`path`, `text`, `mtime` in seconds),
`remove` (`path`), `mkdir` (`path`). Before the first step every seeded file's access and
modification time is set to the scenario clock, so whether a copy is current never depends on
when the tree was written.
"""

from __future__ import annotations

import glob
import logging
import os
import shutil
from contextlib import ExitStack
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self
from unittest import mock

from embyx_manager.config.models import MappingConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.mapping import MappingPipeline
from embyx_manager.monitor.reports import RunContext
from operations.archive import RecordingFs, resolve

if TYPE_CHECKING:
    from collections.abc import Iterator

    from runner import Context

_COPY2 = shutil.copy2
_PATH_RMDIR = Path.rmdir
_SCANDIR = os.scandir


class _Listing:
    """`os.scandir()`'s context manager over a list."""

    def __init__(self, entries: list[os.DirEntry[str]]) -> None:
        self._entries = entries

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        del exc

    def __iter__(self) -> Iterator[os.DirEntry[str]]:
        return iter(self._entries)

    def close(self) -> None:
        return None


def sorted_scandir(path: Any = '.') -> _Listing:
    """`os.scandir` listing in name order (bytes)."""
    with _SCANDIR(path) as listing:
        return _Listing(sorted(listing, key=lambda entry: os.fsencode(entry.name)))


class MappingFs(RecordingFs):
    """`RecordingFs` plus `copy2` and `rmdir`, listing directories in name order."""

    def patches(self) -> list[Any]:
        fs = self

        def copy2(src: Any, dst: Any, **kwargs: Any) -> Any:
            return fs._mutation(
                'copy2', {'src': str(src), 'dst': str(dst)}, lambda: _COPY2(src, dst, **kwargs), src, dst
            )

        def rmdir(self: Path) -> Any:
            return fs._mutation('rmdir', {'path': str(self)}, lambda: _PATH_RMDIR(self), self)

        return [
            *super().patches(),
            mock.patch.object(shutil, 'copy2', copy2),
            mock.patch.object(Path, 'rmdir', rmdir),
            mock.patch.object(os, 'scandir', sorted_scandir),
            mock.patch.object(glob._StringGlobber, 'scandir', staticmethod(sorted_scandir)),  # noqa: SLF001
        ]


def _pin_times(ctx: Context) -> None:
    """Every seeded file's times set to the scenario clock."""
    stamp = int(ctx.now.timestamp())
    for root in ctx.roots.values():
        for directory, _, files in os.walk(root):
            for name in files:
                path = Path(directory) / name
                if not path.is_symlink():
                    os.utime(path, (stamp, stamp))


def _shape_tree(ctx: Context, op: str, args: dict[str, Any]) -> bool:
    """The unrecorded steps that change the tree between runs; False for any other step."""
    match op:
        case 'write':
            path = resolve(ctx.roots, args['path'])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(args['text'], encoding='utf-8')
            os.utime(path, (args['mtime'], args['mtime']))
        case 'remove':
            resolve(ctx.roots, args['path']).unlink()
        case 'mkdir':
            resolve(ctx.roots, args['path']).mkdir(parents=True, exist_ok=True)
        case _:
            return False
    return True


def _pipeline(ctx: Context, config: dict[str, str]) -> MappingPipeline:
    return MappingPipeline(
        config=MappingConfig(
            enabled=True,
            src_dir=str(resolve(ctx.roots, config['src_dir'])),
            dst_dir=str(resolve(ctx.roots, config['dst_dir'])),
        ),
        avid_parser=AvidParser(),
    )


async def steps(ctx: Context) -> Any:
    pipeline = _pipeline(ctx, ctx.args['config'])
    _pin_times(ctx)
    fs = MappingFs(ctx, ctx.args)
    reported: list[dict[str, Any]] = []
    with ExitStack() as stack:
        for patch in fs.patches():
            stack.enter_context(patch)
        for step in ctx.args['steps']:
            op = step['op']
            step_args = step.get('args', {})
            fs.paused = True
            try:
                shaped = _shape_tree(ctx, op, step_args)
            finally:
                fs.paused = False
            if shaped:
                reported.append({'op': op})
                continue
            run = RunContext(logger=logging.getLogger('scenario-mapping'))
            if step_args.get('stop_before'):
                run.request_stop()
            fs.run_ctx = run
            entry: dict[str, Any] = {'op': op}
            try:
                if op == 'configure':
                    pipeline = _pipeline(ctx, step_args['config'])
                    entry['result'] = None
                elif op == 'full':
                    pipeline.run_full(run)
                    entry['result'] = None
                elif op == 'incremental':
                    failed_changed, failed_deleted = pipeline.run_incremental(
                        run,
                        changed={resolve(ctx.roots, path) for path in step_args.get('changed', [])},
                        deleted={resolve(ctx.roots, path) for path in step_args.get('deleted', [])},
                    )
                    entry['result'] = {
                        'failed_changed': sorted(str(path) for path in failed_changed),
                        'failed_deleted': sorted(str(path) for path in failed_deleted),
                    }
                else:
                    msg = f'unknown step {op!r}'
                    raise ValueError(msg)  # noqa: TRY301 - reported like any other step failure
            except Exception as exc:  # noqa: BLE001 - a step's failure is part of the transcript
                entry['error'] = {'type': type(exc).__name__, 'message': str(exc)}
            entry['stats'] = dict(run.stats)
            entry['log'] = list(run.log_tail)
            entry['errors'] = list(run.errors)
            reported.append(entry)
    return {'steps': reported}


OPERATIONS = {'mapping.steps': steps}
