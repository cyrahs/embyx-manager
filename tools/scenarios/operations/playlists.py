"""The playlists pipeline against the ledger database and the fake Emby (WP8).

`playlists.steps` builds `PlaylistSyncPipeline` over the baseline's `PlaylistRepository` on
the scenario database, the baseline's `EmbyClient` talking to the scripted Emby fake (the
spec's `emby.routes`; `GET /emby/Items` answers both the movie index and the playlist
listing, so a run's two listings are two `times = 1` routes in order) and a stub ranking
source: `args.source` is `{name, rows, broken_download}`, where `name` is what
`discover_database_name` returns (`JinjierError('page changed')` when absent), `rows` the
`[kind, number, name, note]` rows of the SQLite database `download_database` hands back (the
table `tests/test_jinjier.py` builds) and `broken_download` makes the download raise
`JinjierError('<name>: HTTP 503')`. The stub records each call as a `jinjier` call.

Steps run in order: `run` (a step's `source` replaces the stub's answers from then on;
`stop_before` asks the run to stop before it starts) and `set_enabled` (`key`, `enabled`).
The pipeline's and the repository's clock is the scenario's `now`, moved on by a step's
`advance_seconds`. Each step reports its result or error, counters, log tail and errors.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from embyx_manager.clients.emby import EmbyClient
from embyx_manager.clients.jinjier import JinjierError
from embyx_manager.core.avid import AvidParser
from embyx_manager.db import Database
from embyx_manager.monitor.playlist_sync import PlaylistSyncPipeline
from embyx_manager.monitor.playlists import PlaylistRepository
from embyx_manager.monitor.reports import RunContext

if TYPE_CHECKING:
    from runner import Context

API_KEY = 'scenario-key'


def ranking_database(rows: list[list[Any]]) -> bytes:
    """`tests/test_jinjier.py::make_database(rows)`."""
    connection = sqlite3.connect(':memory:')
    connection.execute(
        'CREATE TABLE ranks ('
        'id INTEGER PRIMARY KEY, kind INTEGER, number INTEGER, name TEXT, date TEXT, note TEXT, icon_url TEXT)',
    )
    connection.executemany(
        'INSERT INTO ranks (kind, number, name, date, note, icon_url) VALUES (?, ?, ?, ?, ?, ?)',
        [(kind, number, name, '2024-01-01', note, '') for kind, number, name, note in rows],
    )
    connection.commit()
    return connection.serialize()


class _Source:
    """The ranking source stub; every call is recorded."""

    def __init__(self, ctx: Context, answers: dict[str, Any]) -> None:
        self._record = ctx.record
        self.answers = answers

    def _call(self, method: str, request: dict[str, Any], error: JinjierError | None, response: Any) -> None:
        call: dict[str, Any] = {'fake': 'jinjier', 'method': method, 'request': request}
        if error is not None:
            call['error'] = {'type': type(error).__name__, 'message': str(error)}
        else:
            call['response'] = response
        self._record(call)

    async def discover_database_name(self) -> str:
        name = self.answers.get('name')
        if name is None:
            error = JinjierError('page changed')
            self._call('discover_database_name', {}, error, None)
            raise error
        self._call('discover_database_name', {}, None, name)
        return name

    async def download_database(self, name: str) -> bytes:
        if self.answers.get('broken_download'):
            error = JinjierError(f'{name}: HTTP 503')
            self._call('download_database', {'name': name}, error, None)
            raise error
        rows = self.answers.get('rows', [])
        self._call('download_database', {'name': name}, None, {'rows': len(rows)})
        return ranking_database(rows)


async def steps(ctx: Context) -> Any:
    current = [ctx.now]
    database = Database(ctx.dsn)
    emby = EmbyClient(base_url=ctx.http_fakes['emby'], api_key=API_KEY)
    try:
        repository = PlaylistRepository(database)
        source = _Source(ctx, dict(ctx.args.get('source', {})))
        pipeline = PlaylistSyncPipeline(
            repository=repository,
            source=source,  # type: ignore[arg-type]
            emby=emby,
            avid_of=AvidParser().get_avid,
            now=lambda: current[0],
        )
        reported: list[dict[str, Any]] = []
        for step in ctx.args['steps']:
            op = step['op']
            step_args = step.get('args', {})
            current[0] += timedelta(seconds=step_args.get('advance_seconds', 0))
            run = RunContext(logger=logging.getLogger('scenario-playlists'))
            entry: dict[str, Any] = {'op': op}
            try:
                if op == 'run':
                    if 'source' in step_args:
                        source.answers = dict(step_args['source'])
                    if step_args.get('stop_before'):
                        run.request_stop()
                    await pipeline.run(run)
                    entry['result'] = None
                elif op == 'set_enabled':
                    record = await repository.set_enabled(
                        step_args['key'], enabled=step_args['enabled'], now=current[0]
                    )
                    entry['result'] = None if record is None else {'key': record.key, 'enabled': record.enabled}
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
    finally:
        await emby.aclose()
        await database.aclose()


OPERATIONS = {'playlists.steps': steps}
