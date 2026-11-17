"""The RSS pipeline against the ledger and subscriptions in PostgreSQL and the fake CloudDrive (WP8).

`rss.steps` builds the baseline's `RssPipeline` over its `SubscriptionRepository` and
`AcquisitionRepository` on the scenario database, the baseline's `AsyncCloudDrive` talking to
the fake, an `ArchivePipeline` over the spec's library (`args.config`, as in
`operations/archive.py`, seen through its `RecordingFs` so `args.faults` and `args.stops`
apply) and stubs for everything that would leave the machine:

- `args.feeds` maps a feed URL to the body `fetch(url)` returns, or to an HTTP status it
  raises as `httpx.HTTPStatusError`; a URL not listed raises `KeyError`. A `run` step's
  `feeds` replaces the map from then on. Each fetch is recorded as a `feed` call.
- `args.sukebei` / `args.javbus` answer as in `operations/intake.py`.
- `args.cast` maps an AVID to the talent ids the catalog credits, or to `"error"`
  (`RuntimeError('catalog down')`); an AVID not listed is not in the catalog.
- `args.talents` maps a name to the talent `{talent_id, name, aliases}` AVBase lists under
  it, or to `"error"`. Both lookups are recorded as `avbase` calls.

`args.rss` is the `rss` configuration section. Steps run in order: `run` (`stop_after_fetch`
asks the run to stop once that URL has been fetched; `stop_before` before it starts) and the
unrecorded `write`/`remove` that shape the tree. The pipeline's, the intake's and the
repositories' clock is the scenario's `now`, moved on by a step's `advance_seconds`;
`CLOUD_RETRY`'s waits are skipped (the Rust test records them instead). Each run reports its
result or error, the magnets handed to CloudDrive (`on_submitted`), counters, log tail and
errors.
"""

from __future__ import annotations

import logging
from contextlib import ExitStack
from datetime import UTC, date, timedelta
from typing import TYPE_CHECKING, Any
from unittest import mock

import httpx

from embyx_manager.clients.avbase import AvbaseTalent
from embyx_manager.clients.clouddrive import AsyncCloudDrive, CloudDriveClient
from embyx_manager.config.models import RssConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.db import Database
from embyx_manager.monitor import intake as intake_module
from embyx_manager.monitor import rss as rss_module
from embyx_manager.monitor.acquisitions import AcquisitionRepository
from embyx_manager.monitor.archive import ArchivePipeline
from embyx_manager.monitor.intake import AcquisitionIntake
from embyx_manager.monitor.reports import RunContext
from embyx_manager.monitor.rss import CreditedWork, RssPipeline
from embyx_manager.monitor.subscriptions import SubscriptionRepository
from operations.archive import RecordingFs, _Clock, _config, _shape_tree
from operations.intake import TOKEN, _JavBus, _no_wait, _Sukebei

if TYPE_CHECKING:
    from runner import Context


class _Feeds:
    """`fetch(url)`: the spec's bodies and statuses; every call is recorded."""

    def __init__(self, ctx: Context, feeds: dict[str, Any]) -> None:
        self._record = ctx.record
        self.feeds = feeds
        self.stop_after: str | None = None
        self.run: RunContext | None = None

    async def __call__(self, url: str) -> bytes:
        call: dict[str, Any] = {'fake': 'feed', 'method': 'fetch', 'request': {'url': url}}
        try:
            answer = self.feeds[url]
            if isinstance(answer, int):
                httpx.Response(answer, request=httpx.Request('GET', url)).raise_for_status()
            body = answer.encode()
        except Exception as exc:
            self._record({**call, 'error': {'type': type(exc).__name__, 'message': str(exc)}})
            raise
        finally:
            if self.stop_after == url and self.run is not None:
                self.run.request_stop()
        self._record({**call, 'response': {'bytes': len(body)}})
        return body


class _Avbase:
    """The cast and talent lookups; every call is recorded."""

    def __init__(self, ctx: Context, cast: dict[str, Any], talents: dict[str, Any]) -> None:
        self._record = ctx.record
        self._cast = cast
        self._talents = talents

    def _call(self, method: str, request: dict[str, Any], answer: Any) -> None:
        call: dict[str, Any] = {'fake': 'avbase', 'method': method, 'request': request}
        if answer == 'error':
            call['error'] = {'type': 'RuntimeError', 'message': 'catalog down'}
            self._record(call)
            msg = 'catalog down'
            raise RuntimeError(msg)
        self._record({**call, 'response': answer})

    async def cast_lookup(self, avid: str) -> CreditedWork | None:
        answer = self._cast.get(avid)
        self._call('cast', {'avid': avid}, answer)
        if answer is None:
            return None
        return CreditedWork(talent_ids=frozenset(answer), release_date=date(2026, 9, 1))

    async def talent_lookup(self, name: str) -> AvbaseTalent | None:
        answer = self._talents.get(name)
        self._call('talent', {'name': name}, answer)
        if answer is None:
            return None
        return AvbaseTalent(
            talent_id=answer['talent_id'],
            name=answer['name'],
            aliases=tuple(answer.get('aliases', [])),
            total_works=0,
        )


async def steps(ctx: Context) -> Any:  # noqa: PLR0915 - one world, then its step loop
    args = ctx.args
    fs = RecordingFs(ctx, args)
    _Clock.current = ctx.now.astimezone(UTC)
    database = Database(ctx.dsn)
    client = CloudDriveClient(address=ctx.clouddrive_address, api_token=TOKEN, secure=False)
    cloud = AsyncCloudDrive(client)
    retrying = AcquisitionIntake._add_offline_with_retry.retry  # type: ignore[attr-defined]  # noqa: SLF001
    with ExitStack() as stack:
        for module in (rss_module, intake_module):
            stack.enter_context(mock.patch.object(module, 'datetime', _Clock))
        stack.enter_context(mock.patch.object(retrying, 'sleep', _no_wait))
        try:
            # Migrations run before the file system is patched.
            await database.get_pool()
            feeds = _Feeds(ctx, dict(args.get('feeds', {})))
            avbase = _Avbase(ctx, args.get('cast', {}), args.get('talents', {}))
            submitted = [0]

            def on_submitted() -> None:
                submitted[0] += 1

            pipeline = RssPipeline(
                config=RssConfig(**args['rss']),
                avid_parser=AvidParser(),
                subscriptions=SubscriptionRepository(database),
                fetch=feeds,
                cloud=cloud,
                sukebei=_Sukebei(args.get('sukebei', {})),  # type: ignore[arg-type]
                javbus=_JavBus(args.get('javbus', {})),  # type: ignore[arg-type]
                ledger=AcquisitionRepository(database),
                archiver=ArchivePipeline(config=_config(ctx, args['config']), avid_parser=AvidParser()),
                on_submitted=on_submitted,
                cast_lookup=avbase.cast_lookup,
                talent_lookup=avbase.talent_lookup,
            )
            for patch in fs.patches():
                stack.enter_context(patch)
            reported: list[dict[str, Any]] = []
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
                if op != 'run':
                    msg = f'unknown step {op!r}'
                    raise ValueError(msg)
                run = RunContext(logger=logging.getLogger('scenario-rss'))
                fs.run_ctx = run
                feeds.run = run
                feeds.stop_after = step_args.get('stop_after_fetch')
                if 'feeds' in step_args:
                    feeds.feeds = dict(step_args['feeds'])
                if step_args.get('stop_before'):
                    run.request_stop()
                submitted[0] = 0
                entry: dict[str, Any] = {'op': op}
                try:
                    await pipeline.run(run)
                    entry['result'] = None
                except Exception as exc:  # noqa: BLE001 - a step's failure is part of the transcript
                    entry['error'] = {'type': type(exc).__name__, 'message': str(exc)}
                entry['submitted'] = submitted[0]
                entry['stats'] = dict(run.stats)
                entry['log'] = list(run.log_tail)
                entry['errors'] = list(run.errors)
                reported.append(entry)
        finally:
            await cloud.aclose()
            await database.aclose()
    return {'steps': reported}


OPERATIONS = {'rss.steps': steps}
