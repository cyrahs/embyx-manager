"""The HTTP shell of the app (`api.py`, `config/api.py`, the shell of `bootstrap.py`), WP4.

`api.exchange` assembles the app the way `bootstrap.build_app` assembles its shell: the
config store on the migrated database, `create_app` with the database readiness probe,
the bearer token, the request size limit, the config router and (when `args.frontend` is
true) the SPA hosting of the `static` root. It then sends `args.requests` in order through
Starlette's test client, as the browser, the probes and the home worker would, and reports
each response. `args.transcode_requests` go to the transcode listener's app
(`create_transcode_app`), whose routes arrive with WP14, so only its shell answers.

A request is `{method, path, headers?, json? | body? | body_size?, content_type?}`: `json`
is sent compact (`separators=(',', ':')`, `ensure_ascii=False`) with
`content-type: application/json` unless `content_type` says otherwise (an empty
`content_type` sends none); `body` is sent as UTF-8 text, `body_hex` as the bytes it spells;
`body_size` sends that many `x`;
`body_nested = n` sends `{"values":` and `n` nested empty lists as JSON.

With `args.monitor = true` the monitor and playlists routers (WP9) are mounted too, over the
real repositories on the scenario's database: a scheduler that is never started and whose
pipelines all report `args.not_ready` (so nothing can run), the ledger routes without a
CloudDrive to submit to, the subscription registry without an AVBase to resolve talents,
and the playlists without a fill directory. `args.mask_keys` names the keys whose non-null
values in a response body are reported as `<set>`: the times a mutation takes from the clock.
`{clouddrive}` and `{emby}` in a path, header or body are replaced by the fakes' addresses,
and replaced back in what is reported.

A response is `{status, headers, body}`: the headers that carry the contract (content type,
cache control, authentication, redirects, ranges, validators), and the body parsed as JSON
when the content type says JSON, else as text (a multipart boundary is masked). Every file
of the `static` root gets the same modification time first, so validators are stable.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter
from fastapi.testclient import TestClient

from embyx_manager.api import create_app, make_mutation_auth
from embyx_manager.config.api import create_config_router
from embyx_manager.config.models import RssConfig
from embyx_manager.config.store import ConfigStore
from embyx_manager.db import Database
from embyx_manager.fill_actor.postgres_repository import PostgresFillActorRepository
from embyx_manager.merge.transcode_api import create_transcode_app
from embyx_manager.monitor.acquisitions import AcquisitionRepository
from embyx_manager.monitor.api import AcquisitionApi, SubscriptionsApi, create_monitor_router
from embyx_manager.monitor.playlists import PlaylistRepository
from embyx_manager.monitor.playlists_api import create_playlists_router
from embyx_manager.monitor.runs import PipelineRunRepository
from embyx_manager.monitor.scheduler import MonitorScheduler
from embyx_manager.monitor.subscriptions import SubscriptionRepository

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from runner import Context

#: 2026-01-01T00:00:00Z, the modification time of every file the SPA root serves.
STATIC_MTIME_NS = 1_767_225_600 * 1_000_000_000
REPORTED_HEADERS = (
    'content-type',
    'content-length',
    'cache-control',
    'www-authenticate',
    'allow',
    'location',
    'accept-ranges',
    'content-range',
    'etag',
    'last-modified',
)
_BOUNDARY = re.compile(r'boundary=([0-9a-f]+)')


def _substitute(value: Any, replacements: dict[str, str]) -> Any:
    if isinstance(value, str):
        for placeholder, actual in replacements.items():
            value = value.replace(placeholder, actual)
        return value
    if isinstance(value, list):
        return [_substitute(item, replacements) for item in value]
    if isinstance(value, dict):
        return {key: _substitute(item, replacements) for key, item in value.items()}
    return value


def _request_parts(request: dict[str, Any]) -> tuple[dict[str, str], bytes | None]:
    headers = {str(name): str(value) for name, value in request.get('headers', {}).items()}
    content: bytes | None = None
    content_type = request.get('content_type')
    if 'json' in request:
        content = json.dumps(request['json'], ensure_ascii=False, separators=(',', ':')).encode()
        content_type = 'application/json' if content_type is None else content_type
    elif 'body' in request:
        content = str(request['body']).encode()
    elif 'body_hex' in request:
        content = bytes.fromhex(request['body_hex'])
    elif 'body_size' in request:
        content = b'x' * int(request['body_size'])
    elif 'body_nested' in request:
        depth = int(request['body_nested'])
        content = b'{"values":' + b'[' * depth + b']' * depth + b'}'
        content_type = 'application/json' if content_type is None else content_type
    if content_type:
        headers['content-type'] = content_type
    return headers, content


def _response(response: Any) -> dict[str, Any]:
    headers = {name: response.headers[name] for name in REPORTED_HEADERS if name in response.headers}
    boundary = _BOUNDARY.search(headers.get('content-type', ''))
    text = response.content.decode('utf-8', errors='replace')
    if boundary is not None:
        headers['content-type'] = headers['content-type'].replace(boundary.group(1), '<boundary>')
        text = text.replace(boundary.group(1), '<boundary>')
    body: Any = text
    if headers.get('content-type', '').startswith('application/json') and text:
        body = json.loads(text)
    return {'status': response.status_code, 'headers': headers, 'body': body}


def _mask(value: Any, keys: frozenset[str]) -> Any:
    if isinstance(value, list):
        return [_mask(item, keys) for item in value]
    if isinstance(value, dict):
        return {key: '<set>' if key in keys and item is not None else _mask(item, keys) for key, item in value.items()}
    return value


def _send(
    client: TestClient,
    requests: list[dict[str, Any]],
    replacements: dict[str, str],
    mask_keys: frozenset[str] = frozenset(),
) -> list[dict[str, Any]]:
    results = []
    for raw in requests:
        request = _substitute(raw, replacements)
        headers, content = _request_parts(request)
        response = client.request(request['method'], request['path'], headers=headers, content=content)
        result = _response(response)
        result['body'] = _mask(result['body'], mask_keys)
        results.append(result)
    return results


def _monitor_routers(store: ConfigStore, database: Database, mutation_auth: Any, not_ready: str) -> tuple[Any, ...]:
    async def never(_ctx: object) -> None:
        msg = 'a scenario never runs a pipeline'
        raise AssertionError(msg)

    def never_build() -> object:
        msg = 'a scenario never runs a pipeline'
        raise AssertionError(msg)

    runs = PipelineRunRepository(database)
    ledger = AcquisitionRepository(database)
    scheduler = MonitorScheduler(
        store=store,
        runs=runs,
        rss_runner=never,
        archive_runner=never,
        mapping_factory=never_build,
        rss_ready=lambda: not_ready,
        archive_ready=lambda: not_ready,
        mapping_ready=lambda: not_ready,
        playlists_runner=never,
        playlists_ready=lambda: not_ready,
    )
    monitor = create_monitor_router(
        scheduler,
        runs,
        mutation_auth=mutation_auth,
        acquisitions=AcquisitionApi(ledger=ledger, tracker_ready=lambda: not_ready),
        subscriptions=SubscriptionsApi(
            repository=SubscriptionRepository(database),
            categories=lambda: tuple(category.label for category in store.get(RssConfig).categories),
        ),
    )
    playlists = create_playlists_router(PlaylistRepository(database), mutation_auth=mutation_auth, ledger=ledger)
    return monitor, playlists


def _pin_static_mtimes(ctx: Context) -> None:
    root = ctx.roots.get('static')
    if root is None:
        return
    for directory, _, files in os.walk(root):
        for name in files:
            os.utime(os.path.join(directory, name), ns=(STATIC_MTIME_NS, STATIC_MTIME_NS))  # noqa: PTH118


async def exchange(ctx: Context) -> Any:
    replacements = {'{clouddrive}': ctx.clouddrive_address, '{emby}': ctx.http_fakes['emby']}
    _pin_static_mtimes(ctx)
    database = Database(ctx.dsn)
    store = ConfigStore(database)
    repository = PostgresFillActorRepository(database)
    api_token = ctx.args.get('api_token')

    @asynccontextmanager
    async def runtime_lifespan() -> AsyncIterator[None]:
        await store.load()
        try:
            yield
        finally:
            await database.aclose()

    mutation_auth = make_mutation_auth(api_token)
    routers: tuple[Any, ...] = (create_config_router(store, mutation_auth=mutation_auth),)
    if ctx.args.get('monitor'):
        routers += _monitor_routers(store, database, mutation_auth, str(ctx.args.get('not_ready', 'not ready')))
    mask_keys = frozenset(ctx.args.get('mask_keys', []))

    app = create_app(
        app_ready=repository.health_check,
        routers=routers,
        lifespans=(runtime_lifespan,),
        api_token=api_token,
        max_request_bytes=int(ctx.args.get('max_request_bytes', 65_536)),
        frontend_dist=ctx.roots['static'] if ctx.args.get('frontend') else None,
    )
    transcode_app = create_transcode_app(APIRouter())

    def run() -> dict[str, Any]:
        with TestClient(app, follow_redirects=False) as client:
            main = _send(client, ctx.args.get('requests', []), replacements, mask_keys)
        with TestClient(transcode_app, follow_redirects=False) as client:
            transcode = _send(client, ctx.args.get('transcode_requests', []), replacements)
        return {'responses': main, 'transcode_responses': transcode}

    result = await asyncio.to_thread(run)
    return _substitute(result, {actual: placeholder for placeholder, actual in replacements.items()})


OPERATIONS = {'api.exchange': exchange}
