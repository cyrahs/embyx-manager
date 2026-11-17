"""The fill-actor job manager and its API (`fill_actor/jobs.py`, `fill_actor/api.py`), WP12.

`fill_actor.api` builds the service as `fill_actor.service` does (a static runtime over the
scenario's roots, the catalog and brand fakes, a gateway that finds no magnet, the scenario
clock, ids from one counter `token-1`, ...), a `FillActorJobManager` over it and the same
repository (the same clock and counter, `args.max_concurrent_jobs` workers, default 1, a
10 ms poll), and the app as `bootstrap.build_app` assembles the fill-actor part of it:
`create_app` with the database readiness, the fill-actor router, its health block under
`fill_actor`, `FillActorError`'s handler and the token `args.api_token`. The lifespans do
not run (no maintenance loop: it purges by the wall clock). Then `args.steps` run in order;
each is `{op, args}` and is reported as `{op, ...}`:

- `request` (`method`, `path`, `json`?, `headers`?): sent through httpx's ASGI transport;
  reported with `status` and `body` (JSON, `args.mask_keys` masked as in `api.exchange`: the
  ages the job view takes from the wall clock).
- `start`: `jobs.start()`: recovers the expired leases, then starts the workers (a request
  that queues a job starts them too).
- `recover`: `jobs.recover_interrupted_jobs()` (the reaper's pass), reported as `result`.
- `wait`: until no job is queued and none runs under this manager's owner (the rows are
  polled), or with `args.settled`, until none runs at all.
- `restart`: `jobs.aclose()` and `service.aclose()`, then a new service, manager and pool,
  as a process restart; the clock, the counter and the file system carry on.
- `advance_seconds` on any step moves the clock first.

Job rows another build left behind are seeded through `database.sql`.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import httpx

from embyx_manager.api import create_app, make_mutation_auth
from embyx_manager.db import Database
from embyx_manager.fill_actor.api import create_fill_actor_router, fill_actor_health, handle_fill_actor_error
from embyx_manager.fill_actor.errors import FillActorError
from embyx_manager.fill_actor.jobs import FillActorJobManager
from embyx_manager.fill_actor.postgres_repository import PostgresFillActorRepository
from embyx_manager.fill_actor.service import FillActorPaths, FillActorService, static_runtime
from operations.api import _mask
from operations.archive import resolve
from operations.fill_actor_service import _Brands, _Catalog, _Gateway, _World

if TYPE_CHECKING:
    from runner import Context

#: How long `wait` polls before it gives up.
WAIT_SECONDS = 30.0
ACTIVE_SQL = """
SELECT count(*) FROM fill_actor_jobs
WHERE state = 'queued' OR (state = 'running' AND ($1::text IS NULL OR owner_id = $1))
"""


class _Process:
    """One process's service, job manager, app and pool."""

    def __init__(self, ctx: Context, world: _World) -> None:
        args = ctx.args
        library = args.get('library', {})
        paths = FillActorPaths.from_iterable(
            actor_brand_path=resolve(ctx.roots, library.get('actor', 'actor')),
            additional_brand_paths=tuple(
                resolve(ctx.roots, path) for path in library.get('additional', ['additional'])
            ),
            move_in_path=resolve(ctx.roots, library.get('move_in', 'movein')),
        )
        runtime = static_runtime(
            paths=paths,
            root_sentinel=args.get('root_sentinel'),
            move_in_by_brand=args.get('move_in_by_brand', False),
            apply_enabled=args.get('apply_enabled', True),
        )
        self.database = Database(ctx.dsn)
        self.repository = PostgresFillActorRepository(self.database)
        self.service = FillActorService(
            runtime=runtime,
            actor_catalog=_Catalog(args.get('catalog', {})),
            acquisition_gateway=_Gateway(),
            brand_resolver=_Brands(args.get('brands', {}), args.get('brand', 'ABC')),
            repository=self.repository,
            clock=world.clock,
            token_factory=world.token,
        )
        self.jobs = FillActorJobManager(
            service=self.service,
            repository=self.repository,
            clock=world.clock,
            token_factory=world.token,
            max_concurrent_jobs=args.get('max_concurrent_jobs', 1),
            poll_interval=0.01,
        )
        api_token = args.get('api_token')
        router = create_fill_actor_router(
            service=self.service,
            repository=self.repository,
            jobs=self.jobs,
            mutation_auth=make_mutation_auth(api_token),
        )
        self.app = create_app(
            app_ready=self.repository.health_check,
            routers=(router,),
            feature_health={'fill_actor': fill_actor_health(service=self.service, repository=self.repository)},
            exception_handlers={FillActorError: handle_fill_actor_error},
            api_token=api_token,
        )

    async def aclose(self) -> None:
        await self.jobs.aclose()
        await self.service.aclose()
        await self.database.aclose()


async def _wait(process: _Process, *, settled: bool) -> None:
    owner = None if settled else process.jobs._owner_id  # noqa: SLF001 - which rows are this manager's
    pool = await process.repository.get_pool()
    deadline = asyncio.get_running_loop().time() + WAIT_SECONDS
    while await pool.fetchval(ACTIVE_SQL, owner):
        if asyncio.get_running_loop().time() > deadline:
            msg = 'jobs still active'
            raise TimeoutError(msg)
        await asyncio.sleep(0.01)


async def api_steps(ctx: Context) -> Any:
    args = ctx.args
    world = _World(ctx)
    mask_keys = frozenset(args.get('mask_keys', []))
    process = _Process(ctx, world)
    reported: list[dict[str, Any]] = []
    try:
        for step in args['steps']:
            op = step['op']
            step_args = step.get('args', {})
            if 'advance_seconds' in step_args:
                world.now += timedelta(seconds=step_args['advance_seconds'])
            entry: dict[str, Any] = {'op': op}
            if op == 'request':
                transport = httpx.ASGITransport(app=process.app)
                async with httpx.AsyncClient(transport=transport, base_url='http://testserver') as client:
                    response = await client.request(
                        step_args['method'],
                        step_args['path'],
                        headers=step_args.get('headers', {}),
                        json=step_args.get('json'),
                    )
                entry['status'] = response.status_code
                entry['body'] = _mask(response.json(), mask_keys) if response.content else None
            elif op == 'start':
                await process.jobs.start()
            elif op == 'recover':
                entry['result'] = await process.jobs.recover_interrupted_jobs()
            elif op == 'wait':
                await _wait(process, settled=step_args.get('settled', False))
            elif op == 'restart':
                await process.aclose()
                process = _Process(ctx, world)
            else:
                msg = f'unknown step {op!r}'
                raise ValueError(msg)
            reported.append(entry)
    finally:
        await process.aclose()
    return {'steps': reported}


OPERATIONS = {'fill_actor.api': api_steps}
