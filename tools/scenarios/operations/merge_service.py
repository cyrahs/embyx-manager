"""The merge service, auto-merge and the home transcode path (`merge/service.py`, `auto.py`,
`transcode.py`), step by step against the scenario's database, CloudDrive and Jobs (WP14).

`merge.service` builds one world: `MergeService` over `MergeTaskRepository` and
`TranscodeRepository` on the scenario database, `AsyncCloudDrive` against the fake CloudDrive,
`KubeJobs` against the fake Kubernetes API with its Jobs model (the template and token written
to the `secrets` root), the work volume in the `work` root, and an `AutoMerger` over the
mapping tree in the `mapping` root. The parts' sizes come from `args.part_sizes` (path to
bytes; a part not named there cannot be read), the free space from `args.free_gib`. The
configuration is `args.merge` and `args.archive` over the models' defaults, with
`merge.work_dir` the `work` root.

Steps (`args.steps`, each `{op, args}`, reported as `{op, result | error, tasks}` with every
task of the listing after the step):

- `create` (a task, as the merge page queues one), `tick`, `cancel`, `retry`, `use_cpu`,
  `set_paused`; `restart` (a new world on the same database, roots and fakes, as a new process);
- `claim` and `heartbeat` (a worker report: `name`, `version`, `task_id`, `state`), `chunk` (a
  chunk as the transcode API's PUT stores it: `task_id`, `part_index`, `chunk_index`, `start`,
  `duration`, `frames`, the bytes as `text`, `sha256` unless it is the text's), `finish_part`,
  `transcode_view`, `home`;
- `auto` (one round of auto-merge, reporting its status);
- `request` (`app`: `main` or `transcode`, `method`, `path`, `json` or `text`, `headers`): one
  HTTP request through httpx's ASGI transport to the merge router in `create_app` (the token
  `args.api_token`) or to the transcode listener (`create_transcode_app` with the token
  `args.transcode_token`), both over this world's service, catalog, title cache and auto-merge
  status as `build_app` wires them; reported with `status`, the `REPORTED_HEADERS` it has and
  `body` (JSON, else text);
- unrecorded changes: `write` (`path` as `<root>/<relative>`, `text` or `json`), `remove`,
  `advance` (`seconds` on the clock), `job_status` (`name`, `status`: the cluster changed a Job),
  `cloud` (`edit` and its `args`: `FakeCloudDrive.edit`), `free_space` (`gib`).

A step's `cancel_after = "<method>"` cancels it the moment that `AsyncCloudDrive` method has
answered, as a shutdown would, and reports `CancelledError`. Every time comes from the
scenario clock: `datetime` is pinned in the merge modules. The result is the steps and the
clock at the end (a resume continues from there, `segment.py`).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar
from unittest import mock

import httpx

from embyx_manager.api import create_app, make_mutation_auth
from embyx_manager.clients.clouddrive import AsyncCloudDrive, CloudDriveClient
from embyx_manager.config.models import ArchiveConfig, CloudDriveConfig, MappingConfig, MergeConfig
from embyx_manager.db import Database
from embyx_manager.errors import ApiError
from embyx_manager.merge import api as api_module
from embyx_manager.merge import auto as auto_module
from embyx_manager.merge import service as service_module
from embyx_manager.merge import tasks as tasks_module
from embyx_manager.merge import transcode as transcode_module
from embyx_manager.merge import transcode_api as transcode_api_module
from embyx_manager.merge.api import MergeCatalog, MergeTasksApi, TitleScanCache, create_merge_router
from embyx_manager.merge.auto import GIB, AutoMerger
from embyx_manager.merge.kube import KubeJobs
from embyx_manager.merge.service import ChunkRefusedError, MergeActionError, MergeService
from embyx_manager.merge.tasks import MergeTaskRepository
from embyx_manager.merge.transcode import Chunk, TranscodeRepository, WorkerPresence, WorkerReport
from embyx_manager.merge.transcode_api import create_transcode_app, create_transcode_router
from operations.merge import _task

if TYPE_CHECKING:
    from runner import Context

TOKEN = 'scenario-token'  # noqa: S105 - the fake's token, not a secret
#: The response headers a `request` step reports, in order.
REPORTED_HEADERS = ('content-type', 'cache-control', 'www-authenticate')
JOB_TEMPLATE = {
    'apiVersion': 'batch/v1',
    'kind': 'Job',
    'metadata': {'labels': {'app': 'embyx-merge'}},
    'spec': {'template': {'spec': {'containers': [{'name': 'merge', 'command': ['embyx-manager', 'merge-worker']}]}}},
}


class _Clock(datetime):
    """`datetime` with `now()` pinned to the scenario's moment."""

    current: ClassVar[datetime]

    @classmethod
    def now(cls, tz: Any = None) -> datetime:  # noqa: ARG003 - the pinned moment is already UTC
        return cls.current


class _Cloud:
    """`AsyncCloudDrive` that cancels the running step once `cancel_after` has answered."""

    METHODS = (
        'list_directory',
        'stat_file',
        'copy_tasks',
        'copy_file',
        'upload_state',
        'restart_copy_task',
        'cancel_copy_task',
        'delete_files',
        'ensure_directory',
        'move_file',
    )

    def __init__(self, inner: AsyncCloudDrive) -> None:
        self._inner = inner
        self.cancel_after: str | None = None
        self.task: asyncio.Task[Any] | None = None

    def __getattr__(self, name: str) -> Any:
        if name not in self.METHODS:
            raise AttributeError(name)
        method = getattr(self._inner, name)

        async def call(*args: Any, **kwargs: Any) -> Any:
            answer = await method(*args, **kwargs)
            if self.cancel_after == name and self.task is not None:
                self.cancel_after = None
                self.task.cancel()
                await asyncio.sleep(0)
            return answer

        return call


def _model(model: type, fields: dict[str, Any], **extra: Any) -> Any:
    return model.model_validate({**model().model_dump(), **fields, **extra})


class _World:
    """The service, auto-merge and their clients; built again by `restart`."""

    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        args = ctx.args
        self.merge = _model(MergeConfig, args.get('merge', {}), work_dir=str(ctx.roots['work']))
        self.archive = _model(ArchiveConfig, args.get('archive', {}))
        self.free_gib = int(args.get('free_gib', 100))
        secrets = ctx.roots['secrets']
        (secrets / 'job.json').write_text(json.dumps(args.get('job_template', JOB_TEMPLATE)), encoding='utf-8')
        (secrets / 'token').write_text(TOKEN + '\n', encoding='utf-8')
        self.database = Database(ctx.dsn)
        self.repository = MergeTaskRepository(self.database)
        self.cloud = _Cloud(
            AsyncCloudDrive(CloudDriveClient(address=ctx.clouddrive_address, api_token=TOKEN, secure=False))
        )
        self.kube = KubeJobs(
            api_url=ctx.http_fakes['kubernetes'],
            namespace=args.get('namespace', 'media'),
            pod_name=args.get('pod_name', 'embyx-manager-0'),
            template_path=secrets / 'job.json',
            token_path=secrets / 'token',
            ca_path=None,
        )
        self.triggered: list[str] = []
        sizes: dict[str, int] = args.get('part_sizes', {})

        def part_sizes(parts: tuple[str, ...]) -> int:
            missing = [part for part in parts if part not in sizes]
            if missing:
                raise FileNotFoundError(2, 'No such file or directory', missing[0])
            return sum(sizes[part] for part in parts)

        async def trigger(pipeline: Any) -> None:
            self.triggered.append(pipeline.value)

        self.service = MergeService(
            repository=self.repository,
            cloud=lambda: self.cloud,  # type: ignore[arg-type,return-value]
            kube=self.kube,
            merge_config=lambda: self.merge,
            archive_config=lambda: self.archive,
            clouddrive_config=lambda: CloudDriveConfig(address=ctx.clouddrive_address, api_token=TOKEN),
            trigger=trigger,
            transcodes=TranscodeRepository(self.database),
            presence=WorkerPresence(),
            sizes=part_sizes,
        )
        ledger: dict[str, str] = args.get('ledger', {})

        async def task_dirs_for(avids: Any) -> dict[str, str]:
            return {avid: ledger[avid] for avid in avids if avid in ledger}

        catalog = MergeCatalog(
            archive=lambda: self.archive,
            mapping=lambda: MappingConfig(src_dir='/remote', dst_dir=str(ctx.roots['mapping'])),
            task_dirs_for=task_dirs_for,
        )
        cache = TitleScanCache()
        self.auto = AutoMerger(
            repository=self.repository,
            catalog=catalog,
            cache=cache,
            service=self.service,
            merge_config=lambda: self.merge,
            cloud=lambda: self.cloud,  # type: ignore[arg-type,return-value]
            free_space=lambda _path: self.free_gib * GIB,
        )
        api_token = args.get('api_token')

        async def ready() -> bool:
            return True

        self.apps = {
            'main': create_app(
                app_ready=ready,
                routers=(
                    create_merge_router(
                        catalog,
                        tasks=MergeTasksApi(
                            repository=self.repository, service=self.service, auto_status=self.auto.status
                        ),
                        mutation_auth=make_mutation_auth(api_token),
                        cache=cache,
                    ),
                ),
                api_token=api_token,
            ),
            'transcode': create_transcode_app(
                create_transcode_router(self.service, token=args.get('transcode_token')),
            ),
        }

    async def close(self) -> None:
        await self.service.aclose()
        await self.cloud._inner.aclose()  # noqa: SLF001
        await self.database.aclose()


async def _request(world: _World, args: dict[str, Any]) -> dict[str, Any]:
    """One request to one of the world's apps, as `TestClient` sends it."""
    transport = httpx.ASGITransport(app=world.apps[args.get('app', 'main')])
    sent: dict[str, Any] = {'headers': args.get('headers', {})}
    if 'json' in args:
        sent['json'] = args['json']
    elif 'text' in args:
        sent['content'] = args['text'].encode()
    async with httpx.AsyncClient(transport=transport, base_url='http://testserver') as client:
        response = await client.request(args['method'], args['path'], **sent)
    reported: dict[str, Any] = {'status': response.status_code}
    reported['headers'] = {name: response.headers[name] for name in REPORTED_HEADERS if name in response.headers}
    if response.headers.get('content-type') == 'application/json':
        reported['body'] = response.json()
    else:
        reported['body'] = response.text
    return reported


def _resolve(ctx: Context, spec_path: str) -> Any:
    root, _, relative = spec_path.partition('/')
    return ctx.roots[root] / relative if relative else ctx.roots[root]


def _report(report: dict[str, Any]) -> WorkerReport:
    return WorkerReport(
        seen_at=_Clock.now(UTC),
        name=report.get('name', ''),
        version=report.get('version', ''),
        task_id=report.get('task_id'),
        state=report.get('state', 'idle'),
    )


async def _task_of(world: _World, task_id: int) -> Any:
    task = await world.repository.get(task_id)
    if task is None:
        msg = f'no task {task_id}'
        raise KeyError(msg)
    return task


async def _chunk(world: _World, args: dict[str, Any]) -> Any:
    """What the transcode API's PUT does with a chunk whose bytes arrived whole."""
    task = await world.service.transcoding_task(args['task_id'], args['part_index'])
    staging = world.service.staging_chunk(task, args['part_index'], args['chunk_index'])
    data = args['text'].encode()
    staging.parent.mkdir(parents=True, exist_ok=True)
    staging.write_bytes(data)
    chunk = Chunk(
        part_index=args['part_index'],
        chunk_index=args['chunk_index'],
        start=float(args['start']),
        duration=float(args['duration']),
        frames=int(args.get('frames', 1)),
        bytes=len(data),
        sha256=args.get('sha256', hashlib.sha256(data).hexdigest()),
    )
    return {'stored': await world.service.accept_chunk(task, chunk, staging)}


async def _run(world: _World, ctx: Context, op: str, args: dict[str, Any]) -> Any:  # noqa: C901, PLR0911, PLR0912 - one case per step
    service = world.service
    match op:
        case 'create':
            return _task(
                await world.repository.create(
                    avid=args['avid'],
                    source=args['source'],
                    library_dir=args['library_dir'],
                    brand=args['brand'],
                    parts=tuple(args['parts']),
                    merged_name=args['merged_name'],
                )
            )
        case 'tick':
            await service.tick()
            triggered, world.triggered = world.triggered, []
            return {'triggered': triggered}
        case 'cancel':
            return _task(await service.cancel(args['id']))
        case 'retry':
            return _task(await service.retry(args['id']))
        case 'use_cpu':
            return _task(await service.use_cpu(args['id']))
        case 'set_paused':
            return _task(await service.set_paused(args['id'], paused=args['paused']))
        case 'claim':
            return await service.claim(_report(args))
        case 'heartbeat':
            return await service.heartbeat(_report(args))
        case 'chunk':
            return await _chunk(world, args)
        case 'finish_part':
            task = await service.transcoding_task(args['task_id'], args['part_index'])
            await service.finish_part(task, args['part_index'])
            return {'finished': True}
        case 'transcode_view':
            return await service.transcode_view(await _task_of(world, args['id']))
        case 'home':
            tasks = await world.repository.open_tasks()
            return {
                'home_bound': [task.id for task in tasks if service.home_bound(task)],
                'parked': [task.id for task in tasks if service.parked(task)],
                'reserved_bytes': service.reserved_bytes(tasks),
                'online': service.presence.online(),
            }
        case 'request':
            return await _request(world, args)
        case 'auto':
            await world.auto.step()
            return world.auto.status().model_dump(mode='json')
        case 'write':
            path = _resolve(ctx, args['path'])
            path.parent.mkdir(parents=True, exist_ok=True)
            text = json.dumps(args['json']) if 'json' in args else args['text']
            path.write_text(text, encoding='utf-8')
            return None
        case 'remove':
            _resolve(ctx, args['path']).unlink()
            return None
        case 'advance':
            _Clock.current += timedelta(seconds=args['seconds'])
            return _Clock.current.isoformat()
        case 'job_status':
            ctx.fakes['kubernetes'].set_job_status(args['name'], args['status'])
            return None
        case 'cloud':
            ctx.fakes['clouddrive'].edit(args['edit'], args.get('args', {}))
            return None
        case 'free_space':
            world.free_gib = int(args['gib'])
            return None
    msg = f'unknown step {op!r}'
    raise ValueError(msg)


def _error(exc: BaseException) -> dict[str, Any]:
    if isinstance(exc, ChunkRefusedError):
        return {'type': 'ChunkRefusedError', 'code': exc.code, 'status': exc.status, 'details': exc.details}
    if isinstance(exc, MergeActionError):
        return {'type': 'MergeActionError', 'code': exc.code}
    if isinstance(exc, ApiError):
        return {'type': 'ApiError', 'code': exc.code, 'status': exc.status_code}
    return {'type': type(exc).__name__, 'message': str(exc)}


async def run_steps(ctx: Context, steps: list[dict[str, Any]], now: datetime) -> dict[str, Any]:
    """Run `steps` in a world built on `ctx` from the moment `now`; the reports and the clock."""
    _Clock.current = now.astimezone(UTC)
    reported = []
    with ExitStack() as stack:
        for module in (service_module, tasks_module, transcode_module, auto_module, api_module, transcode_api_module):
            stack.enter_context(mock.patch.object(module, 'datetime', _Clock))
        world = _World(ctx)
        try:
            for step in steps:
                op = step['op']
                step_args = step.get('args', {})
                entry: dict[str, Any] = {'op': op}
                if op == 'restart':
                    await world.close()
                    world = _World(ctx)
                    entry['result'] = None
                else:
                    world.cloud.cancel_after = step_args.get('cancel_after')
                    task = asyncio.create_task(_run(world, ctx, op, step_args))
                    world.cloud.task = task
                    try:
                        entry['result'] = await task
                    except asyncio.CancelledError:
                        entry['error'] = {'type': 'CancelledError', 'message': ''}
                    except Exception as exc:  # noqa: BLE001 - a step's failure is part of the transcript
                        entry['error'] = _error(exc)
                    world.cloud.task = None
                    world.cloud.cancel_after = None
                entry['tasks'] = [_task(task) for task in sorted(await world.repository.listing(), key=lambda t: t.id)]
                reported.append(entry)
        finally:
            await world.close()
    return {'steps': reported, 'clock': _Clock.current.isoformat()}


async def service(ctx: Context) -> Any:
    return await run_steps(ctx, ctx.args['steps'], ctx.now)


OPERATIONS = {'merge.service': service}
