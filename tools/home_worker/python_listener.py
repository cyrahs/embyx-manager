"""Serve the baseline's transcode listener for the Rust home worker (contract 5, WP15).

The Rust test `crates/embyx-transcode-worker/tests/python_listener.rs` starts this script,
points the Rust worker at the port it prints and drives the cluster's side through stdin. The
service is the baseline's `MergeService` on a PostgreSQL database of its own (created from
`EMBYX_MANAGER_TEST_DATABASE_URL` and dropped at the end), with CloudDrive and the Job API
replaced by the stand-ins of `tests/test_merge_service.py`, as `tests/test_merge_transcode.py`
builds it. The parts are real files under `mount`, which stands for the CloudDrive mount.

    uv run python tools/home_worker/python_listener.py '{"work": "...", "mount": "...", "token": "..."}'

It prints `{"port": N}` once listening, then answers one JSON line on stdout for each JSON
line read on stdin:

* `{"op": "create", "avid": ..., "parts": [...]}`: a queued merge task; its `id` and the
  `directory` its transcode uses;
* `{"op": "tick"}`: one pass of the service;
* `{"op": "state", "id": N}`: the task's `state`, `transcoder` and `phase`, and the Jobs
  created so far as `[name, args]`;
* `{"op": "chunks", "id": N}`: the chunks the cluster holds, as `[part, index, start,
  duration, frames, bytes]`;
* `{"op": "quit"}`: stop and drop the database.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
from pathlib import Path
from typing import Any

import asyncpg
import uvicorn

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.test_merge_service import ARCHIVE, FakeCloud, FakeKube

from embyx_manager.config.models import CloudDriveConfig, MergeConfig
from embyx_manager.db import Database
from embyx_manager.merge.service import MergeService
from embyx_manager.merge.tasks import MergeTaskRepository
from embyx_manager.merge.transcode import TranscodeRepository, transcode_dir
from embyx_manager.merge.transcode_api import create_transcode_app, create_transcode_router

#: Over the 115 GB a title may have, so it goes to the home worker.
BIG = 200 * 10**9


async def with_database(admin_dsn: str, name: str, statement: str) -> None:
    connection = await asyncpg.connect(admin_dsn)
    try:
        await connection.execute(statement.format(name=name))
    finally:
        await connection.close()


def database_dsn(admin_dsn: str, name: str) -> str:
    head, _, query = admin_dsn.partition('?')
    return f'{head.rsplit("/", 1)[0]}/{name}' + (f'?{query}' if query else '')


async def serve(settings: dict[str, Any], dsn: str) -> None:
    work = Path(settings['work'])
    database = Database(dsn)
    repository = MergeTaskRepository(database)
    transcodes = TranscodeRepository(database)
    kube = FakeKube()
    cloud = FakeCloud()
    mount = settings['mount']
    config = MergeConfig(work_dir=str(work), home_transcode_enabled=True, cloud_mount_prefix=mount)
    archive = ARCHIVE.model_copy(update={'src_dir': f'{mount}/115/embyx_in', 'dst_dir': f'{mount}/library'})

    async def trigger(_pipeline: object) -> None:
        return None

    service = MergeService(
        repository=repository,
        cloud=lambda: cloud,  # type: ignore[arg-type,return-value]
        kube=kube,  # type: ignore[arg-type]
        merge_config=lambda: config,
        archive_config=lambda: archive,
        clouddrive_config=lambda: CloudDriveConfig(address='cd:19798', api_token='t'),  # noqa: S106 - a stand-in
        trigger=trigger,
        transcodes=transcodes,
        sizes=lambda _parts: BIG,
    )
    app = create_transcode_app(create_transcode_router(service, token=settings.get('token') or None))
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    server = uvicorn.Server(uvicorn.Config(app, log_level='warning', lifespan='off'))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    while not server.started:  # noqa: ASYNC110 - uvicorn's server offers no event to wait on
        await asyncio.sleep(0.01)
    say({'port': listener.getsockname()[1]})
    loop = asyncio.get_running_loop()
    try:
        while True:
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break
            command = json.loads(line)
            if command['op'] == 'quit':
                break
            say(await answer(command, service, repository, transcodes, kube, work))
    finally:
        server.should_exit = True
        await serving
        await database.aclose()


async def answer(  # noqa: PLR0913, PLR0917 - the service and its stand-ins
    command: dict[str, Any],
    service: MergeService,
    repository: MergeTaskRepository,
    transcodes: TranscodeRepository,
    kube: FakeKube,
    work: Path,
) -> dict[str, Any]:
    op = command['op']
    if op == 'create':
        avid = command['avid']
        task = await repository.create(
            avid=avid,
            source='vr',
            library_dir='type/vr',
            brand=avid.split('-', maxsplit=1)[0],
            parts=tuple(command['parts']),
            merged_name=f'{avid}.mp4',
        )
        return {'id': task.id, 'directory': str(transcode_dir(work, task.id))}
    if op == 'tick':
        await service.tick()
        return {}
    if op == 'state':
        task = await repository.get(command['id'])
        if task is None:
            msg = f'no task {command["id"]}'
            raise ValueError(msg)
        return {
            'state': task.state.value,
            'transcoder': task.transcoder,
            'phase': task.phase,
            'jobs': [[name, args] for name, args in kube.created],
        }
    if op == 'chunks':
        chunks = await transcodes.chunks(command['id'])
        return {
            'chunks': [
                [chunk.part_index, chunk.chunk_index, chunk.start, chunk.duration, chunk.frames, chunk.bytes]
                for chunk in chunks
            ],
        }
    msg = f'unknown op {op!r}'
    raise ValueError(msg)


def say(message: dict[str, Any]) -> None:
    print(json.dumps(message, ensure_ascii=False), flush=True)


async def main() -> None:
    settings = json.loads(sys.argv[1])
    admin_dsn = os.environ['EMBYX_MANAGER_TEST_DATABASE_URL']
    name = f'embyx_home_worker_{os.getpid()}'
    await with_database(admin_dsn, name, 'CREATE DATABASE "{name}"')
    try:
        await serve(settings, database_dsn(admin_dsn, name))
    finally:
        await with_database(admin_dsn, name, 'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


if __name__ == '__main__':
    asyncio.run(main())
