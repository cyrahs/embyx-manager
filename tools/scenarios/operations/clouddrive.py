"""The CloudDrive client and wrapper (`clients/clouddrive/`), cross-version contract 6 (WP3).

`clouddrive.steps` runs `args.steps` in order against the fake CloudDrive through the
baseline's own `AsyncCloudDrive` (and, for `rename_file`, which the wrapper does not offer,
its `CloudDriveClient`), and reports what each step returned or raised. Each step is
`{op, args}`; the ops are the wrapper's methods by name. Values are reported as plain JSON:
the wrapper's dicts as they are, tuples as lists, enums by value, `UploadState` with its
`fatal`, protobuf answers in the fake's JSON mapping. An error is `{type, message}` with
`type(exc).__name__` and `str(exc)`; a `grpc.RpcError` is `{type, code, details}` instead,
since a transport failure's text names the peer's port and is each stack's own (D40).

`clouddrive.cancelled_move` starts a move, cancels its caller while the RPC is on its way,
and reports the cancellation; the transcript must still show the move with its answer.
"""

from __future__ import annotations

import asyncio
from enum import Enum
from typing import TYPE_CHECKING, Any

import grpc
from fakes.clouddrive import to_json
from google.protobuf.message import Message

from embyx_manager.clients.clouddrive import AsyncCloudDrive, CloudDriveClient
from embyx_manager.clients.clouddrive.aio import UploadState

if TYPE_CHECKING:
    from runner import Context

TOKEN = 'scenario-token'  # noqa: S105 - the fake's token, not a secret


def plain(value: Any) -> Any:
    if isinstance(value, UploadState):
        return {'status': value.status, 'transferred': value.transferred, 'error': value.error, 'fatal': value.fatal}
    if isinstance(value, Message):
        return to_json(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value


async def _run(cloud: AsyncCloudDrive, client: CloudDriveClient, op: str, args: dict[str, Any]) -> Any:  # noqa: C901, PLR0911, PLR0912 - one case per step
    match op:
        case 'check':
            return await cloud.check()
        case 'list_directory':
            return await cloud.list_directory(args['path'], force_refresh=args.get('force_refresh', True))
        case 'stat_file':
            return await cloud.stat_file(args['path'])
        case 'ensure_directory':
            return await cloud.ensure_directory(args['parent'], args['name'])
        case 'move_file':
            return await cloud.move_file(args['source'], args['destination'])
        case 'copy_file':
            return await cloud.copy_file(args['source'], args['destination'])
        case 'copy_tasks':
            return await cloud.copy_tasks()
        case 'upload_state':
            return await cloud.upload_state(args['path'])
        case 'restart_copy_task':
            return await cloud.restart_copy_task(args['source'], args['destination'])
        case 'cancel_copy_task':
            return await cloud.cancel_copy_task(args['source'], args['destination'])
        case 'delete_files':
            return await cloud.delete_files(args['paths'])
        case 'add_offline_files':
            return await cloud.add_offline_files(args['urls'], args['folder'])
        case 'list_offline_files':
            return await cloud.list_offline_files(args['path'])
        case 'remove_offline_files':
            return await cloud.remove_offline_files(args['hashes'], args['path'], delete_files=args['delete_files'])
        case 'rename_file':
            return await asyncio.to_thread(client.rename_file, args['path'], args['name'])
    msg = f'unknown step {op!r}'
    raise ValueError(msg)


def error_of(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, grpc.RpcError):
        return {'type': type(exc).__name__, 'code': exc.code().name, 'details': exc.details()}
    return {'type': type(exc).__name__, 'message': str(exc)}


async def steps(ctx: Context) -> Any:
    client = CloudDriveClient(address=ctx.clouddrive_address, api_token=TOKEN, secure=False)
    cloud = AsyncCloudDrive(client)
    reported = []
    try:
        for step in ctx.args['steps']:
            entry: dict[str, Any] = {'op': step['op']}
            try:
                entry['result'] = plain(await _run(cloud, client, step['op'], step.get('args', {})))
            except Exception as exc:  # noqa: BLE001 - each step's failure is part of the transcript
                entry['error'] = error_of(exc)
            reported.append(entry)
    finally:
        await cloud.aclose()
    return {'steps': reported}


async def cancelled_move(ctx: Context) -> Any:
    client = CloudDriveClient(address=ctx.clouddrive_address, api_token=TOKEN, secure=False)
    cloud = AsyncCloudDrive(client)
    try:
        task = asyncio.create_task(cloud.move_file(ctx.args['source'], ctx.args['destination']))
        # One step of the caller: the wrapper has handed the RPC to its worker thread and waits
        # for it. Cancelling now is cancelling a call that is on its way.
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            outcome = 'cancelled'
        else:
            outcome = 'finished'
    finally:
        await cloud.aclose()
    return {'caller': outcome}


OPERATIONS = {'clouddrive.steps': steps, 'clouddrive.cancelled_move': cancelled_move}
