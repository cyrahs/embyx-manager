"""Async, cancellation-safe wrapper around the synchronous CloudDrive gRPC client.

Every call runs the blocking gRPC operation in a worker thread and, when the awaiting
task is cancelled, still waits for that thread to finish before re-raising the
cancellation. This prevents "fire and forget" mutations: a caller can never observe a
cancelled move/create while the underlying RPC is still in flight.
"""

import asyncio
import posixpath
from collections.abc import Callable
from contextlib import suppress
from typing import Any

from embyx_manager.clients.clouddrive.client import CloudDriveClient

CloudFile = dict[str, object]

_ASCII_CONTROL_LIMIT = 32
_ASCII_DELETE = 127
MOVE_CONFLICT_SKIP = 2


def validate_api_path(value: str, *, allow_root: bool) -> str:
    if (
        not value.startswith('/')
        or value.startswith('//')
        or '\x00' in value
        or '\\' in value
        or any(ord(character) < _ASCII_CONTROL_LIMIT or ord(character) == _ASCII_DELETE for character in value)
        or posixpath.normpath(value) != value
        or (not allow_root and value == '/')
    ):
        msg = 'CloudDrive API path must be a canonical absolute POSIX path'
        raise ValueError(msg)
    return value


def validate_path_segment(value: str) -> str:
    if (
        not value
        or value in {'.', '..'}
        or '/' in value
        or '\\' in value
        or '\x00' in value
        or any(ord(character) < _ASCII_CONTROL_LIMIT or ord(character) == _ASCII_DELETE for character in value)
    ):
        msg = 'CloudDrive folder name must be one safe path segment'
        raise ValueError(msg)
    return value


def _cloud_file_to_dict(file: Any) -> CloudFile:
    write_time: dict[str, int] | None = None
    if file.HasField('writeTime'):
        write_time = {
            'seconds': int(file.writeTime.seconds),
            'nanos': int(file.writeTime.nanos),
        }
    return {
        'id': str(file.id),
        'name': str(file.name),
        'full_path': str(file.fullPathName),
        'size': int(file.size),
        'is_directory': bool(file.isDirectory),
        'write_time': write_time,
        'hashes': dict(sorted((str(key), str(value)) for key, value in file.fileHashes.items())),
    }


async def _run_sync_complete(function: Callable[..., Any], *args: object, **kwargs: object) -> Any:
    """Wait for a sync gRPC call to finish even when its asyncio caller is cancelled."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    completed = asyncio.get_running_loop().create_future()

    def notify_done(_task: asyncio.Task[Any]) -> None:
        if not completed.done():
            completed.set_result(None)

    task.add_done_callback(notify_done)
    cancelled = False
    while not completed.done():
        try:
            await asyncio.shield(completed)
        except asyncio.CancelledError:
            cancelled = True
    if cancelled:
        with suppress(Exception):
            task.result()
        raise asyncio.CancelledError
    return task.result()


class AsyncCloudDrive:
    def __init__(self, client: CloudDriveClient) -> None:
        self._client = client

    async def aclose(self) -> None:
        await asyncio.to_thread(self._client.close)

    async def check(self) -> dict[str, object]:
        """Connectivity probe: unauthenticated server ping plus one authenticated listing."""
        info = await _run_sync_complete(self._client.get_system_info)
        await self.list_directory('/')
        return {'reachable': True, 'authenticated': True, 'version': str(getattr(info, 'cloudAppVersion', ''))}

    async def list_directory(self, api_dir: str, *, force_refresh: bool = True) -> tuple[CloudFile, ...]:
        """Return fresh CloudDrive metadata for one API-native directory path."""
        directory = validate_api_path(api_dir, allow_root=True)
        files = await _run_sync_complete(self._client.get_sub_files, directory, force_refresh=force_refresh)
        return tuple(_cloud_file_to_dict(file) for file in files)

    async def stat_file(self, api_path: str) -> CloudFile | None:
        """Return fresh metadata for an exact CloudDrive API path, if it exists."""
        path = validate_api_path(api_path, allow_root=False)
        parent, name = posixpath.split(path)
        try:
            files = await self.list_directory(parent)
        except FileNotFoundError:
            return None
        return next(
            (file for file in files if file['name'] == name and file['full_path'] == path),
            None,
        )

    async def ensure_directory(self, parent_api_dir: str, folder_name: str) -> dict[str, object]:
        """Ensure one direct child directory exists and verify it through a fresh listing."""
        parent = validate_api_path(parent_api_dir, allow_root=True)
        name = validate_path_segment(folder_name)
        expected_path = posixpath.join(parent, name)
        files = await self.list_directory(parent)
        existing = next(
            (file for file in files if file['full_path'] == expected_path and file['name'] == name),
            None,
        )
        if existing is not None:
            return {'success': bool(existing['is_directory']), 'created': False, 'path': expected_path}

        create_error: Exception | None = None
        try:
            await _run_sync_complete(self._client.create_folder, parent, name)
        except Exception as exc:  # noqa: BLE001  # Follow-up listing resolves timeout/already-exists ambiguity.
            create_error = exc
        files = await self.list_directory(parent)
        created = next(
            (file for file in files if file['full_path'] == expected_path and file['name'] == name),
            None,
        )
        if created is not None and bool(created['is_directory']):
            return {'success': True, 'created': True, 'path': expected_path}
        if create_error is not None:
            raise create_error
        return {'success': False, 'created': False, 'path': expected_path}

    async def move_file(self, source_api_path: str, destination_api_dir: str) -> dict[str, object]:
        """Move one CloudDrive file without overwriting an existing destination."""
        source = validate_api_path(source_api_path, allow_root=False)
        destination = validate_api_path(destination_api_dir, allow_root=True)
        result = await _run_sync_complete(
            self._client.move_file,
            [source],
            destination,
            MOVE_CONFLICT_SKIP,
        )
        return {
            'success': bool(result.success),
            'error_message': str(result.errorMessage),
            'result_file_paths': tuple(str(path) for path in result.resultFilePaths),
        }

    async def add_offline_files(self, urls: list[str], dst_dir: str) -> Any:
        directory = validate_api_path(dst_dir, allow_root=True)
        return await _run_sync_complete(self._client.add_offline_file, urls, directory)

    async def list_finished_offline_files(self, path: str) -> Any:
        directory = validate_api_path(path, allow_root=True)
        result = await _run_sync_complete(self._client.list_finished_offline_files_by_path, directory)
        return result.offlineFiles

    async def clear_finished_offline_files(self, path: str) -> None:
        directory = validate_api_path(path, allow_root=True)
        await _run_sync_complete(self._client.clear_finished_offline_files, directory)
