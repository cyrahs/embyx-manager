import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

from embyx_manager.clients.clouddrive import AsyncCloudDrive, clouddrive_pb2
from embyx_manager.clients.clouddrive import client as client_module
from embyx_manager.clients.clouddrive.aio import OfflineStatus
from embyx_manager.clients.clouddrive.client import GRPC_TIMEOUT_SECONDS, CloudDriveClient
from embyx_manager.core.magnet import extract_info_hash


def _make_client(monkeypatch: pytest.MonkeyPatch, stub: SimpleNamespace, *, secure: bool = True) -> CloudDriveClient:
    channel = SimpleNamespace(close=Mock())
    monkeypatch.setattr(client_module.grpc, 'secure_channel', Mock(return_value=channel))
    monkeypatch.setattr(client_module.grpc, 'insecure_channel', Mock(return_value=channel))
    monkeypatch.setattr(client_module.clouddrive_pb2_grpc, 'CloudDriveFileSrvStub', Mock(return_value=stub))
    return CloudDriveClient(address='clouddrive.internal:80', api_token='test-token', secure=secure)


def test_clouddrive_calls_include_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    finished_result = clouddrive_pb2.OfflineFileListResult()
    stub = SimpleNamespace(
        GetSystemInfo=Mock(return_value=object()),
        GetSubFiles=Mock(return_value=[]),
        CreateFolder=Mock(return_value=object()),
        RenameFile=Mock(return_value=object()),
        MoveFile=Mock(return_value=object()),
        AddOfflineFiles=Mock(return_value=object()),
        ListOfflineFilesByPath=Mock(return_value=finished_result),
    )
    client = _make_client(monkeypatch, stub)

    client.get_system_info()
    client.get_sub_files('/media')
    client.create_folder('/media', 'new')
    client.rename_file('/media/old', 'new')
    client.move_file(['/media/file'], '/media/dst')
    client.add_offline_file('magnet:?xt=urn:btih:abc', '/media')
    client.list_offline_files_by_path('/media')

    for grpc_call in [
        stub.GetSystemInfo,
        stub.GetSubFiles,
        stub.CreateFolder,
        stub.RenameFile,
        stub.MoveFile,
        stub.AddOfflineFiles,
        stub.ListOfflineFilesByPath,
    ]:
        assert grpc_call.call_args.kwargs['timeout'] == GRPC_TIMEOUT_SECONDS


@pytest.mark.parametrize('secure', [True, False])
def test_clouddrive_channel_selection_and_auth_metadata(monkeypatch: pytest.MonkeyPatch, *, secure: bool) -> None:
    channel = SimpleNamespace(close=Mock())
    get_sub_files = Mock(return_value=[])
    stub = SimpleNamespace(GetSubFiles=get_sub_files)
    secure_channel = Mock(return_value=channel)
    insecure_channel = Mock(return_value=channel)
    monkeypatch.setattr(client_module.grpc, 'secure_channel', secure_channel)
    monkeypatch.setattr(client_module.grpc, 'insecure_channel', insecure_channel)
    monkeypatch.setattr(client_module.clouddrive_pb2_grpc, 'CloudDriveFileSrvStub', Mock(return_value=stub))

    client = CloudDriveClient(address='clouddrive.internal:80', api_token='test-token', secure=secure)
    assert client.get_sub_files('/cloud/library') == []

    if secure:
        secure_channel.assert_called_once()
        assert secure_channel.call_args.args[0] == 'clouddrive.internal:80'
        insecure_channel.assert_not_called()
    else:
        insecure_channel.assert_called_once_with('clouddrive.internal:80')
        secure_channel.assert_not_called()
    assert get_sub_files.call_args.kwargs['metadata'] == [('authorization', 'Bearer test-token')]


def _async_wrapper(sync_client: SimpleNamespace) -> AsyncCloudDrive:
    return AsyncCloudDrive(sync_client)  # type: ignore[arg-type]


async def test_cloud_file_metadata_uses_fresh_exact_parent_listing() -> None:
    matching = clouddrive_pb2.CloudDriveFile(
        id='file-id',
        name='ABC-001.mp4',
        fullPathName='/cloud/library/source-b/ABC/ABC-001.mp4',
        size=123,
        isDirectory=False,
    )
    matching.writeTime.seconds = 456
    matching.writeTime.nanos = 789
    matching.fileHashes[2] = 'sha1-value'
    other = clouddrive_pb2.CloudDriveFile(
        id='other-id',
        name='ABC-001.mp4',
        fullPathName='/different/path/ABC-001.mp4',
    )
    get_sub_files = Mock(return_value=[other, matching])
    cloud = _async_wrapper(SimpleNamespace(get_sub_files=get_sub_files))

    listing = await cloud.list_directory('/cloud/library/source-b/ABC')
    metadata = await cloud.stat_file('/cloud/library/source-b/ABC/ABC-001.mp4')

    assert listing[1] == {
        'id': 'file-id',
        'name': 'ABC-001.mp4',
        'full_path': '/cloud/library/source-b/ABC/ABC-001.mp4',
        'size': 123,
        'is_directory': False,
        'write_time': {'seconds': 456, 'nanos': 789},
        'hashes': {'2': 'sha1-value'},
    }
    assert metadata == listing[1]
    assert get_sub_files.call_args_list == [
        call('/cloud/library/source-b/ABC', force_refresh=True),
        call('/cloud/library/source-b/ABC', force_refresh=True),
    ]


async def test_cloud_file_stat_returns_none_for_missing_parent() -> None:
    cloud = _async_wrapper(SimpleNamespace(get_sub_files=Mock(side_effect=FileNotFoundError)))

    assert await cloud.stat_file('/cloud/library/source-b/ABC/missing.mp4') is None


async def test_move_cloud_file_always_uses_skip_conflict_policy() -> None:
    result = clouddrive_pb2.FileOperationResult(
        success=True,
        resultFilePaths=['/cloud/library/move-in/ABC/ABC-001.mp4'],
    )
    move_file = Mock(return_value=result)
    cloud = _async_wrapper(SimpleNamespace(move_file=move_file))

    response = await cloud.move_file(
        '/cloud/library/source-b/ABC/ABC-001.mp4',
        '/cloud/library/move-in/ABC',
    )

    assert response == {
        'success': True,
        'error_message': '',
        'result_file_paths': ('/cloud/library/move-in/ABC/ABC-001.mp4',),
    }
    move_file.assert_called_once_with(
        ['/cloud/library/source-b/ABC/ABC-001.mp4'],
        '/cloud/library/move-in/ABC',
        2,
    )


async def test_ensure_cloud_directory_creates_and_force_refreshes_one_safe_child() -> None:
    folder = clouddrive_pb2.CloudDriveFile(
        id='folder-id',
        name='ABC',
        fullPathName='/cloud/library/move-in/ABC',
        isDirectory=True,
    )
    get_sub_files = Mock(side_effect=[[], [folder]])
    create_folder = Mock(return_value=clouddrive_pb2.CreateFolderResult())
    cloud = _async_wrapper(SimpleNamespace(get_sub_files=get_sub_files, create_folder=create_folder))

    result = await cloud.ensure_directory('/cloud/library/move-in', 'ABC')

    assert result == {'success': True, 'created': True, 'path': '/cloud/library/move-in/ABC'}
    create_folder.assert_called_once_with('/cloud/library/move-in', 'ABC')


async def test_ensure_cloud_directory_accepts_verified_folder_after_create_error() -> None:
    folder = clouddrive_pb2.CloudDriveFile(
        id='folder-id',
        name='ABC',
        fullPathName='/cloud/library/move-in/ABC',
        isDirectory=True,
    )
    get_sub_files = Mock(side_effect=[[], [folder]])
    create_folder = Mock(side_effect=TimeoutError)
    cloud = _async_wrapper(SimpleNamespace(get_sub_files=get_sub_files, create_folder=create_folder))

    result = await cloud.ensure_directory('/cloud/library/move-in', 'ABC')

    assert result == {'success': True, 'created': True, 'path': '/cloud/library/move-in/ABC'}


async def test_ensure_cloud_directory_rejects_same_named_file_without_creating() -> None:
    file = clouddrive_pb2.CloudDriveFile(
        id='file-id',
        name='ABC',
        fullPathName='/cloud/library/move-in/ABC',
        isDirectory=False,
    )
    get_sub_files = Mock(return_value=[file])
    create_folder = Mock()
    cloud = _async_wrapper(SimpleNamespace(get_sub_files=get_sub_files, create_folder=create_folder))

    result = await cloud.ensure_directory('/cloud/library/move-in', 'ABC')

    assert result == {'success': False, 'created': False, 'path': '/cloud/library/move-in/ABC'}
    create_folder.assert_not_called()


@pytest.mark.parametrize('name', ['', '.', '..', 'nested/ABC', 'bad\\name', 'bad\nname'])
async def test_ensure_cloud_directory_rejects_unsafe_child_name(name: str) -> None:
    cloud = _async_wrapper(SimpleNamespace())

    with pytest.raises(ValueError, match='safe path segment'):
        await cloud.ensure_directory('/cloud/library/move-in', name)


@pytest.mark.parametrize('value', ['relative/path', '//host/path', '/path/../escape', '/path/', '/path\nname'])
async def test_async_clouddrive_rejects_noncanonical_api_paths(value: str) -> None:
    cloud = _async_wrapper(SimpleNamespace())

    with pytest.raises(ValueError, match='canonical absolute POSIX path'):
        await cloud.list_directory(value)


async def test_cancelled_cloud_move_waits_for_sync_call_before_returning() -> None:
    started = threading.Event()
    release = threading.Event()
    close = Mock()

    def move_file(_source: list[str], _destination: str, _policy: int) -> clouddrive_pb2.FileOperationResult:
        started.set()
        assert release.wait(timeout=2)
        msg = 'late gRPC failure'
        raise RuntimeError(msg)

    cloud = _async_wrapper(SimpleNamespace(move_file=move_file, close=close))
    task = asyncio.create_task(
        cloud.move_file(
            '/cloud/library/source-b/ABC/ABC-001.mp4',
            '/cloud/library/move-in/ABC',
        ),
    )
    assert await asyncio.to_thread(started.wait, 1)

    task.cancel()
    await asyncio.sleep(0.01)
    assert not task.done()
    close.assert_not_called()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task


def _offline_file(**overrides: object) -> clouddrive_pb2.OfflineFile:
    fields: dict[str, object] = {
        'name': 'ABC-123',
        'size': 1024,
        'url': 'magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335aa7c1367a88a',
        'status': clouddrive_pb2.OfflineFileStatus.OFFLINE_DOWNLOADING,
        'infoHash': 'c12fe1c06bba254a9dc9f519b335aa7c1367a88a',
        'fileId': '42',
        'add_time': 1_755_000_000,
        'percendDone': 33.33,
        'peers': 7,
    }
    fields.update(overrides)
    return clouddrive_pb2.OfflineFile(**fields)  # type: ignore[arg-type]


def test_list_offline_files_by_path_keeps_every_status(monkeypatch: pytest.MonkeyPatch) -> None:
    result = clouddrive_pb2.OfflineFileListResult(
        offlineFiles=[
            _offline_file(status=clouddrive_pb2.OfflineFileStatus.OFFLINE_DOWNLOADING),
            _offline_file(status=clouddrive_pb2.OfflineFileStatus.OFFLINE_FINISHED),
            _offline_file(status=clouddrive_pb2.OfflineFileStatus.OFFLINE_ERROR),
        ],
    )
    stub = SimpleNamespace(ListOfflineFilesByPath=Mock(return_value=result))
    client = _make_client(monkeypatch, stub)

    files = client.list_offline_files_by_path('/media/tasks')

    statuses = [file.status for file in files]
    assert statuses == [
        clouddrive_pb2.OfflineFileStatus.OFFLINE_DOWNLOADING,
        clouddrive_pb2.OfflineFileStatus.OFFLINE_FINISHED,
        clouddrive_pb2.OfflineFileStatus.OFFLINE_ERROR,
    ]


async def test_offline_task_view_exposes_the_fields_the_tracker_joins_on() -> None:
    result = clouddrive_pb2.OfflineFileListResult(offlineFiles=[_offline_file()])
    cloud = _async_wrapper(SimpleNamespace(list_offline_files_by_path=Mock(return_value=list(result.offlineFiles))))

    tasks = await cloud.list_offline_files('/media/tasks')

    assert tasks == (
        {
            'name': 'ABC-123',
            'size': 1024,
            'url': 'magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335aa7c1367a88a',
            'status': OfflineStatus.DOWNLOADING,
            'info_hash': 'C12FE1C06BBA254A9DC9F519B335AA7C1367A88A',
            'file_id': '42',
            'add_time': 1_755_000_000,
            'progress': pytest.approx(33.33),
            'peers': 7,
        },
    )


async def test_offline_task_hash_matches_the_hash_parsed_from_our_magnet() -> None:
    task = _offline_file()
    cloud = _async_wrapper(SimpleNamespace(list_offline_files_by_path=Mock(return_value=[task])))

    tasks = await cloud.list_offline_files('/media/tasks')

    assert tasks[0]['info_hash'] == extract_info_hash(task.url)
