from types import SimpleNamespace
from unittest.mock import AsyncMock

import grpc
import pytest

from embyx_manager.adapters import (
    AvidBrandResolver,
    CloudDriveFileMover,
    CloudDriveUnconfiguredError,
    JavBusActorCatalog,
    LedgerAcquisitionGateway,
)
from embyx_manager.fill_actor.cloud_moves import CloudMoveRefusedError
from embyx_manager.fill_actor.ports import AcquisitionOutcome
from embyx_manager.monitor.acquisitions import AcquisitionSource
from embyx_manager.monitor.intake import IntakeOutcome


async def test_actor_catalog_delegates_to_javbus() -> None:
    client = SimpleNamespace(scrape=AsyncMock(return_value=['ABC-001', 'ABC-002']))
    catalog = JavBusActorCatalog(client)  # type: ignore[arg-type]

    assert await catalog.list_video_ids('actor-1') == ['ABC-001', 'ABC-002']
    client.scrape.assert_awaited_once_with('actor-1', progress_callback=None)


async def test_actor_catalog_forwards_progress_callback() -> None:
    client = SimpleNamespace(scrape=AsyncMock(return_value=[]))
    catalog = JavBusActorCatalog(client)  # type: ignore[arg-type]
    progress = AsyncMock()

    await catalog.list_video_ids('actor-1', progress_callback=progress)
    client.scrape.assert_awaited_once_with('actor-1', progress_callback=progress)


async def test_acquisition_gateway_maps_the_intake_outcome() -> None:
    intake = SimpleNamespace(enqueue=AsyncMock(return_value=IntakeOutcome.SUBMITTED))
    gateway = LedgerAcquisitionGateway(lambda: intake, task_dir=lambda: '/115/fill')  # type: ignore[arg-type, return-value]

    assert await gateway.submit_missing('ABC-001') is AcquisitionOutcome.SUBMITTED
    assert intake.enqueue.await_args.args == ('ABC-001',)
    assert intake.enqueue.await_args.kwargs['source'] is AcquisitionSource.FILL_ACTOR
    assert intake.enqueue.await_args.kwargs['task_dir_path'] == '/115/fill'


async def test_acquisition_gateway_fails_while_clouddrive_is_unconfigured() -> None:
    gateway = LedgerAcquisitionGateway(lambda: None, task_dir=lambda: '/115/fill')

    assert await gateway.submit_missing('ABC-001') is AcquisitionOutcome.SUBMIT_FAILED


async def test_acquisition_gateway_fails_without_an_offline_directory() -> None:
    intake = SimpleNamespace(enqueue=AsyncMock(return_value=IntakeOutcome.SUBMITTED))
    gateway = LedgerAcquisitionGateway(lambda: intake, task_dir=lambda: '')  # type: ignore[arg-type, return-value]

    assert await gateway.submit_missing('ABC-001') is AcquisitionOutcome.SUBMIT_FAILED
    intake.enqueue.assert_not_awaited()


def test_brand_resolver_uses_avid_rules() -> None:
    resolver = AvidBrandResolver()

    assert resolver.resolve_brand('ABC-001') == 'ABC'
    assert resolver.resolve_brand('NOBRAND') is None


async def test_cloud_mover_converts_listing_and_skips_directories() -> None:
    cloud = SimpleNamespace(
        list_directory=AsyncMock(
            return_value=(
                {
                    'id': 'dir-id',
                    'name': 'ABC',
                    'full_path': '/cloud/ABC',
                    'size': 0,
                    'is_directory': True,
                    'write_time': {'seconds': 1, 'nanos': 0},
                    'hashes': {},
                },
                {
                    'id': 'file-id',
                    'name': 'ABC-001.mp4',
                    'full_path': '/cloud/ABC/ABC-001.mp4',
                    'size': 123,
                    'is_directory': False,
                    'write_time': {'seconds': 456, 'nanos': 789},
                    'hashes': {'2': 'sha1-value'},
                },
            ),
        ),
    )
    mover = CloudDriveFileMover(lambda: cloud)  # type: ignore[arg-type, return-value]

    files = await mover.list_directory('/cloud/ABC')

    assert len(files) == 1
    assert files[0].path == '/cloud/ABC/ABC-001.mp4'
    assert files[0].write_time == 456 * 1_000_000_000 + 789


async def test_cloud_mover_rejects_missing_write_time() -> None:
    cloud = SimpleNamespace(
        list_directory=AsyncMock(
            return_value=(
                {
                    'id': 'file-id',
                    'name': 'ABC-001.mp4',
                    'full_path': '/cloud/ABC/ABC-001.mp4',
                    'size': 123,
                    'is_directory': False,
                    'write_time': None,
                    'hashes': {},
                },
            ),
        ),
    )
    mover = CloudDriveFileMover(lambda: cloud)  # type: ignore[arg-type, return-value]

    with pytest.raises(TypeError, match='invalid write time'):
        await mover.list_directory('/cloud/ABC')


async def test_cloud_mover_ensure_directory_returns_success_flag() -> None:
    cloud = SimpleNamespace(
        ensure_directory=AsyncMock(return_value={'success': True, 'created': False, 'path': '/cloud/ABC'}),
    )
    mover = CloudDriveFileMover(lambda: cloud)  # type: ignore[arg-type, return-value]

    assert await mover.ensure_directory('/cloud', 'ABC') is True
    cloud.ensure_directory.assert_awaited_once_with('/cloud', 'ABC')


async def test_cloud_mover_move_file_maps_response() -> None:
    cloud = SimpleNamespace(
        move_file=AsyncMock(
            return_value={
                'success': True,
                'error_message': '',
                'result_file_paths': ('/cloud/dst/ABC-001.mp4',),
            },
        ),
    )
    mover = CloudDriveFileMover(lambda: cloud)  # type: ignore[arg-type, return-value]

    response = await mover.move_file('/cloud/src/ABC-001.mp4', '/cloud/dst')

    assert response.success is True
    assert response.result_paths == ('/cloud/dst/ABC-001.mp4',)


async def test_cloud_mover_raises_when_unconfigured() -> None:
    mover = CloudDriveFileMover(lambda: None)

    with pytest.raises(CloudDriveUnconfiguredError):
        await mover.list_directory('/cloud/ABC')


class _RpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        super().__init__(details)
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


@pytest.mark.parametrize(
    ('status', 'expected'),
    [
        (grpc.StatusCode.PERMISSION_DENIED, 'cloud_move_permission_denied'),
        (grpc.StatusCode.UNAUTHENTICATED, 'cloud_move_unauthenticated'),
        (grpc.StatusCode.INVALID_ARGUMENT, 'cloud_move_invalid_request'),
        (grpc.StatusCode.UNIMPLEMENTED, 'cloud_move_unsupported'),
    ],
)
async def test_cloud_mover_reports_a_refused_move_as_a_conclusion(
    status: grpc.StatusCode,
    expected: str,
) -> None:
    cloud = SimpleNamespace(move_file=AsyncMock(side_effect=_RpcError(status, 'move permission required')))
    mover = CloudDriveFileMover(lambda: cloud)  # type: ignore[arg-type, return-value]

    with pytest.raises(CloudMoveRefusedError) as refused:
        await mover.move_file('/cloud/src/ABC-001.mp4', '/cloud/dst')

    assert refused.value.code == expected
    assert 'move permission required' in str(refused.value)


@pytest.mark.parametrize(
    'status',
    [grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.INTERNAL],
)
async def test_cloud_mover_leaves_an_ambiguous_failure_ambiguous(status: grpc.StatusCode) -> None:
    """These can arrive after the server already moved the file, so they are not answers."""
    cloud = SimpleNamespace(move_file=AsyncMock(side_effect=_RpcError(status, 'transport failed')))
    mover = CloudDriveFileMover(lambda: cloud)  # type: ignore[arg-type, return-value]

    with pytest.raises(grpc.RpcError) as raised:
        await mover.move_file('/cloud/src/ABC-001.mp4', '/cloud/dst')

    assert not isinstance(raised.value, CloudMoveRefusedError)
