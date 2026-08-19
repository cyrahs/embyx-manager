from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from embyx_manager.adapters import (
    AvidBrandResolver,
    CloudDriveFileMover,
    CloudDriveUnconfiguredError,
    JavBusActorCatalog,
    LedgerAcquisitionGateway,
)
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


async def test_acquisition_gateway_queues_and_wakes_the_tracker() -> None:
    intake = SimpleNamespace(queue=AsyncMock(return_value=IntakeOutcome.QUEUED))
    nudges: list[bool] = []
    gateway = LedgerAcquisitionGateway(  # type: ignore[arg-type, return-value]
        lambda: intake,
        task_dir=lambda: '/115/fill',
        on_queued=lambda: nudges.append(True),
    )

    assert await gateway.queue_missing('ABC-001') is AcquisitionOutcome.QUEUED
    assert intake.queue.await_args.args == ('ABC-001',)
    assert intake.queue.await_args.kwargs['source'] is AcquisitionSource.FILL_ACTOR
    assert intake.queue.await_args.kwargs['task_dir_path'] == '/115/fill'
    assert nudges == [True]


async def test_acquisition_gateway_does_not_wake_the_tracker_for_tracked_avids() -> None:
    intake = SimpleNamespace(queue=AsyncMock(return_value=IntakeOutcome.ALREADY_TRACKED))
    nudges: list[bool] = []
    gateway = LedgerAcquisitionGateway(  # type: ignore[arg-type, return-value]
        lambda: intake,
        task_dir=lambda: '/115/fill',
        on_queued=lambda: nudges.append(True),
    )

    assert await gateway.queue_missing('ABC-001') is AcquisitionOutcome.ALREADY_TRACKED
    assert nudges == []


async def test_acquisition_gateway_reports_unconfigured_clouddrive() -> None:
    gateway = LedgerAcquisitionGateway(lambda: None, task_dir=lambda: '/115/fill')

    assert await gateway.queue_missing('ABC-001') is AcquisitionOutcome.CLOUD_NOT_CONFIGURED


async def test_acquisition_gateway_reports_missing_offline_directory() -> None:
    intake = SimpleNamespace(queue=AsyncMock(return_value=IntakeOutcome.QUEUED))
    gateway = LedgerAcquisitionGateway(lambda: intake, task_dir=lambda: '')  # type: ignore[arg-type, return-value]

    assert await gateway.queue_missing('ABC-001') is AcquisitionOutcome.TASK_DIR_NOT_CONFIGURED
    intake.queue.assert_not_awaited()


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
