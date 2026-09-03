from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from embyx_manager.adapters import (
    ActorNotFoundError,
    AvidBrandResolver,
    CloudDriveFileMover,
    CloudDriveUnconfiguredError,
    LedgerAcquisitionGateway,
    UnionActorCatalog,
)
from embyx_manager.clients.avbase import AvbaseTalent, AvbaseWork
from embyx_manager.clients.javbus import JavBusActor, JavBusActorPage
from embyx_manager.fill_actor.ports import AcquisitionOutcome
from embyx_manager.monitor.acquisitions import AcquisitionSource
from embyx_manager.monitor.intake import IntakeOutcome


class FakeAvbase:
    def __init__(self, talents: dict[str, AvbaseTalent], pages: list[tuple[int, int, list[AvbaseWork]]]) -> None:
        self.talents = talents
        self.pages = pages
        self.looked_up: list[str] = []

    async def talent(self, name: str) -> AvbaseTalent | None:
        self.looked_up.append(name)
        return self.talents.get(name)

    async def talent_pages(self, _name: str):
        for page in self.pages:
            yield page


class FakeJavBus:
    def __init__(
        self, *, stars: dict[str, list[JavBusActor]], pages: dict[str, JavBusActorPage], catalogs: dict[str, list[str]]
    ) -> None:
        self.stars = stars
        self.pages = pages
        self.catalogs = catalogs
        self.scraped: list[str] = []

    async def search_stars(self, name: str) -> list[JavBusActor]:
        return self.stars.get(name, [])

    async def get_actor(self, actor_id: str) -> JavBusActorPage | None:
        return self.pages.get(actor_id)

    async def scrape(self, actor_id: str, progress_callback=None) -> list[str]:  # noqa: ARG002
        self.scraped.append(actor_id)
        return self.catalogs[actor_id]


SAIKA = AvbaseTalent(talent_id=5022, name='河北彩花', aliases=('河北彩伽',), total_works=2)


def work(work_id: str, day: date | None) -> AvbaseWork:
    return AvbaseWork(work_id=work_id, prefix='', title=work_id, release_date=day, cast=())


async def test_the_union_catalog_joins_avbase_works_with_every_javbus_star_of_the_actor() -> None:
    avbase = FakeAvbase(
        {'河北彩伽': SAIKA, '河北彩花': SAIKA},
        [(1, 2, [work('SONE-100', date(2026, 1, 1))]), (2, 2, [work('SSIS-900', None)])],
    )
    javbus = FakeJavBus(
        stars={
            '河北彩花': [JavBusActor(actor_id='sl1', name='河北彩花'), JavBusActor(actor_id='zzz', name='河北彩花子')],
            '河北彩伽': [JavBusActor(actor_id='new1', name='河北彩伽')],
        },
        pages={},
        catalogs={'sl1': ['SSIS-900', 'OLD-001'], 'new1': ['SONE-100', 'SONE-101']},
    )
    catalog = UnionActorCatalog(avbase=avbase, javbus=javbus)  # type: ignore[arg-type]
    progress = AsyncMock()

    listing = await catalog.list_videos('河北彩伽', progress_callback=progress)

    assert (listing.actor_name, listing.talent_id, listing.aliases) == ('河北彩花', 5022, ('河北彩伽',))
    # AVBase first, then each JavBus star page; the fuzzy search hit under another name is dropped.
    assert listing.video_ids == ('SONE-100', 'SSIS-900', 'OLD-001', 'SONE-101')
    assert listing.release_dates == {'SONE-100': date(2026, 1, 1)}
    assert listing.source_counts == {'avbase': 2, 'javbus:sl1': 2, 'javbus:new1': 2}
    assert sorted(javbus.scraped) == ['new1', 'sl1']
    progress.assert_any_await(1, 2, 1)


async def test_a_javbus_star_id_names_the_actor_through_its_star_page() -> None:
    avbase = FakeAvbase({'河北彩花': SAIKA}, [(1, 1, [work('SONE-100', None)])])
    javbus = FakeJavBus(
        stars={'河北彩花': [JavBusActor(actor_id='sl1', name='河北彩花')]},
        pages={'sl1': JavBusActorPage(actor_id='sl1', name='河北彩花', video_ids=('SSIS-900',))},
        catalogs={'sl1': ['SSIS-900']},
    )
    catalog = UnionActorCatalog(avbase=avbase, javbus=javbus)  # type: ignore[arg-type]

    listing = await catalog.list_videos('sl1')

    assert listing.talent_id == 5022
    assert avbase.looked_up == ['sl1', '河北彩花']
    assert listing.video_ids == ('SONE-100', 'SSIS-900')


async def test_an_actor_neither_catalog_knows_is_an_error() -> None:
    catalog = UnionActorCatalog(  # type: ignore[arg-type]
        avbase=FakeAvbase({}, []),
        javbus=FakeJavBus(stars={}, pages={}, catalogs={}),
    )

    with pytest.raises(ActorNotFoundError):
        await catalog.list_videos('nobody')


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
