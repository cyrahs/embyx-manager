"""Automatic merging over a fake mapping tree, task store, CloudDrive listing and disk."""

import asyncio
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

from embyx_manager.config.models import MappingConfig, MergeConfig
from embyx_manager.merge.api import AutoState, MergeCatalog, TitleScanCache
from embyx_manager.merge.auto import GIB, AutoMerger
from embyx_manager.merge.tasks import MergeState, MergeTask
from tests.test_merge_api import task
from tests.test_merge_detect import ARCHIVE, LIBRARY, parts

PART = GIB


class FakeRepository:
    def __init__(self) -> None:
        self.tasks: list[MergeTask] = []

    async def create(self, **fields) -> MergeTask:
        created = task(len(self.tasks) + 1, fields['avid'], source=fields['source'], parts=fields['parts'])
        self.tasks.append(created)
        return created

    async def open_tasks(self) -> tuple[MergeTask, ...]:
        return tuple(item for item in self.tasks if item.state not in {MergeState.DONE, MergeState.CANCELLED})

    async def cancelled_avids(self) -> frozenset[str]:
        return frozenset(item.avid for item in self.tasks if item.state == MergeState.CANCELLED)

    async def last_done_at(self) -> datetime | None:
        return max((item.finished_at for item in self.tasks if item.finished_at), default=None)


class FakeService:
    def __init__(self) -> None:
        self.woken = 0

    def unavailable(self) -> str | None:
        return None

    def wake(self) -> None:
        self.woken += 1


class FakeCloud:
    """Lists every part of the mapping tree's titles at ``PART`` bytes each."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.listed: list[str] = []

    async def list_directory(self, api_dir: str, *, force_refresh: bool = True) -> tuple[dict, ...]:
        del force_refresh
        self.listed.append(api_dir)
        names = {
            path.read_text(encoding='utf-8').strip()
            for path in self.root.glob('**/*.strm')
            if str(PurePosixPath(path.read_text(encoding='utf-8').strip()).parent) == f'/mnt/cd2{api_dir}'
        }
        return tuple({'name': PurePosixPath(name).name, 'size': PART, 'is_directory': False} for name in names)


async def no_task_dirs(avids) -> dict[str, str]:
    del avids
    return {}


def make_merger(
    tmp_path: Path,
    repository: FakeRepository,
    *,
    free_gib: int = 100,
    **config: object,
) -> tuple[AutoMerger, FakeService]:
    service = FakeService()
    merge = MergeConfig(auto_enabled=True, **config)
    cloud = FakeCloud(tmp_path)
    merger = AutoMerger(
        repository=repository,  # type: ignore[arg-type]
        catalog=MergeCatalog(
            archive=lambda: ARCHIVE,
            mapping=lambda: MappingConfig(src_dir='/remote', dst_dir=str(tmp_path)),
            task_dirs_for=no_task_dirs,
        ),
        cache=TitleScanCache(),
        service=service,  # type: ignore[arg-type]
        merge_config=lambda: merge,
        cloud=lambda: cloud,  # type: ignore[arg-type,return-value]
        free_space=lambda _path: free_gib * GIB,
    )
    return merger, service


def test_queues_the_title_with_most_parts_and_leaves_stacked_ones(tmp_path: Path) -> None:
    parts(tmp_path, 'type/vr/SQTEVR/SQTEVR-009', 'SQTEVR-009', 20, 'type/vr')
    parts(tmp_path, 'type/vr/VRKM/VRKM-385', 'VRKM-385', 12, 'type/vr')
    parts(tmp_path, 'rank/ABP/ABP-123', 'ABP-123', 3, 'rank')
    repository = FakeRepository()
    merger, service = make_merger(tmp_path, repository)

    asyncio.run(merger.step())

    assert [item.avid for item in repository.tasks] == ['SQTEVR-009']
    assert repository.tasks[0].parts[0] == f'{LIBRARY}/type/vr/SQTEVR/SQTEVR-009-cd1.mp4'
    assert merger.status().state == AutoState.BUSY
    assert service.woken == 1


def test_waits_for_the_open_task_to_be_filed(tmp_path: Path) -> None:
    parts(tmp_path, 'type/vr/SQTEVR/SQTEVR-009', 'SQTEVR-009', 20, 'type/vr')
    parts(tmp_path, 'type/vr/VRKM/VRKM-385', 'VRKM-385', 12, 'type/vr')
    repository = FakeRepository()
    merger, _ = make_merger(tmp_path, repository)
    asyncio.run(merger.step())

    repository.tasks[0] = task(1, 'SQTEVR-009', MergeState.ARCHIVING)
    asyncio.run(merger.step())
    assert len(repository.tasks) == 1
    assert merger.status().state == AutoState.BUSY

    repository.tasks[0] = task(1, 'SQTEVR-009', MergeState.FAILED, failed_state=MergeState.UPLOADING)
    asyncio.run(merger.step())
    assert len(repository.tasks) == 1
    assert merger.status().state == AutoState.PAUSED

    # Filed: its parts left the library, and the next title goes.
    for strm_file in (tmp_path / 'type/vr/SQTEVR/SQTEVR-009').glob('*.strm'):
        strm_file.unlink()
    repository.tasks[0] = task(1, 'SQTEVR-009', MergeState.DONE, finished_at=datetime.now(UTC))
    asyncio.run(merger.step())
    assert [item.avid for item in repository.tasks] == ['SQTEVR-009', 'VRKM-385']


def test_skips_a_title_that_does_not_fit_and_takes_one_that_does(tmp_path: Path) -> None:
    parts(tmp_path, 'type/vr/SQTEVR/SQTEVR-009', 'SQTEVR-009', 20, 'type/vr')
    parts(tmp_path, 'type/vr/VRKM/VRKM-385', 'VRKM-385', 12, 'type/vr')
    repository = FakeRepository()
    # 35 GiB free less the 20 GiB reserve leaves room for 15 parts.
    merger, _ = make_merger(tmp_path, repository, free_gib=35)

    asyncio.run(merger.step())

    assert [item.avid for item in repository.tasks] == ['VRKM-385']
    status = merger.status()
    assert (status.state, status.room, status.skipped_count) == (AutoState.BUSY, 15 * GIB, 1)
    assert (status.skipped[0].avid, status.skipped[0].size, status.skipped[0].reason) == (
        'SQTEVR-009',
        20 * PART,
        'too_big',
    )


def test_stays_idle_when_nothing_fits(tmp_path: Path) -> None:
    parts(tmp_path, 'type/vr/SQTEVR/SQTEVR-009', 'SQTEVR-009', 20, 'type/vr')
    repository = FakeRepository()
    merger, _ = make_merger(tmp_path, repository, free_gib=30)

    asyncio.run(merger.step())

    assert repository.tasks == []
    assert (merger.status().state, merger.status().skipped_count) == (AutoState.IDLE, 1)


def test_takes_stacked_titles_only_when_asked_and_never_a_removed_one(tmp_path: Path) -> None:
    parts(tmp_path, 'type/vr/SQTEVR/SQTEVR-009', 'SQTEVR-009', 20, 'type/vr')
    parts(tmp_path, 'rank/ABP/ABP-123', 'ABP-123', 3, 'rank')
    repository = FakeRepository()
    repository.tasks.append(task(1, 'SQTEVR-009', MergeState.CANCELLED))
    merger, _ = make_merger(tmp_path, repository, auto_include_stackable=True)

    asyncio.run(merger.step())

    assert [item.avid for item in repository.tasks] == ['SQTEVR-009', 'ABP-123']


def test_remembers_titles_that_need_a_source_picked(tmp_path: Path) -> None:
    parts(tmp_path, 'type/x/DEF/DEF-001', 'DEF-001', 12, 'type/x')
    repository = FakeRepository()
    merger, _ = make_merger(tmp_path, repository)

    asyncio.run(merger.step())
    asyncio.run(merger.step())

    assert repository.tasks == []
    status = merger.status()
    assert (status.state, status.skipped[0].reason) == (AutoState.IDLE, 'merge_source_required')


def test_does_nothing_while_switched_off(tmp_path: Path) -> None:
    parts(tmp_path, 'type/vr/SQTEVR/SQTEVR-009', 'SQTEVR-009', 20, 'type/vr')
    repository = FakeRepository()
    merger, _ = make_merger(tmp_path, repository)
    merger._merge_config = MergeConfig  # noqa: SLF001 - the default config has it off

    asyncio.run(merger.step())

    assert repository.tasks == []
    assert merger.status().state == AutoState.OFF
