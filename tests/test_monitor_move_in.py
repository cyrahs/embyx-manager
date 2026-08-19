import logging
from pathlib import Path

from embyx_manager.config.models import ArchiveConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.acquisitions import AcquisitionSource, AcquisitionState
from embyx_manager.monitor.archive import ArchivePipeline
from embyx_manager.monitor.move_in import MoveInSweeper
from embyx_manager.monitor.reports import RunContext
from tests.test_monitor_rss import FakeLedger


def make_ctx() -> RunContext:
    return RunContext(logger=logging.getLogger('test-move-in'))


def write_video(path: Path, size: int = 10) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'x' * size)


def build(tmp_path: Path, *, task_dir_path: str = '/task/intake') -> tuple[MoveInSweeper, FakeLedger]:
    config = ArchiveConfig(
        enabled=True,
        src_dir=str(tmp_path / 'task'),
        dst_dir=str(tmp_path / 'library'),
        mapping={'intake': 'sorted'},
        min_size_mb=0,
    )
    (tmp_path / 'task' / 'intake').mkdir(parents=True, exist_ok=True)
    (tmp_path / 'library' / 'sorted').mkdir(parents=True, exist_ok=True)
    (tmp_path / 'movein').mkdir(parents=True, exist_ok=True)
    archiver = ArchivePipeline(config=config, avid_parser=AvidParser())
    ledger = FakeLedger()
    sweeper = MoveInSweeper(
        ledger=ledger,
        archiver=archiver,
        avid_parser=AvidParser(),
        move_in_root=tmp_path / 'movein',
        task_dir_path=task_dir_path,
    )
    return sweeper, ledger


async def test_staged_videos_are_archived_and_recorded(tmp_path: Path) -> None:
    sweeper, ledger = build(tmp_path)
    write_video(tmp_path / 'movein' / 'ABC' / 'ABC-123.mp4')
    write_video(tmp_path / 'movein' / 'DEF-456.mp4')

    await sweeper.run(make_ctx())

    assert (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    assert (tmp_path / 'library' / 'sorted' / 'DEF' / 'DEF-456.mp4').exists()
    assert not (tmp_path / 'movein' / 'ABC' / 'ABC-123.mp4').exists()
    assert ledger.states['ABC-123'] is AcquisitionState.ARCHIVED
    assert ledger.states['DEF-456'] is AcquisitionState.ARCHIVED
    assert ledger.sources['ABC-123'] is AcquisitionSource.FILL_ACTOR
    # The emptied brand directory stages nothing and is deliberately left behind.
    assert (tmp_path / 'movein' / 'ABC').is_dir()


async def test_a_multi_part_set_travels_together(tmp_path: Path) -> None:
    sweeper, _ = build(tmp_path)
    write_video(tmp_path / 'movein' / 'ABC' / 'ABC-123-cd1.mp4')
    write_video(tmp_path / 'movein' / 'ABC' / 'ABC-123-cd2.mp4')

    await sweeper.run(make_ctx())

    assert (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123-cd1.mp4').exists()
    assert (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123-cd2.mp4').exists()


async def test_a_held_avid_is_parked_and_its_file_left(tmp_path: Path) -> None:
    sweeper, ledger = build(tmp_path)
    write_video(tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4')
    write_video(tmp_path / 'movein' / 'ABC' / 'ABC-123.mp4')

    first = make_ctx()
    await sweeper.run(first)
    second = make_ctx()
    await sweeper.run(second)

    assert (tmp_path / 'movein' / 'ABC' / 'ABC-123.mp4').exists()
    assert ledger.states['ABC-123'] is AcquisitionState.NEEDS_ATTENTION
    # The warning fires when the AVID is parked, not on every later pass.
    assert first.stats.get('needs_attention') == 1
    assert second.stats.get('needs_attention') is None


async def test_an_unrouted_staging_tree_moves_nothing(tmp_path: Path) -> None:
    sweeper, ledger = build(tmp_path, task_dir_path='/somewhere/else')
    write_video(tmp_path / 'movein' / 'ABC' / 'ABC-123.mp4')

    await sweeper.run(make_ctx())

    assert (tmp_path / 'movein' / 'ABC' / 'ABC-123.mp4').exists()
    assert not ledger.states


async def test_a_missing_task_dir_moves_nothing(tmp_path: Path) -> None:
    sweeper, ledger = build(tmp_path, task_dir_path='')
    write_video(tmp_path / 'movein' / 'ABC-123.mp4')

    await sweeper.run(make_ctx())

    assert (tmp_path / 'movein' / 'ABC-123.mp4').exists()
    assert not ledger.states


async def test_an_unreadable_file_is_reported_once_and_left(tmp_path: Path) -> None:
    sweeper, _ = build(tmp_path)
    write_video(tmp_path / 'movein' / 'holiday footage.mp4')

    first = make_ctx()
    await sweeper.run(first)
    second = make_ctx()
    await sweeper.run(second)

    assert (tmp_path / 'movein' / 'holiday footage.mp4').exists()
    assert first.stats.get('needs_attention') == 1
    assert second.stats.get('needs_attention') is None


async def test_repeated_runs_are_idempotent(tmp_path: Path) -> None:
    sweeper, ledger = build(tmp_path)
    write_video(tmp_path / 'movein' / 'ABC' / 'ABC-123.mp4')

    for _ in range(3):
        await sweeper.run(make_ctx())

    assert (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    assert ledger.states['ABC-123'] is AcquisitionState.ARCHIVED
