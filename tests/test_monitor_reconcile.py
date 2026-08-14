import logging
from pathlib import Path

from embyx_manager.config.models import ArchiveConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.acquisitions import AcquisitionSource, AcquisitionState
from embyx_manager.monitor.archive import ArchivePipeline
from embyx_manager.monitor.reconcile import ReconcileScanner
from embyx_manager.monitor.reports import RunContext
from tests.test_monitor_rss import FakeLedger, now_stub


def make_ctx() -> RunContext:
    return RunContext(logger=logging.getLogger('test-reconcile'))


def write_video(path: Path, size: int = 10) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'x' * size)


def build(tmp_path: Path, **overrides) -> tuple[ReconcileScanner, FakeLedger, ArchivePipeline]:
    values = {
        'enabled': True,
        'src_dir': str(tmp_path / 'task'),
        'dst_dir': str(tmp_path / 'library'),
        'mapping': {'intake': 'sorted'},
        'min_size_mb': 0,
    }
    values.update(overrides)
    config = ArchiveConfig(**values)
    (tmp_path / 'task' / 'intake').mkdir(parents=True, exist_ok=True)
    (tmp_path / 'library' / 'sorted').mkdir(parents=True, exist_ok=True)
    archiver = ArchivePipeline(config=config, avid_parser=AvidParser())
    ledger = FakeLedger()
    return ReconcileScanner(ledger=ledger, archiver=archiver, config=config), ledger, archiver


async def test_a_folder_is_archived_only_once_it_has_settled(tmp_path: Path) -> None:
    scanner, ledger, _ = build(tmp_path)
    folder = tmp_path / 'task' / 'intake' / 'ABC-123 release'
    write_video(folder / 'ABC-123.mp4')

    await scanner.run(make_ctx())
    # First sighting only measures it; nothing is moved yet.
    assert folder.exists()
    assert not (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()

    await scanner.run(make_ctx())

    assert (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    assert ledger.states['ABC-123'] is AcquisitionState.ARCHIVED
    assert ledger.sources['ABC-123'] is AcquisitionSource.RECONCILE


async def test_a_growing_folder_is_left_for_the_next_pass(tmp_path: Path) -> None:
    scanner, _, _ = build(tmp_path)
    folder = tmp_path / 'task' / 'intake' / 'ABC-123 release'
    write_video(folder / 'ABC-123.mp4', size=10)

    await scanner.run(make_ctx())
    write_video(folder / 'ABC-123.mp4', size=999)
    await scanner.run(make_ctx())

    assert folder.exists()
    assert not (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()

    await scanner.run(make_ctx())
    assert (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()


async def test_folders_the_tracker_owns_are_left_alone(tmp_path: Path) -> None:
    scanner, ledger, _ = build(tmp_path)
    await ledger.discover('ABC-123', source=AcquisitionSource.RSS_ACTOR, now=now_stub())
    ledger.states['ABC-123'] = AcquisitionState.DOWNLOADING
    folder = tmp_path / 'task' / 'intake' / 'ABC-123 release'
    write_video(folder / 'ABC-123.mp4')

    await scanner.run(make_ctx())
    await scanner.run(make_ctx())

    assert folder.exists()
    assert not (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()


async def test_a_folder_with_two_avids_is_reported_once_and_left_intact(tmp_path: Path) -> None:
    scanner, _, _ = build(tmp_path)
    folder = tmp_path / 'task' / 'intake' / 'mixed'
    write_video(folder / 'ABC-123.mp4')
    write_video(folder / 'DEF-456.mp4')

    await scanner.run(make_ctx())
    second = make_ctx()
    await scanner.run(second)
    third = make_ctx()
    await scanner.run(third)

    # Two AVIDs in one folder means there is no key to park it under, so the
    # scanner remembers it instead; either way the warning fires once, not forever.
    assert second.stats.get('needs_attention') == 1
    assert third.stats.get('needs_attention') is None
    assert (folder / 'ABC-123.mp4').exists()


async def test_repeated_runs_are_idempotent(tmp_path: Path) -> None:
    scanner, ledger, _ = build(tmp_path)
    write_video(tmp_path / 'task' / 'intake' / 'ABC-123 release' / 'ABC-123.mp4')

    for _ in range(4):
        await scanner.run(make_ctx())

    assert (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    assert ledger.states['ABC-123'] is AcquisitionState.ARCHIVED


async def test_a_missing_route_source_is_not_a_failure(tmp_path: Path) -> None:
    scanner, _, _ = build(tmp_path, mapping={'intake': 'sorted', 'gone': 'elsewhere'})

    ctx = make_ctx()
    await scanner.run(ctx)

    assert ctx.stats.get('items_failed') is None


async def test_an_archived_avid_is_not_downgraded_by_a_later_scan(tmp_path: Path) -> None:
    scanner, ledger, _ = build(tmp_path)
    await ledger.discover('ABC-123', source=AcquisitionSource.RSS_ACTOR, now=now_stub())
    ledger.states['ABC-123'] = AcquisitionState.ARCHIVED
    folder = tmp_path / 'task' / 'intake' / 'ABC-123 leftover'
    write_video(folder / 'ABC-123.mp4')

    await scanner.run(make_ctx())
    await scanner.run(make_ctx())

    assert ledger.states['ABC-123'] is AcquisitionState.ARCHIVED
