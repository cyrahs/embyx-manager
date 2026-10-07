import errno
import logging
import os
import shutil
import time
from pathlib import Path

from embyx_manager.config.models import MappingConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.mapping import MappingPipeline
from embyx_manager.monitor.reports import RunContext


def make_ctx() -> RunContext:
    return RunContext(logger=logging.getLogger('test-mapping'))


def make_pipeline(tmp_path: Path) -> MappingPipeline:
    config = MappingConfig(
        enabled=True,
        src_dir=str(tmp_path / 'flat'),
        dst_dir=str(tmp_path / 'emby'),
    )
    (tmp_path / 'flat').mkdir(exist_ok=True)
    return MappingPipeline(config=config, avid_parser=AvidParser())


def write_strm(path: Path, content: str = '/cloud/video.mp4') -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding='utf-8')
    return path


def test_full_sync_maps_strm_into_per_title_directory(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    write_strm(pipeline.src_dir / 'brand' / 'ABC' / 'ABC-123.strm')

    ctx = make_ctx()
    pipeline.run_full(ctx)

    mapped = pipeline.dst_dir / 'brand' / 'ABC' / 'ABC-123' / 'ABC-123.strm'
    assert mapped.read_text(encoding='utf-8') == '/cloud/video.mp4'
    assert ctx.stats['files_updated'] == 1


def test_full_sync_skips_unchanged_files(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    write_strm(pipeline.src_dir / 'ABC-123.strm')
    pipeline.run_full(make_ctx())

    ctx = make_ctx()
    pipeline.run_full(ctx)

    assert ctx.stats.get('files_updated') is None
    assert ctx.stats['files_skipped'] == 1


def test_full_sync_deletes_strays_and_empty_dirs(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    source = write_strm(pipeline.src_dir / 'brand' / 'ABC-123.strm')
    pipeline.run_full(make_ctx())
    source.unlink()

    ctx = make_ctx()
    pipeline.run_full(ctx)

    assert not (pipeline.dst_dir / 'brand').exists()
    assert ctx.stats['files_deleted'] == 1
    assert ctx.stats['dirs_deleted'] >= 1


def test_full_sync_overwrites_when_source_newer(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    source = write_strm(pipeline.src_dir / 'ABC-123.strm', 'old')
    pipeline.run_full(make_ctx())
    time.sleep(0.01)
    source.write_text('new', encoding='utf-8')
    future = time.time() + 5
    os.utime(source, (future, future))

    pipeline.run_full(make_ctx())

    mapped = pipeline.dst_dir / 'ABC-123' / 'ABC-123.strm'
    assert mapped.read_text(encoding='utf-8') == 'new'


def test_incremental_update_and_delete(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    kept = write_strm(pipeline.src_dir / 'brand' / 'ABC-123.strm')
    removed = write_strm(pipeline.src_dir / 'brand' / 'DEF-456.strm')
    pipeline.run_full(make_ctx())
    removed.unlink()

    ctx = make_ctx()
    failed_changed, failed_deleted = pipeline.run_incremental(
        ctx,
        changed={kept},
        deleted={removed},
    )

    assert failed_changed == set()
    assert failed_deleted == set()
    assert (pipeline.dst_dir / 'brand' / 'ABC-123' / 'ABC-123.strm').exists()
    assert not (pipeline.dst_dir / 'brand' / 'DEF-456').exists()


def test_incremental_reports_failed_paths(tmp_path: Path, monkeypatch) -> None:
    pipeline = make_pipeline(tmp_path)
    changed = write_strm(pipeline.src_dir / 'ABC-123.strm')

    def boom(*_args, **_kwargs):
        msg = 'disk detached'
        raise OSError(msg)

    monkeypatch.setattr(pipeline, 'update_one', boom)

    failed_changed, failed_deleted = pipeline.run_incremental(make_ctx(), changed={changed}, deleted=set())

    assert failed_changed == {changed}
    assert failed_deleted == set()


def test_tokyo_hot_separated_id_maps_to_canonical_directory(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    write_strm(pipeline.src_dir / 'rest' / 'N' / 'N-0893.strm')

    ctx = make_ctx()
    pipeline.run_full(ctx)

    assert (pipeline.dst_dir / 'rest' / 'N' / 'N0893' / 'N-0893.strm').exists()
    assert not any('failed to get avid' in line for line in ctx.log_tail)


def test_non_avid_strm_is_skipped_with_warning(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    write_strm(pipeline.src_dir / '!!!.strm')

    ctx = make_ctx()
    pipeline.run_full(ctx)

    assert not any(pipeline.dst_dir.glob('**/*.strm'))
    assert any('failed to get avid' in line for line in ctx.log_tail)


def test_full_sync_skips_dir_that_is_not_empty_on_disk(tmp_path: Path, monkeypatch) -> None:
    """A stale NFS directory cache lists a directory as empty while the server
    still holds its .strm; rmtree then fails with ENOTEMPTY. That one directory
    must be skipped, not abort the run, and the other empty dirs still go."""
    pipeline = make_pipeline(tmp_path)
    stale = write_strm(pipeline.src_dir / 'stale' / 'STALE-001.strm')
    gone = write_strm(pipeline.src_dir / 'gone' / 'GONE-001.strm')
    pipeline.run_full(make_ctx())
    stale.unlink()
    gone.unlink()

    stale_dir = pipeline.dst_dir / 'stale' / 'STALE-001'
    real_rmtree = shutil.rmtree

    def fake_rmtree(path, *args, **kwargs):
        if Path(path) == stale_dir:
            raise OSError(errno.ENOTEMPTY, 'Directory not empty', str(path))
        real_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(shutil, 'rmtree', fake_rmtree)

    ctx = make_ctx()
    pipeline.run_full(ctx)

    assert stale_dir.exists()
    assert not (pipeline.dst_dir / 'gone').exists()
    assert ctx.stats['dirs_skipped'] == 1
    assert ctx.stats['dirs_deleted'] >= 1
    assert ctx.errors == ()
    assert any('not empty on disk' in line and 'STALE-001' in line for line in ctx.log_tail)


def test_full_sync_reports_other_delete_errors_and_continues(tmp_path: Path, monkeypatch) -> None:
    pipeline = make_pipeline(tmp_path)
    write_strm(pipeline.src_dir / 'a' / 'AAA-001.strm').unlink()
    write_strm(pipeline.src_dir / 'b' / 'BBB-001.strm')
    pipeline.run_full(make_ctx())
    (pipeline.src_dir / 'b' / 'BBB-001.strm').unlink()
    (pipeline.dst_dir / 'locked' / 'LOCK-001').mkdir(parents=True)
    real_rmtree = shutil.rmtree

    def fake_rmtree(path, *args, **kwargs):
        if Path(path).name == 'LOCK-001':
            raise OSError(errno.EACCES, 'Permission denied', str(path))
        real_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(shutil, 'rmtree', fake_rmtree)

    ctx = make_ctx()
    pipeline.run_full(ctx)

    assert not (pipeline.dst_dir / 'b').exists()
    assert ctx.stats['dirs_skipped'] == 1
    assert len(ctx.errors) == 1
    assert 'LOCK-001' in ctx.errors[0]


def test_incremental_delete_stops_at_dir_that_is_not_empty_on_disk(tmp_path: Path, monkeypatch) -> None:
    pipeline = make_pipeline(tmp_path)
    source = write_strm(pipeline.src_dir / 'brand' / 'ABC-123.strm')
    pipeline.run_full(make_ctx())
    source.unlink()

    title_dir = pipeline.dst_dir / 'brand' / 'ABC-123'
    real_rmdir = Path.rmdir

    def fake_rmdir(self: Path) -> None:
        if self == title_dir:
            raise OSError(errno.ENOTEMPTY, 'Directory not empty', str(self))
        real_rmdir(self)

    monkeypatch.setattr(Path, 'rmdir', fake_rmdir)

    ctx = make_ctx()
    _, failed_deleted = pipeline.run_incremental(ctx, changed=set(), deleted={source})

    assert failed_deleted == set()
    assert not (title_dir / 'ABC-123.strm').exists()
    assert title_dir.exists()
    assert ctx.stats['files_deleted'] == 1
    assert 'dirs_deleted' not in ctx.stats
    assert ctx.errors == ()


def touch(directory: Path, *names: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for name in names:
        (directory / name).write_text('x', encoding='utf-8')


def test_full_sync_deletes_metadata_of_parts_merged_into_one_file(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    write_strm(pipeline.src_dir / 'clt' / 'SQTEVR' / 'SQTEVR-009.strm')
    title = pipeline.dst_dir / 'clt' / 'SQTEVR' / 'SQTEVR-009'
    touch(title, 'SQTEVR-009.nfo', 'SQTEVR-009-mediainfo.json', 'poster.jpg', 'fanart.jpg')
    touch(title, 'SQTEVR-009-cd1.nfo', 'SQTEVR-009-cd1-poster.jpg', 'SQTEVR-009-cd10-landscape.jpg')

    ctx = make_ctx()
    pipeline.run_full(ctx)

    assert sorted(path.name for path in title.iterdir()) == [
        'SQTEVR-009-mediainfo.json',
        'SQTEVR-009.nfo',
        'SQTEVR-009.strm',
        'fanart.jpg',
        'poster.jpg',
    ]
    assert ctx.stats['sidecars_deleted'] == 3


def test_full_sync_keeps_metadata_a_live_strm_claims(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    write_strm(pipeline.src_dir / 'ABC-123-cd1-4K.strm')
    write_strm(pipeline.src_dir / 'ABC-123-cd2.strm')
    title = pipeline.dst_dir / 'ABC-123'
    touch(title, 'ABC-123-cd1-4K.nfo', 'ABC-123-cd2-poster.jpg', 'ABC-123-cd3.nfo')

    pipeline.run_full(make_ctx())

    assert (title / 'ABC-123-cd1-4K.nfo').exists()
    assert (title / 'ABC-123-cd2-poster.jpg').exists()
    assert not (title / 'ABC-123-cd3.nfo').exists()


def test_incremental_delete_takes_the_strms_own_metadata_only(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    base = write_strm(pipeline.src_dir / 'ABC-123.strm')
    part = write_strm(pipeline.src_dir / 'ABC-123-cd1.strm')
    write_strm(pipeline.src_dir / 'ABC-123-cd2.strm')
    pipeline.run_full(make_ctx())
    title = pipeline.dst_dir / 'ABC-123'
    touch(title, 'ABC-123.nfo', 'ABC-123-poster.jpg', 'ABC-123-cd1.nfo', 'ABC-123-cd1-poster.jpg')
    touch(title, 'ABC-123-cd2-poster.jpg', 'fanart.jpg')

    part.unlink()
    pipeline.run_incremental(make_ctx(), changed=set(), deleted={part})
    assert not (title / 'ABC-123-cd1.nfo').exists()
    assert not (title / 'ABC-123-cd1-poster.jpg').exists()
    assert (title / 'ABC-123.nfo').exists()

    base.unlink()
    pipeline.run_incremental(make_ctx(), changed=set(), deleted={base})
    assert sorted(path.name for path in title.iterdir()) == ['ABC-123-cd2-poster.jpg', 'ABC-123-cd2.strm', 'fanart.jpg']


def test_full_sync_moves_a_title_out_of_a_directory_its_id_no_longer_maps_to(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    write_strm(pipeline.src_dir / 'vr' / 'VRKM' / 'VRKM-01468-cd1.strm')
    write_strm(pipeline.src_dir / 'vr' / 'VRKM' / 'VRKM-01468-cd2.strm')
    old = pipeline.dst_dir / 'vr' / 'VRKM' / 'VRKM-01468'
    touch(old, 'VRKM-01468-cd1.strm', 'VRKM-01468-cd2.strm', 'VRKM-01468-cd1.nfo', 'VRKM-01468-cd1-poster.jpg')

    ctx = make_ctx()
    pipeline.run_full(ctx)

    assert not old.exists()
    current = pipeline.dst_dir / 'vr' / 'VRKM' / 'VRKM-1468'
    assert sorted(path.name for path in current.iterdir()) == ['VRKM-01468-cd1.strm', 'VRKM-01468-cd2.strm']
    assert ctx.stats['files_deleted'] == 2


def test_full_sync_keeps_a_strm_whose_id_no_longer_reads(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    write_strm(pipeline.src_dir / 'misc' / 'holiday.strm')
    kept = pipeline.dst_dir / 'misc' / 'HOLIDAY' / 'holiday.strm'
    touch(kept.parent, kept.name)

    pipeline.run_full(make_ctx())

    assert kept.exists()
