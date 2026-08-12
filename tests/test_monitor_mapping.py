import logging
import os
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


def test_non_avid_strm_is_skipped_with_warning(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    write_strm(pipeline.src_dir / '!!!.strm')

    ctx = make_ctx()
    pipeline.run_full(ctx)

    assert not any(pipeline.dst_dir.glob('**/*.strm'))
    assert any('failed to get avid' in line for line in ctx.log_tail)
