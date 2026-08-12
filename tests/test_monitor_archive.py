import logging
from pathlib import Path

import pytest

from embyx_manager.config.models import ArchiveConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.archive import (
    ArchivePipeline,
    is_4k_video,
    multi_part_video_check,
    normalize_copy_suffix,
    remove_00,
)
from embyx_manager.monitor.reports import RunContext


def make_ctx() -> RunContext:
    return RunContext(logger=logging.getLogger('test-archive'))


def make_pipeline(tmp_path: Path, **overrides) -> ArchivePipeline:
    values = {
        'enabled': True,
        'src_dir': str(tmp_path / 'task'),
        'dst_dir': str(tmp_path / 'library'),
        'mapping': {'intake': 'sorted'},
        'min_size_mb': 0,
        'brand_mapping': {},
    }
    values.update(overrides)
    config = ArchiveConfig(**values)
    (tmp_path / 'task' / 'intake').mkdir(parents=True, exist_ok=True)
    (tmp_path / 'library' / 'sorted').mkdir(parents=True, exist_ok=True)
    return ArchivePipeline(config=config, avid_parser=AvidParser())


@pytest.mark.parametrize(
    ('avid', 'expected'),
    [
        ('ABC-00123', 'ABC-123'),
        ('ABC-123', 'ABC-123'),
        ('ABC-0012', 'ABC-0012'),
    ],
)
def test_remove_00(avid: str, expected: str) -> None:
    assert remove_00(avid) == expected


def test_multi_part_video_check() -> None:
    assert multi_part_video_check([Path('a-1.mp4'), Path('a-2.mp4')]) is True
    assert multi_part_video_check([Path('a-A.mp4'), Path('a-B.mp4')]) is True
    assert multi_part_video_check([Path('a-1.mp4'), Path('b-extra.mp4')]) is False
    with pytest.raises(ValueError, match='only one video file'):
        multi_part_video_check([Path('a.mp4')])


def test_is_4k_video() -> None:
    assert is_4k_video(Path('ABC-123 4k.mp4')) is True
    assert is_4k_video(Path('ABC-123-4K.mp4')) is True
    assert is_4k_video(Path('ABC-1234k.mp4')) is False
    assert is_4k_video(Path('4k.mp4')) is False
    assert is_4k_video(Path('ABC-123.mp4')) is False


def test_normalize_copy_suffix() -> None:
    assert normalize_copy_suffix('ABC-123 (1)') == 'ABC-123'
    assert normalize_copy_suffix('ABC-123') == 'ABC-123'


def write_video(path: Path, size: int = 10) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'0' * size)
    return path


def test_flatten_moves_single_video_and_removes_folder(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    intake = pipeline.src_dir / 'intake'
    write_video(intake / 'ABC-123 release' / 'ABC-123.mp4')

    pipeline.flatten(intake, pipeline.dst_dir / 'sorted', make_ctx())

    assert (intake / 'ABC-123.mp4').exists()
    assert not (intake / 'ABC-123 release').exists()


def test_flatten_skips_folder_with_multiple_avids(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    intake = pipeline.src_dir / 'intake'
    write_video(intake / 'mixed' / 'ABC-123.mp4')
    write_video(intake / 'mixed' / 'DEF-456.mp4')

    pipeline.flatten(intake, pipeline.dst_dir / 'sorted', make_ctx())

    assert (intake / 'mixed' / 'ABC-123.mp4').exists()
    assert (intake / 'mixed' / 'DEF-456.mp4').exists()


def test_flatten_moves_multi_part_videos(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    intake = pipeline.src_dir / 'intake'
    write_video(intake / 'set' / 'ABC-123-1.mp4')
    write_video(intake / 'set' / 'ABC-123-2.mp4')

    pipeline.flatten(intake, pipeline.dst_dir / 'sorted', make_ctx())

    assert (intake / 'ABC-123-1.mp4').exists()
    assert (intake / 'ABC-123-2.mp4').exists()


def test_flatten_prefers_4k_variant(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    intake = pipeline.src_dir / 'intake'
    write_video(intake / 'dual' / 'ABC-123.mp4')
    write_video(intake / 'dual' / 'ABC-123 4k.mp4')

    pipeline.flatten(intake, pipeline.dst_dir / 'sorted', make_ctx())

    assert (intake / 'ABC-123 4k.mp4').exists()
    assert not (intake / 'ABC-123.mp4').exists()
    assert not (intake / 'dual').exists()


def test_flatten_honors_min_size(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path, min_size_mb=1)
    intake = pipeline.src_dir / 'intake'
    write_video(intake / 'small' / 'ABC-123.mp4', size=100)

    pipeline.flatten(intake, pipeline.dst_dir / 'sorted', make_ctx())

    assert (intake / 'small' / 'ABC-123.mp4').exists()


def test_flatten_deletes_junk_folder_when_video_already_archived(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path, min_size_mb=1)
    intake = pipeline.src_dir / 'intake'
    write_video(intake / 'ABC-123 junk' / 'ad.mp4', size=10)
    write_video(pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4', size=10)

    pipeline.flatten(intake, pipeline.dst_dir / 'sorted', make_ctx())

    assert not (intake / 'ABC-123 junk').exists()


def test_flatten_drops_duplicate_copies(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    intake = pipeline.src_dir / 'intake'
    write_video(intake / 'dup' / 'ABC-123.mp4', size=50)
    write_video(intake / 'dup' / 'ABC-123 (1).mp4', size=50)

    pipeline.flatten(intake, pipeline.dst_dir / 'sorted', make_ctx())

    assert (intake / 'ABC-123.mp4').exists()
    assert not (intake / 'ABC-123 (1).mp4').exists()


def test_rename_to_avid_and_multi_part(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    intake = pipeline.src_dir / 'intake'
    write_video(intake / '[site] abc-123 special.mp4')
    write_video(intake / 'def-456 part1.mp4')
    write_video(intake / 'def-456 part2.mp4')

    pipeline.rename(intake, make_ctx())

    assert (intake / 'ABC-123.mp4').exists()
    assert (intake / 'DEF-456-cd1.mp4').exists()
    assert (intake / 'DEF-456-cd2.mp4').exists()


def test_rename_conflict_raises_before_any_rename(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    intake = pipeline.src_dir / 'intake'
    write_video(intake / 'abc-123 raw.mp4')
    # A directory occupying the target name is not collected as a video,
    # so the planned rename target already exists and must fail atomically.
    (intake / 'ABC-123.mp4').mkdir()

    with pytest.raises(FileExistsError):
        pipeline.rename(intake, make_ctx())

    assert (intake / 'abc-123 raw.mp4').exists()


def test_two_files_with_same_avid_become_multi_part(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    intake = pipeline.src_dir / 'intake'
    write_video(intake / 'abc-123 raw.mp4')
    write_video(intake / 'ABC-123.mp4')

    pipeline.rename(intake, make_ctx())

    assert (intake / 'ABC-123-cd1.mp4').exists()
    assert (intake / 'ABC-123-cd2.mp4').exists()


def test_clear_dirname_rewrites_video_suffix_folders(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    intake = pipeline.src_dir / 'intake'
    (intake / 'ABC-123.mp4').mkdir(parents=True)

    pipeline.clear_dirname(intake, make_ctx())

    assert (intake / 'ABC-123-mp4').is_dir()
    assert not (intake / 'ABC-123.mp4').exists()


def test_archive_moves_video_to_brand_directory(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    intake = pipeline.src_dir / 'intake'
    sorted_dir = pipeline.dst_dir / 'sorted'
    write_video(intake / 'ABC-123.mp4')

    pipeline.archive(intake, sorted_dir, make_ctx())

    assert (sorted_dir / 'ABC' / 'ABC-123.mp4').exists()


def test_archive_respects_brand_mapping(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path, brand_mapping={'special': ('ABC',)})
    intake = pipeline.src_dir / 'intake'
    sorted_dir = pipeline.dst_dir / 'sorted'
    write_video(intake / 'ABC-123.mp4')

    pipeline.archive(intake, sorted_dir, make_ctx())

    assert (pipeline.dst_dir / 'special' / 'ABC' / 'ABC-123.mp4').exists()


def test_archive_skips_existing_destination(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    intake = pipeline.src_dir / 'intake'
    sorted_dir = pipeline.dst_dir / 'sorted'
    write_video(intake / 'ABC-123.mp4')
    write_video(sorted_dir / 'ABC' / 'ABC-123.mp4')

    pipeline.archive(intake, sorted_dir, make_ctx())

    assert (intake / 'ABC-123.mp4').exists()


def test_full_run_processes_each_mapping(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr('embyx_manager.monitor.archive.POST_MUTATION_SLEEP_SECONDS', 0)
    pipeline = make_pipeline(tmp_path)
    intake = pipeline.src_dir / 'intake'
    write_video(intake / 'ABC-123 release' / 'abc-123 hd.mp4')

    ctx = make_ctx()
    pipeline.run(ctx)

    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    assert ctx.stats['folders_flattened'] == 1
    assert ctx.stats['videos_renamed'] == 1
    assert ctx.stats['videos_archived'] == 1
