import logging
from pathlib import Path

import pytest

from embyx_manager.config.models import ArchiveConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.archive import (
    ArchivePipeline,
    Outcome,
    _isolate,
    is_4k_video,
    multi_part_video_check,
    normalize_copy_suffix,
)
from embyx_manager.monitor.reports import RunCancelledError, RunContext


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


def make_priority_pipeline(tmp_path: Path, **overrides) -> ArchivePipeline:
    """Pipeline with a priority route vip -> starred alongside the normal intake -> sorted."""
    overrides.setdefault('priority_mapping', {'vip': 'starred'})
    pipeline = make_pipeline(tmp_path, **overrides)
    (tmp_path / 'task' / 'vip').mkdir(parents=True, exist_ok=True)
    (tmp_path / 'library' / 'starred').mkdir(parents=True, exist_ok=True)
    return pipeline


def write_video(path: Path, size: int = 10) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'x' * size)
    return path


def archive(pipeline: ArchivePipeline, folder: Path, dst: str = 'sorted', **kwargs):
    return pipeline.archive_folder(folder, dst, make_ctx(), **kwargs)


# -- helpers ------------------------------------------------------------------


def test_multi_part_video_check() -> None:
    assert multi_part_video_check([Path('a-1.mp4'), Path('a-2.mp4')]) is True
    assert multi_part_video_check([Path('a-A.mp4'), Path('a-B.mp4')]) is True
    assert multi_part_video_check([Path('a-1.mp4'), Path('b-extra.mp4')]) is False
    # A lone video is simply not a multi-part set; callers no longer guard the call.
    assert multi_part_video_check([Path('a.mp4')]) is False
    assert multi_part_video_check([]) is False


def test_is_4k_video() -> None:
    assert is_4k_video(Path('ABC-123 4k.mp4')) is True
    assert is_4k_video(Path('ABC-123-4K.mp4')) is True
    assert is_4k_video(Path('ABC-123-2160p.mp4')) is True
    assert is_4k_video(Path('ABC-1234k.mp4')) is False
    assert is_4k_video(Path('ABC-123.mp4')) is False


def test_normalize_copy_suffix() -> None:
    assert normalize_copy_suffix('ABC-123 (1)') == 'ABC-123'
    assert normalize_copy_suffix('ABC-123') == 'ABC-123'


def test_isolate_contains_a_failure_and_records_it() -> None:
    ctx = make_ctx()
    error = RuntimeError('nope')
    with _isolate(ctx, 'boom for %s', 'thing'):
        raise error
    assert ctx.stats['items_failed'] == 1


def test_isolate_lets_cancellation_through() -> None:
    ctx = make_ctx()
    with pytest.raises(RunCancelledError), _isolate(ctx, 'boom'):
        raise RunCancelledError


# -- archiving one folder -----------------------------------------------------


def test_folder_is_archived_under_its_brand_and_then_deleted(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'ABC-123 release'
    write_video(folder / 'ABC-123.mp4')

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert result.avid == 'ABC-123'
    assert result.archived_paths == ('sorted/ABC/ABC-123.mp4',)
    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    assert not folder.exists()


def test_video_is_renamed_after_its_avid(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'release'
    write_video(folder / '[somesite] abc-123 1080p.mp4')

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()


def test_nested_video_is_found_rather_than_deleted_with_the_folder(tmp_path: Path) -> None:
    # Only direct children used to count, while the folder was removed
    # recursively, so a video one level down was deleted as junk.
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'ABC-123 release'
    write_video(folder / 'inner' / 'ABC-123.mp4')

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    assert not folder.exists()


def test_multi_part_set_becomes_cd_parts(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'set'
    write_video(folder / 'ABC-123-1.mp4')
    write_video(folder / 'ABC-123-2.mp4')

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123-cd1.mp4').exists()
    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123-cd2.mp4').exists()
    assert result.archived_paths == ('sorted/ABC/ABC-123-cd1.mp4', 'sorted/ABC/ABC-123-cd2.mp4')


def test_high_resolution_cut_wins_over_the_smaller_one(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'dual'
    write_video(folder / 'ABC-123.mp4')
    write_video(folder / 'ABC-123 4k.mp4')

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert result.archived_paths == ('sorted/ABC/ABC-123.mp4',)
    assert not folder.exists()


def test_duplicate_copies_are_dropped(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'dupes'
    write_video(folder / 'ABC-123.mp4')
    write_video(folder / 'ABC-123 (1).mp4')

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()


def test_folder_without_a_qualifying_video_is_junk(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path, min_size_mb=1)
    folder = pipeline.src_dir / 'intake' / 'ads'
    write_video(folder / 'ABC-123.mp4', size=100)

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.JUNK
    # Junk is reported, not deleted here: the caller decides, because for a
    # tracked download it also means "try the next magnet".
    assert folder.exists()


def test_several_unrelated_videos_need_attention(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'mixed'
    write_video(folder / 'ABC-123.mp4')
    write_video(folder / 'DEF-456.mp4')

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.NEEDS_ATTENTION
    assert 'multiple avids' in result.reason
    # Nothing is moved or deleted while the identity is in doubt.
    assert (folder / 'ABC-123.mp4').exists()
    assert (folder / 'DEF-456.mp4').exists()


def test_unreadable_avid_needs_attention(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'whatever'
    write_video(folder / 'movie.mp4')

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.NEEDS_ATTENTION
    assert folder.exists()


def test_folder_name_supplies_the_avid_when_the_file_name_does_not(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'ABC-123'
    write_video(folder / 'movie.mp4')

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()


def test_expected_avid_mismatch_is_reported_and_nothing_moves(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'surprise'
    write_video(folder / 'DEF-456.mp4')

    result = archive(pipeline, folder, expected_avid='ABC-123')

    assert result.outcome is Outcome.NEEDS_ATTENTION
    assert result.reason == 'expected ABC-123 but found DEF-456'
    assert (folder / 'DEF-456.mp4').exists()


def test_expected_avid_match_archives_normally(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'download'
    write_video(folder / 'ABC-123.mp4')

    result = archive(pipeline, folder, expected_avid='ABC-123')

    assert result.outcome is Outcome.ARCHIVED
    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()


def test_existing_destination_is_left_alone(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    write_video(pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4', size=999)
    folder = pipeline.src_dir / 'intake' / 'again'
    write_video(folder / 'ABC-123.mp4')

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.NEEDS_ATTENTION
    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4').stat().st_size == 999
    assert folder.exists()


def test_brand_mapping_overrides_the_route_destination(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path, brand_mapping={'special': ('ABC',)})
    folder = pipeline.src_dir / 'intake' / 'release'
    write_video(folder / 'ABC-123.mp4')

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert (pipeline.dst_dir / 'special' / 'ABC' / 'ABC-123.mp4').exists()


def test_failure_while_archiving_is_reported_not_raised(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'release'
    write_video(folder / 'ABC-123.mp4')

    def explode(*_args: object, **_kwargs: object) -> None:
        msg = 'mount went away'
        raise OSError(msg)

    monkeypatch.setattr(Path, 'rename', explode)
    result = archive(pipeline, folder)

    assert result.outcome is Outcome.FAILED
    assert folder.exists()


# -- priority routes ----------------------------------------------------------


def test_priority_route_promotes_a_copy_out_of_the_normal_destination(tmp_path: Path) -> None:
    pipeline = make_priority_pipeline(tmp_path)
    write_video(pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4')
    folder = pipeline.src_dir / 'vip' / 'release'
    write_video(folder / 'ABC-123-4k.mp4')

    ctx = make_ctx()
    result = pipeline.archive_folder(folder, 'starred', ctx, priority=True)

    assert (pipeline.dst_dir / 'starred' / 'ABC' / 'ABC-123.mp4').exists()
    assert not (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    assert ctx.stats['duplicates_promoted'] == 1
    # The promoted copy now occupies the destination, so the newly arrived one is
    # left for a human rather than overwriting what was just rescued.
    assert result.outcome is Outcome.NEEDS_ATTENTION
    assert folder.exists()


def test_priority_promotion_moves_every_part_of_a_set(tmp_path: Path) -> None:
    pipeline = make_priority_pipeline(tmp_path)
    write_video(pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123-cd1.mp4')
    write_video(pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123-cd2.mp4')
    folder = pipeline.src_dir / 'vip' / 'release'
    write_video(folder / 'DEF-456.mp4')

    pipeline.archive_folder(folder, 'starred', make_ctx(), priority=True)
    # Promotion is keyed on the avid being archived, so a different avid moves nothing.
    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123-cd1.mp4').exists()

    other = pipeline.src_dir / 'vip' / 'abc'
    write_video(other / 'ABC-123.mp4')
    pipeline.archive_folder(other, 'starred', make_ctx(), priority=True)

    assert (pipeline.dst_dir / 'starred' / 'ABC' / 'ABC-123-cd1.mp4').exists()
    assert (pipeline.dst_dir / 'starred' / 'ABC' / 'ABC-123-cd2.mp4').exists()


def test_promotion_does_not_claim_a_longer_avid(tmp_path: Path) -> None:
    pipeline = make_priority_pipeline(tmp_path)
    write_video(pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-1234.mp4')
    folder = pipeline.src_dir / 'vip' / 'release'
    write_video(folder / 'ABC-123.mp4')

    pipeline.archive_folder(folder, 'starred', make_ctx(), priority=True)

    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-1234.mp4').exists()


def test_promotion_keeps_both_when_the_target_exists(tmp_path: Path) -> None:
    pipeline = make_priority_pipeline(tmp_path)
    write_video(pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4', size=11)
    write_video(pipeline.dst_dir / 'starred' / 'ABC' / 'ABC-123.mp4', size=22)
    folder = pipeline.src_dir / 'vip' / 'release'
    write_video(folder / 'ABC-123.mp4')

    result = pipeline.archive_folder(folder, 'starred', make_ctx(), priority=True)

    assert result.outcome is Outcome.NEEDS_ATTENTION
    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4').stat().st_size == 11
    assert (pipeline.dst_dir / 'starred' / 'ABC' / 'ABC-123.mp4').stat().st_size == 22


def test_normal_route_skips_an_avid_the_priority_route_holds(tmp_path: Path) -> None:
    pipeline = make_priority_pipeline(tmp_path)
    write_video(pipeline.dst_dir / 'starred' / 'ABC' / 'ABC-123.mp4')
    folder = pipeline.src_dir / 'intake' / 'release'
    write_video(folder / 'ABC-123.mp4')

    result = pipeline.archive_folder(folder, 'sorted', make_ctx())

    assert result.outcome is Outcome.NEEDS_ATTENTION
    assert not (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    assert folder.exists()


def test_brand_mapped_brands_ignore_the_priority_guards(tmp_path: Path) -> None:
    pipeline = make_priority_pipeline(tmp_path, brand_mapping={'special': ('ABC',)})
    write_video(pipeline.dst_dir / 'starred' / 'ABC' / 'ABC-123.mp4')
    folder = pipeline.src_dir / 'intake' / 'release'
    write_video(folder / 'ABC-123.mp4')

    result = pipeline.archive_folder(folder, 'sorted', make_ctx())

    assert result.outcome is Outcome.ARCHIVED
    assert (pipeline.dst_dir / 'special' / 'ABC' / 'ABC-123.mp4').exists()


# -- the full scan ------------------------------------------------------------


def test_run_archives_every_route(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path, mapping={'intake': 'sorted', 'extra': 'other'})
    write_video(pipeline.src_dir / 'intake' / 'one' / 'ABC-123.mp4')
    write_video(pipeline.src_dir / 'extra' / 'two' / 'DEF-456.mp4')

    pipeline.run(make_ctx())

    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    assert (pipeline.dst_dir / 'other' / 'DEF' / 'DEF-456.mp4').exists()


def test_run_takes_priority_routes_first(tmp_path: Path) -> None:
    pipeline = make_priority_pipeline(tmp_path)
    write_video(pipeline.src_dir / 'intake' / 'normal' / 'ABC-123.mp4')
    write_video(pipeline.src_dir / 'vip' / 'better' / 'ABC-123.mp4')

    pipeline.run(make_ctx())

    # The priority copy lands first, so the normal one finds the avid taken.
    assert (pipeline.dst_dir / 'starred' / 'ABC' / 'ABC-123.mp4').exists()
    assert not (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()


def test_run_files_loose_videos_left_in_a_route(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    write_video(pipeline.src_dir / 'intake' / 'ABC-123.mp4')

    pipeline.run(make_ctx())

    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()


def test_run_treats_a_missing_route_source_as_nothing_to_do(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path, mapping={'intake': 'sorted', 'gone': 'elsewhere'})

    ctx = make_ctx()
    pipeline.run(ctx)

    assert ctx.stats.get('items_failed') is None


def test_run_continues_after_one_folder_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pipeline = make_pipeline(tmp_path)
    write_video(pipeline.src_dir / 'intake' / 'aaa-bad' / 'ABC-123.mp4')
    write_video(pipeline.src_dir / 'intake' / 'zzz-good' / 'DEF-456.mp4')
    original = Path.rglob

    def explode(self: Path, pattern: str):
        if self.name == 'aaa-bad':
            msg = 'mount went away'
            raise OSError(msg)
        return original(self, pattern)

    monkeypatch.setattr(Path, 'rglob', explode)
    ctx = make_ctx()
    pipeline.run(ctx)

    assert ctx.stats['items_failed'] == 1
    assert (pipeline.dst_dir / 'sorted' / 'DEF' / 'DEF-456.mp4').exists()


def test_run_stops_when_cancelled(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    write_video(pipeline.src_dir / 'intake' / 'one' / 'ABC-123.mp4')
    ctx = make_ctx()
    ctx.request_stop()

    with pytest.raises(RunCancelledError):
        pipeline.run(ctx)


def test_padded_content_id_archives_under_the_same_avid_rss_would_record(tmp_path: Path) -> None:
    # RSS derived the AVID without un-padding while the archiver un-padded it, so
    # the same release could be recorded as ABC-00123 and filed as ABC-123. Both
    # now read it through one parser.
    pipeline = make_pipeline(tmp_path)
    parser = AvidParser()
    folder = pipeline.src_dir / 'intake' / 'release'
    write_video(folder / 'abc00123.mp4')

    result = archive(pipeline, folder)

    assert parser.get_avid('[sukebei] abc00123 1080p') == 'ABC-123'
    assert result.avid == 'ABC-123'
    assert (pipeline.dst_dir / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
