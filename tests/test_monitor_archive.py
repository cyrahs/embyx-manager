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


# -- untangling several videos in one folder ----------------------------------
# The naming shapes below are taken verbatim from the reconcile backlog of
# 2026-08-14, where every entry was parked as "several unrelated videos in one
# folder"; sizes mirror the real files in MB.


def test_unnumbered_first_part_leads_its_numbered_siblings(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / '[44x.me]rvg-080'
    write_video(folder / '[44x.me]rvg-080.mp4', size=5486)
    write_video(folder / '[44x.me]rvg-080-2.mp4', size=6165)
    write_video(folder / '[44x.me]rvg-080-3.mp4', size=5825)
    write_video(folder / '[44x.me]rvg-080-4.mp4', size=5824)

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert result.archived_paths == (
        'sorted/RVG/RVG-080-cd1.mp4',
        'sorted/RVG/RVG-080-cd2.mp4',
        'sorted/RVG/RVG-080-cd3.mp4',
        'sorted/RVG/RVG-080-cd4.mp4',
    )
    # The unnumbered file is the first part, although it sorts last by name.
    assert (pipeline.dst_dir / 'sorted' / 'RVG' / 'RVG-080-cd1.mp4').stat().st_size == 5486
    assert (pipeline.dst_dir / 'sorted' / 'RVG' / 'RVG-080-cd2.mp4').stat().st_size == 6165
    assert not folder.exists()


def test_letter_parts_become_cd_parts(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'PPT-057'
    write_video(folder / 'PPT-057A_HD.mp4', size=4712)
    write_video(folder / 'PPT-057B_HD.mp4', size=5865)
    write_video(folder / 'PPT-057C_HD.mp4', size=3764)

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert result.archived_paths == (
        'sorted/PPT/PPT-057-cd1.mp4',
        'sorted/PPT/PPT-057-cd2.mp4',
        'sorted/PPT/PPT-057-cd3.mp4',
    )


def test_lowercase_letter_parts_become_cd_parts(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'dvdms-353'
    write_video(folder / 'dvdms-353a.mp4', size=1752)
    write_video(folder / 'dvdms-353b.mp4', size=1567)

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert result.archived_paths == ('sorted/DVDMS/DVDMS-353-cd1.mp4', 'sorted/DVDMS/DVDMS-353-cd2.mp4')


def test_letter_parts_with_a_quality_marker_and_odd_container(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / '[HD]XV923-WMV'
    write_video(folder / 'xv923A.HD.wmv', size=2650)
    write_video(folder / 'xv923B.HD.wmv', size=2508)

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert result.archived_paths == ('sorted/XV/XV-923-cd1.wmv', 'sorted/XV/XV-923-cd2.wmv')


def test_mixed_index_separators_are_still_one_set(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'TBW-19'
    write_video(folder / 'TBW-19-1.mp4', size=465)
    write_video(folder / 'TBW-19_02.mp4', size=912)

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert result.archived_paths == ('sorted/TBW/TBW-19-cd1.mp4', 'sorted/TBW/TBW-19-cd2.mp4')
    assert (pipeline.dst_dir / 'sorted' / 'TBW' / 'TBW-19-cd1.mp4').stat().st_size == 465


def test_phone_rip_is_dropped_for_the_main_video(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'hjd2048.com-1129nhdtb201-h264'
    write_video(folder / 'hjd2048.com-1129nhdtb201-h264.mp4', size=6527)
    write_video(folder / 'nhdtb201-5.mp4', size=986)

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert result.archived_paths == ('sorted/NHDTB/NHDTB-201.mp4',)
    assert (pipeline.dst_dir / 'sorted' / 'NHDTB' / 'NHDTB-201.mp4').stat().st_size == 6527
    assert not folder.exists()


def test_phone_rip_is_dropped_when_the_main_video_carries_a_tag(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'OKAX449'
    write_video(folder / 'okax449-h264.mp4', size=7196)
    write_video(folder / 'okax449-5.mp4', size=1072)

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert result.archived_paths == ('sorted/OKAX/OKAX-449.mp4',)


def test_oversized_phone_rip_still_needs_attention(tmp_path: Path) -> None:
    # A "-5" file above half the main file's size is not confidently a rip.
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'OKAX449'
    write_video(folder / 'okax449-h264.mp4', size=1000)
    write_video(folder / 'okax449-5.mp4', size=800)

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.NEEDS_ATTENTION
    assert result.reason == 'several unrelated videos in one folder'
    assert (folder / 'okax449-h264.mp4').exists()
    assert (folder / 'okax449-5.mp4').exists()


def test_vr_set_keeps_the_high_resolution_cut_of_each_part(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'urvrsp-430'
    write_video(folder / '4k2.com@urvrsp00430_1_8k.mp4', size=3467)
    write_video(folder / '4k2.com@urvrsp00430_1_hq.mp4', size=1446)
    write_video(folder / '4k2.com@urvrsp00430_2_8k.mp4', size=4067)
    write_video(folder / '4k2.com@urvrsp00430_2_hq.mp4', size=4018)

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.ARCHIVED
    assert result.archived_paths == ('sorted/URVRSP/URVRSP-430-cd1.mp4', 'sorted/URVRSP/URVRSP-430-cd2.mp4')
    assert (pipeline.dst_dir / 'sorted' / 'URVRSP' / 'URVRSP-430-cd1.mp4').stat().st_size == 3467
    assert (pipeline.dst_dir / 'sorted' / 'URVRSP' / 'URVRSP-430-cd2.mp4').stat().st_size == 4067


def test_same_avid_videos_that_read_as_nothing_still_need_attention(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path)
    folder = pipeline.src_dir / 'intake' / 'mystery'
    write_video(folder / 'ABC-123.mp4')
    write_video(folder / 'ABC-123 making of.mp4')

    result = archive(pipeline, folder)

    assert result.outcome is Outcome.NEEDS_ATTENTION
    assert result.reason == 'several unrelated videos in one folder'
    assert (folder / 'ABC-123.mp4').exists()
    assert (folder / 'ABC-123 making of.mp4').exists()


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


def test_a_nested_route_is_not_archived_by_the_route_that_contains_it(tmp_path: Path) -> None:
    """A category downloading into a subdirectory of the shared inbox.

    Without the guard the outer scan treats the inner route as one download:
    it would file a single video out of everything below it and then delete the
    directory, taking the other downloads with it.
    """
    pipeline = make_pipeline(tmp_path, mapping={'intake': 'sorted', 'intake/rank': 'sorted/rank'})
    nested = tmp_path / 'task' / 'intake' / 'rank'
    write_video(nested / 'DEF-456 release' / 'DEF-456.mp4')
    write_video(tmp_path / 'task' / 'intake' / 'ABC-123 release' / 'ABC-123.mp4')

    ctx = make_ctx()
    pipeline.run(ctx)

    assert (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    # The nested route files its own downloads, into its own destination.
    assert (tmp_path / 'library' / 'sorted' / 'rank' / 'DEF' / 'DEF-456.mp4').exists()
    assert nested.is_dir()


def test_a_nested_route_is_skipped_even_when_it_is_deeper(tmp_path: Path) -> None:
    pipeline = make_pipeline(tmp_path, mapping={'intake': 'sorted', 'intake/rank/new': 'sorted/rank'})
    (tmp_path / 'task' / 'intake' / 'rank' / 'new').mkdir(parents=True)
    write_video(tmp_path / 'task' / 'intake' / 'rank' / 'new' / 'DEF-456 release' / 'DEF-456.mp4')

    pipeline.run(make_ctx())

    # 'rank' holds a route without being one, so it is left alone too.
    assert (tmp_path / 'task' / 'intake' / 'rank' / 'new').is_dir()
    assert (tmp_path / 'library' / 'sorted' / 'rank' / 'DEF' / 'DEF-456.mp4').exists()


# -- the library check ---------------------------------------------------------


def test_route_for_task_dir_matches_by_path_suffix(tmp_path: Path) -> None:
    pipeline = make_priority_pipeline(tmp_path)

    assert pipeline.route_for_task_dir('/task/intake') == ('sorted', False)
    assert pipeline.route_for_task_dir('/task/vip') == ('starred', True)
    assert pipeline.route_for_task_dir('/task/unknown') is None
    # The suffix must match in order, not merely share segments.
    assert pipeline.route_for_task_dir('/intake/task') is None


def test_library_holdings_reports_every_copy(tmp_path: Path) -> None:
    pipeline = make_priority_pipeline(tmp_path)
    write_video(tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4')
    write_video(tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123-cd2.mp4')

    held = pipeline.library_holdings('ABC-123', make_ctx(), task_dir_path='/task/intake')

    assert held == ('sorted/ABC/ABC-123-cd2.mp4', 'sorted/ABC/ABC-123.mp4')


def test_library_holdings_is_empty_for_an_absent_avid(tmp_path: Path) -> None:
    pipeline = make_priority_pipeline(tmp_path)
    # A longer AVID sharing the prefix must not count as a copy.
    write_video(tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-1234.mp4')

    assert pipeline.library_holdings('ABC-123', make_ctx(), task_dir_path='/task/intake') == ()


def test_a_priority_task_dir_promotes_the_normal_copy(tmp_path: Path) -> None:
    pipeline = make_priority_pipeline(tmp_path)
    copy = write_video(tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4')

    held = pipeline.library_holdings('ABC-123', make_ctx(), task_dir_path='/task/vip')

    assert held == ('starred/ABC/ABC-123.mp4',)
    assert not copy.exists()
    assert (tmp_path / 'library' / 'starred' / 'ABC' / 'ABC-123.mp4').exists()


def test_a_normal_task_dir_leaves_the_priority_copy_where_it_is(tmp_path: Path) -> None:
    pipeline = make_priority_pipeline(tmp_path)
    copy = write_video(tmp_path / 'library' / 'starred' / 'ABC' / 'ABC-123.mp4')

    held = pipeline.library_holdings('ABC-123', make_ctx(), task_dir_path='/task/intake')

    assert held == ('starred/ABC/ABC-123.mp4',)
    assert copy.exists()


def test_an_unmatched_task_dir_still_finds_the_copy(tmp_path: Path) -> None:
    # No route to promote into, but the one-copy answer is route-independent.
    pipeline = make_priority_pipeline(tmp_path)
    write_video(tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4')

    held = pipeline.library_holdings('ABC-123', make_ctx(), task_dir_path='/elsewhere/entirely')

    assert held == ('sorted/ABC/ABC-123.mp4',)
