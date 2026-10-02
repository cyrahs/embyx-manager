"""Multi-part detection over a fake mapping tree, and the routes a title re-enters through."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from embyx_manager.config.models import ArchiveConfig, MappingConfig
from embyx_manager.merge.api import MergeCatalog, create_merge_router
from embyx_manager.merge.detect import (
    SourceBasis,
    TitleProblem,
    resolve_source,
    scan_multipart,
    source_for_library_dir,
    source_for_task_dir,
)

LIBRARY = '/mnt/cd2/115/embyx'

ARCHIVE = ArchiveConfig(
    src_dir='/mnt/cd2/115/embyx_in',
    dst_dir=LIBRARY,
    mapping={'hh': 'type/hh', 'rank': 'rank', 'rest': 'rest', 'special': 'type/special'},
    priority_mapping={'vr': 'type/vr', 'clt': 'actor/clt'},
)


def strm(root: Path, directory: str, name: str, target: str) -> None:
    path = root / directory / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(target + '\n', encoding='utf-8')


def parts(root: Path, directory: str, avid: str, count: int, library_dir: str, *, skip: tuple[int, ...] = ()) -> None:
    brand = avid.split('-', 1)[0]
    for index in range(1, count + 1):
        if index in skip:
            continue
        strm(root, directory, f'{avid}-cd{index}.strm', f'{LIBRARY}/{library_dir}/{brand}/{avid}-cd{index}.mp4')


def test_scan_groups_parts_by_title_and_orders_by_part_count(tmp_path: Path) -> None:
    parts(tmp_path, 'type/vr/SQTEVR/SQTEVR-009', 'SQTEVR-009', 20, 'type/vr')
    parts(tmp_path, 'rank/ABP/ABP-123', 'ABP-123', 3, 'rank')
    strm(tmp_path, 'rank/ABP/ABP-456', 'ABP-456.strm', f'{LIBRARY}/rank/ABP/ABP-456.mp4')
    strm(tmp_path, 'rank/ABP/ABP-789', 'ABP-789-cd1.strm', f'{LIBRARY}/rank/ABP/ABP-789-cd1.mp4')

    titles = scan_multipart(tmp_path, library_root=LIBRARY)

    assert [(title.avid, title.part_count) for title in titles] == [('SQTEVR-009', 20), ('ABP-123', 3)]
    big, small = titles
    assert [part.index for part in big.parts] == list(range(1, 21))
    assert big.parts[9].target == f'{LIBRARY}/type/vr/SQTEVR/SQTEVR-009-cd10.mp4'
    assert big.directory == 'type/vr/SQTEVR/SQTEVR-009'
    assert (big.library_dir, big.brand) == ('type/vr', 'SQTEVR')
    assert not big.stackable
    assert big.complete
    assert small.stackable


def test_scan_reports_missing_parts(tmp_path: Path) -> None:
    parts(tmp_path, 'rest/XYZ/XYZ-001', 'XYZ-001', 12, 'rest', skip=(1, 7))

    (title,) = scan_multipart(tmp_path, library_root=LIBRARY)

    assert title.missing == (1, 7)
    assert not title.complete
    assert not title.stackable


def test_scan_flags_targets_outside_the_library_and_scattered_parts(tmp_path: Path) -> None:
    strm(tmp_path, 'a/OUT-001', 'OUT-001-cd1.strm', '/elsewhere/OUT/OUT-001-cd1.mp4')
    strm(tmp_path, 'a/OUT-001', 'OUT-001-cd2.strm', '/elsewhere/OUT/OUT-001-cd2.mp4')
    strm(tmp_path, 'b/SPL-001', 'SPL-001-cd1.strm', f'{LIBRARY}/rank/SPL/SPL-001-cd1.mp4')
    strm(tmp_path, 'b/SPL-001', 'SPL-001-cd2.strm', f'{LIBRARY}/rest/SPL/SPL-001-cd2.mp4')
    strm(tmp_path, 'c/BAD-001', 'BAD-001-cd1.strm', 'not a path')
    strm(tmp_path, 'c/BAD-001', 'BAD-001-cd2.strm', f'{LIBRARY}/rank/BAD/BAD-001-cd2.mp4')

    problems = {title.avid: title.problem for title in scan_multipart(tmp_path, library_root=LIBRARY)}

    assert problems == {
        'OUT-001': TitleProblem.OUTSIDE_LIBRARY,
        'SPL-001': TitleProblem.SCATTERED_PARTS,
        'BAD-001': TitleProblem.UNREADABLE_STRM,
    }


def test_library_dir_names_the_route_when_exactly_one_feeds_it() -> None:
    assert source_for_library_dir(ARCHIVE, 'type/vr') == 'vr'
    assert source_for_library_dir(ARCHIVE, 'actor/clt') == 'clt'
    assert source_for_library_dir(ARCHIVE, 'rank') == 'rank'
    assert source_for_library_dir(ARCHIVE, 'type/unknown') is None
    shared = ARCHIVE.model_copy(update={'mapping': {**ARCHIVE.mapping, 'hh2': 'type/hh'}})
    assert source_for_library_dir(shared, 'type/hh') is None


def test_task_dir_matches_a_route_by_path_suffix() -> None:
    assert source_for_task_dir(ARCHIVE, '/115/embyx_in/clt') == 'clt'
    assert source_for_task_dir(ARCHIVE, '/115/embyx_in/vr/') == 'vr'
    assert source_for_task_dir(ARCHIVE, '/115/other') is None
    assert source_for_task_dir(ARCHIVE, None) is None


def test_resolve_source_prefers_the_library_and_falls_back_to_the_ledger(tmp_path: Path) -> None:
    parts(tmp_path, 'x/ABC-001', 'ABC-001', 2, 'type/vr')
    parts(tmp_path, 'y/DEF-001', 'DEF-001', 2, 'type/brand-only')
    by_avid = {title.avid: title for title in scan_multipart(tmp_path, library_root=LIBRARY)}

    library = resolve_source(ARCHIVE, by_avid['ABC-001'], '/115/embyx_in/clt')
    ledger = resolve_source(ARCHIVE, by_avid['DEF-001'], '/115/embyx_in/clt')
    unknown = resolve_source(ARCHIVE, by_avid['DEF-001'], None)

    assert (library.source, library.basis) == ('vr', SourceBasis.LIBRARY)
    assert (ledger.source, ledger.basis) == ('clt', SourceBasis.LEDGER)
    assert (unknown.source, unknown.basis) == (None, None)


def make_client(tmp_path: Path, task_dirs: dict[str, str], *, mapping_dst: str | None = None) -> TestClient:
    async def task_dirs_for(avids) -> dict[str, str]:
        return {avid: path for avid, path in task_dirs.items() if avid in avids}

    app = FastAPI()
    app.include_router(
        create_merge_router(
            MergeCatalog(
                archive=lambda: ARCHIVE,
                mapping=lambda: MappingConfig(
                    src_dir='/remote', dst_dir=str(tmp_path) if mapping_dst is None else mapping_dst
                ),
                task_dirs_for=task_dirs_for,
            ),
        ),
    )
    return TestClient(app)


def test_titles_endpoint_lists_titles_with_their_sources(tmp_path: Path) -> None:
    parts(tmp_path, 'type/vr/SQTEVR/SQTEVR-009', 'SQTEVR-009', 20, 'type/vr')
    parts(tmp_path, 'type/x/DEF/DEF-001', 'DEF-001', 2, 'type/x')

    body = make_client(tmp_path, {'DEF-001': '/115/embyx_in/special'}).get('/api/merge/titles').json()

    assert body['routes'] == ['vr', 'clt', 'hh', 'rank', 'rest', 'special']
    assert body['reason'] is None
    assert body['scanned_at'] is not None
    first, second = body['items']
    assert first == {
        'avid': 'SQTEVR-009',
        'directory': 'type/vr/SQTEVR/SQTEVR-009',
        'part_count': 20,
        'parts': list(range(1, 21)),
        'missing': [],
        'library_dir': 'type/vr',
        'brand': 'SQTEVR',
        'problem': None,
        'source': 'vr',
        'source_basis': 'library',
        'stackable': False,
        'mergeable': True,
    }
    assert (second['avid'], second['source'], second['source_basis'], second['stackable']) == (
        'DEF-001',
        'special',
        'ledger',
        True,
    )


def test_titles_endpoint_explains_missing_configuration(tmp_path: Path) -> None:
    body = make_client(tmp_path, {}, mapping_dst='').get('/api/merge/titles').json()

    assert body['items'] == []
    assert body['scanned_at'] is None
    assert body['reason'] == 'mapping.dst_dir and archive.dst_dir must be configured'
