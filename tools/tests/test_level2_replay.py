"""The level 2 decision replay and GET differ (tools/level2), on synthetic data only."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import replay

ROOT = Path(__file__).resolve().parents[2]


def _export(directory: Path) -> Path:
    avid = {'id_exceptions': ['T28-555'], 'ignored_id_patterns': [r'(?i:hhd800\.com@)']}
    archive = {
        'dst_dir': '/115//library/',
        'mapping': {'clt': 'collection', 'vr': 'vr', 'x': 'collection'},
        'brand_mapping': {'priority': ['SSIS']},
    }
    (directory / 'app_config.jsonl').write_text(
        ''.join(json.dumps({'section': name, 'value_json': json.dumps(value)}) + '\n' for name, value in (('avid', avid), ('archive', archive))),
    )  # fmt: skip
    (directory / 'archive_acquisitions.jsonl').write_text(
        json.dumps({'avid': 'SSIS-001', 'archived_paths_json': '["/115/library/priority/SSIS/SSIS-001-cd2.mp4"]',
                    'replace_paths_json': None, 'task_dir_path': '/downloads/hhd800.com@ABP-123'}) + '\n'
    )  # fmt: skip
    (directory / 'archive_magnet_attempts.jsonl').write_text(
        json.dumps({'magnet': 'magnet:?xt=urn:btih:00&dn=T28-555.HD'}) + '\n'
    )
    (directory / 'playlists.jsonl').write_text(
        json.dumps({'entries_json': json.dumps([[1, 'SSIS-001', 'a title n1234']])}) + '\n'
    )
    return directory


def test_names_come_from_every_table_once(tmp_path: Path) -> None:
    assert replay.names(_export(tmp_path)) == [
        'SSIS-001', 'hhd800.com@ABP-123', 'SSIS-001-cd2.mp4', 'T28-555.HD', 'a title n1234',
    ]  # fmt: skip


def test_python_decisions_follow_the_stored_rules_and_routes(tmp_path: Path) -> None:
    directory = _export(tmp_path)
    decisions = replay.python_decisions(replay.sections(directory), replay.names(directory))
    by_input = {decision['input']: decision for decision in decisions}
    assert by_input['SSIS-001-cd2.mp4']['routes'] == ['/115/library/priority/SSIS', '/115/library/priority/SSIS']
    assert by_input['hhd800.com@ABP-123']['routes'] == ['/115/library/collection/ABP', '/115/library/vr/ABP']
    assert by_input['T28-555.HD']['avid'] == 'T28-555'
    assert by_input['a title n1234'] == {
        'input': 'a title n1234', 'avid': 'N1234', 'brand': None, 'routes': [None, None],
        'log': ['WARNING: failed to get brand for N1234, skipping find_dst'] * 2,
    }  # fmt: skip


def test_compare_reports_each_differing_name() -> None:
    left = [{'input': 'a', 'avid': 'A-1'}, {'input': 'b', 'avid': 'B-1'}]
    right = [{'input': 'a', 'avid': 'A-1'}, {'input': 'b', 'avid': 'B-2'}]
    report = replay.compare(left, right)
    assert (report['equal'], [item['input'] for item in report['different']]) == (1, ['b'])
    assert replay.compare(left, right[:1])['different'][0]['rust'] == '1 answers'


@pytest.mark.skipif(not os.environ.get('EMBYX_L2_REPLAY_BINARY'), reason='needs the built `replay` example')
def test_the_rust_example_decides_like_python(tmp_path: Path) -> None:
    directory = _export(tmp_path)
    texts, inputs = replay.sections(directory), replay.names(directory)
    rust = replay.rust_decisions(os.environ['EMBYX_L2_REPLAY_BINARY'], texts, inputs)
    assert replay.compare(replay.python_decisions(texts, inputs), rust)['different'] == []


def test_get_diff_reports_json_paths_and_drops_ignored_keys() -> None:
    import get_diff  # noqa: PLC0415 - imports the benchmark's server helpers

    left = get_diff.strip({'a': 1, 'items': [{'x': 1, 'checked_at': 'then'}], 'only': 0}, frozenset({'checked_at'}))
    right = get_diff.strip({'a': 1, 'items': [{'x': 2, 'checked_at': 'now'}]}, frozenset({'checked_at'}))
    assert get_diff.differences(left, right) == [
        {'path': '$.items[0].x', 'python': '1', 'rust': '2'},
        {'path': '$.only', 'only': 'python'},
    ]
    assert get_diff.differences([1, 2], [1]) == [{'path': '$', 'python': '[1, 2]', 'rust': '[1]'}]
    assert get_diff.differences({'on': True, 'n': 5}, {'on': 1, 'n': 5.0}) == [
        {'path': '$.on', 'python': 'true', 'rust': '1'},
        {'path': '$.n', 'python': '5', 'rust': '5.0'},
    ]


def test_get_diff_takes_clock_values_within_the_window_as_equal() -> None:
    import get_diff  # noqa: PLC0415 - imports the benchmark's server helpers

    left = {'scanned_at': '2026-11-14T17:34:24.640766Z', 'job': {'progress': {'elapsed_seconds': 2554974, 'done': 3}}}
    right = {'scanned_at': '2026-11-14T17:34:29.338770Z', 'job': {'progress': {'elapsed_seconds': 2554979, 'done': 3}}}
    assert get_diff.differences(left, right, window=10.0) == []
    # Without a window, and beyond it, a clock value is compared like any other.
    assert [item['path'] for item in get_diff.differences(left, right)] == [
        '$.scanned_at',
        '$.job.progress.elapsed_seconds',
    ]
    assert [item['path'] for item in get_diff.differences(left, right, window=4.0)] == [
        '$.scanned_at',
        '$.job.progress.elapsed_seconds',
    ]
    # Only the clock keys, only values of one type, and never a missing key.
    assert get_diff.differences({'done': 3}, {'done': 4}, window=10.0) == [
        {'path': '$.done', 'python': '3', 'rust': '4'}
    ]
    assert get_diff.differences({'elapsed_seconds': 5}, {'elapsed_seconds': 5.0}, window=10.0) == [
        {'path': '$.elapsed_seconds', 'python': '5', 'rust': '5.0'},
    ]
    assert get_diff.differences({'elapsed_seconds': None}, {'elapsed_seconds': 3}, window=10.0) == [
        {'path': '$.elapsed_seconds', 'python': 'null', 'rust': '3'},
    ]
    assert get_diff.differences({'scanned_at': 'then'}, {'scanned_at': 'now'}, window=10.0) == [
        {'path': '$.scanned_at', 'python': '"then"', 'rust': '"now"'},
    ]
    assert get_diff.differences({'scanned_at': 'x'}, {}, window=10.0) == [{'path': '$.scanned_at', 'only': 'python'}]


def test_get_diff_percent_encodes_stored_ids() -> None:
    import get_diff  # noqa: PLC0415 - imports the benchmark's server helpers

    paths = get_diff.item_paths({'playlist': ['k4:第一届 奖/2021'], 'avid': ['ABC-123'], 'run': ['a b']})
    assert paths == [
        '/api/monitor/acquisitions/ABC-123',
        '/api/monitor/replace?avid=ABC-123',
        '/api/monitor/runs/a%20b',
        '/api/playlists/k4%3A%E7%AC%AC%E4%B8%80%E5%B1%8A%20%E5%A5%96%2F2021/missing',
        '/api/playlists/k4%3A%E7%AC%AC%E4%B8%80%E5%B1%8A%20%E5%A5%96%2F2021/fill',
    ]


def test_the_tools_run_as_scripts() -> None:
    for script in ('replay.py', 'get_diff.py', 'mask.py'):
        done = subprocess.run(
            [sys.executable, str(ROOT / 'tools' / 'level2' / script), '--help'],
            capture_output=True, check=False, cwd=ROOT, timeout=120,
        )  # fmt: skip
        assert done.returncode == 0, done.stderr.decode()
