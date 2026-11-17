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


def _answer(body: object, sent: float, done: float, status: int = 200, spawned: float = 0.0) -> dict[str, object]:
    answer = {'status': status, 'content_type': 'application/json', 'body': body}
    return answer | {'sent': sent, 'done': done, 'spawned': spawned}


def test_get_diff_takes_a_clock_value_as_equal_only_when_rust_is_later_by_the_right_time() -> None:
    import get_diff  # noqa: PLC0415 - imports the benchmark's server helpers

    slack = get_diff.CLOCK_SLACK
    # The Python server started at 90 and answered at 100.0-100.2; the Rust one started at 125
    # and answered at 130.0-130.1. A value read while the request ran is 29.8 to 30.1 seconds
    # later in Rust; one read between the start and the request is 24.8 to 40.1 seconds later.
    gaps = get_diff.clock_gaps(_answer(None, 100.0, 100.2, spawned=90.0), _answer(None, 130.0, 130.1, spawned=125.0))
    assert gaps == {
        'request': (130.0 - 100.2 - slack, 130.1 - 100.0 + slack),
        'server': (125.0 - 100.2 - slack, 130.1 - 90.0 + slack),
    }

    left = {'scanned_at': '2031-02-03T04:05:06.100000Z', 'jobs': [{'progress': {'elapsed_seconds': 700, 'last_progress_seconds': 650, 'done': 3}}]}  # fmt: skip
    right = {'scanned_at': '2031-02-03T04:05:41.150000Z', 'jobs': [{'progress': {'elapsed_seconds': 730, 'last_progress_seconds': 681, 'done': 3}}]}  # fmt: skip
    tolerated: list[dict[str, str]] = []
    assert get_diff.differences(left, right, gaps=gaps, tolerated=tolerated) == []
    assert tolerated == [
        {'path': '$.scanned_at', 'python': '2031-02-03T04:05:06.100000Z', 'rust': '2031-02-03T04:05:41.150000Z'},
        {'path': '$.jobs[0].progress.elapsed_seconds', 'python': '700', 'rust': '730'},
        {'path': '$.jobs[0].progress.last_progress_seconds', 'python': '650', 'rust': '681'},
    ]
    # Without the gaps a clock value is compared like any other.
    assert [item['path'] for item in get_diff.differences(left, right)] == [item['path'] for item in tolerated]

    def differs(python: object, rust: object, key: str = 'elapsed_seconds') -> bool:
        return bool(get_diff.differences({key: python}, {key: rust}, gaps=gaps))

    # Too little, too much, earlier, clamped to zero: none is the time between the requests.
    assert all(differs(700, value) for value in (728, 732, 670, 0))
    assert not differs(700, 729)
    assert not differs(700, 731)
    # Two values swapped between the keys are each off by more than the slack.
    assert (
        get_diff.differences(
            {'elapsed_seconds': 700, 'last_progress_seconds': 650},
            {'elapsed_seconds': 680, 'last_progress_seconds': 730},
            gaps=gaps,
        )
        != []
    )
    # One type on both sides, and never a missing key or a null.
    assert differs(700, 730.0)
    assert differs(None, 730)
    assert get_diff.differences({'scanned_at': 'x'}, {}, gaps=gaps) == [{'path': '$.scanned_at', 'only': 'python'}]
    # A time must be written the same way on both sides, in the same zone.
    python_time = '2031-02-03T04:05:06.100000Z'
    for rust_time in (
        '2031-02-03T04:05:36.150000+00:00',
        '2031-02-03T04:05:36Z',
        '2031-02-03T04:05:36.150000',
        '2031-02-03T12:05:36.150000+08:00',
        '2031-02-03',
        'soon',
    ):
        assert differs(python_time, rust_time, 'scanned_at'), rust_time
    assert differs('2031-02-03T04:05:06.100000+00:00', '2031-02-03T12:05:36.150000+08:00', 'scanned_at')
    # The scan time has the wider bracket (server start to request), and no more than that.
    assert not differs(python_time, '2031-02-03T04:05:31.100000Z', 'scanned_at')
    assert not differs(python_time, '2031-02-03T04:05:46.100000Z', 'scanned_at')
    assert differs(python_time, '2031-02-03T04:05:29.100000Z', 'scanned_at')
    assert differs(python_time, '2031-02-03T04:05:48.100000Z', 'scanned_at')
    assert differs(700, 740)
    # Only the clock keys.
    assert get_diff.differences({'done': 3}, {'done': 33}, gaps=gaps) == [
        {'path': '$.done', 'python': '3', 'rust': '33'}
    ]


def test_get_diff_compare_lists_what_the_clock_rule_let_pass_and_every_non_200() -> None:
    import get_diff  # noqa: PLC0415 - imports the benchmark's server helpers

    paths = ['/a', '/b', '/c']
    python = {
        '/a': _answer({'elapsed_seconds': 10, 'n': 1}, 100.0, 100.1),
        '/b': _answer({'elapsed_seconds': 10}, 101.0, 101.1),
        '/c': _answer({'error': 'gone'}, 102.0, 102.1, status=404),
    }
    rust = {
        '/a': _answer({'elapsed_seconds': 30, 'n': 1}, 120.0, 120.1, spawned=110.0),
        # Answered 25 seconds after the Python build, yet 300 seconds ahead.
        '/b': _answer({'elapsed_seconds': 310}, 126.0, 126.1, spawned=110.0),
        '/c': _answer({'error': 'gone'}, 127.0, 127.1, status=404, spawned=110.0),
    }
    report = get_diff.compare(paths, python, rust, frozenset())
    assert report['equal'] == 2
    assert [item['request'] for item in report['different']] == ['/b']
    assert report['clock_tolerated'] == [
        {'request': '/a', 'requests_apart': 20.0, 'path': '$.elapsed_seconds', 'python': '10', 'rust': '30'},
    ]
    assert report['python_statuses'] == {'200': 2, '404': 1}
    assert report['python_non_200'] == [{'request': '/c', 'status': 404}]


def test_get_diff_percent_encodes_stored_ids() -> None:
    import get_diff  # noqa: PLC0415 - imports the benchmark's server helpers

    found = {
        'playlist': ['k4:第一届 奖 2021'],
        'avid': ['ABC-123', 'A&B=1%41?#'],
        'run': ['a b'],
        'plan': ['p1'],
        'job': ['p1', 'j2'],
    }
    assert get_diff.item_paths(found) == [
        '/api/monitor/acquisitions/ABC-123',
        '/api/monitor/replace?avid=ABC-123',
        '/api/monitor/acquisitions/A%26B%3D1%2541%3F%23',
        '/api/monitor/replace?avid=A%26B%3D1%2541%3F%23',
        '/api/monitor/runs/a%20b',
        # A plan id and the job ids, each once.
        '/api/fill-actor/plans/p1',
        '/api/fill-actor/plans/j2',
        '/api/playlists/k4%3A%E7%AC%AC%E4%B8%80%E5%B1%8A%20%E5%A5%96%202021/missing',
        '/api/playlists/k4%3A%E7%AC%AC%E4%B8%80%E5%B1%8A%20%E5%A5%96%202021/fill',
    ]


def test_the_tools_run_as_scripts() -> None:
    for script in ('replay.py', 'get_diff.py', 'mask.py'):
        done = subprocess.run(
            [sys.executable, str(ROOT / 'tools' / 'level2' / script), '--help'],
            capture_output=True, check=False, cwd=ROOT, timeout=120,
        )  # fmt: skip
        assert done.returncode == 0, done.stderr.decode()
