"""Python `json.dumps` byte formats that stored data depends on (WP1's JSON writer).

The `*_json` TEXT columns, the merge status files and manifests are written with
`json.dumps`; a Rust writer must produce the same bytes (stored JSON is a cross-version
contract). Each case gives the input as JSON text (what `json.loads` reads) and the exact
output of every format below: `ascii`, `plain` and `sorted_compact_ascii` (the fill
actor's `postgres_repository.py`) are the ones embyx-manager uses; the others come with the
copied helper and are kept so its tests stay whole.

Copied from fav's golden generator (`tools/goldens/areas/json_dumps.py` at the commit named in
docs/CONVENTIONS.md), as the goldens of the helpers copied with it.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import Any

# name -> (call site it mirrors, function)
FORMATS: dict[str, tuple[str, Any]] = {
    'hash': (
        (
            "fav's content hashes: sort_keys, separators=(',', ':'), ensure_ascii=False, default=str "
            '(no embyx-manager call site)'
        ),
        lambda value: json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str),
    ),
    'pretty': (
        (
            "fav's manifests: indent=2, sort_keys, ensure_ascii=False, default=str, plus a newline "
            '(no embyx-manager call site)'
        ),
        lambda value: json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str) + '\n',
    ),
    'compact': (
        "separators=(',', ':'), ensure_ascii=False, insertion order (no embyx-manager call site)",
        lambda value: json.dumps(value, ensure_ascii=False, separators=(',', ':')),
    ),
    'plain': (
        (
            'ensure_ascii=False, default separators, insertion order: monitor/playlists.py ranked entries, '
            'merge/tasks.py parts, merge/worker.py status file'
        ),
        lambda value: json.dumps(value, ensure_ascii=False),
    ),
    'pretty_unsorted': (
        'indent=2, ensure_ascii=False, insertion order, no trailing newline (no embyx-manager call site)',
        lambda value: json.dumps(value, ensure_ascii=False, indent=2),
    ),
    'sorted_plain': (
        'ensure_ascii=False, sort_keys, default separators (no embyx-manager call site)',
        lambda value: json.dumps(value, ensure_ascii=False, sort_keys=True),
    ),
    'sorted_compact_ascii': (
        "separators=(',', ':'), sort_keys, ensure_ascii=True: fill_actor/postgres_repository.py cloud file snapshots",
        lambda value: json.dumps(value, separators=(',', ':'), sort_keys=True),
    ),
    'ascii': (
        (
            'json.dumps defaults (ensure_ascii=True): the other *_json columns (fill_actor, monitor), '
            'merge/service.py manifest, merge/home_worker.py status'
        ),
        json.dumps,
    ),
}

# Lone surrogates are left out on purpose: Python cannot encode them to UTF-8 either, so
# no stored row can contain one.
INPUTS: list[str] = [
    'null',
    'true',
    '0',
    '-17',
    '123456789012345678901234567890',
    '-9223372036854775809',
    '0.1',
    '1.0',
    '-0.0',
    '1e16',
    '1.5e-7',
    '0.0001',
    '0.00001',
    '123456789.123456789',
    '1e300',
    '2.5e-320',
    '3.141592653589793',
    '100.0',
    '1e22',
    '12345678901234567.0',
    '""',
    '"plain ascii"',
    '"quote \\" backslash \\\\ slash /"',
    '"\\b\\f\\n\\r\\t"',
    '"\\u0000\\u0001\\u001f\\u007f\\u0080\\u009f"',
    '"\\u2028\\u2029"',
    '"中文 繁體 日本語 한국어"',
    '"emoji 😀 and 🇨🇳"',
    '"\\ufeffbom"',
    '[]',
    '{}',
    '[[], {}, [{}], {"a": []}]',
    '[1, "two", 3.0, null, true, false]',
    '{"b": 1, "a": 2, "c": {"z": 0, "y": [3, 2, 1]}}',
    '{"B": 1, "a": 2, "_": 3, "1": 4, "": 5}',
    '{"中": 1, "a": 2, "é": 3, "😀": 4, "Z": 5, "ü": 6}',
    '{"\\u2028": "line", "\\ud83d\\ude00": "pair"}',
    '{"nested": {"deeper": {"deepest": [{"k": "v", "j": [1.5, -2, 0.0]}]}}}',
    '{"url": "https://example.com/a?b=1&c=<2>", "path": "collection/bd2/角色/x.png"}',
]


def _dumps_cases() -> list[dict[str, Any]]:
    cases = []
    for text in INPUTS:
        value = json.loads(text)
        cases.append({'input': text, 'outputs': {name: function(value) for name, (_, function) in FORMATS.items()}})
    return cases


def _formats() -> list[dict[str, str]]:
    return [{'name': name, 'mirrors': mirrors} for name, (mirrors, _) in FORMATS.items()]


# `default=str` only matters for values JSON cannot hold. These are the kinds the hashed
# payloads can carry; the Rust side builds the same value from `kind` and `value`.
DEFAULT_STR_VALUES: list[tuple[str, str, object]] = [
    ('datetime', '2026-11-09T22:56:54', datetime(2026, 11, 9, 22, 56, 54)),  # noqa: DTZ001 - naive on purpose
    ('datetime', '2026-11-09T22:56:54.000123', datetime(2026, 11, 9, 22, 56, 54, 123)),  # noqa: DTZ001
    ('datetime', '2026-11-09T22:56:54+00:00', datetime(2026, 11, 9, 22, 56, 54, tzinfo=UTC)),
    (
        'datetime',
        '2026-11-09T22:56:54.5+08:00',
        datetime(2026, 11, 9, 22, 56, 54, 500000, tzinfo=timezone(timedelta(hours=8))),
    ),
    ('date', '2026-11-09', date(2026, 11, 9)),
    ('path', 'collection/nikke/c010/skin.json', PurePosixPath('collection/nikke/c010/skin.json')),
]


# Values `json.loads` can never produce, but call sites can pass: non-string keys (which
# `sort_keys` orders by their own type, so 9 sorts before 10), tuples, and the non-finite
# floats Python writes by default (`allow_nan=True`). `expr` is the Python source of the
# value; the Rust test builds the same value by hand from it.
PYTHON_VALUES: list[tuple[str, object]] = [
    ("{10: 'a', 9: 'b', 100: 'c'}", {10: 'a', 9: 'b', 100: 'c'}),
    ("{-1: 'neg', 0: 'zero', 2: 'two'}", {-1: 'neg', 0: 'zero', 2: 'two'}),
    ("{1.5: 'x', 0.25: 'y'}", {1.5: 'x', 0.25: 'y'}),
    ("{True: 't', False: 'f'}", {True: 't', False: 'f'}),
    ("{None: 'none'}", {None: 'none'}),
    ("{'k': (1, 'two', (3,))}", {'k': (1, 'two', (3,))}),
    ("[float('nan'), float('inf'), float('-inf')]", [float('nan'), float('inf'), float('-inf')]),
    ("{'b': 1.0, 'a': -0.0}", {'b': 1.0, 'a': -0.0}),
]


def _python_value_cases() -> list[dict[str, Any]]:
    return [
        {'expr': expr, 'outputs': {name: function(value) for name, (_, function) in FORMATS.items()}}
        for expr, value in PYTHON_VALUES
    ]


def _default_str_cases() -> list[dict[str, Any]]:
    hash_dumps = FORMATS['hash'][1]
    return [
        {'kind': kind, 'value': text, 'str': str(value), 'hash_of_object': hash_dumps({'at': value, 'n': 1})}
        for kind, text, value in DEFAULT_STR_VALUES
    ]


GOLDENS = {
    'json/formats.json': _formats,
    'json/dumps.json': _dumps_cases,
    'json/default_str.json': _default_str_cases,
    'json/python_values.json': _python_value_cases,
}
