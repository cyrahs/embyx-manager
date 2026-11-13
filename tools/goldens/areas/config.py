"""The nine config sections (`config/models.py`, `config/store.py`), cross-version contract 2.

A section is stored as `model_dump_json()` bytes in `app_config.value_json`, read back with
`model_validate_json`, and edited by merging submitted values onto `model_dump()` and
running `model_validate`. Python forbids unknown keys, and a stored section that fails
validation reads as its defaults, so the Rust port must accept exactly what pydantic
accepts, coerce it the same way and write the same bytes. Recorded here:

- `config/sections.json`: per section, the field order, the defaults' stored bytes, the
  masked values and secret flags the API shows, and the `configured` properties.
- `config/validate_python.json`: `model_validate(values)` over JSON-shaped input (what a
  merge produces): the stored bytes on success, or pydantic's errors (`type`, `loc`, `msg`).
- `config/validate_json.json`: `model_validate_json(text)` over stored-looking text, the
  `jsonb::text` form the migrations leave included.
- `config/floats.json`: how the one float field is written (pydantic's float format, not
  `repr`'s).
"""

from __future__ import annotations

import json
import math
import random
import struct
from typing import Any

from pydantic import ValidationError

from embyx_manager.config.models import SECTION_MODELS, ConfigSection, MappingConfig
from embyx_manager.config.store import masked_values, secret_flags

INF = math.inf
NAN = math.nan


def _errors(exc: ValidationError) -> list[dict[str, Any]]:
    return [
        {'type': error['type'], 'loc': list(error['loc']), 'msg': error['msg']}
        for error in exc.errors(include_url=False, include_context=False, include_input=False)
    ]


def _properties(section: ConfigSection) -> dict[str, bool]:
    names = ('configured', 'cloud_configured')
    return {name: getattr(section, name) for name in names if hasattr(type(section), name)}


def _view(section: ConfigSection) -> dict[str, Any]:
    return {
        'dump_json': section.model_dump_json(),
        'masked': masked_values(section),
        'secret_flags': secret_flags(section),
        'properties': _properties(section),
    }


def _sections() -> list[dict[str, Any]]:
    return [
        {
            'section': name,
            'model': model.__name__,
            'fields': list(model.model_fields),
            'secret_fields': sorted(model.SECRET_FIELDS),
            **_view(model()),
        }
        for name, model in SECTION_MODELS.items()
    ]


# Values for every field type the sections use (bool, int, float, str, tuple of str, dict of
# str, dict of tuple of str, tuple of RssCategory), lax coercions and refusals alike.
BOOLS: list[Any] = [
    True,
    False,
    0,
    1,
    2,
    -1,
    0.0,
    1.0,
    -0.0,
    0.5,
    NAN,
    'true',
    'True',
    'TRUE',
    'false',
    'yes',
    'YES',
    'no',
    'on',
    'OFF',
    'y',
    'n',
    't',
    'f',
    'T',
    'F',
    '1',
    '0',
    '2',
    '',
    ' yes ',
    'true ',
    '1.0',
    None,
    [],
    {},
    'İ',
    2.5,
    9.2e18,
    9.3e18,
    1e19,
    1e20,
    -1e20,
    1e300,
    2**31,
    2**63 - 1,
    2**63,
    -(2**63),
    -(2**63) - 1,
    2**64,
]
INTS: list[Any] = [
    0,
    1,
    -1,
    5,
    1800,
    True,
    False,
    1.0,
    12.0,
    -0.0,
    1.5,
    INF,
    -INF,
    NAN,
    9.2e18,
    9.3e18,
    -9.3e18,
    1e19,
    '12',
    ' 12 ',
    '+12',
    '-12',
    '1_000',
    '1__0',
    '_1',
    '1_',
    '1.0',
    '1.00',
    '0012.000',
    '1.',
    '1.5',
    '1e3',
    '0x10',
    '0b1',
    '١٢',
    '１',
    '',
    ' ',
    '-',
    '+',
    '+-1',
    '1 0',
    ' 12 ',
    '　12',
    '\x1c12',
    '12\x0b',
    '-0',
    '00',
    'NaN',
    'inf',
    None,
    [],
    {},
    [1],
    '9223372036854775807',
    '-9223372036854775808',
    # Beyond i64, which pydantic takes and the port refuses (DIVERGENCES.md D10).
    '9223372036854775808',
    2**63,
    -(2**63) - 1,
    10**30,
    1e19,
]
FLOATS: list[Any] = [
    0,
    1,
    -1,
    2.0,
    2.5,
    0.1,
    1e16,
    1e-7,
    5e-324,
    -0.0,
    1.5e300,
    True,
    False,
    INF,
    -INF,
    NAN,
    '1.5',
    ' 2 ',
    '.5',
    '5.',
    '+.5',
    '-1e-3',
    '1E3',
    'inf',
    'INF',
    '-Infinity',
    '+inf',
    'nAn',
    'infinity ',
    'infinit',
    '0x1p3',
    '1,5',
    '1_000.5',
    '1e1_0',
    '1__0',
    '_1',
    '1_',
    '',
    ' ',
    'e5',
    '+1',
    '--1',
    '1d',
    '١',
    ' 1.5',
    None,
    [],
    {},
    10**30,
    2**53 + 1,
    123456789.123,
    1e22,
]
STRS: list[Any] = [
    '',
    'x',
    ' x ',
    'ä',
    '\x00',
    '\x01\x7f "\\/',
    'line\nbreak',
    '\U0001f600',
    1,
    1.5,
    True,
    None,
    [],
    {},
    ['x'],
]
STR_TUPLES: list[Any] = [
    [],
    ['a'],
    ['a', 'b'],
    ['a', 'a'],
    [''],
    [' '],
    'a',
    None,
    [1],
    [None],
    [['a']],
    {},
    {'a': 1},
    ['a', 1, None],
]
STR_DICTS: list[Any] = [
    {},
    {'a': 'b'},
    [],
    'x',
    None,
    {'a': 1},
    {'a': ['x']},
    {'a': None},
    {'a': 'b', 'c': 2, 'd': None},
]


def _field_cases() -> list[tuple[str, dict[str, Any]]]:
    cases: list[tuple[str, dict[str, Any]]] = []
    cases += [('rss', {'enabled': value}) for value in BOOLS]
    cases += [('rss', {'interval_seconds': value}) for value in INTS]
    cases += [('mapping', {'debounce_seconds': value}) for value in FLOATS]
    cases += [('mapping', {'src_dir': value}) for value in STRS]
    cases += [('avid', {'id_exceptions': value}) for value in STR_TUPLES]
    cases += [('archive', {'mapping': value}) for value in STR_DICTS]
    cases += [('clouddrive', {'address': value}) for value in STRS]
    return cases


def _section_cases() -> list[tuple[str, dict[str, Any]]]:
    cases: list[tuple[str, dict[str, Any]]] = []
    add = cases.append
    for name in SECTION_MODELS:
        add((name, {}))
        add((name, {'zz': 1}))
        add((name, {'zz': 1, 'aa': None}))
    # clouddrive
    add(('clouddrive', {'address': 'cd.internal:19798', 'api_token': 'secret-token', 'secure': False}))
    add(('clouddrive', {'address': 'cd.internal:19798'}))
    add(('clouddrive', {'api_token': 'x'}))
    add(('clouddrive', {'address': 'clouddrive:19798', 'api_token': 'x', 'task_dir_path': '/115/embyx_in'}))
    add(('clouddrive', {'cloud_name': 'x', 'cloud_account_id': 'y'}))
    add(('clouddrive', {'secure': 'false', 'address': 1, 'api_token': None}))
    # emby
    for address in [
        '',
        'http://embyx.internal:8096/',
        'http://embyx.internal',
        'https://EMBYX.internal/emby//',
        'embyx.internal:8096',
        'http://user:pw@embyx.internal',
        'http://user@embyx.internal',
        'http://:pw@embyx.internal',
        'http://embyx.internal?x=1',
        'http://embyx.internal#frag',
        'http://embyx.internal/?',
        'http://embyx.internal/#',
        'ftp://embyx.internal',
        'http://',
        'http:///path',
        'http://embyx.internal:0',
        'http://embyx.internal:65535',
        'http://embyx.internal:65536',
        'http://embyx.internal:port',
        'http://embyx.internal:',
        'http://[::1]:8096/',
        'http://[::1',
        'http://[1.2.3.4]',
        'HTTP://Host/Path/',
        'http://host/a/b/',
        'http://host//',
        ' http://host',
        'http://host ',
        'http://ho st',
        'http://host/ä',
        'http://höst',
        'http://host:+80',
        'http://host:٨٠',
        'http://host/path;params',
        'http:host',
        'https://host/a?',
        'http://host:080',
        '//host/path',
        'http://\tho\nst/',
        'http://[v1.x]/',
        'http://[::1]x/',
        'http://＃host',
        'http://host%41',
    ]:
        add(('emby', {'address': address, 'api_key': 'k'}))
    add(('emby', {'address': 'http://embyx.internal'}))
    add(('emby', {'api_key': 'k'}))
    # avid
    add(('avid', {'id_exceptions': ['WEIRD-SPECIAL'], 'ignored_id_patterns': ['1080P', 'X1080X']}))
    add(('avid', {'id_exceptions': ('a', 'b'), 'ignored_id_patterns': '(?'}))
    add(('avid', {'ignored_id_patterns': ['(?', '[']}))
    # rss
    add(('rss', {'categories': [{'label': ' Rank ', 'task_dir_path': '/115/embyx_in/rank/'}]}))
    for label in ['', '   ', '　', 'Rank', ' 排行 ', 1, None]:
        add(('rss', {'categories': [{'label': label, 'task_dir_path': '/115/rank'}]}))
    for task_dir in [
        '',
        '   ',
        'embyx_in/rank',
        '/115/./rank//',
        '/115/../rank',
        '/a\x00b',
        '//115/x',
        '///115/x',
        '/',
        ' /x ',
        '/x/..',
        '/..x',
        1,
        None,
    ]:
        add(('rss', {'categories': [{'label': 'Rank', 'task_dir_path': task_dir}]}))
    add(('rss', {'categories': [{'label': 'Rank'}]}))
    add(('rss', {'categories': [{}]}))
    add(('rss', {'categories': [{'task_dir_path': '/x', 'extra': 1}]}))
    add(('rss', {'zz': 1, 'categories': [{'extra': 1, 'task_dir_path': 1}], 'enabled': 'x'}))
    add(('clouddrive', {'bogus': 1, 'address': 1}))
    add(
        (
            'rss',
            {
                'categories': [
                    {'label': 'Rank', 'task_dir_path': '/115/a'},
                    {'label': 'Rank', 'task_dir_path': '/115/b'},
                ]
            },
        )
    )
    add(
        (
            'rss',
            {
                'categories': [
                    {'label': 'B', 'task_dir_path': '/a'},
                    {'label': ' A', 'task_dir_path': '/a'},
                    {'label': 'B ', 'task_dir_path': '/b'},
                    {'label': 'A', 'task_dir_path': '/c'},
                ]
            },
        )
    )
    add(('rss', {'categories': [{'label': "it's", 'task_dir_path': '/a'}, {'label': "it's", 'task_dir_path': '/b'}]}))
    add(
        (
            'rss',
            {
                'categories': [
                    {'label': 'Actor', 'task_dir_path': '/115/embyx_in'},
                    {'label': 'Star', 'task_dir_path': '/115/embyx_in'},
                ]
            },
        )
    )
    add(
        (
            'rss',
            {
                'categories': [
                    'x',
                    None,
                    {'label': 'a', 'task_dir_path': '/x', 'z': 1},
                    {'label': 1, 'task_dir_path': 2},
                ]
            },
        )
    )
    add(('rss', {'categories': 'x'}))
    add(('rss', {'categories': {'label': 'a', 'task_dir_path': '/x'}}))
    add(('rss', {'rank_label': 'Rank'}))
    add(('rss', {'actor_label': 'Actor', 'rank_label': 'Rank', 'enabled': True}))
    add(('rss', {'zz': 1, 'interval_seconds': 'x', 'enabled': 'q'}))
    add(('rss', {'interval_seconds': 0, 'failed_avid_cooldown_seconds': -1}))
    add(('rss', {'interval_seconds': '900', 'failed_avid_cooldown_seconds': 3600.0, 'enabled': 'on'}))
    # archive
    for expr in [
        '',
        'every day at four',
        '0 4 * *',
        '99 4 * * *',
        '  30  3 * * 1 ',
        '0 4 * * 0,2-6',
        '0\t4 * * *',
        '0 4 * * 7',
        '0 4 * * 8',
        '*/0 * * * *',
        '0 4 30 2 *',
        '0 4 31 4,6 *',
        '0 4 L * *',
        '0 4 LW * *',
        '0 4 * * 5L',
        '0 4 * * 1#2',
        '0 4 * * 1#6',
        '0 4 * jan,JUL sun-sat',
        '0 4 * * mon-',
        '@daily',
        '0 0 4 * * *',
        '0 4 * * ?',
        '0 4 15W * *',
        '1-2-3 * * * *',
        '5/2 * * * *',
        '1-10/3 * * * *',
        '0,30/15 * * * *',
        'L/2 * * * *',
        '0 4 L/2 * *',
        '0 4 * * *　',
        '0 4 * * 1,',
        ',0 4 * * *',
        '0 4 * * MON#2',
        '0 4 * * 1L',
        '0 4 * * L',
        '0 4 * * ²L',
        '0 4 ²³ * *',
        '00 04 * * *',
        '-1 4 * * *',
        '0 4 * * 1#0',
        '0 4 * * 1#x',
        '0 4 * * #1',
        '0 4 30,31 2 *',
        '0 4 29 2 *',
        '0 4 1 */2 *',
        0,
        None,
    ]:
        add(('archive', {'scan_cron': expr}))
    add(('archive', {'src_dir': '/downloads', 'dst_dir': '/library', 'mapping': {' intake/ ': './sorted'}}))
    add(('archive', {'src_dir': '/downloads', 'dst_dir': '/library', 'priority_mapping': {' vip/ ': './starred'}}))
    add(
        (
            'archive',
            {
                'src_dir': '/downloads',
                'dst_dir': '/library',
                'mapping': {'intake': 'sorted'},
                'priority_mapping': {'vip': 'sorted'},
            },
        )
    )
    add(('archive', {'brand_mapping': {'special': (' abc ', 'ABC', 'def')}}))
    add(('archive', {'brand_mapping': {'special': ['ß', 'ı', 'ﬁx'], 'other/': ['A'], 'x': 'ABC'}}))
    for values in [
        {'mapping': {'': 'sorted'}},
        {'mapping': {'intake': ''}},
        {'mapping': {'/intake': 'sorted'}},
        {'mapping': {'intake': '../escape'}},
        {'mapping': {'a\x00': 'b'}},
        {'mapping': {'a': 'b/../c'}},
        {'mapping': {'a//b/./': 'c'}},
        {'mapping': {'./': 'c'}},
        {'mapping': {'.': 'c'}},
        {'mapping': {'a/.': 'c', 'a': 'd'}},
        {'mapping': {'x': 'y', 'x/': 'z'}},
        {'brand_mapping': {'special': ()}},
        {'brand_mapping': {'special': ('  ',)}},
        {'brand_mapping': {'/special': ('ABC',)}},
        {'brand_mapping': {'a': ('X',), 'a/': ('Y',)}},
        {'brand_mapping': {'': ('X',)}},
        {'priority_mapping': {'/vip': 'starred'}},
        {'priority_mapping': {'vip': '../escape'}},
        {'priority_mapping': {'vip': 'starred', ' vip/ ': 'other'}},
        {'mapping': {'intake': 'sorted'}, 'priority_mapping': {'./intake': 'starred'}},
        {'mapping': {'b': 'x', 'a': 'y'}, 'priority_mapping': {'a': 'z', 'b': 'w'}},
        {'mapping': {"it's": 'x'}, 'priority_mapping': {"it's": 'z'}},
        {'scan_cron': 'x', 'min_size_mb': -1, 'mapping': {'/a': 'b', 'c': '../d'}},
        {'tracker_interval_seconds': 0, 'stall_timeout_hours': 0, 'max_attempts': 0, 'min_size_mb': 0},
        {'tracker_interval_seconds': 300},
        {'task_dir_local': '/x', 'task_dst': 'y', 'task_priority': 1},
        {'mapping': {'a': 'b'}, 'scan_cron': 'bad'},
        {'src_dir': '/d', 'dst_dir': '/l'},
        {'src_dir': '/d', 'mapping': {'a': 'b'}},
        {'enabled': True, 'src_dir': 'relative', 'dst_dir': 'also relative', 'mapping': {'a': 'b'}},
    ]:
        add(('archive', values))
    # mapping
    add(('mapping', {'src_dir': '/mnt/strm', 'dst_dir': '/mnt/library'}))
    add(('mapping', {'full_sync_interval_seconds': 0}))
    add(('mapping', {'debounce_seconds': -1, 'src_dir': 'relative'}))
    # playlists
    for values in [
        {'source_url': 'https://mirror.test/sql/'},
        {'source_url': ''},
        {'source_url': 'mirror.test'},
        {'source_url': 'https://jinjier.art/sql?x=1'},
        {'source_url': 1},
        {'task_dir_path': '/115/embyx_in/rank/'},
        {'task_dir_path': 'relative/dir'},
        {'task_dir_path': '   '},
        {'interval_seconds': 0},
        {'interval_seconds': -5, 'source_url': '', 'task_dir_path': 'x'},
        {'enabled': True, 'interval_seconds': 3600},
    ]:
        add(('playlists', values))
    # fill_actor
    for values in [
        {'actor_root': 'media/actor'},
        {'actor_root': '/a/../b'},
        {'actor_root': ' /media/actor/ '},
        {'additional_roots': ('/a', '/a')},
        {'additional_roots': ('/a', '/a/')},
        {'additional_roots': ('/a', ' ', '', '/b')},
        {'additional_roots': ('/a', 'b')},
        {'actor_root': '/a', 'additional_roots': ('/a',), 'move_in_root': '/m'},
        {'cloud_move_in_root': '/cloud/in'},
        {'apply_enabled': True},
        {'root_sentinel': 'nested/file'},
        {'root_sentinel': ' .x '},
        {'root_sentinel': ''},
        {'root_sentinel': '..'},
        {'root_sentinel': 'a\x00'},
        {
            'additional_roots': ('/a', '/b'),
            'cloud_strm_mount_prefix': '/mnt',
            'cloud_source_roots': ('/cloud/a',),
            'cloud_move_in_root': '/cloud/in',
        },
        {
            'actor_root': '/media/actor',
            'additional_roots': ['/media/extra'],
            'move_in_root': '/media/in',
            'move_in_by_brand': True,
            'cloud_strm_mount_prefix': '/mnt/cd2/',
            'cloud_source_roots': ['/115/extra'],
            'cloud_move_in_root': '/115/in',
            'apply_enabled': True,
            'task_dir_path': '/115/embyx_in/fill/',
        },
        {'actor_root': '/media/actor', 'additional_roots': ['/media/extra'], 'move_in_root': '/media/in'},
        {'task_dir_path': 'x', 'move_in_root': 'y', 'cloud_strm_mount_prefix': 'z'},
        {'actor_root': '/a', 'additional_roots': ('/a',), 'apply_enabled': True},
    ]:
        add(('fill_actor', values))
    # merge
    for values in [
        {'work_dir': ''},
        {'work_dir': 'relative'},
        {'work_dir': '/downloads/upload/', 'cloud_staging_dir': '/115//upload'},
        {'free_space_reserve_gib': -1},
        {'free_space_reserve_gib': '5', 'auto_enabled': 'yes', 'home_transcode_enabled': 1},
        {'cloud_mount_prefix': '', 'cloud_work_dir': ''},
    ]:
        add(('merge', values))
    return cases


def _validate_python() -> list[dict[str, Any]]:
    cases = []
    for name, values in [*_field_cases(), *_section_cases()]:
        model = SECTION_MODELS[name]
        try:
            result: dict[str, Any] = _view(model.model_validate(values))
        except ValidationError as exc:
            result = {'errors': _errors(exc)}
        cases.append({'section': name, 'values': values, **result})
    for name, value in [('rss', []), ('rss', None), ('rss', 'x'), ('archive', 1)]:
        model = SECTION_MODELS[name]
        try:
            result = _view(model.model_validate(value))
        except ValidationError as exc:
            result = {'errors': _errors(exc)}
        cases.append({'section': name, 'values': value, **result})
    return cases


def _stored_texts() -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = [(name, model().model_dump_json()) for name, model in SECTION_MODELS.items()]
    texts += [
        ('rss', '{"interval_seconds": "12"}'),
        ('rss', '{"enabled": 1, "interval_seconds": true}'),
        ('rss', '{"interval_seconds": 12.0}'),
        ('rss', '{"interval_seconds": 1e2}'),
        ('rss', '{"interval_seconds": 1.5}'),
        ('rss', '{"interval_seconds":1,"interval_seconds":"x"}'),
        ('rss', '{"interval_seconds":"x","interval_seconds":2}'),
        ('rss', '{"a":1,"a":2}'),
        ('rss', '[]'),
        ('rss', '"x"'),
        ('rss', 'null'),
        ('rss', ''),
        ('rss', 'x'),
        ('rss', '{"enabled":true,}'),
        ('rss', '{"enabled":true'),
        ('rss', ' {"enabled": true} '),
        ('rss', '{"enabled": true} x'),
        (
            'rss',
            (
                '{"categories": [{"label": "Actor", "task_dir_path": "/115/embyx_in"}, {"label": "Rank", '
                '"task_dir_path": "/115/embyx_in"}], "enabled": true, "interval_seconds": 900, '
                '"failed_avid_cooldown_seconds": 3600}'
            ),
        ),
        ('rss', '{"categories": ["x"]}'),
        ('rss', '{"categories": [{"label": "a"}]}'),
        ('rss', '{"categories": {"label": "a"}}'),
        ('rss', '{"actor_label": "Actor"}'),
        ('mapping', '{"debounce_seconds": NaN}'),
        ('mapping', '{"debounce_seconds": Infinity}'),
        ('mapping', '{"debounce_seconds": -Infinity}'),
        ('mapping', '{"debounce_seconds": null}'),
        ('mapping', '{"debounce_seconds": 2}'),
        ('mapping', '{"debounce_seconds": 1E400}'),
        ('mapping', '{"debounce_seconds": 0.1e1, "full_sync_interval_seconds": 86400.0}'),
        ('mapping', '{"src_dir": "\\u00e4\\ud83d\\ude00", "dst_dir": "/x"}'),
        ('avid', '{"id_exceptions": ["a"], "ignored_id_patterns": []}'),
        ('avid', '{"id_exceptions": "a"}'),
        ('clouddrive', '{"address": "clouddrive:19798", "api_token": "x", "secure": false}'),
        ('clouddrive', '{"address": "clouddrive:19798", "secure": true, "api_token": "x", "task_dir_path": "/x"}'),
        ('clouddrive', '{"secure": "true"}'),
        ('clouddrive', '{"secure": "yes"}'),
        ('clouddrive', '{"address": 1, "bogus": 1}'),
        ('clouddrive', '{"bogus": 1, "address": 1, "secure": "x", "more": 2}'),
        ('clouddrive', '{"secure": 2}'),
        ('clouddrive', '{"secure": 999999999999999999}'),
        ('clouddrive', '{"secure": 1000000000000000000}'),
        ('clouddrive', '{"secure": -999999999999999999}'),
        ('clouddrive', '{"secure": -1000000000000000000}'),
        ('clouddrive', '{"secure": 9223372036854775807}'),
        ('clouddrive', '{"secure": 1e20}'),
        ('clouddrive', '{"secure": 9.2e18}'),
        ('clouddrive', '{"secure": 2.5}'),
        ('rss', '{"categories": [{"task_dir_path": 1, "extra": 1}], "zz": 1, "enabled": "x"}'),
        ('archive', '{"brand_mapping": {"x": ["a"]}, "mapping": {"a/": "b/"}, "tracker_interval_seconds": 1800}'),
        ('archive', '{"scan_cron": "0  4 * * *"}'),
        ('archive', '{"mapping": {"a": "b"}, "priority_mapping": {"a": "c"}}'),
        ('archive', '{"mapping": {"1": "b"}}'),
        ('archive', '{"mapping": {"a": "b", "a": "c"}}'),
        ('fill_actor', '{"additional_roots": ["/a"], "cloud_source_roots": []}'),
        ('fill_actor', '{"apply_enabled": true}'),
        ('merge', '{"auto_enabled": "true", "free_space_reserve_gib": "20"}'),
        ('emby', '{"address": "http://Host:8096/", "api_key": "k"}'),
        ('playlists', '{"source_url": "https://jinjier.art/sql/"}'),
    ]
    return texts


def _validate_json() -> list[dict[str, Any]]:
    cases = []
    for name, text in _stored_texts():
        model = SECTION_MODELS[name]
        try:
            result: dict[str, Any] = {'dump_json': model.model_validate_json(text).model_dump_json()}
        except ValidationError as exc:
            result = {'errors': _errors(exc)}
        cases.append({'section': name, 'text': text, **result})
    return cases


def _floats() -> list[dict[str, Any]]:
    values = [
        0.0,
        -0.0,
        1.0,
        2.0,
        0.1,
        0.5,
        1.5,
        100.0,
        1e15,
        1e16,
        1e17,
        1e-4,
        1e-5,
        1e-7,
        1e22,
        1e100,
        1e-100,
        5e-324,
        1.7976931348623157e308,
        2.2250738585072014e-308,
        123456789.123,
        1.2345678901234567e19,
        9007199254740993.0,
        0.30000000000000004,
        1 / 3,
        2 / 3,
        1e21,
        1e-6,
        12345678901234567.0,
        4.35,
        0.000123,
        1234567.0,
        1e16 + 2,
    ]
    rng = random.Random(20261113)  # noqa: S311 - fixed inputs, not cryptography
    values += [struct.unpack('<d', struct.pack('<Q', rng.getrandbits(64)))[0] for _ in range(300)]
    values += [rng.uniform(-1000, 1000) for _ in range(100)]
    values += [10.0 ** rng.randint(-30, 30) * rng.random() for _ in range(100)]
    cases = []
    for value in values:
        if math.isnan(value) or math.isinf(value):
            continue
        cases.append(
            {
                'bits': struct.unpack('<Q', struct.pack('<d', value))[0],
                'json': json.dumps(value),
                'dump': MappingConfig(debounce_seconds=value).model_dump_json(),
            }
        )
    return cases


GOLDENS = {
    'config/sections.json': _sections,
    'config/validate_python.json': _validate_python,
    'config/validate_json.json': _validate_json,
    'config/floats.json': _floats,
}
