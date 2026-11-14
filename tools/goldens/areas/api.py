"""The HTTP shell's pure parts (`api.py` over Starlette and FastAPI), WP4.

- `api/mimetypes.json`: CPython's built-in `mimetypes` tables (no system `mime.types` read)
  and what `guess_type` makes of file names, for `FileResponse`'s content type.
- `api/static_paths.json`: `StaticFiles.get_path` and `SpaStaticFiles`' page-path test.
- `api/ranges.json`: `FileResponse._parse_range_header` on `Range` headers.
- `api/parsedate.json`: `email.utils.parsedate`, which `If-Modified-Since` goes through.
- `api/file_headers.json`: `FileResponse`'s `last-modified` and `etag` for a file time.
- `api/content_types.json`: whether FastAPI parses a body with this content type as JSON.
- `api/request_models.json`: what pydantic (Python mode, as FastAPI validates a body) makes
  of request bodies for `UpdateSectionRequest` and `TestConnectionRequest`.
- `api/json_bodies.json`: `json.loads(bytes)` on request bodies: the value, or which
  exception (`JSONDecodeError` is FastAPI's 422, anything else its 400).
- `api/openapi_main.json`, `api/openapi_transcode.json`: the two apps' OpenAPI documents
  as `bootstrap.build_app` assembles them, for the structural comparison.
"""

from __future__ import annotations

import email.message
import json
import email.utils
import mimetypes
import os
import tempfile
from pathlib import Path
from typing import Any

from starlette.responses import FileResponse, MalformedRangeHeader, RangeNotSatisfiable
from starlette.staticfiles import StaticFiles

from pydantic import TypeAdapter, ValidationError

from embyx_manager.bootstrap import build_app
from embyx_manager.config.api import TestConnectionRequest, UpdateSectionRequest
from embyx_manager.settings import Settings

#: What `mimetypes` knows without a system file: the image reads none (it has no
#: `/etc/mime.types`), so this is what Starlette's `guess_type` answers there.
_DEFAULT = mimetypes.MimeTypes(filenames=())

GUESS_PATHS = [
    'index.html',
    'a.HTML',
    'app.js',
    'app.mjs',
    'style.css',
    'favicon.svg',
    'favicon.ico',
    'logo.png',
    'robots.txt',
    'blob.bin',
    'report.JSON',
    'manifest.webmanifest',
    'module.wasm',
    'font.woff2',
    'font.woff',
    'app.js.map',
    'bundle.js.gz',
    'bundle.js.Z',
    'bundle.js.br',
    'bundle.js.GZ',
    'icons.svgz',
    'backup.tgz',
    'backup.TGZ',
    'archive.tar.xz',
    'noext',
    '.hidden',
    'dir.d/noext',
    'a.b.c.txt',
    'trailing.',
    'unknown.zzz',
    'x.gz',
    '.gz',
]

STATIC_ROUTE_PATHS = [
    '/',
    '/settings',
    '/settings/',
    '/fill-actor/plans/abc',
    '/assets/app.js',
    '/assets',
    '/assets/',
    '/api',
    '/api/',
    '/api/nope',
    '/apix/nope',
    '/a//b',
    '/a/./b',
    '/a/../b',
    '/../../etc/passwd',
    '/a/../../b',
    '/./',
    '//double',
    '///triple',
    '/\\etc',
    '/v1.2/page',
    '/page.',
    '/a.b/c',
    '/�',
]

RANGE_HEADERS = [
    'bytes=0-4',
    'bytes=5-',
    'bytes=-3',
    'bytes=-0',
    'bytes=0-0',
    'bytes=9-9',
    'bytes=0-99',
    'bytes=10-',
    'bytes=10-12',
    'bytes=4-2',
    'bytes=0-1,3-4',
    'bytes=3-4,0-1',
    'bytes=0-3,2-6',
    'bytes=0-1,1-2',
    'bytes=0-1,2-3',
    'bytes=0-1, ,-,x,3-4',
    'bytes= 1 - 2 ',
    'BYTES=1-2',
    ' bytes =1-2',
    'items=0-1',
    'bytes',
    'bytes=',
    'bytes=x-y',
    'bytes=-',
    'bytes=1',
    'bytes=--3',
    'bytes=0--3',
    'bytes=+1-+2',
    'bytes=1_0-1_2',
    'bytes=١-٢',
    'bytes=0-99999999999999999999999999999999999999999999',
    'bytes=-99999999999999999999999999999999999999999999',
    'bytes=1-2=3',
    'bytes=' + ','.join(['0-1'] * 100),
    'bytes=' + ','.join(['0-1'] * 101),
    'bytes=' + '1' * 5000 + '-2',
]

PARSEDATE_TEXTS = [
    'Thu, 01 Jan 2026 00:00:00 GMT',
    'Thu, 01 Jan 2026 00:00:01 GMT',
    'Wed, 31 Dec 2025 23:59:59 GMT',
    'Thu, 1 Jan 2026 00:00:00 +0000',
    'Thursday, 01-Jan-26 00:00:00 GMT',
    'Thu Jan  1 00:00:00 2026',
    '01 Jan 2026 00:00:00',
    'Thu,01 Jan 2026 00:00:00 GMT',
    'Jan 01 2026 00:00:00 GMT',
    '1 January 2026 00:00 GMT',
    '1 jan 70 00:00:00',
    '1 jan 68 00:00:00',
    '1 jan 69 00:00:00',
    '1 jan 2026 00.00.01',
    '1 jan 2026 00.00',
    '1 jan 2026 1.2.3.4',
    '1 jan 2026, 00:00:00,',
    '1 jan 2026 00:00:00+0100',
    '1 jan 2026 00:00:00-0500',
    'Thu, 01 Foo 2026 00:00:00 GMT',
    'Thu, 01 Jan 2026 00:00 GMT',
    'Thu, 01 Jan 2026',
    'Thu, 01 Jan x 00:00:00 GMT',
    'Thu, 01 Jan 2026 aa:00:00 GMT',
    'Thu, 01 Jan 2026 00:00:00:00 GMT',
    'Thu, 01 Jan , 00:00:00 GMT',
    'Thu, 01 Jan 2026 00:00:00 GMT extra words',
    'Thu, 01 Jan ²2026 00:00:00 GMT',
    'Thu, ١ Jan 2026 00:00:00 GMT',
    '\xa0Thu,\xa001 Jan 2026 00:00:00 GMT',
    '',
    '   ',
    'garbage',
    '00:00:00 1 jan 2026',
    'jan 1 00:00:00 2026',
]

#: (mtime in ns, size): file times as `os.stat` reports them.
FILE_TIMES = [
    (1_767_225_600 * 1_000_000_000, 57),
    (1_767_225_600 * 1_000_000_000 + 123_456_789, 0),
    (1_767_225_600 * 1_000_000_000 + 999_999_499, 10),
    (1_767_225_600 * 1_000_000_000 + 999_999_500, 10),
    (1_767_225_600 * 1_000_000_000 + 999_999_999, 10),
    (1_000_000_000_000_000_001, 1),
    (0, 1_234_567_890_123),
    (86_399 * 1_000_000_000 + 500_000_000, 5),
    (1_700_000_000_000_000_000 + 1, 5),
    (1_700_000_000_000_000_000 + 100_000_000, 5),
]

CONTENT_TYPES = [
    'application/json',
    'application/json; charset=utf-8',
    'APPLICATION/JSON',
    'application/merge-patch+json',
    'application/vnd.api+json;x=y',
    'application/jsonx',
    'application/x-json',
    'text/json',
    'text/plain',
    'application/x-www-form-urlencoded',
    ' application/json ',
    'application/json;',
    ';application/json',
    'application/json/x',
    'application',
    'json',
    'application/+json',
    '\xa0application/json\x85',
    'application/json,text/plain',
    'application /json',
    'Application/Json; Charset=UTF-8',
]


def _mimetypes() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = [
        {'types_map': dict(sorted(_DEFAULT.types_map[True].items()))},
        {'encodings_map': dict(sorted(_DEFAULT.encodings_map.items()))},
        {'suffix_map': dict(sorted(_DEFAULT.suffix_map.items()))},
    ]
    for name in GUESS_PATHS:
        path = f'/srv/static/{name}'
        media_type, encoding = _DEFAULT.guess_type(path)
        cases.append({'path': path, 'type': media_type, 'encoding': encoding})
    return cases


def _static_paths() -> list[dict[str, Any]]:
    files = StaticFiles(directory='/', check_dir=False)
    cases = []
    for route_path in STATIC_ROUTE_PATHS:
        path = files.get_path({'type': 'http', 'path': route_path, 'root_path': ''})
        is_page_path = not path.startswith('api/') and '.' not in path.rsplit('/', 1)[-1]
        cases.append({'route_path': route_path, 'path': path, 'is_page_path': is_page_path})
    return cases


def _ranges() -> list[dict[str, Any]]:
    cases = []
    for header in RANGE_HEADERS:
        for size in (10, 0):
            case: dict[str, Any] = {'header': header, 'size': size}
            try:
                case['ranges'] = [list(pair) for pair in FileResponse._parse_range_header(header, size)]  # noqa: SLF001
            except MalformedRangeHeader as exc:
                case['malformed'] = exc.content
            except RangeNotSatisfiable as exc:
                case['not_satisfiable'] = exc.max_size
            cases.append(case)
    return cases


def _parsedate() -> list[dict[str, Any]]:
    cases = []
    for text in PARSEDATE_TEXTS:
        parsed = email.utils.parsedate(text)
        cases.append({'text': text, 'parsed': None if parsed is None else list(parsed)})
    return cases


def _file_headers() -> list[dict[str, Any]]:
    cases = []
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'file'
        path.write_bytes(b'')
        for mtime_ns, size in FILE_TIMES:
            os.utime(path, ns=(mtime_ns, mtime_ns))
            stat = path.stat()
            # The file's real `st_mtime` float with the case's size.
            response = FileResponse(path, stat_result=_Stat(size, stat.st_mtime))  # type: ignore[arg-type]
            cases.append(
                {
                    'mtime_ns': mtime_ns,
                    'size': size,
                    'st_mtime': repr(stat.st_mtime),
                    'last_modified': response.headers['last-modified'],
                    'etag': response.headers['etag'],
                }
            )
    return cases


class _Stat:
    """The two fields `FileResponse.set_stat_headers` reads."""

    def __init__(self, size: int, mtime: float) -> None:
        self.st_size = size
        self.st_mtime = mtime


def _content_types() -> list[dict[str, Any]]:
    cases = []
    for value in CONTENT_TYPES:
        message = email.message.Message()
        message['content-type'] = value
        is_json = message.get_content_maintype() == 'application' and (
            message.get_content_subtype() == 'json' or message.get_content_subtype().endswith('+json')
        )
        cases.append({'content_type': value, 'json': is_json})
    return cases


REQUEST_BODIES: list[Any] = [
    {'values': {}},
    {'values': {'address': 'x'}, 'version': 3},
    {'values': {}, 'version': None},
    {'values': {}, 'version': 0},
    {'values': {}, 'version': -1},
    {'values': {}, 'version': 1.0},
    {'values': {}, 'version': 1.5},
    {'values': {}, 'version': True},
    {'values': {}, 'version': False},
    {'values': {}, 'version': '2'},
    {'values': {}, 'version': ' 2 '},
    {'values': {}, 'version': '2.0'},
    {'values': {}, 'version': '-0'},
    {'values': {}, 'version': '1_0'},
    {'values': {}, 'version': 'two'},
    {'values': {}, 'version': ''},
    {'values': {}, 'version': []},
    {'values': {}, 'version': {}},
    {'values': {}, 'version': 9_223_372_036_854_775_807},
    {'values': {}, 'version': 1e300},
    {'values': {}, 'extra': 1},
    {'version': 1},
    {'values': None},
    {'values': []},
    {'values': 'x'},
    {'values': {'nested': {'deep': [1, {'x': None}]}}},
    {},
    [],
    [{'values': {}}],
    'values',
    3,
    None,
    True,
]


def _request_models() -> list[dict[str, Any]]:
    cases = []
    for model in (UpdateSectionRequest, TestConnectionRequest):
        adapter = TypeAdapter(model)
        for body in REQUEST_BODIES:
            case: dict[str, Any] = {'model': model.__name__, 'body': body}
            try:
                validated = adapter.validate_python(body, from_attributes=True)
            except ValidationError:
                case['ok'] = False
            else:
                case['ok'] = True
                case['validated'] = validated.model_dump()
            cases.append(case)
    return cases


JSON_BODIES_HEX = [
    '7b7d',
    'efbbbf7b7d',
    'fffe7b007d00',
    'feff007b007d',
    'fffe00007b0000007d000000',
    '0000feff0000007b0000007d',
    '7b007d00',
    '007b007d',
    '7b0000007d000000',
    '0000007b0000007d',
    'ff7b7d',
    '7bff7d',
    'fffe7b',
    '7b',
    '7d',
    '',
    '20',
    '22c3a922',
    '22eda08022',
    '225c75643830302200',
    '225c756438303022',
    '5b312c5d',
    '7b2261223a317d78',
    'efbbbfefbbbf7b7d',
    '7b22613a317d',
    '313030303030303030303030303030303030303030303030303030303030303030303030303030303030',
    '4e614e',
    '496e66696e697479',
    '2d496e66696e697479',
    '312e30',
    '31653330',
]


def _json_bodies() -> list[dict[str, Any]]:
    cases = []
    for text in JSON_BODIES_HEX:
        case: dict[str, Any] = {'hex': text}
        try:
            value = json.loads(bytes.fromhex(text))
        except json.JSONDecodeError:
            case['error'] = 'JSONDecodeError'
        except Exception as exc:  # noqa: BLE001 - recorded, which class is the point
            case['error'] = type(exc).__name__
        else:
            case['value'] = repr(value)
        cases.append(case)
    return cases


def _apps() -> tuple[Any, Any]:
    # Nothing connects while the app is assembled: the DSN is never dialled.
    settings = Settings(database_url='postgresql://openapi@127.0.0.1:9/openapi')
    app = build_app(settings)
    return app, app.state.transcode_app


def _openapi_main() -> list[dict[str, Any]]:
    return [_apps()[0].openapi()]


def _openapi_transcode() -> list[dict[str, Any]]:
    return [_apps()[1].openapi()]


GOLDENS = {
    'api/mimetypes.json': _mimetypes,
    'api/static_paths.json': _static_paths,
    'api/ranges.json': _ranges,
    'api/parsedate.json': _parsedate,
    'api/file_headers.json': _file_headers,
    'api/content_types.json': _content_types,
    'api/request_models.json': _request_models,
    'api/json_bodies.json': _json_bodies,
    'api/openapi_main.json': _openapi_main,
    'api/openapi_transcode.json': _openapi_transcode,
}
