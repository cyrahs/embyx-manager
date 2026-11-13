"""`urllib.parse`, `pathlib`, `json.loads` errors and httpx's status texts (the helpers copied from fav).

Copied from fav's golden generator (`tools/goldens/areas/pyweb.py` at the commit named in
docs/CONVENTIONS.md) without its HTML-parser cases: embyx-manager parses HTML with lxml, not
`html.parser`, so those helpers were not copied. The cases feed the tests of `embyx_core::pyurl`,
`pypath`, `pyjson` and `embyx_clients::net::httpx`.
"""

from __future__ import annotations

import json
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import unquote, urljoin, urlsplit

import httpx

UNQUOTE_CASES: list[str] = [
    'File:Disc_Good_Night.png',
    'File:Disc%20Good%20Night.png',
    '%E4%B8%AD%E6%96%87',
    '%e4%b8%ad',
    '%E4%B8',
    '%FF%FE%41',
    '%C3%28',
    '%ED%A0%80',
    '%F0%9F%98%80',
    '%zz%4',
    '%',
    '100%',
    '中文%20混合',
    '%2Fslash%3Aname%23',
    '+plus+',
]

URLJOIN_CASES: list[tuple[str, str]] = [
    ('https://stellasora.miraheze.org', '/wiki/Freesia'),
    ('https://stellasora.miraheze.org', '//static.wikitide.net/stellasorawiki/2/2f/Freesia.png'),
    ('https://stellasora.miraheze.org', 'https://other.example/x'),
    ('https://stellasora.miraheze.org', 'relative/path'),
    ('https://stellasora.miraheze.org/wiki/Page', 'File:X.png'),
    ('https://stellasora.miraheze.org/wiki/Page', '../up'),
    ('https://stellasora.miraheze.org/wiki/Page', './here/./x/..'),
    ('https://stellasora.miraheze.org/wiki/Page', '?q=1'),
    ('https://stellasora.miraheze.org/wiki/Page?a=b', '#frag'),
    ('https://stellasora.miraheze.org/wiki/Page;p', ''),
    ('https://stellasora.miraheze.org/a/b/c', 'g;x?y#s'),
    ('https://stellasora.miraheze.org/a/b/c', '../../../../g'),
    ('https://stellasora.miraheze.org/a//b/c', 'd//e'),
    ('https://stellasora.miraheze.org', 'mailto:x@y'),
    ('https://stellasora.miraheze.org', 'javascript:void(0)'),
    ('https://stellasora.miraheze.org', ' /wiki/Leading_space'),
    ('https://stellasora.miraheze.org', '/wiki/Tab\there'),
    ('https://stellasora.miraheze.org', 'HTTPS://UPPER.example/X'),
    ('https://stellasora.miraheze.org', 'http:/wiki/x'),
    ('https://stellasora.miraheze.org', '//[::1]:8080/x'),
    ('https://stellasora.miraheze.org', '//[::1/x'),
    ('https://stellasora.miraheze.org', '//[1.2.3.4]/x'),
    ('https://stellasora.miraheze.org', '//[v1.fe]/x'),
    ('https://stellasora.miraheze.org', '//a[::1]/x'),
    ('https://stellasora.miraheze.org', '//host℀x/p'),
    ('https://stellasora.miraheze.org', '//ｈｏｓｔ/full-width'),  # noqa: RUF001 - NFKC changes it, but not into URL syntax
    ('', '/x'),
    ('http://a/b', 'c:d'),
]

SPLIT_CASES: list[str] = [
    'https://i.pximg.net/img-original/img/2024/01/01/00/00/00/1_p0.jpg',
    'https://i.pximg.net/no-extension',
    'https://i.pximg.net/a.b/c?x=1.png#y.gif',
    '/wiki/File:A.png',
    'no-scheme.jpg',
    '1:2/x.png',
    'HTTP://Host/Path.PNG',
]

PATH_CASES: list[str] = [
    '/img/x/1_p0.jpg',
    '/no-extension',
    'a/b.tar.gz',
    '.hidden',
    'trailing.',
    'dir/',
    'dir/.',
    '/',
    '',
    'A/B.png',
    'Disc_Good_Night.png',
    '..',
    'a/..',
    'x.',
    'x..png',
    '中文.PNG',
]

JOIN_CASES: list[tuple[str, str]] = [
    ('collection/pixiv', 'artist'),
    ('./collection/stellasora', 'disc'),
    ('/data', ''),
    ('/data', '/abs'),
    ('.', 'x'),
    ('a/', 'b//c/'),
    ('collection', 'a/./b'),
    ('//net', 'x'),
]


def _unquote() -> list[dict[str, str]]:
    return [{'text': text, 'result': unquote(text)} for text in UNQUOTE_CASES]


def _outcome(call: Any) -> dict[str, str]:
    try:
        return {'result': call()}
    except ValueError as exc:
        return {'error': str(exc)}


def _urljoin() -> list[dict[str, Any]]:
    return [
        {'base': base, 'url': url, **_outcome(lambda base=base, url=url: urljoin(base, url))}
        for base, url in URLJOIN_CASES
    ]


def _urlsplit() -> list[dict[str, Any]]:
    cases = []
    for url in SPLIT_CASES:
        parts = urlsplit(url)
        cases.append({'url': url, 'parts': list(parts)})
    return cases


def _paths() -> list[dict[str, Any]]:
    parts = [
        {
            'path': path,
            'name': PurePosixPath(path).name,
            'suffix': PurePosixPath(path).suffix,
            'stem': PurePosixPath(path).stem,
        }
        for path in PATH_CASES
    ]
    joins = [{'base': base, 'part': part, 'joined': str(PurePosixPath(base) / part)} for base, part in JOIN_CASES]
    return [{'parts': parts, 'joins': joins}]


def _reason_phrases() -> list[list[Any]]:
    # `raise_for_status()` names the status with httpx's table (no status line to read it from).
    return [[code, httpx.codes.get_reason_phrase(code)] for code in range(100, 600)]


STATUS_ERROR_CASES: list[tuple[int, str, str | None]] = [
    (200, 'https://example.com/ok', None),
    (204, 'https://example.com/empty', None),
    (101, 'https://example.com/upgrade', None),
    (301, 'https://example.com/old', 'https://example.com/new'),
    (302, 'https://example.com/a?b=1', '/relative'),
    (304, 'https://example.com/cached', 'https://example.com/elsewhere'),
    (307, 'https://example.com/tmp', None),
    (400, 'https://www.pixiv.net/ajax/user/1/illusts/bookmarks?tag=&offset=0&limit=48&rest=show', None),
    (403, 'https://i.pximg.net/img-original/img/x/1_p0.jpg', None),
    (404, 'https://stellasora.miraheze.org/wiki/Characters', None),
    (418, 'https://example.com/teapot', None),
    (429, 'https://example.com/slow', 'https://example.com/ignored'),
    (499, 'https://example.com/odd', None),
    (500, 'https://example.com/boom', None),
    (503, 'https://example.com/busy', None),
    (599, 'https://example.com/unknown', None),
]


def _status_errors() -> list[dict[str, Any]]:
    # `str(exc)` of `raise_for_status()`; `None` when the status passes.
    cases = []
    for status, url, location in STATUS_ERROR_CASES:
        headers = {'location': location} if location is not None else {}
        response = httpx.Response(status, headers=headers, request=httpx.Request('GET', url))
        try:
            response.raise_for_status()
            message = None
        except httpx.HTTPStatusError as exc:
            message = str(exc)
        cases.append({'status': status, 'url': url, 'location': location, 'message': message})
    return cases


JSON_ERROR_CASES: list[str] = [
    '',
    '<!DOCTYPE html><html>challenge</html>',
    '\n\n  x',
    '{"a": 1,}',
    '{"a" 1}',
    '[1 2]',
    '{"a": "unterminated',
    '{"a": "ctrl\u0001"}',
    '{"a": "\\q"}',
    '{"a": 1}\n\nextra',
    '中文\n{',
    '{1: 2}',
]


def _json_errors() -> list[dict[str, Any]]:
    # `str(JSONDecodeError)` of `json.loads` on API answers that are not JSON.
    cases = []
    for doc in JSON_ERROR_CASES:
        try:
            json.loads(doc)
            message = None
        except json.JSONDecodeError as exc:
            message = str(exc)
        cases.append({'doc': doc, 'message': message})
    return cases


GOLDENS = {
    'pyweb/json_errors.json': _json_errors,
    'pyweb/httpx_reason_phrases.json': _reason_phrases,
    'pyweb/httpx_status_errors.json': _status_errors,
    'pyweb/unquote.json': _unquote,
    'pyweb/urljoin.json': _urljoin,
    'pyweb/urlsplit.json': _urlsplit,
    'pyweb/paths.json': _paths,
}
