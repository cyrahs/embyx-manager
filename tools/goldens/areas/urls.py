"""`urllib.parse` and `pathlib` as the sources use them (WP5).

Jandan picks candidate URLs, hosts and file extensions with `urlsplit`/`urlunsplit` and
`Path(...).suffix`; Kemono names files with `Path(name).stem`/`.suffix` and reads creator
ids out of URL paths; Jandan's request field is `quote`d and `unquote`d. The Rust port
follows CPython's code (`fav_core::pyurl`), not the WHATWG URL standard, and these are
Python 3.12's answers it must equal.

Copied from fav's golden generator (`tools/goldens/areas/urls.py` at the commit named in
docs/CONVENTIONS.md), as the goldens of the helpers copied with it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit, urlunsplit

URLS = [
    'http://wx1.sinaimg.cn/mw600/abc.jpg',
    'https://img.wangmoyu.com/mw1024/abc.png?x=1&y=2#frag',
    '//img.wangmoyu.com/mw600/demo-b.png',
    'HTTP://WX4.SINAIMG.CN/Large/ABC.JPG',
    'https://user:pw@Host.Example:8443/p/a/t/h?q#f',
    'https://pawchive.pw/fanbox/user/70050825?o=50',
    'https://kemono.cr/patreon/user/9069743/posts',
    'fanbox/user/70050825',
    '70050825',
    'mailto:someone@example.com',
    'http:/one-slash/path',
    'http:path-only',
    'file:///etc/hosts',
    'svn+ssh://host/repo',
    '  \x01 http://lead.example/ctrl  ',
    'http://ta\tb.example/new\nline\r',
    'https://[::1]:8080/v6',
    'https://[fe80::1%25eth0]/zone',
    'https://[fe80::1%Eth0]/zone-case',
    'https://[v1.fe]/future',
    'https://[v1]/future-bad',
    'https://[127.0.0.1]/v4-in-brackets',
    'https://[::1/missing-close',
    'https://::1]/missing-open',
    'https://x[::1]/before-bracket',
    'https://[::1]x/after-bracket',
    'https://[not-an-ip]/bad',
    'https://[::1]:80:90/two-ports',
    'https://host:/empty-port',
    'https://@/empty-host',
    'https:///triple-slash',
    'https://h/a?b?c#d#e',
    'https://h#frag?notquery',
    'https://ÉXAMPLE.com/Ünicode',
    'h-t.t+p://scheme-chars/ok',
    '1http://digit-first/path',
    'ht_tp://underscore/not-scheme',
    '://no-scheme',
    '',
    '/just/a/path.jpg',
    'https://img.wangmoyu.com/images/default_d_h_mw600.gif#101',
]

QUOTE_TEXTS = [
    'QUJD+/=',
    'plain-text_with.safe~chars',
    'slash/and space',
    '中文',
    '%41%42%zz%4',
    '%E4%B8%AD%E6%96%87',
    '%E4%B8%ADx%',
    '%E4%B8',
    '%ff%fe',
    'mixed 中%E6%96%87%',
    '',
]

PATHS = [
    'cover.jpeg',
    'archive.tar.gz',
    '.hidden',
    '.hidden.png',
    'name.',
    'a..b',
    '..',
    '.',
    '',
    '/',
    'dir/file.png',
    'dir/sub/',
    '/aa/bb/659e1483.jpeg',
    'dir/./',
    'back\\slash.txt',
    'x' * 10 + '.PNG',
    'no-ext',
    '/abc.unknown',
    '/path/abc.gif',
]


def _urlsplit() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for url in URLS:
        try:
            parts = urlsplit(url)
        except ValueError as exc:
            result: Any = f'ValueError: {exc}'
        else:
            result = [*parts, parts.hostname, urlunsplit(parts)]
        cases.append({'url': url, 'result': result})
    return cases


def _quote() -> list[dict[str, Any]]:
    return [
        {'text': text, 'quote_no_safe': quote(text, safe=''), 'quote': quote(text), 'unquote': unquote(text)}
        for text in QUOTE_TEXTS
    ]


def _paths() -> list[dict[str, Any]]:
    return [
        {'path': path, 'name': Path(path).name, 'suffix': Path(path).suffix, 'stem': Path(path).stem} for path in PATHS
    ]


GOLDENS = {
    'urls/urlsplit.json': _urlsplit,
    'urls/quote.json': _quote,
    'urls/paths.json': _paths,
}
