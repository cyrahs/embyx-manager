"""What the site clients read out of the pages and answers they are given (WP2).

The clients (`clients/javbus.py`, `sukebei.py`, `rss_magnet.py`, `avbase.py`, `jinjier.py`)
and the feed parser (`monitor/feeds.py`) are fed markup and JSON here with their HTTP calls
answered in process, as the tests do, so no request leaves the machine. Every case records
what the baseline returns, or the class and text of what it raises. The Rust tests run the
same inputs through the port and compare.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import humanfriendly
import nyaapy.parser
import nyaapy.torrent

from embyx_manager.clients import avbase
from embyx_manager.clients.javbus import JavBusClient
from embyx_manager.clients.jinjier import is_excluded_code, is_playlist_kind, list_key
from embyx_manager.clients.rss_magnet import get_magnet_from_html
from embyx_manager.clients.sukebei import SukebeiClient
from embyx_manager.monitor.feeds import avid_candidate_from_link, parse_feed

REPO_ROOT = Path(__file__).resolve().parents[3]
SUKEBEI_PAGE = (REPO_ROOT / 'crates/embyx-clients/src/sukebei/search.html').read_text(encoding='utf-8')

logging.disable(logging.CRITICAL)


def _raised(exc: BaseException) -> dict[str, str]:
    return {'error': type(exc).__name__, 'message': str(exc)}


# ---------- humanfriendly.parse_size ----------

SIZES = [
    '1 GiB',
    '2.02GB',
    '1.5GB',
    '900.5 MiB',
    '10 GiB',
    '  2 GiB  ',
    '2GiB',
    '1 G',
    '1 g',
    '1 gb',
    '1 Gb',
    '1',
    '0',
    '1 B',
    '1 byte',
    '12 bytes',
    '1KB',
    '1 kb',
    '1.5 KiB',
    '3 TB',
    '1 PiB',
    '2 EB',
    '1 ZB',
    '1 YiB',
    '10 M',
    '10 MB',
    '10 mb',
    '10 MiB',
    '10 mib',
    '1.0',
    '.5 GB',
    '5. GB',
    '1e3',
    '1,5 GB',
    '-1 GB',
    '1.2.3 GB',
    '0.0001 KB',
    '1 GiB extra',
    '1 GB 2 MB',
    'GB',
    '',
    'unknown',
    'nan',
    'inf GB',
    '1_000 KB',
    '١ GB',
    '١٫٥ GB',
    '²',
    '½ GB',
    '1²',
    '٣ MiB',
    '1 KIB',
    '1 kIb',
    '1\tGB',
    '1　GB',
    '1 GiB\n',
    '0x10 KB',
    '+1 GB',
]


def _sizes() -> list[dict[str, Any]]:
    cases = []
    for text in SIZES:
        try:
            cases.append({'text': text, 'value': humanfriendly.parse_size(text)})
        except humanfriendly.InvalidSize as exc:
            cases.append({'text': text, 'error': 'InvalidSize', 'message': str(exc)})
        except ValueError as exc:
            cases.append({'text': text, **_raised(exc)})
    return cases


# ---------- JavBus ----------

ACTOR_PAGES = [
    (
        '<html><a class="movie-box" href="https://www.javbus.com/abc-001/"></a>'
        '<a class="movie-box" href="/ABC-002?x=1"></a><a class="movie-box" href="/abc-001"></a>'
        '<a class="movie-box"></a><a class="movie-box" href=" "></a>'
        '<a href="/star/ACTOR-1/2">2</a><a href="/star/ACTOR-1/3/">3</a><a href="/star/ACTOR-1/0">0</a>'
        '<a href="/star/ACTOR-1/04">4</a><a href="/star/ACTOR-12/5">x</a>'
        '<a href="https://elsewhere.test/star/ACTOR-1/9">9</a><a href="https://www.javbus.com/star/ACTOR-1/7">7</a>'
        '<a href="//www.javbus.com/star/ACTOR-1/8">8</a><a href="/star/ACTOR-1/1٢">12</a></html>'
    ),
    '<div><A class="movie-box" HREF="/up-1"></A><a class="movie-box" href="/down-1">x</a></div>',
    '<html></html>',
    '   ',
    (
        '<a class="movie-box featured" href="https://www.javbus.com/VID-001/"></a>'
        '<a class="movie-box" href="https://www.javbus.com/VID-002"></a> <!-- dup -->'
    ),
    '<p><a href="http://[::1/star/ACTOR-1/2">bad</a></p>',
]

MAGNET_PAGES = [
    (
        'TEST-001',
        (
            '<html><tr><td width="70%"><a href="magnet:?xt=urn:btih:DE439FCA97A0365B47D9B087010115A94CAD6853&dn=r1">'
            'release1</a></td><td><a href="magnet:?xt=urn:btih:de439fca">2.02GB</a></td><td>2025-01-01</td></tr>'
            '<tr><td><a href="magnet:?xt=urn:btih:a1b2&dn=r2">r2</a> <a class="btn">高清</a> <a class="btn"> 字幕 </a>'
            '<a class="btn">  </a></td><td> 1.5GB </td></tr>'
            '<tr><td><a href="magnet:?xt=urn:btih:DE439FCA97A0365B47D9B087010115A94CAD6853">dup</a></td>'
            '<td>9GB</td></tr>'
            '<tr><td><a href="magnet:?xt=urn:sha1:x">no btih</a></td><td>1GB</td></tr>'
            '<tr><td><a href="https://x/">no magnet</a></td><td>1GB</td></tr>'
            '<tr><td>one cell</td></tr>'
            '<tr><td><a href="magnet:?xt=urn:btih:ccc">c</a></td><td>huge</td></tr>'
            '<a href="http://other-link.com">Other</a></html>'
        ),
    ),
    (
        'TEST-002',
        (
            '<table><tbody><tr><td><a href="magnet:?xt=urn:btih:111">a</a><a>tag</a></td><td>1 GiB</td></tr>'
            '<tr><td><a href="magnet:?xt=urn:btih:222">b</a></td><td>1 GiB</td></tr></tbody></table>'
        ),
    ),
    ('TEST-003', '<tr><td><a href="magnet:?xt=urn:btih:333">a</a></td><td>²</td></tr>'),
    ('TEST-004', ''),
    ('TEST-005', '</a>'),
]

MAIN_PAGE = "<script>var gid = 12345;\nvar uc = 0;\nvar img = '/pics/cover/x.jpg';</script>"

VIDEO_PAGES = [
    (
        '<html><a href="/star/a123"></a><a href="https://www.javbus.com/star/B456/"><img alt="演员乙"></a>'
        '<a class="avatar-box" href="/star/A123"><span>演员甲</span></a><a href="/star/A123">重复演员</a>'
        '<a href="/genre/7">其他链接</a></html>'
    ),
    '<div><a href="/star/x1" title=" 名 前 "></a><a href="/star/x2" aria-label="X2"><img title="Title" alt="Alt"></a>'
    '<a href="/star/x3"><img alt="x3"></a><a href="/star/%E5%90%8D"></a><a href="/star/bad%20id">b</a>'
    '<a href="/star/' + 'y' * 33 + '">long</a><a href="/star/x4/extra">no</a><a href="/star/x5?a=1">q</a></div>',
    '<a href="/star/K">k</a><a href="/star/k"></a>',
]

SEARCH_PAGES = [
    (
        '<div id="waterfall"><a class="avatar-box text-center" href="https://www.javbus.com/star/sl1">'
        '<span class="pb10">河北彩花</span></a><a class="avatar-box" href="https://www.javbus.com/star/new1">'
        '<span class="pb10">  河北\n彩伽 </span></a><a href="https://www.javbus.com/star/sl1">dup</a>'
        '<a href="https://www.javbus.com/genre/1">not a star</a><a href="/star/SL1">case</a>'
        '<a href="/star/plain"> Plain   Name </a><a href="/star/empty"></a></div>'
    ),
    '<html></html>',
]


def _answer(text: str) -> SimpleNamespace:
    return SimpleNamespace(status_code=200, text=text, raise_for_status=lambda: None)


async def _javbus() -> list[dict[str, Any]]:
    client = JavBusClient()
    cases: list[dict[str, Any]] = []
    try:
        for html in ACTOR_PAGES:
            try:
                ids, pages = client._parse_actor_page('ACTOR-1', html)  # noqa: SLF001
                cases.append({'kind': 'actor_page', 'html': html, 'ids': list(ids), 'pages': sorted(pages)})
            except Exception as exc:  # noqa: BLE001 - the golden records what is raised
                cases.append({'kind': 'actor_page', 'html': html, **_raised(exc)})
        for video_id, html in MAGNET_PAGES:
            client._client.get = AsyncMock(side_effect=[_answer(MAIN_PAGE), _answer(html)])  # noqa: SLF001
            try:
                magnets = await client.get_magnets(video_id)
                cases.append(
                    {
                        'kind': 'magnets',
                        'video_id': video_id,
                        'html': html,
                        'magnets': [{**magnet, 'tags': list(magnet['tags'])} for magnet in magnets],
                    }
                )
            except Exception as exc:  # noqa: BLE001
                cases.append({'kind': 'magnets', 'video_id': video_id, 'html': html, **_raised(exc)})
        for kind, pages, call in [
            ('video_actors', VIDEO_PAGES, client.get_video_actors),
            ('search_stars', SEARCH_PAGES, client.search_stars),
        ]:
            for html in pages:
                client._client.get = AsyncMock(return_value=_answer(html))  # noqa: SLF001
                actors = await call('ABC-123')
                cases.append({'kind': kind, 'html': html, 'actors': [[actor.actor_id, actor.name] for actor in actors]})
    finally:
        await client.aclose()
    return cases


# ---------- feed magnet tables ----------

RSS_TABLES = [
    (
        '<table><tbody><tr><td><a href="magnet:?xt=urn:btih:small&dn=small">small</a></td><td>1 GiB</td></tr>'
        '<tr><td><a href="magnet:?xt=urn:btih:big&dn=b%20ig">big</a></td><td>3 GiB</td></tr></tbody></table>'
    ),
    (
        '<table><tbody><tr><td><a href="magnet:?xt=urn:btih:abc&dn=name">name</a></td><td>unknown</td></tr>'
        '</tbody></table>'
    ),
    '<table><tr><td><a href="magnet:?xt=urn:btih:a&dn=A">A</a></td><td>1 GB</td></tr></table>',
    '<table><tbody><tr><td>x</td><td><a href="magnet:?xt=urn:btih:a">1 GB</a></td></tr></tbody></table>',
    (
        '<table><tbody><tr><td><a href="magnet:?xt=urn:btih:a&tr=x">a</a></td><td>2 GB</td></tr>'
        '<tr><td><a href="magnet:?xt=urn:btih:b&dn=B&dn=C">b</a></td><td>1 GB</td></tr>'
        '<tr><td><a href="magnet:?xt=urn:btih:c&dn=">c</a></td><td>2 GB</td></tr></tbody></table>'
    ),
    '<table><tbody><tr><td><a href="magnet:?xt=urn:btih:a">a</a></td><td>²</td></tr></tbody></table>',
    (
        '<img src="cover.jpg"/><div id="video_info"><table><tbody><tr><td>ID:</td><td>ABC-123</td></tr></tbody>'
        '</table></div>'
    ),
    '<TABLE><TBODY><TR><TD><a href="magnet:?xt=urn:btih:up">up</a></TD><TD>1 GB</TD></TR></TBODY></TABLE>',
    '<p>no table</p>',
    '',
    '   ',
    '</p>',
    '<?xml version="1.0" encoding="utf-8"?><table/>',
]


def _rss() -> list[dict[str, Any]]:
    cases = []
    for html in RSS_TABLES:
        try:
            cases.append({'html': html, 'magnet': get_magnet_from_html(html, 'ABC-123')})
        except Exception as exc:  # noqa: BLE001
            cases.append({'html': html, **_raised(exc)})
    return cases


# ---------- sukebei ----------

NYAA_PAGES = [
    SUKEBEI_PAGE,
    '',
    '<table><tbody><tr><td>short</td></tr></tbody></table>',
    '<table><tbody><tr><td>2_2</td></tr></tbody></table>',
    '<table><tbody><tr><td><a>no href</a></td></tr></tbody></table>',
    '<table><tr><td>?c=2_2</td></tr></table>',
]

SEARCH_RESULTS = [
    ('ABC-123', [('unknown', 'bad', 'trusted', 'ABC-123 bad'), ('2 GiB', 'good', 'regular', 'ABC-123 good')]),
    ('ABC-123', [('10 GiB', 'big', 'regular', 'ABC-123 big'), ('9 GiB', 'trusted', 'trusted', 'ABC-123 t')]),
    ('ABC-123', [('10 GiB', 'big', 'regular', 'ABC-123 big'), ('1 GiB', 'trusted', 'trusted', 'ABC-123 t')]),
    ('ABC-123', [('10 GiB', 'big', 'regular', 'ABC-123 big'), ('8 GiB', 'trusted', 'trusted', 'ABC-123 t')]),
    ('NHDTC-180', [('9 GiB', 'other', 'trusted', 'NHDTC-18006 FHD'), ('3 GiB', 'right', 'regular', '[HD] nhdtc00180')]),
    ('NHDTC-180', [('9 GiB', 'other', 'trusted', 'NHDTC-18006')]),
    ('FC2-1234567', [('1 GiB', 'fc2', 'regular', 'FC2-PPV-1234567')]),
    ('heydouga-4030-123', [('1 GiB', 'h', 'default', 'anything')]),
    (
        'abc-0123',
        [
            ('1 GiB', 'a', 'default', 'xABC-123'),
            ('2 GiB', 'b', 'default', 'ABC 123 x'),
            ('3 GiB', 'c', 'default', 'ABC_0123'),
        ],
    ),
    ('ABC-123', [('2 GiB', 'first', 'default', 'ABC-123'), ('2 GiB', 'second', 'default', 'ABC-123')]),
    ('ABC-123', [('1 GiB', 'x', 'default', 'ABC-123'), ('²', 'y', 'default', 'ABC-123')]),
    ('ABC-123', [('1 GiB', 'x', 'default', None)]),
    ('ABC-123', []),
]


async def _sukebei() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for page in NYAA_PAGES:
        try:
            rows = nyaapy.parser.parse_nyaa(page, limit=None, site=nyaapy.torrent.TorrentSite.SUKEBEINYAASI)
            cases.append({'kind': 'parse', 'html': page, 'rows': rows})
        except Exception as exc:  # noqa: BLE001
            cases.append({'kind': 'parse', 'html': page, **_raised(exc)})
    client = SukebeiClient()
    try:
        for keyword, rows in SEARCH_RESULTS:
            results = [
                {'size': size, 'magnet': f'magnet:?xt=urn:btih:{name}&dn={name}', 'type': kind, 'name': title}
                for size, name, kind, title in rows
            ]
            given = [dict(result) for result in results]
            client.search = AsyncMock(return_value=results)
            try:
                cases.append(
                    {'kind': 'choose', 'keyword': keyword, 'results': given, 'magnet': await client.get_magnet(keyword)}
                )
            except Exception as exc:  # noqa: BLE001
                cases.append({'kind': 'choose', 'keyword': keyword, 'results': given, **_raised(exc)})
    finally:
        await client.aclose()
    return cases


# ---------- AVBase ----------

DATES = [
    'Fri Oct 02 2026 09:00:00 GMT+0900 (Japan Standard Time)',
    'Fri Oct 2 2026',
    'Fri Oct 2 2026 x',
    'Thu Feb 30 2026 09:00:00',
    'Fri Foo 02 2026',
    'fri oct 02 2026',
    'Fri OCT 02 2026',
    'Fri Oct 123 2026',
    'Fri Oct 02 202',
    'Fri Oct 02 0000',
    'Fri Oct ٠٢ ٢٠٢٦',
    'Fri Oct 02 2026',
    '2026-02-19T00:00:00.000Z',
    '2026-02-19',
    '20260219',
    '2026-W08',
    '2026-W08-4',
    '2026W084',
    '2026W08',
    '2026-W53',
    '2026-W08-8',
    '2026-02-30',
    '0000-01-01',
    '2026-2-19',
    '2026/02/19',
    '２０２６-02-19',
    '2026-02-19x',
    '20260219T00',
    '20260219xx',
    '2026021912',
    '2026-0219',
    '2026-02-1',
    '2026W0849',
    '2026W08-4',
    '2026-W084',
    '20260219é',
    '2026021é',
    '9999-W52-6',
    '9999-W52-5',
    '0000-W01',
    '2020-W53',
    '2020W537',
    '2026-02',
    '2026',
    '',
    ' 2026-02-19',
    None,
    20260219,
    2026.5,
    True,
    ['2026-02-19'],
]

TALENT_QUERIES = [
    '河北彩伽',
    ' 5022 ',
    '5022',
    '٥٠٢٢',
    '²',
    'https://www.avbase.net/talents/5022',
    'https://www.avbase.net/talents/5022/feed',
    'https://www.avbase.net/talents/5022/feed/',
    'https://www.avbase.net/talents/%E6%B2%B3%E5%8C%97%E5%BD%A9%E8%8A%B1?tab=works',
    '/talents/abc',
    '/talents/%20',
    'https://www.avbase.net/works/MIZD-555',
    '/actors/1',
    '',
    '   ',
    'http://[::1/talents/1',
    'avbase.net/talents/5022',
    'https://www.avbase.net/talents/a/b',
    'https://www.avbase.net/talents/a#x',
]

FEEDS = [
    (
        '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title><![CDATA[「河北彩花」のフィード]]>'
        '</title><link>https://www.avbase.net/talents/%E6%B2%B3%E5%8C%97%E5%BD%A9%E8%8A%B1</link></channel></rss>'
    ),
    '<rss><channel><title>「 名前 」の「x」</title><link>https://www.avbase.net/talents/5022</link></channel></rss>',
    '<rss><channel><title>no brackets</title><link> </link></channel></rss>',
    '<rss><channel><link>https://www.avbase.net/talents/%E5%90%8D/</link></channel></rss>',
    '<rss/>',
    'not xml',
    '<!DOCTYPE x><rss/>',
    '<rss><channel><title>「a\nb」</title></channel></rss>',
    '<rss><channel><link>http://[::1/talents/a</link></channel></rss>',
]

PAYLOADS: list[dict[str, Any]] = [
    {
        'talent': {
            'id': 5022,
            'primary': {'id': 5031, 'name': ' 河北彩花 '},
            'actors': [{'name': '河北彩花'}, {'name': '河北彩伽'}, None, {'name': 7}, {'name': '河北彩伽'}, {}],
        },
        'total': 31,
    },
    {'name': 'fallback', 'talent': {'id': '12', 'primary': None, 'actors': None}, 'total': '3'},
    {'talent': {'id': 1.9}, 'total': 2.5},
    {'talent': {'id': 1, 'primary': 3}},
    {'talent': {'actors': []}},
    {'talent': {'id': None}},
    {'talent': {'id': 1, 'actors': 5}},
    {'talent': {'id': 1}, 'total': 'x'},
]

WORKS: list[Any] = [
    {
        'work_id': 'moodyz:MIZD-555',
        'prefix': 'moodyz',
        'title': 'best',
        'min_date': '2026-02-19',
        'actors': [
            {'id': 1, 'name': ' A '},
            {'id': '2', 'name': 'B'},
            {'id': 3, 'name': ''},
            'x',
            {'id': True, 'name': 'C', 'talent': {'id': 9}},
            {'id': 4, 'name': 'D', 'talent': {'id': 'x'}},
        ],
        'casts': [
            {'actor': {'id': 5, 'name': 'E', 'talent': {'id': 50}}},
            {'actor': None},
            'x',
            {'actor': 'y'},
            {'actor': {'id': 6, 'name': 'F', 'talent': [1]}},
        ],
    },
    {'work_id': None, 'prefix': None, 'title': 12, 'min_date': None},
    {'actors': 5},
    {'casts': [1]},
    {'work_id': ['a'], 'casts': {'k': 1}},
]


def _avbase_text() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for value in DATES:
        parsed = avbase.parse_release_date(value)
        cases.append({'kind': 'date', 'value': value, 'date': parsed.isoformat() if parsed else None})
    for query in TALENT_QUERIES:
        try:
            talent_id, name = avbase.parse_talent_query(query)
            cases.append({'kind': 'query', 'query': query, 'talent_id': talent_id, 'name': name})
        except ValueError as exc:
            cases.append({'kind': 'query', 'query': query, **_raised(exc)})
    for feed in FEEDS:
        try:
            cases.append({'kind': 'feed', 'xml': feed, 'name': avbase._talent_name_from_feed(feed)})  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            cases.append({'kind': 'feed', 'xml': feed, **_raised(exc)})
    return cases


def _avbase_payloads() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for props in PAYLOADS:
        try:
            talent = avbase._talent_from_props(props)  # noqa: SLF001
            cases.append(
                {
                    'kind': 'talent',
                    'props': props,
                    'talent': [talent.talent_id, talent.name, list(talent.aliases), talent.total_works],
                }
            )
        except Exception as exc:  # noqa: BLE001
            cases.append({'kind': 'talent', 'props': props, **_raised(exc)})
    for builder in ('listing', 'detail'):
        for work in WORKS:
            try:
                built = (avbase._work_from_listing if builder == 'listing' else avbase._work_from_detail)(work)  # noqa: SLF001
                cases.append(
                    {
                        'kind': builder,
                        'work': work,
                        'built': [
                            built.work_id,
                            built.prefix,
                            built.title,
                            built.release_date.isoformat() if built.release_date else None,
                            [[int(m.actor_id), m.name, m.talent_id] for m in built.cast],
                        ],
                    }
                )
            except Exception as exc:  # noqa: BLE001
                cases.append({'kind': builder, 'work': work, **_raised(exc)})
    return cases


# ---------- jinjier and the feed parser ----------

CODES = [
    'SSNI-497',
    'FC2-4025269',
    'fc2-1',
    '010115_001',
    '092415-159',
    '092415-159\n',
    '0924151-59',
    '١٢٣٤٥٦-١٢٣',
    'HEYZO-1031',
    'heyzo-1031',
    'LAFBD-041',
    'SKY-247',
    'SKYHD-084',
    'BT-123',
    'BTH-123',
    'CARIB',
    '1PONDO-1-2',
    '',
    '-1',
]


def _jinjier() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = [{'code': code, 'excluded': is_excluded_code(code)} for code in CODES]
    cases.extend(
        {'kind': kind, 'playlist': is_playlist_kind(kind), 'key': list_key(kind, 'n')}
        for kind in (0, 2, 3, 4, 5, 6, 7, 8, 2007, 2008, 2024, 2100, 2101)
    )
    return cases


FEED_BODIES = [
    (
        b'<?xml version="1.0"?><rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/"><channel>'
        b'<title>  JavBus \n - x </title><item><title>A</title><link> https://x/A </link><guid>g1</guid>'
        b'<description>d</description><content:encoded> <![CDATA[<p>c</p>]]> </content:encoded></item>'
        b'<item><title>B</title><link>https://x/B</link></item><item><title>  </title></item>'
        b'<item><guid>  </guid><title>T<!-- c -->U</title><description><![CDATA[x]]>y</description></item>'
        b'</channel></rss>'
    ),
    (
        b'<feed xmlns="http://www.w3.org/2005/Atom"><title>f</title><entry><title>A</title>'
        b'<link rel="enclosure" href="https://x/enc"/><link href=""/><link rel="alternate" href=" https://x/A "/>'
        b'<content>c</content><summary>s</summary></entry><entry><id>i2</id><summary>s</summary></entry>'
        b'<entry><title>no key</title><link rel="self" href="https://x/self"/></entry></feed>'
    ),
    b'\xef\xbb\xbf<rss><channel><item><title>bom</title></item></channel></rss>',
    '<?xml version="1.0" encoding="ISO-8859-1"?><rss><channel><title>caf\xe9</title></channel></rss>'.encode('latin-1'),
    '<rss><channel><title>ü</title></channel></rss>'.encode('utf-16'),
    b'<rss version="2.0"/>',
    b'<rss><item/></rss>',
    b'<RSS><channel/></RSS>',
    b'<html><body>not a feed</body></html>',
    b'definitely not xml',
    b'',
    b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY e "x">]><rss version="2.0"><channel/></rss>',
    b'<rss><channel><title>&amp; &lt; &#x41;</title></channel></rss>',
    b'<rss><channel><title>a</title></channel></rss><extra/>',
    b'<rss><channel><title>\xff</title></channel></rss>',
]

LINKS = [
    'https://www.avbase.net/works/MIZD-555',
    'https://www.avbase.net/works/moodyz:MIZD-555',
    'https://www.avbase.net/works/sodcreate%3A3DSVR-2013/',
    'https://www.javlibrary.com/cn/?v=javli6',
    None,
    '',
    'MIZD-555',
    '/works/a:b:c ',
    'https://x/works/%20A-1%20',
    'https://x/',
    'https://x',
    'http://[::1/works/x',
]


def _feeds() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for body in FEED_BODIES:
        text = body.decode('latin-1')
        try:
            feed = parse_feed(body)
            items = [[item.key, item.title, item.link, item.content] for item in feed.items]
            cases.append({'kind': 'feed', 'body_latin1': text, 'title': feed.title, 'items': items})
        except Exception as exc:  # noqa: BLE001
            cases.append({'kind': 'feed', 'body_latin1': text, **_raised(exc)})
    for link in LINKS:
        try:
            cases.append({'kind': 'link', 'link': link, 'candidate': avid_candidate_from_link(link)})
        except ValueError as exc:
            cases.append({'kind': 'link', 'link': link, **_raised(exc)})
    return cases


GOLDENS = {
    'clients/size.json': _sizes,
    'clients/javbus.json': lambda: asyncio.run(_javbus()),
    'clients/rss_magnet.json': _rss,
    'clients/sukebei.json': lambda: asyncio.run(_sukebei()),
    'clients/avbase.json': lambda: _avbase_text() + _avbase_payloads(),
    'clients/jinjier.json': _jinjier,
    'clients/feeds.json': _feeds,
}
