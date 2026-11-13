"""What the baseline's HTML parsing sees on the pages its tests carry (WP0 parser probe, WP2).

JavBus pages (`clients/javbus.py`) and the magnet tables inside feed items
(`clients/rss_magnet.py`) are read with pyquery, which parses with lxml and, as
`pyquery.fromstring` does without a parser argument, tries lxml's XML parser first and falls
back to `lxml.html` only when the text is not well-formed XML. sukebei's search page goes
through nyaapy, which walks an `lxml.etree.HTMLParser` tree with XPath. The Rust side
(`scraper`, html5ever) builds an HTML5 tree, so the same selector can match differently
(an implied `tbody`, a `<p>` closed by a table, upper-case tags under the XML parser).

Inputs: every string literal in the HTML-parsing tests (`tests/test_javbus.py`,
`tests/test_magnet_sources.py`) that contains markup, and a few pages written for the probe
(marked `synthetic`) where the tests carry none: a sukebei result table and malformed HTML of
the kinds real pages have. Outputs: for each input and each selector the code uses, the
matches with their tag, attributes, pyquery `.text()` and lxml `text_content()`; and for the
sukebei layout, what nyaapy's XPath walk reads.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import lxml.html
from lxml import etree
from pyquery import PyQuery

REPO_ROOT = Path(__file__).resolve().parents[3]
TEST_FILES = ['tests/test_javbus.py', 'tests/test_magnet_sources.py']

# Selectors evaluated on the whole document (javbus.py, rss_magnet.py).
DOCUMENT_SELECTORS = [
    'a[href]',
    'a.movie-box',
    '.avatar-box .pb10',
    'title',
    'tr',
    'table tbody tr',
]
# Selectors evaluated inside each `tr` of the document (javbus.py get_magnets, rss_magnet.py).
ROW_SELECTORS = [
    'td',
    'td:first-child a[href^="magnet"]',
    'td:first-child a',
    "td:nth-child(1) a[href^='magnet:']",
    'td:nth-child(2)',
    "a[href^='magnet:']",
]
# Selectors evaluated inside each `a[href]` (javbus.py search_stars, get_video_actors).
ANCHOR_SELECTORS = ['.pb10', 'img']

SYNTHETIC: list[tuple[str, str]] = [
    (
        'sukebei result table (layout of a nyaa search page)',
        (
            '<!DOCTYPE html><html><head><title>Sukebei</title></head><body>'
            '<table class="table torrent-list"><thead><tr><th>Category</th><th>Name</th></tr></thead><tbody>'
            '<tr class="success"><td><a href="/?c=2_2" title="Real Life - Videos">'
            '<img src="/static/img/icons/sukebei/2_2.png" alt="Real Life - Videos"></a></td>'
            '<td colspan="2"><a href="/view/4000001#comments" class="comments">'
            '<i class="fa fa-comments-o"></i>2</a>'
            '<a href="/view/4000001" title="ABC-123 FHD">ABC-123 FHD</a></td>'
            '<td class="text-center"><a href="/download/4000001.torrent">'
            '<i class="fa fa-fw fa-download"></i></a>'
            '<a href="magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&amp;dn=ABC-123">'
            '<i class="fa fa-fw fa-magnet"></i></a></td>'
            '<td class="text-center">2.1 GiB</td>'
            '<td class="text-center" data-timestamp="1767225600">2026-01-01 00:00</td>'
            '<td class="text-center">12</td><td class="text-center">0</td><td class="text-center">80</td></tr>'
            '<tr class="default"><td><a href="/?c=2_2"><img src="x.png"></a></td>'
            '<td colspan="2"><a href="/view/4000002" title="[HD] abc00123">[HD] abc00123</a></td>'
            '<td class="text-center"><a href="/download/4000002.torrent"></a>'
            '<a href="magnet:?xt=urn:btih:fedcba9876543210fedcba9876543210fedcba98&dn=x"></a></td>'
            '<td class="text-center">900.5 MiB</td><td class="text-center">2025-12-31 10:00</td>'
            '<td class="text-center">3</td><td class="text-center">1</td><td class="text-center">7</td></tr>'
            '</tbody></table></body></html>'
        ),
    ),
    (
        'magnet table without tbody, as a feed item may carry it',
        (
            '<table><tr><td><a href="magnet:?xt=urn:btih:aaa&dn=A">A</a></td><td>1.5GB</td></tr>'
            '<tr><td><a href="magnet:?xt=urn:btih:bbb&dn=B">B</a></td><td>2GB</td></tr></table>'
        ),
    ),
    (
        'unclosed cells and an unquoted attribute',
        (
            '<html><body><table><tr><td><a href=magnet:?xt=urn:btih:ccc&dn=C>C<td>3.0GB<tr><td>x<td>y</table>'
            '</body></html>'
        ),
    ),
    (
        'a paragraph left open before a table',
        '<div class="avatar-box"><p class="pb10">  Star\n Name </p><p>open<table><tr><td>cell</td></tr></table>',
    ),
    (
        'upper-case tags in well-formed markup (parsed as XML by pyquery)',
        (
            '<html><body><A HREF="/star/abc">Upper</A><a href="/star/def">lower</a>'
            '<TABLE><TR><TD>1</TD></TR></TABLE></body></html>'
        ),
    ),
    (
        # What an empty JavBus page leaves after the markup around it is gone.
        'an end tag and no element',
        '</a>',
    ),
    (
        'entities and a CDATA-like script',
        '<html><body><a href="/star/x?a=1&amp;b=2">A &amp; B&nbsp;C</a><script>if (a < b) {}</script></body></html>',
    ),
]


def _fixtures() -> list[tuple[str, str]]:
    fixtures: list[tuple[str, str]] = []
    seen: set[str] = set()
    for relative in TEST_FILES:
        tree = ast.parse((REPO_ROOT / relative).read_text(encoding='utf-8'))
        # Pieces of an f-string are not documents of their own: the page it renders depends on
        # runtime values, so it is left out rather than taken apart.
        pieces = {id(part) for node in ast.walk(tree) if isinstance(node, ast.JoinedStr) for part in node.values}
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in pieces:
                text = node.value
                if '<' in text and '>' in text and text.strip() and text not in seen:
                    seen.add(text)
                    fixtures.append((f'{relative}:{node.lineno}', text))
    fixtures.sort(key=lambda item: (item[0].split(':')[0], int(item[0].split(':')[1])))
    fixtures += [(f'synthetic: {name}', text) for name, text in SYNTHETIC]
    return fixtures


def _parsed_as(text: str) -> str:
    try:
        etree.fromstring(text)
    except etree.XMLSyntaxError:
        return 'html'
    return 'xml'


def _match(element: Any) -> dict[str, Any]:
    return {
        'tag': element.tag if isinstance(element.tag, str) else str(element.tag),
        'attrs': [[str(key), str(value)] for key, value in element.attrib.items()],
        'text': PyQuery(element).text(),
        'text_content': element.text_content() if hasattr(element, 'text_content') else ''.join(element.itertext()),
    }


def _select(scope: PyQuery, selector: str) -> list[dict[str, Any]]:
    return [_match(element) for element in scope(selector)]


def _nyaapy_rows(text: str) -> list[dict[str, Any]]:
    """What `nyaapy.parser.parse_nyaa` reads: rows under any tbody, cells, direct links."""
    tree = etree.fromstring(text, etree.HTMLParser())
    if tree is None:
        return []
    rows = []
    for tr in tree.xpath('//tbody//tr'):
        cells = [
            {
                'text': td.text,
                'links': [{'href': link.attrib.get('href'), 'text': link.text} for link in td.xpath('./a')],
            }
            for td in tr.xpath('./td')
        ]
        rows.append({'class': tr.attrib.get('class'), 'cells': cells})
    return rows


def _cases() -> list[dict[str, Any]]:
    cases = []
    for name, text in _fixtures():
        try:
            document = PyQuery(text)
        except (etree.ParserError, etree.XMLSyntaxError) as exc:
            # pyquery raises for these; the Rust parser must refuse them the same way.
            cases.append({'name': name, 'html': text, 'error': f'{type(exc).__name__}: {exc}'})
            continue
        rows = list(document('tr').items())
        anchors = list(document('a[href]').items())
        cases.append(
            {
                'name': name,
                'html': text,
                'parsed_as': _parsed_as(text),
                'document': {selector: _select(document, selector) for selector in DOCUMENT_SELECTORS},
                'rows': [{selector: _select(row, selector) for selector in ROW_SELECTORS} for row in rows],
                'anchors': [
                    {selector: _select(anchor, selector) for selector in ANCHOR_SELECTORS} for anchor in anchors
                ],
                'nyaapy': _nyaapy_rows(text),
                'lxml_html_root': lxml.html.fromstring(text).tag,
            }
        )
    return cases


GOLDENS = {'html/selectors.json': _cases}
