"""Feed parsing for the subscription poller: RSS 2.0 and Atom into items.

Every subscribed source speaks one of the two: RSSHub emits RSS 2.0 by default,
and so do the AVBase talent feeds and sukebei. An item is reduced to the four
things the poller needs — a key to remember it by, the title and link the AVID
is read from, and the HTML body a magnet table may sit in.
"""

from dataclasses import dataclass
from urllib.parse import unquote, urlsplit
from xml.etree.ElementTree import Element

from defusedxml.common import DefusedXmlException
from defusedxml.ElementTree import ParseError, fromstring

ATOM_NS = 'http://www.w3.org/2005/Atom'
CONTENT_NS = 'http://purl.org/rss/1.0/modules/content/'


class FeedParseError(ValueError):
    """The body was not a feed this poller can read."""


@dataclass(frozen=True)
class FeedItem:
    #: What the poller remembers the item by: guid or id, else link, else title.
    key: str
    title: str
    link: str | None
    #: The item body as HTML; a magnet table lives here on JavBus-style feeds.
    content: str


@dataclass(frozen=True)
class ParsedFeed:
    title: str | None
    items: tuple[FeedItem, ...]


def parse_feed(data: bytes) -> ParsedFeed:
    try:
        root = fromstring(data, forbid_dtd=True)
    except (ParseError, DefusedXmlException, ValueError) as exc:
        msg = f'not well-formed XML: {exc}'
        raise FeedParseError(msg) from exc
    if root.tag == 'rss':
        channel = root.find('channel')
        if channel is None:
            msg = 'RSS document has no channel'
            raise FeedParseError(msg)
        items = (_rss_item(element) for element in channel.findall('item'))
        return ParsedFeed(title=_title(channel.find('title')), items=tuple(item for item in items if item.key))
    if root.tag == f'{{{ATOM_NS}}}feed':
        items = (_atom_entry(element) for element in root.findall(f'{{{ATOM_NS}}}entry'))
        return ParsedFeed(
            title=_title(root.find(f'{{{ATOM_NS}}}title')), items=tuple(item for item in items if item.key)
        )
    msg = f'unsupported feed root element {root.tag!r}'
    raise FeedParseError(msg)


def avid_candidate_from_link(link: str | None) -> str:
    """The last path segment of an item link, with any catalog prefix stripped.

    AVBase links look like ``/works/MIZD-555`` and, for prefixed works,
    ``/works/moodyz:MIZD-555``; the prefix is the storefront, not part of the
    ID, and left in place it misleads the parser (``sodcreate:3DSVR-2013``
    reads as ``DSVR-2013``).
    """
    if not link:
        return ''
    segment = unquote(urlsplit(link).path.rstrip('/').rsplit('/', 1)[-1])
    return segment.rsplit(':', 1)[-1].strip()


def _rss_item(element: Element) -> FeedItem:
    title = _title(element.find('title')) or ''
    link = _text(element.find('link'))
    guid = _text(element.find('guid'))
    content = _text(element.find(f'{{{CONTENT_NS}}}encoded')) or _text(element.find('description')) or ''
    return FeedItem(key=guid or link or title, title=title, link=link, content=content)


def _atom_entry(element: Element) -> FeedItem:
    title = _title(element.find(f'{{{ATOM_NS}}}title')) or ''
    link = None
    for candidate in element.findall(f'{{{ATOM_NS}}}link'):
        href = candidate.get('href')
        if href and (candidate.get('rel') or 'alternate') == 'alternate':
            link = href.strip()
            break
    entry_id = _text(element.find(f'{{{ATOM_NS}}}id'))
    content = _text(element.find(f'{{{ATOM_NS}}}content')) or _text(element.find(f'{{{ATOM_NS}}}summary')) or ''
    return FeedItem(key=entry_id or link or title, title=title, link=link, content=content)


def _text(element: Element | None) -> str | None:
    if element is None or element.text is None:
        return None
    text = element.text.strip()
    return text or None


def _title(element: Element | None) -> str | None:
    text = _text(element)
    return ' '.join(text.split()) if text else None
