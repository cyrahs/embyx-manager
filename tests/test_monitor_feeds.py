import pytest

from embyx_manager.monitor.feeds import FeedItem, FeedParseError, avid_candidate_from_link, parse_feed

RSS = """<?xml version="1.0"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>JavBus - 演员</title>
    <item>
      <title>ABC-123 title</title>
      <link>https://www.javbus.com/ABC-123</link>
      <guid isPermaLink="true">https://www.javbus.com/ABC-123</guid>
      <description><![CDATA[<table><tbody><tr>
        <td><a href="magnet:?xt=urn:btih:abc">x</a></td><td>2 GiB</td>
      </tr></tbody></table>]]></description>
    </item>
    <item>
      <title>  spaced   title </title>
      <content:encoded><![CDATA[<p>body</p>]]></content:encoded>
    </item>
    <item>
      <description>no key at all</description>
    </item>
  </channel>
</rss>
""".encode()

ATOM = b"""<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>feed</title>
  <entry>
    <title>DEF-456</title>
    <id>tag:1</id>
    <link rel="enclosure" href="https://x/enc"/>
    <link href="https://x/DEF-456"/>
    <summary>s</summary>
  </entry>
</feed>
"""


def test_rss_items_carry_key_title_link_and_body() -> None:
    feed = parse_feed(RSS)

    assert feed.title == 'JavBus - 演员'
    assert [item.key for item in feed.items] == ['https://www.javbus.com/ABC-123', 'spaced title']
    first = feed.items[0]
    assert first.link == 'https://www.javbus.com/ABC-123'
    assert 'magnet:?xt=urn:btih:abc' in first.content
    # content:encoded wins over description; whitespace in titles is collapsed.
    assert feed.items[1] == FeedItem(key='spaced title', title='spaced title', link=None, content='<p>body</p>')


def test_atom_entries_take_the_alternate_link() -> None:
    feed = parse_feed(ATOM)

    assert feed.items == (FeedItem(key='tag:1', title='DEF-456', link='https://x/DEF-456', content='s'),)


@pytest.mark.parametrize(
    'body',
    [
        b'<html><body>not a feed</body></html>',
        b'definitely not xml',
        b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY e "x">]><rss version="2.0"><channel/></rss>',
    ],
)
def test_bodies_that_are_not_feeds_are_rejected(body: bytes) -> None:
    with pytest.raises(FeedParseError):
        parse_feed(body)


@pytest.mark.parametrize(
    ('link', 'expected'),
    [
        ('https://www.avbase.net/works/MIZD-555', 'MIZD-555'),
        ('https://www.avbase.net/works/moodyz:MIZD-555', 'MIZD-555'),
        ('https://www.avbase.net/works/sodcreate%3A3DSVR-2013/', '3DSVR-2013'),
        ('https://www.javlibrary.com/cn/?v=javli6', 'cn'),
        (None, ''),
        ('', ''),
    ],
)
def test_avid_candidate_is_the_last_path_segment_without_its_catalog_prefix(link: str | None, expected: str) -> None:
    assert avid_candidate_from_link(link) == expected
