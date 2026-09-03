from unittest.mock import AsyncMock

import pytest

from embyx_manager.clients.rss_magnet import get_magnet_from_html
from embyx_manager.clients.sukebei import SukebeiClient


def test_rss_magnet_skips_invalid_size() -> None:
    content = """
    <table><tbody>
      <tr>
        <td><a href="magnet:?xt=urn:btih:abc&dn=name">name</a></td>
        <td>unknown</td>
      </tr>
    </tbody></table>
    """

    assert get_magnet_from_html(content, 'ABC-123') is None


def test_rss_magnet_prefers_largest() -> None:
    content = """
    <table><tbody>
      <tr>
        <td><a href="magnet:?xt=urn:btih:small&dn=small">small</a></td>
        <td>1 GiB</td>
      </tr>
      <tr>
        <td><a href="magnet:?xt=urn:btih:big&dn=big">big</a></td>
        <td>3 GiB</td>
      </tr>
    </tbody></table>
    """

    assert get_magnet_from_html(content, 'ABC-123') == 'magnet:?xt=urn:btih:big&dn=ABC-123'


def test_rss_magnet_handles_an_empty_body() -> None:
    assert get_magnet_from_html('', 'ABC-123') is None


def test_rss_magnet_is_absent_from_a_javlibrary_item() -> None:
    """javlibrary items carry a cover and an info table, never a magnet.

    They lose this candidate and fall back on sukebei and javbus, which is why
    a javlibrary feed needs no parser of its own.
    """
    content = (
        '<img src="cover.jpg"/><div id="video_info"><table><tbody>'
        '<tr><td>ID:</td><td>ABC-123</td></tr>'
        '<tr><td>Release Date:</td><td>2026-01-01</td></tr>'
        '</tbody></table></div>'
    )

    assert get_magnet_from_html(content, 'ABC-123') is None


@pytest.fixture
async def sukebei():
    client = SukebeiClient()
    yield client
    await client.aclose()


async def test_sukebei_magnet_skips_invalid_size(sukebei: SukebeiClient) -> None:
    sukebei.search = AsyncMock(
        return_value=[
            {'size': 'unknown', 'magnet': 'magnet:?xt=urn:btih:bad&dn=bad', 'type': 'trusted', 'name': 'bad'},
            {'size': '2 GiB', 'magnet': 'magnet:?xt=urn:btih:good&dn=good', 'type': 'regular', 'name': 'good'},
        ],
    )

    assert await sukebei.get_magnet('ABC-123') == 'magnet:?xt=urn:btih:good&dn=ABC-123'


async def test_sukebei_prefers_trusted_within_tolerance(sukebei: SukebeiClient) -> None:
    sukebei.search = AsyncMock(
        return_value=[
            {'size': '10 GiB', 'magnet': 'magnet:?xt=urn:btih:big&dn=big', 'type': 'regular', 'name': 'big'},
            {'size': '9 GiB', 'magnet': 'magnet:?xt=urn:btih:trusted&dn=trusted', 'type': 'trusted', 'name': 'trusted'},
        ],
    )

    assert await sukebei.get_magnet('ABC-123') == 'magnet:?xt=urn:btih:trusted&dn=ABC-123'


async def test_sukebei_prefers_largest_when_trusted_is_too_small(sukebei: SukebeiClient) -> None:
    sukebei.search = AsyncMock(
        return_value=[
            {'size': '10 GiB', 'magnet': 'magnet:?xt=urn:btih:big&dn=big', 'type': 'regular', 'name': 'big'},
            {'size': '1 GiB', 'magnet': 'magnet:?xt=urn:btih:trusted&dn=trusted', 'type': 'trusted', 'name': 'trusted'},
        ],
    )

    assert await sukebei.get_magnet('ABC-123') == 'magnet:?xt=urn:btih:big&dn=ABC-123'


async def test_sukebei_returns_none_for_empty_results(sukebei: SukebeiClient) -> None:
    sukebei.search = AsyncMock(return_value=[])

    assert await sukebei.get_magnet('ABC-123') is None
