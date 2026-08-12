from unittest.mock import AsyncMock

import pytest

from embyx_manager.clients.rss_magnet import get_magnet_from_item
from embyx_manager.clients.sukebei import SukebeiClient


def test_rss_magnet_skips_invalid_size() -> None:
    item = {
        'title': 'ABC-123',
        'summary': {
            'content': """
            <table><tbody>
              <tr>
                <td><a href="magnet:?xt=urn:btih:abc&dn=name">name</a></td>
                <td>unknown</td>
              </tr>
            </tbody></table>
            """,
        },
    }

    assert get_magnet_from_item(item, 'ABC-123') is None


def test_rss_magnet_prefers_largest() -> None:
    item = {
        'title': 'ABC-123',
        'summary': {
            'content': """
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
            """,
        },
    }

    assert get_magnet_from_item(item, 'ABC-123') == 'magnet:?xt=urn:btih:big&dn=ABC-123'


def test_rss_magnet_handles_missing_summary() -> None:
    assert get_magnet_from_item({'title': 'ABC-123'}, 'ABC-123') is None


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
