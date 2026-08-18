from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from embyx_manager.clients.freshrss import FreshRSSClient
from embyx_manager.fill_actor.subscriptions import find_subscribed_actor_ids


def _status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request('POST', 'http://example.test/edit-tag')
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError('bad status', request=request, response=response)


@pytest.fixture
async def client():
    freshrss = FreshRSSClient(url='http://example.test', api_key='key')
    freshrss.read_items.retry.sleep = AsyncMock()  # type: ignore[attr-defined]
    freshrss.get_items.retry.sleep = AsyncMock()  # type: ignore[attr-defined]
    freshrss.get_subscription_urls.retry.sleep = AsyncMock()  # type: ignore[attr-defined]
    yield freshrss
    await freshrss.aclose()


async def test_read_items_refreshes_stale_edit_token(client: FreshRSSClient) -> None:
    fresh_value = 'fresh-value'
    tokens: list[str] = []

    def raise_401() -> None:
        raise _status_error(401)

    token_response = SimpleNamespace(text=fresh_value, raise_for_status=lambda: None)
    post_responses = [
        SimpleNamespace(raise_for_status=raise_401),
        SimpleNamespace(raise_for_status=lambda: None),
    ]

    async def capture_post(*_args: object, **kwargs: object) -> SimpleNamespace:
        data = kwargs['data']
        assert isinstance(data, dict)
        tokens.append(data['T'])
        return post_responses.pop(0)

    client._client.get = AsyncMock(return_value=token_response)  # noqa: SLF001
    client._client.post = AsyncMock(side_effect=capture_post)  # noqa: SLF001
    client._edit_token = 'stale-token'  # noqa: SLF001

    await client.read_items(['item-1'])

    assert tokens == ['stale-token', fresh_value]
    assert client._edit_token == fresh_value  # noqa: SLF001


async def test_get_items_raises_for_http_errors(client: FreshRSSClient) -> None:
    def raise_500() -> None:
        raise _status_error(500)

    bad_response = SimpleNamespace(raise_for_status=raise_500)
    get = AsyncMock(return_value=bad_response)
    client._client.get = get  # noqa: SLF001

    with pytest.raises(httpx.HTTPStatusError):
        await client.get_items('Actor')

    assert get.call_count == 3


async def test_get_items_follows_continuation(client: FreshRSSClient) -> None:
    pages = [
        SimpleNamespace(raise_for_status=lambda: None, json=lambda: {'items': [{'id': '1'}], 'continuation': 'c1'}),
        SimpleNamespace(raise_for_status=lambda: None, json=lambda: {'items': [{'id': '2'}]}),
    ]
    get = AsyncMock(side_effect=pages)
    client._client.get = get  # noqa: SLF001

    items = await client.get_items('Actor')

    assert [item['id'] for item in items] == ['1', '2']
    assert get.call_count == 2
    assert get.call_args_list[1].kwargs['params']['c'] == 'c1'


async def test_fetch_token_returns_and_caches_token(client: FreshRSSClient) -> None:
    token_response = SimpleNamespace(text='token-value', raise_for_status=lambda: None)
    client._client.get = AsyncMock(return_value=token_response)  # noqa: SLF001

    assert await client.fetch_token() == 'token-value'
    assert client._edit_token == 'token-value'  # noqa: SLF001


async def test_get_subscription_urls_reads_freshrss_subscription_list(client: FreshRSSClient) -> None:
    response = SimpleNamespace(
        raise_for_status=lambda: None,
        json=lambda: {
            'subscriptions': [
                {'id': 'feed/1', 'url': 'https://rsshub.example/javbus/star/A123'},
                {'id': 'feed/2', 'url': 'https://example.test/unrelated'},
                {'id': 'feed/3'},
            ]
        },
    )
    client._client.get = AsyncMock(return_value=response)  # noqa: SLF001

    assert await client.get_subscription_urls() == (
        'https://rsshub.example/javbus/star/A123',
        'https://example.test/unrelated',
    )
    client._client.get.assert_awaited_once_with(  # noqa: SLF001
        'http://example.test/subscription/list',
        headers={'Authorization': 'GoogleLogin auth=key'},
        params={'output': 'json'},
        timeout=10,
    )


def test_find_subscribed_actor_ids_matches_javbus_star_feeds_in_request_order() -> None:
    assert find_subscribed_actor_ids(
        ['A123', 'B-456', 'not-present'],
        [
            'https://old-rsshub.example/prefix/javbus/star/b-456/',
            'https://rsshub.example/javbus/star/A123?format=rss',
            'https://rsshub.example/javbus/actor/not-present',
        ],
    ) == ('A123', 'B-456')
