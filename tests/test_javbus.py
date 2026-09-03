import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from embyx_manager.clients.javbus import JavBusActor, JavBusClient, JavBusPaginationError, magnet_score

MAIN_PAGE_HTML = """
<html>
<script>
    var gid = 12345;
    var uc = 0;
    var img = '/pics/cover/sample.jpg';
</script>
</html>
"""

AJAX_RESPONSE_HTML = """
<html>
    <tr>
        <td width="70%">
            <a href="magnet:?xt=urn:btih:de439fca97a0365b47d9b087010115a94cad6853&dn=release1">release1</a>
        </td>
        <td style="text-align:center">
            <a href="magnet:?xt=urn:btih:de439fca97a0365b47d9b087010115a94cad6853&dn=release1">2.02GB</a>
        </td>
        <td style="text-align:center">
            <a href="magnet:?xt=urn:btih:de439fca97a0365b47d9b087010115a94cad6853&dn=release1">2025-01-01</a>
        </td>
    </tr>
    <tr>
        <td width="70%">
            <a href="magnet:?xt=urn:btih:a1b2c3d4e5f67890abcdef1234567890abcdef12&dn=release2">release2</a>
            <a class="btn btn-mini-new btn-primary"
               href="magnet:?xt=urn:btih:a1b2c3d4e5f67890abcdef1234567890abcdef12&dn=release2">高清</a>
            <a class="btn btn-mini-new btn-warning"
               href="magnet:?xt=urn:btih:a1b2c3d4e5f67890abcdef1234567890abcdef12&dn=release2">字幕</a>
        </td>
        <td style="text-align:center">
            <a href="magnet:?xt=urn:btih:a1b2c3d4e5f67890abcdef1234567890abcdef12&dn=release2">1.5GB</a>
        </td>
        <td style="text-align:center">
            <a href="magnet:?xt=urn:btih:a1b2c3d4e5f67890abcdef1234567890abcdef12&dn=release2">2025-01-02</a>
        </td>
    </tr>
    <a href="http://other-link.com">Other Link</a>
</html>
"""


@pytest.fixture
async def client():
    javbus = JavBusClient()
    yield javbus
    await javbus.aclose()


def set_get(client: JavBusClient, mock_get: AsyncMock) -> None:
    client._client.get = mock_get  # noqa: SLF001


async def test_get_magnets_success(client: JavBusClient) -> None:
    mock_response_main = MagicMock(text=MAIN_PAGE_HTML, status_code=200)
    mock_response_ajax = MagicMock(text=AJAX_RESPONSE_HTML, status_code=200)
    mock_get = AsyncMock(side_effect=[mock_response_main, mock_response_ajax])
    set_get(client, mock_get)

    video_id = 'TEST-001'
    magnets = await client.get_magnets(video_id)

    assert len(magnets) == 2
    magnet1 = next(m for m in magnets if 'de439fca97a0365b47d9b087010115a94cad6853' in m['magnet'])
    assert magnet1['magnet'] == f'magnet:?xt=urn:btih:de439fca97a0365b47d9b087010115a94cad6853&dn={video_id}'
    assert magnet1['size'] == '2.02GB'
    assert magnet1['size_int'] > 0

    magnet2 = next(m for m in magnets if 'a1b2c3d4e5f67890abcdef1234567890abcdef12' in m['magnet'])
    assert magnet2['magnet'] == f'magnet:?xt=urn:btih:a1b2c3d4e5f67890abcdef1234567890abcdef12&dn={video_id}'
    assert magnet2['size'] == '1.5GB'
    # The smaller upload carries the quality badges, so it is offered first.
    assert magnet1['tags'] == ()
    assert magnet2['tags'] == ('高清', '字幕')
    assert magnets == [magnet2, magnet1]

    assert mock_get.call_count == 2
    args, _ = mock_get.call_args_list[0]
    assert str(args[0]).endswith(f'/{video_id}')
    args, kwargs = mock_get.call_args_list[1]
    assert 'uncledatoolsbyajax.php' in str(args[0])
    assert 'gid=12345' in str(args[0])
    assert kwargs['headers']['Referer'].endswith(f'/{video_id}')


def test_magnet_score_prefers_tags_over_size() -> None:
    gib = 1 << 30
    assert magnet_score(gib, ('字幕',)) > magnet_score(100 * gib, ())
    # A second badge is worth a (3/2)^8 ≈ 25x size gap, more than any same-title spread.
    assert magnet_score(gib, ('高清', '字幕')) > magnet_score(20 * gib, ('高清',))
    assert magnet_score(2 * gib, ()) > magnet_score(gib, ())
    # An unreadable size cannot be trusted, badges or not.
    assert magnet_score(0, ('高清', '字幕')) == 0


async def test_get_magnets_no_variables(client: JavBusClient) -> None:
    mock_response_main = MagicMock(text='<html>No variables here</html>', status_code=200)
    mock_get = AsyncMock(return_value=mock_response_main)
    set_get(client, mock_get)

    assert await client.get_magnets('TEST-002') == []
    assert mock_get.call_count == 1


async def test_get_video_actors_reads_names_and_deduplicates_ids(client: JavBusClient) -> None:
    html = """
    <html>
        <a href="/star/a123"></a>
        <a href="https://www.javbus.com/star/B456/"><img alt="演员乙"></a>
        <a class="avatar-box" href="/star/A123"><span>演员甲</span></a>
        <a href="/star/A123">重复演员</a>
        <a href="/genre/7">其他链接</a>
    </html>
    """
    response = MagicMock(text=html, status_code=200)
    mock_get = AsyncMock(return_value=response)
    set_get(client, mock_get)

    actors = await client.get_video_actors('ABC-123')

    assert [(actor.actor_id, actor.name) for actor in actors] == [
        ('a123', '演员甲'),
        ('B456', '演员乙'),
    ]
    mock_get.assert_awaited_once_with(url=f'{client.host}/ABC-123')


async def test_scrape_one_page(client: JavBusClient) -> None:
    html = """
    <html>
        <a class="movie-box featured" href="https://www.javbus.com/VID-001/"></a>
        <a class="movie-box" href="https://www.javbus.com/VID-002"></a>
        <a class="movie-box" href="https://www.javbus.com/VID-001"></a> <!-- Duplicate -->
    </html>
    """
    mock_response = MagicMock(text=html, status_code=200)
    mock_get = AsyncMock(return_value=mock_response)
    set_get(client, mock_get)

    ids = await client.scrape_one_page('ACTOR-1', 1)
    assert sorted(ids) == ['VID-001', 'VID-002']
    mock_get.assert_called_once_with(url=f'{client.host}/star/ACTOR-1')

    mock_get.reset_mock()
    await client.scrape_one_page('ACTOR-1', 2)
    mock_get.assert_called_once_with(url=f'{client.host}/star/ACTOR-1/2')


async def test_get_total_page(client: JavBusClient) -> None:
    html = """
    <html>
        <a href="/star/ACTOR-1/1">1</a>
        <a href="/star/ACTOR-1/2">2</a>
        <a href="/star/ACTOR-1/3">3</a>
    </html>
    """
    mock_response = MagicMock(text=html, status_code=200)
    mock_get = AsyncMock(return_value=mock_response)
    set_get(client, mock_get)

    assert await client.get_total_page('ACTOR-1') == 3
    mock_get.assert_any_call(url=f'{client.host}/star/ACTOR-1')
    mock_get.assert_any_call(url=f'{client.host}/star/ACTOR-1/4')

    mock_response.text = '<html></html>'
    assert await client.get_total_page('ACTOR-1') == 1


async def test_scrape(client: JavBusClient) -> None:
    with (
        patch.object(client, 'get_total_page', new_callable=AsyncMock) as mock_get_total_page,
        patch.object(client, 'scrape_one_page', new_callable=AsyncMock) as mock_scrape_one_page,
    ):
        mock_get_total_page.return_value = 2
        mock_scrape_one_page.side_effect = [['A', 'B'], ['C']]

        ids = await client.scrape('ACTOR-1')

        assert set(ids) == {'A', 'B', 'C'}
        mock_get_total_page.assert_called_once_with('ACTOR-1')
        assert mock_scrape_one_page.call_count == 2


async def test_get_total_page_follows_sliding_windows_through_page_26(client: JavBusClient) -> None:
    windows = {
        1: range(1, 11),
        10: range(6, 16),
        15: range(11, 21),
        20: range(16, 26),
        25: range(17, 27),
        26: (),
    }
    requested: list[int] = []

    async def get(*, url: str) -> httpx.Response:
        page = 1 if url.endswith('/ACTOR-1') else int(url.rsplit('/', 1)[-1])
        requested.append(page)
        request = httpx.Request('GET', url)
        if page == 27:
            return httpx.Response(404, request=request)
        links = ''.join(f'<a href="/star/ACTOR-1/{number}">{number}</a>' for number in windows[page])
        return httpx.Response(
            200,
            text=f'<a class="movie-box" href="/{page:03d}"></a>{links}',
            request=request,
        )

    set_get(client, AsyncMock(side_effect=get))

    assert await client.get_total_page('ACTOR-1') == 26
    assert requested == [1, 10, 15, 20, 25, 26, 27]


async def test_scrape_reports_page_progress_and_globally_deduplicates(client: JavBusClient) -> None:
    events: list[tuple[int, int | None, int | None]] = []

    async def progress(completed: int, total: int | None, current: int | None) -> None:
        events.append((completed, total, current))

    async def scrape_page(_actor_id: str, page: int) -> list[str]:
        await asyncio.sleep((4 - page) * 0.001)
        return {1: ['A', 'B'], 2: ['B', 'C'], 3: ['D']}[page]

    with (
        patch.object(client, 'get_total_page', new_callable=AsyncMock, return_value=3) as total,
        patch.object(client, 'scrape_one_page', new_callable=AsyncMock, side_effect=scrape_page),
    ):
        ids = await client.scrape('ACTOR-1', progress_callback=progress)

    assert ids == ['A', 'B', 'C', 'D']
    total.assert_awaited_once_with('ACTOR-1', progress_callback=progress)
    assert events[:2] == [(0, None, None), (0, 3, None)]
    assert [event[0] for event in events[2:]] == [1, 2, 3]
    assert {event[2] for event in events[2:]} == {1, 2, 3}


async def test_terminal_404_is_not_retried(client: JavBusClient) -> None:
    url = f'{client.host}/star/ACTOR-1/2'
    response = httpx.Response(404, request=httpx.Request('GET', url))
    mock_get = AsyncMock(return_value=response)
    set_get(client, mock_get)

    assert await client.scrape_one_page('ACTOR-1', 2) == []
    mock_get.assert_awaited_once_with(url=url)


async def test_pagination_limit_fails_instead_of_returning_partial_results(client: JavBusClient) -> None:
    html = """
    <a class="movie-box" href="/VID-001"></a>
    <a href="/star/ACTOR-1/3">3</a>
    """
    response = MagicMock(text=html, status_code=200)
    set_get(client, AsyncMock(return_value=response))
    client.max_actor_pages = 3

    with pytest.raises(JavBusPaginationError, match='safety limit'):
        await client.get_total_page('ACTOR-1')


async def test_scrape_rejects_an_empty_page_inside_discovered_range(client: JavBusClient) -> None:
    async def scrape_page(_actor_id: str, page: int) -> list[str]:
        return {1: ['A'], 2: [], 3: ['C']}[page]

    with (
        patch.object(client, 'get_total_page', new_callable=AsyncMock, return_value=3),
        patch.object(client, 'scrape_one_page', new_callable=AsyncMock, side_effect=scrape_page),
        pytest.raises(JavBusPaginationError, match='empty page at 2'),
    ):
        await client.scrape('ACTOR-1')


async def test_search_stars_reads_every_star_page_the_search_lists(client: JavBusClient) -> None:
    html = """
    <div id="waterfall">
      <a class="avatar-box text-center" href="https://www.javbus.com/star/sl1"><span class="pb10">河北彩花</span></a>
      <a class="avatar-box text-center" href="https://www.javbus.com/star/new1"><span class="pb10">河北彩伽</span></a>
      <a href="https://www.javbus.com/star/sl1">dup</a>
      <a href="https://www.javbus.com/genre/1">not a star</a>
    </div>
    """
    client._client.get = AsyncMock(  # noqa: SLF001
        return_value=SimpleNamespace(status_code=200, text=html, raise_for_status=lambda: None)
    )

    stars = await client.search_stars('河北彩')

    assert stars == [JavBusActor(actor_id='sl1', name='河北彩花'), JavBusActor(actor_id='new1', name='河北彩伽')]
    client._client.get.assert_awaited_once_with(  # noqa: SLF001
        'https://www.javbus.com/searchstar/%E6%B2%B3%E5%8C%97%E5%BD%A9',
    )
