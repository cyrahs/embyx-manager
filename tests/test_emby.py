"""Emby client: request shapes, index paging, and error mapping."""

import json
from urllib.parse import parse_qs

import httpx
import pytest

from embyx_manager.clients.emby import EmbyAuthError, EmbyClient, EmbyError
from embyx_manager.core.avid import AvidParser

BASE = 'http://emby.test'
KEY = 'k3y'


class FakeServer:
    """Answers the handful of routes the client uses; records every request."""

    def __init__(self, movies: list[dict] | None = None) -> None:
        self.movies = movies or []
        self.playlists: dict[str, list[tuple[str, str]]] = {}  # id -> [(entry_id, item_id)]
        self.names: dict[str, str] = {}
        self.calls: list[tuple[str, str, dict[str, list[str]]]] = []
        self._next = 100
        self.status_override: int | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:  # noqa: C901, PLR0911 - one route table
        params = parse_qs(request.url.query.decode())
        self.calls.append((request.method, request.url.path, params))
        if request.headers.get('X-Emby-Token') != KEY:
            return httpx.Response(401)
        if self.status_override is not None:
            return httpx.Response(self.status_override)
        path = request.url.path
        if path == '/emby/System/Info':
            return httpx.Response(200, json={'ServerName': 'embyx', 'Version': '4.10.0.40', 'Id': 'srv'})
        if path == '/emby/Items' and params.get('IncludeItemTypes') == ['Movie']:
            start = int(params.get('StartIndex', ['0'])[0])
            limit = int(params.get('Limit', ['100'])[0])
            page = self.movies[start : start + limit]
            return httpx.Response(200, json={'Items': page, 'TotalRecordCount': len(self.movies)})
        if path == '/emby/Items' and params.get('IncludeItemTypes') == ['Playlist']:
            items = [{'Id': pid, 'Name': self.names[pid]} for pid in self.playlists]
            return httpx.Response(200, json={'Items': items, 'TotalRecordCount': len(items)})
        if path == '/emby/Playlists' and request.method == 'POST':
            pid = self._new_id()
            self.names[pid] = params['Name'][0]
            self.playlists[pid] = [(self._new_id(), item) for item in params.get('Ids', [''])[0].split(',') if item]
            return httpx.Response(200, json={'Id': pid, 'Name': self.names[pid]})
        if path.startswith('/emby/Playlists/') and path.endswith('/Items'):
            pid = path.split('/')[3]
            if request.method == 'GET':
                items = [{'Id': item, 'PlaylistItemId': entry} for entry, item in self.playlists[pid]]
                return httpx.Response(200, json={'Items': items, 'TotalRecordCount': len(items)})
            if request.method == 'POST':
                self.playlists[pid].extend((self._new_id(), item) for item in params['Ids'][0].split(','))
                return httpx.Response(204)
            if request.method == 'DELETE':
                gone = set(params['EntryIds'][0].split(','))
                self.playlists[pid] = [(e, i) for e, i in self.playlists[pid] if e not in gone]
                return httpx.Response(204)
        if path == '/emby/Items/Delete' and request.method == 'POST':
            for pid in params['Ids'][0].split(','):
                self.playlists.pop(pid, None)
                self.names.pop(pid, None)
            return httpx.Response(204)
        return httpx.Response(404)

    def _new_id(self) -> str:
        self._next += 1
        return str(self._next)


def make_client(server: FakeServer, *, key: str = KEY) -> EmbyClient:
    return EmbyClient(f'{BASE}/', key, transport=httpx.MockTransport(server.handler))


def movie(item_id: str, path: str) -> dict:
    return {'Id': item_id, 'Name': path.rsplit('/', maxsplit=1)[-1], 'Path': path}


async def test_system_info_sends_the_key_under_the_emby_prefix() -> None:
    server = FakeServer()
    client = make_client(server)
    try:
        info = await client.system_info()
    finally:
        await client.aclose()

    assert (info.name, info.version, info.server_id) == ('embyx', '4.10.0.40', 'srv')
    assert server.calls[0][:2] == ('GET', '/emby/System/Info')


async def test_rejected_key_is_an_auth_error() -> None:
    client = make_client(FakeServer(), key='wrong')
    try:
        with pytest.raises(EmbyAuthError):
            await client.system_info()
    finally:
        await client.aclose()


async def test_other_http_errors_and_transport_failures_are_emby_errors() -> None:
    server = FakeServer()
    server.status_override = 503
    client = make_client(server)
    try:
        with pytest.raises(EmbyError, match='HTTP 503'):
            await client.system_info()
    finally:
        await client.aclose()

    def explode(request: httpx.Request) -> httpx.Response:
        msg = 'refused'
        raise httpx.ConnectError(msg, request=request)

    client = EmbyClient(BASE, KEY, transport=httpx.MockTransport(explode))
    try:
        with pytest.raises(EmbyError, match='ConnectError'):
            await client.system_info()
    finally:
        await client.aclose()


async def test_movie_index_keys_on_the_parent_directory_and_pages_through(monkeypatch) -> None:
    monkeypatch.setattr('embyx_manager.clients.emby.INDEX_PAGE_SIZE', 2)
    server = FakeServer(
        [
            movie('1', '/media/local/actor/HMN/HMN-911/HMN-911.strm'),
            # The directory, not the file name, names the title: a subtitled part
            # keeps its suffix on the file only.
            movie('2', '/media/local/rank/SNOS/SNOS-377/SNOS-377-C.strm'),
            # Padding differs across the library; the parser normalizes both spellings.
            movie('3', '/media/local/rest/LAF/LAF-06/LAF-06.strm'),
            # A second copy of an AVID does not displace the first.
            movie('4', '/media/local/type/HMN/HMN-911/HMN-911-4K.strm'),
            {'Id': '5', 'Name': 'no path'},
        ],
    )
    client = make_client(server)
    try:
        index = await client.movie_index(AvidParser().get_avid)
    finally:
        await client.aclose()

    assert index == {'HMN-911': '1', 'SNOS-377': '2', 'LAF-006': '3'}
    pages = [call for call in server.calls if call[1] == '/emby/Items']
    assert [page[2]['StartIndex'] for page in pages] == [['0'], ['2'], ['4']]
    assert all(page[2]['Fields'] == ['Path'] and page[2]['Recursive'] == ['true'] for page in pages)


async def test_playlist_round_trip_keeps_order_and_addresses_entries() -> None:
    server = FakeServer()
    client = make_client(server)
    try:
        playlist_id = await client.create_playlist('JavDB 有码 TOP250', ['30', '10', '20'])
        assert [p.name for p in await client.list_playlists()] == ['JavDB 有码 TOP250']

        entries = await client.playlist_entries(playlist_id)
        assert [entry.item_id for entry in entries] == ['30', '10', '20']

        await client.remove_entries(playlist_id, [entries[1].entry_id])
        await client.add_entries(playlist_id, ['40'])
        assert [e.item_id for e in await client.playlist_entries(playlist_id)] == ['30', '20', '40']

        await client.remove_entries(playlist_id, [])
        await client.add_entries(playlist_id, [])
        await client.delete_playlist(playlist_id)
        assert await client.list_playlists() == ()
    finally:
        await client.aclose()

    create = next(call for call in server.calls if call[:2] == ('POST', '/emby/Playlists'))
    assert create[2] == {'Name': ['JavDB 有码 TOP250'], 'Ids': ['30,10,20'], 'MediaType': ['Video']}
    delete = next(call for call in server.calls if call[1] == '/emby/Items/Delete')
    assert delete[2] == {'Ids': [playlist_id]}
    # Empty edits never reach the server.
    assert sum(1 for call in server.calls if call[0] in {'POST', 'DELETE'} and call[1].endswith('/Items')) == 2


async def test_non_json_answers_are_reported_not_raised_raw() -> None:
    def html(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
        return httpx.Response(200, text='<html>login</html>')

    client = EmbyClient(BASE, KEY, transport=httpx.MockTransport(html))
    try:
        with pytest.raises(EmbyError, match='not a JSON response'):
            await client.system_info()
    finally:
        await client.aclose()


async def test_playlist_creation_without_an_id_is_an_error() -> None:
    def no_id(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
        return httpx.Response(200, content=json.dumps({}))

    client = EmbyClient(BASE, KEY, transport=httpx.MockTransport(no_id))
    try:
        with pytest.raises(EmbyError, match='no id'):
            await client.create_playlist('x', ['1'])
    finally:
        await client.aclose()
