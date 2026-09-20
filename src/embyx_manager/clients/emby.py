"""Emby server client: the movie index and playlist maintenance.

Only what the playlist sync needs. Every request carries the API key in the
``X-Emby-Token`` header; a rejected key surfaces as :class:`EmbyAuthError` so the
settings page and the pipeline can name the cause. Endpoint shapes were verified
against Emby 4.10 (see docs/plans/ranking-playlists.md).
"""

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from posixpath import basename, dirname
from typing import Any

import httpx

log = logging.getLogger('embyx-manager.emby')

_HTTP_UNAUTHORIZED = 401
#: Items per page when walking the library; the whole index is a few pages.
INDEX_PAGE_SIZE = 10_000


class EmbyError(RuntimeError):
    """The server could not be reached or answered with an error."""


class EmbyAuthError(EmbyError):
    """The API key was rejected."""


@dataclass(frozen=True)
class EmbyServerInfo:
    name: str
    version: str
    server_id: str


@dataclass(frozen=True)
class EmbyPlaylist:
    playlist_id: str
    name: str


@dataclass(frozen=True)
class EmbyPlaylistEntry:
    #: The membership's own id, which is what removal addresses.
    entry_id: str
    item_id: str


class EmbyClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout: float = 60.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=f'{base_url.rstrip("/")}/emby',
            headers={'X-Emby-Token': api_key, 'Accept': 'application/json'},
            timeout=timeout,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    # -- server ---------------------------------------------------------------

    async def system_info(self) -> EmbyServerInfo:
        """The server's identity; also the cheapest check that the key is accepted."""
        body = await self._get_json('/System/Info')
        return EmbyServerInfo(
            name=str(body.get('ServerName') or ''),
            version=str(body.get('Version') or ''),
            server_id=str(body.get('Id') or ''),
        )

    # -- library ---------------------------------------------------------------

    async def movie_index(self, avid_of: Callable[[str], str]) -> dict[str, str]:
        """Every movie's AVID mapped to its item id.

        ``avid_of`` reads an AVID out of a directory name: the STRM mapping keeps
        one directory per title named after it, so the parent directory of the
        item's path is the key. When two items share an AVID (a re-encode next
        to the original) the first one the server lists wins.
        """
        index: dict[str, str] = {}
        start = 0
        while True:
            body = await self._get_json(
                '/Items',
                params={
                    'Recursive': 'true',
                    'IncludeItemTypes': 'Movie',
                    'Fields': 'Path',
                    'StartIndex': start,
                    'Limit': INDEX_PAGE_SIZE,
                },
            )
            items = body.get('Items') or []
            for item in items:
                path = str(item.get('Path') or '')
                item_id = str(item.get('Id') or '')
                if not path or not item_id:
                    continue
                avid = avid_of(basename(dirname(path)))
                if avid:
                    index.setdefault(avid, item_id)
            start += len(items)
            total = int(body.get('TotalRecordCount') or 0)
            if not items or start >= total:
                return index

    # -- playlists -------------------------------------------------------------

    async def list_playlists(self) -> tuple[EmbyPlaylist, ...]:
        body = await self._get_json(
            '/Items',
            params={'Recursive': 'true', 'IncludeItemTypes': 'Playlist'},
        )
        return tuple(
            EmbyPlaylist(playlist_id=str(item['Id']), name=str(item.get('Name') or ''))
            for item in body.get('Items') or []
            if item.get('Id')
        )

    async def create_playlist(self, name: str, item_ids: Sequence[str]) -> str:
        """Create a video playlist holding ``item_ids`` in that order; returns its id."""
        body = await self._post_json(
            '/Playlists',
            params={'Name': name, 'Ids': ','.join(item_ids), 'MediaType': 'Video'},
        )
        playlist_id = str(body.get('Id') or '')
        if not playlist_id:
            msg = 'playlist creation returned no id'
            raise EmbyError(msg)
        return playlist_id

    async def playlist_entries(self, playlist_id: str) -> tuple[EmbyPlaylistEntry, ...]:
        body = await self._get_json(f'/Playlists/{playlist_id}/Items')
        return tuple(
            EmbyPlaylistEntry(entry_id=str(item['PlaylistItemId']), item_id=str(item['Id']))
            for item in body.get('Items') or []
            if item.get('PlaylistItemId') and item.get('Id')
        )

    async def add_entries(self, playlist_id: str, item_ids: Sequence[str]) -> None:
        if not item_ids:
            return
        await self._request('POST', f'/Playlists/{playlist_id}/Items', params={'Ids': ','.join(item_ids)})

    async def remove_entries(self, playlist_id: str, entry_ids: Sequence[str]) -> None:
        if not entry_ids:
            return
        await self._request('DELETE', f'/Playlists/{playlist_id}/Items', params={'EntryIds': ','.join(entry_ids)})

    async def delete_playlist(self, playlist_id: str) -> None:
        await self._request('POST', '/Items/Delete', params={'Ids': playlist_id})

    # -- transport -------------------------------------------------------------

    async def _get_json(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return _json_object(await self._request('GET', path, params=params))

    async def _post_json(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return _json_object(await self._request('POST', path, params=params))

    async def _request(self, method: str, path: str, *, params: dict[str, Any] | None = None) -> httpx.Response:
        try:
            response = await self._client.request(method, path, params=params)
        except httpx.HTTPError as exc:
            msg = f'{method} {path}: {exc.__class__.__name__}: {exc}'
            raise EmbyError(msg) from exc
        if response.status_code == _HTTP_UNAUTHORIZED:
            msg = f'{method} {path}: the API key was rejected'
            raise EmbyAuthError(msg)
        if response.is_error:
            msg = f'{method} {path}: HTTP {response.status_code}'
            raise EmbyError(msg)
        return response


def _json_object(response: httpx.Response) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError as exc:
        msg = f'{response.request.method} {response.request.url.path}: not a JSON response'
        raise EmbyError(msg) from exc
    if not isinstance(body, dict):
        msg = f'{response.request.method} {response.request.url.path}: unexpected JSON shape'
        raise EmbyError(msg)
    return body
