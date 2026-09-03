"""AVBase catalog client: talents with their aliases, their works, a work's cast.

AVBase puts a Cloudflare challenge in front of its pages that plain HTTP
clients fail, but the Next.js data routes behind them answer to a browser TLS
fingerprint, which curl_cffi provides. Only the catalog goes through here; the
talent feeds are open endpoints the subscription poller reads with httpx.

The data routes are keyed by the site's build id, which changes on every
deployment: it is read from the home page once and refreshed when a route
answers 404, so a real miss is only reported after the id was confirmed.
"""

import asyncio
import logging
import re
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol
from urllib.parse import quote, unquote, urlsplit

from curl_cffi.requests import AsyncSession
from defusedxml.ElementTree import fromstring

log = logging.getLogger('embyx-manager.avbase')

DEFAULT_HOST = 'https://www.avbase.net'
WORKS_PER_PAGE = 30
_BUILD_ID_RE = re.compile(r'"buildId":"([^"]+)"')
_JS_DATE_RE = re.compile(r'^[A-Za-z]{3} ([A-Za-z]{3}) (\d{1,2}) (\d{4})')
_MONTHS = {
    name: index
    for index, name in enumerate(
        ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'), start=1
    )
}
_HTTP_OK = 200
_HTTP_NOT_FOUND = 404


class AvbaseError(RuntimeError):
    """The catalog could not be read."""


class AvbaseUnavailableError(AvbaseError):
    """A challenge page or another non-JSON answer stood in for the data route."""


class _Response(Protocol):
    status_code: int
    text: str
    headers: Mapping[str, str]

    def json(self) -> Any: ...


class _Session(Protocol):
    async def get(self, url: str, *, params: Mapping[str, Any] | None = None) -> _Response: ...

    async def close(self) -> None: ...


@dataclass(frozen=True)
class AvbaseTalent:
    talent_id: int
    #: The primary name; AVBase lists the talent's page under it.
    name: str
    #: Every other name the talent is credited under.
    aliases: tuple[str, ...]
    total_works: int

    @property
    def names(self) -> tuple[str, ...]:
        return (self.name, *self.aliases)


@dataclass(frozen=True)
class AvbaseCastMember:
    actor_id: int
    name: str
    #: The talent the credited name belongs to; None when the listing omits it.
    talent_id: int | None = None


@dataclass(frozen=True)
class AvbaseWork:
    #: The bare ID, without the storefront prefix (``moodyz:MIZD-555`` -> ``MIZD-555``).
    work_id: str
    prefix: str
    title: str
    #: The earliest product date; an approximation of the release (see cadence).
    release_date: date | None
    cast: tuple[AvbaseCastMember, ...]


class AvbaseClient:
    def __init__(  # noqa: PLR0913
        self,
        *,
        host: str = DEFAULT_HOST,
        impersonate: str = 'chrome',
        timeout: float = 30.0,
        max_concurrency: int = 4,
        proxy: str | None = None,
        session: _Session | None = None,
    ) -> None:
        self.host = host.rstrip('/')
        self._session: _Session = session or AsyncSession(
            impersonate=impersonate,
            timeout=timeout,
            proxy=proxy,
            headers={'Referer': f'{self.host}/', 'Accept-Language': 'ja,en;q=0.8'},
        )
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._build_id: str | None = None
        self._build_lock = asyncio.Lock()

    async def aclose(self) -> None:
        await self._session.close()

    async def build_id(self, *, refresh: bool = False) -> str:
        async with self._build_lock:
            if self._build_id is None or refresh:
                response = await self._get(f'{self.host}/')
                match = _BUILD_ID_RE.search(response.text) if response.status_code == _HTTP_OK else None
                if match is None:
                    msg = f'no build id on the AVBase home page (HTTP {response.status_code})'
                    raise AvbaseUnavailableError(msg)
                self._build_id = match.group(1)
            return self._build_id

    async def talent(self, name: str) -> AvbaseTalent | None:
        """The talent credited as ``name``; any of its aliases finds the same one."""
        props = await self._talent_page(name, page=1)
        return _talent_from_props(props) if props is not None else None

    async def find_talent(self, query: str) -> AvbaseTalent | None:
        """The talent behind whatever a person pastes: a name, an id, or a talent URL.

        Talent pages are addressed by name, so a numeric id (or an id URL, which
        is what the feed links carry) goes through the feed, whose channel link
        names the talent; the name then finds the aliases as usual.
        """
        try:
            talent_id, name = parse_talent_query(query)
        except ValueError:
            return None
        if talent_id is not None:
            return await self._talent_by_id(talent_id)
        return await self.talent(name) if name else None

    async def _talent_by_id(self, talent_id: int) -> AvbaseTalent | None:
        response = await self._get(f'{self.host}/talents/{talent_id}/feed')
        if response.status_code == _HTTP_NOT_FOUND:
            return None
        if response.status_code != _HTTP_OK:
            msg = f'AVBase answered HTTP {response.status_code} for the feed of talent {talent_id}'
            raise AvbaseUnavailableError(msg)
        name = _talent_name_from_feed(response.text)
        if name is None:
            return None
        talent = await self.talent(name)
        if talent is not None and talent.talent_id == talent_id:
            return talent
        return AvbaseTalent(talent_id=talent_id, name=name, aliases=(), total_works=0)

    async def talent_pages(self, name: str) -> AsyncIterator[tuple[int, int, list[AvbaseWork]]]:
        """The talent's works page by page as ``(page, pages, works)``, newest release first."""
        first = await self._talent_page(name, page=1)
        if first is None:
            msg = f'AVBase has no talent named {name!r}'
            raise AvbaseError(msg)
        total = int(first.get('total') or 0)
        pages = max(1, -(-total // WORKS_PER_PAGE))
        yield 1, pages, [_work_from_listing(entry) for entry in first.get('works') or []]
        for page in range(2, pages + 1):
            props = await self._talent_page(name, page=page)
            if props is None:
                break
            yield page, pages, [_work_from_listing(entry) for entry in props.get('works') or []]

    async def talent_works(self, name: str) -> list[AvbaseWork]:
        """Every work of the talent credited as ``name``, newest release first."""
        return [work async for _, _, works in self.talent_pages(name) for work in works]

    async def search_works(self, query: str) -> list[AvbaseWork]:
        """Works matching a query (an ID, a name, a title fragment) as the site's search lists them.

        Listing entries credit actors by name only; talent ids come from :meth:`work`.
        """
        data = await self._data('works.json', {'q': query})
        if data is None:
            return []
        return [_work_from_listing(entry) for entry in (data.get('pageProps') or {}).get('works') or []]

    async def work(self, work_id: str) -> AvbaseWork | None:
        """One work by its ID, with its credited cast and their talent ids.

        A work from a storefront with a prefix only answers under
        ``<prefix>:<id>``; a bare ID goes through the search first, which
        reports the prefix, so callers can pass whatever ID they have.
        """
        if ':' in work_id:
            return await self._work_route(work_id)
        wanted = strip_prefix(work_id).casefold()
        for candidate in await self.search_works(work_id):
            if candidate.work_id.casefold() == wanted:
                route_id = f'{candidate.prefix}:{candidate.work_id}' if candidate.prefix else candidate.work_id
                return await self._work_route(route_id)
        return None

    async def _work_route(self, work_id: str) -> AvbaseWork | None:
        data = await self._data(f'works/{quote(work_id, safe="")}.json', {'id': work_id})
        if data is None:
            return None
        work = (data.get('pageProps') or {}).get('work')
        return _work_from_detail(work) if work else None

    async def _talent_page(self, name: str, *, page: int) -> dict[str, Any] | None:
        params: dict[str, Any] = {'name': name}
        if page > 1:
            params['page'] = page
        data = await self._data(f'talents/{quote(name, safe="")}.json', params)
        if data is None:
            return None
        props = data.get('pageProps') or {}
        return props if props.get('talent') else None

    async def _data(self, path: str, params: Mapping[str, Any]) -> dict[str, Any] | None:
        """One Next.js data route; None when it does not exist.

        A 404 is ambiguous: the route may be gone, or the build id may be stale
        after a deployment. The id is refreshed once, and the request repeated
        only if it actually changed.
        """
        build = await self.build_id()
        for _ in range(2):
            response = await self._get(f'{self.host}/_next/data/{build}/{path}', params=params)
            if response.status_code == _HTTP_NOT_FOUND:
                refreshed = await self.build_id(refresh=True)
                if refreshed == build:
                    return None
                build = refreshed
                continue
            if response.status_code != _HTTP_OK:
                msg = f'AVBase answered HTTP {response.status_code} for {path}'
                raise AvbaseUnavailableError(msg)
            if 'json' not in (response.headers.get('content-type') or ''):
                msg = f'AVBase answered {path} with something other than JSON (a challenge page?)'
                raise AvbaseUnavailableError(msg)
            payload = response.json()
            if not isinstance(payload, dict):
                msg = f'AVBase answered {path} with a non-object'
                raise AvbaseUnavailableError(msg)
            return payload
        return None

    async def _get(self, url: str, *, params: Mapping[str, Any] | None = None) -> _Response:
        async with self._semaphore:
            return await self._session.get(url, params=params)


_TALENT_PATH_RE = re.compile(r'/talents/([^/?#]+)(?:/feed)?/?$')


def parse_talent_query(query: str) -> tuple[int | None, str]:
    """Split a pasted talent reference into a numeric id or a name.

    Accepts a bare name or alias, a bare id, or an AVBase talent URL of either
    form (with or without the trailing ``/feed``). Anything else is a ValueError.
    """
    text = query.strip()
    if '://' in text or text.startswith('/'):
        match = _TALENT_PATH_RE.search(urlsplit(text).path)
        if match is None:
            msg = f'not an AVBase talent URL: {query!r}'
            raise ValueError(msg)
        text = unquote(match.group(1)).strip()
    if not text:
        msg = 'empty talent reference'
        raise ValueError(msg)
    if text.isdigit():
        return int(text), ''
    return None, text


def _talent_name_from_feed(xml_text: str) -> str | None:
    """The talent name from its feed: the channel link is the name-addressed page."""
    try:
        # A str with an encoding declaration is refused by ElementTree; bytes are not.
        root = fromstring(xml_text.encode('utf-8'), forbid_dtd=True)
    except Exception:  # noqa: BLE001 - any malformed feed simply names nobody
        return None
    channel = root.find('channel')
    if channel is None:
        return None
    link = (channel.findtext('link') or '').strip()
    segment = unquote(urlsplit(link).path.rstrip('/').rsplit('/', 1)[-1]).strip() if link else ''
    if segment and not segment.isdigit():
        return segment
    match = re.search(r'「(.+?)」', channel.findtext('title') or '')
    return match.group(1).strip() if match else None


def strip_prefix(work_id: str) -> str:
    """``moodyz:MIZD-555`` -> ``MIZD-555``; a bare ID is returned as is."""
    return work_id.rsplit(':', 1)[-1].strip()


def parse_release_date(value: object) -> date | None:
    """AVBase dates come as JavaScript ``Date`` strings or ISO text."""
    if not isinstance(value, str) or not value:
        return None
    match = _JS_DATE_RE.match(value)
    if match is not None:
        month = _MONTHS.get(match.group(1))
        if month is None:
            return None
        try:
            return date(int(match.group(3)), month, int(match.group(2)))
        except ValueError:
            return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _talent_from_props(props: Mapping[str, Any]) -> AvbaseTalent:
    talent = props['talent']
    primary = talent.get('primary') or {}
    name = str(primary.get('name') or props.get('name') or '').strip()
    seen = {name}
    aliases: list[str] = []
    for actor in talent.get('actors') or []:
        alias = str((actor or {}).get('name') or '').strip()
        if alias and alias not in seen:
            seen.add(alias)
            aliases.append(alias)
    return AvbaseTalent(
        talent_id=int(talent['id']),
        name=name,
        aliases=tuple(aliases),
        total_works=int(props.get('total') or 0),
    )


def _cast_from_actor(actor: Mapping[str, Any]) -> AvbaseCastMember | None:
    actor_id = actor.get('id')
    name = str(actor.get('name') or '').strip()
    if not isinstance(actor_id, int) or not name:
        return None
    talent = actor.get('talent')
    talent_id = talent.get('id') if isinstance(talent, Mapping) else None
    return AvbaseCastMember(actor_id=actor_id, name=name, talent_id=talent_id if isinstance(talent_id, int) else None)


def _work_from_listing(entry: Mapping[str, Any]) -> AvbaseWork:
    cast = (_cast_from_actor(actor) for actor in entry.get('actors') or [] if isinstance(actor, Mapping))
    return AvbaseWork(
        work_id=strip_prefix(str(entry.get('work_id') or '')),
        prefix=str(entry.get('prefix') or ''),
        title=str(entry.get('title') or ''),
        release_date=parse_release_date(entry.get('min_date')),
        cast=tuple(member for member in cast if member is not None),
    )


def _work_from_detail(work: Mapping[str, Any]) -> AvbaseWork:
    members = []
    for entry in work.get('casts') or []:
        actor = entry.get('actor') if isinstance(entry, Mapping) else None
        member = _cast_from_actor(actor) if isinstance(actor, Mapping) else None
        if member is not None:
            members.append(member)
    return AvbaseWork(
        work_id=strip_prefix(str(work.get('work_id') or '')),
        prefix=str(work.get('prefix') or ''),
        title=str(work.get('title') or ''),
        release_date=parse_release_date(work.get('min_date')),
        cast=tuple(members),
    )
