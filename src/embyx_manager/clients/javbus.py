"""Async JavBus scraping client: actor catalogs and per-video magnet listings."""

import asyncio
import inspect
import logging
import random
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from urllib.parse import unquote, urlparse

import httpx
import humanfriendly
from pyquery import PyQuery
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

log = logging.getLogger('embyx-manager.javbus')

PageProgressCallback = Callable[[int, int | None, int | None], Awaitable[None] | None]

DEFAULT_HOST = 'https://www.javbus.com'
DEFAULT_MAX_ACTOR_PAGES = 200
_NOT_FOUND_STATUSES = frozenset({404, 410})
_ACTOR_ID_RE = re.compile(r'^[A-Za-z0-9_-]{1,32}$')


@dataclass(frozen=True)
class JavBusActor:
    actor_id: str
    name: str


@dataclass(frozen=True)
class JavBusActorPage:
    """A star page's display name and the video IDs listed on its first page."""

    actor_id: str
    name: str
    video_ids: tuple[str, ...]


class JavBusPaginationError(RuntimeError):
    """Raised when JavBus pagination cannot be completed without silently losing pages."""


class JavBusClient:
    def __init__(
        self,
        *,
        host: str = DEFAULT_HOST,
        proxy: str | None = None,
        timeout: float = 60,
        max_connections: int = 10,
        max_actor_pages: int = DEFAULT_MAX_ACTOR_PAGES,
    ) -> None:
        self.host = host.rstrip('/')
        self.max_actor_pages = max_actor_pages
        self._client = httpx.AsyncClient(
            headers={
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Cookie': 'existmag=all',
            },
            timeout=timeout,
            limits=httpx.Limits(max_keepalive_connections=0, max_connections=max_connections),
            proxy=proxy or None,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def get_actor(self, actor_id: str) -> JavBusActorPage | None:
        """The star page's display name and first-page video IDs; None when there is no such star."""
        response = await self._fetch_actor_page(actor_id, 1)
        if response is None:
            return None
        doc = PyQuery(response.text)
        name = ' '.join(doc('.avatar-box .pb10').eq(0).text().split())
        if not name:
            name = ' '.join(doc('title').text().split(' - ')[0].split())
        ids, _ = self._parse_actor_page(actor_id, response.text)
        return JavBusActorPage(actor_id=actor_id, name=name, video_ids=ids)

    async def scrape_one_page(self, actor_id: str, page: int) -> list[str]:
        res = await self._fetch_actor_page(actor_id, page)
        if res is None:
            return []
        ids, _ = self._parse_actor_page(actor_id, res.text)
        return list(ids)

    async def get_total_page(  # noqa: C901
        self,
        actor_id: str,
        progress_callback: PageProgressCallback | None = None,
    ) -> int:
        """Discover the real final page instead of trusting one sliding pagination window."""
        first = await self._fetch_actor_page(actor_id, 1)
        if first is None:
            message = f'JavBus actor {actor_id!r} was not found'
            raise JavBusPaginationError(message)

        ids, linked_pages = self._parse_actor_page(actor_id, first.text)
        await self._notify_progress(progress_callback, 0, None, 1)
        if not ids and not linked_pages:
            return 1

        fingerprints = {ids} if ids else set()
        highest = 1
        current_links = linked_pages

        while True:
            linked_max = max(current_links, default=highest)
            if linked_max > self.max_actor_pages:
                message = f'JavBus actor pagination exceeds the {self.max_actor_pages}-page safety limit'
                raise JavBusPaginationError(message)

            if linked_max > highest:
                highest = linked_max
                response = await self._fetch_actor_page(actor_id, highest)
                if response is None:
                    message = f'JavBus actor pagination has a gap at page {highest}'
                    raise JavBusPaginationError(message)
                page_ids, current_links = self._parse_actor_page(actor_id, response.text)
                if page_ids:
                    fingerprints.add(page_ids)
                await self._notify_progress(progress_callback, 0, None, highest)
                continue

            if highest >= self.max_actor_pages:
                message = f'JavBus actor pagination reached the {self.max_actor_pages}-page safety limit without an end'
                raise JavBusPaginationError(message)

            probe = highest + 1
            response = await self._fetch_actor_page(actor_id, probe)
            if response is None:
                return highest
            page_ids, page_links = self._parse_actor_page(actor_id, response.text)
            await self._notify_progress(progress_callback, 0, None, probe)
            if not page_ids:
                if max(page_links, default=highest) > highest:
                    message = f'JavBus actor pagination returned an empty gap at page {probe}'
                    raise JavBusPaginationError(message)
                return highest
            if page_ids in fingerprints:
                return highest

            fingerprints.add(page_ids)
            highest = probe
            current_links = page_links

    async def scrape(
        self,
        actor_id: str,
        progress_callback: PageProgressCallback | None = None,
    ) -> list[str]:
        await self._notify_progress(progress_callback, 0, None, None)
        if progress_callback is None:
            total_page = await self.get_total_page(actor_id)
        else:
            total_page = await self.get_total_page(actor_id, progress_callback=progress_callback)
        await self._notify_progress(progress_callback, 0, total_page, None)

        async def fetch(page: int) -> tuple[int, list[str]]:
            return page, await self.scrape_one_page(actor_id, page)

        pending = [asyncio.create_task(fetch(page)) for page in range(1, total_page + 1)]
        by_page: dict[int, list[str]] = {}
        completed = 0
        try:
            for task in asyncio.as_completed(pending):
                page, page_ids = await task
                by_page[page] = page_ids
                completed += 1
                await self._notify_progress(progress_callback, completed, total_page, page)
        finally:
            for task in pending:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        pages = [by_page[page] for page in range(1, total_page + 1)]

        if total_page > 1 and any(not page for page in pages):
            empty_page = next(index for index, page in enumerate(pages, start=1) if not page)
            message = f'JavBus actor pagination returned an empty page at {empty_page}'
            raise JavBusPaginationError(message)
        return list(dict.fromkeys(video_id for page in pages for video_id in page))

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        reraise=True,
    )
    async def get_video_actors(self, video_id: str) -> list[JavBusActor]:
        """Read the credited actors from a JavBus video detail page."""
        response = await self._client.get(url=f'{self.host}/{video_id}')
        response.raise_for_status()
        doc = PyQuery(response.text)
        actors: dict[str, JavBusActor] = {}
        for item in doc('a[href]').items():
            path = urlparse(str(item.attr('href') or '')).path.rstrip('/')
            match = re.fullmatch(r'/star/([^/]+)', path)
            if match is None:
                continue
            actor_id = unquote(match.group(1)).strip()
            key = actor_id.casefold()
            if not _ACTOR_ID_RE.fullmatch(actor_id):
                continue
            name_candidates = (
                item.text(),
                item.attr('title'),
                item.attr('aria-label'),
                item('img').attr('title'),
                item('img').attr('alt'),
            )
            name = next(
                (
                    cleaned
                    for candidate in name_candidates
                    if candidate and (cleaned := ' '.join(str(candidate).split())).casefold() != key
                ),
                '',
            )
            existing = actors.get(key)
            if existing is None:
                actors[key] = JavBusActor(actor_id=actor_id, name=name or actor_id)
            elif existing.name.casefold() == key and name:
                # JavBus can emit an empty metadata link before the named avatar card.
                actors[key] = JavBusActor(actor_id=existing.actor_id, name=name)
        return list(actors.values())

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        reraise=True,
    )
    async def _fetch_actor_page(self, actor_id: str, page: int) -> httpx.Response | None:
        url = f'{self.host}/star/{actor_id}' if page == 1 else f'{self.host}/star/{actor_id}/{page}'
        response = await self._client.get(url=url)
        if response.status_code in _NOT_FOUND_STATUSES:
            return None
        response.raise_for_status()
        return response

    def _parse_actor_page(self, actor_id: str, html: str) -> tuple[tuple[str, ...], frozenset[int]]:
        doc = PyQuery(html)
        ids: list[str] = []
        for item in doc('a.movie-box').items():
            href = str(item.attr('href') or '')
            path = urlparse(href).path.rstrip('/')
            video_id = path.rsplit('/', 1)[-1].strip().upper()
            if video_id:
                ids.append(video_id)

        page_pattern = re.compile(rf'/star/{re.escape(actor_id)}/([1-9]\d*)/?')
        linked_pages: set[int] = set()
        for item in doc('a[href]').items():
            href = str(item.attr('href') or '')
            parsed = urlparse(href)
            if parsed.netloc and parsed.netloc != urlparse(self.host).netloc:
                continue
            match = page_pattern.fullmatch(parsed.path)
            if match is not None:
                linked_pages.add(int(match.group(1)))
        return tuple(dict.fromkeys(ids)), frozenset(linked_pages)

    @staticmethod
    async def _notify_progress(
        callback: PageProgressCallback | None,
        completed: int,
        total: int | None,
        current: int | None,
    ) -> None:
        if callback is None:
            return
        result = callback(completed, total, current)
        if inspect.isawaitable(result):
            await result

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        reraise=True,
    )
    async def get_magnets(self, video_id: str) -> list[dict]:
        """Get magnet links with filesize for a video ID.

        Returns:
            List of dicts with keys: magnet, size, size_int
            Example: [{"magnet": "magnet:?xt=...", "size": "2.02GB", "size_int": 2168958156}]
        """
        url = f'{self.host}/{video_id}'
        res = await self._client.get(url)
        res.raise_for_status()
        content = res.text

        gid_match = re.search(r'var gid = (\d+);', content)
        uc_match = re.search(r'var uc = (\d+);', content)
        img_match = re.search(r"var img = '([^']+)';", content)

        if not (gid_match and uc_match and img_match):
            return []

        gid = gid_match.group(1)
        uc = uc_match.group(1)
        img = img_match.group(1)
        floor = str(random.randint(1, 1000))  # noqa: S311

        ajax_url = f'{self.host}/ajax/uncledatoolsbyajax.php?gid={gid}&lang=zh&img={img}&uc={uc}&floor={floor}'
        res = await self._client.get(ajax_url, headers={'Referer': url})
        res.raise_for_status()
        doc = PyQuery(res.text)

        results = []
        seen_magnets = set()
        # Each row contains: title+magnet, size, date
        for row in doc('tr').items():
            cells = row('td')
            if cells.length < 2:  # noqa: PLR2004
                continue
            raw_magnet = row('td:first-child a[href^="magnet"]').attr('href')
            if not raw_magnet:
                continue
            hash_match = re.search(r'urn:btih:([a-fA-F0-9]+)', raw_magnet)
            if not hash_match:
                continue
            magnet_hash = hash_match.group(1).lower()
            if magnet_hash in seen_magnets:
                continue
            seen_magnets.add(magnet_hash)

            magnet_link = f'magnet:?xt=urn:btih:{magnet_hash}&dn={video_id}'
            size_text = row('td:nth-child(2)').text().strip()
            try:
                size_int = humanfriendly.parse_size(size_text)
            except humanfriendly.InvalidSize:
                size_int = 0

            results.append({'magnet': magnet_link, 'size': size_text, 'size_int': size_int})

        return results
