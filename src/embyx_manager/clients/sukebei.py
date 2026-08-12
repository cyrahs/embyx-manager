"""Async Sukebei (nyaa.si) search client with size/trust-weighted magnet selection."""

import asyncio
import logging

import httpx
import humanfriendly
import nyaapy.parser
import nyaapy.torrent
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

log = logging.getLogger('embyx-manager.sukebei')

_NOTICE_LEVEL = 25
# Prefer the largest trusted result unless an untrusted one is materially larger.
TRUSTED_SIZE_TOLERANCE = 0.8


class SukebeiClient:
    site = nyaapy.torrent.TorrentSite.SUKEBEINYAASI

    def __init__(self, *, proxy: str | None = None, timeout: float = 20, max_concurrency: int = 5) -> None:
        self._client = httpx.AsyncClient(
            proxy=proxy or None,
            limits=httpx.Limits(max_connections=max_concurrency),
            timeout=timeout,
        )
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self.url = self.site.value

    async def aclose(self) -> None:
        await self._client.aclose()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        reraise=True,
    )
    async def search(
        self,
        keyword: str,
        category: int = 2,
        subcategory: int = 2,
        filters: int = 0,
        page: int = 0,
    ) -> list[dict] | None:
        # category 2 and subcategory 2 refer to videos
        params: dict[str, int | str] = {
            'f': filters,
            'c': f'{category}_{subcategory}',
            'q': keyword,
        }
        if page > 0:
            params['p'] = page
        async with self._semaphore:
            try:
                res = await self._client.get(self.url, params=params)
                res.raise_for_status()
            except (httpx.HTTPError, httpx.TimeoutException):
                log.exception('Failed to get %s with %s', self.url, params)
                raise
        return nyaapy.parser.parse_nyaa(res.text, limit=None, site=self.site)

    async def get_magnet(
        self,
        keyword: str,
        category: int = 2,
        subcategory: int = 2,
        filters: int = 0,
        page: int = 0,
    ) -> str | None:
        try:
            result = await self.search(keyword, category=category, subcategory=subcategory, filters=filters, page=page)
        except (httpx.HTTPError, httpx.TimeoutException):
            return None
        if not result:
            return None
        parsed_result = []
        for item in result:
            try:
                item['size_int'] = humanfriendly.parse_size(item['size'])
            except humanfriendly.InvalidSize:
                log.warning('Skipping result with invalid size %s for %s', item.get('size'), keyword)
                continue
            item['magnet'] = item['magnet'].split('&')[0] + f'&dn={keyword}'
            parsed_result.append(item)
        if not parsed_result:
            return None
        parsed_result.sort(key=lambda x: x['size_int'], reverse=True)
        trusted = [x for x in parsed_result if x['type'] == 'trusted']
        chosen = (
            parsed_result.index(trusted[0])
            if trusted and trusted[0]['size_int'] >= parsed_result[0]['size_int'] * TRUSTED_SIZE_TOLERANCE
            else 0
        )
        display_result = parsed_result[:5]
        if trusted and trusted[0] not in display_result:
            display_result.append(trusted[0])
        log.log(_NOTICE_LEVEL, 'Found %d results for %s from searching:', len(parsed_result), keyword)
        log_lines: list[str] = ['Display top 5 largest + trusted:']
        for index, item in enumerate(display_result):
            trust_mark = '🔰' if item['type'] == 'trusted' else '--'
            chosen_mark = '✅' if index == chosen else '--'
            log_lines.append(f'{chosen_mark}{trust_mark}{item["size"]} {item["name"]}')
            log_lines.append(item['magnet'])
        log.info('\n'.join(log_lines))

        return parsed_result[chosen]['magnet']
