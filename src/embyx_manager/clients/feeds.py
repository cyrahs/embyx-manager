"""Fetching subscribed feeds over HTTP."""

import httpx

#: A feed is a few hundred items at most; anything bigger is not one.
MAX_FEED_BYTES = 8 * 1024 * 1024


class FeedTooLargeError(RuntimeError):
    def __init__(self, url: str, limit: int) -> None:
        super().__init__(f'feed at {url} exceeds {limit} bytes')


class HttpFeedFetcher:
    """One shared client for every subscription; ``fetch`` returns the raw body.

    RSSHub's javbus route builds its feed from one detail page per item, so a
    cold fetch can take a minute; the read timeout allows for that rather than
    treating a slow first fetch as a failure.
    """

    def __init__(self, *, timeout: float = 120.0, proxy: str | None = None, max_bytes: int = MAX_FEED_BYTES) -> None:
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, connect=20.0),
            follow_redirects=True,
            max_redirects=5,
            headers={
                'Accept': 'application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.5',
                'User-Agent': 'embyx-manager/1.0 (+feed poller)',
            },
            proxy=proxy or None,
        )
        self._max_bytes = max_bytes

    async def fetch(self, url: str) -> bytes:
        async with self._client.stream('GET', url) as response:
            response.raise_for_status()
            chunks: list[bytes] = []
            size = 0
            async for chunk in response.aiter_bytes():
                size += len(chunk)
                if size > self._max_bytes:
                    raise FeedTooLargeError(url, self._max_bytes)
                chunks.append(chunk)
        return b''.join(chunks)

    async def aclose(self) -> None:
        await self._client.aclose()
