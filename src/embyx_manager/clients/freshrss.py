"""Async FreshRSS Google-Reader-API client."""

import logging

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

log = logging.getLogger('embyx-manager.freshrss')

RETRYABLE_EXCEPTIONS = (httpx.HTTPError, httpx.RequestError, httpx.TimeoutException)
ITEM_RETRYABLE_EXCEPTIONS = (*RETRYABLE_EXCEPTIONS, KeyError, ValueError)
_AUTH_FAILURE_STATUSES = frozenset({401, 403})


class FreshRSSClient:
    def __init__(self, *, url: str, api_key: str, proxy: str | None = None, timeout: float = 10) -> None:
        self._url = url.rstrip('/')
        self._headers = {'Authorization': f'GoogleLogin auth={api_key}'}
        self._timeout = timeout
        self._client = httpx.AsyncClient(proxy=proxy or None)
        self._edit_token: str | None = None

    async def aclose(self) -> None:
        await self._client.aclose()
        self._edit_token = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        reraise=True,
    )
    async def fetch_token(self) -> str:
        """Fetch a fresh edit token; doubles as an authenticated connectivity check."""
        res = await self._client.get(f'{self._url}/token', headers=self._headers, timeout=self._timeout)
        res.raise_for_status()
        self._edit_token = res.text
        return self._edit_token

    async def _get_edit_token(self) -> str:
        if self._edit_token is None:
            return await self.fetch_token()
        return self._edit_token

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(ITEM_RETRYABLE_EXCEPTIONS),
        reraise=True,
    )
    async def get_items(self, label: str) -> list[dict]:
        """Get unread RSS items carrying a specific label."""
        params = {'xt': 'user/-/state/com.google/read'}
        items: list[dict] = []
        while True:
            url = f'{self._url}/stream/contents/user/-/label/{label}'
            res = await self._client.get(url, headers=self._headers, params=params, timeout=self._timeout)
            res.raise_for_status()
            content = res.json()
            items += content['items']
            if content.get('continuation'):
                params['c'] = content['continuation']
            else:
                break
        return items

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        reraise=True,
    )
    async def read_items(self, item_ids: list[str]) -> None:
        """Mark multiple items as read in a single API call."""
        if not item_ids:
            return
        body = {
            'i': item_ids,
            'a': 'user/-/state/com.google/read',
            'T': await self._get_edit_token(),
        }
        res = await self._client.post(
            f'{self._url}/edit-tag',
            headers=self._headers,
            data=body,
            timeout=self._timeout,
        )
        try:
            res.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in _AUTH_FAILURE_STATUSES:
                self._edit_token = None
            raise
