"""The subscription registry: which feeds the poller reads, and where they file.

A subscription is either a plain feed URL (an RSSHub route, a sukebei search, an
AVBase talent feed pasted as-is) or, once the AVBase catalog is wired in, a
talent whose feed URL is derived from its id. Every subscription belongs to one
of the configured RSS categories, which is what decides the offline directory
its downloads go to and the ledger source they are recorded under.

The poller remembers, per subscription, a bounded list of the item keys it has
seen. That cursor exists only so an item is not re-read on every poll — the
acquisition ledger is what decides whether an AVID needs anything done, and it
does so by AVID, not by item.
"""

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from urllib.parse import urlsplit

import asyncpg

from embyx_manager.db import Database

AVBASE_FEED_URL = 'https://www.avbase.net/talents/{talent_id}/feed'
#: Item keys remembered per subscription. Larger than any feed we read, so an
#: item that drops out of a feed and comes back is still known.
CURSOR_LIMIT = 500


class SubscriptionKind(StrEnum):
    RSS = 'rss'
    #: Created by the AVBase catalog integration; the feed URL follows from the id.
    AVBASE_TALENT = 'avbase_talent'


class SubscriptionExistsError(Exception):
    """The same feed (or talent) is already subscribed."""

    def __init__(self, what: str) -> None:
        super().__init__(f'already subscribed: {what}')
        self.what = what


@dataclass(frozen=True)
class SubscriptionRecord:
    id: int
    kind: SubscriptionKind
    category: str
    enabled: bool
    url: str | None
    talent_id: int | None
    name: str | None
    aliases: tuple[str, ...]
    cursor: tuple[str, ...]
    #: True until the first poll, which then records the feed's current items as
    #: seen without ingesting them — for subscriptions whose backlog a catalog
    #: scan already covered.
    seed_pending: bool
    last_polled_at: datetime | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime

    @property
    def feed_url(self) -> str:
        if self.kind is SubscriptionKind.AVBASE_TALENT:
            return AVBASE_FEED_URL.format(talent_id=self.talent_id)
        return self.url or ''

    @property
    def display_name(self) -> str:
        return self.name or self.feed_url


def validate_feed_url(value: str) -> str:
    """An absolute HTTP(S) URL without credentials; a query string is fine."""
    url = value.strip()
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        msg = 'feed url must be an absolute HTTP(S) URL'
        raise ValueError(msg) from exc
    if parsed.scheme not in {'http', 'https'} or not parsed.hostname or port == 0:
        msg = 'feed url must be an absolute HTTP(S) URL'
        raise ValueError(msg)
    if parsed.username is not None or parsed.password is not None:
        msg = 'feed url must not include credentials'
        raise ValueError(msg)
    return url


def trim_cursor(keys: Sequence[str]) -> tuple[str, ...]:
    """Newest-last, de-duplicated, capped at CURSOR_LIMIT."""
    return tuple(dict.fromkeys(keys))[-CURSOR_LIMIT:]


class SubscriptionRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def list(self) -> tuple[SubscriptionRecord, ...]:
        pool = await self._database.get_pool()
        rows = await pool.fetch(
            'SELECT * FROM feed_subscriptions ORDER BY category, kind, COALESCE(name, url), id',
        )
        return tuple(_from_row(row) for row in rows)

    async def get(self, subscription_id: int) -> SubscriptionRecord | None:
        pool = await self._database.get_pool()
        row = await pool.fetchrow('SELECT * FROM feed_subscriptions WHERE id = $1', subscription_id)
        return _from_row(row) if row is not None else None

    async def add_rss(
        self,
        *,
        url: str,
        category: str,
        now: datetime,
        name: str | None = None,
        seed_pending: bool = False,
    ) -> SubscriptionRecord:
        pool = await self._database.get_pool()
        try:
            row = await pool.fetchrow(
                """
                INSERT INTO feed_subscriptions
                    (kind, category, enabled, url, name, seed_pending, created_at, updated_at)
                VALUES ('rss', $1, TRUE, $2, $3, $4, $5, $5)
                RETURNING *
                """,
                category,
                url,
                name,
                seed_pending,
                now,
            )
        except asyncpg.UniqueViolationError as exc:
            raise SubscriptionExistsError(url) from exc
        return _from_row(row)

    async def add_talent(  # noqa: PLR0913
        self,
        *,
        talent_id: int,
        name: str,
        aliases: Sequence[str],
        category: str,
        now: datetime,
        seed_pending: bool = False,
    ) -> SubscriptionRecord:
        pool = await self._database.get_pool()
        try:
            row = await pool.fetchrow(
                """
                INSERT INTO feed_subscriptions
                    (kind, category, enabled, talent_id, name, aliases_json, seed_pending, created_at, updated_at)
                VALUES ('avbase_talent', $1, TRUE, $2, $3, $4, $5, $6, $6)
                RETURNING *
                """,
                category,
                talent_id,
                name,
                json.dumps(list(aliases)),
                seed_pending,
                now,
            )
        except asyncpg.UniqueViolationError as exc:
            what = f'talent {talent_id}'
            raise SubscriptionExistsError(what) from exc
        return _from_row(row)

    async def update(
        self,
        subscription_id: int,
        *,
        now: datetime,
        enabled: bool | None = None,
        category: str | None = None,
        url: str | None = None,
    ) -> SubscriptionRecord | None:
        pool = await self._database.get_pool()
        try:
            row = await pool.fetchrow(
                """
                UPDATE feed_subscriptions
                SET enabled = COALESCE($2, enabled), category = COALESCE($3, category),
                    url = COALESCE($5, url), updated_at = $4
                WHERE id = $1
                RETURNING *
                """,
                subscription_id,
                enabled,
                category,
                now,
                url,
            )
        except asyncpg.UniqueViolationError as exc:
            raise SubscriptionExistsError(url or '') from exc
        return _from_row(row) if row is not None else None

    async def delete(self, subscription_id: int) -> bool:
        pool = await self._database.get_pool()
        status = await pool.execute('DELETE FROM feed_subscriptions WHERE id = $1', subscription_id)
        return int(status.rsplit(' ', 1)[-1]) == 1

    async def record_poll(
        self,
        subscription_id: int,
        *,
        now: datetime,
        cursor: Sequence[str] | None,
        error: str | None,
    ) -> None:
        """Note the outcome of one poll; ``cursor`` None keeps the stored one.

        A successful poll hands over the new cursor and clears the error; a
        failed one leaves the cursor alone so the items it did not get to are
        read next time. Storing a cursor also settles a pending seed.
        """
        pool = await self._database.get_pool()
        await pool.execute(
            """
            UPDATE feed_subscriptions
            SET last_polled_at = $2, last_error = $3,
                cursor_json = COALESCE($4, cursor_json),
                seed_pending = CASE WHEN $4 IS NULL THEN seed_pending ELSE FALSE END,
                updated_at = $2
            WHERE id = $1
            """,
            subscription_id,
            now,
            error,
            None if cursor is None else json.dumps(list(trim_cursor(cursor))),
        )


def _from_row(row: asyncpg.Record) -> SubscriptionRecord:
    return SubscriptionRecord(
        id=row['id'],
        kind=SubscriptionKind(row['kind']),
        category=row['category'],
        enabled=row['enabled'],
        url=row['url'],
        talent_id=row['talent_id'],
        name=row['name'],
        aliases=tuple(json.loads(row['aliases_json'])),
        cursor=tuple(json.loads(row['cursor_json'])),
        seed_pending=row['seed_pending'],
        last_polled_at=row['last_polled_at'],
        last_error=row['last_error'],
        created_at=row['created_at'],
        updated_at=row['updated_at'],
    )
