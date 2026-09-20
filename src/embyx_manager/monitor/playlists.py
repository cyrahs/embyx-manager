"""Ranking lists mirrored as Emby playlists: the stored lists and their sync state.

One row per list the ranking source publishes. The entries are the source's
ranked codes after the library's exclusions; ``present`` and ``missing`` are what
the last sync found in the Emby index, in ranking order. ``enabled`` is the
operator's choice: a disabled list keeps its entries and statistics but has no
playlist on the server.
"""

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

import asyncpg

from embyx_manager.clients.jinjier import RankedEntry, RankedList
from embyx_manager.db import Database


@dataclass(frozen=True)
class PlaylistRecord:
    key: str
    kind: int
    note: str
    name: str
    enabled: bool
    entries: tuple[RankedEntry, ...]
    #: Codes the last sync found in the library, in ranking order.
    present: tuple[str, ...]
    #: Codes the last sync did not find, in ranking order.
    missing: tuple[str, ...]
    emby_playlist_id: str | None
    last_synced_at: datetime | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class PlaylistSourceState:
    #: The ``yyyymmdd`` stamp of the database the stored entries came from.
    database_name: str
    fetched_at: datetime


class PlaylistRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def list(self) -> tuple[PlaylistRecord, ...]:
        pool = await self._database.get_pool()
        rows = await pool.fetch('SELECT * FROM playlists ORDER BY kind, note, key')
        return tuple(_from_row(row) for row in rows)

    async def get(self, key: str) -> PlaylistRecord | None:
        pool = await self._database.get_pool()
        row = await pool.fetchrow('SELECT * FROM playlists WHERE key = $1', key)
        return _from_row(row) if row is not None else None

    async def replace_lists(self, lists: Sequence[RankedList], *, now: datetime) -> None:
        """Store a fresh download: new lists start enabled, known ones keep the operator's flag.

        A list the source no longer publishes keeps its row and playlist but is
        marked so; a renamed ranking should not silently take the operator's
        playlists away.
        """
        pool = await self._database.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            for ranked in lists:
                await connection.execute(
                    """
                    INSERT INTO playlists (key, kind, note, name, entries_json, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $6)
                    ON CONFLICT (key) DO UPDATE SET
                        kind = excluded.kind, note = excluded.note, name = excluded.name,
                        entries_json = excluded.entries_json, updated_at = excluded.updated_at
                    """,
                    ranked.key,
                    ranked.kind,
                    ranked.note,
                    ranked.name,
                    json.dumps([[entry.rank, entry.avid, entry.title] for entry in ranked.entries], ensure_ascii=False),
                    now,
                )
            await connection.execute(
                """
                UPDATE playlists SET last_error = $2, updated_at = $3
                WHERE key <> ALL($1::text[])
                """,
                [ranked.key for ranked in lists],
                'the source no longer publishes this list',
                now,
            )

    async def set_enabled(self, key: str, *, enabled: bool, now: datetime) -> PlaylistRecord | None:
        pool = await self._database.get_pool()
        row = await pool.fetchrow(
            'UPDATE playlists SET enabled = $2, updated_at = $3 WHERE key = $1 RETURNING *',
            key,
            enabled,
            now,
        )
        return _from_row(row) if row is not None else None

    async def record_sync(
        self,
        key: str,
        *,
        now: datetime,
        present: Sequence[str],
        missing: Sequence[str],
        emby_playlist_id: str | None,
    ) -> None:
        pool = await self._database.get_pool()
        await pool.execute(
            """
            UPDATE playlists
            SET present_json = $2, missing_json = $3, emby_playlist_id = $4,
                last_synced_at = $5, last_error = NULL, updated_at = $5
            WHERE key = $1
            """,
            key,
            json.dumps(list(present)),
            json.dumps(list(missing)),
            emby_playlist_id,
            now,
        )

    async def record_error(self, key: str, *, now: datetime, error: str) -> None:
        pool = await self._database.get_pool()
        await pool.execute(
            'UPDATE playlists SET last_error = $2, updated_at = $3 WHERE key = $1',
            key,
            error,
            now,
        )

    async def source(self) -> PlaylistSourceState | None:
        pool = await self._database.get_pool()
        row = await pool.fetchrow('SELECT database_name, fetched_at FROM playlist_source WHERE id')
        if row is None:
            return None
        return PlaylistSourceState(database_name=row['database_name'], fetched_at=row['fetched_at'])

    async def record_source(self, database_name: str, *, now: datetime) -> None:
        pool = await self._database.get_pool()
        await pool.execute(
            """
            INSERT INTO playlist_source (id, database_name, fetched_at) VALUES (TRUE, $1, $2)
            ON CONFLICT (id) DO UPDATE SET database_name = excluded.database_name, fetched_at = excluded.fetched_at
            """,
            database_name,
            now,
        )


def _from_row(row: asyncpg.Record) -> PlaylistRecord:
    return PlaylistRecord(
        key=row['key'],
        kind=row['kind'],
        note=row['note'],
        name=row['name'],
        enabled=row['enabled'],
        entries=tuple(
            RankedEntry(rank=int(rank), avid=str(avid), title=str(title))
            for rank, avid, title in json.loads(row['entries_json'])
        ),
        present=tuple(json.loads(row['present_json'])),
        missing=tuple(json.loads(row['missing_json'])),
        emby_playlist_id=row['emby_playlist_id'],
        last_synced_at=row['last_synced_at'],
        last_error=row['last_error'],
        created_at=row['created_at'],
        updated_at=row['updated_at'],
    )
