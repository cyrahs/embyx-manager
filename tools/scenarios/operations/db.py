"""The schema and its migrations (`db.py`), cross-version contract 1.

- `db.catalog` runs the spec's catalog query (`args.catalog`, one row, one JSON text) on the
  database the runner migrated (`database.schema = "app"`), so both builds' schemas are
  compared column by column, constraint by constraint and index by index.
- `db.migrate_from` builds the schema of an older version with the baseline's own
  statements, seeds rows shaped the way that version left them (`args.seed`), then opens the
  database the way the application does, which applies the remaining migrations; the
  transcript's rows show what the data migrations made of the seed.
- `db.open` opens the database as the application does and reports the versions, or the
  refusal of a schema newer than this build.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from embyx_manager.db import _MIGRATIONS, Database

if TYPE_CHECKING:
    from runner import Context

SCHEMA_MIGRATIONS = """
CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL
)
"""


async def _versions(ctx: Context) -> list[int]:
    connection = await ctx.connection()
    return [row['version'] for row in await connection.fetch('SELECT version FROM schema_migrations ORDER BY version')]


async def _open(ctx: Context) -> None:
    database = Database(ctx.dsn)
    try:
        await database.get_pool()
    finally:
        await database.aclose()


async def catalog(ctx: Context) -> Any:
    connection = await ctx.connection()
    return json.loads(await connection.fetchval(ctx.args['catalog']))


async def migrate_from(ctx: Context) -> Any:
    version = int(ctx.args['from_version'])
    connection = await ctx.connection()
    async with connection.transaction():
        await connection.execute(SCHEMA_MIGRATIONS)
        for applied in range(1, version + 1):
            for statement in _MIGRATIONS[applied]:
                await connection.execute(statement)
            await connection.execute(
                'INSERT INTO schema_migrations (version, applied_at) VALUES ($1, $2)', applied, ctx.now
            )
    for statement in ctx.args.get('seed', []):
        await connection.execute(statement)
    await _open(ctx)
    return {'versions': await _versions(ctx)}


async def open_database(ctx: Context) -> Any:
    await _open(ctx)
    return {'versions': await _versions(ctx)}


OPERATIONS = {'db.catalog': catalog, 'db.migrate_from': migrate_from, 'db.open': open_database}
