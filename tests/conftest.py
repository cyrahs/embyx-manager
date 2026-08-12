"""Shared repository fixtures.

PostgreSQL-backed tests run when ``EMBYX_MANAGER_TEST_DATABASE_URL`` points at a
disposable database (its ``public`` schema is dropped between tests); they skip
otherwise. Example:

    EMBYX_MANAGER_TEST_DATABASE_URL=postgresql://postgres:test@localhost:54329/embyx_test
"""

import asyncio
import os
from pathlib import Path

import asyncpg
import pytest

from embyx_manager.db import Database
from embyx_manager.fill_actor.persistence import MemoryFillActorRepository
from embyx_manager.fill_actor.postgres_repository import PostgresFillActorRepository

TEST_DATABASE_URL = os.environ.get('EMBYX_MANAGER_TEST_DATABASE_URL')

_active_databases: list[Database] = []


def postgres_test_dsn() -> str:
    if not TEST_DATABASE_URL:
        pytest.skip('EMBYX_MANAGER_TEST_DATABASE_URL is not set')
    return TEST_DATABASE_URL


def make_database() -> Database:
    database = Database(postgres_test_dsn())
    _active_databases.append(database)
    return database


def make_postgres_repository() -> PostgresFillActorRepository:
    return PostgresFillActorRepository(make_database())


def make_repository(kind: str, tmp_path: Path) -> MemoryFillActorRepository | PostgresFillActorRepository:
    del tmp_path
    if kind == 'memory':
        return MemoryFillActorRepository()
    if kind == 'postgres':
        return make_postgres_repository()
    msg = f'unknown repository kind: {kind}'
    raise ValueError(msg)


def reset_public_schema() -> None:
    """Drop and recreate the public schema (sync helper for TestClient-loop tests)."""
    if not TEST_DATABASE_URL:
        return

    async def reset() -> None:
        connection = await asyncpg.connect(TEST_DATABASE_URL)
        try:
            await connection.execute('DROP SCHEMA public CASCADE')
            await connection.execute('CREATE SCHEMA public')
        finally:
            await connection.close()

    asyncio.run(reset())


@pytest.fixture(autouse=True)
async def _postgres_isolation():
    yield
    global _active_databases
    databases, _active_databases = _active_databases, []
    for database in databases:
        await database.aclose()
    if databases and TEST_DATABASE_URL:
        connection = await asyncpg.connect(TEST_DATABASE_URL)
        try:
            await connection.execute('DROP SCHEMA public CASCADE')
            await connection.execute('CREATE SCHEMA public')
        finally:
            await connection.close()
