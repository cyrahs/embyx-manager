"""Shared repository fixtures.

PostgreSQL-backed tests run when ``EMBYX_MANAGER_TEST_DATABASE_URL`` points at a
disposable database (its ``public`` schema is dropped between tests); they skip
otherwise. Example:

    EMBYX_MANAGER_TEST_DATABASE_URL=postgresql://postgres:test@localhost:54329/embyx_test
"""

import os
from pathlib import Path

import asyncpg
import pytest

from embyx_manager.fill_actor.persistence import MemoryFillActorRepository
from embyx_manager.fill_actor.postgres_repository import PostgresFillActorRepository

TEST_DATABASE_URL = os.environ.get('EMBYX_MANAGER_TEST_DATABASE_URL')

_active_postgres_repositories: list[PostgresFillActorRepository] = []


def postgres_test_dsn() -> str:
    if not TEST_DATABASE_URL:
        pytest.skip('EMBYX_MANAGER_TEST_DATABASE_URL is not set')
    return TEST_DATABASE_URL


def make_postgres_repository() -> PostgresFillActorRepository:
    repository = PostgresFillActorRepository(postgres_test_dsn())
    _active_postgres_repositories.append(repository)
    return repository


def make_repository(kind: str, tmp_path: Path) -> MemoryFillActorRepository | PostgresFillActorRepository:
    del tmp_path
    if kind == 'memory':
        return MemoryFillActorRepository()
    if kind == 'postgres':
        return make_postgres_repository()
    msg = f'unknown repository kind: {kind}'
    raise ValueError(msg)


@pytest.fixture(autouse=True)
async def _postgres_isolation():
    yield
    global _active_postgres_repositories
    repositories, _active_postgres_repositories = _active_postgres_repositories, []
    for repository in repositories:
        await repository.aclose()
    if repositories and TEST_DATABASE_URL:
        connection = await asyncpg.connect(TEST_DATABASE_URL)
        try:
            await connection.execute('DROP SCHEMA public CASCADE')
            await connection.execute('CREATE SCHEMA public')
        finally:
            await connection.close()
