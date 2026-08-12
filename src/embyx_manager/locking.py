import asyncio
import fcntl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO

if TYPE_CHECKING:
    import asyncpg

# Shared advisory-lock namespace; see fill_actor.postgres_repository.
_ADVISORY_NAMESPACE = 0x454D4258  # 'EMBX'
MUTATION_LOCK_KEY = 2


class AsyncFileLock:
    """Cancellation-responsive advisory lock shared by all application processes."""

    def __init__(self, path: Path, *, retry_interval: float = 0.05) -> None:
        if retry_interval <= 0:
            msg = 'lock retry interval must be positive'
            raise ValueError(msg)
        self._path = path
        self._retry_interval = retry_interval

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[None]:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        handle = self._path.open('a+b')
        acquired = False
        try:
            while True:
                try:
                    self._try_lock(handle)
                except BlockingIOError:
                    await asyncio.sleep(self._retry_interval)
                else:
                    acquired = True
                    break
            yield
        finally:
            try:
                if acquired:
                    self._unlock(handle)
            finally:
                handle.close()

    @staticmethod
    def _try_lock(handle: BinaryIO) -> None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    @staticmethod
    def _unlock(handle: BinaryIO) -> None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class PostgresAdvisoryLock:
    """Cross-process mutation lock backed by a PostgreSQL session advisory lock.

    Holds one pooled connection for the duration of the critical section; the
    server queues waiters, so acquisition is fair and cancellation-responsive
    (cancelling the awaiting task cancels the server-side wait).
    """

    def __init__(self, pool_provider, key: int = MUTATION_LOCK_KEY) -> None:  # noqa: ANN001
        """``pool_provider`` is an async callable returning an asyncpg pool."""
        self._pool_provider = pool_provider
        self._key = key

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[None]:
        pool: asyncpg.Pool = await self._pool_provider()
        connection = await pool.acquire()
        acquired = False
        try:
            await connection.execute('SELECT pg_advisory_lock($1, $2)', _ADVISORY_NAMESPACE, self._key)
            acquired = True
            yield
        finally:
            try:
                if acquired:
                    await connection.execute(
                        'SELECT pg_advisory_unlock($1, $2)',
                        _ADVISORY_NAMESPACE,
                        self._key,
                    )
            finally:
                await pool.release(connection)
