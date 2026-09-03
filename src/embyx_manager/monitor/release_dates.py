"""Filling in the release date an acquisition was recorded without.

Feed items carry no release date, so an AVID sighted in a feed enters the
ledger without one and the resolve schedule falls back to its fixed cooldown.
The date is looked up the first time the row has to be parked without a
magnet: only rows that actually wait pay for the lookup, and rows recorded
before dates existed pick theirs up on their next retry.
"""

import logging
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Protocol

from embyx_manager.monitor.acquisitions import AcquisitionRecord, AcquisitionRepository

LOGGER = logging.getLogger('embyx-manager.release-dates')

#: ``lookup(avid)`` answers the release date the catalog knows, or None.
ReleaseDateLookup = Callable[[str], Awaitable[date | None]]


class _CatalogWork(Protocol):
    work_id: str
    release_date: date | None


class ReleaseDateFinder:
    """Release dates from a catalog search, remembering what it did not know.

    A miss is cached for ``miss_ttl`` so a work the catalog has not listed is
    not asked about on every retry; a failed request is not a miss and is not
    cached. Errors are logged and answered with None: the schedule then falls
    back, and magnet resolution is never held up by the catalog.
    """

    def __init__(
        self,
        search: Callable[[str], Awaitable[Sequence[_CatalogWork]]],
        *,
        miss_ttl: timedelta = timedelta(days=7),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._search = search
        self._miss_ttl = miss_ttl
        self._clock = clock or (lambda: datetime.now(UTC))
        self._misses: dict[str, datetime] = {}

    async def __call__(self, avid: str) -> date | None:
        now = self._clock()
        missed_at = self._misses.get(avid)
        if missed_at is not None and now - missed_at < self._miss_ttl:
            return None
        try:
            works = await self._search(avid)
        except Exception:
            LOGGER.warning('Release date lookup failed for %s', avid, exc_info=True)
            return None
        wanted = avid.casefold()
        for work in works:
            if work.work_id.casefold() == wanted and work.release_date is not None:
                self._misses.pop(avid, None)
                return work.release_date
        self._misses[avid] = now
        return None


async def ensure_release_date(
    ledger: AcquisitionRepository,
    record: AcquisitionRecord,
    lookup: ReleaseDateLookup | None,
) -> date | None:
    """The record's release date, looked up and written back when it has none."""
    if record.release_date is not None or lookup is None:
        return record.release_date
    found = await lookup(record.avid)
    if found is not None:
        await ledger.set_release_date(record.avid, found)
    return found
