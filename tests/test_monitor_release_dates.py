from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from embyx_manager.monitor.release_dates import ReleaseDateFinder, ensure_release_date
from tests.test_monitor_rss import FakeLedger

NOW = datetime(2026, 9, 3, tzinfo=UTC)


@dataclass(frozen=True)
class Work:
    work_id: str
    release_date: date | None


def make_finder(
    answers: dict[str, list[Work]], *, fail: frozenset[str] = frozenset()
) -> tuple[ReleaseDateFinder, list[str], list[datetime]]:
    calls: list[str] = []
    clock = [NOW]

    async def search(query: str) -> list[Work]:
        calls.append(query)
        if query in fail:
            msg = 'challenge page'
            raise RuntimeError(msg)
        return answers.get(query, [])

    return ReleaseDateFinder(search, clock=lambda: clock[0]), calls, clock


async def test_the_date_comes_from_the_work_matching_the_avid() -> None:
    finder, calls, _ = make_finder(
        {'ABC-123': [Work('ABC-1234', date(2026, 1, 1)), Work('abc-123', date(2026, 10, 2))]}
    )

    assert await finder('ABC-123') == date(2026, 10, 2)
    assert calls == ['ABC-123']


async def test_a_miss_is_remembered_for_a_week_but_a_failure_is_not() -> None:
    finder, calls, clock = make_finder({}, fail=frozenset({'DOWN-1'}))

    assert await finder('ABC-123') is None
    assert await finder('ABC-123') is None
    assert calls == ['ABC-123']
    clock[0] = NOW + timedelta(days=8)
    assert await finder('ABC-123') is None
    assert calls == ['ABC-123', 'ABC-123']

    assert await finder('DOWN-1') is None
    assert await finder('DOWN-1') is None
    assert calls[-2:] == ['DOWN-1', 'DOWN-1']


async def test_a_work_without_a_date_yet_counts_as_a_miss() -> None:
    finder, calls, _ = make_finder({'ABC-123': [Work('ABC-123', None)]})

    assert await finder('ABC-123') is None
    assert await finder('ABC-123') is None
    assert calls == ['ABC-123']


async def test_ensure_writes_the_found_date_back_and_skips_rows_that_have_one() -> None:
    ledger = FakeLedger()
    await ledger.discover('ABC-123', source='rss:Actor', now=NOW)
    await ledger.discover('DEF-456', source='rss:Actor', now=NOW, release_date=date(2026, 5, 5))
    asked: list[str] = []

    async def lookup(avid: str) -> date | None:
        asked.append(avid)
        return date(2026, 10, 2)

    first = await ledger.get('ABC-123')
    second = await ledger.get('DEF-456')
    assert first is not None
    assert second is not None

    assert await ensure_release_date(ledger, first, lookup) == date(2026, 10, 2)
    assert await ensure_release_date(ledger, second, lookup) == date(2026, 5, 5)
    assert await ensure_release_date(ledger, first, None) is None
    assert asked == ['ABC-123']
    assert ledger.release_dates == {'ABC-123': date(2026, 10, 2), 'DEF-456': date(2026, 5, 5)}
