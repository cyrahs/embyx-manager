from datetime import UTC, date, datetime, timedelta

from embyx_manager.monitor.cadence import (
    LONG_TAIL_FLOOR,
    PREHEAT_INTERVAL,
    WINDOW_INTERVAL,
    next_resolve_at,
)

RELEASE = date(2026, 10, 2)
RELEASE_AT = datetime(2026, 10, 2, tzinfo=UTC)
FALLBACK = timedelta(hours=24)


def at(**delta: int) -> datetime:
    return RELEASE_AT + timedelta(**delta)


def test_no_release_date_means_the_fixed_cooldown() -> None:
    now = datetime(2026, 9, 1, tzinfo=UTC)

    assert next_resolve_at(now, None, fallback=FALLBACK) == now + FALLBACK


def test_weeks_ahead_of_the_release_probes_weekly() -> None:
    now = at(days=-40)

    assert next_resolve_at(now, RELEASE, fallback=FALLBACK) == now + PREHEAT_INTERVAL


def test_the_last_preheat_probe_lands_on_the_first_day_of_the_window() -> None:
    assert next_resolve_at(at(days=-10), RELEASE, fallback=FALLBACK) == at(days=-7)


def test_the_release_window_probes_every_few_hours() -> None:
    for now in (at(days=-7), at(days=0, hours=3), at(days=13, hours=23)):
        assert next_resolve_at(now, RELEASE, fallback=FALLBACK) == now + WINDOW_INTERVAL


def test_the_long_tail_backs_off_with_the_age_of_the_release() -> None:
    assert next_resolve_at(at(days=14), RELEASE, fallback=FALLBACK) == at(days=15)
    assert next_resolve_at(at(days=30), RELEASE, fallback=FALLBACK) == at(days=33)
    assert next_resolve_at(at(days=60), RELEASE, fallback=FALLBACK) == at(days=67)


def test_an_old_release_is_still_probed_monthly() -> None:
    now = at(days=400)

    assert next_resolve_at(now, RELEASE, fallback=FALLBACK) == now + LONG_TAIL_FLOOR
