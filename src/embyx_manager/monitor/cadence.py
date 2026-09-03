"""When to look for magnets again: a schedule anchored on the release date.

A work is registered at its catalog weeks before it is released, and a magnet
usually appears within days of the release. Retrying on a fixed cooldown from
the moment of discovery spends most of its passes before anything can exist,
then may lag a day behind once something does. Anchoring the schedule on the
release date puts the frequent passes where they pay off.

The release date is the earliest product date the catalog knows, which is only
an approximation: a pre-release on one storefront can put a torrent out weeks
earlier. The weekly probes ahead of the window are there for exactly those.
"""

from datetime import UTC, date, datetime, time, timedelta

#: Ahead of the release window: one probe a week catches early leaks cheaply.
PREHEAT_INTERVAL = timedelta(days=7)
#: The release window, where most magnets appear.
WINDOW_BEFORE = timedelta(days=7)
WINDOW_AFTER = timedelta(days=14)
WINDOW_INTERVAL = timedelta(hours=4)
#: After the window the interval grows with the age of the release: each entry
#: is (the age the band ends at, the interval within it).
LONG_TAIL: tuple[tuple[timedelta, timedelta], ...] = (
    (timedelta(days=21), timedelta(days=1)),
    (timedelta(days=42), timedelta(days=3)),
    (timedelta(days=90), timedelta(days=7)),
)
#: An old release with no magnet is still worth a look now and then: re-uploads
#: happen, and a monthly pass costs three requests.
LONG_TAIL_FLOOR = timedelta(days=30)


def next_resolve_at(now: datetime, release_date: date | None, *, fallback: timedelta) -> datetime:
    """When to try resolving magnets again after a pass came up empty.

    Without a release date there is nothing to anchor on and the fixed
    ``fallback`` cooldown applies, as it did before release dates existed.
    The date is a calendar day in the catalog's time zone; taking it as UTC
    midnight is hours off, which the day-scale windows do not notice.
    """
    if release_date is None:
        return now + fallback
    release = datetime.combine(release_date, time.min, tzinfo=UTC)
    window_start = release - WINDOW_BEFORE
    if now < window_start:
        # Never overshoot the window: the last preheat probe lands on its first day.
        return min(now + PREHEAT_INTERVAL, window_start)
    if now < release + WINDOW_AFTER:
        return now + WINDOW_INTERVAL
    age = now - release
    for band_end, interval in LONG_TAIL:
        if age < band_end:
            return now + interval
    return now + LONG_TAIL_FLOOR
