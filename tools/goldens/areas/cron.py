"""Cron next-fire times and validation as the baseline computes them (cronsim 2.7).

`monitor/scheduler.py::_next_cron_fire` evaluates the archive scan's cron in the server's
local time (`now.astimezone()`), returns the next fire in UTC without microseconds, and
returns None for an expression cronsim refuses. `config/models.py` validates `scan_cron`
by constructing `CronSim(expr, datetime(2000, 1, 1))`, and its error message ends up in
the section's validation error. Both are recorded for every expression in the defaults and
the tests plus a corpus of what a person may type into the settings page; each next-fire
chain runs in several local time zones, across their DST changes.

The WP0 probe (`crates/embyx-monitor/src/probes.rs`) compares `croner` against these; WP9
ports the evaluator that passes them.
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from cronsim import CronSim, CronSimError

from embyx_manager.monitor.scheduler import _next_cron_fire

# The default, the tests' expressions, and what the settings page could be given.
EXPRESSIONS: list[str] = [
    '0 4 * * *',
    '30 3 * * 1',
    '*/15 * * * *',
    '*/30 * * * *',
    '0 */6 * * *',
    '0 4 * * 0,2-6',
    '0 4 * * 7',
    '0 9 * * sun',
    '0 9 * * mon-fri',
    '0 9 * * MON-FRI',
    '0 0 1 * *',
    '0 0 31 * *',
    '0 0 29 2 *',
    '59 23 31 12 *',
    '0 4 1-7 * 1',
    '0 4 * jan,jul *',
    '0 0 L * *',
    '0 0 * * 5L',
    '0 0 * * 1#2',
    '0 0 15W * *',
    '0 0 LW * *',
    '30 2 * * *',
    '15 1 * * *',
    '0 0-23/7 * * *',
    '5-55/10 * * * *',
    '0 4 * * ?',
    '0 4 ? * 1',
    '@daily',
    '@hourly',
    '0 4 * * * *',
    '',
    'every day at four',
    '0 4 * *',
    '99 4 * * *',
    '0 24 * * *',
    '0 4 32 * *',
    '0 4 * 13 *',
    '0 4 * * 8',
    '*/0 * * * *',
    '0 4 30 2 *',
    '0 4 * * mon-',
]

# Local zones the server could run in (production runs in UTC).
ZONES = ['UTC', 'Asia/Shanghai', 'America/New_York', 'Europe/Berlin']

# Starting instants (UTC), including ones next to the 2026 DST changes of the zones above.
STARTS = [
    datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
    datetime(2026, 11, 13, 18, 22, 7, 123456, tzinfo=UTC),
    datetime(2026, 3, 8, 6, 59, 0, tzinfo=UTC),
    datetime(2026, 3, 29, 0, 30, 0, tzinfo=UTC),
    datetime(2026, 10, 25, 0, 30, 0, tzinfo=UTC),
    datetime(2026, 11, 1, 4, 59, 59, tzinfo=UTC),
    datetime(2028, 2, 28, 23, 0, 0, tzinfo=UTC),
]

CHAIN = 6


def _set_zone(zone: str) -> None:
    os.environ['TZ'] = zone
    time.tzset()


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _validation() -> list[dict[str, Any]]:
    cases = []
    for expr in EXPRESSIONS:
        normalized = ' '.join(expr.split())
        try:
            CronSim(normalized, datetime(2000, 1, 1))  # noqa: DTZ001 - as config/models.py probes it
            error = None
        except CronSimError as exc:
            error = str(exc)
        cases.append({'expr': expr, 'normalized': normalized, 'error': error})
    return cases


def _next_fires() -> list[dict[str, Any]]:
    cases = []
    try:
        for zone in ZONES:
            _set_zone(zone)
            for expr in EXPRESSIONS:
                for start in STARTS:
                    fires: list[str | None] = []
                    now: datetime | None = start
                    for _ in range(CHAIN):
                        fire = _next_cron_fire(expr, now=now)
                        fires.append(_iso(fire))
                        if fire is None:
                            break
                        # The scheduler asks again once the fire time has passed.
                        now = fire + timedelta(seconds=1)
                    cases.append({'zone': zone, 'expr': expr, 'start': start.isoformat(), 'fires': fires})
    finally:
        _set_zone('UTC')
    return cases


GOLDENS = {
    'cron/validation.json': _validation,
    'cron/next_fires.json': _next_fires,
}
