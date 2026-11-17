"""The ledger's pure parts (WP5): run reporting, the resolve cadence, the state machine, feed URLs.

`monitor/run_context.json` replays call sequences on `RunContext` and records the persisted
tail, errors and counters (`pipeline_runs.log_json` / `errors_json` / `stats_json` keep these
bytes). `monitor/cadence.json` is `next_resolve_at` over a grid of moments around several
release dates. `monitor/state_machine.json` is the acquisition and attempt edge tables with
the `IllegalTransitionError` text of every non-edge (cross-version contract 3).
`monitor/subscriptions.json` covers `validate_feed_url`, `trim_cursor` and the derived feed
URL. `monitor/operation_result.json` is `str()` of CloudDrive's `FileOperationResult`, which a
refused submission writes into the run log. `monitor/resolve.json` runs the intake's
`resolve()` over stub magnet sources and records the candidates and the run log.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import httpx

from embyx_manager.clients.clouddrive import clouddrive_pb2
from embyx_manager.monitor import acquisitions
from embyx_manager.monitor.acquisitions import AcquisitionState, AttemptState, IllegalTransitionError
from embyx_manager.monitor.cadence import next_resolve_at
from embyx_manager.monitor.intake import AcquisitionIntake
from embyx_manager.monitor.reports import RunContext
from embyx_manager.monitor.subscriptions import (
    SubscriptionKind,
    SubscriptionRecord,
    trim_cursor,
    validate_feed_url,
)

# The context logs through `logging`; keep the generator's output quiet.
logging.getLogger('golden-monitor').disabled = True

# -- run reporting -------------------------------------------------------------------------


def _raise(kind: str, message: str) -> None:
    exception_type = {'OSError': OSError, 'RuntimeError': RuntimeError, 'ValueError': ValueError}[kind]
    raise exception_type(message)


def _replay(calls: list[list[Any]]) -> dict[str, Any]:
    ctx = RunContext(logger=logging.getLogger('golden-monitor'))
    for call in calls:
        match call[0]:
            case 'info':
                ctx.info(call[1])
            case 'warning':
                ctx.warning(call[1])
            case 'error':
                ctx.error(call[1])
            case 'exception':
                if call[2] is None:
                    ctx.exception(call[1])
                else:
                    try:
                        _raise(*call[2])
                    except Exception:  # noqa: BLE001 - the context reads sys.exc_info()
                        ctx.exception(call[1])
            case 'add':
                ctx.add(call[1], call[2])
            case 'set':
                ctx.set(call[1], call[2])
    import json  # noqa: PLC0415

    return {
        'calls': calls,
        'log_tail': list(ctx.log_tail),
        'errors': list(ctx.errors),
        'stats_json': json.dumps(ctx.stats),
        'log_json': json.dumps(list(ctx.log_tail)),
        'errors_json': json.dumps(list(ctx.errors)),
    }


def _run_context() -> list[dict[str, Any]]:
    cases: dict[str, list[list[Any]]] = {
        'plain lines': [
            ['info', 'Added magnet for ABC-123'],
            ['warning', 'Failed to get any magnet for ABC-123'],
            ['error', 'Failed to add magnet for ABC-123: errorMessage: "full"\n'],
        ],
        'exceptions': [
            ['exception', 'Failed to get magnets for ABC-123', ['RuntimeError', 'sukebei down']],
            ['exception', 'mapping run failed', ['OSError', "disk 'detached'"]],
            ['exception', 'outside a handler', None],
            ['exception', '演员 失败', ['ValueError', '名字\n"quoted"']],
        ],
        'percent signs are kept': [['info', '100% done'], ['warning', '%s literally']],
        'counters keep insertion order': [
            ['add', 'zeta', 1],
            ['add', 'alpha', 2],
            ['add', 'zeta', 3],
            ['set', 'retry_due', 4],
            ['add', 'candidates_recorded', 0],
            ['set', 'zeta', -1],
        ],
        'the tail keeps the last 200 lines': [['info', f'line {index}'] for index in range(205)],
        'errors stop at 50': [
            *(['error', f'failure {index}'] for index in range(30)),
            *(['exception', f'raised {index}', ['RuntimeError', str(index)]] for index in range(30)),
            ['info', 'after'],
        ],
        'unicode': [['info', '\uff21\uff22\uff23\u3000演员\U0001f600'], ['warning', '\x00\x7f\u2028']],
    }
    return [{'name': name, **_replay(calls)} for name, calls in cases.items()]


# -- cadence ----------------------------------------------------------------------------------


def _cadence() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    releases = [date(2026, 10, 2), date(2024, 2, 29), date(2030, 1, 1)]
    offsets_hours = [
        -24 * 400,
        -24 * 40,
        -24 * 14 - 1,
        -24 * 14,
        -24 * 10,
        -24 * 7 - 1,
        -24 * 7,
        -24 * 7 + 1,
        -1,
        0,
        3,
        24 * 13 + 23,
        24 * 14 - 1,
        24 * 14,
        24 * 20 + 23,
        24 * 21,
        24 * 30,
        24 * 41,
        24 * 42,
        24 * 60,
        24 * 89,
        24 * 90,
        24 * 400,
    ]
    for release in releases:
        anchor = datetime(release.year, release.month, release.day, tzinfo=UTC)
        for hours in offsets_hours:
            for extra in (timedelta(0), timedelta(minutes=17, microseconds=123456)):
                now = anchor + timedelta(hours=hours) + extra
                cases.append(_cadence_case(now, release, 86_400))
    cases.extend(
        _cadence_case(datetime(2026, 9, 1, 12, 30, tzinfo=UTC), None, fallback)
        for fallback in (1, 3600, 86_400, 7 * 86_400)
    )
    # The edges of datetime's range: Python raises OverflowError.
    cases.append(_cadence_case(datetime(9999, 12, 31, tzinfo=UTC), None, 86_400))
    cases.append(_cadence_case(datetime(9999, 12, 20, tzinfo=UTC), date(9999, 12, 30), 86_400))
    cases.append(_cadence_case(datetime(2026, 1, 1, tzinfo=UTC), date(1, 1, 3), 86_400))
    cases.append(_cadence_case(datetime(9999, 12, 30, tzinfo=UTC), date(9999, 12, 1), 86_400))
    return cases


def _cadence_case(now: datetime, release: date | None, fallback_seconds: int) -> dict[str, Any]:
    case: dict[str, Any] = {
        'now': now.isoformat(),
        'release_date': release.isoformat() if release else None,
        'fallback_seconds': fallback_seconds,
    }
    try:
        case['next'] = next_resolve_at(now, release, fallback=timedelta(seconds=fallback_seconds)).isoformat()
    except OverflowError as error:
        case['error'] = str(error)
    return case


# -- state machine ----------------------------------------------------------------------------


def _edges(table: dict[Any, frozenset[Any]], members: list[Any]) -> list[dict[str, Any]]:
    rows = []
    for source in members:
        targets = table[source]
        rows.append(
            {
                'from': source.value,
                'to': sorted(target.value for target in targets),
                'refused': {
                    target.value: str(IllegalTransitionError(source, target))
                    for target in members
                    if target not in targets
                },
            }
        )
    return rows


def _state_machine() -> list[dict[str, Any]]:
    return [
        {
            'acquisition': _edges(acquisitions._ACQUISITION_EDGES, list(AcquisitionState)),  # noqa: SLF001
            'attempt': _edges(acquisitions._ATTEMPT_EDGES, list(AttemptState)),  # noqa: SLF001
            'sets': {
                name: sorted(state.value for state in getattr(acquisitions, name))
                for name in (
                    'ACTIVE_STATES',
                    'REPLACEABLE_STATES',
                    'RETRYABLE_STATES',
                    'DUE_STATES',
                    'IN_FLIGHT_ATTEMPT_STATES',
                    'LIVE_ATTEMPT_STATES',
                )
            },
        }
    ]


# -- subscriptions ----------------------------------------------------------------------------

FEED_URLS = [
    'https://rsshub.test/javbus/star/rwt',
    'http://rsshub:1200/x?limit=20',
    ' https://sukebei.nyaa.si/?page=rss&q=a ',
    '\thttps://feeds.test/a\n',
    'ftp://x',
    'https://user:pw@rsshub.test/x',
    'https://user@rsshub.test/x',
    'https://:pw@rsshub.test/x',
    'https://rsshub.test:0/x',
    'https://rsshub.test:00/x',
    'https://rsshub.test:65535/x',
    'https://rsshub.test:65536/x',
    'https://rsshub.test:abc/x',
    'https://rsshub.test:/x',
    'https://[::1]:8080/x',
    'https://[::1/x',
    'HTTPS://RSSHub.Test/X',
    'https:///x',
    'https://',
    '//rsshub.test/x',
    'rsshub.test/x',
    '',
    '   ',
    'https://rsshub.test/路径',
    'https://例子.测试/x',
    'https://rsshub.test/x#frag',
    'http://@rsshub.test/x',
    'https://rsshub.test:\uff11\uff12/x',
]


def _feed_url_case(url: str) -> dict[str, Any]:
    try:
        return {'url': url, 'valid': validate_feed_url(url)}
    except ValueError as error:
        return {'url': url, 'error': str(error)}


def _subscriptions() -> list[dict[str, Any]]:
    now = datetime(2026, 9, 2, tzinfo=UTC)

    def record(**fields: Any) -> SubscriptionRecord:
        base: dict[str, Any] = {
            'id': 1,
            'kind': SubscriptionKind.RSS,
            'category': 'Actor',
            'enabled': True,
            'url': None,
            'talent_id': None,
            'name': None,
            'aliases': (),
            'cursor': (),
            'seed_pending': False,
            'last_polled_at': None,
            'last_error': None,
            'created_at': now,
            'updated_at': now,
        }
        base.update(fields)
        return SubscriptionRecord(**base)

    records = [
        record(url='https://rsshub.test/a'),
        record(url='https://rsshub.test/a', name='演员甲'),
        record(url='https://rsshub.test/a', name=''),
        record(kind=SubscriptionKind.AVBASE_TALENT, talent_id=5022),
        record(kind=SubscriptionKind.AVBASE_TALENT, talent_id=5022, name='河北彩花'),
    ]
    cursors = [
        [],
        ['k1', 'k2', 'k1'],
        ['b', 'a', 'b', 'c', 'a'],
        [f'k{index}' for index in range(520)],
        [f'k{index % 7}' for index in range(30)] + [f'n{index}' for index in range(600)],
    ]
    return [
        {'feed_urls': [_feed_url_case(url) for url in FEED_URLS]},
        {'cursors': [{'keys': keys, 'trimmed': list(trim_cursor(keys))} for keys in cursors]},
        {
            'records': [
                {
                    'kind': item.kind.value,
                    'url': item.url,
                    'talent_id': item.talent_id,
                    'name': item.name,
                    'feed_url': item.feed_url,
                    'display_name': item.display_name,
                }
                for item in records
            ]
        },
    ]


# -- CloudDrive's FileOperationResult in the run log ------------------------------------------


def _operation_results() -> list[dict[str, Any]]:
    values = [
        {},
        {'success': True},
        {'success': False, 'errorMessage': 'quota exceeded'},
        {'errorMessage': '任务已存在 "x"\n\\ \'q\''},
        {'errorMessage': '\x00\x01\x1f\x7f\x80\u2028 é'},
        {'success': True, 'resultFilePaths': ['/115/a', '/115/路径']},
        {'errorMessage': 'e', 'resultFilePaths': ['', '\t']},
    ]
    return [{'fields': value, 'str': str(clouddrive_pb2.FileOperationResult(**value))} for value in values]


# -- magnet resolution --------------------------------------------------------------------------

HASH_A = 'C12FE1C06BBA254A9DC9F519B335AA7C1367A88A'
HASH_B = 'D23FE1C06BBA254A9DC9F519B335AA7C1367A88B'
BASE32 = 'YEX6DQDLXISUVHOJ6UM3GNNKPQJWPKEK'


def _magnet(info_hash: str, name: str = 'ABC-123') -> str:
    return f'magnet:?xt=urn:btih:{info_hash}&dn={name}'


def _javbus(magnet: str, size: int) -> dict[str, Any]:
    return {'magnet': magnet, 'size': '', 'size_int': size, 'tags': [], 'score': size}


RESOLVE_CASES: list[dict[str, Any]] = [
    {'name': 'nothing anywhere'},
    {'name': 'sukebei only', 'sukebei': _magnet(HASH_A)},
    {
        'name': 'order and duplicates',
        'sukebei': _magnet(HASH_A),
        'item': _magnet(HASH_A.lower(), 'other'),
        'javbus': [_javbus(_magnet(HASH_B), 5), _javbus(_magnet(BASE32), 3), _javbus(_magnet(HASH_A), 1)],
    },
    {
        'name': 'not magnets and unusable hashes',
        'sukebei': 'https://sukebei.test/view/1',
        'item': 'MAGNET:?dn=ABC-123',
        'javbus': [_javbus('', 1), _javbus('magnet:?xt=urn:btih:XYZ', 2), _javbus('Magnet:?xt=urn:btih:' + HASH_B, 3)],
    },
    {
        'name': 'five at most',
        'javbus': [_javbus(_magnet(f'{index:040X}'), 10 - index) for index in range(1, 8)],
    },
    {'name': 'javbus does not list it yet', 'sukebei': _magnet(HASH_A), 'javbus_status': 404},
    {'name': 'javbus fails', 'sukebei': _magnet(HASH_A), 'javbus_status': 503},
    {'name': 'javbus raises', 'item': _magnet(HASH_B), 'javbus_raises': ['RuntimeError', 'parser broke']},
    {'name': 'sukebei raises', 'sukebei_raises': ['RuntimeError', 'sukebei down']},
    {'name': 'an empty item magnet', 'item': '', 'sukebei': None},
]


def _resolve_case(case: dict[str, Any]) -> dict[str, Any]:
    if 'sukebei_raises' in case:
        sukebei = SimpleNamespace(get_magnet=AsyncMock(side_effect=RuntimeError(case['sukebei_raises'][1])))
    else:
        sukebei = SimpleNamespace(get_magnet=AsyncMock(return_value=case.get('sukebei')))
    if 'javbus_status' in case:
        request = httpx.Request('GET', 'https://www.javbus.com/ABC-123')
        response = httpx.Response(case['javbus_status'], request=request)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            javbus = SimpleNamespace(get_magnets=AsyncMock(side_effect=error))
    elif 'javbus_raises' in case:
        javbus = SimpleNamespace(get_magnets=AsyncMock(side_effect=RuntimeError(case['javbus_raises'][1])))
    else:
        javbus = SimpleNamespace(get_magnets=AsyncMock(return_value=case.get('javbus', [])))
    intake = AcquisitionIntake(
        ledger=None,  # type: ignore[arg-type]
        sukebei=sukebei,  # type: ignore[arg-type]
        javbus=javbus,  # type: ignore[arg-type]
        cloud=None,  # type: ignore[arg-type]
        failed_cooldown_seconds=3600,
    )
    ctx = RunContext(logger=logging.getLogger('golden-monitor'))
    result: dict[str, Any] = {**case}
    try:
        candidates = asyncio.run(intake.resolve('ABC-123', ctx=ctx, item_magnet=case.get('item')))
    except Exception as error:  # noqa: BLE001 - the case records what resolve raises
        result['raises'] = [type(error).__name__, str(error)]
    else:
        result['candidates'] = [
            {
                'magnet': candidate.magnet,
                'info_hash': candidate.info_hash,
                'source': candidate.source,
                'size_hint': candidate.size_hint,
            }
            for candidate in candidates
        ]
    result['log_tail'] = list(ctx.log_tail)
    result['errors'] = list(ctx.errors)
    return result


def _resolve() -> list[dict[str, Any]]:
    return [_resolve_case(case) for case in RESOLVE_CASES]


GOLDENS = {
    'monitor/run_context.json': _run_context,
    'monitor/cadence.json': _cadence,
    'monitor/state_machine.json': _state_machine,
    'monitor/subscriptions.json': _subscriptions,
    'monitor/operation_result.json': _operation_results,
    'monitor/resolve.json': _resolve,
}
