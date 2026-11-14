"""Level 3: one capped, anonymous, read-only live run of the site clients (docs/rewrite/PLAN.md).

The same fixed list of reads goes through the Python baseline's clients (the control) and the
Rust clients (`crates/embyx-monitor/examples/level3.rs`), and the parsed results are compared:

    JavBus   one star search, that star's page, one video's magnets
    sukebei  one search
    AVBase   the build id, one talent, one work
    jinjier  the query page and the database archive (parsed into ranked lists)
    RSSHub   one public feed (parsed)

Rules (WP16 brief, item 7): one run, no credentials, GETs only, at least `--gap` seconds (2.5 by
default) between two calls of a side, and no retry loop: the clients' own `tenacity` retries are
cut to one attempt (Rust: the retry sleeper ends the call instead of waiting). A call may still
send the requests the client itself sends back to back (JavBus's magnets: the page, then its
AJAX table; AVBase: the build id once, a bare work id's search, then the work).

    uv run python tools/live/level3.py --side python --out py.json
    cargo run -q -p embyx-monitor --example level3 -- --out rust.json
    uv run python tools/live/level3.py --compare py.json rust.json

The output holds parsed values only (names, ids, magnets, list sizes), no page bodies. Run it only
from where live reads are allowed; it never runs in CI.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import ssl
import sys
import time
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

# The fixed reads. Both sides read exactly these (the star page uses each side's own search
# result, so a parser difference there shows up twice rather than being hidden).
STAR_NAME = '三上悠亜'
VIDEO_ID = 'SSIS-001'
SUKEBEI_QUERY = 'SSIS-001'
AVBASE_TALENT = '三上悠亜'
AVBASE_WORK = 'SSIS-001'
RSSHUB_FEED = 'https://rsshub.app/github/issue/DIYgod/RSSHub'


def _plain(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _plain(asdict(value))
    if isinstance(value, dict):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if isinstance(value, bytes):
        return {'bytes': len(value)}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


class Pacer:
    def __init__(self, gap: float) -> None:
        self.gap = gap
        self.last = 0.0

    async def wait(self) -> None:
        delay = self.last + self.gap - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
        self.last = time.monotonic()


def _one_attempt() -> None:
    """Cut every `tenacity` retry on the clients to one attempt (no retry loop on a refusal)."""
    from tenacity import stop_after_attempt  # noqa: PLC0415

    from embyx_manager.clients import javbus, sukebei  # noqa: PLC0415

    for owner in (javbus.JavBusClient, sukebei.SukebeiClient):
        for name in dir(owner):
            retrying = getattr(getattr(owner, name), 'retry', None)
            if retrying is not None and hasattr(retrying, 'stop'):
                retrying.stop = stop_after_attempt(1)


async def _call(results: list[dict[str, Any]], pacer: Pacer, step: str, call: Any) -> Any:
    await pacer.wait()
    started = time.monotonic()
    try:
        value = await call()
    except Exception as exc:  # noqa: BLE001 - a refusal is a result here
        results.append({'step': step, 'error': {'class': type(exc).__name__, 'message': str(exc)[:300]}})
        return None
    results.append({'step': step, 'seconds': round(time.monotonic() - started, 2), 'value': _summary(step, value)})
    return value


def _summary(step: str, value: Any) -> Any:
    """What is compared: the parsed result, with bulky lists reduced to their size and head."""
    plain = _plain(value)
    if step == 'jinjier.database':
        return {'bytes': len(value)}
    if step == 'jinjier.lists':
        return [
            {
                'key': item['key'],
                'kind': item['kind'],
                'note': item['note'],
                'entries': len(item['entries']),
                'first': item['entries'][:3],
            }
            for item in plain
        ]
    if step == 'rsshub.feed':
        return {'bytes': len(value)}
    if step == 'rsshub.parsed':
        return {
            'title': plain['title'],
            'items': len(plain['items']),
            'keys': [item['key'] for item in plain['items']][:10],
            'titles': [item['title'] for item in plain['items']][:10],
        }
    return plain


async def run_python(gap: float) -> list[dict[str, Any]]:
    from curl_cffi.requests import AsyncSession  # noqa: PLC0415

    from embyx_manager.clients.avbase import AvbaseClient  # noqa: PLC0415
    from embyx_manager.clients.feeds import HttpFeedFetcher  # noqa: PLC0415
    from embyx_manager.clients.javbus import JavBusClient  # noqa: PLC0415
    from embyx_manager.clients.jinjier import JinjierClient, parse_lists  # noqa: PLC0415
    from embyx_manager.clients.sukebei import SukebeiClient  # noqa: PLC0415
    from embyx_manager.core.avid import AvidParser  # noqa: PLC0415
    from embyx_manager.monitor.feeds import parse_feed  # noqa: PLC0415

    _one_attempt()
    pacer = Pacer(gap)
    results: list[dict[str, Any]] = []
    javbus = JavBusClient()
    sukebei = SukebeiClient()
    # The session `AvbaseClient` builds, plus the CA bundle of a TLS-inspecting egress proxy when
    # the environment names one (curl_cffi does not read SSL_CERT_FILE itself).
    ca = os.environ.get('SSL_CERT_FILE')
    session = AsyncSession(
        impersonate='chrome',
        timeout=30.0,
        headers={'Referer': 'https://www.avbase.net/', 'Accept-Language': 'ja,en;q=0.8'},
        verify=ca or True,
    )
    avbase = AvbaseClient(session=session)
    jinjier = JinjierClient()
    feeds = HttpFeedFetcher()
    try:
        stars = await _call(results, pacer, 'javbus.search_stars', lambda: javbus.search_stars(STAR_NAME))
        if stars:
            await _call(results, pacer, 'javbus.get_actor', lambda: javbus.get_actor(stars[0].actor_id))
        await _call(results, pacer, 'javbus.get_magnets', lambda: javbus.get_magnets(VIDEO_ID))
        await _call(results, pacer, 'sukebei.search', lambda: sukebei.search(SUKEBEI_QUERY))
        await _call(results, pacer, 'avbase.build_id', avbase.build_id)
        await _call(results, pacer, 'avbase.talent', lambda: avbase.talent(AVBASE_TALENT))
        await _call(results, pacer, 'avbase.work', lambda: avbase.work(AVBASE_WORK))
        name = await _call(results, pacer, 'jinjier.page', jinjier.discover_database_name)
        if name:
            database = await _call(results, pacer, 'jinjier.database', lambda: jinjier.download_database(name))
            if database:
                parser = AvidParser()
                lists = parse_lists(database, parser.get_avid)
                results.append({'step': 'jinjier.lists', 'value': _summary('jinjier.lists', lists)})
        body = await _call(results, pacer, 'rsshub.feed', lambda: feeds.fetch(RSSHUB_FEED))
        if body:
            results.append({'step': 'rsshub.parsed', 'value': _summary('rsshub.parsed', parse_feed(body))})
    finally:
        for client in (javbus, sukebei, avbase, jinjier, feeds):
            await client.aclose()
    return results


def compare(python: Path, rust: Path) -> int:
    left = {entry['step']: entry for entry in json.loads(python.read_text(encoding='utf-8'))['results']}
    right = {entry['step']: entry for entry in json.loads(rust.read_text(encoding='utf-8'))['results']}
    status = 0
    compared = 0
    for step in sorted(set(left) | set(right), key=lambda name: list(left).index(name) if name in left else 99):
        a, b = left.get(step), right.get(step)
        if a is None or b is None:
            print(f'{step}: only on {"python" if b is None else "rust"}')
            status = 1
            continue
        if 'error' in a or 'error' in b:
            verdict = (
                'both refused: inconclusive from this network' if 'error' in a and 'error' in b else 'ONE SIDE REFUSED'
            )
            if verdict == 'ONE SIDE REFUSED':
                status = 1
            print(f'{step}: {verdict}: python={a.get("error")} rust={b.get("error")}')
            continue
        same = a['value'] == b['value']
        compared += 1
        status |= 0 if same else 1
        print(f'{step}: {"equal" if same else "DIFFERENT"}')
        if not same:
            print(f'  python: {json.dumps(a["value"], ensure_ascii=False)[:600]}')
            print(f'  rust:   {json.dumps(b["value"], ensure_ascii=False)[:600]}')
    print(f'{compared} step(s) compared')
    if not compared:
        print('nothing was compared: every read was refused (inconclusive), or the files are empty')
        return 1
    return status


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--side', choices=['python'])
    parser.add_argument('--out', type=Path)
    parser.add_argument('--gap', type=float, default=2.5)
    parser.add_argument('--compare', nargs=2, type=Path, metavar=('PYTHON', 'RUST'))
    args = parser.parse_args()
    if args.compare:
        return compare(*args.compare)
    if args.side != 'python' or args.out is None:
        parser.error('give --side python --out FILE, or --compare PYTHON RUST')
    ssl.create_default_context()  # fail early on a broken CA setup
    started = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    results = asyncio.run(run_python(args.gap))
    document = {'side': 'python', 'started': started, 'gap': args.gap, 'results': results}
    args.out.write_text(json.dumps(document, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    for entry in results:
        print(entry['step'], 'ERROR ' + entry['error']['class'] if 'error' in entry else 'ok')
    return 0


if __name__ == '__main__':
    sys.exit(main())
