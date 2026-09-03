"""Move JavBus star subscriptions over to AVBase talents, a batch at a time.

A one-off, run by hand against a deployed embyx-manager:

    uv run python scripts/migrate_javbus_subscriptions.py resolve --api URL [--opml FILE]
    uv run python scripts/migrate_javbus_subscriptions.py apply   --api URL --batch 5
    uv run python scripts/migrate_javbus_subscriptions.py verify  --api URL [--trigger]
    uv run python scripts/migrate_javbus_subscriptions.py status

``resolve`` reads the subscriptions (the manager's list, or a FreshRSS OPML
export before the import has run), finds the AVBase talent behind every
``/javbus/star/<id>`` feed, and writes ``mapping.json`` for review. Two
bridges: the actor's name (the feed title, then the JavBus star page), which
AVBase resolves through every alias; and, failing that, the works on the star
page's first page — the one talent credited on all of them is the actor.

``apply`` takes the next batch of resolved rows, creates an ``avbase_talent``
subscription with a pending seed (the JavBus feed already covered the backlog)
and disables — never deletes — the JavBus row it replaces, so a batch can be
undone by re-enabling it. ``verify`` checks every applied row: the talent feed
is reachable and names the expected talent, the new row exists, the old one is
off, and once the poller has been past, the seed settled without an error.

Writes need ``EMBYX_MANAGER_API_TOKEN`` in the environment.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

import httpx
from defusedxml.ElementTree import fromstring

from embyx_manager.clients.avbase import AvbaseClient, AvbaseError
from embyx_manager.clients.javbus import JavBusActorPage, JavBusClient
from embyx_manager.monitor.feeds import parse_feed
from embyx_manager.monitor.subscriptions import AVBASE_FEED_URL

DEFAULT_MAPPING = Path('mapping.json')
TITLE_PREFIXES = ('JavBus - ', 'JavBus-', 'JavBus ')
#: "name (alias)" in either ASCII or full-width parentheses.
_PARENTHESISED_RE = re.compile(r'^(.+?)\s*[(\uff08]\s*(.+?)\s*[)\uff09]$')
#: Works looked at for the AVID bridge; the star's own page lists newest first.
BRIDGE_WORKS = 6
#: Works that must agree before the bridge counts as a majority.
BRIDGE_MAJORITY = 3
POLL_WAIT_SECONDS = 600
HTTP_CREATED = 201
HTTP_CONFLICT = 409
HTTP_OK = 200


@dataclass
class Row:
    star_id: str
    url: str
    title: str | None
    category: str | None
    subscription_id: int | None = None
    javbus_name: str | None = None
    talent_id: int | None = None
    talent_name: str | None = None
    aliases: list[str] = field(default_factory=list)
    total_works: int | None = None
    method: str = 'unresolved'
    evidence: str = ''
    applied: dict[str, Any] | None = None
    verified: dict[str, Any] | None = None

    @property
    def resolved(self) -> bool:
        return self.talent_id is not None


# -- sources -------------------------------------------------------------------


def star_id_of(url: str) -> str | None:
    parts = [unquote(part) for part in urlsplit(url).path.split('/') if part]
    for index in range(len(parts) - 2):
        if parts[index].casefold() == 'javbus' and parts[index + 1].casefold() == 'star':
            return parts[index + 2] if index + 3 == len(parts) else None
    return None


def rows_from_manager(api: str) -> list[Row]:
    rows = []
    for item in subscriptions(api):
        if item['kind'] != 'rss' or not item['enabled']:
            continue
        star_id = star_id_of(item['url'])
        if star_id:
            rows.append(
                Row(
                    star_id=star_id,
                    url=item['url'],
                    title=item.get('name'),
                    category=item['category'],
                    subscription_id=item['id'],
                )
            )
    return rows


def rows_from_opml(path: Path) -> list[Row]:
    root = fromstring(path.read_bytes(), forbid_dtd=True)
    rows = []

    def walk(node: Any, category: str | None) -> None:
        for outline in node.findall('outline'):
            url = outline.get('xmlUrl')
            if url:
                star_id = star_id_of(url)
                if star_id:
                    title = outline.get('text') or outline.get('title')
                    rows.append(Row(star_id=star_id, url=url, title=title, category=category))
            else:
                walk(outline, outline.get('text') or outline.get('title') or category)

    body = root.find('body')
    walk(body if body is not None else root, None)
    return rows


# -- resolution ----------------------------------------------------------------


def name_candidates(title: str | None) -> list[str]:
    if not title:
        return []
    name = title.strip()
    for prefix in TITLE_PREFIXES:
        if name.startswith(prefix):
            name = name[len(prefix) :].strip()
    candidates = [name]
    match = _PARENTHESISED_RE.match(name)
    if match:
        candidates.extend([match.group(1).strip(), match.group(2).strip()])
    return [candidate for candidate in dict.fromkeys(candidates) if candidate]


def _record_talent(row: Row, talent: Any, *, method: str, evidence: str) -> None:
    row.talent_id, row.talent_name = talent.talent_id, talent.name
    row.aliases, row.total_works = list(talent.aliases), talent.total_works
    row.method, row.evidence = method, evidence


def _unresolved(row: Row, evidence: str) -> None:
    row.talent_id = row.talent_name = None
    row.method, row.evidence = 'unresolved', evidence


async def _resolve_by_name(row: Row, candidates: list[str], avbase: AvbaseClient) -> list[str]:
    """Try each candidate name at AVBase; the names tried are returned for the record."""
    tried: list[str] = []
    for name in dict.fromkeys(candidates):
        tried.append(name)
        talent = await avbase.talent(name)
        if talent is not None:
            _record_talent(row, talent, method='name', evidence=f'AVBase lists {name!r} (talent {talent.talent_id})')
            break
    return tried


async def _resolve_by_works(row: Row, star: JavBusActorPage, avbase: AvbaseClient, tried: list[str]) -> None:
    """The talent credited on every work of the star page's first page is the actor."""
    looked: list[str] = []
    credited: Counter[int] = Counter()
    names: dict[int, str] = {}
    for avid in star.video_ids[:BRIDGE_WORKS]:
        work = await avbase.work(avid)
        if work is None:
            continue
        looked.append(avid)
        for talent_id in {member.talent_id for member in work.cast if member.talent_id is not None}:
            credited[talent_id] += 1
        for member in work.cast:
            if member.talent_id is not None:
                names.setdefault(member.talent_id, member.name)
    if not looked:
        _unresolved(row, f'no AVBase talent for {tried}; none of {list(star.video_ids[:BRIDGE_WORKS])} is on AVBase')
        return
    best, count = credited.most_common(1)[0]
    unanimous = count == len(looked)
    majority = count >= BRIDGE_MAJORITY and count * 2 > len(looked)
    if not (unanimous or majority):
        _unresolved(row, f'no AVBase talent for {tried}; works {looked} share no talent ({dict(credited)})')
        return
    talent = await avbase.talent(names[best])
    if talent is None or talent.talent_id != best:
        _unresolved(row, f'works {looked} point at talent {best} ({names[best]!r}) but its page did not resolve')
        return
    evidence = f'{count}/{len(looked)} works {looked} credit talent {best} ({names[best]!r}); names tried {tried}'
    _record_talent(row, talent, method='avid', evidence=evidence)


async def resolve_row(row: Row, avbase: AvbaseClient, javbus: JavBusClient) -> None:
    star = None
    try:
        star = await javbus.get_actor(row.star_id)
    except Exception as exc:  # noqa: BLE001 - the bridge is optional
        row.evidence = f'javbus star page unavailable: {type(exc).__name__}: {exc}'
    if star is not None:
        row.javbus_name = star.name

    candidates = name_candidates(row.title)
    if star is not None and star.name:
        candidates.append(star.name)
    tried = await _resolve_by_name(row, candidates, avbase)
    if row.resolved:
        return
    if star is None or not star.video_ids:
        _unresolved(row, f'no AVBase talent for {tried}; no star page works to bridge with')
        return
    await _resolve_by_works(row, star, avbase, tried)


async def resolve(rows: list[Row], *, concurrency: int) -> None:
    avbase = AvbaseClient(max_concurrency=concurrency)
    javbus = JavBusClient()
    semaphore = asyncio.Semaphore(concurrency)

    async def one(row: Row) -> None:
        async with semaphore:
            try:
                await resolve_row(row, avbase, javbus)
            except AvbaseError as exc:
                row.method = 'error'
                row.evidence = f'{type(exc).__name__}: {exc}'
            label = row.talent_name or row.title or ''
            print(f'  {row.star_id:>8}  {row.method:<10} {row.talent_id or "-":>6}  {label}  - {row.evidence}')

    try:
        await asyncio.gather(*(one(row) for row in rows))
    finally:
        await avbase.aclose()
        await javbus.aclose()


# -- apply / verify --------------------------------------------------------------


def auth_headers() -> dict[str, str]:
    token = os.environ.get('EMBYX_MANAGER_API_TOKEN')
    if not token:
        sys.exit('EMBYX_MANAGER_API_TOKEN is not set; writes need it')
    return {'Authorization': f'Bearer {token}'}


def subscriptions(api: str) -> list[dict[str, Any]]:
    return httpx.get(f'{api}/api/monitor/subscriptions', timeout=30).raise_for_status().json()['items']


def next_batch(rows: list[Row], *, batch: int, only: set[str] | None) -> list[Row]:
    todo = [
        row
        for row in rows
        if row.resolved and not row.applied and row.category and (only is None or row.star_id in only)
    ]
    return todo[:batch]


def apply_row(client: httpx.Client, api: str, row: Row) -> None:
    payload = {
        'kind': 'avbase_talent',
        'category': row.category,
        'talent_id': row.talent_id,
        'name': row.talent_name,
        'aliases': row.aliases,
        'seed': True,
    }
    response = client.post(f'{api}/api/monitor/subscriptions', json=payload)
    if response.status_code == HTTP_CREATED:
        new_id = response.json()['id']
    elif response.status_code == HTTP_CONFLICT:
        existing = [item for item in subscriptions(api) if item.get('talent_id') == row.talent_id]
        if not existing:
            msg = f'talent {row.talent_id} reported as existing but not listed'
            raise RuntimeError(msg)
        new_id = existing[0]['id']
    else:
        msg = f'creating the talent subscription failed: HTTP {response.status_code} {response.text}'
        raise RuntimeError(msg)
    old_disabled = False
    if row.subscription_id is not None:
        client.patch(
            f'{api}/api/monitor/subscriptions/{row.subscription_id}', json={'enabled': False}
        ).raise_for_status()
        old_disabled = True
    row.applied = {'new_subscription_id': new_id, 'old_disabled': old_disabled, 'at': datetime.now(UTC).isoformat()}


def feed_check(talent_id: int, expected_names: list[str]) -> dict[str, Any]:
    url = AVBASE_FEED_URL.format(talent_id=talent_id)
    response = httpx.get(url, timeout=60, follow_redirects=True)
    if response.status_code != HTTP_OK:
        return {'ok': False, 'reason': f'feed HTTP {response.status_code}'}
    feed = parse_feed(response.content)
    root = fromstring(response.content, forbid_dtd=True)
    link = root.findtext('channel/link') or ''
    feed_name = unquote(link.rstrip('/').rsplit('/', 1)[-1]) if link else ''
    names_ok = feed_name in expected_names
    return {
        'ok': names_ok,
        'reason': None if names_ok else f'feed is for {feed_name!r}, expected one of {expected_names}',
        'feed_name': feed_name,
        'items': len(feed.items),
    }


FeedCheck = Callable[[int, list[str]], dict[str, Any]]


def verify_row(row: Row, listing: dict[int, dict[str, Any]], *, feed_check: FeedCheck = feed_check) -> dict[str, Any]:
    if row.applied is None:
        msg = 'verify only runs on applied rows'
        raise ValueError(msg)
    result: dict[str, Any] = {'at': datetime.now(UTC).isoformat()}
    new = listing.get(row.applied['new_subscription_id'])
    old = listing.get(row.subscription_id) if row.subscription_id is not None else None
    problems = []
    if new is None or new['kind'] != 'avbase_talent' or new['talent_id'] != row.talent_id:
        problems.append('new subscription missing')
    elif not new['enabled']:
        problems.append('new subscription disabled')
    if row.subscription_id is not None and old is not None and old['enabled']:
        problems.append('old javbus subscription still enabled')
    feed = feed_check(row.talent_id or 0, [row.talent_name or '', *row.aliases])
    result['feed'] = feed
    if not feed['ok']:
        problems.append(feed['reason'])
    polled = False
    if new is not None and new['last_polled_at'] and new['last_polled_at'] > row.applied['at']:
        polled = True
        if new['last_error']:
            problems.append(f'poll error: {new["last_error"]}')
        if new['seed_pending']:
            problems.append('seed still pending after a poll')
        if feed.get('items') is not None and new['cursor_size'] != feed['items']:
            problems.append(f'cursor holds {new["cursor_size"]} items, feed has {feed["items"]}')
    result['polled'] = polled
    result['problems'] = problems
    result['ok'] = not problems
    return result


def trigger_and_wait(client: httpx.Client, api: str) -> None:
    response = client.post(f'{api}/api/monitor/rss/trigger')
    if response.status_code not in (202, HTTP_CONFLICT):
        response.raise_for_status()
    print('rss run triggered; waiting for it to finish', end='', flush=True)
    deadline = time.monotonic() + POLL_WAIT_SECONDS
    while time.monotonic() < deadline:
        time.sleep(10)
        status = httpx.get(f'{api}/api/monitor/status', timeout=30).raise_for_status().json()
        rss = next((item for item in status if item['pipeline'] == 'rss'), None)
        print('.', end='', flush=True)
        if rss is not None and not rss.get('running_run_id'):
            print(' done')
            return
    print(' still running after the wait; verify again later')


# -- mapping file ----------------------------------------------------------------


def load_mapping(path: Path) -> list[Row]:
    return [Row(**entry) for entry in json.loads(path.read_text())]


def save_mapping(path: Path, rows: list[Row]) -> None:
    path.write_text(json.dumps([asdict(row) for row in rows], ensure_ascii=False, indent=2) + '\n')


def print_status(rows: list[Row]) -> None:
    counts = Counter(row.method for row in rows)
    applied = sum(1 for row in rows if row.applied)
    verified = sum(1 for row in rows if row.verified and row.verified.get('ok'))
    print(f'{len(rows)} javbus subscriptions: {dict(counts)}; applied {applied}; verified ok {verified}')
    for row in rows:
        state = 'verified' if row.verified and row.verified.get('ok') else 'applied' if row.applied else row.method
        talent = f'{row.talent_id or "-":>6}  {row.talent_name or "-":<12}'
        print(f'  {row.star_id:>8}  {state:<10} {talent} {row.category or "-":<8} {row.title or ""}')
        if row.verified and not row.verified.get('ok'):
            print(f'             problems: {row.verified["problems"]}')


# -- commands ----------------------------------------------------------------------


def cmd_resolve(args: argparse.Namespace) -> None:
    rows = rows_from_opml(args.opml) if args.opml else rows_from_manager(args.api)
    if args.only:
        wanted = set(args.only.split(','))
        rows = [row for row in rows if row.star_id in wanted or (row.title or '') in wanted]
    if args.limit:
        rows = rows[: args.limit]
    print(f'resolving {len(rows)} javbus star subscriptions')
    asyncio.run(resolve(rows, concurrency=args.concurrency))
    if args.mapping.exists() and not args.overwrite:
        previous = {row.star_id: row for row in load_mapping(args.mapping)}
        for row in rows:
            old = previous.get(row.star_id)
            if old is not None and old.applied:
                row.applied, row.verified = old.applied, old.verified
    save_mapping(args.mapping, rows)
    print_status(rows)


def cmd_apply(args: argparse.Namespace) -> None:
    rows = load_mapping(args.mapping)
    todo = next_batch(rows, batch=args.batch, only=set(args.ids.split(',')) if args.ids else None)
    if not todo:
        print('nothing to apply')
        return
    print(f'applying {len(todo)} rows:')
    for row in todo:
        print(f'  {row.star_id:>8}  -> talent {row.talent_id} {row.talent_name} ({row.category})')
    if args.dry_run:
        return
    with httpx.Client(headers=auth_headers(), timeout=60) as client:
        for row in todo:
            apply_row(client, args.api, row)
            save_mapping(args.mapping, rows)
            new_id = row.applied['new_subscription_id'] if row.applied else '?'
            print(f'  {row.star_id:>8}  applied: new subscription {new_id}')
    print_status(rows)


def cmd_verify(args: argparse.Namespace) -> None:
    rows = load_mapping(args.mapping)
    if args.trigger:
        with httpx.Client(headers=auth_headers(), timeout=60) as client:
            trigger_and_wait(client, args.api)
    listing = {item['id']: item for item in subscriptions(args.api)}
    for row in (row for row in rows if row.applied):
        row.verified = verify_row(row, listing)
        flag = 'ok' if row.verified['ok'] else 'FAIL'
        polled = 'polled' if row.verified['polled'] else 'not polled yet'
        print(f'  {row.star_id:>8}  {flag:<4} {polled:<15} {row.talent_name}  {row.verified["problems"] or ""}')
    save_mapping(args.mapping, rows)
    print_status(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--mapping', type=Path, default=DEFAULT_MAPPING)
    sub = parser.add_subparsers(dest='command', required=True)
    api_default = os.environ.get('EMBYX_MANAGER_URL', 'http://127.0.0.1:8000')

    p_resolve = sub.add_parser('resolve')
    p_resolve.add_argument('--api', default=api_default)
    p_resolve.add_argument('--opml', type=Path)
    p_resolve.add_argument('--only', help='comma-separated star ids or titles')
    p_resolve.add_argument('--limit', type=int)
    p_resolve.add_argument('--concurrency', type=int, default=3)
    p_resolve.add_argument('--overwrite', action='store_true', help='drop applied/verified state from a previous run')
    p_resolve.set_defaults(func=cmd_resolve)

    p_apply = sub.add_parser('apply')
    p_apply.add_argument('--api', default=api_default)
    p_apply.add_argument('--batch', type=int, default=5)
    p_apply.add_argument('--ids', help='comma-separated star ids to apply instead of the next batch')
    p_apply.add_argument('--dry-run', action='store_true')
    p_apply.set_defaults(func=cmd_apply)

    p_verify = sub.add_parser('verify')
    p_verify.add_argument('--api', default=api_default)
    p_verify.add_argument('--trigger', action='store_true', help='run the rss pipeline first and wait for it')
    p_verify.set_defaults(func=cmd_verify)

    p_status = sub.add_parser('status')
    p_status.set_defaults(func=lambda args: print_status(load_mapping(args.mapping)))

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
