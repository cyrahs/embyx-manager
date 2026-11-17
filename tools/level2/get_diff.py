"""Level 2 GET differ: the same GET requests to the Python and the Rust server, on the same rows.

Each build runs in turn on its own fresh copy of one export (`mask.py load`), so neither reads
what the other wrote. Before a server starts, the copy's schedulers are switched off (every
section's `enabled`, `auto_enabled` and `home_transcode_enabled` set to false in `app_config`),
so no run starts and the rows stay as exported; every outbound request goes to a local proxy that
refuses it (as in tools/bench/memory.py), so nothing leaves the machine.

The requests: every GET route of the main listener's API and its page root, the list routes with a
few filters and pages, and the item routes for the first `--per-route` ids the copy holds
(acquisition AVIDs, run ids, plan ids and fill-actor job ids, apply-job ids, playlist keys, replace
plans). A query that finds no table or column is reported under `skipped`, not dropped silently.
Not requested: `/openapi.json` (level 1 compares it) and the transcode listener's GET (a part's
bytes), which needs a task in `transcoding`, which an export at rest does not hold. A response pair
is equal when the status, the content type and the body are equal: JSON is parsed and compared with
its types (`true` is not `1`, `5` is not `5.0`), keys in `--ignore-key` dropped at any depth; any
other body is compared whole. Values taken from the rows go into the URL percent-encoded.

Three values are read off a server's clock, so they differ between the builds by design. The Rust
server starts after the Python one has finished, and these count as equal only when both sides have
one type, one text shape and one UTC offset, and the Rust value is later by the right time, give or
take `CLOCK_SLACK` seconds: `elapsed_seconds` and `last_progress_seconds` of any job's progress
(read when the request runs) by the time between the two requests, `scanned_at` (the title scan,
done once between the server's start and the titles request) by the time between those two
stretches. The report lists each value taken as equal this way, both sides. `eta_seconds` of a
running job is derived from `elapsed_seconds` and is compared exactly: an export at rest holds no
running job, and one that does shows as a difference.

    EMBYX_MANAGER_TEST_DATABASE_URL=postgresql://... uv run python tools/level2/get_diff.py DIR \\
        --binary target/release/embyx-manager --out diff.json

The report holds request paths, statuses and the JSON paths that differ with both values cut to
200 characters: values from a masked export only. `python_non_200` lists every request the Python
build did not answer with 200, so a stored row that neither build can serve stays visible; `binary`
is the Rust binary that ran.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'tools' / 'level2'))

from mask import load  # noqa: E402
from tools.bench.memory import API_TOKEN, TRANSCODE_TOKEN, OfflineProxy, Server, free_port, wait_healthy  # noqa: E402

HTTP_TIMEOUT = 600

#: Keys whose values depend on when the request ran, not on the rows.
DEFAULT_IGNORED = ('checked_at', 'generated_at', 'now', 'server_time', 'listed_at', 'age_seconds')
#: Values a server reads off its clock, and when: `request` while it answers the request (the
#: seconds since a job's stage started and since its last progress), `server` once between its
#: start and the request (the title scan, cached from the warm-up). `near` says when two are equal.
CLOCK_KEYS = {'scanned_at': 'server', 'elapsed_seconds': 'request', 'last_progress_seconds': 'request'}
#: Seconds a clock value may be off: both builds floor to whole seconds.
CLOCK_SLACK = 1.0
SWITCHES = ('enabled', 'auto_enabled', 'home_transcode_enabled')


async def switch_off(dsn: str) -> list[str]:
    """Set every scheduler switch of the copy's config to false; the sections changed."""
    import asyncpg  # noqa: PLC0415

    connection = await asyncpg.connect(dsn)
    changed = []
    try:
        for row in await connection.fetch('SELECT section, value_json FROM app_config'):
            value = json.loads(row['value_json'])
            if isinstance(value, dict) and any(value.get(key) is True for key in SWITCHES):
                value.update({key: False for key in SWITCHES if key in value})
                await connection.execute(
                    'UPDATE app_config SET value_json = $1 WHERE section = $2',
                    json.dumps(value, ensure_ascii=False, separators=(',', ':')),
                    row['section'],
                )
                changed.append(row['section'])
    finally:
        await connection.close()
    return changed


async def requests_for(dsn: str, per_route: int, skipped: list[str]) -> list[str]:
    import asyncpg  # noqa: PLC0415

    paths = [
        '/', '/api/health', '/api/auth/session', '/api/config', '/api/monitor/status', '/api/monitor/tracker',
        '/api/monitor/subscriptions', '/api/playlists', '/api/merge/tasks', '/api/merge/titles',
        '/api/monitor/manual/directories', '/api/monitor/manual/directories?path=/',
    ]  # fmt: skip
    paths += [f'/api/monitor/runs?limit={limit}&offset={offset}' for limit, offset in ((20, 0), (5, 5), (100, 0))]
    paths += [f'/api/monitor/runs?pipeline={pipeline}' for pipeline in ('rss', 'archive', 'mapping', 'nope')]
    paths += [f'/api/monitor/acquisitions?limit={limit}&offset={offset}' for limit, offset in ((50, 0), (10, 10))]
    states = ('discovered', 'downloading', 'archived', 'resolve_failed', 'exhausted', 'needs_attention', 'ignored')
    paths += [f'/api/monitor/acquisitions?state={state}' for state in states]
    connection = await asyncpg.connect(dsn)
    try:
        sections = [row['section'] for row in await connection.fetch('SELECT section FROM app_config ORDER BY 1')]
        paths += [f'/api/config/{urllib.parse.quote(section, safe="")}' for section in [*sections, 'unknown']]
        queries = {
            'avid': 'SELECT avid FROM archive_acquisitions ORDER BY updated_at DESC, avid LIMIT $1',
            'run': 'SELECT run_id FROM pipeline_runs ORDER BY started_at DESC LIMIT $1',
            'plan': 'SELECT plan_id FROM fill_actor_plans ORDER BY created_at DESC LIMIT $1',
            # The plan route answers by job id, also for a job whose plan is gone.
            'job': 'SELECT job_id FROM fill_actor_jobs ORDER BY created_at DESC, job_id LIMIT $1',
            'apply': 'SELECT job_id FROM fill_actor_apply_jobs ORDER BY job_id LIMIT $1',
            'playlist': 'SELECT key FROM playlists ORDER BY key LIMIT $1',
        }
        found: dict[str, list[str]] = {}
        for name, query in queries.items():
            try:
                found[name] = [str(row[0]) for row in await connection.fetch(query, per_route)]
            except asyncpg.PostgresError as error:
                skipped.append(f'{name}: {type(error).__name__}')
    finally:
        await connection.close()
    paths += item_paths(found)
    paths += ['/api/monitor/acquisitions/NOPE-000', '/api/monitor/runs/nope', '/api/fill-actor/plans/nope']
    return paths


def item_paths(found: dict[str, list[str]]) -> list[str]:
    """The item routes for the ids a copy holds, each id percent-encoded.

    Production playlist keys hold spaces and CJK text, which `urllib` refuses in a request line.
    """
    found = {name: [urllib.parse.quote(value, safe='') for value in values] for name, values in found.items()}
    paths: list[str] = []
    for avid in found.get('avid', []):
        paths += [f'/api/monitor/acquisitions/{avid}', f'/api/monitor/replace?avid={avid}']
    paths += [f'/api/monitor/runs/{run_id}' for run_id in found.get('run', [])]
    plans = dict.fromkeys([*found.get('plan', []), *found.get('job', [])])
    paths += [f'/api/fill-actor/plans/{plan_id}' for plan_id in plans]
    paths += [f'/api/fill-actor/apply-jobs/{job_id}' for job_id in found.get('apply', [])]
    for key in found.get('playlist', []):
        paths += [f'/api/playlists/{key}/missing', f'/api/playlists/{key}/fill']
    return paths


def answers(command: list[str], dsn: str, paths: list[str]) -> dict[str, Any]:
    work = Path(tempfile.mkdtemp(prefix='embyx-l2-'))
    proxy = OfflineProxy()
    port, transcode_port = free_port(), free_port()
    env = {
        'PATH': os.environ['PATH'], 'HOME': str(work), 'TMPDIR': str(work), 'LANG': 'C.UTF-8',
        'EMBYX_MANAGER_DATABASE_URL': dsn, 'EMBYX_MANAGER_HOST': '127.0.0.1', 'EMBYX_MANAGER_PORT': str(port),
        'EMBYX_MANAGER_TRANSCODE_PORT': str(transcode_port), 'EMBYX_MANAGER_API_TOKEN': API_TOKEN,
        'EMBYX_MANAGER_TRANSCODE_TOKEN': TRANSCODE_TOKEN, 'HTTPS_PROXY': proxy.url, 'HTTP_PROXY': proxy.url,
        'https_proxy': proxy.url, 'http_proxy': proxy.url, 'ALL_PROXY': proxy.url,
        'NO_PROXY': '127.0.0.1,localhost', 'no_proxy': '127.0.0.1,localhost',
    }  # fmt: skip
    with (work / 'server.log').open('wb') as log:
        spawned = time.time()
        process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT, cwd=work)
    try:
        server = Server(port, transcode_port)
        wait_healthy(server, process)
        time.sleep(1.0)
        out = {}
        for path in paths:
            out[path] = fetch(server.base, path) | {'spawned': spawned}
        out['__proxy_hosts__'] = proxy.hosts
        return out
    finally:
        process.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=30)
        if process.poll() is None:
            process.kill()
        shutil.rmtree(work, ignore_errors=True)


def fetch(base: str, path: str) -> dict[str, Any]:
    """One GET with the API token: status, content type, the body (JSON parsed, else text) and when it ran."""
    sent = time.time()
    request = urllib.request.Request(f'{base}{path}', method='GET')  # noqa: S310 - a local server
    request.add_header('authorization', f'Bearer {API_TOKEN}')
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=HTTP_TIMEOUT) as response:
            raw, status, kind = response.read(), response.status, response.headers.get('content-type')
    except urllib.error.HTTPError as error:
        raw, status, kind = error.read(), error.code, error.headers.get('content-type')
    try:
        body = json.loads(raw) if raw and kind and 'json' in kind else raw.decode('utf-8', 'replace')
    except ValueError:
        body = raw.decode('utf-8', 'replace')
    return {'status': status, 'content_type': kind, 'body': body, 'sent': sent, 'done': time.time()}


def strip(value: Any, ignored: frozenset[str]) -> Any:
    if isinstance(value, dict):
        return {key: strip(item, ignored) for key, item in value.items() if key not in ignored}
    if isinstance(value, list):
        return [strip(item, ignored) for item in value]
    return value


def clock_gaps(left: dict[str, Any], right: dict[str, Any]) -> dict[str, tuple[float, float]]:
    """The least and the most seconds the second answer's clock values can be ahead of the first's.

    `request`: both read while the request ran. `server`: both read at some time between the
    server's start and the end of the request.
    """
    return {
        'request': (right['sent'] - left['done'] - CLOCK_SLACK, right['done'] - left['sent'] + CLOCK_SLACK),
        'server': (right['spawned'] - left['done'] - CLOCK_SLACK, right['done'] - left['spawned'] + CLOCK_SLACK),
    }


def near(left: Any, right: Any, gap: tuple[float, float]) -> bool:
    """Whether the Rust clock value is later than the Python one by a time inside `gap`.

    Both must be numbers of one type, or times written in one shape (the same text once every
    digit is replaced) with one UTC offset: `...Z` against `...+00:00`, a time without its
    fraction, or another zone, stays a difference.
    """
    if type(left) is not type(right) or isinstance(left, bool):
        return False
    if isinstance(left, str):
        if re.sub(r'\d', '0', left) != re.sub(r'\d', '0', right):
            return False
        try:
            first, second = dt.datetime.fromisoformat(left), dt.datetime.fromisoformat(right)
        except ValueError:
            return False
        if first.utcoffset() != second.utcoffset():
            return False
        ahead = (second - first).total_seconds()
    elif isinstance(left, int | float):
        ahead = right - left
    else:
        return False
    return gap[0] <= ahead <= gap[1]


def differences(
    left: Any,
    right: Any,
    path: str = '$',
    gaps: dict[str, tuple[float, float]] | None = None,
    tolerated: list[dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    """The JSON paths where two values differ, each side cut to 200 characters.

    With `gaps` (`clock_gaps` of the two answers), a `CLOCK_KEYS` value is equal when `near` says
    so, and it is added to `tolerated` with both sides; without, it is compared like any other.
    """
    if isinstance(left, dict) and isinstance(right, dict):
        out = []
        for key in [*left, *(key for key in right if key not in left)]:
            if key not in left or key not in right:
                side = 'python' if key in left else 'rust'
                out.append({'path': f'{path}.{key}', 'only': side})
            elif gaps is not None and key in CLOCK_KEYS and near(left[key], right[key], gaps[CLOCK_KEYS[key]]):
                if tolerated is not None:
                    tolerated.append({'path': f'{path}.{key}', 'python': str(left[key]), 'rust': str(right[key])})
            else:
                out += differences(left[key], right[key], f'{path}.{key}', gaps, tolerated)
        return out
    if isinstance(left, list) and isinstance(right, list) and len(left) == len(right):
        return [
            item
            for index, pair in enumerate(zip(left, right, strict=True))
            for item in differences(*pair, f'{path}[{index}]', gaps, tolerated)
        ]
    # `type` too: Python's `True == 1 == 1.0` would hide a bool sent as a number.
    if type(left) is type(right) and left == right:
        return []
    return [{
        'path': path,
        'python': json.dumps(left, ensure_ascii=False)[:200],
        'rust': json.dumps(right, ensure_ascii=False)[:200],
    }]  # fmt: skip


def compare(paths: list[str], python: dict[str, Any], rust: dict[str, Any], ignored: frozenset[str]) -> dict[str, Any]:
    """The report's comparison part: equal and differing requests, and what the clock rule let pass."""
    out: dict[str, Any] = {'equal': 0, 'different': [], 'clock_tolerated': []}
    for path in paths:
        left, right = python[path], rust[path]
        found = [
            {'path': field, 'python': str(left[field]), 'rust': str(right[field])}
            for field in ('status', 'content_type')
            if left[field] != right[field]
        ]
        tolerated: list[dict[str, str]] = []
        found += differences(
            strip(left['body'], ignored),
            strip(right['body'], ignored),
            gaps=clock_gaps(left, right),
            tolerated=tolerated,
        )
        # Each clock value that counted as equal, and how long after Python's request Rust's was sent.
        ahead = round(right['sent'] - left['sent'], 1)
        out['clock_tolerated'] += [{'request': path, 'requests_apart': ahead} | item for item in tolerated]
        if found:
            out['different'].append({'request': path, 'differences': found})
        else:
            out['equal'] += 1
    # How many answers of each status the Python build gave (equal answers that are all errors
    # would say little), and every request it did not answer with 200.
    statuses: dict[str, int] = {}
    for path in paths:
        status = str(python[path]['status'])
        statuses[status] = statuses.get(status, 0) + 1
    out['python_statuses'] = dict(sorted(statuses.items()))
    out['python_non_200'] = [
        {'request': path, 'status': python[path]['status']}
        for path in paths
        if python[path]['status'] != 200  # noqa: PLR2004
    ]
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('export', type=Path, help='a directory written by mask.py export')
    parser.add_argument('--binary', required=True, help='the Rust embyx-manager binary')
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--per-route', type=int, default=20)
    parser.add_argument('--ignore-key', action='append', default=list(DEFAULT_IGNORED))
    args = parser.parse_args()
    admin = os.environ['EMBYX_MANAGER_TEST_DATABASE_URL']
    ignored = frozenset(args.ignore_key)
    # Absolute: the server starts in a scratch directory. A name that is no file here is looked up on PATH.
    binary = Path(args.binary)
    binary = (binary if binary.exists() else Path(shutil.which(args.binary) or args.binary)).resolve()
    results: dict[str, dict[str, Any]] = {}
    switched: list[str] = []
    paths: list[str] = []
    skipped: list[str] = []
    for side, command in (
        ('python', [str(ROOT / '.venv/bin/python'), '-m', 'embyx_manager', 'serve']),
        ('rust', [str(binary), 'serve']),
    ):
        name = f'embyx_l2_{side}_{os.getpid()}'
        dsn = asyncio.run(load(args.export, admin, name))
        try:
            switched = asyncio.run(switch_off(dsn))
            if not paths:
                paths = asyncio.run(requests_for(dsn, args.per_route, skipped))
                # Each build serves the front end from its own place (D59); compare the page
                # only when both have one.
                fronts = (ROOT / 'src/embyx_manager/static', binary.parent / 'static')
                if not all((front / 'index.html').is_file() for front in fronts):
                    paths.remove('/')
                    skipped.append('/: a build without its front end (npm run build; static/ beside the binary)')
            results[side] = answers(command, dsn, paths)
        finally:
            asyncio.run(_drop(admin, name))
    report: dict[str, Any] = {
        'binary': str(binary), 'requests': len(paths), 'skipped': skipped, 'switched_off': switched,
    }  # fmt: skip
    report |= compare(paths, results['python'], results['rust'], ignored)
    report['proxy_hosts'] = {side: results[side]['__proxy_hosts__'] for side in results}
    args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(f'{report["equal"]} of {len(paths)} requests equal; {len(report["different"])} different')
    for item in skipped:
        print('  skipped:', item)
    for item in report['different'][:20]:
        print(' ', item['request'], [difference['path'] for difference in item['differences']][:5])
    return 1 if report['different'] else 0


async def _drop(admin: str, name: str) -> None:
    import asyncpg  # noqa: PLC0415

    connection = await asyncpg.connect(admin)
    try:
        await connection.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    finally:
        await connection.close()


if __name__ == '__main__':
    sys.exit(main())
