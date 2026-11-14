"""Level 2 GET differ: the same GET requests to the Python and the Rust server, on the same rows.

Each build runs in turn on its own fresh copy of one export (`mask.py load`), so neither reads
what the other wrote. Before a server starts, the copy's schedulers are switched off (every
section's `enabled`, `auto_enabled` and `home_transcode_enabled` set to false in `app_config`),
so no run starts and the rows stay as exported; every outbound request goes to a local proxy that
refuses it (as in tools/bench/memory.py), so nothing leaves the machine.

The requests: every GET route of the two APIs, the list routes with a few filters and pages, and
the item routes for the first `--per-route` ids the copy holds (acquisition AVIDs, run ids, plan
and apply-job ids, playlist keys, replace plans). A response pair is equal when the status, the
content type and the body (JSON parsed, keys in `--ignore-key` dropped at any depth) are equal.

    EMBYX_MANAGER_TEST_DATABASE_URL=postgresql://... uv run python tools/level2/get_diff.py DIR \\
        --binary target/release/embyx-manager --out diff.json

The report holds request paths, statuses and the JSON paths that differ with both values cut to
200 characters: values from a masked export only.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'tools' / 'level2'))

from mask import load  # noqa: E402
from tools.bench.memory import API_TOKEN, TRANSCODE_TOKEN, OfflineProxy, Server, free_port, wait_healthy  # noqa: E402

#: Keys whose values depend on when the request ran, not on the rows.
DEFAULT_IGNORED = ('checked_at', 'generated_at', 'now', 'server_time', 'listed_at', 'age_seconds')
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


async def requests_for(dsn: str, per_route: int) -> list[str]:
    import asyncpg  # noqa: PLC0415

    paths = [
        '/api/health', '/api/auth/session', '/api/config', '/api/monitor/status', '/api/monitor/tracker',
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
        paths += [f'/api/config/{section}' for section in [*sections, 'unknown']]
        queries = {
            'avid': 'SELECT avid FROM archive_acquisitions ORDER BY updated_at DESC, avid LIMIT $1',
            'run': 'SELECT run_id FROM pipeline_runs ORDER BY started_at DESC LIMIT $1',
            'plan': 'SELECT plan_id FROM fill_actor_plans ORDER BY created_at DESC LIMIT $1',
            'apply': 'SELECT job_id FROM fill_actor_apply_jobs ORDER BY created_at DESC LIMIT $1',
            'playlist': 'SELECT key FROM playlists ORDER BY key LIMIT $1',
        }
        found: dict[str, list[str]] = {}
        for name, query in queries.items():
            with contextlib.suppress(asyncpg.PostgresError):
                found[name] = [str(row[0]) for row in await connection.fetch(query, per_route)]
    finally:
        await connection.close()
    for avid in found.get('avid', []):
        paths += [f'/api/monitor/acquisitions/{avid}', f'/api/monitor/replace?avid={avid}']
    paths += [f'/api/monitor/runs/{run_id}' for run_id in found.get('run', [])]
    paths += [f'/api/fill-actor/plans/{plan_id}' for plan_id in found.get('plan', [])]
    paths += [f'/api/fill-actor/apply-jobs/{job_id}' for job_id in found.get('apply', [])]
    for key in found.get('playlist', []):
        paths += [f'/api/playlists/{key}/missing', f'/api/playlists/{key}/fill']
    paths += ['/api/monitor/acquisitions/NOPE-000', '/api/monitor/runs/nope', '/api/fill-actor/plans/nope']
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
        process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT, cwd=work)
    try:
        server = Server(port, transcode_port)
        wait_healthy(server, process)
        time.sleep(1.0)
        out = {}
        for path in paths:
            status, body = server.call('GET', path)
            out[path] = {'status': status, 'body': body}
        out['__proxy_hosts__'] = proxy.hosts
        return out
    finally:
        process.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=30)
        if process.poll() is None:
            process.kill()
        shutil.rmtree(work, ignore_errors=True)


def strip(value: Any, ignored: frozenset[str]) -> Any:
    if isinstance(value, dict):
        return {key: strip(item, ignored) for key, item in value.items() if key not in ignored}
    if isinstance(value, list):
        return [strip(item, ignored) for item in value]
    return value


def differences(left: Any, right: Any, path: str = '$') -> list[dict[str, str]]:
    """The JSON paths where two values differ, each side cut to 200 characters."""
    if isinstance(left, dict) and isinstance(right, dict):
        out = []
        for key in [*left, *(key for key in right if key not in left)]:
            if key not in left or key not in right:
                side = 'python' if key in left else 'rust'
                out.append({'path': f'{path}.{key}', 'only': side})
            else:
                out += differences(left[key], right[key], f'{path}.{key}')
        return out
    if isinstance(left, list) and isinstance(right, list) and len(left) == len(right):
        return [
            item
            for index, pair in enumerate(zip(left, right, strict=True))
            for item in differences(*pair, f'{path}[{index}]')
        ]
    if left == right:
        return []
    return [{
        'path': path,
        'python': json.dumps(left, ensure_ascii=False)[:200],
        'rust': json.dumps(right, ensure_ascii=False)[:200],
    }]  # fmt: skip


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
    results: dict[str, dict[str, Any]] = {}
    switched: list[str] = []
    paths: list[str] = []
    for side, command in (
        ('python', [str(ROOT / '.venv/bin/python'), '-m', 'embyx_manager', 'serve']),
        ('rust', [args.binary, 'serve']),
    ):
        name = f'embyx_l2_{side}_{os.getpid()}'
        dsn = asyncio.run(load(args.export, admin, name))
        try:
            switched = asyncio.run(switch_off(dsn))
            paths = paths or asyncio.run(requests_for(dsn, args.per_route))
            results[side] = answers(command, dsn, paths)
        finally:
            asyncio.run(_drop(admin, name))
    report: dict[str, Any] = {'requests': len(paths), 'switched_off': switched, 'equal': 0, 'different': []}
    for path in paths:
        left, right = results['python'][path], results['rust'][path]
        found = []
        if left['status'] != right['status']:
            found.append({'path': 'status', 'python': str(left['status']), 'rust': str(right['status'])})
        found += differences(strip(left['body'], ignored), strip(right['body'], ignored))
        if found:
            report['different'].append({'request': path, 'differences': found})
        else:
            report['equal'] += 1
    report['proxy_hosts'] = {side: results[side]['__proxy_hosts__'] for side in results}
    args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(f'{report["equal"]} of {len(paths)} requests equal; {len(report["different"])} different')
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
