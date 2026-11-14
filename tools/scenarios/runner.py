"""Run effect-transcript scenarios on the Python baseline (docs/rewrite/SCENARIOS.md).

A scenario (`tools/scenarios/specs/<area>/<name>.toml`) is a seed (directory tree, database
rows, fake CloudDrive / Kubernetes / Emby state) plus one operation. This runner executes the
operation on the baseline tree against the seed and writes the transcript to
`goldens/scenarios/<area>/<name>.json`; the Rust build runs the same scenario and must
produce the same bytes.

    uv run python tools/scenarios/runner.py            # rewrite the transcripts
    uv run python tools/scenarios/runner.py --check    # what CI runs
    uv run python tools/scenarios/runner.py harness/trivial   # one scenario

Needs `EMBYX_MANAGER_TEST_DATABASE_URL` (a disposable server: each scenario creates and drops
its own database there). Nothing here talks to a real service.
"""

from __future__ import annotations

import argparse
import asyncio
import difflib
import os
import shutil
import sys
import tempfile
import time
import tomllib
import traceback
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
SPEC_DIR = HERE / 'specs'
OUTPUT_DIR = REPO_ROOT / 'goldens' / 'scenarios'
DEFAULT_CLOCK = '2026-01-01T00:00:00+00:00'
URL_VARIABLE = 'EMBYX_MANAGER_TEST_DATABASE_URL'

# The process's local zone is UTC, as in CI; tzset() makes `time` and `datetime` see it.
os.environ['TZ'] = 'UTC'
time.tzset()
sys.path.insert(0, str(REPO_ROOT / 'src'))
sys.path.append(str(HERE))

import asyncpg  # noqa: E402
from fakes.clouddrive import FakeCloudDriveServer  # noqa: E402
from fakes.http import FakeHttpServer  # noqa: E402
from operations import OPERATIONS  # noqa: E402
from transcript import render  # noqa: E402

DUMP_SQL = """
SELECT COALESCE(jsonb_agg(row_value ORDER BY row_value::text), '[]'::jsonb)::text
FROM (SELECT to_jsonb(t) AS row_value FROM {table} AS t) AS dumped
"""
TABLES_SQL = """
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name <> 'schema_migrations'
ORDER BY table_name
"""


@dataclass
class Context:
    """What an operation gets: the roots, the clock, the database and the fakes' addresses."""

    roots: dict[str, Path]
    now: datetime
    dsn: str
    args: dict[str, Any]
    clouddrive_address: str = ''
    http_fakes: dict[str, str] = field(default_factory=dict)
    #: Appends one call record to the transcript's `calls`, for a fake that lives inside the
    #: operation (the recording file system of `operations/archive.py`).
    record: Callable[[dict[str, Any]], None] = field(default=lambda _call: None)
    _connection: asyncpg.Connection | None = None

    async def connection(self) -> asyncpg.Connection:
        if self._connection is None:
            self._connection = await asyncpg.connect(self.dsn)
        return self._connection

    async def execute(self, statement: str, *values: object) -> str:
        return await (await self.connection()).execute(statement, *values)

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()


def load_specs(names: list[str]) -> dict[str, dict[str, Any]]:
    specs = {
        path.relative_to(SPEC_DIR).with_suffix('').as_posix(): tomllib.loads(path.read_text(encoding='utf-8'))
        for path in sorted(SPEC_DIR.rglob('*.toml'))
    }
    if names:
        unknown = sorted(set(names) - set(specs))
        if unknown:
            msg = f'no such scenario: {", ".join(unknown)}'
            raise SystemExit(msg)
        specs = {name: spec for name, spec in specs.items() if name in names}
    return specs


def seed_tree(base: Path, spec: dict[str, Any]) -> dict[str, Path]:
    roots = {name: base / name for name in spec.get('roots', {}).get('names', [])}
    for root in roots.values():
        root.mkdir()
    for entry in spec.get('tree', []):
        root_name, _, relative = entry['path'].partition('/')
        path = roots[root_name] / relative
        kind = entry.get('kind', 'file')
        path.parent.mkdir(parents=True, exist_ok=True)
        if kind == 'dir':
            path.mkdir(exist_ok=True)
        elif kind == 'file':
            data = entry['text'].encode() if 'text' in entry else b'x' * int(entry.get('size', 0))
            path.write_bytes(data)
        elif kind == 'symlink':
            path.symlink_to(entry['target'])
        elif kind == 'hardlink':
            target_root, _, target_relative = entry['target'].partition('/')
            path.hardlink_to(roots[target_root] / target_relative)
        else:
            msg = f'unknown tree entry kind {kind!r}'
            raise ValueError(msg)
    return roots


async def create_database(admin_dsn: str, name: str) -> str:
    connection = await asyncpg.connect(admin_dsn)
    try:
        await connection.execute(f'CREATE DATABASE "{name}"')
    finally:
        await connection.close()
    # Same server and credentials, the scenario's own database.
    head, _, query = admin_dsn.partition('?')
    base = head.rsplit('/', 1)[0]
    return f'{base}/{name}' + (f'?{query}' if query else '')


async def drop_database(admin_dsn: str, name: str) -> None:
    connection = await asyncpg.connect(admin_dsn)
    try:
        await connection.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    finally:
        await connection.close()


async def seed_database(dsn: str, spec: dict[str, Any]) -> None:
    database = spec.get('database', {})
    schema = database.get('schema', 'none')
    if schema == 'app':
        from embyx_manager.db import Database  # noqa: PLC0415

        app = Database(dsn)
        try:
            await app.get_pool()
        finally:
            await app.aclose()
    elif schema != 'none':
        msg = f'database.schema must be "none" or "app", not {schema!r}'
        raise ValueError(msg)
    connection = await asyncpg.connect(dsn)
    try:
        for statement in database.get('sql', []):
            await connection.execute(statement)
    finally:
        await connection.close()


async def dump_rows(dsn: str, spec: dict[str, Any]) -> dict[str, Any]:
    import json  # noqa: PLC0415

    connection = await asyncpg.connect(dsn)
    try:
        await connection.execute("SET TimeZone TO 'UTC'")
        tables = spec.get('database', {}).get('tables') or [
            row['table_name'] for row in await connection.fetch(TABLES_SQL)
        ]
        rows = {}
        for table in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            rows[table] = json.loads(await connection.fetchval(DUMP_SQL.format(table=quoted)))
        return rows
    finally:
        await connection.close()


def _outcome_of(error: BaseException) -> dict[str, Any]:
    return {'error': {'type': type(error).__name__, 'message': str(error)}}


async def run_one(name: str, spec: dict[str, Any], admin_dsn: str, index: int) -> str:
    operation = OPERATIONS.get(spec['operation'])
    if operation is None:
        msg = f'{name}: unknown operation {spec["operation"]!r}'
        raise SystemExit(msg)
    calls: list[dict[str, Any]] = []
    db_name = f'embyx_scenario_{os.getpid()}_{index}'
    base = Path(tempfile.mkdtemp(prefix='embyx-scenario-'))
    dsn = await create_database(admin_dsn, db_name)
    try:
        roots = seed_tree(base, spec)
        await seed_database(dsn, spec)
        with ExitStack() as stack:
            clouddrive = stack.enter_context(FakeCloudDriveServer(spec.get('clouddrive', {}), calls.append))
            http_fakes = {
                fake: stack.enter_context(FakeHttpServer(fake, spec.get(fake, {}), calls.append))
                for fake in ('kubernetes', 'emby')
            }
            context = Context(
                roots=roots,
                now=datetime.fromisoformat(spec.get('clock', DEFAULT_CLOCK)),
                dsn=dsn,
                args=dict(spec.get('args', {})),
                clouddrive_address=clouddrive.address,
                http_fakes={fake: server.url for fake, server in http_fakes.items()},
                record=calls.append,
            )
            try:
                outcome: dict[str, Any] = {'result': await operation(context)}
            except Exception as error:  # noqa: BLE001 - an operation's failure is part of its transcript
                outcome = _outcome_of(error)
            finally:
                await context.close()
            fakes = {
                'clouddrive': clouddrive.fake.dump(),
                **{fake: server.dump() for fake, server in http_fakes.items()},
            }
        rows = await dump_rows(dsn, spec)
        return render(scenario=name, spec=spec, outcome=outcome, calls=calls, roots=roots, rows=rows, fakes=fakes)
    finally:
        shutil.rmtree(base, ignore_errors=True)
        await drop_database(admin_dsn, db_name)


async def run(names: list[str], *, check: bool) -> int:
    admin_dsn = os.environ.get(URL_VARIABLE)
    if not admin_dsn:
        print(f'{URL_VARIABLE} is not set; scenarios need a disposable PostgreSQL server')
        return 2
    problems = 0
    specs = load_specs(names)
    for index, (name, spec) in enumerate(specs.items()):
        try:
            text = await run_one(name, spec, admin_dsn, index)
        except Exception:  # noqa: BLE001 - report every broken scenario, then fail
            print(f'error: {name} did not run:')
            traceback.print_exc()
            problems += 1
            continue
        path = OUTPUT_DIR / f'{name}.json'
        if check:
            have = path.read_text(encoding='utf-8') if path.exists() else None
            if have != text:
                problems += 1
                print(f'drift: goldens/scenarios/{name}.json differs from what the Python baseline produces')
                diff = difflib.unified_diff(
                    (have or '').splitlines(), text.splitlines(), 'committed', 'generated', lineterm='', n=1
                )
                for line in list(diff)[:40]:
                    print(f'  {line}')
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding='utf-8', newline='\n')
    if check and not names:
        expected = {f'{name}.json' for name in specs}
        stale = (
            sorted(path.relative_to(OUTPUT_DIR).as_posix() for path in OUTPUT_DIR.rglob('*.json'))
            if OUTPUT_DIR.exists()
            else []
        )
        for relative in stale:
            if relative not in expected:
                problems += 1
                print(f'stale: goldens/scenarios/{relative} has no scenario')
    print(f'{len(specs)} scenario(s), {problems} problem(s)')
    return 1 if problems else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n', 1)[0])
    parser.add_argument(
        '--check', action='store_true', help='compare with the committed transcripts instead of writing'
    )
    parser.add_argument('names', nargs='*', help='scenarios to run (default: all)')
    args = parser.parse_args()
    return asyncio.run(run(args.names, check=args.check))


if __name__ == '__main__':
    sys.exit(main())
