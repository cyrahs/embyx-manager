"""Level 2 export with masking: production data reduced to what the comparisons need, secrets out.

Level 2 (docs/rewrite/PLAN.md) compares the two builds on a copy of production data. Its export
was not allowed in WP16, so this tool was built and tested on synthetic data only. It never
writes an unmasked value: rows are masked in memory and only then written.

    export   read every table of a database, mask, write `<table>.jsonl` and `manifest.json`
    check    scan an export for anything that still looks like a secret; exit 1 if found
    load     create a scratch database, migrate it with the baseline's migrations and load an
             export into it (for the GET differ and the decision replay)

What is masked:

* `app_config`: every section's `SECRET_FIELDS` (the baseline's models: `clouddrive.api_token`,
  `emby.api_key`, any added later) and, at any depth, any key that names a secret (token, key,
  secret, password, passkey, cookie, authorization, signature). A non-empty value becomes
  `MASKED`, so "configured" stays true; an empty one stays empty.
* Every text value of every table: URL credentials (`user:pass@`), query parameters that name
  a secret, and `Bearer` / `Basic` credentials.
* The literal value of every secret seen in `app_config` and of every environment variable named
  by `--secret-env` (by default the API and transcode tokens), wherever it appears as a
  substring. Values are read from the environment, never from the command line, and never
  printed.

The manifest holds table names, row counts, the schema version and how many values each rule
masked, nothing else. `check` re-reads the export: a literal secret still present, a secret-named
JSON key with an unmasked value, or a secret-named URL parameter fails it.

    EMBYX_EXPORT_DSN=postgresql://... uv run python tools/level2/mask.py export --out DIR
    uv run python tools/level2/mask.py check DIR
    EMBYX_MANAGER_TEST_DATABASE_URL=postgresql://... uv run python tools/level2/mask.py load DIR --name copy
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import datetime as dt
import decimal
import json
import os
import re
import sys
import uuid
from pathlib import Path
from typing import Any

MASKED = 'MASKED'
DEFAULT_SECRET_ENVS = ('EMBYX_MANAGER_API_TOKEN', 'EMBYX_MANAGER_TRANSCODE_TOKEN')
SECRET_KEY = re.compile(
    r'(?:^|[_\-.])(?:api[_\-]?)?(?:token|key|secret|password|passwd|passkey|cookie|authorization|auth|signature|sig)s?$',
    re.IGNORECASE,
)
URL_USERINFO = re.compile(r'(?P<scheme>[a-z][a-z0-9+.\-]*://)(?P<userinfo>[^/@\s"\'?#]+)@', re.IGNORECASE)
URL_PARAM = re.compile(r'(?P<lead>[?&;])(?P<name>[^=&#\s"\'<>]+)=(?P<value>[^&#\s"\'<>]*)')
AUTH_HEADER = re.compile(r'\b(?P<kind>Bearer|Basic)\s+(?P<value>[A-Za-z0-9._~+/=\-]{8,})', re.IGNORECASE)
#: Keys that name a secret but hold none: config fields whose name ends like one.
NOT_SECRET_KEYS = frozenset({'brand_mapping', 'key', 'playlist_key', 'sort_key', 'request_key', 'item_key'})


def secret_key(name: str) -> bool:
    return name not in NOT_SECRET_KEYS and SECRET_KEY.search(name) is not None


def secret_param(name: str) -> bool:
    """A URL query parameter that carries a credential (`?key=` does, unlike a JSON `key`)."""
    return name.lower() in {'key', 'k'} or secret_key(name)


def model_secret_fields() -> dict[str, frozenset[str]]:
    """`SECRET_FIELDS` of every stored section, read off the baseline's models."""
    from embyx_manager.config.store import SECTION_MODELS  # noqa: PLC0415

    return {name: frozenset(getattr(model, 'SECRET_FIELDS', frozenset())) for name, model in SECTION_MODELS.items()}


class Masker:
    def __init__(self, literals: set[str], secret_fields: dict[str, frozenset[str]]) -> None:
        self.literals = {value for value in literals if len(value) >= 4}  # noqa: PLR2004 - shorter ones mask words
        self.secret_fields = secret_fields
        self.counts: dict[str, int] = {}

    def _count(self, rule: str) -> None:
        self.counts[rule] = self.counts.get(rule, 0) + 1

    def collect_config(self, rows: list[dict[str, Any]]) -> None:
        """Learn the literal secrets in `app_config` before anything is masked."""
        for row in rows:
            try:
                value = json.loads(row.get('value_json') or 'null')
            except ValueError:
                continue
            fields = self.secret_fields.get(str(row.get('section')), frozenset())
            self._collect(value, fields)

    def _collect(self, value: Any, fields: frozenset[str]) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if (key in fields or secret_key(key)) and isinstance(item, str) and item:
                    self.literals.add(item)
                else:
                    self._collect(item, frozenset())
        elif isinstance(value, list):
            for item in value:
                self._collect(item, frozenset())

    def text(self, value: str) -> str:
        for literal in sorted(self.literals, key=len, reverse=True):
            if literal in value:
                value = value.replace(literal, MASKED)
                self._count('literal')

        def userinfo(match: re.Match[str]) -> str:
            self._count('url_userinfo')
            return f'{match["scheme"]}{MASKED}@'

        def param(match: re.Match[str]) -> str:
            if not secret_param(match['name']) or not match['value'] or match['value'] == MASKED:
                return match.group(0)
            self._count('url_param')
            return f'{match["lead"]}{match["name"]}={MASKED}'

        def header(match: re.Match[str]) -> str:
            if match['value'] == MASKED:
                return match.group(0)
            self._count('auth_header')
            return f'{match["kind"]} {MASKED}'

        value = URL_USERINFO.sub(userinfo, value)
        value = URL_PARAM.sub(param, value)
        return AUTH_HEADER.sub(header, value)

    def json_value(self, value: Any, fields: frozenset[str] = frozenset()) -> Any:
        if isinstance(value, dict):
            out = {}
            for key, item in value.items():
                if (key in fields or secret_key(key)) and isinstance(item, str):
                    if item:
                        self._count('secret_key')
                    out[key] = MASKED if item else item
                else:
                    out[key] = self.json_value(item)
            return out
        if isinstance(value, list):
            return [self.json_value(item) for item in value]
        if isinstance(value, str):
            return self.text(value)
        return value

    def row(self, table: str, row: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for column, value in row.items():
            if table == 'app_config' and column == 'value_json' and isinstance(value, str):
                fields = self.secret_fields.get(str(row.get('section')), frozenset())
                out[column] = json.dumps(
                    self.json_value(json.loads(value), fields), ensure_ascii=False, separators=(',', ':')
                )
            elif isinstance(value, str) and column.endswith('_json'):
                try:
                    parsed = json.loads(value)
                except ValueError:
                    out[column] = self.text(value)
                else:
                    out[column] = json.dumps(self.json_value(parsed), ensure_ascii=False, separators=(',', ':'))
            elif isinstance(value, str):
                out[column] = self.text(value)
            else:
                out[column] = value
        return out


def plain(value: Any) -> Any:
    """A database value as JSON: times as ISO text with their offset, bytes as base64."""
    if isinstance(value, dt.datetime | dt.date | dt.time):
        return {'$time': value.isoformat()}
    if isinstance(value, bytes | bytearray | memoryview):
        return {'$bytes': base64.b64encode(bytes(value)).decode()}
    if isinstance(value, decimal.Decimal | uuid.UUID):
        return str(value)
    if isinstance(value, list):
        return [plain(item) for item in value]
    return value


def restore(value: Any) -> Any:
    if isinstance(value, dict) and set(value) == {'$time'}:
        text = value['$time']
        return dt.datetime.fromisoformat(text) if 'T' in text else dt.date.fromisoformat(text)
    if isinstance(value, dict) and set(value) == {'$bytes'}:
        return base64.b64decode(value['$bytes'])
    return value


async def tables(connection: Any) -> list[str]:
    rows = await connection.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename",
    )
    return [row['tablename'] for row in rows]


async def export(dsn: str, out: Path, secret_envs: list[str]) -> dict[str, Any]:
    import asyncpg  # noqa: PLC0415

    literals = {os.environ[name] for name in secret_envs if os.environ.get(name)}
    masker = Masker(literals, model_secret_fields())
    out.mkdir(parents=True, exist_ok=True)  # noqa: ASYNC240 - a command-line tool; nothing else runs
    connection = await asyncpg.connect(dsn)
    manifest: dict[str, Any] = {'tables': {}}
    try:
        names = await tables(connection)
        version = await connection.fetchval('SELECT MAX(version) FROM schema_migrations')
        manifest['schema_version'] = version
        if 'app_config' in names:
            masker.collect_config([dict(row) for row in await connection.fetch('SELECT * FROM app_config')])
        for table in names:
            rows = await connection.fetch(f'SELECT * FROM "{table}" ORDER BY 1')  # noqa: S608 - names from pg_tables
            with (out / f'{table}.jsonl').open('w', encoding='utf-8') as handle:
                for row in rows:
                    masked = masker.row(table, {key: plain(value) for key, value in dict(row).items()})
                    handle.write(json.dumps(masked, ensure_ascii=False, sort_keys=True) + '\n')
            manifest['tables'][table] = len(rows)
    finally:
        await connection.close()
    manifest['masked'] = dict(sorted(masker.counts.items()))
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    return manifest


def _text_problems(value: str, literals: set[str]) -> list[str]:
    """What in one text still looks like a secret (rule names only, never the value)."""
    problems = ['a secret from the environment'] if any(literal in value for literal in literals) else []
    problems += ['URL credentials' for match in URL_USERINFO.finditer(value) if match['userinfo'] != MASKED]
    problems += [
        f'URL parameter {match["name"]!r}'
        for match in URL_PARAM.finditer(value)
        if secret_param(match['name']) and match['value'] not in {'', MASKED}
    ]
    problems += [f'{match["kind"]} credentials' for match in AUTH_HEADER.finditer(value) if match['value'] != MASKED]
    return problems


def _walk(value: Any, literals: set[str]) -> list[str]:
    if isinstance(value, dict):
        problems = [
            f'secret-named key {key!r} is not masked'
            for key, item in value.items()
            if secret_key(key) and isinstance(item, str) and item not in {'', MASKED}
        ]
        for item in value.values():
            problems += _walk(item, literals)
        return problems
    if isinstance(value, list):
        return [problem for item in value for problem in _walk(item, literals)]
    if not isinstance(value, str):
        return []
    problems = _text_problems(value, literals)
    if value[:1] in '{[':
        with contextlib.suppress(ValueError):
            problems += _walk(json.loads(value), literals)
    return problems


def check(directory: Path, secret_envs: list[str]) -> list[str]:
    """What still looks like a secret in an export, as `<table>:<line>: <rule>` (no values)."""
    literals = {os.environ[name] for name in secret_envs if len(os.environ.get(name, '')) >= 4}  # noqa: PLR2004
    problems: list[str] = []
    for path in sorted(directory.glob('*.jsonl')):
        for number, line in enumerate(path.read_text(encoding='utf-8').splitlines(), start=1):
            found = dict.fromkeys(_walk(json.loads(line), literals))
            problems += [f'{path.stem}:{number}: {problem}' for problem in found]
    return problems


async def load(directory: Path, admin_dsn: str, name: str) -> str:
    """A scratch database `name` holding the export, migrated by the baseline; its DSN."""
    import asyncpg  # noqa: PLC0415

    from embyx_manager.db import Database  # noqa: PLC0415

    admin = await asyncpg.connect(admin_dsn)
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()
    dsn = admin_dsn.rsplit('/', 1)[0] + f'/{name}'
    database = Database(dsn)
    await database.get_pool()
    await database.aclose()
    connection = await asyncpg.connect(dsn)
    try:
        async with connection.transaction():
            await connection.execute('SET LOCAL session_replication_role = replica')
            for path in sorted(directory.glob('*.jsonl')):  # noqa: ASYNC240 - a command-line tool
                rows = [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines()]
                await connection.execute(f'DELETE FROM "{path.stem}"')  # noqa: S608 - our own file names
                if not rows:
                    continue
                columns = list(rows[0])
                records = [tuple(restore(row[column]) for column in columns) for row in rows]
                await connection.copy_records_to_table(path.stem, records=records, columns=columns)
            for row in await connection.fetch(
                """
                SELECT table_name, column_name, pg_get_serial_sequence(table_name, column_name) AS sequence
                FROM information_schema.columns
                WHERE table_schema = 'public' AND (column_default LIKE 'nextval(%' OR is_identity = 'YES')
                """,
            ):
                await connection.execute(
                    f'SELECT setval($1, COALESCE((SELECT MAX("{row["column_name"]}") FROM "{row["table_name"]}"), 0) + 1, false)',  # noqa: S608, E501
                    row['sequence'],
                )
    finally:
        await connection.close()
    return dsn


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest='command', required=True)
    exporting = commands.add_parser('export')
    exporting.add_argument('--dsn-env', default='EMBYX_EXPORT_DSN', help='environment variable holding the DSN')
    exporting.add_argument('--out', type=Path, required=True)
    exporting.add_argument('--secret-env', action='append', default=None)
    checking = commands.add_parser('check')
    checking.add_argument('directory', type=Path)
    checking.add_argument('--secret-env', action='append', default=None)
    loading = commands.add_parser('load')
    loading.add_argument('directory', type=Path)
    loading.add_argument('--name', required=True, help='the scratch database to create (dropped first)')
    args = parser.parse_args()
    secret_envs = args.secret_env if getattr(args, 'secret_env', None) else list(DEFAULT_SECRET_ENVS)
    if args.command == 'export':
        manifest = asyncio.run(export(os.environ[args.dsn_env], args.out, secret_envs))
        problems = check(args.out, secret_envs)
        print(json.dumps({'tables': len(manifest['tables']), 'masked': manifest['masked'], 'problems': len(problems)}))
        return 1 if problems else 0
    if args.command == 'check':
        problems = check(args.directory, secret_envs)
        for problem in problems:
            print(problem)
        return 1 if problems else 0
    dsn = asyncio.run(load(args.directory, os.environ['EMBYX_MANAGER_TEST_DATABASE_URL'], args.name))
    print(dsn.rsplit('/', 1)[-1])
    return 0


if __name__ == '__main__':
    sys.exit(main())
