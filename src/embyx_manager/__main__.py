import argparse
import asyncio
import os
import sys
import tomllib
from pathlib import Path

import uvicorn

from embyx_manager.bootstrap import build_app
from embyx_manager.config import ConfigStore
from embyx_manager.db import Database
from embyx_manager.settings import Settings


def _serve() -> None:
    settings = Settings.from_env()
    app = build_app(settings)
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        proxy_headers=False,
        server_header=False,
    )


def _import_config(config_path: Path, database_url: str) -> None:
    """One-time import of a legacy embyx-monitor config.toml into the config store."""
    raw = tomllib.loads(config_path.read_text(encoding='utf-8'))
    sections: dict[str, dict] = {}

    clouddrive = raw.get('clouddrive', {})
    if clouddrive:
        sections['clouddrive'] = {
            'address': str(clouddrive.get('address', '')),
            'api_token': str(clouddrive.get('api_token', '')),
            'task_dir_path': str(clouddrive.get('task_dir_path', '')),
        }
    freshrss = raw.get('freshrss', {})
    if freshrss:
        sections['freshrss'] = {
            'url': str(freshrss.get('freshrss_url', '')),
            'api_key': str(freshrss.get('freshrss_api_key', '')),
            'proxy': str(freshrss.get('proxy', '')),
        }
    avid = raw.get('avid', {})
    if avid:
        sections['avid'] = {
            'id_exceptions': [str(value) for value in avid.get('get_id_exceptions', [])],
            'ignored_id_patterns': [str(value) for value in avid.get('ignored_id_pattern', [])],
        }
    archive = raw.get('archive', {})
    if archive:
        sections['archive'] = {
            'src_dir': str(archive.get('src_dir', '')),
            'dst_dir': str(archive.get('dst_dir', '')),
            'mapping': {str(key): str(value) for key, value in archive.get('mapping', {}).items()},
            'priority_mapping': {str(key): str(value) for key, value in archive.get('priority_mapping', {}).items()},
            'min_size_mb': int(archive.get('min_size', 0)),
            'brand_mapping': {
                str(key): [str(brand) for brand in values] for key, values in archive.get('brand_mapping', {}).items()
            },
        }
    mapping = raw.get('mapping', {})
    if mapping:
        sections['mapping'] = {
            'src_dir': str(mapping.get('src_dir', '')),
            'dst_dir': str(mapping.get('dst_dir', '')),
        }

    async def push() -> None:
        database = Database(database_url)
        store = ConfigStore(database)
        try:
            await store.load()
            for section, values in sections.items():
                _, version = await store.update(section, values)
                print(f'imported [{section}] -> version {version}')  # noqa: T201
        finally:
            await database.aclose()

    if not sections:
        print('no recognized sections found in the config file')  # noqa: T201
        return
    asyncio.run(push())


def main() -> None:
    parser = argparse.ArgumentParser(prog='embyx-manager')
    subparsers = parser.add_subparsers(dest='command')
    subparsers.add_parser('serve', help='run the web application (default)')
    import_parser = subparsers.add_parser(
        'import-config',
        help='import a legacy embyx-monitor config.toml into the database config store',
    )
    import_parser.add_argument('config_path', type=Path, help='path to the legacy config.toml')
    import_parser.add_argument(
        '--database-url',
        default=None,
        help='PostgreSQL DSN (defaults to EMBYX_MANAGER_DATABASE_URL)',
    )
    args = parser.parse_args()

    if args.command == 'import-config':
        database_url = args.database_url or os.environ.get('EMBYX_MANAGER_DATABASE_URL')
        if not database_url:
            print('EMBYX_MANAGER_DATABASE_URL or --database-url is required', file=sys.stderr)  # noqa: T201
            raise SystemExit(2)
        _import_config(args.config_path, database_url)
        return
    _serve()


if __name__ == '__main__':
    main()
