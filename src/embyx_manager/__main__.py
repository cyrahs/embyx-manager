import argparse
import asyncio
import os
import sys
import tomllib
from pathlib import Path


def _serve() -> None:
    import uvicorn  # noqa: PLC0415 - the worker subcommands run without the web stack

    from embyx_manager.bootstrap import build_app  # noqa: PLC0415
    from embyx_manager.settings import Settings  # noqa: PLC0415

    settings = Settings.from_env()
    app = build_app(settings)
    transcode_app = getattr(app.state, 'transcode_app', None)
    if settings.transcode_port is None or transcode_app is None:
        uvicorn.run(app, host=settings.host, port=settings.port, proxy_headers=False, server_header=False)
        return
    # The home transcode worker's endpoints listen apart, on a port only its gateway reaches.
    servers = [
        uvicorn.Server(
            uvicorn.Config(app, host=settings.host, port=settings.port, proxy_headers=False, server_header=False)
        ),
        uvicorn.Server(
            uvicorn.Config(
                transcode_app,
                host=settings.host,
                port=settings.transcode_port,
                proxy_headers=False,
                server_header=False,
                lifespan='off',
            )
        ),
    ]

    async def serve_both() -> None:
        await asyncio.gather(*(server.serve() for server in servers))

    asyncio.run(serve_both())


def _import_config(config_path: Path, database_url: str) -> None:
    """One-time import of a legacy embyx-monitor config.toml into the config store."""
    raw = tomllib.loads(config_path.read_text(encoding='utf-8'))
    sections: dict[str, dict] = {}

    clouddrive = raw.get('clouddrive', {})
    if clouddrive:
        sections['clouddrive'] = {
            'address': str(clouddrive.get('address', '')),
            'api_token': str(clouddrive.get('api_token', '')),
        }
        # The legacy config has one offline directory and no notion of categories,
        # while RSS now ingests categories that each name a directory. Recreate
        # the two the pipeline used to have, both pointing at that directory, so
        # an imported deployment behaves as it did before.
        task_dir_path = str(clouddrive.get('task_dir_path', '')).strip()
        if task_dir_path:
            sections['rss'] = {
                'categories': [
                    {'label': 'Actor', 'task_dir_path': task_dir_path},
                    {'label': 'Rank', 'task_dir_path': task_dir_path},
                ],
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

    from embyx_manager.config import ConfigStore  # noqa: PLC0415
    from embyx_manager.db import Database  # noqa: PLC0415

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
    worker_parser = subparsers.add_parser(
        'merge-worker',
        help="merge one title's parts into a single file (run by the merge tab's Kubernetes Job)",
    )
    worker_parser.add_argument('--output', required=True, help='the merged file to write')
    worker_parser.add_argument('--status', required=True, help='the JSON status file the app polls')
    worker_parser.add_argument('--reserve-gib', type=float, default=20.0, help='space to leave free on the volume')
    worker_parser.add_argument(
        '--home-prepare', default=None, help='probe the parts and copy their audio here for the home transcode worker'
    )
    worker_parser.add_argument(
        '--home-assemble', default=None, help="merge the home transcode worker's chunks and the audio kept here"
    )
    worker_parser.add_argument('parts', nargs='*', help='the parts, in order')
    home_parser = subparsers.add_parser(
        'transcode-worker',
        help="re-encode merges too big for 115 on this machine's NVIDIA GPU (the home transcode worker)",
    )
    from embyx_manager.merge import home_worker  # noqa: PLC0415

    home_worker.add_arguments(home_parser)
    args = parser.parse_args()

    if args.command == 'transcode-worker':
        raise SystemExit(home_worker.main(args))
    if args.command == 'merge-worker':
        from embyx_manager.merge import worker  # noqa: PLC0415

        raise SystemExit(
            worker.run(
                args.parts,
                output=args.output,
                status_path=args.status,
                reserve_gib=args.reserve_gib,
                home_prepare=args.home_prepare,
                home_assemble=args.home_assemble,
            )
        )
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
