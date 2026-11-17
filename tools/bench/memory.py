"""Memory of the Python and the Rust server under the same load, everything wired to fakes (WP16).

One run starts one server (the baseline's `python -m embyx_manager serve`, or a Rust
`embyx-manager serve` binary) on a scratch database, wires every section to local fakes and
measures the process while it works:

    idle         45 s after the sections are saved (CloudDrive, Emby, archive, mapping,
                 fill actor, merge), nothing requested yet
    requests     after 350 requests over health, config, monitor, playlists, fill actor, merge
                 and the SPA root (Checkpoint B's mix, plus the merge tab)
    mapping      a mapping run mirroring `--titles` flat .strm files, each with an .nfo and a poster
    archive      two archive runs (the reconcile scan files a folder on the pass after the one that
                 saw it settle) over `--titles` downloads waiting in the intake route
    fill_actor   a fill-actor scan job for one actor, as far as it gets offline: the catalog asks
                 AVBase first, and with AVBase unreachable both builds end the actor there (only
                 an answer from AVBase lets the catalog fall back to JavBus, D29), so this phase
                 measures the job machinery, not a library scan
    chunk        a home-worker chunk upload of `--chunk-mib` MiB to the transcode listener

For each phase it records the peak RSS while the phase ran (the kernel's high-water mark, reset
through `clear_refs` when the phase starts, and a 50 ms sample) and the RSS once it ended, with
PSS and anonymous memory (`/proc/<pid>/smaps_rollup`) and the thread count.

Nothing leaves the machine: CloudDrive is the scenario fake (gRPC on a local port), Emby the
scenario HTTP fake, and every outbound request goes to a local proxy (`HTTPS_PROXY`, which httpx,
curl_cffi, reqwest and wreq all read) that refuses it. A site the run could need would have to be
served under a certificate the client trusts, and neither curl_cffi nor wreq (AVBase) reads
`SSL_CERT_FILE`. The merge task the chunk goes to is a synthetic row in the scratch database.

    uv run python tools/bench/memory.py --side python --out py.json
    uv run python tools/bench/memory.py --side rust --binary target/release/embyx-manager --out rust.json

Needs EMBYX_MANAGER_TEST_DATABASE_URL (a disposable PostgreSQL server; a database is created and
dropped). Linux only (`/proc`). Never runs in CI.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Self

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

API_TOKEN = 'bench-api-token'  # noqa: S105 - a throwaway token for a local server
TRANSCODE_TOKEN = 'bench-transcode-token'  # noqa: S105
BRAND = 'BENCH'
SAMPLE_SECONDS = 0.05
HTTP_OK = 200
HTTP_ACCEPTED = 202


# ---------------------------------------------------------------- measuring


def memory(pid: int) -> dict[str, int]:
    """VmRSS, VmHWM, Pss and Anonymous of a process, in KiB."""
    out: dict[str, int] = {}
    with contextlib.suppress(OSError):
        for line in Path(f'/proc/{pid}/status').read_text().splitlines():
            key, _, value = line.partition(':')
            if key in {'VmRSS', 'VmHWM', 'Threads'}:
                out[key] = int(value.split()[0])
    with contextlib.suppress(OSError):
        for line in Path(f'/proc/{pid}/smaps_rollup').read_text().splitlines():
            key, _, value = line.partition(':')
            if key in {'Pss', 'Anonymous'}:
                out[key] = int(value.split()[0])
    return out


class Sampler:
    """Polls VmRSS while a phase runs and keeps its peak."""

    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.peak = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop.is_set():
            self.peak = max(self.peak, memory(self.pid).get('VmRSS', 0))
            self._stop.wait(SAMPLE_SECONDS)

    def __enter__(self) -> Self:
        # Writing 5 to clear_refs resets VmHWM, so the phase's own high-water mark is exact
        # even for a spike shorter than the sampling interval.
        with contextlib.suppress(OSError):
            Path(f'/proc/{self.pid}/clear_refs').write_text('5')
        self.peak = memory(self.pid).get('VmRSS', 0)
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join()
        self.peak = max(self.peak, memory(self.pid).get('VmHWM', 0))


# ---------------------------------------------------------------- the offline proxy


class OfflineProxy:
    """An HTTP proxy on 127.0.0.1 that refuses every request, so no client reaches the network."""

    def __init__(self) -> None:
        self.hosts: dict[str, int] = {}
        self.loop = asyncio.new_event_loop()
        self.port = 0
        ready = threading.Event()
        threading.Thread(target=self._serve, args=(ready,), daemon=True).start()
        ready.wait()

    def _serve(self, ready: threading.Event) -> None:
        asyncio.set_event_loop(self.loop)
        server = self.loop.run_until_complete(asyncio.start_server(self._client, '127.0.0.1', 0))
        self.port = server.sockets[0].getsockname()[1]
        ready.set()
        self.loop.run_forever()

    @property
    def url(self) -> str:
        return f'http://127.0.0.1:{self.port}'

    async def _client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await reader.readuntil(b'\r\n\r\n')
            target = head.split(b'\r\n', 1)[0].decode('latin-1').split(' ')[1]
            host = target.split('://')[-1].split('/')[0].rsplit(':', 1)[0]
            self.hosts[host] = self.hosts.get(host, 0) + 1
            writer.write(b'HTTP/1.1 403 Forbidden\r\ncontent-length: 0\r\nconnection: close\r\n\r\n')
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, ValueError, IndexError, OSError):
            pass
        finally:
            writer.close()


# ---------------------------------------------------------------- trees and fakes


def sparse(path: Path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as handle:
        handle.truncate(size)


def build_trees(work: Path, titles: int) -> dict[str, Path]:
    paths = {
        name: work / name for name in ('mount', 'strm', 'downloads', 'library', 'actors', 'extra', 'move_in', 'upload')
    }
    for path in paths.values():
        path.mkdir(parents=True)
    # Mapping: flat `<brand>/<avid>.strm` files with an .nfo and a poster beside each, mirrored
    # into `<brand>/<avid>/<avid>.strm` with the sidecars.
    for number in range(1, titles + 1):
        brand = paths['mount'] / f'{BRAND}{number % 20:02d}'
        brand.mkdir(exist_ok=True)
        avid = f'{BRAND}-{number:05d}'
        (brand / f'{avid}.strm').write_text(f'/mnt/cd2/library/{avid}/{avid}.mp4\n')
        (brand / f'{avid}.nfo').write_text(f'<movie><title>{avid}</title></movie>\n')
        (brand / f'{avid}-poster.jpg').write_bytes(b'\xff\xd8\xff' + bytes(4096))
    # Archive: downloads waiting in the intake route, one folder each.
    for number in range(1, titles + 1):
        avid = f'ARCH-{number:05d}'
        sparse(paths['downloads'] / 'intake' / avid / f'{avid}.mp4', 1024**3)
    # Fill actor: roots that pass the readiness check.
    for root in ('actors', 'extra', 'move_in'):
        (paths[root] / '.embyx-root').write_text('')
    return paths


def sections(paths: dict[str, Path], clouddrive: str, emby: str) -> dict[str, dict[str, Any]]:
    return {
        'clouddrive': {'address': clouddrive, 'api_token': 'bench', 'secure': False},
        'emby': {'address': emby, 'api_key': 'bench'},
        'mapping': {'enabled': False, 'src_dir': str(paths['mount']), 'dst_dir': str(paths['strm'])},
        'archive': {
            'enabled': False,
            'src_dir': str(paths['downloads']),
            'dst_dir': str(paths['library']),
            'mapping': {'intake': 'library'},
        },
        'fill_actor': {
            'actor_root': str(paths['actors']),
            'additional_roots': [str(paths['extra'])],
            'move_in_root': str(paths['move_in']),
        },
        'merge': {'work_dir': str(paths['upload']), 'cloud_work_dir': '/upload', 'cloud_staging_dir': '/115/upload',
                  'cloud_mount_prefix': str(paths['mount'])},
    }  # fmt: skip


# ---------------------------------------------------------------- talking to the server


class Server:
    def __init__(self, port: int, transcode_port: int) -> None:
        self.base = f'http://127.0.0.1:{port}'
        self.transcode = f'http://127.0.0.1:{transcode_port}'

    def call(
        self,
        method: str,
        path: str,
        body: Any = None,
        *,
        base: str | None = None,
        token: str = API_TOKEN,
    ) -> tuple[int, Any]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f'{base or self.base}{path}', data=data, method=method)  # noqa: S310
        request.add_header('authorization', f'Bearer {token}')
        if data is not None:
            request.add_header('content-type', 'application/json')
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(request, timeout=600) as response:
                raw = response.read()
                status = response.status
        except urllib.error.HTTPError as error:
            raw, status = error.read(), error.code
        try:
            return status, json.loads(raw) if raw else None
        except ValueError:
            return status, raw[:200].decode('utf-8', 'replace')


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        return probe.getsockname()[1]


def wait_healthy(server: Server, process: subprocess.Popen[bytes], seconds: float = 120) -> float:
    started = time.monotonic()
    while time.monotonic() - started < seconds:
        if process.poll() is not None:
            msg = f'server exited with {process.returncode}'
            raise RuntimeError(msg)
        with contextlib.suppress(OSError):
            status, _ = server.call('GET', '/api/health')
            if status == HTTP_OK:
                return time.monotonic() - started
        time.sleep(0.2)
    msg = 'server never became healthy'
    raise RuntimeError(msg)


def wait_run(server: Server, run_id: str, seconds: float = 1800) -> dict[str, Any]:
    started = time.monotonic()
    while time.monotonic() - started < seconds:
        _, run = server.call('GET', f'/api/monitor/runs/{run_id}')
        if isinstance(run, dict) and run.get('state') not in {'running', 'queued', None}:
            return run
        time.sleep(0.2)
    msg = f'run {run_id} did not end'
    raise RuntimeError(msg)


def wait_plan(server: Server, plan_id: str, seconds: float = 1800) -> dict[str, Any]:
    started = time.monotonic()
    while time.monotonic() - started < seconds:
        _, envelope = server.call('GET', f'/api/fill-actor/plans/{plan_id}')
        job = envelope.get('job', {}) if isinstance(envelope, dict) else {}
        if job.get('state') not in {'queued', 'running', None}:
            return envelope
        time.sleep(0.2)
    msg = f'plan {plan_id} did not end'
    raise RuntimeError(msg)


def request_mix(server: Server) -> dict[int, int]:
    paths = [
        '/api/health', '/api/config', '/api/config/archive', '/api/config/mapping', '/api/monitor/status',
        '/api/monitor/runs', '/api/monitor/acquisitions', '/api/monitor/subscriptions', '/api/monitor/tracker',
        '/api/playlists', '/api/config/fill_actor', '/api/merge/tasks', '/api/merge/titles', '/',
    ]  # fmt: skip
    statuses: dict[int, int] = {}
    for index in range(350):
        status, _ = server.call('GET', paths[index % len(paths)])
        statuses[status] = statuses.get(status, 0) + 1
    return statuses


async def _seed_merge_task(database_url: str) -> int:
    import asyncpg  # noqa: PLC0415

    connection = await asyncpg.connect(database_url)
    try:
        return await connection.fetchval(
            """
            INSERT INTO merge_tasks (avid, source, library_dir, brand, parts_json, merged_name, state, transcoder,
                                     created_at, updated_at, state_changed_at)
            VALUES ('BENCH-99999', 'bench', '/library/BENCH', 'BENCH', $1, 'BENCH-99999.mkv', 'transcoding', 'home',
                    now(), now(), now())
            RETURNING id
            """,
            json.dumps(['/upload/BENCH-99999/BENCH-99999-cd1.mp4', '/upload/BENCH-99999/BENCH-99999-cd2.mp4']),
        )
    finally:
        await connection.close()


def upload_chunk(server: Server, task_id: int, mib: int) -> int:
    """PUT one chunk of `mib` MiB, streamed; the answer's status."""
    block = bytes(range(256)) * 4096  # 1 MiB
    digest = hashlib.sha256()
    for _ in range(mib):
        digest.update(block)
    host, port = server.transcode.removeprefix('http://').split(':')
    with socket.create_connection((host, int(port)), timeout=600) as connection:
        connection.sendall(
            f'PUT /api/transcode/tasks/{task_id}/parts/0/chunks/0 HTTP/1.1\r\nhost: {host}:{port}\r\n'
            f'authorization: Bearer {TRANSCODE_TOKEN}\r\ncontent-type: application/octet-stream\r\n'
            f'content-length: {mib * 1024 * 1024}\r\nx-chunk-start: 0\r\nx-chunk-duration: 60\r\n'
            f'x-chunk-frames: 1440\r\nx-chunk-sha256: {digest.hexdigest()}\r\nconnection: close\r\n\r\n'.encode()
        )
        for _ in range(mib):
            connection.sendall(block)
        answer = b''
        while chunk := connection.recv(65536):
            answer += chunk
    return int(answer.split(b' ', 2)[1])


# ---------------------------------------------------------------- one run


def run(args: argparse.Namespace) -> dict[str, Any]:  # noqa: PLR0915 - one fixed sequence
    import asyncpg  # noqa: PLC0415
    from tools.scenarios.fakes.clouddrive import FakeCloudDriveServer  # noqa: PLC0415
    from tools.scenarios.fakes.http import FakeHttpServer  # noqa: PLC0415

    admin = os.environ['EMBYX_MANAGER_TEST_DATABASE_URL']
    name = f'embyx_bench_{os.getpid()}'
    database_url = admin.rsplit('/', 1)[0] + f'/{name}'

    async def admin_sql(statement: str) -> None:
        connection = await asyncpg.connect(admin)
        try:
            await connection.execute(statement)
        finally:
            await connection.close()

    work = Path(tempfile.mkdtemp(prefix='embyx-bench-'))
    asyncio.run(admin_sql(f'CREATE DATABASE "{name}"'))
    process: subprocess.Popen[bytes] | None = None
    result: dict[str, Any] = {'side': args.side, 'binary': args.binary, 'titles': args.titles,
                              'chunk_mib': args.chunk_mib, 'phases': {}}  # fmt: skip
    try:
        paths = build_trees(work, args.titles)
        proxy = OfflineProxy()
        emby = FakeHttpServer('emby', {'routes': [
            {'method': 'GET', 'path': '/System/Info', 'body': {'ServerName': 'bench', 'Version': '4.8'}, 'times': -1},
        ]}, lambda _: None)  # fmt: skip
        emby._thread.start()  # noqa: SLF001 - the scenario harness starts it the same way
        with FakeCloudDriveServer({'directories': ['/upload', '/115', '/115/upload']}, lambda _: None) as cloud:
            port, transcode_port = free_port(), free_port()
            env = {
                'PATH': os.environ['PATH'], 'HOME': str(work), 'TMPDIR': str(work), 'LANG': 'C.UTF-8',
                'EMBYX_MANAGER_DATABASE_URL': database_url, 'EMBYX_MANAGER_HOST': '127.0.0.1',
                'EMBYX_MANAGER_PORT': str(port), 'EMBYX_MANAGER_TRANSCODE_PORT': str(transcode_port),
                'EMBYX_MANAGER_API_TOKEN': API_TOKEN, 'EMBYX_MANAGER_TRANSCODE_TOKEN': TRANSCODE_TOKEN,
                'HTTPS_PROXY': proxy.url, 'HTTP_PROXY': proxy.url, 'https_proxy': proxy.url, 'http_proxy': proxy.url,
                'ALL_PROXY': proxy.url, 'NO_PROXY': '127.0.0.1,localhost', 'no_proxy': '127.0.0.1,localhost',
            }  # fmt: skip
            if args.side == 'python':
                command = [str(ROOT / '.venv/bin/python'), '-m', 'embyx_manager', 'serve']
            else:
                command = [args.binary, 'serve']
            log = (work / 'server.log').open('wb')
            process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT, cwd=work)
            server = Server(port, transcode_port)
            result['startup_seconds'] = round(wait_healthy(server, process), 2)
            result['phases']['started'] = memory(process.pid)
            for section, values in sections(paths, cloud.address, emby.url).items():
                status, body = server.call('PUT', f'/api/config/{section}', {'values': values})
                if status != HTTP_OK:
                    msg = f'{section}: {status} {body}'
                    raise RuntimeError(msg)
            time.sleep(args.idle_seconds)
            result['phases']['idle'] = memory(process.pid)

            with Sampler(process.pid) as sampler:
                statuses = request_mix(server)
            result['phases']['requests'] = {**memory(process.pid), 'peak': sampler.peak, 'statuses': statuses}

            # The archive's reconcile scan files a folder on the pass after the one that saw it settle.
            for pipeline, passes in (('mapping', 1), ('archive', 2)):
                runs = []
                with Sampler(process.pid) as sampler:
                    started = time.monotonic()
                    for _ in range(passes):
                        status, body = server.call('POST', f'/api/monitor/{pipeline}/trigger')
                        outcome = (
                            wait_run(server, body['run_id'])
                            if status == HTTP_ACCEPTED
                            else {'status': status, 'body': body}
                        )
                        runs.append({key: outcome.get(key) for key in ('state', 'stats', 'status') if key in outcome})
                    seconds = time.monotonic() - started
                result['phases'][pipeline] = {
                    **memory(process.pid), 'peak': sampler.peak, 'seconds': round(seconds, 2), 'runs': runs,
                }  # fmt: skip
            result['trees'] = {
                'strm_files': sum(1 for _ in paths['strm'].rglob('*.strm')),
                'library_videos': sum(1 for _ in paths['library'].rglob('*.mp4')),
                'downloads_left': sum(1 for _ in (paths['downloads'] / 'intake').iterdir()),
            }

            with Sampler(process.pid) as sampler:
                started = time.monotonic()
                status, body = server.call('POST', '/api/fill-actor/plans', {'actor_ids': ['bench-actor']})
                plan_id = (body or {}).get('job', {}).get('plan_id') or (body or {}).get('job', {}).get('job_id')
                envelope = wait_plan(server, plan_id) if status == HTTP_ACCEPTED else {'status': status, 'body': body}
                seconds = time.monotonic() - started
            plan = envelope.get('plan') or {}
            result['phases']['fill_actor'] = {
                **memory(process.pid), 'peak': sampler.peak, 'seconds': round(seconds, 2),
                'job_state': envelope.get('job', {}).get('state'), 'videos': len(plan.get('videos', [])),
            }  # fmt: skip

            task_id = asyncio.run(_seed_merge_task(database_url))
            with Sampler(process.pid) as sampler:
                started = time.monotonic()
                status = upload_chunk(server, task_id, args.chunk_mib)
                seconds = time.monotonic() - started
            result['phases']['chunk'] = {
                **memory(process.pid), 'peak': sampler.peak, 'seconds': round(seconds, 2), 'status': status,
            }  # fmt: skip
            result['phases']['end'] = memory(process.pid)
            result['proxy_hosts'] = proxy.hosts
    finally:
        if process is not None:
            process.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=30)
            if process.poll() is None:
                process.kill()
        asyncio.run(admin_sql(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        if args.keep:
            result['work'] = str(work)
        else:
            shutil.rmtree(work, ignore_errors=True)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--side', choices=['python', 'rust'], required=True)
    parser.add_argument('--binary', help='the Rust embyx-manager binary (--side rust)')
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--titles', type=int, default=2000)
    parser.add_argument('--chunk-mib', type=int, default=1024)
    parser.add_argument('--idle-seconds', type=float, default=45.0)
    parser.add_argument('--keep', action='store_true', help='keep the work directory (server log)')
    args = parser.parse_args()
    if args.side == 'rust' and not args.binary:
        parser.error('--side rust needs --binary')
    result = run(args)
    args.out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    for phase, values in result['phases'].items():
        print(phase, {key: values[key] for key in ('VmRSS', 'peak', 'seconds') if key in values})
    return 0


if __name__ == '__main__':
    sys.exit(main())
