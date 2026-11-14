"""The home transcode worker (`merge/home_worker.py`, WP15), its pieces called one by one.

Every case calls the baseline's own code with the outside world replaced: `subprocess.run`
answers with canned ffprobe output, httpx talks to a `MockTransport` that records each
request and answers as told, `time` is a fake clock, and paths live under a temporary
directory written as `<root>`. The Rust worker (`embyx-transcode-worker`) runs the same
cases and must give the same argument lists, request bytes, sidecar bytes, console lines and
errors (PLAN.md, cross-version contract 5: home worker protocol version 1).
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import socket
import subprocess
import tempfile
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from embyx_manager.merge import home_worker

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

from embyx_manager.merge.home_worker import (
    LocalChunk,
    PartState,
    RateLimiter,
    RunClock,
    Server,
    ServerError,
    TaskRun,
    WorkerConfig,
)

ROOT = '<root>'
#: `WorkerConfig.name` defaults to `socket.gethostname()`, bound when the class was made, so
#: it cannot be patched: the machine's name is written as this instead.
HOST = '<host>'


def outcome(function: Callable[..., Any], *args: Any, **kwargs: Any) -> dict[str, Any]:
    try:
        return {'value': function(*args, **kwargs)}
    except SystemExit as exc:
        return {'exit': str(exc.code)}
    except Exception as exc:  # noqa: BLE001 - the class and text are the golden
        return {'error': {'type': type(exc).__name__, 'message': str(exc)}}


def rooted(text: str, root: Path) -> str:
    return text.replace(str(root), ROOT)


@contextlib.contextmanager
def patched(target: Any, name: str, value: Any) -> Iterator[None]:
    original = getattr(target, name)
    setattr(target, name, value)
    try:
        yield
    finally:
        setattr(target, name, original)


# -- ffmpeg command lines ------------------------------------------------------------------

NVENC = {
    'codec': 'av1_nvenc',
    'preset': 'p7',
    'tune': 'uhq',
    'multipass': 'fullres',
    'lookahead': 32,
    'spatial_aq': True,
    'bit_depth': 10,
    'gop_seconds': 5,
}

ENCODERS: list[dict[str, Any]] = [
    NVENC,
    {'codec': 'av1_nvenc'},
    {**NVENC, 'bit_depth': 8, 'multipass': '', 'lookahead': 0, 'spatial_aq': False},
    {**NVENC, 'bit_depth': '10', 'gop_seconds': '2.5', 'extra': ['-highbitdepth', 1, True, 2.5]},
    {**NVENC, 'tune': 'hq', 'multipass': 'qres', 'lookahead': '20', 'extra': '-x'},
    {'codec': 'hevc_nvenc', 'bit_depth': 10.9, 'preset': 7, 'extra': {'-a': 1, '-b': 2}},
    {'codec': 'libx264', 'extra': ['-preset', 'veryfast', '-bf', '3', '-vf', 'realtime=speed=4']},
    {'codec': 'libsvtav1', 'gop_seconds': 0.01},
    {'codec': 'libx265', 'gop_seconds': 0.5, 'extra': []},
    {'codec': 'libx264', 'gop_seconds': 2.5},
    {'codec': 'libx264', 'gop_seconds': 1.5},
    {'codec': None},
    {'codec': 5},
    {'codec': 'av1_nvenc', 'bit_depth': 'ten'},
    {'codec': 'libx264', 'gop_seconds': 'x'},
    {'codec': 'libx264', 'gop_seconds': None},
    {'codec': 'libx264', 'extra': 7},
    {'codec': 'libx264', 'gop_seconds': float('nan')},
    {'preset': 'p7'},
    {},
]


def _commands() -> list[dict[str, Any]]:
    root = Path('/cache')
    cases: list[dict[str, Any]] = []
    for encoder in ENCODERS:
        cases.append({'kind': 'decoder', 'encoder': encoder, **outcome(home_worker.decoder_args, encoder)})
        for bits, fps in ((6_140_000, 59.94), (300_000, 30000 / 1001), (1, 0.1)):
            cases.append(
                {
                    'kind': 'encoder',
                    'encoder': encoder,
                    'video_bits': bits,
                    'fps': fps,
                    **outcome(home_worker.encoder_args, encoder, bits, fps),
                }
            )
    runs = [
        ('http://10.117.200.1:8080/api/transcode/tasks/7/parts/0', '', 0.0, 60, 0),
        ('http://10.117.200.1:8080/api/transcode/tasks/7/parts/1', 'secret-token', 120.4833, 60, 3),
        ('https://example.test/api/transcode/tasks/7/parts/2', 'tok', 0.125, 2, 12),
        ('/library/part.mp4', '', 1e-7, 2, 0),
        ('/library/part.mp4', 'tok', 0.0000005, 2, 1),
        ('httpfile.mp4', '', 2.0000005, 1, 99999),
        ('file.mp4', '', -1.0, 60, 100000),
    ]
    for url, token, seek, chunk_seconds, first_index in runs:
        for encoder in (NVENC, ENCODERS[6], {'codec': None}):
            config = WorkerConfig(server='http://cluster', token=token, ffmpeg='C:/ffmpeg/bin/ffmpeg.exe')
            directory = root / '7' / 'cd01'
            listing = directory / f'list-{first_index:05d}.csv'
            cases.append(
                {
                    'kind': 'encode',
                    'url': url,
                    'token': token,
                    'ffmpeg': config.ffmpeg,
                    'encoder': encoder,
                    'video_bits': 6_140_000,
                    'fps': 59.94,
                    'seek': seek,
                    'chunk_seconds': chunk_seconds,
                    'first_index': first_index,
                    'directory': str(directory),
                    'listing': str(listing),
                    **outcome(
                        home_worker.encode_command,
                        config,
                        url=url,
                        encoder=encoder,
                        video_bits=6_140_000,
                        fps=59.94,
                        seek=seek,
                        chunk_seconds=chunk_seconds,
                        first_index=first_index,
                        directory=directory,
                        listing=listing,
                    ),
                }
            )
    cases.extend(
        {'kind': 'uses_nvenc', 'encoder': {'codec': codec}, 'value': home_worker.uses_nvenc({'codec': codec})}
        for codec in ('av1_nvenc', 'h264_nvenc', 'AV1_NVENC', 'nvenc', '', 'av1_nvenc ', 12)
    )
    return cases


# -- ffprobe ---------------------------------------------------------------------------------


class FakeRun:
    """`subprocess.run` answering with one canned result and recording the call."""

    def __init__(self, returncode: int, stdout: str, stderr: str, *, timeout: bool = False) -> None:
        self.returncode, self.stdout, self.stderr, self.timeout = returncode, stdout, stderr, timeout
        self.calls: list[dict[str, Any]] = []

    def __call__(self, argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append({'argv': argv, 'timeout': kwargs.get('timeout')})
        if self.timeout:
            raise subprocess.TimeoutExpired(argv, kwargs.get('timeout') or 0)
        return subprocess.CompletedProcess(argv, self.returncode, self.stdout, self.stderr)


PTS_ANSWERS = [
    (0, '0.083333,\n0.000000,\n0.041667,\n', ''),
    (0, '0.166833\n0.100100\n', ''),
    (0, 'N/A,\n1.5,\n', ''),
    (0, '', ''),
    (0, 'N/A\n', 'no packets'),
    (1, '0.1\n', 'Invalid data found when processing input'),
    (1, '', '  x' * 120 + '\n'),
    (0, '  2.5 ,\r\n-0.5\r\n', ''),
    (0, '1e-3,\n', ''),
    (0, '٣,\n', ''),
]
COUNT_ANSWERS = [
    (0, '120\n', ''),
    (0, '120,\n', ''),
    (0, ' 61 \n', ''),
    (0, 'N/A\n', 'no frames'),
    (0, '', ''),
    (1, '120\n', 'broken'),
    (0, '-1\n', ''),
    (0, '1.5\n', ''),
    (0, '٣\n', ''),
    (0, '²\n', ''),
]
FPS_ANSWERS = [
    (0, json.dumps({'programs': [], 'streams': [{'avg_frame_rate': '30000/1001', 'r_frame_rate': '30000/1001'}]}), ''),
    (0, json.dumps({'streams': [{'avg_frame_rate': '0/0', 'r_frame_rate': '60/1'}]}), ''),
    (0, json.dumps({'streams': [{'avg_frame_rate': '25', 'r_frame_rate': '0/0'}]}), ''),
    (0, json.dumps({'streams': [{'avg_frame_rate': '0/0', 'r_frame_rate': '0/0'}]}), 'no rate'),
    (0, json.dumps({'streams': []}), ''),
    (0, json.dumps({}), ''),
    (0, '', 'Connection refused'),
    (1, '', 'Server returned 401 Unauthorized'),
    (0, 'not json', ''),
    (0, json.dumps({'streams': [{'avg_frame_rate': 'x/y', 'r_frame_rate': '24000/1001'}]}), ''),
    (0, json.dumps({'streams': [{'avg_frame_rate': '-30/1', 'r_frame_rate': '1/0'}]}), ''),
    (0, json.dumps({'streams': [{'avg_frame_rate': 30, 'r_frame_rate': None}]}), ''),
]


def _probes() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    path = Path('/cache/7/cd01/chunk-00003.mkv')
    for function, answers in (('first_pts', PTS_ANSWERS), ('count_frames', COUNT_ANSWERS)):
        for returncode, stdout, stderr in answers:
            fake = FakeRun(returncode, stdout, stderr)
            with patched(home_worker.subprocess, 'run', fake):
                result = outcome(getattr(home_worker, function), path, 'ffprobe')
            cases.append(
                {
                    'kind': function,
                    'answer': {'returncode': returncode, 'stdout': stdout, 'stderr': stderr},
                    'calls': fake.calls,
                    **result,
                }
            )
    for token in ('', 'secret-token'):
        config = WorkerConfig(server='http://cluster', token=token, ffprobe='/usr/bin/ffprobe')
        for returncode, stdout, stderr in FPS_ANSWERS:
            fake = FakeRun(returncode, stdout, stderr)
            with patched(home_worker.subprocess, 'run', fake):
                result = outcome(home_worker.probe_fps, 'http://cluster/api/transcode/tasks/7/parts/0', config)
            cases.append(
                {
                    'kind': 'probe_fps',
                    'token': token,
                    'answer': {'returncode': returncode, 'stdout': stdout, 'stderr': stderr},
                    'calls': fake.calls,
                    **result,
                }
            )
    return cases


# -- the cache -------------------------------------------------------------------------------


def _listing(directory: Path) -> list[dict[str, Any]]:
    if not directory.is_dir():
        return []
    return [
        {'name': path.name, 'text': path.read_text(encoding='utf-8', errors='replace')}
        for path in sorted(directory.iterdir())
    ]


CHUNKS = [
    {'part_index': 0, 'index': 0, 'start': 0.0, 'duration': 60.06, 'frames': 3600, 'bytes': 3, 'sha256': 'a' * 64},
    {
        'part_index': 2,
        'index': 17,
        'start': 1021.0210000001,
        'duration': 0.1 + 0.2,
        'frames': 18,
        'bytes': 1,
        'sha256': 'b' * 64,
    },
    {'part_index': 0, 'index': 99999, 'start': 1e16, 'duration': 1e-7, 'frames': 1, 'bytes': 0, 'sha256': ''},
]

SIDECARS: list[tuple[str, str | None, int | None]] = [
    ('a valid sidecar', json.dumps({**CHUNKS[0], 'bytes': 3}), 3),
    ('the chunk is missing', json.dumps({**CHUNKS[0], 'bytes': 3}), None),
    ('the chunk has another size', json.dumps({**CHUNKS[0], 'bytes': 3}), 4),
    ('an extra key', json.dumps({**CHUNKS[0], 'bytes': 3, 'path': 'x'}), 3),
    ('a missing key', json.dumps({key: value for key, value in CHUNKS[0].items() if key != 'frames'}), 3),
    ('not JSON', '{"part_index": 0,', 3),
    ('a JSON list', '[1, 2]', 3),
    ('an int start', json.dumps({**CHUNKS[0], 'bytes': 3, 'start': 6}), 3),
    ('empty', '', 3),
]


def _cache() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        for number, fields in enumerate(CHUNKS):
            folder = root / f'save{number}'
            folder.mkdir()
            chunk = LocalChunk(**fields, path=folder / f'chunk-{fields["index"]:05d}.mkv')
            chunk.save()
            cases.append({'kind': 'save', 'chunk': fields, 'end': chunk.end, 'files': _listing(folder)})
        for number, (title, text, size) in enumerate(SIDECARS):
            folder = root / f'load{number}'
            folder.mkdir()
            sidecar = folder / 'chunk-00000.json'
            sidecar.write_text(text or '', encoding='utf-8')
            if size is not None:
                (folder / 'chunk-00000.mkv').write_bytes(b'x' * size)
            loaded = LocalChunk.load(sidecar)
            value = None
            if loaded is not None:
                value = {key: getattr(loaded, key) for key in CHUNKS[0]} | {'path': rooted(str(loaded.path), root)}
            cases.append({'kind': 'load', 'title': title, 'text': text, 'size': size, 'value': value})
        for number, scenario in enumerate(KEPT):
            folder = root / f'kept{number}'
            folder.mkdir()
            for entry in scenario['files']:
                if entry.get('dir'):
                    (folder / entry['name']).mkdir()
                elif 'chunk' in entry:
                    path = folder / entry['name']
                    path.write_bytes(b'x' * entry['chunk']['bytes'])
                    LocalChunk(**entry['chunk'], path=path).save()
                else:
                    (folder / entry['name']).write_text(entry.get('text', ''), encoding='utf-8')
            kept = home_worker.kept_chunks(folder, scenario['next_index'], scenario['resume_at'])
            cases.append(
                {
                    'kind': 'kept',
                    **scenario,
                    'kept': [chunk.index for chunk in kept],
                    'left': sorted(path.name for path in folder.iterdir()),
                }
            )
        missing = home_worker.kept_chunks(root / 'nowhere', 0, 0.0)
        cases.append({'kind': 'kept', 'title': 'no directory', 'kept': [chunk.index for chunk in missing]})
    return cases


def _chunk(index: int, start: float, duration: float = 2.0, part: int = 0) -> dict[str, Any]:
    return {
        'name': f'chunk-{index:05d}.mkv',
        'chunk': {
            'part_index': part,
            'index': index,
            'start': start,
            'duration': duration,
            'frames': 60,
            'bytes': 1,
            'sha256': '0' * 64,
        },
    }


KEPT: list[dict[str, Any]] = [
    {
        'title': 'a contiguous run, then a gap',
        'next_index': 3,
        'resume_at': 6.0,
        'files': [_chunk(3, 6.0), _chunk(4, 8.0), _chunk(6, 12.0)],
    },
    {
        'title': 'the first chunk starts within a quarter second',
        'next_index': 0,
        'resume_at': 0.0,
        'files': [_chunk(0, 0.25), _chunk(1, 2.25, 1.75), _chunk(2, 4.0)],
    },
    {
        'title': 'the first chunk starts too late',
        'next_index': 0,
        'resume_at': 0.0,
        'files': [_chunk(0, 0.2500001), _chunk(1, 2.25)],
    },
    {
        'title': 'the cluster is already past them',
        'next_index': 5,
        'resume_at': 10.0,
        'files': [_chunk(3, 6.0), _chunk(4, 8.0)],
    },
    {
        'title': 'leftovers: a half-written chunk, a listing, a temporary sidecar, a folder',
        'next_index': 0,
        'resume_at': 0.0,
        'files': [
            _chunk(0, 0.0),
            {'name': 'chunk-00001.mkv', 'text': 'partial'},
            {'name': 'list-00000.csv', 'text': 'chunk-00000.mkv,0.0,2.0\n'},
            {'name': '.chunk-00002.json.tmp', 'text': '{}'},
            {'name': 'notes.txt', 'text': 'x'},
            {'name': 'sub', 'dir': True},
        ],
    },
    {
        'title': 'a sidecar whose chunk is gone breaks the run',
        'next_index': 0,
        'resume_at': 0.0,
        'files': [
            _chunk(0, 0.0),
            {'name': 'chunk-00001.json', 'text': json.dumps(_chunk(1, 2.0)['chunk'])},
            _chunk(2, 4.0),
        ],
    },
    {'title': 'empty', 'next_index': 0, 'resume_at': 0.0, 'files': []},
]


# -- the settings ----------------------------------------------------------------------------

CONFIGS: list[dict[str, Any]] = [
    {
        'title': 'file and flags',
        'toml': 'server = "http://10.117.200.1:8080"\nupload_mbps = 25\n[encoder]\ntune = "hq"\n',
        'argv': ['--jobs', '3'],
    },
    {'title': 'flags only', 'toml': None, 'argv': ['--server', 'http://10.117.200.1:8080/', '--token', 't0k']},
    {'title': 'no server anywhere', 'toml': None, 'argv': []},
    {'title': 'an empty server', 'toml': 'server = ""\n', 'argv': []},
    {'title': 'a server that is not a string', 'toml': 'server = 8080\njobs = 0\n', 'argv': []},
    {'title': 'a missing file named on the command line', 'toml': None, 'argv': ['--config', 'missing.toml']},
    {
        'title': 'every key',
        'toml': (
            'server = "http://h:1///"\ntoken = "abc"\ncache_dir = "D:/cache"\nupload_mbps = "12.5"\njobs = 2.9\n'
            'max_queue_gb = 4\nffmpeg = "C:/ff/ffmpeg.exe"\nffprobe = "C:/ff/ffprobe.exe"\nname = "s7desk"\n'
            '[encoder]\ncodec = "libx264"\nextra = ["-preset", "fast"]\nbit_depth = 8\n'
        ),
        'argv': [],
    },
    {
        'title': 'flags win over the file',
        'toml': 'server = "http://a"\njobs = 5\nupload_mbps = 1\n',
        'argv': [
            '--server',
            'http://b',
            '--jobs',
            '-2',
            '--upload-mbps',
            '0',
            '--max-queue-gb',
            '1.5',
            '--ffmpeg',
            'ff',
            '--ffprobe',
            'fp',
            '--cache-dir',
            'cache',
        ],
    },
    {'title': 'a bad float', 'toml': 'server = "http://a"\nupload_mbps = "fast"\n', 'argv': []},
    {'title': 'a bad int', 'toml': 'server = "http://a"\njobs = "many"\n', 'argv': []},
    {'title': 'an empty cache_dir keeps the default', 'toml': 'server = "http://a"\ncache_dir = ""\n', 'argv': []},
    {'title': 'a cache_dir that is not a path', 'toml': 'server = "http://a"\ncache_dir = 5\n', 'argv': []},
    {'title': 'an encoder that is not a table', 'toml': 'server = "http://a"\nencoder = 5\n', 'argv': []},
    {'title': 'an encoder of pairs', 'toml': 'server = "http://a"\nencoder = [["codec", "libx264"]]\n', 'argv': []},
    {'title': 'a boolean token and name', 'toml': 'server = "http://a"\ntoken = true\nname = 1.5\n', 'argv': []},
    {'title': 'a list server', 'toml': 'server = ["http://a", 2]\n', 'argv': []},
    {'title': 'unknown keys are ignored', 'toml': 'server = "http://a"\ncolour = "red"\n[other]\nx = 1\n', 'argv': []},
    {'title': 'an infinite queue', 'toml': 'server = "http://a"\nmax_queue_gb = inf\n', 'argv': []},
]
ARGVS: list[list[str]] = [
    ['--server', 'http://a', '--jobs', '3'],
    ['--serv', 'http://a', '--job', '2'],
    ['--server', 'http://a', '--server', 'http://b'],
    ['--upload-mbps', '-5', '--jobs', '-1', '--max-queue-gb', '-0.5'],
    ['--upload-mbps', '1_000', '--jobs', ' 4 ', '--max-queue-gb', 'inf'],
    ['--jobs', '٣'],
    ['--jobs', '2.5'],
    ['--upload-mbps', 'fast'],
    ['--cache', 'c'],
    ['--server=http://a', '--token=t'],
    ['--ff', 'x'],
    ['positional'],
]
ENVIRONMENTS = [
    {'APPDATA': '<appdata>', 'LOCALAPPDATA': '<localappdata>'},
    {},
    {'APPDATA': '', 'LOCALAPPDATA': ''},
]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='transcode-worker', exit_on_error=False)
    home_worker.add_arguments(parser)
    return parser


def _config_view(config: WorkerConfig, root: Path) -> dict[str, Any]:
    return {
        'server': config.server,
        'token': config.token,
        'cache_dir': rooted(str(config.cache_dir), root),
        'upload_mbps': config.upload_mbps,
        'jobs': config.jobs,
        'max_queue_gb': config.max_queue_gb,
        'ffmpeg': config.ffmpeg,
        'ffprobe': config.ffprobe,
        'name': HOST if config.name == socket.gethostname() else config.name,
        'encoder': config.encoder,
    }


def _settings() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        home = root / 'home'
        for environment in ENVIRONMENTS:
            values = {
                key: value.replace('<appdata>', str(root / 'appdata')).replace('<localappdata>', str(root / 'local'))
                for key, value in environment.items()
            }
            with _environ(values, home):
                cases.append(
                    {
                        'kind': 'defaults',
                        'environment': environment,
                        'config_path': rooted(str(home_worker.default_config_path()), root),
                        'cache_dir': rooted(str(home_worker.default_cache_dir()), root),
                    }
                )
        for number, spec in enumerate(CONFIGS):
            folder = root / f'config{number}'
            folder.mkdir()
            appdata = folder / 'appdata'
            if spec['toml'] is not None:
                path = appdata / 'embyx-transcode' / 'config.toml'
                path.parent.mkdir(parents=True)
                path.write_text(spec['toml'], encoding='utf-8')
            argv = [str(folder / item) if item == 'missing.toml' else item for item in spec['argv']]
            with _environ({'APPDATA': str(appdata), 'LOCALAPPDATA': str(folder / 'local')}, home):
                result = outcome(home_worker.load_config, _parser().parse_args(argv))
            if 'value' in result:
                result['value'] = _config_view(result['value'], root)
            for key in ('exit',):
                if key in result:
                    result[key] = rooted(result[key], root)
            cases.append({'kind': 'load', 'title': spec['title'], 'toml': spec['toml'], 'argv': spec['argv'], **result})
        for argv in ARGVS:
            try:
                namespace = _parser().parse_args(argv)
            except (argparse.ArgumentError, SystemExit) as exc:
                cases.append({'kind': 'argv', 'argv': argv, 'refused': type(exc).__name__})
                continue
            parsed = {key: (str(value) if isinstance(value, Path) else value) for key, value in vars(namespace).items()}
            cases.append({'kind': 'argv', 'argv': argv, 'value': parsed})
    return cases


@contextlib.contextmanager
def _environ(values: dict[str, str], home: Path) -> Iterator[None]:
    import os  # noqa: PLC0415

    saved = {key: os.environ.get(key) for key in ('APPDATA', 'LOCALAPPDATA', 'HOME')}
    for key in ('APPDATA', 'LOCALAPPDATA'):
        os.environ.pop(key, None)
    os.environ.update(values)
    os.environ['HOME'] = str(home)
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


# -- the requests ----------------------------------------------------------------------------

SHOWN_HEADERS = (
    'authorization',
    'content-type',
    'content-length',
    'transfer-encoding',
    'x-chunk-start',
    'x-chunk-duration',
    'x-chunk-frames',
    'x-chunk-sha256',
)

#: A body shorter than this is shown as text; a chunk's only by its size and hash.
SHOWN_BODY_BYTES = 4096

ANSWERS: list[tuple[int, bytes, str]] = [
    (200, b'{"task": null, "wait": "idle"}', 'application/json'),
    (200, b'{"active": true, "paused": false}', 'application/json'),
    (
        409,
        b'{"error": {"code": "transcode_chunk_out_of_order", "next_chunk": 1, "resume_at": 20.0}}',
        'application/json',
    ),
    (400, b'{"error": {"code": "transcode_chunk_corrupt"}}', 'application/json'),
    (401, b'{"error": {"code": "unauthorized"}}', 'application/json'),
    (422, b'{"error": {"code": "invalid_request"}}', 'application/json'),
    (404, b'{"detail": "Not Found"}', 'application/json'),
    (500, b'Internal Server Error', 'text/plain'),
    (502, b'', 'text/plain'),
    (409, b'{"error": {"code": 5, "x": [1, "y"]}}', 'application/json'),
    (409, b'{"error": null}', 'application/json'),
    (503, b'{"error": {"note": "no code"}}', 'application/json'),
    (201, b'{"stored": true}', 'application/json'),
]


def _requests() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        chunk_path = root / 'chunk-00003.mkv'
        body = bytes(range(256)) * 1100 + b'tail'
        chunk_path.write_bytes(body)
        chunk = LocalChunk(
            part_index=1,
            index=3,
            start=6.006,
            duration=59.993333333,
            frames=3596,
            bytes=len(body),
            sha256=hashlib.sha256(body).hexdigest(),
            path=chunk_path,
        )
        report = {
            'name': 'pc-名前',
            'version': '1',
            'task_id': 7,
            'state': 'encoding',
            'parts': [{'index': 0, 'fps': 29.5, 'position': 12.000001}, {'index': 2, 'fps': 0.0, 'position': 0.0}],
            'upload_bps': 1234567.891,
            'queue_chunks': 2,
            'queue_bytes': 3 * 10**9,
        }
        calls: list[tuple[str, Callable[[Server], Any]]] = [
            ('claim', lambda server: server.claim(home_worker.idle_report(WorkerConfig(server='x', name='pc')))),
            ('heartbeat', lambda server: server.heartbeat(report)),
            (
                'upload',
                lambda server: server.upload(7, chunk, RateLimiter(0), home_worker.RateMeter()),
            ),
            ('finish', lambda server: server.finish(7, 2)),
        ]
        for server_url in ('http://10.117.200.1:8080', 'http://gw:8080/prefix'):
            for token in ('', 'secret-token'):
                for name, call in calls:
                    for status, content, content_type in ANSWERS if name == 'claim' else ANSWERS[:1]:
                        seen: list[dict[str, Any]] = []

                        def handler(
                            request: httpx.Request,
                            *,
                            answer: tuple[int, bytes, str] = (status, content, content_type),
                            seen: list[dict[str, Any]] = seen,
                        ) -> httpx.Response:
                            payload = request.read()
                            seen.append(
                                {
                                    'method': request.method,
                                    'url': str(request.url),
                                    'headers': {
                                        key: value for key, value in request.headers.items() if key in SHOWN_HEADERS
                                    },
                                    'body': payload.decode('utf-8') if len(payload) < SHOWN_BODY_BYTES else None,
                                    'body_bytes': len(payload),
                                    'body_sha256': hashlib.sha256(payload).hexdigest(),
                                }
                            )
                            return httpx.Response(answer[0], content=answer[1], headers={'content-type': answer[2]})

                        config = WorkerConfig(server=server_url, token=token)
                        server = Server(config, transport=httpx.MockTransport(handler))
                        try:
                            result: dict[str, Any] = {'value': call(server)}
                        except ServerError as exc:
                            result = {
                                'server_error': {
                                    'status': exc.status,
                                    'code': exc.code,
                                    'details': exc.details,
                                    'str': str(exc),
                                    'details_repr': repr(exc.details),
                                }
                            }
                        except Exception as exc:  # noqa: BLE001 - the class is the golden
                            result = {'error': {'type': type(exc).__name__}}
                        finally:
                            server.close()
                        cases.append(
                            {
                                'call': name,
                                'server': server_url,
                                'token': token,
                                'part_url': server.part_url(7, 2),
                                'answer': {'status': status, 'body': content.decode('utf-8'), 'type': content_type},
                                'requests': seen,
                                **result,
                            }
                        )
        cases.append({'call': 'upload_chunk', 'size': len(body), 'path': chunk_path.name})
    return cases


# -- a run's pieces --------------------------------------------------------------------------


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)


class Quiet:
    def __init__(self) -> None:
        self.lines: list[dict[str, Any]] = []

    def __call__(self, message: str, *, status: bool = False) -> None:
        self.lines.append({'status': status, 'line': message})


class NoServer:
    def part_url(self, task_id: int, part_index: int) -> str:
        return f'http://cluster/api/transcode/tasks/{task_id}/parts/{part_index}'


def _assignment(parts: list[dict[str, Any]], **extra: Any) -> dict[str, Any]:
    return {
        'id': 7,
        'avid': 'SQTEVR-009',
        'video_bitrate': 6_140_000,
        'chunk_seconds': 60,
        'encoder': dict(NVENC),
        'paused': False,
        'parts': parts,
        **extra,
    }


def _part(index: int, duration: float, *, next_chunk: int = 0, resume_at: float = 0.0, done: bool = False) -> dict:
    return {
        'index': index,
        'name': f'SQTEVR-009-cd{index + 1}.mp4',
        'size': 10**9,
        'duration': duration,
        'done': done,
        'next_chunk': next_chunk,
        'resume_at': resume_at,
    }


STATUS_SCENARIOS: list[dict[str, Any]] = [
    {
        'title': 'two parts encoding, a queue',
        'parts': [_part(0, 3600.0), _part(1, 1800.5), _part(2, 0.0)],
        'states': [
            {'fps': 29.97, 'position': 1234.5, 'resume_at': 1200.0},
            {'fps': 0.5, 'position': 0.0, 'resume_at': 0.0},
            {'finished': True, 'resume_at': 0.0},
        ],
        'active': [0, 1, 2],
        'queue': [500 * 1024**2, 1536 * 1024**2],
        'uploads': [(999.0, 2_000_000), (1000.0, 1_000_000)],
        'paused': False,
        'wind_down': False,
    },
    {
        'title': 'paused, nothing encoding',
        'parts': [_part(0, 10.0), _part(1, 20.0)],
        'states': [{'resume_at': 4.0, 'finished': True}, {'resume_at': 25.0}],
        'active': [],
        'queue': [],
        'uploads': [(985.0, 5)],
        'paused': True,
        'wind_down': True,
    },
    {
        'title': 'winding down',
        'parts': [_part(0, 100.0)],
        'states': [{'fps': 120.4, 'position': 99.995, 'resume_at': 99.5}],
        'active': [0],
        'queue': [1],
        'uploads': [],
        'paused': False,
        'wind_down': True,
    },
    {
        'title': 'every part without a duration',
        'parts': [_part(0, 0.0), _part(1, 0.0)],
        'states': [{'fps': 1.5, 'position': 3.0}, {}],
        'active': [0],
        'queue': [],
        'uploads': [],
        'paused': False,
        'wind_down': False,
    },
]


class FakeProcess:
    pass


def _status(spec: dict[str, Any], clock: FakeClock) -> dict[str, Any]:
    log = Quiet()
    config = WorkerConfig(server='http://cluster', name='pc', cache_dir=Path('/nonexistent'))
    run = TaskRun(config, NoServer(), _assignment(spec['parts']), log)  # type: ignore[arg-type]
    for part, state in zip(run.parts, spec['states'], strict=True):
        for key, value in state.items():
            setattr(part, key, value)
    for at in spec['active']:
        run.processes[run.parts[at].index] = FakeProcess()  # type: ignore[assignment]
    for number, size in enumerate(spec['queue']):
        run.queue.append(
            LocalChunk(
                part_index=0, index=number, start=0.0, duration=1.0, frames=1, bytes=size, sha256='', path=Path('x')
            )
        )
    for at, count in spec['uploads']:
        clock.now = at
        run.meter.add(count)
    clock.now = 1000.0
    run.paused = spec['paused']
    if spec['wind_down']:
        run.wind_down.set()
    run.log_status()
    reports = {state: run.report(state) for state in ('encoding', 'paused', 'stopping')}
    return {'kind': 'status', **spec, 'lines': log.lines, 'reports': reports}


COLLECT_SCENARIOS: list[dict[str, Any]] = [
    {
        'title': 'a first run: the reorder delay is dropped',
        'part': {'next_index': 0, 'resume_at': 0.0},
        'listing': 'chunk-00000.mkv,0.000000,60.093333\nchunk-00001.mkv,60.093333,120.093333\n',
        'pts': '0.083333,\n0.000000,\n',
        'frames': '1800\n',
        'seen': 0,
    },
    {
        'title': 'a resumed run lines up with what the cluster has',
        'part': {'next_index': 3, 'resume_at': 180.06},
        'listing': '"chunk-00003.mkv",0.000000,60.083333\nchunk-00004.mkv,60.083333,62.5\n',
        'pts': '0.083333\n',
        'frames': '1799\n',
        'seen': 0,
    },
    {
        'title': 'a chunk number that does not follow',
        'part': {'next_index': 3, 'resume_at': 180.0},
        'listing': 'chunk-00005.mkv,0.0,60.0\n',
        'pts': '0.0\n',
        'frames': '1800\n',
        'seen': 0,
    },
    {
        'title': 'a row still being written',
        'part': {'next_index': 0, 'resume_at': 0.0},
        'listing': 'chunk-00000.mkv,0.0,60.0\nchunk-00001.mkv,60.0\n',
        'pts': '0.0\n',
        'frames': '1800\n',
        'seen': 0,
    },
    {
        'title': 'rows already picked up are skipped',
        'part': {'next_index': 1, 'resume_at': 60.0},
        'listing': 'chunk-00000.mkv,0.0,60.0\nchunk-00001.mkv,60.0,120.0\n',
        'pts': '0.0\n',
        'frames': '1800\n',
        'seen': 1,
        'shift': 0.0,
    },
    {
        'title': 'ffprobe cannot read the chunk yet',
        'part': {'next_index': 0, 'resume_at': 0.0},
        'listing': 'chunk-00000.mkv,0.0,60.0\n',
        'pts': '',
        'frames': '1800\n',
        'seen': 0,
    },
    {
        'title': 'no listing yet',
        'part': {'next_index': 0, 'resume_at': 0.0},
        'listing': None,
        'pts': '0.0\n',
        'frames': '1800\n',
        'seen': 0,
    },
    {
        'title': 'the frames cannot be counted',
        'part': {'next_index': 0, 'resume_at': 0.0},
        'listing': 'chunk-00000.mkv,0.0,60.0\n',
        'pts': '0.0\n',
        'frames': 'N/A\n',
        'seen': 0,
    },
    {
        'title': 'a full path in the listing, a start before the shift',
        'part': {'next_index': 0, 'resume_at': 10.01},
        'listing': '/elsewhere/chunk-00000.mkv,0.0,10.0933\n',
        'pts': '0.0833\n',
        'frames': '300\n',
        'seen': 0,
    },
]


def _collect(spec: dict[str, Any], root: Path) -> dict[str, Any]:
    folder = root / f'collect{COLLECT_SCENARIOS.index(spec)}'
    directory = folder / '7' / 'cd01'
    directory.mkdir(parents=True)
    log = Quiet()
    config = WorkerConfig(server='http://cluster', name='pc', cache_dir=folder)
    run = TaskRun(config, NoServer(), _assignment([_part(0, 600.0)]), log)  # type: ignore[arg-type]
    part = run.parts[0]
    part.next_index, part.resume_at = spec['part']['next_index'], spec['part']['resume_at']
    for index in range(8):
        (directory / f'chunk-{index:05d}.mkv').write_bytes(f'chunk {index}'.encode())
    listing = directory / f'list-{part.next_index:05d}.csv'
    if spec['listing'] is not None:
        listing.write_text(spec['listing'], encoding='utf-8')
    calls: list[list[str]] = []

    def run_probe(argv: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append([rooted(item, folder) for item in argv])
        stdout = spec['pts'] if 'packet=pts_time' in argv else spec['frames']
        return subprocess.CompletedProcess(argv, 0, stdout, '')

    clock = RunClock(anchor=part.resume_at, shift=spec.get('shift'))
    with patched(home_worker.subprocess, 'run', run_probe):
        seen = run._collect(part, listing, clock, spec['seen'])  # noqa: SLF001 - the method under test
    queued = [
        {key: getattr(chunk, key) for key in CHUNKS[0]} | {'path': rooted(str(chunk.path), folder)}
        for chunk in run.queue
    ]
    return {
        'kind': 'collect',
        **spec,
        'returned': seen,
        'shift_after': clock.shift,
        'next_index': part.next_index,
        'resume_at': part.resume_at,
        'queued': queued,
        'sidecars': [entry for entry in _listing(directory) if entry['name'].endswith('.json')],
        'calls': calls,
        'log': log.lines,
    }


def _limiter() -> list[dict[str, Any]]:
    cases = []
    for bits, steps in (
        (30_000_000, [(0.0, 262144), (0.0, 262144), (0.05, 262144), (2.0, 262144), (2.0, 10_000_000)]),
        (8_000_000, [(0.5, 1_000_000), (0.5, 1), (10.0, 500_000), (10.1, 500_000)]),
        (0, [(0.0, 10**9)]),
        (-5, [(0.0, 1)]),
    ):
        clock = FakeClock()
        with patched(home_worker.time, 'monotonic', clock.monotonic), patched(home_worker.time, 'sleep', clock.sleep):
            limiter = RateLimiter(bits, clock=clock.monotonic)
            for at, count in steps:
                clock.now = 1000.0 + at
                limiter.take(count)
        cases.append({'kind': 'limiter', 'bits_per_second': bits, 'steps': steps, 'sleeps': clock.sleeps})
    return cases


def _meter() -> list[dict[str, Any]]:
    clock = FakeClock()
    rates = []
    with patched(home_worker.time, 'monotonic', clock.monotonic):
        meter = home_worker.RateMeter()
        for at, count in ((0.0, 100), (5.0, 200), (9.99, 300)):
            clock.now = 1000.0 + at
            meter.add(count)
        for at in (9.99, 10.0, 10.000001, 15.0, 20.0, 25.0):
            clock.now = 1000.0 + at
            rates.append({'at': at, 'rate': meter.rate()})
    return [{'kind': 'meter', 'samples': [(0.0, 100), (5.0, 200), (9.99, 300)], 'rates': rates}]


def _run_pieces() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    clock = FakeClock()
    with patched(home_worker.time, 'monotonic', clock.monotonic):
        cases.extend(_status(spec, clock) for spec in STATUS_SCENARIOS)
    with tempfile.TemporaryDirectory() as directory:
        cases.extend(_collect(spec, Path(directory)) for spec in COLLECT_SCENARIOS)
    clock_cases = [
        (10.01, 0.0833, 0.0, 10.0933, 10.01),
        (0.0, 0.083333, 0.0, 60.093333, 0.0),
        (180.06, 0.083333, 60.083333, 120.08, 180.06),
        (5.0, 0.2, 0.1, 2.0, 5.0),
        (5.0, 0.0, 0.3, 2.0, 5.0),
    ]
    for anchor, shift, start, end, resume_at in clock_cases:
        run_clock = RunClock(anchor=anchor, shift=shift)
        begin = run_clock.anchor + max(start - shift, 0.0)
        begin = resume_at if abs(begin - resume_at) <= 0.25 else begin  # noqa: PLR2004 - as `_collect` has it
        cases.append(
            {
                'kind': 'clock',
                'anchor': anchor,
                'shift': shift,
                'start': start,
                'end': end,
                'resume_at': resume_at,
                'begin': begin,
                'duration': run_clock.anchor + (end - shift) - begin,
            }
        )
    cases.extend(_limiter())
    cases.extend(_meter())
    cases.extend(
        {'kind': 'idle_report', 'name': name, 'value': home_worker.idle_report(WorkerConfig(server='x', name=name))}
        for name in ('pc', 'PC-名前')
    )
    for wait, reason in home_worker.WAIT_REASONS.items():
        cases.append({'kind': 'wait_reason', 'wait': wait, 'reason': reason})
    cases.append(
        {
            'kind': 'constants',
            'version': home_worker.VERSION,
            'retry_delays': list(home_worker.RETRY_DELAYS),
            'heartbeat': home_worker.HEARTBEAT_SECONDS,
            'idle_poll': home_worker.IDLE_POLL_SECONDS,
            'status': home_worker.STATUS_SECONDS,
            'upload_piece': home_worker.UPLOAD_PIECE_BYTES,
        }
    )
    return cases


# -- the main loop ---------------------------------------------------------------------------

LOOPS: list[dict[str, Any]] = [
    {
        'title': 'waits, failures and a title that is already done',
        'answers': [
            {'error': 'transport'},
            {'error': 'server', 'status': 401, 'code': 'unauthorized'},
            {'error': 'transport'},
            {'value': {'task': None, 'wait': 'idle'}},
            {'value': {'task': None, 'wait': 'idle'}},
            {'value': {'task': None, 'wait': 'probing', 'task_id': 7}},
            {'value': {'task': None, 'wait': 'mystery'}},
            {'value': {'task': None}},
            {'value': {'task': _assignment([_part(0, 30.0, done=True), _part(1, 20.0, done=True)])}},
            {'value': {'task': None, 'wait': 'idle'}},
        ],
    },
    {
        'title': 'eight failures in a row',
        'answers': [{'error': 'transport'}] * 8 + [{'value': {'task': None, 'wait': 'busy'}}],
    },
    {
        'title': 'a float bit rate and a stale cache',
        'stale': ['3', '12', 'loose.txt'],
        'answers': [
            {
                'value': {
                    'task': _assignment([_part(0, 1.0, done=True)], id=12, video_bitrate=12_345_678.9, avid='ABC-001'),
                }
            },
        ],
    },
]


def _loop(spec: dict[str, Any], root: Path) -> dict[str, Any]:  # noqa: C901 - the fakes are inner classes
    cache = root / f'loop{LOOPS.index(spec)}'
    cache.mkdir()
    for name in spec.get('stale', []):
        if '.' in name:
            (cache / name).write_text('x', encoding='utf-8')
        else:
            (cache / name / 'cd01').mkdir(parents=True)
    answers = deque(spec['answers'])
    log = Quiet()
    sleeps: list[float] = []
    claims: list[dict[str, Any]] = []

    class Stop:
        def __call__(self) -> int:
            return 0 if answers else 1

    stop = Stop()

    class FakeServer:
        def __init__(self, _config: WorkerConfig) -> None:
            pass

        def close(self) -> None:
            return None

        def part_url(self, task_id: int, part_index: int) -> str:
            return f'http://cluster/{task_id}/{part_index}'

        def claim(self, report: dict[str, Any]) -> dict[str, Any]:
            claims.append(report)
            answer = answers.popleft()
            if answer.get('error') == 'transport':
                msg = 'connection refused'
                raise httpx.ConnectError(msg)
            if answer.get('error') == 'server':
                raise ServerError(answer['status'], answer['code'], {'code': answer['code']})
            return answer['value']

        def heartbeat(self, _report: dict[str, Any]) -> dict[str, Any]:
            return {'active': True, 'paused': False}

        def finish(self, _task_id: int, _part: int) -> dict[str, Any]:
            return {'finished': True}

    namespace = _parser().parse_args(['--server', 'http://cluster', '--cache-dir', str(cache), '--upload-mbps', '25.5'])
    with (
        patched(home_worker, 'Server', FakeServer),
        patched(home_worker, 'Console', lambda: log),
        patched(home_worker, 'StopSignal', lambda _log: _Installable(stop)),
        patched(home_worker, 'check_tools', lambda _config: None),
        patched(home_worker, '_sleep', lambda seconds, _stop: sleeps.append(seconds)),
        patched(home_worker.time, 'sleep', lambda _seconds: None),
    ):
        code = home_worker.main(namespace)
    lines = [{**line, 'line': rooted(line['line'], root)} for line in log.lines]
    return {
        'kind': 'loop',
        **spec,
        'code': code,
        'log': lines,
        'sleeps': sleeps,
        'claims': [
            {**claim, 'name': HOST if claim['name'] == socket.gethostname() else claim['name']} for claim in claims
        ],
        'left': sorted(path.name for path in cache.iterdir()),
    }


class _Installable:
    def __init__(self, stop: Callable[[], int]) -> None:
        self._stop = stop

    def install(self) -> None:
        return None

    def __call__(self) -> int:
        return self._stop()


def _loops() -> list[dict[str, Any]]:
    with tempfile.TemporaryDirectory() as directory:
        return [_loop(spec, Path(directory)) for spec in LOOPS]


def _check_tools() -> list[dict[str, Any]]:
    cases = []
    for ffmpeg, ffprobe in (('ffmpeg-embyx-missing', 'ffprobe'), ('sh', 'ffprobe-embyx-missing'), ('sh', 'sh')):
        config = WorkerConfig(server='x', ffmpeg=ffmpeg, ffprobe=ffprobe)
        cases.append({'ffmpeg': ffmpeg, 'ffprobe': ffprobe, 'problem': home_worker.check_tools(config)})
    return cases


def _parts_states() -> list[dict[str, Any]]:
    # PartState's defaults, as TaskRun builds them from an assignment.
    config = WorkerConfig(server='http://cluster', name='pc', cache_dir=Path('/nonexistent'))
    task = _assignment([_part(0, 30.0, next_chunk=2, resume_at=12.5), _part(3, 20.0, done=True)], paused=1)
    run = TaskRun(config, NoServer(), task, Quiet())  # type: ignore[arg-type]
    fields = [field for field in PartState.__dataclass_fields__ if field != 'name']
    return [
        {
            'task': task,
            'encoder_override': {'tune': 'hq', 'codec': 'libx264'},
            'parts': [{key: getattr(part, key) for key in ['name', *fields]} for part in run.parts],
            'paused': run.paused,
            'directory': str(run.directory),
            'max_queue_bytes': run.max_queue_bytes,
            'encoder': TaskRun(
                WorkerConfig(server='x', encoder={'tune': 'hq', 'codec': 'libx264'}),
                NoServer(),
                task,
                Quiet(),  # type: ignore[arg-type]
            ).encoder,
        }
    ]


GOLDENS = {
    'merge/home_worker_commands.json': _commands,
    'merge/home_worker_probes.json': _probes,
    'merge/home_worker_cache.json': _cache,
    'merge/home_worker_settings.json': _settings,
    'merge/home_worker_requests.json': _requests,
    'merge/home_worker_run.json': lambda: [
        *_run_pieces(),
        *({'kind': 'tools', **case} for case in _check_tools()),
        *({'kind': 'task', **case} for case in _parts_states()),
    ],
    'merge/home_worker_loop.json': _loops,
}
