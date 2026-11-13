"""``embyx-manager transcode-worker``: re-encode merges too big for 115 on a home GPU.

Runs in a terminal on the PC with the NVIDIA card (Windows or Linux), started
and stopped by hand. It asks embyx-manager, through the WireGuard tunnel, for a
title waiting to be re-encoded, then for up to ``jobs`` parts at once:

* ffmpeg reads the part over HTTP ranges from the cluster (nothing is
  downloaded first), decodes on the GPU and encodes the video only with
  NVENC AV1, cutting it into chunks of about a minute that start on a
  keyframe;
* each finished chunk is checked (frames, SHA-256) and uploaded, one at a
  time and no faster than ``upload_mbps``; when ``max_queue_gb`` of chunks
  wait, the encoders are suspended until the queue drains;
* a part whose chunks are all received is reported finished. The cluster
  copies the audio meanwhile and assembles the file when every part is in.

Ctrl+C once lets every encoder finish its current chunk, uploads what is
done and exits; a second Ctrl+C exits at once. Either way the next start
carries on from the last chunk the cluster has (chunks finished but not yet
uploaded are kept in the cache and sent first). Pausing on the merge page
does the same as the first Ctrl+C but waits for a resume instead of exiting.

Settings come from a TOML file (``--config``, by default
``%APPDATA%\\embyx-transcode\\config.toml``) and the command line, which wins.
"""

import argparse
import contextlib
import csv
import ctypes
import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import tomllib
from collections import deque
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

VERSION = '1'
HEARTBEAT_SECONDS = 15.0
IDLE_POLL_SECONDS = 30.0
STATUS_SECONDS = 2.0
UPLOAD_PIECE_BYTES = 256 * 1024
REQUEST_TIMEOUT = httpx.Timeout(60.0, read=600.0, write=600.0)
#: Backoff after a failed request or a crashed ffmpeg, doubling up to the last.
RETRY_DELAYS = (5.0, 10.0, 20.0, 40.0, 80.0, 160.0, 300.0)
WAIT_REASONS = {
    'idle': '没有等转码的作品',
    'busy': '集群在合并别的作品，等它完成',
    'probing': '集群正在读取分盘信息',
    'assembling': '这部已全部转完，集群在拼接，等它完成',
    'unavailable': '集群这边暂时不能合并',
}


def default_config_path() -> Path:
    base = os.environ.get('APPDATA')
    root = Path(base) if base else Path.home() / '.config'
    return root / 'embyx-transcode' / 'config.toml'


def default_cache_dir() -> Path:
    base = os.environ.get('LOCALAPPDATA')
    root = Path(base) if base else Path.home() / '.cache'
    return root / 'embyx-transcode'


@dataclass
class WorkerConfig:
    server: str
    token: str = ''
    cache_dir: Path = field(default_factory=default_cache_dir)
    upload_mbps: float = 30.0
    jobs: int = 3
    max_queue_gb: float = 8.0
    ffmpeg: str = 'ffmpeg'
    ffprobe: str = 'ffprobe'
    name: str = field(default_factory=socket.gethostname)
    #: Replaces keys of the encoder settings the cluster sends (for a test without NVENC, say).
    encoder: dict[str, Any] = field(default_factory=dict)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('--config', type=Path, default=None, help='TOML settings file')
    parser.add_argument('--server', default=None, help='the transcode endpoint, e.g. http://10.117.200.1:8080')
    parser.add_argument('--token', default=None, help='bearer token, when the cluster asks for one')
    parser.add_argument('--cache-dir', type=Path, default=None, help='where chunks wait for their upload')
    parser.add_argument('--upload-mbps', type=float, default=None, help='upload speed cap in Mbit/s (default 30)')
    parser.add_argument('--jobs', type=int, default=None, help='parts encoded at once (default 3)')
    parser.add_argument('--max-queue-gb', type=float, default=None, help='chunks kept waiting before encoding pauses')
    parser.add_argument('--ffmpeg', default=None, help='ffmpeg executable')
    parser.add_argument('--ffprobe', default=None, help='ffprobe executable')


def load_config(args: argparse.Namespace) -> WorkerConfig:
    path = args.config or default_config_path()
    values: dict[str, Any] = {}
    if path.is_file():
        values = tomllib.loads(path.read_text(encoding='utf-8'))
    elif args.config is not None:
        msg = f'config file not found: {path}'
        raise SystemExit(msg)
    for key in ('server', 'token', 'cache_dir', 'upload_mbps', 'jobs', 'max_queue_gb', 'ffmpeg', 'ffprobe'):
        value = getattr(args, key, None)
        if value is not None:
            values[key] = value
    if not values.get('server'):
        msg = f'no server given: pass --server or set server in {path}'
        raise SystemExit(msg)
    config = WorkerConfig(server=str(values['server']).rstrip('/'))
    config.token = str(values.get('token', ''))
    if values.get('cache_dir'):
        config.cache_dir = Path(values['cache_dir'])
    config.upload_mbps = float(values.get('upload_mbps', config.upload_mbps))
    config.jobs = max(1, int(values.get('jobs', config.jobs)))
    config.max_queue_gb = float(values.get('max_queue_gb', config.max_queue_gb))
    config.ffmpeg = str(values.get('ffmpeg', config.ffmpeg))
    config.ffprobe = str(values.get('ffprobe', config.ffprobe))
    config.name = str(values.get('name', config.name))
    config.encoder = dict(values.get('encoder', {}))
    return config


# -- ffmpeg --------------------------------------------------------------------


def _process_flags() -> dict[str, Any]:
    """Children of their own process group, so Ctrl+C reaches only the worker, which decides."""
    if os.name == 'nt':
        return {'creationflags': subprocess.CREATE_NEW_PROCESS_GROUP}
    return {'start_new_session': True}


def suspend(process: subprocess.Popen[Any]) -> None:
    if os.name == 'nt':
        ctypes.windll.ntdll.NtSuspendProcess(int(process._handle))  # type: ignore[attr-defined]  # noqa: SLF001
    else:
        os.kill(process.pid, signal.SIGSTOP)


def resume(process: subprocess.Popen[Any]) -> None:
    if os.name == 'nt':
        ctypes.windll.ntdll.NtResumeProcess(int(process._handle))  # type: ignore[attr-defined]  # noqa: SLF001
    else:
        os.kill(process.pid, signal.SIGCONT)


def uses_nvenc(encoder: dict[str, Any]) -> bool:
    return str(encoder.get('codec', '')).endswith('_nvenc')


def decoder_args(encoder: dict[str, Any]) -> list[str]:
    """Decode on the GPU when encoding there, so frames never leave it."""
    if uses_nvenc(encoder):
        return ['-hwaccel', 'cuda', '-hwaccel_output_format', 'cuda']
    return []


def encoder_args(encoder: dict[str, Any], video_bits: int, fps: float) -> list[str]:
    """The video filter and encoder options for ``encoder`` at ``video_bits`` per second."""
    codec = str(encoder['codec'])
    gop = max(1, round(float(encoder.get('gop_seconds', 5)) * fps))
    if not uses_nvenc(encoder):
        return ['-c:v', codec, '-b:v', str(video_bits), '-g', str(gop), *map(str, encoder.get('extra', []))]
    args: list[str] = []
    if int(encoder.get('bit_depth', 8)) == 10:  # noqa: PLR2004 - a bit depth
        args += ['-vf', 'scale_cuda=format=p010le']
    args += [
        '-c:v',
        codec,
        '-preset',
        str(encoder.get('preset', 'p7')),
        '-tune',
        str(encoder.get('tune', 'hq')),
        '-rc',
        'vbr',
        '-b:v',
        str(video_bits),
        '-maxrate',
        str(2 * video_bits),
        '-bufsize',
        str(2 * video_bits),
    ]
    if encoder.get('multipass'):
        args += ['-multipass', str(encoder['multipass'])]
    if encoder.get('lookahead'):
        args += ['-rc-lookahead', str(encoder['lookahead'])]
    if encoder.get('spatial_aq'):
        args += ['-spatial-aq', '1']
    args += ['-g', str(gop), '-forced-idr', '1']
    return args + [str(item) for item in encoder.get('extra', [])]


def encode_command(  # noqa: PLR0913
    config: WorkerConfig,
    *,
    url: str,
    encoder: dict[str, Any],
    video_bits: int,
    fps: float,
    seek: float,
    chunk_seconds: int,
    first_index: int,
    directory: Path,
    listing: Path,
) -> list[str]:
    """ffmpeg reading ``url`` from ``seek`` on, writing chunks ``first_index`` onwards into ``directory``."""
    headers = ['-headers', f'Authorization: Bearer {config.token}\r\n'] if config.token else []
    network = (
        ['-reconnect', '1', '-reconnect_on_network_error', '1', '-reconnect_delay_max', '60']
        if url.startswith('http')
        else []
    )
    return [
        config.ffmpeg,
        '-hide_banner',
        '-nostdin',
        '-loglevel',
        'error',
        '-y',
        *decoder_args(encoder),
        *headers,
        *network,
        *(['-ss', f'{seek:.6f}'] if seek > 0 else []),
        '-i',
        url,
        '-map',
        '0:v:0',
        '-an',
        '-sn',
        '-dn',
        *encoder_args(encoder, video_bits, fps),
        '-force_key_frames',
        f'expr:gte(t,n_forced*{chunk_seconds})',
        '-f',
        'segment',
        '-segment_time',
        str(chunk_seconds),
        '-segment_format',
        'matroska',
        '-reset_timestamps',
        '1',
        '-segment_start_number',
        str(first_index),
        '-segment_list',
        str(listing),
        '-segment_list_type',
        'csv',
        '-progress',
        'pipe:1',
        '-nostats',
        str(directory / 'chunk-%05d.mkv'),
    ]


def first_pts(path: Path, ffprobe: str) -> float:
    """The earliest presentation time among a chunk's first packets.

    The muxer shifts the whole run by the encoder's reorder delay, so the times in
    the segment list are off by exactly this much from the first chunk on.
    """
    completed = subprocess.run(  # noqa: S603 - fixed binary, argument list
        [
            ffprobe,
            '-v',
            'error',
            '-select_streams',
            'v:0',
            '-read_intervals',
            '%+#32',
            '-show_entries',
            'packet=pts_time',
            '-of',
            'csv=p=0',
            str(path),
        ],
        capture_output=True,
        text=True,
        check=False,
        **_process_flags(),
    )
    times = []
    for line in completed.stdout.splitlines():
        try:
            times.append(float(line.strip().rstrip(',')))
        except ValueError:
            continue
    if completed.returncode != 0 or not times:
        msg = f'ffprobe cannot read the timestamps of {path.name}: {completed.stderr.strip()[-200:]}'
        raise RuntimeError(msg)
    return min(times)


@dataclass
class RunClock:
    """Maps one ffmpeg run's segment-list times onto times in the part."""

    #: Where in the part the run's first frame is.
    anchor: float
    #: The run's first presentation time, read from its first chunk.
    shift: float | None = None


def count_frames(path: Path, ffprobe: str) -> int:
    completed = subprocess.run(  # noqa: S603 - fixed binary, argument list
        [
            ffprobe,
            '-v',
            'error',
            '-select_streams',
            'v:0',
            '-count_packets',
            '-show_entries',
            'stream=nb_read_packets',
            '-of',
            'csv=p=0',
            str(path),
        ],
        capture_output=True,
        text=True,
        check=False,
        **_process_flags(),
    )
    value = completed.stdout.strip().split(',')[0]
    if completed.returncode != 0 or not value.isdigit():
        msg = f'ffprobe cannot count the frames of {path.name}: {completed.stderr.strip()[-200:]}'
        raise RuntimeError(msg)
    return int(value)


def probe_fps(url: str, config: WorkerConfig) -> float:
    headers = ['-headers', f'Authorization: Bearer {config.token}\r\n'] if config.token else []
    completed = subprocess.run(  # noqa: S603 - fixed binary, argument list
        [
            config.ffprobe,
            '-v',
            'error',
            *headers,
            '-select_streams',
            'v:0',
            '-show_entries',
            'stream=avg_frame_rate,r_frame_rate',
            '-of',
            'json',
            url,
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=600,
        **_process_flags(),
    )
    streams = json.loads(completed.stdout or '{}').get('streams') or [{}]
    for key in ('avg_frame_rate', 'r_frame_rate'):
        numerator, _, denominator = str(streams[0].get(key, '0/0')).partition('/')
        with contextlib.suppress(ValueError, ZeroDivisionError):
            rate = float(numerator) / float(denominator or 1)
            if rate > 0:
                return rate
    msg = f'ffprobe found no frame rate: {completed.stderr.strip()[-200:]}'
    raise RuntimeError(msg)


def _drain(stream: Any, tail: deque[str]) -> None:
    """Read ffmpeg's stderr to its end, keeping the last lines for the log."""
    for line in stream:
        if line.strip():
            tail.append(line.rstrip())


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        while piece := handle.read(4 * 1024 * 1024):
            digest.update(piece)
    return digest.hexdigest()


# -- the chunks waiting for their upload ------------------------------------------


@dataclass(frozen=True)
class LocalChunk:
    """A finished chunk in the cache, described by the JSON file next to it."""

    part_index: int
    index: int
    start: float
    duration: float
    frames: int
    bytes: int
    sha256: str
    path: Path

    @property
    def end(self) -> float:
        return self.start + self.duration

    def sidecar(self) -> Path:
        return self.path.with_suffix('.json')

    def save(self) -> None:
        payload = {
            'part_index': self.part_index,
            'index': self.index,
            'start': self.start,
            'duration': self.duration,
            'frames': self.frames,
            'bytes': self.bytes,
            'sha256': self.sha256,
        }
        temporary = self.sidecar().with_name(f'.{self.sidecar().name}.tmp')
        temporary.write_text(json.dumps(payload), encoding='utf-8')
        temporary.replace(self.sidecar())

    def remove(self) -> None:
        self.path.unlink(missing_ok=True)
        self.sidecar().unlink(missing_ok=True)

    @classmethod
    def load(cls, sidecar: Path) -> 'LocalChunk | None':
        try:
            payload = json.loads(sidecar.read_text(encoding='utf-8'))
            chunk = cls(path=sidecar.with_suffix('.mkv'), **payload)
        except (OSError, ValueError, TypeError):
            return None
        if not chunk.path.is_file() or chunk.path.stat().st_size != chunk.bytes:
            return None
        return chunk


def kept_chunks(directory: Path, next_index: int, resume_at: float) -> list[LocalChunk]:
    """Chunks in ``directory`` that carry on from what the cluster has; everything else there is dropped."""
    found = {}
    if directory.is_dir():
        for sidecar in directory.glob('chunk-*.json'):
            chunk = LocalChunk.load(sidecar)
            if chunk is not None:
                found[chunk.index] = chunk
    kept: list[LocalChunk] = []
    expected_index, expected_start = next_index, resume_at
    while expected_index in found and abs(found[expected_index].start - expected_start) <= 0.25:  # noqa: PLR2004
        chunk = found.pop(expected_index)
        kept.append(chunk)
        expected_index, expected_start = chunk.index + 1, chunk.end
    keep = {chunk.path.name for chunk in kept} | {chunk.sidecar().name for chunk in kept}
    if directory.is_dir():
        for leftover in directory.iterdir():
            if leftover.name not in keep:
                with contextlib.suppress(OSError):
                    leftover.unlink()
    return kept


class RateLimiter:
    """A token bucket shared by the uploads; ``bits_per_second`` <= 0 means no cap."""

    def __init__(self, bits_per_second: float, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._rate = bits_per_second / 8
        self._clock = clock
        self._allowance = 0.0
        self._last = clock()
        self._lock = threading.Lock()

    def take(self, count: int) -> None:
        if self._rate <= 0:
            return
        with self._lock:
            now = self._clock()
            self._allowance = min(self._allowance + (now - self._last) * self._rate, self._rate)
            self._last = now
            self._allowance -= count
            wait = -self._allowance / self._rate if self._allowance < 0 else 0.0
        if wait > 0:
            time.sleep(wait)


class RateMeter:
    """Bytes per second over the last few seconds."""

    def __init__(self, window: float = 10.0) -> None:
        self._window = window
        self._samples: deque[tuple[float, int]] = deque()
        self._lock = threading.Lock()

    def add(self, count: int) -> None:
        with self._lock:
            self._samples.append((time.monotonic(), count))

    def rate(self) -> float:
        now = time.monotonic()
        with self._lock:
            while self._samples and now - self._samples[0][0] > self._window:
                self._samples.popleft()
            return sum(count for _, count in self._samples) / self._window


# -- talking to the cluster -----------------------------------------------------


class ServerError(Exception):
    def __init__(self, status: int, code: str, details: dict[str, Any]) -> None:
        super().__init__(f'{status} {code}')
        self.status = status
        self.code = code
        self.details = details


class Server:
    def __init__(self, config: WorkerConfig, *, transport: httpx.BaseTransport | None = None) -> None:
        headers = {'Authorization': f'Bearer {config.token}'} if config.token else {}
        self.base = config.server
        self._client = httpx.Client(
            base_url=config.server, headers=headers, timeout=REQUEST_TIMEOUT, transport=transport
        )

    def close(self) -> None:
        self._client.close()

    def part_url(self, task_id: int, part_index: int) -> str:
        return f'{self.base}/api/transcode/tasks/{task_id}/parts/{part_index}'

    def _call(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = self._client.request(method, path, **kwargs)
        if response.is_success:
            return response.json()
        try:
            error = response.json().get('error') or {}
        except ValueError:
            error = {}
        raise ServerError(response.status_code, str(error.get('code', response.reason_phrase)), error)

    def claim(self, report: dict[str, Any]) -> dict[str, Any]:
        return self._call('POST', '/api/transcode/claim', json=report)

    def heartbeat(self, report: dict[str, Any]) -> dict[str, Any]:
        return self._call('POST', '/api/transcode/heartbeat', json=report)

    def upload(self, task_id: int, chunk: LocalChunk, limiter: RateLimiter, meter: RateMeter) -> dict[str, Any]:
        def body() -> Iterator[bytes]:
            with chunk.path.open('rb') as handle:
                while piece := handle.read(UPLOAD_PIECE_BYTES):
                    limiter.take(len(piece))
                    meter.add(len(piece))
                    yield piece

        return self._call(
            'PUT',
            f'/api/transcode/tasks/{task_id}/parts/{chunk.part_index}/chunks/{chunk.index}',
            content=body(),
            headers={
                'Content-Length': str(chunk.bytes),
                'Content-Type': 'application/octet-stream',
                'X-Chunk-Start': f'{chunk.start:.6f}',
                'X-Chunk-Duration': f'{chunk.duration:.6f}',
                'X-Chunk-Frames': str(chunk.frames),
                'X-Chunk-Sha256': chunk.sha256,
            },
        )

    def finish(self, task_id: int, part_index: int) -> dict[str, Any]:
        return self._call('POST', f'/api/transcode/tasks/{task_id}/parts/{part_index}/finish')


# -- one task -----------------------------------------------------------------------


@dataclass
class PartState:
    index: int
    name: str
    duration: float
    next_index: int
    resume_at: float
    fps: float = 0.0
    position: float = 0.0
    #: ffmpeg wrote the part to its end.
    encoded: bool = False
    #: The cluster has every chunk and took the part as finished.
    finished: bool = False
    error: str | None = None
    uploads: int = 0


class TaskRun:
    """Encodes and uploads one task until it is done, the worker stops, or the cluster drops it."""

    def __init__(self, config: WorkerConfig, server: Server, task: dict[str, Any], log: 'Console') -> None:
        self.config = config
        self.server = server
        self.task = task
        self.log = log
        self.task_id = int(task['id'])
        self.encoder = {**task['encoder'], **config.encoder}
        self.directory = config.cache_dir / str(self.task_id)
        self.parts = [
            PartState(
                index=int(part['index']),
                name=str(part['name']),
                duration=float(part['duration']),
                next_index=int(part['next_chunk']),
                resume_at=float(part['resume_at']),
                finished=bool(part['done']),
            )
            for part in task['parts']
        ]
        self.queue: deque[LocalChunk] = deque()
        self.queue_lock = threading.Condition()
        self.limiter = RateLimiter(config.upload_mbps * 1_000_000)
        self.meter = RateMeter()
        self.max_queue_bytes = int(config.max_queue_gb * 1024**3)
        #: Finish the current chunk, then stop encoding (Ctrl+C once, or a pause).
        self.wind_down = threading.Event()
        #: Stop everything now (Ctrl+C twice, the task taken away, the cluster out of step).
        self.abort = threading.Event()
        self.paused = bool(task.get('paused'))
        self.dropped = False
        self.fps: float | None = None
        self.processes: dict[int, subprocess.Popen[str]] = {}
        self.processes_lock = threading.Lock()

    # -- bookkeeping

    def queued_bytes(self) -> int:
        with self.queue_lock:
            return sum(chunk.bytes for chunk in self.queue)

    def report(self, state: str) -> dict[str, Any]:
        with self.queue_lock:
            chunks, size = len(self.queue), sum(chunk.bytes for chunk in self.queue)
        active = [part for part in self.parts if part.index in self.processes]
        return {
            'name': self.config.name,
            'version': VERSION,
            'task_id': self.task_id,
            'state': state,
            'parts': [{'index': part.index, 'fps': part.fps, 'position': part.position} for part in active],
            'upload_bps': self.meter.rate() * 8,
            'queue_chunks': chunks,
            'queue_bytes': size,
        }

    def enqueue(self, chunk: LocalChunk) -> None:
        with self.queue_lock:
            self.queue.append(chunk)
            self.queue_lock.notify_all()

    # -- the parts

    def run(self, stop_requested: Callable[[], int]) -> str:  # noqa: C901, PLR0912, PLR0915 - the task's main loop
        """Work the task; returns ``done``, ``stopped``, ``dropped`` or ``resync``."""
        self.directory.mkdir(parents=True, exist_ok=True)
        for part in self.parts:
            part_dir = self.directory / f'cd{part.index + 1:02d}'
            if part.finished:
                shutil.rmtree(part_dir, ignore_errors=True)
                continue
            for chunk in kept_chunks(part_dir, part.next_index, part.resume_at):
                self.enqueue(chunk)
                part.next_index, part.resume_at = chunk.index + 1, chunk.end
            if part.resume_at >= part.duration - 0.5:
                part.encoded = True
        threads = [
            threading.Thread(target=self._uploader, name='uploader', daemon=True),
            threading.Thread(target=self._heartbeat, name='heartbeat', daemon=True),
        ]
        for thread in threads:
            thread.start()
        encoders: dict[int, threading.Thread] = {}
        last_status = 0.0
        while True:
            presses = stop_requested()
            if presses >= 2:  # noqa: PLR2004 - the second Ctrl+C
                self.abort.set()
            elif presses == 1 or self.paused:
                self.wind_down.set()
            elif self.wind_down.is_set() and not self.paused:
                self.wind_down.clear()
            if self.abort.is_set():
                self._kill_all()
                break
            for index in [index for index, thread in encoders.items() if not thread.is_alive()]:
                del encoders[index]
            if not self.wind_down.is_set():
                for part in self.parts:
                    if len(encoders) >= self.config.jobs:
                        break
                    if part.encoded or part.finished or part.error or part.index in encoders:
                        continue
                    thread = threading.Thread(target=self._encode_part, args=(part,), name=f'cd{part.index + 1}')
                    encoders[part.index] = thread
                    thread.start()
            self._finish_parts()
            if all(part.finished for part in self.parts):
                break
            idle = not encoders and not self.queue
            if self.wind_down.is_set() and idle and (presses or self.dropped):
                break
            if not encoders and not self.queue and all(part.finished or part.error for part in self.parts):
                break
            now = time.monotonic()
            if now - last_status >= STATUS_SECONDS:
                last_status = now
                self.log_status()
            time.sleep(0.25)
        for thread in encoders.values():
            thread.join(timeout=5)
        self._stop_threads(threads)
        if self.dropped:
            return 'dropped'
        if all(part.finished for part in self.parts):
            shutil.rmtree(self.directory, ignore_errors=True)
            return 'done'
        if any(part.error for part in self.parts) and not stop_requested():
            return 'resync'
        return 'stopped'

    def _stop_threads(self, threads: list[threading.Thread]) -> None:
        self.abort.set()
        with self.queue_lock:
            self.queue_lock.notify_all()
        for thread in threads:
            thread.join(timeout=5)

    def _kill_all(self) -> None:
        with self.processes_lock:
            for process in self.processes.values():
                with contextlib.suppress(OSError):
                    resume(process)
                    process.kill()

    def _finish_parts(self) -> None:
        for part in self.parts:
            if not part.encoded or part.finished or part.error:
                continue
            with self.queue_lock:
                waiting = any(chunk.part_index == part.index for chunk in self.queue) or part.uploads
            if waiting:
                continue
            try:
                self.server.finish(self.task_id, part.index)
            except ServerError as exc:
                part.error = f'集群不收这一盘：{exc.code} {exc.details}'
                self.log(f'cd{part.index + 1}: {part.error}')
                continue
            except httpx.HTTPError as exc:
                self.log(f'cd{part.index + 1}: 报告完成失败，稍后重试：{exc}')
                continue
            part.finished = True
            shutil.rmtree(self.directory / f'cd{part.index + 1:02d}', ignore_errors=True)
            self.log(f'cd{part.index + 1} 转完并已全部传到集群')

    def _frame_rate(self) -> float:
        if self.fps is None:
            self.fps = probe_fps(self.server.part_url(self.task_id, self.parts[0].index), self.config)
        return self.fps

    def _encode_part(self, part: PartState) -> None:  # noqa: C901, PLR0912, PLR0915 - one loop per ffmpeg run
        directory = self.directory / f'cd{part.index + 1:02d}'
        directory.mkdir(parents=True, exist_ok=True)
        failures = 0
        while not self.abort.is_set() and not self.wind_down.is_set() and not part.encoded:
            try:
                fps = self._frame_rate()
            except (RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
                self.log(f'cd{part.index + 1}: 读不到帧率：{exc}')
                failures += 1
                if self._rest(failures):
                    return
                continue
            # Half a frame early, so the frame the last chunk stopped before is the first one decoded.
            seek = max(part.resume_at - 0.5 / fps, 0.0) if part.resume_at > 0 else 0.0
            listing = directory / f'list-{part.next_index:05d}.csv'
            listing.unlink(missing_ok=True)
            command = encode_command(
                self.config,
                url=self.server.part_url(self.task_id, part.index),
                encoder=self.encoder,
                video_bits=int(self.task['video_bitrate']),
                fps=fps,
                seek=seek,
                chunk_seconds=int(self.task['chunk_seconds']),
                first_index=part.next_index,
                directory=directory,
                listing=listing,
            )
            process = subprocess.Popen(  # noqa: S603 - fixed binary, argument list
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                errors='replace',
                **_process_flags(),
            )
            with self.processes_lock:
                self.processes[part.index] = process
            tail: deque[str] = deque(maxlen=10)
            readers = [
                threading.Thread(target=self._read_progress, args=(process, part, seek), daemon=True),
                threading.Thread(target=_drain, args=(process.stderr, tail), daemon=True),
            ]
            for reader in readers:
                reader.start()
            clock = RunClock(anchor=part.resume_at)
            seen = 0
            suspended = False
            stopping = False
            while True:
                code = process.poll()
                seen = self._collect(part, listing, clock, seen)
                if code is not None:
                    break
                if self.abort.is_set():
                    process.kill()
                    continue
                if self.wind_down.is_set() and not stopping:
                    stopping = True
                    boundary = seen
                    if suspended:
                        resume(process)
                        suspended = False
                if stopping and seen > boundary:
                    # The chunk under way when the stop came is written: that is all that was owed.
                    process.kill()
                    continue
                full = self.queued_bytes() >= self.max_queue_bytes
                if full and not suspended and not stopping:
                    suspend(process)
                    suspended = True
                elif suspended and self.queued_bytes() < self.max_queue_bytes // 2:
                    resume(process)
                    suspended = False
                time.sleep(0.5)
            for reader in readers:
                reader.join(timeout=5)
            seen = self._collect(part, listing, clock, seen)
            with self.processes_lock:
                self.processes.pop(part.index, None)
            part.fps = 0.0
            # Whatever ffmpeg left past the last listed chunk is half written.
            for leftover in directory.glob('chunk-*.mkv'):
                if not leftover.with_suffix('.json').exists():
                    leftover.unlink(missing_ok=True)
            listing.unlink(missing_ok=True)
            if code == 0 and not stopping and not self.abort.is_set():
                part.encoded = True
                return
            if stopping or self.abort.is_set():
                return
            failures += 1
            self.log(
                f'cd{part.index + 1}: ffmpeg 退出（{code}）：{" | ".join(tail) or "没有输出"}，'
                f'从第 {part.next_index} 块重来'
            )
            if self._rest(failures):
                return

    def _rest(self, failures: int) -> bool:
        """Wait before another try; True when the task is being stopped meanwhile."""
        delay = RETRY_DELAYS[min(failures, len(RETRY_DELAYS)) - 1]
        deadline = time.monotonic() + delay
        while time.monotonic() < deadline:
            if self.abort.is_set() or self.wind_down.is_set():
                return True
            time.sleep(0.5)
        return False

    def _read_progress(self, process: subprocess.Popen[str], part: PartState, seek: float) -> None:
        assert process.stdout is not None  # noqa: S101 - set by stdout=PIPE
        for line in process.stdout:
            key, _, value = line.strip().partition('=')
            if key == 'fps':
                with contextlib.suppress(ValueError):
                    part.fps = float(value)
            elif key == 'out_time_us' and value.isdigit():
                part.position = seek + int(value) / 1_000_000

    def _collect(self, part: PartState, listing: Path, clock: RunClock, seen: int) -> int:
        """Pick up the chunks ffmpeg listed since ``seen``; returns how many it has listed."""
        try:
            rows = list(csv.reader(listing.read_text(encoding='utf-8').splitlines()))
        except OSError:
            return seen
        for row in rows[seen:]:
            if len(row) < 3:  # noqa: PLR2004 - name, start, end
                return seen
            name, start, end = row[0], float(row[1]), float(row[2])
            path = listing.parent / Path(name).name
            index = int(path.stem.rsplit('-', 1)[1])
            if index != part.next_index:
                self.log(f'cd{part.index + 1}: 块编号对不上（{index} != {part.next_index}），丢弃这次输出')
                return len(rows)
            try:
                if clock.shift is None:
                    clock.shift = first_pts(path, self.config.ffprobe)
                begin = clock.anchor + max(start - clock.shift, 0.0)
                begin = part.resume_at if abs(begin - part.resume_at) <= 0.25 else begin  # noqa: PLR2004
                chunk = LocalChunk(
                    part_index=part.index,
                    index=index,
                    start=begin,
                    duration=clock.anchor + (end - clock.shift) - begin,
                    frames=count_frames(path, self.config.ffprobe),
                    bytes=path.stat().st_size,
                    sha256=sha256_of(path),
                    path=path,
                )
            except (OSError, RuntimeError):
                # Not readable yet, or broken: look again next time; once ffmpeg is gone it is
                # left without a sidecar and thrown away, and the part goes on from here.
                return seen
            chunk.save()
            part.next_index, part.resume_at = index + 1, chunk.end
            seen += 1
            self.enqueue(chunk)
        return seen

    # -- uploads and heartbeats

    def _uploader(self) -> None:
        failures = 0
        while not self.abort.is_set():
            with self.queue_lock:
                while not self.queue and not self.abort.is_set():
                    self.queue_lock.wait(timeout=1.0)
                if self.abort.is_set():
                    return
                chunk = self.queue[0]
            part = self.parts_by_index(chunk.part_index)
            part.uploads += 1
            try:
                self.server.upload(self.task_id, chunk, self.limiter, self.meter)
            except ServerError as exc:
                part.uploads -= 1
                if exc.code in {'transcode_task_not_active', 'transcode_task_not_found'}:
                    self.log('集群那边这部作品已经不在转码了（取消或改用了 CPU），停下这部')
                    self.dropped = True
                    self.abort.set()
                    return
                if exc.status == 400:  # noqa: PLR2004 - a corrupt upload: send it again
                    continue
                self.log(f'cd{chunk.part_index + 1} 第 {chunk.index} 块被拒：{exc.code} {exc.details}，重新同步')
                part.error = exc.code
                with self.queue_lock:
                    self.queue = deque(item for item in self.queue if item.part_index != chunk.part_index)
                chunk.remove()
                continue
            except (httpx.HTTPError, OSError) as exc:
                part.uploads -= 1
                failures += 1
                delay = RETRY_DELAYS[min(failures, len(RETRY_DELAYS)) - 1]
                self.log(f'上传失败，{delay:.0f} 秒后重试：{exc}')
                self.abort.wait(delay)
                continue
            failures = 0
            with self.queue_lock:
                if self.queue and self.queue[0] is chunk:
                    self.queue.popleft()
                self.queue_lock.notify_all()
            chunk.remove()
            part.uploads -= 1

    def parts_by_index(self, index: int) -> PartState:
        return next(part for part in self.parts if part.index == index)

    def _heartbeat(self) -> None:
        while not self.abort.wait(HEARTBEAT_SECONDS):
            state = 'paused' if self.paused else ('stopping' if self.wind_down.is_set() else 'encoding')
            try:
                answer = self.server.heartbeat(self.report(state))
            except (ServerError, httpx.HTTPError):
                continue
            if not answer.get('active'):
                self.log('集群那边这部作品已经不在转码了（取消或改用了 CPU），停下这部')
                self.dropped = True
                self.abort.set()
                return
            paused = bool(answer.get('paused'))
            if paused != self.paused:
                self.log('网页上点了暂停：写完当前小块后暂停编码' if paused else '网页上点了继续')
            self.paused = paused

    def log_status(self) -> None:
        active = [part for part in self.parts if part.index in self.processes]
        pieces = [
            f'cd{part.index + 1} {part.position / part.duration:.0%} {part.fps:.0f}fps'
            for part in active
            if part.duration
        ]
        done = sum(part.finished for part in self.parts)
        total = sum(part.duration for part in self.parts) or 1.0
        encoded = sum(min(part.resume_at, part.duration) for part in self.parts)
        with self.queue_lock:
            chunks, size = len(self.queue), sum(chunk.bytes for chunk in self.queue)
        state = ''
        if self.paused:
            state = '已暂停 | '
        elif self.wind_down.is_set():
            state = '收尾中（再按 Ctrl+C 立即退出）| '
        line = (
            f'[{self.task["avid"]}] {state}{" | ".join(pieces) or "没有在编码"} | 完成 {done}/{len(self.parts)} 盘'
            f' | 上传 {self.meter.rate() * 8 / 1e6:.1f} Mbps，队列 {chunks} 块 {size / 1024**3:.2f} GB'
            f' | 总进度 {encoded / total:.1%}'
        )
        self.log(line, status=True)


# -- the terminal ----------------------------------------------------------------------


class Console:
    """Status on one line that keeps being rewritten; events on lines of their own above it."""

    def __init__(self) -> None:
        self._tty = sys.stdout.isatty()
        self._lock = threading.Lock()
        self._status = ''

    def __call__(self, message: str, *, status: bool = False) -> None:
        with self._lock:
            if status:
                self._status = message
                if self._tty:
                    sys.stdout.write('\r\x1b[K' + message[: shutil.get_terminal_size().columns - 1])
                else:
                    sys.stdout.write(message + '\n')
            else:
                stamp = time.strftime('%H:%M:%S')
                if self._tty:
                    sys.stdout.write('\r\x1b[K')
                sys.stdout.write(f'{stamp} {message}\n')
                if self._tty and self._status:
                    sys.stdout.write(self._status[: shutil.get_terminal_size().columns - 1])
            sys.stdout.flush()


class StopSignal:
    """Counts Ctrl+C presses instead of letting the first one kill the worker."""

    def __init__(self, log: Callable[[str], None]) -> None:
        self.presses = 0
        self._log = log

    def install(self) -> None:
        signal.signal(signal.SIGINT, self._handle)
        if hasattr(signal, 'SIGBREAK'):
            signal.signal(signal.SIGBREAK, self._handle)

    def _handle(self, _signum: int, _frame: object) -> None:
        self.presses += 1
        if self.presses == 1:
            self._log('收到 Ctrl+C：写完当前小块、传完队列后退出（再按一次立即退出）')
        else:
            self._log('再次收到 Ctrl+C：立即退出，下次启动从集群收到的最后一块接着做')

    def __call__(self) -> int:
        return self.presses


def idle_report(config: WorkerConfig) -> dict[str, Any]:
    return {'name': config.name, 'version': VERSION, 'task_id': None, 'state': 'waiting', 'parts': []}


def check_tools(config: WorkerConfig) -> str | None:
    for tool in (config.ffmpeg, config.ffprobe):
        if shutil.which(tool) is None and not Path(tool).is_file():
            return f'找不到 {tool}，请先安装 ffmpeg（winget install Gyan.FFmpeg）或用 --ffmpeg 指定路径'
    return None


def main(args: argparse.Namespace) -> int:
    config = load_config(args)
    log = Console()
    problem = check_tools(config)
    if problem:
        log(problem)
        return 2
    stop = StopSignal(log)
    stop.install()
    server = Server(config)
    config.cache_dir.mkdir(parents=True, exist_ok=True)
    log(
        f'transcode-worker {VERSION} 连接 {config.server}，缓存 {config.cache_dir}，'
        f'上传上限 {config.upload_mbps:g} Mbps'
    )
    failures = 0
    last_wait = ''
    try:
        while not stop():
            try:
                answer = server.claim(idle_report(config))
            except (ServerError, httpx.HTTPError) as exc:
                failures += 1
                delay = RETRY_DELAYS[min(failures, len(RETRY_DELAYS)) - 1]
                log(f'连不上集群（{exc}），{delay:.0f} 秒后重试')
                _sleep(delay, stop)
                continue
            failures = 0
            task = answer.get('task')
            if task is None:
                reason = WAIT_REASONS.get(str(answer.get('wait')), str(answer.get('wait')))
                if reason != last_wait:
                    log(f'等待：{reason}')
                    last_wait = reason
                _sleep(IDLE_POLL_SECONDS if answer.get('wait') != 'probing' else 10.0, stop)
                continue
            last_wait = ''
            for stale in config.cache_dir.iterdir():
                if stale.is_dir() and stale.name != str(task['id']):
                    shutil.rmtree(stale, ignore_errors=True)
            left = sum(not part['done'] for part in task['parts'])
            log(
                f'开始 {task["avid"]}：{len(task["parts"])} 盘，还剩 {left} 盘，'
                f'视频码率 {task["video_bitrate"] / 1e6:.2f} Mbps'
            )
            outcome = TaskRun(config, server, task, log).run(stop)
            if outcome == 'done':
                log(f'{task["avid"]} 全部分盘已传到集群，接下来由集群拼接、上传 115')
            elif outcome == 'resync':
                _sleep(10.0, stop)
    finally:
        server.close()
        log('已退出')
    return 0


def _sleep(seconds: float, stop: Callable[[], int]) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and not stop():
        time.sleep(0.25)
