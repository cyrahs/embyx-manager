"""The merge Job's body: concatenate one title's parts into a single file, losslessly.

Runs as ``embyx-manager merge-worker`` inside a Kubernetes Job of its own, so a
long merge neither starves the web app nor dies with it. It reads the parts
through the CloudDrive mount, writes the merged file next to the uploads
CloudDrive will pick up, and reports through a small JSON status file on the
same volume: the app polls that file, and the Job carries no database access or
secrets.

The parts are checked before anything is written: they must exist, fit on the
volume with a reserve to spare, and carry the same streams, since ``-c copy``
cannot reconcile different codecs or resolutions. The result is checked after:
its duration must match the parts' sum. Only then is it renamed into place, so
the app never sees a half-written file under the final name.
"""

import hashlib
import json
import os
import shutil
import subprocess
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

#: The containers a merge can write, by file suffix; the parts' own container is kept.
MUXERS = {
    '.mp4': 'mp4',
    '.m4v': 'mp4',
    '.mov': 'mov',
    '.mkv': 'matroska',
    '.ts': 'mpegts',
    '.avi': 'avi',
    '.wmv': 'asf',
}
#: Stream properties that must agree across parts for a stream copy to play through.
SIGNATURE_KEYS = ('codec_type', 'codec_name', 'width', 'height', 'sample_rate', 'channels')
#: How far the merged duration may drift from the parts' sum: container rounding per part.
DURATION_TOLERANCE_SECONDS = 2.0
DURATION_TOLERANCE_SHARE = 0.002
STATUS_INTERVAL_SECONDS = 5.0
HASH_CHUNK_BYTES = 8 * 1024 * 1024
GIB = 1024**3
#: ffmpeg's stderr is kept to this many trailing lines for the failure report.
STDERR_TAIL_LINES = 20


class MergeWorkerError(Exception):
    """A reason the merge cannot go on, worded for the operator."""


@dataclass(frozen=True)
class Probe:
    duration: float
    signature: tuple[tuple[object, ...], ...]


def partial_path(output: Path) -> Path:
    """Where the merge is written until it has been checked."""
    return output.with_name(f'.{output.name}.part')


class StatusFile:
    """The worker's progress, replaced atomically so a reader never sees half a write."""

    def __init__(self, path: Path, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._path = path
        self._clock = clock
        self._last_write = float('-inf')
        self._fields: dict[str, Any] = {'state': 'running'}

    def update(self, *, force: bool = True, **fields: Any) -> None:
        self._fields.update(fields)
        now = self._clock()
        if not force and now - self._last_write < STATUS_INTERVAL_SECONDS:
            return
        self._last_write = now
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {**self._fields, 'updated_at': datetime.now(UTC).isoformat()}
        temporary = self._path.with_name(f'.{self._path.name}.tmp')
        temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
        temporary.replace(self._path)


def read_status(path: Path) -> dict[str, Any] | None:
    """The last status a worker wrote, or None when there is none or it cannot be read."""
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def probe(path: Path, *, ffprobe: str = 'ffprobe') -> Probe:
    completed = subprocess.run(  # noqa: S603 - fixed binary, argument list
        [ffprobe, '-v', 'error', '-print_format', 'json', '-show_format', '-show_streams', str(path)],
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        message = completed.stderr.decode('utf-8', 'replace').strip().splitlines()
        msg = f'ffprobe cannot read {path.name}: {message[-1] if message else "no output"}'
        raise MergeWorkerError(msg)
    try:
        info = json.loads(completed.stdout)
        duration = float(info['format']['duration'])
    except (ValueError, KeyError, TypeError) as exc:
        msg = f'ffprobe reports no duration for {path.name}'
        raise MergeWorkerError(msg) from exc
    streams = [
        tuple(stream.get(key) for key in SIGNATURE_KEYS)
        for stream in info.get('streams', [])
        if stream.get('codec_type') in {'video', 'audio'}
    ]
    if not any(stream[0] == 'video' for stream in streams):
        msg = f'{path.name} has no video stream'
        raise MergeWorkerError(msg)
    return Probe(duration=duration, signature=tuple(streams))


def concat_list(parts: Sequence[Path]) -> str:
    """An ffmpeg concat-demuxer script naming each part; quotes are escaped the demuxer's way."""
    lines = []
    for part in parts:
        quoted = str(part).replace("'", "'\\''")
        lines.append(f"file '{quoted}'")
    return '\n'.join(lines) + '\n'


def sha1_of(path: Path, *, on_progress: Callable[[int], None] | None = None) -> str:
    digest = hashlib.sha1()  # noqa: S324 - 115 identifies files by SHA-1, so that is what is compared
    done = 0
    with path.open('rb') as handle:
        while chunk := handle.read(HASH_CHUNK_BYTES):
            digest.update(chunk)
            done += len(chunk)
            if on_progress is not None:
                on_progress(done)
    return digest.hexdigest().upper()


def _check_space(output: Path, needed: int, reserve_bytes: int) -> None:
    usage = shutil.disk_usage(output.parent)
    # A leftover from an interrupted attempt is about to be overwritten.
    reclaimable = 0
    with suppress(FileNotFoundError):
        reclaimable = partial_path(output).stat().st_size
    available = usage.free + reclaimable
    if available < needed + reserve_bytes:
        msg = (
            f'not enough space in {output.parent}: {available / GIB:.1f} GiB free, '
            f'{needed / GIB:.1f} GiB needed plus a {reserve_bytes / GIB:.0f} GiB reserve'
        )
        raise MergeWorkerError(msg)


def _check_parts(parts: Sequence[Path], output: Path) -> int:
    if len(parts) < 2:  # noqa: PLR2004 - one part is not a merge
        msg = 'a merge needs at least two parts'
        raise MergeWorkerError(msg)
    suffixes = {part.suffix.lower() for part in parts}
    if suffixes != {output.suffix.lower()}:
        msg = f'parts and output must share one container, got {sorted(suffixes)}'
        raise MergeWorkerError(msg)
    if output.suffix.lower() not in MUXERS:
        msg = f'cannot write a {output.suffix} container'
        raise MergeWorkerError(msg)
    total = 0
    for part in parts:
        try:
            total += part.stat().st_size
        except OSError as exc:
            msg = f'cannot read {part}: {exc.strerror or exc}'
            raise MergeWorkerError(msg) from exc
    return total


def _run_ffmpeg(
    command: list[str],
    *,
    duration: float,
    status: StatusFile,
) -> None:
    with subprocess.Popen(  # noqa: S603 - fixed binary, argument list
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as process:
        assert process.stdout is not None  # noqa: S101 - set by stdout=PIPE
        assert process.stderr is not None  # noqa: S101 - set by stderr=PIPE
        for line in process.stdout:
            key, _, value = line.strip().partition('=')
            if key == 'out_time_us' and value.isdigit() and duration > 0:
                status.update(force=False, progress=min(int(value) / 1_000_000 / duration, 1.0))
            elif key == 'total_size' and value.isdigit():
                status.update(force=False, written_bytes=int(value))
        stderr = process.stderr.read()
        returncode = process.wait()
    if returncode != 0:
        tail = stderr.strip().splitlines()[-STDERR_TAIL_LINES:]
        raise MergeWorkerError('ffmpeg failed: ' + (' | '.join(tail) if tail else f'exit {returncode}'))


def merge(  # noqa: PLR0913
    parts: Sequence[Path],
    output: Path,
    status: StatusFile,
    *,
    reserve_bytes: int,
    workdir: Path,
    ffmpeg: str = 'ffmpeg',
    ffprobe: str = 'ffprobe',
) -> dict[str, Any]:
    """Merge ``parts`` into ``output``; returns the final status fields."""
    status.update(phase='checking', progress=0.0)
    total_bytes = _check_parts(parts, output)
    _check_space(output, total_bytes, reserve_bytes)
    status.update(total_bytes=total_bytes)

    status.update(phase='probing')
    probes = []
    for index, part in enumerate(parts, start=1):
        probes.append(probe(part, ffprobe=ffprobe))
        status.update(force=False, progress=index / len(parts))
    first = probes[0].signature
    for part, result in zip(parts, probes, strict=True):
        if result.signature != first:
            msg = f'{part.name} carries different streams than {parts[0].name}; a stream copy would break'
            raise MergeWorkerError(msg)
    expected = sum(result.duration for result in probes)

    status.update(phase='merging', progress=0.0, duration=expected)
    target = partial_path(output)
    script = workdir / 'concat.txt'
    script.write_text(concat_list(parts), encoding='utf-8')
    _run_ffmpeg(
        [
            ffmpeg,
            '-hide_banner',
            '-nostdin',
            '-loglevel',
            'error',
            '-y',
            '-f',
            'concat',
            '-safe',
            '0',
            '-i',
            str(script),
            '-map',
            '0:v',
            '-map',
            '0:a?',
            '-c',
            'copy',
            '-f',
            MUXERS[output.suffix.lower()],
            '-progress',
            'pipe:1',
            '-nostats',
            str(target),
        ],
        duration=expected,
        status=status,
    )

    status.update(phase='verifying', progress=1.0)
    merged = probe(target, ffprobe=ffprobe)
    tolerance = max(DURATION_TOLERANCE_SECONDS, expected * DURATION_TOLERANCE_SHARE)
    if abs(merged.duration - expected) > tolerance:
        msg = f'merged duration {merged.duration:.1f}s does not match the parts ({expected:.1f}s)'
        raise MergeWorkerError(msg)

    size = target.stat().st_size
    status.update(phase='hashing', progress=0.0, size=size)
    digest = sha1_of(target, on_progress=lambda done: status.update(force=False, progress=done / size if size else 1.0))
    target.replace(output)
    return {'size': size, 'sha1': digest, 'duration': merged.duration}


def run(argv_parts: Sequence[str], *, output: str, status_path: str, reserve_gib: float) -> int:
    """The ``merge-worker`` subcommand: exit 0 once the merged file is in place."""
    status = StatusFile(Path(status_path))
    output_path = Path(output)
    workdir = Path(os.environ.get('TMPDIR', '/tmp'))  # noqa: S108 - the Job mounts an emptyDir there
    try:
        result = merge(
            [Path(part) for part in argv_parts],
            output_path,
            status,
            reserve_bytes=int(reserve_gib * GIB),
            workdir=workdir,
        )
    except (MergeWorkerError, OSError) as exc:
        partial_path(output_path).unlink(missing_ok=True)
        status.update(state='failed', error=str(exc))
        return 1
    status.update(state='done', phase='done', progress=1.0, **result)
    return 0
