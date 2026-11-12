"""The merge Job's body: concatenate one title's parts into a single file.

Runs as ``embyx-manager merge-worker`` inside a Kubernetes Job of its own, so a
long merge neither starves the web app nor dies with it. It reads the parts
through the CloudDrive mount, writes the merged file next to the uploads
CloudDrive will pick up, and reports through a small JSON status file on the
same volume: the app polls that file, and the Job carries no database access or
secrets.

The parts are checked before anything is written: they must exist, fit on the
volume with a reserve to spare, and carry the same streams, since ``-c copy``
cannot reconcile different codecs or resolutions. Parts that carry the same
streams in a different order are first remuxed, video first, into a staging
directory next to the output, as the concat demuxer pairs streams by index. An
MP4 output's index stays in memory until ffmpeg ends, so a title with more
packets than the Job's memory limit can index is refused up front rather than
killed hours in.

A merge is a lossless stream copy unless the parts add up to more than 115
accepts in one file: then the video is re-encoded to AV1 (SVT-AV1) at a
bitrate worked out from the duration, so the result fits, while audio is still
copied. Only MP4 and Matroska outputs are re-encoded, as MOV cannot carry AV1;
other containers are refused as before. The result is checked after: its duration must match the parts' sum,
and a re-encode must have come in under the limit. Only then is it renamed into
place, so the app never sees a half-written file under the final name.
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable, Sequence
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
#: ffmpeg is stopped once neither its output nor its position has moved for this long.
STALL_SECONDS = 15 * 60
#: The largest single file 115 accepts ("单文件不能大于115GB"); a bigger merge could never be uploaded.
MAX_UPLOAD_BYTES = 115 * 10**9
#: What a re-encode aims for: this share of 115's limit, leaving room for the
#: container and for the encoder overshooting its average bitrate.
TRANSCODE_BUDGET_SHARE = 0.9
#: And no more than this share of the parts' own video bitrate. Measured on a 4K60
#: VR clip (UNVRSP-003, 12 Mb/s H.264): the image's SVT-AV1 with the settings below
#: scored VMAF 93.1 (min 89) at 40%, as x265 "faster" did at 60% (93.5), so a
#: re-encode keeps the picture and the file comes out about a third smaller than an
#: HEVC one would.
TRANSCODE_SOURCE_SHARE = 0.4
#: Assumed for an audio stream whose container does not state its bitrate.
FALLBACK_AUDIO_BITS = 320_000
#: The muxers a re-encoded AV1 stream may go into (MOV cannot carry AV1).
TRANSCODE_MUXERS = frozenset({'mp4', 'matroska'})
#: SVT-AV1's speed preset and level of parallelism. The image's SVT-AV1 (2.3) buffers
#: more 4K frames per level: on that clip lp=2 peaked at 1.8 GiB and ran at 12 fps
#: (x265 "faster" managed 9.5 on three cores), lp=3 passed 2.3 GiB and lp=4 2.6 GiB,
#: too much for the Job's 3 GiB once the MP4 index is added.
TRANSCODE_PRESET = '8'
TRANSCODE_THREADS = 2
#: SVT-AV1's lookahead. 2.3 raises 0 to the least the preset allows (25 frames at
#: preset 8); its default is longer and took lp=2 past 2.6 GiB.
TRANSCODE_LOOKAHEAD = 0
#: SVT-AV1's and the decoder's memory for a frame of TRANSCODE_REFERENCE_PIXELS,
#: on top of the muxer's index; scaled by frame size, never below the floor.
TRANSCODE_MEMORY_BYTES = 2 * 1024**3
TRANSCODE_REFERENCE_PIXELS = 4096 * 2048
TRANSCODE_MEMORY_FLOOR_BYTES = 512 * 1024**2
#: The muxers that hold an entry per packet in memory until they write the index at the end.
INDEXED_MUXERS = frozenset({'mp4', 'mov'})
#: Memory per packet held by those muxers (ffmpeg 6.1 measured ~70 bytes, growth included),
#: and ffmpeg's and the worker's own baseline.
INDEX_BYTES_PER_PACKET = 80
INDEX_BASELINE_BYTES = 192 * 1024**2
#: The Job container's memory limit under cgroup v2: a byte count, or ``max``.
CGROUP_MEMORY_MAX = Path('/sys/fs/cgroup/memory.max')


class MergeWorkerError(Exception):
    """A reason the merge cannot go on, worded for the operator."""


class MergeRefusedError(MergeWorkerError):
    """The parts can never be merged by a stream copy, however often the Job runs.

    The check did its job, so the Job still ends cleanly: the status file tells
    the app the merge is off, and only a crash or a broken run fails the Job.
    """


@dataclass(frozen=True)
class Probe:
    duration: float
    #: The video and audio streams' properties, in the file's order.
    signature: tuple[tuple[object, ...], ...]
    #: Every stream's type in index order, which the concat demuxer pairs streams by.
    layout: tuple[str, ...] = ()
    #: Video and audio packets, when the container counts them (MP4 does, Matroska does not).
    packets: int | None = None
    #: The audio streams' bitrate in bits per second, as the container states it or assumed.
    audio_bits: int = 0

    def by_kind(self) -> tuple[tuple[object, ...], ...]:
        """The signature with video before audio, which a remux with ``-map 0:v -map 0:a`` yields."""
        return tuple(sorted(self.signature, key=lambda stream: stream[0] != 'video'))

    def in_order(self) -> bool:
        """Whether the file holds nothing but its video streams followed by its audio streams."""
        return self.layout == tuple(stream[0] for stream in self.by_kind())


def partial_path(output: Path) -> Path:
    """Where the merge is written until it has been checked."""
    return output.with_name(f'.{output.name}.part')


def staging_dir(output: Path) -> Path:
    """Where parts remuxed into stream order are kept while the merge runs."""
    return output.with_name(f'.{output.name}.parts')


def memory_limit(path: Path = CGROUP_MEMORY_MAX) -> int | None:
    """The container's memory limit in bytes, or None when it has none or it cannot be read."""
    try:
        value = path.read_text(encoding='utf-8').strip()
    except OSError:
        return None
    return int(value) if value.isdigit() else None


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
    every = info.get('streams', [])
    media = [stream for stream in every if stream.get('codec_type') in {'video', 'audio'}]
    streams = [tuple(stream.get(key) for key in SIGNATURE_KEYS) for stream in media]
    if not any(stream[0] == 'video' for stream in streams):
        msg = f'{path.name} has no video stream'
        raise MergeRefusedError(msg)
    counts = [str(stream.get('nb_frames', '')) for stream in media]
    audio_rates = [str(stream.get('bit_rate', '')) for stream in media if stream.get('codec_type') == 'audio']
    return Probe(
        duration=duration,
        signature=tuple(streams),
        layout=tuple(str(stream.get('codec_type')) for stream in every),
        packets=sum(map(int, counts)) if all(count.isdigit() for count in counts) else None,
        audio_bits=sum(int(rate) if rate.isdigit() else FALLBACK_AUDIO_BITS for rate in audio_rates),
    )


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
        raise MergeRefusedError(msg)
    suffixes = {part.suffix.lower() for part in parts}
    if suffixes != {output.suffix.lower()}:
        msg = f'parts and output must share one container, got {sorted(suffixes)}'
        raise MergeRefusedError(msg)
    if output.suffix.lower() not in MUXERS:
        msg = f'cannot write a {output.suffix} container'
        raise MergeRefusedError(msg)
    total = 0
    for part in parts:
        try:
            total += part.stat().st_size
        except OSError as exc:
            msg = f'cannot read {part}: {exc.strerror or exc}'
            raise MergeWorkerError(msg) from exc
    if total > MAX_UPLOAD_BYTES and MUXERS[output.suffix.lower()] not in TRANSCODE_MUXERS:
        msg = (
            f'the parts add up to {total / 10**9:.1f} GB, more than the {MAX_UPLOAD_BYTES // 10**9} GB '
            f'115 accepts in one file, and a {output.suffix} merge cannot be re-encoded to fit'
        )
        raise MergeRefusedError(msg)
    return total


def transcode_bitrate(total_bytes: int, duration: float, audio_bits: int) -> int:
    """The video bitrate, in bits per second, that brings a re-encode under 115's limit.

    The lower of what the budget leaves after the copied audio and a share of
    the parts' own video bitrate.
    """
    budget = MAX_UPLOAD_BYTES * TRANSCODE_BUDGET_SHARE * 8 / duration - audio_bits
    source = max(total_bytes * 8 / duration - audio_bits, 0) * TRANSCODE_SOURCE_SHARE
    bits = int(min(budget, source))
    if bits <= 0:
        msg = f'the audio alone leaves no room for video under {MAX_UPLOAD_BYTES // 10**9} GB'
        raise MergeRefusedError(msg)
    return bits


def transcode_memory(result: Probe) -> int:
    """The encoder's and decoder's memory, in bytes, for a re-encode of ``result``'s video."""
    pixels = 0
    for stream in result.signature:
        width, height = stream[2], stream[3]
        if stream[0] == 'video' and isinstance(width, int) and isinstance(height, int):
            pixels = max(pixels, width * height)
    if not pixels:
        return TRANSCODE_MEMORY_BYTES
    return max(TRANSCODE_MEMORY_FLOOR_BYTES, TRANSCODE_MEMORY_BYTES * pixels // TRANSCODE_REFERENCE_PIXELS)


def _check_memory(probes: Sequence[Probe], muxer: str, limit: int | None, *, transcode: bool = False) -> None:
    if muxer not in INDEXED_MUXERS or limit is None or any(result.packets is None for result in probes):
        return
    packets = sum(result.packets or 0 for result in probes)
    needed = packets * INDEX_BYTES_PER_PACKET + INDEX_BASELINE_BYTES
    if transcode:
        needed += transcode_memory(probes[0])
    if needed > limit:
        msg = (
            f'the parts hold {packets:,} packets, whose {muxer} index needs about {needed / GIB:.1f} GiB '
            f'of memory, more than the Job is allowed ({limit / GIB:.1f} GiB)'
        )
        raise MergeRefusedError(msg)


def _keep_tail(stream: Iterable[str], tail: deque[str]) -> None:
    """Read ``stream`` to its end, keeping only its last lines.

    ffmpeg's stderr has to be read while it runs: a part with a damaged stretch
    can make the demuxer log an error every few bytes, and once the pipe's
    64 KiB fill up ffmpeg stops mid-write while the worker waits on stdout.
    """
    for line in stream:
        if line.strip():
            tail.append(line.rstrip())


class _StallWatch:
    """Kills the process once ``advanced`` has not been called for ``stall_seconds``."""

    def __init__(
        self,
        process: subprocess.Popen[str],
        stall_seconds: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._process = process
        self._stall_seconds = stall_seconds
        self._clock = clock
        self._last = clock()
        self._done = threading.Event()
        self.fired = False
        self._thread = threading.Thread(target=self._watch, name='ffmpeg-stall-watch', daemon=True)
        self._thread.start()

    def advanced(self) -> None:
        self._last = self._clock()

    def stop(self) -> None:
        self._done.set()
        self._thread.join()

    def _watch(self) -> None:
        interval = min(max(self._stall_seconds / 10, 0.05), 30.0)
        while not self._done.wait(interval):
            if self._clock() - self._last > self._stall_seconds:
                self.fired = True
                self._process.kill()
                return


def _run_ffmpeg(
    command: list[str],
    *,
    duration: float,
    status: StatusFile,
    stall_seconds: float = STALL_SECONDS,
) -> None:
    tail: deque[str] = deque(maxlen=STDERR_TAIL_LINES)
    with subprocess.Popen(  # noqa: S603 - fixed binary, argument list
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        errors='replace',
    ) as process:
        assert process.stdout is not None  # noqa: S101 - set by stdout=PIPE
        assert process.stderr is not None  # noqa: S101 - set by stderr=PIPE
        reader = threading.Thread(target=_keep_tail, args=(process.stderr, tail), name='ffmpeg-stderr', daemon=True)
        reader.start()
        watch = _StallWatch(process, stall_seconds)
        # ffmpeg keeps printing progress blocks while stuck, so only a change counts as progress.
        position: tuple[str, str] = ('', '')
        try:
            for line in process.stdout:
                key, _, value = line.strip().partition('=')
                if key == 'out_time_us' and value.isdigit():
                    if value != position[0]:
                        position = (value, position[1])
                        watch.advanced()
                    if duration > 0:
                        status.update(force=False, progress=min(int(value) / 1_000_000 / duration, 1.0))
                elif key == 'total_size' and value.isdigit():
                    if value != position[1]:
                        position = (position[0], value)
                        watch.advanced()
                    status.update(force=False, written_bytes=int(value))
            returncode = process.wait()
        finally:
            watch.stop()
            if process.poll() is None:
                process.kill()
        reader.join()
    if watch.fired:
        span = f'{stall_seconds / 60:.0f} minutes' if stall_seconds >= 60 else f'{stall_seconds:.0f} seconds'  # noqa: PLR2004
        msg = f'ffmpeg made no progress for {span} and was stopped'
        raise MergeWorkerError(msg + (': ' + ' | '.join(tail) if tail else ''))
    if returncode != 0:
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
    memory_limit: int | None = None,
) -> dict[str, Any]:
    """Merge ``parts`` into ``output``; returns the final status fields."""
    status.update(phase='checking', progress=0.0)
    total_bytes = _check_parts(parts, output)
    transcode = total_bytes > MAX_UPLOAD_BYTES
    # A re-encode writes at most 115's limit, not the parts' size.
    _check_space(output, MAX_UPLOAD_BYTES if transcode else total_bytes, reserve_bytes)
    status.update(total_bytes=total_bytes)
    muxer = MUXERS[output.suffix.lower()]

    status.update(phase='probing')
    probes = []
    for index, part in enumerate(parts, start=1):
        probes.append(probe(part, ffprobe=ffprobe))
        status.update(force=False, progress=index / len(parts))
    first = probes[0]
    for part, result in zip(parts, probes, strict=True):
        if result.by_kind() != first.by_kind():
            msg = f'{part.name} carries different streams than {parts[0].name}; a stream copy would break'
            raise MergeRefusedError(msg)
    _check_memory(probes, muxer, memory_limit, transcode=transcode)
    expected = sum(result.duration for result in probes)
    video_bits = transcode_bitrate(total_bytes, expected, first.audio_bits) if transcode else None

    # Same streams in another order: put every part that strays video first.
    stray = []
    if any(result.layout != first.layout for result in probes):
        stray = [index for index, result in enumerate(probes) if not result.in_order()]
        written = MAX_UPLOAD_BYTES if transcode else total_bytes
        _check_space(output, written + sum(parts[index].stat().st_size for index in stray), reserve_bytes)
    staging = staging_dir(output)
    target = partial_path(output)
    inputs = list(parts)
    try:
        for index in stray:
            status.update(phase='reordering', progress=0.0)
            staging.mkdir(exist_ok=True)
            inputs[index] = staging / parts[index].name
            _run_ffmpeg(
                [
                    ffmpeg,
                    '-hide_banner',
                    '-nostdin',
                    '-loglevel',
                    'error',
                    '-y',
                    '-i',
                    str(parts[index]),
                    '-map',
                    '0:v',
                    '-map',
                    '0:a?',
                    '-c',
                    'copy',
                    '-f',
                    muxer,
                    '-progress',
                    'pipe:1',
                    '-nostats',
                    str(inputs[index]),
                ],
                duration=probes[index].duration,
                status=status,
            )
        _concat(inputs, target, muxer, expected, status, workdir=workdir, ffmpeg=ffmpeg, video_bits=video_bits)
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    status.update(phase='verifying', progress=1.0)
    merged = probe(target, ffprobe=ffprobe)
    tolerance = max(DURATION_TOLERANCE_SECONDS, expected * DURATION_TOLERANCE_SHARE)
    if abs(merged.duration - expected) > tolerance:
        msg = f'merged duration {merged.duration:.1f}s does not match the parts ({expected:.1f}s)'
        raise MergeWorkerError(msg)

    size = target.stat().st_size
    if size > MAX_UPLOAD_BYTES:
        msg = (
            f'the re-encoded file came to {size / 10**9:.1f} GB, still more than the '
            f'{MAX_UPLOAD_BYTES // 10**9} GB 115 accepts in one file'
        )
        raise MergeRefusedError(msg)
    status.update(phase='hashing', progress=0.0, size=size)
    digest = sha1_of(target, on_progress=lambda done: status.update(force=False, progress=done / size if size else 1.0))
    target.replace(output)
    return {'size': size, 'sha1': digest, 'duration': merged.duration}


def _concat(  # noqa: PLR0913
    parts: Sequence[Path],
    target: Path,
    muxer: str,
    expected: float,
    status: StatusFile,
    *,
    workdir: Path,
    ffmpeg: str,
    video_bits: int | None = None,
) -> None:
    """Concatenate ``parts`` by stream copy, or re-encode the video to AV1 at ``video_bits``."""
    if video_bits is None:
        status.update(phase='merging', progress=0.0, duration=expected)
        codecs = ['-c', 'copy']
    else:
        status.update(phase='transcoding', progress=0.0, duration=expected, video_bitrate=video_bits)
        codecs = [
            '-c:v',
            'libsvtav1',
            '-preset',
            TRANSCODE_PRESET,
            '-b:v',
            str(video_bits),
            '-svtav1-params',
            f'lp={TRANSCODE_THREADS}:lookahead={TRANSCODE_LOOKAHEAD}',
            '-c:a',
            'copy',
        ]
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
            *codecs,
            '-f',
            muxer,
            '-progress',
            'pipe:1',
            '-nostats',
            str(target),
        ],
        duration=expected,
        status=status,
    )


def run(argv_parts: Sequence[str], *, output: str, status_path: str, reserve_gib: float) -> int:
    """The ``merge-worker`` subcommand.

    Exits 0 once the merged file is in place, and also when the parts were
    refused as unmergeable: the status file then says ``failed`` with
    ``refused`` set, which is a finished answer rather than a broken Job.
    """
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
            memory_limit=memory_limit(),
        )
    except MergeRefusedError as exc:
        partial_path(output_path).unlink(missing_ok=True)
        status.update(state='failed', refused=True, error=str(exc))
        print(f'merge refused: {exc}', file=sys.stderr)  # noqa: T201
        return 0
    except (MergeWorkerError, OSError) as exc:
        partial_path(output_path).unlink(missing_ok=True)
        status.update(state='failed', error=str(exc))
        # Also in the Job's log, for a look at a failed Job without the status file.
        print(f'merge failed: {exc}', file=sys.stderr)  # noqa: T201
        return 1
    status.update(state='done', phase='done', progress=1.0, **result)
    return 0
