"""Re-encoding a merge too big for 115 on a GPU outside the cluster.

A title whose parts add up to more than 115 accepts in one file has its video
re-encoded before the merge. The merge Job can do that itself on the CPU
(SVT-AV1, about four days for a 24 h 4K VR title); with
``merge.home_transcode_enabled`` on, the task waits instead for the home
transcode worker (``embyx-manager transcode-worker``), which runs NVENC on a
PC that never joins the cluster:

1. The worker asks for work. The oldest such task moves from queued to
   transcoding once nothing else is under way, and a Job probes the parts
   (the same checks a merge makes, and the video bitrate that fits) and then
   copies their audio onto the work volume. CloudDrive downloads a whole part
   whatever is read of it, so the audio is read in full, part by part, while
   the worker encodes.
2. The worker reads each part over HTTP ranges from this app, encodes the
   video only, and uploads it in chunks of about a minute that start on a
   keyframe. Each chunk is recorded with where it starts in its part, so an
   interrupted worker carries on from the last chunk received.
3. Once every part is in and the audio is copied, the merge Job assembles the
   chunks and the audio into one file, each part starting where it started
   in the original, and the task goes on as any merge does.

The worker reaches the app through a WireGuard gateway pod and a listener of
its own (``EMBYX_MANAGER_TRANSCODE_PORT``) that serves only these endpoints,
so nothing else of the app is exposed to it. A task waiting for the worker,
or one whose worker went away, does not hold up the other merges.
"""

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from embyx_manager.db import Database

#: How long after its last report the worker counts as gone.
WORKER_TIMEOUT = timedelta(seconds=90)
#: The length the worker cuts the video into; each chunk starts on a keyframe.
CHUNK_SECONDS = 60
#: A chunk never comes near this; anything bigger is refused unread.
MAX_CHUNK_BYTES = 2 * 1024**3
#: How far a chunk may start from where the previous one ended (a frame or two).
CHUNK_GAP_SECONDS = 0.25
#: How far the chunks of a finished part may fall short of or run past the part.
PART_DURATION_TOLERANCE_SECONDS = 2.0
#: What the worker hands NVENC, sent with every task so the worker needs no update
#: when they change. Chosen from the test on the home 5090 (UNVRSP-003, 4K60 VR).
ENCODER: dict[str, Any] = {
    'codec': 'av1_nvenc',
    'preset': 'p7',
    'tune': 'uhq',
    'bit_depth': 10,
    'multipass': 'fullres',
    'lookahead': 32,
    'spatial_aq': True,
    'gop_seconds': 5,
}


@dataclass(frozen=True)
class Chunk:
    part_index: int
    chunk_index: int
    #: Where the chunk starts in its part, in seconds.
    start: float
    duration: float
    frames: int
    bytes: int
    sha256: str

    @property
    def end(self) -> float:
        return self.start + self.duration


@dataclass
class WorkerReport:
    """What the worker last said about itself; kept in memory, refreshed every few seconds."""

    seen_at: datetime
    name: str = ''
    version: str = ''
    task_id: int | None = None
    #: ``encoding``, ``paused``, ``stopping``, ``waiting`` or ``idle``.
    state: str = 'idle'
    #: One entry per part being encoded: index, fps, position (seconds into the part).
    parts: list[dict[str, Any]] = field(default_factory=list)
    upload_bps: float = 0.0
    queue_chunks: int = 0
    queue_bytes: int = 0


class WorkerPresence:
    """Whether the home worker is there; a single worker is expected."""

    def __init__(self, *, timeout: timedelta = WORKER_TIMEOUT) -> None:
        self._timeout = timeout
        self._last: WorkerReport | None = None

    def report(self, report: WorkerReport) -> None:
        self._last = report

    def last(self) -> WorkerReport | None:
        return self._last

    def online(self, now: datetime | None = None) -> bool:
        if self._last is None:
            return False
        return (now or datetime.now(UTC)) - self._last.seen_at <= self._timeout

    def view(self) -> dict[str, Any] | None:
        if self._last is None:
            return None
        payload = asdict(self._last)
        payload['online'] = self.online()
        return payload


def transcode_dir(work_dir: Path, task_id: int) -> Path:
    """Where one task's chunks, copied audio and audio Job status live."""
    return work_dir / '.transcode' / str(task_id)


def chunk_path(directory: Path, part_index: int, chunk_index: int) -> Path:
    return directory / f'cd{part_index + 1:02d}' / f'chunk-{chunk_index:05d}.mkv'


def audio_path(directory: Path, part_index: int) -> Path:
    return directory / 'audio' / f'cd{part_index + 1:02d}.mka'


def audio_status_path(directory: Path) -> Path:
    return directory / 'audio.json'


def audio_job_name(task_id: int, avid: str) -> str:
    from embyx_manager.merge.kube import MAX_JOB_NAME, job_name  # noqa: PLC0415 - keeps the worker light

    return job_name(task_id, avid).replace('embyx-merge-', 'embyx-audio-', 1)[:MAX_JOB_NAME].rstrip('-')


class TranscodeRepository:
    """The chunks the worker delivered and the parts it finished, per task."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def chunks(self, task_id: int) -> tuple[Chunk, ...]:
        pool = await self._database.get_pool()
        rows = await pool.fetch(
            'SELECT * FROM merge_transcode_chunks WHERE task_id = $1 ORDER BY part_index, chunk_index',
            task_id,
        )
        return tuple(
            Chunk(
                part_index=row['part_index'],
                chunk_index=row['chunk_index'],
                start=row['start_seconds'],
                duration=row['duration_seconds'],
                frames=row['frames'],
                bytes=row['bytes'],
                sha256=row['sha256'],
            )
            for row in rows
        )

    async def add_chunk(self, task_id: int, chunk: Chunk) -> bool:
        """Record ``chunk``; False when the part already has one under its index."""
        pool = await self._database.get_pool()
        result = await pool.execute(
            """
            INSERT INTO merge_transcode_chunks (
                task_id, part_index, chunk_index, start_seconds, duration_seconds, frames, bytes, sha256, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT DO NOTHING
            """,
            task_id,
            chunk.part_index,
            chunk.chunk_index,
            chunk.start,
            chunk.duration,
            chunk.frames,
            chunk.bytes,
            chunk.sha256,
            datetime.now(UTC),
        )
        return result.endswith(' 1')

    async def finished_parts(self, task_id: int) -> frozenset[int]:
        pool = await self._database.get_pool()
        rows = await pool.fetch('SELECT part_index FROM merge_transcode_parts WHERE task_id = $1', task_id)
        return frozenset(int(row['part_index']) for row in rows)

    async def finish_part(self, task_id: int, part_index: int) -> None:
        pool = await self._database.get_pool()
        await pool.execute(
            """
            INSERT INTO merge_transcode_parts (task_id, part_index, finished_at) VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            """,
            task_id,
            part_index,
            datetime.now(UTC),
        )

    async def clear(self, task_id: int) -> None:
        """Forget everything the worker delivered for the task."""
        pool = await self._database.get_pool()
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute('DELETE FROM merge_transcode_chunks WHERE task_id = $1', task_id)
            await connection.execute('DELETE FROM merge_transcode_parts WHERE task_id = $1', task_id)


def concat_script(entries: list[tuple[Path, float]]) -> str:
    """A concat-demuxer script whose files each take exactly the given duration.

    The demuxer starts each file where the previous one's stated duration ends,
    so the video chunks and the copied audio line up part by part as the
    originals did, however long each stream of a part actually runs.
    """
    lines = []
    for path, duration in entries:
        quoted = str(path).replace("'", "'\\''")
        lines.append(f"file '{quoted}'")
        lines.append(f'duration {duration:.6f}')
    return '\n'.join(lines) + '\n'


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None
