"""Driving merge tasks from the queue to the library.

Each task walks one path, one step per poll, and every step can be repeated
after a restart without harm:

1. **queued → merging**: start the title's merge Job, one Job at a time.
   A title the library already holds whole, next to its parts, is refused:
   the archive would never file the merged copy.
2. **merging → uploading**: the Job wrote the merged file and its SHA-1 to the
   work volume, which CloudDrive also serves.
3. **uploading → verifying**: CloudDrive copied it into a staging folder of its
   own under the 115 staging directory. A copy that failed is restarted; a
   merged file that went missing before the copy finished is merged again.
4. **verifying → replacing**: the staged file has the merged file's size and
   SHA-1, and the library still holds no whole copy of the title (one may
   have been filed during the upload, e.g. by a merge of the same title
   under another spelling of its ID).
5. **replacing → archiving**: the original parts are deleted (into 115's
   recycle bin), the merged file moves into ``{intake}/{source}/{AVID}/`` and
   the local copy is removed. The parts have to leave first: the archive
   refuses a title the library already holds.
6. **archiving → done**: the archive pipeline filed the folder away.
   Should a whole copy reach the library anyway, the archive leaves the
   folder alone and the task fails; removing it then deletes the merged
   copy, which the library no longer needs.

A step that cannot go on fails the task and records which state it failed in,
so a retry resumes there. A passing problem (CloudDrive unreachable, 115 not
yet reporting a hash) is shown as a notice while the step is retried.
"""

import asyncio
import json
import logging
import shutil
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

from embyx_manager.clients.clouddrive.aio import AsyncCloudDrive, CopyStatus
from embyx_manager.config.models import ArchiveConfig, CloudDriveConfig, MergeConfig
from embyx_manager.merge.detect import whole_copies
from embyx_manager.merge.kube import KubeError, KubeJobs, job_failure, job_name, job_outcome
from embyx_manager.merge.tasks import (
    CANCELLABLE_STATES,
    MergeState,
    MergeTask,
    MergeTaskRepository,
)
from embyx_manager.merge.transcode import (
    CHUNK_GAP_SECONDS,
    CHUNK_SECONDS,
    ENCODER,
    PART_DURATION_TOLERANCE_SECONDS,
    Chunk,
    TranscodeRepository,
    WorkerPresence,
    WorkerReport,
    audio_job_name,
    audio_status_path,
    chunk_path,
    transcode_dir,
)
from embyx_manager.merge.worker import (
    HOME_SOURCE_SHARE,
    MAX_UPLOAD_BYTES,
    MUXERS,
    TRANSCODE_BUDGET_SHARE,
    TRANSCODE_MUXERS,
    partial_path,
    read_status,
    staging_dir,
)
from embyx_manager.monitor.reports import PipelineName

LOGGER = logging.getLogger(__name__)

POLL_SECONDS = 10.0
MAX_UPLOAD_ATTEMPTS = 5
#: 115 computes a fresh upload's SHA-1 shortly after it lands; wait this long for it.
HASH_WAIT = timedelta(minutes=30)
#: CloudDrive's listing can trail a finished copy by a little.
STAGED_FILE_WAIT = timedelta(minutes=10)
#: How often an unfiled title asks the archive pipeline for another pass; the
#: reconcile scan files a folder only once it has seen it unchanged twice.
ARCHIVE_NUDGE_INTERVAL = timedelta(minutes=5)
#: How long a copy CloudDrive accepted may take to show up among its copy tasks.
COPY_LISTING_GRACE = timedelta(minutes=2)
#: CloudDrive's words for an upload 115 will never take, however often it is retried.
PERMANENT_UPLOAD_ERRORS = ('code: 10025', '单文件不能大于')
#: How long an upload that ended in error rests before its next attempt, by the
#: attempts made so far: a network outage outlasts a few quick retries.
UPLOAD_RETRY_DELAYS = (
    timedelta(minutes=5),
    timedelta(minutes=15),
    timedelta(hours=1),
    timedelta(hours=3),
)
#: After this long without the archive filing the title, the task fails for a look.
ARCHIVE_WAIT = timedelta(hours=2)
SHA1_HASH_KEY = '2'
NO_JOB_TEMPLATE = 'the merge Job template is not mounted (EMBYX_MANAGER_MERGE_JOB_TEMPLATE)'
WAITING_FOR_WORKER = 'waiting for the home transcode worker'
WAITING_FOR_SLOT = 'every part is re-encoded; waiting for the running merge Job to finish'
#: Where a home transcode may go after a failure, by the state it failed in.
HOME_RETRY_STATES = frozenset({MergeState.TRANSCODING, MergeState.MERGING})


def part_sizes(parts: tuple[str, ...]) -> int:
    """The parts' total size as the CloudDrive mount reports it."""
    return sum(Path(part).stat().st_size for part in parts)


def upload_retry_delay(attempts: int) -> timedelta:
    """The rest before the next upload attempt; none after a retry the operator asked for."""
    if attempts <= 0:
        return timedelta(0)
    return UPLOAD_RETRY_DELAYS[min(attempts, len(UPLOAD_RETRY_DELAYS)) - 1]


def retry_notice(attempts: int, delay: timedelta, error: str) -> str:
    minutes = int(delay.total_seconds()) // 60
    wait = f'{minutes // 60} h' if minutes % 60 == 0 else f'{minutes} min'
    return f'upload attempt {attempts + 1} of {MAX_UPLOAD_ATTEMPTS} starts in {wait}: {error}'


class MergeFailedError(Exception):
    """The task cannot go on; the message is shown to the operator."""


class ChunkRefusedError(Exception):
    """A chunk or call from the home worker that does not fit the task; ``code`` is the API error code."""

    def __init__(self, code: str, status: int = 409, **details: object) -> None:
        super().__init__(code)
        self.code = code
        self.status = status
        self.details = details


class MergeActionError(Exception):
    """A cancel or retry the task's state does not allow; ``code`` is the API error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class MergePaths:
    """Where one task's files live, local and in CloudDrive."""

    output: Path
    status: Path
    cloud_source: str
    staging_dir: str
    staged_file: str
    intake_dir: str
    target_dir: str
    part_api_paths: tuple[str, ...]


def api_path(mounted: str, mount_prefix: str) -> str:
    """The CloudDrive API path of a file seen through the mount."""
    try:
        relative = PurePosixPath(mounted).relative_to(mount_prefix)
    except ValueError as exc:
        msg = f'{mounted} is outside the CloudDrive mount {mount_prefix}'
        raise MergeFailedError(msg) from exc
    return str(PurePosixPath('/') / relative)


def merge_paths(task: MergeTask, merge: MergeConfig, archive: ArchiveConfig) -> MergePaths:
    work = Path(merge.work_dir)
    staging_dir = f'{merge.cloud_staging_dir}/merge-{task.id}'
    intake_dir = api_path(archive.src_dir, merge.cloud_mount_prefix)
    return MergePaths(
        output=work / task.merged_name,
        status=work / '.merge' / f'{task.id}.json',
        cloud_source=f'{merge.cloud_work_dir}/{task.merged_name}',
        staging_dir=staging_dir,
        staged_file=f'{staging_dir}/{task.merged_name}',
        intake_dir=f'{intake_dir}/{task.source}',
        target_dir=f'{intake_dir}/{task.source}/{task.avid}',
        part_api_paths=tuple(api_path(part, merge.cloud_mount_prefix) for part in task.parts),
    )


class MergeService:
    def __init__(  # noqa: PLR0913
        self,
        *,
        repository: MergeTaskRepository,
        cloud: Callable[[], AsyncCloudDrive | None],
        kube: KubeJobs | None,
        merge_config: Callable[[], MergeConfig],
        archive_config: Callable[[], ArchiveConfig],
        clouddrive_config: Callable[[], CloudDriveConfig],
        trigger: Callable[[PipelineName], Awaitable[None]],
        poll_seconds: float = POLL_SECONDS,
        transcodes: TranscodeRepository | None = None,
        presence: WorkerPresence | None = None,
        sizes: Callable[[tuple[str, ...]], int] = part_sizes,
    ) -> None:
        self._repository = repository
        self._cloud = cloud
        self._kube = kube
        self._merge_config = merge_config
        self._archive_config = archive_config
        self._clouddrive_config = clouddrive_config
        self._trigger = trigger
        self._poll_seconds = poll_seconds
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._tick_lock = asyncio.Lock()
        self._transcodes = transcodes
        self._presence = presence or WorkerPresence()
        self._sizes = sizes
        #: The parts' total size per task, read once from the mount.
        self._totals: dict[int, int] = {}
        #: Whether a merge Job runs; set by each tick before the tasks are stepped.
        self._slot_taken = False

    @property
    def presence(self) -> WorkerPresence:
        return self._presence

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name='merge-loop')

    async def aclose(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._kube is not None:
            await self._kube.aclose()

    def wake(self) -> None:
        self._wake.set()

    def unavailable(self) -> str | None:
        """Why merging cannot run here, or None when it can."""
        if self._kube is None:
            return NO_JOB_TEMPLATE
        if not self._clouddrive_config().configured:
            return 'CloudDrive is not configured'
        archive = self._archive_config()
        if not archive.src_dir or not archive.dst_dir:
            return 'archive.src_dir and archive.dst_dir must be configured'
        merge = self._merge_config()
        if not merge.configured:
            return 'merge directories must be configured'
        try:
            api_path(archive.src_dir, merge.cloud_mount_prefix)
        except MergeFailedError:
            return 'archive.src_dir must lie under merge.cloud_mount_prefix'
        return None

    async def _loop(self) -> None:
        while True:
            try:
                await self.tick()
            except Exception:
                LOGGER.exception('merge poll failed')
            with suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self._poll_seconds)
            self._wake.clear()

    # -- polling -------------------------------------------------------------

    async def tick(self) -> None:
        """Advance every open task by at most one step."""
        async with self._tick_lock:
            if self.unavailable() is not None:
                return
            tasks = await self._repository.open_tasks()
            for task in tasks:
                if task.state == MergeState.QUEUED:
                    await self._total_bytes(task)
            online = self._presence.online()
            self._slot_taken = any(task.state == MergeState.MERGING for task in tasks)
            # The home GPU at work holds the queue as a merge Job does.
            gpu_busy = online and any(task.state == MergeState.TRANSCODING for task in tasks)
            for task in tasks:
                if task.state == MergeState.FAILED:
                    continue
                if task.state == MergeState.QUEUED:
                    if self.home_bound(task):
                        await self._note_waiting(task, online=online)
                        continue
                    if self._slot_taken or gpu_busy:
                        continue
                    self._slot_taken = True
                await self._step(task)

    async def _step(self, task: MergeTask) -> None:
        handlers = {
            MergeState.QUEUED: self._start_merge,
            MergeState.TRANSCODING: self._watch_transcode,
            MergeState.MERGING: self._watch_merge,
            MergeState.UPLOADING: self._watch_upload,
            MergeState.VERIFYING: self._verify,
            MergeState.REPLACING: self._replace,
            MergeState.ARCHIVING: self._watch_archive,
        }
        try:
            paths = merge_paths(task, self._merge_config(), self._archive_config())
            await handlers[task.state](task, paths)
        except MergeFailedError as failure:
            LOGGER.warning('merge of %s failed while %s: %s', task.avid, task.state, failure)
            await self._repository.transition(
                task.id,
                task.state,
                MergeState.FAILED,
                failed_state=task.state.value,
                error=str(failure),
                notice=None,
            )
        except Exception as exc:
            LOGGER.exception('merge of %s could not advance while %s', task.avid, task.state)
            await self._repository.update(task.id, notice=str(exc) or type(exc).__name__)

    def _require_cloud(self) -> AsyncCloudDrive:
        cloud = self._cloud()
        if cloud is None:
            msg = 'CloudDrive is not configured'
            raise RuntimeError(msg)
        return cloud

    def _require_kube(self) -> KubeJobs:
        if self._kube is None:
            raise RuntimeError(NO_JOB_TEMPLATE)
        return self._kube

    # -- queued / merging ------------------------------------------------------

    async def _start_merge(self, task: MergeTask, paths: MergePaths) -> None:
        kube = self._require_kube()
        await self._refuse_whole_copy(task, paths)
        name = job_name(task.id, task.avid)
        paths.status.unlink(missing_ok=True)
        args = [
            '--output',
            str(paths.output),
            '--status',
            str(paths.status),
            '--reserve-gib',
            str(self._merge_config().free_space_reserve_gib),
            '--',
            *task.parts,
        ]
        await kube.create_job(name=name, args=args, task_id=task.id)
        await self._repository.transition(
            task.id,
            MergeState.QUEUED,
            MergeState.MERGING,
            job_name=name,
            phase='starting',
            progress=0.0,
            error=None,
            notice=None,
        )

    async def _watch_merge(self, task: MergeTask, paths: MergePaths) -> None:
        kube = self._require_kube()
        status = read_status(paths.status) or {}
        if status.get('state') == 'done':
            size = status.get('size')
            if not paths.output.is_file() or paths.output.stat().st_size != size:
                msg = f'the Job reported {paths.output.name} done, but it is not on the work volume'
                raise MergeFailedError(msg)
            await self._repository.transition(
                task.id,
                MergeState.MERGING,
                MergeState.UPLOADING,
                merged_bytes=int(size),
                merged_sha1=str(status.get('sha1', '')).upper(),
                phase=None,
                progress=None,
                uploaded_bytes=0,
                upload_attempts=0,
                notice=None,
            )
            if task.job_name:
                with suppress(KubeError):
                    await kube.delete_job(task.job_name)
            paths.status.unlink(missing_ok=True)
            if task.transcoder == 'home':
                # The chunks are in the merged file now, and would only crowd its upload.
                await self._discard_transcode(task)
            return
        if status.get('state') == 'failed':
            raise MergeFailedError(str(status.get('error') or 'the merge Job failed'))
        job = await kube.get_job(task.job_name) if task.job_name else None
        if job is None:
            msg = 'the merge Job disappeared before it finished'
            raise MergeFailedError(msg)
        if job_outcome(job) == 'failed':
            # Killed (out of memory, past its deadline) before it could clean up after itself.
            partial_path(paths.output).unlink(missing_ok=True)
            shutil.rmtree(staging_dir(paths.output), ignore_errors=True)
            tail = ''
            with suppress(KubeError):
                tail = await kube.log_tail(task.job_name or '')
            details = [detail for detail in (job_failure(job), tail) if detail]
            raise MergeFailedError('the merge Job failed' + (f': {" | ".join(details)}' if details else ''))
        if job_outcome(job) == 'succeeded':
            # A refusal ends the Job cleanly too, but the worker reports before
            # it exits; a report written since the read above waits for the next tick.
            if (read_status(paths.status) or {}).get('state') in {'done', 'failed'}:
                return
            msg = 'the merge Job ended without reporting a result'
            raise MergeFailedError(msg)
        await self._repository.update(
            task.id,
            phase=status.get('phase') or task.phase,
            progress=status.get('progress', task.progress),
            notice=None,
        )

    # -- home transcode ----------------------------------------------------------

    async def _total_bytes(self, task: MergeTask) -> int | None:
        """The parts' total size, read from the mount once per task; None while it cannot be read."""
        if task.id not in self._totals:
            try:
                self._totals[task.id] = await asyncio.to_thread(self._sizes, task.parts)
            except OSError:
                LOGGER.warning('could not read the part sizes of %s', task.avid)
                return None
        return self._totals[task.id]

    def home_bound(self, task: MergeTask) -> bool:
        """Whether the task waits for the home worker rather than going to the merge Job's encoder."""
        if self._transcodes is None or task.transcoder == 'cpu':
            return False
        if task.transcoder == 'home':
            return True
        if not self._merge_config().home_transcode_enabled:
            return False
        total = self._totals.get(task.id)
        suffix = Path(task.merged_name).suffix.lower()
        return total is not None and total > MAX_UPLOAD_BYTES and MUXERS.get(suffix) in TRANSCODE_MUXERS

    def parked(self, task: MergeTask) -> bool:
        """A task waiting on the home worker while it is away, which holds up nothing else."""
        if task.state == MergeState.QUEUED:
            return self.home_bound(task)
        return task.state == MergeState.TRANSCODING and not self._presence.online()

    def reserved_bytes(self, tasks: tuple[MergeTask, ...]) -> int:
        """What home transcodes still mean to write to the work volume: chunks plus the assembled file."""
        reserved = 0
        for task in tasks:
            if task.state not in {MergeState.QUEUED, MergeState.TRANSCODING} or not self.home_bound(task):
                continue
            total = self._totals.get(task.id, MAX_UPLOAD_BYTES)
            reserved += 2 * int(min(MAX_UPLOAD_BYTES * TRANSCODE_BUDGET_SHARE, total * HOME_SOURCE_SHARE))
        return reserved

    async def _note_waiting(self, task: MergeTask, *, online: bool) -> None:
        notice = None if online else WAITING_FOR_WORKER
        if task.notice != notice:
            await self._repository.update(task.id, notice=notice)

    def _transcode_dir(self, task: MergeTask) -> Path:
        return transcode_dir(Path(self._merge_config().work_dir), task.id)

    async def _start_prepare_job(self, task: MergeTask, paths: MergePaths) -> None:
        """The Job that probes the parts and copies their audio for a home transcode."""
        directory = self._transcode_dir(task)
        await asyncio.to_thread(directory.mkdir, parents=True, exist_ok=True)
        await self._require_kube().create_job(
            name=audio_job_name(task.id, task.avid),
            args=[
                '--output',
                str(paths.output),
                '--status',
                str(audio_status_path(directory)),
                '--reserve-gib',
                str(self._merge_config().free_space_reserve_gib),
                '--home-prepare',
                str(directory),
                '--',
                *task.parts,
            ],
            task_id=task.id,
        )

    async def _discard_transcode(self, task: MergeTask) -> None:
        """Stop the audio Job and drop the chunks and audio a home transcode left."""
        if self._kube is not None:
            with suppress(KubeError):
                await self._kube.delete_job(audio_job_name(task.id, task.avid))
        await asyncio.to_thread(shutil.rmtree, self._transcode_dir(task), ignore_errors=True)
        if self._transcodes is not None:
            await self._transcodes.clear(task.id)

    async def _watch_transcode(self, task: MergeTask, paths: MergePaths) -> None:
        """Follow the audio Job and the worker; assemble once both are done."""
        transcodes = self._require_transcodes()
        directory = self._transcode_dir(task)
        prepared = read_status(audio_status_path(directory)) or {}
        if prepared.get('state') == 'failed':
            raise MergeFailedError(str(prepared.get('error') or 'the audio Job failed'))
        if prepared.get('state') != 'done':
            await self._watch_prepare_job(task, paths, prepared)
        chunks = await transcodes.chunks(task.id)
        finished = await transcodes.finished_parts(task.id)
        parts = prepared.get('parts') or []
        total = sum(float(part['duration']) for part in parts)
        encoded = sum(chunk.duration for chunk in chunks)
        if parts and prepared.get('state') == 'done' and len(finished) == len(task.parts):
            if self._slot_taken:
                if task.notice != WAITING_FOR_SLOT:
                    await self._repository.update(task.id, notice=WAITING_FOR_SLOT)
                return
            self._slot_taken = True
            await self._start_assembly(task, paths, chunks)
            return
        await self._repository.update(
            task.id,
            phase='transcoding' if parts else 'probing',
            progress=min(encoded / total, 1.0) if total else 0.0,
            notice=None if self._presence.online() else WAITING_FOR_WORKER,
        )

    async def _watch_prepare_job(self, task: MergeTask, paths: MergePaths, prepared: dict[str, object]) -> None:
        kube = self._require_kube()
        name = audio_job_name(task.id, task.avid)
        job = await kube.get_job(name)
        if job is None:
            # Gone with a restart of the task, or never started: (re)start it; audio already copied is kept.
            await self._start_prepare_job(task, paths)
            return
        outcome = job_outcome(job)
        if outcome == 'failed':
            tail = ''
            with suppress(KubeError):
                tail = await kube.log_tail(name)
            details = [detail for detail in (job_failure(job), tail) if detail]
            raise MergeFailedError('the audio Job failed' + (f': {" | ".join(details)}' if details else ''))
        if outcome == 'succeeded' and prepared.get('state') not in {'done', 'failed'}:
            fresh = read_status(audio_status_path(self._transcode_dir(task))) or {}
            if fresh.get('state') not in {'done', 'failed'}:
                msg = 'the audio Job ended without reporting a result'
                raise MergeFailedError(msg)

    async def _start_assembly(self, task: MergeTask, paths: MergePaths, chunks: tuple[Chunk, ...]) -> None:
        directory = self._transcode_dir(task)
        manifest = {
            'parts': [
                {
                    'index': index,
                    'chunks': [
                        {'index': chunk.chunk_index, 'duration': chunk.duration}
                        for chunk in chunks
                        if chunk.part_index == index
                    ],
                }
                for index in range(len(task.parts))
            ],
        }
        await asyncio.to_thread((directory / 'manifest.json').write_text, json.dumps(manifest), encoding='utf-8')
        name = job_name(task.id, task.avid)
        paths.status.unlink(missing_ok=True)
        await self._require_kube().create_job(
            name=name,
            args=[
                '--output',
                str(paths.output),
                '--status',
                str(paths.status),
                '--reserve-gib',
                str(self._merge_config().free_space_reserve_gib),
                '--home-assemble',
                str(directory),
            ],
            task_id=task.id,
        )
        moved = await self._repository.transition(
            task.id,
            MergeState.TRANSCODING,
            MergeState.MERGING,
            job_name=name,
            phase='starting',
            progress=0.0,
            notice=None,
        )
        if moved is not None:
            with suppress(KubeError):
                await self._require_kube().delete_job(audio_job_name(task.id, task.avid))

    def _require_transcodes(self) -> TranscodeRepository:
        if self._transcodes is None:
            msg = 'home transcoding is not set up in this deployment'
            raise RuntimeError(msg)
        return self._transcodes

    # -- the home worker's calls ---------------------------------------------------

    async def claim(self, report: WorkerReport) -> dict[str, object]:
        """Hand the worker its task: the one it was on, or the oldest waiting once nothing else runs."""
        self._presence.report(report)
        async with self._tick_lock:
            if self.unavailable() is not None or self._transcodes is None:
                return {'task': None, 'wait': 'unavailable'}
            tasks = await self._repository.open_tasks()
            current = next((task for task in tasks if task.state == MergeState.TRANSCODING), None)
            if current is None:
                for task in tasks:
                    if task.state == MergeState.QUEUED:
                        await self._total_bytes(task)
                waiting = [task for task in tasks if task.state == MergeState.QUEUED and self.home_bound(task)]
                if not waiting:
                    return {'task': None, 'wait': 'idle'}
                others = [task for task in tasks if task.state != MergeState.FAILED and task not in waiting]
                if others:
                    return {'task': None, 'wait': 'busy'}
                try:
                    current = await self._begin_transcode(waiting[0])
                except MergeFailedError as failure:
                    LOGGER.warning('merge of %s failed before its transcode: %s', waiting[0].avid, failure)
                    await self._repository.transition(
                        waiting[0].id,
                        MergeState.QUEUED,
                        MergeState.FAILED,
                        failed_state=MergeState.QUEUED.value,
                        error=str(failure),
                        notice=None,
                    )
                    return {'task': None, 'wait': 'idle'}
                if current is None:
                    return {'task': None, 'wait': 'busy'}
        self.wake()
        return await self._assignment(current)

    async def _begin_transcode(self, task: MergeTask) -> MergeTask | None:
        await self._discard_transcode(task)
        await self._refuse_whole_copy(task, merge_paths(task, self._merge_config(), self._archive_config()))
        started = await self._repository.transition(
            task.id,
            MergeState.QUEUED,
            MergeState.TRANSCODING,
            transcoder='home',
            job_name=None,
            phase='probing',
            progress=0.0,
            error=None,
            notice=None,
        )
        if started is not None:
            # Should the Job API fail now, the next tick finds no audio Job and starts it.
            with suppress(KubeError):
                await self._start_prepare_job(
                    started, merge_paths(started, self._merge_config(), self._archive_config())
                )
        return started

    async def _assignment(self, task: MergeTask) -> dict[str, object]:
        prepared = read_status(audio_status_path(self._transcode_dir(task))) or {}
        parts = prepared.get('parts')
        bitrate = prepared.get('video_bitrate')
        if not isinstance(parts, list) or not isinstance(bitrate, int):
            return {'task': None, 'wait': 'probing', 'task_id': task.id}
        chunks = await self._require_transcodes().chunks(task.id)
        finished = await self._require_transcodes().finished_parts(task.id)
        if len(finished) >= len(task.parts):
            # Nothing left to encode: the cluster joins the parts once the audio is ready.
            return {'task': None, 'wait': 'assembling', 'task_id': task.id}
        views = []
        for index, (part, info) in enumerate(zip(task.parts, parts, strict=True)):
            mine = [chunk for chunk in chunks if chunk.part_index == index]
            views.append(
                {
                    'index': index,
                    'name': Path(part).name,
                    'size': int(info['size']),
                    'duration': float(info['duration']),
                    'done': index in finished,
                    'next_chunk': len(mine),
                    'resume_at': mine[-1].end if mine else 0.0,
                },
            )
        return {
            'task': {
                'id': task.id,
                'avid': task.avid,
                'video_bitrate': bitrate,
                'chunk_seconds': CHUNK_SECONDS,
                'encoder': ENCODER,
                'paused': task.transcode_paused,
                'parts': views,
            },
            'wait': None,
        }

    async def heartbeat(self, report: WorkerReport) -> dict[str, object]:
        """Record the worker's report; tell it whether to pause or drop its task."""
        self._presence.report(report)
        task = await self._repository.get(report.task_id) if report.task_id is not None else None
        active = task is not None and task.state == MergeState.TRANSCODING
        return {'active': active, 'paused': bool(task and task.transcode_paused)}

    async def transcoding_task(self, task_id: int, part_index: int) -> MergeTask:
        """The task the worker addresses, refused unless it is being transcoded and has the part."""
        task = await self._repository.get(task_id)
        if task is None:
            msg = 'transcode_task_not_found'
            raise ChunkRefusedError(msg, 404)
        if task.state != MergeState.TRANSCODING:
            msg = 'transcode_task_not_active'
            raise ChunkRefusedError(msg)
        if not 0 <= part_index < len(task.parts):
            msg = 'transcode_part_not_found'
            raise ChunkRefusedError(msg, 404)
        return task

    def staging_chunk(self, task: MergeTask, part_index: int, chunk_index: int) -> Path:
        """Where an incoming chunk is written before it is checked."""
        path = chunk_path(self._transcode_dir(task), part_index, chunk_index)
        return path.with_name(f'.{path.name}.part')

    async def accept_chunk(self, task: MergeTask, chunk: Chunk, received: Path) -> bool:
        """Keep a chunk written to ``received``; False when it was a repeat of one already kept."""
        transcodes = self._require_transcodes()
        if chunk.part_index in await transcodes.finished_parts(task.id):
            await asyncio.to_thread(received.unlink, missing_ok=True)
            mine = [item for item in await transcodes.chunks(task.id) if item.part_index == chunk.part_index]
            if any(item.chunk_index == chunk.chunk_index and item.sha256 == chunk.sha256 for item in mine):
                return False
            msg = 'transcode_part_finished'
            raise ChunkRefusedError(msg)
        mine = [item for item in await transcodes.chunks(task.id) if item.part_index == chunk.part_index]
        if chunk.chunk_index < len(mine):
            await asyncio.to_thread(received.unlink, missing_ok=True)
            if mine[chunk.chunk_index].sha256 == chunk.sha256:
                return False
            msg = 'transcode_chunk_conflict'
            raise ChunkRefusedError(msg, next_chunk=len(mine))
        expected = mine[-1].end if mine else 0.0
        if chunk.chunk_index > len(mine) or abs(chunk.start - expected) > CHUNK_GAP_SECONDS:
            await asyncio.to_thread(received.unlink, missing_ok=True)
            msg = 'transcode_chunk_out_of_order'
            raise ChunkRefusedError(msg, next_chunk=len(mine), resume_at=expected)
        target = chunk_path(self._transcode_dir(task), chunk.part_index, chunk.chunk_index)
        await asyncio.to_thread(received.replace, target)
        await transcodes.add_chunk(task.id, chunk)
        return True

    async def finish_part(self, task: MergeTask, part_index: int) -> None:
        """The worker says the part is fully encoded; its chunks must cover it."""
        transcodes = self._require_transcodes()
        prepared = read_status(audio_status_path(self._transcode_dir(task))) or {}
        parts = prepared.get('parts') or []
        mine = [item for item in await transcodes.chunks(task.id) if item.part_index == part_index]
        duration = float(parts[part_index]['duration']) if part_index < len(parts) else None
        covered = mine[-1].end if mine else 0.0
        if duration is None or abs(covered - duration) > PART_DURATION_TOLERANCE_SECONDS:
            msg = 'transcode_part_incomplete'
            raise ChunkRefusedError(msg, covered=covered, duration=duration)
        await transcodes.finish_part(task.id, part_index)
        self.wake()

    async def transcode_view(self, task: MergeTask) -> dict[str, object] | None:
        """What the merge page shows of a home transcode."""
        if task.transcoder != 'home' or task.state != MergeState.TRANSCODING or self._transcodes is None:
            return None
        prepared = read_status(audio_status_path(self._transcode_dir(task))) or {}
        finished = await self._transcodes.finished_parts(task.id)
        chunks = await self._transcodes.chunks(task.id)
        return {
            'parts_done': len(finished),
            'part_count': len(task.parts),
            'encoded_seconds': sum(chunk.duration for chunk in chunks),
            'encoded_bytes': sum(chunk.bytes for chunk in chunks),
            'duration': prepared.get('duration'),
            'video_bitrate': prepared.get('video_bitrate'),
            'audio_done': prepared.get('audio_done'),
            'audio_state': prepared.get('state'),
            'paused': task.transcode_paused,
        }

    async def set_paused(self, task_id: int, *, paused: bool) -> MergeTask:
        async with self._tick_lock:
            task = await self._repository.get(task_id)
            if task is None:
                msg = 'merge_task_not_found'
                raise MergeActionError(msg)
            if task.state not in {MergeState.QUEUED, MergeState.TRANSCODING} or not self.home_bound(task):
                msg = 'merge_task_not_transcoding'
                raise MergeActionError(msg)
            updated = await self._repository.update(task.id, transcode_paused=paused)
            if updated is None:
                msg = 'merge_task_changed'
                raise MergeActionError(msg)
            return updated

    async def use_cpu(self, task_id: int) -> MergeTask:
        """Send a home transcode back to the merge Job's own encoder, dropping what the worker did."""
        async with self._tick_lock:
            task = await self._repository.get(task_id)
            if task is None:
                msg = 'merge_task_not_found'
                raise MergeActionError(msg)
            failed_home = task.state == MergeState.FAILED and task.failed_state in {
                MergeState.QUEUED,
                *HOME_RETRY_STATES,
            }
            movable = task.state in {MergeState.QUEUED, MergeState.TRANSCODING} or failed_home
            if not movable or not self.home_bound(task):
                msg = 'merge_task_not_transcoding'
                raise MergeActionError(msg)
            await self._discard_transcode(task)
            if task.job_name and self._kube is not None:
                with suppress(KubeError):
                    await self._kube.delete_job(task.job_name)
            moved = await self._repository.transition(
                task.id,
                task.state,
                MergeState.QUEUED,
                transcoder='cpu',
                transcode_paused=False,
                job_name=None,
                phase=None,
                progress=None,
                failed_state=None,
                error=None,
                notice=None,
            )
            if moved is None:
                msg = 'merge_task_changed'
                raise MergeActionError(msg)
        self.wake()
        return moved

    # -- uploading / verifying -------------------------------------------------

    async def _watch_upload(self, task: MergeTask, paths: MergePaths) -> None:
        cloud = self._require_cloud()
        copy = await self._copy_task(cloud, paths)
        if copy is not None and copy['status'] == CopyStatus.COMPLETED:
            await self._repository.transition(
                task.id,
                MergeState.UPLOADING,
                MergeState.VERIFYING,
                uploaded_bytes=task.merged_bytes,
                notice=None,
            )
            return
        if not paths.output.is_file():
            # Accepted for now: a redeploy can take the work volume's file with it.
            await self._repository.transition(
                task.id,
                MergeState.UPLOADING,
                MergeState.QUEUED,
                job_name=None,
                merged_bytes=None,
                merged_sha1=None,
                uploaded_bytes=None,
                notice='the merged file was lost before the upload finished; merging again',
            )
            return
        if copy is None:
            if task.upload_attempts:
                # CloudDrive may have dropped a finished task from its list, or not
                # listed a fresh one yet; neither calls for uploading it all again.
                staged = await cloud.stat_file(paths.staged_file)
                if staged is not None and staged['size'] == task.merged_bytes:
                    await self._repository.transition(
                        task.id,
                        MergeState.UPLOADING,
                        MergeState.VERIFYING,
                        uploaded_bytes=task.merged_bytes,
                        notice=None,
                    )
                    return
                if datetime.now(UTC) - task.updated_at < COPY_LISTING_GRACE:
                    return
            await self._issue_copy(task, paths, cloud)
            return
        if copy['status'] == CopyStatus.FAILED:
            errors = '; '.join(copy['errors']) or 'CloudDrive reported the copy as failed'
            if task.upload_attempts >= MAX_UPLOAD_ATTEMPTS:
                msg = f'upload failed {task.upload_attempts} times: {errors}'
                raise MergeFailedError(msg)
            await self._restart_upload_in_time(task, paths, cloud, str(copy['source_path']), errors)
            return
        await self._follow_upload(task, paths, cloud, copy)

    async def _follow_upload(
        self, task: MergeTask, paths: MergePaths, cloud: AsyncCloudDrive, copy: dict[str, object]
    ) -> None:
        """Record how far a running upload got, or deal with CloudDrive giving up on it."""
        upload = await cloud.upload_state(paths.staged_file)
        if upload is not None and upload.fatal is not None:
            await self._handle_fatal_upload(task, paths, cloud, str(copy['source_path']), upload.fatal)
            return
        # The copy task counts nothing until the whole file is up; the upload queue counts as it goes.
        await self._repository.update(
            task.id,
            uploaded_bytes=max(int(str(copy['uploaded_bytes'])), upload.transferred if upload is not None else 0),
            notice='CloudDrive has paused the upload' if copy['paused'] else None,
        )

    async def _handle_fatal_upload(
        self, task: MergeTask, paths: MergePaths, cloud: AsyncCloudDrive, source_path: str, fatal: str
    ) -> None:
        """Deal with an upload CloudDrive gave up on."""
        permanent = any(marker in fatal for marker in PERMANENT_UPLOAD_ERRORS)
        if permanent or task.upload_attempts >= MAX_UPLOAD_ATTEMPTS:
            msg = f'CloudDrive gave up uploading {task.merged_name}: {fatal}'
            raise MergeFailedError(msg)
        # A network error ends an upload the same way; restart it once its rest is over.
        await self._restart_upload_in_time(task, paths, cloud, source_path, fatal)

    async def _restart_upload_in_time(
        self, task: MergeTask, paths: MergePaths, cloud: AsyncCloudDrive, source_path: str, error: str
    ) -> None:
        """Restart an upload that ended in ``error`` after the rest its attempt count calls for.

        The task's notice says which step it is at: ``error`` itself right after a
        restart, while CloudDrive queues the upload again, and a retry notice while
        it rests; ``updated_at`` dates either.
        """
        elapsed = datetime.now(UTC) - task.updated_at
        if task.notice == error and elapsed < COPY_LISTING_GRACE:
            return
        delay = upload_retry_delay(task.upload_attempts)
        if delay:
            waiting = retry_notice(task.upload_attempts, delay, error)
            if task.notice != waiting:
                await self._repository.update(task.id, notice=waiting)
                return
            if elapsed < delay:
                return
        await cloud.restart_copy_task(source_path, paths.staging_dir)
        await self._repository.update(task.id, upload_attempts=task.upload_attempts + 1, notice=error)

    @staticmethod
    async def _copy_task(cloud: AsyncCloudDrive, paths: MergePaths) -> dict[str, object] | None:
        """The latest copy into this task's staging folder; the folder is the task's alone."""
        return next(
            (entry for entry in reversed(await cloud.copy_tasks()) if entry['dest_path'] == paths.staging_dir),
            None,
        )

    async def _issue_copy(self, task: MergeTask, paths: MergePaths, cloud: AsyncCloudDrive) -> None:
        if task.upload_attempts >= MAX_UPLOAD_ATTEMPTS:
            msg = f'CloudDrive did not keep the upload after {task.upload_attempts} attempts'
            raise MergeFailedError(msg)
        staging_parent, staging_name = paths.staging_dir.rsplit('/', 1)
        await cloud.ensure_directory(staging_parent or '/', staging_name)
        result = await cloud.copy_file(paths.cloud_source, paths.staging_dir)
        await self._repository.update(
            task.id,
            upload_attempts=task.upload_attempts + 1,
            notice=None if result['success'] else str(result['error_message'] or 'CloudDrive refused the copy'),
        )

    async def _verify(self, task: MergeTask, paths: MergePaths) -> None:
        cloud = self._require_cloud()
        waited = datetime.now(UTC) - task.state_changed_at
        staged = await cloud.stat_file(paths.staged_file)
        if staged is None:
            if waited < STAGED_FILE_WAIT:
                await self._repository.update(task.id, notice='waiting for CloudDrive to list the uploaded file')
                return
            msg = f'CloudDrive finished the copy, but {paths.staged_file} is not there'
            raise MergeFailedError(msg)
        if staged['size'] != task.merged_bytes:
            msg = f'uploaded size {staged["size"]} differs from the merged file ({task.merged_bytes})'
            raise MergeFailedError(msg)
        hashes = staged.get('hashes')
        sha1 = str(hashes.get(SHA1_HASH_KEY, '')).upper() if isinstance(hashes, dict) else ''
        if not sha1:
            if waited < HASH_WAIT:
                await self._repository.update(task.id, notice='waiting for 115 to report the SHA-1')
                return
            msg = '115 never reported a SHA-1 for the uploaded file'
            raise MergeFailedError(msg)
        if sha1 != task.merged_sha1:
            msg = f'uploaded SHA-1 {sha1} differs from the merged file ({task.merged_sha1})'
            raise MergeFailedError(msg)
        # The last look before the parts are deleted.
        await self._refuse_whole_copy(task, paths)
        await self._repository.transition(task.id, MergeState.VERIFYING, MergeState.REPLACING, notice=None)

    async def _refuse_whole_copy(self, task: MergeTask, paths: MergePaths) -> None:
        whole = await self._whole_copy(task, paths)
        if whole is not None:
            msg = (
                f'the library already holds {whole}, a whole copy of {task.avid}; '
                'merging its parts would only duplicate it'
            )
            raise MergeFailedError(msg)

    async def _whole_copy(self, task: MergeTask, paths: MergePaths) -> str | None:
        """A video beside the parts that holds the whole title, by name; None when there is none."""
        cloud = self._require_cloud()
        directory = str(PurePosixPath(paths.part_api_paths[0]).parent)
        try:
            entries = await cloud.list_directory(directory)
        except FileNotFoundError:
            return None
        names = [str(entry['name']) for entry in entries if not entry['is_directory']]
        return next(iter(whole_copies(task.avid, names)), None)

    # -- replacing / archiving -------------------------------------------------

    async def _replace(self, task: MergeTask, paths: MergePaths) -> None:
        cloud = self._require_cloud()
        remaining = await self._present(cloud, paths.part_api_paths)
        if remaining:
            result = await cloud.delete_files(list(remaining))
            remaining = await self._present(cloud, paths.part_api_paths)
            if remaining:
                await self._repository.update(
                    task.id,
                    notice=f'{len(remaining)} original parts are still listed: {result["error_message"] or "retrying"}',
                )
                return

        intake_parent, intake_name = paths.intake_dir.rsplit('/', 1)
        await cloud.ensure_directory(intake_parent or '/', intake_name)
        await cloud.ensure_directory(paths.intake_dir, task.avid)
        target_file = f'{paths.target_dir}/{task.merged_name}'
        if await cloud.stat_file(paths.staged_file) is not None:
            moved = await cloud.move_file(paths.staged_file, paths.target_dir)
            if not moved['success']:
                await self._repository.update(task.id, notice=str(moved['error_message'] or 'the move was refused'))
                return
        if await cloud.stat_file(target_file) is None:
            msg = f'the merged file is neither in {paths.staging_dir} nor in {paths.target_dir}'
            raise MergeFailedError(msg)
        with suppress(Exception):
            await cloud.delete_files([paths.staging_dir])
        paths.output.unlink(missing_ok=True)
        partial_path(paths.output).unlink(missing_ok=True)
        paths.status.unlink(missing_ok=True)
        if await self._repository.transition(task.id, MergeState.REPLACING, MergeState.ARCHIVING, notice=None):
            await self._trigger(PipelineName.ARCHIVE)

    @staticmethod
    async def _present(cloud: AsyncCloudDrive, files: tuple[str, ...]) -> tuple[str, ...]:
        """Which of ``files`` CloudDrive still lists, one fresh listing per directory."""
        listed: set[str] = set()
        for directory in sorted({str(PurePosixPath(path).parent) for path in files}):
            try:
                entries = await cloud.list_directory(directory)
            except FileNotFoundError:
                continue
            listed.update(str(entry['full_path']) for entry in entries if not entry['is_directory'])
        return tuple(path for path in files if path in listed)

    async def _watch_archive(self, task: MergeTask, paths: MergePaths) -> None:
        cloud = self._require_cloud()
        try:
            entries = await cloud.list_directory(paths.intake_dir)
        except FileNotFoundError:
            entries = ()
        if not any(entry['full_path'] == paths.target_dir for entry in entries):
            await self._repository.transition(
                task.id,
                MergeState.ARCHIVING,
                MergeState.DONE,
                finished_at=datetime.now(UTC),
                notice=None,
            )
            await self._trigger(PipelineName.MAPPING)
            return
        now = datetime.now(UTC)
        if now - task.state_changed_at > ARCHIVE_WAIT:
            whole = await self._whole_copy(task, paths)
            if whole is not None:
                msg = (
                    f'the library already holds {whole}, so the archive leaves {paths.target_dir} alone; '
                    'remove the task to delete the merged copy'
                )
                raise MergeFailedError(msg)
            msg = f'the archive has not filed {paths.target_dir}; check the archive run log'
            raise MergeFailedError(msg)
        if now - task.updated_at > ARCHIVE_NUDGE_INTERVAL:
            await self._trigger(PipelineName.ARCHIVE)
            await self._repository.update(task.id, notice='waiting for the archive run to file it')

    # -- operator actions ------------------------------------------------------

    async def cancel(self, task_id: int) -> MergeTask:  # noqa: C901 - one guard per state
        async with self._tick_lock:
            task = await self._repository.get(task_id)
            if task is None:
                msg = 'merge_task_not_found'
                raise MergeActionError(msg)
            if task.state not in CANCELLABLE_STATES:
                msg = 'merge_task_not_cancellable'
                raise MergeActionError(msg)
            if task.state == MergeState.FAILED and task.failed_state == MergeState.REPLACING:
                # The originals may already be gone; the merged file must not be dropped.
                msg = 'merge_task_not_cancellable'
                raise MergeActionError(msg)
            paths = merge_paths(task, self._merge_config(), self._archive_config())
            if task.state == MergeState.FAILED and task.failed_state == MergeState.ARCHIVING:
                await self._discard_duplicate(task, paths)
            if task.job_name and self._kube is not None:
                with suppress(KubeError):
                    await self._kube.delete_job(task.job_name)
            cloud = self._cloud()
            if cloud is not None and task.merged_sha1:
                with suppress(Exception):
                    copy = await self._copy_task(cloud, paths)
                    if copy is not None and copy['status'] != CopyStatus.COMPLETED:
                        await cloud.cancel_copy_task(str(copy['source_path']), paths.staging_dir)
                with suppress(Exception):
                    await cloud.delete_files([paths.staging_dir])
            for path in (paths.output, partial_path(paths.output), paths.status):
                path.unlink(missing_ok=True)
            shutil.rmtree(staging_dir(paths.output), ignore_errors=True)
            if task.transcoder == 'home':
                await self._discard_transcode(task)
            cancelled = await self._repository.transition(
                task.id,
                task.state,
                MergeState.CANCELLED,
                finished_at=datetime.now(UTC),
                notice=None,
            )
            if cancelled is None:
                msg = 'merge_task_changed'
                raise MergeActionError(msg)
            return cancelled

    async def _discard_duplicate(self, task: MergeTask, paths: MergePaths) -> None:
        """Delete a merged copy the archive refused because the library holds the title whole.

        The parts are gone by now, so the merged file may be dropped only when
        that whole copy is there to take its place.
        """
        cloud = self._cloud()
        try:
            whole = None if cloud is None else await self._whole_copy(task, paths)
        except Exception as exc:
            LOGGER.exception('could not look for a whole copy of %s', task.avid)
            msg = 'merge_discard_failed'
            raise MergeActionError(msg) from exc
        if cloud is None or whole is None:
            msg = 'merge_task_not_cancellable'
            raise MergeActionError(msg)
        try:
            await cloud.delete_files([paths.target_dir])
            intake = await cloud.list_directory(paths.intake_dir)
        except FileNotFoundError:
            intake = ()
        except Exception as exc:
            LOGGER.exception('could not delete the merged copy of %s', task.avid)
            msg = 'merge_discard_failed'
            raise MergeActionError(msg) from exc
        if any(entry['full_path'] == paths.target_dir for entry in intake):
            msg = 'merge_discard_failed'
            raise MergeActionError(msg)

    async def _reset_prepare(self, task: MergeTask) -> None:
        """Clear a failed audio Job and its report, so the next tick starts it again."""
        status = audio_status_path(self._transcode_dir(task))
        if (read_status(status) or {}).get('state') == 'failed':
            status.unlink(missing_ok=True)
        if self._kube is not None:
            with suppress(KubeError):
                await self._kube.delete_job(audio_job_name(task.id, task.avid))

    async def retry(self, task_id: int) -> MergeTask:
        async with self._tick_lock:
            task = await self._repository.get(task_id)
            if task is None:
                msg = 'merge_task_not_found'
                raise MergeActionError(msg)
            if task.state != MergeState.FAILED:
                msg = 'merge_task_not_failed'
                raise MergeActionError(msg)
            resume = {
                MergeState.QUEUED: MergeState.QUEUED,
                MergeState.MERGING: MergeState.QUEUED,
                MergeState.UPLOADING: MergeState.UPLOADING,
                MergeState.VERIFYING: MergeState.UPLOADING,
                MergeState.REPLACING: MergeState.REPLACING,
                MergeState.ARCHIVING: MergeState.ARCHIVING,
            }.get(task.failed_state or MergeState.QUEUED, MergeState.QUEUED)
            if task.transcoder == 'home' and task.failed_state in HOME_RETRY_STATES:
                # The chunks are kept: a failed audio copy is redone, a failed assembly run again.
                resume = MergeState.TRANSCODING
                await self._reset_prepare(task)
            fields: dict[str, object] = {'failed_state': None, 'error': None, 'notice': None}
            if resume in {MergeState.QUEUED, MergeState.TRANSCODING}:
                # The failed Job outlives the task by its TTL and holds the name
                # the new one is started under, which would count as started.
                if task.job_name and self._kube is not None:
                    with suppress(KubeError):
                        await self._kube.delete_job(task.job_name)
                fields.update(job_name=None, phase=None, progress=None)
            if resume == MergeState.UPLOADING:
                fields.update(upload_attempts=0)
            retried = await self._repository.transition(task.id, MergeState.FAILED, resume, **fields)
            if retried is None:
                msg = 'merge_task_changed'
                raise MergeActionError(msg)
        self.wake()
        return retried
