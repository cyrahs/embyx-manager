"""Driving merge tasks from the queue to the library.

Each task walks one path, one step per poll, and every step can be repeated
after a restart without harm:

1. **queued → merging**: start the title's merge Job, one Job at a time.
2. **merging → uploading**: the Job wrote the merged file and its SHA-1 to the
   work volume, which CloudDrive also serves.
3. **uploading → verifying**: CloudDrive copied it into a staging folder of its
   own under the 115 staging directory. A copy that failed is restarted; a
   merged file that went missing before the copy finished is merged again.
4. **verifying → replacing**: the staged file has the merged file's size and
   SHA-1.
5. **replacing → archiving**: the original parts are deleted (into 115's
   recycle bin), the merged file moves into ``{intake}/{source}/{AVID}/`` and
   the local copy is removed. The parts have to leave first: the archive
   refuses a title the library already holds.
6. **archiving → done**: the archive pipeline filed the folder away.

A step that cannot go on fails the task and records which state it failed in,
so a retry resumes there. A passing problem (CloudDrive unreachable, 115 not
yet reporting a hash) is shown as a notice while the step is retried.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

from embyx_manager.clients.clouddrive.aio import AsyncCloudDrive, CopyStatus
from embyx_manager.config.models import ArchiveConfig, CloudDriveConfig, MergeConfig
from embyx_manager.merge.kube import KubeError, KubeJobs, job_name, job_outcome
from embyx_manager.merge.tasks import (
    CANCELLABLE_STATES,
    MergeState,
    MergeTask,
    MergeTaskRepository,
)
from embyx_manager.merge.worker import partial_path, read_status
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
#: After this long without the archive filing the title, the task fails for a look.
ARCHIVE_WAIT = timedelta(hours=2)
SHA1_HASH_KEY = '2'
NO_JOB_TEMPLATE = 'the merge Job template is not mounted (EMBYX_MANAGER_MERGE_JOB_TEMPLATE)'


class MergeFailedError(Exception):
    """The task cannot go on; the message is shown to the operator."""


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
            merging = any(task.state == MergeState.MERGING for task in tasks)
            for task in tasks:
                if task.state == MergeState.FAILED:
                    continue
                if task.state == MergeState.QUEUED:
                    if merging:
                        continue
                    merging = True
                await self._step(task)

    async def _step(self, task: MergeTask) -> None:
        handlers = {
            MergeState.QUEUED: self._start_merge,
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
            return
        if status.get('state') == 'failed':
            raise MergeFailedError(str(status.get('error') or 'the merge Job failed'))
        job = await kube.get_job(task.job_name) if task.job_name else None
        if job is None:
            msg = 'the merge Job disappeared before it finished'
            raise MergeFailedError(msg)
        if job_outcome(job) == 'failed':
            tail = ''
            with suppress(KubeError):
                tail = await kube.log_tail(task.job_name or '')
            raise MergeFailedError('the merge Job failed' + (f': {tail}' if tail else ''))
        await self._repository.update(
            task.id,
            phase=status.get('phase') or task.phase,
            progress=status.get('progress', task.progress),
            notice=None,
        )

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
            await cloud.restart_copy_task(str(copy['source_path']), paths.staging_dir)
            await self._repository.update(task.id, upload_attempts=task.upload_attempts + 1, notice=errors)
            return
        await self._repository.update(
            task.id,
            uploaded_bytes=int(copy['uploaded_bytes']),
            notice='CloudDrive has paused the upload' if copy['paused'] else None,
        )

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
        await self._repository.transition(task.id, MergeState.VERIFYING, MergeState.REPLACING, notice=None)

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
            msg = f'the archive has not filed {paths.target_dir}; check the archive run log'
            raise MergeFailedError(msg)
        if now - task.updated_at > ARCHIVE_NUDGE_INTERVAL:
            await self._trigger(PipelineName.ARCHIVE)
            await self._repository.update(task.id, notice='waiting for the archive run to file it')

    # -- operator actions ------------------------------------------------------

    async def cancel(self, task_id: int) -> MergeTask:
        async with self._tick_lock:
            task = await self._repository.get(task_id)
            if task is None:
                msg = 'merge_task_not_found'
                raise MergeActionError(msg)
            if task.state not in CANCELLABLE_STATES:
                msg = 'merge_task_not_cancellable'
                raise MergeActionError(msg)
            if task.state == MergeState.FAILED and task.failed_state in {MergeState.REPLACING, MergeState.ARCHIVING}:
                # The originals may already be gone; the merged file must not be dropped.
                msg = 'merge_task_not_cancellable'
                raise MergeActionError(msg)
            paths = merge_paths(task, self._merge_config(), self._archive_config())
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
            fields: dict[str, object] = {'failed_state': None, 'error': None, 'notice': None}
            if resume == MergeState.QUEUED:
                fields.update(job_name=None, phase=None, progress=None)
            if resume == MergeState.UPLOADING:
                fields.update(upload_attempts=0)
            retried = await self._repository.transition(task.id, MergeState.FAILED, resume, **fields)
            if retried is None:
                msg = 'merge_task_changed'
                raise MergeActionError(msg)
        self.wake()
        return retried
