"""Merge tasks walked through their states against an in-memory CloudDrive and Job API."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any

import httpx
import pytest

from embyx_manager.clients.clouddrive.aio import CopyStatus, UploadState
from embyx_manager.config.models import ArchiveConfig, CloudDriveConfig, MergeConfig
from embyx_manager.merge.kube import KubeJobs, build_job, job_name, job_outcome
from embyx_manager.merge.service import MergeActionError, MergeService, api_path
from embyx_manager.merge.tasks import MergeState, MergeTask, MergeTaskRepository
from embyx_manager.monitor.reports import PipelineName
from tests.conftest import make_database

LIBRARY = '/mnt/cd2/115/embyx'
PARTS = tuple(f'{LIBRARY}/type/vr/SQTEVR/SQTEVR-009-cd{index}.mp4' for index in range(1, 4))
ARCHIVE = ArchiveConfig(
    src_dir='/mnt/cd2/115/embyx_in',
    dst_dir=LIBRARY,
    mapping={'rank': 'rank'},
    priority_mapping={'vr': 'type/vr'},
)
SHA1 = 'A' * 40


class FakeCloud:
    """CloudDrive's API paths as a dict; copies finish when a test says so."""

    def __init__(self) -> None:
        self.files: dict[str, dict[str, Any]] = {}
        self.directories: set[str] = {'/', '/115', '/115/upload', '/115/embyx_in', '/115/embyx/type/vr/SQTEVR'}
        self.copies: list[dict[str, object]] = []
        self.restarted: list[tuple[str, str]] = []
        self.deleted: list[str] = []
        self.fatal: dict[str, str] = {}
        #: Bytes the upload queue reports sent, by destination path.
        self.sent: dict[str, int] = {}

    def add_file(self, path: str, size: int, sha1: str = '') -> None:
        self.files[path] = {'size': size, 'hashes': {'2': sha1} if sha1 else {}}

    async def copy_tasks(self) -> tuple[dict[str, object], ...]:
        return tuple(self.copies)

    async def copy_file(self, source: str, destination: str) -> dict[str, object]:
        self.copies.append(
            {
                'source_path': source,
                'dest_path': destination,
                'status': CopyStatus.PENDING,
                'uploaded_bytes': 0,
                'total_bytes': 0,
                'paused': False,
                'errors': (),
            },
        )
        return {'success': True, 'error_message': ''}

    async def upload_state(self, path: str) -> UploadState | None:
        if path in self.fatal:
            return UploadState(status='FatalError', transferred=0, error=self.fatal[path])
        if path in self.sent:
            return UploadState(status='Transfer', transferred=self.sent[path])
        return None

    async def restart_copy_task(self, source: str, destination: str) -> None:
        self.restarted.append((source, destination))

    async def cancel_copy_task(self, source: str, destination: str) -> None:
        del source
        self.copies = [copy for copy in self.copies if copy['dest_path'] != destination]

    async def delete_files(self, paths: list[str]) -> dict[str, object]:
        for path in paths:
            self.deleted.append(path)
            self.files.pop(path, None)
            self.directories.discard(path)
        return {'success': True, 'error_message': ''}

    async def stat_file(self, path: str) -> dict[str, object] | None:
        entry = self.files.get(path)
        if entry is None:
            return None
        return {'full_path': path, 'name': PurePosixPath(path).name, 'is_directory': False, **entry}

    async def list_directory(self, directory: str) -> tuple[dict[str, object], ...]:
        if directory not in self.directories:
            raise FileNotFoundError(directory)
        files = [
            {'full_path': path, 'name': PurePosixPath(path).name, 'is_directory': False, **entry}
            for path, entry in self.files.items()
            if str(PurePosixPath(path).parent) == directory
        ]
        folders = [
            {'full_path': path, 'name': PurePosixPath(path).name, 'is_directory': True}
            for path in self.directories
            if path != '/' and str(PurePosixPath(path).parent) == directory
        ]
        return tuple(files + folders)

    async def ensure_directory(self, parent: str, name: str) -> dict[str, object]:
        self.directories.add(f'{parent.rstrip("/")}/{name}')
        return {'success': True}

    async def move_file(self, source: str, destination: str) -> dict[str, object]:
        target = f'{destination}/{PurePosixPath(source).name}'
        self.files[target] = self.files.pop(source)
        return {'success': True, 'error_message': ''}


class FakeKube:
    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, Any]] = {}
        self.created: list[tuple[str, list[str]]] = []
        self.deleted: list[str] = []

    async def create_job(self, *, name: str, args: list[str], task_id: int) -> None:
        del task_id
        self.created.append((name, list(args)))
        # As the API answers 409 for a name in use, an existing Job is kept.
        self.jobs.setdefault(name, {'status': {}})

    async def get_job(self, name: str) -> dict[str, Any] | None:
        return self.jobs.get(name)

    async def delete_job(self, name: str) -> None:
        self.deleted.append(name)
        self.jobs.pop(name, None)

    async def log_tail(self, name: str, *, lines: int = 20) -> str:
        del name, lines
        return 'ffmpeg: boom'

    async def aclose(self) -> None:
        return None


class Harness:
    def __init__(self, tmp_path: Path) -> None:
        self.work = tmp_path / 'upload'
        self.work.mkdir()
        self.cloud = FakeCloud()
        for part in PARTS:
            self.cloud.add_file(api_path(part, '/mnt/cd2'), 100)
        self.kube = FakeKube()
        self.triggered: list[PipelineName] = []
        self.repository = MergeTaskRepository(make_database())
        self.config = MergeConfig(work_dir=str(self.work))

        async def trigger(pipeline: PipelineName) -> None:
            self.triggered.append(pipeline)

        self.service = MergeService(
            repository=self.repository,
            cloud=lambda: self.cloud,  # type: ignore[arg-type,return-value]
            kube=self.kube,  # type: ignore[arg-type]
            merge_config=lambda: self.config,
            archive_config=lambda: ARCHIVE,
            clouddrive_config=lambda: CloudDriveConfig(address='cd:19798', api_token='t'),
            trigger=trigger,
        )

    async def create(self) -> MergeTask:
        return await self.repository.create(
            avid='SQTEVR-009',
            source='vr',
            library_dir='type/vr',
            brand='SQTEVR',
            parts=PARTS,
            merged_name='SQTEVR-009.mp4',
        )

    async def state(self, task: MergeTask) -> MergeTask:
        current = await self.repository.get(task.id)
        assert current is not None
        return current

    def finish_merge(self, task: MergeTask, *, size: int = 300) -> None:
        (self.work / 'SQTEVR-009.mp4').write_bytes(b'x' * size)
        status = self.work / '.merge' / f'{task.id}.json'
        status.parent.mkdir(exist_ok=True)
        status.write_text(json.dumps({'state': 'done', 'size': size, 'sha1': SHA1.lower()}))

    async def age(self, task: MergeTask, by: timedelta) -> None:
        """Date the task's last update ``by`` earlier than now."""
        pool = await self.repository._database.get_pool()  # noqa: SLF001
        await pool.execute(
            'UPDATE merge_tasks SET updated_at = $2 WHERE id = $1',
            task.id,
            datetime.now(UTC) - by,
        )

    def staging(self, task: MergeTask) -> str:
        return f'/115/upload/merge-{task.id}'

    def complete_copy(self, task: MergeTask, *, size: int = 300, sha1: str = SHA1) -> None:
        for copy in self.cloud.copies:
            copy['status'] = CopyStatus.COMPLETED
        self.cloud.add_file(f'{self.staging(task)}/SQTEVR-009.mp4', size, sha1)


@pytest.fixture
def harness(tmp_path: Path) -> Harness:
    return Harness(tmp_path)


async def test_a_title_walks_from_the_queue_into_the_intake_route(harness: Harness) -> None:
    task = await harness.create()

    await harness.service.tick()
    task = await harness.state(task)
    assert task.state == MergeState.MERGING
    ((name, args),) = harness.kube.created
    assert name == task.job_name == f'embyx-merge-{task.id}-sqtevr-009'
    assert args[:6] == [
        '--output',
        str(harness.work / 'SQTEVR-009.mp4'),
        '--status',
        str(harness.work / '.merge' / f'{task.id}.json'),
        '--reserve-gib',
        '20',
    ]
    assert args[6:] == ['--', *PARTS]

    harness.finish_merge(task)
    await harness.service.tick()
    task = await harness.state(task)
    assert (task.state, task.merged_bytes, task.merged_sha1) == (MergeState.UPLOADING, 300, SHA1)
    assert harness.kube.deleted == [name]

    await harness.service.tick()
    assert [(copy['source_path'], copy['dest_path']) for copy in harness.cloud.copies] == [
        ('/downloads/upload/SQTEVR-009.mp4', harness.staging(task)),
    ]
    assert (await harness.state(task)).upload_attempts == 1

    harness.complete_copy(task)
    await harness.service.tick()
    assert (await harness.state(task)).state == MergeState.VERIFYING
    await harness.service.tick()
    assert (await harness.state(task)).state == MergeState.REPLACING

    await harness.service.tick()
    task = await harness.state(task)
    assert task.state == MergeState.ARCHIVING
    assert not any(path.startswith('/115/embyx/') for path in harness.cloud.files)
    assert '/115/embyx_in/vr/SQTEVR-009/SQTEVR-009.mp4' in harness.cloud.files
    assert harness.staging(task) in harness.cloud.deleted
    assert not (harness.work / 'SQTEVR-009.mp4').exists()
    assert harness.triggered == [PipelineName.ARCHIVE]

    await harness.service.tick()
    assert (await harness.state(task)).state == MergeState.ARCHIVING
    harness.cloud.files.pop('/115/embyx_in/vr/SQTEVR-009/SQTEVR-009.mp4')
    harness.cloud.directories.discard('/115/embyx_in/vr/SQTEVR-009')
    await harness.service.tick()
    task = await harness.state(task)
    assert task.state == MergeState.DONE
    assert task.finished_at is not None
    assert harness.triggered == [PipelineName.ARCHIVE, PipelineName.MAPPING]


async def test_only_one_title_merges_at_a_time(harness: Harness) -> None:
    first = await harness.create()
    second = await harness.repository.create(
        avid='ABP-123', source='rank', library_dir='rank', brand='ABP', parts=PARTS, merged_name='ABP-123.mp4'
    )

    await harness.service.tick()
    await harness.service.tick()

    assert (await harness.state(first)).state == MergeState.MERGING
    assert (await harness.state(second)).state == MergeState.QUEUED
    assert len(harness.kube.created) == 1


async def test_a_failed_job_fails_the_task_with_its_log(harness: Harness) -> None:
    task = await harness.create()
    await harness.service.tick()
    task = await harness.state(task)
    harness.kube.jobs[task.job_name or '']['status'] = {'conditions': [{'type': 'Failed', 'status': 'True'}]}

    await harness.service.tick()

    task = await harness.state(task)
    assert (task.state, task.failed_state) == (MergeState.FAILED, MergeState.MERGING)
    assert task.error == 'the merge Job failed: ffmpeg: boom'


async def test_a_refused_merge_fails_the_task_though_its_job_completed(harness: Harness) -> None:
    task = await harness.create()
    await harness.service.tick()
    task = await harness.state(task)
    status = harness.work / '.merge' / f'{task.id}.json'
    status.parent.mkdir(exist_ok=True)
    status.write_text(json.dumps({'state': 'failed', 'refused': True, 'error': 'cd2 carries different streams'}))
    harness.kube.jobs[task.job_name or '']['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}]}

    await harness.service.tick()

    task = await harness.state(task)
    assert (task.state, task.failed_state) == (MergeState.FAILED, MergeState.MERGING)
    assert task.error == 'cd2 carries different streams'


async def test_a_job_that_completed_without_a_report_fails_the_task(harness: Harness) -> None:
    task = await harness.create()
    await harness.service.tick()
    task = await harness.state(task)
    harness.kube.jobs[task.job_name or '']['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}]}

    await harness.service.tick()

    task = await harness.state(task)
    assert (task.state, task.failed_state) == (MergeState.FAILED, MergeState.MERGING)
    assert task.error == 'the merge Job ended without reporting a result'


async def test_a_killed_job_leaves_no_half_written_merge_behind(harness: Harness) -> None:
    task = await harness.create()
    await harness.service.tick()
    task = await harness.state(task)
    # An OOM kill gives the worker no chance to remove these itself.
    partial = harness.work / '.SQTEVR-009.mp4.part'
    partial.write_bytes(b'x')
    (harness.work / '.SQTEVR-009.mp4.parts').mkdir()
    (harness.work / '.SQTEVR-009.mp4.parts' / 'SQTEVR-009-cd1.mp4').write_bytes(b'x')
    harness.kube.jobs[task.job_name or '']['status'] = {'conditions': [{'type': 'Failed', 'status': 'True'}]}

    await harness.service.tick()

    assert (await harness.state(task)).state == MergeState.FAILED
    assert sorted(path.name for path in harness.work.iterdir()) == []


async def test_a_retried_merge_starts_a_fresh_job_under_the_same_name(harness: Harness) -> None:
    task = await harness.create()
    await harness.service.tick()
    task = await harness.state(task)
    name = task.job_name or ''
    harness.kube.jobs[name]['status'] = {'conditions': [{'type': 'Failed', 'status': 'True'}]}
    await harness.service.tick()

    await harness.service.retry(task.id)
    await harness.service.tick()
    await harness.service.tick()

    task = await harness.state(task)
    assert task.state == MergeState.MERGING
    assert harness.kube.deleted == [name]
    assert harness.kube.jobs[name] == {'status': {}}


async def test_a_job_past_its_deadline_fails_the_task_with_the_reason(harness: Harness) -> None:
    task = await harness.create()
    await harness.service.tick()
    task = await harness.state(task)
    harness.kube.jobs[task.job_name or '']['status'] = {
        'conditions': [
            {
                'type': 'Failed',
                'status': 'True',
                'reason': 'DeadlineExceeded',
                'message': 'Job was active longer than specified deadline',
            }
        ]
    }

    await harness.service.tick()

    task = await harness.state(task)
    assert task.error == 'the merge Job failed: Job was active longer than specified deadline | ffmpeg: boom'


async def test_a_merged_file_lost_before_the_upload_finished_is_merged_again(harness: Harness) -> None:
    task = await harness.create()
    await harness.service.tick()
    harness.finish_merge(await harness.state(task))
    await harness.service.tick()
    await harness.service.tick()

    (harness.work / 'SQTEVR-009.mp4').unlink()
    await harness.service.tick()

    task = await harness.state(task)
    assert task.state == MergeState.QUEUED
    assert task.notice == 'the merged file was lost before the upload finished; merging again'


async def test_a_copy_cloud_drive_stopped_listing_is_not_uploaded_again(harness: Harness) -> None:
    task = await harness.create()
    await harness.service.tick()
    harness.finish_merge(await harness.state(task))
    await harness.service.tick()
    await harness.service.tick()

    harness.cloud.copies.clear()
    await harness.service.tick()
    assert len(harness.cloud.copies) == 0
    assert (await harness.state(task)).state == MergeState.UPLOADING

    harness.cloud.add_file(f'{harness.staging(task)}/SQTEVR-009.mp4', 300, SHA1)
    await harness.service.tick()
    assert (await harness.state(task)).state == MergeState.VERIFYING
    assert len(harness.cloud.copies) == 0


async def test_upload_progress_comes_from_the_upload_queue_while_the_copy_task_counts_nothing(
    harness: Harness,
) -> None:
    task = await harness.create()
    await harness.service.tick()
    harness.finish_merge(await harness.state(task))
    await harness.service.tick()
    await harness.service.tick()
    # CloudDrive keeps the copy task at "Scanned" with 0 bytes until the file is up.
    harness.cloud.copies[0].update(status=CopyStatus.SCANNED)
    harness.cloud.sent[f'{harness.staging(task)}/SQTEVR-009.mp4'] = 240

    await harness.service.tick()

    task = await harness.state(task)
    assert (task.state, task.uploaded_bytes, task.notice) == (MergeState.UPLOADING, 240, None)


async def test_a_failed_copy_is_restarted_then_given_up_on(harness: Harness) -> None:
    task = await harness.create()
    await harness.service.tick()
    harness.finish_merge(await harness.state(task))
    await harness.service.tick()
    await harness.service.tick()
    harness.cloud.copies[0].update(status=CopyStatus.FAILED, errors=('115 refused',))

    # The first failure rests five minutes before the copy is restarted.
    await harness.service.tick()
    await harness.service.tick()
    assert harness.cloud.restarted == []
    assert (await harness.state(task)).notice == 'upload attempt 2 of 5 starts in 5 min: 115 refused'

    await harness.age(task, timedelta(minutes=5))
    await harness.service.tick()
    assert harness.cloud.restarted == [('/downloads/upload/SQTEVR-009.mp4', harness.staging(task))]
    task = await harness.state(task)
    assert (task.upload_attempts, task.notice) == (2, '115 refused')

    await harness.repository.update(task.id, upload_attempts=5)
    await harness.service.tick()
    task = await harness.state(task)
    assert (task.state, task.failed_state) == (MergeState.FAILED, MergeState.UPLOADING)


async def test_an_upload_clouddrive_gave_up_on_fails_the_task(harness: Harness) -> None:
    task = await harness.create()
    await harness.service.tick()
    harness.finish_merge(await harness.state(task))
    await harness.service.tick()
    await harness.service.tick()
    # The copy task itself still reads "Scanned"; only the upload queue knows.
    harness.cloud.copies[0].update(status=CopyStatus.SCANNED)
    harness.cloud.fatal[f'{harness.staging(task)}/SQTEVR-009.mp4'] = 'code: 10025, message: 单文件不能大于115GB'

    await harness.service.tick()

    task = await harness.state(task)
    assert (task.state, task.failed_state) == (MergeState.FAILED, MergeState.UPLOADING)
    assert task.error == 'CloudDrive gave up uploading SQTEVR-009.mp4: code: 10025, message: 单文件不能大于115GB'


async def test_an_upload_lost_to_a_network_error_is_restarted_then_given_up_on(harness: Harness) -> None:
    task = await harness.create()
    await harness.service.tick()
    harness.finish_merge(await harness.state(task))
    await harness.service.tick()
    await harness.service.tick()
    harness.cloud.copies[0].update(status=CopyStatus.SCANNED)
    harness.cloud.fatal[f'{harness.staging(task)}/SQTEVR-009.mp4'] = 'Reqwest error error sending request'

    await harness.service.tick()
    await harness.service.tick()

    # Each failure rests longer before the next attempt: 5 min, 15 min, 1 h, 3 h.
    assert harness.cloud.restarted == []
    task = await harness.state(task)
    assert (task.state, task.notice) == (
        MergeState.UPLOADING,
        'upload attempt 2 of 5 starts in 5 min: Reqwest error error sending request',
    )
    await harness.age(task, timedelta(minutes=4))
    await harness.service.tick()
    assert harness.cloud.restarted == []

    await harness.age(task, timedelta(minutes=5))
    await harness.service.tick()
    await harness.service.tick()
    # Restarted once, then left alone while CloudDrive queues it again.
    assert harness.cloud.restarted == [('/downloads/upload/SQTEVR-009.mp4', harness.staging(task))]
    task = await harness.state(task)
    assert (task.upload_attempts, task.notice) == (2, 'Reqwest error error sending request')

    # Still failing once CloudDrive had its moment: the next attempt rests 15 minutes.
    await harness.age(task, timedelta(minutes=2))
    await harness.service.tick()
    task = await harness.state(task)
    assert task.notice == 'upload attempt 3 of 5 starts in 15 min: Reqwest error error sending request'
    await harness.age(task, timedelta(minutes=14))
    await harness.service.tick()
    assert len(harness.cloud.restarted) == 1
    await harness.age(task, timedelta(minutes=15))
    await harness.service.tick()
    assert len(harness.cloud.restarted) == 2

    await harness.repository.update(task.id, upload_attempts=4, notice=None)
    await harness.service.tick()
    assert (await harness.state(task)).notice == (
        'upload attempt 5 of 5 starts in 3 h: Reqwest error error sending request'
    )

    await harness.repository.update(task.id, upload_attempts=5)
    await harness.service.tick()
    task = await harness.state(task)
    assert (task.state, task.failed_state) == (MergeState.FAILED, MergeState.UPLOADING)


async def test_a_sha1_mismatch_fails_before_any_original_is_touched(harness: Harness) -> None:
    task = await harness.create()
    await harness.service.tick()
    harness.finish_merge(await harness.state(task))
    for _ in range(2):
        await harness.service.tick()
    harness.complete_copy(task, sha1='B' * 40)
    await harness.service.tick()
    await harness.service.tick()

    task = await harness.state(task)
    assert (task.state, task.failed_state) == (MergeState.FAILED, MergeState.VERIFYING)
    assert 'SHA-1' in (task.error or '')
    assert all(api_path(part, '/mnt/cd2') in harness.cloud.files for part in PARTS)

    retried = await harness.service.retry(task.id)
    assert retried.state == MergeState.UPLOADING


async def test_verifying_waits_for_115_to_report_the_hash(harness: Harness) -> None:
    task = await harness.create()
    await harness.service.tick()
    harness.finish_merge(await harness.state(task))
    for _ in range(2):
        await harness.service.tick()
    harness.complete_copy(task, sha1='')
    await harness.service.tick()

    await harness.service.tick()

    task = await harness.state(task)
    assert task.state == MergeState.VERIFYING
    assert task.notice == 'waiting for 115 to report the SHA-1'


async def test_cancelling_a_merge_removes_its_job_and_files(harness: Harness) -> None:
    task = await harness.create()
    await harness.service.tick()
    task = await harness.state(task)
    (harness.work / '.SQTEVR-009.mp4.part').write_bytes(b'partial')

    cancelled = await harness.service.cancel(task.id)

    assert cancelled.state == MergeState.CANCELLED
    assert harness.kube.deleted == [task.job_name]
    assert not (harness.work / '.SQTEVR-009.mp4.part').exists()
    with pytest.raises(MergeActionError, match='merge_task_not_cancellable'):
        await harness.service.cancel(task.id)


async def test_a_task_that_failed_after_the_originals_went_cannot_be_dismissed(harness: Harness) -> None:
    task = await harness.create()
    for state in (MergeState.MERGING, MergeState.UPLOADING, MergeState.VERIFYING, MergeState.REPLACING):
        task = await harness.repository.transition(task.id, task.state, state)  # type: ignore[assignment]
    await harness.repository.transition(task.id, MergeState.REPLACING, MergeState.FAILED, failed_state='replacing')

    with pytest.raises(MergeActionError, match='merge_task_not_cancellable'):
        await harness.service.cancel(task.id)
    assert (await harness.service.retry(task.id)).state == MergeState.REPLACING


WHOLE = '/115/embyx/type/vr/SQTEVR/SQTEVR-0009.mp4'


async def test_a_title_the_library_holds_whole_is_not_merged(harness: Harness) -> None:
    harness.cloud.add_file(WHOLE, 900)
    task = await harness.create()

    await harness.service.tick()

    task = await harness.state(task)
    assert (task.state, task.failed_state) == (MergeState.FAILED, MergeState.QUEUED)
    assert task.error == (
        'the library already holds SQTEVR-0009.mp4, a whole copy of SQTEVR-009; '
        'merging its parts would only duplicate it'
    )
    assert harness.kube.created == []
    assert (await harness.service.cancel(task.id)).state == MergeState.CANCELLED


async def test_a_whole_copy_filed_during_the_upload_keeps_the_parts(harness: Harness) -> None:
    task = await harness.create()
    await harness.service.tick()
    harness.finish_merge(await harness.state(task))
    for _ in range(2):
        await harness.service.tick()
    harness.complete_copy(task)
    await harness.service.tick()
    harness.cloud.add_file(WHOLE, 900)

    await harness.service.tick()

    task = await harness.state(task)
    assert (task.state, task.failed_state) == (MergeState.FAILED, MergeState.VERIFYING)
    assert all(api_path(part, '/mnt/cd2') in harness.cloud.files for part in PARTS)
    assert (await harness.service.cancel(task.id)).state == MergeState.CANCELLED
    assert harness.staging(task) in harness.cloud.deleted


async def archiving_too_long(harness: Harness) -> MergeTask:
    """A task whose merged file has sat unfiled in the intake route past the wait."""
    task = await harness.create()
    for state in (
        MergeState.MERGING,
        MergeState.UPLOADING,
        MergeState.VERIFYING,
        MergeState.REPLACING,
        MergeState.ARCHIVING,
    ):
        task = await harness.repository.transition(task.id, task.state, state)  # type: ignore[assignment]
    for part in PARTS:
        harness.cloud.files.pop(api_path(part, '/mnt/cd2'))
    harness.cloud.directories.add('/115/embyx_in/vr')
    harness.cloud.directories.add('/115/embyx_in/vr/SQTEVR-009')
    harness.cloud.add_file('/115/embyx_in/vr/SQTEVR-009/SQTEVR-009.mp4', 300, SHA1)
    pool = await harness.repository._database.get_pool()  # noqa: SLF001
    await pool.execute(
        'UPDATE merge_tasks SET state_changed_at = $2, updated_at = $2 WHERE id = $1',
        task.id,
        datetime.now(UTC) - timedelta(hours=3),
    )
    return task


async def test_a_merged_copy_the_archive_refuses_for_a_whole_copy_can_be_removed(harness: Harness) -> None:
    task = await archiving_too_long(harness)
    harness.cloud.add_file(WHOLE, 900)

    await harness.service.tick()

    task = await harness.state(task)
    assert (task.state, task.failed_state) == (MergeState.FAILED, MergeState.ARCHIVING)
    assert task.error == (
        'the library already holds SQTEVR-0009.mp4, so the archive leaves /115/embyx_in/vr/SQTEVR-009 alone; '
        'remove the task to delete the merged copy'
    )
    cancelled = await harness.service.cancel(task.id)
    assert cancelled.state == MergeState.CANCELLED
    assert harness.cloud.deleted == ['/115/embyx_in/vr/SQTEVR-009']
    assert WHOLE in harness.cloud.files


async def test_the_only_merged_copy_cannot_be_removed(harness: Harness) -> None:
    task = await archiving_too_long(harness)

    await harness.service.tick()

    task = await harness.state(task)
    assert task.error == 'the archive has not filed /115/embyx_in/vr/SQTEVR-009; check the archive run log'
    with pytest.raises(MergeActionError, match='merge_task_not_cancellable'):
        await harness.service.cancel(task.id)
    assert harness.cloud.deleted == []
    assert (await harness.service.retry(task.id)).state == MergeState.ARCHIVING


async def test_unavailable_without_a_job_template(harness: Harness) -> None:
    service = MergeService(
        repository=harness.repository,
        cloud=lambda: None,
        kube=None,
        merge_config=MergeConfig,
        archive_config=lambda: ARCHIVE,
        clouddrive_config=lambda: CloudDriveConfig(address='cd:19798', api_token='t'),
        trigger=harness.service._trigger,  # noqa: SLF001
    )
    assert service.unavailable() == 'the merge Job template is not mounted (EMBYX_MANAGER_MERGE_JOB_TEMPLATE)'
    outside = ARCHIVE.model_copy(update={'src_dir': '/elsewhere/embyx_in'})
    harness.service._archive_config = lambda: outside  # noqa: SLF001
    assert harness.service.unavailable() == 'archive.src_dir must lie under merge.cloud_mount_prefix'


def test_job_names_are_dns_labels() -> None:
    assert job_name(7, 'SQTEVR-009') == 'embyx-merge-7-sqtevr-009'
    assert job_name(12345, 'A' * 80) == 'embyx-merge-12345-' + 'a' * 34
    assert len(job_name(1, 'X_Y.Z' * 20)) <= 52


def test_build_job_fills_in_name_image_args_and_label() -> None:
    template = {
        'apiVersion': 'batch/v1',
        'kind': 'Job',
        'metadata': {'labels': {'app': 'embyx-merge'}},
        'spec': {
            'template': {'spec': {'containers': [{'name': 'merge', 'command': ['embyx-manager', 'merge-worker']}]}}
        },
    }

    job = build_job(template, name='embyx-merge-1-abc', image='ghcr.io/x@sha256:1', args=['--output', 'o'], task_id=1)

    assert job['metadata'] == {
        'name': 'embyx-merge-1-abc',
        'labels': {'app': 'embyx-merge', 'embyx-manager/merge-task': '1'},
    }
    container = job['spec']['template']['spec']['containers'][0]
    assert (container['image'], container['args']) == ('ghcr.io/x@sha256:1', ['--output', 'o'])
    assert 'name' not in template['metadata']


def test_job_outcome_reads_conditions_and_counters() -> None:
    assert job_outcome({'status': {'conditions': [{'type': 'Complete', 'status': 'True'}]}}) == 'succeeded'
    assert job_outcome({'status': {'conditions': [{'type': 'Failed', 'status': 'True'}]}}) == 'failed'
    assert job_outcome({'status': {'failed': 1}}) == 'failed'
    assert job_outcome({'status': {'active': 1}}) == 'running'


async def test_kube_client_runs_the_job_on_the_pods_own_image(tmp_path: Path) -> None:
    template = tmp_path / 'job.json'
    template.write_text(
        json.dumps({'metadata': {}, 'spec': {'template': {'spec': {'containers': [{'name': 'merge'}]}}}}),
    )
    token = tmp_path / 'token'
    token.write_text('secret-token\n')
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == '/api/v1/namespaces/media/pods/app-0':
            return httpx.Response(
                200,
                json={
                    'spec': {'containers': [{'image': 'ghcr.io/cyrahs/embyx-manager:latest'}]},
                    'status': {'containerStatuses': [{'imageID': 'ghcr.io/cyrahs/embyx-manager@sha256:abc'}]},
                },
            )
        if request.method == 'POST':
            return httpx.Response(409, json={'reason': 'AlreadyExists'})
        return httpx.Response(404, json={})

    kube = KubeJobs(
        api_url='https://kube.test',
        namespace='media',
        pod_name='app-0',
        template_path=template,
        token_path=token,
        ca_path=None,
        transport=httpx.MockTransport(handler),
    )
    try:
        await kube.create_job(name='embyx-merge-1-x', args=['a'], task_id=1)
        assert await kube.get_job('embyx-merge-1-x') is None
    finally:
        await kube.aclose()

    created = json.loads(seen[1].content)
    assert created['spec']['template']['spec']['containers'][0]['image'] == 'ghcr.io/cyrahs/embyx-manager@sha256:abc'
    assert seen[1].headers['Authorization'] == 'Bearer secret-token'


async def test_archiving_gives_up_after_the_archive_never_files_the_title(harness: Harness) -> None:
    task = await harness.create()
    for state in (
        MergeState.MERGING,
        MergeState.UPLOADING,
        MergeState.VERIFYING,
        MergeState.REPLACING,
        MergeState.ARCHIVING,
    ):
        task = await harness.repository.transition(task.id, task.state, state)  # type: ignore[assignment]
    harness.cloud.directories.update({'/115/embyx_in/vr', '/115/embyx_in/vr/SQTEVR-009'})
    pool = await harness.repository._database.get_pool()  # noqa: SLF001
    await pool.execute(
        'UPDATE merge_tasks SET state_changed_at = $2, updated_at = $2 WHERE id = $1',
        task.id,
        datetime.now(UTC) - timedelta(hours=3),
    )

    await harness.service.tick()

    task = await harness.state(task)
    assert (task.state, task.failed_state) == (MergeState.FAILED, MergeState.ARCHIVING)


async def test_the_repository_remembers_removed_titles_and_the_last_filed_one(harness: Harness) -> None:
    removed = await harness.create()
    assert await harness.repository.last_done_at() is None
    await harness.service.cancel(removed.id)
    filed = await harness.create()
    finished_at = datetime(2026, 10, 5, 19, 0, tzinfo=UTC)
    await harness.repository.transition(filed.id, MergeState.QUEUED, MergeState.DONE, finished_at=finished_at)

    assert await harness.repository.cancelled_avids() == frozenset({'SQTEVR-009'})
    assert await harness.repository.last_done_at() == finished_at
