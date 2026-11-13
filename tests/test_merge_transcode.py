"""Titles over 115 GB handed to the home transcode worker, against PostgreSQL and the worker's API."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from embyx_manager.config.models import CloudDriveConfig, MergeConfig
from embyx_manager.merge import service as service_module
from embyx_manager.merge.service import MergeActionError, MergeService
from embyx_manager.merge.tasks import MergeState, MergeTask, MergeTaskRepository
from embyx_manager.merge.transcode import (
    TranscodeRepository,
    WorkerReport,
    audio_job_name,
    audio_status_path,
    chunk_path,
    transcode_dir,
)
from embyx_manager.merge.transcode_api import create_transcode_app, create_transcode_router
from tests.conftest import make_database
from tests.test_merge_service import ARCHIVE, PARTS, SHA1, FakeCloud, FakeKube

BIG = 200 * 10**9
DURATIONS = (30.0, 20.0, 25.0)


class Harness:
    def __init__(self, tmp_path: Path) -> None:
        self.work = tmp_path / 'upload'
        self.work.mkdir()
        self.cloud = FakeCloud()
        self.kube = FakeKube()
        database = make_database()
        self.repository = MergeTaskRepository(database)
        self.transcodes = TranscodeRepository(database)
        self.config = MergeConfig(work_dir=str(self.work), home_transcode_enabled=True)
        self.part_file = tmp_path / 'part.bin'
        self.part_file.write_bytes(bytes(range(256)) * 64)

        async def trigger(_pipeline: object) -> None:
            return None

        self.service = MergeService(
            repository=self.repository,
            cloud=lambda: self.cloud,  # type: ignore[arg-type,return-value]
            kube=self.kube,  # type: ignore[arg-type]
            merge_config=lambda: self.config,
            archive_config=lambda: ARCHIVE,
            clouddrive_config=lambda: CloudDriveConfig(address='cd:19798', api_token='t'),
            trigger=trigger,
            transcodes=self.transcodes,
            # The SQTEVR parts are the oversize title; anything else is small.
            sizes=lambda parts: BIG if 'SQTEVR' in parts[0] else 300,
        )
        app = create_transcode_app(create_transcode_router(self.service, token='secret'))
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url='http://worker',
            headers={'Authorization': 'Bearer secret'},
        )

    async def create(self, avid: str = 'SQTEVR-009', parts: tuple[str, ...] = PARTS) -> MergeTask:
        return await self.repository.create(
            avid=avid,
            source='vr',
            library_dir='type/vr',
            brand=avid.split('-', maxsplit=1)[0],
            parts=parts,
            merged_name=f'{avid}.mp4',
        )

    async def state(self, task: MergeTask) -> MergeTask:
        current = await self.repository.get(task.id)
        assert current is not None
        return current

    async def claim(self) -> dict[str, Any]:
        response = await self.client.post('/api/transcode/claim', json={'name': 'pc', 'state': 'idle'})
        assert response.status_code == 200, response.text
        return response.json()

    def prepare(self, task: MergeTask, *, state: str = 'audio') -> None:
        """What the audio Job reports once it has probed the parts."""
        path = audio_status_path(transcode_dir(self.work, task.id))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    'state': state,
                    'video_bitrate': 20_000_000,
                    'duration': sum(DURATIONS),
                    'parts': [{'duration': seconds, 'size': BIG // 3} for seconds in DURATIONS],
                },
            ),
        )

    async def put(
        self, task: MergeTask, part: int, index: int, start: float, duration: float, body: bytes = b'chunk'
    ) -> httpx.Response:
        return await self.client.put(
            f'/api/transcode/tasks/{task.id}/parts/{part}/chunks/{index}',
            content=body,
            headers={
                'X-Chunk-Start': str(start),
                'X-Chunk-Duration': str(duration),
                'X-Chunk-Frames': str(int(duration * 30)),
                'X-Chunk-Sha256': hashlib.sha256(body).hexdigest(),
            },
        )

    async def finish(self, task: MergeTask, part: int) -> httpx.Response:
        return await self.client.post(f'/api/transcode/tasks/{task.id}/parts/{part}/finish')

    async def encode_all(self, task: MergeTask) -> None:
        for part, seconds in enumerate(DURATIONS):
            assert (await self.put(task, part, 0, 0.0, seconds, f'cd{part}'.encode())).status_code == 200
            assert (await self.finish(task, part)).status_code == 200


@pytest.fixture
async def harness(tmp_path: Path):
    harness = Harness(tmp_path)
    yield harness
    await harness.client.aclose()


async def test_an_oversize_title_waits_for_the_worker_without_holding_up_the_queue(harness: Harness) -> None:
    big = await harness.create()
    small = await harness.create('ABP-123', tuple(f'/mnt/cd2/115/embyx/rank/ABP-123-cd{n}.mp4' for n in (1, 2)))

    await harness.service.tick()

    big = await harness.state(big)
    assert (big.state, big.notice) == (MergeState.QUEUED, service_module.WAITING_FOR_WORKER)
    assert harness.service.parked(big)
    assert (await harness.state(small)).state == MergeState.MERGING
    # The worker waits while the cluster merges something else, and once it is
    # there the title holds back auto-merge so that it comes next.
    assert (await harness.claim()) == {'task': None, 'wait': 'busy'}
    assert not harness.service.parked(big)
    assert harness.service.reserved_bytes((big,)) > 0


async def test_without_the_setting_an_oversize_title_goes_to_the_clusters_own_encoder(harness: Harness) -> None:
    harness.config = MergeConfig(work_dir=str(harness.work))
    task = await harness.create()

    await harness.service.tick()

    task = await harness.state(task)
    assert task.state == MergeState.MERGING
    assert (await harness.claim()) == {'task': None, 'wait': 'idle'}


async def test_the_worker_streams_chunks_and_the_cluster_assembles_them(harness: Harness) -> None:  # noqa: PLR0915
    task = await harness.create()
    await harness.service.tick()

    assert (await harness.claim()) == {'task': None, 'wait': 'probing', 'task_id': task.id}
    task = await harness.state(task)
    assert (task.state, task.transcoder) == (MergeState.TRANSCODING, 'home')
    ((name, args),) = harness.kube.created
    assert name == audio_job_name(task.id, task.avid)
    assert '--home-prepare' in args
    assert args[-3:] == list(PARTS)

    harness.prepare(task)
    answer = await harness.claim()
    assigned = answer['task']
    assert assigned['id'] == task.id
    assert assigned['video_bitrate'] == 20_000_000
    assert [(part['index'], part['next_chunk'], part['resume_at'], part['done']) for part in assigned['parts']] == [
        (0, 0, 0.0, False),
        (1, 0, 0.0, False),
        (2, 0, 0.0, False),
    ]

    first = await harness.put(task, 0, 0, 0.0, 20.0, b'first')
    assert first.json() == {'stored': True}
    # A repeat of a stored chunk is taken as already there.
    assert (await harness.put(task, 0, 0, 0.0, 20.0, b'first')).json() == {'stored': False}
    # Not the chunk the cluster has under that number.
    conflict = await harness.put(task, 0, 0, 0.0, 20.0, b'other')
    assert (conflict.status_code, conflict.json()['error']['code']) == (409, 'transcode_chunk_conflict')
    # A gap in the part.
    gap = await harness.put(task, 0, 1, 25.0, 5.0)
    assert gap.status_code == 409
    assert gap.json()['error'] == {'code': 'transcode_chunk_out_of_order', 'next_chunk': 1, 'resume_at': 20.0}
    # Bytes that do not match their hash.
    corrupt = await harness.client.put(
        f'/api/transcode/tasks/{task.id}/parts/0/chunks/1',
        content=b'broken',
        headers={
            'X-Chunk-Start': '20',
            'X-Chunk-Duration': '10',
            'X-Chunk-Frames': '300',
            'X-Chunk-Sha256': '0' * 64,
        },
    )
    assert (corrupt.status_code, corrupt.json()['error']['code']) == (400, 'transcode_chunk_corrupt')
    incomplete = await harness.finish(task, 0)
    assert incomplete.json()['error'] == {'code': 'transcode_part_incomplete', 'covered': 20.0, 'duration': 30.0}

    assert (await harness.put(task, 0, 1, 20.0, 10.0, b'second')).json() == {'stored': True}
    assert (await harness.finish(task, 0)).json() == {'finished': True}
    directory = transcode_dir(harness.work, task.id)
    assert chunk_path(directory, 0, 1).read_bytes() == b'second'
    assert not list(directory.rglob('.*.part'))
    resumed = (await harness.claim())['task']
    first_part = resumed['parts'][0]
    assert (first_part['next_chunk'], first_part['resume_at'], first_part['done']) == (2, 30.0, True)
    late = await harness.put(task, 0, 2, 30.0, 1.0)
    assert late.json()['error']['code'] == 'transcode_part_finished'

    for part, seconds in ((1, 20.0), (2, 25.0)):
        await harness.put(task, part, 0, 0.0, seconds)
        await harness.finish(task, part)
    await harness.service.tick()
    task = await harness.state(task)
    view = await harness.service.transcode_view(task)
    assert view is not None
    assert (view['parts_done'], view['encoded_seconds']) == (3, 75.0)
    # Everything is encoded, but the audio is still being copied.
    assert task.state == MergeState.TRANSCODING
    assert (await harness.claim()) == {'task': None, 'wait': 'assembling', 'task_id': task.id}

    harness.prepare(task, state='done')
    await harness.service.tick()

    task = await harness.state(task)
    assert task.state == MergeState.MERGING
    name, args = harness.kube.created[-1]
    assert name == task.job_name
    assert args[-2:] == ['--home-assemble', str(directory)]
    manifest = json.loads((directory / 'manifest.json').read_text())
    assert [[chunk['duration'] for chunk in part['chunks']] for part in manifest['parts']] == [
        [20.0, 10.0],
        [20.0],
        [25.0],
    ]
    assert audio_job_name(task.id, task.avid) in harness.kube.deleted
    # The worker has nothing more of it.
    assert (await harness.claim()) == {'task': None, 'wait': 'idle'}
    refused = await harness.put(task, 1, 1, 20.0, 1.0)
    assert refused.json()['error']['code'] == 'transcode_task_not_active'

    (harness.work / 'SQTEVR-009.mp4').write_bytes(b'x' * 300)
    status = harness.work / '.merge' / f'{task.id}.json'
    status.parent.mkdir(exist_ok=True)
    status.write_text(json.dumps({'state': 'done', 'size': 300, 'sha1': SHA1.lower()}))
    await harness.service.tick()

    assert (await harness.state(task)).state == MergeState.UPLOADING
    assert not directory.exists()
    assert await harness.transcodes.chunks(task.id) == ()


async def test_assembly_waits_for_the_merge_slot(harness: Harness) -> None:
    task = await harness.create()
    await harness.claim()
    harness.prepare(task, state='done')
    await harness.encode_all(task)
    other = await harness.create('ABP-123', tuple(f'/mnt/cd2/115/embyx/rank/ABP-123-cd{n}.mp4' for n in (1, 2)))
    await harness.repository.transition(other.id, MergeState.QUEUED, MergeState.MERGING, job_name='elsewhere')

    await harness.service.tick()

    task = await harness.state(task)
    assert (task.state, task.notice) == (MergeState.TRANSCODING, service_module.WAITING_FOR_SLOT)


async def test_the_worker_can_be_paused_and_is_told_so(harness: Harness) -> None:
    task = await harness.create()
    await harness.claim()

    await harness.service.set_paused(task.id, paused=True)

    response = await harness.client.post('/api/transcode/heartbeat', json={'task_id': task.id, 'state': 'paused'})
    assert response.json() == {'active': True, 'paused': True}
    await harness.service.set_paused(task.id, paused=False)
    response = await harness.client.post('/api/transcode/heartbeat', json={'task_id': task.id})
    assert response.json() == {'active': True, 'paused': False}


async def test_moving_a_transcode_to_the_cluster_cpu_drops_the_workers_chunks(harness: Harness) -> None:
    task = await harness.create()
    await harness.claim()
    harness.prepare(task)
    await harness.put(task, 0, 0, 0.0, 30.0)

    moved = await harness.service.use_cpu(task.id)

    assert (moved.state, moved.transcoder) == (MergeState.QUEUED, 'cpu')
    assert not transcode_dir(harness.work, task.id).exists()
    assert await harness.transcodes.chunks(task.id) == ()
    heartbeat = await harness.client.post('/api/transcode/heartbeat', json={'task_id': task.id})
    assert heartbeat.json() == {'active': False, 'paused': False}
    await harness.service.tick()
    task = await harness.state(task)
    assert task.state == MergeState.MERGING
    assert '--home-assemble' not in harness.kube.created[-1][1]
    with pytest.raises(MergeActionError):
        await harness.service.use_cpu(task.id)


async def test_cancelling_a_transcode_removes_its_chunks_and_audio_job(harness: Harness) -> None:
    task = await harness.create()
    await harness.claim()
    harness.prepare(task)
    await harness.put(task, 0, 0, 0.0, 30.0)

    cancelled = await harness.service.cancel(task.id)

    assert cancelled.state == MergeState.CANCELLED
    assert audio_job_name(task.id, task.avid) in harness.kube.deleted
    assert not transcode_dir(harness.work, task.id).exists()
    assert await harness.transcodes.chunks(task.id) == ()


async def test_a_failed_audio_job_fails_the_task_and_a_retry_keeps_the_chunks(harness: Harness) -> None:
    task = await harness.create()
    await harness.claim()
    harness.prepare(task)
    await harness.put(task, 0, 0, 0.0, 30.0)
    harness.kube.jobs[audio_job_name(task.id, task.avid)]['status'] = {
        'conditions': [{'type': 'Failed', 'status': 'True'}],
    }

    await harness.service.tick()

    task = await harness.state(task)
    assert (task.state, task.failed_state) == (MergeState.FAILED, MergeState.TRANSCODING)
    assert task.error == 'the audio Job failed: ffmpeg: boom'
    assert harness.service.parked(task) is False

    retried = await harness.service.retry(task.id)

    assert retried.state == MergeState.TRANSCODING
    assert len(await harness.transcodes.chunks(task.id)) == 1
    await harness.service.tick()
    assert harness.kube.created[-1][0] == audio_job_name(task.id, task.avid)


async def test_the_worker_reads_parts_with_range_requests(harness: Harness) -> None:
    parts = (str(harness.part_file), *PARTS[1:])
    task = await harness.create(parts=parts)
    await harness.repository.transition(task.id, MergeState.QUEUED, MergeState.TRANSCODING, transcoder='home')

    response = await harness.client.get(f'/api/transcode/tasks/{task.id}/parts/0', headers={'Range': 'bytes=10-19'})

    assert response.status_code == 206
    assert response.content == harness.part_file.read_bytes()[10:20]
    missing = await harness.client.get(f'/api/transcode/tasks/{task.id}/parts/7')
    assert missing.status_code == 404


async def test_the_worker_api_needs_its_token(harness: Harness) -> None:
    response = await harness.client.post('/api/transcode/claim', json={}, headers={'Authorization': 'Bearer nope'})

    assert response.status_code == 401


async def test_the_worker_shows_as_online_after_it_reports(harness: Harness) -> None:
    assert not harness.service.presence.online()

    await harness.service.heartbeat(WorkerReport(seen_at=datetime.now(UTC), name='pc'))

    assert harness.service.presence.online()
