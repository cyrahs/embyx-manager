import logging
from datetime import timedelta
from pathlib import Path

import pytest

from embyx_manager.clients.clouddrive.aio import OfflineStatus
from embyx_manager.config.models import ArchiveConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.acquisitions import (
    AcquisitionSource,
    AcquisitionState,
    AttemptState,
    MagnetCandidate,
)
from embyx_manager.monitor.archive import ArchivePipeline
from embyx_manager.monitor.reports import RunContext
from embyx_manager.monitor.tracker import AcquisitionTracker, TrackerSettings
from tests.test_monitor_rss import HASH_A, HASH_B, FakeLedger, now_stub

TASK_DIR = '/115/task'


def make_ctx() -> RunContext:
    return RunContext(logger=logging.getLogger('test-tracker'))


def offline_task(
    name: str,
    info_hash: str,
    status: OfflineStatus = OfflineStatus.DOWNLOADING,
    progress: float = 10.0,
) -> dict:
    return {
        'name': name,
        'size': 1024,
        'url': f'magnet:?xt=urn:btih:{info_hash}',
        'status': status,
        'info_hash': info_hash,
        'file_id': '1',
        'add_time': 0,
        'progress': progress,
        'peers': 3,
    }


class FakeCloud:
    def __init__(self, tasks: list[dict]) -> None:
        self.tasks = tasks
        self.refreshed: list[str] = []

    async def list_offline_files(self, path: str) -> tuple[dict, ...]:
        assert path == TASK_DIR
        return tuple(self.tasks)

    async def list_directory(self, path: str) -> tuple[()]:
        self.refreshed.append(path)
        return ()


def write_video(path: Path, size: int = 10) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'x' * size)


def build(
    tmp_path: Path,
    tasks: list[dict],
    *,
    submit_ok: bool = True,
    max_attempts: int = 5,
    min_size_mb: int = 0,
) -> tuple[AcquisitionTracker, FakeLedger, FakeCloud, list[tuple[str, str]]]:
    local = tmp_path / 'task'
    (local / 'downloads').mkdir(parents=True)
    (tmp_path / 'library' / 'sorted').mkdir(parents=True)
    archiver = ArchivePipeline(
        config=ArchiveConfig(
            enabled=True,
            src_dir=str(local),
            dst_dir=str(tmp_path / 'library'),
            mapping={'downloads': 'sorted'},
            min_size_mb=min_size_mb,
        ),
        avid_parser=AvidParser(),
    )
    ledger = FakeLedger()
    cloud = FakeCloud(tasks)
    submitted: list[tuple[str, str]] = []

    async def submit(avid: str, magnet: str) -> bool:
        submitted.append((avid, magnet))
        return submit_ok

    tracker = AcquisitionTracker(
        ledger=ledger,
        cloud=cloud,  # type: ignore[arg-type]
        archiver=archiver,
        settings=TrackerSettings(
            task_dir_path=TASK_DIR,
            task_dir_local=local / 'downloads',
            task_dst='sorted',
            max_attempts=max_attempts,
        ),
        submit_magnet=submit,
    )
    return tracker, ledger, cloud, submitted


async def seed(ledger: FakeLedger, avid: str, hashes: list[str]) -> None:
    await ledger.discover(avid, source=AcquisitionSource.RSS_ACTOR, now=now_stub())
    await ledger.add_attempts(
        avid,
        [MagnetCandidate(magnet=f'magnet:?xt=urn:btih:{h}', info_hash=h, source='sukebei') for h in hashes],
        now=now_stub(),
    )
    await ledger.claim_next_pending(avid, now=now_stub())
    ledger.states[avid] = AcquisitionState.DOWNLOADING


async def test_finished_download_is_archived_and_the_avid_closed(tmp_path: Path) -> None:
    tracker, ledger, cloud, _ = build(tmp_path, [offline_task('ABC-123 release', HASH_A, OfflineStatus.FINISHED)])
    await seed(ledger, 'ABC-123', [HASH_A])
    write_video(tmp_path / 'task' / 'downloads' / 'ABC-123 release' / 'ABC-123.mp4')

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    assert ledger.states['ABC-123'] is AcquisitionState.ARCHIVED
    assert ledger.attempt_states('ABC-123') == [AttemptState.ARCHIVED]
    assert ctx.stats['archived'] == 1
    # The mount is asked for the finished folder rather than slept on.
    assert cloud.refreshed == [f'{TASK_DIR}/ABC-123 release']


async def test_a_download_of_nothing_but_ads_moves_to_the_next_magnet(tmp_path: Path) -> None:
    tracker, ledger, _, submitted = build(
        tmp_path,
        [offline_task('junk release', HASH_A, OfflineStatus.FINISHED)],
        min_size_mb=1,
    )
    await seed(ledger, 'ABC-123', [HASH_A, HASH_B])
    write_video(tmp_path / 'task' / 'downloads' / 'junk release' / 'advert.mp4', size=10)

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert ledger.attempt_states('ABC-123') == [AttemptState.JUNK, AttemptState.SUBMITTED]
    assert [avid for avid, _ in submitted] == ['ABC-123']
    assert ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING
    assert ctx.stats['junk'] == 1


async def test_errored_task_moves_to_the_next_magnet(tmp_path: Path) -> None:
    tracker, ledger, _, submitted = build(tmp_path, [offline_task('x', HASH_A, OfflineStatus.ERROR)])
    await seed(ledger, 'ABC-123', [HASH_A, HASH_B])

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert ledger.attempt_states('ABC-123') == [AttemptState.ERROR, AttemptState.SUBMITTED]
    assert len(submitted) == 1
    assert ctx.stats['retried'] == 1


async def test_progress_is_recorded_while_the_download_moves(tmp_path: Path) -> None:
    tracker, ledger, _, submitted = build(tmp_path, [offline_task('x', HASH_A, progress=42.5)])
    await seed(ledger, 'ABC-123', [HASH_A, HASH_B])

    await tracker.poll(make_ctx())

    assert ledger.attempts['ABC-123'][0].progress == pytest.approx(42.5)
    assert ledger.attempts['ABC-123'][0].state is AttemptState.DOWNLOADING
    assert submitted == []


async def test_a_download_stuck_past_the_timeout_moves_on(tmp_path: Path) -> None:
    tracker, ledger, _, submitted = build(tmp_path, [offline_task('x', HASH_A, progress=10.0)])
    await seed(ledger, 'ABC-123', [HASH_A, HASH_B])
    # Already downloading, stuck at the same percentage since well before the timeout.
    stale = ledger.attempts['ABC-123'][0]
    ledger.attempts['ABC-123'][0] = stale.__class__(
        **{
            **stale.__dict__,
            'state': AttemptState.DOWNLOADING,
            'progress': 10.0,
            'updated_at': now_stub() - timedelta(days=3),
        },
    )

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert ledger.attempt_states('ABC-123') == [AttemptState.STALLED, AttemptState.SUBMITTED]
    assert len(submitted) == 1


async def test_running_out_of_magnets_marks_the_avid_exhausted(tmp_path: Path) -> None:
    tracker, ledger, _, submitted = build(tmp_path, [offline_task('x', HASH_A, OfflineStatus.ERROR)])
    await seed(ledger, 'ABC-123', [HASH_A])

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert ledger.states['ABC-123'] is AcquisitionState.EXHAUSTED
    assert ledger.next_action_at['ABC-123'] is not None
    assert submitted == []
    assert ctx.stats['exhausted'] == 1


async def test_the_attempt_cap_stops_the_retry_chain(tmp_path: Path) -> None:
    tracker, ledger, _, submitted = build(
        tmp_path,
        [offline_task('x', HASH_A, OfflineStatus.ERROR)],
        max_attempts=1,
    )
    await seed(ledger, 'ABC-123', [HASH_A, HASH_B])

    await tracker.poll(make_ctx())

    assert submitted == []
    assert ledger.states['ABC-123'] is AcquisitionState.EXHAUSTED


async def test_a_folder_that_is_not_what_we_ordered_is_parked(tmp_path: Path) -> None:
    tracker, ledger, _, submitted = build(tmp_path, [offline_task('surprise', HASH_A, OfflineStatus.FINISHED)])
    await seed(ledger, 'ABC-123', [HASH_A, HASH_B])
    write_video(tmp_path / 'task' / 'downloads' / 'surprise' / 'DEF-456.mp4')

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert ledger.states['ABC-123'] is AcquisitionState.NEEDS_ATTENTION
    assert ledger.notes['ABC-123'] == 'expected ABC-123 but found DEF-456'
    # The claim is released rather than consumed, so it can be retried once fixed.
    assert ledger.attempt_states('ABC-123') == [AttemptState.FINISHED, AttemptState.PENDING]
    assert submitted == []
    assert (tmp_path / 'task' / 'downloads' / 'surprise' / 'DEF-456.mp4').exists()


async def test_tasks_outside_the_ledger_are_left_alone(tmp_path: Path) -> None:
    tracker, ledger, _, submitted = build(
        tmp_path,
        [
            offline_task('ours', HASH_A),
            offline_task('someone elses', HASH_B, OfflineStatus.FINISHED),
        ],
    )
    await seed(ledger, 'ABC-123', [HASH_A])
    write_video(tmp_path / 'task' / 'downloads' / 'someone elses' / 'ZZZ-999.mp4')

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert (tmp_path / 'task' / 'downloads' / 'someone elses' / 'ZZZ-999.mp4').exists()
    assert ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING
    assert submitted == []


async def test_a_task_that_vanished_but_landed_is_still_archived(tmp_path: Path) -> None:
    # An operator cleared the offline record; the download itself is on the mount.
    tracker, ledger, _, _ = build(tmp_path, [])
    await seed(ledger, 'ABC-123', [HASH_A])
    write_video(tmp_path / 'task' / 'downloads' / 'ABC-123 release' / 'ABC-123.mp4')

    await tracker.poll(make_ctx())

    assert (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    assert ledger.states['ABC-123'] is AcquisitionState.ARCHIVED


async def test_a_task_that_vanished_without_landing_moves_on(tmp_path: Path) -> None:
    tracker, ledger, _, submitted = build(tmp_path, [])
    await seed(ledger, 'ABC-123', [HASH_A, HASH_B])

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert ledger.attempt_states('ABC-123') == [AttemptState.LOST, AttemptState.SUBMITTED]
    assert len(submitted) == 1


async def test_an_avid_an_operator_parked_is_not_touched(tmp_path: Path) -> None:
    tracker, ledger, _, _ = build(tmp_path, [offline_task('ABC-123 release', HASH_A, OfflineStatus.FINISHED)])
    await seed(ledger, 'ABC-123', [HASH_A])
    ledger.states['ABC-123'] = AcquisitionState.NEEDS_ATTENTION
    write_video(tmp_path / 'task' / 'downloads' / 'ABC-123 release' / 'ABC-123.mp4')

    await tracker.poll(make_ctx())

    assert not (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    assert ledger.states['ABC-123'] is AcquisitionState.NEEDS_ATTENTION
