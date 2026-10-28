import logging
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from embyx_manager.clients.clouddrive.aio import OfflineStatus
from embyx_manager.config.models import ArchiveConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor import tracker as tracker_module
from embyx_manager.monitor.acquisitions import (
    AcquisitionState,
    AttemptState,
    MagnetCandidate,
)
from embyx_manager.monitor.archive import ArchivePipeline
from embyx_manager.monitor.reports import RunContext
from embyx_manager.monitor.tracker import AcquisitionTracker, TrackerSettings
from tests.test_monitor_rss import HASH_A, HASH_B, FakeLedger, now_stub

HASH_C = 'C' * 40

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
    def __init__(self, tasks: list[dict] | dict[str, list[dict]]) -> None:
        self.tasks_by_dir: dict[str, list[dict]] = {TASK_DIR: tasks} if isinstance(tasks, list) else tasks
        self.listed: list[str] = []
        self.refreshed: list[str] = []
        self.removed: list[tuple[str, str, bool]] = []

    async def list_offline_files(self, path: str) -> tuple[dict, ...]:
        self.listed.append(path)
        return tuple(self.tasks_by_dir.get(path, ()))

    async def list_directory(self, path: str) -> tuple[()]:
        self.refreshed.append(path)
        return ()

    async def remove_offline_files(self, info_hashes: list[str], path: str, *, delete_files: bool) -> None:
        self.removed.extend((info_hash, path, delete_files) for info_hash in info_hashes)


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
    config = ArchiveConfig(
        enabled=True,
        src_dir=str(local),
        dst_dir=str(tmp_path / 'library'),
        mapping={'downloads': 'sorted'},
        min_size_mb=min_size_mb,
        max_attempts=max_attempts,
    )
    archiver = ArchivePipeline(config=config, avid_parser=AvidParser())
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
        settings=TrackerSettings.from_config(config, task_dir_paths=(TASK_DIR,)),
        submit_magnet=submit,
    )
    return tracker, ledger, cloud, submitted


async def seed(ledger: FakeLedger, avid: str, hashes: list[str]) -> None:
    await ledger.discover(avid, source='rss:Actor', now=now_stub())
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
    # The mount is asked for the finished folder rather than slept on, and the
    # task directory goes first: a persistent cache (115) never re-reads it on
    # its own, so the new folder does not resolve until its parent is refreshed.
    assert cloud.refreshed == [TASK_DIR, f'{TASK_DIR}/ABC-123 release']
    # The task record goes, its files kept: they are already in the library, and
    # every record left behind slows CloudDrive's listing down.
    assert cloud.removed == [(HASH_A, TASK_DIR, False)]
    assert ctx.stats['archived_tasks_cleared'] == 1


async def test_finished_download_uses_expected_avid_when_file_has_single_zero_padding(tmp_path: Path) -> None:
    tracker, ledger, _, _ = build(tmp_path, [offline_task('vrkm-1846', HASH_A, OfflineStatus.FINISHED)])
    await seed(ledger, 'VRKM-1846', [HASH_A])
    folder = tmp_path / 'task' / 'downloads' / 'vrkm-1846'
    write_video(folder / 'masex.tv@vrkm01846_1_8k.mp4')
    write_video(folder / 'masex.tv@vrkm01846_2_8k.mp4')

    await tracker.poll(make_ctx())

    assert ledger.states['VRKM-1846'] is AcquisitionState.ARCHIVED
    assert ledger.attempt_states('VRKM-1846') == [AttemptState.ARCHIVED]
    assert ledger.archived_paths['VRKM-1846'] == (
        'sorted/VRKM/VRKM-1846-cd1.mp4',
        'sorted/VRKM/VRKM-1846-cd2.mp4',
    )
    assert not folder.exists()


async def test_a_finished_download_is_filed_by_the_route_that_holds_it(tmp_path: Path) -> None:
    """The task directory is one of the archive routes; here a priority one."""
    local = tmp_path / 'task'
    (local / 'vip').mkdir(parents=True)
    config = ArchiveConfig(
        enabled=True,
        src_dir=str(local),
        dst_dir=str(tmp_path / 'library'),
        mapping={'downloads': 'sorted'},
        priority_mapping={'vip': 'starred'},
    )
    ledger = FakeLedger()
    cloud = FakeCloud([offline_task('ABC-123 release', HASH_A, OfflineStatus.FINISHED)])

    async def submit(avid: str, magnet: str) -> bool:  # noqa: ARG001 - nothing should retry here
        pytest.fail('no magnet should be submitted')

    tracker = AcquisitionTracker(
        ledger=ledger,
        cloud=cloud,  # type: ignore[arg-type]
        archiver=ArchivePipeline(config=config, avid_parser=AvidParser()),
        settings=TrackerSettings.from_config(config, task_dir_paths=(TASK_DIR,)),
        submit_magnet=submit,
    )
    await seed(ledger, 'ABC-123', [HASH_A])
    write_video(local / 'vip' / 'ABC-123 release' / 'ABC-123.mp4')

    await tracker.poll(make_ctx())

    assert (tmp_path / 'library' / 'starred' / 'ABC' / 'ABC-123.mp4').exists()
    assert ledger.states['ABC-123'] is AcquisitionState.ARCHIVED


async def test_a_finished_task_not_yet_on_the_mount_is_left_for_the_next_poll(tmp_path: Path) -> None:
    tracker, ledger, _, submitted = build(tmp_path, [offline_task('ABC-123 release', HASH_A, OfflineStatus.FINISHED)])
    await seed(ledger, 'ABC-123', [HASH_A])
    # No folder written: the mount has not caught up with CloudDrive yet.

    await tracker.poll(make_ctx())

    assert ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING
    assert ledger.attempt_states('ABC-123') == [AttemptState.FINISHED]
    assert submitted == []


async def test_a_download_of_nothing_but_ads_moves_to_the_next_magnet(tmp_path: Path) -> None:
    tracker, ledger, cloud, submitted = build(
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
    # The junk folder goes with the task: nothing will come back for it.
    assert cloud.removed == [(HASH_A, TASK_DIR, True)]


async def test_errored_task_moves_to_the_next_magnet(tmp_path: Path) -> None:
    tracker, ledger, cloud, submitted = build(tmp_path, [offline_task('x', HASH_A, OfflineStatus.ERROR)])
    await seed(ledger, 'ABC-123', [HASH_A, HASH_B])

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert ledger.attempt_states('ABC-123') == [AttemptState.ERROR, AttemptState.SUBMITTED]
    assert len(submitted) == 1
    assert ctx.stats['retried'] == 1
    # The failed task is dropped at CloudDrive along with whatever it downloaded.
    assert cloud.removed == [(HASH_A, TASK_DIR, True)]


async def test_progress_is_recorded_while_the_download_moves(tmp_path: Path) -> None:
    tracker, ledger, _, submitted = build(tmp_path, [offline_task('x', HASH_A, progress=42.5)])
    await seed(ledger, 'ABC-123', [HASH_A, HASH_B])

    await tracker.poll(make_ctx())

    assert ledger.attempts['ABC-123'][0].progress == pytest.approx(42.5)
    assert ledger.attempts['ABC-123'][0].state is AttemptState.DOWNLOADING
    assert submitted == []


async def test_a_download_stuck_past_the_timeout_moves_on(tmp_path: Path) -> None:
    tracker, ledger, cloud, submitted = build(tmp_path, [offline_task('x', HASH_A, progress=10.0)])
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
    # A stalled task left at CloudDrive could finish later and drop a duplicate
    # folder next to whatever the retry archives; it goes, files included.
    assert cloud.removed == [(HASH_A, TASK_DIR, True)]


async def test_running_out_of_magnets_marks_the_avid_exhausted(tmp_path: Path) -> None:
    tracker, ledger, _, submitted = build(tmp_path, [offline_task('x', HASH_A, OfflineStatus.ERROR)])
    await seed(ledger, 'ABC-123', [HASH_A])

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert ledger.states['ABC-123'] is AcquisitionState.EXHAUSTED
    assert ledger.next_action_at['ABC-123'] is not None
    assert submitted == []
    assert ctx.stats['exhausted'] == 1


async def test_exhausting_a_row_without_a_release_date_looks_it_up_first(tmp_path: Path) -> None:
    tracker, ledger, _, _ = build(tmp_path, [offline_task('x', HASH_A, OfflineStatus.ERROR)])
    await seed(ledger, 'ABC-123', [HASH_A])
    asked: list[str] = []

    async def lookup(avid: str) -> date | None:
        asked.append(avid)
        return datetime.now(UTC).date() + timedelta(days=60)

    tracker._release_date_lookup = lookup  # noqa: SLF001 - wiring the optional hook after build()

    await tracker.poll(make_ctx())

    assert asked == ['ABC-123']
    assert ledger.release_dates['ABC-123'] == datetime.now(UTC).date() + timedelta(days=60)
    # Two months out: the weekly preheat rather than the stall timeout.
    wait = ledger.next_action_at['ABC-123'] - datetime.now(UTC)
    assert timedelta(days=6, hours=23) < wait <= timedelta(days=7)


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
    tracker, ledger, cloud, submitted = build(tmp_path, [])
    await seed(ledger, 'ABC-123', [HASH_A, HASH_B])

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert ledger.attempt_states('ABC-123') == [AttemptState.LOST, AttemptState.SUBMITTED]
    assert len(submitted) == 1
    # CloudDrive no longer lists the task, so there is nothing to remove.
    assert cloud.removed == []


async def test_a_cancelled_task_of_an_ignored_avid_is_not_resubmitted(tmp_path: Path) -> None:
    # An operator ignored the duplicate and cancelled its offline task. The
    # sweep still books the attempt as lost, but must not resurrect the AVID
    # with its next candidate magnet.
    tracker, ledger, _, submitted = build(tmp_path, [])
    await seed(ledger, 'ABC-123', [HASH_A, HASH_B])
    ledger.states['ABC-123'] = AcquisitionState.IGNORED

    await tracker.poll(make_ctx())

    assert ledger.attempt_states('ABC-123') == [AttemptState.LOST, AttemptState.PENDING]
    assert submitted == []


async def test_an_avid_an_operator_parked_is_not_touched(tmp_path: Path) -> None:
    tracker, ledger, _, _ = build(tmp_path, [offline_task('ABC-123 release', HASH_A, OfflineStatus.FINISHED)])
    await seed(ledger, 'ABC-123', [HASH_A])
    ledger.states['ABC-123'] = AcquisitionState.NEEDS_ATTENTION
    write_video(tmp_path / 'task' / 'downloads' / 'ABC-123 release' / 'ABC-123.mp4')

    await tracker.poll(make_ctx())

    assert not (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    assert ledger.states['ABC-123'] is AcquisitionState.NEEDS_ATTENTION


RANK_DIR = f'{TASK_DIR}/rank'


def build_nested(tmp_path: Path, tasks: dict[str, list[dict]]) -> tuple[AcquisitionTracker, FakeLedger, FakeCloud]:
    """A category directory nested inside the shared inbox, each its own route."""
    local = tmp_path / 'task'
    (local / 'downloads' / 'rank').mkdir(parents=True)
    config = ArchiveConfig(
        enabled=True,
        src_dir=str(local),
        dst_dir=str(tmp_path / 'library'),
        mapping={'downloads': 'sorted', 'downloads/rank': 'sorted/rank'},
    )
    ledger = FakeLedger()
    cloud = FakeCloud(tasks)

    async def submit(avid: str, magnet: str) -> bool:  # noqa: ARG001 - nothing should retry here
        pytest.fail('no magnet should be submitted')

    tracker = AcquisitionTracker(
        ledger=ledger,
        cloud=cloud,  # type: ignore[arg-type]
        archiver=ArchivePipeline(config=config, avid_parser=AvidParser()),
        settings=TrackerSettings.from_config(config, task_dir_paths=(TASK_DIR, RANK_DIR)),
        submit_magnet=submit,
    )
    return tracker, ledger, cloud


async def test_every_configured_directory_is_polled(tmp_path: Path) -> None:
    tracker, ledger, cloud = build_nested(
        tmp_path,
        {
            TASK_DIR: [offline_task('ABC-123 release', HASH_A, OfflineStatus.FINISHED)],
            RANK_DIR: [offline_task('DEF-456 release', HASH_B, OfflineStatus.FINISHED)],
        },
    )
    await seed(ledger, 'ABC-123', [HASH_A])
    await seed(ledger, 'DEF-456', [HASH_B])
    write_video(tmp_path / 'task' / 'downloads' / 'ABC-123 release' / 'ABC-123.mp4')
    write_video(tmp_path / 'task' / 'downloads' / 'rank' / 'DEF-456 release' / 'DEF-456.mp4')

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert cloud.listed == [TASK_DIR, RANK_DIR]
    assert ctx.stats['offline_tasks'] == 2
    # Each download is filed by the route that holds it, which is what puts the
    # rank category in a library subdirectory of its own.
    assert (tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4').exists()
    assert (tmp_path / 'library' / 'sorted' / 'rank' / 'DEF' / 'DEF-456.mp4').exists()


async def test_the_mount_refresh_uses_the_directory_the_task_was_listed_under(tmp_path: Path) -> None:
    tracker, ledger, cloud = build_nested(
        tmp_path,
        {RANK_DIR: [offline_task('DEF-456 release', HASH_B, OfflineStatus.FINISHED)]},
    )
    await seed(ledger, 'DEF-456', [HASH_B])
    write_video(tmp_path / 'task' / 'downloads' / 'rank' / 'DEF-456 release' / 'DEF-456.mp4')

    await tracker.poll(make_ctx())

    assert cloud.refreshed == [RANK_DIR, f'{RANK_DIR}/DEF-456 release']


async def test_a_hash_listed_in_two_directories_is_advanced_once(tmp_path: Path) -> None:
    task = offline_task('ABC-123 release', HASH_A, OfflineStatus.FINISHED)
    tracker, ledger, cloud = build_nested(tmp_path, {TASK_DIR: [task], RANK_DIR: [dict(task)]})
    await seed(ledger, 'ABC-123', [HASH_A])
    write_video(tmp_path / 'task' / 'downloads' / 'ABC-123 release' / 'ABC-123.mp4')

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert ctx.stats['duplicate_offline_tasks'] == 1
    assert ledger.states['ABC-123'] is AcquisitionState.ARCHIVED
    assert cloud.refreshed == [TASK_DIR, f'{TASK_DIR}/ABC-123 release']


async def test_no_configured_directory_does_not_conclude_the_downloads_in_flight(tmp_path: Path) -> None:
    """An empty directory list is no evidence, not evidence of everything vanishing."""
    config = ArchiveConfig(
        enabled=True,
        src_dir=str(tmp_path / 'task'),
        dst_dir=str(tmp_path / 'library'),
        mapping={'downloads': 'sorted'},
    )
    ledger = FakeLedger()
    cloud = FakeCloud([])
    tracker = AcquisitionTracker(
        ledger=ledger,
        cloud=cloud,  # type: ignore[arg-type]
        archiver=ArchivePipeline(config=config, avid_parser=AvidParser()),
        settings=TrackerSettings.from_config(config, task_dir_paths=()),
        submit_magnet=AsyncMock(return_value=True),
    )
    await seed(ledger, 'ABC-123', [HASH_A])

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert cloud.listed == []
    assert ledger.attempt_states('ABC-123') == [AttemptState.SUBMITTED]
    assert any('no offline directory' in line for line in ctx.log_tail)


async def test_only_finished_tasks_the_ledger_archived_are_cleared(tmp_path: Path) -> None:
    """A task added by hand, or one not archived yet, stays listed."""
    tracker, ledger, cloud, _ = build(
        tmp_path,
        [
            offline_task('archived earlier', HASH_A, OfflineStatus.FINISHED),
            offline_task('someone elses', HASH_B, OfflineStatus.FINISHED),
            offline_task('not on the mount yet', HASH_C, OfflineStatus.FINISHED),
        ],
    )
    await seed(ledger, 'ABC-123', [HASH_A])
    ledger.attempts['ABC-123'][0] = replace(ledger.attempts['ABC-123'][0], state=AttemptState.ARCHIVED)
    ledger.states['ABC-123'] = AcquisitionState.ARCHIVED
    await seed(ledger, 'DEF-456', [HASH_C])

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert cloud.removed == [(HASH_A, TASK_DIR, False)]
    assert ledger.attempt_states('DEF-456') == [AttemptState.FINISHED]  # waits for the mount


async def test_archived_tasks_are_cleared_in_batches_and_a_failure_is_shrugged_off(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tracker_module, 'CLEAR_BATCH', 2)
    hashes = [f'{index:040X}' for index in range(5)]
    tracker, ledger, cloud, _ = build(
        tmp_path,
        [offline_task(f'done {index}', info_hash, OfflineStatus.FINISHED) for index, info_hash in enumerate(hashes)],
    )
    for index, info_hash in enumerate(hashes):
        avid = f'ABC-{index:03d}'
        await seed(ledger, avid, [info_hash])
        ledger.attempts[avid][0] = replace(ledger.attempts[avid][0], state=AttemptState.ARCHIVED)
    calls: list[list[str]] = []

    async def remove(info_hashes: list[str], path: str, *, delete_files: bool) -> None:
        del path, delete_files
        calls.append(info_hashes)
        if len(calls) == 2:
            msg = 'CloudDrive is busy'
            raise RuntimeError(msg)

    cloud.remove_offline_files = remove  # type: ignore[method-assign]
    ctx = make_ctx()
    await tracker.poll(ctx)

    assert calls == [hashes[0:2], hashes[2:4], hashes[4:5]]
    assert ctx.stats['archived_tasks_cleared'] == 3


# -- replacements (换版) --------------------------------------------------------


class MountedCloud(FakeCloud):
    """A CloudDrive whose API tree is the test's tmp_path, as the mount shows it."""

    def __init__(self, tasks: list[dict], root: Path, *, deletes: bool = True) -> None:
        super().__init__(tasks)
        self.root = root
        self.deletes = deletes
        self.deleted: list[str] = []

    async def list_directory(self, path: str) -> tuple[dict, ...]:  # type: ignore[override]
        self.refreshed.append(path)
        local = self.root / path.lstrip('/')
        if not local.is_dir():
            return ()
        return tuple(
            {'name': child.name, 'full_path': f'{path.rstrip("/")}/{child.name}', 'is_directory': child.is_dir()}
            for child in sorted(local.iterdir())
        )

    async def delete_files(self, api_paths: list[str]) -> dict[str, object]:
        self.deleted.extend(api_paths)
        if self.deletes:
            for path in api_paths:
                (self.root / path.lstrip('/')).unlink()
        return {'success': True, 'error_message': ''}


def build_replacing(
    tmp_path: Path,
    tasks: list[dict],
    *,
    deletes: bool = True,
    mount_prefix: str | None = None,
) -> tuple[AcquisitionTracker, FakeLedger, MountedCloud, list[tuple[str, str]]]:
    tracker, ledger, _, submitted = build(tmp_path, tasks)
    cloud = MountedCloud(tasks, tmp_path, deletes=deletes)
    tracker._cloud = cloud  # type: ignore[assignment]  # noqa: SLF001 - swap in the mounted fake
    tracker._settings = replace(  # noqa: SLF001
        tracker._settings,  # noqa: SLF001
        library_mount_prefix=str(tmp_path) if mount_prefix is None else mount_prefix,
    )
    return tracker, ledger, cloud, submitted


async def seed_replacement(ledger: FakeLedger, avid: str, old_paths: tuple[str, ...], hashes: list[str]) -> None:
    """An archived title whose first download (HASH_C) produced the copy being replaced."""
    await seed(ledger, avid, [HASH_C])
    ledger.attempts[avid][0] = replace(ledger.attempts[avid][0], state=AttemptState.ARCHIVED)
    ledger.states[avid] = AcquisitionState.ARCHIVED
    await ledger.begin_replacement(
        avid, source='manual', task_dir_path=TASK_DIR, replace_paths=old_paths, now=now_stub()
    )
    await ledger.add_attempts(
        avid,
        [MagnetCandidate(magnet=f'magnet:?xt=urn:btih:{h}', info_hash=h, source='sukebei') for h in hashes],
        now=now_stub(),
    )
    await ledger.claim_next_pending(avid, now=now_stub())
    ledger.states[avid] = AcquisitionState.DOWNLOADING


async def test_a_replacement_deletes_the_old_copy_before_filing_the_new_one(tmp_path: Path) -> None:
    tracker, ledger, cloud, _ = build_replacing(
        tmp_path, [offline_task('ABC-123 proper', HASH_A, OfflineStatus.FINISHED)]
    )
    old = tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4'
    write_video(old, size=3)
    await seed_replacement(ledger, 'ABC-123', ('sorted/ABC/ABC-123.mp4',), [HASH_A])
    write_video(tmp_path / 'task' / 'downloads' / 'ABC-123 proper' / 'ABC-123.mp4', size=20)

    ctx = make_ctx()
    await tracker.poll(ctx)

    assert cloud.deleted == ['/library/sorted/ABC/ABC-123.mp4']
    assert old.read_bytes() == b'x' * 20
    assert ledger.states['ABC-123'] is AcquisitionState.ARCHIVED
    assert ledger.attempt_states('ABC-123') == [AttemptState.ARCHIVED, AttemptState.ARCHIVED]
    # The replacement is over once the new copy is in.
    assert ledger.replace_paths.get('ABC-123') is None
    assert ctx.stats['replaced_copies_deleted'] == 1


async def test_a_replacement_into_parts_drops_the_single_old_file(tmp_path: Path) -> None:
    tracker, ledger, _, _ = build_replacing(tmp_path, [offline_task('ABC-123 parts', HASH_A, OfflineStatus.FINISHED)])
    old = tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4'
    write_video(old)
    await seed_replacement(ledger, 'ABC-123', ('sorted/ABC/ABC-123.mp4',), [HASH_A])
    folder = tmp_path / 'task' / 'downloads' / 'ABC-123 parts'
    write_video(folder / 'ABC-123-1.mp4')
    write_video(folder / 'ABC-123-2.mp4')

    await tracker.poll(make_ctx())

    assert not old.exists()
    assert sorted(path.name for path in old.parent.iterdir()) == ['ABC-123-cd1.mp4', 'ABC-123-cd2.mp4']
    assert ledger.states['ABC-123'] is AcquisitionState.ARCHIVED


async def test_a_replacement_whose_download_is_the_wrong_title_keeps_the_old_copy(tmp_path: Path) -> None:
    tracker, ledger, cloud, _ = build_replacing(
        tmp_path, [offline_task('ABC-123 again', HASH_A, OfflineStatus.FINISHED)]
    )
    old = tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4'
    write_video(old)
    await seed_replacement(ledger, 'ABC-123', ('sorted/ABC/ABC-123.mp4',), [HASH_A])
    write_video(tmp_path / 'task' / 'downloads' / 'ABC-123 again' / 'XYZ-999.mp4')

    await tracker.poll(make_ctx())

    assert cloud.deleted == []
    assert old.exists()
    assert ledger.states['ABC-123'] is AcquisitionState.NEEDS_ATTENTION
    assert ledger.replace_paths['ABC-123'] == ('sorted/ABC/ABC-123.mp4',)


async def test_a_replacement_waits_while_the_mount_still_shows_the_old_copy(tmp_path: Path) -> None:
    tracker, ledger, cloud, _ = build_replacing(
        tmp_path, [offline_task('ABC-123 proper', HASH_A, OfflineStatus.FINISHED)], deletes=False
    )
    old = tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4'
    write_video(old, size=3)
    await seed_replacement(ledger, 'ABC-123', ('sorted/ABC/ABC-123.mp4',), [HASH_A])
    folder = tmp_path / 'task' / 'downloads' / 'ABC-123 proper'
    write_video(folder / 'ABC-123.mp4', size=20)

    await tracker.poll(make_ctx())

    # Released for the next poll, nothing filed over the old copy.
    assert cloud.deleted == ['/library/sorted/ABC/ABC-123.mp4']
    assert old.read_bytes() == b'xxx'
    assert (folder / 'ABC-123.mp4').exists()
    assert ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING
    assert ledger.attempt_states('ABC-123')[-1] is AttemptState.FINISHED

    # The mount catches up; the next pass files it.
    old.unlink()
    await tracker.poll(make_ctx())
    assert old.read_bytes() == b'x' * 20
    assert ledger.states['ABC-123'] is AcquisitionState.ARCHIVED


async def test_a_replacement_never_deletes_a_recorded_path_that_is_not_the_title(tmp_path: Path) -> None:
    tracker, ledger, cloud, _ = build_replacing(
        tmp_path, [offline_task('ABC-123 proper', HASH_A, OfflineStatus.FINISHED)]
    )
    other = tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-1234.mp4'
    write_video(other)
    await seed_replacement(ledger, 'ABC-123', ('sorted/ABC/ABC-1234.mp4', '../task/x.mp4'), [HASH_A])
    write_video(tmp_path / 'task' / 'downloads' / 'ABC-123 proper' / 'ABC-123.mp4')

    await tracker.poll(make_ctx())

    assert cloud.deleted == []
    assert other.exists()
    assert ledger.states['ABC-123'] is AcquisitionState.ARCHIVED


async def test_a_replacement_without_a_mount_prefix_is_parked(tmp_path: Path) -> None:
    tracker, ledger, cloud, _ = build_replacing(
        tmp_path, [offline_task('ABC-123 proper', HASH_A, OfflineStatus.FINISHED)], mount_prefix=''
    )
    old = tmp_path / 'library' / 'sorted' / 'ABC' / 'ABC-123.mp4'
    write_video(old)
    await seed_replacement(ledger, 'ABC-123', ('sorted/ABC/ABC-123.mp4',), [HASH_A])
    write_video(tmp_path / 'task' / 'downloads' / 'ABC-123 proper' / 'ABC-123.mp4')

    await tracker.poll(make_ctx())

    assert cloud.deleted == []
    assert old.exists()
    assert ledger.states['ABC-123'] is AcquisitionState.NEEDS_ATTENTION


async def test_a_replacement_counts_only_its_own_attempts_against_the_cap(tmp_path: Path) -> None:
    tracker, ledger, _, submitted = build_replacing(
        tmp_path, [offline_task('ABC-123 bad', HASH_A, OfflineStatus.ERROR)]
    )
    tracker._settings = replace(tracker._settings, max_attempts=2)  # noqa: SLF001
    await seed_replacement(ledger, 'ABC-123', ('sorted/ABC/ABC-123.mp4',), [HASH_A, HASH_B])

    await tracker.poll(make_ctx())

    # The archived download before the replacement is the old copy's history:
    # with it counted, this first failure would already exhaust a cap of two.
    assert ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING
    assert submitted == [('ABC-123', f'magnet:?xt=urn:btih:{HASH_B}')]
