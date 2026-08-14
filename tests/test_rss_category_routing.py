"""One category's items, end to end: feed -> its own offline directory -> library.

The unit tests cover each stage on its own. This one walks the seam between
them, because the whole point of per-category directories is that a category's
downloads stay separated all the way into the library, and every stage has to
agree on the directory for that to hold.
"""

import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from embyx_manager.clients.clouddrive.aio import OfflineStatus
from embyx_manager.config.models import ArchiveConfig, RssCategory, RssConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.acquisitions import AcquisitionState
from embyx_manager.monitor.archive import ArchivePipeline
from embyx_manager.monitor.reconcile import ReconcileScanner
from embyx_manager.monitor.reports import RunContext
from embyx_manager.monitor.rss import RssPipeline
from embyx_manager.monitor.tracker import AcquisitionTracker, TrackerSettings
from tests.test_monitor_rss import HASH_A, HASH_B, FakeLedger, make_item
from tests.test_monitor_tracker import offline_task, write_video

# The deployment this mirrors: a shared inbox with the rank category nested in it.
INBOX = '/115/embyx_in'
RANK_DIR = f'{INBOX}/rank'
MAGNET_ACTOR = f'magnet:?xt=urn:btih:{HASH_A}'
MAGNET_RANK = f'magnet:?xt=urn:btih:{HASH_B}'
MAGNETS = {'ABC-123': MAGNET_ACTOR, 'DEF-456': MAGNET_RANK}


def make_ctx() -> RunContext:
    return RunContext(logger=logging.getLogger('test-routing'))


class RecordingCloud:
    """Offline tasks appear under whichever directory they were submitted to."""

    def __init__(self) -> None:
        self.tasks_by_dir: dict[str, list[dict]] = {}
        self.submitted: list[tuple[str, str]] = []
        self.refreshed: list[str] = []

    async def add_offline_files(self, urls: list[str], dst_dir: str) -> SimpleNamespace:
        for url in urls:
            self.submitted.append((url, dst_dir))
        return SimpleNamespace(success=True)

    async def list_offline_files(self, path: str) -> tuple[dict, ...]:
        return tuple(self.tasks_by_dir.get(path, ()))

    async def list_directory(self, path: str) -> tuple[()]:
        self.refreshed.append(path)
        return ()


async def test_a_category_stays_in_its_own_directory_from_feed_to_library(tmp_path: Path) -> None:
    mount = tmp_path / 'mount'
    (mount / 'embyx_in' / 'rank').mkdir(parents=True)
    archive_config = ArchiveConfig(
        enabled=True,
        src_dir=str(mount),
        dst_dir=str(tmp_path / 'library'),
        # The rank route nests inside the inbox route, mirroring the directories.
        mapping={'embyx_in': 'embyx', 'embyx_in/rank': 'embyx/rank'},
    )
    ledger = FakeLedger()
    cloud = RecordingCloud()

    rss = RssPipeline(
        config=RssConfig(
            enabled=True,
            categories=(
                RssCategory(label='Actor', task_dir_path=INBOX),
                RssCategory(label='Rank', task_dir_path=RANK_DIR),
            ),
        ),
        avid_parser=AvidParser(),
        freshrss=SimpleNamespace(
            get_items=AsyncMock(
                side_effect=lambda label: {
                    'Actor': [make_item('item-1', 'ABC-123 release')],
                    # A javlibrary title: the id runs straight into the title.
                    'Rank': [make_item('item-2', 'DEF-456中出し')],
                }[label],
            ),
            read_items=AsyncMock(),
        ),
        cloud=cloud,
        sukebei=SimpleNamespace(get_magnet=AsyncMock(side_effect=MAGNETS.get)),
        javbus=SimpleNamespace(get_magnets=AsyncMock(return_value=[])),
        ledger=ledger,
    )

    await rss.run(make_ctx())

    # Each category's magnet went to its own directory.
    assert dict(cloud.submitted) == {MAGNET_ACTOR: INBOX, MAGNET_RANK: RANK_DIR}

    # CloudDrive finishes both downloads, each under the directory it was queued in.
    cloud.tasks_by_dir = {
        INBOX: [offline_task('ABC-123 release', HASH_A, OfflineStatus.FINISHED)],
        RANK_DIR: [offline_task('DEF-456 release', HASH_B, OfflineStatus.FINISHED)],
    }
    write_video(mount / 'embyx_in' / 'ABC-123 release' / 'ABC-123.mp4')
    write_video(mount / 'embyx_in' / 'rank' / 'DEF-456 release' / 'DEF-456.mp4')

    archiver = ArchivePipeline(config=archive_config, avid_parser=AvidParser())
    tracker = AcquisitionTracker(
        ledger=ledger,
        cloud=cloud,  # type: ignore[arg-type]
        archiver=archiver,
        settings=TrackerSettings.from_config(archive_config, task_dir_paths=(INBOX, RANK_DIR)),
        submit_magnet=AsyncMock(return_value=True),
    )
    await tracker.poll(make_ctx())

    assert (tmp_path / 'library' / 'embyx' / 'ABC' / 'ABC-123.mp4').exists()
    assert (tmp_path / 'library' / 'embyx' / 'rank' / 'DEF' / 'DEF-456.mp4').exists()
    assert ledger.states['ABC-123'] is AcquisitionState.ARCHIVED
    assert ledger.states['DEF-456'] is AcquisitionState.ARCHIVED
    # Each AVID carries its category's directory, so a later retry follows it.
    assert ledger.task_dirs == {'ABC-123': INBOX, 'DEF-456': RANK_DIR}


async def test_the_fallback_scan_leaves_the_nested_category_directory_intact(tmp_path: Path) -> None:
    """The inbox route must not swallow the category directory nested in it."""
    mount = tmp_path / 'mount'
    (mount / 'embyx_in' / 'rank').mkdir(parents=True)
    archive_config = ArchiveConfig(
        enabled=True,
        src_dir=str(mount),
        dst_dir=str(tmp_path / 'library'),
        mapping={'embyx_in': 'embyx', 'embyx_in/rank': 'embyx/rank'},
    )
    # A download still running under the rank directory, plus a settled one.
    write_video(mount / 'embyx_in' / 'rank' / 'DEF-456 release' / 'DEF-456.mp4')
    write_video(mount / 'embyx_in' / 'ABC-123 release' / 'ABC-123.mp4')

    ledger = FakeLedger()
    scanner = ReconcileScanner(
        ledger=ledger,
        archiver=ArchivePipeline(config=archive_config, avid_parser=AvidParser()),
        config=archive_config,
    )
    await scanner.run(make_ctx())
    await scanner.run(make_ctx())  # a folder needs two identical sightings

    assert (mount / 'embyx_in' / 'rank').is_dir()
    assert (tmp_path / 'library' / 'embyx' / 'ABC' / 'ABC-123.mp4').exists()
    assert (tmp_path / 'library' / 'embyx' / 'rank' / 'DEF' / 'DEF-456.mp4').exists()
