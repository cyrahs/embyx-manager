"""The manual input source: an operator's own list of AVIDs into the shared intake."""

import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from embyx_manager.config.models import ArchiveConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.acquisitions import AcquisitionState, AttemptState
from embyx_manager.monitor.archive import ArchivePipeline
from embyx_manager.monitor.intake import AcquisitionIntake
from embyx_manager.monitor.manual import (
    MAX_MANUAL_INPUTS,
    CloudUnavailableError,
    DirectoryNotFoundError,
    DirectoryNotRoutedError,
    ManualIntakeSource,
    ManualOutcome,
    TooManyInputsError,
)
from tests.test_monitor_intake import MAGNET_A
from tests.test_monitor_rss import FakeLedger

# The deployment these mirror: an inbox route and a rank route nested in it,
# both mounted locally under src_dir, plus a directory with no route at all.
INBOX = '/115/embyx_in'
RANK_DIR = f'{INBOX}/rank'
UNROUTED_DIR = '/115/misc'


def make_archiver(tmp_path: Path) -> ArchivePipeline:
    # The mount exposes the cloud tree unchanged, which is what lets a route root
    # be matched to a CloudDrive API path by suffix.
    config = ArchiveConfig(
        enabled=True,
        src_dir=str(tmp_path / 'mount' / '115'),
        dst_dir=str(tmp_path / 'library'),
        mapping={'embyx_in': 'embyx', 'embyx_in/rank': 'embyx/rank'},
    )
    return ArchivePipeline(config=config, avid_parser=AvidParser())


class FakeCloud:
    """CloudDrive listings for browsing, plus the offline submissions made."""

    def __init__(self, tree: dict[str, tuple[dict, ...]] | None = None) -> None:
        self.tree = tree or {}
        self.submitted: list[tuple[list[str], str]] = []

    async def list_directory(self, path: str) -> tuple[dict, ...]:
        if path not in self.tree:
            raise FileNotFoundError(path)
        return self.tree[path]

    async def add_offline_files(self, urls: list[str], dst_dir: str) -> SimpleNamespace:
        self.submitted.append((urls, dst_dir))
        return SimpleNamespace(success=True)


def entry(name: str, parent: str, *, is_directory: bool = True) -> dict:
    return {'name': name, 'full_path': f'{parent.rstrip("/")}/{name}', 'is_directory': is_directory}


def make_source(
    tmp_path: Path,
    *,
    ledger: FakeLedger | None = None,
    cloud: FakeCloud | None = None,
    magnet: str | None = MAGNET_A,
    configured_dirs: tuple[str, ...] = (INBOX,),
    with_cloud: bool = True,
) -> tuple[ManualIntakeSource, SimpleNamespace]:
    deps = SimpleNamespace()
    deps.ledger = ledger or FakeLedger()
    deps.cloud = cloud or FakeCloud({INBOX: (), RANK_DIR: (), UNROUTED_DIR: ()})
    deps.intake = AcquisitionIntake(
        ledger=deps.ledger,
        sukebei=SimpleNamespace(get_magnet=AsyncMock(return_value=magnet)),
        javbus=SimpleNamespace(get_magnets=AsyncMock(return_value=[])),
        cloud=deps.cloud,
        failed_cooldown_seconds=3600,
    )
    source = ManualIntakeSource(
        ledger=deps.ledger,
        intake_factory=lambda: deps.intake if with_cloud else None,
        cloud_factory=lambda: deps.cloud if with_cloud else None,
        archiver_factory=lambda: make_archiver(tmp_path),
        parser_factory=AvidParser,
        configured_dirs=lambda: configured_dirs,
        logger=logging.getLogger('test-manual'),
    )
    return source, deps


# -- browsing -----------------------------------------------------------------


async def test_browse_marks_which_directories_can_be_submitted_to(tmp_path: Path) -> None:
    """Routed and configured are separate facts: only the first decides submittability."""
    cloud = FakeCloud(
        {
            '/115': (
                entry('embyx_in', '/115'),
                entry('misc', '/115'),
                entry('note.txt', '/115', is_directory=False),
            ),
        },
    )
    source, _ = make_source(tmp_path, cloud=cloud)

    listing = await source.browse('/115')

    assert listing.path == '/115'
    assert listing.parent == '/'
    assert [(item.name, item.configured, item.routed) for item in listing.entries] == [
        ('embyx_in', True, True),
        ('misc', False, False),
    ]


async def test_browse_finds_a_route_for_a_directory_no_source_declares(tmp_path: Path) -> None:
    """The rank route exists in the archive tables without being an input source's own."""
    cloud = FakeCloud({INBOX: (entry('rank', INBOX),)})
    source, _ = make_source(tmp_path, cloud=cloud)

    listing = await source.browse(INBOX)

    assert [(item.name, item.configured, item.routed) for item in listing.entries] == [('rank', False, True)]


async def test_browse_reports_the_root_and_a_missing_directory(tmp_path: Path) -> None:
    source, _ = make_source(tmp_path, cloud=FakeCloud({'/': ()}))

    listing = await source.browse('/')

    assert listing.parent is None
    with pytest.raises(DirectoryNotFoundError):
        await source.browse('/nope')
    with pytest.raises(DirectoryNotFoundError):
        await source.browse('relative/path')


async def test_browse_without_clouddrive(tmp_path: Path) -> None:
    source, _ = make_source(tmp_path, with_cloud=False)

    with pytest.raises(CloudUnavailableError):
        await source.browse('/')


# -- the remembered directory -------------------------------------------------


async def test_default_directory_remembers_the_last_manual_submission(tmp_path: Path) -> None:
    source, deps = make_source(tmp_path)

    assert await source.default_directory() == INBOX  # nothing submitted yet: a configured one

    await source.submit(['ABC-123'], task_dir_path=RANK_DIR)

    assert deps.ledger.task_dirs['ABC-123'] == RANK_DIR
    assert await source.default_directory() == RANK_DIR


async def test_default_directory_skips_a_configured_directory_without_a_route(tmp_path: Path) -> None:
    """Offering it would only produce a refusal when the operator submits."""
    source, _ = make_source(tmp_path, configured_dirs=(UNROUTED_DIR,))

    assert await source.default_directory() is None


# -- submitting ---------------------------------------------------------------


async def test_submit_reads_dirty_names_and_pins_the_chosen_directory(tmp_path: Path) -> None:
    source, deps = make_source(tmp_path)

    submission = await source.submit(
        ['[JAV] ABC-123 1080p.mp4', '  def-456  ', ''],
        task_dir_path=RANK_DIR,
    )

    assert [(item.avid, item.outcome) for item in submission.entries] == [
        ('ABC-123', ManualOutcome.SUBMITTED),
        ('DEF-456', ManualOutcome.SUBMITTED),
    ]
    assert submission.task_dir_path == RANK_DIR
    assert deps.ledger.sources == {'ABC-123': 'manual', 'DEF-456': 'manual'}
    assert deps.ledger.task_dirs == {'ABC-123': RANK_DIR, 'DEF-456': RANK_DIR}
    assert deps.cloud.submitted == [([MAGNET_A], RANK_DIR), ([MAGNET_A], RANK_DIR)]


async def test_submit_refuses_a_directory_without_an_archive_route(tmp_path: Path) -> None:
    """A download there could never be located, so no ledger row is written for it."""
    source, deps = make_source(tmp_path)

    with pytest.raises(DirectoryNotRoutedError):
        await source.submit(['ABC-123'], task_dir_path=UNROUTED_DIR)

    assert deps.ledger.states == {}
    assert deps.cloud.submitted == []


async def test_submit_refuses_a_directory_clouddrive_does_not_have(tmp_path: Path) -> None:
    source, deps = make_source(tmp_path, cloud=FakeCloud({INBOX: ()}))

    with pytest.raises(DirectoryNotFoundError):
        await source.submit(['ABC-123'], task_dir_path=RANK_DIR)

    assert deps.ledger.states == {}


async def test_submit_reports_a_line_no_avid_can_be_read_from(tmp_path: Path) -> None:
    source, deps = make_source(tmp_path)

    submission = await source.submit(['???', 'ABC-123'], task_dir_path=INBOX)

    assert [(item.text, item.avid, item.outcome) for item in submission.entries] == [
        ('???', None, ManualOutcome.UNREADABLE),
        ('ABC-123', 'ABC-123', ManualOutcome.SUBMITTED),
    ]
    assert deps.ledger.states == {'ABC-123': AcquisitionState.DOWNLOADING}


async def test_submit_reports_an_avid_the_ledger_already_owns(tmp_path: Path) -> None:
    ledger = FakeLedger(known={'ABC-123': AcquisitionState.DOWNLOADING})
    source, deps = make_source(tmp_path, ledger=ledger)

    submission = await source.submit(['ABC-123', 'ABC-123'], task_dir_path=INBOX)

    assert [item.outcome for item in submission.entries] == [
        ManualOutcome.ALREADY_TRACKED,
        ManualOutcome.ALREADY_TRACKED,
    ]
    assert deps.cloud.submitted == []


async def test_submit_parks_an_avid_with_no_magnet(tmp_path: Path) -> None:
    source, deps = make_source(tmp_path, magnet=None)

    submission = await source.submit(['ABC-123'], task_dir_path=INBOX)

    assert [item.outcome for item in submission.entries] == [ManualOutcome.NO_MAGNET]
    assert deps.ledger.states['ABC-123'] is AcquisitionState.RESOLVE_FAILED


async def test_submit_reports_what_the_library_already_holds(tmp_path: Path) -> None:
    """Nothing is recorded: the operator asked for a video they already have."""
    held = tmp_path / 'library' / 'embyx' / 'ABC' / 'ABC-123.mp4'
    held.parent.mkdir(parents=True)
    held.write_bytes(b'0')
    source, deps = make_source(tmp_path)

    submission = await source.submit(['ABC-123'], task_dir_path=INBOX)

    assert [(item.outcome, item.archived_paths) for item in submission.entries] == [
        (ManualOutcome.ALREADY_IN_LIBRARY, ('embyx/ABC/ABC-123.mp4',)),
    ]
    assert deps.ledger.states == {}
    assert deps.cloud.submitted == []


async def test_submit_reports_a_magnet_clouddrive_rejects(tmp_path: Path) -> None:
    cloud = FakeCloud({INBOX: ()})
    cloud.add_offline_files = AsyncMock(return_value=SimpleNamespace(success=False))  # type: ignore[method-assign]
    source, deps = make_source(tmp_path, cloud=cloud)

    submission = await source.submit(['ABC-123'], task_dir_path=INBOX)

    assert [item.outcome for item in submission.entries] == [ManualOutcome.SUBMIT_FAILED]
    assert deps.ledger.attempt_states('ABC-123') == [AttemptState.ERROR]


async def test_submit_refuses_more_inputs_than_it_will_look_up(tmp_path: Path) -> None:
    source, deps = make_source(tmp_path)

    with pytest.raises(TooManyInputsError):
        await source.submit([f'ABC-{index:03d}' for index in range(MAX_MANUAL_INPUTS + 1)], task_dir_path=INBOX)

    assert deps.ledger.states == {}


async def test_submit_without_clouddrive(tmp_path: Path) -> None:
    source, _ = make_source(tmp_path, with_cloud=False)

    with pytest.raises(CloudUnavailableError):
        await source.submit(['ABC-123'], task_dir_path=INBOX)
