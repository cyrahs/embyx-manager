"""The manual intake source: an operator's own list of AVIDs.

The third input source, beside the RSS categories and fill actor. It differs
from them only in where the AVIDs come from — pasted by an operator rather than
read from a feed or a library gap — and takes the same road afterwards: the
shared :class:`~embyx_manager.monitor.intake.AcquisitionIntake` records each one
in the ledger, resolves magnet candidates, submits the first, and the tracker
owns the download from there.

What it cannot borrow is a configured offline directory. RSS categories and fill
actor each declare one; a manual submission names its own per batch, browsed
from CloudDrive and defaulting to whichever directory the last manual batch
used. Two rules make an arbitrary directory safe to pick:

* it must have an archive route, or the finished download would sit in a folder
  the tracker never looks in — :meth:`submit` refuses the batch rather than
  writing ledger rows nothing could ever conclude;
* the tracker's poll set follows the ledger (``active_task_dirs``), so a
  directory outside the configured ones is polled for as long as it holds
  something in flight.

Input is read the way file names are read everywhere else in the pipeline, so a
pasted release name, a URL-ish title, or a bare id all resolve to the same
canonical AVID the rest of the ledger is keyed by.
"""

import asyncio
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum

from embyx_manager.clients.clouddrive import AsyncCloudDrive
from embyx_manager.config.models import normalize_absolute_path
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.acquisitions import AcquisitionRepository, AcquisitionSource
from embyx_manager.monitor.archive import ArchivePipeline
from embyx_manager.monitor.intake import AcquisitionIntake, IntakeOutcome
from embyx_manager.monitor.reports import RunContext

LOGGER = logging.getLogger('embyx-manager.manual')

#: How many lines one submission may carry. Each one costs magnet lookups at
#: two external sites, so a runaway paste is refused rather than served slowly.
MAX_MANUAL_INPUTS = 100


class ManualOutcome(StrEnum):
    """What became of one line of the operator's input."""

    SUBMITTED = 'submitted'
    ALREADY_TRACKED = 'already_tracked'
    ALREADY_IN_LIBRARY = 'already_in_library'
    NO_MAGNET = 'no_magnet'
    SUBMIT_FAILED = 'submit_failed'
    #: No AVID could be read from the line at all.
    UNREADABLE = 'unreadable'


@dataclass(frozen=True)
class ManualEntry:
    """One input line and how far it got."""

    text: str
    avid: str | None
    outcome: ManualOutcome
    #: Where the library already holds it, for ALREADY_IN_LIBRARY.
    archived_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class ManualSubmission:
    """One batch's outcome, with the directory it actually went to."""

    task_dir_path: str
    entries: tuple[ManualEntry, ...]


@dataclass(frozen=True)
class OfflineDirectory:
    """One CloudDrive directory offered as a submission target."""

    path: str
    name: str
    #: An input source's own directory, so the tracker polls it unconditionally.
    configured: bool
    #: Has an archive route, which is what makes it submittable at all.
    routed: bool


@dataclass(frozen=True)
class DirectoryListing:
    path: str
    #: The parent directory, or None at the CloudDrive root.
    parent: str | None
    entries: tuple[OfflineDirectory, ...] = ()


class ManualIntakeError(Exception):
    """A submission that cannot be attempted, with the reason as its code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class CloudUnavailableError(ManualIntakeError):
    def __init__(self) -> None:
        super().__init__('clouddrive_not_configured')


class DirectoryNotFoundError(ManualIntakeError):
    def __init__(self) -> None:
        super().__init__('directory_not_found')


class DirectoryNotRoutedError(ManualIntakeError):
    """The directory has no archive route, so a download there could never be filed."""

    def __init__(self) -> None:
        super().__init__('directory_not_routed')


class TooManyInputsError(ManualIntakeError):
    def __init__(self) -> None:
        super().__init__('too_many_inputs')


@dataclass(frozen=True)
class ManualIntakeSource:
    """Everything the manual source needs, resolved from live configuration.

    Each dependency is a callable rather than a value because the deployment is
    configured from the web UI: CloudDrive, the archive routes, and the AVID
    rules can all change between two submissions.
    """

    ledger: AcquisitionRepository
    intake_factory: Callable[[], AcquisitionIntake | None]
    cloud_factory: Callable[[], AsyncCloudDrive | None]
    archiver_factory: Callable[[], ArchivePipeline]
    parser_factory: Callable[[], AvidParser]
    #: The input sources' own offline directories, marked as such when browsing.
    configured_dirs: Callable[[], tuple[str, ...]]
    logger: logging.Logger = field(default=LOGGER)

    # -- picking a directory ---------------------------------------------------

    async def browse(self, path: str) -> DirectoryListing:
        """The subdirectories of one CloudDrive directory, annotated for picking."""
        cloud = self.cloud_factory()
        if cloud is None:
            raise CloudUnavailableError
        directory = _normalize_dir(path)
        try:
            files = await cloud.list_directory(directory)
        except FileNotFoundError as exc:
            raise DirectoryNotFoundError from exc
        except ValueError as exc:
            # An API path CloudDrive will not accept at all.
            raise DirectoryNotFoundError from exc
        configured = frozenset(self.configured_dirs())
        archiver = self.archiver_factory()
        entries = tuple(
            sorted(
                (
                    OfflineDirectory(
                        path=str(file['full_path']),
                        name=str(file['name']),
                        configured=str(file['full_path']) in configured,
                        routed=archiver.route_for_task_dir(str(file['full_path'])) is not None,
                    )
                    for file in files
                    if file['is_directory']
                ),
                key=lambda entry: entry.name,
            ),
        )
        return DirectoryListing(path=directory, parent=_parent_of(directory), entries=entries)

    async def default_directory(self) -> str | None:
        """Where to start: the last manual submission's directory, else a configured one.

        A configured directory is only offered when it has a route, since the
        submission would be refused otherwise; None leaves the operator to browse.
        """
        remembered = await self.ledger.latest_task_dir(source=AcquisitionSource.MANUAL)
        if remembered:
            return remembered
        archiver = self.archiver_factory()
        for directory in self.configured_dirs():
            if archiver.route_for_task_dir(directory) is not None:
                return directory
        return None

    # -- submitting ------------------------------------------------------------

    async def submit(self, inputs: Sequence[str], *, task_dir_path: str) -> ManualSubmission:
        """Run every input line through the shared intake; report each one's fate."""
        lines = [line.strip() for line in inputs if line.strip()]
        if len(lines) > MAX_MANUAL_INPUTS:
            raise TooManyInputsError
        intake = self.intake_factory()
        cloud = self.cloud_factory()
        if intake is None or cloud is None:
            raise CloudUnavailableError
        task_dir = _normalize_dir(task_dir_path)
        archiver = self.archiver_factory()
        if archiver.route_for_task_dir(task_dir) is None:
            raise DirectoryNotRoutedError
        try:
            await cloud.list_directory(task_dir)
        except FileNotFoundError as exc:
            raise DirectoryNotFoundError from exc

        parser = self.parser_factory()
        ctx = RunContext(logger=self.logger)
        entries: list[ManualEntry] = []
        for line in lines:
            avid = parser.get_avid(line)
            if not avid:
                ctx.warning('Failed to read an avid from %s', line)
                entries.append(ManualEntry(text=line, avid=None, outcome=ManualOutcome.UNREADABLE))
                continue
            entries.append(
                await self._submit_one(line, avid, task_dir=task_dir, archiver=archiver, intake=intake, ctx=ctx),
            )
        ctx.info('Manual submission: %d lines into %s', len(lines), task_dir)
        return ManualSubmission(task_dir_path=task_dir, entries=tuple(entries))

    async def _submit_one(  # noqa: PLR0913 - one call site, all of it per-line state
        self,
        line: str,
        avid: str,
        *,
        task_dir: str,
        archiver: ArchivePipeline,
        intake: AcquisitionIntake,
        ctx: RunContext,
    ) -> ManualEntry:
        held = await self._library_holdings(avid, task_dir, archiver, ctx)
        if held:
            # Nothing is recorded: the operator asked for something they already
            # have, and a ledger row would only claim an acquisition that never
            # happened. Telling them where it sits is the useful answer.
            ctx.info('%s is already in the library at %s', avid, held[0])
            return ManualEntry(
                text=line,
                avid=avid,
                outcome=ManualOutcome.ALREADY_IN_LIBRARY,
                archived_paths=held,
            )
        outcome = await intake.enqueue(
            avid,
            source=AcquisitionSource.MANUAL,
            task_dir_path=task_dir,
            ctx=ctx,
        )
        return ManualEntry(text=line, avid=avid, outcome=_OUTCOMES[outcome])

    async def _library_holdings(
        self,
        avid: str,
        task_dir: str,
        archiver: ArchivePipeline,
        ctx: RunContext,
    ) -> tuple[str, ...]:
        """Where the library already holds this AVID; () when it does not.

        An unreadable library is not a held one: the submission proceeds and the
        archive-time check gets the final word, exactly as it does for RSS.
        """
        try:
            return await asyncio.to_thread(archiver.library_holdings, avid, ctx, task_dir_path=task_dir)
        except Exception:  # noqa: BLE001 - unverifiable is not held
            ctx.exception('Failed to check the library for %s', avid)
            return ()


#: The shared intake's outcomes, in the manual source's own vocabulary.
_OUTCOMES: dict[IntakeOutcome, ManualOutcome] = {
    IntakeOutcome.SUBMITTED: ManualOutcome.SUBMITTED,
    IntakeOutcome.ALREADY_TRACKED: ManualOutcome.ALREADY_TRACKED,
    IntakeOutcome.NO_MAGNET: ManualOutcome.NO_MAGNET,
    IntakeOutcome.SUBMIT_FAILED: ManualOutcome.SUBMIT_FAILED,
}


def _normalize_dir(path: str) -> str:
    """An absolute CloudDrive API directory, or DirectoryNotFoundError."""
    try:
        normalized = normalize_absolute_path('directory', path)
    except ValueError as exc:
        raise DirectoryNotFoundError from exc
    if not normalized:
        raise DirectoryNotFoundError
    return normalized


def _parent_of(directory: str) -> str | None:
    if directory == '/':
        return None
    parent = directory.rsplit('/', maxsplit=1)[0]
    return parent or '/'
