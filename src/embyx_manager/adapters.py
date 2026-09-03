"""Direct client-backed implementations of the fill-actor service ports.

These replace embyx-web's origin-checked runtime module loader: the scraping,
magnet, brand, and CloudDrive capabilities now live in this package as regular
clients, so the ports are satisfied with thin adapters instead of loaded
callables.
"""

import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date

from embyx_manager.clients.avbase import AvbaseClient, AvbaseError, AvbaseTalent
from embyx_manager.clients.clouddrive import AsyncCloudDrive
from embyx_manager.clients.javbus import JavBusActor, JavBusClient
from embyx_manager.core import avid
from embyx_manager.fill_actor.cloud_moves import CloudFileMetadata, CloudFileMover, CloudMoveResponse
from embyx_manager.fill_actor.ports import AcquisitionOutcome, CatalogListing, PageProgressCallback
from embyx_manager.monitor.acquisitions import AcquisitionSource
from embyx_manager.monitor.intake import AcquisitionIntake, IntakeOutcome
from embyx_manager.monitor.reports import RunContext

LOGGER = logging.getLogger(__name__)

NANOSECONDS_PER_SECOND = 1_000_000_000
#: What a JavBus star id looks like; a name that happens to fit is simply also tried as one.
_STAR_ID_RE = re.compile(r'^[A-Za-z0-9_-]{1,32}$')


class CloudDriveUnconfiguredError(RuntimeError):
    def __init__(self) -> None:
        super().__init__('CloudDrive connection is not configured')


class ActorNotFoundError(LookupError):
    def __init__(self, actor_ref: str) -> None:
        super().__init__(f'neither AVBase nor JavBus knows an actor called {actor_ref!r}')


@dataclass(frozen=True)
class UnionActorCatalog:
    """AVBase's catalog joined with JavBus's, so neither's gaps become the scan's.

    AVBase knows the actor under every alias and dates every work, but drops
    delisted titles and an actor's pre-rename catalog; JavBus keeps those but
    holds one star page per alias. The actor may be named either way: an
    AVBase name (any alias) or a JavBus star id. JavBus being unreachable only
    costs its share of the listing; AVBase being unreachable is reported when
    JavBus cannot stand in.
    """

    avbase: AvbaseClient
    javbus: JavBusClient

    async def list_videos(
        self,
        actor_ref: str,
        *,
        progress_callback: PageProgressCallback | None = None,
    ) -> CatalogListing:
        ref = actor_ref.strip()
        talent = await self._find_talent(ref)
        names = list(talent.names) if talent is not None else []
        stars = await self._find_stars(ref, names)
        if talent is None and not stars:
            raise ActorNotFoundError(ref)

        video_ids: dict[str, None] = {}
        release_dates: dict[str, date] = {}
        counts: dict[str, int] = {}
        if talent is not None:
            listed = 0
            async for page, pages, works in self.avbase.talent_pages(talent.name):
                if progress_callback is not None:
                    await progress_callback(page, pages, page)
                for work in works:
                    video_ids.setdefault(work.work_id, None)
                    listed += 1
                    if work.release_date is not None:
                        release_dates.setdefault(work.work_id, work.release_date)
            counts['avbase'] = listed
        for star in stars:
            try:
                ids = await self.javbus.scrape(star.actor_id, progress_callback=progress_callback)
            except Exception:  # noqa: BLE001 - JavBus is the supplement, not the source of truth
                LOGGER.warning('JavBus star %s could not be listed; continuing without it', star.actor_id)
                counts[f'javbus:{star.actor_id}'] = -1
                continue
            counts[f'javbus:{star.actor_id}'] = len(ids)
            for video_id in ids:
                video_ids.setdefault(video_id, None)
        return CatalogListing(
            actor_name=talent.name if talent is not None else stars[0].name,
            talent_id=talent.talent_id if talent is not None else None,
            aliases=tuple(talent.aliases) if talent is not None else (),
            video_ids=tuple(video_ids),
            release_dates=release_dates,
            source_counts=counts,
        )

    async def _find_talent(self, ref: str) -> AvbaseTalent | None:
        try:
            talent = await self.avbase.talent(ref)
            if talent is None and _STAR_ID_RE.fullmatch(ref):
                # A JavBus star id: the star page names the actor, and AVBase may know that name.
                page = await self._star_page(ref)
                if page is not None and page.name:
                    talent = await self.avbase.talent(page.name)
        except AvbaseError:
            LOGGER.warning('AVBase could not be read for %r; falling back to JavBus alone', ref)
            return None
        return talent

    async def _star_page(self, star_id: str):  # noqa: ANN202 - JavBusActorPage, kept out of the signature
        try:
            return await self.javbus.get_actor(star_id)
        except Exception:  # noqa: BLE001
            LOGGER.warning('JavBus star page %s could not be read', star_id)
            return None

    async def _find_stars(self, ref: str, names: list[str]) -> list[JavBusActor]:
        # Ordered: the primary name first, then aliases, so the listing is reproducible.
        wanted: dict[str, None] = dict.fromkeys(names or [ref])
        stars: dict[str, JavBusActor] = {}
        if _STAR_ID_RE.fullmatch(ref):
            page = await self._star_page(ref)
            if page is not None:
                stars[page.actor_id.casefold()] = JavBusActor(actor_id=page.actor_id, name=page.name)
                wanted.setdefault(page.name, None)
        for name in wanted:
            try:
                found = await self.javbus.search_stars(name)
            except Exception:  # noqa: BLE001
                LOGGER.warning('JavBus star search for %r failed', name)
                continue
            for star in found:
                # JavBus search is fuzzy: only a star credited under one of the actor's names counts.
                if star.name in wanted:
                    stars.setdefault(star.actor_id.casefold(), star)
        return list(stars.values())


@dataclass(frozen=True)
class LedgerAcquisitionGateway:
    """Fill-actor port implementation backed by the monitor acquisition intake.

    ``intake_factory`` resolves the intake from the live configuration per call,
    or ``None`` while CloudDrive is unconfigured; ``task_dir`` names the offline
    directory fill-actor submissions are queued under, '' while unset. Either
    gap fails the submission with an outcome naming the missing configuration,
    without creating ledger rows nothing would ever advance.

    Queueing only records the AVID; the tracker's background pass resolves
    magnets and submits later. ``on_queued`` pokes that pass awake so the first
    resolve starts within the fast-check window instead of a full poll interval.
    """

    intake_factory: Callable[[], AcquisitionIntake | None]
    task_dir: Callable[[], str]
    on_queued: Callable[[], None] | None = None

    async def queue_missing(self, video_id: str, *, release_date: date | None = None) -> AcquisitionOutcome:
        intake = self.intake_factory()
        if intake is None:
            return AcquisitionOutcome.CLOUD_NOT_CONFIGURED
        task_dir_path = self.task_dir()
        if not task_dir_path:
            return AcquisitionOutcome.TASK_DIR_NOT_CONFIGURED
        ctx = RunContext(logger=LOGGER)
        outcome = await intake.queue(
            video_id,
            source=AcquisitionSource.FILL_ACTOR,
            task_dir_path=task_dir_path,
            ctx=ctx,
            release_date=release_date,
        )
        if outcome is IntakeOutcome.QUEUED and self.on_queued is not None:
            self.on_queued()
        return AcquisitionOutcome(outcome.value)


class AvidBrandResolver:
    def resolve_brand(self, video_id: str) -> str | None:
        return avid.get_brand(video_id)


@dataclass(frozen=True)
class CloudDriveFileMover(CloudFileMover):
    """Cloud mover resolving the current client per call.

    ``get_cloud`` returns the AsyncCloudDrive built from the live configuration,
    or ``None`` while CloudDrive is unconfigured — operations then fail with
    CloudDriveUnconfiguredError, which the service's readiness checks treat as
    "cloud not ready".
    """

    get_cloud: Callable[[], AsyncCloudDrive | None]

    def _cloud(self) -> AsyncCloudDrive:
        cloud = self.get_cloud()
        if cloud is None:
            raise CloudDriveUnconfiguredError
        return cloud

    async def list_directory(self, api_directory: str) -> tuple[CloudFileMetadata, ...]:
        values = await self._cloud().list_directory(api_directory)
        files: list[CloudFileMetadata] = []
        for value in values:
            is_directory = value.get('is_directory')
            if not isinstance(is_directory, bool):
                msg = 'CloudDrive listing returned invalid file type'
                raise TypeError(msg)
            if is_directory:
                continue
            write_time = value.get('write_time')
            if not isinstance(write_time, Mapping):
                msg = 'CloudDrive listing returned invalid write time'
                raise TypeError(msg)
            seconds = write_time.get('seconds')
            nanos = write_time.get('nanos')
            if (
                not isinstance(seconds, int)
                or isinstance(seconds, bool)
                or not isinstance(nanos, int)
                or isinstance(nanos, bool)
                or nanos < 0
                or nanos >= NANOSECONDS_PER_SECOND
            ):
                msg = 'CloudDrive listing returned invalid write time'
                raise TypeError(msg)
            files.append(
                CloudFileMetadata.from_mapping(
                    {
                        'path': value.get('full_path'),
                        'id': value.get('id'),
                        'name': value.get('name'),
                        'size': value.get('size'),
                        'write_time': seconds * NANOSECONDS_PER_SECOND + nanos,
                        'hashes': value.get('hashes', {}),
                    }
                )
            )
        return tuple(files)

    async def ensure_directory(self, parent_api_directory: str, folder_name: str) -> bool:
        value = await self._cloud().ensure_directory(parent_api_directory, folder_name)
        success = value.get('success')
        if not isinstance(success, bool):
            msg = 'CloudDrive returned invalid directory result'
            raise TypeError(msg)
        return success

    async def move_file(self, source_api_path: str, destination_api_directory: str) -> CloudMoveResponse:
        value = await self._cloud().move_file(source_api_path, destination_api_directory)
        return CloudMoveResponse.from_mapping(
            {
                'success': value.get('success'),
                'error_message': value.get('error_message'),
                'result_paths': value.get('result_file_paths', ()),
            }
        )
