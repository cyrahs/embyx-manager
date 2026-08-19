"""Direct client-backed implementations of the fill-actor service ports.

These replace embyx-web's origin-checked runtime module loader: the scraping,
magnet, brand, and CloudDrive capabilities now live in this package as regular
clients, so the ports are satisfied with thin adapters instead of loaded
callables.
"""

import logging
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from embyx_manager.clients.clouddrive import AsyncCloudDrive
from embyx_manager.clients.javbus import JavBusClient
from embyx_manager.core import avid
from embyx_manager.fill_actor.cloud_moves import CloudFileMetadata, CloudFileMover, CloudMoveResponse
from embyx_manager.fill_actor.ports import AcquisitionOutcome, PageProgressCallback
from embyx_manager.monitor.acquisitions import AcquisitionSource
from embyx_manager.monitor.intake import AcquisitionIntake, IntakeOutcome
from embyx_manager.monitor.reports import RunContext

LOGGER = logging.getLogger(__name__)

NANOSECONDS_PER_SECOND = 1_000_000_000


class CloudDriveUnconfiguredError(RuntimeError):
    def __init__(self) -> None:
        super().__init__('CloudDrive connection is not configured')


@dataclass(frozen=True)
class JavBusActorCatalog:
    client: JavBusClient

    async def list_video_ids(
        self,
        actor_id: str,
        *,
        progress_callback: PageProgressCallback | None = None,
    ) -> Iterable[str]:
        return await self.client.scrape(actor_id, progress_callback=progress_callback)


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

    async def queue_missing(self, video_id: str) -> AcquisitionOutcome:
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
