"""Direct client-backed implementations of the fill-actor service ports.

These replace embyx-web's origin-checked runtime module loader: the scraping,
magnet, brand, and CloudDrive capabilities now live in this package as regular
clients, so the ports are satisfied with thin adapters instead of loaded
callables.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from embyx_manager.clients.clouddrive import AsyncCloudDrive
from embyx_manager.clients.javbus import JavBusClient
from embyx_manager.clients.sukebei import SukebeiClient
from embyx_manager.core import avid
from embyx_manager.fill_actor.cloud_moves import CloudFileMetadata, CloudFileMover, CloudMoveResponse
from embyx_manager.fill_actor.ports import PageProgressCallback

NANOSECONDS_PER_SECOND = 1_000_000_000


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
class SukebeiMagnetProvider:
    client: SukebeiClient

    async def find_magnet(self, video_id: str) -> str | None:
        return await self.client.get_magnet(video_id)


class AvidBrandResolver:
    def resolve_brand(self, video_id: str) -> str | None:
        return avid.get_brand(video_id)


@dataclass(frozen=True)
class CloudDriveFileMover(CloudFileMover):
    cloud: AsyncCloudDrive

    async def list_directory(self, api_directory: str) -> tuple[CloudFileMetadata, ...]:
        values = await self.cloud.list_directory(api_directory)
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
        value = await self.cloud.ensure_directory(parent_api_directory, folder_name)
        success = value.get('success')
        if not isinstance(success, bool):
            msg = 'CloudDrive returned invalid directory result'
            raise TypeError(msg)
        return success

    async def move_file(self, source_api_path: str, destination_api_directory: str) -> CloudMoveResponse:
        value = await self.cloud.move_file(source_api_path, destination_api_directory)
        return CloudMoveResponse.from_mapping(
            {
                'success': value.get('success'),
                'error_message': value.get('error_message'),
                'result_paths': value.get('result_file_paths', ()),
            }
        )
