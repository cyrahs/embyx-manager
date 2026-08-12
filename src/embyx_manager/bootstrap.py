from pathlib import Path

from fastapi import FastAPI

from embyx_manager.adapters import (
    AvidBrandResolver,
    CloudDriveFileMover,
    JavBusActorCatalog,
    SukebeiMagnetProvider,
)
from embyx_manager.api import create_app
from embyx_manager.clients.clouddrive import AsyncCloudDrive, CloudDriveClient
from embyx_manager.clients.javbus import JavBusClient
from embyx_manager.clients.sukebei import SukebeiClient
from embyx_manager.fill_actor.feeds import RSSHubFeedWarmer
from embyx_manager.fill_actor.jobs import FillActorJobManager
from embyx_manager.fill_actor.postgres_repository import PostgresFillActorRepository
from embyx_manager.fill_actor.service import FillActorPaths, FillActorService
from embyx_manager.locking import PostgresAdvisoryLock
from embyx_manager.settings import Settings


def build_app(settings: Settings) -> FastAPI:
    settings.validate_exposure()
    actor_root, additional_roots, move_in_root = settings.require_fill_actor_paths()

    javbus = JavBusClient()
    sukebei = SukebeiClient()
    cloud: AsyncCloudDrive | None = None
    if settings.clouddrive_address and settings.clouddrive_api_token:
        cloud = AsyncCloudDrive(
            CloudDriveClient(
                address=settings.clouddrive_address,
                api_token=settings.clouddrive_api_token,
                secure=settings.clouddrive_secure,
            ),
        )
    if settings.cloud_move_paths is not None and cloud is None:
        msg = 'the CloudDrive move path configuration requires the CloudDrive connection settings'
        raise ValueError(msg)

    repository = PostgresFillActorRepository(settings.database_url)
    service = FillActorService(
        paths=FillActorPaths.from_iterable(
            actor_brand_path=actor_root,
            additional_brand_paths=additional_roots,
            move_in_path=move_in_root,
        ),
        actor_catalog=JavBusActorCatalog(javbus),
        magnet_provider=SukebeiMagnetProvider(sukebei),
        brand_resolver=AvidBrandResolver(),
        max_actors=settings.max_actors,
        max_videos=settings.max_videos,
        magnet_concurrency=settings.magnet_concurrency,
        root_sentinel=settings.root_sentinel,
        move_in_by_brand=settings.move_in_by_brand,
        apply_enabled=settings.apply_enabled,
        cloud_file_mover=CloudDriveFileMover(cloud) if cloud is not None and settings.cloud_move_paths else None,
        cloud_move_paths=settings.cloud_move_paths,
        repository=repository,
        mutation_lock=PostgresAdvisoryLock(repository.get_pool),
    )
    feed_warmer = (
        RSSHubFeedWarmer(
            repository=repository,
            rsshub_url=settings.rsshub_url,
            freshrss_url=settings.freshrss_url,
            freshrss_rsshub_url=settings.freshrss_rsshub_url,
        )
        if settings.rsshub_url is not None
        else None
    )
    jobs = FillActorJobManager(service=service, repository=repository, feed_warmer=feed_warmer)

    async def close_runtime() -> None:
        try:
            await javbus.aclose()
        finally:
            try:
                await sukebei.aclose()
            finally:
                try:
                    if cloud is not None:
                        await cloud.aclose()
                finally:
                    await repository.aclose()

    frontend_dist = Path(__file__).resolve().parent / 'static'
    return create_app(
        service=service,
        repository=repository,
        jobs=jobs,
        api_token=settings.api_token,
        max_request_bytes=settings.max_request_bytes,
        runtime_close=close_runtime,
        frontend_dist=frontend_dist,
        freshrss_url=settings.freshrss_url,
        freshrss_rsshub_url=settings.freshrss_rsshub_url,
    )
