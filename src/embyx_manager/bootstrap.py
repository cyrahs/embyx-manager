import threading
from pathlib import Path

from fastapi import FastAPI

from embyx_manager.adapters import (
    AvidBrandResolver,
    CloudDriveFileMover,
    JavBusActorCatalog,
    SukebeiMagnetProvider,
)
from embyx_manager.api import create_app, make_mutation_auth
from embyx_manager.clients.clouddrive import AsyncCloudDrive, CloudDriveClient
from embyx_manager.clients.javbus import JavBusClient
from embyx_manager.clients.sukebei import SukebeiClient
from embyx_manager.config import CloudDriveConfig, ConfigStore, FeedsConfig
from embyx_manager.config.api import create_config_router
from embyx_manager.db import Database
from embyx_manager.fill_actor.feeds import RSSHubFeedWarmer
from embyx_manager.fill_actor.jobs import FillActorJobManager
from embyx_manager.fill_actor.postgres_repository import PostgresFillActorRepository
from embyx_manager.fill_actor.service import FillActorPaths, FillActorService
from embyx_manager.locking import PostgresAdvisoryLock
from embyx_manager.settings import Settings


class CloudDriveHandle:
    """Owns the CloudDrive client and swaps it when its configuration changes."""

    def __init__(self, store: ConfigStore) -> None:
        self._store = store
        self._lock = threading.Lock()
        self._version: int | None = None
        self._client: CloudDriveClient | None = None
        self._cloud: AsyncCloudDrive | None = None

    def current(self) -> AsyncCloudDrive | None:
        config, version = self._store.get_with_version('clouddrive')
        if not isinstance(config, CloudDriveConfig):  # pragma: no cover - registry keeps types aligned
            msg = 'clouddrive config has unexpected type'
            raise TypeError(msg)
        with self._lock:
            if version != self._version:
                if self._client is not None:
                    self._client.close()
                self._client = None
                self._cloud = None
                self._version = version
                if config.configured:
                    self._client = CloudDriveClient(
                        address=config.address,
                        api_token=config.api_token,
                        secure=config.secure,
                        cloud_name=config.cloud_name,
                        cloud_account_id=config.cloud_account_id,
                    )
                    self._cloud = AsyncCloudDrive(self._client)
            return self._cloud

    def close(self) -> None:
        with self._lock:
            if self._client is not None:
                self._client.close()
            self._client = None
            self._cloud = None


def build_app(settings: Settings) -> FastAPI:
    settings.validate_exposure()
    actor_root, additional_roots, move_in_root = settings.require_fill_actor_paths()

    database = Database(settings.database_url)
    store = ConfigStore(database)
    javbus = JavBusClient()
    sukebei = SukebeiClient()
    cloud_handle = CloudDriveHandle(store)

    repository = PostgresFillActorRepository(database)
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
        cloud_file_mover=(CloudDriveFileMover(cloud_handle.current) if settings.cloud_move_paths is not None else None),
        cloud_move_paths=settings.cloud_move_paths,
        repository=repository,
        mutation_lock=PostgresAdvisoryLock(database.get_pool),
    )

    def current_feeds() -> FeedsConfig:
        return store.get(FeedsConfig)

    feed_warmer = RSSHubFeedWarmer(
        repository=repository,
        rsshub_url=lambda: current_feeds().rsshub_url or None,
        freshrss_url=lambda: current_feeds().freshrss_url or None,
        freshrss_rsshub_url=lambda: current_feeds().freshrss_rsshub_url or None,
    )
    jobs = FillActorJobManager(service=service, repository=repository, feed_warmer=feed_warmer)

    config_router = create_config_router(store, mutation_auth=make_mutation_auth(settings.api_token))

    async def close_runtime() -> None:
        try:
            await javbus.aclose()
        finally:
            try:
                await sukebei.aclose()
            finally:
                try:
                    cloud_handle.close()
                finally:
                    await database.aclose()

    frontend_dist = Path(__file__).resolve().parent / 'static'
    return create_app(
        service=service,
        repository=repository,
        jobs=jobs,
        api_token=settings.api_token,
        max_request_bytes=settings.max_request_bytes,
        runtime_close=close_runtime,
        frontend_dist=frontend_dist,
        freshrss_url=lambda: current_feeds().freshrss_url or None,
        freshrss_rsshub_url=lambda: current_feeds().freshrss_rsshub_url or None,
        extra_routers=(config_router,),
        on_startup=store.load,
    )
