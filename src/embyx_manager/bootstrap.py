import logging
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import grpc
from fastapi import FastAPI

from embyx_manager.adapters import (
    AvidBrandResolver,
    CloudDriveFileMover,
    JavBusActorCatalog,
    SukebeiMagnetProvider,
)
from embyx_manager.api import create_app, make_mutation_auth
from embyx_manager.clients.clouddrive import AsyncCloudDrive, CloudDriveClient
from embyx_manager.clients.freshrss import FreshRSSClient
from embyx_manager.clients.javbus import JavBusClient
from embyx_manager.clients.sukebei import SukebeiClient
from embyx_manager.config import (
    ArchiveConfig,
    CloudDriveConfig,
    ConfigStore,
    FeedsConfig,
    FillActorConfig,
    FreshRSSConfig,
    MappingConfig,
    RssConfig,
)
from embyx_manager.config.api import create_config_router
from embyx_manager.core.avid import AvidConfig, AvidParser
from embyx_manager.db import Database
from embyx_manager.fill_actor.api import (
    create_fill_actor_router,
    fill_actor_health,
    fill_actor_lifespan,
    handle_fill_actor_error,
)
from embyx_manager.fill_actor.cloud_moves import CloudMovePaths
from embyx_manager.fill_actor.errors import FillActorError
from embyx_manager.fill_actor.feeds import RSSHubFeedWarmer
from embyx_manager.fill_actor.jobs import FillActorJobManager
from embyx_manager.fill_actor.postgres_repository import PostgresFillActorRepository
from embyx_manager.fill_actor.service import FillActorPaths, FillActorRuntime, FillActorService
from embyx_manager.locking import PostgresAdvisoryLock
from embyx_manager.monitor.acquisitions import AcquisitionRepository
from embyx_manager.monitor.api import create_monitor_router
from embyx_manager.monitor.archive import ArchivePipeline
from embyx_manager.monitor.mapping import MappingPipeline
from embyx_manager.monitor.reconcile import ReconcileScanner
from embyx_manager.monitor.reports import RunContext
from embyx_manager.monitor.rss import RssPipeline
from embyx_manager.monitor.runs import PipelineRunRepository
from embyx_manager.monitor.scheduler import MonitorScheduler
from embyx_manager.monitor.tracker import AcquisitionTracker, TrackerSettings
from embyx_manager.settings import Settings

LOGGER = logging.getLogger(__name__)


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
                    )
                    self._cloud = AsyncCloudDrive(self._client)
            return self._cloud

    def close(self) -> None:
        with self._lock:
            if self._client is not None:
                self._client.close()
            self._client = None
            self._cloud = None


class FillActorRuntimeHandle:
    """Turns the stored Fill Actor section into the snapshot the service reads.

    Rebuilt only when the section's version changes, so the hot scan path does no
    parsing. An invalid stored value leaves the feature unconfigured rather than
    taking the process down — the Settings page is how it gets fixed.
    """

    def __init__(self, store: ConfigStore) -> None:
        self._store = store
        self._lock = threading.Lock()
        self._version: int | None = None
        self._runtime = FillActorRuntime()

    def current(self) -> FillActorRuntime:
        config, version = self._store.get_with_version('fill_actor')
        if not isinstance(config, FillActorConfig):  # pragma: no cover - registry keeps types aligned
            msg = 'fill_actor config has unexpected type'
            raise TypeError(msg)
        with self._lock:
            if version != self._version:
                self._version = version
                self._runtime = _build_fill_actor_runtime(config, version)
            return self._runtime


def _build_fill_actor_runtime(config: FillActorConfig, version: int) -> FillActorRuntime:
    if not config.configured:
        return FillActorRuntime(version=version)
    try:
        cloud_move_paths = (
            CloudMovePaths.from_values(
                strm_mount_prefix=config.cloud_strm_mount_prefix,
                source_api_roots=config.cloud_source_roots,
                move_in_api_root=config.cloud_move_in_root,
            )
            if config.cloud_configured
            else None
        )
    except ValueError:
        LOGGER.exception('stored fill_actor CloudDrive paths are invalid; moves stay disabled')
        return FillActorRuntime(version=version)
    return FillActorRuntime(
        version=version,
        paths=FillActorPaths.from_iterable(
            actor_brand_path=Path(config.actor_root),
            additional_brand_paths=tuple(Path(root) for root in config.additional_roots),
            move_in_path=Path(config.move_in_root),
        ),
        root_sentinel=config.root_sentinel,
        move_in_by_brand=config.move_in_by_brand,
        apply_enabled=config.apply_enabled,
        cloud_move_paths=cloud_move_paths,
    )


class AvidParserHandle:
    """Rebuilds the AvidParser when the avid rules configuration changes."""

    def __init__(self, store: ConfigStore) -> None:
        self._store = store
        self._version: int | None = None
        self._parser = AvidParser()

    def current(self) -> AvidParser:
        rules, version = self._store.get_with_version('avid')
        if version != self._version:
            self._parser = AvidParser(
                AvidConfig(
                    id_exceptions=tuple(rules.id_exceptions),
                    ignored_id_patterns=tuple(rules.ignored_id_patterns),
                ),
            )
            self._version = version
        return self._parser


def build_app(settings: Settings) -> FastAPI:  # noqa: C901, PLR0915 - assembly root
    settings.validate_exposure()

    database = Database(settings.database_url)
    store = ConfigStore(database)
    javbus = JavBusClient()
    sukebei = SukebeiClient()
    cloud_handle = CloudDriveHandle(store)

    repository = PostgresFillActorRepository(database)
    fill_actor_runtime = FillActorRuntimeHandle(store)
    service = FillActorService(
        runtime=fill_actor_runtime.current,
        actor_catalog=JavBusActorCatalog(javbus),
        magnet_provider=SukebeiMagnetProvider(sukebei),
        brand_resolver=AvidBrandResolver(),
        max_actors=settings.max_actors,
        max_videos=settings.max_videos,
        magnet_concurrency=settings.magnet_concurrency,
        # The mover is always available; whether moves may run is the runtime's call,
        # since the CloudDrive paths it needs now live in the database.
        cloud_file_mover=CloudDriveFileMover(cloud_handle.current),
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

    mutation_auth = make_mutation_auth(settings.api_token)
    config_router = create_config_router(store, mutation_auth=mutation_auth)
    fill_actor_router = create_fill_actor_router(
        service=service,
        repository=repository,
        jobs=jobs,
        mutation_auth=mutation_auth,
        freshrss_url=lambda: current_feeds().freshrss_url or None,
        freshrss_rsshub_url=lambda: current_feeds().freshrss_rsshub_url or None,
    )

    avid_handle = AvidParserHandle(store)
    pipeline_runs = PipelineRunRepository(database)
    ledger = AcquisitionRepository(database)

    def rss_trigger_ready() -> str | None:
        return _rss_configuration_gap(store)

    async def rss_runner(ctx: RunContext, rank: bool) -> None:  # noqa: FBT001
        cloud = cloud_handle.current()
        if cloud is None:
            msg = 'CloudDrive is not configured'
            raise RuntimeError(msg)
        freshrss_config = store.get(FreshRSSConfig)
        rss_config = store.get(RssConfig)
        clouddrive_config = store.get(CloudDriveConfig)
        client = FreshRSSClient(
            url=freshrss_config.url,
            api_key=freshrss_config.api_key,
            proxy=freshrss_config.proxy or None,
        )

        try:
            pipeline = RssPipeline(
                config=rss_config,
                avid_parser=avid_handle.current(),
                freshrss=client,
                cloud=cloud,
                sukebei=sukebei,
                javbus=javbus,
                task_dir_path=clouddrive_config.task_dir_path,
                ledger=ledger,
            )
            await pipeline.run(ctx, rank=rank)
        finally:
            await client.aclose()

    def archive_ready() -> str | None:
        if not store.get(ArchiveConfig).configured:
            return 'archive source, destination, and mapping must be configured'
        return None

    # Held across runs: settling a folder takes two passes to observe.
    reconcile_scanner: list[ReconcileScanner] = []

    async def archive_runner(ctx: RunContext) -> None:
        clouddrive_config = store.get(CloudDriveConfig)
        cloud = cloud_handle.current()
        if cloud is not None and clouddrive_config.task_dir_path:
            try:
                await cloud.list_directory(clouddrive_config.task_dir_path)
            except Exception:  # noqa: BLE001
                ctx.exception('failed to refresh the CloudDrive task directory')
        archive_config = store.get(ArchiveConfig)
        archiver = ArchivePipeline(config=archive_config, avid_parser=avid_handle.current())
        if reconcile_scanner:
            reconcile_scanner[0].rebind(archiver=archiver, config=archive_config)
        else:
            reconcile_scanner.append(
                ReconcileScanner(ledger=ledger, archiver=archiver, config=archive_config),
            )
        await reconcile_scanner[0].run(ctx)

    def tracker_ready() -> str | None:
        archive_config = store.get(ArchiveConfig)
        if not archive_config.enabled:
            return 'archive is disabled'
        if not archive_config.tracker_configured:
            return 'the archive task directory and destination must be configured'
        if cloud_handle.current() is None or not store.get(CloudDriveConfig).task_dir_path:
            return 'CloudDrive and its task directory must be configured'
        return None

    async def tracker_poll(ctx: RunContext) -> None:
        cloud = cloud_handle.current()
        if cloud is None:
            return
        archive_config = store.get(ArchiveConfig)
        clouddrive_config = store.get(CloudDriveConfig)
        archiver = ArchivePipeline(config=archive_config, avid_parser=avid_handle.current())

        async def submit_magnet(avid: str, magnet: str) -> bool:
            try:
                result = await cloud.add_offline_files([magnet], clouddrive_config.task_dir_path)
            except grpc.RpcError as exc:
                # Already queued at CloudDrive: the hash is what we track, so this
                # counts as submitted either way.
                if '任务已存在' in (exc.details() or ''):
                    return True
                ctx.exception('failed to add a magnet for %s', avid)
                return False
            except Exception:  # noqa: BLE001
                ctx.exception('failed to add a magnet for %s', avid)
                return False
            return bool(getattr(result, 'success', False))

        tracker = AcquisitionTracker(
            ledger=ledger,
            cloud=cloud,
            archiver=archiver,
            settings=TrackerSettings(
                task_dir_path=clouddrive_config.task_dir_path,
                task_dir_local=Path(archive_config.task_dir_local),
                task_dst=archive_config.task_dst,
                task_priority=archive_config.task_priority,
                stall_timeout_hours=archive_config.stall_timeout_hours,
                max_attempts=archive_config.max_attempts,
            ),
            submit_magnet=submit_magnet,
        )
        await tracker.poll(ctx)

    def mapping_ready() -> str | None:
        if not store.get(MappingConfig).configured:
            return 'mapping source and destination directories must be configured'
        return None

    def mapping_factory() -> MappingPipeline:
        return MappingPipeline(config=store.get(MappingConfig), avid_parser=avid_handle.current())

    scheduler = MonitorScheduler(
        store=store,
        runs=pipeline_runs,
        rss_runner=rss_runner,
        archive_runner=archive_runner,
        mapping_factory=mapping_factory,
        rss_ready=rss_trigger_ready,
        archive_ready=archive_ready,
        mapping_ready=mapping_ready,
        tracker_poll=tracker_poll,
        tracker_ready=tracker_ready,
    )
    monitor_router = create_monitor_router(scheduler, pipeline_runs, mutation_auth=mutation_auth)

    @asynccontextmanager
    async def runtime_lifespan() -> AsyncIterator[None]:
        """Config store, scheduler and shared clients: everything the features sit on."""
        await store.load()
        await scheduler.start()
        try:
            yield
        finally:
            try:
                await scheduler.aclose()
            finally:
                await _close_clients()

    async def _close_clients() -> None:
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
        app_ready=repository.health_check,
        routers=(fill_actor_router, config_router, monitor_router),
        feature_health={'fill_actor': fill_actor_health(service=service, repository=repository)},
        exception_handlers={FillActorError: handle_fill_actor_error},
        # The runtime comes up first and goes down last; features stack on top of it.
        lifespans=(
            runtime_lifespan,
            fill_actor_lifespan(service=service, repository=repository, jobs=jobs),
        ),
        api_token=settings.api_token,
        max_request_bytes=settings.max_request_bytes,
        frontend_dist=frontend_dist,
    )


def _rss_configuration_gap(store: ConfigStore) -> str | None:
    if not store.get(FreshRSSConfig).configured:
        return 'FreshRSS is not configured'
    clouddrive = store.get(CloudDriveConfig)
    if not clouddrive.configured:
        return 'CloudDrive is not configured'
    if not clouddrive.task_dir_path:
        return 'CloudDrive task directory is not configured'
    return None
