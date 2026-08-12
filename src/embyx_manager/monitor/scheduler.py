"""Scheduler that runs the monitor pipelines.

Replaces embyx-monitor's hand-written thread/loop orchestration with asyncio
tasks:

- an update cycle running rss then archive every ``rss.interval_seconds``;
- a mapping loop with one full sync at startup, a periodic full sync as a
  safety net, and debounced incremental syncs fed by a watchdog observer;
- manual triggers from the dashboard, one in-flight run per pipeline.

Configuration is read from the live config store at every decision point, so
edits apply without a restart.
"""

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from embyx_manager.config.models import ArchiveConfig, MappingConfig, RssConfig
from embyx_manager.config.store import ConfigStore
from embyx_manager.monitor.reports import (
    PipelineName,
    RunCancelledError,
    RunContext,
    RunState,
    RunTrigger,
)
from embyx_manager.monitor.runs import PipelineRunRepository

LOGGER = logging.getLogger('embyx-manager.scheduler')

FULL_SYNC_RETRY_SECONDS = 300.0
IDLE_POLL_SECONDS = 0.5
STARTUP_RETRY_SECONDS = 60.0
CONFIG_REFRESH_SECONDS = 15.0


class PipelineBusyError(Exception):
    def __init__(self, pipeline: PipelineName) -> None:
        super().__init__(f'pipeline {pipeline.value} already has a run in flight')
        self.pipeline = pipeline


class PipelineNotConfiguredError(Exception):
    def __init__(self, pipeline: PipelineName, reason: str) -> None:
        super().__init__(f'pipeline {pipeline.value} is not runnable: {reason}')
        self.pipeline = pipeline
        self.reason = reason


@dataclass(frozen=True)
class PipelineStatus:
    pipeline: PipelineName
    enabled: bool
    configured: bool
    reason: str | None
    running_run_id: str | None
    next_scheduled_at: datetime | None


class _StrmEventHandler(FileSystemEventHandler):
    """Marshals watchdog thread events into the scheduler's event loop."""

    def __init__(self, loop: asyncio.AbstractEventLoop, notify: Callable[[str, bool], None]) -> None:
        self._loop = loop
        self._notify = notify

    def _forward(self, path: str | None, *, is_directory: bool, deleted: bool) -> None:
        if is_directory or not path or Path(path).suffix.lower() != '.strm':
            return
        self._loop.call_soon_threadsafe(self._notify, path, deleted)

    def on_created(self, event) -> None:  # noqa: ANN001
        self._forward(event.src_path, is_directory=event.is_directory, deleted=False)

    def on_modified(self, event) -> None:  # noqa: ANN001
        self._forward(event.src_path, is_directory=event.is_directory, deleted=False)

    def on_deleted(self, event) -> None:  # noqa: ANN001
        self._forward(event.src_path, is_directory=event.is_directory, deleted=True)

    def on_moved(self, event) -> None:  # noqa: ANN001
        self._forward(event.src_path, is_directory=event.is_directory, deleted=True)
        dest_path = getattr(event, 'dest_path', None)
        self._forward(dest_path, is_directory=event.is_directory, deleted=False)


class MonitorScheduler:
    def __init__(  # noqa: PLR0913
        self,
        *,
        store: ConfigStore,
        runs: PipelineRunRepository,
        rss_runner: Callable[[RunContext, bool], object],
        archive_runner: Callable[[RunContext], object],
        mapping_factory: Callable[[], object],
        rss_ready: Callable[[], str | None],
        archive_ready: Callable[[], str | None],
        mapping_ready: Callable[[], str | None],
    ) -> None:
        """Runner callables execute one pipeline pass.

        ``rss_runner(ctx, rank)`` and ``archive_runner(ctx)`` return awaitables;
        ``mapping_factory()`` builds a MappingPipeline-compatible object from the
        current configuration. ``*_ready()`` return None when runnable or a
        human-readable reason string when not.
        """
        self._store = store
        self._runs = runs
        self._rss_runner = rss_runner
        self._archive_runner = archive_runner
        self._mapping_factory = mapping_factory
        self._ready = {
            PipelineName.RSS: rss_ready,
            PipelineName.ARCHIVE: archive_ready,
            PipelineName.MAPPING: mapping_ready,
        }
        self._active: dict[PipelineName, tuple[str, RunContext]] = {}
        self._locks = {name: asyncio.Lock() for name in PipelineName}
        self._tasks: list[asyncio.Task[None]] = []
        self._stop = asyncio.Event()
        self._next_update_at: datetime | None = None
        self._next_full_sync_at: datetime | None = None
        # watchdog state (event-loop confined)
        self._observer: Observer | None = None
        self._observed_dir: str | None = None
        self._changed_paths: set[Path] = set()
        self._deleted_paths: set[Path] = set()
        self._fs_event_count = 0
        self._last_fs_event = 0.0

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        recovered = await self._runs.fail_stale_running(error='process restarted while the run was in flight')
        if recovered:
            LOGGER.warning('marked %d orphaned pipeline runs as failed', recovered)
        self._tasks = [
            asyncio.create_task(self._update_loop(), name='monitor-update-loop'),
            asyncio.create_task(self._mapping_loop(), name='monitor-mapping-loop'),
            asyncio.create_task(self._config_refresh_loop(), name='config-refresh-loop'),
        ]

    async def aclose(self) -> None:
        self._stop.set()
        for _, ctx in self._active.values():
            ctx.request_stop()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        self._stop_observer()

    # -- public API ----------------------------------------------------------

    async def trigger(self, pipeline: PipelineName, *, rank: bool = False) -> str:
        reason = self._ready[pipeline]()
        if reason is not None:
            raise PipelineNotConfiguredError(pipeline, reason)
        if self._locks[pipeline].locked():
            raise PipelineBusyError(pipeline)
        started: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        task = asyncio.create_task(
            self._execute(pipeline, RunTrigger.MANUAL, rank=rank, started=started),
            name=f'manual-{pipeline.value}',
        )
        self._tasks.append(task)
        task.add_done_callback(lambda finished: self._tasks.remove(finished) if finished in self._tasks else None)
        return await started

    async def cancel_running(self, pipeline: PipelineName) -> bool:
        active = self._active.get(pipeline)
        if active is None:
            return False
        active[1].request_stop()
        return True

    def status(self) -> tuple[PipelineStatus, ...]:
        statuses = []
        for name in PipelineName:
            reason = self._ready[name]()
            enabled = self._pipeline_enabled(name)
            active = self._active.get(name)
            if name is PipelineName.MAPPING:
                next_at = self._next_full_sync_at
            elif name is PipelineName.ARCHIVE:
                next_at = self._next_update_at if enabled else None
            else:
                next_at = self._next_update_at
            statuses.append(
                PipelineStatus(
                    pipeline=name,
                    enabled=enabled,
                    configured=reason is None,
                    reason=reason,
                    running_run_id=active[0] if active is not None else None,
                    next_scheduled_at=next_at if enabled and reason is None else None,
                ),
            )
        return tuple(statuses)

    # -- execution -------------------------------------------------------------

    async def _execute(  # noqa: C901, PLR0912 - single dispatch point for run bookkeeping
        self,
        pipeline: PipelineName,
        trigger: RunTrigger,
        *,
        rank: bool = False,
        started: asyncio.Future[str] | None = None,
        incremental_batch: tuple[set[Path], set[Path]] | None = None,
    ) -> RunState | None:
        lock = self._locks[pipeline]
        if lock.locked():
            if started is not None and not started.done():
                started.set_exception(PipelineBusyError(pipeline))
            return None
        async with lock:
            ctx = RunContext(logger=logging.getLogger(f'embyx-manager.{pipeline.value}'))
            try:
                run_id = await self._runs.start_run(pipeline, trigger)
            except Exception as exc:
                LOGGER.exception('failed to persist run start for %s', pipeline.value)
                if started is not None and not started.done():
                    started.set_exception(exc)
                return None
            if started is not None and not started.done():
                started.set_result(run_id)
            self._active[pipeline] = (run_id, ctx)
            state = RunState.COMPLETED
            try:
                if pipeline is PipelineName.RSS:
                    await self._rss_runner(ctx, rank)
                elif pipeline is PipelineName.ARCHIVE:
                    await self._archive_runner(ctx)
                elif incremental_batch is not None:
                    await self._run_mapping_incremental(ctx, incremental_batch)
                else:
                    pipeline_obj = self._mapping_factory()
                    await asyncio.to_thread(pipeline_obj.run_full, ctx)
            except RunCancelledError:
                state = RunState.CANCELLED
                ctx.warning('run cancelled by operator')
            except asyncio.CancelledError:
                state = RunState.CANCELLED
                ctx.warning('run cancelled by shutdown')
            except Exception:  # noqa: BLE001
                state = RunState.FAILED
                ctx.exception('%s run failed', pipeline.value)
            finally:
                self._active.pop(pipeline, None)
                with contextlib.suppress(Exception):
                    await self._runs.finish_run(
                        run_id,
                        state=state,
                        stats=ctx.stats,
                        errors=ctx.errors,
                        log_tail=ctx.log_tail,
                    )
            return state

    async def _run_mapping_incremental(self, ctx: RunContext, batch: tuple[set[Path], set[Path]]) -> None:
        changed, deleted = batch
        pipeline_obj = self._mapping_factory()
        failed_changed, failed_deleted = await asyncio.to_thread(
            pipeline_obj.run_incremental,
            ctx,
            changed=changed,
            deleted=deleted,
        )
        if failed_changed or failed_deleted:
            requeued = len(failed_changed) + len(failed_deleted)
            ctx.warning('incremental mapping failed for %d paths, requeueing', requeued)
            self._changed_paths |= failed_changed
            self._deleted_paths |= failed_deleted
            self._last_fs_event = time.monotonic()

    # -- update loop (rss + archive) ---------------------------------------------

    async def _update_loop(self) -> None:
        while not self._stop.is_set():
            interval = max(60, self._store.get(RssConfig).interval_seconds)
            started = time.monotonic()
            self._next_update_at = None
            if self._pipeline_enabled(PipelineName.RSS) and self._ready[PipelineName.RSS]() is None:
                await self._execute(PipelineName.RSS, RunTrigger.SCHEDULED)
            if self._stop.is_set():
                break
            if self._pipeline_enabled(PipelineName.ARCHIVE) and self._ready[PipelineName.ARCHIVE]() is None:
                await self._execute(PipelineName.ARCHIVE, RunTrigger.SCHEDULED)
            elapsed = time.monotonic() - started
            delay = max(0.0, interval - elapsed)
            self._next_update_at = datetime.now(UTC).replace(microsecond=0) + _seconds(delay)
            if await self._wait_stop(delay):
                break

    async def _config_refresh_loop(self) -> None:
        """Converge on config changes written by other replicas."""
        while not await self._wait_stop(CONFIG_REFRESH_SECONDS):
            try:
                await self._store.refresh()
            except Exception:
                LOGGER.exception('config refresh failed')

    # -- mapping loop -----------------------------------------------------------

    async def _mapping_loop(self) -> None:  # noqa: C901
        # Startup full sync retries until it succeeds or mapping is disabled.
        while not self._stop.is_set():
            self._sync_observer()
            if not self._mapping_runnable():
                if await self._wait_stop(STARTUP_RETRY_SECONDS):
                    return
                continue
            state = await self._execute(PipelineName.MAPPING, RunTrigger.STARTUP)
            if state is RunState.COMPLETED:
                break
            if await self._wait_stop(FULL_SYNC_RETRY_SECONDS):
                return

        self._schedule_next_full_sync()
        while not self._stop.is_set():
            self._sync_observer()
            if not self._mapping_runnable():
                self._next_full_sync_at = None
                if await self._wait_stop(STARTUP_RETRY_SECONDS):
                    return
                self._schedule_next_full_sync()
                continue

            now = datetime.now(UTC)
            if self._next_full_sync_at is not None and now >= self._next_full_sync_at:
                events_before = self._fs_event_count
                state = await self._execute(PipelineName.MAPPING, RunTrigger.SCHEDULED)
                if state is RunState.COMPLETED and self._fs_event_count == events_before:
                    # Nothing changed during the sync: pending events are covered by it.
                    self._changed_paths.clear()
                    self._deleted_paths.clear()
                self._schedule_next_full_sync(retry=state is not RunState.COMPLETED)
                continue

            batch = self._take_debounced_batch()
            if batch is not None:
                await self._execute(PipelineName.MAPPING, RunTrigger.WATCHDOG, incremental_batch=batch)
                continue

            if await self._wait_stop(IDLE_POLL_SECONDS):
                return

    def _take_debounced_batch(self) -> tuple[set[Path], set[Path]] | None:
        if not self._changed_paths and not self._deleted_paths:
            return None
        debounce = self._store.get(MappingConfig).debounce_seconds
        if time.monotonic() - self._last_fs_event < debounce:
            return None
        changed, self._changed_paths = self._changed_paths, set()
        deleted, self._deleted_paths = self._deleted_paths, set()
        return changed, deleted

    def _schedule_next_full_sync(self, *, retry: bool = False) -> None:
        configured = float(self._store.get(MappingConfig).full_sync_interval_seconds)
        interval = FULL_SYNC_RETRY_SECONDS if retry else configured
        self._next_full_sync_at = datetime.now(UTC).replace(microsecond=0) + _seconds(interval)

    # -- watchdog ---------------------------------------------------------------

    def _on_fs_event(self, path: str, deleted: bool) -> None:  # noqa: FBT001
        path_obj = Path(path)
        if deleted:
            self._changed_paths.discard(path_obj)
            self._deleted_paths.add(path_obj)
        else:
            self._deleted_paths.discard(path_obj)
            self._changed_paths.add(path_obj)
        self._fs_event_count += 1
        self._last_fs_event = time.monotonic()

    def _sync_observer(self) -> None:
        """Start, restart, or stop the watchdog observer to match configuration."""
        config = self._store.get(MappingConfig)
        wanted = config.src_dir if (config.enabled and config.configured and Path(config.src_dir).is_dir()) else None
        if wanted == self._observed_dir:
            return
        self._stop_observer()
        if wanted is None:
            return
        handler = _StrmEventHandler(asyncio.get_running_loop(), self._on_fs_event)
        observer = Observer()
        try:
            observer.schedule(handler, wanted, recursive=True)
            observer.start()
        except OSError:
            LOGGER.exception('failed to watch %s', wanted)
            return
        self._observer = observer
        self._observed_dir = wanted
        LOGGER.info('watching %s for .strm changes', wanted)

    def _stop_observer(self) -> None:
        if self._observer is not None:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None
            self._observed_dir = None

    # -- helpers ------------------------------------------------------------------

    def _pipeline_enabled(self, pipeline: PipelineName) -> bool:
        if pipeline is PipelineName.RSS:
            return self._store.get(RssConfig).enabled
        if pipeline is PipelineName.ARCHIVE:
            return self._store.get(ArchiveConfig).enabled
        return self._store.get(MappingConfig).enabled

    def _mapping_runnable(self) -> bool:
        return self._pipeline_enabled(PipelineName.MAPPING) and self._ready[PipelineName.MAPPING]() is None

    async def _wait_stop(self, seconds: float) -> bool:
        if seconds <= 0:
            return self._stop.is_set()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), seconds)
        return self._stop.is_set()


def _seconds(value: float) -> timedelta:
    return timedelta(seconds=value)
