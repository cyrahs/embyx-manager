import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from embyx_manager.config.models import SECTION_MODELS, ArchiveConfig, MappingConfig, RssConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor import scheduler as scheduler_module
from embyx_manager.monitor.mapping import MappingPipeline
from embyx_manager.monitor.reports import PipelineName, RunContext, RunState, RunTrigger
from embyx_manager.monitor.runs import PipelineRunRecord
from embyx_manager.monitor.scheduler import (
    MonitorScheduler,
    PipelineBusyError,
    PipelineNotConfiguredError,
    _next_cron_fire,
)


class FakeStore:
    def __init__(self, **sections) -> None:
        self._sections = {name: model() for name, model in SECTION_MODELS.items()}
        self._versions = dict.fromkeys(SECTION_MODELS, 0)
        for name, value in sections.items():
            self.put(name, value)

    def put(self, name, value) -> None:
        self._sections[name] = value
        self._versions[name] += 1

    def get(self, section_type):
        for name, model in SECTION_MODELS.items():
            if model is section_type:
                return self._sections[name]
        raise KeyError(section_type)

    def get_with_version(self, name):
        return self._sections[name], self._versions[name]

    async def refresh(self) -> None:
        return None


class FakeRuns:
    def __init__(self) -> None:
        self.records: dict[str, PipelineRunRecord] = {}
        self.order: list[str] = []

    async def start_run(self, pipeline: PipelineName, trigger: RunTrigger) -> str:
        run_id = uuid.uuid4().hex
        self.records[run_id] = PipelineRunRecord(
            run_id=run_id,
            pipeline=pipeline,
            trigger=trigger,
            state=RunState.RUNNING,
            started_at=datetime.now(UTC),
            finished_at=None,
            stats={},
            errors=(),
            log_tail=(),
        )
        self.order.append(run_id)
        return run_id

    async def finish_run(self, run_id: str, *, state, stats, errors, log_tail) -> None:
        record = self.records[run_id]
        self.records[run_id] = PipelineRunRecord(
            run_id=run_id,
            pipeline=record.pipeline,
            trigger=record.trigger,
            state=state,
            started_at=record.started_at,
            finished_at=datetime.now(UTC),
            stats=dict(stats),
            errors=tuple(errors),
            log_tail=tuple(log_tail),
        )

    async def fail_stale_running(self, *, error: str) -> int:
        del error
        return 0

    def finished(self, pipeline: PipelineName) -> list[PipelineRunRecord]:
        return [
            self.records[run_id]
            for run_id in self.order
            if self.records[run_id].pipeline is pipeline and self.records[run_id].state is not RunState.RUNNING
        ]


def ready() -> str | None:
    return None


def make_scheduler(
    store: FakeStore,
    runs: FakeRuns,
    *,
    rss_runner=None,
    archive_runner=None,
    mapping_factory=None,
    rss_ready=ready,
    archive_ready=ready,
    mapping_ready=ready,
) -> MonitorScheduler:
    async def default_rss(ctx: RunContext, rank: bool) -> None:  # noqa: ARG001, FBT001
        ctx.add('rss_runs')

    async def default_archive(ctx: RunContext) -> None:
        ctx.add('archive_runs')

    return MonitorScheduler(
        store=store,  # type: ignore[arg-type]
        runs=runs,  # type: ignore[arg-type]
        rss_runner=rss_runner or default_rss,
        archive_runner=archive_runner or default_archive,
        mapping_factory=mapping_factory or (lambda: None),
        rss_ready=rss_ready,
        archive_ready=archive_ready,
        mapping_ready=mapping_ready,
    )


async def test_manual_trigger_persists_completed_run() -> None:
    store = FakeStore()
    runs = FakeRuns()
    scheduler = make_scheduler(store, runs)

    run_id = await scheduler.trigger(PipelineName.RSS)
    await asyncio.sleep(0.05)

    record = runs.records[run_id]
    assert record.state is RunState.COMPLETED
    assert record.trigger is RunTrigger.MANUAL
    assert record.stats == {'rss_runs': 1}
    await scheduler.aclose()


async def test_trigger_rejects_concurrent_run() -> None:
    store = FakeStore()
    runs = FakeRuns()
    release = asyncio.Event()

    async def blocking_rss(ctx: RunContext, rank: bool) -> None:  # noqa: ARG001, FBT001
        await release.wait()

    scheduler = make_scheduler(store, runs, rss_runner=blocking_rss)
    await scheduler.trigger(PipelineName.RSS)
    await asyncio.sleep(0.01)

    with pytest.raises(PipelineBusyError):
        await scheduler.trigger(PipelineName.RSS)

    release.set()
    await asyncio.sleep(0.05)
    await scheduler.aclose()


async def test_trigger_rejects_unconfigured_pipeline() -> None:
    scheduler = make_scheduler(FakeStore(), FakeRuns(), rss_ready=lambda: 'FreshRSS is not configured')

    with pytest.raises(PipelineNotConfiguredError):
        await scheduler.trigger(PipelineName.RSS)
    await scheduler.aclose()


async def test_failed_run_is_recorded_with_errors() -> None:
    store = FakeStore()
    runs = FakeRuns()

    async def broken_rss(ctx: RunContext, rank: bool) -> None:  # noqa: ARG001, FBT001
        msg = 'feed exploded'
        raise RuntimeError(msg)

    scheduler = make_scheduler(store, runs, rss_runner=broken_rss)
    run_id = await scheduler.trigger(PipelineName.RSS)
    await asyncio.sleep(0.05)

    record = runs.records[run_id]
    assert record.state is RunState.FAILED
    assert any('rss run failed' in error for error in record.errors)
    await scheduler.aclose()


async def test_cancel_running_marks_run_cancelled() -> None:
    store = FakeStore()
    runs = FakeRuns()
    started = asyncio.Event()

    async def cancellable_rss(ctx: RunContext, rank: bool) -> None:  # noqa: ARG001, FBT001
        started.set()
        while True:
            ctx.check_cancelled()
            await asyncio.sleep(0.01)

    scheduler = make_scheduler(store, runs, rss_runner=cancellable_rss)
    run_id = await scheduler.trigger(PipelineName.RSS)
    await asyncio.wait_for(started.wait(), 1)

    assert await scheduler.cancel_running(PipelineName.RSS) is True
    await asyncio.sleep(0.1)

    assert runs.records[run_id].state is RunState.CANCELLED
    assert await scheduler.cancel_running(PipelineName.RSS) is False
    await scheduler.aclose()


async def test_rss_loop_runs_on_its_interval_and_archive_waits_for_its_cron() -> None:
    store = FakeStore(
        rss=RssConfig(enabled=True, interval_seconds=3600),
        archive=ArchiveConfig(enabled=True, src_dir='/x', dst_dir='/y', mapping={'a': 'b'}),
    )
    runs = FakeRuns()
    scheduler = make_scheduler(store, runs)

    await scheduler.start()
    for _ in range(100):
        if runs.finished(PipelineName.RSS):
            break
        await asyncio.sleep(0.02)
    await scheduler.aclose()

    rss_runs = runs.finished(PipelineName.RSS)
    assert len(rss_runs) == 1
    assert rss_runs[0].trigger is RunTrigger.SCHEDULED
    # The archive scan no longer piggybacks on the rss interval.
    assert runs.finished(PipelineName.ARCHIVE) == []


async def test_archive_scan_fires_when_its_cron_time_arrives(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scheduler_module, 'CRON_POLL_SECONDS', 0.01)
    store = FakeStore(archive=ArchiveConfig(enabled=True, src_dir='/x', dst_dir='/y', mapping={'a': 'b'}))
    runs = FakeRuns()
    scheduler = make_scheduler(store, runs)

    await scheduler.start()
    for _ in range(100):
        if scheduler._next_archive_at is not None:  # noqa: SLF001
            break
        await asyncio.sleep(0.01)
    # Pull the next fire into the past instead of waiting out a real minute.
    scheduler._next_archive_at = datetime.now(UTC) - timedelta(seconds=1)  # noqa: SLF001
    for _ in range(200):
        if runs.finished(PipelineName.ARCHIVE):
            break
        await asyncio.sleep(0.02)
    next_after_fire = scheduler._next_archive_at  # noqa: SLF001
    await scheduler.aclose()

    archive_runs = runs.finished(PipelineName.ARCHIVE)
    assert len(archive_runs) == 1
    assert archive_runs[0].trigger is RunTrigger.SCHEDULED
    assert next_after_fire is not None
    assert next_after_fire > datetime.now(UTC)


def test_next_cron_fire_returns_the_next_local_occurrence_in_utc() -> None:
    base = datetime(2026, 8, 14, 3, 59, 30, tzinfo=UTC)
    fire = _next_cron_fire('0 4 * * *', now=base)

    assert fire is not None
    assert fire.tzinfo is UTC
    assert fire > base
    local = fire.astimezone()
    assert (local.hour, local.minute) == (4, 0)


def test_next_cron_fire_schedules_nothing_for_an_unparsable_expression() -> None:
    assert _next_cron_fire('every day at four') is None


async def test_update_loop_skips_disabled_pipelines() -> None:
    store = FakeStore(rss=RssConfig(enabled=False, interval_seconds=3600))
    runs = FakeRuns()
    scheduler = make_scheduler(store, runs)

    await scheduler.start()
    await asyncio.sleep(0.2)
    await scheduler.aclose()

    assert runs.finished(PipelineName.RSS) == []


async def test_mapping_loop_runs_startup_full_sync_and_watchdog_batch(tmp_path: Path) -> None:
    src = tmp_path / 'flat'
    src.mkdir()
    mapping_config = MappingConfig(
        enabled=True,
        src_dir=str(src),
        dst_dir=str(tmp_path / 'emby'),
        debounce_seconds=0.01,
        full_sync_interval_seconds=86_400,
    )
    store = FakeStore(mapping=mapping_config)
    runs = FakeRuns()

    def mapping_factory() -> MappingPipeline:
        return MappingPipeline(config=mapping_config, avid_parser=AvidParser())

    scheduler = make_scheduler(store, runs, mapping_factory=mapping_factory)
    await scheduler.start()

    for _ in range(100):
        if runs.finished(PipelineName.MAPPING):
            break
        await asyncio.sleep(0.02)
    startup_runs = runs.finished(PipelineName.MAPPING)
    assert len(startup_runs) == 1
    assert startup_runs[0].trigger is RunTrigger.STARTUP

    strm = src / 'ABC-123.strm'
    strm.write_text('/cloud/abc.mp4', encoding='utf-8')
    scheduler._on_fs_event(str(strm), False)  # noqa: FBT003, SLF001

    for _ in range(200):
        if len(runs.finished(PipelineName.MAPPING)) >= 2:
            break
        await asyncio.sleep(0.02)
    await scheduler.aclose()

    watchdog_runs = [run for run in runs.finished(PipelineName.MAPPING) if run.trigger is RunTrigger.WATCHDOG]
    assert len(watchdog_runs) == 1
    assert watchdog_runs[0].stats.get('files_updated') == 1
    assert (tmp_path / 'emby' / 'ABC-123' / 'ABC-123.strm').exists()
