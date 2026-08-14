from embyx_manager.monitor.reports import PipelineName, RunState, RunTrigger
from embyx_manager.monitor.runs import PipelineRunRepository
from tests.conftest import make_database, postgres_test_dsn


def make_runs() -> PipelineRunRepository:
    postgres_test_dsn()
    return PipelineRunRepository(make_database())


async def test_run_lifecycle_round_trip() -> None:
    runs = make_runs()

    run_id = await runs.start_run(PipelineName.RSS, RunTrigger.SCHEDULED)
    started = await runs.get_run(run_id)
    assert started is not None
    assert started.state is RunState.RUNNING
    assert started.finished_at is None

    await runs.finish_run(
        run_id,
        state=RunState.COMPLETED,
        stats={'items': 3, 'magnets_found': 2},
        errors=(),
        log_tail=('line-1', 'line-2'),
    )

    finished = await runs.get_run(run_id)
    assert finished is not None
    assert finished.state is RunState.COMPLETED
    assert finished.stats == {'items': 3, 'magnets_found': 2}
    assert finished.log_tail == ('line-1', 'line-2')
    assert finished.finished_at is not None


async def test_list_and_latest_runs_are_ordered() -> None:
    runs = make_runs()
    first = await runs.start_run(PipelineName.MAPPING, RunTrigger.STARTUP)
    await runs.finish_run(first, state=RunState.COMPLETED, stats={}, errors=(), log_tail=())
    second = await runs.start_run(PipelineName.MAPPING, RunTrigger.WATCHDOG)
    other = await runs.start_run(PipelineName.RSS, RunTrigger.SCHEDULED)

    mapping_runs = await runs.list_runs(PipelineName.MAPPING)
    assert [run.run_id for run in mapping_runs] == [second, first]

    latest = await runs.latest_run(PipelineName.MAPPING)
    assert latest is not None
    assert latest.run_id == second

    everything = await runs.list_runs()
    assert {run.run_id for run in everything} == {first, second, other}


async def test_fail_stale_running_marks_orphans() -> None:
    runs = make_runs()
    orphan = await runs.start_run(PipelineName.ARCHIVE, RunTrigger.SCHEDULED)

    count = await runs.fail_stale_running(error='process restarted')

    assert count == 1
    record = await runs.get_run(orphan)
    assert record is not None
    assert record.state is RunState.FAILED
    assert record.errors == ('process restarted',)


async def test_prune_keeps_recent_runs_per_pipeline() -> None:
    runs = make_runs()
    kept: list[str] = []
    for _ in range(4):
        run_id = await runs.start_run(PipelineName.RSS, RunTrigger.SCHEDULED)
        await runs.finish_run(run_id, state=RunState.COMPLETED, stats={}, errors=(), log_tail=())
        kept.append(run_id)

    deleted = await runs.prune(keep_per_pipeline=2)

    assert deleted == 2
    remaining = await runs.list_runs(PipelineName.RSS, limit=10)
    assert [run.run_id for run in remaining] == [kept[3], kept[2]]
