"""The playlist table on PostgreSQL (CI only): upsert semantics, flags, sync state."""

from datetime import UTC, datetime, timedelta

from embyx_manager.clients.jinjier import RankedEntry, RankedList
from embyx_manager.monitor.acquisitions import AcquisitionRepository, AcquisitionState
from embyx_manager.monitor.playlists import PlaylistRepository
from tests.conftest import make_database, postgres_test_dsn

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
LATER = NOW + timedelta(hours=1)


def ranked(key: str, kind: int, note: str, avids: list[str]) -> RankedList:
    return RankedList(
        key=key,
        kind=kind,
        note=note,
        entries=tuple(RankedEntry(rank=i + 1, avid=avid, title=f'title {avid}') for i, avid in enumerate(avids)),
    )


async def test_replace_lists_inserts_updates_and_marks_the_departed() -> None:
    postgres_test_dsn()
    database = make_database()
    repository = PlaylistRepository(database)

    await repository.replace_lists(
        [ranked('k7', 7, '有码', ['A-001', 'B-002']), ranked('k5', 5, 'JL', ['C-003'])], now=NOW
    )
    await repository.set_enabled('k7', enabled=False, now=NOW)
    await repository.replace_lists([ranked('k7', 7, '有码 v2', ['B-002', 'A-001'])], now=LATER)

    records = {record.key: record for record in await repository.list()}
    assert [entry.avid for entry in records['k7'].entries] == ['B-002', 'A-001']
    assert records['k7'].name == '有码 v2'
    assert records['k7'].enabled is False
    assert records['k7'].last_error is None
    assert records['k7'].created_at == NOW
    assert records['k7'].updated_at == LATER
    assert records['k5'].last_error == 'the source no longer publishes this list'
    assert [entry.avid for entry in records['k5'].entries] == ['C-003']
    assert records['k5'].enabled is True


async def test_sync_state_and_source_round_trip() -> None:
    postgres_test_dsn()
    repository = PlaylistRepository(make_database())
    await repository.replace_lists([ranked('k2024', 2024, '2024', ['A-001', 'B-002', 'C-003'])], now=NOW)

    assert await repository.source() is None
    await repository.record_source('20260112', now=NOW)
    await repository.record_source('20260201', now=LATER)
    source = await repository.source()
    assert (source.database_name, source.fetched_at) == ('20260201', LATER)

    await repository.record_error('k2024', now=NOW, error='boom')
    assert (await repository.get('k2024')).last_error == 'boom'

    await repository.record_sync(
        'k2024', now=LATER, present=['A-001', 'C-003'], missing=['B-002'], emby_playlist_id='pl9'
    )
    record = await repository.get('k2024')
    assert record.present == ('A-001', 'C-003')
    assert record.missing == ('B-002',)
    assert record.emby_playlist_id == 'pl9'
    assert record.last_synced_at == LATER
    assert record.last_error is None

    assert await repository.get('missing') is None
    assert await repository.set_enabled('missing', enabled=True, now=NOW) is None


async def test_ledger_states_for_only_reports_tracked_avids() -> None:
    postgres_test_dsn()
    database = make_database()
    ledger = AcquisitionRepository(database)
    await ledger.discover('A-001', source='manual', now=NOW, task_dir_path='/115/x')

    assert await ledger.states_for(['A-001', 'B-002']) == {'A-001': AcquisitionState.DISCOVERED}
    assert await ledger.states_for([]) == {}
