"""The playlists pipeline: source refresh, order-preserving sync, retirement, failure isolation."""

import logging
from datetime import UTC, datetime

import pytest

from embyx_manager.clients.emby import EmbyError, EmbyPlaylist, EmbyPlaylistEntry
from embyx_manager.clients.jinjier import JinjierError, RankedEntry
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.playlist_sync import PlaylistSyncPipeline
from embyx_manager.monitor.playlists import PlaylistRecord, PlaylistSourceState
from embyx_manager.monitor.reports import RunContext
from tests.test_jinjier import make_database

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)


class FakeRepository:
    """The playlist table in memory, with the same write surface as the real one."""

    def __init__(self, records: list[PlaylistRecord] | None = None, source: PlaylistSourceState | None = None) -> None:
        self.records: dict[str, PlaylistRecord] = {r.key: r for r in records or []}
        self.stored_source = source
        self.replaced: list[list[str]] = []

    async def list(self) -> tuple[PlaylistRecord, ...]:
        return tuple(sorted(self.records.values(), key=lambda r: (r.kind, r.note, r.key)))

    async def get(self, key: str) -> PlaylistRecord | None:
        return self.records.get(key)

    async def source(self) -> PlaylistSourceState | None:
        return self.stored_source

    async def record_source(self, database_name: str, *, now: datetime) -> None:
        self.stored_source = PlaylistSourceState(database_name=database_name, fetched_at=now)

    async def replace_lists(self, lists, *, now: datetime) -> None:
        self.replaced.append([ranked.key for ranked in lists])
        for ranked in lists:
            current = self.records.get(ranked.key)
            self.records[ranked.key] = PlaylistRecord(
                key=ranked.key,
                kind=ranked.kind,
                note=ranked.note,
                name=ranked.name,
                enabled=current.enabled if current else True,
                entries=ranked.entries,
                present=current.present if current else (),
                missing=current.missing if current else (),
                emby_playlist_id=current.emby_playlist_id if current else None,
                last_synced_at=current.last_synced_at if current else None,
                last_error=None,
                created_at=current.created_at if current else now,
                updated_at=now,
            )

    async def record_sync(self, key: str, *, now: datetime, present, missing, emby_playlist_id) -> None:
        record = self.records[key]
        self.records[key] = PlaylistRecord(
            **{
                **record.__dict__,
                'present': tuple(present),
                'missing': tuple(missing),
                'emby_playlist_id': emby_playlist_id,
                'last_synced_at': now,
                'last_error': None,
                'updated_at': now,
            },
        )

    async def set_enabled(self, key: str, *, enabled: bool, now: datetime) -> PlaylistRecord | None:
        record = self.records.get(key)
        if record is None:
            return None
        self.records[key] = PlaylistRecord(**{**record.__dict__, 'enabled': enabled, 'updated_at': now})
        return self.records[key]

    async def record_error(self, key: str, *, now: datetime, error: str) -> None:
        record = self.records[key]
        self.records[key] = PlaylistRecord(**{**record.__dict__, 'last_error': error, 'updated_at': now})


class FakeSource:
    def __init__(self, name: str | None = '20260112', database: bytes | None = None) -> None:
        self.name = name
        self.database = database if database is not None else make_database()
        self.downloads = 0
        self.broken_download = False

    async def discover_database_name(self) -> str:
        if self.name is None:
            msg = 'page changed'
            raise JinjierError(msg)
        return self.name

    async def download_database(self, name: str) -> bytes:
        self.downloads += 1
        if self.broken_download:
            msg = f'{name}: HTTP 503'
            raise JinjierError(msg)
        return self.database


class FakeEmby:
    """Movie index plus playlists that keep insertion order, like the server."""

    def __init__(self, index: dict[str, str]) -> None:
        self.index = index
        self.playlists: dict[str, list[tuple[str, str]]] = {}
        self.names: dict[str, str] = {}
        self.calls: list[tuple[str, ...]] = []
        self.fail_on: set[str] = set()
        self._next = 0

    async def movie_index(self, avid_of) -> dict[str, str]:  # noqa: ARG002
        return dict(self.index)

    async def list_playlists(self) -> tuple[EmbyPlaylist, ...]:
        return tuple(EmbyPlaylist(playlist_id=pid, name=self.names[pid]) for pid in self.playlists)

    async def create_playlist(self, name: str, item_ids) -> str:
        if name in self.fail_on:
            msg = f'POST /Playlists: HTTP 500 for {name}'
            raise EmbyError(msg)
        self._next += 1
        pid = f'pl{self._next}'
        self.names[pid] = name
        self.playlists[pid] = [(self._entry(), item) for item in item_ids]
        self.calls.append(('create', name, tuple(item_ids)))
        return pid

    async def playlist_entries(self, playlist_id: str) -> tuple[EmbyPlaylistEntry, ...]:
        return tuple(EmbyPlaylistEntry(entry_id=e, item_id=i) for e, i in self.playlists[playlist_id])

    async def add_entries(self, playlist_id: str, item_ids) -> None:
        self.playlists[playlist_id].extend((self._entry(), item) for item in item_ids)
        self.calls.append(('add', playlist_id, tuple(item_ids)))

    async def remove_entries(self, playlist_id: str, entry_ids) -> None:
        gone = set(entry_ids)
        self.playlists[playlist_id] = [(e, i) for e, i in self.playlists[playlist_id] if e not in gone]
        self.calls.append(('remove', playlist_id, tuple(entry_ids)))

    async def delete_playlist(self, playlist_id: str) -> None:
        self.playlists.pop(playlist_id)
        self.names.pop(playlist_id)
        self.calls.append(('delete', playlist_id))

    def items(self, playlist_id: str) -> list[str]:
        return [item for _, item in self.playlists[playlist_id]]

    def _entry(self) -> str:
        self._next += 1
        return f'e{self._next}'


def record(
    key: str,
    avids: list[str],
    *,
    enabled: bool = True,
    emby_playlist_id: str | None = None,
    kind: int = 7,
    name: str = 'JavDB 有码 TOP250',
) -> PlaylistRecord:
    return PlaylistRecord(
        key=key,
        kind=kind,
        note=name,
        name=name,
        enabled=enabled,
        entries=tuple(RankedEntry(rank=i + 1, avid=avid, title=avid.lower()) for i, avid in enumerate(avids)),
        present=(),
        missing=(),
        emby_playlist_id=emby_playlist_id,
        last_synced_at=None,
        last_error=None,
        created_at=NOW,
        updated_at=NOW,
    )


def make_pipeline(repository: FakeRepository, source: FakeSource, emby: FakeEmby) -> PlaylistSyncPipeline:
    return PlaylistSyncPipeline(
        repository=repository,  # type: ignore[arg-type]
        source=source,  # type: ignore[arg-type]
        emby=emby,  # type: ignore[arg-type]
        avid_of=AvidParser().get_avid,
        now=lambda: NOW,
    )


def ctx() -> RunContext:
    return RunContext(logger=logging.getLogger('test-playlists'))


# -- source refresh -------------------------------------------------------------


async def test_first_run_downloads_and_stores_the_lists_then_creates_playlists() -> None:
    repository = FakeRepository()
    source = FakeSource()
    emby = FakeEmby({'SSNI-497': '10', 'ABP-984': '30', 'MIAB-317': '40', 'MOON-018': '50'})
    run = ctx()

    await make_pipeline(repository, source, emby).run(run)

    assert source.downloads == 1
    assert repository.stored_source == PlaylistSourceState(database_name='20260112', fetched_at=NOW)
    assert set(repository.records) == {
        'k5',
        'k7',
        'k2024',
        'k4:第四届JAV金鸡儿奖 最佳故事片 提名',
        'k4:第四届JAV金鸡儿奖 最佳故事片 获奖',
    }
    top = repository.records['k7']
    # IPX-811 sits between the two present titles in the ranking; the playlist keeps their order.
    assert top.present == ('SSNI-497', 'ABP-984')
    assert top.missing == ('IPX-811',)
    assert emby.items(top.emby_playlist_id) == ['10', '30']
    assert emby.names[top.emby_playlist_id] == 'JavDB 有码 TOP250'
    assert top.last_synced_at == NOW
    # JavLibrary's two titles are both absent: counted, but no empty playlist.
    assert repository.records['k5'].missing == ('START-257', 'SHKD-724')
    assert repository.records['k5'].emby_playlist_id is None
    assert run.stats['source_updated'] == 1
    assert run.stats['lists_created'] == 3
    assert run.stats['lists_synced'] == 5
    assert run.stats['missing_total'] == 1 + 2 + 2 + 1 + 1


async def test_same_database_name_skips_the_download() -> None:
    repository = FakeRepository([record('k7', ['SSNI-497'])], PlaylistSourceState('20260112', NOW))
    source = FakeSource()
    emby = FakeEmby({'SSNI-497': '10'})
    run = ctx()

    await make_pipeline(repository, source, emby).run(run)

    assert source.downloads == 0
    assert 'source_updated' not in run.stats
    assert emby.items(repository.records['k7'].emby_playlist_id) == ['10']


async def test_new_database_name_replaces_entries_but_keeps_the_operators_flag() -> None:
    old = record('k7', ['OLD-001'], enabled=False, emby_playlist_id=None)
    repository = FakeRepository([old], PlaylistSourceState('20250101', NOW))
    source = FakeSource(name='20260112')
    emby = FakeEmby({'SSNI-497': '10'})

    await make_pipeline(repository, source, emby).run(ctx())

    assert source.downloads == 1
    assert repository.records['k7'].enabled is False
    assert [e.avid for e in repository.records['k7'].entries] == ['SSNI-497', 'IPX-811', 'ABP-984']
    # Disabled: statistics kept current, no playlist made.
    assert repository.records['k7'].present == ('SSNI-497',)
    assert emby.playlists == {}


async def test_source_outage_keeps_syncing_the_stored_lists() -> None:
    repository = FakeRepository([record('k7', ['SSNI-497'])], PlaylistSourceState('20260112', NOW))
    emby = FakeEmby({'SSNI-497': '10'})
    run = ctx()

    await make_pipeline(repository, FakeSource(name=None), emby).run(run)

    assert any('ranking source unavailable' in line for line in run.log_tail)
    assert run.errors == ()
    assert emby.items(repository.records['k7'].emby_playlist_id) == ['10']

    broken = FakeSource(name='20270101')
    broken.broken_download = True
    run = ctx()
    await make_pipeline(repository, broken, emby).run(run)
    assert any('could not be read' in line for line in run.log_tail)
    assert repository.stored_source.database_name == '20260112'
    assert run.stats['lists_unchanged'] == 1


async def test_nothing_stored_and_no_source_is_a_failed_run() -> None:
    with pytest.raises(RuntimeError, match='no ranking lists are stored'):
        await make_pipeline(FakeRepository(), FakeSource(name=None), FakeEmby({})).run(ctx())


# -- syncing one list --------------------------------------------------------------


async def test_playlist_in_the_right_order_is_left_alone() -> None:
    emby = FakeEmby({'A-001': '1', 'B-002': '2'})
    pid = await emby.create_playlist('JavDB 有码 TOP250', ['1', '2'])
    emby.calls.clear()
    repository = FakeRepository([record('k7', ['A-001', 'B-002'], emby_playlist_id=pid)], PlaylistSourceState('x', NOW))
    run = ctx()

    await make_pipeline(repository, FakeSource(name='x'), emby).run(run)

    assert emby.calls == []
    assert run.stats['lists_unchanged'] == 1


async def test_changed_order_or_membership_rebuilds_the_playlist_in_one_pass() -> None:
    emby = FakeEmby({'A-001': '1', 'B-002': '2', 'C-003': '3'})
    pid = await emby.create_playlist('JavDB 有码 TOP250', ['2', '1'])
    old_entries = [e for e, _ in emby.playlists[pid]]
    emby.calls.clear()
    repository = FakeRepository(
        [record('k7', ['A-001', 'B-002', 'C-003'], emby_playlist_id=pid)],
        PlaylistSourceState('x', NOW),
    )
    run = ctx()

    await make_pipeline(repository, FakeSource(name='x'), emby).run(run)

    assert emby.calls == [('remove', pid, tuple(old_entries)), ('add', pid, ('1', '2', '3'))]
    assert emby.items(pid) == ['1', '2', '3']
    assert run.stats['lists_rebuilt'] == 1


async def test_playlist_deleted_on_the_server_is_recreated() -> None:
    emby = FakeEmby({'A-001': '1'})
    repository = FakeRepository([record('k7', ['A-001'], emby_playlist_id='gone')], PlaylistSourceState('x', NOW))
    run = ctx()

    await make_pipeline(repository, FakeSource(name='x'), emby).run(run)

    new_id = repository.records['k7'].emby_playlist_id
    assert new_id != 'gone'
    assert emby.items(new_id) == ['1']
    assert run.stats['lists_created'] == 1


async def test_disabled_list_has_its_playlist_removed_and_gap_still_counted() -> None:
    emby = FakeEmby({'A-001': '1'})
    pid = await emby.create_playlist('JavDB 有码 TOP250', ['1'])
    repository = FakeRepository(
        [record('k7', ['A-001', 'B-002'], enabled=False, emby_playlist_id=pid)],
        PlaylistSourceState('x', NOW),
    )
    run = ctx()

    await make_pipeline(repository, FakeSource(name='x'), emby).run(run)

    assert emby.playlists == {}
    assert repository.records['k7'].emby_playlist_id is None
    assert repository.records['k7'].present == ('A-001',)
    assert repository.records['k7'].missing == ('B-002',)
    assert run.stats['lists_removed'] == 1
    assert 'lists_synced' not in run.stats

    # Already gone (or never made): nothing to delete, no error.
    run = ctx()
    await make_pipeline(repository, FakeSource(name='x'), emby).run(run)
    assert 'lists_removed' not in run.stats


async def test_one_list_failing_on_emby_is_recorded_and_the_rest_continue() -> None:
    emby = FakeEmby({'A-001': '1', 'B-002': '2'})
    emby.fail_on.add('Broken')
    repository = FakeRepository(
        [
            record('k7', ['A-001'], kind=7, name='Broken'),
            record('k2024', ['B-002'], kind=2024, name='JavDB 2024 TOP250'),
        ],
        PlaylistSourceState('x', NOW),
    )
    run = ctx()

    await make_pipeline(repository, FakeSource(name='x'), emby).run(run)

    assert repository.records['k7'].last_error == 'POST /Playlists: HTTP 500 for Broken'
    assert repository.records['k7'].emby_playlist_id is None
    assert emby.items(repository.records['k2024'].emby_playlist_id) == ['2']
    assert run.stats == {
        'lists': 2,
        'library_titles': 2,
        'lists_failed': 1,
        'lists_created': 1,
        'lists_synced': 1,
        'missing_total': 0,
    }
    assert len(run.errors) == 1
