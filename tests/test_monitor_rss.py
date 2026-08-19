import dataclasses
import logging
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import grpc
import pytest

from embyx_manager.config.models import ArchiveConfig, RssCategory, RssConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.acquisitions import (
    AcquisitionRecord,
    AcquisitionState,
    AttemptState,
    MagnetAttemptRecord,
    MagnetCandidate,
)
from embyx_manager.monitor.archive import ArchivePipeline
from embyx_manager.monitor.reports import RunContext
from embyx_manager.monitor.rss import RssPipeline

HASH_A = 'C12FE1C06BBA254A9DC9F519B335AA7C1367A88A'
HASH_B = 'D23FE1C06BBA254A9DC9F519B335AA7C1367A88B'
HASH_C = 'E34FE1C06BBA254A9DC9F519B335AA7C1367A88C'
MAGNET_A = f'magnet:?xt=urn:btih:{HASH_A}&dn=ABC-123'
MAGNET_B = f'magnet:?xt=urn:btih:{HASH_B}&dn=ABC-123'
TASK_DIR = '/115/task'


def make_ctx() -> RunContext:
    return RunContext(logger=logging.getLogger('test-rss'))


class FakeLedger:
    """In-memory stand-in for AcquisitionRepository.

    The real repository is exercised against PostgreSQL in
    tests/test_monitor_acquisitions.py; here the point is what RSS asks of it.
    """

    def __init__(self, *, known: dict[str, AcquisitionState] | None = None) -> None:
        self.states: dict[str, AcquisitionState] = dict(known or {})
        self.sources: dict[str, str] = {}
        self.notes: dict[str, str | None] = {}
        self.next_action_at: dict[str, datetime | None] = {}
        self.attempts: dict[str, list[MagnetAttemptRecord]] = {}
        self.task_dirs: dict[str, str | None] = {}
        self.archived_paths: dict[str, tuple[str, ...]] = {}

    async def discover(
        self,
        avid: str,
        *,
        source: str,
        now: datetime,
        task_dir_path: str | None = None,
        next_action_at: datetime | None = None,
    ) -> bool:
        if avid not in self.states:
            self.states[avid] = AcquisitionState.DISCOVERED
            self.sources[avid] = source
            self.task_dirs[avid] = task_dir_path
            self.next_action_at[avid] = next_action_at
            return True
        state = self.states[avid]
        if state is AcquisitionState.DISCOVERED:
            accepted = True
        elif state in {AcquisitionState.RESOLVE_FAILED, AcquisitionState.EXHAUSTED}:
            due = self.next_action_at.get(avid)
            accepted = due is None or due <= now
        else:
            return False
        if accepted and task_dir_path is not None:
            self.task_dirs[avid] = task_dir_path
        if accepted and next_action_at is not None:
            self.next_action_at[avid] = next_action_at
        return accepted

    async def get(self, avid: str) -> AcquisitionRecord | None:
        if avid not in self.states:
            return None
        return AcquisitionRecord(
            avid=avid,
            state=self.states[avid],
            source=self.sources.get(avid, 'rss:Actor'),
            note=self.notes.get(avid),
            archived_paths=(),
            next_action_at=self.next_action_at.get(avid),
            created_at=now_stub(),
            updated_at=now_stub(),
            task_dir_path=self.task_dirs.get(avid),
        )

    async def transition(
        self,
        avid: str,
        *,
        expected: AcquisitionState,
        target: AcquisitionState,
        now: datetime,
        note: str | None = None,
        next_action_at: datetime | None = None,
        archived_paths: object = None,
    ) -> bool:
        del now
        if self.states.get(avid) is not expected:
            return False
        self.states[avid] = target
        self.notes[avid] = note
        self.next_action_at[avid] = next_action_at
        if archived_paths is not None:
            self.archived_paths[avid] = tuple(archived_paths)  # type: ignore[arg-type]
        return True

    async def add_attempts(self, avid: str, candidates: list[MagnetCandidate], *, now: datetime) -> int:
        existing = self.attempts.setdefault(avid, [])
        seen = {attempt.info_hash for attempt in existing if attempt.info_hash}
        added = 0
        for candidate in candidates:
            if candidate.info_hash and candidate.info_hash in seen:
                continue
            if candidate.info_hash:
                seen.add(candidate.info_hash)
            existing.append(
                MagnetAttemptRecord(
                    avid=avid,
                    attempt_no=len(existing) + 1,
                    magnet=candidate.magnet,
                    info_hash=candidate.info_hash,
                    magnet_source=candidate.source,
                    size_hint=candidate.size_hint,
                    state=AttemptState.PENDING,
                    progress=None,
                    error=None,
                    submitted_at=None,
                    updated_at=now,
                ),
            )
            added += 1
        return added

    async def claim_next_pending(self, avid: str, *, now: datetime) -> MagnetAttemptRecord | None:
        for index, attempt in enumerate(self.attempts.get(avid, [])):
            if attempt.state is AttemptState.PENDING:
                claimed = _replace(attempt, state=AttemptState.SUBMITTED, submitted_at=now)
                self.attempts[avid][index] = claimed
                return claimed
        return None

    async def transition_attempt(
        self,
        avid: str,
        attempt_no: int,
        *,
        expected: AttemptState,
        target: AttemptState,
        now: datetime,
        error: str | None = None,
    ) -> bool:
        del now
        for index, attempt in enumerate(self.attempts.get(avid, [])):
            if attempt.attempt_no == attempt_no and attempt.state is expected:
                self.attempts[avid][index] = _replace(attempt, state=target, error=error)
                return True
        return False

    async def attempts_by_info_hash(self, info_hashes) -> dict[str, MagnetAttemptRecord]:
        wanted = {h for h in info_hashes if h}
        live = {AttemptState.SUBMITTED, AttemptState.DOWNLOADING, AttemptState.FINISHED, AttemptState.ARCHIVING}
        return {
            attempt.info_hash: attempt
            for attempts in self.attempts.values()
            for attempt in attempts
            if attempt.info_hash in wanted and attempt.state in live
        }

    async def in_flight_attempts(self) -> tuple[MagnetAttemptRecord, ...]:
        return tuple(
            attempt
            for attempts in self.attempts.values()
            for attempt in attempts
            if attempt.state in {AttemptState.SUBMITTED, AttemptState.DOWNLOADING}
        )

    async def attempts_for(self, avid: str) -> tuple[MagnetAttemptRecord, ...]:
        return tuple(self.attempts.get(avid, []))

    async def record_progress(
        self,
        avid: str,
        attempt_no: int,
        *,
        state: AttemptState,
        progress: float | None,
        now: datetime,
    ) -> bool:
        for index, attempt in enumerate(self.attempts.get(avid, [])):
            if attempt.attempt_no != attempt_no:
                continue
            if attempt.state not in {AttemptState.SUBMITTED, AttemptState.DOWNLOADING}:
                return False
            if attempt.state is state and attempt.progress == progress:
                return False
            self.attempts[avid][index] = _replace(attempt, state=state, progress=progress, updated_at=now)
            return True
        return False

    async def active_avids(self) -> frozenset[str]:
        return frozenset(
            avid
            for avid, state in self.states.items()
            if state in {AcquisitionState.DISCOVERED, AcquisitionState.DOWNLOADING}
        )

    async def active_task_dirs(self) -> tuple[str, ...]:
        active = await self.active_avids()
        return tuple(sorted({task_dir for avid, task_dir in self.task_dirs.items() if avid in active and task_dir}))

    async def latest_task_dir(self, *, source: str) -> str | None:
        # Insertion order stands in for created_at: the fake records one sighting per AVID.
        for avid in reversed(list(self.sources)):
            if self.sources[avid] == source and self.task_dirs.get(avid):
                return self.task_dirs[avid]
        return None

    async def due_for_retry(self, *, now: datetime, limit: int = 50) -> tuple[AcquisitionRecord, ...]:
        due = sorted(
            avid
            for avid, state in self.states.items()
            if state in {AcquisitionState.RESOLVE_FAILED, AcquisitionState.EXHAUSTED}
            and self.next_action_at.get(avid) is not None
            and self.next_action_at[avid] <= now  # type: ignore[operator]
        )
        records = [await self.get(avid) for avid in due[:limit]]
        return tuple(record for record in records if record is not None)

    async def claim_attempt(self, avid: str, attempt_no: int, *, now: datetime) -> MagnetAttemptRecord | None:
        for index, attempt in enumerate(self.attempts.get(avid, [])):
            if attempt.attempt_no == attempt_no and attempt.state is AttemptState.PENDING:
                claimed = _replace(attempt, state=AttemptState.SUBMITTED, submitted_at=now)
                self.attempts[avid][index] = claimed
                return claimed
        return None

    async def list_acquisitions(self, *, states=None, limit: int = 50, offset: int = 0):
        wanted = set(states) if states is not None else None
        rows = [avid for avid, state in self.states.items() if wanted is None or state in wanted]
        return tuple([await self.get(avid) for avid in sorted(rows)][offset : offset + limit])

    async def count_by_state(self) -> dict[AcquisitionState, int]:
        counts: dict[AcquisitionState, int] = {}
        for state in self.states.values():
            counts[state] = counts.get(state, 0) + 1
        return counts

    # -- assertions helpers -------------------------------------------------

    def attempt_states(self, avid: str) -> list[AttemptState]:
        return [attempt.state for attempt in self.attempts.get(avid, [])]

    def magnets(self, avid: str) -> list[str]:
        return [attempt.magnet for attempt in self.attempts.get(avid, [])]


def _replace(attempt: MagnetAttemptRecord, **changes: object) -> MagnetAttemptRecord:
    return dataclasses.replace(attempt, **changes)  # type: ignore[arg-type]


def now_stub() -> datetime:
    return datetime(2026, 8, 13, tzinfo=UTC)


def make_item(item_id: str, title: str, magnet_html: str = '') -> dict:
    return {'id': item_id, 'title': title, 'summary': {'content': magnet_html}}


def make_pipeline(
    *,
    items: list[dict] | None = None,
    items_by_label: dict[str, list[dict]] | None = None,
    categories: tuple[RssCategory, ...] = (RssCategory(label='Actor', task_dir_path=TASK_DIR),),
    sukebei_magnets: dict[str, str] | None = None,
    javbus_magnets: dict[str, list[dict]] | None = None,
    ledger: FakeLedger | None = None,
    add_result: object | None = None,
    add_side_effect: Exception | list[object] | None = None,
    freshrss_side_effect: object | None = None,
    archive_config: ArchiveConfig | None = None,
) -> tuple[RssPipeline, SimpleNamespace]:
    deps = SimpleNamespace()
    deps.ledger = ledger or FakeLedger()
    if freshrss_side_effect is not None:
        get_items = AsyncMock(side_effect=freshrss_side_effect)
    elif items_by_label is not None:

        async def by_label(label: str) -> list[dict]:
            return items_by_label.get(label, [])

        get_items = AsyncMock(side_effect=by_label)
    else:
        get_items = AsyncMock(return_value=items or [])
    deps.freshrss = SimpleNamespace(get_items=get_items, read_items=AsyncMock())

    async def sukebei_get(avid: str) -> str | None:
        return (sukebei_magnets or {}).get(avid)

    deps.sukebei = SimpleNamespace(get_magnet=AsyncMock(side_effect=sukebei_get))

    async def javbus_get(avid: str) -> list[dict]:
        return (javbus_magnets or {}).get(avid, [])

    deps.javbus = SimpleNamespace(get_magnets=AsyncMock(side_effect=javbus_get))
    if add_side_effect is not None:
        deps.cloud = SimpleNamespace(add_offline_files=AsyncMock(side_effect=add_side_effect))
    else:
        deps.cloud = SimpleNamespace(
            add_offline_files=AsyncMock(return_value=add_result or SimpleNamespace(success=True)),
        )
    pipeline = RssPipeline(
        config=RssConfig(enabled=True, categories=categories),
        avid_parser=AvidParser(),
        freshrss=deps.freshrss,
        cloud=deps.cloud,
        sukebei=deps.sukebei,
        javbus=deps.javbus,
        ledger=deps.ledger,
        # An unconfigured archive has no routes, so the library check passes
        # everything through without touching the filesystem.
        archiver=ArchivePipeline(config=archive_config or ArchiveConfig(), avid_parser=AvidParser()),
    )
    return pipeline, deps


async def test_discovery_records_the_avid_and_submits_the_first_magnet() -> None:
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123 release')],
        sukebei_magnets={'ABC-123': MAGNET_A},
    )

    ctx = make_ctx()
    await pipeline.run(ctx)

    deps.cloud.add_offline_files.assert_awaited_once_with([MAGNET_A], '/115/task')
    assert deps.ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING
    assert deps.ledger.attempt_states('ABC-123') == [AttemptState.SUBMITTED]
    assert ctx.stats['magnets_added'] == 1


async def test_items_are_read_as_soon_as_the_avid_is_recorded() -> None:
    # FreshRSS no longer doubles as the retry queue, so nothing is left unread
    # to be picked up next run.
    pipeline, deps = make_pipeline(items=[make_item('item-1', 'ABC-123'), make_item('item-2', 'ABC-123')])

    ctx = make_ctx()
    await pipeline.run(ctx)

    deps.freshrss.read_items.assert_awaited_once_with(['item-1', 'item-2'])
    assert ctx.stats['items_marked_read'] == 2
    assert deps.ledger.states['ABC-123'] is AcquisitionState.RESOLVE_FAILED


async def test_every_source_contributes_candidates_deduplicated_by_hash() -> None:
    html = f"""
    <table><tbody><tr>
      <td><a href="magnet:?xt=urn:btih:{HASH_B}&dn=x">x</a></td>
      <td>2 GiB</td>
    </tr></tbody></table>
    """
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123', html)],
        sukebei_magnets={'ABC-123': MAGNET_A},
        javbus_magnets={
            'ABC-123': [
                # The same torrent sukebei returned, advertised in base32.
                {'magnet': 'magnet:?xt=urn:btih:YEX6DQDLXISUVHOJ6UM3GNNKPQJWPKEK', 'size_int': 9},
                {'magnet': f'magnet:?xt=urn:btih:{HASH_C}', 'size_int': 5},
            ],
        },
    )

    await pipeline.run(make_ctx())

    # The item's magnet comes back re-labelled with the AVID as its display name.
    assert deps.ledger.magnets('ABC-123') == [MAGNET_A, MAGNET_B, f'magnet:?xt=urn:btih:{HASH_C}']


async def test_candidates_are_capped() -> None:
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123')],
        javbus_magnets={
            'ABC-123': [{'magnet': f'magnet:?xt=urn:btih:{index:040X}', 'size_int': index} for index in range(1, 12)],
        },
    )

    await pipeline.run(make_ctx())

    assert len(deps.ledger.magnets('ABC-123')) == 5


async def test_magnets_without_a_usable_hash_are_not_recorded() -> None:
    # CloudDrive reports tasks by info hash; one we cannot compute is untrackable.
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123')],
        sukebei_magnets={'ABC-123': 'magnet:?dn=ABC-123'},
    )

    await pipeline.run(make_ctx())

    assert deps.ledger.magnets('ABC-123') == []
    assert deps.ledger.states['ABC-123'] is AcquisitionState.RESOLVE_FAILED


async def test_unresolvable_avid_gets_a_cooldown_instead_of_a_retry_loop() -> None:
    pipeline, deps = make_pipeline(items=[make_item('item-1', 'ABC-123')])

    ctx = make_ctx()
    await pipeline.run(ctx)

    assert deps.ledger.states['ABC-123'] is AcquisitionState.RESOLVE_FAILED
    assert deps.ledger.next_action_at['ABC-123'] is not None
    assert ctx.stats['magnets_failed'] == 1


@pytest.mark.parametrize(
    'state',
    [AcquisitionState.DOWNLOADING, AcquisitionState.ARCHIVED, AcquisitionState.IGNORED],
)
async def test_avids_the_ledger_already_owns_are_skipped(state: AcquisitionState) -> None:
    ledger = FakeLedger(known={'ABC-123': state})
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123')],
        sukebei_magnets={'ABC-123': MAGNET_A},
        ledger=ledger,
    )

    ctx = make_ctx()
    await pipeline.run(ctx)

    deps.cloud.add_offline_files.assert_not_awaited()
    deps.sukebei.get_magnet.assert_not_awaited()
    assert ctx.stats['skipped_known'] == 1
    # The item is still read: re-reading it would only re-derive the same AVID.
    deps.freshrss.read_items.assert_awaited_once_with(['item-1'])


async def test_a_failed_submission_falls_through_to_the_next_candidate() -> None:
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123')],
        sukebei_magnets={'ABC-123': MAGNET_A},
        javbus_magnets={'ABC-123': [{'magnet': MAGNET_B, 'size_int': 1}]},
        add_side_effect=[RuntimeError('offline queue rejected it'), SimpleNamespace(success=True)],
    )

    await pipeline.run(make_ctx())

    assert deps.ledger.attempt_states('ABC-123') == [AttemptState.ERROR, AttemptState.SUBMITTED]
    assert deps.ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING


async def test_all_candidates_failing_leaves_the_avid_undownloaded() -> None:
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123')],
        sukebei_magnets={'ABC-123': MAGNET_A},
        add_side_effect=RuntimeError('offline queue rejected it'),
    )

    await pipeline.run(make_ctx())

    assert deps.ledger.attempt_states('ABC-123') == [AttemptState.ERROR]
    assert deps.ledger.states['ABC-123'] is AcquisitionState.DISCOVERED


async def test_duplicate_offline_task_counts_as_submitted() -> None:
    error = grpc.RpcError()
    error.details = lambda: '任务已存在'
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123')],
        sukebei_magnets={'ABC-123': MAGNET_A},
        add_side_effect=error,
    )

    ctx = make_ctx()
    await pipeline.run(ctx)

    # CloudDrive is already working on this hash, which is what we track.
    assert deps.ledger.attempt_states('ABC-123') == [AttemptState.SUBMITTED]
    assert deps.ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING
    assert ctx.stats['duplicates'] == 1


async def test_empty_feed_does_nothing() -> None:
    pipeline, deps = make_pipeline(items=[])

    ctx = make_ctx()
    await pipeline.run(ctx)

    deps.freshrss.read_items.assert_not_awaited()
    deps.cloud.add_offline_files.assert_not_awaited()
    assert ctx.stats['items'] == 0


async def test_unparseable_titles_are_reported_and_left_unread() -> None:
    pipeline, deps = make_pipeline(items=[make_item('item-1', '!!!')])

    ctx = make_ctx()
    await pipeline.run(ctx)

    deps.freshrss.read_items.assert_not_awaited()
    assert ctx.stats['unique_avids'] == 0
    assert any('Failed to get avid' in line for line in ctx.log_tail)


async def test_each_category_is_recorded_under_its_own_source() -> None:
    pipeline, deps = make_pipeline(
        items_by_label={'Actor': [make_item('item-1', 'ABC-123')], 'Rank': [make_item('item-2', 'DEF-456')]},
        categories=(
            RssCategory(label='Actor', task_dir_path=TASK_DIR),
            RssCategory(label='Rank', task_dir_path=TASK_DIR),
        ),
        sukebei_magnets={'ABC-123': MAGNET_A, 'DEF-456': MAGNET_B},
    )

    await pipeline.run(make_ctx())

    assert deps.ledger.sources == {'ABC-123': 'rss:Actor', 'DEF-456': 'rss:Rank'}
    assert [call.args[0] for call in deps.freshrss.get_items.await_args_list] == ['Actor', 'Rank']


async def test_a_category_downloads_into_its_own_directory() -> None:
    pipeline, deps = make_pipeline(
        items_by_label={'Actor': [make_item('item-1', 'ABC-123')], 'Rank': [make_item('item-2', 'DEF-456')]},
        categories=(
            RssCategory(label='Actor', task_dir_path=TASK_DIR),
            RssCategory(label='Rank', task_dir_path='/115/embyx_in/rank'),
        ),
        sukebei_magnets={'ABC-123': MAGNET_A, 'DEF-456': MAGNET_B},
    )

    await pipeline.run(make_ctx())

    submitted = {call.args[0][0]: call.args[1] for call in deps.cloud.add_offline_files.await_args_list}
    assert submitted == {MAGNET_A: TASK_DIR, MAGNET_B: '/115/embyx_in/rank'}
    # Each AVID carries its category's directory, so a later retry follows it.
    assert deps.ledger.task_dirs == {'ABC-123': TASK_DIR, 'DEF-456': '/115/embyx_in/rank'}


async def test_no_categories_ingests_nothing() -> None:
    pipeline, deps = make_pipeline(items=[make_item('item-1', 'ABC-123')], categories=())

    await pipeline.run(make_ctx())

    deps.freshrss.get_items.assert_not_awaited()
    assert deps.ledger.states == {}


async def test_one_failing_category_does_not_cost_the_others_their_pass() -> None:
    pipeline, deps = make_pipeline(
        freshrss_side_effect=[RuntimeError('freshrss is down'), [make_item('item-2', 'DEF-456')]],
        categories=(
            RssCategory(label='Actor', task_dir_path=TASK_DIR),
            RssCategory(label='Rank', task_dir_path=TASK_DIR),
        ),
        sukebei_magnets={'DEF-456': MAGNET_B},
    )

    ctx = make_ctx()
    await pipeline.run(ctx)

    assert deps.ledger.states['DEF-456'] is AcquisitionState.DOWNLOADING
    assert ctx.stats['categories_failed'] == 1


async def test_stats_cover_every_category_in_the_run() -> None:
    # Counters accumulate across categories; a later one must not overwrite the
    # tally of the one before it.
    pipeline, _ = make_pipeline(
        items_by_label={
            'Actor': [make_item('item-1', 'ABC-123')],
            'Rank': [make_item('item-2', 'DEF-456'), make_item('item-3', 'GHI-789')],
        },
        categories=(
            RssCategory(label='Actor', task_dir_path=TASK_DIR),
            RssCategory(label='Rank', task_dir_path=TASK_DIR),
        ),
        sukebei_magnets={'ABC-123': MAGNET_A, 'DEF-456': MAGNET_B},
    )

    ctx = make_ctx()
    await pipeline.run(ctx)

    assert ctx.stats['items'] == 3
    assert ctx.stats['unique_avids'] == 3
    assert ctx.stats['magnets_found'] == 2
    assert ctx.stats['magnets_added'] == 2


# -- the library check ---------------------------------------------------------


def library_config(tmp_path) -> ArchiveConfig:
    """Routes mirroring production: a priority actor inbox and a rank inbox."""
    return ArchiveConfig(
        src_dir=str(tmp_path / 'mnt' / '115' / 'embyx_in'),
        dst_dir=str(tmp_path / 'library'),
        mapping={'rank': 'rank'},
        priority_mapping={'clt': 'actor/clt'},
    )


def write_library_copy(tmp_path, relative: str):
    path = tmp_path / 'library' / relative
    path.parent.mkdir(parents=True)
    path.write_bytes(b'v')
    return path


async def test_an_avid_the_library_holds_is_settled_without_a_download(tmp_path) -> None:
    existing = write_library_copy(tmp_path, 'actor/clt/ABC/ABC-123.mp4')
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123 release')],
        categories=(RssCategory(label='Rank', task_dir_path='/115/embyx_in/rank'),),
        sukebei_magnets={'ABC-123': MAGNET_A},
        archive_config=library_config(tmp_path),
    )

    ctx = make_ctx()
    await pipeline.run(ctx)

    deps.cloud.add_offline_files.assert_not_awaited()
    deps.sukebei.get_magnet.assert_not_awaited()
    deps.freshrss.read_items.assert_awaited_once_with(['item-1'])
    assert deps.ledger.states['ABC-123'] is AcquisitionState.ARCHIVED
    assert deps.ledger.notes['ABC-123'] == 'already in library'
    assert deps.ledger.archived_paths['ABC-123'] == ('actor/clt/ABC/ABC-123.mp4',)
    # A normal category leaves the priority copy where it is.
    assert existing.exists()
    assert ctx.stats['already_in_library'] == 1


async def test_a_priority_category_pulls_the_copy_out_of_a_normal_route(tmp_path) -> None:
    existing = write_library_copy(tmp_path, 'rank/ABC/ABC-123.mp4')
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123 release')],
        categories=(RssCategory(label='Actor', task_dir_path='/115/embyx_in/clt'),),
        sukebei_magnets={'ABC-123': MAGNET_A},
        archive_config=library_config(tmp_path),
    )

    await pipeline.run(make_ctx())

    deps.cloud.add_offline_files.assert_not_awaited()
    assert deps.ledger.states['ABC-123'] is AcquisitionState.ARCHIVED
    assert deps.ledger.archived_paths['ABC-123'] == ('actor/clt/ABC/ABC-123.mp4',)
    assert not existing.exists()
    assert (tmp_path / 'library' / 'actor' / 'clt' / 'ABC' / 'ABC-123.mp4').exists()


async def test_an_absent_avid_still_downloads_when_routes_are_configured(tmp_path) -> None:
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123 release')],
        categories=(RssCategory(label='Rank', task_dir_path='/115/embyx_in/rank'),),
        sukebei_magnets={'ABC-123': MAGNET_A},
        archive_config=library_config(tmp_path),
    )

    await pipeline.run(make_ctx())

    deps.cloud.add_offline_files.assert_awaited_once_with([MAGNET_A], '/115/embyx_in/rank')
    assert deps.ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING
