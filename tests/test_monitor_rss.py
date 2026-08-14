import dataclasses
import logging
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import grpc
import pytest

from embyx_manager.config.models import RssConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.acquisitions import (
    AcquisitionRecord,
    AcquisitionSource,
    AcquisitionState,
    AttemptState,
    MagnetAttemptRecord,
    MagnetCandidate,
)
from embyx_manager.monitor.reports import RunContext
from embyx_manager.monitor.rss import RssPipeline

HASH_A = 'C12FE1C06BBA254A9DC9F519B335AA7C1367A88A'
HASH_B = 'D23FE1C06BBA254A9DC9F519B335AA7C1367A88B'
HASH_C = 'E34FE1C06BBA254A9DC9F519B335AA7C1367A88C'
MAGNET_A = f'magnet:?xt=urn:btih:{HASH_A}&dn=ABC-123'
MAGNET_B = f'magnet:?xt=urn:btih:{HASH_B}&dn=ABC-123'


def make_ctx() -> RunContext:
    return RunContext(logger=logging.getLogger('test-rss'))


class FakeLedger:
    """In-memory stand-in for AcquisitionRepository.

    The real repository is exercised against PostgreSQL in
    tests/test_monitor_acquisitions.py; here the point is what RSS asks of it.
    """

    def __init__(self, *, known: dict[str, AcquisitionState] | None = None) -> None:
        self.states: dict[str, AcquisitionState] = dict(known or {})
        self.sources: dict[str, AcquisitionSource] = {}
        self.notes: dict[str, str | None] = {}
        self.next_action_at: dict[str, datetime | None] = {}
        self.attempts: dict[str, list[MagnetAttemptRecord]] = {}

    async def discover(self, avid: str, *, source: AcquisitionSource, now: datetime) -> bool:
        if avid not in self.states:
            self.states[avid] = AcquisitionState.DISCOVERED
            self.sources[avid] = source
            return True
        state = self.states[avid]
        if state is AcquisitionState.DISCOVERED:
            return True
        if state in {AcquisitionState.RESOLVE_FAILED, AcquisitionState.EXHAUSTED}:
            due = self.next_action_at.get(avid)
            return due is None or due <= now
        return False

    async def get(self, avid: str) -> AcquisitionRecord | None:
        if avid not in self.states:
            return None
        return AcquisitionRecord(
            avid=avid,
            state=self.states[avid],
            source=self.sources.get(avid, AcquisitionSource.RSS_ACTOR),
            note=self.notes.get(avid),
            archived_paths=(),
            next_action_at=self.next_action_at.get(avid),
            created_at=now_stub(),
            updated_at=now_stub(),
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
        del now, archived_paths
        if self.states.get(avid) is not expected:
            return False
        self.states[avid] = target
        self.notes[avid] = note
        self.next_action_at[avid] = next_action_at
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
    items: list[dict],
    sukebei_magnets: dict[str, str] | None = None,
    javbus_magnets: dict[str, list[dict]] | None = None,
    ledger: FakeLedger | None = None,
    add_result: object | None = None,
    add_side_effect: Exception | list[object] | None = None,
) -> tuple[RssPipeline, SimpleNamespace]:
    deps = SimpleNamespace()
    deps.ledger = ledger or FakeLedger()
    deps.freshrss = SimpleNamespace(get_items=AsyncMock(return_value=items), read_items=AsyncMock())

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
        config=RssConfig(enabled=True),
        avid_parser=AvidParser(),
        freshrss=deps.freshrss,
        cloud=deps.cloud,
        sukebei=deps.sukebei,
        javbus=deps.javbus,
        task_dir_path='/115/task',
        ledger=deps.ledger,
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


async def test_rank_runs_are_recorded_under_their_own_source() -> None:
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123')],
        sukebei_magnets={'ABC-123': MAGNET_A},
    )

    await pipeline.run(make_ctx(), rank=True)

    assert deps.ledger.sources['ABC-123'] is AcquisitionSource.RSS_RANK
    deps.freshrss.get_items.assert_awaited_once_with('Rank')
