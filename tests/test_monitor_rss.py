import dataclasses
import logging
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from xml.sax.saxutils import escape

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
from embyx_manager.monitor.subscriptions import (
    SubscriptionExistsError,
    SubscriptionKind,
    SubscriptionRecord,
    trim_cursor,
)

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
        self.release_dates: dict[str, date | None] = {}

    async def discover(
        self,
        avid: str,
        *,
        source: str,
        now: datetime,
        task_dir_path: str | None = None,
        next_action_at: datetime | None = None,
        release_date: date | None = None,
        wake: bool = False,
    ) -> bool:
        if avid not in self.states:
            self.states[avid] = AcquisitionState.DISCOVERED
            self.sources[avid] = source
            self.task_dirs[avid] = task_dir_path
            self.next_action_at[avid] = next_action_at
            self.release_dates[avid] = release_date
            return True
        state = self.states[avid]
        if state is AcquisitionState.DISCOVERED:
            accepted = True
        elif state in {AcquisitionState.RESOLVE_FAILED, AcquisitionState.EXHAUSTED}:
            due = self.next_action_at.get(avid)
            accepted = wake or due is None or due <= now
        else:
            return False
        if accepted and release_date is not None and self.release_dates.get(avid) is None:
            self.release_dates[avid] = release_date
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
            release_date=self.release_dates.get(avid),
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


def feed_url(label: str) -> str:
    return f'https://feeds.test/{label}'


def make_subscription(
    subscription_id: int,
    *,
    category: str = 'Actor',
    url: str | None = None,
    name: str | None = None,
    enabled: bool = True,
    cursor: tuple[str, ...] = (),
    seed_pending: bool = False,
) -> SubscriptionRecord:
    return SubscriptionRecord(
        id=subscription_id,
        kind=SubscriptionKind.RSS,
        category=category,
        enabled=enabled,
        url=url or feed_url(category),
        talent_id=None,
        name=name,
        aliases=(),
        cursor=cursor,
        seed_pending=seed_pending,
        last_polled_at=None,
        last_error=None,
        created_at=now_stub(),
        updated_at=now_stub(),
    )


class FakeSubscriptions:
    """In-memory stand-in for SubscriptionRepository; the real one is CI-tested."""

    def __init__(self, records: list[SubscriptionRecord] | None = None) -> None:
        self.records: dict[int, SubscriptionRecord] = {record.id: record for record in records or []}
        self.polls: list[tuple[int, tuple[str, ...] | None, str | None]] = []

    async def list(self) -> tuple[SubscriptionRecord, ...]:
        return tuple(
            sorted(
                self.records.values(), key=lambda record: (record.category, record.kind, record.display_name, record.id)
            )
        )

    async def get(self, subscription_id: int) -> SubscriptionRecord | None:
        return self.records.get(subscription_id)

    async def add_rss(
        self,
        *,
        url: str,
        category: str,
        now: datetime,
        name: str | None = None,
        seed_pending: bool = False,
    ) -> SubscriptionRecord:
        if any(record.url == url for record in self.records.values()):
            raise SubscriptionExistsError(url)
        record = dataclasses.replace(
            make_subscription(
                max(self.records, default=0) + 1,
                category=category,
                url=url,
                name=name,
                seed_pending=seed_pending,
            ),
            created_at=now,
            updated_at=now,
        )
        self.records[record.id] = record
        return record

    async def update(
        self,
        subscription_id: int,
        *,
        now: datetime,
        enabled: bool | None = None,
        category: str | None = None,
    ) -> SubscriptionRecord | None:
        record = self.records.get(subscription_id)
        if record is None:
            return None
        changes: dict[str, object] = {'updated_at': now}
        if enabled is not None:
            changes['enabled'] = enabled
        if category is not None:
            changes['category'] = category
        record = dataclasses.replace(record, **changes)  # type: ignore[arg-type]
        self.records[subscription_id] = record
        return record

    async def delete(self, subscription_id: int) -> bool:
        return self.records.pop(subscription_id, None) is not None

    async def record_poll(
        self,
        subscription_id: int,
        *,
        now: datetime,
        cursor: Sequence[str] | None,
        error: str | None,
    ) -> None:
        self.polls.append((subscription_id, None if cursor is None else tuple(cursor), error))
        record = self.records[subscription_id]
        changes: dict[str, object] = {'last_polled_at': now, 'last_error': error, 'updated_at': now}
        if cursor is not None:
            changes['cursor'] = trim_cursor(cursor)
            changes['seed_pending'] = False
        self.records[subscription_id] = dataclasses.replace(record, **changes)  # type: ignore[arg-type]


def make_item(item_id: str, title: str, magnet_html: str = '', link: str | None = None) -> dict:
    return {'id': item_id, 'title': title, 'content': magnet_html, 'link': link}


def feed_xml(items: list[dict]) -> bytes:
    """An RSS 2.0 body holding the given items, guid first so the key is the id."""
    entries = []
    for item in items:
        parts = [f'<guid>{escape(item["id"])}</guid>', f'<title>{escape(item["title"])}</title>']
        if item.get('link'):
            parts.append(f'<link>{escape(item["link"])}</link>')
        if item.get('content'):
            parts.append(f'<description><![CDATA[{item["content"]}]]></description>')
        entries.append('<item>' + ''.join(parts) + '</item>')
    body = (
        '<?xml version="1.0"?><rss version="2.0"><channel><title>test</title>' + ''.join(entries) + '</channel></rss>'
    )
    return body.encode()


def make_pipeline(
    *,
    items: list[dict] | None = None,
    items_by_label: dict[str, list[dict]] | None = None,
    categories: tuple[RssCategory, ...] = (RssCategory(label='Actor', task_dir_path=TASK_DIR),),
    subscriptions: FakeSubscriptions | None = None,
    sukebei_magnets: dict[str, str] | None = None,
    javbus_magnets: dict[str, list[dict]] | None = None,
    ledger: FakeLedger | None = None,
    add_result: object | None = None,
    add_side_effect: Exception | list[object] | None = None,
    fetch_side_effect: object | None = None,
    archive_config: ArchiveConfig | None = None,
) -> tuple[RssPipeline, SimpleNamespace]:
    deps = SimpleNamespace()
    deps.ledger = ledger or FakeLedger()
    # One subscription per label, numbered in label order, each serving its feed.
    # A bare item list belongs to whichever category is configured, as the old
    # single-label FreshRSS fixture did.
    if items_by_label is not None:
        feeds = items_by_label
    else:
        feeds = {categories[0].label if categories else 'Actor': items or []}
    if subscriptions is None:
        subscriptions = FakeSubscriptions(
            [make_subscription(index, category=label) for index, label in enumerate(feeds, start=1)],
        )
    deps.subscriptions = subscriptions
    bodies = {feed_url(label): feed_xml(feed_items) for label, feed_items in feeds.items()}
    if fetch_side_effect is not None:
        deps.fetch = AsyncMock(side_effect=fetch_side_effect)
    else:

        async def fetch(url: str) -> bytes:
            return bodies[url]

        deps.fetch = AsyncMock(side_effect=fetch)

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
        subscriptions=subscriptions,
        fetch=deps.fetch,
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


async def test_items_enter_the_cursor_as_soon_as_they_are_seen() -> None:
    # The cursor is not a retry queue: an item is remembered whatever became of
    # its AVID, and the ledger's schedule drives the retry.
    pipeline, deps = make_pipeline(items=[make_item('item-1', 'ABC-123'), make_item('item-2', 'ABC-123')])

    ctx = make_ctx()
    await pipeline.run(ctx)

    assert deps.subscriptions.records[1].cursor == ('item-1', 'item-2')
    assert deps.subscriptions.records[1].last_error is None
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
    # The item still enters the cursor: re-reading it would only re-derive the same AVID.
    assert deps.subscriptions.records[1].cursor == ('item-1',)


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

    deps.cloud.add_offline_files.assert_not_awaited()
    assert ctx.stats['items'] == 0
    assert deps.subscriptions.records[1].last_error is None


async def test_unparseable_titles_are_reported_once_and_not_re_read() -> None:
    pipeline, deps = make_pipeline(items=[make_item('item-1', '!!!')])

    ctx = make_ctx()
    await pipeline.run(ctx)

    assert deps.subscriptions.records[1].cursor == ('item-1',)
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
    assert [call.args[0] for call in deps.fetch.await_args_list] == [feed_url('Actor'), feed_url('Rank')]


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

    deps.fetch.assert_not_awaited()
    assert deps.ledger.states == {}
    # The subscription is reported rather than guessed a directory for.
    assert deps.subscriptions.records[1].last_error == 'category Actor is not configured'


async def test_one_failing_category_does_not_cost_the_others_their_pass() -> None:
    pipeline, deps = make_pipeline(
        items_by_label={'Actor': [], 'Rank': []},
        fetch_side_effect=[RuntimeError('rsshub is down'), feed_xml([make_item('item-2', 'DEF-456')])],
        categories=(
            RssCategory(label='Actor', task_dir_path=TASK_DIR),
            RssCategory(label='Rank', task_dir_path=TASK_DIR),
        ),
        sukebei_magnets={'DEF-456': MAGNET_B},
    )

    ctx = make_ctx()
    await pipeline.run(ctx)

    assert deps.ledger.states['DEF-456'] is AcquisitionState.DOWNLOADING
    assert ctx.stats['subscriptions_failed'] == 1
    assert deps.subscriptions.records[1].last_error == 'RuntimeError: rsshub is down'
    assert deps.subscriptions.records[2].last_error is None


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
    assert deps.subscriptions.records[1].cursor == ('item-1',)
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


def magnet_table(info_hash: str) -> str:
    return f"""
    <table><tbody><tr>
      <td><a href="magnet:?xt=urn:btih:{info_hash}&dn=x">x</a></td>
      <td>2 GiB</td>
    </tr></tbody></table>
    """


async def test_an_item_carrying_a_magnet_wakes_a_parked_avid() -> None:
    # The magnet is evidence the wait is over: the cooldown no longer applies.
    ledger = FakeLedger(known={'ABC-123': AcquisitionState.RESOLVE_FAILED})
    ledger.next_action_at['ABC-123'] = datetime.now(UTC) + timedelta(days=1)
    pipeline, deps = make_pipeline(items=[make_item('item-1', 'ABC-123', magnet_table(HASH_B))], ledger=ledger)

    ctx = make_ctx()
    await pipeline.run(ctx)

    deps.cloud.add_offline_files.assert_awaited_once_with([MAGNET_B], TASK_DIR)
    assert deps.ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING
    assert ctx.stats.get('skipped_known', 0) == 0


async def test_an_item_without_a_magnet_leaves_a_parked_avid_cooling() -> None:
    ledger = FakeLedger(known={'ABC-123': AcquisitionState.RESOLVE_FAILED})
    due = datetime.now(UTC) + timedelta(days=1)
    ledger.next_action_at['ABC-123'] = due
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123')],
        ledger=ledger,
        sukebei_magnets={'ABC-123': MAGNET_A},
    )

    ctx = make_ctx()
    await pipeline.run(ctx)

    assert ctx.stats['skipped_known'] == 1
    deps.cloud.add_offline_files.assert_not_awaited()
    assert deps.ledger.next_action_at['ABC-123'] == due


# -- subscriptions and cursors ------------------------------------------------


async def test_items_seen_last_poll_are_not_read_again() -> None:
    pipeline, deps = make_pipeline(items=[make_item('item-1', 'ABC-123')], sukebei_magnets={'ABC-123': MAGNET_A})
    await pipeline.run(make_ctx())
    # Pretend the download failed and the row is waiting: a re-read would try again.
    deps.ledger.states['ABC-123'] = AcquisitionState.RESOLVE_FAILED
    deps.ledger.next_action_at['ABC-123'] = None

    ctx = make_ctx()
    await pipeline.run(ctx)

    assert ctx.stats['new_items'] == 0
    assert ctx.stats.get('unique_avids', 0) == 0
    deps.cloud.add_offline_files.assert_awaited_once()


async def test_a_pending_seed_records_the_feed_without_ingesting_it() -> None:
    subscriptions = FakeSubscriptions([make_subscription(1, seed_pending=True)])
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123')],
        subscriptions=subscriptions,
        sukebei_magnets={'ABC-123': MAGNET_A},
    )

    ctx = make_ctx()
    await pipeline.run(ctx)

    assert ctx.stats['subscriptions_seeded'] == 1
    assert deps.ledger.states == {}
    deps.fetch.assert_awaited_once_with(feed_url('Actor'))
    record = subscriptions.records[1]
    assert record.cursor == ('item-1',)
    assert record.seed_pending is False


async def test_a_disabled_subscription_is_not_polled() -> None:
    subscriptions = FakeSubscriptions([make_subscription(1, enabled=False)])
    pipeline, deps = make_pipeline(items=[make_item('item-1', 'ABC-123')], subscriptions=subscriptions)

    await pipeline.run(make_ctx())

    deps.fetch.assert_not_awaited()
    assert deps.ledger.states == {}


async def test_the_avid_falls_back_to_the_item_link_with_the_catalog_prefix_stripped() -> None:
    # An AVBase talent feed: the title is the work's title, the ID only in the link.
    pipeline, deps = make_pipeline(
        items=[
            make_item(
                'https://www.avbase.net/works/MIZD-555',
                '乳首ギンギンのけ反りギュンッ',
                link='https://www.avbase.net/works/moodyz:MIZD-555',
            ),
        ],
        sukebei_magnets={'MIZD-555': MAGNET_A},
    )

    await pipeline.run(make_ctx())

    assert deps.ledger.states['MIZD-555'] is AcquisitionState.DOWNLOADING


async def test_a_feed_that_cannot_be_parsed_is_reported_on_the_subscription() -> None:
    pipeline, deps = make_pipeline(fetch_side_effect=[b'<html><body>not a feed</body></html>'])

    ctx = make_ctx()
    await pipeline.run(ctx)

    assert ctx.stats['subscriptions_failed'] == 1
    record = deps.subscriptions.records[1]
    assert record.last_error is not None
    assert record.last_error.startswith('FeedParseError')
    assert record.cursor == ()
