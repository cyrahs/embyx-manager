import asyncio
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from embyx_manager.monitor.acquisitions import (
    AcquisitionRepository,
    AcquisitionSource,
    AcquisitionState,
    AttemptState,
    IllegalTransitionError,
    MagnetCandidate,
    rss_source,
)
from tests.conftest import make_database, postgres_test_dsn

NOW = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)


def make_ledger() -> AcquisitionRepository:
    postgres_test_dsn()
    return AcquisitionRepository(make_database())


def candidate(name: str, *, source: str = 'sukebei', size: int | None = None) -> MagnetCandidate:
    return MagnetCandidate(
        magnet=f'magnet:?xt=urn:btih:{name}',
        info_hash=name.upper(),
        source=source,
        size_hint=size,
    )


async def start_downloading(ledger: AcquisitionRepository, avid: str, *, count: int = 2) -> None:
    """Discover an AVID, give it candidates, and submit the first one."""
    await ledger.discover(avid, source=rss_source('Actor'), now=NOW)
    candidates = [candidate(f'{avid.replace("-", "")}hash{index}') for index in range(count)]
    await ledger.add_attempts(avid, candidates, now=NOW)
    await ledger.claim_next_pending(avid, now=NOW)
    await ledger.transition(
        avid,
        expected=AcquisitionState.DISCOVERED,
        target=AcquisitionState.DOWNLOADING,
        now=NOW,
    )


async def test_discover_inserts_then_reports_the_avid_as_taken() -> None:
    ledger = make_ledger()

    assert await ledger.discover('ABC-123', source=rss_source('Actor'), now=NOW) is True
    # Still only discovered, so a second sighting should still ask for magnets.
    assert await ledger.discover('ABC-123', source=rss_source('Rank'), now=NOW) is True

    record = await ledger.get('ABC-123')
    assert record is not None
    assert record.state is AcquisitionState.DISCOVERED
    assert record.source == 'rss:Actor'  # first sighting wins

    await ledger.transition(
        'ABC-123',
        expected=AcquisitionState.DISCOVERED,
        target=AcquisitionState.DOWNLOADING,
        now=NOW,
    )
    assert await ledger.discover('ABC-123', source=rss_source('Actor'), now=NOW) is False


@pytest.mark.parametrize('state', [AcquisitionState.ARCHIVED, AcquisitionState.IGNORED])
async def test_discover_skips_terminal_avids(state: AcquisitionState) -> None:
    ledger = make_ledger()
    await ledger.discover('ABC-123', source=rss_source('Actor'), now=NOW)
    await ledger.transition('ABC-123', expected=AcquisitionState.DISCOVERED, target=state, now=NOW)

    assert await ledger.discover('ABC-123', source=rss_source('Actor'), now=NOW) is False


async def test_discover_respects_the_resolve_cooldown() -> None:
    ledger = make_ledger()
    await ledger.discover('ABC-123', source=rss_source('Actor'), now=NOW)
    await ledger.transition(
        'ABC-123',
        expected=AcquisitionState.DISCOVERED,
        target=AcquisitionState.RESOLVE_FAILED,
        now=NOW,
        next_action_at=NOW + timedelta(hours=24),
    )

    assert await ledger.discover('ABC-123', source=rss_source('Actor'), now=NOW) is False
    assert await ledger.discover('ABC-123', source=rss_source('Actor'), now=NOW + timedelta(hours=25)) is True


async def test_transition_is_compare_and_set() -> None:
    ledger = make_ledger()
    await ledger.discover('ABC-123', source=rss_source('Actor'), now=NOW)

    assert (
        await ledger.transition(
            'ABC-123',
            expected=AcquisitionState.DISCOVERED,
            target=AcquisitionState.DOWNLOADING,
            now=NOW,
        )
        is True
    )
    # The row already moved: the same CAS must not apply twice.
    assert (
        await ledger.transition(
            'ABC-123',
            expected=AcquisitionState.DISCOVERED,
            target=AcquisitionState.DOWNLOADING,
            now=NOW,
        )
        is False
    )


async def test_transition_rejects_edges_outside_the_state_machine() -> None:
    ledger = make_ledger()
    await ledger.discover('ABC-123', source=rss_source('Actor'), now=NOW)

    with pytest.raises(IllegalTransitionError):
        await ledger.transition(
            'ABC-123',
            expected=AcquisitionState.ARCHIVED,
            target=AcquisitionState.DOWNLOADING,
            now=NOW,
        )


async def test_archiving_records_paths_with_the_state() -> None:
    ledger = make_ledger()
    await start_downloading(ledger, 'ABC-123')

    await ledger.transition(
        'ABC-123',
        expected=AcquisitionState.DOWNLOADING,
        target=AcquisitionState.ARCHIVED,
        now=NOW,
        archived_paths=('library/ABC/ABC-123.mp4',),
    )

    record = await ledger.get('ABC-123')
    assert record is not None
    assert record.state is AcquisitionState.ARCHIVED
    assert record.archived_paths == ('library/ABC/ABC-123.mp4',)


async def test_add_attempts_numbers_sequentially_and_skips_known_hashes() -> None:
    ledger = make_ledger()
    await ledger.discover('ABC-123', source=rss_source('Actor'), now=NOW)

    assert await ledger.add_attempts('ABC-123', [candidate('aaa'), candidate('bbb')], now=NOW) == 2
    # 'aaa' was already tried; only the new hash is appended.
    assert await ledger.add_attempts('ABC-123', [candidate('aaa'), candidate('ccc')], now=NOW) == 1

    attempts = await ledger.attempts_for('ABC-123')
    assert [attempt.attempt_no for attempt in attempts] == [1, 2, 3]
    assert [attempt.info_hash for attempt in attempts] == ['AAA', 'BBB', 'CCC']
    assert all(attempt.state is AttemptState.PENDING for attempt in attempts)


async def test_claim_next_pending_hands_each_attempt_out_once() -> None:
    ledger = make_ledger()
    await ledger.discover('ABC-123', source=rss_source('Actor'), now=NOW)
    await ledger.add_attempts('ABC-123', [candidate('aaa'), candidate('bbb')], now=NOW)

    first = await ledger.claim_next_pending('ABC-123', now=NOW)
    assert first is not None
    assert (first.attempt_no, first.state) == (1, AttemptState.SUBMITTED)
    assert first.submitted_at == NOW

    second = await ledger.claim_next_pending('ABC-123', now=NOW)
    assert second is not None
    assert second.attempt_no == 2

    assert await ledger.claim_next_pending('ABC-123', now=NOW) is None


async def test_concurrent_claims_never_hand_out_the_same_attempt() -> None:
    ledger = make_ledger()
    await ledger.discover('ABC-123', source=rss_source('Actor'), now=NOW)
    await ledger.add_attempts('ABC-123', [candidate('aaa'), candidate('bbb')], now=NOW)

    claims = await asyncio.gather(*(ledger.claim_next_pending('ABC-123', now=NOW) for _ in range(4)))
    claimed = sorted(claim.attempt_no for claim in claims if claim is not None)

    assert claimed == [1, 2]


async def test_concurrent_archiving_claims_leave_one_winner() -> None:
    ledger = make_ledger()
    await start_downloading(ledger, 'ABC-123')
    await ledger.transition_attempt(
        'ABC-123',
        1,
        expected=AttemptState.SUBMITTED,
        target=AttemptState.FINISHED,
        now=NOW,
    )

    results = await asyncio.gather(
        *(
            ledger.transition_attempt(
                'ABC-123',
                1,
                expected=AttemptState.FINISHED,
                target=AttemptState.ARCHIVING,
                now=NOW,
            )
            for _ in range(4)
        ),
    )

    assert sum(results) == 1


async def test_transition_attempt_rejects_illegal_edges() -> None:
    ledger = make_ledger()
    await start_downloading(ledger, 'ABC-123')

    with pytest.raises(IllegalTransitionError):
        await ledger.transition_attempt(
            'ABC-123',
            1,
            expected=AttemptState.ARCHIVED,
            target=AttemptState.PENDING,
            now=NOW,
        )


async def test_record_progress_only_writes_when_the_download_moved() -> None:
    ledger = make_ledger()
    await start_downloading(ledger, 'ABC-123')
    later = NOW + timedelta(minutes=5)

    assert await ledger.record_progress('ABC-123', 1, state=AttemptState.DOWNLOADING, progress=0.5, now=NOW) is True
    # Same progress on the next poll: updated_at must stay put so a stall stays visible.
    assert await ledger.record_progress('ABC-123', 1, state=AttemptState.DOWNLOADING, progress=0.5, now=later) is False

    attempts = await ledger.attempts_for('ABC-123')
    assert attempts[0].progress == pytest.approx(0.5)
    assert attempts[0].updated_at == NOW

    assert await ledger.record_progress('ABC-123', 1, state=AttemptState.DOWNLOADING, progress=0.75, now=later) is True
    assert (await ledger.attempts_for('ABC-123'))[0].updated_at == later


async def test_record_progress_ignores_attempts_that_already_concluded() -> None:
    ledger = make_ledger()
    await start_downloading(ledger, 'ABC-123')
    await ledger.transition_attempt(
        'ABC-123',
        1,
        expected=AttemptState.SUBMITTED,
        target=AttemptState.FINISHED,
        now=NOW,
    )

    assert await ledger.record_progress('ABC-123', 1, state=AttemptState.DOWNLOADING, progress=0.9, now=NOW) is False
    with pytest.raises(ValueError, match='in-flight'):
        await ledger.record_progress('ABC-123', 1, state=AttemptState.ARCHIVED, progress=None, now=NOW)


async def test_attempts_are_found_by_info_hash() -> None:
    ledger = make_ledger()
    await start_downloading(ledger, 'ABC-123')
    await start_downloading(ledger, 'DEF-456')

    found = await ledger.attempts_by_info_hash(['ABC123HASH0', 'DEF456HASH0', 'MISSING', ''])

    assert set(found) == {'ABC123HASH0', 'DEF456HASH0'}
    assert found['ABC123HASH0'].avid == 'ABC-123'
    assert found['DEF456HASH0'].avid == 'DEF-456'


async def test_concluded_attempts_drop_out_of_the_hash_lookup() -> None:
    ledger = make_ledger()
    await start_downloading(ledger, 'ABC-123')
    await ledger.transition_attempt(
        'ABC-123',
        1,
        expected=AttemptState.SUBMITTED,
        target=AttemptState.STALLED,
        now=NOW,
    )

    assert await ledger.attempts_by_info_hash(['ABC123HASH0']) == {}
    # The hash stays on the row for history, and may be tried again later.
    assert (await ledger.attempts_for('ABC-123'))[0].info_hash == 'ABC123HASH0'


async def test_a_hash_cannot_be_live_for_two_attempts_at_once() -> None:
    ledger = make_ledger()
    await ledger.discover('ABC-123', source=rss_source('Actor'), now=NOW)
    await ledger.discover('DEF-456', source=rss_source('Actor'), now=NOW)
    await ledger.add_attempts('ABC-123', [candidate('shared')], now=NOW)
    await ledger.add_attempts('DEF-456', [candidate('shared')], now=NOW)

    assert await ledger.claim_next_pending('ABC-123', now=NOW) is not None
    with pytest.raises(asyncpg.UniqueViolationError):
        await ledger.claim_next_pending('DEF-456', now=NOW)


async def test_in_flight_attempts_exclude_concluded_ones() -> None:
    ledger = make_ledger()
    await start_downloading(ledger, 'ABC-123')
    await start_downloading(ledger, 'DEF-456')
    await ledger.transition_attempt(
        'ABC-123',
        1,
        expected=AttemptState.SUBMITTED,
        target=AttemptState.ERROR,
        now=NOW,
        error='offline task failed',
    )

    in_flight = await ledger.in_flight_attempts()

    assert [attempt.avid for attempt in in_flight] == ['DEF-456']


async def test_due_for_retry_returns_expired_cooldowns_in_order() -> None:
    ledger = make_ledger()
    for avid, hours in (('ABC-123', 1), ('DEF-456', 3), ('GHI-789', 5)):
        await ledger.discover(avid, source=rss_source('Actor'), now=NOW)
        await ledger.transition(
            avid,
            expected=AcquisitionState.DISCOVERED,
            target=AcquisitionState.RESOLVE_FAILED,
            now=NOW,
            next_action_at=NOW + timedelta(hours=hours),
        )

    due = await ledger.due_for_retry(now=NOW + timedelta(hours=4))

    assert [record.avid for record in due] == ['ABC-123', 'DEF-456']


async def test_active_avids_covers_only_the_states_the_tracker_owns() -> None:
    ledger = make_ledger()
    await start_downloading(ledger, 'ABC-123')
    await ledger.discover('DEF-456', source=AcquisitionSource.RECONCILE, now=NOW)
    await ledger.discover('GHI-789', source=AcquisitionSource.MANUAL, now=NOW)
    await ledger.transition(
        'GHI-789',
        expected=AcquisitionState.DISCOVERED,
        target=AcquisitionState.NEEDS_ATTENTION,
        now=NOW,
        note='multiple avids in folder',
    )

    assert await ledger.active_avids() == frozenset({'ABC-123', 'DEF-456'})


async def test_listing_filters_by_state_and_counts() -> None:
    ledger = make_ledger()
    await start_downloading(ledger, 'ABC-123')
    await ledger.discover('DEF-456', source=rss_source('Actor'), now=NOW)
    await ledger.transition(
        'DEF-456',
        expected=AcquisitionState.DISCOVERED,
        target=AcquisitionState.NEEDS_ATTENTION,
        now=NOW,
        note='failed to parse avid',
    )

    attention = await ledger.list_acquisitions(states=[AcquisitionState.NEEDS_ATTENTION])
    assert [record.avid for record in attention] == ['DEF-456']
    assert attention[0].note == 'failed to parse avid'

    assert await ledger.count_by_state() == {
        AcquisitionState.DOWNLOADING: 1,
        AcquisitionState.NEEDS_ATTENTION: 1,
    }


async def test_the_offline_directory_is_pinned_at_discovery() -> None:
    ledger = make_ledger()

    await ledger.discover('ABC-123', source=rss_source('Rank'), now=NOW, task_dir_path='/115/embyx_in/rank')

    record = await ledger.get('ABC-123')
    assert record is not None
    assert record.task_dir_path == '/115/embyx_in/rank'
    assert record.source == 'rss:Rank'


async def test_an_acquisition_with_no_directory_of_its_own_follows_the_default() -> None:
    ledger = make_ledger()

    await ledger.discover('ABC-123', source=rss_source('Actor'), now=NOW)

    record = await ledger.get('ABC-123')
    assert record is not None
    assert record.task_dir_path is None


async def test_rediscovery_repoints_a_retryable_row_at_its_category_directory() -> None:
    """A category that moved takes its cooling-down AVIDs with it on the next pass."""
    ledger = make_ledger()
    await ledger.discover('ABC-123', source=rss_source('Rank'), now=NOW, task_dir_path='/115/old')
    await ledger.transition(
        'ABC-123',
        expected=AcquisitionState.DISCOVERED,
        target=AcquisitionState.RESOLVE_FAILED,
        now=NOW,
        next_action_at=NOW,
    )

    assert await ledger.discover('ABC-123', source=rss_source('Rank'), now=NOW, task_dir_path='/115/new') is True

    record = await ledger.get('ABC-123')
    assert record is not None
    assert record.task_dir_path == '/115/new'


async def test_an_avid_someone_else_owns_keeps_its_directory() -> None:
    ledger = make_ledger()
    await start_downloading(ledger, 'ABC-123')

    assert await ledger.discover('ABC-123', source=rss_source('Rank'), now=NOW, task_dir_path='/115/rank') is False

    record = await ledger.get('ABC-123')
    assert record is not None
    assert record.task_dir_path is None
