import logging
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx

from embyx_manager.monitor.acquisitions import (
    AcquisitionSource,
    AcquisitionState,
    AttemptState,
    MagnetCandidate,
)
from embyx_manager.monitor.intake import AcquisitionIntake, IntakeOutcome
from embyx_manager.monitor.reports import RunContext
from tests.test_monitor_rss import FakeLedger

TASK_DIR = '/115/task'
HASH_A = 'C12FE1C06BBA254A9DC9F519B335AA7C1367A88A'
HASH_B = 'D23FE1C06BBA254A9DC9F519B335AA7C1367A88B'
HASH_C = 'E34FE1C06BBA254A9DC9F519B335AA7C1367A88C'
MAGNET_A = f'magnet:?xt=urn:btih:{HASH_A}&dn=ABC-123'
MAGNET_B = f'magnet:?xt=urn:btih:{HASH_B}&dn=ABC-123'
MAGNET_C = f'magnet:?xt=urn:btih:{HASH_C}&dn=ABC-123'


def make_ctx() -> RunContext:
    return RunContext(logger=logging.getLogger('test-intake'))


def make_intake(
    *,
    ledger: FakeLedger | None = None,
    sukebei_magnet: str | None = None,
    sukebei_error: Exception | None = None,
    javbus_magnets: list[dict] | None = None,
    javbus_error: Exception | None = None,
    add_side_effect: Exception | list[object] | None = None,
    release_date_lookup: object = None,
) -> tuple[AcquisitionIntake, SimpleNamespace]:
    deps = SimpleNamespace()
    deps.ledger = ledger or FakeLedger()
    if sukebei_error is not None:
        deps.sukebei = SimpleNamespace(get_magnet=AsyncMock(side_effect=sukebei_error))
    else:
        deps.sukebei = SimpleNamespace(get_magnet=AsyncMock(return_value=sukebei_magnet))
    if javbus_error is not None:
        deps.javbus = SimpleNamespace(get_magnets=AsyncMock(side_effect=javbus_error))
    else:
        deps.javbus = SimpleNamespace(get_magnets=AsyncMock(return_value=javbus_magnets or []))
    if add_side_effect is not None:
        deps.cloud = SimpleNamespace(add_offline_files=AsyncMock(side_effect=add_side_effect))
    else:
        deps.cloud = SimpleNamespace(add_offline_files=AsyncMock(return_value=SimpleNamespace(success=True)))
    intake = AcquisitionIntake(
        ledger=deps.ledger,
        sukebei=deps.sukebei,
        javbus=deps.javbus,
        cloud=deps.cloud,
        failed_cooldown_seconds=3600,
        release_date_lookup=release_date_lookup,  # type: ignore[arg-type]
    )
    return intake, deps


async def enqueue(intake: AcquisitionIntake, avid: str = 'ABC-123') -> IntakeOutcome:
    return await intake.enqueue(
        avid,
        source=AcquisitionSource.FILL_ACTOR,
        task_dir_path=TASK_DIR,
        ctx=make_ctx(),
    )


async def test_enqueue_submits_the_first_candidate() -> None:
    intake, deps = make_intake(sukebei_magnet=MAGNET_A)

    outcome = await enqueue(intake)

    assert outcome is IntakeOutcome.SUBMITTED
    deps.cloud.add_offline_files.assert_awaited_once_with([MAGNET_A], TASK_DIR)
    assert deps.ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING
    assert deps.ledger.sources['ABC-123'] == 'fill_actor'
    assert deps.ledger.task_dirs['ABC-123'] == TASK_DIR
    assert deps.ledger.attempt_states('ABC-123') == [AttemptState.SUBMITTED]


async def test_queue_records_a_due_discovery_without_resolving() -> None:
    intake, deps = make_intake(sukebei_magnet=MAGNET_A)

    outcome = await intake.queue('ABC-123', source=AcquisitionSource.FILL_ACTOR, task_dir_path=TASK_DIR, ctx=make_ctx())

    assert outcome is IntakeOutcome.QUEUED
    assert deps.ledger.states['ABC-123'] is AcquisitionState.DISCOVERED
    assert deps.ledger.task_dirs['ABC-123'] == TASK_DIR
    assert deps.ledger.next_action_at['ABC-123'] is not None
    deps.sukebei.get_magnet.assert_not_awaited()
    deps.cloud.add_offline_files.assert_not_awaited()


async def test_queue_skips_an_avid_the_ledger_already_owns() -> None:
    ledger = FakeLedger(known={'ABC-123': AcquisitionState.ARCHIVED})
    intake, _ = make_intake(ledger=ledger)

    outcome = await intake.queue('ABC-123', source=AcquisitionSource.FILL_ACTOR, task_dir_path=TASK_DIR, ctx=make_ctx())

    assert outcome is IntakeOutcome.ALREADY_TRACKED


async def test_retry_resolves_and_submits_a_queued_discovery() -> None:
    ledger = FakeLedger(known={'ABC-123': AcquisitionState.DISCOVERED})
    intake, deps = make_intake(ledger=ledger, sukebei_magnet=MAGNET_A)
    record = await ledger.get('ABC-123')
    assert record is not None

    outcome = await intake.retry(record, ctx=make_ctx(), fallback_task_dir=TASK_DIR)

    assert outcome is IntakeOutcome.SUBMITTED
    deps.cloud.add_offline_files.assert_awaited_once_with([MAGNET_A], TASK_DIR)
    assert deps.ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING


async def test_retry_parks_a_queued_discovery_with_no_magnet() -> None:
    ledger = FakeLedger(known={'ABC-123': AcquisitionState.DISCOVERED})
    intake, deps = make_intake(ledger=ledger)
    record = await ledger.get('ABC-123')
    assert record is not None

    outcome = await intake.retry(record, ctx=make_ctx(), fallback_task_dir=TASK_DIR)

    assert outcome is IntakeOutcome.NO_MAGNET
    assert deps.ledger.states['ABC-123'] is AcquisitionState.RESOLVE_FAILED
    assert deps.ledger.next_action_at['ABC-123'] is not None


async def test_enqueue_skips_an_avid_the_ledger_already_owns() -> None:
    ledger = FakeLedger(known={'ABC-123': AcquisitionState.ARCHIVED})
    intake, deps = make_intake(ledger=ledger, sukebei_magnet=MAGNET_A)

    outcome = await enqueue(intake)

    assert outcome is IntakeOutcome.ALREADY_TRACKED
    deps.sukebei.get_magnet.assert_not_awaited()
    deps.cloud.add_offline_files.assert_not_awaited()


async def test_enqueue_parks_an_avid_with_no_magnet() -> None:
    intake, deps = make_intake()

    outcome = await enqueue(intake)

    assert outcome is IntakeOutcome.NO_MAGNET
    assert deps.ledger.states['ABC-123'] is AcquisitionState.RESOLVE_FAILED
    assert deps.ledger.next_action_at['ABC-123'] is not None
    deps.cloud.add_offline_files.assert_not_awaited()


async def test_enqueue_parks_when_resolution_raises() -> None:
    intake, deps = make_intake(sukebei_error=RuntimeError('sukebei down'))

    outcome = await enqueue(intake)

    assert outcome is IntakeOutcome.NO_MAGNET
    assert deps.ledger.states['ABC-123'] is AcquisitionState.RESOLVE_FAILED
    assert deps.ledger.next_action_at['ABC-123'] is not None


async def test_enqueue_reports_failure_when_every_submission_errors() -> None:
    intake, deps = make_intake(
        sukebei_magnet=MAGNET_A,
        add_side_effect=RuntimeError('offline queue rejected it'),
    )

    outcome = await enqueue(intake)

    assert outcome is IntakeOutcome.SUBMIT_FAILED
    assert deps.ledger.attempt_states('ABC-123') == [AttemptState.ERROR]
    assert deps.ledger.states['ABC-123'] is AcquisitionState.DISCOVERED


async def test_enqueue_falls_through_to_the_next_candidate() -> None:
    intake, deps = make_intake(
        sukebei_magnet=MAGNET_A,
        javbus_magnets=[{'magnet': MAGNET_B, 'size_int': 1}],
        add_side_effect=[RuntimeError('offline queue rejected it'), SimpleNamespace(success=True)],
    )

    outcome = await enqueue(intake)

    assert outcome is IntakeOutcome.SUBMITTED
    assert deps.ledger.attempt_states('ABC-123') == [AttemptState.ERROR, AttemptState.SUBMITTED]
    assert deps.ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING


def parked_ledger(state: AcquisitionState, *, due: bool = True, task_dir: str | None = TASK_DIR) -> FakeLedger:
    ledger = FakeLedger(known={'ABC-123': state})
    delta = timedelta(hours=-1 if due else 1)
    ledger.next_action_at['ABC-123'] = datetime.now(UTC) + delta
    ledger.task_dirs['ABC-123'] = task_dir
    return ledger


async def test_retry_submits_to_the_directory_pinned_on_the_record() -> None:
    intake, deps = make_intake(
        ledger=parked_ledger(AcquisitionState.RESOLVE_FAILED),
        sukebei_magnet=MAGNET_A,
    )

    processed = await intake.retry_due(ctx=make_ctx(), fallback_task_dir='/115/other')

    assert processed == 1
    deps.cloud.add_offline_files.assert_awaited_once_with([MAGNET_A], TASK_DIR)
    assert deps.ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING
    assert deps.ledger.attempt_states('ABC-123') == [AttemptState.SUBMITTED]


async def test_retry_falls_back_when_the_record_has_no_directory() -> None:
    intake, deps = make_intake(
        ledger=parked_ledger(AcquisitionState.RESOLVE_FAILED, task_dir=None),
        sukebei_magnet=MAGNET_A,
    )

    await intake.retry_due(ctx=make_ctx(), fallback_task_dir='/115/other')

    deps.cloud.add_offline_files.assert_awaited_once_with([MAGNET_A], '/115/other')
    assert deps.ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING


async def test_retry_reparks_when_no_directory_exists_at_all() -> None:
    intake, deps = make_intake(
        ledger=parked_ledger(AcquisitionState.RESOLVE_FAILED, task_dir=None),
        sukebei_magnet=MAGNET_A,
    )

    await intake.retry_due(ctx=make_ctx())

    deps.sukebei.get_magnet.assert_not_awaited()
    deps.cloud.add_offline_files.assert_not_awaited()
    assert deps.ledger.states['ABC-123'] is AcquisitionState.RESOLVE_FAILED
    assert deps.ledger.next_action_at['ABC-123'] > datetime.now(UTC)


async def test_retry_rearms_the_cooldown_when_nothing_resolves() -> None:
    # An exhausted record parks back as resolve_failed with a fresh deadline;
    # leaving the expired one in place would re-select the row every pass.
    intake, deps = make_intake(ledger=parked_ledger(AcquisitionState.EXHAUSTED))

    await intake.retry_due(ctx=make_ctx())

    assert deps.ledger.states['ABC-123'] is AcquisitionState.RESOLVE_FAILED
    assert deps.ledger.next_action_at['ABC-123'] > datetime.now(UTC)
    deps.cloud.add_offline_files.assert_not_awaited()


async def test_retry_rearms_the_cooldown_when_every_candidate_was_already_tried() -> None:
    ledger = parked_ledger(AcquisitionState.RESOLVE_FAILED)
    now = datetime.now(UTC)
    await ledger.add_attempts(
        'ABC-123',
        [MagnetCandidate(magnet=MAGNET_A, info_hash=HASH_A, source='sukebei', size_hint=None)],
        now=now,
    )
    await ledger.claim_next_pending('ABC-123', now=now)
    await ledger.transition_attempt(
        'ABC-123',
        1,
        expected=AttemptState.SUBMITTED,
        target=AttemptState.ERROR,
        now=now,
    )
    intake, deps = make_intake(ledger=ledger, sukebei_magnet=MAGNET_A)
    record = await ledger.get('ABC-123')
    assert record is not None

    outcome = await intake.retry(record, ctx=make_ctx())

    assert outcome is IntakeOutcome.SUBMIT_FAILED
    assert deps.ledger.states['ABC-123'] is AcquisitionState.RESOLVE_FAILED
    assert deps.ledger.next_action_at['ABC-123'] > datetime.now(UTC)
    deps.cloud.add_offline_files.assert_not_awaited()


async def test_retry_due_skips_records_still_cooling_down() -> None:
    intake, deps = make_intake(
        ledger=parked_ledger(AcquisitionState.RESOLVE_FAILED, due=False),
        sukebei_magnet=MAGNET_A,
    )

    processed = await intake.retry_due(ctx=make_ctx())

    assert processed == 0
    deps.sukebei.get_magnet.assert_not_awaited()
    assert deps.ledger.states['ABC-123'] is AcquisitionState.RESOLVE_FAILED


async def test_resolve_slots_the_item_magnet_between_sukebei_and_javbus() -> None:
    intake, _ = make_intake(
        sukebei_magnet=MAGNET_A,
        javbus_magnets=[{'magnet': MAGNET_C, 'size_int': 5}],
    )

    candidates = await intake.resolve('ABC-123', ctx=make_ctx(), item_magnet=MAGNET_B)

    assert [candidate.magnet for candidate in candidates] == [MAGNET_A, MAGNET_B, MAGNET_C]
    assert [candidate.source for candidate in candidates] == ['sukebei', 'rss_item', 'javbus']


async def test_a_sighting_with_a_magnet_wakes_a_cooling_row() -> None:
    intake, deps = make_intake(ledger=parked_ledger(AcquisitionState.RESOLVE_FAILED, due=False))

    outcome = await intake.enqueue(
        'ABC-123',
        source=AcquisitionSource.MANUAL,
        task_dir_path=TASK_DIR,
        ctx=make_ctx(),
        item_magnet=MAGNET_A,
    )

    assert outcome is IntakeOutcome.SUBMITTED
    deps.cloud.add_offline_files.assert_awaited_once_with([MAGNET_A], TASK_DIR)
    assert deps.ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING
    assert deps.ledger.attempt_states('ABC-123') == [AttemptState.SUBMITTED]


async def test_a_sighting_without_a_magnet_leaves_a_cooling_row_alone() -> None:
    intake, deps = make_intake(
        ledger=parked_ledger(AcquisitionState.RESOLVE_FAILED, due=False),
        sukebei_magnet=MAGNET_A,
    )

    outcome = await enqueue(intake)

    assert outcome is IntakeOutcome.ALREADY_TRACKED
    deps.sukebei.get_magnet.assert_not_awaited()


async def test_a_woken_row_that_resolves_nothing_is_reparked_from_its_own_state() -> None:
    # A magnet without a usable hash wakes the row but cannot be submitted; the
    # park must start from resolve_failed, not assume a fresh discovery.
    ledger = parked_ledger(AcquisitionState.RESOLVE_FAILED, due=False)
    stale_deadline = ledger.next_action_at['ABC-123']
    intake, deps = make_intake(ledger=ledger)

    outcome = await intake.enqueue(
        'ABC-123',
        source=AcquisitionSource.MANUAL,
        task_dir_path=TASK_DIR,
        ctx=make_ctx(),
        item_magnet='magnet:?dn=ABC-123',
    )

    assert outcome is IntakeOutcome.NO_MAGNET
    assert deps.ledger.states['ABC-123'] is AcquisitionState.RESOLVE_FAILED
    assert deps.ledger.next_action_at['ABC-123'] != stale_deadline
    assert deps.ledger.next_action_at['ABC-123'] > datetime.now(UTC)


async def test_park_follows_the_release_schedule_when_the_date_is_known() -> None:
    intake, deps = make_intake()
    release = (datetime.now(UTC) + timedelta(days=60)).date()

    outcome = await intake.enqueue(
        'ABC-123',
        source=AcquisitionSource.FILL_ACTOR,
        task_dir_path=TASK_DIR,
        ctx=make_ctx(),
        release_date=release,
    )

    assert outcome is IntakeOutcome.NO_MAGNET
    assert deps.ledger.release_dates['ABC-123'] == release
    # Weeks ahead of the release: a weekly probe, not the one-hour fallback cooldown.
    wait = deps.ledger.next_action_at['ABC-123'] - datetime.now(UTC)
    assert timedelta(days=6, hours=23) < wait <= timedelta(days=7)


async def test_queue_stores_the_release_date() -> None:
    intake, deps = make_intake()
    release = date(2026, 10, 2)

    await intake.queue(
        'ABC-123',
        source=AcquisitionSource.FILL_ACTOR,
        task_dir_path=TASK_DIR,
        ctx=make_ctx(),
        release_date=release,
    )

    assert deps.ledger.release_dates['ABC-123'] == release


# -- release dates filled in at parking time ---------------------------------


async def test_parking_a_row_without_a_release_date_looks_it_up_and_anchors_the_schedule() -> None:
    asked: list[str] = []

    async def lookup(avid: str) -> date | None:
        asked.append(avid)
        return datetime.now(UTC).date() + timedelta(days=30)

    intake, deps = make_intake(release_date_lookup=lookup)

    assert await enqueue(intake) is IntakeOutcome.NO_MAGNET

    assert asked == ['ABC-123']
    assert deps.ledger.release_dates['ABC-123'] == datetime.now(UTC).date() + timedelta(days=30)
    # A month out: the weekly preheat, not the one-hour fallback cooldown.
    wait = deps.ledger.next_action_at['ABC-123'] - datetime.now(UTC)
    assert timedelta(days=6, hours=23) < wait <= timedelta(days=7)


async def test_a_row_that_already_has_a_release_date_is_not_looked_up() -> None:
    lookup = AsyncMock(return_value=date(2026, 1, 1))
    intake, deps = make_intake(release_date_lookup=lookup)

    await intake.enqueue(
        'ABC-123', source='rss:Actor', task_dir_path=TASK_DIR, ctx=make_ctx(), release_date=date(2030, 1, 1)
    )

    lookup.assert_not_called()
    assert deps.ledger.release_dates['ABC-123'] == date(2030, 1, 1)


async def test_an_unknown_release_date_keeps_the_fallback_cooldown() -> None:
    intake, deps = make_intake(release_date_lookup=AsyncMock(return_value=None))

    assert await enqueue(intake) is IntakeOutcome.NO_MAGNET

    assert deps.ledger.release_dates['ABC-123'] is None
    wait = deps.ledger.next_action_at['ABC-123'] - datetime.now(UTC)
    assert timedelta(minutes=59) < wait <= timedelta(hours=1)


async def test_a_retry_fills_in_the_release_date_of_an_old_row() -> None:
    ledger = FakeLedger()
    await ledger.discover('ABC-123', source='rss:Actor', now=datetime.now(UTC), task_dir_path=TASK_DIR)
    released = datetime.now(UTC).date() - timedelta(days=3)
    intake, deps = make_intake(ledger=ledger, release_date_lookup=AsyncMock(return_value=released))

    record = await ledger.get('ABC-123')
    assert record is not None
    assert await intake.retry(record, ctx=make_ctx()) is IntakeOutcome.NO_MAGNET

    assert deps.ledger.release_dates['ABC-123'] == released
    # Inside the release window: four-hourly.
    wait = deps.ledger.next_action_at['ABC-123'] - datetime.now(UTC)
    assert timedelta(hours=3, minutes=59) < wait <= timedelta(hours=4)


async def test_javbus_not_listing_a_work_yet_is_not_an_error() -> None:
    request = httpx.Request('GET', 'https://www.javbus.com/ABC-123')
    missing = httpx.HTTPStatusError('404', request=request, response=httpx.Response(404, request=request))
    intake, _ = make_intake(javbus_error=missing)
    ctx = make_ctx()

    assert await intake.resolve('ABC-123', ctx=ctx) == []
    assert len(ctx.errors) == 0

    broken = httpx.HTTPStatusError('503', request=request, response=httpx.Response(503, request=request))
    intake, _ = make_intake(javbus_error=broken)
    ctx = make_ctx()

    assert await intake.resolve('ABC-123', ctx=ctx) == []
    assert len(ctx.errors) == 1
