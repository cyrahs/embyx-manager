import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

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
    add_side_effect: Exception | list[object] | None = None,
) -> tuple[AcquisitionIntake, SimpleNamespace]:
    deps = SimpleNamespace()
    deps.ledger = ledger or FakeLedger()
    if sukebei_error is not None:
        deps.sukebei = SimpleNamespace(get_magnet=AsyncMock(side_effect=sukebei_error))
    else:
        deps.sukebei = SimpleNamespace(get_magnet=AsyncMock(return_value=sukebei_magnet))
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
