"""Replacing a title the library holds (换版): what is planned and what reaches CloudDrive."""

import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.acquisitions import AcquisitionState, AttemptState, MagnetCandidate
from embyx_manager.monitor.intake import AcquisitionIntake
from embyx_manager.monitor.replace import ReplaceError, ReplacementSource, ReplaceOutcome
from tests.test_monitor_intake import MAGNET_A
from tests.test_monitor_manual import INBOX, RANK_DIR, FakeCloud, make_archiver
from tests.test_monitor_rss import HASH_A, HASH_B, FakeLedger, now_stub

MAGNET_B = f'magnet:?xt=urn:btih:{HASH_B}'


def make_replacer(
    tmp_path: Path,
    *,
    ledger: FakeLedger | None = None,
    magnet: str | None = MAGNET_A,
    configured_dirs: tuple[str, ...] = (INBOX, RANK_DIR),
    submit_ok: bool = True,
) -> tuple[ReplacementSource, SimpleNamespace]:
    deps = SimpleNamespace(ledger=ledger or FakeLedger(), cloud=FakeCloud(), submitted=[])
    deps.intake = AcquisitionIntake(
        ledger=deps.ledger,
        sukebei=SimpleNamespace(get_magnet=AsyncMock(return_value=magnet)),
        javbus=SimpleNamespace(get_magnets=AsyncMock(return_value=[])),
        cloud=deps.cloud,
        failed_cooldown_seconds=3600,
    )

    async def submit(avid: str, link: str) -> bool:
        deps.submitted.append((avid, link))
        return submit_ok

    source = ReplacementSource(
        ledger=deps.ledger,
        intake_factory=lambda: deps.intake,
        archiver_factory=lambda: make_archiver(tmp_path),
        parser_factory=AvidParser,
        configured_dirs=lambda: configured_dirs,
        submit_magnet=submit,
        logger=logging.getLogger('test-replace'),
    )
    return source, deps


def hold(tmp_path: Path, *relative: str) -> None:
    for path in relative:
        file = tmp_path / 'library' / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(b'old')


async def test_plan_names_the_copy_to_delete_and_where_the_new_one_comes_from(tmp_path: Path) -> None:
    hold(tmp_path, 'embyx/rank/IPZ/IPZ-661.mp4')
    source, _ = make_replacer(tmp_path)

    plan = await source.plan('ipz-661')

    assert plan.avid == 'IPZ-661'
    assert plan.holdings == ('embyx/rank/IPZ/IPZ-661.mp4',)
    assert plan.state is None
    # The rank route feeds the directory the old copy sits in.
    assert plan.task_dir_path == RANK_DIR
    assert plan.blocked is None


async def test_plan_reports_what_blocks_a_replacement(tmp_path: Path) -> None:
    ledger = FakeLedger(known={'ABC-123': AcquisitionState.DOWNLOADING})
    hold(tmp_path, 'embyx/ABC/ABC-123.mp4')
    source, _ = make_replacer(tmp_path, ledger=ledger)

    assert (await source.plan('ABC-123')).blocked == 'acquisition_active'
    assert (await source.plan('XYZ-001')).blocked == 'not_in_library'
    with pytest.raises(ReplaceError) as caught:
        await source.plan('hello world')
    assert caught.value.code == 'unreadable_avid'


async def test_start_resolves_a_fresh_magnet_and_keeps_the_old_copy_until_filing(tmp_path: Path) -> None:
    hold(tmp_path, 'embyx/ABC/ABC-123.mp4')
    source, deps = make_replacer(tmp_path)

    result = await source.start('ABC-123')

    assert result.outcome is ReplaceOutcome.SUBMITTED
    assert result.record.state is AcquisitionState.DOWNLOADING
    assert result.record.replace_paths == ('embyx/ABC/ABC-123.mp4',)
    assert deps.cloud.submitted == [([MAGNET_A], INBOX)]
    assert (tmp_path / 'library' / 'embyx' / 'ABC' / 'ABC-123.mp4').exists()


async def test_start_submits_the_pasted_magnet_ahead_of_older_candidates(tmp_path: Path) -> None:
    hold(tmp_path, 'embyx/ABC/ABC-123.mp4')
    ledger = FakeLedger(known={'ABC-123': AcquisitionState.ARCHIVED})
    await ledger.add_attempts(
        'ABC-123', [MagnetCandidate(magnet=MAGNET_A, info_hash=HASH_A, source='javbus')], now=now_stub()
    )
    source, deps = make_replacer(tmp_path, ledger=ledger)

    result = await source.start('ABC-123', magnet=MAGNET_B)

    assert result.outcome is ReplaceOutcome.SUBMITTED
    assert deps.submitted == [('ABC-123', MAGNET_B)]
    assert ledger.attempt_states('ABC-123') == [AttemptState.PENDING, AttemptState.SUBMITTED]
    assert ledger.states['ABC-123'] is AcquisitionState.DOWNLOADING


async def test_start_refuses_a_magnet_already_tried_before_writing_anything(tmp_path: Path) -> None:
    hold(tmp_path, 'embyx/ABC/ABC-123.mp4')
    ledger = FakeLedger(known={'ABC-123': AcquisitionState.ARCHIVED})
    await ledger.add_attempts(
        'ABC-123', [MagnetCandidate(magnet=MAGNET_A, info_hash=HASH_A, source='sukebei')], now=now_stub()
    )
    source, _ = make_replacer(tmp_path, ledger=ledger)

    for magnet, code in ((MAGNET_A, 'magnet_already_tried'), ('not a magnet', 'unusable_magnet')):
        with pytest.raises(ReplaceError) as caught:
            await source.start('ABC-123', magnet=magnet)
        assert caught.value.code == code
    assert ledger.states['ABC-123'] is AcquisitionState.ARCHIVED
    assert ledger.replace_paths == {}


async def test_start_without_a_magnet_parks_the_replacement_for_later(tmp_path: Path) -> None:
    hold(tmp_path, 'embyx/ABC/ABC-123.mp4')
    source, deps = make_replacer(tmp_path, magnet=None)

    result = await source.start('ABC-123')

    assert result.outcome is ReplaceOutcome.NO_MAGNET
    assert result.record.state is AcquisitionState.RESOLVE_FAILED
    assert result.record.next_action_at is not None
    # Still a replacement: a magnet pasted later finishes the job.
    assert result.record.replace_paths == ('embyx/ABC/ABC-123.mp4',)
    assert deps.cloud.submitted == []


async def test_start_refuses_a_title_the_tracker_is_working_on(tmp_path: Path) -> None:
    hold(tmp_path, 'embyx/ABC/ABC-123.mp4')
    source, _ = make_replacer(tmp_path, ledger=FakeLedger(known={'ABC-123': AcquisitionState.DISCOVERED}))

    with pytest.raises(ReplaceError) as caught:
        await source.start('ABC-123')
    assert caught.value.code == 'acquisition_active'
