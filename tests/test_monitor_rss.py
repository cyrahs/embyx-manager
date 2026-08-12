import logging
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import grpc

from embyx_manager.config.models import RssConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.reports import RunContext
from embyx_manager.monitor.rss import RssPipeline


def make_ctx() -> RunContext:
    return RunContext(logger=logging.getLogger('test-rss'))


class FakeCooldowns:
    def __init__(self, active: frozenset[str] = frozenset()) -> None:
        self.active = active
        self.recorded: set[str] = set()

    async def lookup(self, _now: datetime) -> frozenset[str]:
        return self.active

    async def record(self, avids: set[str], _now: datetime) -> None:
        self.recorded |= avids


def make_item(item_id: str, title: str, magnet_html: str = '') -> dict:
    return {'id': item_id, 'title': title, 'summary': {'content': magnet_html}}


def make_pipeline(
    *,
    items: list[dict],
    sukebei_magnets: dict[str, str] | None = None,
    javbus_magnets: dict[str, list[dict]] | None = None,
    cooldowns: FakeCooldowns | None = None,
    add_result: object | None = None,
    add_side_effect: Exception | None = None,
) -> tuple[RssPipeline, SimpleNamespace]:
    deps = SimpleNamespace()
    deps.cooldowns = cooldowns or FakeCooldowns()
    deps.freshrss = SimpleNamespace(
        get_items=AsyncMock(return_value=items),
        read_items=AsyncMock(),
    )

    async def sukebei_get(avid: str) -> str | None:
        return (sukebei_magnets or {}).get(avid)

    deps.sukebei = SimpleNamespace(get_magnet=AsyncMock(side_effect=sukebei_get))

    async def javbus_get(avid: str) -> list[dict]:
        return (javbus_magnets or {}).get(avid, [])

    deps.javbus = SimpleNamespace(get_magnets=AsyncMock(side_effect=javbus_get))
    deps.cloud = SimpleNamespace(
        add_offline_files=AsyncMock(
            return_value=add_result or SimpleNamespace(success=True),
            side_effect=add_side_effect,
        ),
        list_directory=AsyncMock(return_value=()),
        list_finished_offline_files=AsyncMock(return_value=[]),
        clear_finished_offline_files=AsyncMock(),
    )
    if add_side_effect is None:
        deps.cloud.add_offline_files = AsyncMock(return_value=add_result or SimpleNamespace(success=True))
    pipeline = RssPipeline(
        config=RssConfig(enabled=True),
        avid_parser=AvidParser(),
        freshrss=deps.freshrss,
        cloud=deps.cloud,
        sukebei=deps.sukebei,
        javbus=deps.javbus,
        task_dir_path='/115/task',
        cooldown_lookup=deps.cooldowns.lookup,
        cooldown_record=deps.cooldowns.record,
    )
    return pipeline, deps


async def test_run_resolves_magnet_via_sukebei_and_marks_read(monkeypatch) -> None:
    monkeypatch.setattr('embyx_manager.monitor.rss.POST_ADD_SLEEP_SECONDS', 0)
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123 release')],
        sukebei_magnets={'ABC-123': 'magnet:?xt=urn:btih:abc&dn=ABC-123'},
    )

    ctx = make_ctx()
    await pipeline.run(ctx)

    deps.cloud.add_offline_files.assert_awaited_once_with(['magnet:?xt=urn:btih:abc&dn=ABC-123'], '/115/task')
    deps.freshrss.read_items.assert_awaited_once_with(['item-1'])
    assert ctx.stats['magnets_found'] == 1
    assert ctx.stats['magnets_added'] == 1
    assert ctx.stats['items_marked_read'] == 1


async def test_run_falls_back_to_rss_item_magnet(monkeypatch) -> None:
    monkeypatch.setattr('embyx_manager.monitor.rss.POST_ADD_SLEEP_SECONDS', 0)
    html = """
    <table><tbody><tr>
      <td><a href="magnet:?xt=urn:btih:fromrss&dn=x">x</a></td>
      <td>2 GiB</td>
    </tr></tbody></table>
    """
    pipeline, deps = make_pipeline(items=[make_item('item-1', 'ABC-123', html)])

    await pipeline.run(make_ctx())

    added = deps.cloud.add_offline_files.await_args.args[0][0]
    assert added == 'magnet:?xt=urn:btih:fromrss&dn=ABC-123'


async def test_run_falls_back_to_javbus_largest(monkeypatch) -> None:
    monkeypatch.setattr('embyx_manager.monitor.rss.POST_ADD_SLEEP_SECONDS', 0)
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123')],
        javbus_magnets={
            'ABC-123': [
                {'magnet': 'magnet:?xt=urn:btih:small&dn=ABC-123', 'size_int': 1},
                {'magnet': 'magnet:?xt=urn:btih:big&dn=ABC-123', 'size_int': 100},
            ],
        },
    )

    await pipeline.run(make_ctx())

    added = deps.cloud.add_offline_files.await_args.args[0][0]
    assert added == 'magnet:?xt=urn:btih:big&dn=ABC-123'


async def test_run_records_failed_avids_for_cooldown() -> None:
    cooldowns = FakeCooldowns()
    pipeline, _deps = make_pipeline(items=[make_item('item-1', 'ABC-123')], cooldowns=cooldowns)

    ctx = make_ctx()
    await pipeline.run(ctx)

    assert cooldowns.recorded == {'ABC-123'}
    assert ctx.stats['magnets_failed'] == 1


async def test_run_skips_avids_on_cooldown() -> None:
    cooldowns = FakeCooldowns(active=frozenset({'ABC-123'}))
    pipeline, deps = make_pipeline(items=[make_item('item-1', 'ABC-123')], cooldowns=cooldowns)

    ctx = make_ctx()
    await pipeline.run(ctx)

    deps.sukebei.get_magnet.assert_not_awaited()
    assert ctx.stats['skipped_cooldown'] == 1


async def test_duplicate_offline_task_still_marks_read(monkeypatch) -> None:
    monkeypatch.setattr('embyx_manager.monitor.rss.POST_ADD_SLEEP_SECONDS', 0)

    class FakeRpcError(grpc.RpcError):
        def details(self) -> str:
            return '任务已存在'

    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123')],
        sukebei_magnets={'ABC-123': 'magnet:?xt=urn:btih:abc&dn=ABC-123'},
        add_side_effect=FakeRpcError(),
    )

    ctx = make_ctx()
    await pipeline.run(ctx)

    deps.freshrss.read_items.assert_awaited_once_with(['item-1'])
    assert ctx.stats['duplicates'] == 1


async def test_multiple_items_same_avid_leave_one_unread_on_failure() -> None:
    pipeline, deps = make_pipeline(
        items=[make_item('item-1', 'ABC-123 a'), make_item('item-2', 'ABC-123 b')],
    )

    await pipeline.run(make_ctx())

    deps.freshrss.read_items.assert_awaited_once_with(['item-2'])


async def test_empty_feed_only_refreshes_finished() -> None:
    pipeline, deps = make_pipeline(items=[])
    deps.cloud.list_finished_offline_files = AsyncMock(
        return_value=[SimpleNamespace(name='ABC-123')],
    )

    ctx = make_ctx()
    await pipeline.run(ctx)

    deps.cloud.list_directory.assert_any_await('/115/task')
    deps.cloud.list_directory.assert_any_await('/115/task/ABC-123')
    deps.cloud.clear_finished_offline_files.assert_awaited_once_with('/115/task')
    assert ctx.stats['finished_refreshed'] == 1
    deps.sukebei.get_magnet.assert_not_awaited()
