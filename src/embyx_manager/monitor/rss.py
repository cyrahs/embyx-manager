"""RSS ingestion pipeline: unread FreshRSS items -> magnets -> CloudDrive offline tasks.

Ported from embyx-monitor's rss.py. Magnet resolution tries sukebei search
first (most reliable for recent releases), then the magnet table embedded in
the RSS item, then javbus as a last resort. Failed AVIDs go on a persisted
cooldown so restarts do not retry them immediately.
"""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import grpc
import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from embyx_manager.clients.clouddrive import AsyncCloudDrive
from embyx_manager.clients.freshrss import FreshRSSClient
from embyx_manager.clients.javbus import JavBusClient
from embyx_manager.clients.rss_magnet import get_magnet_from_item
from embyx_manager.clients.sukebei import SukebeiClient
from embyx_manager.config.models import RssConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.reports import RunContext

MAGNET_BATCH_SIZE = 20
POST_ADD_SLEEP_SECONDS = 10

CLOUD_RETRY = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((grpc.RpcError, httpx.HTTPError, httpx.TimeoutException, OSError)),
    reraise=True,
)


class RssPipeline:
    def __init__(  # noqa: PLR0913
        self,
        *,
        config: RssConfig,
        avid_parser: AvidParser,
        freshrss: FreshRSSClient,
        cloud: AsyncCloudDrive,
        sukebei: SukebeiClient,
        javbus: JavBusClient,
        task_dir_path: str,
        cooldown_lookup: Callable[[datetime], Awaitable[frozenset[str]]],
        cooldown_record: Callable[[set[str], datetime], Awaitable[None]],
    ) -> None:
        self._config = config
        self._avid = avid_parser
        self._freshrss = freshrss
        self._cloud = cloud
        self._sukebei = sukebei
        self._javbus = javbus
        self._task_dir_path = task_dir_path
        self._cooldown_lookup = cooldown_lookup
        self._cooldown_record = cooldown_record

    async def run(self, ctx: RunContext, *, rank: bool = False) -> None:
        label = self._config.rank_label if rank else self._config.actor_label
        items = await self._freshrss.get_items(label)
        ctx.set('items', len(items))
        ctx.info('Find %d items in %s', len(items), label)
        if not items:
            await self._refresh_finished_magnets(ctx)
            return

        avid_item: dict[str, list[dict]] = {}
        for item in items:
            avid = self._avid.get_avid(item['title'])
            if not avid:
                ctx.warning('Failed to get avid for %s', item['title'])
                continue
            avid_item.setdefault(avid, []).append(item)
        ctx.set('unique_avids', len(avid_item))
        ctx.info('Find %d unique avids in %s', len(avid_item), label)

        now = datetime.now(UTC)
        cooldown = await self._cooldown_lookup(now)
        active_avid_item = {avid: avid_items for avid, avid_items in avid_item.items() if avid not in cooldown}
        skipped = len(avid_item) - len(active_avid_item)
        if skipped:
            ctx.set('skipped_cooldown', skipped)
            ctx.info('Skipping %d avids due to cooldown', skipped)
        if not active_avid_item:
            await self._refresh_finished_magnets(ctx)
            return

        ctx.check_cancelled()
        avid_magnet: dict[str, str] = {}
        await asyncio.gather(
            *(
                self._get_magnet_safely(avid, avid_items, avid_magnet, ctx)
                for avid, avid_items in active_avid_item.items()
            ),
        )
        ctx.set('magnets_found', len(avid_magnet))
        ctx.info('Found %d magnets', len(avid_magnet))

        failed_avids = {avid for avid in active_avid_item if avid not in avid_magnet}
        if failed_avids:
            ctx.set('magnets_failed', len(failed_avids))
            ctx.warning('Failed to get magnets for %d avids: %s', len(failed_avids), ' '.join(sorted(failed_avids)))
            await self._cooldown_record(failed_avids, datetime.now(UTC))

        ctx.check_cancelled()
        await self._add_magnets_and_read(avid_magnet, active_avid_item, ctx)
        await self._refresh_finished_magnets(ctx)

    # -- magnet resolution ---------------------------------------------------

    async def _get_magnet_safely(
        self,
        avid: str,
        items: list[dict],
        avid_magnet: dict[str, str],
        ctx: RunContext,
    ) -> None:
        try:
            await self._get_magnet(avid, items, avid_magnet, ctx)
        except Exception:  # noqa: BLE001
            ctx.exception('Failed to get magnet for %s', avid)

    async def _get_magnet(
        self,
        avid: str,
        items: list[dict],
        avid_magnet: dict[str, str],
        ctx: RunContext,
    ) -> None:
        # first try searching because most tasks are recent videos; sukebei is more reliable
        link = await self._sukebei.get_magnet(avid)
        if link:
            avid_magnet[avid] = link
            return
        link = get_magnet_from_item(items[0], avid)
        if link:
            avid_magnet[avid] = link
            return
        try:
            magnets = await self._javbus.get_magnets(avid)
            if magnets:
                best = max(magnets, key=lambda x: x['size_int'])
                avid_magnet[avid] = best['magnet']
                return
        except Exception:  # noqa: BLE001
            ctx.exception('Failed to get magnet from javbus for %s', avid)
        # leave one item unread when no magnet was found
        if len(items) == 1:
            ctx.warning('Failed to get magnet for %s', items[0]['title'])
        else:
            ctx.warning(
                'Failed to get magnet for %s. Found %d items, leaving 1 unread.',
                items[0]['title'],
                len(items),
            )
            item_ids = [item['id'] for item in items[1:]]
            try:
                await self._freshrss.read_items(item_ids)
            except Exception:  # noqa: BLE001
                ctx.exception('Failed to mark %d items as read', len(item_ids))

    # -- CloudDrive offline tasks ---------------------------------------------

    async def _add_magnets_and_read(
        self,
        avid_magnet: dict[str, str],
        avid_item: dict[str, list[dict]],
        ctx: RunContext,
    ) -> None:
        magnets = list(avid_magnet.values())
        avids = list(avid_magnet.keys())
        for i in range(0, len(magnets), MAGNET_BATCH_SIZE):
            ctx.check_cancelled()
            magnets_batch = magnets[i : i + MAGNET_BATCH_SIZE]
            avid_batch = avids[i : i + MAGNET_BATCH_SIZE]
            mark_as_read_item_ids: list[str] = []
            for avid, link in zip(avid_batch, magnets_batch, strict=True):
                outcome = await self._add_magnet(avid, link, ctx)
                if outcome in {'success', 'duplicate'}:
                    mark_as_read_item_ids.extend([item['id'] for item in avid_item[avid]])
            if mark_as_read_item_ids:
                try:
                    await self._freshrss.read_items(mark_as_read_item_ids)
                    ctx.add('items_marked_read', len(mark_as_read_item_ids))
                except Exception:  # noqa: BLE001
                    ctx.exception('Failed to mark %d items as read', len(mark_as_read_item_ids))
        if magnets and not ctx.stop_requested:
            ctx.info('Waiting %d seconds for offline tasks to register', POST_ADD_SLEEP_SECONDS)
            await asyncio.sleep(POST_ADD_SLEEP_SECONDS)

    async def _add_magnet(self, avid: str, link: str, ctx: RunContext) -> str:
        if not link.lower().startswith('magnet:'):
            ctx.error('Magnet link must start with "magnet:", got %s', link)
            return 'failed'
        try:
            result = await self._add_offline_with_retry(link)
        except grpc.RpcError as exc:
            if '任务已存在' in (exc.details() or ''):
                ctx.warning('Duplicate magnet for %s', avid)
                ctx.add('duplicates')
                return 'duplicate'
            ctx.exception('Failed to add magnet for %s: %s', avid, exc.details())
            ctx.add('add_failed')
            return 'failed'
        except Exception:  # noqa: BLE001
            ctx.exception('Failed to add magnet for %s', avid)
            ctx.add('add_failed')
            return 'failed'
        if getattr(result, 'success', False):
            ctx.info('Added magnet for %s', avid)
            ctx.add('magnets_added')
            return 'success'
        ctx.error('Failed to add magnet for %s: %s', avid, result)
        ctx.add('add_failed')
        return 'failed'

    @CLOUD_RETRY
    async def _add_offline_with_retry(self, link: str) -> object:
        return await self._cloud.add_offline_files([link], self._task_dir_path)

    async def _refresh_finished_magnets(self, ctx: RunContext) -> None:
        ctx.info('Refreshing finished magnets')
        try:
            await self._refresh_task_dir()
            targets = await self._list_finished_targets()
        except Exception:  # noqa: BLE001
            ctx.exception('Failed to list finished offline files')
            return
        all_success = True
        for target in targets:
            ctx.check_cancelled()
            name = getattr(target, 'name', '')
            try:
                ctx.info('Refreshing %s', name)
                await self._refresh_finished_target(name)
                ctx.add('finished_refreshed')
            except FileNotFoundError:
                ctx.warning('Path not found, skip: %s', name)
                continue
            except NotADirectoryError:
                ctx.warning('Not a directory, skip: %s', name)
                continue
            except Exception:  # noqa: BLE001
                ctx.exception('Failed to refresh %s', name)
                all_success = False
                continue
        if all_success and targets:
            ctx.info('Clearing finished magnet records')
            try:
                await self._clear_finished_magnets()
            except Exception:  # noqa: BLE001
                ctx.exception('Failed to clear finished offline files')

    @CLOUD_RETRY
    async def _refresh_task_dir(self) -> None:
        await self._cloud.list_directory(self._task_dir_path)

    @CLOUD_RETRY
    async def _list_finished_targets(self) -> tuple[object, ...]:
        return tuple(await self._cloud.list_finished_offline_files(self._task_dir_path))

    @CLOUD_RETRY
    async def _refresh_finished_target(self, name: str) -> None:
        await self._cloud.list_directory(f'{self._task_dir_path}/{name}')

    @CLOUD_RETRY
    async def _clear_finished_magnets(self) -> None:
        await self._cloud.clear_finished_offline_files(self._task_dir_path)
