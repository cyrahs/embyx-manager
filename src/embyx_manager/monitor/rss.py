"""RSS discovery: unread FreshRSS items -> AVIDs -> magnet candidates -> offline tasks.

This pipeline only discovers. It reads unread items, records each AVID in the
acquisition ledger, and hands resolution and submission to the shared intake;
what happens to that download afterwards belongs to the tracker.

Two consequences of the ledger owning the outcome. Items are marked read as soon
as their AVID is recorded, because retries are driven by the ledger's cooldown
rather than by an item staying unread until the next run. And magnets are
resolved as a ranked list rather than a single pick, so a magnet that errors,
stalls, or turns out to be an ad reel has a successor waiting.
"""

import asyncio
from datetime import UTC, datetime

from embyx_manager.clients.clouddrive import AsyncCloudDrive
from embyx_manager.clients.freshrss import FreshRSSClient
from embyx_manager.clients.javbus import JavBusClient
from embyx_manager.clients.rss_magnet import get_magnet_from_item
from embyx_manager.clients.sukebei import SukebeiClient
from embyx_manager.config.models import RssConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.acquisitions import (
    AcquisitionRepository,
    AcquisitionSource,
    MagnetCandidate,
)
from embyx_manager.monitor.intake import AcquisitionIntake
from embyx_manager.monitor.reports import RunContext


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
        ledger: AcquisitionRepository,
    ) -> None:
        self._config = config
        self._avid = avid_parser
        self._freshrss = freshrss
        self._ledger = ledger
        self._intake = AcquisitionIntake(
            ledger=ledger,
            sukebei=sukebei,
            javbus=javbus,
            cloud=cloud,
            task_dir_path=task_dir_path,
            failed_cooldown_seconds=config.failed_avid_cooldown_seconds,
        )

    async def run(self, ctx: RunContext, *, rank: bool = False) -> None:
        label = self._config.rank_label if rank else self._config.actor_label
        source = AcquisitionSource.RSS_RANK if rank else AcquisitionSource.RSS_ACTOR
        items = await self._freshrss.get_items(label)
        ctx.set('items', len(items))
        ctx.info('Find %d items in %s', len(items), label)
        if not items:
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
        wanted: dict[str, list[dict]] = {}
        for avid, avid_items in avid_item.items():
            if await self._ledger.discover(avid, source=source, now=now):
                wanted[avid] = avid_items
            else:
                ctx.add('skipped_known')
        if len(wanted) != len(avid_item):
            ctx.info('Skipping %d avids already tracked', len(avid_item) - len(wanted))

        # Every recognized AVID is in the ledger now, so its items have done their
        # job whatever happens next; leaving them unread would only re-read them.
        await self._mark_read(ctx, [item['id'] for avid_items in avid_item.values() for item in avid_items])
        if not wanted:
            return

        ctx.check_cancelled()
        resolved = await self._resolve_all(wanted, ctx)
        ctx.check_cancelled()
        await self._submit_all(resolved, ctx)

    # -- magnet resolution ---------------------------------------------------

    async def _resolve_all(
        self,
        avid_item: dict[str, list[dict]],
        ctx: RunContext,
    ) -> dict[str, list[MagnetCandidate]]:
        resolved: dict[str, list[MagnetCandidate]] = {}
        await asyncio.gather(*(self._resolve_safely(avid, items, resolved, ctx) for avid, items in avid_item.items()))
        ctx.set('magnets_found', sum(len(candidates) for candidates in resolved.values()))
        ctx.info('Found magnets for %d of %d avids', len(resolved), len(avid_item))

        for avid in avid_item:
            if avid in resolved:
                continue
            ctx.add('magnets_failed')
            ctx.warning('Failed to get any magnet for %s', avid)
            await self._intake.park_unresolved(avid)
        return resolved

    async def _resolve_safely(
        self,
        avid: str,
        items: list[dict],
        resolved: dict[str, list[MagnetCandidate]],
        ctx: RunContext,
    ) -> None:
        try:
            candidates = await self._intake.resolve(avid, ctx=ctx, item_magnet=get_magnet_from_item(items[0], avid))
        except Exception:  # noqa: BLE001
            ctx.exception('Failed to get magnets for %s', avid)
            return
        if candidates:
            resolved[avid] = candidates

    # -- CloudDrive offline tasks ---------------------------------------------

    async def _submit_all(self, resolved: dict[str, list[MagnetCandidate]], ctx: RunContext) -> None:
        for avid, candidates in resolved.items():
            ctx.check_cancelled()
            await self._intake.record_and_submit(avid, candidates, ctx=ctx)

    async def _mark_read(self, ctx: RunContext, item_ids: list[str]) -> None:
        if not item_ids:
            return
        try:
            await self._freshrss.read_items(item_ids)
            ctx.add('items_marked_read', len(item_ids))
        except Exception:  # noqa: BLE001
            ctx.exception('Failed to mark %d items as read', len(item_ids))
