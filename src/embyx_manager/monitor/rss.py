"""RSS discovery: unread FreshRSS items -> AVIDs -> magnet candidates -> offline tasks.

This pipeline only discovers. It reads unread items, records each AVID in the
acquisition ledger, collects every magnet worth trying, and submits the first
one; what happens to that download afterwards belongs to the tracker.

Two consequences of the ledger owning the outcome. Items are marked read as soon
as their AVID is recorded, because retries are driven by the ledger's cooldown
rather than by an item staying unread until the next run. And magnets are
resolved as a ranked list rather than a single pick, so a magnet that errors,
stalls, or turns out to be an ad reel has a successor waiting.

One run covers every configured category in turn. A category names a FreshRSS
label and the offline directory its downloads belong in, which is how one feed
category ends up in a library subdirectory of its own; categories are otherwise
independent, so one failing does not cost the others their pass.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import grpc
import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from embyx_manager.clients.clouddrive import AsyncCloudDrive
from embyx_manager.clients.freshrss import FreshRSSClient
from embyx_manager.clients.javbus import JavBusClient
from embyx_manager.clients.rss_magnet import get_magnet_from_item
from embyx_manager.clients.sukebei import SukebeiClient
from embyx_manager.config.models import RssCategory, RssConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.core.magnet import extract_info_hash
from embyx_manager.monitor.acquisitions import (
    AcquisitionRepository,
    AcquisitionState,
    AttemptState,
    MagnetCandidate,
    rss_source,
)
from embyx_manager.monitor.reports import RunCancelledError, RunContext

MAX_CANDIDATES = 5

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
        ledger: AcquisitionRepository,
    ) -> None:
        self._config = config
        self._avid = avid_parser
        self._freshrss = freshrss
        self._cloud = cloud
        self._sukebei = sukebei
        self._javbus = javbus
        self._ledger = ledger

    async def run(self, ctx: RunContext) -> None:
        """Ingest every configured category."""
        for category in self._config.categories:
            ctx.check_cancelled()
            try:
                await self._run_category(ctx, category)
            except RunCancelledError:
                raise
            except Exception:  # noqa: BLE001 - one category's failure is not the run's
                ctx.exception('Failed to ingest the %s category', category.label)
                ctx.add('categories_failed')

    async def _run_category(self, ctx: RunContext, category: RssCategory) -> None:
        label = category.label
        source = rss_source(label)
        # Pinned onto each acquisition so its retries keep landing here even if
        # the category is later repointed or removed.
        task_dir = category.task_dir_path
        items = await self._freshrss.get_items(label)
        ctx.add('items', len(items))
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
        ctx.add('unique_avids', len(avid_item))
        ctx.info('Find %d unique avids in %s', len(avid_item), label)

        now = datetime.now(UTC)
        wanted: dict[str, list[dict]] = {}
        for avid, avid_items in avid_item.items():
            if await self._ledger.discover(avid, source=source, now=now, task_dir_path=task_dir):
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
        await self._submit_all(resolved, task_dir, ctx)

    # -- magnet resolution ---------------------------------------------------

    async def _resolve_all(
        self,
        avid_item: dict[str, list[dict]],
        ctx: RunContext,
    ) -> dict[str, list[MagnetCandidate]]:
        resolved: dict[str, list[MagnetCandidate]] = {}
        await asyncio.gather(*(self._resolve_safely(avid, items, resolved, ctx) for avid, items in avid_item.items()))
        ctx.add('magnets_found', sum(len(candidates) for candidates in resolved.values()))
        ctx.info('Found magnets for %d of %d avids', len(resolved), len(avid_item))

        now = datetime.now(UTC)
        cooldown = timedelta(seconds=self._config.failed_avid_cooldown_seconds)
        for avid in avid_item:
            if avid in resolved:
                continue
            ctx.add('magnets_failed')
            ctx.warning('Failed to get any magnet for %s', avid)
            await self._ledger.transition(
                avid,
                expected=AcquisitionState.DISCOVERED,
                target=AcquisitionState.RESOLVE_FAILED,
                now=now,
                note='no magnet found',
                next_action_at=now + cooldown,
            )
        return resolved

    async def _resolve_safely(
        self,
        avid: str,
        items: list[dict],
        resolved: dict[str, list[MagnetCandidate]],
        ctx: RunContext,
    ) -> None:
        try:
            candidates = await self._candidates_for(avid, items, ctx)
        except Exception:  # noqa: BLE001
            ctx.exception('Failed to get magnets for %s', avid)
            return
        if candidates:
            resolved[avid] = candidates

    async def _candidates_for(self, avid: str, items: list[dict], ctx: RunContext) -> list[MagnetCandidate]:
        """Every magnet worth trying for one AVID, best first.

        Sukebei leads because most items are recent releases it indexes well; the
        item's own magnet table and javbus fill in behind it. Only tracked hashes
        are kept: a magnet CloudDrive cannot report back on is one the tracker
        could never conclude.
        """
        candidates: list[MagnetCandidate] = []
        seen: set[str] = set()

        def collect(magnet: str | None, source: str, size_hint: int | None = None) -> None:
            if not magnet or not magnet.lower().startswith('magnet:'):
                return
            info_hash = extract_info_hash(magnet)
            if info_hash is None:
                ctx.warning('Skipping magnet without a usable info hash for %s', avid)
                return
            if info_hash in seen:
                return
            seen.add(info_hash)
            candidates.append(MagnetCandidate(magnet=magnet, info_hash=info_hash, source=source, size_hint=size_hint))

        collect(await self._sukebei.get_magnet(avid), 'sukebei')
        collect(get_magnet_from_item(items[0], avid), 'rss_item')
        try:
            magnets = await self._javbus.get_magnets(avid)
        except Exception:  # noqa: BLE001
            ctx.exception('Failed to get magnets from javbus for %s', avid)
        else:
            for magnet in sorted(magnets, key=lambda entry: entry['size_int'], reverse=True):
                collect(magnet['magnet'], 'javbus', magnet['size_int'])
        return candidates[:MAX_CANDIDATES]

    # -- CloudDrive offline tasks ---------------------------------------------

    async def _submit_all(self, resolved: dict[str, list[MagnetCandidate]], task_dir: str, ctx: RunContext) -> None:
        now = datetime.now(UTC)
        for avid, candidates in resolved.items():
            ctx.check_cancelled()
            added = await self._ledger.add_attempts(avid, candidates, now=now)
            ctx.add('candidates_recorded', added)
            await self._submit_next(avid, task_dir, ctx)

    async def _submit_next(self, avid: str, task_dir: str, ctx: RunContext) -> bool:
        """Submit this AVID's next untried magnet; False when none is left."""
        now = datetime.now(UTC)
        attempt = await self._ledger.claim_next_pending(avid, now=now)
        if attempt is None:
            return False
        outcome = await self._add_magnet(avid, attempt.magnet, task_dir, ctx)
        if outcome == 'failed':
            await self._ledger.transition_attempt(
                avid,
                attempt.attempt_no,
                expected=AttemptState.SUBMITTED,
                target=AttemptState.ERROR,
                now=now,
                error='failed to add the offline task',
            )
            return await self._submit_next(avid, task_dir, ctx)
        record = await self._ledger.get(avid)
        if record is not None and record.state is not AcquisitionState.DOWNLOADING:
            await self._ledger.transition(
                avid,
                expected=record.state,
                target=AcquisitionState.DOWNLOADING,
                now=now,
            )
        return True

    async def _add_magnet(self, avid: str, link: str, task_dir: str, ctx: RunContext) -> str:
        try:
            result = await self._add_offline_with_retry(link, task_dir)
        except grpc.RpcError as exc:
            if '任务已存在' in (exc.details() or ''):
                # Already queued at CloudDrive: the hash is what we track, so this
                # attempt is live either way.
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
    async def _add_offline_with_retry(self, link: str, task_dir: str) -> object:
        return await self._cloud.add_offline_files([link], task_dir)

    async def _mark_read(self, ctx: RunContext, item_ids: list[str]) -> None:
        if not item_ids:
            return
        try:
            await self._freshrss.read_items(item_ids)
            ctx.add('items_marked_read', len(item_ids))
        except Exception:  # noqa: BLE001
            ctx.exception('Failed to mark %d items as read', len(item_ids))
