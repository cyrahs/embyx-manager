"""Subscription polling: feed items -> AVIDs -> magnet candidates -> offline tasks.

This pipeline only discovers. It polls every enabled subscription, records each
AVID in the acquisition ledger, and hands resolution and submission to the
shared intake; what happens to that download afterwards belongs to the tracker.

Two consequences of the ledger owning the outcome. Each subscription remembers
a bounded cursor of the item keys it has seen, only so an item is not re-read on
every poll; retries are driven by the ledger's schedule, never by an item
lingering in a feed. And magnets are resolved as a ranked list rather than a
single pick, so a magnet that errors, stalls, or turns out to be an ad reel has
a successor waiting.

One run covers every configured category in turn, and every enabled
subscription in it. A category names the offline directory its downloads belong
in, which is how one group of feeds ends up in a library subdirectory of its
own; subscriptions are otherwise independent, so one failing does not cost the
others their pass.
"""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from embyx_manager.clients.clouddrive import AsyncCloudDrive
from embyx_manager.clients.javbus import JavBusClient
from embyx_manager.clients.rss_magnet import get_magnet_from_html
from embyx_manager.clients.sukebei import SukebeiClient
from embyx_manager.config.models import RssCategory, RssConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.acquisitions import (
    AcquisitionRepository,
    AcquisitionState,
    MagnetCandidate,
    rss_source,
)
from embyx_manager.monitor.archive import ArchivePipeline
from embyx_manager.monitor.feeds import FeedItem, avid_candidate_from_link, parse_feed
from embyx_manager.monitor.intake import AcquisitionIntake
from embyx_manager.monitor.reports import RunCancelledError, RunContext
from embyx_manager.monitor.subscriptions import SubscriptionRecord, SubscriptionRepository, trim_cursor

#: States discovery hands back for a fresh look; a library hit settles them.
RECHECKABLE_STATES = frozenset(
    {AcquisitionState.DISCOVERED, AcquisitionState.RESOLVE_FAILED, AcquisitionState.EXHAUSTED},
)

FeedFetcher = Callable[[str], Awaitable[bytes]]

_ERROR_LIMIT = 500


class RssPipeline:
    def __init__(  # noqa: PLR0913
        self,
        *,
        config: RssConfig,
        avid_parser: AvidParser,
        subscriptions: SubscriptionRepository,
        fetch: FeedFetcher,
        cloud: AsyncCloudDrive,
        sukebei: SukebeiClient,
        javbus: JavBusClient,
        ledger: AcquisitionRepository,
        archiver: ArchivePipeline,
        on_submitted: Callable[[], None] | None = None,
    ) -> None:
        """``fetch(url)`` returns a feed body; ``on_submitted()`` fires per magnet at CloudDrive."""
        self._config = config
        self._avid = avid_parser
        self._subscriptions = subscriptions
        self._fetch = fetch
        self._ledger = ledger
        self._archiver = archiver
        self._intake = AcquisitionIntake(
            ledger=ledger,
            sukebei=sukebei,
            javbus=javbus,
            cloud=cloud,
            failed_cooldown_seconds=config.failed_avid_cooldown_seconds,
            on_submitted=on_submitted,
        )

    async def run(self, ctx: RunContext) -> None:
        """Poll every enabled subscription, category by category."""
        by_category: dict[str, list[SubscriptionRecord]] = {}
        for subscription in await self._subscriptions.list():
            if subscription.enabled:
                by_category.setdefault(subscription.category, []).append(subscription)
        for category in self._config.categories:
            for subscription in by_category.pop(category.label, ()):
                ctx.check_cancelled()
                await self._poll(ctx, category, subscription)
        # A subscription whose category was removed from the configuration has
        # no offline directory to file into; it is reported, not guessed at.
        for label, orphans in by_category.items():
            for subscription in orphans:
                ctx.warning('%s belongs to the unconfigured category %s', subscription.display_name, label)
                ctx.add('subscriptions_failed')
                await self._subscriptions.record_poll(
                    subscription.id,
                    now=datetime.now(UTC),
                    cursor=None,
                    error=f'category {label} is not configured',
                )

    async def _poll(self, ctx: RunContext, category: RssCategory, subscription: SubscriptionRecord) -> None:
        try:
            await self._poll_subscription(ctx, category, subscription)
        except RunCancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - one subscription's failure is not the run's
            ctx.exception('Failed to poll %s', subscription.display_name)
            ctx.add('subscriptions_failed')
            await self._subscriptions.record_poll(
                subscription.id,
                now=datetime.now(UTC),
                cursor=None,
                error=_describe(exc),
            )

    async def _poll_subscription(
        self,
        ctx: RunContext,
        category: RssCategory,
        subscription: SubscriptionRecord,
    ) -> None:
        feed = parse_feed(await self._fetch(subscription.feed_url))
        ctx.add('subscriptions_polled')
        if subscription.seed_pending:
            # The backlog was covered elsewhere: remember what the feed holds
            # today and ingest nothing.
            ctx.info('Seeded %s with %d items', subscription.display_name, len(feed.items))
            ctx.add('subscriptions_seeded')
            await self._subscriptions.record_poll(
                subscription.id,
                now=datetime.now(UTC),
                cursor=[item.key for item in feed.items],
                error=None,
            )
            return
        ctx.add('items', len(feed.items))
        seen = set(subscription.cursor)
        fresh = [item for item in feed.items if item.key not in seen]
        ctx.add('new_items', len(fresh))
        ctx.info('Find %d items (%d new) in %s', len(feed.items), len(fresh), subscription.display_name)
        if fresh:
            await self._ingest(ctx, category, fresh)
        await self._subscriptions.record_poll(
            subscription.id,
            now=datetime.now(UTC),
            cursor=trim_cursor([*subscription.cursor, *(item.key for item in fresh)]),
            error=None,
        )

    async def _ingest(self, ctx: RunContext, category: RssCategory, items: list[FeedItem]) -> None:
        source = rss_source(category.label)
        # Pinned onto each acquisition so its retries keep landing here even if
        # the category is later repointed or removed.
        task_dir = category.task_dir_path

        avid_item: dict[str, list[FeedItem]] = {}
        for item in items:
            avid = self._avid_of(item)
            if not avid:
                ctx.warning('Failed to get avid for %s', item.title or item.link or item.key)
                continue
            avid_item.setdefault(avid, []).append(item)
        ctx.add('unique_avids', len(avid_item))

        now = datetime.now(UTC)
        wanted: dict[str, list[FeedItem]] = {}
        item_magnets: dict[str, str | None] = {}
        for avid, avid_items in avid_item.items():
            # An item carrying a magnet is evidence the wait is over: it wakes a
            # row still cooling down from an earlier empty pass.
            item_magnet = get_magnet_from_html(avid_items[0].content, avid)
            accepted = await self._ledger.discover(
                avid,
                source=source,
                now=now,
                task_dir_path=task_dir,
                wake=item_magnet is not None,
            )
            if accepted:
                wanted[avid] = avid_items
                item_magnets[avid] = item_magnet
            else:
                ctx.add('skipped_known')
        if len(wanted) != len(avid_item):
            ctx.info('Skipping %d avids already tracked', len(avid_item) - len(wanted))
        if not wanted:
            return

        ctx.check_cancelled()
        wanted = await self._skip_library_held(wanted, task_dir, ctx)
        if not wanted:
            return

        ctx.check_cancelled()
        resolved = await self._resolve_all(wanted, item_magnets, ctx)
        ctx.check_cancelled()
        await self._submit_all(resolved, task_dir, ctx)

    def _avid_of(self, item: FeedItem) -> str:
        """The AVID an item is about: from its title, else from its link.

        JavBus and javlibrary put the ID in the title. AVBase titles are the
        work's title alone, with the ID only in the link, so the link's last
        path segment is the fallback.
        """
        if item.title:
            avid = self._avid.get_avid(item.title)
            if avid:
                return avid
        candidate = avid_candidate_from_link(item.link)
        return self._avid.get_avid(candidate) if candidate else ''

    # -- the library check ----------------------------------------------------

    async def _skip_library_held(
        self,
        wanted: dict[str, list[FeedItem]],
        task_dir: str,
        ctx: RunContext,
    ) -> dict[str, list[FeedItem]]:
        """Drop AVIDs the library already holds, settling their ledger rows.

        The ledger only knows acquisitions that passed through it, so a chart
        re-listing something acquired before the ledger existed sails through
        discovery; this is the check that asks the library itself. A held AVID
        becomes an archived row pointing at the existing copy — terminal, so
        its next sighting stops at the ledger without touching the mount. An
        unreadable library is not a held one: the download proceeds and the
        archive-time check gets the final word.
        """
        kept: dict[str, list[FeedItem]] = {}
        for avid, items in wanted.items():
            ctx.check_cancelled()
            try:
                held = await asyncio.to_thread(self._archiver.library_holdings, avid, ctx, task_dir_path=task_dir)
            except Exception:  # noqa: BLE001 - unverifiable is not held
                ctx.exception('Failed to check the library for %s', avid)
                held = ()
            if not held:
                kept[avid] = items
                continue
            ctx.add('already_in_library')
            ctx.info('%s is already in the library at %s', avid, held[0])
            record = await self._ledger.get(avid)
            if record is not None and record.state in RECHECKABLE_STATES:
                await self._ledger.transition(
                    avid,
                    expected=record.state,
                    target=AcquisitionState.ARCHIVED,
                    now=datetime.now(UTC),
                    note='already in library',
                    archived_paths=held,
                )
        return kept

    # -- magnet resolution ---------------------------------------------------

    async def _resolve_all(
        self,
        avid_item: dict[str, list[FeedItem]],
        item_magnets: dict[str, str | None],
        ctx: RunContext,
    ) -> dict[str, list[MagnetCandidate]]:
        resolved: dict[str, list[MagnetCandidate]] = {}
        await asyncio.gather(*(self._resolve_safely(avid, item_magnets.get(avid), resolved, ctx) for avid in avid_item))
        ctx.add('magnets_found', sum(len(candidates) for candidates in resolved.values()))
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
        item_magnet: str | None,
        resolved: dict[str, list[MagnetCandidate]],
        ctx: RunContext,
    ) -> None:
        try:
            candidates = await self._intake.resolve(avid, ctx=ctx, item_magnet=item_magnet)
        except Exception:  # noqa: BLE001
            ctx.exception('Failed to get magnets for %s', avid)
            return
        if candidates:
            resolved[avid] = candidates

    # -- CloudDrive offline tasks ---------------------------------------------

    async def _submit_all(self, resolved: dict[str, list[MagnetCandidate]], task_dir: str, ctx: RunContext) -> None:
        for avid, candidates in resolved.items():
            ctx.check_cancelled()
            await self._intake.record_and_submit(avid, candidates, task_dir, ctx=ctx)


def _describe(exc: BaseException) -> str:
    text = f'{type(exc).__name__}: {exc}' if str(exc) else type(exc).__name__
    return text[:_ERROR_LIMIT]
