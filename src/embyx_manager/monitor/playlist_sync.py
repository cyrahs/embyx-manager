"""The playlists pipeline: ranking lists from jinjier.art mirrored as Emby playlists.

One run refreshes the stored lists when the source published a new database,
reads the Emby movie index once, and then brings every enabled list's playlist
to its ranked, library-present titles. Order is what the playlist is for, so a
playlist whose entries differ from the target in any position is emptied and
refilled in one go rather than patched. A disabled list has its playlist
removed; enabling it again recreates it on the next run.

The source failing is a warning, not a failure: the stored entries still sync.
One list failing on the Emby side is recorded on that list and the run goes on.
"""

from collections.abc import Callable
from datetime import UTC, datetime

from embyx_manager.clients.emby import EmbyClient, EmbyError
from embyx_manager.clients.jinjier import JinjierClient, JinjierError, parse_lists
from embyx_manager.monitor.playlists import PlaylistRecord, PlaylistRepository
from embyx_manager.monitor.reports import RunContext


class PlaylistSyncPipeline:
    def __init__(
        self,
        *,
        repository: PlaylistRepository,
        source: JinjierClient,
        emby: EmbyClient,
        avid_of: Callable[[str], str],
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._repository = repository
        self._source = source
        self._emby = emby
        self._avid_of = avid_of
        self._now = now

    async def run(self, ctx: RunContext) -> None:
        records = await self._refresh_lists(ctx)
        ctx.check_cancelled()
        index = await self._emby.movie_index(self._avid_of)
        ctx.set('library_titles', len(index))
        existing = {playlist.playlist_id for playlist in await self._emby.list_playlists()}
        for record in records:
            ctx.check_cancelled()
            try:
                if record.enabled:
                    await self._sync_one(record, index, existing, ctx)
                else:
                    await self._retire(record, index, existing, ctx)
            except EmbyError as exc:
                ctx.error('%s: %s', record.name, exc)
                ctx.add('lists_failed')
                await self._repository.record_error(record.key, now=self._now(), error=str(exc))

    # -- source ---------------------------------------------------------------

    async def _refresh_lists(self, ctx: RunContext) -> tuple[PlaylistRecord, ...]:
        """Download and store the lists when the source moved on; else keep the stored ones."""
        records = await self._repository.list()
        stored = await self._repository.source()
        try:
            name = await self._source.discover_database_name()
        except JinjierError as exc:
            ctx.warning('ranking source unavailable, keeping the stored lists: %s', exc)
            name = None
        if name is not None and (stored is None or stored.database_name != name or not records):
            try:
                lists = parse_lists(await self._source.download_database(name), self._avid_of)
            except JinjierError as exc:
                ctx.warning('ranking database %s could not be read, keeping the stored lists: %s', name, exc)
            else:
                now = self._now()
                await self._repository.replace_lists(lists, now=now)
                await self._repository.record_source(name, now=now)
                ctx.set('source_updated', 1)
                ctx.info('stored %d lists from ranking database %s', len(lists), name)
                records = await self._repository.list()
        if not records:
            msg = 'no ranking lists are stored and the source could not be read'
            raise RuntimeError(msg)
        ctx.set('lists', len(records))
        return records

    # -- one list ----------------------------------------------------------------

    async def _sync_one(
        self,
        record: PlaylistRecord,
        index: dict[str, str],
        existing: set[str],
        ctx: RunContext,
    ) -> None:
        present = [entry.avid for entry in record.entries if entry.avid in index]
        missing = [entry.avid for entry in record.entries if entry.avid not in index]
        target = [index[avid] for avid in present]
        playlist_id = record.emby_playlist_id if record.emby_playlist_id in existing else None
        if playlist_id is None:
            if target:
                # An empty playlist would only say the library holds nothing of
                # this list; the missing count says that already.
                playlist_id = await self._emby.create_playlist(record.name, target)
                existing.add(playlist_id)
                ctx.add('lists_created')
                ctx.info('created playlist %s with %d titles', record.name, len(target))
        else:
            current = await self._emby.playlist_entries(playlist_id)
            if [entry.item_id for entry in current] != target:
                await self._emby.remove_entries(playlist_id, [entry.entry_id for entry in current])
                await self._emby.add_entries(playlist_id, target)
                ctx.add('lists_rebuilt')
                ctx.info('rebuilt playlist %s: %d titles', record.name, len(target))
            else:
                ctx.add('lists_unchanged')
        await self._repository.record_sync(
            record.key,
            now=self._now(),
            present=present,
            missing=missing,
            emby_playlist_id=playlist_id,
        )
        ctx.add('lists_synced')
        ctx.add('missing_total', len(missing))

    async def _retire(
        self,
        record: PlaylistRecord,
        index: dict[str, str],
        existing: set[str],
        ctx: RunContext,
    ) -> None:
        """A disabled list: no playlist on the server, but its gap stays known."""
        if record.emby_playlist_id is not None and record.emby_playlist_id in existing:
            await self._emby.delete_playlist(record.emby_playlist_id)
            existing.discard(record.emby_playlist_id)
            ctx.add('lists_removed')
            ctx.info('removed playlist %s: the list is disabled', record.name)
        await self._repository.record_sync(
            record.key,
            now=self._now(),
            present=[entry.avid for entry in record.entries if entry.avid in index],
            missing=[entry.avid for entry in record.entries if entry.avid not in index],
            emby_playlist_id=None,
        )
