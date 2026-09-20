"""Playlist endpoints: the lists the sync manages, their gaps, the enable flag, and filling a gap.

Filling hands a list's missing codes to the same intake the manual source and
fill actor use, under a ``playlist:<key>`` source label, into one directory:
the playlists section's own, or failing that the RSS category named like a
ranking, since that is where rankings already download to.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict

from embyx_manager.config.models import PlaylistsConfig, RssConfig
from embyx_manager.errors import ApiError
from embyx_manager.monitor.acquisitions import AcquisitionRepository, AcquisitionState, playlist_source
from embyx_manager.monitor.manual import ManualIntakeError, ManualIntakeSource
from embyx_manager.monitor.playlists import PlaylistRecord, PlaylistRepository, PlaylistSourceState

#: Which HTTP status each refused fill answers with; the rest are 422.
_MANUAL_STATUS = {'directory_not_found': 404}
NO_FILL_DIRECTORY = 'no fill directory: set playlists.task_dir_path or an RSS category labelled Rank'


def resolve_fill_dir(playlists: PlaylistsConfig, rss: RssConfig) -> tuple[str | None, str | None]:
    """Where a fill goes: the configured directory, else the ranking category's; with the reason when neither."""
    if playlists.task_dir_path:
        return playlists.task_dir_path, None
    for category in rss.categories:
        if 'rank' in category.label.lower() or '榜' in category.label:
            return category.task_dir_path, None
    return None, NO_FILL_DIRECTORY


@dataclass(frozen=True)
class PlaylistFillApi:
    """The fill route's needs; the route is mounted only when this is supplied."""

    manual: ManualIntakeSource
    #: ``(directory, None)`` when a fill can go somewhere, ``(None, reason)`` otherwise.
    task_dir: Callable[[], tuple[str | None, str | None]]


class PlaylistView(BaseModel):
    key: str
    kind: int
    note: str
    name: str
    enabled: bool
    total: int
    present: int
    missing: int
    emby_playlist_id: str | None
    last_synced_at: datetime | None
    last_error: str | None

    @classmethod
    def from_record(cls, record: PlaylistRecord) -> 'PlaylistView':
        return cls(
            key=record.key,
            kind=record.kind,
            note=record.note,
            name=record.name,
            enabled=record.enabled,
            total=len(record.entries),
            present=len(record.present),
            missing=len(record.missing),
            emby_playlist_id=record.emby_playlist_id,
            last_synced_at=record.last_synced_at,
            last_error=record.last_error,
        )


class PlaylistSourceView(BaseModel):
    database_name: str
    fetched_at: datetime

    @classmethod
    def from_state(cls, state: PlaylistSourceState) -> 'PlaylistSourceView':
        return cls(database_name=state.database_name, fetched_at=state.fetched_at)


class PlaylistListView(BaseModel):
    items: list[PlaylistView]
    source: PlaylistSourceView | None
    #: Where a fill would go, or why it cannot go anywhere yet.
    fill_task_dir: str | None = None
    fill_reason: str | None = None


class MissingEntryView(BaseModel):
    rank: int
    avid: str
    title: str
    #: The ledger's state when the title is already being acquired; None otherwise.
    tracked: AcquisitionState | None


class MissingListView(BaseModel):
    key: str
    name: str
    items: list[MissingEntryView]


class UpdatePlaylistRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    enabled: bool


class FillEntryView(BaseModel):
    avid: str
    outcome: str


class FillView(BaseModel):
    key: str
    task_dir_path: str
    items: list[FillEntryView]
    #: How many codes ended in each outcome.
    counts: dict[str, int]


def create_playlists_router(
    repository: PlaylistRepository,
    *,
    mutation_auth: Any,
    ledger: AcquisitionRepository | None = None,
    fill: PlaylistFillApi | None = None,
) -> APIRouter:
    router = APIRouter(prefix='/api/playlists')

    @router.get('')
    async def list_playlists() -> PlaylistListView:
        records = await repository.list()
        source = await repository.source()
        fill_dir, fill_reason = fill.task_dir() if fill is not None else (None, 'filling is not available')
        return PlaylistListView(
            items=[PlaylistView.from_record(record) for record in records],
            source=PlaylistSourceView.from_state(source) if source is not None else None,
            fill_task_dir=fill_dir,
            fill_reason=fill_reason,
        )

    @router.get('/{key}/missing')
    async def list_missing(key: str) -> MissingListView:
        record = await _load(repository, key)
        states = await _states(ledger, record.missing)
        missing = set(record.missing)
        return MissingListView(
            key=record.key,
            name=record.name,
            items=[
                MissingEntryView(rank=entry.rank, avid=entry.avid, title=entry.title, tracked=states.get(entry.avid))
                for entry in record.entries
                if entry.avid in missing
            ],
        )

    @router.patch('/{key}', dependencies=[Depends(mutation_auth)])
    async def update_playlist(key: str, request: UpdatePlaylistRequest) -> PlaylistView:
        record = await repository.set_enabled(key, enabled=request.enabled, now=datetime.now(UTC))
        if record is None:
            raise ApiError(404, 'unknown_playlist')
        return PlaylistView.from_record(record)

    if fill is not None:

        @router.post('/{key}/fill', dependencies=[Depends(mutation_auth)])
        async def fill_playlist(key: str) -> FillView:
            """Queue every title the list lacks, as one batch, into the fill directory."""
            record = await _load(repository, key)
            task_dir, _reason = fill.task_dir()
            if task_dir is None:
                # The list endpoint carries the reason; the refusal only needs the code.
                raise ApiError(422, 'fill_directory_unavailable')
            try:
                submission = await fill.manual.submit(
                    record.missing,
                    task_dir_path=task_dir,
                    source=playlist_source(record.key),
                    limit=None,
                )
            except ManualIntakeError as exc:
                raise ApiError(_MANUAL_STATUS.get(exc.code, 422), exc.code) from exc
            items = [
                FillEntryView(avid=entry.avid or entry.text, outcome=entry.outcome.value)
                for entry in submission.entries
            ]
            counts: dict[str, int] = {}
            for item in items:
                counts[item.outcome] = counts.get(item.outcome, 0) + 1
            return FillView(key=record.key, task_dir_path=submission.task_dir_path, items=items, counts=counts)

    return router


async def _load(repository: PlaylistRepository, key: str) -> PlaylistRecord:
    record = await repository.get(key)
    if record is None:
        raise ApiError(404, 'unknown_playlist')
    return record


async def _states(ledger: AcquisitionRepository | None, avids: Sequence[str]) -> dict[str, AcquisitionState]:
    if ledger is None or not avids:
        return {}
    return await ledger.states_for(avids)
