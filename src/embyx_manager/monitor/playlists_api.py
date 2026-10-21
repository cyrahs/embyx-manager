"""Playlist endpoints: the lists the sync manages, their gaps, the enable flag, and filling a gap.

Filling hands a list's missing codes to the same intake the manual source and
fill actor use, under a ``playlist:<key>`` source label, into one directory:
the playlists section's own, or failing that the RSS category named like a
ranking, since that is where rankings already download to.

A fill runs in the background: a list can lack hundreds of titles, and each one
costs a library lookup through the CloudDrive mount, more than a request behind
Cloudflare's 120-second proxy timeout can wait for. The request validates the
target and answers at once; the page polls the fill's progress. Each title is
only recorded in the ledger (``queue``), so the tracker's background pass
resolves magnets and submits them, as it does for the fill actor's scans.
"""

import asyncio
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict

from embyx_manager.config.models import PlaylistsConfig, RssConfig
from embyx_manager.errors import ApiError
from embyx_manager.monitor.acquisitions import AcquisitionRepository, AcquisitionState, playlist_source
from embyx_manager.monitor.manual import ManualEntry, ManualIntakeError, ManualIntakeSource
from embyx_manager.monitor.playlists import PlaylistRecord, PlaylistRepository, PlaylistSourceState

LOGGER = logging.getLogger('embyx-manager.playlists')

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


class FillEntryView(BaseModel):
    avid: str
    outcome: str


FillState = Literal['running', 'done', 'failed']


class FillView(BaseModel):
    key: str
    task_dir_path: str
    state: FillState
    #: How many of the gap's ``total`` codes the fill has handled so far.
    done: int
    total: int
    items: list[FillEntryView]
    #: How many codes ended in each outcome.
    counts: dict[str, int]
    #: The error code a failed fill stopped on.
    error: str | None = None
    started_at: datetime
    finished_at: datetime | None = None


class PlaylistListView(BaseModel):
    items: list[PlaylistView]
    source: PlaylistSourceView | None
    #: Where a fill would go, or why it cannot go anywhere yet.
    fill_task_dir: str | None = None
    fill_reason: str | None = None
    #: The latest fill of each list since the server started, running ones included.
    fills: list[FillView] = []


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


@dataclass
class _FillJob:
    """One list's fill as it runs; kept after it ends so the page can show the outcome."""

    key: str
    task_dir_path: str
    total: int
    started_at: datetime
    state: FillState = 'running'
    items: list[FillEntryView] = field(default_factory=list)
    error: str | None = None
    finished_at: datetime | None = None

    def record(self, entry: ManualEntry) -> None:
        self.items.append(FillEntryView(avid=entry.avid or entry.text, outcome=entry.outcome.value))

    def finish(self, error: str | None = None) -> None:
        self.state = 'failed' if error else 'done'
        self.error = error
        self.finished_at = datetime.now(UTC)

    def view(self) -> FillView:
        counts: dict[str, int] = {}
        for item in self.items:
            counts[item.outcome] = counts.get(item.outcome, 0) + 1
        return FillView(
            key=self.key,
            task_dir_path=self.task_dir_path,
            state=self.state,
            done=len(self.items),
            total=self.total,
            items=list(self.items),
            counts=counts,
            error=self.error,
            started_at=self.started_at,
            finished_at=self.finished_at,
        )


def create_playlists_router(  # noqa: C901 - one function per endpoint over shared wiring
    repository: PlaylistRepository,
    *,
    mutation_auth: Any,
    ledger: AcquisitionRepository | None = None,
    fill: PlaylistFillApi | None = None,
) -> APIRouter:
    router = APIRouter(prefix='/api/playlists')
    fills: dict[str, _FillJob] = {}
    # Strong references: the event loop keeps only weak ones to running tasks.
    fill_tasks: set[asyncio.Task[None]] = set()

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
            fills=[job.view() for job in fills.values()],
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

        async def run_fill(job: _FillJob, avids: Sequence[str]) -> None:
            try:
                await fill.manual.submit(
                    avids,
                    task_dir_path=job.task_dir_path,
                    source=playlist_source(job.key),
                    limit=None,
                    queue=True,
                    on_entry=job.record,
                )
            except ManualIntakeError as exc:
                job.finish(exc.code)
            except Exception:
                LOGGER.exception('filling playlist %s failed', job.key)
                job.finish('fill_failed')
            else:
                job.finish()

        @router.post('/{key}/fill', status_code=202, dependencies=[Depends(mutation_auth)])
        async def fill_playlist(key: str) -> FillView:
            """Start queueing every title the list lacks, as one batch, into the fill directory.

            Answers at once with the fill's progress; a fill already running for
            the list is reported rather than started twice.
            """
            record = await _load(repository, key)
            running = fills.get(record.key)
            if running is not None and running.state == 'running':
                return running.view()
            task_dir, _reason = fill.task_dir()
            if task_dir is None:
                # The list endpoint carries the reason; the refusal only needs the code.
                raise ApiError(422, 'fill_directory_unavailable')
            try:
                task_dir = await fill.manual.check_target(task_dir)
            except ManualIntakeError as exc:
                raise ApiError(_MANUAL_STATUS.get(exc.code, 422), exc.code) from exc
            job = _FillJob(
                key=record.key,
                task_dir_path=task_dir,
                total=len(record.missing),
                started_at=datetime.now(UTC),
            )
            fills[record.key] = job
            task = asyncio.create_task(run_fill(job, record.missing), name=f'playlist-fill-{record.key}')
            fill_tasks.add(task)
            task.add_done_callback(fill_tasks.discard)
            return job.view()

        @router.get('/{key}/fill')
        async def get_fill(key: str) -> FillView:
            """The list's latest fill since the server started."""
            job = fills.get(key)
            if job is None:
                raise ApiError(404, 'no_fill')
            return job.view()

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
