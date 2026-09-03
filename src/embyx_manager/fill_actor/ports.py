from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from typing import Protocol

PageProgressCallback = Callable[[int, int | None, int | None], Awaitable[None]]


@dataclass(frozen=True)
class CatalogListing:
    """What the catalogs know about one actor: who they are and what they are credited in."""

    #: The catalog's primary name for the actor, when it knows them.
    actor_name: str | None
    #: The AVBase talent id, when AVBase knows them.
    talent_id: int | None
    aliases: tuple[str, ...]
    #: Every credited video id, as the sources spell them.
    video_ids: tuple[str, ...]
    #: Release dates by video id, for the ones a source dated.
    release_dates: Mapping[str, date] = field(default_factory=dict)
    #: How many ids each source contributed, for the plan's record.
    source_counts: Mapping[str, int] = field(default_factory=dict)


class ActorCatalog(Protocol):
    """Source that lists the videos an actor is credited in."""

    async def list_videos(
        self,
        actor_ref: str,
        *,
        progress_callback: PageProgressCallback | None = None,
    ) -> CatalogListing: ...


class AcquisitionOutcome(StrEnum):
    """How far a missing video got into the download tracking system.

    ``QUEUED`` and ``ALREADY_TRACKED`` mirror the monitor intake's outcomes so
    the adapter maps by value without fill_actor importing monitor; the two
    ``*_NOT_CONFIGURED`` values are the adapter's own, naming the configuration
    gap that kept it from reaching the intake at all, so the scan can report
    "fix the settings" instead of a generic failure. The rest are legacy values
    from when scans resolved magnets and submitted inline, kept for plans
    persisted back then.
    """

    QUEUED = 'queued'
    SUBMITTED = 'submitted'
    ALREADY_TRACKED = 'already_tracked'
    NO_MAGNET = 'no_magnet'
    SUBMIT_FAILED = 'submit_failed'
    CLOUD_NOT_CONFIGURED = 'cloud_not_configured'
    TASK_DIR_NOT_CONFIGURED = 'task_dir_not_configured'


class AcquisitionGateway(Protocol):
    """Hands a missing video to the acquisition ledger; the tracker owns it afterwards."""

    async def queue_missing(self, video_id: str, *, release_date: date | None = None) -> AcquisitionOutcome: ...


class BrandResolver(Protocol):
    """Resolver for the library brand directory of a video identifier."""

    def resolve_brand(self, video_id: str) -> str | None: ...
