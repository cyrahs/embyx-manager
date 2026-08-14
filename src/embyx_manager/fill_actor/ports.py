from collections.abc import Awaitable, Callable, Iterable
from enum import StrEnum
from typing import Protocol

PageProgressCallback = Callable[[int, int | None, int | None], Awaitable[None]]


class ActorCatalog(Protocol):
    """Source that lists video identifiers associated with an actor."""

    async def list_video_ids(
        self,
        actor_id: str,
        *,
        progress_callback: PageProgressCallback | None = None,
    ) -> Iterable[str]: ...


class AcquisitionOutcome(StrEnum):
    """How far a missing video got into the download tracking system.

    Values mirror the monitor intake's outcomes so the adapter maps by value
    without fill_actor importing monitor.
    """

    SUBMITTED = 'submitted'
    ALREADY_TRACKED = 'already_tracked'
    NO_MAGNET = 'no_magnet'
    SUBMIT_FAILED = 'submit_failed'


class AcquisitionGateway(Protocol):
    """Hands a missing video to the acquisition ledger; the tracker owns it afterwards."""

    async def submit_missing(self, video_id: str) -> AcquisitionOutcome: ...


class BrandResolver(Protocol):
    """Resolver for the library brand directory of a video identifier."""

    def resolve_brand(self, video_id: str) -> str | None: ...
