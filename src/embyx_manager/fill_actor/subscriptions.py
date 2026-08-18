"""FreshRSS subscription matching for Fill Actor preflight checks."""

from collections.abc import Sequence
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit

from embyx_manager.clients.freshrss import FreshRSSSubscription


@dataclass(frozen=True, slots=True)
class SubscribedActor:
    actor_id: str
    actor_name: str | None


def find_subscribed_actors(
    actor_ids: Sequence[str],
    subscriptions: Sequence[FreshRSSSubscription],
) -> tuple[SubscribedActor, ...]:
    """Return requested actors already present as an RSSHub JavBus star feed."""
    subscribed: dict[str, str | None] = {}
    for subscription in subscriptions:
        actor_id = _javbus_actor_id(subscription.url)
        if actor_id is None:
            continue
        key = actor_id.casefold()
        if key not in subscribed or subscribed[key] is None:
            subscribed[key] = subscription.title
    return tuple(
        SubscribedActor(actor_id=actor_id, actor_name=subscribed[actor_id.casefold()])
        for actor_id in actor_ids
        if actor_id.casefold() in subscribed
    )


def _javbus_actor_id(url: str) -> str | None:
    try:
        parts = tuple(unquote(part) for part in urlsplit(url).path.split('/') if part)
    except ValueError:
        return None
    for index in range(len(parts) - 2):
        if parts[index].casefold() == 'javbus' and parts[index + 1].casefold() == 'star':
            return parts[index + 2] if index + 3 == len(parts) else None
    return None
