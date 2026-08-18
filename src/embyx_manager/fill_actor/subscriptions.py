"""FreshRSS subscription matching for Fill Actor preflight checks."""

from collections.abc import Sequence
from urllib.parse import unquote, urlsplit


def find_subscribed_actor_ids(
    actor_ids: Sequence[str],
    subscription_urls: Sequence[str],
) -> tuple[str, ...]:
    """Return requested actors already present as an RSSHub JavBus star feed."""
    subscribed = {
        actor_id.casefold()
        for url in subscription_urls
        if (actor_id := _javbus_actor_id(url)) is not None
    }
    return tuple(actor_id for actor_id in actor_ids if actor_id.casefold() in subscribed)


def _javbus_actor_id(url: str) -> str | None:
    try:
        parts = tuple(unquote(part) for part in urlsplit(url).path.split('/') if part)
    except ValueError:
        return None
    for index in range(len(parts) - 2):
        if parts[index].casefold() == 'javbus' and parts[index + 1].casefold() == 'star':
            return parts[index + 2] if index + 3 == len(parts) else None
    return None
