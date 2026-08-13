"""Deployment-level settings.

Only what must be known before the database is reachable, or what should not be
changeable from the browser, lives here. Everything else — including Fill Actor's
library roots — is stored in the config store and edited on the Settings page.
"""

import ipaddress
import os
from dataclasses import dataclass


def _positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    value = int(raw) if raw is not None else default
    if value < 1:
        msg = f'{name} must be positive'
        raise ValueError(msg)
    return value


def _boolean(name: str) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return False
    if raw.casefold() in {'1', 'true', 'yes', 'on'}:
        return True
    if raw.casefold() in {'0', 'false', 'no', 'off'}:
        return False
    msg = f'{name} must be a boolean'
    raise ValueError(msg)


def _is_loopback_host(host: str) -> bool:
    if host.casefold() == 'localhost':
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@dataclass(frozen=True)
class Settings:
    database_url: str = 'postgresql://localhost/embyx_manager'
    api_token: str | None = None
    tls_terminated: bool = False
    host: str = '127.0.0.1'
    port: int = 8000
    max_request_bytes: int = 65_536
    # Per-plan limits and lookup concurrency stay here: raising them raises the request
    # pressure on JavBus and Sukebei, which is an operator call rather than a UI toggle.
    max_actors: int = 20
    max_videos: int = 2_000
    magnet_concurrency: int = 8

    @classmethod
    def from_env(cls) -> 'Settings':
        settings = cls(
            database_url=os.environ.get('EMBYX_MANAGER_DATABASE_URL', 'postgresql://localhost/embyx_manager'),
            api_token=os.environ.get('EMBYX_MANAGER_API_TOKEN') or None,
            tls_terminated=_boolean('EMBYX_MANAGER_TLS_TERMINATED'),
            host=os.environ.get('EMBYX_MANAGER_HOST', '127.0.0.1'),
            port=_positive_int('EMBYX_MANAGER_PORT', 8000),
            max_request_bytes=_positive_int('EMBYX_MANAGER_MAX_REQUEST_BYTES', 65_536),
            max_actors=_positive_int('EMBYX_MANAGER_MAX_ACTORS', 20),
            max_videos=_positive_int('EMBYX_MANAGER_MAX_VIDEOS', 2_000),
            magnet_concurrency=_positive_int('EMBYX_MANAGER_MAGNET_CONCURRENCY', 8),
        )
        settings.validate_exposure()
        return settings

    def validate_exposure(self) -> None:
        if not _is_loopback_host(self.host):
            if self.api_token is None:
                msg = 'EMBYX_MANAGER_API_TOKEN is required when binding to a non-loopback host'
                raise ValueError(msg)
            if not self.tls_terminated:
                msg = 'EMBYX_MANAGER_TLS_TERMINATED must be true when binding to a non-loopback host'
                raise ValueError(msg)
