import ipaddress
import os
from dataclasses import dataclass
from pathlib import Path

from embyx_manager.fill_actor.cloud_moves import CloudMovePaths


def _optional_path(value: str | None) -> Path | None:
    return Path(value).expanduser() if value else None


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
    actor_brand_path: Path | None = None
    additional_brand_paths: tuple[Path, ...] = ()
    move_in_path: Path | None = None
    api_token: str | None = None
    tls_terminated: bool = False
    host: str = '127.0.0.1'
    port: int = 8000
    max_request_bytes: int = 65_536
    max_actors: int = 20
    max_videos: int = 2_000
    magnet_concurrency: int = 8
    root_sentinel: str = '.embyx-root'
    move_in_by_brand: bool = False
    apply_enabled: bool = False
    cloud_move_paths: CloudMovePaths | None = None

    @classmethod
    def from_env(cls) -> 'Settings':
        additional = tuple(
            Path(value).expanduser()
            for value in os.environ.get('EMBYX_MANAGER_ADDITIONAL_ROOTS', '').split(os.pathsep)
            if value
        )
        cloud_source_roots = tuple(
            value for value in os.environ.get('EMBYX_MANAGER_CLOUD_SOURCE_ROOTS', '').split(os.pathsep) if value
        )
        cloud_strm_mount_prefix = os.environ.get('EMBYX_MANAGER_CLOUD_STRM_MOUNT_PREFIX') or None
        cloud_move_in_root = os.environ.get('EMBYX_MANAGER_CLOUD_MOVE_IN_ROOT') or None
        cloud_values_configured = (
            cloud_strm_mount_prefix is not None,
            bool(cloud_source_roots),
            cloud_move_in_root is not None,
        )
        if any(cloud_values_configured) and not all(cloud_values_configured):
            msg = (
                'EMBYX_MANAGER_CLOUD_STRM_MOUNT_PREFIX, EMBYX_MANAGER_CLOUD_SOURCE_ROOTS, '
                'and EMBYX_MANAGER_CLOUD_MOVE_IN_ROOT must be configured together'
            )
            raise ValueError(msg)
        cloud_move_paths = (
            CloudMovePaths.from_values(
                strm_mount_prefix=cloud_strm_mount_prefix,
                source_api_roots=cloud_source_roots,
                move_in_api_root=cloud_move_in_root,
            )
            if cloud_strm_mount_prefix is not None and cloud_move_in_root is not None
            else None
        )
        if cloud_move_paths is not None and len(cloud_move_paths.source_api_roots) != len(additional):
            msg = 'EMBYX_MANAGER_CLOUD_SOURCE_ROOTS must match EMBYX_MANAGER_ADDITIONAL_ROOTS one-for-one'
            raise ValueError(msg)
        apply_enabled = _boolean('EMBYX_MANAGER_APPLY_ENABLED')
        if apply_enabled and cloud_move_paths is None:
            msg = 'EMBYX_MANAGER_APPLY_ENABLED requires the CloudDrive move path configuration'
            raise ValueError(msg)
        settings = cls(
            database_url=os.environ.get('EMBYX_MANAGER_DATABASE_URL', 'postgresql://localhost/embyx_manager'),
            actor_brand_path=_optional_path(os.environ.get('EMBYX_MANAGER_ACTOR_ROOT')),
            additional_brand_paths=additional,
            move_in_path=_optional_path(os.environ.get('EMBYX_MANAGER_MOVE_IN_ROOT')),
            api_token=os.environ.get('EMBYX_MANAGER_API_TOKEN') or None,
            tls_terminated=_boolean('EMBYX_MANAGER_TLS_TERMINATED'),
            host=os.environ.get('EMBYX_MANAGER_HOST', '127.0.0.1'),
            port=_positive_int('EMBYX_MANAGER_PORT', 8000),
            max_request_bytes=_positive_int('EMBYX_MANAGER_MAX_REQUEST_BYTES', 65_536),
            max_actors=_positive_int('EMBYX_MANAGER_MAX_ACTORS', 20),
            max_videos=_positive_int('EMBYX_MANAGER_MAX_VIDEOS', 2_000),
            magnet_concurrency=_positive_int('EMBYX_MANAGER_MAGNET_CONCURRENCY', 8),
            root_sentinel=os.environ.get('EMBYX_MANAGER_ROOT_SENTINEL', '.embyx-root'),
            move_in_by_brand=_boolean('EMBYX_MANAGER_MOVE_IN_BY_BRAND'),
            apply_enabled=apply_enabled,
            cloud_move_paths=cloud_move_paths,
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

    def require_fill_actor_paths(self) -> tuple[Path, tuple[Path, ...], Path]:
        if self.actor_brand_path is None or self.move_in_path is None or not self.additional_brand_paths:
            msg = (
                'EMBYX_MANAGER_ACTOR_ROOT, EMBYX_MANAGER_ADDITIONAL_ROOTS, '
                'and EMBYX_MANAGER_MOVE_IN_ROOT must be configured'
            )
            raise ValueError(msg)
        return self.actor_brand_path, self.additional_brand_paths, self.move_in_path
