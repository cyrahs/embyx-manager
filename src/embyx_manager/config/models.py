"""Typed configuration sections stored in the database and edited from the web UI.

Secret fields are declared per section; the API layer masks them on read and
treats empty submitted values as "keep the stored secret".
"""

from datetime import datetime
from pathlib import PurePosixPath
from typing import ClassVar
from urllib.parse import urlsplit, urlunsplit

from cronsim import CronSim, CronSimError
from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator


def normalize_http_base_url(name: str, value: str) -> str:
    """Validate and normalize an absolute HTTP(S) base URL; '' stays ''."""
    if not value:
        return ''
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        msg = f'{name} must be an absolute HTTP(S) URL'
        raise ValueError(msg) from exc
    if parsed.scheme not in {'http', 'https'} or not parsed.hostname or port == 0:
        msg = f'{name} must be an absolute HTTP(S) URL'
        raise ValueError(msg)
    if parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
        msg = f'{name} must not include credentials, a query, or a fragment'
        raise ValueError(msg)
    path = parsed.path.rstrip('/')
    return urlunsplit((parsed.scheme, parsed.netloc, path, '', ''))


def normalize_absolute_path(name: str, value: str) -> str:
    """Validate an absolute filesystem path with no trailing slash; '' stays ''."""
    trimmed = value.strip()
    if not trimmed:
        return ''
    if not trimmed.startswith('/'):
        msg = f'{name} must be an absolute path'
        raise ValueError(msg)
    if '\x00' in trimmed:
        msg = f'{name} must not contain a null byte'
        raise ValueError(msg)
    normalized = str(PurePosixPath(trimmed))
    if '..' in PurePosixPath(normalized).parts:
        msg = f'{name} must not contain ".."'
        raise ValueError(msg)
    return normalized


def normalize_relative_subpath(name: str, value: str) -> str:
    """Validate a subdirectory path relative to a configured root."""
    trimmed = value.strip().rstrip('/')
    if not trimmed:
        msg = f'{name} must not be empty'
        raise ValueError(msg)
    if trimmed.startswith('/'):
        msg = f'{name} must be relative to the configured root'
        raise ValueError(msg)
    if '\x00' in trimmed:
        msg = f'{name} must not contain a null byte'
        raise ValueError(msg)
    normalized = str(PurePosixPath(trimmed))
    if '..' in PurePosixPath(normalized).parts:
        msg = f'{name} must not contain ".."'
        raise ValueError(msg)
    return normalized


def normalize_absolute_paths(name: str, values: tuple[str, ...]) -> tuple[str, ...]:
    """Normalizes a list of absolute paths, dropping blanks and rejecting duplicates."""
    normalized = tuple(normalize_absolute_path(name, value) for value in values if value.strip())
    if len(set(normalized)) != len(normalized):
        msg = f'{name} must not repeat a path'
        raise ValueError(msg)
    return normalized


class ConfigSection(BaseModel):
    model_config = ConfigDict(extra='forbid')

    SECRET_FIELDS: ClassVar[frozenset[str]] = frozenset()


class CloudDriveConfig(ConfigSection):
    SECRET_FIELDS: ClassVar[frozenset[str]] = frozenset({'api_token'})

    address: str = ''
    api_token: str = ''
    secure: bool = True

    @property
    def configured(self) -> bool:
        return bool(self.address and self.api_token)


class FreshRSSConfig(ConfigSection):
    SECRET_FIELDS: ClassVar[frozenset[str]] = frozenset({'api_key'})

    url: str = ''
    api_key: str = ''
    proxy: str = ''

    @field_validator('url')
    @classmethod
    def _validate_url(cls, value: str) -> str:
        return normalize_http_base_url('freshrss.url', value)

    @property
    def configured(self) -> bool:
        return bool(self.url and self.api_key)


class FeedsConfig(ConfigSection):
    """RSSHub / FreshRSS URLs used by the fill-actor page's feed integration."""

    rsshub_url: str = ''
    freshrss_url: str = ''
    freshrss_rsshub_url: str = ''

    @field_validator('rsshub_url')
    @classmethod
    def _validate_rsshub(cls, value: str) -> str:
        return normalize_http_base_url('feeds.rsshub_url', value)

    @field_validator('freshrss_url')
    @classmethod
    def _validate_freshrss(cls, value: str) -> str:
        return normalize_http_base_url('feeds.freshrss_url', value)

    @field_validator('freshrss_rsshub_url')
    @classmethod
    def _validate_freshrss_rsshub(cls, value: str) -> str:
        return normalize_http_base_url('feeds.freshrss_rsshub_url', value)


class AvidRulesConfig(ConfigSection):
    """Video-ID parsing rules shared by the pipelines."""

    id_exceptions: tuple[str, ...] = ()
    ignored_id_patterns: tuple[str, ...] = ()


class RssCategory(BaseModel):
    """One FreshRSS category and the offline directory its items download to.

    Every category names its own directory. There is deliberately no shared
    default to fall back on: the directory decides which archive route files the
    download, so leaving it implicit would hide where a category's videos land.
    """

    model_config = ConfigDict(extra='forbid')

    #: The FreshRSS category (label) whose unread items this entry ingests.
    label: str
    #: The CloudDrive API path this category's offline tasks are queued under.
    task_dir_path: str

    @field_validator('label')
    @classmethod
    def _validate_label(cls, value: str) -> str:
        trimmed = value.strip()
        if not trimmed:
            msg = 'rss.categories label must not be empty'
            raise ValueError(msg)
        return trimmed

    @field_validator('task_dir_path')
    @classmethod
    def _validate_task_dir(cls, value: str) -> str:
        normalized = normalize_absolute_path('rss.categories task_dir_path', value)
        if not normalized:
            msg = 'rss.categories task_dir_path must not be empty'
            raise ValueError(msg)
        return normalized


class RssConfig(ConfigSection):
    enabled: bool = False
    interval_seconds: int = 1800
    #: Every configured category is ingested on each scheduled run; a manual
    #: trigger may scope itself to one of them by label. Empty until an operator
    #: adds one, because a category cannot be guessed: it needs a directory.
    categories: tuple[RssCategory, ...] = ()
    failed_avid_cooldown_seconds: int = 86_400

    @field_validator('interval_seconds', 'failed_avid_cooldown_seconds')
    @classmethod
    def _positive(cls, value: int) -> int:
        if value < 1:
            msg = 'must be positive'
            raise ValueError(msg)
        return value

    @field_validator('categories')
    @classmethod
    def _unique_labels(cls, value: tuple[RssCategory, ...]) -> tuple[RssCategory, ...]:
        # A repeated label would ingest the same items twice in one run, and the
        # second entry's directory would silently lose to the first.
        labels = [category.label for category in value]
        duplicates = sorted({label for label in labels if labels.count(label) > 1})
        if duplicates:
            msg = f'rss.categories must not repeat the label {", ".join(repr(label) for label in duplicates)}'
            raise ValueError(msg)
        return value


class ArchiveConfig(ConfigSection):
    enabled: bool = False
    # When the fallback scan runs, as a five-field cron expression in the
    # server's local time. The tracker polls on its own interval below.
    scan_cron: str = '0 4 * * *'
    src_dir: str = ''
    dst_dir: str = ''
    # Per-subdirectory routing below src_dir/dst_dir, e.g. {'intake': 'library'}.
    # The CloudDrive offline task directory is expected to be one of these
    # sources: the tracker locates finished downloads in the route tables and
    # files them by that route, so it needs no directory config of its own.
    mapping: dict[str, str] = {}
    # Same shape as mapping; these routes run first and claim duplicates already
    # archived under a mapping destination.
    priority_mapping: dict[str, str] = {}
    min_size_mb: int = 0
    # Destination subdirectory -> list of brands routed into it.
    brand_mapping: dict[str, tuple[str, ...]] = {}
    tracker_interval_seconds: int = 300
    stall_timeout_hours: int = 24
    max_attempts: int = 5

    @field_validator('scan_cron')
    @classmethod
    def _validate_scan_cron(cls, value: str) -> str:
        expr = ' '.join(value.split())
        try:
            CronSim(expr, datetime(2000, 1, 1))  # noqa: DTZ001 - probe parse only, never iterated
        except CronSimError as exc:
            msg = f'archive.scan_cron is not a valid cron expression: {exc}'
            raise ValueError(msg) from exc
        return expr

    @field_validator('tracker_interval_seconds', 'stall_timeout_hours', 'max_attempts')
    @classmethod
    def _tracker_positive(cls, value: int) -> int:
        if value < 1:
            msg = 'must be positive'
            raise ValueError(msg)
        return value

    @field_validator('min_size_mb')
    @classmethod
    def _non_negative(cls, value: int) -> int:
        if value < 0:
            msg = 'must not be negative'
            raise ValueError(msg)
        return value

    @field_validator('mapping', 'priority_mapping')
    @classmethod
    def _validate_mapping(cls, value: dict[str, str], info: ValidationInfo) -> dict[str, str]:
        label = f'archive.{info.field_name}'
        mapping: dict[str, str] = {}
        for src, dst in value.items():
            source = normalize_relative_subpath(f'{label} source', src)
            if source in mapping:
                msg = f'{label} must not repeat the source subdirectory {source!r}'
                raise ValueError(msg)
            mapping[source] = normalize_relative_subpath(f'{label} destination', dst)
        return mapping

    @field_validator('brand_mapping')
    @classmethod
    def _validate_brand_mapping(cls, value: dict[str, tuple[str, ...]]) -> dict[str, tuple[str, ...]]:
        mapping: dict[str, tuple[str, ...]] = {}
        for dst, brands in value.items():
            destination = normalize_relative_subpath('archive.brand_mapping destination', dst)
            if destination in mapping:
                msg = f'archive.brand_mapping must not repeat the destination subdirectory {destination!r}'
                raise ValueError(msg)
            # Brands are matched against upper-cased video IDs, so a lower-cased
            # entry here would silently never route anything.
            normalized = tuple(dict.fromkeys(brand.strip().upper() for brand in brands if brand.strip()))
            if not normalized:
                msg = f'archive.brand_mapping[{destination!r}] must list at least one brand'
                raise ValueError(msg)
            mapping[destination] = normalized
        return mapping

    @model_validator(mode='after')
    def _validate_route_sources(self) -> 'ArchiveConfig':
        # A destination may be shared by both tables, but a source subdirectory
        # belongs to exactly one category or the routing would be ambiguous.
        overlap = sorted(set(self.mapping) & set(self.priority_mapping))
        if overlap:
            msg = (
                'archive.mapping and archive.priority_mapping must not share the source '
                f'subdirectory {", ".join(repr(source) for source in overlap)}'
            )
            raise ValueError(msg)
        return self

    @property
    def configured(self) -> bool:
        return bool(self.src_dir and self.dst_dir and (self.mapping or self.priority_mapping))


class MappingConfig(ConfigSection):
    enabled: bool = False
    src_dir: str = ''
    dst_dir: str = ''
    debounce_seconds: float = 2.0
    full_sync_interval_seconds: int = 86_400

    @field_validator('full_sync_interval_seconds')
    @classmethod
    def _positive(cls, value: int) -> int:
        if value < 1:
            msg = 'must be positive'
            raise ValueError(msg)
        return value

    @property
    def configured(self) -> bool:
        return bool(self.src_dir and self.dst_dir)


class FillActorConfig(ConfigSection):
    """Library roots and move settings for the Fill Actor feature.

    Every field defaults to empty: a deployment's real paths belong in the database,
    never in this file. An unconfigured section simply leaves the feature not ready.
    """

    actor_root: str = ''
    additional_roots: tuple[str, ...] = ()
    move_in_root: str = ''
    move_in_by_brand: bool = False
    root_sentinel: str = '.embyx-root'
    apply_enabled: bool = False
    cloud_strm_mount_prefix: str = ''
    cloud_source_roots: tuple[str, ...] = ()
    cloud_move_in_root: str = ''

    @field_validator('actor_root')
    @classmethod
    def _validate_actor_root(cls, value: str) -> str:
        return normalize_absolute_path('fill_actor.actor_root', value)

    @field_validator('move_in_root')
    @classmethod
    def _validate_move_in_root(cls, value: str) -> str:
        return normalize_absolute_path('fill_actor.move_in_root', value)

    @field_validator('additional_roots')
    @classmethod
    def _validate_additional_roots(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return normalize_absolute_paths('fill_actor.additional_roots', value)

    @field_validator('cloud_strm_mount_prefix')
    @classmethod
    def _validate_mount_prefix(cls, value: str) -> str:
        return normalize_absolute_path('fill_actor.cloud_strm_mount_prefix', value)

    @field_validator('cloud_move_in_root')
    @classmethod
    def _validate_cloud_move_in_root(cls, value: str) -> str:
        return normalize_absolute_path('fill_actor.cloud_move_in_root', value)

    @field_validator('cloud_source_roots')
    @classmethod
    def _validate_cloud_source_roots(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return normalize_absolute_paths('fill_actor.cloud_source_roots', value)

    @field_validator('root_sentinel')
    @classmethod
    def _validate_sentinel(cls, value: str) -> str:
        trimmed = value.strip()
        if trimmed in {'', '.', '..'} or '/' in trimmed or '\x00' in trimmed:
            msg = 'fill_actor.root_sentinel must be a single path segment'
            raise ValueError(msg)
        return trimmed

    @model_validator(mode='after')
    def _validate_combination(self) -> 'FillActorConfig':
        if self.actor_root and self.actor_root in self.additional_roots:
            msg = 'fill_actor.actor_root must not also be an additional root'
            raise ValueError(msg)
        cloud_values = (
            bool(self.cloud_strm_mount_prefix),
            bool(self.cloud_source_roots),
            bool(self.cloud_move_in_root),
        )
        if any(cloud_values) and not all(cloud_values):
            msg = (
                'fill_actor.cloud_strm_mount_prefix, fill_actor.cloud_source_roots, and '
                'fill_actor.cloud_move_in_root must be configured together'
            )
            raise ValueError(msg)
        if self.cloud_source_roots and len(self.cloud_source_roots) != len(self.additional_roots):
            msg = 'fill_actor.cloud_source_roots must match fill_actor.additional_roots one-for-one'
            raise ValueError(msg)
        if self.apply_enabled and not all(cloud_values):
            msg = 'fill_actor.apply_enabled requires the CloudDrive move path configuration'
            raise ValueError(msg)
        return self

    @property
    def cloud_configured(self) -> bool:
        return bool(self.cloud_strm_mount_prefix and self.cloud_source_roots and self.cloud_move_in_root)

    @property
    def configured(self) -> bool:
        """True once scanning has somewhere to look and somewhere to move into."""
        return bool(self.actor_root and self.additional_roots and self.move_in_root)


SECTION_MODELS: dict[str, type[ConfigSection]] = {
    'clouddrive': CloudDriveConfig,
    'freshrss': FreshRSSConfig,
    'feeds': FeedsConfig,
    'avid': AvidRulesConfig,
    'rss': RssConfig,
    'archive': ArchiveConfig,
    'mapping': MappingConfig,
    'fill_actor': FillActorConfig,
}
