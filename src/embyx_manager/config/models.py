"""Typed configuration sections stored in the database and edited from the web UI.

Secret fields are declared per section; the API layer masks them on read and
treats empty submitted values as "keep the stored secret".
"""

from typing import ClassVar
from urllib.parse import urlsplit, urlunsplit

from pydantic import BaseModel, ConfigDict, field_validator


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


class ConfigSection(BaseModel):
    model_config = ConfigDict(extra='forbid')

    SECRET_FIELDS: ClassVar[frozenset[str]] = frozenset()


class CloudDriveConfig(ConfigSection):
    SECRET_FIELDS: ClassVar[frozenset[str]] = frozenset({'api_token'})

    address: str = ''
    api_token: str = ''
    secure: bool = True
    task_dir_path: str = ''
    cloud_name: str = ''
    cloud_account_id: str = ''

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


class RssConfig(ConfigSection):
    enabled: bool = False
    interval_seconds: int = 1800
    actor_label: str = 'Actor'
    rank_label: str = 'Rank'
    failed_avid_cooldown_seconds: int = 86_400

    @field_validator('interval_seconds', 'failed_avid_cooldown_seconds')
    @classmethod
    def _positive(cls, value: int) -> int:
        if value < 1:
            msg = 'must be positive'
            raise ValueError(msg)
        return value


class ArchiveConfig(ConfigSection):
    enabled: bool = False
    src_dir: str = ''
    dst_dir: str = ''
    # Per-subdirectory routing below src_dir/dst_dir, e.g. {'intake': 'library'}.
    mapping: dict[str, str] = {}
    min_size_mb: int = 0
    # Destination subdirectory -> list of brands routed into it.
    brand_mapping: dict[str, tuple[str, ...]] = {}

    @field_validator('min_size_mb')
    @classmethod
    def _non_negative(cls, value: int) -> int:
        if value < 0:
            msg = 'must not be negative'
            raise ValueError(msg)
        return value

    @property
    def configured(self) -> bool:
        return bool(self.src_dir and self.dst_dir and self.mapping)


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


SECTION_MODELS: dict[str, type[ConfigSection]] = {
    'clouddrive': CloudDriveConfig,
    'freshrss': FreshRSSConfig,
    'feeds': FeedsConfig,
    'avid': AvidRulesConfig,
    'rss': RssConfig,
    'archive': ArchiveConfig,
    'mapping': MappingConfig,
}
