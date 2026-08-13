"""Archive configuration section: routing tables the settings UI edits as rows."""

import pytest

from embyx_manager.config import ArchiveConfig


def test_defaults_leave_the_pipeline_unconfigured() -> None:
    config = ArchiveConfig()

    assert config.mapping == {}
    assert config.brand_mapping == {}
    assert config.configured is False


def test_mapping_normalizes_subdirectories() -> None:
    config = ArchiveConfig(src_dir='/downloads', dst_dir='/library', mapping={' intake/ ': './sorted'})

    assert config.mapping == {'intake': 'sorted'}
    assert config.configured is True


def test_brand_mapping_upper_cases_and_deduplicates_brands() -> None:
    """Brands are matched against upper-cased video IDs, so a lower-cased entry never routes."""
    config = ArchiveConfig(brand_mapping={'special': (' abc ', 'ABC', 'def')})

    assert config.brand_mapping == {'special': ('ABC', 'DEF')}


@pytest.mark.parametrize(
    ('values', 'message'),
    [
        ({'mapping': {'': 'sorted'}}, 'source must not be empty'),
        ({'mapping': {'intake': ''}}, 'destination must not be empty'),
        ({'mapping': {'/intake': 'sorted'}}, 'source must be relative'),
        ({'mapping': {'intake': '../escape'}}, 'destination must not contain'),
        ({'brand_mapping': {'special': ()}}, 'must list at least one brand'),
        ({'brand_mapping': {'special': ('  ',)}}, 'must list at least one brand'),
        ({'brand_mapping': {'/special': ('ABC',)}}, 'destination must be relative'),
    ],
)
def test_rejects_unusable_routes(values: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        ArchiveConfig(**values)
