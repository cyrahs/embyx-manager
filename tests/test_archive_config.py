"""Archive configuration section: routing tables the settings UI edits as rows."""

import pytest

from embyx_manager.config import ArchiveConfig


def test_defaults_leave_the_pipeline_unconfigured() -> None:
    config = ArchiveConfig()

    assert config.mapping == {}
    assert config.priority_mapping == {}
    assert config.brand_mapping == {}
    assert config.configured is False


def test_scan_cron_normalizes_whitespace() -> None:
    assert ArchiveConfig().scan_cron == '0 4 * * *'
    assert ArchiveConfig(scan_cron='  30  3 * * 1 ').scan_cron == '30 3 * * 1'


@pytest.mark.parametrize('expr', ['', 'every day at four', '0 4 * *', '99 4 * * *'])
def test_rejects_an_unusable_scan_cron(expr: str) -> None:
    with pytest.raises(ValueError, match='scan_cron'):
        ArchiveConfig(scan_cron=expr)


def test_mapping_normalizes_subdirectories() -> None:
    config = ArchiveConfig(src_dir='/downloads', dst_dir='/library', mapping={' intake/ ': './sorted'})

    assert config.mapping == {'intake': 'sorted'}
    assert config.configured is True


def test_priority_mapping_normalizes_subdirectories() -> None:
    config = ArchiveConfig(src_dir='/downloads', dst_dir='/library', priority_mapping={' vip/ ': './starred'})

    assert config.priority_mapping == {'vip': 'starred'}
    assert config.configured is True


def test_both_tables_may_share_a_destination() -> None:
    config = ArchiveConfig(
        src_dir='/downloads',
        dst_dir='/library',
        mapping={'intake': 'sorted'},
        priority_mapping={'vip': 'sorted'},
    )

    assert config.mapping == {'intake': 'sorted'}
    assert config.priority_mapping == {'vip': 'sorted'}


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
        ({'priority_mapping': {'/vip': 'starred'}}, r'archive\.priority_mapping source must be relative'),
        ({'priority_mapping': {'vip': '../escape'}}, r'archive\.priority_mapping destination must not contain'),
        (
            {'priority_mapping': {'vip': 'starred', ' vip/ ': 'other'}},
            r"archive\.priority_mapping must not repeat the source subdirectory 'vip'",
        ),
        (
            {'mapping': {'intake': 'sorted'}, 'priority_mapping': {'./intake': 'starred'}},
            "must not share the source subdirectory 'intake'",
        ),
    ],
)
def test_rejects_unusable_routes(values: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        ArchiveConfig(**values)
