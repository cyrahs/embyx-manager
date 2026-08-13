"""Fill Actor configuration section: validation and the runtime snapshot built from it."""

from pathlib import Path

import pytest

from embyx_manager.bootstrap import _build_fill_actor_runtime
from embyx_manager.config import FillActorConfig


def test_defaults_carry_no_deployment_paths() -> None:
    """Real paths belong in the database; the shipped defaults must stay empty."""
    config = FillActorConfig()

    assert config.actor_root == ''
    assert config.additional_roots == ()
    assert config.move_in_root == ''
    assert config.apply_enabled is False
    assert config.configured is False


@pytest.mark.parametrize(
    ('values', 'message'),
    [
        ({'actor_root': 'media/actor'}, 'must be an absolute path'),
        ({'actor_root': '/a/../b'}, 'must not contain'),
        ({'additional_roots': ('/a', '/a')}, 'must not repeat'),
        (
            {'actor_root': '/a', 'additional_roots': ('/a',), 'move_in_root': '/m'},
            'must not also be an additional root',
        ),
        ({'cloud_move_in_root': '/cloud/in'}, 'must be configured together'),
        ({'apply_enabled': True}, 'requires the CloudDrive move path configuration'),
        ({'root_sentinel': 'nested/file'}, 'must be a single path segment'),
        (
            {
                'additional_roots': ('/a', '/b'),
                'cloud_strm_mount_prefix': '/mnt',
                'cloud_source_roots': ('/cloud/a',),
                'cloud_move_in_root': '/cloud/in',
            },
            'one-for-one',
        ),
    ],
)
def test_rejects_invalid_combinations(values: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        FillActorConfig(**values)


def test_runtime_snapshot_tracks_the_stored_version() -> None:
    config = FillActorConfig(
        actor_root='/media/actor',
        additional_roots=('/media/extra',),
        move_in_root='/media/in',
        move_in_by_brand=True,
    )

    runtime = _build_fill_actor_runtime(config, 7)

    assert runtime.version == 7
    assert runtime.configured is True
    assert runtime.paths is not None
    assert runtime.paths.actor_brand_path == Path('/media/actor')
    assert runtime.paths.additional_brand_paths == (Path('/media/extra'),)
    assert runtime.move_in_by_brand is True
    assert runtime.cloud_move_paths is None


def test_unconfigured_section_yields_an_idle_runtime() -> None:
    runtime = _build_fill_actor_runtime(FillActorConfig(), 3)

    assert runtime.version == 3
    assert runtime.configured is False
    assert runtime.paths is None
