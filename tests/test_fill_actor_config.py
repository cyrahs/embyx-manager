"""Fill Actor configuration section: validation and the environment-variable seeding."""

from pathlib import Path

import pytest

from embyx_manager.bootstrap import _build_fill_actor_runtime, _seed_fill_actor_config
from embyx_manager.config import FillActorConfig
from embyx_manager.config.store import ConfigVersionConflictError
from embyx_manager.settings import Settings


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


class RecordingStore:
    """Stands in for ConfigStore, mimicking its optimistic version check."""

    def __init__(self, *, stored_version: int = 0) -> None:
        self.stored_version = stored_version
        self.writes: list[tuple[str, dict, int | None]] = []

    async def update(self, section: str, values: dict, *, expected_version: int | None = None):
        if expected_version is not None and expected_version != self.stored_version:
            raise ConfigVersionConflictError(section, expected_version, self.stored_version)
        self.writes.append((section, values, expected_version))
        self.stored_version += 1
        return FillActorConfig(**values), self.stored_version


def env_settings(tmp_path: Path) -> Settings:
    return Settings(
        actor_brand_path=tmp_path / 'actor',
        additional_brand_paths=(tmp_path / 'extra',),
        move_in_path=tmp_path / 'move-in',
        move_in_by_brand=True,
        root_sentinel='.embyx-root',
    )


@pytest.mark.asyncio
async def test_seeding_writes_the_environment_values_against_version_zero(tmp_path: Path) -> None:
    store = RecordingStore()

    await _seed_fill_actor_config(store, env_settings(tmp_path))

    assert len(store.writes) == 1
    section, values, expected_version = store.writes[0]
    assert section == 'fill_actor'
    # Version 0 is what makes this land only on a section nobody has saved yet.
    assert expected_version == 0
    assert values['actor_root'] == str(tmp_path / 'actor')
    assert values['additional_roots'] == (str(tmp_path / 'extra'),)
    assert values['move_in_root'] == str(tmp_path / 'move-in')
    assert values['move_in_by_brand'] is True


@pytest.mark.asyncio
async def test_seeding_never_overwrites_a_section_saved_from_the_settings_page(tmp_path: Path) -> None:
    store = RecordingStore(stored_version=4)

    await _seed_fill_actor_config(store, env_settings(tmp_path))

    assert store.writes == []


@pytest.mark.asyncio
async def test_seeding_is_skipped_when_no_path_variables_are_set() -> None:
    store = RecordingStore()

    await _seed_fill_actor_config(store, Settings())

    assert store.writes == []
