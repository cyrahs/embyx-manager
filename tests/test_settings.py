import pytest

from embyx_manager.settings import Settings


def test_settings_read_the_deployment_level_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('EMBYX_MANAGER_DATABASE_URL', 'postgresql://db.internal/embyx')
    monkeypatch.setenv('EMBYX_MANAGER_MAX_ACTORS', '5')
    monkeypatch.setenv('EMBYX_MANAGER_MAX_VIDEOS', '50')
    monkeypatch.setenv('EMBYX_MANAGER_MAGNET_CONCURRENCY', '2')

    settings = Settings.from_env()

    assert settings.database_url == 'postgresql://db.internal/embyx'
    assert settings.max_actors == 5
    assert settings.max_videos == 50
    assert settings.magnet_concurrency == 2


def test_library_paths_are_not_read_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """They live in the config store; a leftover variable must not affect startup."""
    monkeypatch.setenv('EMBYX_MANAGER_ACTOR_ROOT', '/media/actor')
    monkeypatch.setenv('EMBYX_MANAGER_APPLY_ENABLED', 'true')
    monkeypatch.setenv('EMBYX_MANAGER_CLOUD_SOURCE_ROOTS', '/cloud/a')

    settings = Settings.from_env()

    assert not hasattr(settings, 'actor_brand_path')
    assert not hasattr(settings, 'apply_enabled')


def test_positive_limits_are_rejected_when_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('EMBYX_MANAGER_MAX_ACTORS', '0')

    with pytest.raises(ValueError, match='MAX_ACTORS must be positive'):
        Settings.from_env()


def test_non_loopback_binding_requires_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('EMBYX_MANAGER_HOST', '192.0.2.1')
    monkeypatch.delenv('EMBYX_MANAGER_API_TOKEN', raising=False)

    with pytest.raises(ValueError, match='API_TOKEN'):
        Settings.from_env()


def test_non_loopback_binding_accepts_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('EMBYX_MANAGER_HOST', '192.0.2.1')
    monkeypatch.setenv('EMBYX_MANAGER_API_TOKEN', 'configured-at-runtime')
    monkeypatch.setenv('EMBYX_MANAGER_TLS_TERMINATED', 'true')

    assert Settings.from_env().host == '192.0.2.1'


def test_non_loopback_binding_requires_tls_termination(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('EMBYX_MANAGER_HOST', '192.0.2.1')
    monkeypatch.setenv('EMBYX_MANAGER_API_TOKEN', 'configured-at-runtime')
    monkeypatch.delenv('EMBYX_MANAGER_TLS_TERMINATED', raising=False)

    with pytest.raises(ValueError, match='TLS_TERMINATED'):
        Settings.from_env()
