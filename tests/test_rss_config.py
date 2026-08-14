"""RSS configuration section: the categories the settings UI edits as rows."""

import pytest

from embyx_manager.config import RssConfig
from embyx_manager.config.models import RssCategory


def test_defaults_have_no_categories() -> None:
    # A category needs a directory, which cannot be guessed, so a fresh install
    # starts empty and the readiness gate asks for one.
    assert RssConfig().categories == ()


def test_a_category_directory_is_normalized() -> None:
    config = RssConfig(categories=[{'label': ' Rank ', 'task_dir_path': '/115/embyx_in/rank/'}])

    assert config.categories[0].label == 'Rank'
    assert config.categories[0].task_dir_path == '/115/embyx_in/rank'


@pytest.mark.parametrize('label', ['', '   '])
def test_rejects_a_category_without_a_label(label: str) -> None:
    with pytest.raises(ValueError, match='label'):
        RssConfig(categories=[{'label': label, 'task_dir_path': '/115/rank'}])


@pytest.mark.parametrize('task_dir', [None, '', '   '])
def test_rejects_a_category_without_a_directory(task_dir: str | None) -> None:
    # There is no shared default to fall back on: the directory decides which
    # archive route files the download, so it has to be stated.
    entry = {'label': 'Rank'} if task_dir is None else {'label': 'Rank', 'task_dir_path': task_dir}
    with pytest.raises(ValueError, match='task_dir_path'):
        RssConfig(categories=[entry])


def test_rejects_a_repeated_label() -> None:
    # Both entries would ingest the same items, and the second directory would
    # silently lose to the first.
    with pytest.raises(ValueError, match='must not repeat the label'):
        RssConfig(
            categories=[
                {'label': 'Rank', 'task_dir_path': '/115/a'},
                {'label': 'Rank', 'task_dir_path': '/115/b'},
            ],
        )


def test_categories_may_share_one_directory() -> None:
    config = RssConfig(
        categories=[
            {'label': 'Actor', 'task_dir_path': '/115/embyx_in'},
            {'label': 'Star', 'task_dir_path': '/115/embyx_in'},
        ],
    )

    assert {category.task_dir_path for category in config.categories} == {'/115/embyx_in'}


def test_rejects_a_relative_category_directory() -> None:
    with pytest.raises(ValueError, match='absolute path'):
        RssConfig(categories=[{'label': 'Rank', 'task_dir_path': 'embyx_in/rank'}])


def test_rejects_an_unknown_key() -> None:
    with pytest.raises(ValueError, match='rank_label'):
        RssConfig(rank_label='Rank')


def test_no_categories_is_valid_but_leaves_nothing_to_ingest() -> None:
    # The readiness gate reports this; the model itself stays permissive so a
    # half-finished edit in the settings UI is not a validation error.
    assert RssConfig(categories=()).categories == ()


def test_a_category_round_trips_through_the_stored_json() -> None:
    config = RssConfig(categories=(RssCategory(label='Rank', task_dir_path='/115/rank'),))

    assert RssConfig.model_validate_json(config.model_dump_json()) == config
