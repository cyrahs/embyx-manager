from datetime import UTC, datetime, timedelta

import pytest

from embyx_manager.monitor.subscriptions import (
    SubscriptionExistsError,
    SubscriptionKind,
    SubscriptionRepository,
    validate_feed_url,
)
from tests.conftest import make_database, postgres_test_dsn

NOW = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)


def make_repository() -> SubscriptionRepository:
    postgres_test_dsn()
    return SubscriptionRepository(make_database())


@pytest.mark.parametrize(
    'url',
    [
        'https://rsshub.test/javbus/star/rwt',
        'http://rsshub:1200/x?limit=20',
        ' https://sukebei.nyaa.si/?page=rss&q=a ',
        'ftp://x',
    ],
)
def test_feed_urls_must_be_http_without_credentials(url: str) -> None:
    if url.startswith('ftp'):
        with pytest.raises(ValueError, match='absolute HTTP'):
            validate_feed_url(url)
    else:
        assert validate_feed_url(url) == url.strip()
    with pytest.raises(ValueError, match='credentials'):
        validate_feed_url('https://user:pw@rsshub.test/x')


async def test_add_list_and_get_round_trip() -> None:
    repository = make_repository()

    created = await repository.add_rss(
        url='https://rsshub.test/javbus/star/rwt', category='Actor', now=NOW, name='演员甲'
    )

    assert created.kind is SubscriptionKind.RSS
    assert created.feed_url == 'https://rsshub.test/javbus/star/rwt'
    assert created.display_name == '演员甲'
    assert created.enabled is True
    assert created.cursor == ()
    assert created.seed_pending is False
    assert await repository.list() == (created,)
    assert await repository.get(created.id) == created


async def test_the_same_feed_cannot_be_subscribed_twice() -> None:
    repository = make_repository()
    await repository.add_rss(url='https://rsshub.test/a', category='Actor', now=NOW)

    with pytest.raises(SubscriptionExistsError):
        await repository.add_rss(url='https://rsshub.test/a', category='Rank', now=NOW)


async def test_update_and_delete() -> None:
    repository = make_repository()
    created = await repository.add_rss(url='https://rsshub.test/a', category='Actor', now=NOW)

    disabled = await repository.update(created.id, now=NOW + timedelta(minutes=1), enabled=False)
    assert disabled is not None
    assert disabled.enabled is False
    assert disabled.category == 'Actor'

    moved = await repository.update(created.id, now=NOW, category='Rank')
    assert moved is not None
    assert moved.category == 'Rank'
    assert moved.enabled is False

    assert await repository.update(created.id + 1000, now=NOW, enabled=True) is None
    assert await repository.delete(created.id) is True
    assert await repository.delete(created.id) is False


async def test_record_poll_stores_the_cursor_and_settles_the_seed() -> None:
    repository = make_repository()
    created = await repository.add_rss(url='https://rsshub.test/a', category='Actor', now=NOW, seed_pending=True)

    await repository.record_poll(created.id, now=NOW, cursor=['k1', 'k2', 'k1'], error=None)
    record = await repository.get(created.id)
    assert record is not None
    assert record.cursor == ('k1', 'k2')
    assert record.seed_pending is False
    assert record.last_polled_at == NOW
    assert record.last_error is None

    # A failed poll keeps the cursor so the items it missed are read next time.
    await repository.record_poll(created.id, now=NOW + timedelta(hours=1), cursor=None, error='boom')
    record = await repository.get(created.id)
    assert record is not None
    assert record.cursor == ('k1', 'k2')
    assert record.last_error == 'boom'
    assert record.last_polled_at == NOW + timedelta(hours=1)
