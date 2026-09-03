"""The one-off JavBus -> AVBase subscription migration script, exercised with fakes."""

import importlib.util
import sys
from pathlib import Path

import pytest

from embyx_manager.clients.avbase import AvbaseCastMember, AvbaseTalent, AvbaseWork
from embyx_manager.clients.javbus import JavBusActorPage

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'migrate_javbus_subscriptions.py'
spec = importlib.util.spec_from_file_location('migrate_javbus_subscriptions', SCRIPT)
assert spec is not None
assert spec.loader is not None
migrate = importlib.util.module_from_spec(spec)
# Registered before executing: dataclasses resolve the module's postponed annotations through sys.modules.
sys.modules[spec.name] = migrate
spec.loader.exec_module(migrate)


class FakeAvbase:
    def __init__(self, talents: dict[str, AvbaseTalent], works: dict[str, AvbaseWork] | None = None) -> None:
        self.talents = talents
        self.works = works or {}
        self.looked_up: list[str] = []

    async def talent(self, name: str) -> AvbaseTalent | None:
        self.looked_up.append(name)
        return self.talents.get(name)

    async def work(self, work_id: str) -> AvbaseWork | None:
        return self.works.get(work_id)

    async def search_works(self, query: str) -> list[AvbaseWork]:
        work = self.works.get(query)
        return [work] if work is not None else []


class FakeJavBus:
    def __init__(self, pages: dict[str, JavBusActorPage]) -> None:
        self.pages = pages

    async def get_actor(self, actor_id: str) -> JavBusActorPage | None:
        return self.pages.get(actor_id)


MIO = AvbaseTalent(talent_id=46144, name='石川澪', aliases=(), total_works=184)
SAIKA = AvbaseTalent(talent_id=5022, name='河北彩花', aliases=('河北彩伽',), total_works=283)


def work(work_id: str, *talent_ids: int) -> AvbaseWork:
    names = {46144: '石川澪', 5022: '河北彩花', 35: '松本いちか', 2037: '七沢みあ'}
    return AvbaseWork(
        work_id=work_id,
        prefix='',
        title=work_id,
        release_date=None,
        cast=tuple(
            AvbaseCastMember(actor_id=talent_id, name=names[talent_id], talent_id=talent_id) for talent_id in talent_ids
        ),
    )


@pytest.mark.parametrize(
    ('url', 'expected'),
    [
        ('http://rsshub/javbus/star/rwt', 'rwt'),
        ('https://old.example/prefix/javbus/star/A-1/', 'A-1'),
        ('https://rsshub.example/javbus/star/rwt?format=rss', 'rwt'),
        ('http://rsshub/javbus/ja/star/rwd', 'rwd'),
        ('http://rsshub/javbus/en/star/x/extra', None),
        ('https://rsshub.example/javbus/actor/rwt', None),
        ('https://rsshub.example/javlibrary/rank', None),
    ],
)
def test_star_ids_are_read_from_rsshub_javbus_urls(url: str, expected: str | None) -> None:
    assert migrate.star_id_of(url) == expected


def test_feed_titles_yield_name_candidates_without_the_javbus_prefix() -> None:
    assert migrate.name_candidates('JavBus - 石川澪') == ['石川澪']
    fullwidth = '河北彩花\uff08河北彩伽\uff09'
    assert migrate.name_candidates(fullwidth) == [fullwidth, '河北彩花', '河北彩伽']
    assert migrate.name_candidates(None) == []


def test_opml_rows_carry_their_category(tmp_path: Path) -> None:
    opml = tmp_path / 'feeds.opml'
    opml.write_text(
        '<?xml version="1.0"?><opml version="1.0"><body>'
        '<outline text="Actor"><outline type="rss" text="JavBus - 石川澪" xmlUrl="http://rsshub/javbus/star/xvn"/>'
        '<outline type="rss" text="other" xmlUrl="http://rsshub/javlibrary/rank"/></outline>'
        '<outline text="Rank">'
        '<outline type="rss" text="JavBus - 七沢みあ" xmlUrl="http://rsshub/javbus/star/rwt"/></outline>'
        '</body></opml>',
    )

    rows = migrate.rows_from_opml(opml)

    assert [(row.star_id, row.title, row.category) for row in rows] == [
        ('xvn', 'JavBus - 石川澪', 'Actor'),
        ('rwt', 'JavBus - 七沢みあ', 'Rank'),
    ]


async def test_the_feed_title_resolves_the_talent_through_any_alias() -> None:
    row = migrate.Row(star_id='sl1', url='http://rsshub/javbus/star/sl1', title='JavBus - 河北彩伽', category='Actor')
    avbase = FakeAvbase({'河北彩伽': SAIKA, '河北彩花': SAIKA})

    await migrate.resolve_row(row, avbase, FakeJavBus({}))

    assert (row.talent_id, row.talent_name, row.aliases, row.method) == (5022, '河北彩花', ['河北彩伽'], 'name')


async def test_the_star_page_name_is_tried_when_the_title_is_useless() -> None:
    row = migrate.Row(star_id='xvn', url='http://rsshub/javbus/star/xvn', title='renamed feed', category='Actor')
    avbase = FakeAvbase({'石川澪': MIO})
    javbus = FakeJavBus({'xvn': JavBusActorPage(actor_id='xvn', name='石川澪', video_ids=('MIDA-798',))})

    await migrate.resolve_row(row, avbase, javbus)

    assert (row.talent_id, row.method, row.javbus_name) == (46144, 'name', '石川澪')
    assert avbase.looked_up == ['renamed feed', '石川澪']
    assert row.cross_check == 'skipped'


async def test_a_name_match_is_cross_checked_against_a_work_off_the_star_page() -> None:
    star = JavBusActorPage(actor_id='xvn', name='石川澪', video_ids=('GONE-1', 'A-1'))
    javbus = FakeJavBus({'xvn': star})

    confirmed = migrate.Row(star_id='xvn', url='u', title='JavBus - 石川澪', category='Actor')
    await migrate.resolve_row(confirmed, FakeAvbase({'石川澪': MIO}, works={'A-1': work('A-1', 46144, 35)}), javbus)
    assert confirmed.cross_check == 'ok'

    # The star's works credit other names (a compilation, or another person): flagged, ordered last.
    unconfirmed = migrate.Row(star_id='xvn', url='u', title='JavBus - 石川澪', category='Actor')
    await migrate.resolve_row(unconfirmed, FakeAvbase({'石川澪': MIO}, works={'A-1': work('A-1', 35)}), javbus)
    assert unconfirmed.cross_check == 'unconfirmed'
    assert migrate.next_batch([unconfirmed, confirmed], batch=5, only=None) == [confirmed, unconfirmed]


async def test_the_works_on_the_star_page_bridge_a_name_avbase_does_not_know() -> None:
    row = migrate.Row(star_id='xvn', url='http://rsshub/javbus/star/xvn', title='JavBus - 旧芸名', category='Actor')
    avbase = FakeAvbase(
        {'石川澪': MIO},
        works={
            'A-1': work('A-1', 46144),
            'A-2': work('A-2', 46144, 35, 2037),
            'A-3': work('A-3', 46144, 2037),
        },
    )
    javbus = FakeJavBus(
        {'xvn': JavBusActorPage(actor_id='xvn', name='旧芸名', video_ids=('A-1', 'A-2', 'A-3', 'GONE-1'))}
    )

    await migrate.resolve_row(row, avbase, javbus)

    assert (row.talent_id, row.talent_name, row.method) == (46144, '石川澪', 'avid')
    assert "3/3 works ['A-1', 'A-2', 'A-3'] credit talent 46144" in row.evidence


async def test_works_that_share_no_talent_leave_the_row_unresolved() -> None:
    row = migrate.Row(star_id='xvn', url='http://rsshub/javbus/star/xvn', title='JavBus - 旧芸名', category='Actor')
    avbase = FakeAvbase({}, works={'A-1': work('A-1', 46144), 'A-2': work('A-2', 35)})
    javbus = FakeJavBus({'xvn': JavBusActorPage(actor_id='xvn', name='旧芸名', video_ids=('A-1', 'A-2'))})

    await migrate.resolve_row(row, avbase, javbus)

    assert row.talent_id is None
    assert row.method == 'unresolved'
    assert 'share no talent' in row.evidence


async def test_a_missing_star_page_is_reported_not_fatal() -> None:
    row = migrate.Row(star_id='zzz', url='http://rsshub/javbus/star/zzz', title='JavBus - nobody', category='Actor')

    await migrate.resolve_row(row, FakeAvbase({}), FakeJavBus({}))

    assert row.method == 'unresolved'
    assert 'no star page works' in row.evidence


def test_the_next_batch_skips_applied_and_unresolved_rows() -> None:
    rows = [
        migrate.Row(star_id='a', url='u', title=None, category='Actor', talent_id=1, talent_name='A', method='name'),
        migrate.Row(
            star_id='b',
            url='u',
            title=None,
            category='Actor',
            talent_id=2,
            talent_name='B',
            method='name',
            applied={'new_subscription_id': 9},
        ),
        migrate.Row(star_id='c', url='u', title=None, category='Actor'),
        migrate.Row(star_id='d', url='u', title=None, category='Actor', talent_id=4, talent_name='D', method='avid'),
    ]

    assert [row.star_id for row in migrate.next_batch(rows, batch=5, only=None)] == ['a', 'd']
    assert [row.star_id for row in migrate.next_batch(rows, batch=1, only=None)] == ['a']
    assert [row.star_id for row in migrate.next_batch(rows, batch=5, only={'d'})] == ['d']


def test_verify_flags_a_seed_that_did_not_settle() -> None:
    row = migrate.Row(
        star_id='a',
        url='u',
        title=None,
        category='Actor',
        subscription_id=1,
        talent_id=46144,
        talent_name='石川澪',
        method='name',
        applied={'new_subscription_id': 2, 'old_disabled': True, 'at': '2026-09-03T00:00:00+00:00'},
    )
    listing = {
        1: {'id': 1, 'kind': 'rss', 'enabled': False, 'talent_id': None},
        2: {
            'id': 2,
            'kind': 'avbase_talent',
            'enabled': True,
            'talent_id': 46144,
            'last_polled_at': '2026-09-03T01:00:00+00:00',
            'last_error': None,
            'seed_pending': True,
            'cursor_size': 0,
        },
    }
    feed = {'ok': True, 'reason': None, 'feed_name': '石川澪', 'items': 30}

    result = migrate.verify_row(row, listing, feed_check=lambda _talent_id, _names: feed)

    assert result['polled'] is True
    assert result['ok'] is False
    assert result['problems'] == ['seed still pending after a poll', 'cursor holds 0 items, feed has 30']


def test_verify_passes_a_settled_seed() -> None:
    row = migrate.Row(
        star_id='a',
        url='u',
        title=None,
        category='Actor',
        subscription_id=1,
        talent_id=46144,
        talent_name='石川澪',
        method='name',
        applied={'new_subscription_id': 2, 'old_disabled': True, 'at': '2026-09-03T00:00:00+00:00'},
    )
    listing = {
        1: {'id': 1, 'kind': 'rss', 'enabled': False, 'talent_id': None},
        2: {
            'id': 2,
            'kind': 'avbase_talent',
            'enabled': True,
            'talent_id': 46144,
            'last_polled_at': '2026-09-03T01:00:00+00:00',
            'last_error': None,
            'seed_pending': False,
            'cursor_size': 30,
        },
    }
    feed = {'ok': True, 'reason': None, 'feed_name': '石川澪', 'items': 30}

    result = migrate.verify_row(row, listing, feed_check=lambda _talent_id, _names: feed)

    assert result == {**result, 'ok': True, 'problems': [], 'polled': True}


def test_rows_round_trip_through_the_mapping_file(tmp_path: Path) -> None:
    path = tmp_path / 'mapping.json'
    rows = [migrate.Row(star_id='a', url='u', title='t', category='Actor', talent_id=1, talent_name='A', method='name')]

    migrate.save_mapping(path, rows)

    assert migrate.load_mapping(path) == rows


def test_the_manager_listing_keeps_disabled_javbus_rows_and_applied_state_is_recovered() -> None:
    items = [
        {
            'id': 1,
            'kind': 'rss',
            'url': 'http://rsshub/javbus/star/a',
            'name': 'A',
            'category': 'Actor',
            'enabled': False,
        },
        {
            'id': 2,
            'kind': 'rss',
            'url': 'http://rsshub/javbus/star/b',
            'name': 'B',
            'category': 'Actor',
            'enabled': True,
        },
        {
            'id': 3,
            'kind': 'rss',
            'url': 'http://rsshub/javlibrary/rank',
            'name': 'R',
            'category': 'Rank',
            'enabled': True,
        },
        {
            'id': 9,
            'kind': 'avbase_talent',
            'talent_id': 100,
            'enabled': True,
            'created_at': '2026-09-03T02:00:00+00:00',
        },
    ]

    rows = migrate.rows_from_listing(items)
    assert [(row.star_id, row.old_enabled) for row in rows] == [('a', False), ('b', True)]

    rows[0].talent_id, rows[0].method = 100, 'name'
    rows[1].talent_id, rows[1].method = 200, 'name'
    migrate.reconcile_applied(rows, items)
    assert rows[0].applied == {
        'new_subscription_id': 9,
        'old_disabled': True,
        'at': '2026-09-03T02:00:00+00:00',
        'reconciled': True,
    }
    assert rows[1].applied is None


def test_resolving_a_subset_keeps_the_rest_of_the_mapping() -> None:
    kept = migrate.Row(
        star_id='a',
        url='u',
        title='A',
        category='Actor',
        talent_id=1,
        method='name',
        applied={'new_subscription_id': 9},
    )
    stale = migrate.Row(
        star_id='b',
        url='u',
        title='B',
        category='Actor',
        talent_id=2,
        method='name',
        applied={'new_subscription_id': 8},
    )
    fresh = migrate.Row(star_id='b', url='u', title='B renamed', category='Actor', talent_id=2, method='name')

    merged = migrate.merge_rows([kept, stale], [fresh])

    assert [row.star_id for row in merged] == ['a', 'b']
    assert merged[1].title == 'B renamed'
    assert merged[1].applied == {'new_subscription_id': 8}


def test_verify_reports_a_feed_check_that_blew_up_instead_of_stopping() -> None:
    row = migrate.Row(
        star_id='a',
        url='u',
        title=None,
        category='Actor',
        subscription_id=1,
        talent_id=46144,
        talent_name='石川澪',
        method='name',
        applied={'new_subscription_id': 2, 'old_disabled': True, 'at': '2026-09-03T00:00:00+00:00'},
    )
    listing = {
        1: {'id': 1, 'kind': 'rss', 'enabled': False, 'talent_id': None},
        2: {'id': 2, 'kind': 'avbase_talent', 'enabled': True, 'talent_id': 46144, 'last_polled_at': None},
    }

    def boom(_talent_id: int, _names: list[str]) -> dict:
        msg = 'not a feed'
        raise ValueError(msg)

    result = migrate.verify_row(row, listing, feed_check=boom)

    assert result['ok'] is False
    assert result['problems'] == ['feed check failed: ValueError: not a feed']
