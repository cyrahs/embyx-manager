import json
from datetime import date

import pytest

from embyx_manager.clients.avbase import (
    AvbaseCastMember,
    AvbaseClient,
    AvbaseTalent,
    AvbaseUnavailableError,
    parse_release_date,
    parse_talent_query,
    strip_prefix,
)

HOST = 'https://avbase.test'


class FakeResponse:
    def __init__(self, status: int, content_type: str, body: str) -> None:
        self.status_code = status
        self.headers = {'content-type': content_type}
        self.text = body

    def json(self) -> object:
        return json.loads(self.text)


class FakeSession:
    """Serves the home page (with the current build id) and a table of data routes."""

    def __init__(self, routes: dict[str, object], *, build_id: str = 'B1') -> None:
        self.routes = routes
        self.build_id = build_id
        self.calls: list[tuple[str, dict]] = []
        self.closed = False

    async def get(self, url: str, *, params: dict | None = None) -> FakeResponse:
        self.calls.append((url, dict(params or {})))
        if url == f'{HOST}/':
            return FakeResponse(
                200, 'text/html', f'<script id="__NEXT_DATA__">{{"buildId":"{self.build_id}"}}</script>'
            )
        key = url.removeprefix(f'{HOST}/_next/data/')
        page = (params or {}).get('page')
        if page:
            key = f'{key}?page={page}'
        body = self.routes.get(key)
        if body is None:
            return FakeResponse(404, 'text/html', '<html>404</html>')
        if isinstance(body, str):
            return FakeResponse(200, 'text/html', body)
        return FakeResponse(200, 'application/json; charset=utf-8', json.dumps(body))

    async def close(self) -> None:
        self.closed = True


def talent_props(*, total: int = 2, works: list[dict] | None = None) -> dict:
    return {
        'pageProps': {
            'name': '河北彩伽',
            'talent': {
                'id': 5022,
                'primary': {'id': 5031, 'name': '河北彩花'},
                'actors': [{'id': 5031, 'name': '河北彩花'}, {'id': 66947, 'name': '河北彩伽'}, {'id': 1, 'name': ' '}],
            },
            'total': total,
            'works': works
            if works is not None
            else [
                {
                    'work_id': 'MIZD-555',
                    'prefix': 'moodyz',
                    'title': 'best',
                    'min_date': 'Fri Oct 02 2026 09:00:00 GMT+0900 (Japan Standard Time)',
                    'actors': [{'id': 5031, 'name': '河北彩花'}],
                },
            ],
        },
    }


def make_client(routes: dict[str, object], **session_options: object) -> tuple[AvbaseClient, FakeSession]:
    session = FakeSession(routes, **session_options)  # type: ignore[arg-type]
    return AvbaseClient(host=HOST, session=session), session


async def test_a_talent_is_found_under_any_of_its_names_with_the_others_as_aliases() -> None:
    client, session = make_client({'B1/talents/%E6%B2%B3%E5%8C%97%E5%BD%A9%E4%BC%BD.json': talent_props()})

    talent = await client.talent('河北彩伽')

    assert talent == AvbaseTalent(talent_id=5022, name='河北彩花', aliases=('河北彩伽',), total_works=2)
    assert talent.names == ('河北彩花', '河北彩伽')
    assert session.calls[0][0] == f'{HOST}/'
    assert session.calls[1][1] == {'name': '河北彩伽'}


async def test_a_stale_build_id_is_refreshed_once_and_the_route_retried() -> None:
    client, session = make_client({'B2/talents/x.json': talent_props()})
    await client.build_id()  # learned B1 before the site was redeployed
    session.build_id = 'B2'

    talent = await client.talent('x')

    assert talent is not None
    assert [url.removeprefix(HOST) for url, _ in session.calls] == [
        '/',
        '/_next/data/B1/talents/x.json',
        '/',
        '/_next/data/B2/talents/x.json',
    ]


async def test_an_unknown_name_is_none_once_the_build_id_is_confirmed() -> None:
    client, session = make_client({})

    assert await client.talent('nobody') is None
    # One refresh to rule out a stale id, no second data request.
    assert [url.removeprefix(HOST) for url, _ in session.calls] == ['/', '/_next/data/B1/talents/nobody.json', '/']


async def test_a_challenge_page_in_place_of_json_is_an_unavailable_error() -> None:
    client, _ = make_client({'B1/talents/x.json': '<html>Just a moment...</html>'})

    with pytest.raises(AvbaseUnavailableError):
        await client.talent('x')


async def test_works_are_read_across_every_page_of_the_talent() -> None:
    second = talent_props(
        total=31,
        works=[{'work_id': 'REBD-1013', 'prefix': '', 'title': 'old', 'min_date': '2026-02-19', 'actors': []}],
    )
    client, session = make_client({'B1/talents/x.json': talent_props(total=31), 'B1/talents/x.json?page=2': second})

    works = await client.talent_works('x')

    assert [(work.work_id, work.prefix, work.release_date) for work in works] == [
        ('MIZD-555', 'moodyz', date(2026, 10, 2)),
        ('REBD-1013', '', date(2026, 2, 19)),
    ]
    assert [params.get('page') for _, params in session.calls[1:]] == [None, 2]


async def test_a_work_carries_its_cast_with_talent_ids() -> None:
    client, _ = make_client(
        {
            'B1/works/moodyz%3AMIZD-555.json': {
                'pageProps': {
                    'work': {
                        'work_id': 'MIZD-555',
                        'prefix': 'moodyz',
                        'title': 'best',
                        'min_date': 'Fri Oct 02 2026 09:00:00 GMT+0900 (Japan Standard Time)',
                        'casts': [
                            {'actor': {'id': 35, 'name': '松本いちか', 'talent': {'id': 35}}},
                            {'actor': {'id': 2041, 'name': '七沢みあ', 'talent': {'id': 2037}}},
                            {'actor': {'id': 9, 'name': ''}},
                        ],
                    },
                },
            },
        },
    )

    work = await client.work('moodyz:MIZD-555')

    assert work is not None
    assert work.work_id == 'MIZD-555'
    assert work.release_date == date(2026, 10, 2)
    assert work.cast == (
        AvbaseCastMember(actor_id=35, name='松本いちか', talent_id=35),
        AvbaseCastMember(actor_id=2041, name='七沢みあ', talent_id=2037),
    )


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        ('Fri Oct 02 2026 09:00:00 GMT+0900 (Japan Standard Time)', date(2026, 10, 2)),
        ('2026-02-19T00:00:00.000Z', date(2026, 2, 19)),
        ('Thu Feb 30 2026 09:00:00', None),
        ('', None),
        (None, None),
    ],
)
def test_release_dates_are_read_from_javascript_or_iso_text(value: object, expected: date | None) -> None:
    assert parse_release_date(value) == expected


FEED_XML = (
    '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel>'
    '<title><![CDATA[「河北彩花」のフィード]]></title>'
    '<link>https://www.avbase.net/talents/%E6%B2%B3%E5%8C%97%E5%BD%A9%E8%8A%B1</link>'
    '</channel></rss>'
)


@pytest.mark.parametrize(
    ('query', 'expected'),
    [
        ('河北彩伽', (None, '河北彩伽')),
        (' 5022 ', (5022, '')),
        ('https://www.avbase.net/talents/5022', (5022, '')),
        ('https://www.avbase.net/talents/5022/feed', (5022, '')),
        ('https://www.avbase.net/talents/%E6%B2%B3%E5%8C%97%E5%BD%A9%E8%8A%B1?tab=works', (None, '河北彩花')),
    ],
)
def test_a_talent_query_is_a_name_an_id_or_a_talent_url(query: str, expected: tuple[int | None, str]) -> None:
    assert parse_talent_query(query) == expected


@pytest.mark.parametrize('query', ['', '   ', 'https://www.avbase.net/works/MIZD-555', '/actors/1'])
def test_other_text_is_not_a_talent_query(query: str) -> None:
    with pytest.raises(ValueError, match='talent'):
        parse_talent_query(query)


async def test_a_talent_is_found_from_its_id_through_the_feed() -> None:
    client, session = make_client(
        {
            'B1/talents/%E6%B2%B3%E5%8C%97%E5%BD%A9%E8%8A%B1.json': talent_props(),
            'B1/talents/%E6%B2%B3%E5%8C%97%E5%BD%A9%E4%BC%BD.json': talent_props(),
            f'{HOST}/talents/5022/feed': FEED_XML,
        }
    )
    try:
        by_url = await client.find_talent('https://www.avbase.net/talents/5022/feed')
        by_name = await client.find_talent('河北彩伽')
        missing = await client.find_talent('99999')
        nonsense = await client.find_talent('https://www.avbase.net/works/MIZD-555')
    finally:
        await client.aclose()

    assert by_url == AvbaseTalent(talent_id=5022, name='河北彩花', aliases=('河北彩伽',), total_works=2)
    assert by_name == by_url
    assert missing is None
    assert nonsense is None
    assert (f'{HOST}/talents/5022/feed', {}) in session.calls


async def test_a_feed_naming_a_talent_whose_page_is_gone_still_yields_the_id() -> None:
    client, _ = make_client({f'{HOST}/talents/5022/feed': FEED_XML})
    try:
        talent = await client.find_talent('5022')
    finally:
        await client.aclose()

    assert talent == AvbaseTalent(talent_id=5022, name='河北彩花', aliases=(), total_works=0)


def test_the_storefront_prefix_is_stripped_from_work_ids() -> None:
    assert strip_prefix('moodyz:MIZD-555') == 'MIZD-555'
    assert strip_prefix('DLDSS-515') == 'DLDSS-515'


async def test_a_bare_work_id_is_found_through_the_search_and_its_prefixed_route() -> None:
    detail = {
        'pageProps': {
            'work': {
                'work_id': 'MDVR-394',
                'prefix': 'moodyz',
                'title': 'vr',
                'min_date': '2026-01-01',
                'casts': [{'actor': {'id': 1, 'name': '輝星きら', 'talent': {'id': 67548}}}],
            },
        },
    }
    listing = {
        'pageProps': {'works': [{'work_id': 'MDVR-394', 'prefix': 'moodyz', 'actors': [{'id': 1, 'name': '輝星きら'}]}]}
    }
    client, session = make_client({'B1/works.json': listing, 'B1/works/moodyz%3AMDVR-394.json': detail})

    work = await client.work('MDVR-394')

    assert work is not None
    assert work.cast[0].talent_id == 67548
    assert [params for _, params in session.calls[1:]] == [{'q': 'MDVR-394'}, {'id': 'moodyz:MDVR-394'}]
    assert await client.work('NOPE-1') is None
