"""jinjier.art source: which lists and entries survive, and how the archive is fetched."""

import io
import sqlite3
import zipfile

import httpx
import pytest

from embyx_manager.clients.jinjier import (
    JinjierClient,
    JinjierError,
    RankedEntry,
    is_excluded_code,
    list_key,
    parse_lists,
)
from embyx_manager.core.avid import AvidParser

# A slice of the real table: every family the rules have to tell apart.
ROWS: list[tuple[int, int, str, str]] = [
    # kind, number, name, note
    (7, 1, 'SSNI-497 新任なのに常にパンスト挑発してくる小悪魔な美脚女教師 橋本ありな', 'JavDB 有码 TOP250'),
    (7, 2, 'IPX-811 -媚薬で翌朝まで覚醒絶頂- キメセク相部屋NTR姦', 'JavDB 有码 TOP250'),
    (7, 3, 'ssni-497 same title, other spelling', 'JavDB 有码 TOP250'),
    (7, 4, 'ABP-984 中出し 射精執行官 05', 'JavDB 有码 TOP250'),
    (6, 1, 'LAFBD-41  無碼 ラフォーレ ガール Vol.41', 'JavDB TOP250'),
    (6, 2, 'SSNI-497 新任なのに', 'JavDB TOP250'),
    (8, 1, 'LAFBD-41  無碼 ラフォーレ ガール Vol.41', 'JavDB 无码 TOP250'),
    (8, 2, 'SKY-247 sky angel', 'JavDB 无码 TOP250'),
    (8, 3, 'SERO-0127 sneaks into the censored year list untagged', 'JavDB 无码 TOP250'),
    (5, 1, 'START-257 苦手な同僚と', 'JavLibray TOP500'),
    (5, 2, 'SHKD-724 オフィスレディの湿ったパンスト 桜木凛', 'JavLibray TOP500'),
    (2024, 1, 'MIAB-317 不登校オタク生徒を', 'JavDB 2024 TOP250'),
    (2024, 2, 'blacked.24.05.10 Our Little Secret', 'JavDB 2024 TOP250'),
    (2024, 3, 'FC2-4025269 【ここに舞い降りし神秘の秘宝】', 'JavDB 2024 TOP250'),
    (2024, 4, '010115_001 caribbeancom', 'JavDB 2024 TOP250'),
    (2024, 5, 'SERO-127 untagged but listed as uncensored', 'JavDB 2024 TOP250'),
    (2024, 6, 'HEYZO-1031 heyzo', 'JavDB 2024 TOP250'),
    (2024, 7, 'SONE-308 美脚下半身が', 'JavDB 2024 TOP250'),
    (2024, 8, 'TEK-099 18Gold 金松季歩', 'JavDB 2024 TOP250'),
    (2024, 9, 'TEK-099 18Gold 金松季歩 （ブルーレイディスク）', 'JavDB 2024 TOP250'),  # noqa: RUF001 - source spelling
    (4, 231, 'MOON-018 いつも濡れすぎて', '第四届JAV金鸡儿奖 最佳故事片 提名'),
    (4, 232, 'START-073 MINAMO ZOMBIE', '第四届JAV金鸡儿奖 最佳故事片 提名'),
    (4, 240, 'きとるね川口', '第四届JAV金鸡儿奖 最佳导演 提名'),
    (4, 241, '木村浩之', '第四届JAV金鸡儿奖 最佳导演 提名'),
    (4, 250, 'START-196 ある日', '第四届JAV金鸡儿奖 最佳故事片 获奖'),
    (2, 1, 'FWAY-088 MONSTER 瀬戸環奈', '影片榜 - 2025年12月'),
    (3, 1, 'TEK-099 18Gold', '影片榜 - 2024上半年'),
    (0, 1, '涼森れむ', '女优榜 - 2025年12月'),
    (9, 1, 'BangBus.16.07.13 The Bus Gets Recognized', 'JavDB 欧美 TOP250'),
    (10, 1, 'FC2-3061625 人生初めての', 'JavDB FC2 TOP250'),
]


def make_database(rows: list[tuple[int, int, str, str]] = ROWS) -> bytes:
    connection = sqlite3.connect(':memory:')
    connection.execute(
        'CREATE TABLE ranks ('
        'id INTEGER PRIMARY KEY, kind INTEGER, number INTEGER, name TEXT, date TEXT, note TEXT, icon_url TEXT)',
    )
    connection.executemany(
        'INSERT INTO ranks (kind, number, name, date, note, icon_url) VALUES (?, ?, ?, ?, ?, ?)',
        [(kind, number, name, '2024-01-01', note, '') for kind, number, name, note in rows],
    )
    connection.commit()
    return connection.serialize()


def test_parse_keeps_only_the_playlist_families_and_drops_uncensored_entries() -> None:
    lists = {ranked.key: ranked for ranked in parse_lists(make_database(), AvidParser().get_avid)}

    assert set(lists) == {
        'k5',
        'k7',
        'k2024',
        'k4:第四届JAV金鸡儿奖 最佳故事片 提名',
        'k4:第四届JAV金鸡儿奖 最佳故事片 获奖',
    }
    # Same code twice in a list keeps its first position; later entries close the gap.
    assert [(e.rank, e.avid) for e in lists['k7'].entries] == [(1, 'SSNI-497'), (2, 'IPX-811'), (3, 'ABP-984')]
    assert lists['k7'].name == 'JavDB 有码 TOP250'
    # Western, FC2, date codes, a label from the uncensored set, an uncensored
    # brand, and a Blu-ray duplicate all go; the survivors are renumbered.
    assert [e.avid for e in lists['k2024'].entries] == ['MIAB-317', 'SONE-308', 'TEK-099']
    assert [e.rank for e in lists['k2024'].entries] == [1, 2, 3]
    # The people rows of an awards category vanish with it: no empty lists.
    assert lists['k4:第四届JAV金鸡儿奖 最佳故事片 提名'].entries == (
        RankedEntry(rank=1, avid='MOON-018', title='いつも濡れすぎて'),
        RankedEntry(rank=2, avid='START-073', title='MINAMO ZOMBIE'),
    )
    assert lists['k5'].entries[1].title == 'オフィスレディの湿ったパンスト 桜木凛'


def test_parse_reads_the_uncensored_set_through_the_parser_normalization() -> None:
    # SERO-0127 in the uncensored list and SERO-127 in the year list are the same
    # title once both spellings pass through the parser.
    parser = AvidParser()
    assert parser.get_avid('SERO-0127') == parser.get_avid('SERO-127') == 'SERO-127'
    lists = {ranked.key: ranked for ranked in parse_lists(make_database(), parser.get_avid)}
    assert 'SERO-127' not in {e.avid for e in lists['k2024'].entries}


def test_parse_rejects_bytes_that_are_not_a_ranking_database() -> None:
    with pytest.raises(JinjierError, match='not a readable ranking database'):
        parse_lists(b'not sqlite at all', AvidParser().get_avid)

    empty = sqlite3.connect(':memory:')
    empty.execute('CREATE TABLE other (x)')
    with pytest.raises(JinjierError, match='not a readable ranking database'):
        parse_lists(empty.serialize(), AvidParser().get_avid)


@pytest.mark.parametrize(
    ('avid', 'excluded'),
    [
        ('SSNI-497', False),
        ('FC2-4025269', True),
        ('010115_001', True),
        ('092415-159', True),
        ('HEYZO-1031', True),
        ('LAFBD-041', True),
        ('SKY-247', True),
        ('SKYHD-084', True),
        ('BT-123', True),
        ('BTH-123', False),
    ],
)
def test_excluded_codes(avid: str, excluded: bool) -> None:  # noqa: FBT001 - parametrized expectation
    assert is_excluded_code(avid) is excluded


def test_list_key_only_joins_the_note_for_awards() -> None:
    assert list_key(7, 'JavDB 有码 TOP250') == 'k7'
    assert list_key(2024, 'JavDB 2024 TOP250') == 'k2024'
    assert list_key(4, '第四届JAV金鸡儿奖 最佳导演 提名') == 'k4:第四届JAV金鸡儿奖 最佳导演 提名'


# -- HTTP ---------------------------------------------------------------------

SQL_PAGE = (
    '<html><script>const wasmFile="x";let defaultSQL="...";'
    'dbFile=new URLSearchParams(location.search).get("v");'
    '"2023"==dbFile?(dbFile="https://cdn/2023.gif"):dbFile="20260112.gif";</script></html>'
)


def archive(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buffer.getvalue()


def make_client(routes: dict[str, httpx.Response]) -> tuple[JinjierClient, list[str]]:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return routes.get(str(request.url), httpx.Response(404))

    return JinjierClient('https://jinjier.test/sql', transport=httpx.MockTransport(handler)), calls


async def test_discover_reads_the_current_database_name_from_the_query_page() -> None:
    client, calls = make_client({'https://jinjier.test/sql': httpx.Response(200, text=SQL_PAGE)})
    try:
        assert await client.discover_database_name() == '20260112'
    finally:
        await client.aclose()
    assert calls == ['https://jinjier.test/sql']


async def test_discover_fails_loudly_when_the_page_changed_shape() -> None:
    client, _ = make_client({'https://jinjier.test/sql': httpx.Response(200, text='<html>redesigned</html>')})
    try:
        with pytest.raises(JinjierError, match='no database name'):
            await client.discover_database_name()
    finally:
        await client.aclose()


async def test_download_unpacks_the_sqlite_member_from_the_root_level_archive() -> None:
    database = make_database()
    client, calls = make_client(
        {'https://jinjier.test/20260112.gif': httpx.Response(200, content=archive({'jinjier.sqlite3': database}))},
    )
    try:
        assert await client.download_database('20260112') == database
    finally:
        await client.aclose()
    # The archive lives at the origin root, not under the query page's path.
    assert calls == ['https://jinjier.test/20260112.gif']


async def test_download_rejects_non_archives_and_archives_without_a_database() -> None:
    client, _ = make_client(
        {
            'https://jinjier.test/a.gif': httpx.Response(200, content=b'GIF89a...'),
            'https://jinjier.test/b.gif': httpx.Response(200, content=archive({'readme.txt': b'hi'})),
            'https://jinjier.test/c.gif': httpx.Response(503),
        },
    )
    try:
        with pytest.raises(JinjierError, match='not a zip archive'):
            await client.download_database('a')
        with pytest.raises(JinjierError, match=r'no \.sqlite3 member'):
            await client.download_database('b')
        with pytest.raises(JinjierError, match='HTTP 503'):
            await client.download_database('c')
    finally:
        await client.aclose()


async def test_transport_failures_are_jinjier_errors() -> None:
    def explode(request: httpx.Request) -> httpx.Response:
        msg = 'refused'
        raise httpx.ConnectError(msg, request=request)

    client = JinjierClient('https://jinjier.test/sql', transport=httpx.MockTransport(explode))
    try:
        with pytest.raises(JinjierError, match='ConnectError'):
            await client.discover_database_name()
    finally:
        await client.aclose()
