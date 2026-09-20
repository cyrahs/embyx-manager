"""jinjier.art ranking database: discovery, download, and the lists we keep.

The site publishes every ranking it shows as one SQLite database inside a zip
that is served with a ``.gif`` name (``/<yyyymmdd>.gif``). The current name is
only written into the inline script of its SQL query page, so a sync first reads
that page, then fetches the archive when the name changed.

The database is one table, ``ranks(id, kind, number, name, date, note,
icon_url)``: ``kind`` tells the ranking family, ``note`` names the individual
list, ``number`` is the position and ``name`` starts with the title's code.
Which families become playlists, and which entries are dropped (the library
collects regular censored JAV only), are the module constants below.
"""

import io
import logging
import re
import sqlite3
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

import httpx

log = logging.getLogger('embyx-manager.jinjier')

DEFAULT_SOURCE_URL = 'https://jinjier.art/sql'

#: Ranking families that become playlists. Year lists are keyed by the year
#: itself, so anything that looks like one is taken.
KIND_JAVDB_CENSORED = 7
KIND_JAVLIBRARY = 5
KIND_AWARDS = 4
FIRST_YEAR_KIND = 2008
LAST_YEAR_KIND = 2100
#: The uncensored family: not a playlist itself, but its codes are the surest
#: way to recognise an uncensored entry sitting in another list.
KIND_UNCENSORED = 8

#: Codes that are never regular censored JAV: date-numbered uncensored sites
#: (Caribbeancom, 1pondo, ...) and the uncensored labels whose codes look like
#: ordinary BRAND-NUMBER ones.
_DATE_CODE_RE = re.compile(r'^\d{6}[_-]\d{3}$')
_FC2_PREFIX = 'FC2-'
EXCLUDED_BRANDS = frozenset(
    {
        'HEYZO',
        'CARIB',
        '1PONDO',
        '10MU',
        'PACOPACO',
        'MKBD',
        'SMBD',
        'LAFBD',
        'CWPBD',
        'LAF',
        'SKYHD',
        'SKY',
        'KIRARI',
        'MCB3DBD',
        'MCDV',
        'BT',
        'HEYDOUGA',
    },
)
_UNCENSORED_TAG = ' 無碼 '

_DB_FILE_RE = re.compile(r'dbFile\s*=\s*"(\d{8})\.gif"')
_ZIP_MAGIC = b'PK\x03\x04'
_DATABASE_MEMBER_SUFFIX = '.sqlite3'


class JinjierError(RuntimeError):
    """The ranking source could not be read."""


@dataclass(frozen=True)
class RankedEntry:
    #: 1-based position within the list after exclusions and de-duplication.
    rank: int
    avid: str
    title: str


@dataclass(frozen=True)
class RankedList:
    #: Stable identity across downloads: ``k7``, ``k2024``, ``k4:<note>``.
    key: str
    kind: int
    note: str
    entries: tuple[RankedEntry, ...]

    @property
    def name(self) -> str:
        """The playlist name; the source's own list name serves as is."""
        return self.note


def list_key(kind: int, note: str) -> str:
    """The awards family holds many small lists, so only there the note joins the key."""
    return f'k{kind}:{note}' if kind == KIND_AWARDS else f'k{kind}'


def is_playlist_kind(kind: int) -> bool:
    return kind in {KIND_JAVDB_CENSORED, KIND_JAVLIBRARY, KIND_AWARDS} or FIRST_YEAR_KIND <= kind <= LAST_YEAR_KIND


def is_excluded_code(avid: str) -> bool:
    """True for codes the library never holds, whatever list they appear in."""
    if avid.startswith(_FC2_PREFIX) or _DATE_CODE_RE.match(avid):
        return True
    brand, _, _ = avid.partition('-')
    return brand in EXCLUDED_BRANDS


def parse_lists(database: bytes, avid_of: Callable[[str], str]) -> tuple[RankedList, ...]:
    """Every list that becomes a playlist, entries in ranking order.

    ``avid_of`` reads a code out of a title (the AVID parser), which also
    normalizes spellings such as ``LAFBD-41`` to the library's ``LAFBD-041``.
    Entries whose code cannot be read (western releases, the people rows of the
    awards lists) are skipped; so are uncensored ones, recognised by the
    source's own tag, by membership of its uncensored list, or by the code.
    A code seen twice in one list keeps its first position.
    """
    connection = sqlite3.connect(':memory:')
    try:
        try:
            connection.deserialize(database)
            rows = connection.execute(
                'SELECT kind, note, number, name FROM ranks ORDER BY kind, note, number, id',
            ).fetchall()
        except sqlite3.Error as exc:
            msg = f'not a readable ranking database: {exc}'
            raise JinjierError(msg) from exc
    finally:
        connection.close()

    uncensored = {avid for kind, _, _, name in rows if kind == KIND_UNCENSORED and (avid := _code_of(name, avid_of))}
    lists: dict[str, tuple[int, str, list[RankedEntry], set[str]]] = {}
    for kind, note, _, name in rows:
        if not is_playlist_kind(kind):
            continue
        key = list_key(kind, note)
        if key not in lists:
            lists[key] = (kind, note, [], set())
        _, _, entries, seen = lists[key]
        avid = _code_of(name, avid_of)
        if not avid or avid in seen:
            continue
        if _UNCENSORED_TAG in name or avid in uncensored or is_excluded_code(avid):
            continue
        seen.add(avid)
        entries.append(RankedEntry(rank=len(entries) + 1, avid=avid, title=_title_of(name)))
    return tuple(
        RankedList(key=key, kind=kind, note=note, entries=tuple(entries))
        for key, (kind, note, entries, _) in lists.items()
        if entries
    )


def _code_of(name: str, avid_of: Callable[[str], str]) -> str:
    head, _, _ = name.strip().partition(' ')
    return avid_of(head) or avid_of(name)


def _title_of(name: str) -> str:
    _, _, rest = name.strip().partition(' ')
    return rest.strip()


class JinjierClient:
    """Reads the current database name from the query page and fetches the archive."""

    def __init__(
        self,
        source_url: str = DEFAULT_SOURCE_URL,
        *,
        timeout: float = 120.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._source_url = source_url
        parts = urlsplit(source_url)
        self._origin = urlunsplit((parts.scheme, parts.netloc, '', '', ''))
        self._client = httpx.AsyncClient(timeout=timeout, transport=transport, follow_redirects=True)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def discover_database_name(self) -> str:
        """The ``yyyymmdd`` stamp of the database the site currently serves."""
        page = await self._get(self._source_url)
        match = _DB_FILE_RE.search(page.text)
        if match is None:
            msg = f'{self._source_url}: no database name in the page'
            raise JinjierError(msg)
        return match.group(1)

    def database_url(self, name: str) -> str:
        return f'{self._origin}/{name}.gif'

    async def download_database(self, name: str) -> bytes:
        """The SQLite file inside the archive published under ``name``."""
        url = self.database_url(name)
        response = await self._get(url)
        if not response.content.startswith(_ZIP_MAGIC):
            msg = f'{url}: not a zip archive'
            raise JinjierError(msg)
        try:
            with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
                member = next((n for n in archive.namelist() if n.endswith(_DATABASE_MEMBER_SUFFIX)), None)
                if member is None:
                    msg = f'{url}: no {_DATABASE_MEMBER_SUFFIX} member in the archive'
                    raise JinjierError(msg)
                return archive.read(member)
        except zipfile.BadZipFile as exc:
            msg = f'{url}: corrupt zip archive'
            raise JinjierError(msg) from exc

    async def _get(self, url: str) -> httpx.Response:
        try:
            response = await self._client.get(url)
        except httpx.HTTPError as exc:
            msg = f'{url}: {exc.__class__.__name__}: {exc}'
            raise JinjierError(msg) from exc
        if response.is_error:
            msg = f'{url}: HTTP {response.status_code}'
            raise JinjierError(msg)
        return response
