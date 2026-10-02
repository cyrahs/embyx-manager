"""Finding multi-part titles in the library and where each one came from.

The mapping pipeline's local tree holds one directory per title, and a title
archived in parts holds one ``{AVID}-cdN.strm`` per part. Each strm names the
part's file on the CloudDrive mount, so the scan never touches 115 itself.

Emby stacks parts cd1 through cd9 into one item and shows every later part as
an item of its own. Both kinds can be merged; the page only lists them apart.

Merging re-enters a title through the archive flow, so it needs the intake
route (``clt``, ``rank``, ``vr``, ...) whose destination holds the title today.
The library directory names it whenever exactly one route feeds that
directory; otherwise the acquisition ledger's offline directory is asked.
"""

import os
import re
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path, PurePosixPath

from embyx_manager.config.models import ArchiveConfig

#: ``ABC-123-cd4.strm``: the stem before ``-cd`` is the title, the number its part.
CD_STRM_RE = re.compile(r'^(?P<stem>.+)-cd(?P<index>\d+)\.strm$', re.IGNORECASE)
#: Emby stacks a single digit only; ``cd10`` onwards each become their own item.
MAX_STACKED_PARTS = 9
#: A strm is one short line; anything larger is not one.
MAX_STRM_BYTES = 4096


class SourceBasis(StrEnum):
    #: Exactly one route feeds the library directory the parts live in.
    LIBRARY = 'library'
    #: The acquisition ledger remembers which offline directory fetched it.
    LEDGER = 'ledger'


class TitleProblem(StrEnum):
    #: A strm could not be read or does not hold one absolute path.
    UNREADABLE_STRM = 'unreadable_strm'
    #: A part's file is not under the archive library root.
    OUTSIDE_LIBRARY = 'outside_library'
    #: The parts are spread over more than one brand directory.
    SCATTERED_PARTS = 'scattered_parts'


@dataclass(frozen=True)
class Part:
    index: int
    #: The strm, relative to the mapping tree's root.
    strm: str
    #: The part's file on the CloudDrive mount, as the strm names it.
    target: str


@dataclass(frozen=True)
class MultipartTitle:
    avid: str
    #: The title's directory in the mapping tree, relative to its root.
    directory: str
    parts: tuple[Part, ...]
    #: Part numbers absent from 1..highest.
    missing: tuple[int, ...]
    #: Where the parts live, relative to the archive library root, e.g. ``type/vr``.
    library_dir: str | None
    brand: str | None
    problem: TitleProblem | None = None

    @property
    def part_count(self) -> int:
        return len(self.parts)

    @property
    def stackable(self) -> bool:
        """Emby already shows it as one item."""
        return self.part_count <= MAX_STACKED_PARTS and not self.missing

    @property
    def complete(self) -> bool:
        return not self.missing and self.problem is None


@dataclass(frozen=True)
class SourceRoute:
    source: str | None
    basis: SourceBasis | None


def scan_multipart(root: Path, *, library_root: str) -> list[MultipartTitle]:
    """Every title under ``root`` with at least two ``-cdN`` parts, most parts first."""
    groups: dict[tuple[str, str], list[tuple[int, Path]]] = {}
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            match = CD_STRM_RE.match(name)
            if match is None:
                continue
            key = (dirpath, match['stem'].upper())
            groups.setdefault(key, []).append((int(match['index']), Path(dirpath) / name))
    titles = [
        _title(root, stem, sorted(entries), library_root=PurePosixPath(library_root))
        for (_dirpath, stem), entries in groups.items()
        if len(entries) > 1
    ]
    titles.sort(key=lambda title: (-title.part_count, title.avid))
    return titles


def _title(root: Path, stem: str, entries: list[tuple[int, Path]], *, library_root: PurePosixPath) -> MultipartTitle:
    directory = entries[0][1].parent
    indexes = {index for index, _path in entries}
    missing = tuple(index for index in range(1, max(indexes) + 1) if index not in indexes)
    problem: TitleProblem | None = None
    parts: list[Part] = []
    for index, path in entries:
        target = _read_strm(path)
        if target is None:
            problem = TitleProblem.UNREADABLE_STRM
            target = ''
        parts.append(Part(index=index, strm=str(path.relative_to(root)), target=target))

    library_dir: str | None = None
    brand: str | None = None
    if problem is None:
        parents = {PurePosixPath(part.target).parent for part in parts}
        if len(parents) > 1:
            problem = TitleProblem.SCATTERED_PARTS
        else:
            try:
                relative = parents.pop().relative_to(library_root)
            except ValueError:
                problem = TitleProblem.OUTSIDE_LIBRARY
            else:
                if len(relative.parts) < 2:  # noqa: PLR2004 - a library directory plus the brand
                    problem = TitleProblem.OUTSIDE_LIBRARY
                else:
                    library_dir = str(relative.parent)
                    brand = relative.name
    return MultipartTitle(
        avid=stem,
        directory=str(directory.relative_to(root)),
        parts=tuple(parts),
        missing=missing,
        library_dir=library_dir,
        brand=brand,
        problem=problem,
    )


def _read_strm(path: Path) -> str | None:
    try:
        payload = path.read_bytes()
    except OSError:
        return None
    if not payload or len(payload) > MAX_STRM_BYTES:
        return None
    try:
        lines = payload.decode('utf-8').strip().splitlines()
    except UnicodeDecodeError:
        return None
    if len(lines) != 1 or not lines[0].startswith('/'):
        return None
    return lines[0].strip()


def route_sources(archive: ArchiveConfig) -> tuple[str, ...]:
    """Every intake route, priority routes first, as the archive config lists them."""
    return (*archive.priority_mapping, *archive.mapping)


def source_for_library_dir(archive: ArchiveConfig, library_dir: str | None) -> str | None:
    """The one route whose destination is ``library_dir``; None when none or several are."""
    if library_dir is None:
        return None
    routes = {**archive.priority_mapping, **archive.mapping}
    sources = [source for source, destination in routes.items() if destination == library_dir]
    return sources[0] if len(sources) == 1 else None


def source_for_task_dir(archive: ArchiveConfig, task_dir_path: str | None) -> str | None:
    """The route rooted at an offline directory, matched by path suffix like the archive does."""
    if not task_dir_path or not archive.src_dir:
        return None
    wanted = PurePosixPath(task_dir_path).parts[1:]
    if not wanted:
        return None
    for source in route_sources(archive):
        parts = (PurePosixPath(archive.src_dir) / source).parts
        if len(parts) >= len(wanted) and parts[len(parts) - len(wanted) :] == wanted:
            return source
    return None


def resolve_source(archive: ArchiveConfig, title: MultipartTitle, ledger_task_dir: str | None) -> SourceRoute:
    source = source_for_library_dir(archive, title.library_dir)
    if source is not None:
        return SourceRoute(source, SourceBasis.LIBRARY)
    source = source_for_task_dir(archive, ledger_task_dir)
    if source is not None:
        return SourceRoute(source, SourceBasis.LEDGER)
    return SourceRoute(None, None)
