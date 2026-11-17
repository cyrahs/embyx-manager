"""The `.strm` mapping's name rules and file-system calls (WP8).

`monitor/mapping_names.json`: for names given as bytes (hex), what `monitor/mapping.py` reads
out of them: `suffix.lower() == '.strm'`, `stem`, the stem `PART_SIDECAR_RE` matches, the
owner `_owner` picks among candidate stems, and the order `sorted()` puts paths in. Names that
are not UTF-8 are decoded the way the file system hands them to Python (`surrogateescape`).

`monitor/mapping_fs.json`: on a temporary tree (paths shown relative to it as `{tmp}`), the
order `Path.glob('**/*.strm')` and `Path.glob('**/*')` yield entries in when every directory
lists in name order (what the mapping scenarios patch in), `shutil.copy2`'s copied times and
mode, and the errors of `copy2`, `Path.mkdir(parents=True)`, `Path.rmdir` and
`filecmp.cmp(shallow=False)` on the cases the pipeline can meet.
"""

from __future__ import annotations

import filecmp
import glob
import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self
from unittest import mock

from embyx_manager.monitor import mapping

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

NAMES = [
    b'ABC-123.strm',
    b'ABC-123.STRM',
    b'ABC-123.Strm',
    b'.strm',
    b'a.strm.',
    b'ABC-123.nfo',
    b'ABC-123-cd1.nfo',
    b'ABC-123-CD2-poster.jpg',
    b'ABC-123-cd10-landscape.jpg',
    b'ABC-123-cd1-4K.nfo',
    b'ABC-123-cd.nfo',
    b'-cd1.nfo',
    b'x-cd1.nfo',
    b'ABC-123-cd1',
    b'ABC-123-cd1-cd2.nfo',
    b'ABC-123-cd1x.nfo',
    b'line\n-cd1.nfo',
    b'ABC-123-cd\xd9\xa3.nfo',
    b'ABC\xff-cd1.nfo',
    b'ABC\xc3-cd1.nfo',
    b'\xc3\xa9-cd2-poster.jpg',
    b'ABC-123\xff.strm',
    b'ABC-123.str\xff',
    b'ABC-123',
]

OWNER_CASES = [
    (b'ABC-123-cd1-poster.jpg', [b'ABC-123', b'ABC-123-cd1']),
    (b'ABC-123.nfo', [b'ABC-123', b'ABC-123-cd1']),
    (b'ABC-1234.nfo', [b'ABC-123']),
    (b'ABC-123_x.nfo', [b'ABC-123']),
    (b'ABC-123.nfo', [b'ABC-12', b'ABC-123', b'ABC']),
    (b'ABC\xff.nfo', [b'ABC\xff', b'ABC']),
    (b'ABC\xc3.nfo', [b'ABC\xc3', b'ABC\xc3\xa9']),
    (b'poster.jpg', [b'ABC-123']),
]

SORT_CASES = [
    [b'/m/b/x.strm', b'/m/a/y.strm', b'/m/a.strm', b'/m/a/b', b'/m/a b/c', b'/m/A/z.strm'],
    [b'/m/\xff', b'/m/\xee\x80\x80', b'/m/z', b'/m/\xc3\xa9'],
    [b'm/a', b'm', b'm/a/b', b'n'],
]


def _hex(value: str | None) -> str | None:
    return None if value is None else os.fsencode(value).hex()


def _names() -> list[Any]:
    names = []
    for raw in NAMES:
        name = os.fsdecode(raw)
        path = Path('/m') / name
        part = mapping.PART_SIDECAR_RE.match(name)
        names.append(
            {
                'name': raw.hex(),
                'is_strm': path.suffix.lower() == '.strm',
                'stem': _hex(path.stem),
                'part_stem': _hex(part['stem'] if part is not None else None),
            }
        )
    owners = [
        {
            'name': raw.hex(),
            'stems': [stem.hex() for stem in stems],
            'owner': _hex(mapping._owner(os.fsdecode(raw), [os.fsdecode(stem) for stem in stems])),  # noqa: SLF001
        }
        for raw, stems in OWNER_CASES
    ]
    orders = [
        {
            'paths': [raw.hex() for raw in case],
            'sorted': [os.fsencode(str(path)).hex() for path in sorted(Path(os.fsdecode(raw)) for raw in case)],
        }
        for case in SORT_CASES
    ]
    return [{'names': names, 'owners': owners, 'orders': orders}]


# -- the file system --------------------------------------------------------------------

_SCANDIR = os.scandir


class _Listing:
    """`os.scandir()`'s context manager over a list."""

    def __init__(self, entries: list[os.DirEntry[str]]) -> None:
        self._entries = entries

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        del exc

    def __iter__(self) -> Iterator[os.DirEntry[str]]:
        return iter(self._entries)

    def close(self) -> None:
        return None


def sorted_scandir(path: Any = '.') -> _Listing:
    """`os.scandir` listing in name order (bytes), the order the mapping scenarios use."""
    with _SCANDIR(path) as listing:
        return _Listing(sorted(listing, key=lambda entry: os.fsencode(entry.name)))


@contextmanager
def sorted_listings() -> Iterator[None]:
    with (
        mock.patch.object(os, 'scandir', sorted_scandir),
        mock.patch.object(glob._StringGlobber, 'scandir', staticmethod(sorted_scandir)),  # noqa: SLF001
    ):
        yield


TREE = [
    'b.strm',
    'a.strm',
    'z/c.strm',
    'z/notes.txt',
    'z/y/d.strm',
    'z/y/x/e.strm',
    'a/f.strm',
    'a/.hidden.strm',
    'a/q/g.STRM',
    'a/q/r/h.strm',
    'm/i.strm',
    'dir.strm/j.strm',
]


def _relative(root: Path, paths: Any) -> list[str]:
    return [str(Path(path).relative_to(root)) for path in paths]


def _error(call: Callable[[], Any], tmp: Path) -> dict[str, Any]:
    try:
        result = call()
    except OSError as exc:
        return {
            'type': type(exc).__name__,
            'message': str(exc).replace(str(tmp), '{tmp}'),
            'repr': repr(exc).replace(str(tmp), '{tmp}'),
        }
    return {'result': result}


def _fs() -> list[Any]:
    with tempfile.TemporaryDirectory() as directory:
        tmp = Path(directory)
        root = tmp / 'tree'
        for relative in TREE:
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('x', encoding='utf-8')
        (root / 'empty' / 'deeper').mkdir(parents=True)
        (root / 'link').symlink_to(root / 'z')
        with sorted_listings():
            strm = _relative(root, root.glob('**/*.strm'))
            everything = _relative(root, root.glob('**/*'))
            strm_in_link = _relative(root, (root / 'link').glob('**/*.strm'))

        src = tmp / 'src.strm'
        src.write_text('/cloud/video.mp4', encoding='utf-8')
        os.chmod(src, 0o640)  # noqa: PTH101
        os.utime(src, ns=(1_700_000_000_123_456_789, 1_600_000_000_987_654_321))
        dst = tmp / 'out' / 'dst.strm'
        dst.parent.mkdir()
        shutil.copy2(src, dst)
        copied = dst.stat()
        into_dir = tmp / 'into'
        into_dir.mkdir()
        shutil.copy2(src, into_dir)
        same_size = tmp / 'same.strm'
        same_size.write_text('/cloud/video.mp5', encoding='utf-8')
        (tmp / 'full').mkdir()
        (tmp / 'full' / 'x').write_text('x', encoding='utf-8')
        (tmp / 'exists').mkdir()
        (tmp / 'a-file').write_text('x', encoding='utf-8')
        copy = {
            'mtime_ns': copied.st_mtime_ns,
            'mode': oct(copied.st_mode & 0o7777),
            'content': dst.read_text(encoding='utf-8'),
            'into_dir': sorted(path.name for path in into_dir.iterdir()),
            'st_mtime_equal': src.stat().st_mtime == copied.st_mtime,
        }
        errors = {
            'copy_missing_source': _error(lambda: shutil.copy2(tmp / 'missing.strm', tmp / 'out' / 'x.strm'), tmp),
            'copy_missing_directory': _error(lambda: shutil.copy2(src, tmp / 'nowhere' / 'x.strm'), tmp),
            'copy_source_is_directory': _error(lambda: shutil.copy2(tmp / 'full', tmp / 'out' / 'y.strm'), tmp),
            'copy_same_file': _error(lambda: shutil.copy2(src, src), tmp),
            'mkdir_existing': _error(lambda: (tmp / 'exists').mkdir(parents=True), tmp),
            'mkdir_under_file': _error(lambda: (tmp / 'a-file' / 'x' / 'y').mkdir(parents=True), tmp),
            'mkdir_new': _error(lambda: (tmp / 'new' / 'one' / 'two').mkdir(parents=True), tmp),
            'rmdir_not_empty': _error((tmp / 'full').rmdir, tmp),
            'rmdir_missing': _error((tmp / 'gone').rmdir, tmp),
            'cmp_equal': _error(lambda: filecmp.cmp(src, dst, shallow=False), tmp),
            'cmp_same_size_differs': _error(lambda: filecmp.cmp(src, same_size, shallow=False), tmp),
            'cmp_directory': _error(lambda: filecmp.cmp(src, tmp / 'full', shallow=False), tmp),
            'cmp_missing': _error(lambda: filecmp.cmp(src, tmp / 'missing.strm', shallow=False), tmp),
        }
    return [
        {
            'tree': TREE,
            'glob_strm': strm,
            'glob_all': everything,
            'glob_strm_in_link': strm_in_link,
            'copy2': copy,
            'errors': errors,
        }
    ]


GOLDENS = {
    'monitor/mapping_names.json': _names,
    'monitor/mapping_fs.json': _fs,
}
