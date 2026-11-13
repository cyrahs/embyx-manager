"""Multi-part detection (`merge/detect.py`, WP13) over mapping trees, and the routes a title re-enters through.

`scan_multipart` reads the strm files of the mapping tree; each case lays a tree out in a
temporary directory (one title per directory, so the file system's listing order never
decides the result) and records every title the scan reports. The route lookups run over one
archive configuration. The Rust port (`embyx_merge::detect`) must give the same answers.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from embyx_manager.config.models import ArchiveConfig
from embyx_manager.merge import detect

LIBRARY = '/mnt/cd2/115/embyx'

ARCHIVE = {
    'src_dir': '/mnt/cd2/115/embyx_in',
    'dst_dir': LIBRARY,
    'mapping': {'hh': 'type/hh', 'rank': 'rank', 'rest': 'rest', 'special': 'type/special', 'hh2': 'type/hh2'},
    'priority_mapping': {'vr': 'type/vr', 'clt': 'actor/clt', 'deep/er': 'deep'},
}


def _part(library_dir: str, name: str) -> str:
    return f'{LIBRARY}/{library_dir}/{name}\n'


def _trees() -> list[dict[str, Any]]:
    """Each tree: relative strm path -> its bytes (as text, or a list of byte values)."""
    trees: list[dict[str, Any]] = []
    plain = {f'a/T-001/T-001-cd{index}.strm': _part('rank/T', f'T-001-cd{index}.mp4') for index in (1, 2, 3)}
    trees.append({'name': 'plain', 'files': plain})
    trees.append(
        {
            'name': 'strm_shapes',
            'files': {
                'b/CR-001/CR-001-cd1.strm': f'{LIBRARY}/rank/CR/CR-001-cd1.mp4\r\n',
                'b/CR-001/CR-001-cd2.strm': f'  \n{LIBRARY}/rank/CR/CR-001-cd2.mp4  \n\n',
                'c/BOM-001/BOM-001-cd1.strm': '﻿' + _part('rank/BOM', 'BOM-001-cd1.mp4'),
                'c/BOM-001/BOM-001-cd2.strm': _part('rank/BOM', 'BOM-001-cd2.mp4'),
                'd/TWO-001/TWO-001-cd1.strm': _part('rank/TWO', 'a.mp4') + _part('rank/TWO', 'b.mp4'),
                'd/TWO-001/TWO-001-cd2.strm': _part('rank/TWO', 'TWO-001-cd2.mp4'),
                'e/EMP-001/EMP-001-cd1.strm': '',
                'e/EMP-001/EMP-001-cd2.strm': _part('rank/EMP', 'EMP-001-cd2.mp4'),
                'f/BIG-001/BIG-001-cd1.strm': '/' + 'x' * 5000,
                'f/BIG-001/BIG-001-cd2.strm': _part('rank/BIG', 'BIG-001-cd2.mp4'),
                'g/REL-001/REL-001-cd1.strm': 'rank/REL/REL-001-cd1.mp4\n',
                'g/REL-001/REL-001-cd2.strm': 'rank/REL/REL-001-cd2.mp4\n',
                'h/BIN-001/BIN-001-cd1.strm': [0x2F, 0xFF, 0xFE, 0x0A],
                'h/BIN-001/BIN-001-cd2.strm': _part('rank/BIN', 'BIN-001-cd2.mp4'),
                'i/SEP-001/SEP-001-cd1.strm': f'{LIBRARY}/rank/SEP/SEP-001-cd1.mp4 more\n',
                'i/SEP-001/SEP-001-cd2.strm': _part('rank/SEP', 'SEP-001-cd2.mp4'),
            },
        }
    )
    trees.append(
        {
            'name': 'target_paths',
            'files': {
                'a/DOT-001/DOT-001-cd1.strm': f'{LIBRARY}//rank/./DOT/DOT-001-cd1.mp4\n',
                'a/DOT-001/DOT-001-cd2.strm': f'{LIBRARY}/rank/DOT/DOT-001-cd2.mp4\n',
                'b/UP-001/UP-001-cd1.strm': f'{LIBRARY}/rank/../rank/UP/UP-001-cd1.mp4\n',
                'b/UP-001/UP-001-cd2.strm': f'{LIBRARY}/rank/../rank/UP/UP-001-cd2.mp4\n',
                'c/ROOT-001/ROOT-001-cd1.strm': f'{LIBRARY}/ROOT-001-cd1.mp4\n',
                'c/ROOT-001/ROOT-001-cd2.strm': f'{LIBRARY}/ROOT-001-cd2.mp4\n',
                'd/ONE-001/ONE-001-cd1.strm': f'{LIBRARY}/brand/ONE-001-cd1.mp4\n',
                'd/ONE-001/ONE-001-cd2.strm': f'{LIBRARY}/brand/ONE-001-cd2.mp4\n',
                'e/DEEP-001/DEEP-001-cd1.strm': f'{LIBRARY}/type/vr/sub/DEEP/DEEP-001-cd1.mp4\n',
                'e/DEEP-001/DEEP-001-cd2.strm': f'{LIBRARY}/type/vr/sub/DEEP/DEEP-001-cd2.mp4\n',
                'f/SLASH-001/SLASH-001-cd1.strm': f'/{LIBRARY}/rank/SLASH/SLASH-001-cd1.mp4\n',
                'f/SLASH-001/SLASH-001-cd2.strm': f'/{LIBRARY}/rank/SLASH/SLASH-001-cd2.mp4\n',
                'g/PRE-001/PRE-001-cd1.strm': f'{LIBRARY}x/rank/PRE/PRE-001-cd1.mp4\n',
                'g/PRE-001/PRE-001-cd2.strm': f'{LIBRARY}x/rank/PRE/PRE-001-cd2.mp4\n',
            },
        }
    )
    trees.append(
        {
            'name': 'names',
            'files': {
                'a/UP-001/up-001-CD1.STRM': _part('rank/UP', 'up-001-cd1.mp4'),
                'a/UP-001/UP-001-cd2.strm': _part('rank/UP', 'UP-001-cd2.mp4'),
                'b/AR-001/AR-001-cd١.strm': _part('rank/AR', 'AR-001-cd1.mp4'),
                'b/AR-001/AR-001-cd02.strm': _part('rank/AR', 'AR-001-cd2.mp4'),
                'c/DUP-001/DUP-001-cd1.strm': _part('rank/DUP', 'DUP-001-cd1.mp4'),
                'c/DUP-001/DUP-001-cd01.strm': _part('rank/DUP', 'DUP-001-cd01.mp4'),
                'd/GAP-001/GAP-001-cd2.strm': _part('rank/GAP', 'GAP-001-cd2.mp4'),
                'd/GAP-001/GAP-001-cd4.strm': _part('rank/GAP', 'GAP-001-cd4.mp4'),
                'e/ZERO-001/ZERO-001-cd0.strm': _part('rank/ZERO', 'ZERO-001-cd0.mp4'),
                'e/ZERO-001/ZERO-001-cd1.strm': _part('rank/ZERO', 'ZERO-001-cd1.mp4'),
                'f/X-cd1.strm': _part('rank/X', 'X-cd1.mp4'),
                'f/X-cd2.strm': _part('rank/X', 'X-cd2.mp4'),
                'g/NOT-001/NOT-001-cd.strm': _part('rank/NOT', 'NOT-001-cd.mp4'),
                'g/NOT-001/NOT-001-cd1.strm.bak': _part('rank/NOT', 'NOT-001-cd1.mp4'),
                'g/NOT-001/NOT-001-cd1.strm': _part('rank/NOT', 'NOT-001-cd1.mp4'),
                'h/ONE-001/ONE-001-cd7.strm': _part('rank/ONE', 'ONE-001-cd7.mp4'),
                'i/MANY-001/' + 'MANY-001-cd1.strm': _part('type/vr/MANY', 'MANY-001-cd1.mp4'),
                **{
                    f'i/MANY-001/MANY-001-cd{index}.strm': _part('type/vr/MANY', f'MANY-001-cd{index}.mp4')
                    for index in range(2, 12)
                },
            },
        }
    )
    trees.append(
        {
            'name': 'whole_copies',
            'files': {
                'a/W-0123/W-0123-cd1.strm': _part('rank/W', 'W-0123-cd1.mp4'),
                'a/W-0123/W-0123-cd2.strm': _part('rank/W', 'W-0123-cd2.mp4'),
                'a/W-0123/w-123.STRM': _part('rank/W', 'w-123.mp4'),
                'b/V-001/V-001-cd1.strm': _part('rank/V', 'V-001-cd1.mp4'),
                'b/V-001/V-001-cd2.strm': _part('rank/V', 'V-001-cd2.mp4'),
                'b/V-001/V-001-CD9.strm.txt': 'x',
                'b/V-001/V-001.nfo': 'x',
                'c/S-001/S-001-cd1.strm': 'broken',
                'c/S-001/S-001-cd2.strm': _part('rank/S', 'S-001-cd2.mp4'),
                'c/S-001/ S-001 .strm': _part('rank/S', 'S-001.mp4'),
            },
        }
    )
    # Symbolic links: a linked directory is listed but not entered (os.walk), a linked strm is
    # read through the link, a dangling one is unreadable.
    trees.append(
        {
            'name': 'links',
            'files': {
                'x/L-001/L-001-cd1.strm': _part('rank/L', 'L-001-cd1.mp4'),
                'x/L-001/L-001-cd2.strm': _part('rank/L', 'L-001-cd2.mp4'),
                'y/LINKED': {'symlink': '../x/L-001'},
                'z/F-001/F-001-cd1.strm': {'symlink': '../../x/L-001/L-001-cd1.strm'},
                'z/F-001/F-001-cd2.strm': {'symlink': 'missing.strm'},
            },
        }
    )
    return trees


def _title(title: detect.MultipartTitle) -> dict[str, Any]:
    return {
        'avid': title.avid,
        'directory': title.directory,
        'parts': [[part.index, part.strm, part.target] for part in title.parts],
        'missing': list(title.missing),
        'library_dir': title.library_dir,
        'brand': title.brand,
        'problem': None if title.problem is None else title.problem.value,
        'part_count': title.part_count,
        'stackable': title.stackable,
        'complete': title.complete,
    }


def _scans() -> list[dict[str, Any]]:
    cases = []
    for tree in _trees():
        for library_root in (LIBRARY, LIBRARY + '/', '/mnt/cd2/115/embyx/rank', 'mnt/cd2/115/embyx', '/'):
            with tempfile.TemporaryDirectory(prefix='merge-detect-') as directory:
                root = Path(directory)
                for relative, content in tree['files'].items():
                    path = root / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    if isinstance(content, dict):
                        path.symlink_to(content['symlink'])
                    elif isinstance(content, list):
                        path.write_bytes(bytes(content))
                    else:
                        path.write_bytes(content.encode('utf-8'))
                titles = detect.scan_multipart(root, library_root=library_root)
            cases.append(
                {
                    'tree': tree['name'],
                    'files': tree['files'],
                    'library_root': library_root,
                    'titles': [_title(title) for title in titles],
                }
            )
    return cases


def _routes() -> list[dict[str, Any]]:
    archive = ArchiveConfig.model_validate(ARCHIVE)
    bare = ArchiveConfig.model_validate({**ARCHIVE, 'src_dir': ''})
    library_dirs = [None, '', 'type/vr', 'actor/clt', 'rank', 'type/hh', 'type/hh2', 'abs', 'type/unknown', 'deep']
    task_dirs = [
        None,
        '',
        '/',
        '/115/embyx_in/clt',
        '/115/embyx_in/vr/',
        'embyx_in/hh',
        '/hh',
        'hh',
        '/115/other',
        '/abs',
        '/x/abs',
        '/mnt/cd2/115/embyx_in/deep/er',
        '/deep/er',
        '//115/embyx_in/rank',
        '/a/b/c/d/e/f/embyx_in/rest',
    ]
    whole = [
        ('VRKM-1429', 'VRKM-01429'),
        ('VRKM-1429-cd1', 'VRKM-01429'),
        (' vrkm-1429 ', 'VRKM-01429'),
        ('VRKM-1429-CD12', 'VRKM-1429'),
        ('VRKM-14290', 'VRKM-1429'),
        ('FC2-PPV-123', 'FC2-PPV-0123'),
        ('ABC-001', 'abc-1'),
        ('', ''),
        ('ABC-001-cd', 'ABC-001'),
    ]
    copies = [
        (
            'AVOPVR-117',
            ['AVOPVR-117-cd1.mp4', 'AVOPVR-117.mp4', 'AVOPVR-117.nfo', 'AVOPVR-1170.mp4', 'avopvr-0117.mkv'],
        ),
        (
            'SAVR-01135',
            ['SAVR-1135.mp4', 'SAVR-1135-cd2.mp4', 'SAVR-1135.MP4', 'SAVR-1135.strm', '.mp4', 'a/SAVR-1135.ts'],
        ),
    ]
    return [
        {
            'archive': ARCHIVE,
            'route_sources': list(detect.route_sources(archive)),
            'library_dirs': [[value, detect.source_for_library_dir(archive, value)] for value in library_dirs],
            'task_dirs': [
                [value, detect.source_for_task_dir(archive, value), detect.source_for_task_dir(bare, value)]
                for value in task_dirs
            ],
            'is_whole_copy': [[stem, avid, detect.is_whole_copy(stem, avid)] for stem, avid in whole],
            'whole_copies': [[avid, names, list(detect.whole_copies(avid, names))] for avid, names in copies],
        }
    ]


GOLDENS = {
    'merge/detect_scan.json': _scans,
    'merge/detect_routes.json': _routes,
}
