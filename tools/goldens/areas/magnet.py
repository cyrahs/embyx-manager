"""Magnet info hashes (`core/magnet.py`) and video suffixes (`core/media.py`).

The info hash is the join key between a submitted magnet and the offline task CloudDrive
reports, so its spelling is stored in `archive_magnet_attempts.info_hash`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from embyx_manager.core.magnet import extract_info_hash
from embyx_manager.core.media import VIDEO_SUFFIXES, has_video_suffix

HEX = 'C12FE1C06BBA254A9DC9F519B335AA7C1367A88A'
BASE32 = 'YEX6DQDLXISUVHOJ6UM3GNNKPQJWPKEK'

MAGNETS: list[str] = [
    f'magnet:?xt=urn:btih:{HEX}',
    f'magnet:?xt=urn:btih:{HEX.lower()}',
    f'magnet:?xt=urn:btih:{HEX}&dn=ABC-123',
    f'magnet:?dn=ABC-123&xt=urn:btih:{HEX}&tr=udp://tracker',
    f'magnet:?xt=urn:btih:{BASE32}',
    f'magnet:?xt=urn:btih:{BASE32.lower()}&dn=ABC-123',
    f'MAGNET:?XT=URN:BTIH:{HEX}',
    '',
    'not a magnet',
    'magnet:?dn=ABC-123',
    f'magnet:?xt=urn:sha1:{HEX}',
    f'magnet:?xt=urn:btih:{HEX[:-1]}',
    'magnet:?xt=urn:btih:' + 'Z' * 40,
    'magnet:?xt=urn:btmh:1220caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad099f3a302e',
    f'magnet:?xt=urn:btih:{HEX}0',
    f'magnet:?xt=urn:btih:{BASE32}A',
    f'magnet:?xt=urn:btih:{BASE32[:-1]}1',
    f'magnet:?xt=urn:btih:{BASE32[:-1]}0',
    f'magnet:?xt=urn:btih:{BASE32[:-1]}8',
    'magnet:?xt=urn:btih:' + 'A' * 32,
    'magnet:?xt=urn:btih:' + '2' * 32,
    'magnet:?xt=urn:btih:' + '7' * 32,
    f'magnet:?xt=urn:btih:{HEX[:20]}-{HEX[20:]}',
    f'magnet:?xt=urn:btih:{HEX[:20]}é{HEX[20:]}',
    f'magnet:?xt=urn:btih:١{HEX[1:]}',
    f'magnet:?xt=urn:btih:Ａ{HEX[1:]}',
    f'urn:btih:{HEX}',
    f'xt=urn:btih:abc&xt=urn:btih:{HEX}',
    f'magnet:?xt=urn:BtIh:{HEX.lower()}&dn=x',
    f'magnet:?xt=urn:btih:{HEX}K',
    f'magnet:?xt=urn:btih:{HEX[:-1]}K',
    f'magnet:?xt=urn:btih:{BASE32[:-1]}ſ',
    f'magnet:?xt=urn:btih:{BASE32[:-1]}ı',
    f'magnet:?xt=urn:btih:{BASE32[:-1]}İ',
    f'magnet:?xt=urn:btih:{HEX}ı',
    f'magnet:?xt=urn:btih:{HEX}İ',
    f'magnet:?xt=urn:bİth:{HEX}',
    f'magnet:?xt=urn:bıth:{HEX}',
    'magnet:?xt=urn:btih:',
    'magnet:?xt=urn:btih:0x' + HEX[2:],
    'magnet:?xt=urn:btih:' + '0' * 40,
    'magnet:?xt=urn:btih:' + 'f' * 40,
]

NAMES: list[str] = [
    'a.mp4',
    'a.MKV',
    'a.Avi',
    'a.wmv',
    'a.mov',
    'a.flv',
    'a.m4v',
    'a.ts',
    'a.rmvb',
    'a.strm',
    'a.mp4.part',
    '.mp4',
    'a.',
    'a',
    'dir.mp4/x',
    'a.tar.ts',
    'a.MP4 ',
    'a.ＭＰ４',
    'a.İ',
    'a.ts\n',
    'x/y/z.mkv',
    'ABC-123.Rmvb',
    '',
    'a..mp4',
    'a.mp4.',
    'a.TS',
]


def _magnets() -> list[dict[str, Any]]:
    return [{'magnet': magnet, 'info_hash': extract_info_hash(magnet)} for magnet in MAGNETS]


def _media() -> list[dict[str, Any]]:
    return [
        {'suffixes': sorted(VIDEO_SUFFIXES)},
        *({'name': name, 'has_video_suffix': has_video_suffix(Path(name))} for name in NAMES),
    ]


GOLDENS = {
    'magnet/info_hash.json': _magnets,
    'magnet/media.json': _media,
}
