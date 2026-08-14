"""Magnet URI parsing, reduced to the one field the pipeline tracks.

The BitTorrent info hash is what ties a magnet we submitted to the offline task
CloudDrive reports back, so it is extracted once when the candidate is recorded
and used as the join key from then on. Hashes are normalized to upper-case hex,
because the same torrent is advertised as hex by some sources and base32 by
others.
"""

import base64
import binascii
import re

_BTIH_RE = re.compile(r'urn:btih:([A-Za-z0-9]+)', re.IGNORECASE)
_HEX_LENGTH = 40
_BASE32_LENGTH = 32


def extract_info_hash(magnet: str) -> str | None:
    """Return the magnet's info hash as upper-case hex, or None when it has none.

    Only v1 (SHA-1) hashes are recognized; a v2-only magnet has no 40-hex hash
    for CloudDrive to report and cannot be tracked.
    """
    match = _BTIH_RE.search(magnet)
    if match is None:
        return None
    value = match.group(1)
    if len(value) == _HEX_LENGTH:
        try:
            int(value, 16)
        except ValueError:
            return None
        return value.upper()
    if len(value) == _BASE32_LENGTH:
        try:
            decoded = base64.b32decode(value.upper())
        except (binascii.Error, ValueError):
            return None
        return decoded.hex().upper()
    return None
