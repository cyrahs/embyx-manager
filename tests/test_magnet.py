import pytest

from embyx_manager.core.magnet import extract_info_hash

HEX = 'C12FE1C06BBA254A9DC9F519B335AA7C1367A88A'
BASE32 = 'YEX6DQDLXISUVHOJ6UM3GNNKPQJWPKEK'


@pytest.mark.parametrize(
    ('magnet', 'expected'),
    [
        (f'magnet:?xt=urn:btih:{HEX}', HEX),
        (f'magnet:?xt=urn:btih:{HEX.lower()}', HEX),
        (f'magnet:?xt=urn:btih:{HEX}&dn=ABC-123', HEX),
        (f'magnet:?dn=ABC-123&xt=urn:btih:{HEX}&tr=udp://tracker', HEX),
        (f'magnet:?xt=urn:btih:{BASE32}', HEX),
        (f'magnet:?xt=urn:btih:{BASE32.lower()}&dn=ABC-123', HEX),
        (f'MAGNET:?XT=URN:BTIH:{HEX}', HEX),
    ],
)
def test_info_hash_is_normalized_to_upper_hex(magnet: str, expected: str) -> None:
    assert extract_info_hash(magnet) == expected


@pytest.mark.parametrize(
    'magnet',
    [
        '',
        'not a magnet',
        'magnet:?dn=ABC-123',
        'magnet:?xt=urn:sha1:C12FE1C06BBA254A9DC9F519B335AA7C1367A88A',
        f'magnet:?xt=urn:btih:{HEX[:-1]}',  # too short to be either encoding
        'magnet:?xt=urn:btih:' + 'Z' * 40,  # right length, not hex
    ],
)
def test_unusable_magnets_yield_no_hash(magnet: str) -> None:
    assert extract_info_hash(magnet) is None


def test_v2_only_magnet_is_not_tracked() -> None:
    # btmh (v2) carries no 40-hex v1 hash for CloudDrive to report back.
    magnet = 'magnet:?xt=urn:btmh:1220caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad099f3a302e'
    assert extract_info_hash(magnet) is None
