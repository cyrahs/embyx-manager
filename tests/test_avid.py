import pytest

from embyx_manager.core.avid import (
    HIGH_RESOLUTION_TAGS,
    AvidConfig,
    AvidParser,
    get_brand,
    get_cd,
    variant_tags,
)


@pytest.fixture
def parser() -> AvidParser:
    return AvidParser()


@pytest.mark.parametrize(
    ('title', 'expected'),
    [
        ('ABC-123', 'ABC-123'),
        ('abc-123.mp4', 'ABC-123'),
        ('ABC123', 'ABC-123'),
        ('[prestige] ABP-123 something', 'ABP-123'),
        ('FC2-PPV-1234567', 'FC2-1234567'),
        ('fc2ppv 123456', 'FC2-123456'),
        ('HEYDOUGA-4017-233', 'HEYDOUGA-4017-233'),
        ('hey-4017-233', 'HEYDOUGA-4017-233'),
        ('GETCHU-1234567', 'GETCHU-1234567'),
        ('GYUTTO-123456', 'GYUTTO-123456'),
        ('259LUXU-1234', '259LUXU-1234'),
        ('32ID-020', '32ID-020'),  # the ID series keeps its varying digit prefix
        ('17id_021.mp4', '17ID-021'),
        # 3D+2D Blu-rays keep their 3D2(D)BD token: CATWALK POISON's early
        # volumes are CW3D2BD, its later volumes CW3D2DBD, MUGEN's MK3D2DBD.
        ('CW3D2BD-04 3D キャットウォーク ポイズン 04 : 波多野結衣', 'CW3D2BD-04'),
        ('[BD-ISO] 3D CATWALK POISON 05 : Megumi Shino (CW3D2BD-05)', 'CW3D2BD-05'),
        ('CW3D2DBD-24 キャットウォーク ポイズン 24', 'CW3D2DBD-24'),
        ('3D-CW3D2DBD-28.mp4', 'CW3D2DBD-28'),
        ('MK3D2DBD-01', 'MK3D2DBD-01'),
        # MUGEN's other Blu-ray forms, which the ordinary patterns would misread.
        ('MKBD-S03', 'MKBD-S03'),
        ('S2MBD-048', 'S2MBD-048'),
        ('S2M-001', 'S2M-001'),
        ('IBW-123z', 'IBW-123Z'),
        ('T28-557', 'T28-557'),
        ('n1234', 'N1234'),
        ('N-0893.strm', 'N0893'),  # Tokyo-Hot id written with a separator
        ('k_1234', 'K1234'),
        ('123456-789', '123456-789'),
        ('123456_789', '123456_789'),
    ],
)
def test_get_avid_recognizes_common_forms(parser: AvidParser, title: str, expected: str) -> None:
    assert parser.get_avid(title) == expected


@pytest.mark.parametrize(
    ('title', 'expected'),
    [
        # javlibrary's listing puts the id and the title in sibling divs, so the
        # RSS item title concatenates them with no separator at all.
        ('ABP-123中出し温泉旅行', 'ABP-123'),
        ('START-123あの子と', 'START-123'),
        ('SSIS-001 新人NO.1STYLE', 'SSIS-001'),
        ('259LUXU-1234 ラグジュTV', '259LUXU-1234'),
        ('FC2-PPV-1234567 個人撮影', 'FC2-1234567'),
    ],
)
def test_get_avid_reads_javlibrary_item_titles(parser: AvidParser, title: str, expected: str) -> None:
    """Titles shaped the way the RSSHub javlibrary routes emit them."""
    assert parser.get_avid(title) == expected


def test_get_avid_strips_suspicious_domain_prefix(parser: AvidParser) -> None:
    assert parser.get_avid('hjd2048.com-0601meyd524-h264.mp4') == 'MEYD-524'


def test_get_avid_falls_back_to_parent_directory(parser: AvidParser) -> None:
    assert parser.get_avid('ABC-123/whatever!!.mp4') == 'ABC-123'


def test_get_avid_returns_empty_when_unmatched(parser: AvidParser) -> None:
    assert parser.get_avid('!!!') == ''


def test_id_exceptions_win_over_pattern_matching() -> None:
    parser = AvidParser(AvidConfig(id_exceptions=('WEIRD-SPECIAL',)))
    assert parser.get_avid('prefix weird-special suffix 123') == 'WEIRD-SPECIAL'


def test_ignored_id_patterns_are_stripped_before_matching() -> None:
    parser = AvidParser(AvidConfig(ignored_id_patterns=(r'1080P', r'X1080X')))
    assert parser.get_avid('ABC-123 1080P') == 'ABC-123'


@pytest.mark.parametrize(
    ('avid', 'expected'),
    [
        ('ABC-123', 'ABC'),
        ('259LUXU-1234', '259LUXU'),
        ('NOBRAND', None),
        ('123456-789', '12'),
    ],
)
def test_get_brand(avid: str, expected: str | None) -> None:
    assert get_brand(avid) == expected


def test_get_cd() -> None:
    assert get_cd('ABC-123 CD2.mp4') == '2'
    assert get_cd('ABC-123.mp4') is None


# -- content-id zero padding (was archive.remove_00) --------------------------


@pytest.mark.parametrize(
    ('title', 'expected'),
    [
        ('ABC-00123', 'ABC-123'),
        ('abc00123.mp4', 'ABC-123'),
        ('ssis00123', 'SSIS-123'),
        ('118abp00077.mp4', 'ABP-077'),
        ('ABC-00012', 'ABC-012'),  # padded down to three digits, not to twelve
        ('ABP-012', 'ABP-012'),  # a genuine leading zero is left alone
        ('ABC-123', 'ABC-123'),
    ],
)
def test_content_id_padding_is_removed(parser: AvidParser, title: str, expected: str) -> None:
    assert parser.get_avid(title) == expected


@pytest.mark.parametrize('title', ['FC2-00123', '259LUXU-00123'])
def test_brands_without_content_ids_keep_their_digits(parser: AvidParser, title: str) -> None:
    assert parser.get_avid(title) == title


def test_padding_removal_needs_the_full_content_id_padding(parser: AvidParser) -> None:
    # A single leading zero is part of the number, not DMM's five-digit padding.
    assert parser.get_avid('ABC-0123') == 'ABC-0123'


# -- release noise ------------------------------------------------------------


@pytest.mark.parametrize(
    ('title', 'expected'),
    [
        ('heyzo_hd_1380_full.mp4', 'HEYZO-1380'),  # 'hd' would otherwise win
        ('heyzo_lt_1380_full.mp4', 'HEYZO-1380'),
        ('@x@imouser.com_SDMF-016_1.2K.mp4', 'SDMF-016'),  # 'sd' must not eat SDMF
        ('[Uncensored] SDDE-618', 'SDDE-618'),
        ('062620-001-carib-1080p.mp4', '062620-001'),
        ('1pondo-020317-001.mp4', '020317-001'),
        ('[Bdxav.Club]Tokyo-Hot-n1340 (title)', 'N1340'),
        ('[psk.la]Carib-080520-001-FHD', '080520-001'),
        ('HD_GS-333', 'GS-333'),
        ('ABP-030-C-c_c-C-Cd1-cd4.mp4', 'ABP-030'),
    ],
)
def test_release_noise_does_not_reach_the_id(parser: AvidParser, title: str, expected: str) -> None:
    assert parser.get_avid(title) == expected


def test_long_dotted_titles_keep_their_id(parser: AvidParser) -> None:
    # Treating everything after the first dot as an extension threw the id away.
    assert parser.get_avid('test.xxx@133ARA-030 hello') == 'ARA-030'
    assert parser.get_avid('hhd800.com@HUNTB-269') == 'HUNTB-269'


# -- variant tags (was archive.is_4k_video) -----------------------------------


@pytest.mark.parametrize(
    ('name', 'expected'),
    [
        ('ABC-123-4K.mp4', {'4K'}),
        ('vema-181-4k-C.mp4', {'4K'}),
        ('GIGL-677_4K60FPS.mp4', {'4K', '60FPS'}),  # tags with no separator between them
        ('ABC-123-2160p.mkv', {'2160P'}),
        ('ABC-123-x264-leak.mp4', {'X264', 'LEAK'}),
        ('ABC-123.mp4', set()),
        ('ABC-1234k.mp4', set()),  # '4k' here is part of the number
    ],
)
def test_variant_tags(name: str, expected: set[str]) -> None:
    assert variant_tags(name) == expected


@pytest.mark.parametrize(
    ('name', 'high_resolution'),
    [
        ('ABC-123-4K.mp4', True),
        ('ABC-123-2160P.mp4', True),
        ('ABC-123-1080p.mp4', False),
        ('ABC-123.mp4', False),
        ('4k.mp4', True),
    ],
)
def test_high_resolution_detection(name: str, *, high_resolution: bool) -> None:
    assert bool(variant_tags(name) & HIGH_RESOLUTION_TAGS) is high_resolution
