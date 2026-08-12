import pytest

from embyx_manager.core.avid import AvidConfig, AvidParser, get_brand, get_cd


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
        ('IBW-123z', 'IBW-123Z'),
        ('T28-557', 'T28-557'),
        ('n1234', 'N1234'),
        ('123456-789', '123456-789'),
        ('123456_789', '123456_789'),
    ],
)
def test_get_avid_recognizes_common_forms(parser: AvidParser, title: str, expected: str) -> None:
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
