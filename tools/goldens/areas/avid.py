"""The AVID parser (`core/avid.py`) over both corpora and the inputs its tests use.

`get_avid` keys the acquisition ledger and names library files (cross-version contract 7),
so every row of `tests/data/avid_corpus_*.tsv` is recorded with what the baseline makes of
it, the xfail rows of the JavSP corpus included (their recorded value is the wrong one the
baseline gives, which the Rust port must give too). The hand-written cases add the tests'
own inputs and the corners a port can get wrong: non-ASCII digits and letters, newlines
before `$`, separators doubled by the noise removal, the parent-directory fallback of
`get_id`, the `)(` retry, and the deployment rules (`id_exceptions`, `ignored_id_patterns`).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from embyx_manager.core.avid import (
    AvidConfig,
    AvidParser,
    get_brand,
    get_cd,
    normalize_number,
    strip_extension,
    strip_variant_tags,
    variant_tags,
)

CORPUS_DIR = Path(__file__).resolve().parents[3] / 'tests' / 'data'

# Inputs of tests/test_avid.py and the corners named above.
EXTRA_TITLES: list[str] = [
    'ABC-123',
    'abc-123.mp4',
    'ABC123',
    '[prestige] ABP-123 something',
    'FC2-PPV-1234567',
    'fc2ppv 123456',
    'HEYDOUGA-4017-233',
    'hey-4017-233',
    'GETCHU-1234567',
    'GYUTTO-123456',
    '259LUXU-1234',
    '32ID-020',
    '17id_021.mp4',
    'CW3D2BD-04 3D キャットウォーク ポイズン 04 : 波多野結衣',
    '[BD-ISO] 3D CATWALK POISON 05 : Megumi Shino (CW3D2BD-05)',
    'CW3D2DBD-24 キャットウォーク ポイズン 24',
    '3D-CW3D2DBD-28.mp4',
    'MK3D2DBD-01',
    'MKBD-S03',
    'S2MBD-048',
    'S2M-001',
    'IBW-123z',
    'T28-557',
    'n1234',
    'N-0893.strm',
    'k_1234',
    '123456-789',
    '123456_789',
    'ABP-123中出し温泉旅行',
    'START-123あの子と',
    'SSIS-001 新人NO.1STYLE',
    '259LUXU-1234 ラグジュTV',
    'FC2-PPV-1234567 個人撮影',
    'hjd2048.com-0601meyd524-h264.mp4',
    'hhd800.com@T-28621.mp4',
    'ABC-123/whatever!!.mp4',
    '!!!',
    'prefix weird-special suffix 123',
    'ABC-123 1080P',
    'ABC-00123',
    'abc00123.mp4',
    'ssis00123',
    '118abp00077.mp4',
    'ABC-00012',
    'ABP-012',
    'FC2-00123',
    'ABC-0123',
    'heyzo_hd_1380_full.mp4',
    'heyzo_lt_1380_full.mp4',
    '@x@imouser.com_SDMF-016_1.2K.mp4',
    '[Uncensored] SDDE-618',
    '062620-001-carib-1080p.mp4',
    '1pondo-020317-001.mp4',
    '[Bdxav.Club]Tokyo-Hot-n1340 (title)',
    '[psk.la]Carib-080520-001-FHD',
    'HD_GS-333',
    'ABP-030-C-c_c-C-Cd1-cd4.mp4',
    'test.xxx@133ARA-030 hello',
    'hhd800.com@HUNTB-269',
    'HTTM-0066',
    'MXDLP-0337.mp4',
    '3DSVR-0707',
    'TBW-19',
    'abp-01',
    'IPZZ-1234',
    'T-28621',
    'IBW-949z',
    'MKD-S03',
    # Corners for the port.
    '',
    ' ',
    '.mp4',
    'ABC-123.',
    'ABC-123.longextension',
    'ABC-123.日本語ext',
    'ABC-123.日本',
    'ABC-١٢٣',
    'ABC-１２３',
    'ABC-٣٤',
    'ＡＢＣ-123',
    'ABC-123\n',
    'ABC-12\n',
    'ABC-123 CD2.mp4',
    'abc-123-cd1\n',
    'abc-123-C \n',
    'X-1',
    'AB-12',
    'A-12',
    'ABCDEFGHIJK-123',
    'ABCDEFGHIJ-123456',
    'abc_123_456',
    '(ABC)(123)',
    'foo)(bar',
    'AB)(12',
    'xx)(ABC123',
    'NOBRAND',
    'RED123',
    'SKY234',
    'EX0012',
    'red199 sky399',
    'MARRA-A030',
    'abc-a030',
    '4102-023',
    'xx4102_023yy',
    'K-1234-5',
    'n0893 k0001',
    'T-12345',
    'T_12345.mp4',
    'aT-12345',
    'T38-123',
    'T23-001',
    'HEYZO-1380',
    'heyzo 1380',
    'heyzo_1380',
    'HEYDOUGA_4017_0233',
    'heydouga-4017-02331',
    'GETCHU_123',
    'getchu123',
    'GYUTTO_123',
    '259luxu-001',
    '259LUXU_001',
    'FC2PPV-12345678',
    'fc2-ppv_1234',
    'FC2 PPV 1234567',
    'fc2_1234567_1080p.mp4',
    '[FHD]abc-123',
    'fhd-abc-123',
    'sd_abc-123',
    'hd-abc-123',
    'SDAB-123-sd',
    'uhd-ABC-123-4K-HQ.mkv',
    'ABC-123-x265-10bit.mkv',
    'ABC-123_2160p_uncensored_leak.mp4',
    'GIGL-677_4K60FPS.mp4',
    'vema-181-4k-C.mp4',
    'ABC-1234k.mp4',
    '4k.mp4',
    'caribbeancom 123456-789',
    'caribbeancompr-123456_789',
    '10musume-123456_01',
    'pacopacomama 012345_123',
    'muramura-123456_789',
    'tokyo hot n1234',
    'tokyo_hot-k0001',
    'tokyohot abc123',
    '1pon 123456_001',
    'pond-123456-001',
    'carib',
    'carib-',
    'abc.com',
    'abc.com-',
    'uus75.com',
    'xyz.net abc-123',
    'site.app-ABC-123',
    'site.xyz.ABC-123',
    'foo.COM.bar',
    'aa.com bb.net',
    'Kelvin-K123',
    'İPZ-123',
    'ıpz-123',
    'ß-123',
    'ﬀ-123',
    'ABC-123ABC-456',
    'ABC--123',
    'ABC__123',
    'ABC. .123',
    'ABC - 123',
    'abc-123-ch',
    'abc-123ch',
    'abc-123-uc',
    'abc-123hhb',
    'abc-123hhb3',
    'abc-123-cd12',
    'abc-123-cd123',
    'abc-123_c_ch',
    'ABC-123 chinese',
    'abc-123z',
    'ibw_123Z',
    'IBW-12345z',
    '17ID_02',
    '170ID-12345',
    '1ID-12',
    'S2M_05',
    'S2MBD-1234',
    'mkbd_s1',
    'MKD-S123',
    'CW3D2BD_4',
    'AB3D2BD-12',
    'ABCDE3D2BD-12',
    '1_2_3',
    '/',
    'a/b/c',
    '..',
    'ABC-123/..',
]

GET_ID_PATHS: list[str] = [
    'ABC-123/whatever!!.mp4',
    'library/ABC-123/cover.jpg',
    '/mnt/library/SSIS-001/SSIS-001.mp4',
    '/mnt/library/nothing/here.mp4',
    '/',
    '',
    '.',
    '..',
    'a/..',
    'ABC-123/..',
    'ABC-123/.',
    'ABC-123//x.mp4',
    '//ABC-123/x',
    'FC2-1234567/readme.txt',
    'x)(y/ABC-123.mp4',
    'ABC-123/x)(y',
    'hjd2048.com-xyz/ABP-984/x',
]

RULES: list[dict[str, Any]] = [
    {'id_exceptions': ['WEIRD-SPECIAL'], 'ignored_id_patterns': []},
    {'id_exceptions': [], 'ignored_id_patterns': ['1080P', 'X1080X']},
    {'id_exceptions': ['ABC-123', 'AB'], 'ignored_id_patterns': ['']},
    {'id_exceptions': [], 'ignored_id_patterns': [r'\[.*?\]', r'(?i)www\.\w+\.com']},
    {'id_exceptions': [], 'ignored_id_patterns': ['^ABC-', '-?CD\\d+$']},
    {'id_exceptions': ['weird-lower'], 'ignored_id_patterns': ['(?P<x>XYZ)']},
    {'id_exceptions': [], 'ignored_id_patterns': ['(', 'x']},
]

RULE_TITLES: list[str] = [
    'prefix weird-special suffix 123',
    'WEIRD-SPECIAL',
    'ABC-123 1080P',
    'X1080X-ABC-123',
    '[www.site.com] ABP-123',
    'WWW.SITE.COM ABP-123',
    'abc-123',
    'ABC-123-CD1',
    'ABC-123 cd2',
    'xyz-123',
    'XYZ123 ABC-456',
    'weird-lower',
    'something AB in it',
    'ab-123',
]


def _rows(name: str) -> list[list[str]]:
    lines = (CORPUS_DIR / name).read_text(encoding='utf-8').splitlines()
    return [line.split('\t') for line in lines if line and not line.startswith('#')]


def _parse(parser: AvidParser, title: str) -> dict[str, Any]:
    avid = parser.get_avid(title)
    return {
        'title': title,
        'avid': avid,
        'brand': get_brand(avid),
        'variant_tags': sorted(variant_tags(title)),
        'strip_variant_tags': strip_variant_tags(title),
        'strip_extension': strip_extension(title),
        'cd': get_cd(title),
    }


def _corpus() -> list[dict[str, Any]]:
    parser = AvidParser()
    cases = []
    for name, expected, *reason in _rows('avid_corpus_javsp.tsv'):
        cases.append(
            {'corpus': 'javsp', 'expected': expected, 'xfail': reason[0] if reason else None, **_parse(parser, name)}
        )
    for noisy, clean in _rows('avid_corpus_metatube.tsv'):
        cases.append(
            {'corpus': 'metatube', 'clean': clean, 'clean_avid': parser.get_avid(clean), **_parse(parser, noisy)}
        )
    return cases


def _cases() -> list[dict[str, Any]]:
    parser = AvidParser()
    return [_parse(parser, title) for title in EXTRA_TITLES]


def _get_id() -> list[dict[str, Any]]:
    parser = AvidParser()
    return [{'path': path, 'id': parser.get_id(path)} for path in GET_ID_PATHS]


def _normalize_number() -> list[dict[str, Any]]:
    values = [
        'ABC-123',
        'ABC-0123',
        'ABC-00012',
        'ABC-1',
        'ABC-0',
        'ABC-000',
        'ABC-1234',
        'ABC-01234',
        'A1-007',
        '123-045',
        '1A-5',
        'FC2-00123',
        'HEYZO-0012',
        'MKBD-012',
        'CW3D2DBD-01',
        'abc-012',
        'ABC-12A',
        'ABC_012',
        'ABC-12\n',
        'ABC-١٢',
        'ABC-0٣',
        '-12',
        'ABC-',
        'ABC-12-3',
        '',
        'S2M-01',
        'GETCHU-01',
        'KIND-007',
        'ABC-0' + '9' * 40,
        'ABC-' + '0' * 50,
        'ABC-' + '0' * 45 + '12',
    ]
    return [{'avid': value, 'normalized': normalize_number(value)} for value in values]


def _brand_and_cd() -> list[dict[str, Any]]:
    avids = [
        'ABC-123',
        '259LUXU-1234',
        'NOBRAND',
        '123456-789',
        '1-2',
        '12-3',
        '123\n-4',
        '12\n-3',
        '١٢٣-4',
        'A-B-C',
        '-1',
        '',
    ]
    titles = ['ABC-123 CD2.mp4', 'ABC-123.mp4', 'CD12x', 'CD1', 'CD1\n', 'cd2.mp4', 'ABC CD١x', 'xCD03.mkv']
    return [
        *({'avid': avid, 'brand': get_brand(avid)} for avid in avids),
        *({'title': title, 'cd': get_cd(title)} for title in titles),
    ]


def _rules() -> list[dict[str, Any]]:
    cases = []
    for rules in RULES:
        try:
            parser = AvidParser(
                AvidConfig(
                    id_exceptions=tuple(rules['id_exceptions']),
                    ignored_id_patterns=tuple(rules['ignored_id_patterns']),
                )
            )
        except re.error as exc:
            cases.append({**rules, 'error': type(exc).__name__})
            continue
        cases.append({**rules, 'results': [{'title': title, 'avid': parser.get_avid(title)} for title in RULE_TITLES]})
    return cases


GOLDENS = {
    'avid/corpus.json': _corpus,
    'avid/cases.json': _cases,
    'avid/get_id.json': _get_id,
    'avid/normalize_number.json': _normalize_number,
    'avid/brand_and_cd.json': _brand_and_cd,
    'avid/rules.json': _rules,
}
