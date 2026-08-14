"""Extract and normalize video IDs (DVD ID / AVID) from titles and file names.

This is the single place an AVID is derived, so that RSS, the archiver, and the
acquisition ledger can never disagree about what to call the same video.

The pattern branches are derived from JavSP's ``javsp/avid.py``
(https://github.com/Yuukiy/JavSP), Copyright (C) JavSP contributors, licensed
under GPL-3.0; site-specific quirks are documented inline in Chinese where they
originate from JavSP. This project is likewise distributed under GPL-3.0 (see
LICENSE at the repository root).

The release-noise vocabulary (``_TAG_ALTERNATIVES``, the studio and suffix
patterns) and the zero-padding rule are ported from metatube-sdk-go
(https://github.com/metatube-community/metatube-sdk-go), Copyright (C) MetaTube
contributors, licensed under Apache-2.0. JavSP recognizes IDs but leaves quality
tags to per-deployment configuration; metatube strips that noise systematically
but does not rebuild a canonical ID. Taking the recognizer from one and the
vocabulary from the other is what lets ``heyzo_hd_1380`` resolve to HEYZO-1380
instead of HD-1380.
"""

import re
from dataclasses import dataclass
from pathlib import Path

MIN_BRAND_LENGTH = 2
#: Longest suffix still treated as a file extension. A title like
#: "test.xxx@133ARA-030 hello" has a long trailing "suffix" that is part of the
#: name, and cutting it would throw the ID away.
MAX_EXTENSION_LENGTH = 7

_DOMAIN_RE = re.compile(r'\w{3,10}\.(COM|NET|APP|XYZ)', re.IGNORECASE)

#: Release noise: containers, codecs, resolutions, and provenance markers. The
#: tag must stand alone between non-alphanumerics, or 'sd' would eat the start of
#: SDMF-016 and leave MF-016 behind.
_TAG_ALTERNATIVES = (
    r'dvd|iso|mkv|mp4|c?avi|\d*fps|whole|(?:f|hhb)?hd\d*|sd\d*|lt'
    r'|(?:144|240|360|480|720|1080|2160)[pi]|x1080x|uhd|hq|uncensored|leak|[2468]ks?|[xh]26[45]'
)
_TAG_RE = re.compile(rf'(?:^|(?<=[^a-z\d]))(?:{_TAG_ALTERNATIVES})+(?=$|[^a-z\d])', re.IGNORECASE)
_SINGLE_TAG_RE = re.compile(rf'({_TAG_ALTERNATIVES})', re.IGNORECASE)
#: Separators left doubled up by stripping; the ID patterns expect exactly one.
_REPEATED_SEPARATOR_RE = re.compile(r'[-_. ]{2,}')
_QUALITY_PREFIX_RE = re.compile(r'^(?:f?hd|sd)[-_]', re.IGNORECASE)
#: Trailing disc/subtitle markers, stripped repeatedly. Unlike metatube we do not
#: strip a bare trailing A-D: without a separator it eats real ID suffixes such
#: as IBW-123z's siblings, and multi-part sets are detected from the file set.
_SUFFIX_RE = re.compile(r'(?:[-_](?:c|uc|ch|cd\d{1,2})|hhb\d*|ch)\s*$', re.IGNORECASE)
#: Uncensored studios that prefix their own date-style IDs; the ID is what stays.
_STUDIO_RE = re.compile(
    r'(?:^|(?<=[^a-z\d]))(?:carib(?:b?ean)?(?:com)?(?:pr)?|1?pond?o?|10mu(?:sume)?'
    r'|paco(?:paco)?(?:mama)?|mura(?:mura)?|tokyo[-_\s]?hot)'
    r'(?:[-_\s]+(?P<kept>\d{4,}[-_]\d{2,}|[a-z]{1,4}\d{2,4})|$)',
    re.IGNORECASE,
)

#: Variant markers that mean "this file is the higher-resolution cut".
HIGH_RESOLUTION_TAGS = frozenset({'2K', '4K', '6K', '8K', '2160P', '2160I', 'UHD'})

#: Content-ID zero padding: DMM pads the number to five digits, so ABC-00123 and
#: ABC-123 are the same video. Only 00-prefixed runs are touched, which leaves a
#: genuinely three-digit ID like ABP-012 alone.
_PADDED_NUMBER_RE = re.compile(r'^([A-Z]{2,10})-(00\d{3,4})$')
#: Brands whose numbers are not DMM content IDs and must not be un-padded.
_NON_CID_BRANDS = frozenset({'FC2', 'HEYZO', 'HEYDOUGA', 'GETCHU', 'GYUTTO', 'MKBD', 'MKD', 'S2M', 'S2MBD'})


def strip_extension(name: str) -> str:
    """Drop a trailing file extension, leaving long dotted titles intact."""
    suffix = Path(name).suffix
    if 0 < len(suffix) < MAX_EXTENSION_LENGTH:
        return name[: -len(suffix)]
    return name


def strip_variant_tags(name: str) -> str:
    """The name without its extension or quality tags; separators are left behind.

    What remains of two file names being equal is how the archiver knows they
    are cuts of the same video rather than parts of one.
    """
    return _TAG_RE.sub('', strip_extension(name))


def variant_tags(name: str) -> frozenset[str]:
    """Quality markers on a release name, upper-cased (``4K``, ``1080P``, ``H264``).

    Shares one vocabulary with the noise the parser strips, so "is this the 4K
    cut" and "ignore this when reading the ID" can never drift apart. Runs are
    located first and split afterwards, so tags that pile up without separators
    ("_4K60FPS") are still read as two tags rather than none.
    """
    return frozenset(
        tag.group(1).upper()
        for run in _TAG_RE.finditer(strip_extension(name))
        for tag in _SINGLE_TAG_RE.finditer(run.group(0))
    )


def _strip_release_noise(stem: str) -> str:
    """Remove quality tags and studio prefixes, leaving the ID and its separators.

    Boundaries are matched zero-width so that a bracket or an at-sign around the
    noise survives; only the separators the removal leaves doubled are collapsed.
    """
    stem = _QUALITY_PREFIX_RE.sub('', stem)
    stem = _TAG_RE.sub('', stem)
    stem = _STUDIO_RE.sub(lambda match: match.group('kept') or '', stem)
    while (trimmed := _SUFFIX_RE.sub('', stem)) != stem:
        stem = trimmed
    return _REPEATED_SEPARATOR_RE.sub('-', stem).strip()


def _match_odd_numbering(norm: str) -> str:
    """Studios whose numbering does not fit the BRAND-NUMBER shape.

    Tried after the regular patterns for every branch, so a name that looks
    ordinary is never claimed by one of these narrower rules.
    """
    # 尝试匹配TMA制作的影片(如'T28-557', 他家的番号很乱)
    if match := re.search(r'(T[23]8[-_]\d{3})', norm):
        return match.group(1)
    # 尝试匹配东热n, k系列
    if match := re.search(r'(N\d{4}|K\d{4})', norm, re.IGNORECASE):
        return match.group(1)
    # 尝试匹配纯数字番号, 无码影片
    if match := re.search(r'(\d{6}[-_]\d{2,3})', norm):
        return match.group(1)
    # Numbers carrying a letter prefix, e.g. MARRA-A030; a bare letter before the
    # digits is a weak signal, so this only runs once everything else has failed.
    if match := re.search(r'([A-Z]{2,10})[-_]([A-Z]\d{2,5})', norm):
        return match.group(1) + '-' + match.group(2)
    # Short numeric ids such as 4102-023, which the six-digit rule above misses.
    if match := re.search(r'(\d{4}[-_]\d{3})', norm):
        return match.group(1)
    return ''


def _unpad_content_id(avid: str) -> str:
    match = _PADDED_NUMBER_RE.match(avid)
    if match is None or match.group(1) in _NON_CID_BRANDS:
        return avid
    return f'{match.group(1)}-{int(match.group(2)):03d}'


@dataclass(frozen=True)
class AvidConfig:
    """Deployment-tunable ID parsing rules.

    ``id_exceptions`` are returned verbatim when found in a title (upper-cased match).
    ``ignored_id_patterns`` are regexes stripped from the input before matching.
    """

    id_exceptions: tuple[str, ...] = ()
    ignored_id_patterns: tuple[str, ...] = ()


class AvidParser:
    def __init__(self, config: AvidConfig | None = None) -> None:
        self._config = config or AvidConfig()
        patterns = [pattern for pattern in self._config.ignored_id_patterns if pattern]
        self._ignore_re = re.compile('|'.join(patterns)) if patterns else None

    @property
    def config(self) -> AvidConfig:
        return self._config

    def get_avid(self, title: str) -> str:
        """The canonical ID for a title or file name; '' when none can be read."""
        title = title.replace('/', '')
        upper = title.upper()
        for exception in self._config.id_exceptions:
            if exception in upper:
                return exception
        return _unpad_content_id(self.get_id(title).upper())

    def get_id(self, filepath_str: str) -> str:  # noqa: C901, PLR0911, PLR0912
        """从给定的文件路径中提取番号(DVD ID)"""
        filepath = Path(filepath_str)
        stem = _strip_release_noise(strip_extension(filepath.name))
        if self._ignore_re is not None:
            stem = self._ignore_re.sub('', stem)
        norm = stem.upper()
        if 'FC2' in norm:
            # 根据FC2 Club的影片数据, FC2编号为5-7个数字
            match = re.search(r'FC2[^A-Z\d]{0,5}(PPV[^A-Z\d]{0,5})?(\d{5,7})', norm, re.IGNORECASE)
            if match:
                return 'FC2-' + match.group(2)
        elif 'HEYDOUGA' in norm:
            match = re.search(r'(HEYDOUGA)[-_]*(\d{4})[-_]0?(\d{3,5})', norm, re.IGNORECASE)
            if match:
                return '-'.join(match.groups())
        elif 'GETCHU' in norm:
            match = re.search(r'GETCHU[-_]*(\d+)', norm, re.IGNORECASE)
            if match:
                return 'GETCHU-' + match.group(1)
        elif 'GYUTTO' in norm:
            match = re.search(r'GYUTTO-(\d+)', norm, re.IGNORECASE)
            if match:
                return 'GYUTTO-' + match.group(1)
        elif '259LUXU' in norm:  # special case having form of '259luxu'
            match = re.search(r'259LUXU-(\d+)', norm, re.IGNORECASE)
            if match:
                return '259LUXU-' + match.group(1)
        else:
            # 先尝试移除可疑域名进行匹配, 如果匹配不到再使用原始文件名进行匹配
            no_domain = _DOMAIN_RE.sub('', norm)
            if no_domain != norm:
                avid = self.get_avid(no_domain)
                if avid:
                    return avid
            # 匹配缩写成hey的heydouga影片。由于番号分三部分, 要先于后面分两部分的进行匹配
            match = re.search(r'(?:HEY)[-_]*(\d{4})[-_]0?(\d{3,5})', norm, re.IGNORECASE)
            if match:
                return 'heydouga-' + '-'.join(match.groups())
            # 匹配片商 MUGEN 的奇怪番号。由于MK3D2DBD的模式, 要放在普通番号模式之前进行匹配
            match = re.search(r'(MKB?D)[-_]*(S\d{2,3})|(MK3D2DBD|S2M|S2MBD)[-_]*(\d{2,3})', norm, re.IGNORECASE)
            if match:
                if match.group(1) is not None:
                    return match.group(1) + '-' + match.group(2)
                return match.group(3) + '-' + match.group(4)
            # 匹配IBW这样带有后缀z的番号
            match = re.search(r'(IBW)[-_](\d{2,5}z)', norm, re.IGNORECASE)
            if match:
                return match.group(1) + '-' + match.group(2)
            # 普通番号, 优先尝试匹配带分隔符的, 如ABC-123
            match = re.search(r'([A-Z]{2,10})[-_](\d{2,5})', norm, re.IGNORECASE)
            if match:
                return match.group(1) + '-' + match.group(2)
            # 普通番号, 运行到这里时表明无法匹配到带分隔符的番号
            # 先尝试匹配东热的red, sky, ex三个不带-分隔符的系列
            # 这三个系列已停止更新, 因此根据其作品编号将数字范围限制得小一些以降低误匹配概率
            match = re.search(r'(RED[01]\d\d|SKY[0-3]\d\d|EX00[01]\d)', norm, re.IGNORECASE)
            if match:
                return match.group(1)
            # 然后再将影片视作缺失了-分隔符来匹配
            match = re.search(r'([A-Z]{2,})(\d{2,5})', norm, re.IGNORECASE)
            if match:
                return match.group(1) + '-' + match.group(2)
        if fallback := _match_odd_numbering(norm):
            return fallback
        # 如果还是匹配不了, 尝试将')('替换为'-'后再试, 少部分影片的番号是由')('分隔的
        if ')(' in norm:
            avid = self.get_avid(norm.replace(')(', '-'))
            if avid:
                return avid
        # 如果最后仍然匹配不了番号, 则尝试使用文件所在文件夹的名字去匹配
        if filepath.parent.name != '':  # haven't reached '.' or '/'
            return self.get_avid(filepath.parent.name)
        return ''


def get_brand(avid: str) -> str | None:
    if '-' not in avid:
        return None
    brand = avid.split('-', maxsplit=1)[0]
    if re.match(r'^\d+$', brand) and len(brand) >= MIN_BRAND_LENGTH:
        brand = brand[:MIN_BRAND_LENGTH]
    return brand


def get_cd(title: str) -> str | None:
    if result := re.search(r'CD(\d+).', title):
        return result.group(1)
    return None
