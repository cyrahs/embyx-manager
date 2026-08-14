"""Regression corpora for the AVID parser, imported from two upstream projects.

See the header of each .tsv for its provenance and license. The two corpora test
different things and are checked differently:

- JavSP ships expected ids, so those rows assert the id outright.
- metatube ships its own trimmed form, which is not canonical the way ours is.
  Those rows assert the property that survives the difference: stripping the
  noise must not change which id we read.
"""

from pathlib import Path

import pytest

from embyx_manager.core.avid import AvidParser

CORPUS_DIR = Path(__file__).parent / 'data'


def _rows(name: str) -> list[tuple[str, ...]]:
    lines = (CORPUS_DIR / name).read_text(encoding='utf-8').splitlines()
    return [tuple(line.split('\t')) for line in lines if line and not line.startswith('#')]


def _javsp_cases() -> list[object]:
    cases: list[object] = []
    for row in _rows('avid_corpus_javsp.tsv'):
        name, expected, *reason = row
        marks = [pytest.mark.xfail(reason=reason[0], strict=True)] if reason else []
        cases.append(pytest.param(name, expected, marks=marks, id=name[:60]))
    return cases


def _metatube_cases() -> list[object]:
    return [pytest.param(noisy, clean, id=noisy[:60]) for noisy, clean in _rows('avid_corpus_metatube.tsv')]


@pytest.fixture(scope='module')
def parser() -> AvidParser:
    return AvidParser()


def test_corpora_are_present() -> None:
    # A mis-parsed corpus would silently turn into zero test cases.
    assert len(_javsp_cases()) > 600
    assert len(_metatube_cases()) > 150


@pytest.mark.parametrize(('name', 'expected'), _javsp_cases())
def test_javsp_corpus(parser: AvidParser, name: str, expected: str) -> None:
    # Compared case-insensitively: this parser upper-cases, JavSP does not.
    assert parser.get_avid(name).upper() == expected.upper()


@pytest.mark.parametrize(('noisy', 'clean'), _metatube_cases())
def test_metatube_noise_invariance(parser: AvidParser, noisy: str, clean: str) -> None:
    from_clean = parser.get_avid(clean)
    assert from_clean != ''
    assert parser.get_avid(noisy) == from_clean
