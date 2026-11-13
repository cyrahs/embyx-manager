"""Python `str` predicates the copied `pytext` helper must equal.

`str.strip()` and `str.split()` cut at `str.isspace()` characters, which include the
ASCII separators U+001C..U+001F that Unicode's White_Space does not; `str.isdigit()` and
`str.isdecimal()` accept every Unicode digit, not only ASCII ones (AVID and number parsing
from WP1 on). These goldens are the full code-point scan of Python 3.13 (Unicode 15.1).

Copied from fav's golden generator (`tools/goldens/areas/text.py` at the commit named in
docs/CONVENTIONS.md), as the goldens of the helpers copied with it.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


def _ranges(predicate: Callable[[str], bool]) -> list[list[int]]:
    ranges: list[list[int]] = []
    for code in range(sys.maxunicode + 1):
        if not predicate(chr(code)):
            continue
        if ranges and ranges[-1][1] == code - 1:
            ranges[-1][1] = code
        else:
            ranges.append([code, code])
    return ranges


def _isspace() -> list[dict[str, Any]]:
    return [{'ranges': _ranges(str.isspace)}]


def _isdigit() -> list[dict[str, Any]]:
    return [{'ranges': _ranges(str.isdigit)}]


def _isdecimal() -> list[dict[str, Any]]:
    return [{'ranges': _ranges(str.isdecimal)}]


GOLDENS = {
    'text/isspace.json': _isspace,
    'text/isdigit.json': _isdigit,
    'text/isdecimal.json': _isdecimal,
}
