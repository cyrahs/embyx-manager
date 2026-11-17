"""Generate the golden files the Rust tests compare against, from the Python baseline.

Run from the repository root on the `rust` branch, whose Python tree is the baseline commit
in `BASELINE` (`tools/check_tree.sh` enforces that), with the pinned Python:

    uv run --python 3.13.18 python tools/goldens/generate.py          # rewrite goldens/
    uv run --python 3.13.18 python tools/goldens/generate.py --check  # fail on any drift

Every golden is produced by calling the real Python code. The Rust side reads these files
but never writes them, and no golden is ever edited by hand: change the case list in
`areas/<area>.py` and regenerate.

Each module in `areas/` exposes `GOLDENS: dict[str, Callable[[], list]]`, mapping a path
under `goldens/` to a function returning the list of cases. Cases must be deterministic:
the clock, ids and ordering pinned, no randomness, no network, no database. Transcripts
of the effect scenarios are not made here but by `tools/scenarios/runner.py` (they need a
database), into `goldens/scenarios/`.
"""

from __future__ import annotations

import argparse
import difflib
import importlib
import json
import os
import pkgutil
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
GOLDEN_DIR = REPO_ROOT / 'goldens'
# Python version the goldens are made with (docs/CONVENTIONS.md): `str`, `repr`, `re` and
# Unicode tables differ between versions.
PINNED_PYTHON = (3, 13, 18)
# Owned by tools/scenarios/runner.py, not by an area here.
FOREIGN_DIRS = ('scenarios/',)

os.environ['TZ'] = 'UTC'
time.tzset()
# The repository's `src` goes first and `areas` is found after it, so nothing under tools/
# can shadow the Python code the goldens must come from (`_assert_real_sources`).
sys.path.insert(0, str(REPO_ROOT / 'src'))
sys.path.append(str(HERE))


def _assert_real_sources() -> None:
    """Fail unless `embyx_manager` is the repository's own tree."""
    import embyx_manager  # noqa: PLC0415

    location = Path(embyx_manager.__file__).resolve().parent
    if location != REPO_ROOT / 'src' / 'embyx_manager':
        msg = f'embyx_manager resolves to {location}, not {REPO_ROOT / "src" / "embyx_manager"}'
        raise SystemExit(msg)
    tools = REPO_ROOT / 'tools'
    for name, module in list(sys.modules.items()):
        file = getattr(module, '__file__', None)
        if not file or name == 'areas' or name.startswith('areas.'):
            continue
        resolved = Path(file).resolve()
        if resolved != Path(__file__).resolve() and resolved.is_relative_to(tools):
            msg = f'module {name} was loaded from {file}, inside tools/'
            raise SystemExit(msg)


def _assert_pinned_python() -> None:
    if sys.version_info[:3] != PINNED_PYTHON:
        pinned = '.'.join(map(str, PINNED_PYTHON))
        msg = f'goldens are generated with Python {pinned}, this is {sys.version.split()[0]} (uv run --python {pinned})'
        raise SystemExit(msg)


def _render(area: str, cases: list[object]) -> str:
    document = {
        '_generated': 'by tools/goldens/generate.py from the Python baseline; do not edit',
        'baseline': (REPO_ROOT / 'BASELINE').read_text(encoding='utf-8').strip(),
        'python': '.'.join(map(str, PINNED_PYTHON)),
        'area': area,
        'cases': cases,
    }
    return json.dumps(document, ensure_ascii=False, indent=2) + '\n'


def build() -> dict[str, str]:
    _assert_pinned_python()
    _assert_real_sources()
    import areas  # noqa: PLC0415

    outputs: dict[str, str] = {}
    for info in sorted(pkgutil.iter_modules(areas.__path__), key=lambda item: item.name):
        module = importlib.import_module(f'areas.{info.name}')
        for relative, producer in module.GOLDENS.items():
            if relative in outputs:
                msg = f'golden {relative} is produced twice'
                raise SystemExit(msg)
            if relative.startswith(FOREIGN_DIRS):
                msg = f'golden {relative} is under a directory another tool owns'
                raise SystemExit(msg)
            outputs[relative] = _render(info.name, producer())
    _assert_real_sources()
    return outputs


def _existing() -> dict[str, str]:
    if not GOLDEN_DIR.exists():
        return {}
    return {
        relative: path.read_text(encoding='utf-8')
        for path in sorted(GOLDEN_DIR.rglob('*.json'))
        if not (relative := path.relative_to(GOLDEN_DIR).as_posix()).startswith(FOREIGN_DIRS)
    }


def check(outputs: dict[str, str]) -> int:
    existing = _existing()
    problems = 0
    for relative in sorted(set(outputs) | set(existing)):
        want, have = outputs.get(relative), existing.get(relative)
        if want == have:
            continue
        problems += 1
        if have is None:
            print(f'missing: goldens/{relative} (run tools/goldens/generate.py and commit it)')
        elif want is None:
            print(f'stale: goldens/{relative} has no generator (delete it or restore its producer)')
        else:
            print(f'drift: goldens/{relative} differs from what the Python baseline produces')
            diff = difflib.unified_diff(
                have.splitlines(), want.splitlines(), 'committed', 'generated', lineterm='', n=1
            )
            for line in list(diff)[:40]:
                print(f'  {line}')
    if problems:
        print(f'{problems} golden file(s) out of date')
        return 1
    print(f'{len(outputs)} golden files match the Python baseline')
    return 0


def write(outputs: dict[str, str]) -> None:
    for relative in _existing().keys() - outputs.keys():
        (GOLDEN_DIR / relative).unlink()
    for relative, text in outputs.items():
        path = GOLDEN_DIR / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding='utf-8', newline='\n')
    print(f'wrote {len(outputs)} golden files under goldens/')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n', 1)[0])
    parser.add_argument('--check', action='store_true', help='compare instead of writing; exit 1 on any difference')
    args = parser.parse_args()
    outputs = build()
    if args.check:
        return check(outputs)
    write(outputs)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
