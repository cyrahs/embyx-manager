"""Test parity between the Python suite (`tests/`) and the Rust port (`crates/`).

Every Python test function is either ported, waived with a reason, or still pending. A Rust
test claims a Python test with a comment directly above its test attribute:

    // py: tests/test_avid.py::test_get_avid
    #[test]
    fn get_avid() { ... }

A parametrized Python test (pytest collects it as several cases) is claimed with the number of
cases the Rust test covers, and the claims of one Python test must add up to pytest's count:

    // py: tests/test_avid.py::test_get_avid_corpus cases=412
    #[test]
    fn avid_corpus() { ... }

One Rust test may claim several Python tests (stack the comments), and one Python test may be
claimed by several Rust tests. Waivers live in `tools/parity/waivers.toml`, and
`tools/parity/scope.toml` assigns every Python test to exactly one work package. When a work
package is marked done, every test in its scope must be ported or waived.

    uv run pytest --collect-only -q > collected.txt
    python3 tools/parity/parity.py --write --pytest-collected collected.txt   # regenerate PARITY.md
    python3 tools/parity/parity.py --check --pytest-collected collected.txt   # what CI runs

The baseline's numbers (747 test functions, 1,754 cases) are asserted, so a test that
disappears from either side is noticed. Standard library only; it never imports the code
under test. Adapted from fav (`rust/tools/parity/parity.py`, docs/CONVENTIONS.md).
"""

from __future__ import annotations

import argparse
import ast
import fnmatch
import re
import sys
import tomllib
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TESTS_DIR = REPO_ROOT / 'tests'
RUST_DIR = REPO_ROOT / 'crates'
SCOPE_FILE = REPO_ROOT / 'tools' / 'parity' / 'scope.toml'
WAIVERS_FILE = REPO_ROOT / 'tools' / 'parity' / 'waivers.toml'
PARITY_MD = REPO_ROOT / 'docs' / 'rewrite' / 'PARITY.md'
# The baseline's suite (BASELINE). WP16 updates both when it catches up with `main`.
EXPECTED_FUNCTIONS: int | None = 747
EXPECTED_CASES: int | None = 1754

ANNOTATION = re.compile(r'^\s*//\s*py:\s*(?P<id>\S+)(?:\s+cases=(?P<cases>\d+))?\s*$')
LOOSE_ANNOTATION = re.compile(r'^\s*//\s*py:')
TEST_ATTRIBUTE = re.compile(r'^\s*#\[\s*(?:[\w:]+::)?test\b')
FN_LINE = re.compile(r'^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+(?P<name>\w+)')
MIN_REASON_CHARS = 20
STATUSES = ('pending', 'in-progress', 'done')
# The plan's work packages and nothing else, so tests cannot be parked in an invented one.
WORK_PACKAGES = tuple(f'WP{number}' for number in range(17))
FINAL_WP = 'WP16'


@dataclass(frozen=True, slots=True)
class RustTest:
    path: str
    line: int
    name: str
    cases: int | None


@dataclass(slots=True)
class Report:
    errors: list[str] = field(default_factory=list)

    def error(self, message: str) -> None:
        self.errors.append(message)


# ---------- Python side ----------


def python_tests(tests_dir: Path | None = None) -> list[str]:
    """Collect test ids the way pytest's defaults do: `test_*.py`, `test*` functions, `Test*` classes."""
    ids: list[str] = []
    for path in sorted((tests_dir or TESTS_DIR).glob('test_*.py')):
        relative = path.relative_to(REPO_ROOT).as_posix()
        tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        for node in tree.body:
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name.startswith('test'):
                ids.append(f'{relative}::{node.name}')
            elif isinstance(node, ast.ClassDef) and node.name.startswith('Test'):
                ids.extend(
                    f'{relative}::{node.name}::{item.name}'
                    for item in node.body
                    if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef) and item.name.startswith('test')
                )
    return ids


def pytest_cases(collected: Path) -> Counter[str]:
    """Cases per test id from `pytest --collect-only -q` output (parametrization stripped)."""
    cases: Counter[str] = Counter()
    for line in collected.read_text(encoding='utf-8').splitlines():
        if '::' in line and not line.startswith(' '):
            cases[line.split('[', 1)[0].strip()] += 1
    return cases


# ---------- Rust side ----------


def rust_sources(root: Path | None = None) -> list[Path]:
    root = root or RUST_DIR
    if not root.exists():
        return []
    return sorted(path for path in root.rglob('*.rs') if 'target' not in path.relative_to(root).parts)


def rust_annotations(report: Report, root: Path | None = None) -> dict[str, list[RustTest]]:
    claims: dict[str, list[RustTest]] = defaultdict(list)
    for path in rust_sources(root):
        lines = path.read_text(encoding='utf-8').splitlines()
        relative = path.relative_to(REPO_ROOT).as_posix()
        index = 0
        while index < len(lines):
            match = ANNOTATION.match(lines[index])
            if not match:
                if LOOSE_ANNOTATION.match(lines[index]):
                    message = 'malformed claim; write `// py: <test id>` or `// py: <test id> cases=N`'
                    report.error(f'{relative}:{index + 1}: {message}')
                index += 1
                continue
            start = index
            found: list[tuple[str, int | None]] = []
            while index < len(lines) and (annotation := ANNOTATION.match(lines[index])):
                cases = annotation.group('cases')
                found.append((annotation.group('id'), int(cases) if cases is not None else None))
                index += 1
            # Attributes (and plain comments) may sit between the claims and the fn.
            saw_test_attribute = False
            while index < len(lines):
                stripped = lines[index].strip()
                if TEST_ATTRIBUTE.match(lines[index]):
                    saw_test_attribute = True
                elif not (stripped.startswith(('#[', '//')) or stripped == ''):
                    break
                index += 1
            fn = FN_LINE.match(lines[index]) if index < len(lines) else None
            if not saw_test_attribute or fn is None:
                message = '`// py:` must sit directly above a test function (#[test], #[tokio::test], ...)'
                report.error(f'{relative}:{start + 1}: {message}')
                continue
            for test_id, cases in found:
                claims[test_id].append(RustTest(relative, start + 1, fn.group('name'), cases))
    return claims


def rust_test_names(listing: Path) -> set[str]:
    """Function names from `cargo test -- --list --format terse` (`path::to::name: test`)."""
    names = set()
    for line in listing.read_text(encoding='utf-8').splitlines():
        if line.endswith(': test'):
            names.add(line.removesuffix(': test').rsplit('::', 1)[-1])
    return names


# ---------- scope and waivers ----------


def load_scope(report: Report, known: list[str]) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Return (test id -> WP, WP -> status, WP -> title)."""
    return _load_scope_text(report, known, SCOPE_FILE.read_text(encoding='utf-8'))


def _load_scope_text(
    report: Report, known: list[str], text: str
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    raw = tomllib.loads(text)
    statuses: dict[str, str] = {}
    titles: dict[str, str] = {}
    patterns: list[tuple[str, str]] = []
    for wp, entry in raw.items():
        if wp not in WORK_PACKAGES:
            report.error(
                f'scope.toml: {wp} is not a work package of the plan ({WORK_PACKAGES[0]}..{WORK_PACKAGES[-1]})'
            )
        status = entry.get('status', 'pending')
        if status not in STATUSES:
            report.error(f'scope.toml: {wp} has status {status!r}; use one of {", ".join(STATUSES)}')
        statuses[wp] = status
        titles[wp] = entry.get('title', '')
        for pattern in entry.get('tests', []):
            if not any(_matches(pattern, test_id) for test_id in known):
                report.error(f'scope.toml: {wp} pattern {pattern!r} matches no Python test')
            patterns.append((wp, pattern))

    assignment: dict[str, str] = {}
    for test_id in known:
        matches = [(_specificity(pattern), wp, pattern) for wp, pattern in patterns if _matches(pattern, test_id)]
        if not matches:
            report.error(f'scope.toml: {test_id} is not assigned to any work package')
            continue
        best = max(score for score, _, _ in matches)
        owners = sorted({wp for score, wp, _ in matches if score == best})
        if len(owners) > 1:
            report.error(f'scope.toml: {test_id} is assigned to {", ".join(owners)} with equally specific patterns')
            continue
        assignment[test_id] = owners[0]
    return assignment, statuses, titles


def _matches(pattern: str, test_id: str) -> bool:
    if '::' not in pattern:
        return test_id.split('::', 1)[0] == pattern
    return fnmatch.fnmatchcase(test_id, pattern)


def _specificity(pattern: str) -> int:
    # A whole file < a wildcard inside it < one exact test.
    if '::' not in pattern:
        return 0
    return 1 if any(char in pattern for char in '*?[') else 2


def check_against_base(
    report: Report, base_scope: Path, known: list[str], assignment: dict[str, str], statuses: dict[str, str]
) -> None:
    """A finished package stays finished: its status and its tests cannot move later."""
    base_report = Report()
    base_assignment, base_statuses, _ = _load_scope_text(base_report, known, base_scope.read_text(encoding='utf-8'))
    for wp, status in base_statuses.items():
        if status == 'done' and statuses.get(wp) != 'done':
            report.error(f'scope.toml: {wp} was done on the base branch and cannot be reopened')
    for test_id, base_wp in base_assignment.items():
        if base_statuses.get(base_wp) == 'done' and assignment.get(test_id) != base_wp:
            moved_to = assignment.get(test_id)
            report.error(
                f'scope.toml: {test_id} belonged to {base_wp}, done on the base branch; it cannot move to {moved_to}'
            )


def load_waivers(report: Report, known: set[str]) -> dict[str, str]:
    raw = tomllib.loads(WAIVERS_FILE.read_text(encoding='utf-8'))
    waivers: dict[str, str] = {}
    for index, entry in enumerate(raw.get('waiver', []), start=1):
        test_id = entry.get('test', '')
        reason = ' '.join(str(entry.get('reason', '')).split())
        if test_id not in known:
            report.error(f'waivers.toml #{index}: {test_id!r} is not a Python test')
        if len(reason) < MIN_REASON_CHARS:
            report.error(f'waivers.toml #{index}: {test_id} needs a reason of at least {MIN_REASON_CHARS} characters')
        if test_id in waivers:
            report.error(f'waivers.toml #{index}: {test_id} is waived twice')
        waivers[test_id] = reason
    return waivers


# ---------- report ----------


@dataclass(slots=True)
class Parity:
    known: list[str]
    cases: Counter[str]
    claims: dict[str, list[RustTest]]
    waivers: dict[str, str]
    assignment: dict[str, str]
    statuses: dict[str, str]
    titles: dict[str, str]

    def state(self, test_id: str) -> str:
        if test_id in self.claims:
            return 'ported'
        if test_id in self.waivers:
            return 'waived'
        return 'pending'

    def counts(self, ids: list[str]) -> dict[str, int]:
        counts = {'ported': 0, 'waived': 0, 'pending': 0}
        for test_id in ids:
            counts[self.state(test_id)] += 1
        return counts

    def case_counts(self, ids: list[str]) -> dict[str, int]:
        counts = {'ported': 0, 'waived': 0, 'pending': 0}
        for test_id in ids:
            counts[self.state(test_id)] += self.cases[test_id]
        return counts


def render(parity: Parity) -> str:
    by_wp: dict[str, list[str]] = defaultdict(list)
    by_file: dict[str, list[str]] = defaultdict(list)
    for test_id in parity.known:
        by_wp[parity.assignment.get(test_id, '?')].append(test_id)
        by_file[test_id.split('::', 1)[0]].append(test_id)
    overall = parity.counts(parity.known)
    cases = parity.case_counts(parity.known)
    total_cases = sum(parity.cases[test_id] for test_id in parity.known)

    lines = [
        '# Test parity',
        '',
        '<!-- Generated by tools/parity/parity.py --write. Do not edit by hand; CI fails if it is stale. -->',
        '',
        (
            f'{len(parity.known)} Python test functions at the baseline: {overall["ported"]} ported, '
            f'{overall["waived"]} waived, {overall["pending"]} pending.'
        ),
        (
            f'{total_cases} cases as pytest collects them: {cases["ported"]} ported, {cases["waived"]} waived, '
            f'{cases["pending"]} pending.'
        ),
        '',
        'A test is *ported* when a Rust test carries `// py: <id>` above it (with `cases=N` for a',
        "parametrized test, adding up to pytest's count), *waived* when `tools/parity/waivers.toml`",
        'gives a reason, and *pending* otherwise. Scope per work package comes from',
        '`tools/parity/scope.toml`; a package marked done may have nothing pending.',
        '',
        '## By work package',
        '',
        '| WP | Title | Status | Tests | Cases | Ported | Waived | Pending |',
        '| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |',
    ]
    for wp in sorted(parity.statuses, key=_wp_order):
        ids = by_wp.get(wp, [])
        counts = parity.counts(ids)
        lines.append(
            f'| {wp} | {parity.titles.get(wp, "")} | {parity.statuses[wp]} | {len(ids)} '
            f'| {sum(parity.cases[test_id] for test_id in ids)} '
            f'| {counts["ported"]} | {counts["waived"]} | {counts["pending"]} |'
        )
    lines += [
        '',
        '## By Python test file',
        '',
        '| File | WP | Tests | Cases | Ported | Waived | Pending |',
        '| --- | --- | ---: | ---: | ---: | ---: | ---: |',
    ]
    for path, ids in sorted(by_file.items()):
        owners = ', '.join(sorted({parity.assignment.get(test_id, '?') for test_id in ids}, key=_wp_order))
        counts = parity.counts(ids)
        lines.append(
            f'| `{path}` | {owners} | {len(ids)} | {sum(parity.cases[test_id] for test_id in ids)} '
            f'| {counts["ported"]} | {counts["waived"]} | {counts["pending"]} |'
        )
    lines += ['', '## Ported and waived tests', '']
    done = [test_id for test_id in parity.known if parity.state(test_id) != 'pending']
    if not done:
        lines.append('None yet.')
    else:
        lines += ['| Python test | WP | State | Rust test or reason |', '| --- | --- | --- | --- |']
        for test_id in done:
            owner = parity.assignment.get(test_id, '?')
            lines.append(f'| `{test_id}` | {owner} | {parity.state(test_id)} | {_where(parity, test_id)} |')
    return '\n'.join(lines) + '\n'


def _where(parity: Parity, test_id: str) -> str:
    if test_id in parity.claims:
        unique = sorted({(claim.path, claim.name) for claim in parity.claims[test_id]})
        return '<br>'.join(f'`{path}` `{name}`' for path, name in unique)
    return parity.waivers[test_id].replace('|', '\\|')


def _wp_order(wp: str) -> tuple[int, str]:
    digits = ''.join(char for char in wp if char.isdigit())
    return (int(digits) if digits else 10_000, wp)


# ---------- entry point ----------


def _cross_check_pytest(report: Report, known: set[str], cases: Counter[str]) -> None:
    collected = set(cases)
    for missing in sorted(collected - known):
        report.error(f'pytest collects {missing}, which the AST scan missed')
    for extra in sorted(known - collected):
        report.error(f'AST scan found {extra}, which pytest does not collect')


def _check_baseline_counts(report: Report, known: list[str], cases: Counter[str]) -> None:
    total = sum(cases[test_id] for test_id in known)
    if EXPECTED_FUNCTIONS is not None and len(known) != EXPECTED_FUNCTIONS:
        report.error(f'the baseline has {EXPECTED_FUNCTIONS} Python test functions, this tree {len(known)}')
    if EXPECTED_CASES is not None and total != EXPECTED_CASES:
        report.error(f'the baseline has {EXPECTED_CASES} Python test cases, pytest collects {total}')


def _check_claims(report: Report, claims: dict[str, list[RustTest]], cases: Counter[str], listing: Path | None) -> None:
    for test_id, rust in sorted(claims.items()):
        if test_id not in cases:
            for claim in rust:
                report.error(f'{claim.path}:{claim.line}: `// py: {test_id}` names no Python test')
            continue
        want = cases[test_id]
        first = rust[0]
        if want == 1:
            for claim in rust:
                if claim.cases not in (None, 1):
                    report.error(
                        f'{claim.path}:{claim.line}: {test_id} is not parametrized; drop `cases={claim.cases}`'
                    )
            continue
        unnumbered = [claim for claim in rust if claim.cases is None]
        for claim in unnumbered:
            report.error(f'{claim.path}:{claim.line}: {test_id} has {want} cases; claim it with `cases=N`')
        have = sum(claim.cases or 0 for claim in rust)
        if not unnumbered and have != want:
            report.error(f'{first.path}:{first.line}: {test_id} has {want} cases, its claims cover {have}')
    if listing is None:
        return
    compiled = rust_test_names(listing)
    for rust in claims.values():
        for claim in rust:
            if claim.name not in compiled:
                message = f'{claim.name} carries a `// py:` claim but cargo does not list it as a test'
                report.error(f'{claim.path}:{claim.line}: {message}')


def collect(
    report: Report, *, pytest_listing: Path, rust_listing: Path | None, base_scope: Path | None = None
) -> Parity:
    known = python_tests()
    known_set = set(known)
    if len(known_set) != len(known):
        report.error('duplicate Python test ids (a test function defined twice in one file?)')
    cases = pytest_cases(pytest_listing)
    _cross_check_pytest(report, known_set, cases)
    _check_baseline_counts(report, known, cases)

    claims = rust_annotations(report)
    _check_claims(report, claims, cases, rust_listing)

    waivers = load_waivers(report, known_set)
    for test_id in sorted(set(waivers) & set(claims)):
        report.error(f'{test_id} is both ported and waived; drop the waiver')

    assignment, statuses, titles = load_scope(report, known)
    if base_scope is not None:
        check_against_base(report, base_scope, known, assignment, statuses)
    parity = Parity(known, cases, claims, waivers, assignment, statuses, titles)
    for test_id, wp in assignment.items():
        if statuses.get(wp) == 'done' and parity.state(test_id) == 'pending':
            report.error(f'{wp} is done but {test_id} is neither ported nor waived')
    if statuses.get(FINAL_WP) == 'done':
        for test_id in known:
            if parity.state(test_id) == 'pending':
                report.error(f'{FINAL_WP} (the parity audit) is done but {test_id} is still pending')
    return parity


def run(*, write: bool, pytest_listing: Path, rust_listing: Path | None, base_scope: Path | None = None) -> int:
    report = Report()
    parity = collect(report, pytest_listing=pytest_listing, rust_listing=rust_listing, base_scope=base_scope)
    text = render(parity)
    if write:
        PARITY_MD.parent.mkdir(parents=True, exist_ok=True)
        PARITY_MD.write_text(text, encoding='utf-8')
        print(f'wrote {PARITY_MD.relative_to(REPO_ROOT)}')
    elif not PARITY_MD.exists() or PARITY_MD.read_text(encoding='utf-8') != text:
        report.error(f'{PARITY_MD.relative_to(REPO_ROOT)} is stale; run tools/parity/parity.py --write')

    for message in report.errors:
        print(f'error: {message}')
    counts = parity.counts(parity.known)
    cases = parity.case_counts(parity.known)
    print(
        f'{len(parity.known)} Python tests: {counts["ported"]} ported, {counts["waived"]} waived, '
        f'{counts["pending"]} pending; {sum(cases.values())} cases: {cases["ported"]} ported, '
        f'{cases["waived"]} waived, {cases["pending"]} pending'
    )
    return 1 if report.errors else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n', 1)[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--check', action='store_true', help='validate and fail on any problem or a stale PARITY.md')
    mode.add_argument('--write', action='store_true', help='validate and regenerate PARITY.md')
    parser.add_argument(
        '--pytest-collected', type=Path, required=True, help='output of `pytest --collect-only -q`: the case counts'
    )
    parser.add_argument(
        '--rust-tests', type=Path, help='output of `cargo test -- --list --format terse`, to check claims compile'
    )
    parser.add_argument(
        '--base-scope',
        type=Path,
        help="the base branch's scope.toml; packages done there must stay done and keep their tests",
    )
    args = parser.parse_args()
    return run(
        write=args.write, pytest_listing=args.pytest_collected, rust_listing=args.rust_tests, base_scope=args.base_scope
    )


if __name__ == '__main__':
    sys.exit(main())
