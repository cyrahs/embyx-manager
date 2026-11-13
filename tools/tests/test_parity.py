"""Self-tests for tools/parity/parity.py, the guard against silently dropped tests (adapted from fav)."""

from __future__ import annotations

from pathlib import Path

import pytest

import parity


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    (tmp_path / 'tests').mkdir()
    (tmp_path / 'tools' / 'parity').mkdir(parents=True)
    (tmp_path / 'docs' / 'rewrite').mkdir(parents=True)
    (tmp_path / 'tests' / 'test_alpha.py').write_text(
        'def test_one():\n    pass\n\nasync def test_two():\n    pass\n\ndef helper():\n    pass\n\n'
        'class TestGroup:\n    def test_three(self):\n        pass\n\n    def not_a_test(self):\n        pass\n',
        encoding='utf-8',
    )
    (tmp_path / 'tests' / 'test_beta.py').write_text('def test_four():\n    pass\n', encoding='utf-8')
    (tmp_path / 'tests' / 'helpers.py').write_text('def test_ignored():\n    pass\n', encoding='utf-8')
    monkeypatch.setattr(parity, 'REPO_ROOT', tmp_path)
    monkeypatch.setattr(parity, 'RUST_DIR', tmp_path / 'crates')
    monkeypatch.setattr(parity, 'TESTS_DIR', tmp_path / 'tests')
    monkeypatch.setattr(parity, 'SCOPE_FILE', tmp_path / 'tools' / 'parity' / 'scope.toml')
    monkeypatch.setattr(parity, 'WAIVERS_FILE', tmp_path / 'tools' / 'parity' / 'waivers.toml')
    monkeypatch.setattr(parity, 'PARITY_MD', tmp_path / 'docs' / 'rewrite' / 'PARITY.md')
    monkeypatch.setattr(parity, 'EXPECTED_FUNCTIONS', None)
    monkeypatch.setattr(parity, 'EXPECTED_CASES', None)
    _scope(tmp_path, alpha='pending', beta='pending')
    (tmp_path / 'tools' / 'parity' / 'waivers.toml').write_text('', encoding='utf-8')
    _listing(tmp_path, COLLECTED)
    return tmp_path


COLLECTED = (
    'tests/test_alpha.py::test_one[a]\ntests/test_alpha.py::test_one[b]\ntests/test_alpha.py::test_one[c]\n'
    'tests/test_alpha.py::test_two\ntests/test_alpha.py::TestGroup::test_three\ntests/test_beta.py::test_four\n'
    '\n6 tests collected in 0.1s\n'
)


def _listing(root: Path, text: str) -> Path:
    path = root / 'collected.txt'
    path.write_text(text, encoding='utf-8')
    return path


def _scope(root: Path, *, alpha: str, beta: str, extra: str = '') -> None:
    (root / 'tools' / 'parity' / 'scope.toml').write_text(
        f'[WP1]\nstatus = "{alpha}"\ntests = ["tests/test_alpha.py"]\n\n[WP2]\nstatus = "{beta}"\ntests = ["tests/test_beta.py"{extra}]\n',
        encoding='utf-8',
    )


def _rust(root: Path, text: str) -> None:
    path = root / 'crates' / 'demo' / 'src' / 'lib.rs'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding='utf-8')


def _collect(root: Path, **kwargs: Path) -> tuple[parity.Parity, list[str]]:
    report = parity.Report()
    listing = kwargs.get('pytest_listing', root / 'collected.txt')
    result = parity.collect(report, pytest_listing=listing, rust_listing=kwargs.get('rust_listing'))
    return result, report.errors


def test_collects_functions_async_functions_and_class_methods_like_pytest(repo: Path) -> None:
    assert parity.python_tests() == [
        'tests/test_alpha.py::test_one',
        'tests/test_alpha.py::test_two',
        'tests/test_alpha.py::TestGroup::test_three',
        'tests/test_beta.py::test_four',
    ]


def test_claims_attach_through_stacked_comments_and_attributes(repo: Path) -> None:
    _rust(
        repo,
        '#[cfg(test)]\nmod tests {\n'
        '    // py: tests/test_alpha.py::test_one cases=3\n'
        '    // py: tests/test_alpha.py::test_two\n'
        '    #[tokio::test(flavor = "multi_thread")]\n'
        '    // the second claim covers the async variant\n'
        '    async fn both() {}\n\n'
        '    // py: tests/test_beta.py::test_four\n'
        '    #[test]\n'
        '    #[should_panic]\n'
        '    fn four() {}\n'
        '}\n',
    )
    result, errors = _collect(repo)
    assert errors == []
    assert {test_id: [claim.name for claim in claims] for test_id, claims in result.claims.items()} == {
        'tests/test_alpha.py::test_one': ['both'],
        'tests/test_alpha.py::test_two': ['both'],
        'tests/test_beta.py::test_four': ['four'],
    }
    assert result.state('tests/test_alpha.py::TestGroup::test_three') == 'pending'


def test_a_claim_on_a_non_test_function_is_rejected(repo: Path) -> None:
    _rust(repo, '// py: tests/test_alpha.py::test_one\nfn helper() {}\n')
    result, errors = _collect(repo)
    assert result.claims == {}
    assert any('must sit directly above a test function' in error for error in errors)


def test_a_claim_naming_a_missing_python_test_is_rejected(repo: Path) -> None:
    _rust(repo, '// py: tests/test_alpha.py::test_invented\n#[test]\nfn invented() {}\n')
    _, errors = _collect(repo)
    assert any('names no Python test' in error for error in errors)


def test_a_claim_on_a_test_cargo_does_not_compile_is_rejected(repo: Path) -> None:
    _rust(repo, '// py: tests/test_alpha.py::test_two\n#[test]\nfn orphan() {}\n')
    listing = repo / 'cargo-tests.txt'
    listing.write_text('demo::tests::other: test\n', encoding='utf-8')
    _, errors = _collect(repo, rust_listing=listing)
    assert any('cargo does not list it' in error for error in errors)
    listing.write_text('demo::tests::orphan: test\n', encoding='utf-8')
    _, errors = _collect(repo, rust_listing=listing)
    assert errors == []


def test_waivers_need_a_real_test_and_a_reason(repo: Path) -> None:
    (repo / 'tools' / 'parity' / 'waivers.toml').write_text(
        '[[waiver]]\ntest = "tests/test_alpha.py::test_one"\nreason = "short"\n\n'
        '[[waiver]]\ntest = "tests/test_alpha.py::test_nope"\nreason = "The behaviour does not exist in the Rust build at all."\n',
        encoding='utf-8',
    )
    _, errors = _collect(repo)
    assert any('needs a reason' in error for error in errors)
    assert any('is not a Python test' in error for error in errors)


def test_a_done_package_may_have_nothing_pending(repo: Path) -> None:
    _scope(repo, alpha='pending', beta='done')
    _, errors = _collect(repo)
    assert errors == ['WP2 is done but tests/test_beta.py::test_four is neither ported nor waived']

    (repo / 'tools' / 'parity' / 'waivers.toml').write_text(
        '[[waiver]]\ntest = "tests/test_beta.py::test_four"\nreason = "Covers a code path the Rust build does not have."\n',
        encoding='utf-8',
    )
    result, errors = _collect(repo)
    assert errors == []
    assert result.state('tests/test_beta.py::test_four') == 'waived'


def test_a_specific_pattern_moves_a_test_to_another_package(repo: Path) -> None:
    _scope(repo, alpha='pending', beta='pending', extra=', "tests/test_alpha.py::test_t*"')
    result, errors = _collect(repo)
    assert errors == []
    assert result.assignment['tests/test_alpha.py::test_one'] == 'WP1'
    assert result.assignment['tests/test_alpha.py::test_two'] == 'WP2'
    assert result.assignment['tests/test_alpha.py::TestGroup::test_three'] == 'WP1'


def test_every_test_needs_exactly_one_owner(repo: Path) -> None:
    (repo / 'tools' / 'parity' / 'scope.toml').write_text(
        '[WP1]\ntests = ["tests/test_alpha.py"]\n\n[WP2]\ntests = ["tests/test_alpha.py"]\n', encoding='utf-8'
    )
    _, errors = _collect(repo)
    assert any('tests/test_beta.py::test_four is not assigned' in error for error in errors)
    assert any('equally specific' in error for error in errors)


def test_pytest_cross_check_reports_both_directions(repo: Path) -> None:
    listing = _listing(
        repo,
        'tests/test_alpha.py::test_one[case-a]\ntests/test_alpha.py::test_one[case-b]\ntests/test_alpha.py::test_two\n'
        'tests/test_alpha.py::TestGroup::test_three\ntests/test_gamma.py::test_dynamic\n\n5 tests collected in 0.1s\n',
    )
    _, errors = _collect(repo, pytest_listing=listing)
    assert 'pytest collects tests/test_gamma.py::test_dynamic, which the AST scan missed' in errors
    assert 'AST scan found tests/test_beta.py::test_four, which pytest does not collect' in errors


def test_parametrized_claims_must_add_up_to_pytests_cases(repo: Path) -> None:
    _rust(repo, '// py: tests/test_alpha.py::test_one\n#[test]\nfn one() {}\n')
    _, errors = _collect(repo)
    assert any('has 3 cases; claim it with `cases=N`' in error for error in errors)

    _rust(repo, '// py: tests/test_alpha.py::test_one cases=2\n#[test]\nfn one() {}\n')
    _, errors = _collect(repo)
    assert any('has 3 cases, its claims cover 2' in error for error in errors)

    _rust(
        repo,
        '// py: tests/test_alpha.py::test_one cases=2\n#[test]\nfn one() {}\n\n'
        '// py: tests/test_alpha.py::test_one cases=1\n#[test]\nfn one_more() {}\n',
    )
    result, errors = _collect(repo)
    assert errors == []
    assert result.case_counts(result.known) == {'ported': 3, 'waived': 0, 'pending': 3}


def test_a_case_count_on_a_plain_test_and_a_malformed_claim_are_rejected(repo: Path) -> None:
    _rust(
        repo,
        '// py: tests/test_alpha.py::test_two cases=4\n#[test]\nfn two() {}\n\n// py: tests/test_alpha.py::test_two cases=two\n#[test]\nfn bad() {}\n',
    )
    _, errors = _collect(repo)
    assert any('is not parametrized; drop `cases=4`' in error for error in errors)
    assert any('malformed claim' in error for error in errors)


def test_the_baseline_counts_are_asserted(repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(parity, 'EXPECTED_FUNCTIONS', 4)
    monkeypatch.setattr(parity, 'EXPECTED_CASES', 6)
    _, errors = _collect(repo)
    assert errors == []
    monkeypatch.setattr(parity, 'EXPECTED_CASES', 7)
    _, errors = _collect(repo)
    assert errors == ['the baseline has 7 Python test cases, pytest collects 6']


def test_parity_md_must_be_regenerated(repo: Path) -> None:
    listing = repo / 'collected.txt'
    assert parity.run(write=False, pytest_listing=listing, rust_listing=None) == 1
    assert parity.run(write=True, pytest_listing=listing, rust_listing=None) == 0
    assert parity.run(write=False, pytest_listing=listing, rust_listing=None) == 0
    text = (repo / 'docs' / 'rewrite' / 'PARITY.md').read_text(encoding='utf-8')
    assert '4 Python test functions at the baseline: 0 ported, 0 waived, 4 pending.' in text
    assert '6 cases as pytest collects them: 0 ported, 0 waived, 6 pending.' in text


def test_tests_cannot_be_parked_in_an_invented_package(repo: Path) -> None:
    _scope(repo, alpha='pending', beta='pending', extra=']\n\n[WP99]\ntests = ["tests/test_beta.py::test_four"')
    _, errors = _collect(repo)
    assert any('WP99 is not a work package of the plan' in error for error in errors)


def test_a_package_done_on_the_base_branch_keeps_its_status_and_tests(repo: Path) -> None:
    base = repo / 'base-scope.toml'
    base.write_text(
        '[WP1]\nstatus = "pending"\ntests = ["tests/test_alpha.py"]\n\n[WP2]\nstatus = "done"\ntests = ["tests/test_beta.py"]\n',
        encoding='utf-8',
    )
    (repo / 'tools' / 'parity' / 'waivers.toml').write_text(
        '[[waiver]]\ntest = "tests/test_beta.py::test_four"\nreason = "Covers a code path the Rust build does not have."\n',
        encoding='utf-8',
    )
    _scope(repo, alpha='pending', beta='done')
    report = parity.Report()
    parity.collect(report, pytest_listing=repo / 'collected.txt', rust_listing=None, base_scope=base)
    assert report.errors == []

    _scope(repo, alpha='pending', beta='pending')
    report = parity.Report()
    parity.collect(report, pytest_listing=repo / 'collected.txt', rust_listing=None, base_scope=base)
    assert any('WP2 was done on the base branch and cannot be reopened' in error for error in report.errors)

    (repo / 'tools' / 'parity' / 'scope.toml').write_text(
        '[WP1]\nstatus = "pending"\ntests = ["tests/test_alpha.py", "tests/test_beta.py::test_four"]\n\n'
        '[WP2]\nstatus = "done"\ntests = ["tests/test_beta.py"]\n',
        encoding='utf-8',
    )
    report = parity.Report()
    parity.collect(report, pytest_listing=repo / 'collected.txt', rust_listing=None, base_scope=base)
    assert any('belonged to WP2, done on the base branch; it cannot move to WP1' in error for error in report.errors)


def test_the_final_audit_requires_nothing_pending_anywhere(repo: Path) -> None:
    (repo / 'tools' / 'parity' / 'scope.toml').write_text(
        '[WP1]\nstatus = "pending"\ntests = ["tests/test_alpha.py"]\n\n[WP16]\nstatus = "done"\ntests = ["tests/test_beta.py"]\n',
        encoding='utf-8',
    )
    (repo / 'tools' / 'parity' / 'waivers.toml').write_text(
        '[[waiver]]\ntest = "tests/test_beta.py::test_four"\nreason = "Covers a code path the Rust build does not have."\n',
        encoding='utf-8',
    )
    _, errors = _collect(repo)
    assert sum('the parity audit) is done but' in error for error in errors) == 3
