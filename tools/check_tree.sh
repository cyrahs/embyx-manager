#!/usr/bin/env bash
# Repository rules for the Rust build that no compiler checks (docs/CONVENTIONS.md):
#   1. The Python tree is the reference: every file that exists at the baseline commit in
#      BASELINE is byte-identical to it, except `.gitignore`, which may only gain lines at
#      its end. No new file goes under the Python or React tree (src/, tests/, frontend/).
#   2. No ignored Rust tests: a test that cannot run is ported differently or waived.
#   3. `unsafe` is denied by the workspace lints; the only place allowed to lift that is
#      the home worker's process-control module (WP15) and its probe.
# Needs the baseline commit in the local clone (CI checks out with full history).
# Adapted from fav (rust/tools/check_tree.sh).
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
# Tripwire: the baseline is fixed for the whole rewrite (docs/rewrite/PLAN.md). Changing
# it means editing this line too, which a reviewer cannot miss.
expected_baseline=5a3c083468e9626e9b79d59ed3afb24ba7e6b4ff
baseline="$(tr -d '[:space:]' < BASELINE)"
status=0

if [[ "${baseline}" != "${expected_baseline}" ]]; then
  echo "error: BASELINE is ${baseline}, but the agreed baseline is ${expected_baseline}"
  exit 1
fi

if ! git cat-file -e "${baseline}^{commit}" 2>/dev/null; then
  echo "error: baseline ${baseline} is not in this clone; fetch it first (git fetch origin ${baseline})"
  exit 1
fi

if ! git merge-base --is-ancestor "${baseline}" HEAD; then
  echo "error: the baseline ${baseline} is not an ancestor of HEAD"
  status=1
fi

# Modified, deleted, renamed or retyped files of the baseline (added files are fine here).
changed="$(git diff --name-only --diff-filter=MDRT "${baseline}" -- . ':(exclude).gitignore')"
if [[ -n "${changed}" ]]; then
  echo "error: files of the Python baseline ${baseline:0:7} were changed:"
  sed 's/^/  /' <<< "${changed}"
  status=1
fi

baseline_ignore="$(git show "${baseline}:.gitignore")"
if [[ "$(head -c "${#baseline_ignore}" .gitignore)" != "${baseline_ignore}" ]]; then
  echo "error: .gitignore may only gain lines after the baseline's content"
  status=1
fi

added="$(git diff --name-only --diff-filter=A "${baseline}" -- src tests frontend)"
if [[ -n "${added}" ]]; then
  echo "error: new files under the Python or React tree:"
  sed 's/^/  /' <<< "${added}"
  status=1
fi

if [[ -e rust ]]; then
  echo "error: the workspace lives at the repository root; there is no rust/ folder"
  status=1
fi

# `#[ignore]`, `#[ignore = "..."]` and `#[cfg_attr(..., ignore)]` on one line. CI also
# fails when `cargo test` reports any ignored test, which catches multi-line spellings.
ignored="$(grep -rnE '#!?\[[^]]*\bignore\b' --include='*.rs' crates/ || true)"
if [[ -n "${ignored}" ]]; then
  echo "error: ignored tests are not allowed:"
  sed 's/^/  /' <<< "${ignored}"
  status=1
fi

unsafe_allowed='^crates/embyx-transcode-worker/src/(process(/[^:]*)?\.rs|probes\.rs):'
unsafe="$(grep -rnE '(allow|expect)\([^)]*\bunsafe_code\b' --include='*.rs' crates/ | grep -vE "${unsafe_allowed}" || true)"
if [[ -n "${unsafe}" ]]; then
  echo "error: unsafe code is allowed only in crates/embyx-transcode-worker/src/process*:"
  sed 's/^/  /' <<< "${unsafe}"
  status=1
fi

if [[ ${status} -eq 0 ]]; then
  echo "tree check passed: Python tree matches ${baseline:0:7}, no ignored Rust tests, unsafe confined"
fi
exit ${status}
