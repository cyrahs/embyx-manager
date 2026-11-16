#!/usr/bin/env bash
# Repository rules for the Rust build that no compiler checks (docs/CONVENTIONS.md):
#   1. The Python tree is the reference: every file that exists at the baseline commit in
#      BASELINE is byte-identical to it, except `.gitignore`, which may only gain lines at
#      its end. No new file goes under the Python or React tree (src/, tests/, frontend/).
#   2. No ignored Rust tests: a test that cannot run is ported differently or waived.
#   3. `unsafe` is denied by the workspace lints, which every crate must use; the only place
#      allowed to lift that is the home worker's process-control module (WP15) and its probe.
#   4. `.github/` holds the baseline's files plus `rust.yml`, which is checks-only (it keeps
#      `contents: read` and asks for no write permission), and `rust-image.yml`, which
#      publishes the Rust image for the deployment trial: it runs only on pushes to `rust`,
#      may write packages and nothing else, and never writes the `latest` tag.
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

# Byte-exact prefix, the baseline's final newline included, so its last line cannot change.
baseline_ignore_size="$(git cat-file -s "${baseline}:.gitignore")"
if ! cmp -s -n "${baseline_ignore_size}" <(git show "${baseline}:.gitignore") .gitignore \
  || [[ "$(stat -c %s .gitignore)" -lt "${baseline_ignore_size}" ]]; then
  echo "error: .gitignore may only gain lines after the baseline's content"
  status=1
fi

added="$(git diff --name-only --diff-filter=A "${baseline}" -- src tests frontend)"
if [[ -n "${added}" ]]; then
  echo "error: new files under the Python or React tree:"
  sed 's/^/  /' <<< "${added}"
  status=1
fi

github_files="$(git ls-files .github | sort)"
expected_github="$( (git ls-tree -r --name-only "${baseline}" -- .github; echo .github/workflows/rust.yml;
  echo .github/workflows/rust-image.yml) | sort)"
if [[ "${github_files}" != "${expected_github}" ]]; then
  echo "error: .github/ may hold only the baseline's files, rust.yml and rust-image.yml:"
  diff <(echo "${expected_github}") <(echo "${github_files}") | sed 's/^/  /' || true
  status=1
fi
if [[ -f .github/workflows/rust.yml ]]; then
  if grep -nE ':[[:space:]]*write\b|write-all' .github/workflows/rust.yml; then
    echo "error: rust.yml must not ask for a write permission"
    status=1
  fi
  if ! grep -qE '^[[:space:]]*contents:[[:space:]]*read[[:space:]]*$' .github/workflows/rust.yml; then
    echo "error: rust.yml must keep permissions contents: read"
    status=1
  fi
fi
if [[ -f .github/workflows/rust-image.yml ]]; then
  # The one workflow that may publish. The rules read the whole file, comments included, so
  # nothing can hide behind a `#`: its comments avoid the words the rules look for.
  image_workflow=.github/workflows/rust-image.yml
  block() {
    perl -0777 -ne 'print $1 if /^'"$1"':\n((?:[ \t]+.*\n|\n)+)/m' "${image_workflow}" | grep -vE '^[[:space:]]*$' || true
  }
  if [[ "$(grep -cE '^["'\'']?(on|true)["'\'']?[[:space:]]*:' "${image_workflow}")" != 1 ||
    "$(block on)" != $'  push:\n    branches: [ rust ]' ]]; then
    echo "error: rust-image.yml must run only on pushes to rust; its triggers are:"
    block on | sed 's/^/  /'
    status=1
  fi
  if [[ "$(grep -c 'permissions' "${image_workflow}")" != 1 ||
    "$(block permissions)" != $'  contents: read\n  packages: write' ||
    "$(grep -ciE 'write' "${image_workflow}")" != 1 ]]; then
    echo "error: rust-image.yml must have one permissions block, contents: read and packages: write, and no other mention of write"
    grep -niE 'permissions|write' "${image_workflow}" | sed 's/^/  /' || true
    status=1
  fi
  if grep -niE 'latest' "${image_workflow}"; then
    echo "error: rust-image.yml must not mention the latest tag in any spelling (Flux follows it)"
    status=1
  fi
  if [[ "$(grep -i 'secrets' "${image_workflow}")" != '        password: ${{ secrets.GITHUB_TOKEN }}' ]]; then
    echo "error: rust-image.yml may use no secret but GITHUB_TOKEN, once, as the registry password"
    status=1
  fi
  # A reference without a tag means the tag Flux follows, so the image may be named only as
  # "${IMAGE}:rust" or "${IMAGE}:rust-${GITHUB_SHA}", and nothing may add tags on its own.
  references="$(sed -E -e 's/^      IMAGE: ghcr\.io\/\$\{\{ github\.repository \}\}$//' -e 's/^        registry: ghcr\.io$//' \
    -e 's/"\$\{IMAGE\}:rust(-\$\{GITHUB_SHA\})?"//g' "${image_workflow}")"
  if grep -nE 'IMAGE|ghcr\.io|github\.repository' <<< "${references}" ||
    grep -niE 'all-tags|push[[:space:]]+-a\b|metadata-action|push:[[:space:]]*["'\'']?true|imagetools[[:space:]]+create' "${image_workflow}"; then
    echo "error: rust-image.yml may name the image only as \"\${IMAGE}:rust\" or \"\${IMAGE}:rust-\${GITHUB_SHA}\" and push it with docker push"
    status=1
  fi
  if ! grep -qE '^      run: tools/check_tree\.sh$' "${image_workflow}" || grep -nE 'continue-on-error|^[[:space:]]+if:' "${image_workflow}"; then
    echo "error: rust-image.yml must run tools/check_tree.sh, with no step allowed to fail or be skipped"
    status=1
  fi
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

# The attribute may span lines (rustfmt splits `#[allow(unsafe_code, reason = "...")]`), so
# match across them; the same goes for `forbid`-lifting `allow`/`expect` in any spelling.
unsafe_allowed='^crates/embyx-transcode-worker/src/(process(/[^:]*)?\.rs|probes\.rs):'
unsafe="$(find crates -name '*.rs' -print0 | xargs -0 perl -0777 -ne \
  'while (/(allow|expect)\s*\([^)]*\bunsafe_code\b/g) { my $line = 1 + (substr($_, 0, $-[0]) =~ tr/\n//); print "$ARGV:$line\n" }' \
  | grep -vE "${unsafe_allowed}" || true)"
if [[ -n "${unsafe}" ]]; then
  echo "error: unsafe code is allowed only in crates/embyx-transcode-worker/src/process*:"
  sed 's/^/  /' <<< "${unsafe}"
  status=1
fi

# A crate without `[lints] workspace = true` would lose `deny(unsafe_code)` and the rest.
for manifest in crates/*/Cargo.toml; do
  if ! perl -0777 -ne 'exit(/^\[lints\]\s*\nworkspace\s*=\s*true/m ? 0 : 1)' "${manifest}"; then
    echo "error: ${manifest} must have [lints] workspace = true"
    status=1
  fi
done

if [[ ${status} -eq 0 ]]; then
  echo "tree check passed: Python tree matches ${baseline:0:7}, no ignored Rust tests, unsafe confined"
fi
exit ${status}
