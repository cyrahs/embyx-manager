#!/usr/bin/env bash
# Repository rules that no compiler checks (docs/CONVENTIONS.md); CI runs this first:
#   1. No ignored tests: a test that cannot run is written differently or removed.
#   2. `unsafe` is denied by the workspace lints, which every crate must use; the only place
#      allowed to lift that is the home worker's process-control module and its probe.
# (Until the Python tree left the repository, two more rules kept it byte-identical to the
# baseline and held the Rust workflows to read-only; see the history before that change.)
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
status=0

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
  echo "tree check passed: no ignored tests, unsafe confined, every crate on the workspace lints"
fi
exit ${status}
