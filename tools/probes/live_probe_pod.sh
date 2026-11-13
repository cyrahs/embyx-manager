#!/usr/bin/env bash
# The probe pod's script (WP0, approved once by Max on 2026-11-13; docs/rewrite/API-COVERAGE.md).
# Runs in a public rust image with no volumes, no secrets and no service-account token:
# fetches this repository at one commit, builds the Rust probe, installs the baseline's
# Python environment and runs tools/probes/live_fingerprint.py --live once.
#
#   live_probe_pod.sh <commit>
set -euo pipefail
commit="${1:?usage: live_probe_pod.sh <commit>}"

export DEBIAN_FRONTEND=noninteractive CARGO_INCREMENTAL=0 CARGO_TERM_COLOR=never UV_PYTHON=3.13.18
apt-get update -qq
apt-get install -y -qq --no-install-recommends cmake clang libclang-dev openssl git ca-certificates curl >/dev/null

mkdir -p /work && cd /work
git init -q .
git fetch -q --depth 1 https://github.com/cyrahs/embyx-manager.git "$commit"
git checkout -q FETCH_HEAD
echo "probe at $(git rev-parse HEAD)"

time cargo build -q --locked -j 2 -p embyx-clients --example fetch_probe

curl -LsSf https://astral.sh/uv/install.sh | env UV_NO_MODIFY_PATH=1 sh >/dev/null
export PATH="$HOME/.local/bin:$PATH"
uv sync -q --locked

uv run --with hpack==4.1.0 python tools/probes/live_fingerprint.py --live
