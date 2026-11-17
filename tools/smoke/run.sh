#!/usr/bin/env bash
# Page smoke (WP4): build the unmodified front end in a copy of frontend/ (its build writes
# beside the Python package, so never in the checkout), build the server, make a scratch
# database on the server EMBYX_MANAGER_TEST_DATABASE_URL names, and run smoke.mjs.
#
# Needs node and npm, and a Chromium for the pinned Playwright: CI installs one with
# `npx playwright install --with-deps chromium` in tools/smoke; a machine that already has
# one sets PLAYWRIGHT_BROWSERS_PATH. SMOKE_SCREENSHOTS keeps a screenshot of the settings.
set -euo pipefail

repo=$(cd "$(dirname "$0")/../.." && pwd)
server_url=${EMBYX_MANAGER_TEST_DATABASE_URL:?EMBYX_MANAGER_TEST_DATABASE_URL must name a disposable PostgreSQL server}
database="embyx_test_smoke_$$"
database_url="${server_url%%\?*}"
database_url="${database_url%/*}/${database}"
work=$(mktemp -d)

cleanup() {
  psql "$server_url" -qc "DROP DATABASE IF EXISTS \"${database}\" WITH (FORCE)" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

echo "== front end (npm ci, npm run build) in a copy"
tar -C "$repo" --exclude=frontend/node_modules --exclude=frontend/dist -cf - frontend | tar -C "$work" -xf -
(cd "$work/frontend" && npm ci --no-audit --no-fund && npm run build)
built="$work/src/embyx_manager/static"
test -f "$built/index.html"

echo "== server binary"
(cd "$repo" && cargo build -p embyx-manager --locked)

echo "== smoke dependencies"
(cd "$repo/tools/smoke" && npm ci --no-audit --no-fund)

echo "== scratch database ${database}"
psql "$server_url" -qc "CREATE DATABASE \"${database}\""

echo "== smoke"
SMOKE_BINARY="$repo/target/debug/embyx-manager" \
SMOKE_STATIC="$built" \
SMOKE_DATABASE_URL="$database_url" \
  node "$repo/tools/smoke/smoke.mjs"
