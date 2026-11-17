# Conventions

How embyx-manager is written and tested. The app was ported from Python to Rust in November 2026
(`docs/rewrite/`, the history). The Python app at that time (commit 5a3c083, tag `python-final`) is still
the reference for anything users, stored data or other services see: its outputs are frozen in `goldens/`,
and rolling back to its image must keep working on the same database.

## Ground rules

- Behaviour that users, stored data or other services see (API responses, file and folder names, database
  rows and their JSON bytes, CloudDrive / Kubernetes / Emby calls, status files, Telegram text) stays as
  Python has it, unless a change says otherwise on purpose. `DIVERGENCES.md` lists every place the Rust
  build differs and why; add an entry, with the next free number, when you differ on purpose.
- The cross-version contracts in `docs/rewrite/PLAN.md` hold while a rollback to the Python image
  (`python-final`) is possible: schema stays at version 19, config sections never gain or lose a key, a CloudDrive mutation that was sent is awaited to its end even when the caller is
  cancelled, stored JSON bytes equal Python's.
- Never point a test, a dev build or a probe at a production service: not the embyx database, CloudDrive,
  Emby, the cluster's Kubernetes API, or any path under `/mnt/cd2`, `/root/data/embyx`, `/upload`,
  `/downloads`. Tests use fakes, a local PostgreSQL and temporary directories. No test talks to the internet.
- Secrets (DSN, API token, CloudDrive token, Emby key, transcode token, WireGuard keys) never reach a log, a
  `Debug` output, the repository or a report.
- Code, comments and docs in English.

## Layout

```
Cargo.toml                 workspace: members, shared dependency versions, lints
rust-toolchain.toml        pinned toolchain
rustfmt.toml, clippy.toml  width 120; clippy thresholds
crates/embyx-core          Python-compatible helpers (pyjson, pyre, pyurl, pypath, pytext), goldens and
                           transcript support; from WP1 settings, errors, AVID, magnets
crates/embyx-db            pool, schema v19, advisory locks; test databases, scenario rows
crates/embyx-clients       HTTP client (httpx-shaped) and retries, subprocess runner, CloudDrive over
                           tonic (generated from proto/clouddrive.fds.bin, frozen: it was extracted from
                           the Python app's clouddrive_pb2.py), the fakes; from WP2 the site
                           clients
crates/embyx-monitor       monitor: acquisitions, archive, tracker, RSS, scheduler (WP5-WP9)
crates/embyx-fill-actor    fill actor (WP10-WP12)
crates/embyx-merge         merge service and worker (WP13, WP14)
crates/embyx-transcode-worker  home transcode worker; the only place `unsafe` may appear (WP15)
crates/embyx-api           axum routes (WP4 and later)
crates/embyx-manager       the binary: `serve`, `merge-worker`, `transcode-worker`
frontend/                  the React front end; its build is served from `static/` next to the binary
goldens/<area>/            outputs of the Python baseline, frozen (see Goldens)
goldens/scenarios/         effect transcripts (docs/rewrite/SCENARIOS.md)
tools/check_tree.sh        repository rules no compiler checks
tools/smoke/               page smoke: the built front end against the server in headless Chromium
tools/scenarios/specs/     the scenario specs the transcript tests run (seeds and steps)
docs/rewrite/              the rewrite's history: PLAN, PARITY, API-COVERAGE, SCENARIOS, DEPLOY, REPORT
```

A module ported from Python names the Python module in its doc comment
(`src/embyx_manager/monitor/archive.py`), and a ported test its Python test (`// py:`, below). Those paths,
and the `tools/...` Python tooling some comments name, are in the tree at tag `python-final` (the app) or
at commit b2ffd31 (the tooling, which ran against that same app), not in the current tree.

## Code

- Edition 2024, toolchain from `rust-toolchain.toml`. Dependencies go in `[workspace.dependencies]` and
  crates use `dep.workspace = true`; pin release candidates exactly (`=x.y.z-rc.n`).
- Lints are workspace-wide (`[workspace.lints]`: clippy pedantic, `unsafe_code` denied, `todo!`,
  `unimplemented!` and `dbg!` denied, `print_stdout`/`print_stderr` warned); CI runs clippy with
  `-D warnings`. `unsafe` is allowed only in `crates/embyx-transcode-worker/src/process*` and its probe, with
  `#[allow(unsafe_code, reason = "...")]` on the smallest item; `tools/check_tree.sh` enforces where. Every
  other `#[allow]` or `#[expect]` carries a `reason`.
- No placeholder bodies in merged code.
- Errors: `thiserror` enums in library crates, `anyhow` only in the binary and tests. Text that Python
  surfaces (API `detail`, task `error` columns, status files, Telegram) is copied verbatim.
- Python-compatible data: `embyx_core::pyjson` reads and writes JSON the way Python 3.13 does (`1` and `1.0`
  stay apart, `json.dumps` byte formats including `sort_keys` and `ensure_ascii`, decoder messages). Use it
  wherever bytes are stored or compared; never `serde_json::to_string` for stored data. `pytext` has Python's
  `isspace`/`isdigit`/`isdecimal`/`strip`/`split`; `pyurl` has `quote`/`unquote`/`urlsplit`/`urljoin`;
  `pypath` has `PurePosixPath` joins and parts; `pyre` compiles Python `re` patterns as written (D2).
- All external effects are injected: HTTP client, database pool, clock, CloudDrive client, Kubernetes
  client, subprocess runner, filesystem roots, sleeper. Retry and pacing sleeps go through
  `embyx_clients::net::retry::Sleeper`; tests pass `RecordingSleeper` and assert the delays.
- Advisory locks: a wait on `pg_advisory_lock` that is dropped keeps waiting on the server and takes the
  lock later; cancel it with `pg_cancel_backend` (probe in `embyx-db`, `API-COVERAGE.md`).
- Build and test with `CARGO_INCREMENTAL=0` (CI sets it).

## Tests

- A test ported from Python carries the Python test id on the line(s) directly above its test attribute:
  `// py: tests/test_avid.py::test_extracts_plain_id` (with `cases=N` where a parametrized test was split).
  The rewrite's parity check matched these against pytest's collection (`docs/rewrite/PARITY.md`, final
  state); it cannot run without the Python tree, so the comments are history now. Keep them on the tests
  they mark; new tests need none. Never weaken an assertion of a ported test: an exact string stays exact.
- Database tests run against real PostgreSQL, one temporary database per test, created through
  `EMBYX_MANAGER_TEST_DATABASE_URL` (`embyx_db::testdb::TestDatabase`, behind the `test-support` feature).
  A test that needs it and does not find it fails; it does not skip. Drop leftover `embyx_test_*`
  databases after a crashed run:
  `psql "$EMBYX_MANAGER_TEST_DATABASE_URL" -Atc "SELECT format('DROP DATABASE %I;', datname) FROM pg_database WHERE datname LIKE 'embyx\_test\_%'" | psql "$EMBYX_MANAGER_TEST_DATABASE_URL"`.
- HTTP is faked with `embyx_clients::net::fakeweb::FakeWeb` (a local proxy the client is pointed at:
  `Reply::status`/`bytes`/`json`, `Reply::Close` for a hang-up, `Reply::Stall` for a stall) or a local
  server; CloudDrive with `embyx_clients::clouddrive::fake::FakeCloudDrive` over a real socket. Write the
  transport-failure tests (hang-up, cut short, stall) for every fetch path up front.
- No `#[ignore]` (CI fails on any). Platform-specific tests use `#[cfg(...)]`.

## Goldens

`goldens/<area>/` holds deterministic outputs of the Python baseline, and `goldens/scenarios/` its effect
transcripts (`docs/rewrite/SCENARIOS.md`), for the specs in `tools/scenarios/specs/`. Rust tests read them
with `embyx_core::golden` (`golden::load("json/dumps.json")`) or `embyx_core::scenario` and compare.

They are frozen fixtures. The generators (`tools/goldens/generate.py`, `tools/scenarios/runner.py`) import
the Python app's own code, so they run only from commit b2ffd31, against the tree there (the app at tag
`python-final`), under the pinned Python 3.13.18. A golden the Rust build cannot meet is a bug, unless the
change is deliberate: then the PR updates that golden or transcript by hand, says so, and adds a
`DIVERGENCES.md` entry (the Python image would behave the old way after a rollback).

## Code copied from fav

The Python-compatible helpers and the HTTP stack were written for fav's Rust rewrite and copied here, not
depended on (no Git dependency on fav):

- Source: cyrahs/fav at commit `b1ed1601de2ed42a96bd2d7d31c1e0187508df98` (`main`, 2026-11-13).
- Copied: `fav-core` `pyjson`, `pytext`, `pyurl`, `pypath` (with `PyPath` from `validate.rs`), `golden`;
  `fav-sources` `pyre` and the fake web (`testutil.rs`, here `net/fakeweb.rs`); `fav-net` `lib.rs`,
  `client.rs` (httpx-shaped client), `httpx.rs` (error classes and texts), `retry.rs` (ladders and the
  recording sleeper), `proxy.rs`, `download.rs`, `subprocess.rs` (`httpx.rs`, `proxy.rs`, `download.rs` and `subprocess.rs`
  removed in WP16: nothing used them); `fav-db` `testdb.rs` (adapted). The rewrite's parity script and
  its self-tests were adapted from fav's tooling too (removed with the Python tree).
- Changes on copy: names and environment variables renamed to embyx's, `pyjson` moved to Python 3.13's
  decoder messages (trailing commas) and given the `sorted_compact_ascii` format; otherwise copied whole,
  unused parts included (PLAN.md, "Cleanup notes": WP16 removes what nothing calls).
  Each file's doc comment says where it came from; "fav's Dn" in a comment names fav's own
  `docs/DIVERGENCES.md`.
- Licence: fav has no licence file and its crates are `license = "UNLICENSED"`; it is a public repository
  of the same owner as this one, who decided the copy (plan, 2026-11-13). The copied code is distributed
  here under this repository's GPL-3.0. A later fix in fav is not picked up automatically: copy it by hand
  and name the fav commit.

The cron evaluator was not copied: `croner` meets the cron goldens with a validation pre-check
(`API-COVERAGE.md`).

## Before you push

```bash
tools/check_tree.sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
EMBYX_MANAGER_TEST_DATABASE_URL=postgresql://... CARGO_INCREMENTAL=0 cargo test --workspace --locked
(cd frontend && npm ci && npm run lint && npm test && npm run build)
```

and, for a change a page can see, the page smoke (`EMBYX_MANAGER_TEST_DATABASE_URL=... tools/smoke/run.sh`).
CI (`.github/workflows/ci.yml`) runs all of it on every pull request, plus the home worker's crate and
binary on Windows and the image build.

## Pull requests

Checks green; no `todo!()`, `unimplemented!()`, ignored tests or placeholder bodies; goldens and
transcripts changed only for a deliberate change, with its `DIVERGENCES.md` entry; schema, config keys and
the merge Job and home worker protocols unchanged unless the PR says how a rollback to `python-final` still
works (the cross-version contracts).
