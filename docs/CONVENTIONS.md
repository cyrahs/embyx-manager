# Conventions (Rust rewrite)

How the Rust build on branch `rust` is written and tested while it is ported from the Python app
(`docs/rewrite/PLAN.md`). The Python tree at the baseline (`BASELINE`) is the reference: it stays
byte-identical on `rust` and generates every golden and transcript.

## Ground rules

- Behaviour that users, stored data or other services see (API responses, file and folder names, database
  rows and their JSON bytes, CloudDrive / Kubernetes / Emby calls, status files, Telegram text) stays as
  Python has it. `DIVERGENCES.md` lists every place the Rust build differs and why; add an entry, with a
  number from your WP's range, when you differ on purpose or copy a Python bug.
- The cross-version contracts in `PLAN.md` are binding: schema stays at version 19, config sections never
  gain or lose a key, a CloudDrive mutation that was sent is awaited to its end even when the caller is
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
BASELINE                   the Python commit everything is compared with
crates/embyx-core          Python-compatible helpers (pyjson, pyre, pyurl, pypath, pytext), goldens and
                           transcript support; from WP1 settings, errors, AVID, magnets
crates/embyx-db            pool, schema v19, advisory locks; test databases, scenario rows
crates/embyx-clients       HTTP client (httpx-shaped) and retries, subprocess runner, CloudDrive over
                           tonic (generated from proto/clouddrive.fds.bin), the fakes; from WP2 the site
                           clients
crates/embyx-monitor       monitor: acquisitions, archive, tracker, RSS, scheduler (WP5-WP9)
crates/embyx-fill-actor    fill actor (WP10-WP12)
crates/embyx-merge         merge service and worker (WP13, WP14)
crates/embyx-transcode-worker  home transcode worker; the only place `unsafe` may appear (WP15)
crates/embyx-api           axum routes (WP4 and later)
crates/embyx-manager       the binary: `serve`, `merge-worker`, `transcode-worker`
goldens/<area>/            outputs of the Python baseline, generated (see Goldens)
goldens/scenarios/         effect transcripts (docs/rewrite/SCENARIOS.md)
tools/                     Python tooling: goldens, scenarios, parity, codegen, probes (tools/README.md)
docs/rewrite/              PLAN, PARITY (generated), API-COVERAGE, SCENARIOS
```

A module names the Python module it ports in its doc comment (`src/embyx_manager/monitor/archive.py`).

## Code

- Edition 2024, toolchain from `rust-toolchain.toml`. Dependencies go in `[workspace.dependencies]` and
  crates use `dep.workspace = true`; pin release candidates exactly (`=x.y.z-rc.n`).
- Lints are workspace-wide (`[workspace.lints]`: clippy pedantic, `unsafe_code` denied, `todo!`,
  `unimplemented!` and `dbg!` denied, `print_stdout`/`print_stderr` warned); CI runs clippy with
  `-D warnings`. `unsafe` is allowed only in `crates/embyx-transcode-worker/src/process*` and its probe, with
  `#[allow(unsafe_code, reason = "...")]` on the smallest item; `tools/check_tree.sh` enforces where. Every
  other `#[allow]` or `#[expect]` carries a `reason`.
- No placeholder bodies in merged code. The one exception until their WPs: the three subcommands of the
  binary exit with "not implemented yet" (WP4, WP13, WP15 replace them).
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
  `// py: tests/test_avid.py::test_extracts_plain_id`. A parametrized Python test may be split over several
  Rust tests; each claims its share with `cases=N` (`// py: tests/test_x.py::test_y cases=12`) and the
  shares add up to pytest's count. Never weaken an assertion: an exact string stays exact.
- `tools/parity/scope.toml` assigns every Python test to a WP; `tools/parity/waivers.toml` holds tests not
  ported, each with a reason. `parity.py --check` fails on a claim of a test that does not exist, a case
  count that does not add up, or a test in a merged WP that is neither ported nor waived.
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

`goldens/<area>/` holds deterministic outputs of the Python baseline. The generator
`tools/goldens/generate.py` (areas in `tools/goldens/areas/`) imports the baseline's own code under the
pinned Python 3.13.18; clock, ids and ordering are pinned. Rust tests read them with
`embyx_core::golden` (`golden::load("json/dumps.json")`) and compare.

```
UV_PYTHON=3.13.18 uv run python tools/goldens/generate.py            # rewrite every area
UV_PYTHON=3.13.18 uv run python tools/goldens/generate.py --check    # what CI runs
```

Goldens come from the baseline, never from a porting session's head. Never edit one by hand; a golden the
Rust build cannot meet is a bug or a `DIVERGENCES.md` entry. Effect transcripts work the same way
(`docs/rewrite/SCENARIOS.md`).

## Code copied from fav

The Python-compatible helpers and the HTTP stack were written for fav's Rust rewrite and copied here, not
depended on (no Git dependency on fav):

- Source: cyrahs/fav at commit `b1ed1601de2ed42a96bd2d7d31c1e0187508df98` (`main`, 2026-11-13).
- Copied: `fav-core` `pyjson`, `pytext`, `pyurl`, `pypath` (with `PyPath` from `validate.rs`), `golden`;
  `fav-sources` `pyre` and the fake web (`testutil.rs`, here `net/fakeweb.rs`); `fav-net` `lib.rs`,
  `client.rs` (httpx-shaped client), `httpx.rs` (error classes and texts), `retry.rs` (ladders and the
  recording sleeper), `proxy.rs`, `download.rs`, `subprocess.rs`; `fav-db` `testdb.rs` (adapted). The
  parity script and its self-tests, and `tools/ruff.toml`, are adapted from fav's tooling.
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
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
EMBYX_MANAGER_TEST_DATABASE_URL=postgresql://... CARGO_INCREMENTAL=0 cargo test --workspace --locked
tools/check_tree.sh
uv run python tools/parity/parity.py --check \
  --pytest-collected <(uv run pytest --collect-only -q) \
  --rust-tests <(cargo test --workspace --locked -- --list --format terse)
uv run ruff check . && uv run ruff format --check .
```

and, when Python tooling or goldens changed, the golden, scenario and descriptor `--check` runs from
`tools/README.md`. CI (`.github/workflows/rust.yml` and `ci.yml`) runs all of it on every pull request into
`rust`.

## Definition of done (per WP)

Checks green; every Python test in the WP's scope ported or waived with a reason; no `todo!()`,
`unimplemented!()`, ignored tests or placeholder bodies; goldens and transcripts for the scope committed;
the WP's status row and experiment log row in `PLAN.md` updated; `DIVERGENCES.md` entries written; an
independent review done and its findings fixed or answered on the PR.
