# embyx-manager Rust rewrite: plan and status

Approved by Max on 2026-11-13 18:22Z ("按默认项开始": WP0 starts, the home transcode worker is ported to Rust and
shipped as an exe from GitHub Releases, everything else follows the defaults below). Written by the planning
thread (规划 embyx-manager 的 Rust 改写方案); this file is its repository copy and, from WP0 on, the source of truth
for status. Every coding thread updates its row in "Work packages", adds a row to "Experiment log" and, where it
built a shared piece twice, a line to "Cleanup notes". Divergences go to `docs/DIVERGENCES.md`, conventions to
`docs/CONVENTIONS.md`, library choices to `docs/rewrite/API-COVERAGE.md`; test parity is generated into
`docs/rewrite/PARITY.md`.

## Decisions ([Max] = confirmed by Max on 2026-11-13 18:22Z with "按默认项开始")

1. [Max] Scope: full parity. `serve` (API + all pipelines + fill actor + merge), `merge-worker`, `transcode-worker`.
   Dropped: `import-config` (one-time import of a legacy embyx-monitor file, production is long migrated),
   the in-memory fill-actor repository (test-only), the `/docs` and `/redoc` pages.
2. [Max] Baseline: `main` HEAD when WP0 starts (today `5a3c083`, #80, which is also the deployed commit, image digest
   `sha256:06fd3f0979c89c8f99d73df73c0506f80726ab2c3b7a0d1818d94f0d796f6208`). Recorded in `BASELINE`.
   `main` is hot-fix only during the rewrite. Every commit that lands on `main` after the baseline is ported in
   one catch-up step before the switch (WP16). Do not chase `main` in between.
3. [Max] Verification: level 1 offline checks (mandatory, in CI) + level 2 read-only replay of production data
   (Max is asked before any export) + level 3 capped live runs against anonymous read-only sites.
4. [Max] Merging: PRs merge into branch `rust` without asking Max, after CI green + one independent review by a fresh
   session. `rust` -> `main` always needs Max.
5. [Max] Parallelism: WP0 and WP1 serial. From WP2 at most 2 threads in parallel.
6. [Max] Models: coding with Opus 5.5 at high effort. Planning, checkpoint retros and the final report with Fable 5.2 high.
7. [Max] No stops at checkpoints (as Max decided for fav on 2026-11-10): after a WP merges the next one starts. Only
   the final report goes to Max, plus the few questions that are his (listed at the end).
8. [Max] Home transcode worker: ported to Rust and shipped as a Windows `embyx-manager.exe` from GitHub Releases,
   same command line, same `config.toml`, same protocol. The Python worker stays installable from
   tag `python-final` until Max has run one real title on the Rust exe.
9. [Max] Repo layout: Rust-native from the first commit on `rust` (Cargo workspace at the repo root, `crates/`), with
   the Python tree left untouched next to it until the cleanup. No `rust/` directory, so no move PR afterwards.

## Hard safety rules (until Max lifts them for the deployment)

- Never deploy. No image publish from `rust`, no change to charys117/nas-gitops, no workflow dispatch, no tags.
  (`ci.yml` publishes `latest` from `main` only; Flux follows `latest` by digest and restarts the pod.)
- Never touch what is running: the live pod, its merge Jobs, the home worker and its WireGuard gateway, the
  files under `/upload/merge`, the embyx PostgreSQL database. A restart of the pod interrupts uploads to 115 and
  the home worker's session. At planning time task 265 (UNVRSP-003, home 5090, 20 of 33 parts) is in flight.
- Never point a Rust build at the production database, the production CloudDrive, the production Emby, the
  Kubernetes API of the cluster, or any path under `/mnt/cd2`, `/root/data/embyx`, `/upload`, `/downloads`.
- Writes to the embyx PostgreSQL database by a thread need Max's 「确认」 (standing rule). Merge retry and cancel
  API calls need Max's explicit ask (standing rule).
- Never run live before the deployment: any CloudDrive mutation (move, copy, delete, rename, create folder, add
  or remove offline tasks), any Emby write (playlists), any Kubernetes Job. These are verified under fakes only.
- Allowed live (level 3, a few requests each, polite rate): read-only anonymous pages of JavBus, sukebei, AVBase,
  jinjier.art and one RSSHub feed.
- The Python tree on `rust` stays untouched: it is the reference and the golden generator. Do not merge `main`
  into `rust`. PRs target `rust`, never `main`.
- Secrets (database DSN, API token, CloudDrive token, Emby key, transcode token, WireGuard keys) never appear in
  a thread, a log, the repo or a report.

## Goals and non-goals

Goals: behaviour parity with the baseline; drop-in replaceable in both directions (same env vars, same schema at
version 19, same stored JSON, same HTTP surface, same file names and directory layout, same Job contract, same
worker protocol), so that Python can be put back on the same database at any time; idle RSS under 50 MiB
(Python: 30-day p50 192 MiB, request 256Mi); `merge-worker` overhead under 15 MiB (Python 63 MiB); an experiment
log (rework count and review findings per WP).

Non-goals: touching `frontend/` (React) beyond its build output path; rewriting ffmpeg, CloudDrive or Emby; new
features or behaviour changes (Python bugs are logged in DIVERGENCES.md and copied unless they lose or damage
data); new schema migrations or new config keys (they would break the rollback, see "Cross-version contracts");
the memory wins offered for Python on 2026-11-12 (Max declined them).

## Current system (baseline facts, measured 2026-11-13 at `5a3c083`)

- Python 3.13. `src/embyx_manager`: 23,470 lines without the generated CloudDrive stubs (9,370 more).
  Tests: 18,715 lines, 747 test functions, 1,754 collected cases (825 are the AVID corpus). Front end: 13,096
  lines of TypeScript, 117 vitest tests.
- Lines by package: app root 2,224 (`bootstrap.py` 719, `db.py` 675), `core` 366, `config` 850, `clients` 1,925
  (CloudDrive wrapper 491), `monitor` 7,206, `fill_actor` 5,837 (1,065 of them the in-memory test repository),
  `merge` 5,062 (`service.py` 1,222, `home_worker.py` 1,141, `worker.py` 877).
- One server process (`embyx-manager serve`: uvicorn + FastAPI, about 21 threads) plus three other commands:
  `merge-worker` (body of the Kubernetes Job, no database, no secrets, reports through a JSON status file),
  `transcode-worker` (runs by hand on Max's Windows PC with the RTX 5090, installed with
  `uv tool install git+https://github.com/cyrahs/embyx-manager`), `import-config`.
- HTTP: 52 routes. 47 on the main listener (8000): auth session, health, config (5), monitor (20), playlists (5),
  fill actor (7), merge (8). 5 under `/api/transcode/*` on a second listener (8001) that only the WireGuard
  gateway reaches. Errors are `{"error": {"code": ...}}`; request validation errors are a fixed 422
  `invalid_request`. Mutations need a bearer token; reads are open.
- PostgreSQL through asyncpg, native SQL, 130 call sites in 11 files, 19 tables. Schema built by 19 numbered
  migrations recorded in `schema_migrations`; Python refuses to start on a version above 19. Advisory locks:
  namespace `0x454D4258`, keys 0 (migration), 1 (enqueue), 2 (mutation).
- Settings: 9 sections in `app_config` as pydantic JSON, versioned, hot-applied; secrets masked on read.
  Section models forbid unknown keys, and a section that fails validation silently reads as its defaults.
- External systems: CloudDrive over gRPC (14 RPCs of its API; moves, copies and deletes on 115, offline tasks);
  Kubernetes API (Jobs, pods, logs, plain HTTPS with the service-account token); Emby HTTP API (movie index,
  playlists); JavBus and sukebei (httpx + lxml); AVBase (curl_cffi with a Chrome TLS fingerprint, behind a
  Cloudflare challenge); jinjier.art (a zip holding a SQLite file); RSS/Atom feeds; ffmpeg and ffprobe; inotify
  on the `.strm` tree; the CloudDrive FUSE mount; the `upload` and `downloads` volumes.
- Consumers of the API: the bundled React UI, the home transcode worker, the kube probes and Gatus (`GET /`).
  Nothing else (checked nas-gitops, homepage, nas-mcp, nas-mitm-go). No MCP.
- Deployment: nas-gitops `apps/emby/embyx-manager` (namespace `media`, Deployment `embyx-manager`, Recreate,
  pinned to nas-us-2, uid 0 with `DAC_OVERRIDE`, read-only root filesystem, `HOME=/tmp`), image `latest` + digest
  with marker `# {"$imagepolicy": "flux-system:embyx-manager-ghcr:digest"}`, automation in
  `infra/image-automation/embyx-manager-ghcr.yaml`. Merge Job template `merge-job.json` (ConfigMap) runs
  `embyx-manager merge-worker` in the image digest the pod itself runs. `transcode-gateway/` is the WireGuard pod.
- Both repos involved are public: cyrahs/embyx-manager (GPL-3.0) and cyrahs/fav.

## What is different from fav

1. The app's job is to move and delete. Archive renames videos into the library and deletes the download folder;
   the tracker removes offline tasks with their data; 换版 and merge delete library copies through CloudDrive;
   mapping deletes `.strm` files and sidecars; fill actor moves files with a journal. fav only added files. A
   parity gap here can lose library files, and 115 cannot be backed up. Hence "effect transcripts" in level 1,
   "decision replay" in level 2, and a deployment that starts with everything paused.
2. State is in flight at all times: merge tasks (Job running, CloudDrive copy under way, chunks on disk),
   downloads being tracked, fill-actor jobs with leases, a move journal that is replayed at start. The Rust build
   must pick up every state Python left, and Python must pick up every state Rust left (rollback).
3. `main` moves. 30 PRs merged in the last six weeks (#51-#80), ten since 2026-11-10, four of them today.
   fav's `main` stood still.
   Hence the hot-fix-only rule and the catch-up step.
4. One part runs outside the cluster, on Windows, started by hand, and is installed from the Git repository.
   Deleting the Python package from `main` breaks that install command.
5. A browser TLS fingerprint is already required today (AVBase). fav met this class of problem only at the end
   (D182, Cloudflare refused the Rust client). Here it is checked in WP0.
6. Simpler than fav in three ways: SQL is already native PostgreSQL with real migrations; API errors are fixed
   codes, not pydantic texts; there is no MCP, no browser automation and no Telegram.

## Technology mapping

Versions and crate choices are pinned in WP0 after reading current docs; WP0 checks API coverage of the rows
marked (probe) without touching a real service, and reports alternatives where one falls short.

| Python | Rust | Notes |
| --- | --- | --- |
| FastAPI, uvicorn, pydantic | axum + serde; utoipa or a hand table for `/openapi.json` | Two listeners. Same paths, fields, status codes, error codes. Range requests on the part-read route as Starlette's `FileResponse` answers them (ffmpeg reads and resumes through it) |
| asyncpg | sqlx (runtime-checked queries) | Statements copied; `$n` placeholders stay. `TimeZone=UTC` per connection |
| migrations in `db.py` | the same 19 migrations, statement for statement | Production is at 19, so nothing runs there. Same `schema_migrations` bookkeeping and advisory lock |
| grpcio + generated stubs | tonic + prost, generated from the descriptor embedded in `clouddrive_pb2.py` (probe) | No `.proto` file is in the repo; WP0 extracts a `FileDescriptorSet` from the baseline stubs. Only the 14 RPCs in use need wrappers |
| httpx | reqwest behind one client factory, taken from fav's `fav-net` | fav's client already writes httpx's request head (fav #152) and maps errors to httpx's classes |
| curl_cffi (`impersonate='chrome'`) | an impersonating client (probe): a BoringSSL-based crate of the `wreq`/`rquest` family first, a `curl-impersonate` binary run as a subprocess second | The biggest unknown. Must be proven against AVBase from the production network, not from a cloud session (its egress proxy re-terminates TLS) |
| pyquery (lxml), nyaapy parser | `scraper` (html5ever) or an lxml-compatible tree (probe) | The two parsers build different trees for the same broken HTML (implied `tbody`, for one). Parser outputs are goldens from recorded pages |
| defusedxml | `roxmltree` or `quick-xml`, DTDs and entities refused | Feed and AVBase XML |
| sqlite3 + zipfile (jinjier) | rusqlite (bundled SQLite) + `zip` | Read-only, in memory or a temp file |
| cronsim | own cron evaluator (fav has one) or `croner` (probe) | Next-fire goldens; evaluated in the server's local time, as Python does |
| watchdog | `notify` (inotify) | Same debounce and batch rules |
| tenacity | fav's retry helper | 3 attempts, exponential 1-10 s |
| humanfriendly `parse_size` | ported | Goldens (decimal and binary units) |
| `re` | `regex`, `fancy-regex` where look-around is used (the AVID parser does) | fav's `pyre` rules; the AVID corpus (840 rows) decides |
| threads (`to_thread`, two executors) | tokio + `spawn_blocking` with a bounded pool | Filesystem work on the FUSE mount stays off the async workers |
| `ctypes` `renameat2(RENAME_NOREPLACE)`, `os.link`, `fcntl.flock` | `rustix` | No `unsafe` in our code |
| `ctypes` `NtSuspendProcess` / `SIGSTOP`, `CREATE_NEW_PROCESS_GROUP`, `SIGBREAK` (home worker) | `windows-sys` / `nix` behind one small module | The only place where `unsafe` may be unavoidable; isolated and reviewed |
| subprocess (ffmpeg, ffprobe) | fav's subprocess runner | Argument lists are goldens |
| colorlog / logging | tracing to stdout | Run logs stored in `pipeline_runs.log_json` keep their wording |

Reuse from fav (public repo, pinned commit, copied into this workspace, not a Git dependency): the Python-compatible
JSON writer and text helpers (`pyjson`, `pyre`, `pyurl`, `pypath`), the httpx-shaped client, retry ladders,
subprocess runner, the fake web server and the recording sleeper, the cron goldens tooling, the parity and
golden-generator scripts, `ci.yml`'s caching design. A coding thread reads fav with `add_repo`.

## Target architecture

- One binary `embyx-manager`: `serve` (default), `merge-worker`, `transcode-worker`. Same flags as today.
  The Windows release asset is the same program built with only `transcode-worker`.
- Cargo workspace at the repo root, about 9 crates: `embyx-core` (env settings, AVID parser, magnet, media,
  config models, errors, Python-compatible helpers), `embyx-db` (pool, migrations, advisory locks),
  `embyx-clients` (HTTP factory, JavBus, sukebei, AVBase, jinjier, Emby, feeds, CloudDrive, Kubernetes),
  `embyx-monitor` (ledger, intake, tracker, archive, mapping, rss, playlists, scheduler), `embyx-fill-actor`,
  `embyx-merge` (detect, tasks, service, auto, transcode bookkeeping, the merge-worker body),
  `embyx-transcode-worker` (builds on Windows and Linux, no database or gRPC dependencies), `embyx-api` (axum
  routers, auth, health, SPA hosting, the transcode listener), `embyx-manager` (entry point).
- Every external dependency (database, CloudDrive, Kubernetes, Emby, HTTP sites, clock, filesystem roots,
  subprocess) is injected, so tests and the scenario runner can replace it.
- Memory rules: stream chunk uploads and part reads; never hold a file in memory; listings in batches.
  Allocator chosen by measurement in WP16 (fav chose jemalloc).
- Code rules: no `unsafe` outside the one process-control module; clippy warnings are errors; no `todo!()`,
  `unimplemented!()` or ignored tests in merged code; code, comments and docs in English (UI strings and the
  worker's console stay Chinese, as today).
- Image: `debian:trixie-slim` with the same `ffmpeg` package the live image has (Debian 13, ffmpeg
  7.1.5-0+deb13u1, libsvtav1enc2 2.3.0; the merge worker's SVT-AV1 settings and memory figures were measured on
  that build). The image test checks both versions. Entry point and command names unchanged, so the Deployment
  and `merge-job.json` need no change.

## Cross-version contracts (identical in both directions, each one held by a golden or a test)

These are what make "swap in, swap back" safe. A WP that touches one of them states it in its PR.

1. Schema: version stays 19. No new migration and no new column until Max retires the rollback path.
2. Stored JSON: same key set per config section (Python forbids unknown keys and falls back to defaults on a
   validation failure, so one extra key would silently unconfigure a section after a rollback). Same value
   shapes in every `*_json` column. Written with the Python-compatible writer so the bytes match
   (`model_dump_json` form, `json.dumps` default form, and the canonical `sort_keys` + `(',', ':')` form used for
   stored cloud-file metadata).
3. State machines: acquisition and attempt states, merge task states with `failed_state`, fill-actor job leases
   and the move journal (`prepared`, `linked`, `source_removed`, `reconciled`), cloud move operations. Every
   compare-and-set statement is copied. Each persisted state must resume the same way in either build.
4. Merge Job contract: Job names (`embyx-merge-<id>-<slug>`, `embyx-audio-<id>-…`, 52 characters), the task
   label, the worker's arguments, the status file (`/upload/merge/.merge/<id>.json`: fields, atomic replace,
   5 s interval), the partial and staging paths, the transcode directory layout
   (`.transcode/<id>/` chunks, audio, manifest), ffmpeg and ffprobe argument lists, exit codes (refusals exit 0).
5. Home worker protocol, version `1`: claim and heartbeat bodies, the encoder settings sent with a task, chunk
   headers (`X-Chunk-Start`, `-Duration`, `-Frames`, `-Sha256`), refusal codes, the local cache with its sidecar
   files, the resume rules. A Python worker must work against a Rust server and the reverse.
6. CloudDrive semantics: a mutation that was sent is always awaited to its end, also when the caller is
   cancelled or the app shuts down (Python waits for the thread; a dropped tokio future would abandon the RPC
   and leave a move in an unknown state). Timeouts 30 s, 300 s for move and delete, 120 s for the offline list.
   Error texts the code matches on (`任务已存在`, `can't open a file as directory`), `NOT_FOUND` handling, path
   validation, lower-casing info hashes on removal, `checkFolderAfterSecs=10`.
7. AVID spelling: the parser output keys the ledger and names library files. Any drift duplicates or orphans rows.
8. HTTP surface: 64 KiB body limit with 413 `request_too_large` (not on the transcode listener),
   `Cache-Control: no-store` under `/api/`, SPA fallback rule, 401 with `WWW-Authenticate: Bearer`, constant-time
   token comparison, timestamp format in responses.
9. Environment: every `EMBYX_MANAGER_*` variable with its default and validation (non-loopback bind needs the
   token and `TLS_TERMINATED`), `POD_NAME`, `POD_NAMESPACE`, `EMBYX_MANAGER_MERGE_JOB_TEMPLATE`.

## Verification

Level 1, offline, all in CI:

1. Test parity. Every Python test function has a Rust counterpart annotated with its origin
   (`// py: tests/test_x.py::test_y`); a script checks all 747 functions, and for parametrized ones the number
   of cases (1,754 in all; the two corpus TSV files are read as they are). Non-applicable tests go to a waiver
   list with a reason. Port the intent of the assertions, not the mocking mechanics. Database tests run against
   real PostgreSQL, one temporary database per test.
2. Goldens. A Python script run on the baseline tree dumps deterministic outputs that are committed; Rust tests
   compare them. Cover at least: AVID parsing, magnet hashes, settings defaults, validation results and masking
   for all 9 sections, env settings, cron next-fire times, the resolve cadence, `parse_size`, the parsers
   (JavBus, sukebei, AVBase, jinjier, RSS and Atom, feed magnet tables) over recorded pages, magnet ranking,
   Job names and the built Job object, every ffmpeg and ffprobe argument list, status-file sequences, concat
   scripts, encoder arguments of the home worker, stored JSON bytes, API responses over a seeded database.
   Goldens come from Python, never hand-written by the porting session.
3. Effect transcripts (new). For every operation that moves or deletes, a scenario runner executes the Python
   baseline on a temporary directory tree with recording fakes for CloudDrive, Kubernetes and Emby, and dumps
   the ordered calls with their arguments, the final tree and the final database rows. The Rust build runs the
   same scenario and must match. Covered: archive (each outcome), reconcile, move-in, mapping (full and
   incremental), tracker (finished, junk, stalled, lost, error, 换版), intake and retry, fill-actor apply (local
   and cloud) and its recovery from each journal state, merge service step by step with cancel and retry at each
   state, auto-merge, the home transcode path. Scenarios start from the Python tests' fixtures; new ones are
   written in Python first, so the expected result is always Python's.
4. API contract. Structural diff of `/openapi.json` (both listeners) against Python's. The unmodified React
   front end builds, its 117 tests pass, and a page smoke test passes against the Rust API with a seeded database.
5. Schema. Both sides migrate an empty database; `pg_dump --schema-only` diff is empty.
6. Cross-version. Python worker against Rust server and Rust worker against Python server, with real ffmpeg on
   tiny generated parts and a software encoder (the worker's `[encoder]` override exists for this); the Rust
   merge-worker and the Python one on the same parts give the same status sequence and a file of the same
   duration; a resume matrix (rows and files written by one build, continued by the other).

Level 2, read-only replay (Max is asked first; captured in the cluster, nothing written to production, the Rust
binary never connects to a production service, captures deleted afterwards):
- A dump of the embyx database with secrets masked inside the pod (`clouddrive.api_token`, `emby.api_key`, any
  other secret field found when the tables are listed) into a temporary database in the cloud session. Python
  API and Rust API both point at it; every GET is diffed.
- Decision replay. Read-only snapshots of what the pipelines look at: the CloudDrive offline-task list and the
  listings of the task directories, names and sizes (no content) of the intake and library trees, the two
  `.strm` trees, Emby's movie index and playlists. Rebuilt as sparse files and fakes. One pass of each pipeline
  in Python and in Rust; the effect transcripts must be equal.

Level 3, capped live runs, anonymous and read-only: JavBus (one star search, one star page, one video's
magnets), sukebei (one search), AVBase (build id, one talent, one work), jinjier (page and archive), one RSSHub
feed. Where the TLS fingerprint matters the probe runs from the production network (see question 3).

## Work packages

| WP | Phase | Status | Content | Python lines (approx.) | Needs |
| --- | --- | --- | --- | --- | --- |
| WP0 | 0 foundation | done (#81); live fingerprint proof done (follow-up PR) | Workspace at the root, `rust.yml` (checks only, with the caching fav ended up with), docs (`PLAN`, `CONVENTIONS`, `DIVERGENCES`, `PARITY`, `API-COVERAGE`), `BASELINE`, golden generator, parity script, scenario runner skeleton, helpers copied from fav, API-coverage probes (impersonating client, tonic from the descriptor, HTML parser, cron, notify, Windows build of an empty worker crate) | new | |
| WP1 | 1 core | done (#83) | Env settings, database layer and the 19 migrations, advisory locks, config models and store, AVID parser, magnet, media, errors | 2,300 | WP0 |
| WP2 | 2 clients | done (#85) | HTTP factory, JavBus, sukebei, feeds, feed magnets, AVBase on the impersonating client, jinjier, Emby | 1,450 | WP1 |
| WP3 | 2 clients | done (#84) | CloudDrive over tonic with the cancel-safe wrapper and a fake server; Kubernetes Jobs client | 700 | WP1 |
| WP4 | 2 API | done (#88) | axum shell: auth, health, size limit, SPA hosting, config API with the two connection tests, OpenAPI check, page smoke. Checkpoint A (note only) | 900 | WP2, WP3 |
| WP5 | 3 monitor | done (#87) | Ledger, runs, reports, cadence, release dates, intake, subscriptions | 2,000 | WP2, WP3 |
| WP6 | 3 monitor | done (#89) | Archive, reconcile, move-in (filesystem effects) | 1,100 | WP1 |
| WP7 | 3 monitor | done (#91) | Tracker, 换版, manual intake | 1,250 | WP5, WP6 |
| WP8 | 3 monitor | done (#PR) | RSS pipeline, mapping pipeline, playlists and playlist sync | 1,150 | WP5 |
| WP9 | 3 monitor | pending | Scheduler (cron, watcher, tracker bursts), monitor API, playlists API, wiring | 1,700 | WP7, WP8, WP4 |
| WP10 | 4 fill actor | done (#90) | PostgreSQL repository, models, cloud moves, adapters | 1,850 | WP3, WP5 |
| WP11 | 4 fill actor | pending | Service: scan, plan, local moves with journal and recovery, cloud moves | 2,100 | WP10 |
| WP12 | 4 fill actor | pending | Job manager, API, health, lifespan | 1,200 | WP11, WP4 |
| WP13 | 5 merge | done (#86) | Detect, task repository, the `merge-worker` command (probe, checks, concat, remux, AV1 re-encode, home prepare and assemble) | 1,650 | WP1 |
| WP14 | 5 merge | pending | Merge service, auto-merge, transcode bookkeeping, merge API, transcode listener. May be split into "service" and "API + transcode" | 2,300 | WP13, WP3, WP9 |
| WP15 | 5 merge | pending | Home transcode worker, Windows build and a Windows CI job, protocol tests both ways | 1,150 | WP14 |
| WP16 | 6 wrap-up | pending | `Dockerfile.rust` (build only), memory benchmark, parity audit, catch-up with `main`, level 2 and level 3 as far as Max allowed, final report data | new | all |

Order: WP0, WP1; then WP2 + WP3; WP4 + WP5; WP6 + WP13; WP7 + WP8; WP9 + WP10; WP11 + WP14; WP12 + WP15; WP16
alone. Each WP ports the tests in its scope. Checkpoint notes (A after WP4: the front end works against the Rust
backend; B after WP12: level 1 complete for monitor and fill actor, first memory numbers) go into `PLAN.md` only.

Size estimate: 45-50k lines of Rust outside tests, about 80k with tests, 17 threads. fav was 41k lines of Python
and took about 18 hours of wall-clock time for its packages; this is 23k lines with heavier verification.

## How threads work

- One WP = one thread = one PR. Branch from `rust`, PR into `rust`.
- Handoff lives in the repo: `docs/rewrite/PLAN.md` (WP list, status, experiment log), `docs/CONVENTIONS.md`,
  `docs/DIVERGENCES.md` (numbered D entries, a range per WP), `docs/rewrite/PARITY.md` (generated),
  `docs/rewrite/API-COVERAGE.md`. Read them first, update them last. A handoff brief for the WP threads is kept
  at `/mnt/project-files/embyx-rust/handoff.md` by the planning thread.
- Definition of done for a PR: CI green (fmt, clippy, tests, parity script, goldens, effect transcripts); every
  Python test in scope mapped or waived with a reason; no placeholder implementations; `PLAN.md` updated.
- Independent review before merge by a fresh session against the Python source, looking for weakened
  assertions, missed behaviour and broken cross-version contracts. Findings are recorded on the PR.
- When a PR merges, the thread reports to the planning thread (PR number, squash commit, review result, what
  the next WPs must know), which hands off the next WP. Max is asked only for what is his call.
- Lessons carried over from fav: build with `CARGO_INCREMENTAL=0` (a `target/` grew to 28G); run the full
  workspace tests after every merge; write transport-failure tests up front; keep secrets out of `Debug`; drop
  leftover per-test databases after a crashed run; generate goldens with one pinned uv-managed Python 3.13.

## Deployment and rollback (only after the final report, on Max's 「确认」)

Same shape as fav's, with the pipelines paused first because of what they can delete.

1. Window: no merge task in `merging` or `transcoding` (no Job running, home worker idle or stopped), no
   fill-actor job running, no pipeline run in flight.
2. Image `ghcr.io/cyrahs/embyx-manager:rust-<sha>` from a separate workflow on `rust` (`ci.yml` untouched).
3. Backups before anything changes: `pg_dump -Fc` kept in the postgresql pod's data directory, with row counts
   and the schema dump; a tar of the two `.strm` trees; the masked `GET /api/config` and the monitor status;
   listings (names and sizes) of the library and intake trees for the later diff; the Python image digest.
   Manual Longhorn snapshots are not a safety net (recurring jobs purge them within a day). 115 itself cannot be
   backed up: its recycle bin is the net, so nothing empties it during the trial.
4. Pause through the running Python API, recording the previous values: rss, archive, mapping, playlists,
   auto-merge, home transcode, fill-actor apply.
5. Swap: nas-gitops PR pins the Rust image by tag and digest and removes the `$imagepolicy` marker. Verify the
   pod, schema version 19, equal row counts, `GET /api/config` equal to the recorded one.
6. Manual round, least dangerous first, checking the data after each step and restoring the setting when it
   passes: read-only pages; mapping full sync (expect no change on a tree that is in sync); playlist sync; one
   RSS run; archive and tracker (first filed downloads checked one by one: right folder, right name, nothing
   else changed); one manual AVID; one small lossless merge through to the library; fill-actor scan; the home
   worker (Python worker against the Rust server first, the Rust exe after).
7. A functional failure leaves that pipeline off and is fixed forward on `rust`. Data loss or damage: stop,
   revert the nas-gitops PR (Python image on the same database), restore from the recycle bin and the backups,
   report to Max.
8. On Max's word: `rust` into `main`, `latest` becomes the Rust image, marker restored.
9. Cleanup WP, as fav's: git tag and image tag `python-final` first; delete the Python tree; `Dockerfile.rust`
   becomes `Dockerfile`; one `ci.yml` (fmt, clippy, tests, front end, image with cargo-chef and registry cache,
   publish); a release workflow for the Windows exe; README and `docs/home-transcode.md` rewritten; nas-gitops
   memory request lowered to what was measured. Backups are deleted on Max's word.

## Known risks

- A parity gap in a delete or move path loses library files. Mitigation is everything above; residual risk is
  in paths no scenario covers.
- AVBase behind Cloudflare: if no Rust client passes, fill actor's catalog, talent subscriptions and release
  dates stop working. Fallback inside the plan: `curl-impersonate` as a subprocess.
- JavBus and sukebei see a different TLS handshake from reqwest than from httpx (fav D182 class).
- lxml and html5ever disagree on malformed pages; recorded pages cannot cover every page the sites serve.
- The home worker's Windows process control (suspend, resume, Ctrl+C handling) and NVENC cannot be tested in a
  cloud session; CI covers Windows with a software encoder, the GPU run is Max's.
- `main` moving under the rewrite: each hot fix costs a port in the catch-up.
- AI weakening tests: goldens and transcripts from Python, parity script in CI, independent review.
- Size: about 80k lines written and reviewed by AI in a day or two; fav's reviews kept finding real defects
  (2 blockers, 61 should-fix), so some remain in paths no test reaches.

## Questions for Max (state on 2026-11-13 18:22Z)

1. Start with the defaults: ANSWERED, yes.
2. Home worker: ANSWERED, Rust exe from GitHub Releases.
3. A one-off pod without mounts or secrets on nas-us-2 sending a few anonymous GETs to AVBase and JavBus, to
   prove the client's fingerprint from the production network: NOT ANSWERED, so not allowed. Until Max says yes,
   no thread starts a pod or sends a request from the cluster; the fingerprint is compared offline (ClientHello
   and HTTP/2 settings captured from curl_cffi and from the Rust client against a local listener) and the live
   proof moves to the deployment.
4. Level 2 export (masked database dump and read-only listings): asked when WP16 reaches it. Not allowed before.

## Cleanup notes

Shared pieces that parallel work packages build twice, or that one package leaves for another. WP16 closes them
or says why one stays.

- WP0 copied fav's helpers whole (`embyx-core`: `pyjson`, `pyre`, `pyurl`, `pypath`, `pytext`; `embyx-clients`:
  `net` with the httpx-shaped client, retry ladders and recording sleeper, subprocess runner, proxy rule,
  downloads, fake web). WP2 and WP13 decide which parts the application uses; WP16 removes what nothing calls.
- `json/formats.json` keeps fav's five formats next to the two embyx-manager uses (`ascii`, `plain`) and the
  one WP0 added (`sorted_compact_ascii`), so the copied helper's tests stay whole. Drop the unused ones with the
  unused `DumpOptions` constructors in WP16.
- The Rust fake CloudDrive (`embyx-clients/src/clouddrive/fake.rs`) serves all 14 RPCs rule for rule from
  `tools/scenarios/fakes/clouddrive.py` (WP3), with test hooks the Python fake lacks (`seen()` for headers and
  deadlines, `hold(method)` to keep a call in flight). The HTTP fakes are `embyx_clients::fakehttp` (a port of
  `fakes/http.py`); `embyx_clients::scenario::ScenarioFakes` starts all three as the runner does. WP14 (Jobs)
  replaces a route table with a state model in both fakes at once if it needs one, keeping the record shape
  (WP8's Emby playlists did not: scripted routes were enough).
- `embyx_core::posixpath` (WP3) is Python's `posixpath` (`normpath`, `split`, `dirname`, `basename`, `join`) for
  CloudDrive API paths, next to fav's `pypath` (`pathlib` semantics for local paths). Use `posixpath` where
  Python calls `posixpath`, `pypath` where it uses `PurePosixPath`; they differ on `..`, `//` and trailing
  slashes (`goldens/text/posixpath.json`).
- `tools/parity/parity.py` checks that a claimed Rust test is compiled by its bare function name in the Linux
  test listing. A claim on a function that is not compiled passes if another compiled test has the same name,
  and a Windows-only test cannot be claimed. WP15 (the first Windows-only tests) makes the check match module
  paths and accept the Windows job's listing.
- `examples/fetch_probe.rs`, `tools/probes/tls_fingerprint.py`, `live_fingerprint.py` and `live_probe_pod.sh`
  exist for the AVBase and JavBus client decision; WP2 reruns the comparison against its configured clients,
  WP16 removes them if nothing else needs them.
- WP2 left these for later packages: `examples/fetch_probe.rs` gained the configured clients (`avbase`,
  `javbus`) and `wreq-tuned` now takes its emulation from `avbase::session::chrome_emulation`, so the probe
  and the application cannot drift apart. `rusqlite` is pinned to 0.39 because sqlx's `libsqlite3-sys` 0.37
  claims the same `links = "sqlite3"`; move both together. `kube.rs` (WP3) words its UTF-8 error for an invalid
  start byte only, while `embyx_clients::http::utf8_error_message` (WP2) covers every case Python words; WP16
  can point kube at it. `pytext::casefold` moved there from `settings` for the clients to share.
- `embyx_core::config::lax` (WP1) emulates pydantic's lax mode for the field types the nine sections use (`str`,
  `bool`, `int` as `i64`, `float`, tuples of `str`, `dict[str, str]`, `dict[str, tuple[str, ...]]`, a nested
  model list) with pydantic's error `type`, `loc` and `msg`. WP4 (request bodies) and WP10/WP12 (fill-actor
  models) extend it there rather than writing a second one; a new type gets its own golden cases.
- `embyx_core::cron` (WP1) is cronsim's parser only (what `archive.scan_cron` validation needs); WP9 computes
  fire times with croner (API-COVERAGE) and should check both agree on what they accept.
- The config snapshot refresh loop and the store's wiring into the app are WP4's / WP9's (`bootstrap.py`);
  `embyx_db::config_store::ConfigStore` offers `load`, `refresh`, `get::<Section>()`, `get_with_version`,
  `sections` and `update`.
- `embyx_merge::py` (WP13) holds private Python-semantics helpers the worker needed (CPython's float `sum`,
  `float()` of text, `==` across int/float/bool, `.Nf` and `{:,}` formatting, `splitlines`, the `OSError` text
  `[Errno N] strerror: 'path'`). `embyx-clients` has its own `OSError` text (`PyOsError`) and `embyx-core`'s
  `pytext` overlaps; WP16 moves what two crates use into `embyx-core`.
- Two subprocess runners: `embyx_clients::net::subprocess::SystemRunner` (async, D3) and the merge worker's
  blocking `embyx_merge::worker::exec::SystemExec` (D140). WP15's home worker picks one; WP16 drops the
  other if nothing else uses it.
- `embyx-merge`'s database half sits behind its default `db` feature and the binary leaves it out, so the
  binary still builds on Windows (D144). WP14 puts the service behind `db` too; WP4 (`serve`) and WP15 (the
  Windows asset with only `transcode-worker`) settle the binary's features.
- The real-ffmpeg cross-check (`crates/embyx-merge/tests/ffmpeg_parity.rs` with
  `tools/merge_ffmpeg/python_worker.py`) runs the Python worker from the Rust tests, so `rust.yml` installs
  ffmpeg and uv before `cargo test`. CI's ffmpeg is Ubuntu's (6.1), the image's Debian 13's (7.1.5); the
  argument lists are pinned by goldens, so only the real-ffmpeg tests see the version. WP16 keeps or drops
  the cross-check with the Python tree.
- WP5 left these for later packages. `embyx_monitor::acquisitions::Ledger` is a dyn-compatible trait over the
  ledger: `AcquisitionRepository` implements it, and `embyx_monitor::testing::FakeLedger` (feature
  `test-support`) is Python's `FakeLedger` from `tests/test_monitor_rss.py` rule for rule, so WP6-WP9 port their
  intake, RSS, tracker, manual, replace and API tests against the same fake (it adds `states_for` and
  `task_dirs_for`, which the Python fake lacks). `reports::RunContext` and `reports::PyException` are how a
  pipeline writes Python's run-log bytes (`: repr(exc)` included); `reports::Raised` stands for any exception
  that exists only as text. `support::Clock`, `fixed_clock` and `py_add`/`py_sub` (datetime arithmetic with
  Python's `OverflowError`) are shared by the monitor crate; `support::uuid4_hex` uses `getrandom`, which WP14
  can reuse for Job names. `intake::operation_result_text` is `str(FileOperationResult)`, for any later log line
  that quotes a CloudDrive result. The `intake/steps` scenario (`tools/scenarios/operations/intake.py`) pins clocks
  by patching the modules' `datetime` and skips `CLOUD_RETRY`'s waits; WP6-WP9 scenarios can do the same.
- WP6 left these for later packages. `embyx_monitor::fs` is the file system as `pathlib`/`shutil` see it (D70):
  the `Fs` trait, `LocalFs` (with `_rmtree_safe_fd` on `rustix`), the free functions `is_dir`, `is_file`,
  `exists`, `is_symlink`, `is_video`, `iterdir`, `rglob`, `mkdir_parents`, and `PyOsError` with Python's class,
  `str()` and `repr()`; `fs::recording::RecordingFs` (feature `test-support`) records every mutation in the
  scenario call shape and injects `OSError`s and stop requests by operation and path. WP7's tracker and 换版 and
  WP11's local moves should take an `Arc<dyn Fs>` rather than calling `std::fs`. `PyOsError` is a third copy of
  the `OSError` text (with `embyx_clients`' `PyOsError` and `embyx_merge::py`); WP16 merges them into
  `embyx-core`. `ArchivePipeline` keeps Python's public surface (`archive_folder`, `inspect_folder` +
  `file_selection`, `archive_loose_group`, `replaced_copies`, `library_holdings`, `route_for_task_dir`,
  `avid_of`, `covers_other_route`, `routes`) for WP7 and WP9; its file work is synchronous and async callers wrap
  it in `archive::blocking` (D71). `reconcile::{record_archived, record_attention}` are the shared ledger writes
  of reconcile and move-in. The library move keeps Python's exists-then-rename; `RENAME_NOREPLACE` (`rustix`,
  as WP11 needs for fill actor) would close the window between the check and the rename in both builds, a
  behaviour change for Max to decide after the rewrite. The scenario operation
  (`tools/scenarios/operations/archive.py`, `archive/scenarios.rs`) runs `{op, args}` steps over a seeded
  tree through `RecordingFs` (faults, stops, `advance_seconds`); WP7 can add tracker steps to it.
- WP7 left these for later packages. `tracker::AcquisitionTracker::new(TrackerParts { .. })` with
  `TrackerSettings::from_config(config, task_dir_paths, library_mount_prefix)`; WP9 rebuilds the settings for
  each poll from the ledger's active directories (`settings_mut` or a new tracker), wires `submit_magnet` as
  bootstrap's `_submit_magnet_at_cloud` (`tracker/scenarios.rs` has the decision part: the row's directory or
  the first configured one, `任务已存在` counted as accepted, other CloudDrive failures `false`, a ledger
  failure returned as the error; WP9 adds the process-log lines, the "no offline directory is configured"
  refusal and `notify_submission`), and builds the tracker and that closure on clones of one
  `AsyncCloudDrive::scoped()` handle: after cancelling a poll (a burst cut short, shutdown) it awaits
  `AcquisitionTracker::settle()` before it touches the ledger, so a sent `DeleteFiles`, `RemoveOfflineFiles`
  or `AddOfflineFiles` has ended (D82). CloudDrive goes through `cloud::CloudListing` and `cloud::TrackerCloud`
  (`AsyncCloudDrive` implements both, and `intake::OfflineFiles`); `cloud::is_file_not_found` is Python's
  `except FileNotFoundError`. `manual::ManualIntakeSource` and `replace::ReplacementSource` are Python's
  dataclasses of factories; their errors carry `code()` (the API's answer) and Python's class names for the
  monitor API (WP9). WP8's playlists call `ManualIntakeSource::submit` with `SubmitOptions { source, limit:
  None, queue: true, on_entry }`. The `tracker.steps` operation (`operations/tracker.py`) holds the whole
  acquisition world (ledger, CloudDrive fake, mount, tracker, 换版, manual); WP8 and WP9 can add their steps to
  it rather than build another.

- WP8 left these for later packages. `rss::RssPipeline::new(RssParts { .. })` keeps the catalog answers
  (`cast_lookup`) on the pipeline as Python's object does, and bootstrap's `rss_runner` builds a new pipeline
  for every run: WP9 must do the same, or the answers outlive the run. `HttpFeedFetcher` implements
  `rss::FeedFetcher` and `SubscriptionRepository` implements `rss::SubscriptionStore`; WP9 implements
  `rss::CastLookup` and `rss::TalentLookup` over the AVBase client as bootstrap's lookups do, and reuses
  `testing::{FakeSubscriptions, SubscriptionSpec, make_subscription, feed_url}` (the Python tests' fake, under
  `test-support`) for `test_monitor_api.py` and `test_rss_category_routing.py`.
  `mapping::MappingPipeline::{new, with_fs}` is synchronous (`run_full`, `run_incremental(ctx, changed,
  deleted)` returning the failed paths): the watcher and the scheduler call it in `archive::blocking`.
  `playlist_sync::PlaylistSyncPipeline::new(store, source, emby, avid_of).with_clock(..)` takes
  `playlists::PlaylistStore`, `RankingSource` (implemented by `JinjierClient`) and `PlaylistServer` (implemented
  by `EmbyClient`); the playlists API (WP9's `playlists_api.py`, which also owns the `ManualIntakeSource::submit`
  call WP7 described) reads and toggles lists through `PlaylistRepository`. `EmbyClient::movie_index` now takes
  `&(dyn Fn(&str) -> String + Sync)`; `embyx_clients::jinjier::fixtures::make_database` builds the ranking
  database for tests (`test-support`). `fs::Fs` gained `rmdir`, `copy2` (`shutil.copy2`), `cmp_files`
  (`filecmp.cmp(shallow=False)`, no cache, D90), `mkdir_parents_new` and `glob_recursive`, and `RecordingFs`
  records `copy2` and `rmdir` and can list directories in name order; `PyOsError::classed` builds an `OSError`
  subclass from a message and `strerror()` is an accessor now (the error had to shrink under clippy's size
  limit). The Emby fake needed no state model: scripted `times = 1` routes give a run's listings in order.

- WP4 left these for later packages. The request models (`UpdateSectionRequest`, `TestConnectionRequest`) are
  validated in `embyx_api::body` against pydantic goldens (`goldens/api/request_models.json`) rather than through
  `embyx_core::config::lax`: they are two small models whose lax rules (a lax `int | None` with `ge=0`, a `dict`
  field) the section types do not use. WP9/WP12/WP14 request models either extend `lax` or follow `body.rs`;
  WP16 merges whichever two exist. `embyx_db::health::health_check` is Python's
  `PostgresFillActorRepository.health_check` (the readiness probe of `/api/health`), placed in `embyx-db` because
  the shell needs it before WP10; WP10 calls it instead of writing it again. A feature joins the app as Python's
  do: an `ApiRouter` (`routing::ApiRouter::route(method, path, openapi_json, handler)`), a
  `(name, FeatureHealthProbe)` and a `Lifespan`, wired in `bootstrap::build_app`; its operations leave
  `openapi::PENDING` in the same PR (the structural test fails otherwise). Feature errors are the handler's
  answer (`http::json(status, &http::error_envelope(code))`), an `Internal` is the 500 nothing handles. Handlers read bodies with
  `body::read_json` so the order body, auth, validation stays FastAPI's (`config_api.rs`). The transcode app is
  `create_transcode_app(ApiRouter::new("/api/transcode"))` with no size limit or `no-store`; WP14 adds its
  routes, its error handlers and its `RequestValidationError` answer. `tools/scenarios/operations/api.py` replays
  request lists through both apps; later API packages add specs under `specs/api/` rather than a new operation.
  `tools/smoke/run.sh` is the page smoke (CI, after the parity check); it seeds through the API only, so a page
  whose API arrives later shows its own error state and the smoke only checks that nothing throws.

- WP10 left these for later packages. `embyx_fill_actor::postgres_repository::PostgresFillActorRepository` is
  the only repository (D110): every Python method with its SQL, compare-and-set and lease rule copied, records
  validated on save and on read as `__post_init__` does, `RepositoryError::class_name()` and `is_value_error()`
  for the service's `except ValueError` (a `ValidationError` and the transition errors are ones). Its
  `health_check` is `embyx_db::health::health_check`. The ports (`ports.rs`) are object-safe traits returning
  `BoxFuture`; `adapters.rs` implements them over traits the clients implement (`AvbaseCatalog`,
  `JavBusCatalog`, `Intake`, `CloudDriveApi`), so WP11's service tests can use the same small fakes as
  `tests/adapters.rs`. `CloudFileMover` gained `scoped()` / `settle()` (D116): WP11 runs each execution's cloud
  work on `mover.scoped()` and awaits `settle()` after cancelling it, before it releases the lease. WP11's local
  moves should take WP6's `Arc<dyn Fs>`. `FillActorError` (`errors.rs`) carries the API codes for WP12. The
  `fill_actor/repository` scenario (`tools/scenarios/operations/fill_actor.py`) takes `{op, args}` steps named
  after the repository methods; WP11's recovery scenarios can seed rows through `database.sql` the way it does.
- Copies WP16 should fold together: `embyx_fill_actor::cloud_moves::splitlines` is a copy of
  `embyx_merge::py::splitlines`; `embyx_fill_actor::models::str_enum!` repeats `embyx_monitor`'s `str_enum!`
  (with `ALL` and `parse` returning `Option`). `embyx_fill_actor::pydatetime` (pydantic's `datetime`/`date` in
  JSON mode, D111) stays in the crate until a second crate needs it (WP12's request models, if any carry a
  datetime). `embyx_core::config::lax::Validator::finish_ignoring_extras` is the `extra='ignore'` end of a model
  validation, next to `finish` (`extra='forbid'`). `embyx_monitor::support::StoreError::is_unique_violation` is
  public now for the fill-actor repository.

## Checkpoint notes

- Checkpoint A (after WP4, 2026-11-14): the unmodified React front end, built with its own `npm run build`,
  runs against the Rust server (`tools/smoke`): the login gate refuses a wrong token and accepts the right
  one, all six pages open without an uncaught error, the settings page reads every section, saves Emby (version
  bump, address normalized, key kept and masked), runs the Emby connection test against a stand-in, and a
  reloaded deep link is served by the SPA fallback; SIGTERM ends the server with its shutdown lines and exit 0.
  The shell's HTTP surface (contract 8) matches Python's recorded transcript (status, contract headers with
  `content-length`, bodies) over 140 requests (112 + 4 + 24)
  (`goldens/scenarios/api/`), the `/openapi.json` structure matches for the 7 ported operations with 45 pending
  (WP9, WP12, WP14). Pages other than settings still show their error states, since their APIs are not ported.

## Experiment log

One row per merged work package. "Rework" counts pushes after the PR was opened that fixed something CI or
the independent review found; review findings are summarised with a link to the PR comment.

| WP | PR | Time spent | Rework | Independent review findings | Notes |
| --- | --- | --- | --- | --- | --- |
| WP0 | #81 | 2 h (18:30Z-20:30Z), plus the offline fingerprint work added at 19:23Z | 1 push (review fixes); CI green first time | 0 high, 3 medium (unsafe guard missed multi-line attributes; placeholders in API-COVERAGE; three WP4 tests needing fill-actor code), 7 low, nits: all fixed, except parity's bare-name claim check (deferred to WP15, Cleanup notes) and the live probe's per-request sessions (documented in API-COVERAGE) | Started 2026-11-13 18:30Z. Probes: sqlx's dropped lock wait keeps the lock (port cancels with `pg_cancel_backend`); croner matches cronsim once evaluated in the fixed offset `astimezone()` gives; wreq's ClientHello matches curl_cffi's (Chrome 149 vs 150 sig-alg difference), headers need configuring; scraper differs from lxml on implied `tbody`, bare `<tr>` rows and XML-parsed upper-case tags. Rust and Python render the harness transcript byte for byte. Live run from nas-us-2 (follow-up PR): both wreq candidates got AVBase, `httpx-rs` was challenged; JavBus served every client |
| WP1 | #83 | ~1 h 30 min (20:10Z-21:40Z), review fixes included | 1 push (review fixes); CI green first time | 0 blocker, 1 major (a dropped advisory-lock wait queued its cancel on the pool, so waiters holding every connection were never cancelled: now a connection of its own, test with a full pool), 4 minor (magnet regex without the `İ`/`ı` rewrite; `int()` skipping U+001C-U+001F; JSON-mode error order and `bool_type` for big numbers; DSN scheme unchecked, leaving a scheme-less URL's password in `Debug`), 6 nits: all fixed with Python-derived goldens, except non-UTF-8 env values and the 4300-digit `int()` limit (D19, D20) and a weak test (replaced) | Started 2026-11-13 ~20:10Z. 70 tests / 996 cases ported, 0 waived. Both AVID corpora and ~190 corners equal; 442 + 55 pydantic validation cases equal (type, loc, msg) except D10 / D12; four transcripts (schema from the catalog, v4 upgrade through the data migrations, newer-schema refusal, config store) byte for byte; local `pg_dump --schema-only` of a Python- and a Rust-migrated database identical. Found while porting: Python's ignorecase lets `İ`/`ı` match `i` and `[a-z]` (D15, AVID patterns rewritten); `int(x, 16)` accepts a `0x` prefix in a 40-char btih; pydantic writes 1e-05 as `0.00001` but 1e-06 as `1e-6`; `casefold()` turns `ß` and the `ﬀ`-`ﬆ` ligatures into ASCII (settings booleans and `localhost`) |
| WP2 | #85 | ~1 h 45 min (21:28Z-23:10Z), review fixes included | 1 push (review fixes); CI green first time | 0 blocker, 1 major (jinjier refused a whole database for a NULL in a row Python never reads: names and notes are now checked exactly where Python reads them, D30 reworded), 4 minor (AVBase `json()`/`text` ignored curl_cffi's charset rules; AVBase transport classes collapsed to four, now chosen by the libcurl code curl_cffi would give, checked on a local server; nyaapy's `ValueError` for an XML declaration with an encoding; the Emby round-trip test lacked the delete and empty-edit assertions), 5 nits (jinjier regexes now through `pyre`; full Unicode case folding for AVBase work IDs; exact int/float comparison in sukebei's trusted pick; D1 updated for `PoolTimeout` and the texts that reach users; JavBus `test_scrape` actor-id check left implicit): all fixed except the last | Started 2026-11-13 21:28Z. 58 tests / 85 cases ported, 0 waived. Seven golden files (`goldens/clients/`, 280+ cases: sizes, JavBus pages, magnet tables, nyaapy rows and choices, AVBase dates/queries/feeds/payloads, jinjier codes and kinds, feeds and links) equal; they caught `date.fromisoformat` ignoring what follows a complete 10-byte date (`20260219T0`). pyquery over html5ever needed libxml2's tree rebuilt (implied elements unwrapped, bare table parts kept in a synthetic table). TLS probe on the configured clients: AVBase equal to curl_cffi but for the ML-DSA signature algorithms, JavBus's head equal to httpx's (Accept-Encoding `gzip, deflate`, D25) |
| WP3 | #84 | ~1 h (21:28Z-22:30Z), review fixes included | 1 push (review fixes); CI green first time | 0 blocker, 1 major (a dropped caller finishes before its RPC, where Python's cancelled caller waits: added `AsyncCloudDrive::scoped()` / `settle()` for code that cancels and then acts, D41, and the ported test now asserts the wait), 3 minor (a `close()` racing a new call could return before that call was admitted: admission token taken before the closed check, with a race test; `TypeError` text for indexing a string; NaN/infinity in the template now refused like httpx), 3 nits (D43 widened to the lenient reads of answers Kubernetes does not send and the UTF-8 error text; the fake refuses an unknown conflict policy as Python's does): all fixed or documented | Started 2026-11-13 21:28Z. 21 tests / 31 cases ported, 0 waived. grpcio's `str(exc)` for server statuses is `<_InactiveRpcError ...>` with `debug_error_string = "<CODE>:<details>"`, reproduced byte for byte; transport failures differ (D40). A plain tonic call dropped mid-flight is reset and the server never finishes it (control test), which is why every call runs in a tracked task. Python's `aclose()` and config swap cancel in-flight RPCs (D41, fixed). Six transcripts equal byte for byte, among them a cancelled move that still shows its response. Notes for later WPs: `CloudDriveClient::new` needs a Tokio runtime (lazy channel); WP4 awaits `close()` of the old client on a config swap and at shutdown; WP11/WP14 cancel through `scoped()` + `settle()`; WP14 suppresses with `KubeJobsError::is_kube()` |
| WP4 | #88 | ~1 h 30 min (23:36Z-01:05Z), review fixes included | 1 push (review fixes); CI green first time | 0 blocker, 0 major, 5 minor (D57 misdescribed Python: a failed lifespan or bind exits 3 in Python too, only the after-signal status differs, now reworded; a port past 65535 bound a random port, now Python's `OverflowError` and exit 1; the two ported connection tests did not assert the throwaway client's close, now seen through a proxy and a kept-alive stand-in, token compared case-sensitively; the OpenAPI diff dropped schema properties named `title`/`description`; D56 left out the unbounded request drain), 10 nits (Starlette's `$` matching before a final newline, now emulated with scenario cases; signals caught only after the binds and no `force_exit`, now installed before the lifespans with a second signal ending the drain; health-probe error mapping, bind error text, the image's `mime.types`, D41 and Checkpoint A wording, a doc comment and a test name, all documented or fixed). Not changed: static files run `realpath`/`stat` on the async workers and read a whole file per response, which is fine for the front end's build | Started 2026-11-13 23:36Z. 8 tests ported, 0 waived; `test_bootstrap_wires_repository_api_and_shutdown` moved to WP12 (fill-actor assertions). Starlette's dispatcher, StaticFiles and FileResponse are ported rule for rule and checked by 140 recorded requests through both apps plus 71 goldens; the scenario's stored CloudDrive address had to be pinned, since the fake's port made the dumped row unstable. The page smoke (`tools/smoke`) runs the unmodified front end in CI. Notes for later WPs are in Cleanup notes (how a feature joins the app, `PENDING`, request bodies, the API scenario operation) |
| WP5 | #87 | ~1 h (23:12Z-00:10Z), review fixes included | 1 push (review fixes); CI green first time | 0 blocker, 2 major (a retry run-log line lost its trailing word `in`: fixed, the ported test now asserts the tail; database failures carried sqlx's `error returned from database: ` prefix into `repr(exc)`: now PostgreSQL's message as asyncpg words it, with a test on a real unique violation), 1 minor (`add_attempts`' out-of-range size used `execute`'s text where Python's `executemany` adds `in element #N`: fixed, D61), 4 nits (a stale D number in a comment; the fake ledger now slices like Python for negative limits and refuses a size it cannot hold instead of dropping it; the text-only retry predicate now knows the `socket`/`ssl`/`io` `OSError` subclasses, D63): all fixed | Started 2026-11-13 23:12Z. 93 tests / 97 cases ported, 0 waived. Six goldens (`goldens/monitor/`: run-context call replays incl. the 200-line tail and 50-error cap, a 146-point cadence grid with `datetime` overflow edges, both edge tables with every refusal text, 28 feed URLs, cursors, `str(FileOperationResult)` escaping, `resolve()` over stub sources) and the `intake/steps` transcript (ledger rows, attempts and `pipeline_runs` bytes) equal byte for byte. Found while porting: Python's `FakeLedger.due_for_retry` skips `discovered` rows the real query returns; a `'任务已存在'` RPC error is still retried three times before it counts as a duplicate; `uuid4().hex` run ids escape the transcript's UUID normalization (marked volatile). Notes for later WPs: see Cleanup notes (WP5) |
| WP6 | #89 | ~1 h (00:12Z-01:15Z), review fixes included | 1 push (review fixes); CI green first time | 0 blocker, 0 major, 3 minor (rmtree listed each directory by opening `.` again where `os.scandir(fd)` reads a duplicate descriptor, so an empty directory without search permission stopped a discard as a regular user: now `fdopendir(dup)`'s way, tested as `nobody`; lossy non-UTF-8 names could make two different videos look like a copy or variant pair and delete one: such folders now need attention, and promotion keeps a name's bytes, D72; no transcript ran a real discard through links into the library or a promotion failure: new `archive/links_and_promotion`), 2 nits (`mkdir(parents=True)` retry raising `FileNotFoundError` as is; a NUL byte in a path, documented in D70): all fixed | Started 2026-11-14 ~00:12Z. 69 tests ported, 0 waived, plus 8 `fs` tests with texts read off Python 3.13 and 2 non-UTF-8 safety tests. Eleven effect transcripts, written in Python first, equal byte for byte: ordered file calls with their errors, final tree, ledger rows; they cover every archive outcome, collisions (existing destination, promotion target taken, copies, hard and symbolic links), priority routes, full runs, injected rename/rglob/rmtree/mkdir/stat/unlink failures, stops after a given call, reconcile and move-in passes. Found while porting: `shutil.rmtree` names the walked directory, not the entry, when an unlink fails (its `onexc` re-raises inside the walk's own `try`); a symbolic link to a folder is filed from but then refused by `rmtree` (`[Errno None] None: PosixPath(...)`); Python's archive moves with exists-then-rename, not `RENAME_NOREPLACE` (kept; Cleanup notes). Notes for later WPs: see Cleanup notes (WP6) |
| WP7 | #91 | ~1 h 30 min (~01:15Z-02:45Z), review fixes included | 1 push (review fixes); CI green first time | 0 blocker, 1 major (the lossy `replace_paths` of an old copy whose name is not UTF-8 matched no file, so the old copy counted as gone and the new one was filed beside it, two copies of one title; the `NotUtf8` branch D81 described could not run: now `replaced_copies_present` counts such a file without comparing lossy text, never asks CloudDrive to delete it, and the outcome is Python's FAILED-every-poll, tested end to end, D81 rewritten), 2 minor (`settle()` covers `submit_magnet`'s `AddOfflineFiles` only when the closure shares the scoped handle: documented for WP9; `SubmitMagnet` returned a bare `bool`, so a ledger failure in the application's closure could not end the poll as in Python: now `Result<bool, LedgerError>`), 1 coverage gap (no transcript for a refused `DeleteFiles`, an unlistable old-copy directory or a library outside the mount: new `replace/failures` and `replace/outside`), D82 wording nits: all fixed | 57 tests ported, 0 waived, plus 8 Rust-only (helpers, a listing failure, a stop, the non-UTF-8 old copy, `settle()` after a dropped poll with `DeleteFiles` held at the fake). Nine effect transcripts, written in Python first, equal byte for byte: tracker outcomes (finished, junk, error, stalled, lost, orphaned, grace, progress, waiting for the mount), failures and cancellation; 换版 filing (old copy deleted only once the new one files as that AVID, filing waits for the mount), failures, outside the mount, a poll cancelled right after `DeleteFiles`, plan/start with refusals and a cancelled start; the manual intake end to end with a batch cancelled midway. Found while porting: the baseline joins attempts in PostgreSQL row order (D80); a 换版 start or a manual line cancelled right after `AddOfflineFiles` leaves the row `discovered` with its attempt submitted, which the next poll moves to `downloading` while the row stays `discovered` (both builds). Notes for later WPs: see Cleanup notes (WP7) |
| WP10 | #90 | ~1 h 20 min (01:10Z-02:30Z), review fixes included | 2 pushes (PARITY.md refresh, review fixes) | 0 blocker, 0 major, 2 minor (a non-UTF-8 candidate path was a `ValueError` of this port's where asyncpg raises `DataError` naming the argument and the surrogate escapes: the text is now rebuilt byte for byte, D113 rewritten, unit test with texts from Python; the repository checked its own rules before a record's `__post_init__` ones, so a record breaking both gave the other message: records now validate first, in Python's construction order, with scenario steps and a repository test), 5 nits (`cancel_job`'s impossible states are `RuntimeError`; the JavBus warning no longer logs the error, as Python's does not; a scoped mover drops handles once settled and the trait says to take one per unit of work; PLAN placeholders): all fixed | Started 2026-11-14 ~01:10Z. 35 tests / 51 cases ported, 0 waived, plus 5 tests of this port's (gateway outcomes, write-time range, scoped mover settling a dropped move, record-before-repository order, asyncpg's text for a non-UTF-8 path). Four goldens (models, cloud moves, adapters, persistence values) and the `fill_actor/repository` transcript (about 60 repository calls over rows seeded as Python wrote them, rows dumped after) equal byte for byte; plans, results, id lists and CloudDrive snapshots are stored as Python's JSON. The in-memory repository is not ported (D110). Notes for later WPs: see Cleanup notes (WP10); WP11 takes one `CloudFileMover::scoped()` per execution and awaits `settle()` before releasing the lease (D116) |
| WP13 | #86 | ~1 h 15 min (22:30Z-23:45Z), review fixes included | 1 push (review fixes); CI green first time | 0 blocker, 0 major, 2 minor (a NaN in a stream property made Rust refuse a merge Python completes, since `json.loads` gives every NaN one object and tuples compare by identity first: fixed, golden `merge_nan_in_signature`; argparse's prefix options, repeated options and `--reserve-gib -1` refused by clap: now accepted, D145), 3 nits (`sum` kept a leading `-0.0`: fixed; D141 widened to the `.part` removal and cgroup decode texts; a thread that could not start left ffmpeg running: now killed, reaped and raised as `RuntimeError`), coverage gaps closed with goldens (a failing reorder remux, ffprobe missing, symlinked and dangling entries in the scan) | 24 tests / 26 cases ported, 0 waived (two titles-endpoint tests moved to WP14). 63 recorded worker runs (47 merge, 16 home) replay Python's `worker.run` call for call: argv, concat scripts, every status write, exit code, stderr or the traceback's last line, files left. Real ffmpeg: concat, reorder, refusal, AV1, home prepare and assemble give the same status sequence, SHA-1 and size in both (SVT-AV1 output differs run to run, so only the encode's bytes are left out). `merge/tasks` transcript byte for byte, `parts_json` included. The binary leaves `embyx-merge`'s `db` feature out to keep building on Windows (D144). Notes for later WPs: WP14 uses `MergeTaskRepository` with typed `TaskField`s, `worker::read_status`, the `transcode` path helpers and `detect::{scan_multipart, resolve_source, source_for_task_dir}`, and puts the service behind `db`; WP15 chooses between `SystemExec` and `SystemRunner` and the binary's features |
