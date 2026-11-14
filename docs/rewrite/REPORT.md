# embyx-manager Rust rewrite: final report data

The data behind the final report of the rewrite (`docs/rewrite/PLAN.md`), written at the end of WP16 on
2026-11-14; the level 2 results were added the same day (#102), after Max allowed the export. The
rewrite ran from Max's approval on 2026-11-13 18:22Z to WP16's last merge, about 16 hours of wall-clock
time, in 17 work packages and 21 pull requests into `rust` (#81-#101). Nothing was deployed.
A copy of this file is at `/mnt/project-files/embyx-rust/report-data.md`.

## Where it stands

- **Scope.** Full parity with the baseline (`5a3c083`, `main` and the deployed commit): `serve` (API, every
  pipeline, the fill actor, the merge service and its transcode listener), `merge-worker` and
  `transcode-worker`, in one binary. The decided drops: the
  in-memory fill-actor repository (D110), `/docs` with `/redoc` (D50), and the one-off `import-config`
  command (no D row; named in the introduction of `docs/DIVERGENCES.md` and in PLAN.md).
- **`main` catch-up.** `main` is still at the baseline (rechecked 2026-11-14 10:17Z, before the last PR), so there is nothing to
  port.
- **Level 1.** Complete and green: locally on cd657f6, the last commit that changed crate code, and in CI
  on 6c5120b (#100's review fixes: tools, `Dockerfile.rust`, `rust.yml` and docs, no crate code), merged as
  cf10954. #101 adds only this report and PLAN.md. #102 (level 2) changes the tools, the docs and one
  test's lease in `embyx-fill-actor`, no product code; its CI ran the whole suite again.
  - parity 747 of 747 Python test functions and 1,754 of 1,754 cases, 0 waived;
  - `openapi::PENDING` and the smoke's `NOT_YET_PORTED` empty;
  - goldens and transcripts equal byte for byte;
  - both listeners' `/openapi.json` match FastAPI's;
  - the React build passes its tests (121; the plan counted 117) and the page smoke;
  - an empty `pg_dump --schema-only` diff;
  - the cross-version tests in every pairing.
- **Level 2.** Run on production data on 2026-11-14, after WP16, once Max allowed the export (17:23Z).
  During WP16 the tooling had only seen synthetic data (101 of 101 GETs, 77 of 77 and 52 of 52 names).
  - **Export.** `mask.py export` ran on the nas-us host over a read-only connection
    (`default_transaction_read_only=on`): 19 tables, 23,339 rows, schema version 19. Two values were
    masked (`clouddrive.api_token`, `emby.api_key`). `mask.py check` found nothing, and a second,
    independent check found none of the 4 known secret values (the API token, the database password and
    the two above) in the 39 MB written. Only the masked export left the cluster; the host's staging
    folder was deleted right after the transfer.
  - **GET differ.** The GET routes of the main API, with the item routes for every stored acquisition
    (4,712, each also as a replace plan), run (8,383), playlist (70, `missing` and `fill`) and apply job
    (18): 18,005 requests, 18,005 answered alike in status, content type and body. Not requested:
    `/openapi.json` (level 1 compares it), the page `/` (skipped: this build had no front end beside the
    binary) and the transcode listener's GET. Six keys that hold the time of the request are dropped
    before comparing (`get_diff.py` `DEFAULT_IGNORED`).
    - *Rows read through both builds:* the 9 config sections, the 4,712 acquisitions with their 9,514
      magnet attempts, the 8,383 runs, the 292 subscriptions, the 70 playlists, the 18 apply jobs with
      their job rows, and of the 265 merge tasks the open ones and the 20 most recently finished (what
      `GET /api/merge/tasks` lists). None was refused by the Rust build (D62, D92, and D112 for the apply
      jobs' `candidate_ids_json`).
    - *Rows not read:* the other finished merge tasks, and the 38 `fill_actor_jobs` rows that no apply
      job points to (plan jobs). The tool asked the plan route for the ids in `fill_actor_plans`, which
      held none (plans expire); a plan job's row, with the `actor_ids_json` that D112 is also about, is
      served by the same route under its job id. The tool asks for those now, but the export was
      already deleted. Eight tables were empty.
    - *Answers that say less than a production server's would:* the 4,712 replace plans found no
      holdings, as the replay machine has no library, so they compare the AVID's parse, the row's state
      and its task directory, not the holdings; the 70 `fill` answers are the 404 of a server that has
      run no fill; and `/api/merge/titles` scanned roots that are not there.
    - *Statuses (Python):* 17,927 with 200; 75 with 404 (the 70 `fill` answers and 5 requests built to
      miss); 2 with 500, the CloudDrive directory listing, which has no CloudDrive to reach here; 1 with
      422. That request was not noted before the reports were deleted; of the requests sent, only a
      replace plan for a stored AVID the parser cannot read answers 422 (`unreadable_avid`). Rust
      answered each alike.
    - *Clock values:* 37 values in 19 answers are read off the server's clock (`scanned_at` of the merge
      titles, `elapsed_seconds` and `last_progress_seconds` of the 18 apply jobs). A first run of 158
      requests, which held all 19, compared them exactly: on every one Rust was 4 or 5 seconds later,
      in the same format, which is the time between the two runs. The full run then took them as equal
      within 327.6 seconds, the span of both runs: looser than it should have been (#102's review). The
      tool now takes them as equal only when the Rust value is later by the time between the two
      requests (for the scan time, done at start-up: between the two servers' start-to-request
      stretches), give or take 1 second, in one type, one text shape and one zone, and lists each value
      it let pass; that rule ran on synthetic data only.
  - **Decision replay.** AVID, brand and archive routing of every name the export holds: 16,572 of 16,572
    decided alike.
  - **Not done.** The plan's second half of level 2, a pass of each pipeline over a tree rebuilt from
    listings of the library and intake, has no tooling, and no listing was taken. `find_dst_dir` reads only
    the config, so the routing replay does not need one.
  - **Clean-up.** The export, both scratch databases and every report derived from it were deleted from
    the cloud session the same day. The session's own log keeps the lines it printed: counts, config key
    names, and the ids and clock values of the 18 apply jobs.
- **Level 3.** One capped, anonymous, read-only run against the real sites from this cloud container
  (besides WP0's AVBase fingerprint probe, run once from nas-us-2 on Max's approval, #82):
  - 8 of 11 reads were equal;
  - sukebei differed only in live seeder and leecher counts;
  - an AVBase work read exposed a connection-reuse difference, now fixed (D175);
  - RSSHub refused both builds, so the feed is inconclusive from this network.
- **Image.** `Dockerfile.rust` builds in CI on every PR (build only, never pushed). It is 800 MB
  uncompressed (215 MB compressed) against Python's 947 MB (249 MB).
- **Memory.** Idle RSS is 25.5 MiB against Python's 105.8 MiB on the same run, which meets the plan's goal
  of under 50 MiB. The highest RSS over a full workload was 43.0 MiB against 123.1 MiB. The merge worker
  peaked at 18.0 MiB against 40.6 MiB, about 3 MiB over the plan's 15 MiB goal.

## Code size

Lines, comments and docs included. Rust test code is everything under `tests/`, in `tests.rs` files and in
trailing `#[cfg(test)]` modules. Python's 9,370 lines of generated protobuf code are left out; tonic's
generated code is not in the tree. Each Python file is counted in one row. The Rust counts leave out the
crates' `examples/` and `build.rs` (810 lines: the level 2 and 3 tools and the proto build).

| Crate | Rust source | Rust tests | Python counterpart (source lines) |
| --- | --- | --- | --- |
| embyx-core (settings, config models, AVID, magnets, Python-semantics helpers) | 6,599 | 2,022 | `config/` without its API, `core/`, `settings.py`, `errors.py` (1,157) |
| embyx-db (pool, migrations, locks, health) | 1,942 | 1,145 | `db.py`, `locking.py` (763) |
| embyx-clients (JavBus, sukebei, AVBase, jinjier, feeds, Emby, CloudDrive, Kubernetes) | 10,537 | 6,661 | `clients/` without generated code, `merge/kube.py` (2,132) |
| embyx-monitor (archive, reconcile, move-in, tracker, intake, RSS, mapping, playlists, scheduler) | 15,634 | 12,758 | `monitor/` without `api.py` and `playlists_api.py` (6,126) |
| embyx-fill-actor (plans, apply, cloud moves, recovery, jobs) | 10,413 | 9,137 | `fill_actor/` without `api.py` (5,427) |
| embyx-merge (merge worker, detect, tasks, merge service, auto-merge) | 7,486 | 5,379 | `merge/` without `api.py`, `transcode_api.py`, `home_worker.py` and `kube.py` (3,125) |
| embyx-transcode-worker (home worker) | 5,562 | 1,654 | `merge/home_worker.py` (1,141) |
| embyx-api (HTTP shell, every router, bootstrap, transcode listener) | 14,885 | 8,401 | `api.py`, `bootstrap.py`, `adapters.py` and the six API modules of `config/`, `monitor/`, `fill_actor/` and `merge/` (3,424) |
| embyx-manager (the binary) | 204 | 156 | `__main__.py` (175) |
| Total | 73,262 | 47,313 | 23,470 (tests: 18,715) |

The Rust source is about 3.1 times the Python, as fav's was. The causes:

- the Python-semantics layer that keeps error texts, JSON bytes and pydantic's validation identical
  (`embyx_core::{pyjson, pyre, pyos, pyfloat, pytext, config::lax}`);
- explicit types where Python used dicts;
- per-function doc comments naming the Python origin.

The goldens and transcripts that pin this behaviour come from the Python tree, not the porting sessions.

## Tests, parity, goldens and transcripts

| Check | Result (cd657f6 locally, 6c5120b in CI) |
| --- | --- |
| Rust tests (workspace, Linux) | 1,231 passed, 0 failed, 0 ignored |
| Rust tests (Windows job: worker crate and binary, real ffmpeg) | pass (CI) |
| Python tests (baseline suite, unchanged) | 1,745 passed, 9 xfailed (1,754 cases) |
| Test parity (`tools/parity/parity.py`, crate-aware listing) | 747 / 747 functions, 1,754 / 1,754 cases, 0 waived |
| Goldens (`tools/goldens/generate.py --check`, outputs of the baseline's own code) | 82 golden files equal |
| Effect transcripts (`tools/scenarios/runner.py --check`, Python first, then Rust byte for byte, with the resume matrix) | 63 scenarios, 0 problems |
| `/openapi.json`, main and transcode listeners, structural diff against FastAPI's | equal; `openapi::PENDING` empty |
| React front end, unmodified: lint, tests, build; page smoke against the Rust server | lint clean, 121 tests in 11 files pass (the plan counted 117), build passes; page smoke passes (six pages, settings round trip, SPA fallback, exit 0 on SIGTERM) |
| Schema: both builds migrate an empty database, `pg_dump --schema-only` diff | empty (720 lines each) |
| Cross-version: Python worker against the Rust listener, Rust worker against the Python listener and Rust against Rust (contract 5); Rust and Python merge workers on the same parts (real ffmpeg) | pass (part of the workspace tests) |
| Tool self-tests (`tools/tests`) | CI: 44 passed and 1 skipped in the python-tools job; the skipped Rust replay comparison runs in the rust job (6 passed there). Locally on cd657f6: 43 passed, 1 skipped. #102 added three differ tests |
| Tree check: Python tree byte-identical to the baseline, no ignored test, `unsafe` confined | pass |

## Verification level per pipeline

Level 1 is offline and in CI. Level 2 means replay of production data: the GET differ and the decision
replay ran on a masked export of the production database on 2026-11-14 (see "Where it stands"). Level 3
means live reads of the anonymous sites.

| Area | Level 1 | Level 2 (run on the production export) | Level 3 |
| --- | --- | --- | --- |
| Settings, config sections, validation, masking | tests, goldens for all 9 sections | GET differ (`/api/config/*`) | n/a |
| AVID parsing, brands, magnets | tests, goldens (the corpus TSVs) | decision replay | n/a |
| JavBus, sukebei, AVBase, jinjier, RSS/Atom parsers | tests, goldens over recorded pages, TLS fingerprint probe (AVBase, also run live from nas-us-2 in WP0, #82) | n/a | JavBus 3/3 equal; sukebei equal except live counts; AVBase build id and talent equal, work read fixed (D175); jinjier 3/3 equal; RSSHub inconclusive |
| CloudDrive (gRPC) | tests against fakes, transcripts of every mutation | n/a | not allowed (no live CloudDrive) |
| Emby (playlists) | tests, transcripts against a stand-in | n/a | not allowed |
| Kubernetes Jobs | tests, goldens of the built Job | n/a | not allowed |
| Archive, reconcile, move-in | tests, transcripts of each outcome | replay (routing), GET differ (runs, ledger) | n/a |
| Tracker, 换版, manual intake, RSS intake | tests, transcripts | GET differ (acquisitions, replace) | n/a |
| `.strm` mapping, playlists, playlist sync | tests, transcripts (full and incremental) | GET differ (playlists, missing, fill) | n/a |
| Scheduler, monitor API | tests, transcripts of the API | GET differ (status, tracker, runs) | n/a |
| Fill actor: plans, apply (local and cloud), recovery, jobs, API | tests, transcripts from every journal state | GET differ (apply jobs; no plan was stored, and the plan jobs were not asked for) | n/a |
| Merge worker | tests, goldens of every ffmpeg/ffprobe argument list, real-ffmpeg cross-check against the Python worker (CI's 6.1, the image's 7.1.5) | n/a | n/a |
| Merge service, auto-merge, merge API | tests, transcripts with cancel and retry at each state | GET differ (tasks, titles) | n/a |
| Transcode listener and home worker | tests, protocol tests in every pairing with real ffmpeg, Windows job with real ffmpeg | n/a | not allowed (no live worker) |
| HTTP shell and front end | contract-8 transcript, OpenAPI diff, front-end tests, page smoke | GET differ (the API routes; the page was skipped) | n/a |
| Database schema | catalog transcript and `pg_dump` diff | load of an export | n/a |

What no level covers: CloudDrive, Emby and Kubernetes behaviour beyond what the fakes record; the real 115
upload path; a real title on the home 5090; and the write paths on production data (level 2 compared
reads and filing decisions; no pipeline ran on production listings).

## Divergences D1-D175

`docs/DIVERGENCES.md` has 108 entries:

- 55 observable;
- 46 mechanism (the same behaviour reached another way);
- 5 Python bugs (four fixed, one copied);
- 2 dropped.

"Visible" means a user, the front end, an API client or an operator reading logs or stored data could
tell the difference. Most visible entries only show on inputs production does not produce: integers past
64 bits, lone surrogates, names that are not UTF-8, or hand-edited rows. The ones Max might notice in
normal use:

- The server exits 0 after SIGTERM (D57).
- `/docs` and `/redoc` are gone (D50).
- Log lines carry no Python traceback frames (D52, D60, D127); the merge worker's unhandled errors print
  only the traceback's header and last line (D141).
- AVBase resends a GET once when a reused connection dropped, as libcurl does (D175).
- At shutdown the server waits up to 45 s for CloudDrive calls in flight instead of cancelling them
  (D41, D56).
- The tracker polls attempts in info-hash order (D80).
- Mapping no longer keeps a stale comparison cache (D90).
- The home worker's error texts and User-Agent differ (D170), and a probe timeout no longer prints the
  token (D168).

| ID | WP | Kind | Visible or internal | What differs |
| --- | --- | --- | --- | --- |
| D1 | WP0 | observable | visible (error text) | Client error class names follow httpx (`ConnectTimeout`, `ReadTimeout`, ...), mapped from the Rust client's errors |
| D2 | WP0 | observable | visible (edge input) | Python `re` semantics emulated on `regex`/`fancy-regex`; Unicode corners of `\w`/`\b`/case folding can differ |
| D3 | WP0 | mechanism | internal | Async subprocess runner from fav: removed in WP16 (D140 and D165 are the runners that shipped) |
| D4 | WP0 | observable | visible (edge input) | JSON integers beyond i128 and lone surrogate escapes are refused instead of read |
| D5 | WP0 | mechanism | internal | Same request head in the same order; hyper writes header names title-cased |
| D10 | WP1 | observable | visible (edge input) | Config integers beyond i64 are refused with pydantic's size error |
| D11 | WP1 | observable | visible (error text) | Validation error text keeps pydantic's layout without the input value and doc URL |
| D12 | WP1 | observable | visible (edge input) | A repeated unknown config key is reported once, not once per occurrence |
| D13 | WP1 | observable | visible (edge input) | Integer env settings beyond u64 fail at start-up with Python-like text |
| D14 | WP1 | observable | visible (config) | AVID ignore patterns are compiled by the Rust engine; Python's refusals are copied, an unsupported construct is refused |
| D15 | WP1 | mechanism | internal | Case-insensitive matching rewritten so `İ`/`ı` behave as Python's per-code-point lower-casing |
| D16 | WP1 | mechanism | internal | DSN parsing by sqlx instead of asyncpg, same accepted forms |
| D17 | WP1 | mechanism | internal | Advisory-lock waits cancelled with `pg_cancel_backend` on a connection of their own |
| D18 | WP1 | observable | visible (edge input) | Upper-casing follows the toolchain's Unicode version, not Python 3.13's 15.1 |
| D19 | WP1 | observable | visible (edge input) | A non-UTF-8 environment value is decoded lossily (U+FFFD) |
| D20 | WP1 | observable | visible (edge input) | AVID numbers over 4,300 digits are normalized instead of raising |
| D25 | WP2 | mechanism | internal | `Accept-Encoding: gzip, deflate`, as the baseline's httpx sends |
| D26 | WP2 | mechanism | internal | XML parsed with roxmltree and HTML with html5ever (scraper) instead of lxml, checked on recorded pages |
| D27 | WP2 | observable | visible (error text) | RSS/XML syntax errors: same class, roxmltree's position text instead of expat's |
| D28 | WP2 | observable | visible (edge input) | `parse_size` saturates at i128::MAX (about 1.7e38 bytes) |
| D29 | WP2 | observable | visible (error text) | AVBase failures: curl_cffi's class by libcurl code, Rust's own message text |
| D30 | WP2 | python-bug | visible (fixed) | jinjier: an empty or odd ranking archive is a clean `JinjierError` instead of `MemoryError`/`TypeError` |
| D31 | WP2 | observable | visible (logs) | Client log lines are `tracing` events with the same text and logger names |
| D40 | WP3 | observable | visible (error text) | CloudDrive gRPC errors keep grpcio's layout and class names; transport details are tonic's |
| D41 | WP3 | python-bug | visible (fixed) | A CloudDrive call already sent always runs to its end when the client is closed (Python cancels it) |
| D42 | WP3 | mechanism | internal | CloudDrive gRPC connects directly (proxy variables ignored, none is set in the pod); TLS with rustls and Mozilla roots |
| D43 | WP3 | observable | visible (error text) | Kubernetes Job client errors keep the classes and texts the merge service matches; the rest are Rust's |
| D50 | WP4 | dropped | visible | `/docs` and `/redoc` (Swagger) are not served |
| D51 | WP4 | mechanism | internal | `/openapi.json` assembled from tables and diffed against FastAPI's |
| D52 | WP4 | observable | visible (logs) | Server log lines keep uvicorn's prefixes; no traceback, never coloured |
| D53 | WP4 | mechanism | internal | Static media types from Python's built-in `mimetypes` table only |
| D54 | WP4 | observable | visible (edge input) | A config `version` beyond int4 answers 409 without a database read |
| D55 | WP4 | observable | visible (edge input) | Request bodies with lone surrogates are refused as invalid JSON (422) |
| D56 | WP4 | python-bug | visible (fixed) | At shutdown the CloudDrive client waits up to 45 s for calls in flight instead of cancelling them |
| D57 | WP4 | observable | visible (exit status) | After SIGTERM/SIGINT the server exits 0 (uvicorn re-raised the signal: 143/130) |
| D58 | WP4 | observable | visible (platform) | `serve` is built on Unix only; the Windows exe has the workers |
| D59 | WP4 | mechanism | internal | Front end found at `<exe dir>/static` |
| D60 | WP5 | observable | visible (logs) | Stored run logs are byte for byte Python's; the process log has no traceback |
| D61 | WP5 | observable | visible (error text) | Database errors in run logs are worded as asyncpg's classes by SQLSTATE |
| D62 | WP5 | observable | visible (hand-edited rows) | A stored JSON column of the wrong shape is refused instead of misread (the level 2 differ showed it as a 500) |
| D63 | WP5 | mechanism | internal | Work ids case-folded with the `caseless` crate's tables |
| D70 | WP6 | mechanism | internal | Archive file system behind an `Fs` trait for the fakes |
| D71 | WP6 | mechanism | internal | Reconcile and move-in file work on blocking threads |
| D72 | WP6 | observable | visible (non-UTF-8 names) | Non-UTF-8 file names are read lossily for AVIDs and logs; such folders need attention instead of risking a wrong match |
| D73 | WP6 | mechanism | internal | Per-folder error containment as `Result`s instead of `except Exception` |
| D80 | WP7 | observable | visible (order) | The tracker advances attempts in info-hash order instead of PostgreSQL's row order |
| D81 | WP7 | observable | visible (non-UTF-8 names) | A replacement whose old copy's name is not UTF-8 fails every poll, as in Python, without a lossy match |
| D82 | WP7 | mechanism | internal | Tracker and sources take a CloudDrive trait instead of duck typing |
| D90 | WP8 | python-bug | visible (fixed) | Mapping compares a mirror with its source every time (no stale `filecmp` cache) |
| D91 | WP8 | mechanism | internal | Mapping on byte paths; non-UTF-8 names handled without surrogates |
| D92 | WP8 | observable | visible (hand-edited rows) | Playlist entries read back only in the shape the app writes |
| D93 | WP8 | mechanism | internal | RSS pipeline takes traits instead of callables |
| D100 | WP9 | mechanism | internal | Cron next-fire through cronsim's parser and croner, equal on every accepted expression |
| D101 | WP9 | mechanism | internal | Shutdown stops loops at await points, as cancellation does in Python |
| D102 | WP9 | observable | visible (logs) | A scheduler loop whose next run leaves the date range logs an ERROR line before it stops |
| D103 | WP9 | observable | visible (timing) | The `.strm` watcher walks a moved-in directory itself (notify reports only the directory) |
| D104 | WP9 | python-bug | visible (copied from Python) | Move-in listing refreshes only the root, as Python does |
| D105 | WP9 | mechanism | internal | Routers take scheduler traits |
| D106 | WP9 | observable | visible (edge input) | Integer query parameters beyond i64 are refused (Python clamped or accepted any size) |
| D107 | WP9 | mechanism | internal | Playlist fills tracked in a task map |
| D108 | WP9 | observable | visible (logs) | AVID rules that do not compile keep the previous rules and log once per version |
| D109 | WP9 | mechanism | internal | Client construction can fail in Rust; handled at start-up |
| D110 | WP10 | dropped | internal | The in-memory fill-actor repository is not ported; PostgreSQL only |
| D111 | WP10 | observable | visible (edge input) | Plan dates parse the forms pydantic takes in JSON mode, not every ISO 8601 form |
| D112 | WP10 | observable | visible (hand-edited rows) | Fill-actor id lists of the wrong shape are refused instead of misread |
| D113 | WP10 | mechanism | internal | Non-UTF-8 candidate paths carried as bytes, error text rebuilt as asyncpg's |
| D114 | WP10 | observable | visible (edge input) | Fill-actor counts beyond i64 are refused with pydantic's error |
| D115 | WP10 | mechanism | internal | CloudDrive mover checks typed answers |
| D116 | WP10 | mechanism | internal | Cancelled CloudDrive calls finish under `settle()` instead of `asyncio.shield` |
| D120 | WP11 | mechanism | internal | Fill-actor file work through `MoveFs` (links, `renameat2`) |
| D121 | WP11 | mechanism | internal | Moves in tracked tasks instead of an `_in_flight` dict |
| D122 | WP11 | observable | visible (cancel timing) | A cancelled fill-actor file mutation returns at once while its thread keeps the lock until it ends (Python's caller waited for the thread) |
| D123 | WP11 | observable | visible (cancel timing) | A dropped scan stops its thread through a flag at the next check |
| D124 | WP11 | mechanism | internal | Progress callbacks as a trait |
| D125 | WP11 | mechanism | internal | Non-UTF-8 names as bytes in the fill actor |
| D126 | WP11 | mechanism | internal | Runtime settings read once per move as a snapshot |
| D127 | WP11 | observable | visible (logs) | Fill-actor exceptions are logged without a traceback |
| D128 | WP11 | mechanism | internal | Mutation lock held across the CloudDrive calls through a guard |
| D129 | WP11 | observable | visible (edge input) | Device and inode numbers at or above 2^63 are stored as their negative image |
| D130 | WP12 | mechanism | internal | Job stop through a cancellation token |
| D131 | WP12 | mechanism | internal | Manager shutdown order made explicit |
| D132 | WP12 | mechanism | internal | Test hooks instead of monkeypatching |
| D133 | WP12 | mechanism | internal | Mutation lock typed as a trait |
| D134 | WP12 | mechanism | internal | The ported heartbeat tests use a 450 ms lease |
| D135 | WP12 | mechanism | internal | The maintenance task is aborted and awaited at shutdown |
| D136 | WP12 | observable | visible (bug path) | A panic in a scan thread fails the job as Python's exception would, with Rust's text |
| D140 | WP13 | mechanism | internal | Merge worker's blocking subprocess runner with stall timer |
| D141 | WP13 | observable | visible (stderr) | The merge worker's unhandled errors print Python's last traceback line, not the whole traceback |
| D142 | WP13 | observable | visible (non-UTF-8 names) | `merge-worker` refuses a non-UTF-8 argument; the scan reads such names lossily |
| D143 | WP13 | mechanism | internal | Merge task updates through typed field sets |
| D144 | WP13 | mechanism | internal | Merge worker's database half behind a feature so the exe builds on Windows |
| D145 | WP13 | observable | visible (edge input) | `merge-worker` option parsing matches argparse in the cases used; rare argparse corners differ |
| D150 | WP14 | mechanism | internal | CloudDrive RPCs shielded by tasks for ticks and operator actions |
| D151 | WP14 | observable | visible (cleanup) | Removing a cancelled merge's parts folder stops at the first undeletable entry (more may be left) |
| D152 | WP14 | observable | visible (error text) | Merge database errors are worded as asyncpg's classes |
| D153 | WP14 | observable | visible (edge input) | `X-Chunk-Frames` beyond i32 is refused before the body is read |
| D154 | WP14 | mechanism | internal | Refused chunk bodies drained so the connection stays usable |
| D165 | WP15 | mechanism | internal | Home worker's own process module (groups, suspend/resume) |
| D166 | WP15 | mechanism | internal | ffmpeg in a process group of its own so Ctrl+C reaches only the worker |
| D167 | WP15 | observable | visible (Windows) | ffmpeg/ffprobe output is decoded as UTF-8 with U+FFFD, never with the ANSI code page |
| D168 | WP15 | observable | visible (fixed leak) | A probe timeout's text redacts the bearer token Python printed |
| D169 | WP15 | observable | visible (bug path) | Errors that ended a Python encoder thread retry the part after the delay instead |
| D170 | WP15 | observable | visible (error text) | The home worker's HTTP client is ureq: its error text and `User-Agent` differ |
| D171 | WP15 | observable | visible (edge input) | `config.toml` is read as TOML 1.1, which accepts a little more than 1.0 |
| D172 | WP15 | observable | visible (hand-edited files) | A local chunk sidecar is taken only in the shape the worker writes |
| D173 | WP15 | observable | visible (Windows) | Ctrl+Break and closing the window count like Ctrl+C before Windows ends the worker |
| D174 | WP15 | mechanism | internal | The worker is the same binary as the server, without `serve` on Windows |
| D175 | WP16 | observable | visible (network) | AVBase: a GET whose reused connection dropped before the answer is resent once, as libcurl does |

## Benchmarks

### Memory

The machine was a 4-vCPU x86-64 cloud container with local PostgreSQL 16, running release builds.
`tools/bench/memory.py` ran each build as a real server process with every service wired:

- a fake CloudDrive, a fake Emby, and a proxy that refuses all outbound traffic;
- 2,000 titles of `.strm` files and downloads.

The phases ran in order:

1. idle for 45 s after the configuration was written;
2. 350 GET requests;
3. one mapping run (2,000 files updated);
4. two archive runs (2,000 videos archived);
5. a fill-actor job, which stops at AVBase offline in both builds, so the job machinery is measured
   rather than a library scan;
6. a 1 GiB chunk upload to the transcode listener.

RSS is shown in MiB.

| Build | started | idle | requests | mapping | archive | fill actor | chunk | highest | PSS at end | threads |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Python 3.13, uvicorn | 104.8 | 105.8 | 107.0 | 108.5 | 113.9 | 114.1 | 119.1 | 123.1 | 101.7 | 9 |
| Rust, system allocator (chosen) | 24.6 | 25.5 | 26.3 | 30.9 | 34.0 | 34.6 | 36.5 | 43.0 | 33.5 | 8 |
| Rust, jemalloc | 25.9 | 27.0 | 28.0 | 33.6 | 37.5 | 37.9 | 47.2 | 52.1 | 44.1 | 8 |
| Rust, mimalloc | 37.4 | 43.3 | 45.5 | 66.5 | 74.6 | 74.8 | 93.1 | 93.1 | 90.2 | 7 |

- **Durations.** Start-up took 0.84 s for Python and 0.42 s for Rust. Phase times, Python then Rust:

  | Phase | Python | Rust |
  | --- | --- | --- |
  | mapping | 0.64 s | 0.43 s |
  | archive (two runs) | 5.71 s | 4.04 s |
  | chunk upload | 3.21 s | 2.37 s |

- **Allocator.** The system allocator had the lowest figures at every phase, so no allocator crate is
  linked. fav chose jemalloc, but here jemalloc ended 10.7 MiB higher. mimalloc kept its arenas and
  ended at 93.1 MiB.
- **Merge worker.** Three 60-second 720p parts, concatenated with `--reserve-gib 0`, highest RSS of the
  worker process alone (ffmpeg excluded):
  - Rust: 18.0 MiB;
  - Python: 40.6 MiB;
  - the plan's goal was under 15 MiB.

  Both wrote byte-identical outputs. Only the sampler (`mw/sample.py`) was kept, not its raw output. The
  plan's Python baseline for this figure was 63 MiB, measured another way.
- **Production context.** The deployment requests 256Mi and limits at 768Mi. Python's 30-day median in
  production is 192 MiB, above the 106-123 MiB measured here. That figure comes from the pod's metrics,
  which count more than one process's RSS (page cache from the file work, for one).

### Image and executables

| Artifact | Size |
| --- | --- |
| Rust image (`Dockerfile.rust`, Debian 13 slim with ffmpeg 7.1.5) | 800 MB, 215 MB compressed |
| Python image (`Dockerfile`, python:3.13-slim with ffmpeg) | 947 MB, 249 MB compressed |
| `embyx-manager` (Linux, release, thin LTO, debug info stripped) | 36.1 MB (34.4 MiB) |
| `embyx-manager.exe` (Windows, release, the CI artifact) | 1.7 MB as the zipped CI artifact (the unzipped size was not measured here) |

ffmpeg and its libraries account for most of either image. Inside the Rust image:

- the real-ffmpeg tests pass (home worker 4/4, merge worker 18/18);
- Python's `mimetypes` and the Rust table agree;
- neither image has `/etc/mime.types` (D53).

### CI times

These are from the last runs on #100. The Rust image job's first run is cold; later runs use the GHA cache.

| Job | Time |
| --- | --- |
| `rust.yml` rust: fmt, clippy, workspace tests, replay check, listings, page smoke | 6 min 17 s |
| `rust.yml` image: `Dockerfile.rust` build and checks | 8 min 38 s (no usable cache: the base image tag changed in that push; 9 min 20 s on the push before, the first) |
| `rust.yml` windows: clippy, tests with real ffmpeg, release build | 2 min 32 s |
| `rust.yml` python-tools: goldens, transcripts, tool tests, ruff | 1 min 35 s |
| `rust.yml` parity, tree | 13 s, 4 s |
| `ci.yml` (the Python tree's): backend, frontend, image (not pushed from a PR) | 1 min 14 s, 1 min 7 s, 1 min 7 s |
| Wall clock of `rust.yml` per push | about 8 min 40 s, set by the image job; without it, rust and parity together took 6 min 32 s on 6c5120b |

## Experiment log summary

The full rows are in PLAN.md "Experiment log".

- **Work packages and time.** There were 17 work packages over about 16 hours. Work ran two at a time
  from WP2 to WP15, and WP16 ran alone. A porting work package took 1 to 2 hours; WP14 (the merge
  service, two PRs) took about 3 hours.
- **Rework.** CI was green first time and one push held the review fixes, except in two work packages:
  - WP10 had one more push, a PARITY.md refresh;
  - WP15 had 5 pushes: a Unix-only test module that broke Windows, Chocolatey's ffmpeg shim (found with
    a diagnostic push), the review fixes and a libx264 check.

  In WP16, the `rust` job was red on #99's first push and green from the second.
- **Independent reviews.**
  - Severity counts over the whole rewrite (WP1-WP16, #83-#100): 0 blockers, 15 major, 58 minor and
    63 nits, plus WP7's uncounted D82 wording nits. #100 alone had 6 of the majors. WP0 used another
    scale: 0 high, 3 medium, 7 low.
  - **All addressed.** Every finding was fixed or explicitly answered on its PR before the merge.
  - **Major findings.** Most were behaviour gaps the tests had missed; WP11's was a missing test.
    Examples: a dropped lock wait that never cancelled; a dropped caller returning before its CloudDrive
    call finished, where Python waits (the call kept running); a lossy non-UTF-8 name that matched no
    file; an `is_file` that read every `stat` error as "no"; and #100's masking gaps.
- **Found by running for real rather than by review.**
  - The Chocolatey ffmpeg shim (WP15 CI).
  - The AVBase connection-reuse resend (WP16 level 3, D175).
  - The level 2 differ showing D62's refusal on out-of-shape rows.
  - On production rows the level 2 differ itself had three defects, none a difference between the
    builds: playlist keys hold spaces and CJK text, which it put into the URL unencoded; three values
    are read off the server's clock when the request runs, which differed by the seconds between the
    two runs; and a relative `--binary` path broke in the server's scratch directory. All three are
    fixed in #102.
  - #102's review found the first clock fix too loose (any value within
    the span of both runs, either way round, in any time format), the report claiming more than the
    run covered (which rows were read, which routes were asked) and a row that neither build can serve
    hidden in the 404 count. Its second pass found the merge tasks over-counted (the list route shows
    the open ones and 20 finished) and the scan time treated as read at request time (it is read at
    start-up). The clock rule is tightened, the report lists every non-200 answer and every clock value
    let pass, and the plan route is asked by job id; this section says what the run did and did not
    cover.
  - #102's CI: `heartbeat_retries_transient_repository_failure` failed once in the `rust` job and
    passed on the re-run. Its 450 ms lease left a renewal 300 ms to spare, which a loaded runner
    overran; the test now uses twice the lease. No product code changed.
- **WP16.**
  - #99: parity audit and cleanup notes. Its review found 0 blocker, 0 major, 3 minor and 4 nits, all
    answered.
  - #100: image, benchmark, level 2 and 3 tooling, deploy docs. Its review found 0 blocker, 6 major, 6 minor and 4 nits, all answered: masking gaps in `mask.py` (nested and encoded URL parameters, camelCase keys, header credentials, an export kept after a failed check), the GET differ comparing too loosely, a broken query, and a `workflow_run` trigger that would never fire from `rust`.
  - #101: this report.

## Choices for Max

1. **Level 2 on listings.** The database half of level 2 is done (above). A pass of each pipeline over a
   tree rebuilt from production listings would need new tooling and a walk of the library through
   CloudDrive, which spends calls to the 115 API. The paused deployment's manual round checks the same paths on
   the real tree, one pipeline at a time, so this is not recommended.
2. **Pod memory.** Keep 256Mi / 768Mi, or lower the request (for example to 128Mi) after a week of
   watching the pod's working set (DEPLOY.md).
3. **Job Object for the home worker** (HOME-WORKER.md). It would make ffmpeg shims harmless for stops.
   It is not built; winget's ffmpeg needs nothing.
4. **Deployment itself.** Described in DEPLOY.md: the image and release workflows, then switching Flux's
   ImagePolicy filter to `^rust$`, with the way back. None of it is done.

## Open risks

- **No live coverage.** CloudDrive, Emby and Kubernetes were never reached live; their fakes follow the
  Python tests and transcripts. A behaviour of the real services that the Python tests never pinned
  could differ (contract 6 is the most delicate: mutations always awaited).
- **Production data, writes.** Level 2 read the stored acquisitions, attempts, runs, subscriptions,
  playlists, apply jobs and the listed merge tasks through both builds and found no row the Rust build
  refuses (D62, D92, D112) and no differing answer. It did not read the 38 plan-job rows (D112's
  `actor_ids_json`) or the older finished merge tasks, and D172 (the home worker's local sidecars) is
  outside the database. It did not run a pipeline: what a run writes on
  production data is first seen in the deployment's manual round.
- **AVBase fingerprint.** The AVBase client impersonates Chrome through `wreq`/BoringSSL. It passed from
  the production network (nas-us-2, WP0's probe, #82) and from this container (level 3). A Cloudflare
  change can break either build, but the two may not break the same way.
- **Windows exe.** It was built and tested in CI only, never on the 5090, and its `crt-static` release
  build is untried.
- **Cross-build cache resume.** Resuming a home-worker part from a local cache the other build wrote was
  not tested; the cluster-side resume is.

## What was not tested

- A deployment of the Rust image, or the image in the pod's conditions (read-only root, uid 0 with
  `DAC_OVERRIDE`, the CloudDrive FUSE mount).
- A real merge of a real title, the 115 upload and the over-115 GB AV1 re-encode.
- A real title on the home 5090 with the Rust exe.
- The RSSHub feed live (refused from this network for both builds).
- AVBase's work read live after the D175 fix (the run cap allowed one run; the fix is pinned by a test
  that reproduces the dropped connection).
- A pass of the pipelines over production listings (the second half of the plan's level 2).
