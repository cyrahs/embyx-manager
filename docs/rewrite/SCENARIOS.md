# Effect transcripts (scenarios)

A unit golden pins a pure function. An effect transcript pins what an operation *does*: the
calls it makes to CloudDrive, Kubernetes and Emby, what it leaves on disk and in the database.
Both builds run the same scenario; their transcripts must be byte-identical.

- Python side: `tools/scenarios/runner.py` (runner), `transcript.py` (format), `fakes/`,
  `operations/`. It runs on the baseline tree and writes `goldens/scenarios/<area>/<name>.json`.
- Rust side: `embyx_core::scenario` (seed, recorder, render, compare), `embyx_db::scenario`
  (seed and dump rows), `embyx_clients::clouddrive::fake` (the CloudDrive fake). A Rust test per
  scenario runs the ported operation and calls `scenario::assert_transcript`.
- Proven end to end in WP0 by `harness/trivial`
  (`crates/embyx-clients/tests/scenario_harness.rs`). WP1 adds the database-only scenarios `db/*` and
  `config/store` (`crates/embyx-db/src/scenario/tests.rs`), which seed no fake and use
  `embyx_db::scenario::unseeded_fakes` for the fakes' part of the transcript.

```
uv run python tools/scenarios/runner.py                  # rewrite every transcript
uv run python tools/scenarios/runner.py --check          # what CI runs (drift and stale files fail)
uv run python tools/scenarios/runner.py harness/trivial  # one scenario
```

The runner needs `EMBYX_MANAGER_TEST_DATABASE_URL` pointing at a disposable PostgreSQL server; each
scenario creates its own database (`embyx_scenario_<pid>_<n>`) and drops it afterwards. Nothing in
a scenario talks to a real service.

## A scenario

`tools/scenarios/specs/<area>/<name>.toml`:

| Key | Meaning |
| --- | --- |
| `description` | One line. |
| `operation` | A name in `operations.OPERATIONS` (Python) and in the Rust test that ports it. |
| `clock` | The scenario's "now" (RFC 3339 with offset); default `2026-01-01T00:00:00+00:00`. The operation gets it as `ctx.now`; code under test must take its clock from there. The Python runner's local zone is UTC (`TZ=UTC`, `tzset()`); a Rust operation that needs a local zone takes it injected, as UTC, never from the process environment. |
| `args` | Free-form table passed to the operation. |
| `volatile` | Selectors of values that may differ between runs (below). |
| `ordering.mode` | `total` (default) or `lanes`; `ordering.lane_keys.<fake> = "<dotted path>"` in `lanes` mode. |
| `roots.names` | Temporary directories the operation works in (`upload`, `library`, ...). |
| `[[tree]]` | Seed entries: `path = "<root>/<relative>"`, `kind` = `file` (default; `text` or `size` bytes of `x`), `dir`, `symlink` (`target` verbatim) or `hardlink` (`target` = `<root>/<relative>`). |
| `database.schema` | `none` (default) or `app` (the application schema, from WP1 on). |
| `database.sql` | Statements run after the schema. |
| `database.tables` | Tables to dump; default every base table in `public` except `schema_migrations`. |
| `[clouddrive]` | Fake state: `directories`, `files` (`path`, `size`, `hashes` keyed by hash kind number), `offline_tasks`, `copy_tasks`, `uploads`, `errors` (`method`, `code`, `details`, `times`). |
| `[kubernetes]`, `[emby]` | HTTP fake routes: `{method, path, status, body, times}`. |

## The transcript (format 1)

One JSON document, `json.dumps(..., ensure_ascii=False, indent=2, sort_keys=True)` plus a newline
(on the Rust side `pyjson` with the same options):

| Key | Content |
| --- | --- |
| `format` | `1`. A change to anything in this file bumps it and regenerates every transcript. |
| `scenario`, `operation` | The spec's name and operation. |
| `outcome` | `{"result": <JSON the operation returned>}` or `{"error": {"type": <Python class name>, "message": str(error)}}`. A Rust port maps its error to the Python class name. |
| `calls` | Every call to a fake, in the order the fake received it: `{fake, method, request, response \| error, lane, seq}`. |
| `tree` | Per root, every entry below it sorted by path: `{path, kind: "dir"}`, `{path, kind: "symlink", target}` or `{path, kind: "file", size, nlink, sha256}` (first 16 hex digits of the content hash). |
| `rows` | Per dumped table, `to_jsonb(row)` of every row, sorted by the row's JSON text. PostgreSQL renders the rows for both builds (with `TimeZone=UTC`), so neither side maps column types. |
| `fakes` | Each fake's final state (`dump()`): CloudDrive's directories, files, tasks and uploads; the HTTP fakes' `routes_left`. |

Call records use each wire format's own JSON: protobuf's JSON mapping for CloudDrive (proto field
names, fields at their default included, enums by name, 64-bit integers as strings, i.e.
`MessageToDict(preserving_proto_field_name=True, always_print_fields_with_no_presence=True)`;
`goldens/clouddrive/json_mapping.json` pins it), and the parsed JSON body (or text) for HTTP. A
failed gRPC call records `error: {code, details}` instead of `response`.

### Normalization

Applied in this order on both sides, one function each (`transcript.py` and `scenario.rs`):

1. **Root paths.** Every string containing a root's absolute path has it replaced by `{<root>}`,
   longest root first.
2. **Lanes and sequence numbers.** In `total` mode every call is in lane `main` and `seq` counts
   from 1 in arrival order. In `lanes` mode a call's lane is the value at its fake's `lane_keys`
   path (`main` when absent), calls are grouped by lane (stable sort by lane name), and `seq`
   counts within each lane. A lane name may not contain a UUID.
3. **UUIDs.** Every UUID (any case) becomes `<uuid:N>`, N numbered by first appearance walking
   `outcome`, `calls`, `tree`, `rows`, `fakes` with object keys sorted, keys before values.
4. **Volatile values.** Each selector's values become `<volatile:R>`, R the rank of the value
   among the distinct values of that selector (equal values stay equal, order is kept).
   `rows.<table>.<column>` selects a column of every row; `calls.<fake>.<method>.<dotted path>` a
   value inside every matching call.

## Fakes

The fakes' rules are theirs, not the real services': both builds run against the same rules,
which is what makes the transcripts comparable. A port copies a fake rule for rule.

- **CloudDrive** (`fakes/clouddrive.py`; Rust `embyx_clients::clouddrive::fake`): a real gRPC
  server serving the 14 RPCs the client uses, over a socket, so the scenario exercises the real
  client stack. State: directories, files with sizes and hashes, offline tasks, copy tasks, the
  upload queue; scripted errors fail the next `times` calls of a method. The Rust fake serves
  all 14 RPCs rule for rule (WP3). A seeded file may carry an `id` (default: its path) and a
  `write_time` in nanoseconds since the epoch (default: none); both stay with the file when it
  moves and are dumped when seeded (WP11: a moved file keeps its identity, as CloudDrive's do).
- **Kubernetes and Emby** (`fakes/http.py`; Rust `embyx_clients::fakehttp`): a scripted route
  table (first matching route answers, matched on method and path without the query, used up
  after `times`; anything else is 404 `{"error": "no route"}`). Answers are JSON (`json.dumps`
  with `ensure_ascii=False`), so a text body such as a pod log arrives quoted, on both sides.
  A WP that needs a stateful model adds it on both sides, keeping the call record shape: the
  Kubernetes Jobs model (WP14) answers what the routes do not.

`embyx_clients::scenario::ScenarioFakes::start(&spec, &recorder)` starts the three fakes the way
the runner does and `dump()` gives the transcript's `fakes`.

## Adding a scenario

1. Write the spec under `tools/scenarios/specs/<area>/`.
2. Add the operation to `tools/scenarios/operations/<area>.py` (an async function taking the
   context and returning JSON) and register it in `operations/__init__.py`. It calls the
   baseline's own code; it never reimplements it.
3. Run the runner, read the transcript, commit both.
4. Write the Rust test: `Spec::load`, `Seeded::new`, `TestDatabase::create` +
   `embyx_db::scenario::seed`, the fakes with one `Recorder`, the ported operation,
   `embyx_db::scenario::dump_rows`, then `scenario::assert_transcript`. Annotate it with the
   Python tests it stands for, if any.

## What later work packages need from this

- **WP3 (done):** `clouddrive/*` and `kube/jobs` run the wrapper and the Jobs client step by step
  (`clouddrive.steps`, `kube.steps`: a list of `{op, args}` reported as `{op, result | error}`);
  a later WP can add a spec for a client corner without new operation code. An RPC error is
  reported as `{type, code, details}`: a transport failure's text names the peer's port and is
  the stack's own (D40). `clouddrive/cancelled_move` shows a move whose caller was cancelled,
  with its response.
- **WP5 (done):** `intake/steps` runs the intake (`enqueue`, `queue`, `retry_due`) over the real ledger,
  `AsyncCloudDrive` against the fake and stub magnet sources from `args`, and stores the run with
  `PipelineRunRepository`, so the rows pin `archive_acquisitions`, `archive_magnet_attempts` and
  `pipeline_runs` bytes. The operation pins the intake's and the run repository's clock by patching the
  modules' `datetime` (moved by a step's `advance_seconds`) and skips `CLOUD_RETRY`'s waits; the Rust test
  injects the same clock and a recording sleeper. `uuid4().hex` run ids have no dashes, so they are marked
  `volatile`.
- **WP6 (done):** `archive/*` (outcomes, collisions, priority, run, failures, cancel, links and promotion), `reconcile/*` and `move_in/*` run `archive.steps`, `reconcile.steps` and
  `move_in.steps` (`tools/scenarios/operations/archive.py`) over a tree the steps shape (`write`, `remove`,
  unrecorded) and through a recording file system: Python patches `Path.rename`, `unlink`, `mkdir`
  (the outermost call only), `rglob`, `iterdir`, `stat` and `shutil.rmtree`; Rust wraps `LocalFs` in
  `fs::recording::RecordingFs`. Each mutation is a call `{fake: "fs", method, request, response | error}` in the
  order made (the runner's `Context.record`, a new hook any operation can use), and `args.faults`
  (`{op, path, errno | message, times}`) and `args.stops` (`{op, path}`: `request_stop()` right after that call)
  make failures and mid-run cancellation reproducible. Each step reports its result or error, the run's stats,
  log tail and errors; the transcript adds the final `tree` and the ledger `rows`. Ordered calls
  rather than `lanes`, because the order of renames and deletions is the contract. The runner's TOML is read
  in insertion order on both sides (`toml`'s `preserve_order`), so a spec's mapping tables keep their order.
- **WP7 (done):** `tracker/*` (outcomes, failures, cancel), `replace/*` (filing, failures, outside, cancel, start) and
  `manual/submit` run `tracker.steps` (`tools/scenarios/operations/tracker.py`, `tracker/scenarios.rs`): one
  world of the tracker, the replacement source and the manual source over the ledger, `AsyncCloudDrive`
  against the fake and a root standing for the CloudDrive mount (`args.mount`, the tracker's
  `library_mount_prefix`). The spec seeds what the mount shows twice, as `[[tree]]` entries and as
  `clouddrive.files` at the same API paths; a delete through the API (`DeleteFiles`, `RemoveOfflineFiles`
  with `deleteFiles`) also deletes the local copy unless the step sets `mount_keeps_deleted`. Steps: `poll`
  (stats, log tail, errors), `plan`, `start`, `browse`, `default_directory`, `check_target`, `submit`, and
  WP6's `write`/`remove`, `faults` and `stops`. A step's `cancel_after = "<RPC>"` cancels it the moment that
  RPC has answered (Python cancels the step's task, Rust drops its future), which is how a shutdown in the
  middle of a 换版 or a batch is reproduced. Magnets are submitted as the application's
  `_submit_magnet_at_cloud` does. Two harness rules: the baseline's live attempts are taken in info-hash order
  (D80), and grpcio's `debug_error_string` (peer address, time) is scrubbed from error texts on both sides.
  All timestamps come from the scenario clock; nothing is `volatile`. `plan`, `start`, `browse` and `submit`
  make their own `RunContext`, so the replacement and manual sources' log lines are not in the transcripts
  (the ported unit tests and review cover them).
- **WP8 (done):** `mapping/*` (full, incremental, failures) run `mapping.steps`
  (`tools/scenarios/operations/mapping.py`, `mapping/scenarios.rs`) over flat and mirror roots through
  WP6's recording file system with `copy2` and `rmdir` recorded too; every directory is listed in name order
  (bytes) on both sides, because `Path.glob` and `iterdir` follow the file system's order, and the seeded files'
  times are pinned to the clock. Steps: `full`, `incremental` (`changed`/`deleted`), `configure`, and the
  unrecorded `write` (`text`, `mtime`), `remove`, `mkdir`. `playlists/sync` runs `playlists.steps`
  (`operations/playlists.py`, `playlist_sync/scenarios.rs`): the real repository, the real `EmbyClient`
  against the scripted Emby fake (one `times = 1` route per answer, so a run's two `GET /emby/Items` listings
  come in order; no state model was needed) and a ranking-source stub recording `jinjier` calls. `rss/run`
  runs `rss.steps` (`operations/rss.py`, `rss/scenarios.rs`): the real subscription and acquisition
  repositories, `AsyncCloudDrive` against the fake, an `ArchivePipeline` over the spec's library through the
  recording file system, and stubs recording `feed` fetches (a body or an HTTP status per URL) and `avbase`
  cast and talent lookups; `stop_after_fetch` asks the run to stop once a URL has been fetched.
- **WP9 (scheduler):** fires come from `clock`; a scenario runs one tick, not a loop.
- **WP11 (done):** `fill_actor/*` (apply_local, crash_rename, crash_hardlink, crash_reconcile, cancel,
  recovery, offline, cloud_apply, cloud_crash, cloud_recovery) run `fill_actor.service` (`tools/scenarios/operations/fill_actor_service.py`,
  `crates/embyx-fill-actor/src/service/tests/scenarios.rs`): the service over the seeded library (`args.library`:
  actor, additional and move-in roots), the repository on the scenario database and, with `args.cloud`, the
  mover on `AsyncCloudDrive` against the fake. Steps: `scan`, `apply` (`files` of the last plan), `reconcile`,
  `candidates` (the repository's record of each candidate of the last plan), `restart` (a new service
  on a new pool, as a process restart), `journal` (journal states written through the
  repository for a candidate, as either build writes them), `faults`, `cloud_move` (someone else's `MoveFile`)
  and the unrecorded `write`/`remove`/`link`/`rename`. File mutations are `fs` calls (Python patches
  `_rename_no_replace`, `os.link`, `os.rename`, `os.unlink`, `os.mkdir`; Rust wraps `LocalMoveFs` in
  `fs::recording::RecordingMoveFs`) and the repository's writes are `repository` calls, recorded once they
  returned. A step's `crash` (`{fs = <op>, path?, nth?}`, `{repository = <method>, state?}`, `{mover =
  <method>}`) stops the service in front of that call as a dying process would (Python raises a
  `BaseException` nothing catches; Rust panics in the step's task), and `cancel_after` (`{fs = <op>}`, `{mover =
  <method>}`) cancels the step's task once that call returned (Python awaits a future that never ends right
  after the cancel, since the operation patches `asyncio.sleep` not to wait). After every step the operation waits for the
  moves still running (`aclose()`). The rows (`fill_actor_plans`, `_move_results`, `_move_journal`,
  `_cloud_move_operations`) after each crash, restart and recovery are equal on both sides, which is what lets
  either build resume a journal the other left. Candidate rows are not dumped as tables, since their
  fingerprints (device, inode, times) differ from run to run; the `candidates` step compares them with the
  fingerprint cut to its size.
- **WP12 (done):** `fill_actor/api` runs `fill_actor.api` (`tools/scenarios/operations/fill_actor_api.py`,
  `crates/embyx-api/tests/scenario_fill_actor_api.rs`): WP11's service world (same fakes, clock and `token-N`
  counter) under a `FillActorJobManager` (one worker, a 10 ms poll) and an app with the fill-actor router, its
  health block and the token, without the lifespans (the maintenance loop purges by the wall clock). Steps:
  `request` (`method`, `path`, `headers`, `json`; Python sends it through httpx's ASGI transport, Rust calls the
  app; reported as status and JSON body with `args.mask_keys` masked, the ages the job view takes from the wall
  clock), `start`, `recover` (the reaper's pass), `wait` (until nothing is queued and nothing runs under this
  manager, or with `settled` at all; the rows are polled) and `restart` (a new service, manager and pool). Job
  rows seeded through `database.sql` stand for another build's: a queued scan this manager runs, an expired
  lease failed at start, a live lease left to its owner until the reaper's pass after it ends. The rows
  (`fill_actor_jobs`, `_plans`, `_apply_jobs`, `_move_results`, `_move_journal`) and the tree are equal on both
  sides.
- **WP13 (done):** `merge/tasks` runs `MergeTaskRepository` step by step (`merge.tasks`, the same
  `{op, args}` shape); its rows pin the `parts_json` bytes. Timestamps the repository takes from the wall
  clock are `rows.merge_tasks.*` selectors; a WP14 spec can add steps without new operation code.
- **WP14 (service, done in its first PR):** a `jobs` seed (a table, `{}` for none) turns on a model of
  the Jobs API behind the Kubernetes fake's routes on both sides (`fakes/http.py`,
  `embyx_clients::fakehttp`): a request no route answers creates (201, or 409 `AlreadyExists`), reads (404
  `NotFound` when absent) or deletes a Job, kept by name with an empty `status`; the dump adds `jobs`.
  Routes still answer first, so a `times = 1` route scripts a 500 or a vanished Job for one read. The
  CloudDrive fake has `edit(op, args)` on both sides (`add_file`, `add_directory`, `remove`,
  `update_copy_tasks`, `drop_copy_tasks`, `set_uploads`, and `fail` for a scripted failure of the next calls of an RPC): CloudDrive changing on its own, unrecorded. The
  runner's `Context.fakes` hands an operation the running fakes for such steps. `merge/*` (walk,
  merge_failures, upload, cancel, replace_archive, home, home_muxers, over115_cpu, auto) run `merge.service`
  (`tools/scenarios/operations/merge_service.py`, `crates/embyx-merge/tests/service_scenarios.rs`): one
  world of `MergeService`, `AutoMerger` and the transcode bookkeeping over the scenario database, the fakes,
  a `work` root (the merge volume, status files and `.transcode/<id>/`), a `mapping` root (strm tree) and a
  `secrets` root (Job template and token). Steps: `create`, `tick`, `cancel`, `retry`, `use_cpu`,
  `set_paused`, `claim`, `heartbeat`, `chunk` (as the transcode PUT stores one), `finish_part`,
  `transcode_view`, `home`, `auto`, `restart`, and the unrecorded `write`, `remove`, `advance`,
  `job_status`, `cloud`, `free_space`; each reports the task listing after it. `cancel_after = "<method>"`
  cancels the step once that `AsyncCloudDrive` method answered. Python pins `datetime` in the five merge
  modules; Rust hands one clock to every repository, the presence, the title cache, the service and
  auto-merge. **Resume matrix:** `tools/scenarios/segment.py` runs `steps[from:to]` of a spec on the Python
  baseline against a given database, roots, clock and fake states (a dump's `routes_left` become routes
  again) and prints the steps, clock, calls and fake dumps. The Rust test splits a spec at its `restart`
  steps and runs the segments by the two builds in turn, Python first and Rust first; both must render the
  single build's transcript, so task rows, status files and chunk folders one build left are continued by
  the other. A spec run this way carries no scripted CloudDrive `errors` (a dump does not keep them).
- The format is fixed. If a WP finds it cannot express something, it bumps `FORMAT` on both sides
  in the same PR and regenerates every transcript.
