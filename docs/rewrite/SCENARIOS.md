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
  all 14 RPCs rule for rule (WP3).
- **Kubernetes and Emby** (`fakes/http.py`; Rust `embyx_clients::fakehttp`): a scripted route
  table (first matching route answers, matched on method and path without the query, used up
  after `times`; anything else is 404 `{"error": "no route"}`). Answers are JSON (`json.dumps`
  with `ensure_ascii=False`), so a text body such as a pod log arrives quoted, on both sides.
  A WP that needs a stateful model (Kubernetes Jobs for WP14, Emby playlists for WP8) replaces
  the route table for that fake on both sides, keeping the call record shape.

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
- **WP7 (tracker, 换版):** add tracker and replace steps to the same operation; seed hard links and compare
  `nlink` where a copy is linked; database timestamps written with `now()` are `volatile`, timestamps
  written from the scenario clock are not.
- **WP9 (scheduler):** fires come from `clock`; a scenario runs one tick, not a loop.
- **WP11 (fill-actor service):** one scenario per journal state to recover from; seed the journal
  rows and the half-done tree, and let the transcript show the recovered end state.
- **WP13 (done):** `merge/tasks` runs `MergeTaskRepository` step by step (`merge.tasks`, the same
  `{op, args}` shape); its rows pin the `parts_json` bytes. Timestamps the repository takes from the wall
  clock are `rows.merge_tasks.*` selectors; a WP14 spec can add steps without new operation code.
- **WP14 (merge service):** the Kubernetes fake needs Job state (create, read, delete, status) on
  both sides; generated Job names and task UUIDs are covered by the UUID step or by
  `calls.kubernetes.<method>.<path>` selectors.
- The format is fixed. If a WP finds it cannot express something, it bumps `FORMAT` on both sides
  in the same PR and regenerates every transcript.
