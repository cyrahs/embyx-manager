# API coverage of the key crates (WP0)

What the Python baseline relies on, the Rust crate chosen for it, and what an offline probe showed. Every probe
is a test in the crate that will use the library (`src/probes.rs`, compiled only for tests) and runs in CI; none
contacts a real site, the cluster or a production service. The one exception is the live fingerprint run Max
approved on 2026-11-13, from a one-off pod (below, "Live proof from nas-us-2"). Each row has a recommendation and a fallback. Pins
are in the workspace `Cargo.toml`.

| Python | Crate (pinned) | Verdict | Probe |
| --- | --- | --- | --- |
| asyncpg 0.30 | `sqlx` **0.9** (postgres, chrono, json, rustls) | Sufficient; one trap (cancelled lock waits) | `embyx-db/src/probes.rs` |
| grpcio 1.83 + generated `clouddrive_pb2*.py` | `tonic` / `tonic-prost` / `prost` **0.14**, code from the baseline's descriptor | Sufficient | `embyx-clients/src/probes.rs` (`grpc`) |
| `MessageToDict` (the scenario fake's records) | `prost-reflect` **0.16** (serde) | Byte-identical on the golden cases | `embyx-clients/src/probes.rs` |
| pyquery 2.1 / lxml 5.4, nyaapy 0.7 | `scraper` **0.29** (html5ever) | Sufficient with three known tree differences | `embyx-clients/src/probes.rs` (`html`) |
| curl_cffi 0.16.3 `impersonate='chrome'` | `wreq` **=6.0.0-rc.31** + `wreq-util` **=3.0.0-rc.14** (BoringSSL) | Provisional choice `wreq-tuned`; live proof pending | `embyx-clients/src/probes.rs` (`impersonation`), `tools/probes/tls_fingerprint.py`, `tools/probes/live_fingerprint.py` |
| cronsim 2.7 | `croner` **4** + `chrono-tz` | Sufficient for fires; validation needs a pre-check | `embyx-monitor/src/probes.rs` |
| watchdog 6.0 (inotify) | `notify` **8** | Sufficient; directories that arrive must be walked by us | `embyx-monitor/src/probes.rs` (`watcher`) |
| ctypes `renameat2`, `os.link`, `fcntl.flock` | `rustix` **1** (fs) and std | Sufficient | `embyx-fill-actor/src/probes.rs` |
| `os.kill` SIGSTOP/SIGCONT, `start_new_session`, `signal.signal` | `nix` **0.31**, std `process_group`, `tokio::signal` | Sufficient; `setsid` becomes a process group | `embyx-transcode-worker/src/probes.rs` (`unix`) |
| ctypes `ntdll.NtSuspendProcess` / `NtResumeProcess`, `CREATE_NEW_PROCESS_GROUP`, SIGBREAK | `windows-sys` **0.61** + `windows-link` **0.2** + `tokio::signal::windows` | Sufficient; runs in the Windows CI job | `embyx-transcode-worker/src/probes.rs` (`windows`) |
| httpx 0.28 | `reqwest` **0.13** behind fav's httpx-shaped client (`embyx-clients/src/net`) | Copied with fav's tests | `net::*::tests` |

## sqlx 0.9

Probe: `crates/embyx-db/src/probes.rs`, against the CI PostgreSQL 17 service (one temporary database per test).

| Python (`db.py`, `locking.py`) | sqlx | Checked |
| --- | --- | --- |
| Sessions run in the server's `TimeZone` | sqlx sets `TimeZone=UTC` on every connection. Aware timestamps compare equal either way (binary protocol); only text renderings differ, and the scenario dumps set UTC themselves | Test |
| The column types of the 19 tables: TEXT, BIGINT, INTEGER, BOOLEAN, DOUBLE PRECISION, TIMESTAMPTZ, DATE, BIGSERIAL | `String`, `i64`, `i32`, `bool`, `f64`, `DateTime<Utc>`, `NaiveDate`, `RETURNING id` as `i64` | Test |
| `*_json` TEXT columns, `::jsonb` edits in migrations | TEXT comes back byte for byte (`{"keep": [1, 1.0]}` keeps `1.0`); `(col::jsonb - 'key')::text` works; `Json<serde_json::Value>` for a parsed read | Test |
| Array binds for `unnest` and `= ANY($1)` | `Vec<String>` / `Vec<i64>` with `$1::text[]` | Test |
| `asyncpg.UniqueViolationError` | `DatabaseError::code() == "23505"`, `is_unique_violation()`, `constraint()` names the partial index | Test |
| `pg_advisory_lock(namespace, key)` with namespace `0x454D4258`, keys 0 (migrations, transaction-scoped), 2 (mutations) | Same SQL; `i32` binds; the transaction-scoped lock ends with the transaction | Test |
| Cancelling the task that waits in `pg_advisory_lock` cancels the server-side wait (asyncpg sends a cancel request) | **sqlx does not.** Dropping the future that waits on a pooled connection leaves the backend waiting; it takes the lock once the holder lets go, and the connection returns to the pool holding it, so nothing else gets the lock again (`a_dropped_wait_on_a_pooled_connection_takes_the_lock_later`) | Test |

Recommendation for WP1: wait for the mutation lock on a pooled connection whose backend pid was read first, and
on cancellation (a drop guard) cancel the wait with `SELECT pg_cancel_backend(pid)` from another connection; the
wait then fails with `57014` and never takes the lock (`a_wait_cancelled_with_pg_cancel_backend_never_takes_the_lock`).
Detach the waiting connection from the pool if the cancel cannot be confirmed. Fallback: poll
`pg_try_advisory_lock` with the retry interval `locking.py` already uses for the file lock (loses the server's
fair queue; a divergence entry if chosen).

## tonic 0.14 from the baseline's descriptor

`tools/codegen/clouddrive_descriptor.py` builds a `FileDescriptorSet` from the descriptor embedded in the
baseline's `clouddrive_pb2.py` (plus its well-known dependencies, which prost-build needs to derive traits) and
writes `crates/embyx-clients/proto/clouddrive.fds.bin`; CI checks it is current (`--check`). No `.proto` file,
no `protoc`. `crates/embyx-clients/build.rs` keeps only the 14 RPCs `client.py` calls (and fails if that list
does not match the service) and generates client and server with `tonic-prost-build::compile_fds`.

| Python (`clients/clouddrive/client.py`) | tonic | Checked |
| --- | --- | --- |
| `grpc.insecure_channel(address)` (`secure=False`) | `Endpoint::from_shared("http://host:port").connect()` + `CloudDriveFileSrvClient::new(channel)` | Test, real socket |
| `metadata=[('authorization', 'Bearer …')]` | `Request::metadata_mut().insert("authorization", …)` | Test |
| Unary calls (`MoveFile`, `GetSystemInfo`) | Generated methods | Test |
| Server streaming `GetSubFiles` (`for response in stub.GetSubFiles(...)`) | `Streaming<SubFilesReply>::message()` | Test |
| `NOT_FOUND` → `FileNotFoundError(details)`, `INVALID_ARGUMENT` "can't open a file as directory" → `NotADirectoryError` | `Status::code()` / `message()`, intact also when the stream fails before its first message | Test |
| Chinese details (`任务已存在`) | `grpc-message` is percent-encoded on the wire and decoded back | Test |
| `grpc.secure_channel(address, ssl_channel_credentials())` (`secure=True`) | Not probed: needs tonic's `tls-ring` + roots features. WP3 adds them and tests against the fake with a self-signed certificate | — |
| `timeout=` per call | `Request::set_timeout` (sends `grpc-timeout`) or `tokio::time::timeout` around the call; WP3 decides which matches the Python behaviour on expiry | — |

`CloudDriveSystemInfo` has no version field: the Python `check()` always reports version `''`; WP3 keeps that.

The scenario fake records requests and responses with protobuf's JSON mapping. `prost-reflect` re-decodes the
bytes with the same descriptor and serializes with proto field names, defaults included, unset `optional`
fields left out, enums by name and 64-bit integers as strings: equal to `MessageToDict` on every case of
`goldens/clouddrive/json_mapping.json` (empty messages, explicit-presence enums set to their default, int64
sizes, `map<uint32, string>`, timestamps with nanoseconds).

Fallback: none needed. If a later CloudDrive descriptor adds RPCs, rerun the descriptor tool on the new stubs.

## scraper 0.29 (html5ever) against pyquery/lxml

`pyquery.PyQuery(text)` tries lxml's XML parser first and falls back to `lxml.html` only when the text is not
well-formed XML; nyaapy walks an `lxml.etree.HTMLParser` tree. `tools/goldens/areas/html.py` records, for every
markup literal in `tests/test_javbus.py` and `tests/test_magnet_sources.py` and for seven synthetic pages (a sukebei
result table, a table without `tbody`, unclosed cells, a `<p>` left open before a table, upper-case tags,
an end tag with no element, entities); f-string fragments are left out, since the page they render depends on
runtime values, what each selector the code uses matches (tag, attributes, `text_content`). The probe compares
scraper with that and pins the differences:

| Where | lxml / pyquery | scraper | Who handles it |
| --- | --- | --- | --- |
| `table tbody tr` on a table without `tbody` (`rss_magnet.py`) | No match: lxml inserts no implied `tbody` | Matches: html5ever inserts it | WP2: feed magnet tables. The baseline finds no rows in a `tbody`-less table; the port must not find more |
| Bare `<tr>` rows outside a table (JavBus's magnet AJAX answer, `javbus.py` `get_magnets`) | `lxml.html` keeps them | html5ever drops a `tr` outside a table | WP2: parse such a fragment inside `<table>…</table>`; the probe shows that recovers the rows and cells (`bare_rows_need_a_table_context`) |
| Well-formed markup with upper-case tags | Parsed as XML, where `A` is not `a`: no match | HTML: tags lower-cased, matches | Real pages are not well-formed XML; WP2 records it as a divergence only if a recorded page hits it |
| A document with no element (only an end tag) | pyquery raises `ParserError` | An empty document | WP2 maps it to the error the Python code raises |

Everything else matches: attributes (order aside: scraper keeps them sorted, the code reads them by name),
`text_content`, `:first-child`, `:nth-child`, `[href^='magnet:']`, the sukebei layout, entities, scripts.
pyquery's whitespace-squashed `.text()` is in the golden for WP2 to port; no crate reproduces it.

Fallback: none needed.

## The impersonating client for AVBase (and the client for JavBus)

`clients/avbase.py` uses curl_cffi's `AsyncSession(impersonate='chrome')`; `clients/javbus.py` uses plain
httpx. curl_cffi **0.16.3** (the baseline's `uv.lock`) resolves `chrome` to **chrome150**. The candidates
run through `crates/embyx-clients/examples/fetch_probe.rs`:

- `wreq`: `wreq` (BoringSSL, from the `rquest` family) with `wreq-util`'s newest profile, **Chrome149**, as it
  comes;
- `wreq-tuned`: the same profile with curl_cffi's default header list (values and order) and the HEADERS
  priority weight 256 set on the emulation (`Emulation::headers`, `orig_headers`,
  `http2_options.headers_stream_dependency`, all public);
- `httpx-rs`: the httpx-shaped reqwest client copied from fav (`embyx_clients::net::HttpClient`, rustls), the
  candidate for JavBus.

`tools/probes/tls_fingerprint.py` starts a TLS listener on 127.0.0.1 with a throwaway certificate, has each
client make one request with the headers its application caller adds (AVBase: `Referer`,
`Accept-Language`; JavBus: `Accept-Language`, `Cookie: existmag=all`), and compares what the server sees:

| Field | curl_cffi chrome150 | `wreq` Chrome149 | `wreq-tuned` |
| --- | --- | --- | --- |
| TLS cipher suites (order) | | same | same |
| TLS extensions (as a set; Chrome shuffles the order) | GREASE on | same | same |
| Supported groups, key shares (X25519MLKEM768, X25519, P-256, P-384) | | same | same |
| Supported versions, ALPN (`h2`, `http/1.1`), certificate compression, ALPS | | same | same |
| Signature algorithms | `0x0904`-`0x0906` (ML-DSA) first, then Chrome's eight | Chrome's eight only (the 150 vs 149 difference) | same as `wreq` |
| HTTP/2 SETTINGS (order and values), connection WINDOW_UPDATE, pseudo-header order (`:method :authority :scheme :path`) | | same | same |
| HEADERS priority | exclusive, weight 256 | exclusive, weight 220 | same as curl_cffi |
| Header set and order | `sec-ch-ua*`, `upgrade-insecure-requests`, `user-agent`, `accept`, `sec-fetch-site/mode/user/dest`, `accept-encoding`, `accept-language`, `priority`, `referer` | no `upgrade-insecure-requests` / `sec-fetch-user`, a different order, `accept-encoding` last | same as curl_cffi |

So `wreq-tuned` differs from curl_cffi only in the three ML-DSA signature algorithms, which no `wreq-util`
profile offers yet. For JavBus, `httpx-rs` and Python httpx differ in TLS as rustls and OpenSSL do (cipher
list, groups, signature algorithms), and in one header: the Rust client sends `Accept-Encoding: gzip,
deflate, br` where the baseline's httpx sends `gzip, deflate` (brotli is not installed in the baseline's
environment). The order and spelling of the head are otherwise the same.

### Live proof from nas-us-2

Not run yet. Max approved one run from a one-off pod on nas-us-2 (2026-11-13 19:23Z, plan "Questions for
Max", item 3); creating the Pod was stopped by the session's safety check until Max confirms it in the WP0
thread. Everything for the run is in place: `tools/probes/live_probe_pod.sh <commit>` (the pod's script) and
`tools/probes/live_fingerprint.py --live` (at most 10 anonymous GETs per site, 3 s apart, no redirect
followed, no retry; verified against a local fake server). The result lands in a docs-only follow-up PR
into `rust`; if it has not run when WP2 starts, WP2 takes the probe over.

Each request is a fresh client (one `fetch_probe` process, one curl_cffi or httpx session), so the AVBase
data routes go out without the cookies (such as `__cf_bm`) that `clients/avbase.py`'s single session would
carry from the home page. That is the same for the control and the candidates, so the comparison holds; a
challenge on a data route alone may be the missing cookie rather than the fingerprint.

The Pod it creates: `default` namespace, `nodeSelector kubernetes.io/hostname: nas-us-2`, image
`docker.io/library/rust:1.97.0`, `restartPolicy: Never`, `activeDeadlineSeconds: 1800`,
`automountServiceAccountToken: false`, `enableServiceLinks: false`, `dnsPolicy: Default`, no volumes, no
Secret or ConfigMap, requests 100m / 256Mi, limits 2 CPU / 4Gi; the command fetches
`live_probe_pod.sh` from the pushed commit and runs it with that commit.

Recommendation for WP2 (provisional until the live proof): `wreq-tuned` for AVBase, the httpx-shaped client
for JavBus and sukebei as the baseline uses httpx there. Pin `wreq` and `wreq-util` exactly (release candidates).
WP2 reruns `tls_fingerprint.py` against its configured clients and records the result; equal TLS and HTTP/2
fields are the bar, the Chrome major version may differ by one. If WP2 changes a client, it may repeat the
live probe once under the same conditions (plan, "Questions for Max", item 3). Fallback: the
`curl-impersonate` binary (the same library curl_cffi wraps) as a subprocess, behind the same client trait.

Known differences WP2 must keep in mind:

- wreq's ClientHello lacks the three ML-DSA signature algorithms (`0x0904`-`0x0906`) that curl_cffi chrome150
  sends first; no `wreq-util` profile offers them yet. Everything else in TLS and HTTP/2 matches with the
  tuned headers and weight.
- The httpx-shaped client (JavBus, sukebei, the other httpx paths) sends `Accept-Encoding: gzip, deflate,
  br` where the baseline's httpx sends `gzip, deflate`, and its TLS is rustls where Python's is OpenSSL
  (different cipher list, groups and signature algorithms). Decide in WP2 whether to drop `br` to match,
  and record the choice in `DIVERGENCES.md`.

Build: BoringSSL compiles from source (`cmake`, a C/C++ compiler; the GitHub Ubuntu runner has both). A cold
build of `wreq` and `wreq-util` (with BoringSSL and the rest of `embyx-clients`) takes 35 s on 4 cores
here (1 min 47 s of CPU); CI caches it with the rest of the target directory. BoringSSL and rustls (`ring`) link into the same test binaries without symbol clashes; there is no OpenSSL in the tree.

## croner 4 against cronsim 2.7

`monitor/scheduler.py::_next_cron_fire` evaluates the archive scan's cron as
`CronSim(expr, now.astimezone())`: local time, but with the **fixed UTC offset** the zone has at `now`
(`astimezone()` without a zone returns a fixed-offset datetime), so a fire across a DST change keeps the old
offset and the next call, after that fire, uses the new one. `goldens/cron/` records validation for 41
expressions (the default, the tests', and what a person may type) and 1,148 next-fire chains in UTC,
Asia/Shanghai, America/New_York and Europe/Berlin from starts next to the 2026 DST changes.

- Next fires: with `CronParser` set to optional seconds (cronsim's six-field form has seconds first), no
  year field and day-of-month/day-of-week OR-ed, and evaluated in that fixed offset, croner gives cronsim's
  chain for every expression both accept, in every zone. In the zone itself it would differ around DST
  changes, so WP9 must evaluate in the fixed offset (and say so in a comment).
- Validation: croner accepts five expressions cronsim refuses (`15W`, `?` in either day field, `@daily`,
  `@hourly`) and `0 4 30 2 *`, which cronsim refuses as "Bad day-of-month" (croner accepts it and finds no
  fire). `config/models.py` puts cronsim's message into the validation error, so WP1 ports cronsim's
  validation and messages (the golden has them) and lets croner see only what passes.

Fallback: port cronsim's iterator (about 400 lines) if a later expression differs; the goldens are in place.

## notify 8 (the `.strm` watcher)

watchdog 6.0's inotify observer, recursive, feeds `_StrmEventHandler`: created, modified, deleted and moved
`.strm` files. On Linux notify reports a create, a rename inside the tree as one event naming both paths, a
file moved in from outside as a rename "to", a new directory (and, being recursive, files created in it
afterwards), and a removal. **Differences:** watchdog synthesizes events for the files inside a directory
moved into or within the tree (`generate_sub_created_events`, `generate_sub_moved_events`); notify reports
only the directory. WP9 walks such a directory itself and emits one event per `.strm` file under it.

## rustix 1 and std (journaled moves, file lock)

| Python (`fill_actor/service.py`, `locking.py`) | Rust | Checked |
| --- | --- | --- |
| `renameat2(AT_FDCWD, src, AT_FDCWD, dst, RENAME_NOREPLACE)` via ctypes | `rustix::fs::renameat_with(CWD, src, CWD, dst, RenameFlags::NOREPLACE)` | Test: moves, keeps the inode; `EEXIST` with both files intact; `ENOENT` for a missing source |
| `FileExistsError` / `errno in {EINVAL, ENOSYS, EOPNOTSUPP}` (NFS fallback) | `rustix::io::Errno` values | The NFS errnos cannot be produced locally; the mapping is listed in the probe |
| `os.link(src, dst, follow_symlinks=False)` | `rustix::fs::linkat(CWD, src, CWD, dst, AtFlags::empty())` or `std::fs::hard_link` (no follow on Linux) | Test: same inode, `nlink` 2, `EEXIST`, links a symlink itself |
| `fcntl.flock(fd, LOCK_EX | LOCK_NB)`, `BlockingIOError` | `rustix::fs::flock(.., NonBlockingLockExclusive)` (`EWOULDBLOCK`) or `File::try_lock` (`TryLockError::WouldBlock`) | Test, including release on close |

## Process control in the home worker (nix, windows-sys)

| Python (`merge/home_worker.py`) | Rust | Checked |
| --- | --- | --- |
| `subprocess.Popen(..., start_new_session=True)` | `CommandExt::process_group(0)`: own process group, which is what keeps a console Ctrl+C away from ffmpeg. std has no stable `setsid`; ffmpeg runs with `-nostdin`, so leaving the controlling terminal attached changes nothing the worker relies on. WP15 records it as a divergence (or calls `setsid` in `pre_exec` inside its process module, the one place `unsafe` is allowed) | Test (Linux) |
| `os.kill(pid, SIGSTOP)` / `SIGCONT` | `nix::sys::signal::kill` | Test: `/proc/<pid>/stat` shows `T`, then `S` |
| `signal.signal(SIGINT, handler)` counting presses | `tokio::signal::unix::signal(SignalKind::interrupt())` | Test: two raised SIGINTs arrive, the process lives |
| `subprocess.CREATE_NEW_PROCESS_GROUP` | `CommandExt::creation_flags(CREATE_NEW_PROCESS_GROUP)` | Test (Windows job) |
| `ctypes.windll.ntdll.NtSuspendProcess(handle)` / `NtResumeProcess` | Not in windows-sys; declared with `windows_link::link!("ntdll.dll" "system" fn NtSuspendProcess(HANDLE) -> NTSTATUS)` and called on `Child::as_raw_handle()` | Test (Windows job): both return `STATUS_SUCCESS`, the child stays alive |
| `signal.signal(SIGBREAK, handler)` | `tokio::signal::windows::ctrl_break()` (and `ctrl_c()`) | Handlers install (Windows job); raising a console event in CI would hit the test runner |

The workspace denies `unsafe_code`; `tools/check_tree.sh` lets only `crates/embyx-transcode-worker/src/process*`
and that crate's probe lift it.

## The copied fav helpers

`embyx-core` (`pyjson`, `pyre`, `pyurl`, `pypath`, `pytext`, `golden`) and `embyx-clients/src/net` (the
httpx-shaped client over reqwest, its error classes and reason phrases, retry ladders and the recording
sleeper, the subprocess runner, the proxy-scheme rule, downloads, the fake web server) come from fav at the
commit in `docs/CONVENTIONS.md`, with their tests and goldens. Their goldens are regenerated here with Python
3.13.18; `pyjson` needed one change for 3.13 (the "Illegal trailing comma" decode errors). See
`docs/DIVERGENCES.md` D1 to D3 for the differences they carry from fav.
