# Intentional differences from the Python app

Every place where the Rust build deliberately behaves differently from the Python app at the baseline
(`BASELINE`, commit 5a3c083), and every Python bug found while porting (copied or fixed). Add an entry in the
PR that introduces a new difference, with a number from your work package's range (`docs/rewrite/PLAN.md`).
If Rust disagrees with a golden, Rust is wrong or the difference belongs here.

Kinds:

- **dropped**: behaviour the plan removes (`import-config`, the in-memory fill-actor repository, `/docs` and
  `/redoc`).
- **mechanism**: same observable behaviour, different machinery.
- **observable**: a user, the front end, an API client, an operator reading logs or stored data can tell the
  difference.
- **python-bug**: a bug found in Python; say whether Rust copies it or fixes it, and why. Copy Python unless
  the bug loses or damages data.

"fav's Dn" names an entry of fav's own `docs/DIVERGENCES.md` (cyrahs/fav at the commit pinned in
`CONVENTIONS.md`), where the helper was first written and its difference first recorded.

| ID | WP | Kind | Python behaviour | Rust behaviour | Why | Approved by |
| --- | --- | --- | --- | --- | --- | --- |
| D1 | WP0 (for WP2, WP3, WP14) | observable | httpx exception class names reach error text and logs (`ConnectTimeout`, `ReadTimeout`, `ConnectError`, `RemoteProtocolError`, ...); httpx has separate connect, read, write and pool timeouts | `embyx_clients::net::httpx_error_class` maps the client's errors onto httpx's names (fav's D18, with the error-chain walk of fav's WP5): a timeout during connect is `ConnectTimeout`, other timeouts `ReadTimeout`, a connect failure `ConnectError`, a connection that ends before the message is complete `RemoteProtocolError`, another socket error `ReadError`, a content-decoding failure `DecodingError`. Only connect and read timeouts exist. The class, which code branches on, follows httpx; the free text after it is the Rust stack's | Different HTTP stack; httpx's taxonomy is not exposed by any Rust client. The ports in WP2 and later list the texts that reach a user (run errors, API `detail`) | Plan, 2026-11-13 |
| D2 | WP0 (for WP1, WP2) | observable | Python `re`: `\b`, `\w` and `re.IGNORECASE` follow `str.isalnum()` and per-code-point `lower()`; `\s` is `str.isspace()` | `embyx_core::pyre` compiles patterns as written with `regex` (or `fancy-regex` for look-around and back-references) and rewrites `\s` to Python's class (U+001C to U+001F included). `\b`/`\w` and case-insensitive matching use the crates' Unicode tables and simple case folding (fav's D73) | They differ only for non-ASCII letters next to a word boundary or inside a case-insensitive literal (U+0130 `İ` matches `i` in Python). The AVID and page-parser goldens of WP1 and WP2 decide; a case that differs there is added here | Plan, 2026-11-13 |
| D3 | WP0 (for WP13) | mechanism | Subprocesses through `asyncio.create_subprocess_exec` / `subprocess.run`: output that is not UTF-8 raises `UnicodeDecodeError` where text mode is on; a line over asyncio's 64 KiB stream limit raises `ValueError`; the child inherits standard input and outlives a cancelled caller | `embyx_clients::net::subprocess::SystemRunner` (fav's D61): output decoded with replacement characters, lines of any length read whole, standard input `/dev/null`, a child killed when its run is dropped; exit codes and `-signal` return codes match. WP13 checks this against the merge worker's ffmpeg and ffprobe handling (progress lines, stall kill, stderr drain) and records what it keeps | Removes crashes and orphaned processes; ffmpeg reads no standard input in the merge worker | Plan, 2026-11-13 |
| D4 | WP0 (for WP1) | observable | `json.loads` accepts integers of any size and strings with lone surrogates (`"\ud800"`) | `embyx_core::pyjson::loads` refuses integers outside `i128` (`Integer out of range`) and lone surrogate escapes (`Lone surrogate escape`) (fav's D8); Rust strings cannot hold a lone surrogate. Python 3.13's other decoder messages, `Illegal trailing comma before end of object/array` included, are copied (`goldens/pyweb/json_errors.json`) | Neither occurs in anything embyx stores or receives (config sections, task rows, CloudDrive and Emby answers); refusing is louder than corrupting. WP1 re-checks the stored config and state JSON against it | Plan, 2026-11-13 |
| D5 | WP0 (for WP2) | mechanism | httpx writes a request head as `Host`, then `Accept: */*`, `Accept-Encoding`, `Connection: keep-alive`, `User-Agent`, with names a caller sets moved to the end and spelled as given | `embyx_clients::net::HttpClient` builds the same head in the same order (fav's D182), but hyper writes every name title-cased, so a caller's own spelling (`sec-ch-ua` as `Sec-Ch-Ua`) is not kept | Header names are case-insensitive; fav found Cloudflare-fronted sites answer the httpx-shaped head and refuse hyper's default one. AVBase does not use this client: it needs the impersonating client (`API-COVERAGE.md`), which WP2 configures with curl_cffi's header list | Plan, 2026-11-13 |
