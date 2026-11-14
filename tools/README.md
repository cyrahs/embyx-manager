# Rust rewrite tooling

Python tools that run on the baseline tree (`BASELINE`) for the Rust rewrite (`docs/rewrite/PLAN.md`). They
use the repository's uv environment with Python 3.13.18 (`UV_PYTHON=3.13.18`) and never talk to a real
service. CI runs every `--check` in `.github/workflows/rust.yml`. Lint them with `uv run ruff check tools`
(`tools/ruff.toml`).

| Tool | Run it |
| --- | --- |
| Golden generator: outputs of the baseline's own code, one module per area in `tools/goldens/areas/`, written to `goldens/` | `uv run python tools/goldens/generate.py` (write), `--check` (compare) |
| Scenario runner: effect transcripts, `docs/rewrite/SCENARIOS.md`; needs `EMBYX_MANAGER_TEST_DATABASE_URL` (a disposable server) | `uv run python tools/scenarios/runner.py [area/name ...]`, `--check` |
| Parity script: Python tests against the `// py:` claims of Rust tests, writes `docs/rewrite/PARITY.md` | `uv run pytest --collect-only -q > collected.txt`, `cargo test --workspace -- --list --format terse > rust.txt`, then `uv run python tools/parity/parity.py --write --pytest-collected collected.txt --rust-tests rust.txt` (or `--check`; CI adds `--base-scope` with the base branch's scope file) |
| CloudDrive descriptor: the `FileDescriptorSet` embedded in `clouddrive_pb2.py`, for tonic's code generation | `uv run python tools/codegen/clouddrive_descriptor.py` (write), `--check` |
| TLS fingerprint probe: ClientHello and HTTP/2 frames of curl_cffi and httpx against the Rust candidates of `crates/embyx-clients/examples/fetch_probe.rs`, on a local listener only (`docs/rewrite/API-COVERAGE.md`) | `uv run --with hpack==4.1.0 python tools/probes/tls_fingerprint.py [--json OUT]` |
| Live fingerprint run: the same clients against AVBase and JavBus, once, only from the probe pod Max approved (`live_probe_pod.sh <commit>`); without `--live` it stops after the local part | `uv run --with hpack==4.1.0 python tools/probes/live_fingerprint.py [--live]` |
| Merge worker cross-check: the baseline's `worker.run` on real ffmpeg, printing every status write; `crates/embyx-merge/tests/ffmpeg_parity.rs` runs it beside the Rust worker on copies of the same parts (needs ffmpeg) | `uv run python tools/merge_ffmpeg/python_worker.py '<command as JSON>'` (the Rust test calls it) |
| Home worker cross-check: the baseline's `home_worker` (claim, `TaskRun` with ffmpeg, a refusal) against a transcode listener; `crates/embyx-api/src/transcode_api/tests/protocol.rs` serves the Rust one on a local port and runs it (needs ffmpeg) | `uv run python tools/home_worker/python_worker.py '<run as JSON>'` (the Rust test calls it) |
| Page smoke: the unmodified front end, built in a copy of `frontend/`, against the Rust server on a scratch database, in headless Chromium with the Playwright pinned in `tools/smoke/package.json` (needs node, npm, `psql` and a Chromium for that Playwright) | `EMBYX_MANAGER_TEST_DATABASE_URL=... tools/smoke/run.sh` (`SMOKE_SCREENSHOTS=<dir>` keeps a screenshot) |
| Tree check: baseline files unchanged, no ignored Rust tests, `unsafe` only where allowed | `tools/check_tree.sh` |
| Tool self-tests | `uv run pytest tools/tests -q -p no:cacheprovider` |
