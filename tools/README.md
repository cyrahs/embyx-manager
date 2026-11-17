# Tools

| Tool | Run it |
| --- | --- |
| Tree rules: no ignored tests, `unsafe` only in the home worker's process control, every crate on the workspace lints (CI's first step) | `tools/check_tree.sh` |
| Page smoke: the front end, built in a copy of `frontend/`, against the server on a scratch database, in headless Chromium with the Playwright pinned in `tools/smoke/package.json` (needs node, npm, `psql` and a Chromium for that Playwright) | `EMBYX_MANAGER_TEST_DATABASE_URL=... tools/smoke/run.sh` (`SMOKE_SCREENSHOTS=<dir>` keeps a screenshot) |
| Scenario specs: the seeds and steps of the effect-transcript tests (`docs/rewrite/SCENARIOS.md`); the Rust tests run them and compare with `goldens/scenarios/` | read by `embyx_core::scenario::Spec::load` |

The Rust rewrite's Python tooling (golden and transcript generators, the parity check, the CloudDrive
descriptor extractor, the TLS fingerprint probe, the cross-build checks of the merge and home workers, the
memory benchmark, the level 2 export, differ and replay, the level 3 live run) needs the Python app and was
removed with it. It is at commit b2ffd31 (`tools/README.md` there says how each runs), against the Python
app as tagged `python-final`; the goldens and transcripts it made stay in `goldens/` as frozen fixtures.
