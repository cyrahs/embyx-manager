# embyx-manager

Unified web management and monitoring for `embyx` media workflows. Merges the former
`embyx-web` (Fill Actor web UI) and `embyx-monitor` (RSS ingestion, archive, STRM mapping
automation) into one FastAPI + React application backed by PostgreSQL.

## What it does

Three peer features; `/` redirects to the dashboard and no feature owns the app root.

- **Monitor dashboard** (`/dashboard`, the landing page): the embyx-monitor pipelines
  rebuilt as scheduled services with persisted run history —
  - **rss**: subscribed feeds — RSSHub routes, AVBase talent feeds, sukebei searches, any
    RSS/Atom URL, polled by the backend itself → magnet resolution (sukebei → feed item →
    javbus) → 115 offline tasks, re-resolving on a schedule anchored to the release date
    (a fixed cooldown when no source knew it);
  - **archive**: intake normalization (flatten/rename) and per-brand archiving;
  - **mapping**: flat `.strm` tree mirrored to a per-title layout, with a real-time
    watchdog for incremental syncs plus a periodic full sync.
  Each pipeline has enable/disable, manual trigger (rss supports the Rank label), live
  status, and per-run stats/errors/log tail. The download-tracking panel also carries
  **手动添加**, the third input source: paste AVIDs (or file names to read them from) and
  pick the CloudDrive directory they download into, browsed from the tree and defaulting
  to the last one used. Only a directory with an archive route can be picked — the tracker
  locates a finished download through the route tables — and the tracker polls whatever
  directory the ledger still has work in, so a picked one need not be a source's own.
- **Fill Actor** (`/fill-actor`): scan a JavBus actor's catalog against the local library,
  starting from actor IDs or an AVID (single actors continue directly; multi-actor titles
  present a choice), submit missing titles to the acquisition tracker (same intake as the
  rss pipeline), prewarm RSSHub feeds, hand off FreshRSS subscriptions, and safely move
  matching files through CloudDrive — carried over from embyx-web with the same durable
  job queue and move-safety guarantees, now on PostgreSQL.
- **Settings** (`/settings`): CloudDrive, FreshRSS, RSSHub URLs, Fill Actor library
  roots, pipeline behavior, and avid parsing rules are stored in the database, editable
  from the browser, versioned against concurrent edits, and hot-applied without
  restarts. It also manages the rss pipeline's **subscriptions** — feed URL plus
  category, enable/disable, last poll and error — and can import an existing FreshRSS
  subscription list in one step. CloudDrive and FreshRSS
  panels have connection-test buttons that use the unsaved form values (secrets fall back
  to stored ones). Secrets are never echoed back.

## Requirements

- Python 3.13+ and [uv](https://docs.astral.sh/uv/)
- Node.js 22+ (frontend build)
- PostgreSQL 14+

## Configuration

Deployment-level settings come from the environment; everything else lives in the
database and is edited on the Settings page.

| Variable | Scope | Purpose | Default |
| --- | --- | --- | --- |
| `EMBYX_MANAGER_DATABASE_URL` | app | PostgreSQL DSN | `postgresql://localhost/embyx_manager` |
| `EMBYX_MANAGER_API_TOKEN` | app | Bearer token for mutation endpoints | optional on loopback |
| `EMBYX_MANAGER_TLS_TERMINATED` | app | Assert TLS-terminating proxy for non-loopback bind | `false` |
| `EMBYX_MANAGER_HOST` / `EMBYX_MANAGER_PORT` | app | Bind address / port | `127.0.0.1:8000` |
| `EMBYX_MANAGER_MAX_REQUEST_BYTES` | app | Maximum mutation body | `65536` |
| `EMBYX_MANAGER_MAX_ACTORS` / `EMBYX_MANAGER_MAX_VIDEOS` | fill actor | Per-plan limits | `20` / `2000` |
| `EMBYX_MANAGER_MAGNET_CONCURRENCY` | fill actor | Magnet lookup concurrency | `8` |

The `EMBYX_MANAGER_` prefix is historical: the last two configure Fill Actor
specifically, not the app. They stay in the environment because raising them raises the
request pressure on JavBus and Sukebei — an operator decision, not a UI toggle.

### Fill Actor library roots (Settings page)

The library roots live in the database and are edited on the **补全演员** card of the
Settings page — `actor_root`, `additional_roots`, `move_in_root`, `move_in_by_brand`,
`root_sentinel`, `apply_enabled`, and the three CloudDrive move paths. Saving them takes
effect without a restart.

Create the sentinel file in every configured root so an empty mount is never mistaken
for the real library. `additional_roots` must be on the same filesystem as
`move_in_root` (moves are renames); `actor_root` is read-only and may live elsewhere.
Note that unlike embyx-web, the mapping/archive pipelines write to their media mounts —
mount those paths read-write.

Saving the card invalidates any scan produced under the previous settings: applying it
would move files against roots that have since changed, so the browser is asked to
re-scan instead.

Until the section is configured, Fill Actor reports itself as not configured and its
page links to Settings; the dashboard, the pipelines and the health probe are unaffected.
A fresh deployment is therefore configured entirely from the browser.

`EMBYX_MANAGER_ACTOR_ROOT`, `ADDITIONAL_ROOTS`, `MOVE_IN_ROOT`, `MOVE_IN_BY_BRAND`,
`ROOT_SENTINEL`, `APPLY_ENABLED`, `CLOUD_STRM_MOUNT_PREFIX`, `CLOUD_SOURCE_ROOTS` and
`CLOUD_MOVE_IN_ROOT` no longer exist. They are ignored if still present, so a stale
deployment variable cannot override what the Settings page stored.

The schema migrates automatically at startup (`schema_migrations`, advisory-lock
serialized across replicas).

### Health

`GET /api/health` reports **app-level** readiness only: `status` and the HTTP code
(200/503) reflect the database, the one dependency every feature shares. Point container
probes here.

A feature's own dependencies live under its own key and never change `status` — today
that is `fill_actor` (`configured`, `roots`, `cloud`, `legacy_journal`, `apply_enabled`,
`scan_ready`, `apply_ready`). An unmounted library root or an expired CloudDrive authorization degrades
Fill Actor, which says so on its own page, while the monitor pipelines, the settings page
and the probes stay green. The top-bar status chip shows app-level health, so it does not
go red for one feature.

When `EMBYX_MANAGER_API_TOKEN` is set, every mutation (scan, move, pipeline trigger,
config save, connection test) needs `Authorization: Bearer <token>`. The browser then
opens on a login screen: the token is checked against `GET /api/auth/session` before it
is accepted, and stored in `localStorage`, so a reload or a new tab stays signed in
until **退出登录** in the top bar. A token the server no longer accepts is dropped on
the next visit and the login screen explains why. `GET /api/health` reports
`auth_required`, which is what tells the browser whether to ask for a login at all.

The login screen is a convenience gate, not the security boundary — the API enforces the
token on every mutation regardless of what the browser does. Reads (health, monitor
status, run history, config) stay open by design.

### Importing a legacy config.toml

```bash
uv run embyx-manager import-config /path/to/config.toml --database-url postgresql://...
```

Maps the embyx-monitor `[clouddrive]`, `[freshrss]`, `[avid]`, `[archive]`, and
`[mapping]` sections into the config store. Pipelines stay disabled until enabled from
the dashboard.

## Layout

Every feature owns its own slice on both sides, and the app root owns none of them:

| | Backend | Frontend |
| --- | --- | --- |
| app root | `api.py` (shell, auth, health, SPA hosting), `errors.py` | `App.tsx`, `components/{Login,Feedback,Icons}`, `lib/{apiToken,errors}` |
| fill actor | `fill_actor/api.py` | `pages/FillActorPage.tsx`, `components/fill-actor/`, `lib/fill-actor/` |
| monitor | `monitor/api.py` | `pages/DashboardPage.tsx` |
| settings | `config/api.py` | `pages/SettingsPage.tsx` |

`create_app` takes routers, health probes, lifespans and exception handlers — never a
feature's service — so adding or removing a feature touches only `bootstrap.py`. Fill
Actor used to be wired into the app root itself, a leftover from embyx-web.

## Development

```bash
uv sync --locked
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

PostgreSQL-backed tests are gated in GitHub Actions CI, which provides a disposable
Postgres service container via `EMBYX_MANAGER_TEST_DATABASE_URL`. Locally they skip
automatically when that variable is unset — do not point it at a real database: the
fixtures drop and recreate the target database's `public` schema between tests.

Frontend:

```bash
cd frontend
npm ci
npm run dev        # proxies /api to 127.0.0.1:8000
npm run lint
npm test
npm run build      # writes src/embyx_manager/static, served at /
```

Run the app:

```bash
uv run embyx-manager serve
```

## Container image

Self-contained two-stage build (frontend bundle → wheel → slim runtime); no base-image
layering:

```bash
docker build -t embyx-manager:local .
```

Pushes to `main` publish `ghcr.io/<owner>/embyx-manager:latest` and an immutable
`sha-<commit>` tag for `linux/amd64` and `linux/arm64`. Pin the digest in production.

Deployment notes:

- provide `EMBYX_MANAGER_DATABASE_URL` from a Secret;
- mount the media volumes (read-write for the mapping/archive targets) and create the
  sentinel file in each, then point the Settings page's Fill Actor card at those paths;
- bind non-loopback only with `EMBYX_MANAGER_API_TOKEN` and
  `EMBYX_MANAGER_TLS_TERMINATED=true` behind a TLS-terminating proxy;
- CloudDrive/FreshRSS/RSSHub endpoints and credentials are entered on the Settings page
  (stored in PostgreSQL), not in the environment.

## License

GPL-3.0-or-later; see [LICENSE](LICENSE).

The AVID parser in `src/embyx_manager/core/avid.py` is derived from
[JavSP](https://github.com/Yuukiy/JavSP) (GPL-3.0). Parts of its tag-stripping rules and
test corpus come from [metatube-sdk-go](https://github.com/metatube-community/metatube-sdk-go)
(Apache-2.0); the affected files carry the corresponding notices.
