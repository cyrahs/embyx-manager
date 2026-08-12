# embyx-manager

Unified web management and monitoring for `embyx` media workflows. Merges the former
`embyx-web` (Fill Actor web UI) and `embyx-monitor` (RSS ingestion, archive, STRM mapping
automation) into one FastAPI + React application backed by PostgreSQL.

## What it does

- **Fill Actor** (`/`): scan a JavBus actor's catalog against the local library, find
  missing titles, look up magnets, prewarm RSSHub feeds, hand off FreshRSS subscriptions,
  and safely move matching files through CloudDrive — carried over from embyx-web with the
  same durable job queue and move-safety guarantees, now on PostgreSQL.
- **Monitor dashboard** (`/dashboard`): the embyx-monitor pipelines rebuilt as scheduled
  services with persisted run history —
  - **rss**: unread FreshRSS items → magnet resolution (sukebei → RSS table → javbus) →
    115 offline tasks, with a database-backed failed-AVID cooldown;
  - **archive**: intake normalization (flatten/rename) and per-brand archiving;
  - **mapping**: flat `.strm` tree mirrored to a per-title layout, with a real-time
    watchdog for incremental syncs plus a periodic full sync.
  Each pipeline has enable/disable, manual trigger (rss supports the Rank label), live
  status, and per-run stats/errors/log tail.
- **Settings** (`/settings`): CloudDrive, FreshRSS, RSSHub URLs, pipeline behavior, and
  avid parsing rules are stored in the database, editable from the browser, versioned
  against concurrent edits, and hot-applied without restarts. CloudDrive and FreshRSS
  panels have connection-test buttons that use the unsaved form values (secrets fall back
  to stored ones). Secrets are never echoed back.

## Requirements

- Python 3.13+ and [uv](https://docs.astral.sh/uv/)
- Node.js 22+ (frontend build)
- PostgreSQL 14+

## Configuration

Deployment-level settings come from the environment; everything else lives in the
database and is edited on the Settings page.

| Variable | Purpose | Default |
| --- | --- | --- |
| `EMBYX_MANAGER_DATABASE_URL` | PostgreSQL DSN | `postgresql://localhost/embyx_manager` |
| `EMBYX_MANAGER_ACTOR_ROOT` | Primary actor library root | required |
| `EMBYX_MANAGER_ADDITIONAL_ROOTS` | Additional roots (OS path separator) | required |
| `EMBYX_MANAGER_MOVE_IN_ROOT` | Move-in destination root | required |
| `EMBYX_MANAGER_MOVE_IN_BY_BRAND` | Put moved files under `<move-in>/<brand>/` | `false` |
| `EMBYX_MANAGER_APPLY_ENABLED` | Allow CloudDrive move application | `false` |
| `EMBYX_MANAGER_CLOUD_STRM_MOUNT_PREFIX` | Mount prefix inside `.strm` targets | disabled |
| `EMBYX_MANAGER_CLOUD_SOURCE_ROOTS` | API-native source roots (one per additional root) | disabled |
| `EMBYX_MANAGER_CLOUD_MOVE_IN_ROOT` | API-native destination root | disabled |
| `EMBYX_MANAGER_ROOT_SENTINEL` | Required marker file in each root | `.embyx-root` |
| `EMBYX_MANAGER_API_TOKEN` | Bearer token for mutation endpoints | optional on loopback |
| `EMBYX_MANAGER_TLS_TERMINATED` | Assert TLS-terminating proxy for non-loopback bind | `false` |
| `EMBYX_MANAGER_HOST` / `EMBYX_MANAGER_PORT` | Bind address / port | `127.0.0.1:8000` |
| `EMBYX_MANAGER_MAX_ACTORS` / `EMBYX_MANAGER_MAX_VIDEOS` | Per-plan limits | `20` / `2000` |
| `EMBYX_MANAGER_MAGNET_CONCURRENCY` | Magnet lookup concurrency | `8` |
| `EMBYX_MANAGER_MAX_REQUEST_BYTES` | Maximum mutation body | `65536` |

Filesystem roots stay in the environment deliberately: they must match the container's
volume mounts. Create the sentinel file in every configured root so an empty mount is
never mistaken for the real library. Note that unlike embyx-web, the mapping/archive
pipelines write to their media mounts — mount those paths read-write.

The schema migrates automatically at startup (`schema_migrations`, advisory-lock
serialized across replicas).

### Importing a legacy config.toml

```bash
uv run embyx-manager import-config /path/to/config.toml --database-url postgresql://...
```

Maps the embyx-monitor `[clouddrive]`, `[freshrss]`, `[avid]`, `[archive]`, and
`[mapping]` sections into the config store. Pipelines stay disabled until enabled from
the dashboard.

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
- mount the media volumes at the paths referenced by the root variables (read-write for
  the mapping/archive targets) and create the sentinel files;
- bind non-loopback only with `EMBYX_MANAGER_API_TOKEN` and
  `EMBYX_MANAGER_TLS_TERMINATED=true` behind a TLS-terminating proxy;
- CloudDrive/FreshRSS/RSSHub endpoints and credentials are entered on the Settings page
  (stored in PostgreSQL), not in the environment.
