# embyx-manager

Unified web management and monitoring surface for `embyx` media workflows. Merges the former
`embyx-web` (Fill Actor web UI) and `embyx-monitor` (RSS ingestion, archive, STRM mapping
automation) into one FastAPI + React application backed by PostgreSQL.

## Status

Under construction. See `docs/` for design notes.

## Requirements

- Python 3.13 + [uv](https://docs.astral.sh/uv/)
- Node.js (frontend build)
- PostgreSQL

## Development

```bash
uv sync --locked
uv run pytest
uv run ruff check .
```

```bash
cd frontend
npm ci
npm run dev
```
