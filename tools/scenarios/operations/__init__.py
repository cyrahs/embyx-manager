"""Scenario operations: `OPERATIONS` maps an operation name to an async callable.

An operation receives the scenario context (`runner.Context`) and returns a JSON value, the
transcript's `outcome.result`; an exception becomes `outcome.error`. Each module adds its
names here; the Rust harness registers the same names (docs/rewrite/SCENARIOS.md).
"""

from __future__ import annotations

from typing import Any

from operations import (
    api,
    archive,
    clouddrive,
    config,
    db,
    fill_actor,
    fill_actor_api,
    fill_actor_service,
    harness,
    intake,
    kube,
    mapping,
    merge,
    merge_service,
    playlists,
    rss,
    tracker,
)

OPERATIONS: dict[str, Any] = {
    **harness.OPERATIONS,
    **db.OPERATIONS,
    **config.OPERATIONS,
    **clouddrive.OPERATIONS,
    **kube.OPERATIONS,
    **merge.OPERATIONS,
    **merge_service.OPERATIONS,
    **fill_actor.OPERATIONS,
    **fill_actor_service.OPERATIONS,
    **fill_actor_api.OPERATIONS,
    **intake.OPERATIONS,
    **api.OPERATIONS,
    **archive.OPERATIONS,
    **tracker.OPERATIONS,
    **mapping.OPERATIONS,
    **playlists.OPERATIONS,
    **rss.OPERATIONS,
}
