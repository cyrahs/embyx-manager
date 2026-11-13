"""Scenario operations: `OPERATIONS` maps an operation name to an async callable.

An operation receives the scenario context (`runner.Context`) and returns a JSON value, the
transcript's `outcome.result`; an exception becomes `outcome.error`. Each module adds its
names here; the Rust harness registers the same names (docs/rewrite/SCENARIOS.md).
"""

from __future__ import annotations

from typing import Any

from operations import clouddrive, config, db, harness, kube

OPERATIONS: dict[str, Any] = {
    **harness.OPERATIONS,
    **db.OPERATIONS,
    **config.OPERATIONS,
    **clouddrive.OPERATIONS,
    **kube.OPERATIONS,
}
