"""Run one segment of a `merge.service` scenario on the Python baseline, for the resume matrix.

The Rust test `crates/embyx-merge/tests/service_resume.rs` splits a spec's steps at its
`restart` steps and runs the segments in turn, alternating the builds: what one build left in
the database, the roots and the fakes, the other continues. This script is the Python half.
It reads one JSON object on stdin:

    {"spec": "merge/home", "from": 0, "to": 12, "dsn": "postgresql://...",
     "roots": {"work": "/tmp/...", ...}, "now": "2026-11-14T08:00:00+00:00",
     "fakes": {"clouddrive": <dump or seed>, "kubernetes": <dump or seed>, "emby": ...}}

starts the fakes from those states (a dump's `routes_left` become the routes again), runs
`steps[from:to]` with `operations/merge_service.run_steps` and prints one JSON object: the
segment's `steps`, the `clock` at its end, the `calls` its fakes recorded and their `fakes`
dumps. A spec run this way carries no scripted CloudDrive `errors`: a dump does not keep them.

    uv run python tools/scenarios/segment.py < handoff.json
"""

from __future__ import annotations

import asyncio
import json
import sys
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from typing import Any

import runner
from fakes.clouddrive import FakeCloudDriveServer
from fakes.http import FakeHttpServer
from operations import merge_service


def seed_of(state: dict[str, Any]) -> dict[str, Any]:
    """An HTTP fake's seed from its dump (`routes_left`) or from a seed itself."""
    if 'routes_left' not in state:
        return state
    seed = {key: value for key, value in state.items() if key != 'routes_left'}
    seed['routes'] = state['routes_left']
    return seed


async def run(handoff: dict[str, Any]) -> dict[str, Any]:
    spec = runner.load_specs([handoff['spec']])[handoff['spec']]
    if spec['operation'] != 'merge.service':
        msg = f'{handoff["spec"]}: a segment runs only merge.service scenarios'
        raise SystemExit(msg)
    calls: list[dict[str, Any]] = []
    fakes = handoff['fakes']
    with ExitStack() as stack:
        clouddrive = stack.enter_context(FakeCloudDriveServer(fakes.get('clouddrive', {}), calls.append))
        http_fakes = {
            fake: stack.enter_context(FakeHttpServer(fake, seed_of(fakes.get(fake, {})), calls.append))
            for fake in ('kubernetes', 'emby')
        }
        context = runner.Context(
            roots={name: Path(path) for name, path in handoff['roots'].items()},
            now=datetime.fromisoformat(handoff['now']),
            dsn=handoff['dsn'],
            args=dict(spec.get('args', {})),
            clouddrive_address=clouddrive.address,
            http_fakes={fake: server.url for fake, server in http_fakes.items()},
            record=calls.append,
            fakes={'clouddrive': clouddrive.fake, **http_fakes},
        )
        steps = spec['args']['steps'][handoff['from'] : handoff['to']]
        try:
            outcome = await merge_service.run_steps(context, steps, context.now)
        finally:
            await context.close()
        dumped = {
            'clouddrive': clouddrive.fake.dump(),
            **{fake: server.dump() for fake, server in http_fakes.items()},
        }
    return {**outcome, 'calls': calls, 'fakes': dumped}


def main() -> int:
    handoff = json.load(sys.stdin)
    print(json.dumps(asyncio.run(run(handoff)), ensure_ascii=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
