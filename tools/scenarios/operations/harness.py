"""Harness-only operations: they prove the runner, not the application.

`harness.trivial` touches each part of a transcript once, the same way the Rust harness test
does: it renames a seeded file, makes one `MoveFile` call through the baseline's own
CloudDrive client to the fake, and updates one seeded row with the scenario clock.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from fakes.clouddrive import to_json

from embyx_manager.clients.clouddrive.client import CloudDriveClient

if TYPE_CHECKING:
    from runner import Context


async def trivial(ctx: Context) -> Any:
    source = ctx.roots['upload'] / 'in' / 'a.txt'
    target = ctx.roots['upload'] / 'out' / 'a.txt'
    target.parent.mkdir()
    source.rename(target)
    client = CloudDriveClient(address=ctx.clouddrive_address, api_token='scenario-token', secure=False)  # noqa: S106
    try:
        moved = await asyncio.to_thread(client.move_file, ['/115/in/a.txt'], '/115/out', 2)
    finally:
        client.close()
    await ctx.execute("UPDATE items SET state = 'moved', moved_at = $1 WHERE name = $2", ctx.now, 'a.txt')
    return {'move': to_json(moved), 'target': str(target)}


OPERATIONS = {'harness.trivial': trivial}
