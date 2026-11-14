"""Run the baseline's home transcode worker against a transcode listener, once.

The Rust test `crates/embyx-api/src/transcode_api/tests/protocol.rs` serves the Rust listener
on a local port, over a scratch database and fake Job and CloudDrive clients, and runs this
script to check that a worker of protocol v1 written in Python still works against it: the
claim, the heartbeats, the chunk uploads with their headers, the finish calls, ffmpeg reading
the parts over HTTP with range requests, and the refusals as `ServerError` codes.

Usage: `uv run python tools/home_worker/python_worker.py '<run as JSON>'`, where the run has
`server`, `token`, `cache_dir`, `encoder` (settings that replace the cluster's, as a worker's
config file may), `chunk_seconds` (replaces the assignment's, so short parts make several
chunks), `stop_after` (Ctrl+C once the cluster took that many chunks, or null) and `refuse`
(after the run, upload one chunk out of order and report the refusal). It prints the claim's
answer, the run's outcome, the chunks taken and the refusal as JSON.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from embyx_manager.merge import home_worker
from embyx_manager.merge.home_worker import (
    LocalChunk,
    RateLimiter,
    RateMeter,
    Server,
    ServerError,
    TaskRun,
    WorkerConfig,
    idle_report,
)


class CountingServer(Server):
    """The worker's own client, counting the chunks the cluster took."""

    def __init__(self, config: WorkerConfig) -> None:
        super().__init__(config)
        self.taken = 0

    def upload(self, task_id: int, chunk: LocalChunk, limiter: RateLimiter, meter: RateMeter) -> dict[str, Any]:
        answer = super().upload(task_id, chunk, limiter, meter)
        self.taken += 1
        return answer


class StopAfter:
    """Ctrl+C pressed once the cluster took ``chunks`` chunks."""

    def __init__(self, server: CountingServer, chunks: int | None) -> None:
        self.server = server
        self.chunks = chunks

    def __call__(self) -> int:
        return int(self.chunks is not None and self.server.taken >= self.chunks)


class Quiet:
    def __call__(self, *_args: object, **_kwargs: object) -> None:
        return None

    def status(self, _line: str) -> None:
        return None


def refusal(server: Server, task: dict[str, Any], cache_dir: Path) -> dict[str, Any]:
    """Upload a chunk two places past the first part's next one: the cluster refuses it."""
    part = task['parts'][0]
    path = cache_dir / 'refused.bin'
    path.write_bytes(b'not a chunk')
    chunk = LocalChunk(
        path=path,
        part_index=int(part['index']),
        index=int(part['next_chunk']) + 1,
        start=float(part['resume_at']) + 5.0,
        duration=2.0,
        frames=60,
        bytes=path.stat().st_size,
        sha256=home_worker.sha256_of(path),
    )
    try:
        answer = server.upload(int(task['id']), chunk, RateLimiter(0), RateMeter())
    except ServerError as exc:
        return {'status': exc.status, 'code': exc.code, 'details': exc.details}
    return {'answer': answer}


def main() -> None:
    run = json.loads(sys.argv[1])
    config = WorkerConfig(
        server=run['server'],
        token=run['token'],
        cache_dir=Path(run['cache_dir']),
        upload_mbps=0.0,
        jobs=1,
        name='protocol-test',
        encoder=run['encoder'],
    )
    config.cache_dir.mkdir(parents=True, exist_ok=True)
    server = CountingServer(config)
    try:
        answer = server.claim(idle_report(config))
        result: dict[str, Any] = {'claim': answer}
        task = answer.get('task')
        if task is not None:
            task['chunk_seconds'] = run['chunk_seconds']
            outcome = TaskRun(config, server, task, Quiet()).run(StopAfter(server, run['stop_after']))  # type: ignore[arg-type]
            result['outcome'] = outcome
            result['taken'] = server.taken
            if run['refuse']:
                result['refusal'] = refusal(server, server.claim(idle_report(config))['task'], config.cache_dir)
    finally:
        server.close()
    print(json.dumps(result))


if __name__ == '__main__':
    main()
