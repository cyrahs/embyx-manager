"""Run the baseline's merge worker once against real ffmpeg and print every status it wrote.

The Rust test `crates/embyx-merge/tests/ffmpeg_parity.rs` runs the same command through the
Rust worker on a copy of the same parts and compares the two runs: exit code, stderr and
each status write (without `updated_at`). The status clock stands still, so only the
forced writes happen and the sequence does not depend on how fast ffmpeg runs.

Usage: `uv run python tools/merge_ffmpeg/python_worker.py '<command as JSON>'`, where the
command has the `merge-worker` arguments (`parts`, `output`, `status`, `reserve_gib`,
`home_prepare`, `home_assemble`) and optionally `max_upload_bytes` and `transcode_preset`.
"""

from __future__ import annotations

import contextlib
import io
import json
import sys
from typing import TYPE_CHECKING, Any

from embyx_manager.merge import worker

if TYPE_CHECKING:
    from pathlib import Path


def main() -> None:
    command = json.loads(sys.argv[1])
    writes: list[str] = []

    class RecordingStatusFile(worker.StatusFile):
        def __init__(self, path: Path) -> None:
            super().__init__(path, clock=lambda: 0.0)

        def update(self, *, force: bool = True, **fields: Any) -> None:
            # The clock stands still, so asking it here does not change what update does.
            writes_now = force or self._clock() - self._last_write >= worker.STATUS_INTERVAL_SECONDS
            super().update(force=force, **fields)
            if writes_now:
                writes.append(self._path.read_text(encoding='utf-8'))

    worker.StatusFile = RecordingStatusFile  # type: ignore[misc]
    if 'max_upload_bytes' in command:
        worker.MAX_UPLOAD_BYTES = command['max_upload_bytes']
    if 'transcode_preset' in command:
        worker.TRANSCODE_PRESET = command['transcode_preset']
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr):
        code = worker.run(
            command['parts'],
            output=command['output'],
            status_path=command['status'],
            reserve_gib=command['reserve_gib'],
            home_prepare=command.get('home_prepare'),
            home_assemble=command.get('home_assemble'),
        )
    print(json.dumps({'code': code, 'stderr': stderr.getvalue(), 'writes': writes}, ensure_ascii=False))


if __name__ == '__main__':
    main()
