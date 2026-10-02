"""The merge Job's worker, run against real ffmpeg on tiny generated parts."""

import hashlib
import shutil
import subprocess
from pathlib import Path

import pytest

from embyx_manager.merge import worker
from embyx_manager.merge.worker import MergeWorkerError, StatusFile, concat_list, partial_path, read_status

pytestmark = pytest.mark.skipif(shutil.which('ffmpeg') is None, reason='ffmpeg is not installed')


def make_part(path: Path, *, size: str = '160x120', seconds: int = 2) -> Path:
    subprocess.run(  # noqa: S603 - fixed arguments
        [  # noqa: S607 - ffmpeg from PATH, as the image provides it
            'ffmpeg',
            '-hide_banner',
            '-loglevel',
            'error',
            '-y',
            '-f',
            'lavfi',
            '-i',
            f'testsrc=size={size}:rate=10:duration={seconds}',
            '-f',
            'lavfi',
            '-i',
            f'sine=frequency=440:duration={seconds}',
            '-c:v',
            'libx264',
            '-pix_fmt',
            'yuv420p',
            '-c:a',
            'aac',
            '-shortest',
            str(path),
        ],
        check=True,
    )
    return path


@pytest.fixture
def parts(tmp_path: Path) -> list[Path]:
    library = tmp_path / 'library'
    library.mkdir()
    return [make_part(library / f'ABC-123-cd{index}.mp4') for index in range(1, 4)]


def test_merge_concatenates_parts_and_reports_size_and_sha1(tmp_path: Path, parts: list[Path]) -> None:
    out = tmp_path / 'upload'
    out.mkdir()
    status_path = out / '.merge' / '1.json'

    code = worker.run(
        [str(part) for part in parts], output=str(out / 'ABC-123.mp4'), status_path=str(status_path), reserve_gib=0
    )

    assert code == 0
    merged = out / 'ABC-123.mp4'
    status = read_status(status_path)
    assert status is not None
    assert status['state'] == 'done'
    assert status['size'] == merged.stat().st_size
    assert status['sha1'] == hashlib.sha1(merged.read_bytes()).hexdigest().upper()  # noqa: S324
    assert status['duration'] == pytest.approx(6.0, abs=0.5)
    assert not partial_path(merged).exists()


def test_merge_refuses_parts_with_different_streams(tmp_path: Path, parts: list[Path]) -> None:
    odd = make_part(parts[0].parent / 'ABC-123-cd4.mp4', size='320x240')
    status_path = tmp_path / 'status.json'

    code = worker.run(
        [*map(str, parts), str(odd)],
        output=str(tmp_path / 'ABC-123.mp4'),
        status_path=str(status_path),
        reserve_gib=0,
    )

    assert code == 1
    status = read_status(status_path)
    assert status is not None
    assert status['state'] == 'failed'
    assert 'ABC-123-cd4.mp4 carries different streams' in status['error']
    assert not (tmp_path / 'ABC-123.mp4').exists()


def test_merge_refuses_to_start_without_room_for_the_reserve(tmp_path: Path, parts: list[Path]) -> None:
    status = StatusFile(tmp_path / 'status.json')

    with pytest.raises(MergeWorkerError, match='not enough space'):
        worker.merge(parts, tmp_path / 'ABC-123.mp4', status, reserve_bytes=1 << 60, workdir=tmp_path)


def test_merge_keeps_the_parts_container_and_refuses_a_mix(tmp_path: Path, parts: list[Path]) -> None:
    status = StatusFile(tmp_path / 'status.json')

    with pytest.raises(MergeWorkerError, match='share one container'):
        worker.merge(parts, tmp_path / 'ABC-123.mkv', status, reserve_bytes=0, workdir=tmp_path)


def test_concat_list_escapes_quotes() -> None:
    assert concat_list([Path("/a/it's.mp4"), Path('/b/c.mp4')]) == "file '/a/it'\\''s.mp4'\nfile '/b/c.mp4'\n"
