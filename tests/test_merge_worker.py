"""The merge Job's worker, run against real ffmpeg on tiny generated parts."""

import hashlib
import shutil
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from embyx_manager.merge import worker
from embyx_manager.merge.worker import MergeWorkerError, StatusFile, concat_list, partial_path, read_status

needs_ffmpeg = pytest.mark.skipif(shutil.which('ffmpeg') is None, reason='ffmpeg is not installed')


def make_part(path: Path, *, size: str = '160x120', seconds: int = 2, audio_first: bool = False) -> Path:
    order = ['-map', '1:a', '-map', '0:v'] if audio_first else []
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
            *order,
            str(path),
        ],
        check=True,
    )
    return path


def stream_types(path: Path) -> list[str]:
    completed = subprocess.run(  # noqa: S603 - fixed arguments
        ['ffprobe', '-v', 'error', '-show_entries', 'stream=codec_type', '-of', 'csv=p=0', str(path)],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout.split()


@pytest.fixture
def parts(tmp_path: Path) -> list[Path]:
    library = tmp_path / 'library'
    library.mkdir()
    return [make_part(library / f'ABC-123-cd{index}.mp4') for index in range(1, 4)]


@needs_ffmpeg
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


@needs_ffmpeg
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


@needs_ffmpeg
@pytest.mark.parametrize('suffix', ['.mp4', '.mkv'])
def test_merge_puts_parts_with_the_streams_in_another_order_video_first(tmp_path: Path, suffix: str) -> None:
    library = tmp_path / 'library'
    library.mkdir()
    parts = [make_part(library / f'ABC-123-cd{index}{suffix}', audio_first=index in {1, 3}) for index in range(1, 4)]
    assert stream_types(parts[0]) == ['audio', 'video']
    out = tmp_path / 'upload'
    out.mkdir()
    status = StatusFile(out / 'status.json')

    result = worker.merge(parts, out / f'ABC-123{suffix}', status, reserve_bytes=0, workdir=tmp_path)

    assert result['duration'] == pytest.approx(6.0, abs=0.5)
    assert stream_types(out / f'ABC-123{suffix}') == ['video', 'audio']
    assert sorted(path.name for path in out.iterdir()) == [f'ABC-123{suffix}', 'status.json']


@needs_ffmpeg
def test_merge_refuses_an_mp4_whose_index_would_outgrow_the_memory_limit(tmp_path: Path, parts: list[Path]) -> None:
    status = StatusFile(tmp_path / 'status.json')

    with pytest.raises(MergeWorkerError, match=r'hold [\d,]+ packets, whose mp4 index needs about'):
        worker.merge(parts, tmp_path / 'ABC-123.mp4', status, reserve_bytes=0, workdir=tmp_path, memory_limit=1 << 20)
    assert not partial_path(tmp_path / 'ABC-123.mp4').exists()


def test_memory_limit_reads_the_cgroup(tmp_path: Path) -> None:
    limit = tmp_path / 'memory.max'
    limit.write_text('3221225472\n')
    assert worker.memory_limit(limit) == 3 * 1024**3
    limit.write_text('max\n')
    assert worker.memory_limit(limit) is None
    assert worker.memory_limit(tmp_path / 'missing') is None


@needs_ffmpeg
def test_merge_refuses_to_start_without_room_for_the_reserve(tmp_path: Path, parts: list[Path]) -> None:
    status = StatusFile(tmp_path / 'status.json')

    with pytest.raises(MergeWorkerError, match='not enough space'):
        worker.merge(parts, tmp_path / 'ABC-123.mp4', status, reserve_bytes=1 << 60, workdir=tmp_path)


@needs_ffmpeg
def test_merge_keeps_the_parts_container_and_refuses_a_mix(tmp_path: Path, parts: list[Path]) -> None:
    status = StatusFile(tmp_path / 'status.json')

    with pytest.raises(MergeWorkerError, match='share one container'):
        worker.merge(parts, tmp_path / 'ABC-123.mkv', status, reserve_bytes=0, workdir=tmp_path)


def test_concat_list_escapes_quotes() -> None:
    assert concat_list([Path("/a/it's.mp4"), Path('/b/c.mp4')]) == "file '/a/it'\\''s.mp4'\nfile '/b/c.mp4'\n"


def run_fake_ffmpeg(tmp_path: Path, script: str, *, stall_seconds: float = 60) -> BaseException | None:
    """Run ``script`` in ffmpeg's place on a thread, so a deadlock fails the test instead of hanging it."""
    outcome: list[BaseException | None] = []

    def target() -> None:
        try:
            worker._run_ffmpeg(  # noqa: SLF001 - the pipe handling is what is under test
                [sys.executable, '-c', script],
                duration=10.0,
                status=StatusFile(tmp_path / 'status.json'),
                stall_seconds=stall_seconds,
            )
        except BaseException as exc:  # noqa: BLE001 - handed back to the test
            outcome.append(exc)
        else:
            outcome.append(None)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout=30)
    assert not thread.is_alive(), 'the worker hung on ffmpeg'
    return outcome[0]


def test_a_flood_on_stderr_does_not_stall_the_merge(tmp_path: Path) -> None:
    # Far more than a pipe holds is written to stderr before any progress, as a
    # damaged part makes ffmpeg do; then a failure, whose last lines are reported.
    script = (
        'import sys\n'
        'for i in range(20000): sys.stderr.write(f"damaged element {i}\\n")\n'
        'sys.stderr.flush()\n'
        'print("out_time_us=10000000", flush=True)\n'
        'print("total_size=4096", flush=True)\n'
        'sys.exit(1)\n'
    )

    error = run_fake_ffmpeg(tmp_path, script)

    assert isinstance(error, MergeWorkerError)
    assert str(error).startswith('ffmpeg failed: damaged element 19980 |')
    assert str(error).endswith('damaged element 19999')
    status = read_status(tmp_path / 'status.json')
    assert status is not None
    assert status['progress'] == 1.0


def test_ffmpeg_that_stops_moving_is_stopped(tmp_path: Path) -> None:
    # Progress blocks keep coming, as they do from a stuck ffmpeg, but never move.
    script = (
        'import sys, time\n'
        'sys.stderr.write("stuck\\n"); sys.stderr.flush()\n'
        'while True:\n'
        '    print("out_time_us=5000000", flush=True)\n'
        '    print("total_size=4096", flush=True)\n'
        '    time.sleep(0.05)\n'
    )

    error = run_fake_ffmpeg(tmp_path, script, stall_seconds=1)

    assert isinstance(error, MergeWorkerError)
    assert str(error) == 'ffmpeg made no progress for 1 seconds and was stopped: stuck'
