"""The merge Job's worker, run against real ffmpeg on tiny generated parts."""

import hashlib
import shutil
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from embyx_manager.merge import worker
from embyx_manager.merge.worker import (
    MergeRefusedError,
    MergeWorkerError,
    StatusFile,
    concat_list,
    partial_path,
    read_status,
)

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

    # A refusal is the check's answer, not a broken Job: it ends cleanly.
    assert code == 0
    status = read_status(status_path)
    assert status is not None
    assert status['state'] == 'failed'
    assert status['refused'] is True
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

    with pytest.raises(MergeRefusedError, match=r'hold [\d,]+ packets, whose mp4 index needs about'):
        worker.merge(parts, tmp_path / 'ABC-123.mp4', status, reserve_bytes=0, workdir=tmp_path, memory_limit=1 << 20)
    assert not partial_path(tmp_path / 'ABC-123.mp4').exists()


@needs_ffmpeg
def test_merge_re_encodes_parts_bigger_than_115_accepts_to_av1(
    tmp_path: Path, parts: list[Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    total = sum(part.stat().st_size for part in parts)
    monkeypatch.setattr(worker, 'MAX_UPLOAD_BYTES', total - 1)
    monkeypatch.setattr(worker, 'TRANSCODE_PRESET', '12')
    status = StatusFile(tmp_path / 'status.json')

    result = worker.merge(parts, tmp_path / 'ABC-123.mp4', status, reserve_bytes=0, workdir=tmp_path)

    merged = tmp_path / 'ABC-123.mp4'
    assert result['size'] == merged.stat().st_size < total
    assert result['duration'] == pytest.approx(6.0, abs=0.5)
    completed = subprocess.run(  # noqa: S603 - fixed arguments
        [  # noqa: S607
            'ffprobe',
            '-v',
            'error',
            '-show_entries',
            'stream=codec_name,codec_tag_string',
            '-of',
            'csv=p=0',
            str(merged),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert completed.stdout.split() == ['av1,av01', 'aac,mp4a']
    written = read_status(tmp_path / 'status.json')
    assert written is not None
    # The status keeps the bitrate the transcoding phase chose.
    assert written['video_bitrate'] > 0


@needs_ffmpeg
@pytest.mark.parametrize('suffix', ['.avi', '.mov'])
def test_merge_refuses_parts_bigger_than_115_accepts_in_a_container_it_cannot_re_encode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, suffix: str
) -> None:
    # MOV cannot carry AV1, so it is refused like AVI.
    parts = [make_part(tmp_path / f'ABC-123-cd{index}{suffix}') for index in range(1, 3)]
    monkeypatch.setattr(worker, 'MAX_UPLOAD_BYTES', 1000)
    status = StatusFile(tmp_path / 'status.json')

    with pytest.raises(
        MergeRefusedError, match=rf'115 accepts in one file, and a \{suffix} merge cannot be re-encoded'
    ):
        worker.merge(parts, tmp_path / f'ABC-123{suffix}', status, reserve_bytes=0, workdir=tmp_path)


@needs_ffmpeg
def test_merge_refuses_a_re_encode_that_still_comes_out_too_big(
    tmp_path: Path, parts: list[Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    total = sum(part.stat().st_size for part in parts)
    monkeypatch.setattr(worker, 'MAX_UPLOAD_BYTES', total - 1)
    monkeypatch.setattr(worker, 'TRANSCODE_PRESET', '12')
    # A budget the encoder cannot hold to: the size check after the encode has the last word.
    monkeypatch.setattr(worker, 'transcode_bitrate', lambda *_args: 50_000_000)
    status = StatusFile(tmp_path / 'status.json')

    with pytest.raises(MergeRefusedError, match='re-encoded file came to'):
        worker.merge(parts, tmp_path / 'ABC-123.mp4', status, reserve_bytes=0, workdir=tmp_path)
    assert not (tmp_path / 'ABC-123.mp4').exists()


def test_transcode_bitrate_fits_the_budget_or_a_share_of_the_source() -> None:
    hours = 48 * 3600.0
    # IDBD-905: 130 GB over 48 h, 256 kb/s AAC: the source share (0.4 x 5.76 Mb/s) is lower.
    bits = worker.transcode_bitrate(129_600_000_000, hours, 256_000)
    assert bits == pytest.approx(0.4 * (129_600_000_000 * 8 / hours - 256_000), rel=1e-6)
    # A source far above the budget gets the budget, audio included, under 115 GB.
    bits = worker.transcode_bitrate(400_000_000_000, hours, 256_000)
    assert (bits + 256_000) * hours / 8 == pytest.approx(worker.MAX_UPLOAD_BYTES * worker.TRANSCODE_BUDGET_SHARE)
    # The home worker's NVENC overshoots its target, so it aims lower under the limit.
    home = worker.transcode_bitrate(
        400_000_000_000, hours, 256_000, share=worker.HOME_SOURCE_SHARE, budget_share=worker.HOME_BUDGET_SHARE
    )
    assert (home + 256_000) * hours / 8 * 1.15 < worker.MAX_UPLOAD_BYTES
    with pytest.raises(MergeRefusedError, match='audio alone'):
        worker.transcode_bitrate(400_000_000_000, hours, 10**9)


def test_transcode_memory_scales_with_the_frame_and_keeps_a_floor() -> None:
    def video(width: object, height: object) -> worker.Probe:
        return worker.Probe(duration=1.0, signature=(('video', 'h264', width, height, None, None),))

    # UNVRSP-003's 4096x2048 VR frames are the reference.
    assert worker.transcode_memory(video(4096, 2048)) == worker.TRANSCODE_MEMORY_BYTES
    assert worker.transcode_memory(video(1920, 1080)) == worker.TRANSCODE_MEMORY_FLOOR_BYTES
    assert worker.transcode_memory(video(8192, 4096)) == 4 * worker.TRANSCODE_MEMORY_BYTES
    # No size to go by: assume the reference.
    assert worker.transcode_memory(video(None, None)) == worker.TRANSCODE_MEMORY_BYTES


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
def test_a_merge_short_of_space_fails_the_job(tmp_path: Path, parts: list[Path]) -> None:
    status_path = tmp_path / 'status.json'

    code = worker.run(
        list(map(str, parts)), output=str(tmp_path / 'ABC-123.mp4'), status_path=str(status_path), reserve_gib=1 << 30
    )

    # Room can be made, so this is a failure to retry rather than a refusal.
    assert code == 1
    status = read_status(status_path)
    assert status is not None
    assert (status['state'], status.get('refused')) == ('failed', None)


@needs_ffmpeg
def test_merge_keeps_the_parts_container_and_refuses_a_mix(tmp_path: Path, parts: list[Path]) -> None:
    status = StatusFile(tmp_path / 'status.json')

    with pytest.raises(MergeRefusedError, match='share one container'):
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
