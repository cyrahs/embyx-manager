"""The home transcode worker against a stand-in cluster, with real ffmpeg on tiny parts."""

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from embyx_manager.merge import home_worker, worker
from embyx_manager.merge.home_worker import LocalChunk, RunClock, TaskRun, WorkerConfig, kept_chunks
from embyx_manager.merge.transcode import audio_status_path, chunk_path

needs_ffmpeg = pytest.mark.skipif(shutil.which('ffmpeg') is None, reason='ffmpeg is not installed')
# B-frames give the encoder a reorder delay, as NVENC has; ``realtime`` slows it enough to stop it midway.
ENCODER = {'codec': 'libx264', 'extra': ['-preset', 'veryfast', '-bf', '3', '-vf', 'realtime=speed=4']}


def make_part(path: Path, seconds: float, audio_extra: float = 0.0) -> str:
    subprocess.run(  # noqa: S603 - fixed arguments
        [  # noqa: S607 - ffmpeg from PATH
            'ffmpeg',
            '-hide_banner',
            '-loglevel',
            'error',
            '-y',
            '-f',
            'lavfi',
            '-i',
            f'testsrc=size=160x120:rate=30000/1001:duration={seconds}',
            '-f',
            'lavfi',
            '-i',
            f'sine=frequency=440:duration={seconds + audio_extra}',
            '-c:v',
            'libx264',
            '-g',
            '30',
            '-pix_fmt',
            'yuv420p',
            '-c:a',
            'aac',
            str(path),
        ],
        check=True,
    )
    return str(path)


def probe(path: Path, entries: str, stream: str = 'v:0') -> list[str]:
    command = ['ffprobe', '-v', 'error', '-select_streams', stream, '-count_packets', '-show_entries', entries]
    completed = subprocess.run(  # noqa: S603 - fixed arguments
        [*command, '-of', 'csv=p=0', str(path)],
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout.split()


class Cluster:
    """Stands in for the cluster's side: keeps chunks as the service does, in order and contiguous."""

    def __init__(self, directory: Path, parts: list[str], durations: list[float]) -> None:
        self.directory = directory
        self.parts = parts
        self.durations = durations
        self.chunks: dict[int, list[LocalChunk]] = {index: [] for index in range(len(parts))}
        self.finished: set[int] = set()

    def part_url(self, _task_id: int, part_index: int) -> str:
        return self.parts[part_index]

    def heartbeat(self, _report: dict[str, Any]) -> dict[str, Any]:
        return {'active': True, 'paused': False}

    def upload(self, _task_id: int, chunk: LocalChunk, _limiter: object, _meter: object) -> dict[str, Any]:
        mine = self.chunks[chunk.part_index]
        assert chunk.index == len(mine)
        assert abs(chunk.start - (mine[-1].end if mine else 0.0)) <= 0.25
        target = chunk_path(self.directory, chunk.part_index, chunk.index)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(chunk.path, target)
        mine.append(chunk)
        return {'stored': True}

    def finish(self, _task_id: int, part_index: int) -> dict[str, Any]:
        mine = self.chunks[part_index]
        assert abs(mine[-1].end - self.durations[part_index]) <= 2.0
        self.finished.add(part_index)
        return {'finished': True}

    def assignment(self) -> dict[str, Any]:
        return {
            'id': 7,
            'avid': 'SQTEVR-009',
            'video_bitrate': 300_000,
            'chunk_seconds': 2,
            'encoder': ENCODER,
            'paused': False,
            'parts': [
                {
                    'index': index,
                    'name': Path(part).name,
                    'size': Path(part).stat().st_size,
                    'duration': self.durations[index],
                    'done': index in self.finished,
                    'next_chunk': len(self.chunks[index]),
                    'resume_at': self.chunks[index][-1].end if self.chunks[index] else 0.0,
                }
                for index, part in enumerate(self.parts)
            ],
        }

    def manifest(self) -> dict[str, Any]:
        return {
            'parts': [
                {'index': index, 'chunks': [{'index': c.index, 'duration': c.duration} for c in chunks]}
                for index, chunks in self.chunks.items()
            ],
        }


class StopAfter:
    """Ctrl+C pressed ``presses`` times once the cluster holds ``chunks`` chunks."""

    def __init__(self, cluster: Cluster, chunks: int | None, presses: int = 1) -> None:
        self.cluster = cluster
        self.chunks = chunks
        self.presses = presses

    def __call__(self) -> int:
        held = sum(len(chunks) for chunks in self.cluster.chunks.values())
        return self.presses if self.chunks is not None and held >= self.chunks else 0


class Quiet:
    def __call__(self, *_args: object, **_kwargs: object) -> None:
        return None

    def status(self, _line: str) -> None:
        return None


@needs_ffmpeg
def test_a_stopped_and_resumed_transcode_assembles_with_every_frame_in_sync(tmp_path: Path) -> None:
    library = tmp_path / 'library'
    library.mkdir()
    parts = [make_part(library / 'SQTEVR-009-cd1.mp4', 14.0, 0.2), make_part(library / 'SQTEVR-009-cd2.mp4', 5.0)]
    directory = tmp_path / 'upload' / '.transcode' / '7'
    output = tmp_path / 'upload' / 'SQTEVR-009.mp4'

    assert worker.run(parts, output=str(output), status_path=str(audio_status_path(directory)), reserve_gib=0,
                      home_prepare=str(directory)) == 0  # fmt: skip
    prepared = json.loads(audio_status_path(directory).read_text())
    assert prepared['state'] == 'done'
    durations = [part['duration'] for part in prepared['parts']]

    cluster = Cluster(directory, parts, durations)
    config = WorkerConfig(server='http://cluster', cache_dir=tmp_path / 'cache', jobs=1, encoder=ENCODER)
    log = Quiet()
    # Ctrl+C once the first chunk is in: the run finishes the chunk under way and stops.
    outcome = TaskRun(config, cluster, cluster.assignment(), log).run(StopAfter(cluster, 1))  # type: ignore[arg-type]
    assert outcome == 'stopped'
    assert 1 <= len(cluster.chunks[0]) < 6
    assert cluster.chunks[1] == []
    assert not cluster.finished

    outcome = TaskRun(config, cluster, cluster.assignment(), log).run(StopAfter(cluster, None))  # type: ignore[arg-type]
    assert outcome == 'done'
    assert cluster.finished == {0, 1}
    (directory / 'manifest.json').write_text(json.dumps(cluster.manifest()))

    status = tmp_path / 'upload' / '.merge' / '7.json'
    assert worker.run([], output=str(output), status_path=str(status), reserve_gib=0, home_assemble=str(directory)) == 0
    assert json.loads(status.read_text())['state'] == 'done'
    source_frames = sum(int(probe(Path(part), 'stream=nb_read_packets')[0]) for part in parts)
    assert int(probe(output, 'stream=nb_read_packets')[0]) == source_frames
    # The second part's first frame sits where the first part ended, audio and all.
    times = sorted(float(value) for value in probe(output, 'packet=pts_time'))
    first_part_frames = int(probe(Path(parts[0]), 'stream=nb_read_packets')[0])
    assert times[first_part_frames] == pytest.approx(durations[0], abs=0.02)
    audio = float(probe(output, 'stream=duration', 'a:0')[0])
    assert audio == pytest.approx(sum(durations), abs=0.1)


def test_chunk_times_drop_the_encoders_reorder_delay() -> None:
    clock = RunClock(anchor=10.01, shift=0.0833)
    # As the segment list reports a run resumed at 10.01 s: shifted by the B-frame delay.
    start, end = 0.0, 10.0933
    assert clock.anchor + max(start - clock.shift, 0.0) == pytest.approx(10.01)
    assert clock.anchor + (end - clock.shift) == pytest.approx(20.02)


def test_kept_chunks_resume_only_a_contiguous_run(tmp_path: Path) -> None:
    def keep(index: int, start: float) -> None:
        path = tmp_path / f'chunk-{index:05d}.mkv'
        path.write_bytes(b'x')
        LocalChunk(
            part_index=0, index=index, start=start, duration=2.0, frames=60, bytes=1, sha256='0' * 64, path=path
        ).save()

    keep(3, 6.0)
    keep(4, 8.0)
    keep(6, 12.0)

    kept = kept_chunks(tmp_path, 3, 6.0)

    assert [chunk.index for chunk in kept] == [3, 4]


def test_the_config_file_and_flags_combine(tmp_path: Path) -> None:
    path = tmp_path / 'config.toml'
    path.write_text('server = "http://10.117.200.1:8080"\nupload_mbps = 25\n[encoder]\ntune = "hq"\n')
    parser = home_worker.argparse.ArgumentParser()
    home_worker.add_arguments(parser)

    config = home_worker.load_config(parser.parse_args(['--config', str(path), '--jobs', '3']))

    assert (config.server, config.upload_mbps, config.jobs, config.encoder) == (
        'http://10.117.200.1:8080',
        25,
        3,
        {'tune': 'hq'},
    )
