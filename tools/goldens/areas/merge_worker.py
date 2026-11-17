"""The merge Job's worker (`merge/worker.py`, WP13), replayed with canned ffprobe and ffmpeg answers.

Each scenario runs the baseline's own `worker.run` in a temporary directory, as the merge Job
does, with `subprocess`, the disk's free space, the cgroup memory limit, the monotonic clock
and the wall clock replaced. The golden holds the scenario (the files laid out first, what each
ffprobe and ffmpeg call answers) and what the worker did: every ffprobe and ffmpeg argument
list with the concat scripts it handed ffmpeg, every status file text in the order written, the
exit code (or the exception Python did not catch), what it printed and the files left behind.
The Rust worker (`embyx_merge::worker`) replays each scenario and must do the same, byte for
byte (PLAN.md, cross-version contract 4).

Paths are written with `<root>` for the temporary directory. Files are created with zeros
(sparse, so a part of 40 GB costs nothing); a file ffmpeg writes holds `bytes(range(256))`
repeated, or zeros when it is larger than 64 MiB, so its SHA-1 is fixed. The status file's
clock moves one second per update and `updated_at` is fixed.
"""

from __future__ import annotations

import contextlib
import errno
import io
import itertools
import json
import os
import shutil
import tempfile
import traceback
import types
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self

from embyx_manager.merge import transcode, worker

ROOT = '<root>'
SMALL_FILE_BYTES = 1 << 20
NOW = datetime(2026, 11, 13, 22, 30, 26, 123456, tzinfo=UTC)
SPARSE_ABOVE = 64 * 1024 * 1024
GB = 10**9


def written_bytes(size: int) -> bytes:
    """What the fake ffmpeg writes: a fixed pattern, so the merged file's SHA-1 is fixed."""
    pattern = bytes(range(256))
    return (pattern * (size // 256 + 1))[:size]


def stream(kind: str, **fields: Any) -> dict[str, Any]:
    if kind == 'video':
        base = {'codec_name': 'h264', 'codec_type': 'video', 'width': 160, 'height': 120, 'nb_frames': '20'}
    elif kind == 'audio':
        base = {
            'codec_name': 'aac',
            'codec_type': 'audio',
            'sample_rate': '44100',
            'channels': 1,
            'bit_rate': '128000',
            'nb_frames': '94',
        }
    else:
        base = {'codec_name': 'mov_text', 'codec_type': kind}
    base.update(fields)
    return {key: value for key, value in base.items() if value is not None}


def probe(duration: Any, streams: list[dict[str, Any]] | None = None, **extra: Any) -> dict[str, Any]:
    """An ffprobe answer: `-print_format json -show_format -show_streams`."""
    if streams is None:
        streams = [stream('video'), stream('audio')]
    payload = {'streams': [{'index': index, **item} for index, item in enumerate(streams)], 'format': {}}
    if duration is not None:
        payload['format']['duration'] = duration
    payload.update(extra)
    return {'returncode': 0, 'stdout': json.dumps(payload, indent=4) + '\n', 'stderr': ''}


def failed_probe(stderr: str, returncode: int = 1) -> dict[str, Any]:
    return {'returncode': returncode, 'stdout': '', 'stderr': stderr}


def raw_probe(stdout: str) -> dict[str, Any]:
    return {'returncode': 0, 'stdout': stdout, 'stderr': ''}


def progress(duration: float, *, steps: int = 4, size: int = 4096) -> str:
    """ffmpeg's `-progress pipe:1` blocks, ending at `duration`."""
    lines = []
    for step in range(1, steps + 1):
        micros = int(duration * 1_000_000 * step / steps)
        lines += [
            'frame=10',
            f'total_size={size * step}',
            f'out_time_us={micros}',
            f'out_time_ms={micros}',
            'speed=1x',
            f'progress={"end" if step == steps else "continue"}',
        ]
    return '\n'.join(lines) + '\n'


def ffmpeg(write: int | None = 5000, *, stdout: str = '', stderr: str = '', returncode: int = 0) -> dict[str, Any]:
    return {'write': write, 'stdout': stdout, 'stderr': stderr, 'returncode': returncode}


def parts_of(count: int, suffix: str = '.mp4', size: int = 1_000_000, avid: str = 'ABC-123') -> dict[str, Any]:
    return {f'library/{avid}-cd{index}{suffix}': {'size': size} for index in range(1, count + 1)}


def part_paths(count: int, suffix: str = '.mp4', avid: str = 'ABC-123') -> list[str]:
    return [f'{ROOT}/library/{avid}-cd{index}{suffix}' for index in range(1, count + 1)]


def scenario(name: str, **spec: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        'name': name,
        'files': {},
        'command': {
            'parts': [],
            'output': f'{ROOT}/upload/ABC-123.mp4',
            'status': f'{ROOT}/upload/.merge/1.json',
            'reserve_gib': 0.0,
            'home_prepare': None,
            'home_assemble': None,
        },
        'free': 10**12,
        'memory_max': '3221225472\n',
        'probes': {},
        'ffmpeg': [],
    }
    command = spec.pop('command', {})
    base.update(spec)
    base['command'].update(command)
    return base


def merge_scenarios() -> list[dict[str, Any]]:
    three = parts_of(3)
    probes3 = {path: probe('2.000000') for path in part_paths(3)}
    partial = f'{ROOT}/upload/.ABC-123.mp4.part'
    out: list[dict[str, Any]] = []
    out.append(
        scenario(
            'merge_plain_mp4',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes={**probes3, partial: probe('6.016000')},
            ffmpeg=[ffmpeg(5000, stdout=progress(6.0, steps=12))],
        )
    )
    # Twenty MiB written: the hash reports progress block by block.
    out.append(
        scenario(
            'merge_hash_in_blocks',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes={**probes3, partial: probe('6.0')},
            ffmpeg=[ffmpeg(20 * 1024 * 1024 + 17, stdout=progress(6.0))],
        )
    )
    reordered = {
        path: probe('2.0', [stream('audio'), stream('video')] if index in {1, 3} else None)
        for index, path in enumerate(part_paths(3, '.mkv'), start=1)
    }
    out.append(
        scenario(
            'merge_reorders_parts_video_first',
            files={**parts_of(3, '.mkv'), 'upload/': {}},
            command={'parts': part_paths(3, '.mkv'), 'output': f'{ROOT}/upload/ABC-123.mkv'},
            probes={**reordered, f'{ROOT}/upload/.ABC-123.mkv.part': probe('6.0')},
            ffmpeg=[ffmpeg(100, stdout=progress(2.0)), ffmpeg(100, stdout=progress(2.0)), ffmpeg(3000)],
        )
    )
    # The second remux fails: the staging directory goes, the error names that ffmpeg run.
    out.append(
        scenario(
            'merge_reorder_fails',
            files={**parts_of(3, '.mkv'), 'upload/': {}},
            command={'parts': part_paths(3, '.mkv'), 'output': f'{ROOT}/upload/ABC-123.mkv'},
            probes=reordered,
            ffmpeg=[
                ffmpeg(100, stdout=progress(2.0)),
                ffmpeg(40, stderr='[matroska @ 0x1] broken\nError muxing\n', returncode=1),
            ],
        )
    )
    # A NaN in a stream property: json.loads gives every NaN the same object, so the
    # signatures still compare equal and the merge goes ahead.
    nan_probe = raw_probe(
        '{"format": {"duration": "2.0"}, "streams": [{"codec_type": "video", "codec_name": "h264", '
        '"width": NaN, "height": 120}]}'
    )
    out.append(
        scenario(
            'merge_nan_in_signature',
            files={**parts_of(2), 'upload/': {}},
            command={'parts': part_paths(2)},
            probes={**dict.fromkeys(part_paths(2), nan_probe), partial: probe('4.0')},
            ffmpeg=[ffmpeg(5000, stdout=progress(4.0))],
        )
    )
    # ffprobe itself is missing: Python's OSError text, a failed merge.
    out.append(
        scenario(
            'merge_ffprobe_missing',
            files={**parts_of(2), 'upload/': {}},
            command={'parts': part_paths(2)},
            probes={part_paths(2)[0]: {'oserror': errno.ENOENT}},
        )
    )
    out.append(
        scenario(
            'merge_reorder_short_of_space',
            files={**parts_of(3, '.mkv'), 'upload/': {}},
            command={'parts': part_paths(3, '.mkv'), 'output': f'{ROOT}/upload/ABC-123.mkv'},
            probes=reordered,
            free=3_500_000,
        )
    )
    # Three parts of 40 GB, 4 h each: more than 115 accepts, so the video is re-encoded.
    big = {f'library/ABC-123-cd{index}.mp4': {'size': 40 * GB} for index in range(1, 4)}
    big_probe = probe('14400.000000', [stream('video', width=3840, height=1920), stream('audio', bit_rate='256000')])
    out.append(
        scenario(
            'merge_re_encodes_to_av1',
            files={**big, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes={**dict.fromkeys(part_paths(3), big_probe), partial: probe('43200.5')},
            ffmpeg=[ffmpeg(7000, stdout=progress(43200.0, steps=3))],
        )
    )
    out.append(
        scenario(
            'merge_re_encode_still_too_big',
            files={**big, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes={**dict.fromkeys(part_paths(3), big_probe), partial: probe('43200.0')},
            ffmpeg=[ffmpeg(116 * GB)],
        )
    )
    out.append(
        scenario(
            'merge_audio_alone_leaves_no_room',
            files={**big, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes=dict.fromkeys(
                part_paths(3), probe('14400.0', [stream('video'), stream('audio', bit_rate='30000000')])
            ),
        )
    )
    out.append(
        scenario(
            'merge_re_encode_of_nothing',
            files={**big, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes=dict.fromkeys(part_paths(3), probe('0.0')),
        )
    )
    out.extend(
        scenario(
            f'merge_too_big_for_{suffix[1:]}',
            files={**{f'library/ABC-123-cd{index}{suffix}': {'size': 60 * GB} for index in (1, 2)}, 'upload/': {}},
            command={'parts': part_paths(2, suffix), 'output': f'{ROOT}/upload/ABC-123{suffix}'},
        )
        for suffix in ('.mov', '.avi')
    )
    out.append(
        scenario(
            'merge_one_part',
            files={**parts_of(1), 'upload/': {}},
            command={'parts': part_paths(1)},
        )
    )
    out.append(
        scenario(
            'merge_mixed_containers',
            files={**parts_of(2), 'library/ABC-123-cd3.MKV': {'size': 10}, 'upload/': {}},
            command={'parts': [*part_paths(2), f'{ROOT}/library/ABC-123-cd3.MKV']},
        )
    )
    out.append(
        scenario(
            'merge_unknown_container',
            files={**parts_of(2, '.FLV'), 'upload/': {}},
            command={'parts': part_paths(2, '.FLV'), 'output': f'{ROOT}/upload/ABC-123.FLV'},
        )
    )
    out.append(
        scenario(
            'merge_part_missing',
            files={**parts_of(1), 'upload/': {}},
            command={'parts': part_paths(2)},
        )
    )
    out.append(
        scenario(
            'merge_no_video',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes={**probes3, part_paths(3)[1]: probe('2.0', [stream('audio'), stream('subtitle')])},
        )
    )
    out.append(
        scenario(
            'merge_different_streams',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes={**probes3, part_paths(3)[2]: probe('2.0', [stream('video', width=320), stream('audio')])},
        )
    )
    # 1 == 1.0 and '44100' stays a string: equal for Python, so the parts match.
    out.append(
        scenario(
            'merge_streams_equal_as_python_compares',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes={
                **probes3,
                part_paths(3)[1]: probe('2.0', [stream('video', width=160.0), stream('audio', channels=True)]),
                partial: probe('6.0'),
            },
            ffmpeg=[ffmpeg(10)],
        )
    )
    out.append(
        scenario(
            'merge_index_outgrows_memory',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3)},
            memory_max='1048576\n',
            probes={
                **probes3,
                part_paths(3)[0]: probe('2.0', [stream('video', nb_frames='12345678'), stream('audio')]),
            },
        )
    )
    out.append(
        scenario(
            'merge_without_memory_limit',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3)},
            memory_max='max\n',
            probes={**probes3, partial: probe('6.0')},
            ffmpeg=[ffmpeg(10)],
        )
    )
    out.append(
        scenario(
            'merge_uncounted_packets_and_bitrate',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3)},
            memory_max='1\n',
            probes={
                **dict.fromkeys(
                    part_paths(3),
                    probe('2.0', [stream('video', nb_frames=None), stream('audio', bit_rate=None, nb_frames='N/A')]),
                ),
                partial: probe('6.0'),
            },
            ffmpeg=[ffmpeg(10)],
        )
    )
    out.append(
        scenario(
            'merge_short_of_space',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3), 'reserve_gib': 20.0},
            free=5 * 1024**3,
        )
    )
    # A partial from an interrupted attempt is about to be overwritten: its bytes count as free.
    out.append(
        scenario(
            'merge_counts_a_leftover_partial_as_free',
            files={**three, 'upload/.ABC-123.mp4.part': {'size': 2_500_000}},
            command={'parts': part_paths(3)},
            free=1_000_000,
            probes={**probes3, partial: probe('6.0')},
            ffmpeg=[ffmpeg(10)],
        )
    )
    out.append(
        scenario(
            'merge_output_directory_missing',
            files=three,
            command={'parts': part_paths(3), 'output': f'{ROOT}/missing/ABC-123.mp4'},
        )
    )
    out.append(
        scenario(
            'merge_ffprobe_cannot_read',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes={**probes3, part_paths(3)[1]: failed_probe('[mov] moov atom not found\r\nInvalid data found\n\n')},
        )
    )
    out.append(
        scenario(
            'merge_ffprobe_silent_failure',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes={**probes3, part_paths(3)[0]: failed_probe('', 1)},
        )
    )
    for label, answer in (
        ('no_format', raw_probe('{"streams": []}')),
        ('not_json', raw_probe('not json')),
        ('empty', raw_probe('')),
        ('duration_text', probe('N/A')),
        ('no_duration', probe(None)),
        ('duration_null', raw_probe('{"format": {"duration": null}}')),
        ('duration_list', raw_probe('{"format": {"duration": [2]}}')),
        ('format_list', raw_probe('{"format": [1]}')),
        ('top_level_list', raw_probe('[1, 2]')),
    ):
        out.append(
            scenario(
                f'merge_ffprobe_{label}',
                files={**three, 'upload/': {}},
                command={'parts': part_paths(3)},
                probes={**probes3, part_paths(3)[0]: answer},
            )
        )
    # Python's float() reads all of these.
    out.append(
        scenario(
            'merge_durations_python_reads',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes={
                part_paths(3)[0]: probe(' 2.0\n'),
                part_paths(3)[1]: probe(2),
                part_paths(3)[2]: probe('1_9.99e-1'),
                partial: probe('6.0E0'),
            },
            ffmpeg=[ffmpeg(10)],
        )
    )
    for label, answer in (
        ('streams_not_a_list', raw_probe('{"format": {"duration": "2.0"}, "streams": 5}')),
        ('stream_not_a_dict', raw_probe('{"format": {"duration": "2.0"}, "streams": ["video"]}')),
        ('codec_type_a_list', probe('2.0', [{'codec_type': ['video']}])),
        ('superscript_frames', probe('2.0', [stream('video', nb_frames='²'), stream('audio')])),
    ):
        out.append(
            scenario(
                f'merge_uncaught_{label}',
                files={**three, 'upload/': {}},
                command={'parts': part_paths(3)},
                probes={**probes3, part_paths(3)[0]: answer},
            )
        )
    tail = ''.join(f'[h264 @ 0x1] error {index}\n' for index in range(25))
    out.append(
        scenario(
            'merge_ffmpeg_fails_with_its_last_lines',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes=probes3,
            ffmpeg=[ffmpeg(100, stdout=progress(3.0, steps=2), stderr=tail + '   \n\r\nlast words  \n', returncode=1)],
        )
    )
    out.append(
        scenario(
            'merge_ffmpeg_fails_silently',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes=probes3,
            ffmpeg=[ffmpeg(None, returncode=3)],
        )
    )
    out.append(
        scenario(
            'merge_ffmpeg_missing',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes=probes3,
            ffmpeg=[{'oserror': errno.ENOENT}],
        )
    )
    out.append(
        scenario(
            'merge_duration_does_not_match',
            files={**three, 'upload/': {}},
            command={'parts': part_paths(3)},
            probes={**probes3, partial: probe('9.5')},
            ffmpeg=[ffmpeg(100)],
        )
    )
    out.append(
        scenario(
            'merge_paths_as_pathlib_writes_them',
            files={**parts_of(2, avid="it's"), 'upload/': {}},
            command={
                'parts': [f"{ROOT}//library/./it's-cd1.mp4", f"{ROOT}/library//it's-cd2.mp4/"],
                'output': f'{ROOT}/upload/./ABC-123.mp4',
                'status': f'{ROOT}//upload/.merge//1.json',
            },
            probes={
                f"{ROOT}/library/it's-cd1.mp4": probe('2.0'),
                f"{ROOT}/library/it's-cd2.mp4": probe('2.0'),
                partial: probe('4.0'),
            },
            ffmpeg=[ffmpeg(10)],
        )
    )
    return out


def home_scenarios() -> list[dict[str, Any]]:
    transcode_dir = f'{ROOT}/upload/.transcode/7'
    big = {f'library/ABC-123-cd{index}.mp4': {'size': 60 * GB} for index in (1, 2)}
    big_probe = probe('14400.0', [stream('video', width=4096, height=2048), stream('audio', bit_rate='256000')])
    out: list[dict[str, Any]] = []
    prepare = {
        'parts': part_paths(2),
        'home_prepare': transcode_dir,
        'status': f'{ROOT}/upload/.transcode/7/audio.json',
    }
    out.append(
        scenario(
            'prepare_copies_the_audio',
            files={**big, 'upload/': {}},
            command=prepare,
            probes=dict.fromkeys(part_paths(2), big_probe),
            ffmpeg=[ffmpeg(2000, stdout=progress(14400.0)), ffmpeg(2000, stdout=progress(14400.0))],
        )
    )
    out.append(
        scenario(
            'prepare_keeps_audio_already_copied',
            files={**big, 'upload/.transcode/7/audio/cd01.mka': {'size': 1234}},
            command=prepare,
            probes=dict.fromkeys(part_paths(2), big_probe),
            ffmpeg=[ffmpeg(2000)],
        )
    )
    out.append(
        scenario(
            'prepare_without_audio',
            files={**big, 'upload/': {}},
            command=prepare,
            probes=dict.fromkeys(part_paths(2), probe('14400.0', [stream('video')])),
        )
    )
    out.append(
        scenario(
            'prepare_refuses_mov',
            files={f'library/ABC-123-cd{index}.mov': {'size': 10} for index in (1, 2)},
            command={**prepare, 'parts': part_paths(2, '.mov'), 'output': f'{ROOT}/upload/ABC-123.mov'},
        )
    )
    out.append(
        scenario(
            'prepare_short_of_space',
            files={**big, 'upload/': {}},
            command=prepare,
            free=10 * GB,
            probes=dict.fromkeys(part_paths(2), big_probe),
        )
    )
    out.append(
        scenario(
            'prepare_audio_copy_fails',
            files={**big, 'upload/': {}},
            command=prepare,
            probes=dict.fromkeys(part_paths(2), big_probe),
            ffmpeg=[ffmpeg(50, stderr='Stream map 0:a matches no streams.\n', returncode=1)],
        )
    )

    def manifest(parts: list[list[tuple[int, float]]]) -> dict[str, Any]:
        return {
            'text': json.dumps(
                {
                    'parts': [
                        {'index': index, 'chunks': [{'index': chunk, 'duration': length} for chunk, length in chunks]}
                        for index, chunks in enumerate(parts)
                    ]
                }
            )
        }

    def prepared(durations: list[float], *, has_audio: Any = True, state: str = 'done') -> dict[str, Any]:
        return {
            'text': json.dumps(
                {'state': state, 'has_audio': has_audio, 'parts': [{'duration': value} for value in durations]}
            )
        }

    def chunks(layout: list[list[tuple[int, float]]], *, audio: bool = True) -> dict[str, Any]:
        files: dict[str, Any] = {}
        for index, part in enumerate(layout):
            for chunk, _length in part:
                files[f'upload/.transcode/7/cd{index + 1:02d}/chunk-{chunk:05d}.mkv'] = {'size': 1000 + chunk}
            if audio:
                files[f'upload/.transcode/7/audio/cd{index + 1:02d}.mka'] = {'size': 500}
        return files

    layout = [[(0, 60.0), (1, 60.0), (2, 30.5)], [(0, 60.0), (1, 59.9)]]
    chunk_probe = probe('60.0', [stream('video', codec_name='av1', width=4096, height=2048, nb_frames=None)])
    chunk_probes = {
        f'{transcode_dir}/cd01/chunk-00000.mkv': chunk_probe,
        f'{transcode_dir}/cd02/chunk-00000.mkv': chunk_probe,
    }
    assemble = {'home_assemble': transcode_dir, 'parts': []}
    files = {
        **chunks(layout),
        'upload/.transcode/7/manifest.json': manifest(layout),
        'upload/.transcode/7/audio.json': prepared([150.25, 119.9]),
    }
    out.append(
        scenario(
            'assemble_lines_up_chunks_and_audio',
            files=files,
            command=assemble,
            probes={**chunk_probes, f'{ROOT}/upload/.ABC-123.mp4.part': probe('270.15')},
            ffmpeg=[ffmpeg(9000, stdout=progress(270.15))],
        )
    )
    out.append(
        scenario(
            'assemble_without_audio',
            files={
                **chunks(layout, audio=False),
                'upload/.transcode/7/manifest.json': manifest(layout),
                'upload/.transcode/7/audio.json': prepared([150.25, 119.9], has_audio=0),
            },
            command={**assemble, 'output': f'{ROOT}/upload/ABC-123.mkv'},
            probes={**chunk_probes, f'{ROOT}/upload/.ABC-123.mkv.part': probe('270.0')},
            ffmpeg=[ffmpeg(9000)],
        )
    )
    out.append(scenario('assemble_without_a_manifest', files={'upload/.transcode/7/': {}}, command=assemble))
    out.append(
        scenario(
            'assemble_before_the_audio_is_done',
            files={**files, 'upload/.transcode/7/audio.json': prepared([150.25, 119.9], state='running')},
            command=assemble,
        )
    )
    out.append(
        scenario(
            'assemble_with_a_part_too_many',
            files={**files, 'upload/.transcode/7/audio.json': prepared([150.25])},
            command=assemble,
        )
    )
    out.append(
        scenario(
            'assemble_part_without_chunks',
            files={**files, 'upload/.transcode/7/manifest.json': manifest([[(0, 60.0)], []])},
            command=assemble,
            probes=chunk_probes,
        )
    )
    missing = dict(files)
    del missing['upload/.transcode/7/cd02/chunk-00001.mkv']
    out.append(scenario('assemble_chunk_missing', files=missing, command=assemble, probes=chunk_probes))
    out.append(
        scenario(
            'assemble_chunks_that_differ',
            files=files,
            command=assemble,
            probes={**chunk_probes, f'{transcode_dir}/cd02/chunk-00000.mkv': probe('60.0', [stream('video')])},
        )
    )
    # Chunks running past the part: the last one keeps its own length.
    long_layout = [[(0, 100.0), (1, 60.0)], [(0, 200.0)]]
    out.append(
        scenario(
            'assemble_keeps_a_last_chunk_that_runs_long',
            files={
                **chunks(long_layout),
                'upload/.transcode/7/manifest.json': manifest(long_layout),
                'upload/.transcode/7/audio.json': prepared([90.0, 150.0]),
            },
            command=assemble,
            probes={**chunk_probes, f'{ROOT}/upload/.ABC-123.mp4.part': probe('240.0')},
            ffmpeg=[ffmpeg(10)],
        )
    )
    out.append(
        scenario(
            'assemble_uncaught_malformed_manifest',
            files={**files, 'upload/.transcode/7/manifest.json': {'text': '{"parts": [{"index": 0}, {"index": 1}]}'}},
            command=assemble,
        )
    )
    return out


class _FakeProcess:
    def __init__(self, answer: dict[str, Any]) -> None:
        self.stdout = io.StringIO(answer['stdout'], newline=None)
        self.stderr = io.StringIO(answer['stderr'], newline=None)
        self.returncode = answer['returncode']

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def wait(self) -> int:
        return self.returncode

    def poll(self) -> int:
        return self.returncode

    def kill(self) -> None:
        return None


class _Recorder:
    """Stands in for `subprocess` in the worker module and keeps what was asked of it."""

    def __init__(self, spec: dict[str, Any], root: Path) -> None:
        self.spec = spec
        self.root = root
        self.calls: list[dict[str, Any]] = []
        self.ffmpeg = list(spec['ffmpeg'])
        self.PIPE = -1

    def mask(self, text: str) -> str:
        return text.replace(str(self.root), ROOT)

    def run(self, argv: list[str], **_kwargs: Any) -> types.SimpleNamespace:
        self.calls.append({'argv': [self.mask(arg) for arg in argv]})
        answer = self.spec['probes'].get(self.mask(argv[-1]), failed_probe('No such file or directory\n'))
        if 'oserror' in answer:
            raise OSError(answer['oserror'], os.strerror(answer['oserror']), argv[0])
        return types.SimpleNamespace(
            returncode=answer['returncode'],
            stdout=answer['stdout'].encode(),
            stderr=answer['stderr'].encode(),
        )

    def Popen(self, argv: list[str], **_kwargs: Any) -> _FakeProcess:  # noqa: N802 - subprocess.Popen
        scripts = {}
        for flag, value in itertools.pairwise(argv):
            if flag == '-i' and value.endswith('.txt'):
                scripts[self.mask(value)] = self.mask(Path(value).read_text(encoding='utf-8'))
        self.calls.append({'argv': [self.mask(arg) for arg in argv], 'scripts': scripts})
        answer = self.ffmpeg.pop(0)
        if 'oserror' in answer:
            raise OSError(answer['oserror'], os.strerror(answer['oserror']), argv[0])
        if answer['write'] is not None:
            target = Path(argv[-1])
            if answer['write'] > SPARSE_ABOVE:
                with target.open('wb') as handle:
                    handle.truncate(answer['write'])
            else:
                target.write_bytes(written_bytes(answer['write']))
        return _FakeProcess(answer)


def lay_out(root: Path, files: dict[str, Any]) -> None:
    for relative, entry in files.items():
        path = root / relative
        if relative.endswith('/'):
            path.mkdir(parents=True, exist_ok=True)
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        if 'text' in entry:
            path.write_text(entry['text'], encoding='utf-8')
        else:
            with path.open('wb') as handle:
                handle.truncate(entry['size'])


def masked_size(path: Path, root: Path) -> int:
    """Size of a file with the temporary directory written as `<root>` (small files only)."""
    size = path.stat().st_size
    if size > SMALL_FILE_BYTES:
        return size
    return len(path.read_bytes().replace(str(root).encode(), ROOT.encode()))


def tree(root: Path) -> list[list[Any]]:
    found = []
    for dirpath, dirnames, filenames in os.walk(root):
        for name in filenames:
            path = Path(dirpath) / name
            found.append([path.relative_to(root).as_posix(), masked_size(path, root)])
        for name in dirnames:
            path = Path(dirpath) / name
            if not any(path.iterdir()):
                found.append([path.relative_to(root).as_posix() + '/', None])
    return sorted(found)


def replay(spec: dict[str, Any]) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix='merge-golden-') as directory:
        root = Path(directory)
        lay_out(root, spec['files'])
        (root / 'cgroup').mkdir()
        (root / 'cgroup' / 'memory.max').write_text(spec['memory_max'], encoding='utf-8')
        (root / 'tmp').mkdir()
        recorder = _Recorder(spec, root)
        writes: list[str] = []

        class RecordingStatusFile(worker.StatusFile):
            def __init__(self, path: Path) -> None:
                ticks = iter(range(1, 10**6))
                super().__init__(path, clock=lambda: float(next(ticks)))

            def update(self, *, force: bool = True, **fields: Any) -> None:
                before = self._last_write
                super().update(force=force, **fields)
                if self._last_write != before:
                    writes.append(recorder.mask(self._path.read_text(encoding='utf-8')))

        fake_datetime = types.SimpleNamespace(now=lambda _tz=None: NOW)
        fake_shutil = types.SimpleNamespace(
            disk_usage=lambda path: (Path(path).stat(), types.SimpleNamespace(free=spec['free']))[1],
            rmtree=shutil.rmtree,
        )
        real_memory_limit = worker.memory_limit
        command = {
            key: (value.replace(ROOT, str(root)) if isinstance(value, str) else value)
            for key, value in spec['command'].items()
        }
        command['parts'] = [part.replace(ROOT, str(root)) for part in spec['command']['parts']]
        saved = {
            name: getattr(worker, name) for name in ('subprocess', 'shutil', 'datetime', 'StatusFile', 'memory_limit')
        }
        stderr = io.StringIO()
        result: dict[str, Any] = {}
        old_tmpdir = os.environ.get('TMPDIR')
        try:
            worker.subprocess = recorder  # type: ignore[assignment]
            worker.shutil = fake_shutil  # type: ignore[assignment]
            worker.datetime = fake_datetime  # type: ignore[assignment]
            worker.StatusFile = RecordingStatusFile  # type: ignore[misc]
            worker.memory_limit = lambda: real_memory_limit(root / 'cgroup' / 'memory.max')
            os.environ['TMPDIR'] = str(root / 'tmp')
            with contextlib.redirect_stderr(stderr):
                try:
                    result['code'] = worker.run(
                        command['parts'],
                        output=command['output'],
                        status_path=command['status'],
                        reserve_gib=command['reserve_gib'],
                        home_prepare=command['home_prepare'],
                        home_assemble=command['home_assemble'],
                    )
                    result['uncaught'] = None
                except Exception as exc:  # noqa: BLE001 - an uncaught exception is the outcome
                    result['code'] = None
                    result['uncaught'] = type(exc).__name__
                    # The last line of the traceback Python prints.
                    result['traceback_tail'] = recorder.mask(traceback.format_exception_only(exc)[-1].rstrip('\n'))
        finally:
            for name, value in saved.items():
                setattr(worker, name, value)
            if old_tmpdir is None:
                os.environ.pop('TMPDIR', None)
            else:
                os.environ['TMPDIR'] = old_tmpdir
        if recorder.ffmpeg:
            msg = f'{spec["name"]}: {len(recorder.ffmpeg)} ffmpeg answer(s) left over'
            raise AssertionError(msg)
        return {
            'scenario': spec,
            'calls': recorder.calls,
            'writes': writes,
            **result,
            'stderr': recorder.mask(stderr.getvalue()),
            'tree': tree(root),
        }


def _functions() -> list[dict[str, Any]]:
    def outcome(function: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
        try:
            return {'value': function(*args, **kwargs)}
        except Exception as exc:  # noqa: BLE001 - the class and text are the golden
            return {'error': {'type': type(exc).__name__, 'message': str(exc)}}

    bitrates = []
    for total, duration, audio in (
        (129_600_000_000, 172800.0, 256_000),
        (400_000_000_000, 172800.0, 256_000),
        (400_000_000_000, 172800.0, 10**9),
        (120_000_000_000, 43200.0, 0),
        (120_000_000_000, 1e-9, 0),
        (120_000_000_000, 43200.123456789, 320_000),
        (1, 1.0, 0),
        (0, 3.0, 0),
    ):
        for share, budget in ((worker.TRANSCODE_SOURCE_SHARE, worker.TRANSCODE_BUDGET_SHARE), (0.5, 0.8)):
            bitrates.append(
                {
                    'args': [total, duration, audio, share, budget],
                    **outcome(worker.transcode_bitrate, total, duration, audio, share=share, budget_share=budget),
                }
            )
    memory = []
    for width, height in ((4096, 2048), (1920, 1080), (8192, 4096), (None, None), (3840, 1920), (7, 3)):
        result = worker.Probe(duration=1.0, signature=(('video', 'h264', width, height, None, None),))
        memory.append({'width': width, 'height': height, 'value': worker.transcode_memory(result)})
    paths = [
        {
            'part': part,
            'chunk': chunk,
            'chunk_path': str(transcode.chunk_path(Path('/w'), part, chunk)),
            'audio_path': str(transcode.audio_path(Path('/w'), part)),
        }
        for part, chunk in ((0, 0), (8, 12), (99, 99999), (-1, 3), (-3, -5))
    ]
    scripts = [
        {
            'entries': [[path, length] for path, length in entries],
            'concat_script': transcode.concat_script([(Path(path), length) for path, length in entries]),
            'concat_list': worker.concat_list([Path(path) for path, _ in entries]),
        }
        for entries in (
            [('/a/b.mkv', 60.0), ("/a/it's.mkv", 1 / 3), ('/a/中文 x.mka', 1e-7), ('/a/c', 12345.6789995)],
            [('/x', 0.0)],
            [('/y', -2.5), ('/z', 1e21)],
        )
    ]
    with tempfile.TemporaryDirectory(prefix='merge-golden-') as directory:
        limit = Path(directory) / 'memory.max'
        limits = []
        for content in ('3221225472\n', 'max\n', ' 42 ', '', '-5', '1e9', '٣٢'):
            limit.write_text(content, encoding='utf-8')
            limits.append({'content': content, **outcome(worker.memory_limit, limit)})
        limits.append({'content': None, **outcome(worker.memory_limit, Path(directory) / 'missing')})
    return [
        {
            'transcode_bitrate': bitrates,
            'transcode_memory': memory,
            'paths': paths,
            'scripts': scripts,
            'memory_limit': limits,
        }
    ]


def _replays(scenarios: list[dict[str, Any]]) -> list[dict[str, Any]]:
    names = [spec['name'] for spec in scenarios]
    if len(set(names)) != len(names):
        msg = 'scenario names repeat'
        raise AssertionError(msg)
    return [replay(spec) for spec in scenarios]


GOLDENS = {
    'merge/worker_merge.json': lambda: _replays(merge_scenarios()),
    'merge/worker_home.json': lambda: _replays(home_scenarios()),
    'merge/worker_functions.json': _functions,
}
