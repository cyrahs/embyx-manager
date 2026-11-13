"""The CloudDrive wrapper's pure parts (WP3): path validation, message conversions, posixpath.

`clients/clouddrive/aio.py` validates every API path and folder name before a call, turns
the protobuf messages into the dicts the pipelines read (`_cloud_file_to_dict`,
`_offline_file_to_dict`, `_copy_task_to_dict`, `UploadState`), and splits paths with
`posixpath`. The Rust port (`embyx_clients::clouddrive::aio`, `embyx_core::posixpath`) must
give the same values and refuse the same inputs with the same text.

Conversions: the input is the message's serialized bytes (hex, deterministic), so the Rust
side decodes exactly what Python converted.
"""

from __future__ import annotations

import posixpath
from typing import Any

from embyx_manager.clients.clouddrive import aio
from embyx_manager.clients.clouddrive import clouddrive_pb2 as pb

_PATHS = [
    '',
    '/',
    '//',
    '///',
    '/a',
    '/a/',
    '/a//b',
    '/a/./b',
    '/a/../b',
    '/../a',
    '/..',
    '/.',
    'a',
    'a/b',
    'a/../..',
    '../a',
    './a',
    '.',
    '..',
    '//a/b',
    '///a/b',
    '/a/b/',
    '/a/b//',
    '/115/embyx_in/ABC-001.mp4',
    '/115/embyx_in/',
    '/115/a b/c.mp4',
    '/中文/文件.mp4',
    'a//',
    '/a/b/..',
    '/a/b/../..',
    '/a/b/../../..',
    '/...',
    '/a/.../b',
    '/a\\b',
    'a/b/../c/./d',
    '//..',
]

_JOIN_TAILS = ['', 'x', '/abs', 'x/', '..']

_API_PATHS = [
    *_PATHS,
    '/path\nname',
    '/path\x00name',
    '/path\x1fname',
    '/path\x7fname',
    '/path\x80name',
    '/path\u0085name',
    '/path\u2028name',
    '/path\tname',
    '/path name',
    '/路径/ok',
    '/emoji/😀',
]

_SEGMENTS = [
    '',
    '.',
    '..',
    '...',
    'ABC',
    'nested/ABC',
    'bad\\name',
    'bad\nname',
    'bad\x00name',
    'bad\x7fname',
    'bad\x1bname',
    'ok\x80name',
    'ok\u0085',
    ' ',
    'ABC-001 (1)',
    '中文',
    '/',
    'a/',
]


def _posixpath() -> list[dict[str, Any]]:
    return [
        {
            'path': path,
            'normpath': posixpath.normpath(path),
            'split': list(posixpath.split(path)),
            'dirname': posixpath.dirname(path),
            'basename': posixpath.basename(path),
            'joins': [{'tail': tail, 'joined': posixpath.join(path, tail)} for tail in _JOIN_TAILS],
        }
        for path in _PATHS
    ]


def _outcome(function: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
    try:
        return {'value': function(*args, **kwargs)}
    except ValueError as exc:
        return {'error': {'type': type(exc).__name__, 'message': str(exc)}}


def _validation() -> list[dict[str, Any]]:
    paths = [
        {
            'value': value,
            'allow_root': _outcome(aio.validate_api_path, value, allow_root=True),
            'deny_root': _outcome(aio.validate_api_path, value, allow_root=False),
        }
        for value in _API_PATHS
    ]
    segments = [{'value': value, 'outcome': _outcome(aio.validate_path_segment, value)} for value in _SEGMENTS]
    return [{'kind': 'api_path', **case} for case in paths] + [{'kind': 'segment', **case} for case in segments]


def _plain(value: Any) -> Any:
    """The converted dict as JSON: StrEnum members are their value, tuples are lists."""
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if isinstance(value, str):
        return str(value)
    return value


def _cloud_files() -> list[Any]:
    plain = pb.CloudDriveFile(id='file-id', name='ABC-001.mp4', fullPathName='/115/ABC-001.mp4', size=123)
    timed = pb.CloudDriveFile(id='d', name='ABC', fullPathName='/115/ABC', isDirectory=True)
    timed.writeTime.seconds = 456
    timed.writeTime.nanos = 789
    zero_time = pb.CloudDriveFile(id='z', name='z')
    zero_time.writeTime.SetInParent()
    hashed = pb.CloudDriveFile(
        id='h',
        name='h.mp4',
        fullPathName='/115/h.mp4',
        size=115_000_000_000,
        fileHashes={2: 'sha1-value', 10: 'ten', 1: 'md5', 3: ''},
    )
    big = pb.CloudDriveFile(id='', name='', fullPathName='', size=-1)
    return [plain, timed, zero_time, hashed, big, pb.CloudDriveFile()]


def _offline_files() -> list[Any]:
    base: dict[str, Any] = {
        'name': 'ABC-123',
        'size': 1024,
        'url': 'magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335aa7c1367a88a',
        'status': pb.OfflineFileStatus.OFFLINE_DOWNLOADING,
        'infoHash': 'c12fe1c06bba254a9dc9f519b335aa7c1367a88a',
        'fileId': '42',
        'add_time': 1_755_000_000,
        'percendDone': 33.33,
        'peers': 7,
    }
    files = [pb.OfflineFile(**base)]  # type: ignore[arg-type]
    files.extend(pb.OfflineFile(**{**base, 'status': status}) for status in (0, 2, 3, 4, 5, 99, -1))  # type: ignore[arg-type]
    files.extend(
        pb.OfflineFile(**{**base, 'percendDone': progress})  # type: ignore[arg-type]
        for progress in (0.0, 100.0, 0.1, 1e-7, 99.99999999, 12345678.9)
    )
    # Upper-casing outside ASCII on purpose: sharp s (both cases) and the dotless i.
    files.append(pb.OfflineFile(**{**base, 'infoHash': 'ÄbcẞßDEFıi'}))  # type: ignore[arg-type]  # noqa: RUF001
    files.append(pb.OfflineFile(size=2**64 - 1, add_time=2**63, peers=2**40))
    files.append(pb.OfflineFile())
    return files


def _copy_tasks() -> list[Any]:
    tasks = [
        pb.CopyTask(
            sourcePath='/upload/merge/ABC.mp4',
            destPath='/115/upload/merge-1',
            status=status,
            totalBytes=15_888_023_552,
            uploadedBytes=1,
            failedFiles=2,
            paused=paused,
            errors=[pb.TaskError(message='first'), pb.TaskError(message='单文件不能大于115GB')],
        )
        for status, paused in ((0, False), (1, True), (2, False), (3, False), (4, False), (7, False), (-3, True))
    ]
    tasks.append(pb.CopyTask())
    return tasks


def _uploads() -> list[Any]:
    return [
        pb.UploadFileInfo(destPath='/115/upload/a.mp4', status='FatalError', errorMessage='code: 10025'),
        pb.UploadFileInfo(destPath='/115/upload/a.mp4', status='FatalError'),
        pb.UploadFileInfo(destPath='/115/upload/a.mp4', status='Transfer', transferedBytes=15_888_023_552),
        pb.UploadFileInfo(destPath='/115/upload/a.mp4', status='fatalerror', errorMessage='lower case'),
        pb.UploadFileInfo(),
    ]


def _upload_state(info: Any) -> dict[str, Any]:
    state = aio.UploadState(
        status=str(info.status), transferred=int(info.transferedBytes), error=str(info.errorMessage)
    )
    return {'status': state.status, 'transferred': state.transferred, 'error': state.error, 'fatal': state.fatal}


def _conversions() -> list[dict[str, Any]]:
    def case(kind: str, message: Any, value: Any) -> dict[str, Any]:
        return {
            'kind': kind,
            'message': message.DESCRIPTOR.full_name,
            'bytes': message.SerializeToString(deterministic=True).hex(),
            'value': _plain(value),
        }

    return [
        *(case('cloud_file', file, aio._cloud_file_to_dict(file)) for file in _cloud_files()),  # noqa: SLF001
        *(case('offline_file', file, aio._offline_file_to_dict(file)) for file in _offline_files()),  # noqa: SLF001
        *(case('copy_task', task, aio._copy_task_to_dict(task)) for task in _copy_tasks()),  # noqa: SLF001
        *(case('upload_state', info, _upload_state(info)) for info in _uploads()),
    ]


GOLDENS = {
    'text/posixpath.json': _posixpath,
    'clouddrive/validation.json': _validation,
    'clouddrive/conversions.json': _conversions,
}
