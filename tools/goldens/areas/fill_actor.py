"""The fill actor's stored models and pure helpers (WP10).

`fill_actor/models.json` runs `model_validate_json` of each public model over stored-looking
texts (what `postgres_repository.py` reads back) and records `model_dump_json()` or the
errors' `type`, `loc` and `msg`: the stored bytes both builds write (`public_json`,
`result_json`) and what they accept. `fill_actor/cloud_files.json` covers
`CloudFileMetadata.from_mapping` with the canonical `cloud_file_json` bytes, `matches_identity`,
and `CloudMoveResponse.from_mapping`. `fill_actor/cloud_paths.json` covers
`CloudMovePaths.from_values`, `parse_mapping` over mapping file bytes, and
`destination_directory`. `fill_actor/records.json` covers the private records' constructor
checks (persistence.py's `__post_init__`), the default job progress, the move journal and
CloudDrive move state machines with their error texts, the stable enum values and the API
error codes.
"""

from __future__ import annotations

import json
import tempfile
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

from embyx_manager.fill_actor import errors as fill_actor_errors
from embyx_manager.fill_actor import models, persistence
from embyx_manager.fill_actor.cloud_moves import (
    CloudFileMetadata,
    CloudMovePaths,
    CloudMoveResponse,
)
from embyx_manager.fill_actor.persistence import (
    ApplyJobRecord,
    CandidateKind,
    CandidateRecord,
    CloudMoveOperationRecord,
    CloudMoveOperationState,
    FileFingerprint,
    JobOperation,
    JobProgress,
    JobProgressEvent,
    JobProgressUnit,
    JobRecord,
    JobStage,
    JobState,
    MoveJournalState,
    PlanRecord,
)
from embyx_manager.fill_actor.ports import AcquisitionOutcome

if TYPE_CHECKING:
    from collections.abc import Callable
from embyx_manager.fill_actor.postgres_repository import _cloud_file_to_json

_MODELS: dict[str, type[BaseModel]] = {
    'FillActorPlan': models.FillActorPlan,
    'ActorPlan': models.ActorPlan,
    'MoveCandidate': models.MoveCandidate,
    'VideoPlan': models.VideoPlan,
    'MoveResult': models.MoveResult,
    'ApplyResult': models.ApplyResult,
}

_CANDIDATE = {
    'candidate_id': 'c-1',
    'video_id': 'ABC-001',
    'file_name': 'ABC-001 [演员] "x".mp4',
    'source_label': 'additional-1',
    'destination_conflict': False,
}
_VIDEO = {
    'video_id': 'ABC-001',
    'actor_ids': ['actor-1'],
    'state': 'additional_found',
    'release_date': '2026-01-01',
    'existing_files': ['/library/ABC/ABC-001.mp4'],
    'move_candidates': [_CANDIDATE],
    'warnings': ['不同 行', 'tab\there', 'ctrl\x01\x7f'],  # noqa: RUF001 - on purpose
}
_ACTOR = {
    'actor_id': 'actor-1',
    'scraped_count': 2,
    'video_ids': ['ABC-001', 'ABC-002'],
    'error_code': None,
    'actor_name': '河北彩花',
    'talent_id': 5022,
    'aliases': ['河北彩伽'],
    'error_message': None,
}
_PLAN = {
    'plan_id': 'plan-1',
    'revision': 'revision-1',
    'created_at': '2026-07-13T10:00:00.123456Z',
    'expires_at': '2026-07-13T11:00:00Z',
    'actors': [_ACTOR],
    'videos': [_VIDEO],
}
_RESULT = {
    'candidate_id': 'c-1',
    'video_id': 'ABC-001',
    'file_name': 'ABC-001.mp4',
    'state': 'moved',
    'error_code': None,
}


def _with(base: dict[str, Any], **changes: Any) -> dict[str, Any]:
    updated = dict(base)
    for key, value in changes.items():
        if value is _DROP:
            updated.pop(key, None)
        else:
            updated[key] = value
    return updated


_DROP = object()

# The datetimes `model_dump_json` writes and the other forms pydantic takes or refuses.
_DATETIMES: list[Any] = [
    '2026-07-13T10:00:00Z',
    '2026-07-13T10:00:00.120000Z',
    '2026-07-13T10:00:00.1Z',
    '2026-07-13T10:00:00.1234567Z',
    '2026-07-13T10:00:00.123456789+09:00',
    '2026-07-13T10:00:00+09:00',
    '2026-07-13T10:00:00-05:30',
    '2026-07-13T10:00:00-00:00',
    '2026-07-13T10:00:00+0900',
    '2026-07-13 10:00',
    '2026-07-13t10:00:00z',
    '2026-07-13T10:00:00',
    '2026-07-13',
    '0001-01-01T00:00:00Z',
    '9999-12-31T23:59:59.999999-01:00',
    1700000000,
    1700000000.5,
    '1700000000',
    -1,
    20000000000,
    '2026-07-13T10:00:00+09',
    '2026-07-13T25:00:00',
    '2026-07-13T10:00:60',
    '2026-07-13T10:00:00+24:00',
    '2026-07-13T10:00:00.Z',
    '2026-7-13T10:00:00',
    '2026-02-30T10:00:00',
    '2026-07-13T10:00:00 ',
    '',
    'x',
    None,
    True,
    [],
]
_DATES: list[Any] = [
    '2026-01-01',
    '2026-01-01T00:00:00',
    '2026-01-01T00:00:00Z',
    '2026-01-01T00:00:00+09:00',
    '2026-01-01T01:00:00',
    '2026-02-29',
    '2028-02-29',
    1700006400,
    1700000000,
    0,
    '20260101',
    'x',
    None,
    True,
]


def _model_inputs() -> list[tuple[str, str]]:
    cases: list[tuple[str, str]] = [
        ('FillActorPlan', json.dumps(_PLAN, ensure_ascii=False)),
        ('FillActorPlan', json.dumps(_PLAN)),
        ('FillActorPlan', json.dumps(_with(_PLAN, actors=[], videos=[], extra='ignored'))),
        ('FillActorPlan', json.dumps(_with(_PLAN, plan_id=_DROP, actors=_DROP))),
        ('FillActorPlan', json.dumps(_with(_PLAN, actors=[_with(_ACTOR, scraped_count=-1)], videos=[{}]))),
        ('FillActorPlan', '{"plan_id": "p", "revision": "r", "created_at": "2026-07-13T10:00:00Z"'),
        ('FillActorPlan', '[]'),
        ('FillActorPlan', 'null'),
        ('FillActorPlan', ''),
        ('ActorPlan', json.dumps({'actor_id': 'a', 'scraped_count': 0})),
        ('ActorPlan', json.dumps(_with(_ACTOR, scraped_count=3.0, talent_id=None, aliases=[]))),
        ('ActorPlan', json.dumps(_with(_ACTOR, scraped_count=3.5))),
        ('ActorPlan', json.dumps(_with(_ACTOR, scraped_count='3'))),
        ('ActorPlan', json.dumps(_with(_ACTOR, scraped_count=True))),
        ('ActorPlan', json.dumps(_with(_ACTOR, scraped_count=None))),
        ('ActorPlan', json.dumps(_with(_ACTOR, video_ids='ABC-001'))),
        ('ActorPlan', json.dumps(_with(_ACTOR, video_ids=['ABC-001', 1, None]))),
        ('ActorPlan', json.dumps(_with(_ACTOR, error_code='not_found', error_message='AVBase: 403 "x"'))),
        ('ActorPlan', json.dumps(_with(_ACTOR, talent_id='5022'))),
        ('ActorPlan', json.dumps(_with(_ACTOR, talent_id=5022.0))),
        ('ActorPlan', json.dumps(_with(_ACTOR, actor_id=7, error_code=1))),
        ('ActorPlan', json.dumps(_with(_ACTOR, scraped_count=10**30))),
        ('MoveCandidate', json.dumps(_CANDIDATE, ensure_ascii=False)),
        ('MoveCandidate', json.dumps(_with(_CANDIDATE, destination_conflict='true'))),
        ('MoveCandidate', json.dumps(_with(_CANDIDATE, destination_conflict=1))),
        ('MoveCandidate', json.dumps(_with(_CANDIDATE, destination_conflict='maybe'))),
        ('MoveCandidate', json.dumps(_with(_CANDIDATE, destination_conflict=_DROP))),
        ('VideoPlan', json.dumps(_VIDEO, ensure_ascii=False)),
        ('VideoPlan', json.dumps({'video_id': 'X', 'actor_ids': [], 'state': 'queued'})),
        ('VideoPlan', json.dumps(_with(_VIDEO, state='bogus'))),
        ('VideoPlan', json.dumps(_with(_VIDEO, state='QUEUED'))),
        ('VideoPlan', json.dumps(_with(_VIDEO, state=None))),
        ('VideoPlan', json.dumps(_with(_VIDEO, move_candidates=[_with(_CANDIDATE, candidate_id=_DROP)]))),
        ('VideoPlan', json.dumps(_with(_VIDEO, move_candidates={}))),
        ('MoveResult', json.dumps(_RESULT)),
        ('MoveResult', json.dumps(_with(_RESULT, state='failed', error_code='move_failed'))),
        ('MoveResult', json.dumps(_with(_RESULT, state='Moved'))),
        ('MoveResult', json.dumps(_with(_RESULT, error_code=_DROP, file_name=_DROP))),
        (
            'ApplyResult',
            json.dumps({'plan_id': 'p', 'revision': 'r', 'state': 'partial_failed', 'results': [_RESULT, _RESULT]}),
        ),
        ('ApplyResult', json.dumps({'plan_id': 'p', 'revision': 'r', 'state': 'succeeded', 'results': []})),
        ('ApplyResult', json.dumps({'plan_id': 'p', 'revision': 'r', 'state': 'done', 'results': [{}]})),
    ]
    cases += [
        ('VideoPlan', json.dumps({'video_id': 'X', 'actor_ids': ['a'], 'state': s.value})) for s in models.VideoState
    ]
    cases += [('MoveResult', json.dumps(_with(_RESULT, state=s.value))) for s in models.MoveState]
    cases += [('FillActorPlan', json.dumps(_with(_PLAN, created_at=v, actors=[], videos=[]))) for v in _DATETIMES]
    cases += [('VideoPlan', json.dumps(_with(_VIDEO, release_date=v, move_candidates=[]))) for v in _DATES]
    return cases


def _models() -> list[dict[str, Any]]:
    out = []
    for name, text in _model_inputs():
        case: dict[str, Any] = {'model': name, 'input': text}
        try:
            case['dump'] = _MODELS[name].model_validate_json(text).model_dump_json()
        except ValidationError as exc:
            case['errors'] = [
                {'type': error['type'], 'loc': list(error['loc']), 'msg': error['msg']} for error in exc.errors()
            ]
        out.append(case)
    return out


def _error(exc: Exception) -> dict[str, str]:
    return {'type': type(exc).__name__, 'message': str(exc)}


_FILE = {
    'path': '/cloud/library/source-b/ABC/ABC-001.mp4',
    'id': 'file-id',
    'name': 'ABC-001.mp4',
    'size': 12,
    'write_time': 34,
    'hashes': {'sha1': 'abcd', '2': 'Z', 'md5': '', '中': 'é'},
}


def _cloud_file_inputs() -> list[Any]:
    return [
        _FILE,
        _with(_FILE, hashes=_DROP),
        _with(_FILE, hashes={}),
        _with(_FILE, path='/cloud/library/source-b/ABC/not-the-name.mp4'),
        _with(_FILE, id=''),
        _with(_FILE, id=None),
        _with(_FILE, size=-1),
        _with(_FILE, size=0, write_time=0),
        _with(_FILE, size=12.0),
        _with(_FILE, size=True),
        _with(_FILE, size='12'),
        _with(_FILE, size=10**20, write_time=10**25),
        _with(_FILE, write_time=-1),
        _with(_FILE, write_time=False),
        _with(_FILE, write_time=_DROP),
        _with(_FILE, name=_DROP),
        _with(_FILE, path=_DROP),
        _with(_FILE, hashes=[]),
        _with(_FILE, hashes={'sha1': 1}),
        _with(_FILE, hashes={'sha1': None}),
        _with(_FILE, path='cloud/ABC-001.mp4', name='ABC-001.mp4'),
        _with(_FILE, path='/cloud//ABC-001.mp4'),
        _with(_FILE, path='//cloud/ABC-001.mp4'),
        _with(_FILE, path='/cloud/./ABC-001.mp4'),
        _with(_FILE, path='/cloud/../ABC-001.mp4'),
        _with(_FILE, path='/cloud/ABC-001.mp4/', name='ABC-001.mp4'),
        _with(_FILE, path='/cloud\\x/ABC-001.mp4'),
        _with(_FILE, path='/cloud/\x00/ABC-001.mp4'),
        _with(_FILE, path='s3://cloud/ABC-001.mp4'),
        _with(_FILE, path='', name=''),
        _with(_FILE, path='/', name=''),
        _with(_FILE, path='/ABC-001.mp4'),
        {'path': '/a/b.mp4', 'id': 'i', 'name': 'b.mp4', 'size': 1, 'write_time': 2, 'extra': 'ignored'},
    ]


def _metadata(value: CloudFileMetadata) -> dict[str, Any]:
    return {
        'path': value.path,
        'file_id': value.file_id,
        'name': value.name,
        'size': value.size,
        'write_time': value.write_time,
        'hashes': [list(pair) for pair in value.hashes],
        'stored': _cloud_file_to_json(value),
    }


def _cloud_files() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for value in _cloud_file_inputs():
        case: dict[str, Any] = {'kind': 'metadata', 'input': value}
        try:
            case['result'] = _metadata(CloudFileMetadata.from_mapping(value))
        except (ValueError, TypeError) as exc:
            case['error'] = _error(exc)
        out.append(case)
    base = CloudFileMetadata.from_mapping(_FILE)
    others = {
        'same': _FILE,
        'other_path': _with(_FILE, path='/elsewhere/ABC-001.mp4'),
        'other_id': _with(_FILE, id='other'),
        'other_size': _with(_FILE, size=13),
        'other_write_time': _with(_FILE, write_time=35),
        'other_hashes': _with(_FILE, hashes={'sha1': 'abcd'}),
        'no_hashes': _with(_FILE, hashes=_DROP),
    }
    for label, value in others.items():
        out.append(
            {
                'kind': 'identity',
                'label': label,
                'left': _FILE,
                'right': value,
                'matches': base.matches_identity(CloudFileMetadata.from_mapping(value)),
            }
        )
    responses: list[Any] = [
        {'success': True, 'error_message': '', 'result_paths': ['/cloud/library/destination/ABC/ABC-001.mp4']},
        {'success': False, 'error_message': '任务已存在', 'result_paths': []},
        {'success': False, 'error_message': None},
        {'success': True},
        {'success': 1},
        {'success': None},
        {},
        {'success': True, 'error_message': 3},
        {'success': True, 'result_paths': '/x'},
        {'success': True, 'result_paths': ['/x', 2]},
        {'success': True, 'result_paths': None},
        {'success': True, 'result_paths': {}},
    ]
    for value in responses:
        case = {'kind': 'response', 'input': value}
        try:
            response = CloudMoveResponse.from_mapping(value)
            case['result'] = {
                'success': response.success,
                'error_message': response.error_message,
                'result_paths': list(response.result_paths),
            }
        except ValueError as exc:
            case['error'] = _error(exc)
        out.append(case)
    return out


_ROOTS = ('/cloud/library/source-a', '/cloud/library/source-b', '/cloud/library/source-c', '/cloud/library/source-d')


def _cloud_paths() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    configurations: list[dict[str, Any]] = [
        {'strm_mount_prefix': '/mounted-cloud', 'source_api_roots': list(_ROOTS), 'move_in_api_root': '/cloud/dest'},
        {'strm_mount_prefix': '/mnt/cd2', 'source_api_roots': ['/115/embyx'], 'move_in_api_root': '/115/move-in'},
        {'strm_mount_prefix': '/mnt/cd2/', 'source_api_roots': ['/115/embyx/'], 'move_in_api_root': '/115/in/'},
        {'strm_mount_prefix': '/', 'source_api_roots': ['/'], 'move_in_api_root': '/'},
        {'strm_mount_prefix': '/mnt', 'source_api_roots': [], 'move_in_api_root': '/in'},
        {'strm_mount_prefix': 'mnt', 'source_api_roots': ['/a'], 'move_in_api_root': '/in'},
        {'strm_mount_prefix': '/mnt', 'source_api_roots': ['/a/../b'], 'move_in_api_root': '/in'},
        {'strm_mount_prefix': '/mnt', 'source_api_roots': ['/a'], 'move_in_api_root': '/in//x'},
        {'strm_mount_prefix': '/mnt', 'source_api_roots': ['/a'], 'move_in_api_root': ''},
        {'strm_mount_prefix': '//mnt', 'source_api_roots': ['/a'], 'move_in_api_root': '/in'},
    ]
    for config in configurations:
        case: dict[str, Any] = {'kind': 'from_values', 'input': config}
        try:
            paths = CloudMovePaths.from_values(
                strm_mount_prefix=config['strm_mount_prefix'],
                source_api_roots=tuple(config['source_api_roots']),
                move_in_api_root=config['move_in_api_root'],
            )
            case['result'] = {
                'strm_mount_prefix': str(paths.strm_mount_prefix),
                'source_api_roots': [str(root) for root in paths.source_api_roots],
                'move_in_api_root': str(paths.move_in_api_root),
            }
        except ValueError as exc:
            case['error'] = _error(exc)
        out.append(case)

    paths = CloudMovePaths.from_values(
        strm_mount_prefix='/mounted-cloud', source_api_roots=_ROOTS, move_in_api_root='/cloud/library/destination'
    )
    good = '/mounted-cloud/cloud/library/source-b/ABC/ABC-001.mp4'
    mappings: list[tuple[str, bytes, int]] = [
        ('plain', good.encode(), 1),
        ('newline', (good + '\n').encode(), 1),
        ('crlf', (good + '\r\n').encode(), 1),
        ('two_newlines', (good + '\n\n').encode(), 1),
        ('line_separator', (good + ' ').encode(), 1),  # noqa: RUF001 - on purpose
        ('upper_suffix', good.replace('.mp4', '.MKV').encode(), 1),
        ('every_suffix_ts', good.replace('.mp4', '.ts').encode(), 1),
        ('rmvb', good.replace('.mp4', '.rmvb').encode(), 1),
        ('no_suffix', good.removesuffix('.mp4').encode(), 1),
        ('dot_file', b'/mounted-cloud/cloud/library/source-b/ABC/.mp4', 1),
        ('unicode', '/mounted-cloud/cloud/library/source-b/中文/中文-001.mp4'.encode(), 1),
        ('kelvin_suffix', good.replace('.mp4', '.MKV').encode(), 1),  # noqa: RUF001 - on purpose
        ('empty', b'', 1),
        ('relative', b'relative/video.mp4', 1),
        ('url', b'https://example.invalid/video.mp4', 1),
        ('dotdot', b'/mounted-cloud/cloud/library/source-b/../escape/ABC-001.mp4', 1),
        ('dot', b'/mounted-cloud/cloud/library/source-b/./ABC-001.mp4', 1),
        ('double_slash', b'/mounted-cloud/cloud/library/source-b//ABC-001.mp4', 1),
        ('trailing_slash', b'/mounted-cloud/cloud/library/source-b/ABC-001.mp4/', 1),
        ('other_root', b'/mounted-cloud/cloud/library/source-a/ABC-001.mp4', 1),
        ('root_prefix_only', b'/mounted-cloud/cloud/library/source-bb/ABC-001.mp4', 1),
        ('outside_mount', b'/elsewhere/cloud/library/source-b/ABC-001.mp4', 1),
        ('txt', b'/mounted-cloud/cloud/library/source-b/ABC-001.txt', 1),
        ('two_lines', (good + '\n/another.mp4').encode(), 1),
        ('leading_space', (' ' + good).encode(), 1),
        ('trailing_space', (good + ' ').encode(), 1),
        ('trailing_nbsp', (good + ' ').encode(), 1),  # noqa: RUF001 - on purpose
        ('nul', (good + '\x00').encode(), 1),
        ('backslash', b'/mounted-cloud/cloud/library/source-b/A\\B/ABC-001.mp4', 1),
        ('oversized', b'/' + b'a' * 4096, 1),
        ('max_size', ('/mounted-cloud/cloud/library/source-b/' + 'a' * (4096 - 43) + '.mp4').encode(), 1),
        ('not_utf8', b'\xff', 1),
        ('bom', b'\xef\xbb\xbf' + good.encode(), 1),
        ('index_0', good.encode(), 0),
        ('index_4', good.encode(), 4),
        ('index_negative', b'/mounted-cloud/cloud/library/source-d/ABC-001.mp4', -1),
        ('missing_file', b'', 1),
    ]
    with tempfile.TemporaryDirectory() as directory:
        for label, payload, index in mappings:
            mapping = Path(directory) / f'{label}.strm'
            if label != 'missing_file':
                mapping.write_bytes(payload)
            case = {'kind': 'parse_mapping', 'label': label, 'hex': payload.hex(), 'source_index': index}
            try:
                parsed = paths.parse_mapping(mapping, source_index=index)
                case['result'] = {
                    'mapping_sha256': parsed.mapping_sha256,
                    'mounted_path': parsed.mounted_path,
                    'api_path': parsed.api_path,
                }
            except ValueError as exc:
                case['error'] = _error(exc)
            out.append(case)
    for brand in ('ABC', '中文', 'A.B', '...', '.', '..', '', 'A/B', 'A\\B', 'A\x00', ' ', 'FC2-PPV'):
        case = {'kind': 'destination', 'brand': brand}
        try:
            case['result'] = paths.destination_directory(brand)
        except ValueError as exc:
            case['error'] = _error(exc)
        out.append(case)
    return out


_NOW = datetime(2026, 7, 13, 10, 0, tzinfo=UTC)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _progress(value: JobProgress | None) -> Any:
    if value is None:
        return None
    return {
        'stage': value.stage.value,
        'completed': value.completed,
        'total': value.total,
        'unit': value.unit.value,
        'current': value.current,
        'stage_started_at': _iso(value.stage_started_at),
        'updated_at': _iso(value.updated_at),
    }


def _attempt(label: str, build: Callable[[], Any], describe: Callable[[Any], Any] | None = None) -> dict[str, Any]:
    case: dict[str, Any] = {'label': label}
    try:
        built = build()
        case['result'] = describe(built) if describe is not None else 'ok'
    except (ValueError, TypeError) as exc:
        case['error'] = _error(exc)
    return case


def _candidate(**changes: Any) -> CandidateRecord:
    values: dict[str, Any] = {
        'candidate_id': 'c-1',
        'video_id': 'ABC-001',
        'source': Path('/mapping/ABC-001.strm'),
        'source_root': Path('/mapping'),
        'destination': Path('/move-in/ABC/ABC-001.strm'),
        'fingerprint': FileFingerprint(device=1, inode=2, size=3, mtime_ns=4, ctime_ns=5),
        'kind': CandidateKind.CLOUD_STRM,
        'mapping_sha256': 'a' * 64,
        'cloud_source_path': '/cloud/ABC/ABC-001.mp4',
        'cloud_destination_dir': '/cloud/dest/ABC',
        'cloud_file': CloudFileMetadata(
            path='/cloud/ABC/ABC-001.mp4', file_id='f', name='ABC-001.mp4', size=1, write_time=2
        ),
    }
    values.update(changes)
    return CandidateRecord(**values)


def _local(**changes: Any) -> CandidateRecord:
    values: dict[str, Any] = {
        'kind': CandidateKind.LOCAL_FILE,
        'mapping_sha256': None,
        'cloud_source_path': None,
        'cloud_destination_dir': None,
        'cloud_file': None,
    }
    values.update(changes)
    return _candidate(**values)


def _plan(candidates: list[tuple[str, str]], records: list[tuple[str, str]]) -> PlanRecord:
    public = models.FillActorPlan(
        plan_id='p',
        revision='r',
        created_at=_NOW,
        expires_at=_NOW,
        actors=(),
        videos=tuple(
            models.VideoPlan(
                video_id=video_id,
                actor_ids=(),
                state=models.VideoState.ADDITIONAL_FOUND,
                move_candidates=(
                    models.MoveCandidate(
                        candidate_id=candidate_id,
                        video_id=video_id,
                        file_name='f.mp4',
                        source_label='s',
                        destination_conflict=False,
                    ),
                ),
            )
            for candidate_id, video_id in candidates
        ),
    )
    return PlanRecord(
        public=public,
        candidates=tuple(_local(candidate_id=candidate_id, video_id=video_id) for candidate_id, video_id in records),
    )


def _job(**changes: Any) -> JobRecord:
    values: dict[str, Any] = {
        'job_id': 'j',
        'operation': JobOperation.APPLY,
        'state': JobState.QUEUED,
        'created_at': _NOW,
        'updated_at': _NOW + timedelta(seconds=1),
    }
    values.update(changes)
    return JobRecord(**values)


def _apply_result(plan_id: str = 'p', revision: str = 'r') -> models.ApplyResult:
    return models.ApplyResult(plan_id=plan_id, revision=revision, state=models.ApplyState.SUCCEEDED, results=())


def _operation(state: CloudMoveOperationState, error_code: str | None, **changes: Any) -> CloudMoveOperationRecord:
    values: dict[str, Any] = {
        'plan_id': 'p',
        'candidate_id': 'c',
        'attempt_id': 'a',
        'source_path': '/s',
        'destination_dir': '/d',
        'state': state,
        'updated_at': _NOW,
        'error_code': error_code,
    }
    values.update(changes)
    return CloudMoveOperationRecord(**values)


def _constructors() -> list[dict[str, Any]]:
    cases = [
        _attempt('candidate_cloud', _candidate),
        _attempt('candidate_local', _local),
        _attempt('candidate_local_with_digest', lambda: _local(mapping_sha256='a' * 64)),
        _attempt('candidate_local_with_file', lambda: _local(cloud_file=_candidate().cloud_file)),
        _attempt('candidate_cloud_without_file', lambda: _candidate(cloud_file=None)),
        _attempt('candidate_cloud_without_digest', lambda: _candidate(mapping_sha256=None)),
        _attempt('candidate_cloud_without_source', lambda: _candidate(cloud_source_path=None)),
        _attempt('candidate_cloud_without_destination', lambda: _candidate(cloud_destination_dir=None)),
        _attempt('candidate_cloud_path_mismatch', lambda: _candidate(cloud_source_path='/cloud/other.mp4')),
        _attempt('candidate_cloud_short_digest', lambda: _candidate(mapping_sha256='a' * 63)),
        _attempt('candidate_cloud_long_digest', lambda: _candidate(mapping_sha256='a' * 65)),
        _attempt('candidate_cloud_non_hex_digest', lambda: _candidate(mapping_sha256='z' * 64)),
        _attempt('plan_matching', lambda: _plan([('c-1', 'V-1'), ('c-2', 'V-2')], [('c-2', 'V-2'), ('c-1', 'V-1')])),
        _attempt('plan_empty', lambda: _plan([], [])),
        _attempt('plan_missing_record', lambda: _plan([('c-1', 'V-1')], [])),
        _attempt('plan_extra_record', lambda: _plan([], [('c-1', 'V-1')])),
        _attempt('plan_video_mismatch', lambda: _plan([('c-1', 'V-1')], [('c-1', 'V-2')])),
        _attempt('plan_duplicate_records', lambda: _plan([('c-1', 'V-1')], [('c-1', 'V-1'), ('c-1', 'V-1')])),
        _attempt('plan_duplicate_public', lambda: _plan([('c-1', 'V-1'), ('c-1', 'V-1')], [('c-1', 'V-1')])),
    ]
    for completed, total in ((0, None), (3, None), (0, 0), (2, 3), (3, 3), (4, 3), (-1, None), (0, -1), (-1, -2)):
        for kind, build in (
            (
                'progress',
                lambda c=completed, t=total: JobProgress(
                    stage=JobStage.LIBRARY_SCAN,
                    completed=c,
                    total=t,
                    unit=JobProgressUnit.VIDEOS,
                    current=None,
                    stage_started_at=_NOW,
                    updated_at=_NOW,
                ),
            ),
            (
                'progress_event',
                lambda c=completed, t=total: JobProgressEvent(
                    stage=JobStage.LIBRARY_SCAN, completed=c, total=t, unit=JobProgressUnit.VIDEOS
                ),
            ),
        ):
            cases.append(_attempt(f'{kind}_{completed}_{total}', build))
    for label, actor_ids in (('job_no_actors', ()), ('job_actors', ('a', 'b', 'a'))):
        cases.append(_attempt(label, lambda ids=actor_ids: _job(actor_ids=ids), lambda job: _progress(job.progress)))
    explicit = JobProgress(
        stage=JobStage.DONE,
        completed=1,
        total=1,
        unit=JobProgressUnit.STEPS,
        current='x',
        stage_started_at=_NOW + timedelta(hours=1),
        updated_at=_NOW + timedelta(hours=2),
    )
    cases.append(
        _attempt('job_explicit_progress', lambda: _job(progress=explicit), lambda job: _progress(job.progress))
    )
    completed_job = _job(state=JobState.COMPLETED, plan_id='p')
    cases += [
        _attempt('apply_job', lambda: ApplyJobRecord(job=_job(), revision='r', candidate_ids=('c-1', 'c-2'))),
        _attempt('apply_job_empty_candidates', lambda: ApplyJobRecord(job=_job(), revision='r', candidate_ids=())),
        _attempt(
            'apply_job_create_plan',
            lambda: ApplyJobRecord(job=_job(operation=JobOperation.CREATE_PLAN), revision='r', candidate_ids=()),
        ),
        _attempt('apply_job_empty_revision', lambda: ApplyJobRecord(job=_job(), revision='', candidate_ids=())),
        _attempt(
            'apply_job_duplicate_candidates', lambda: ApplyJobRecord(job=_job(), revision='r', candidate_ids=('c', 'c'))
        ),
        _attempt(
            'apply_job_result',
            lambda: ApplyJobRecord(job=completed_job, revision='r', candidate_ids=(), result=_apply_result()),
        ),
        _attempt(
            'apply_job_result_without_plan',
            lambda: ApplyJobRecord(
                job=_job(state=JobState.COMPLETED), revision='r', candidate_ids=(), result=_apply_result('other')
            ),
        ),
        _attempt(
            'apply_job_result_not_completed',
            lambda: ApplyJobRecord(
                job=_job(state=JobState.FAILED), revision='r', candidate_ids=(), result=_apply_result()
            ),
        ),
        _attempt(
            'apply_job_result_revision',
            lambda: ApplyJobRecord(
                job=completed_job, revision='r', candidate_ids=(), result=_apply_result(revision='x')
            ),
        ),
        _attempt(
            'apply_job_result_plan',
            lambda: ApplyJobRecord(job=completed_job, revision='r', candidate_ids=(), result=_apply_result('other')),
        ),
    ]
    for state in CloudMoveOperationState:
        for error_code in (None, 'cloud_move_failed', ''):
            cases.append(
                _attempt(f'operation_{state.value}_{error_code}', lambda s=state, e=error_code: _operation(s, e))
            )
    for field in ('attempt_id', 'source_path', 'destination_dir'):
        cases.append(
            _attempt(
                f'operation_empty_{field}',
                lambda f=field: _operation(CloudMoveOperationState.FAILED, None, **{f: ''}),
            )
        )
    return cases


def _transitions() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for table, members, validate in (
        ('journal', list(MoveJournalState), persistence.validate_journal_transition),
        ('cloud_move', list(CloudMoveOperationState), persistence.validate_cloud_move_transition),
    ):
        for current in [None, *members]:
            for requested in members:
                case: dict[str, Any] = {
                    'table': table,
                    'current': current.value if current is not None else None,
                    'requested': requested.value,
                }
                try:
                    validate(current, requested)  # type: ignore[arg-type]
                    case['allowed'] = True
                except ValueError as exc:
                    case['allowed'] = False
                    case['error'] = _error(exc)
                out.append(case)
    return out


def _values() -> list[dict[str, Any]]:
    enums: dict[str, Any] = {
        'VideoState': models.VideoState,
        'MoveState': models.MoveState,
        'ApplyState': models.ApplyState,
        'JobState': JobState,
        'CancelJobOutcome': persistence.CancelJobOutcome,
        'EnqueueApplyJobOutcome': persistence.EnqueueApplyJobOutcome,
        'JobOperation': JobOperation,
        'JobStage': JobStage,
        'JobProgressUnit': JobProgressUnit,
        'MoveJournalState': MoveJournalState,
        'CandidateKind': CandidateKind,
        'CloudMoveOperationState': CloudMoveOperationState,
        'AcquisitionOutcome': AcquisitionOutcome,
    }
    out: list[dict[str, Any]] = [
        {'enum': name, 'values': [member.value for member in enum]} for name, enum in enums.items()
    ]
    out.append(
        {
            'enum': 'CloudMoveOperationState.terminal',
            'values': [state.value for state in CloudMoveOperationState if state.terminal],
        }
    )
    codes = {
        name: cls.code
        for name, cls in vars(fill_actor_errors).items()
        if isinstance(cls, type) and issubclass(cls, fill_actor_errors.FillActorError)
    }
    out.append({'enum': 'error_codes', 'values': [[name, code] for name, code in codes.items()]})
    out.append({'enum': 'JOB_CANCELLED_ERROR_CODE', 'values': [persistence.JOB_CANCELLED_ERROR_CODE]})
    return out


def _normalize() -> list[dict[str, Any]]:
    values = [
        datetime(2026, 7, 13, 10, 0, 0, 123456, tzinfo=UTC),
        datetime(2026, 7, 13, 10, 0, 0, 123456),  # noqa: DTZ001 - naive on purpose
        datetime(2026, 7, 13, 19, 0, tzinfo=timezone(timedelta(hours=9))),
        datetime(2026, 7, 13, 4, 30, tzinfo=timezone(timedelta(hours=-5, minutes=-30))),
    ]
    return [
        {'input': value.isoformat(), 'normalized': persistence.normalize_datetime(value).isoformat()}
        for value in values
    ]


def _records() -> list[dict[str, Any]]:
    return [
        {'kind': 'constructors', 'cases': _constructors()},
        {'kind': 'transitions', 'cases': _transitions()},
        {'kind': 'values', 'cases': _values()},
        {'kind': 'normalize_datetime', 'cases': _normalize()},
    ]


GOLDENS = {
    'fill_actor/models.json': _models,
    'fill_actor/cloud_files.json': _cloud_files,
    'fill_actor/cloud_paths.json': _cloud_paths,
    'fill_actor/records.json': _records,
}
