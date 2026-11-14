"""The fill-actor repository (`fill_actor/postgres_repository.py`), the rows both builds write.

`fill_actor.repository` runs `args.steps` in order on a `PostgresFillActorRepository` over the
migrated database. Each step is `{op, args}`, `op` a repository method's name, and is reported
as `{op, result | error}`. Records are built from the step's arguments as the service builds
them: a plan's `public` and a move or apply result are `model_validate_json` texts, a
CloudDrive snapshot is `CloudFileMetadata.from_mapping`'s mapping, datetimes are ISO 8601
texts (`datetime.fromisoformat`), enums are values. A record is reported field by field
(`dataclasses.fields`), a model as its `model_dump_json()` text, a datetime as `isoformat()`.
An error is `{type, message}` with `type(exc).__name__` and `str(exc)`.
"""

from __future__ import annotations

import dataclasses
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from embyx_manager.db import Database
from embyx_manager.fill_actor.cloud_moves import CloudFileMetadata
from embyx_manager.fill_actor.models import ApplyResult, FillActorPlan, MoveResult
from embyx_manager.fill_actor.persistence import (
    ApplyJobRecord,
    CandidateKind,
    CandidateRecord,
    CloudMoveOperationRecord,
    CloudMoveOperationState,
    FileFingerprint,
    JobOperation,
    JobProgress,
    JobProgressUnit,
    JobRecord,
    JobStage,
    JobState,
    MoveJournalRecord,
    MoveJournalState,
    PlanRecord,
)
from embyx_manager.fill_actor.postgres_repository import PostgresFillActorRepository

if TYPE_CHECKING:
    from runner import Context


def _report(value: Any) -> Any:  # noqa: PLR0911 - one case per kind of value
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, BaseModel):
        return value.model_dump_json()
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: _report(getattr(value, field.name)) for field in dataclasses.fields(value)}
    if isinstance(value, tuple | list):
        return [_report(item) for item in value]
    msg = f'cannot report {type(value).__name__}'
    raise TypeError(msg)


def _time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def _candidate(args: dict[str, Any]) -> CandidateRecord:
    cloud_file = args.get('cloud_file')
    return CandidateRecord(
        candidate_id=args['candidate_id'],
        video_id=args['video_id'],
        source=Path(args['source']),
        source_root=Path(args['source_root']),
        destination=Path(args['destination']),
        fingerprint=FileFingerprint(**args['fingerprint']),
        kind=CandidateKind(args.get('kind', 'local_file')),
        mapping_sha256=args.get('mapping_sha256'),
        cloud_source_path=args.get('cloud_source_path'),
        cloud_destination_dir=args.get('cloud_destination_dir'),
        cloud_file=CloudFileMetadata.from_mapping(cloud_file) if cloud_file is not None else None,
    )


def _plan(args: dict[str, Any]) -> PlanRecord:
    return PlanRecord(
        public=FillActorPlan.model_validate_json(args['public']),
        candidates=tuple(_candidate(candidate) for candidate in args['candidates']),
        config_version=args.get('config_version', 0),
    )


def _progress(args: dict[str, Any] | None) -> JobProgress | None:
    if args is None:
        return None
    return JobProgress(
        stage=JobStage(args['stage']),
        completed=args['completed'],
        total=args['total'],
        unit=JobProgressUnit(args['unit']),
        current=args['current'],
        stage_started_at=datetime.fromisoformat(args['stage_started_at']),
        updated_at=datetime.fromisoformat(args['updated_at']),
    )


def _job(args: dict[str, Any]) -> JobRecord:
    return JobRecord(
        job_id=args['job_id'],
        operation=JobOperation(args['operation']),
        state=JobState(args['state']),
        created_at=datetime.fromisoformat(args['created_at']),
        updated_at=datetime.fromisoformat(args['updated_at']),
        plan_id=args.get('plan_id'),
        error_code=args.get('error_code'),
        owner_id=args.get('owner_id'),
        lease_expires_at=_time(args.get('lease_expires_at')),
        actor_ids=tuple(args.get('actor_ids', ())),
        progress=_progress(args.get('progress')),
    )


def _apply_job(args: dict[str, Any]) -> ApplyJobRecord:
    result = args.get('result')
    return ApplyJobRecord(
        job=_job(args['job']),
        revision=args['revision'],
        candidate_ids=tuple(args['candidate_ids']),
        result=ApplyResult.model_validate_json(result) if result is not None else None,
    )


def _journal(args: dict[str, Any]) -> MoveJournalRecord:
    return MoveJournalRecord(
        plan_id=args['plan_id'],
        candidate_id=args['candidate_id'],
        state=MoveJournalState(args['state']),
        updated_at=datetime.fromisoformat(args['updated_at']),
    )


def _operation(args: dict[str, Any]) -> CloudMoveOperationRecord:
    return CloudMoveOperationRecord(
        plan_id=args['plan_id'],
        candidate_id=args['candidate_id'],
        attempt_id=args['attempt_id'],
        source_path=args['source_path'],
        destination_dir=args['destination_dir'],
        state=CloudMoveOperationState(args['state']),
        updated_at=datetime.fromisoformat(args['updated_at']),
        error_code=args.get('error_code'),
    )


async def _run(repository: PostgresFillActorRepository, op: str, args: dict[str, Any]) -> Any:  # noqa: C901, PLR0911, PLR0912 - one case per step
    match op:
        case 'save_plan':
            return await repository.save_plan(_plan(args['plan']))
        case 'get_plan':
            return _report(await repository.get_plan(args['plan_id']))
        case 'get_candidate':
            return _report(await repository.get_candidate(args['plan_id'], args['candidate_id']))
        case 'delete_plan':
            return await repository.delete_plan(args['plan_id'])
        case 'purge_expired_plans':
            return await repository.purge_expired_plans(datetime.fromisoformat(args['now']))
        case 'save_move_result':
            return await repository.save_move_result(args['plan_id'], MoveResult.model_validate_json(args['result']))
        case 'get_move_result':
            return _report(await repository.get_move_result(args['plan_id'], args['candidate_id']))
        case 'list_move_results':
            return _report(await repository.list_move_results(args['plan_id']))
        case 'save_job':
            return await repository.save_job(_job(args['job']))
        case 'enqueue_job':
            return await repository.enqueue_job(_job(args['job']), max_active=args['max_active'])
        case 'enqueue_apply_job':
            return _report(
                await repository.enqueue_apply_job(_apply_job(args['record']), max_active=args['max_active'])
            )
        case 'get_apply_job':
            return _report(await repository.get_apply_job(args['job_id']))
        case 'claim_next_job':
            return _report(
                await repository.claim_next_job(
                    owner_id=args['owner_id'],
                    now=datetime.fromisoformat(args['now']),
                    lease_expires_at=datetime.fromisoformat(args['lease_expires_at']),
                )
            )
        case 'renew_owned_job_lease':
            return await repository.renew_owned_job_lease(
                job_id=args['job_id'],
                owner_id=args['owner_id'],
                now=datetime.fromisoformat(args['now']),
                lease_expires_at=datetime.fromisoformat(args['lease_expires_at']),
            )
        case 'update_owned_job_progress':
            return await repository.update_owned_job_progress(
                job_id=args['job_id'],
                owner_id=args['owner_id'],
                progress=_progress(args['progress']),
                now=datetime.fromisoformat(args['now']),
            )
        case 'finish_owned_job':
            result = args.get('apply_result')
            return await repository.finish_owned_job(
                job_id=args['job_id'],
                owner_id=args['owner_id'],
                state=JobState(args['state']),
                error_code=args.get('error_code'),
                now=datetime.fromisoformat(args['now']),
                progress=_progress(args['progress']),
                apply_result=ApplyResult.model_validate_json(result) if result is not None else None,
            )
        case 'cancel_job':
            return _report(await repository.cancel_job(job_id=args['job_id'], now=datetime.fromisoformat(args['now'])))
        case 'fail_expired_jobs':
            return await repository.fail_expired_jobs(
                now=datetime.fromisoformat(args['now']), error_code=args['error_code']
            )
        case 'get_job':
            return _report(await repository.get_job(args['job_id']))
        case 'list_jobs':
            states = args.get('states')
            return _report(
                await repository.list_jobs(tuple(JobState(state) for state in states) if states is not None else None)
            )
        case 'save_move_journal':
            return await repository.save_move_journal(_journal(args['journal']))
        case 'get_move_journal':
            return _report(await repository.get_move_journal(args['plan_id'], args['candidate_id']))
        case 'list_unreconciled_moves':
            return _report(await repository.list_unreconciled_moves())
        case 'save_cloud_move_operation':
            return await repository.save_cloud_move_operation(_operation(args['operation']))
        case 'get_cloud_move_operation':
            return _report(await repository.get_cloud_move_operation(args['plan_id'], args['candidate_id']))
        case 'list_unresolved_cloud_moves':
            return _report(await repository.list_unresolved_cloud_moves())
        case 'finalize_cloud_move':
            return await repository.finalize_cloud_move(
                _operation(args['operation']), MoveResult.model_validate_json(args['result'])
            )
    msg = f'unknown step {op!r}'
    raise ValueError(msg)


async def repository(ctx: Context) -> Any:
    database = Database(ctx.dsn)
    repository = PostgresFillActorRepository(database)
    reported = []
    try:
        for step in ctx.args['steps']:
            entry: dict[str, Any] = {'op': step['op']}
            try:
                entry['result'] = await _run(repository, step['op'], step.get('args', {}))
            except Exception as exc:  # noqa: BLE001 - each step's failure is part of the transcript
                entry['error'] = {'type': type(exc).__name__, 'message': str(exc)}
            reported.append(entry)
    finally:
        await database.aclose()
    return reported


OPERATIONS = {'fill_actor.repository': repository}
