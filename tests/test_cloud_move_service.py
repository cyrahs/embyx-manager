from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

import pytest

from embyx_manager.fill_actor.cloud_moves import CloudFileMetadata, CloudMovePaths, CloudMoveResponse
from embyx_manager.fill_actor.models import MoveState, VideoState
from embyx_manager.fill_actor.persistence import (
    CloudMoveOperationState,
    MemoryFillActorRepository,
    MoveJournalRecord,
    MoveJournalState,
)
from embyx_manager.fill_actor.ports import AcquisitionOutcome
from embyx_manager.fill_actor.service import (
    CLOUD_MOVE_OBSERVATION_GRACE,
    FillActorPaths,
    FillActorService,
    static_runtime,
)

if TYPE_CHECKING:
    from collections.abc import Callable

SOURCE_API_PATH = '/cloud/library/source-b/ABC/ABC-001.mp4'
DESTINATION_API_DIR = '/cloud/library/destination/ABC'
SOURCE_API_ROOT = '/cloud/library/source-b'
DESTINATION_API_ROOT = '/cloud/library/destination'


class ActorCatalog:
    async def list_video_ids(self, _actor_id: str) -> tuple[str, ...]:
        return ('ABC-001',)


class AcquisitionGateway:
    async def queue_missing(self, _video_id: str) -> AcquisitionOutcome:
        return AcquisitionOutcome.NO_MAGNET


class BrandResolver:
    def __init__(self, brand: str = 'ABC') -> None:
        self.brand = brand

    def resolve_brand(self, _video_id: str) -> str:
        return self.brand


class FakeCloudMover:
    def __init__(self, source: CloudFileMetadata) -> None:
        self.directories: dict[str, dict[str, CloudFileMetadata]] = {
            SOURCE_API_ROOT: {},
            str(PurePosixPath(source.path).parent): {source.name: source},
            DESTINATION_API_ROOT: {},
            DESTINATION_API_DIR: {},
        }
        self.move_calls: list[tuple[str, str]] = []
        self.behavior = 'success'
        self.replace_source_on_ensure = False
        self.pending_source_removal_observations = 0

    async def list_directory(self, api_directory: str) -> tuple[CloudFileMetadata, ...]:
        if api_directory not in self.directories:
            raise FileNotFoundError(api_directory)
        if api_directory == str(PurePosixPath(SOURCE_API_PATH).parent) and self.pending_source_removal_observations:
            self.pending_source_removal_observations += 1
            if self.pending_source_removal_observations > 2:
                self.directories[api_directory].pop(PurePosixPath(SOURCE_API_PATH).name, None)
        return tuple(self.directories[api_directory].values())

    async def move_file(self, source_api_path: str, destination_api_directory: str) -> CloudMoveResponse:
        self.move_calls.append((source_api_path, destination_api_directory))
        if self.behavior == 'raise_before_move':
            raise TimeoutError
        if self.behavior == 'eventual_double_visibility':
            source_parent = str(PurePosixPath(source_api_path).parent)
            name = PurePosixPath(source_api_path).name
            destination_path = str(PurePosixPath(destination_api_directory) / name)
            self.directories[destination_api_directory][name] = replace(
                self.directories[source_parent][name],
                path=destination_path,
            )
            self.pending_source_removal_observations = 1
        else:
            self.external_move(source_api_path, destination_api_directory)
        if self.behavior == 'raise_after_move':
            raise TimeoutError
        destination_path = str(PurePosixPath(destination_api_directory) / PurePosixPath(source_api_path).name)
        return CloudMoveResponse(success=True, result_paths=(destination_path,))

    async def ensure_directory(self, parent_api_directory: str, folder_name: str) -> bool:
        if self.replace_source_on_ensure:
            source_parent = str(PurePosixPath(SOURCE_API_PATH).parent)
            self.directories[source_parent][PurePosixPath(SOURCE_API_PATH).name] = replace(
                self.directories[source_parent][PurePosixPath(SOURCE_API_PATH).name],
                file_id='replacement-file',
            )
        directory = str(PurePosixPath(parent_api_directory) / folder_name)
        self.directories.setdefault(directory, {})
        return True

    def external_move(self, source_api_path: str, destination_api_directory: str) -> None:
        source_parent = str(PurePosixPath(source_api_path).parent)
        name = PurePosixPath(source_api_path).name
        source = self.directories[source_parent].pop(name)
        destination_path = str(PurePosixPath(destination_api_directory) / name)
        self.directories[destination_api_directory][name] = replace(source, path=destination_path)


@pytest.fixture
def cloud_file() -> CloudFileMetadata:
    return CloudFileMetadata(
        path=SOURCE_API_PATH,
        file_id='cloud-file-id',
        name='ABC-001.mp4',
        size=123,
        write_time=456,
        hashes=(('sha1', 'abcd'),),
    )


def make_service(
    tmp_path: Path,
    cloud_file: CloudFileMetadata,
    *,
    repository: MemoryFillActorRepository | None = None,
    apply_enabled: bool = True,
    brand: str = 'ABC',
    clock: Callable[[], datetime] | None = None,
) -> tuple[FillActorService, MemoryFillActorRepository, FakeCloudMover, Path]:
    actor = tmp_path / 'actor'
    additional = tmp_path / 'additional'
    mapping_dir = additional / 'ABC'
    for path in (actor, additional, mapping_dir):
        path.mkdir(exist_ok=True)
    mapping = mapping_dir / 'ABC-001.strm'
    mapping.write_text('/mounted-cloud/cloud/library/source-b/ABC/ABC-001.mp4\n', encoding='utf-8')
    paths = FillActorPaths.from_iterable(
        actor_brand_path=actor,
        additional_brand_paths=(additional,),
        move_in_path=actor,
    )
    repo = repository or MemoryFillActorRepository()
    mover = FakeCloudMover(cloud_file)
    service = FillActorService(
        runtime=static_runtime(
            paths=paths,
            move_in_by_brand=True,
            apply_enabled=apply_enabled,
            cloud_move_paths=CloudMovePaths.from_values(
                strm_mount_prefix='/mounted-cloud',
                source_api_roots=('/cloud/library/source-b',),
                move_in_api_root='/cloud/library/destination',
            ),
        ),
        actor_catalog=ActorCatalog(),
        acquisition_gateway=AcquisitionGateway(),
        brand_resolver=BrandResolver(brand),
        repository=repo,
        cloud_file_mover=mover,
        clock=clock,
    )
    return service, repo, mover, mapping


async def create_candidate(service: FillActorService):
    plan = await service.create_plan(['actor'])
    assert plan.videos[0].state is VideoState.ADDITIONAL_FOUND
    candidate = plan.videos[0].move_candidates[0]
    return plan, candidate


@pytest.mark.asyncio
@pytest.mark.parametrize('brand', ['nested/ABC', 'bad\\name', 'bad\x00name'])
async def test_cloud_scan_rejects_unsafe_brand_segments(
    tmp_path: Path,
    cloud_file: CloudFileMetadata,
    brand: str,
) -> None:
    service, _repository, mover, _mapping = make_service(tmp_path, cloud_file, brand=brand)

    plan = await service.create_plan(['actor'])

    assert plan.videos[0].state is VideoState.INVALID_VIDEO_ID
    assert mover.move_calls == []


@pytest.mark.asyncio
async def test_cloud_readiness_checks_configured_api_roots(
    tmp_path: Path,
    cloud_file: CloudFileMetadata,
) -> None:
    service, _repository, mover, _mapping = make_service(tmp_path, cloud_file)

    assert await service.scan_ready() is True
    assert await service.apply_ready() is True

    unavailable, _repository, unavailable_mover, _mapping = make_service(tmp_path, cloud_file)
    unavailable_mover.directories.pop(DESTINATION_API_ROOT)

    assert await unavailable.scan_ready() is False
    assert await unavailable.apply_ready() is False
    assert mover.move_calls == []


@pytest.mark.asyncio
async def test_legacy_local_journal_blocks_only_cloud_apply_readiness(
    tmp_path: Path,
    cloud_file: CloudFileMetadata,
) -> None:
    service, repository, _mover, _mapping = make_service(tmp_path, cloud_file)
    plan, candidate = await create_candidate(service)
    await repository.save_move_journal(
        MoveJournalRecord(
            plan_id=plan.plan_id,
            candidate_id=candidate.candidate_id,
            state=MoveJournalState.PREPARED,
            updated_at=datetime.now(UTC),
        )
    )

    assert await service.scan_ready() is True
    assert await service.legacy_journal_ready() is False
    assert await service.apply_ready() is False


@pytest.mark.asyncio
async def test_cloud_apply_moves_real_file_and_never_mutates_strm(
    tmp_path: Path,
    cloud_file: CloudFileMetadata,
) -> None:
    service, repository, mover, mapping = make_service(tmp_path, cloud_file)
    plan, candidate = await create_candidate(service)
    original_mapping = mapping.read_bytes()

    applied = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert applied.results[0].state is MoveState.MOVED
    assert mover.move_calls == [(SOURCE_API_PATH, DESTINATION_API_DIR)]
    assert '.strm' not in mover.move_calls[0][0]
    assert mapping.read_bytes() == original_mapping
    source_listing = await mover.list_directory(str(PurePosixPath(SOURCE_API_PATH).parent))
    assert SOURCE_API_PATH not in {file.path for file in source_listing}
    operation = await repository.get_cloud_move_operation(plan.plan_id, candidate.candidate_id)
    assert operation is not None
    assert operation.state is CloudMoveOperationState.SUCCEEDED

    converging = await service.create_plan(['actor'])
    assert converging.videos[0].state is VideoState.EXISTS
    assert converging.videos[0].warnings == ('mapping_convergence_pending',)


@pytest.mark.asyncio
async def test_cloud_apply_detects_destination_conflict_without_calling_move(
    tmp_path: Path,
    cloud_file: CloudFileMetadata,
) -> None:
    service, _repository, mover, _mapping = make_service(tmp_path, cloud_file)
    mover.directories[DESTINATION_API_DIR][cloud_file.name] = replace(
        cloud_file,
        path=f'{DESTINATION_API_DIR}/{cloud_file.name}',
        file_id='foreign-file',
    )
    plan, candidate = await create_candidate(service)
    assert candidate.destination_conflict is True

    applied = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert applied.results[0].state is MoveState.CONFLICT
    assert mover.move_calls == []


@pytest.mark.asyncio
async def test_cloud_apply_creates_only_the_missing_brand_directory(
    tmp_path: Path,
    cloud_file: CloudFileMetadata,
) -> None:
    service, _repository, mover, _mapping = make_service(tmp_path, cloud_file)
    mover.directories.pop(DESTINATION_API_DIR)
    plan, candidate = await create_candidate(service)

    applied = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert applied.results[0].state is MoveState.MOVED
    assert DESTINATION_API_DIR in mover.directories
    assert mover.move_calls == [(SOURCE_API_PATH, DESTINATION_API_DIR)]


@pytest.mark.asyncio
async def test_cloud_apply_rechecks_source_identity_after_creating_destination(
    tmp_path: Path,
    cloud_file: CloudFileMetadata,
) -> None:
    service, _repository, mover, _mapping = make_service(tmp_path, cloud_file)
    mover.directories.pop(DESTINATION_API_DIR)
    mover.replace_source_on_ensure = True
    plan, candidate = await create_candidate(service)

    applied = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert applied.results[0].state is MoveState.STALE
    assert mover.move_calls == []


@pytest.mark.asyncio
async def test_cloud_apply_rejects_mapping_changed_after_plan(
    tmp_path: Path,
    cloud_file: CloudFileMetadata,
) -> None:
    service, _repository, mover, mapping = make_service(tmp_path, cloud_file)
    plan, candidate = await create_candidate(service)
    mapping.write_text('/mounted-cloud/cloud/library/source-b/ABC/ABC-999.mp4\n', encoding='utf-8')

    applied = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert applied.results[0].state is MoveState.STALE
    assert mover.move_calls == []


@pytest.mark.asyncio
async def test_cloud_timeout_after_server_move_is_verified_as_success(
    tmp_path: Path,
    cloud_file: CloudFileMetadata,
) -> None:
    service, _repository, mover, _mapping = make_service(tmp_path, cloud_file)
    mover.behavior = 'raise_after_move'
    plan, candidate = await create_candidate(service)

    applied = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert applied.results[0].state is MoveState.MOVED
    assert len(mover.move_calls) == 1


@pytest.mark.asyncio
async def test_cloud_move_waits_when_same_identity_is_temporarily_visible_at_both_paths(
    tmp_path: Path,
    cloud_file: CloudFileMetadata,
) -> None:
    service, _repository, mover, _mapping = make_service(tmp_path, cloud_file)
    mover.behavior = 'eventual_double_visibility'
    plan, candidate = await create_candidate(service)

    applied = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert applied.results[0].state is MoveState.MOVED
    assert len(mover.move_calls) == 1


@pytest.mark.asyncio
async def test_unknown_cloud_move_is_observation_only_until_remote_state_converges(
    tmp_path: Path,
    cloud_file: CloudFileMetadata,
) -> None:
    service, repository, mover, _mapping = make_service(tmp_path, cloud_file)
    mover.behavior = 'raise_before_move'
    plan, candidate = await create_candidate(service)

    first = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )
    second = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert first.results[0].error_code == 'cloud_move_status_unknown'
    assert second.results[0].error_code == 'cloud_move_status_unknown'
    assert len(mover.move_calls) == 1
    assert await repository.get_move_result(plan.plan_id, candidate.candidate_id) is None
    operation = await repository.get_cloud_move_operation(plan.plan_id, candidate.candidate_id)
    assert operation is not None
    assert operation.state is CloudMoveOperationState.UNKNOWN

    mover.external_move(SOURCE_API_PATH, DESTINATION_API_DIR)
    reconciled = await service.reconcile_moves()

    assert reconciled[0].state is MoveState.MOVED
    assert len(mover.move_calls) == 1


class SteppingClock:
    def __init__(self) -> None:
        self.value = datetime(2026, 8, 18, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.value


@pytest.mark.asyncio
async def test_never_observed_move_is_concluded_after_grace_and_retryable(
    tmp_path: Path,
    cloud_file: CloudFileMetadata,
) -> None:
    clock = SteppingClock()
    service, repository, mover, _mapping = make_service(tmp_path, cloud_file, clock=clock)
    mover.behavior = 'raise_before_move'
    plan, candidate = await create_candidate(service)

    applied = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )
    assert applied.results[0].error_code == 'cloud_move_status_unknown'

    # Within the grace period recovery still only observes.
    observed = await service.reconcile_moves()
    assert observed[0].error_code == 'cloud_move_status_unknown'

    clock.value += CLOUD_MOVE_OBSERVATION_GRACE
    concluded = await service.reconcile_moves()

    assert concluded[0].state is MoveState.FAILED
    assert concluded[0].error_code == 'cloud_move_not_observed'
    operation = await repository.get_cloud_move_operation(plan.plan_id, candidate.candidate_id)
    assert operation is not None
    assert operation.state.terminal

    # The source is free again: a fresh scan may move it successfully.
    mover.behavior = 'success'
    new_plan, new_candidate = await create_candidate(service)
    retried = await service.apply(
        plan_id=new_plan.plan_id,
        revision=new_plan.revision,
        candidate_ids=[new_candidate.candidate_id],
    )

    assert retried.results[0].state is MoveState.MOVED
    assert len(mover.move_calls) == 2


@pytest.mark.asyncio
async def test_disabled_apply_does_not_reconcile_unknown_cloud_operation(
    tmp_path: Path,
    cloud_file: CloudFileMetadata,
) -> None:
    service, repository, mover, _mapping = make_service(tmp_path, cloud_file)
    mover.behavior = 'raise_before_move'
    plan, candidate = await create_candidate(service)
    await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )
    disabled, _repo, disabled_mover, _mapping = make_service(
        tmp_path,
        cloud_file,
        repository=repository,
        apply_enabled=False,
    )

    assert await disabled.reconcile_moves() == ()
    assert disabled_mover.move_calls == []
