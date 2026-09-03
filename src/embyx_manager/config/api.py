"""Configuration endpoints: read/update sections and probe external services.

Test endpoints build throwaway clients from the submitted form values, filling
omitted secrets from the stored configuration, so the operator can verify a
change before saving it. Responses never echo secrets.
"""

import asyncio
import logging
from typing import Any

import grpc
from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field

from embyx_manager.clients.clouddrive import AsyncCloudDrive, CloudDriveClient
from embyx_manager.config.models import CloudDriveConfig
from embyx_manager.config.store import (
    ConfigStore,
    ConfigVersionConflictError,
    UnknownConfigSectionError,
    masked_values,
    secret_flags,
)
from embyx_manager.errors import ApiError

LOGGER = logging.getLogger(__name__)

TEST_TIMEOUT_SECONDS = 15.0


class SectionView(BaseModel):
    section: str
    values: dict[str, Any]
    secrets: dict[str, bool]
    version: int


class UpdateSectionRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    values: dict[str, Any]
    version: int | None = Field(default=None, ge=0)


class TestConnectionRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    values: dict[str, Any] = Field(default_factory=dict)


class TestConnectionResult(BaseModel):
    ok: bool
    detail: str = ''


def _section_view(store: ConfigStore, section: str) -> SectionView:
    try:
        value, version = store.get_with_version(section)
    except UnknownConfigSectionError as exc:
        raise ApiError(404, 'unknown_config_section') from exc
    return SectionView(
        section=section,
        values=masked_values(value),
        secrets=secret_flags(value),
        version=version,
    )


def create_config_router(store: ConfigStore, *, mutation_auth: Any) -> APIRouter:
    router = APIRouter(prefix='/api/config')

    @router.get('')
    async def list_sections() -> list[SectionView]:
        return [_section_view(store, section) for section in store.sections()]

    @router.get('/{section}')
    async def get_section(section: str) -> SectionView:
        return _section_view(store, section)

    @router.put('/{section}', dependencies=[Depends(mutation_auth)])
    async def update_section(section: str, request: UpdateSectionRequest) -> SectionView:
        try:
            await store.update(section, request.values, expected_version=request.version)
        except UnknownConfigSectionError as exc:
            raise ApiError(404, 'unknown_config_section') from exc
        except ConfigVersionConflictError as exc:
            raise ApiError(409, 'config_version_conflict') from exc
        except ValueError as exc:
            LOGGER.info('rejected config update for %s: %s', section, exc)
            raise ApiError(422, 'invalid_config_values') from exc
        return _section_view(store, section)

    @router.post('/clouddrive/test', dependencies=[Depends(mutation_auth)])
    async def test_clouddrive(request: TestConnectionRequest) -> TestConnectionResult:
        config = _merge_for_test(store, CloudDriveConfig, request.values)
        if not config.configured:
            return TestConnectionResult(ok=False, detail='address and api_token are required')
        return await _run_clouddrive_test(config)

    return router


def _merge_for_test[SectionT](store: ConfigStore, section_type: type[SectionT], values: dict[str, Any]) -> SectionT:
    stored = store.get(section_type)  # type: ignore[arg-type]
    merged = dict(stored.model_dump())
    for key, value in values.items():
        if key not in merged:
            raise ApiError(422, 'invalid_config_values')
        if key in section_type.SECRET_FIELDS and (value is None or value == ''):  # type: ignore[attr-defined]
            continue
        merged[key] = value
    try:
        return section_type.model_validate(merged)  # type: ignore[attr-defined]
    except ValueError as exc:
        raise ApiError(422, 'invalid_config_values') from exc


async def _run_clouddrive_test(config: CloudDriveConfig) -> TestConnectionResult:
    def build_client() -> CloudDriveClient:
        return CloudDriveClient(
            address=config.address,
            api_token=config.api_token,
            secure=config.secure,
        )

    cloud = AsyncCloudDrive(build_client())
    try:
        async with asyncio.timeout(TEST_TIMEOUT_SECONDS):
            await cloud.check()
    except TimeoutError:
        return TestConnectionResult(ok=False, detail='connection timed out')
    except grpc.RpcError as exc:
        detail = exc.details() if hasattr(exc, 'details') else ''
        code = exc.code().name if hasattr(exc, 'code') else 'RPC_ERROR'
        return TestConnectionResult(ok=False, detail=f'{code}: {detail or "gRPC call failed"}')
    except ValueError as exc:
        return TestConnectionResult(ok=False, detail=str(exc))
    except OSError as exc:
        return TestConnectionResult(ok=False, detail=f'connection failed: {exc}')
    else:
        return TestConnectionResult(ok=True, detail='connected')
    finally:
        await cloud.aclose()
