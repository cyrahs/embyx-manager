"""The home transcode worker's endpoints, served on a listener of their own.

``EMBYX_MANAGER_TRANSCODE_PORT`` opens a second port that serves only these;
the WireGuard gateway pod forwards the worker's requests there, so the worker
reaches nothing else of the app. ``EMBYX_MANAGER_TRANSCODE_TOKEN``, when set, is
a bearer token it must present as well.
"""

import asyncio
import hashlib
import secrets
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, FastAPI, Header, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field

from embyx_manager.errors import ApiError
from embyx_manager.merge.service import ChunkRefusedError, MergeService
from embyx_manager.merge.transcode import MAX_CHUNK_BYTES, Chunk, WorkerReport

#: Received bytes are written out in pieces of about this size.
WRITE_BYTES = 4 * 1024 * 1024
_bearer = HTTPBearer(auto_error=False)


class PartProgress(BaseModel):
    model_config = ConfigDict(extra='ignore')

    index: int
    fps: float = 0.0
    #: Seconds into the part the encoder has reached.
    position: float = 0.0


class ReportIn(BaseModel):
    model_config = ConfigDict(extra='ignore')

    name: str = Field(default='', max_length=128)
    version: str = Field(default='', max_length=64)
    task_id: int | None = None
    state: str = Field(default='idle', max_length=32)
    parts: list[PartProgress] = Field(default_factory=list, max_length=16)
    upload_bps: float = 0.0
    queue_chunks: int = 0
    queue_bytes: int = 0

    def to_report(self) -> WorkerReport:
        return WorkerReport(
            seen_at=datetime.now(UTC),
            name=self.name,
            version=self.version,
            task_id=self.task_id,
            state=self.state,
            parts=[part.model_dump() for part in self.parts],
            upload_bps=self.upload_bps,
            queue_chunks=self.queue_chunks,
            queue_bytes=self.queue_bytes,
        )


def create_transcode_router(service: MergeService, *, token: str | None = None) -> APIRouter:  # noqa: C901 - one endpoint each
    async def require_token(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    ) -> None:
        if token is None:
            return
        if credentials is None or not secrets.compare_digest(credentials.credentials, token):
            raise ApiError(401, 'unauthorized')

    router = APIRouter(prefix='/api/transcode', dependencies=[Depends(require_token)])

    @router.post('/claim')
    async def claim(report: ReportIn) -> dict[str, Any]:
        return await service.claim(report.to_report())

    @router.post('/heartbeat')
    async def heartbeat(report: ReportIn) -> dict[str, Any]:
        return await service.heartbeat(report.to_report())

    @router.get('/tasks/{task_id}/parts/{part_index}')
    async def read_part(task_id: int, part_index: int) -> FileResponse:
        task = await service.transcoding_task(task_id, part_index)
        # Starlette answers Range requests, which is how ffmpeg reads and resumes.
        return FileResponse(task.parts[part_index], media_type='application/octet-stream')

    @router.put('/tasks/{task_id}/parts/{part_index}/chunks/{chunk_index}')
    async def put_chunk(  # noqa: PLR0913, PLR0917 - FastAPI takes the headers as parameters
        task_id: int,
        part_index: int,
        chunk_index: int,
        request: Request,
        x_chunk_start: Annotated[float, Header()],
        x_chunk_duration: Annotated[float, Header(gt=0)],
        x_chunk_frames: Annotated[int, Header(gt=0)],
        x_chunk_sha256: Annotated[str, Header(min_length=64, max_length=64)],
        content_length: Annotated[int, Header(gt=0, le=MAX_CHUNK_BYTES)],
    ) -> dict[str, Any]:
        task = await service.transcoding_task(task_id, part_index)
        if chunk_index < 0:
            msg = 'transcode_chunk_out_of_order'
            raise ChunkRefusedError(msg)
        staging = service.staging_chunk(task, part_index, chunk_index)
        size, digest = await _receive(request, staging, limit=content_length)
        if size != content_length or digest != x_chunk_sha256.lower():
            await asyncio.to_thread(staging.unlink, missing_ok=True)
            msg = 'transcode_chunk_corrupt'
            raise ChunkRefusedError(msg, 400)
        chunk = Chunk(
            part_index=part_index,
            chunk_index=chunk_index,
            start=x_chunk_start,
            duration=x_chunk_duration,
            frames=x_chunk_frames,
            bytes=size,
            sha256=digest,
        )
        stored = await service.accept_chunk(task, chunk, staging)
        return {'stored': stored}

    @router.post('/tasks/{task_id}/parts/{part_index}/finish')
    async def finish_part(task_id: int, part_index: int) -> dict[str, Any]:
        task = await service.transcoding_task(task_id, part_index)
        await service.finish_part(task, part_index)
        return {'finished': True}

    return router


async def _receive(request: Request, target: Path, *, limit: int) -> tuple[int, str]:
    """Write the request body to ``target`` as it arrives; its size and SHA-256."""
    await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
    digest = hashlib.sha256()
    size = 0
    pending = bytearray()
    handle = await asyncio.to_thread(target.open, 'wb')
    try:
        async for piece in request.stream():
            size += len(piece)
            if size > limit:
                break
            digest.update(piece)
            pending += piece
            if len(pending) >= WRITE_BYTES:
                await asyncio.to_thread(handle.write, bytes(pending))
                pending.clear()
        if pending:
            await asyncio.to_thread(handle.write, bytes(pending))
    finally:
        await asyncio.to_thread(handle.close)
    return size, digest.hexdigest()


def create_transcode_app(router: APIRouter) -> FastAPI:
    """The second listener's app: the worker's endpoints and nothing else."""
    app = FastAPI(title='embyx-manager transcode', docs_url=None, redoc_url=None, openapi_url=None)

    @app.exception_handler(ApiError)
    async def handle_api_error(_request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse({'error': {'code': exc.code}}, status_code=exc.status_code)

    @app.exception_handler(ChunkRefusedError)
    async def handle_refusal(_request: Request, exc: ChunkRefusedError) -> JSONResponse:
        return JSONResponse({'error': {'code': exc.code, **exc.details}}, status_code=exc.status)

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(_request: Request, _exc: RequestValidationError) -> JSONResponse:
        return JSONResponse({'error': {'code': 'invalid_request'}}, status_code=422)

    app.include_router(router)
    return app
