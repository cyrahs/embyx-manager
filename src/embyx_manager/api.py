"""The application root: shell, auth, health and static hosting.

Deliberately knows nothing about any individual feature. Features arrive as
routers, health probes, lifespans and exception handlers, so none of them can
become "the app" the way fill-actor was when this was still embyx-web.
"""

import secrets
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from types import MappingProxyType
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from embyx_manager.errors import ApiError

HTTP_UNAUTHORIZED = 401
HTTP_NOT_FOUND = 404

#: Top-level health fields owned by the app; a feature may not report under these.
RESERVED_HEALTH_KEYS = frozenset({'status', 'database', 'auth_required'})

ExceptionHandler = Callable[[Request, Exception], Awaitable[Response]]
FeatureHealthProbe = Callable[[], Awaitable[Mapping[str, object]]]
Lifespan = Callable[[], AsyncIterator[None]]

_NO_HEALTH: Mapping[str, FeatureHealthProbe] = MappingProxyType({})
_NO_HANDLERS: Mapping[type[Exception], ExceptionHandler] = MappingProxyType({})


def make_mutation_auth(api_token: str | None) -> Callable[..., Awaitable[None]]:
    """Bearer-token dependency shared by every mutation endpoint."""
    bearer = HTTPBearer(auto_error=False)

    async def require_mutation_auth(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> None:
        if api_token is None:
            return
        if (
            credentials is None
            or credentials.scheme.casefold() != 'bearer'
            or not secrets.compare_digest(credentials.credentials, api_token)
        ):
            raise ApiError(401, 'unauthorized')

    return require_mutation_auth


class RequestSizeLimitMiddleware:
    def __init__(self, app: ASGIApp, *, max_bytes: int) -> None:
        self._app = app
        self._max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] != 'http' or scope.get('method') not in {'POST', 'PUT', 'PATCH'}:
            await self._app(scope, receive, send)
            return

        messages: list[Message] = []
        received = 0
        while True:
            message = await receive()
            messages.append(message)
            if message['type'] == 'http.request':
                received += len(message.get('body', b''))
                if received > self._max_bytes:
                    response = JSONResponse({'error': {'code': 'request_too_large'}}, status_code=413)
                    await response(scope, receive, send)
                    return
                if not message.get('more_body', False):
                    break
            elif message['type'] == 'http.disconnect':
                break

        iterator = iter(messages)

        async def replay() -> Message:
            try:
                return next(iterator)
            except StopIteration:
                return {'type': 'http.request', 'body': b'', 'more_body': False}

        await self._app(scope, replay, send)


def create_app(  # noqa: C901, PLR0913 - assembly root: every part is an explicit keyword
    *,
    app_ready: Callable[[], Awaitable[bool]],
    routers: Sequence[APIRouter] = (),
    feature_health: Mapping[str, FeatureHealthProbe] = _NO_HEALTH,
    exception_handlers: Mapping[type[Exception], ExceptionHandler] = _NO_HANDLERS,
    lifespans: Sequence[Lifespan] = (),
    api_token: str | None = None,
    max_request_bytes: int = 65_536,
    frontend_dist: Path | None = None,
) -> FastAPI:
    """Assembles the app from feature-supplied parts.

    `app_ready` is the readiness every feature depends on — the database. Each
    entry in `feature_health` reports one feature's own dependencies under its own
    key, and never affects `status` or the HTTP code.
    """
    reserved = RESERVED_HEALTH_KEYS & feature_health.keys()
    if reserved:
        msg = f'feature health keys are reserved by the app: {sorted(reserved)}'
        raise ValueError(msg)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        # An exit stack so a feature that fails to start still unwinds the ones
        # already running, instead of leaving their clients and tasks behind.
        async with AsyncExitStack() as stack:
            for feature_lifespan in lifespans:
                await stack.enter_async_context(feature_lifespan())
            yield

    app = FastAPI(title='embyx-manager', version='0.1.0', lifespan=lifespan)
    app.add_middleware(RequestSizeLimitMiddleware, max_bytes=max_request_bytes)

    @app.middleware('http')
    async def add_api_cache_control(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        response = await call_next(request)
        if request.url.path.startswith('/api/'):
            response.headers['Cache-Control'] = 'no-store'
        return response

    require_mutation_auth = make_mutation_auth(api_token)

    @app.exception_handler(ApiError)
    async def handle_api_error(_request: Request, exc: ApiError) -> JSONResponse:
        headers = {'WWW-Authenticate': 'Bearer'} if exc.status_code == HTTP_UNAUTHORIZED else None
        return JSONResponse({'error': {'code': exc.code}}, status_code=exc.status_code, headers=headers)

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(_request: Request, _exc: RequestValidationError) -> JSONResponse:
        return JSONResponse({'error': {'code': 'invalid_request'}}, status_code=422)

    @app.get('/api/auth/session', dependencies=[Depends(require_mutation_auth)])
    async def auth_session() -> dict[str, bool]:
        """Login probe: 200 means the presented token — or no token at all — is accepted."""
        return {'auth_required': api_token is not None}

    @app.get('/api/health')
    async def health() -> JSONResponse:
        """App-level readiness only.

        `status` and the HTTP code cover what every feature needs — the database —
        so one degraded feature never takes the deployment down with it. A feature's
        own dependencies are reported under its own key for its own page to show.
        """
        database_ready = await app_ready()
        payload: dict[str, object] = {
            'status': 'ok' if database_ready else 'not_ready',
            'database': database_ready,
            # Public so the browser can show its login screen before any authorized call.
            'auth_required': api_token is not None,
        }
        for name, probe in feature_health.items():
            payload[name] = dict(await probe())
        return JSONResponse(payload, status_code=200 if database_ready else 503)

    for exc_type, handler in exception_handlers.items():
        app.add_exception_handler(exc_type, handler)

    for router in routers:
        app.include_router(router)

    if frontend_dist is not None and frontend_dist.is_dir():
        app.mount('/', SpaStaticFiles(directory=frontend_dist, html=True), name='frontend')
    return app


class SpaStaticFiles(StaticFiles):
    """Static files with history-API fallback: unknown page paths serve index.html."""

    async def get_response(self, path: str, scope: Scope) -> Response:
        is_page_path = not path.startswith('api/') and '.' not in path.rsplit('/', 1)[-1]
        try:
            response = await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code == HTTP_NOT_FOUND and is_page_path:
                return await super().get_response('index.html', scope)
            raise
        if response.status_code == HTTP_NOT_FOUND and is_page_path:
            return await super().get_response('index.html', scope)
        return response
