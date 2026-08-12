from fastapi import FastAPI

from embyx_manager.settings import Settings


def build_app(settings: Settings) -> FastAPI:
    """Assemble the production application.

    Wired up once the PostgreSQL repository lands; until then starting the
    server is not supported.
    """
    del settings
    msg = 'embyx-manager bootstrap is not wired yet: the PostgreSQL repository is still in progress'
    raise NotImplementedError(msg)
