import uvicorn

from embyx_manager.bootstrap import build_app
from embyx_manager.settings import Settings


def main() -> None:
    settings = Settings.from_env()
    app = build_app(settings)
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        proxy_headers=False,
        server_header=False,
    )


if __name__ == '__main__':
    main()
