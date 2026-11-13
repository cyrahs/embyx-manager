"""A scripted HTTP fake that records every request: the stand-in for Kubernetes and Emby.

Routes come from the scenario seed: `{method, path, status, body, times}`; the first route
whose method and path match answers (and is used up after `times` answers, if given). An
unmatched request answers 404 with `{"error": "no route"}`. Each call is recorded as
`{'fake': <name>, 'method': <HTTP method>, 'request': {'path', 'query', 'body'}, 'response':
{'status', 'body'}}`; JSON bodies are parsed, other bodies kept as text. Later work packages
replace a route table with a state model where a service needs one (the Kubernetes Jobs of
WP14, Emby's playlists of WP8), keeping this record shape.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Self
from urllib.parse import parse_qs, urlsplit


def _parse_body(raw: bytes) -> Any:
    if not raw:
        return None
    text = raw.decode('utf-8', errors='replace')
    try:
        return json.loads(text)
    except ValueError:
        return text


class FakeHttpServer:
    def __init__(self, name: str, seed: dict[str, Any], record: Any) -> None:
        self.name = name
        self.routes: list[dict[str, Any]] = [dict(route) for route in seed.get('routes', [])]
        lock = threading.Lock()
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def _handle(self) -> None:
                split = urlsplit(self.path)
                length = int(self.headers.get('content-length') or 0)
                body = _parse_body(self.rfile.read(length)) if length else None
                with lock:
                    route = next(
                        (
                            route
                            for route in fake.routes
                            if route['method'] == self.command
                            and route['path'] == split.path
                            and route.get('times', 1) != 0
                        ),
                        None,
                    )
                    if route is None:
                        status, answer = 404, {'error': 'no route'}
                    else:
                        if 'times' in route:
                            route['times'] -= 1
                        status, answer = int(route.get('status', 200)), route.get('body')
                    record(
                        {
                            'fake': fake.name,
                            'method': self.command,
                            'request': {'path': split.path, 'query': parse_qs(split.query), 'body': body},
                            'response': {'status': status, 'body': answer},
                        }
                    )
                payload = b'' if answer is None else json.dumps(answer, ensure_ascii=False).encode()
                self.send_response(status)
                self.send_header('content-type', 'application/json')
                self.send_header('content-length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = _handle  # noqa: N815

            def log_message(self, *args: object) -> None:
                del args

        self._server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.url = f'http://127.0.0.1:{self._server.server_address[1]}'
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def dump(self) -> dict[str, Any]:
        return {'routes_left': [route for route in self.routes if route.get('times', 1) != 0]}

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.shutdown()
        self._server.server_close()
