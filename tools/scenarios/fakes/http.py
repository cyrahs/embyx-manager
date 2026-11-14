"""A scripted HTTP fake that records every request: the stand-in for Kubernetes and Emby.

Routes come from the scenario seed: `{method, path, status, body, times}`; the first route
whose method and path match answers (and is used up after `times` answers, if given). An
unmatched request answers 404 with `{"error": "no route"}`. Each call is recorded as
`{'fake': <name>, 'method': <HTTP method>, 'request': {'path', 'query', 'body'}, 'response':
{'status', 'body'}}`; JSON bodies are parsed, other bodies kept as text.

A seed with a `jobs` table (WP14) adds a model of the Kubernetes Jobs API behind the routes,
for the requests no route answers: `POST /apis/batch/v1/namespaces/<ns>/jobs` keeps the body
under its `metadata.name` with an empty `status` (201, or 409 `{"reason": "AlreadyExists"}`
for a name in use), `GET .../jobs/<name>` answers the kept Job (404 `{"reason": "NotFound"}`
when there is none) and `DELETE .../jobs/<name>` forgets it (200 `{"status": "Success"}`, or
404). `set_job_status` changes a kept Job's `status` without a call, the way the cluster
finishes a Job; the dump then has `jobs` beside `routes_left`.
"""

from __future__ import annotations

import copy
import json
import re
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


_JOBS = re.compile(r'/apis/batch/v1/namespaces/[^/]+/jobs(?:/([^/]+))?')


class FakeHttpServer:
    def __init__(self, name: str, seed: dict[str, Any], record: Any) -> None:
        self.name = name
        self.routes: list[dict[str, Any]] = [dict(route) for route in seed.get('routes', [])]
        #: The Jobs model, by name; None without a `jobs` seed.
        self.jobs: dict[str, Any] | None = copy.deepcopy(seed['jobs']) if 'jobs' in seed else None
        lock = threading.Lock()
        self._lock = lock
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
                        status, answer = fake._model(self.command, split.path, body)
                    else:
                        if 'times' in route:
                            route['times'] -= 1
                        status, answer = int(route.get('status', 200)), route.get('body')
                    record(
                        {
                            'fake': fake.name,
                            'method': self.command,
                            'request': {'path': split.path, 'query': parse_qs(split.query), 'body': body},
                            'response': {'status': status, 'body': copy.deepcopy(answer)},
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

    def _model(self, method: str, path: str, body: Any) -> tuple[int, Any]:  # noqa: PLR0911 - one answer per request
        """The answer of the Jobs model, or the 404 of a request nothing answers."""
        match = _JOBS.fullmatch(path) if self.jobs is not None else None
        if match is None or self.jobs is None:
            return 404, {'error': 'no route'}
        name = match.group(1)
        if name is None and method == 'POST' and isinstance(body, dict):
            created = str(body.get('metadata', {}).get('name', ''))
            if created in self.jobs:
                return 409, {'reason': 'AlreadyExists'}
            self.jobs[created] = {**body, 'status': {}}
            return 201, self.jobs[created]
        if name is not None and method == 'GET':
            return (200, self.jobs[name]) if name in self.jobs else (404, {'reason': 'NotFound'})
        if name is not None and method == 'DELETE':
            if self.jobs.pop(name, None) is None:
                return 404, {'reason': 'NotFound'}
            return 200, {'status': 'Success'}
        return 404, {'error': 'no route'}

    def set_job_status(self, name: str, status: dict[str, Any]) -> None:
        with self._lock:
            if self.jobs is None or name not in self.jobs:
                msg = f'no Job {name!r} to set the status of'
                raise KeyError(msg)
            self.jobs[name]['status'] = copy.deepcopy(status)

    def dump(self) -> dict[str, Any]:
        dumped: dict[str, Any] = {'routes_left': [route for route in self.routes if route.get('times', 1) != 0]}
        if self.jobs is not None:
            dumped['jobs'] = copy.deepcopy(self.jobs)
        return dumped

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.shutdown()
        self._server.server_close()
