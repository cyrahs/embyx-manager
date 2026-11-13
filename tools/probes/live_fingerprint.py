"""The one live run of the HTTP client candidates against AVBase and JavBus (WP0).

Max approved it on 2026-11-13 19:23Z, once, from a one-off pod on nas-us-2 (plan, "Questions
for Max", item 3); it never runs anywhere else, the cloud sessions included (their egress
proxy re-terminates TLS, so the result would mean nothing). `live_probe_pod.sh` is the pod's
script; `docs/rewrite/API-COVERAGE.md` has the manifest and the result.

First every client makes one request to a local listener (`tls_fingerprint.py`), so the
result names the fingerprint each client had. Then, anonymous GETs only, one run, no retry:

- AVBase, as `clients/avbase.py` asks: the home page (build id), one talent data route, one
  work data route, by the control (curl_cffi `impersonate='chrome'`, the baseline's 0.16.3)
  and the two wreq candidates; the httpx-shaped Rust client fetches the home page only, to
  learn whether AVBase needs impersonation at all. 10 requests at most.
- JavBus, as `clients/javbus.py` asks: one star search and one video page, by the baseline's
  own client (httpx), the control and the three Rust candidates. 10 requests at most.

Requests are 3 s apart. A client whose AVBase home page shows no build id (a challenge, a
block) skips its two data routes. What is recorded per request: status, a few response
headers (`server`, `cf-mitigated`, `content-type`, `location`, `cf-cache-status`), the body's
size and the verdict (`page`, `challenge`, `blocked`, `age-gate`, `other`, `error`), never
the body.

    uv run --with hpack==4.1.0 python tools/probes/live_fingerprint.py --live
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))

import tls_fingerprint

AVBASE = 'https://www.avbase.net'
JAVBUS = 'https://www.javbus.com'
TALENT = '河北彩花'
WORK = 'SSIS-001'
AVBASE_CLIENTS = ('curl_cffi', 'wreq', 'wreq-tuned')
JAVBUS_CLIENTS = ('httpx-py', 'curl_cffi', 'httpx-rs', 'wreq', 'wreq-tuned')
MAX_PER_SITE = 10
SPACING = 3.0
BUILD_ID_RE = re.compile(rb'"buildId":"([^"]+)"')


class Budget:
    """Refuses an 11th request to a site and keeps every request SPACING seconds apart."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.last = 0.0

    def spend(self, url: str) -> None:
        host = urlsplit(url).hostname or ''
        if self.counts.get(host, 0) >= MAX_PER_SITE:
            msg = f'request budget for {host} spent'
            raise RuntimeError(msg)
        self.counts[host] = self.counts.get(host, 0) + 1
        wait = self.last + SPACING - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        self.last = time.monotonic()


def verdict(report: dict[str, Any], body: bytes, marker: Any) -> str:
    if 'error' in report:
        return 'error'
    headers = report.get('headers', {})
    status = report.get('status')
    lowered = body[:200_000].lower()
    if headers.get('cf-mitigated') == 'challenge' or b'challenge-platform' in lowered or b'just a moment' in lowered:
        return 'challenge'
    if 'driver-verify' in headers.get('location', '') or 'driver-verify' in report.get('url', ''):
        return 'age-gate'
    if status == 403:
        return 'blocked'
    if status == 200 and marker(body, headers):
        return 'page'
    return 'other'


def _json_props(body: bytes, headers: dict[str, str]) -> bool:
    if 'json' not in headers.get('content-type', ''):
        return False
    try:
        return 'pageProps' in json.loads(body)
    except ValueError:
        return False


def request(  # noqa: PLR0913, PLR0917 - one call site, every part of a probe request
    budget: Budget, site: str, step: str, client: str, url: str, headers: dict[str, str], marker: Any, work: Path
) -> tuple[dict[str, Any], bytes]:
    budget.spend(url)
    body_path = work / 'body'
    body_path.unlink(missing_ok=True)
    started = time.monotonic()
    report = tls_fingerprint.fetch(client, url, headers, body=body_path)
    body = body_path.read_bytes() if body_path.exists() else b''
    body_path.unlink(missing_ok=True)
    report.update(site=site, step=step, seconds=round(time.monotonic() - started, 2))
    report['verdict'] = verdict(report, body, marker)
    print(json.dumps(report, ensure_ascii=False), flush=True)
    return report, body


def run_live(work: Path) -> list[dict[str, Any]]:
    budget = Budget()
    results: list[dict[str, Any]] = []
    avbase_headers = tls_fingerprint.AVBASE_HEADERS
    for client in AVBASE_CLIENTS:
        home, body = request(
            budget, 'avbase', 'home', client, f'{AVBASE}/', avbase_headers, lambda b, _: BUILD_ID_RE.search(b), work
        )
        results.append(home)
        match = BUILD_ID_RE.search(body) if home['verdict'] == 'page' else None
        if match is None:
            continue
        build = match.group(1).decode()
        for step, path, params in (
            ('talent', f'talents/{quote(TALENT, safe="")}.json', {'name': TALENT}),
            ('work', f'works/{quote(WORK, safe="")}.json', {'id': WORK}),
        ):
            url = f'{AVBASE}/_next/data/{build}/{path}?{urlencode(params)}'
            results.append(request(budget, 'avbase', step, client, url, avbase_headers, _json_props, work)[0])
    results.append(
        request(
            budget,
            'avbase',
            'home',
            'httpx-rs',
            f'{AVBASE}/',
            avbase_headers,
            lambda b, _: BUILD_ID_RE.search(b),
            work,
        )[0]
    )
    javbus_headers = tls_fingerprint.JAVBUS_HEADERS
    for client in JAVBUS_CLIENTS:
        for step, url, marker in (
            ('star-search', f'{JAVBUS}/searchstar/{quote(TALENT, safe="")}', lambda b, _: b'/star/' in b),
            ('video', f'{JAVBUS}/{WORK}', lambda b, _: WORK.encode() in b and b'/star/' in b),
        ):
            results.append(request(budget, 'javbus', step, client, url, javbus_headers, marker, work)[0])
    print(json.dumps({'requests_per_site': budget.counts}), flush=True)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n', 1)[0])
    parser.add_argument('--live', action='store_true', help='send the live requests (only in the probe pod)')
    args = parser.parse_args()
    print('== local fingerprints', flush=True)
    captures = tls_fingerprint.run()
    summary = {name: tls_fingerprint.fingerprint(capture) for name, capture in captures.items()}
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    for left, right in tls_fingerprint.PAIRS:
        differences = [line.split('\n')[0] for line in tls_fingerprint.compare(captures, left, right)]
        print(f'{left} vs {right}: {[line for line in differences if line.startswith("DIFF")]}', flush=True)
    if not args.live:
        print('no --live: no request leaves this machine', flush=True)
        return 0
    print('== live', flush=True)
    with tempfile.TemporaryDirectory() as directory:
        results = run_live(Path(directory))
    print('== result', flush=True)
    print(json.dumps({'fingerprints': summary, 'requests': results}, ensure_ascii=False), flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
