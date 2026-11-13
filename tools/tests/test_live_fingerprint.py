"""The live probe's request budget (tools/probes/live_fingerprint.py), against a stubbed fetch.

The live run is allowed once, with at most 10 requests per site, 3 s apart, no retry; these
tests hold the script to that without sending anything.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'probes'))

import live_fingerprint
import tls_fingerprint

PAGES = {
    '/': b'<script>{"buildId":"B1"}</script>',
    'data': json.dumps({'pageProps': {}}).encode(),
    'javbus': b'<a href="/star/abc">SSIS-001</a>',
}


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []
        self.request_times: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def stubbed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Clock, list[tuple[str, str]]]:
    clock = Clock()
    sent: list[tuple[str, str]] = []
    monkeypatch.setattr(live_fingerprint.time, 'monotonic', clock.monotonic)
    monkeypatch.setattr(live_fingerprint.time, 'sleep', clock.sleep)

    def fetch(client: str, url: str, headers: dict[str, str], *, body: Path | None = None, **_: Any) -> dict[str, Any]:
        sent.append((client, url))
        clock.request_times.append(clock.now)
        path = urlsplit(url).path
        if 'javbus' in url:
            content, kind = PAGES['javbus'], 'text/html'
        elif path == '/':
            content, kind = PAGES['/'], 'text/html'
        else:
            content, kind = PAGES['data'], 'application/json'
        if body is not None:
            body.write_bytes(content)
        return {'client': client, 'status': 200, 'url': url, 'headers': {'content-type': kind}}

    monkeypatch.setattr(tls_fingerprint, 'fetch', fetch)
    return clock, sent


def test_every_page_answering_spends_exactly_ten_requests_per_site(stubbed, tmp_path: Path, capsys) -> None:
    clock, sent = stubbed
    results = live_fingerprint.run_live(tmp_path)
    hosts = [urlsplit(url).hostname for _, url in sent]
    assert hosts.count('www.avbase.net') == 10
    assert hosts.count('www.javbus.com') == 10
    assert all(result['verdict'] == 'page' for result in results)
    gaps = [later - earlier for earlier, later in zip(clock.request_times, clock.request_times[1:], strict=False)]
    assert min(gaps) >= live_fingerprint.SPACING
    assert not list(tmp_path.iterdir()), 'bodies are deleted after each request'
    assert 'SSIS-001</a>' not in capsys.readouterr().out, 'bodies are never printed'


def test_a_challenged_home_page_skips_that_clients_data_routes(stubbed, monkeypatch, tmp_path: Path) -> None:
    _, sent = stubbed
    monkeypatch.setitem(PAGES, '/', b'<title>Just a moment...</title>')
    results = live_fingerprint.run_live(tmp_path)
    avbase = [result for result in results if result['site'] == 'avbase']
    assert [result['step'] for result in avbase] == ['home'] * 4
    assert {result['verdict'] for result in avbase} == {'challenge'}
    assert sum('www.avbase.net' in url for _, url in sent) == 4


def test_an_eleventh_request_to_a_site_is_refused(stubbed) -> None:
    budget = live_fingerprint.Budget()
    for _ in range(live_fingerprint.MAX_PER_SITE):
        budget.spend('https://www.avbase.net/')
    with pytest.raises(RuntimeError, match='budget'):
        budget.spend('https://www.avbase.net/x')
    budget.spend('https://www.javbus.com/')
