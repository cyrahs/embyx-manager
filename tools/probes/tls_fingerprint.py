"""Compare what a server sees from the Python clients and the Rust candidates (WP0 probe).

AVBase is fetched with curl_cffi impersonating Chrome (`clients/avbase.py`,
`impersonate='chrome'`), JavBus with httpx (`clients/javbus.py`). The Rust candidates run
through `crates/embyx-clients/examples/fetch_probe.rs`: wreq with its Chrome emulation as it
comes (`wreq`), the same with curl_cffi's header list and HEADERS priority (`wreq-tuned`),
and the httpx-shaped reqwest client (`httpx-rs`). This tool starts a TLS listener on
127.0.0.1 with a throwaway self-signed certificate, lets each client make one request to
it, and records what a server sees: the ClientHello (cipher suites, extensions, groups,
signature algorithms, ALPN, versions) and the HTTP/2 connection preface (SETTINGS,
WINDOW_UPDATE, the HEADERS frame's priority and header order) or the HTTP/1.1 head.
Nothing is sent anywhere else. Chrome shuffles its extension order per connection, so
extensions are compared as sets; everything else is compared in order.

    uv run --with hpack==4.1.0 python tools/probes/tls_fingerprint.py [--json OUT]

Needs `openssl` (certificate), `cargo` (the example) and the project's environment
(curl_cffi, httpx). The result is summarized in docs/rewrite/API-COVERAGE.md ("wreq").
WP2 added `avbase-rs` and `javbus-rs`: the sessions `AvbaseClient` and `JavBusClient` build,
compared with curl_cffi and httpx.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any

from curl_cffi import requests as curl_requests

REPO_ROOT = Path(__file__).resolve().parents[2]
PREFACE = b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n'
GREASE = {0x0A0A + 0x1010 * i for i in range(16)}

EXTENSION_NAMES = {
    0: 'server_name',
    5: 'status_request',
    10: 'supported_groups',
    11: 'ec_point_formats',
    13: 'signature_algorithms',
    16: 'alpn',
    18: 'signed_certificate_timestamp',
    21: 'padding',
    23: 'extended_master_secret',
    27: 'compress_certificate',
    35: 'session_ticket',
    41: 'pre_shared_key',
    43: 'supported_versions',
    45: 'psk_key_exchange_modes',
    51: 'key_share',
    17513: 'application_settings_old',
    17613: 'application_settings',
    65037: 'encrypted_client_hello',
    65281: 'renegotiation_info',
}
SETTING_NAMES = {
    1: 'HEADER_TABLE_SIZE',
    2: 'ENABLE_PUSH',
    3: 'MAX_CONCURRENT_STREAMS',
    4: 'INITIAL_WINDOW_SIZE',
    5: 'MAX_FRAME_SIZE',
    6: 'MAX_HEADER_LIST_SIZE',
    8: 'ENABLE_CONNECT_PROTOCOL',
    9: 'NO_RFC7540_PRIORITIES',
}


def _u16_list(data: bytes) -> list[int]:
    return [struct.unpack('>H', data[i : i + 2])[0] for i in range(0, len(data), 2)]


def _no_grease(values: list[int]) -> list[int]:
    return [value for value in values if value not in GREASE]


def parse_client_hello(record: bytes) -> dict[str, Any]:
    """The fields of a TLS ClientHello that fingerprinting looks at (RFC 8446 4.1.2)."""
    body = record[5:]
    if body[0] != 1:
        msg = 'not a ClientHello'
        raise ValueError(msg)
    hello = body[4:]
    legacy_version = struct.unpack('>H', hello[0:2])[0]
    at = 2 + 32
    session_length = hello[at]
    at += 1 + session_length
    cipher_length = struct.unpack('>H', hello[at : at + 2])[0]
    ciphers = _u16_list(hello[at + 2 : at + 2 + cipher_length])
    at += 2 + cipher_length
    at += 1 + hello[at]  # compression methods
    extensions_length = struct.unpack('>H', hello[at : at + 2])[0]
    at += 2
    end = at + extensions_length
    extensions: list[int] = []
    details: dict[str, Any] = {}
    while at < end:
        kind, length = struct.unpack('>HH', hello[at : at + 4])
        data = hello[at + 4 : at + 4 + length]
        at += 4 + length
        extensions.append(kind)
        if kind == 10:
            details['supported_groups'] = _no_grease(_u16_list(data[2:]))
        elif kind == 13:
            details['signature_algorithms'] = _u16_list(data[2:])
        elif kind == 16:
            protocols, i = [], 2
            while i < len(data):
                size = data[i]
                protocols.append(data[i + 1 : i + 1 + size].decode())
                i += 1 + size
            details['alpn'] = protocols
        elif kind == 43:
            details['supported_versions'] = _no_grease(_u16_list(data[1:]))
        elif kind == 51:
            groups, i = [], 2
            while i < len(data):
                group, size = struct.unpack('>HH', data[i : i + 4])
                groups.append(group)
                i += 4 + size
            details['key_share_groups'] = _no_grease(groups)
        elif kind == 27:
            details['compress_certificate'] = _u16_list(data[1:])
        elif kind in (17513, 17613):
            details['application_settings'] = data[2:].decode(errors='replace')
    named = [EXTENSION_NAMES.get(kind, str(kind)) for kind in _no_grease(extensions)]
    return {
        'legacy_version': hex(legacy_version),
        'ciphers': [hex(cipher) for cipher in _no_grease(ciphers)],
        'extensions_sorted': sorted(named),
        'grease': any(kind in GREASE for kind in extensions + ciphers),
        **details,
    }


def _headers_arrived(data: bytes) -> bool:
    """Whether `data` holds the first HEADERS frame yet; a frame cut short is not an error."""
    import hpack  # noqa: PLC0415 - only the captures need it (`uv run --with hpack`)

    try:
        return 'header_order' in parse_http2(data)
    except (ValueError, IndexError, struct.error, hpack.HPACKError):
        return False


def parse_http2(data: bytes) -> dict[str, Any]:
    """SETTINGS, WINDOW_UPDATE and the first HEADERS frame after the client preface."""
    if not data.startswith(PREFACE):
        msg = 'no HTTP/2 preface'
        raise ValueError(msg)
    at = len(PREFACE)
    result: dict[str, Any] = {'frames': []}
    import hpack  # noqa: PLC0415 - only the captures need it (`uv run --with hpack`)

    decoder = hpack.Decoder()
    while at + 9 <= len(data):
        length = int.from_bytes(data[at : at + 3], 'big')
        kind, flags = data[at + 3], data[at + 4]
        stream = int.from_bytes(data[at + 5 : at + 9], 'big') & 0x7FFFFFFF
        payload = data[at + 9 : at + 9 + length]
        at += 9 + length
        name = {0x1: 'HEADERS', 0x2: 'PRIORITY', 0x4: 'SETTINGS', 0x8: 'WINDOW_UPDATE'}.get(kind, hex(kind))
        if kind == 0x4 and flags & 0x1:
            result['frames'].append('SETTINGS_ACK')
            continue
        result['frames'].append(f'{name}(stream={stream})')
        if kind == 0x4:
            result['settings'] = [
                [SETTING_NAMES.get(ident, str(ident)), value]
                for ident, value in (struct.unpack('>HI', payload[i : i + 6]) for i in range(0, len(payload), 6))
            ]
        elif kind == 0x8:
            result.setdefault('window_updates', []).append([stream, struct.unpack('>I', payload)[0] & 0x7FFFFFFF])
        elif kind == 0x1:
            block = payload
            if flags & 0x8:  # PADDED
                block = block[1 : len(block) - block[0]]
            if flags & 0x20:  # PRIORITY
                dependency, weight = struct.unpack('>IB', block[:5])
                result['headers_priority'] = {
                    'exclusive': bool(dependency >> 31),
                    'depends_on': dependency & 0x7FFFFFFF,
                    'weight': weight + 1,
                }
                block = block[5:]
            headers = decoder.decode(block)
            result['pseudo_header_order'] = [name for name, _ in headers if name.startswith(':')]
            result['header_order'] = [name for name, _ in headers if not name.startswith(':')]
            result['headers'] = {name: value for name, value in headers if not name.startswith(':')}
            break
    return result


class Listener:
    """One TLS connection at a time on 127.0.0.1; keeps the raw ClientHello and preamble."""

    def __init__(self, directory: Path) -> None:
        certificate, key = directory / 'cert.pem', directory / 'key.pem'
        subprocess.run(
            [
                'openssl',
                'req',
                '-x509',
                '-newkey',
                'ec',
                '-pkeyopt',
                'ec_paramgen_curve:P-256',
                '-nodes',
                '-keyout',
                str(key),
                '-out',
                str(certificate),
                '-days',
                '1',
                '-subj',
                '/CN=localhost',
                # webpki (rustls) wants the name in a SAN and refuses a CA certificate as a leaf.
                '-addext',
                'subjectAltName=DNS:localhost',
                '-addext',
                'basicConstraints=critical,CA:FALSE',
            ],
            check=True,
            capture_output=True,
        )
        self.der = directory / 'cert.der'
        self.der.write_bytes(ssl.PEM_cert_to_DER_cert(certificate.read_text()))
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.context.load_cert_chain(certificate, key)
        self.context.set_alpn_protocols(['h2', 'http/1.1'])
        self.socket = socket.create_server(('127.0.0.1', 0))
        self.socket.settimeout(60)
        self.port = self.socket.getsockname()[1]

    def capture(self) -> dict[str, Any]:
        connection, _ = self.socket.accept()
        with connection:
            connection.settimeout(10)
            header = b''
            while len(header) < 5:
                header = connection.recv(5, socket.MSG_PEEK)
            length = struct.unpack('>H', header[3:5])[0]
            record = b''
            while len(record) < 5 + length:
                record = connection.recv(5 + length, socket.MSG_PEEK)
            result: dict[str, Any] = {'tls': parse_client_hello(record)}
            try:
                tls = self.context.wrap_socket(connection, server_side=True)
            except ssl.SSLError as error:
                result['handshake_error'] = str(error)
                return result
            with tls:
                result['negotiated_alpn'] = tls.selected_alpn_protocol()
                data = b''
                if result['negotiated_alpn'] == 'h2':
                    # The server's (empty) SETTINGS: some clients send no request before it.
                    tls.sendall(b'\x00\x00\x00\x04\x00\x00\x00\x00\x00')
                with contextlib.suppress(OSError, TimeoutError):
                    while True:
                        chunk = tls.recv(65536)
                        if not chunk:
                            break
                        data += chunk
                        if result['negotiated_alpn'] != 'h2':
                            if b'\r\n\r\n' in data:
                                break
                            continue
                        if _headers_arrived(data):
                            break
                if result['negotiated_alpn'] == 'h2':
                    result['http2'] = parse_http2(data)
                else:
                    result['http1'] = data.decode(errors='replace').split('\r\n\r\n', 1)[0].split('\r\n')
        return result


AVBASE_HEADERS = {'Referer': 'https://www.avbase.net/', 'Accept-Language': 'ja,en;q=0.8'}
JAVBUS_HEADERS = {'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8', 'Cookie': 'existmag=all'}
FETCH_PROBE = REPO_ROOT / 'target' / 'debug' / 'examples' / 'fetch_probe'
# Client name -> the headers its application caller adds (clients/avbase.py, clients/javbus.py).
CLIENTS = {
    'curl_cffi': AVBASE_HEADERS,
    'wreq': AVBASE_HEADERS,
    'wreq-tuned': AVBASE_HEADERS,
    'httpx-py': JAVBUS_HEADERS,
    'httpx-rs': JAVBUS_HEADERS,
    # WP2: the clients as the application configures them (they add their own headers).
    'avbase-rs': AVBASE_HEADERS,
    'javbus-rs': JAVBUS_HEADERS,
}
# Which capture each candidate is compared with.
PAIRS = (
    ('curl_cffi', 'wreq'),
    ('curl_cffi', 'wreq-tuned'),
    ('httpx-py', 'httpx-rs'),
    ('curl_cffi', 'avbase-rs'),
    ('httpx-py', 'javbus-rs'),
)
# Rust client name -> the fetch_probe client it runs.
PROBE_CLIENTS = {'httpx-rs': 'httpx', 'avbase-rs': 'avbase', 'javbus-rs': 'javbus'}


def build_fetch_probe() -> Path:
    subprocess.run(
        ['cargo', 'build', '-q', '--locked', '-p', 'embyx-clients', '--example', 'fetch_probe'],
        cwd=REPO_ROOT,
        check=True,
    )
    return FETCH_PROBE


def fetch(  # noqa: PLR0913 - one keyword per knob of the probe
    client: str,
    url: str,
    headers: dict[str, str],
    *,
    root_der: Path | None = None,
    body: Path | None = None,
    timeout: float = 30,
) -> dict[str, Any]:
    """One GET with `client`; `root_der` set means the URL is the local listener."""
    if client in {'curl_cffi', 'httpx-py'}:
        return _fetch_python(client, url, headers, local=root_der is not None, body=body, timeout=timeout)
    command = [str(FETCH_PROBE), PROBE_CLIENTS.get(client, client), url]
    for name, value in headers.items():
        command += ['-H', f'{name}: {value}']
    if root_der is not None:
        command += ['--root', str(root_der)]
    if body is not None:
        command += ['--body', str(body)]
    done = subprocess.run(command, capture_output=True, text=True, check=False, timeout=timeout + 30)
    if done.returncode != 0:
        return {'client': client, 'error': done.stderr.strip()[-500:]}
    return {**json.loads(done.stdout), 'client': client}


KEPT_HEADERS = ('server', 'cf-mitigated', 'content-type', 'location', 'cf-cache-status')


def _fetch_python(  # noqa: PLR0913
    client: str, url: str, headers: dict[str, str], *, local: bool, body: Path | None, timeout: float
) -> dict[str, Any]:
    import httpx  # noqa: PLC0415 - only the probes need it

    report: dict[str, Any] = {'client': client}
    try:
        if client == 'curl_cffi':
            # As clients/avbase.py builds its session: impersonate='chrome', session headers. Redirects
            # are not followed, so that one call is one request (the live run counts them).
            with curl_requests.Session(impersonate='chrome', timeout=timeout, headers=headers) as session:
                response = session.get(url, verify=not local, allow_redirects=False)
                got, content = response.headers, response.content
        else:
            # As clients/javbus.py builds its client: httpx defaults, no redirects followed.
            with httpx.Client(headers=headers, timeout=timeout, verify=not local) as session:
                response = session.get(url)
                got, content = response.headers, response.content
    except Exception as error:  # noqa: BLE001 - the probe records any failure
        report['error'] = f'{type(error).__name__}: {error}'
        return report
    report.update(
        status=response.status_code,
        url=str(response.url),
        headers={name: got[name] for name in KEPT_HEADERS if got.get(name) is not None},
        body_bytes=len(content),
    )
    if body is not None:
        body.write_bytes(content)
    return report


def fingerprint(capture: dict[str, Any]) -> dict[str, Any]:
    """Short hashes of what a server fingerprints, to record next to a live result."""

    def digest(value: Any) -> str:
        return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()[:12]

    http2 = capture.get('http2', {})
    tls = dict(capture.get('tls', {}))
    http1_names = [line.split(':', 1)[0].lower() for line in capture.get('http1', [])[1:]]
    return {
        'tls': digest(tls),
        'ciphers': len(tls.get('ciphers', [])),
        'extensions': len(tls.get('extensions_sorted', [])),
        'alpn': capture.get('negotiated_alpn'),
        'http2': digest({key: http2.get(key) for key in ('settings', 'window_updates', 'headers_priority')})
        if http2
        else None,
        'header_order': http2.get('header_order') or http1_names,
    }


def run(clients: dict[str, dict[str, str]] = CLIENTS) -> dict[str, Any]:
    build_fetch_probe()  # first, so the listener's accept does not wait on the compiler
    results: dict[str, Any] = {}
    with tempfile.TemporaryDirectory() as directory:
        listener = Listener(Path(directory))
        url = f'https://localhost:{listener.port}/'
        for name, headers in clients.items():
            captured: dict[str, Any] = {}
            server = threading.Thread(target=lambda captured=captured: captured.update(listener.capture()), daemon=True)
            server.start()
            fetch(name, url, headers, root_der=listener.der, timeout=10)
            server.join(timeout=30)
            results[name] = captured
    results['curl_cffi']['version'] = __import__('curl_cffi').__version__
    results['curl_cffi']['impersonates'] = __import__('curl_cffi.requests.impersonate', fromlist=['x']).DEFAULT_CHROME
    results['wreq']['emulation'] = 'wreq_util::Emulation::Chrome149'
    results['wreq-tuned']['emulation'] = 'Chrome149 with curl_cffi headers and HEADERS weight 256'
    if 'avbase-rs' in results:
        results['avbase-rs']['emulation'] = 'AvbaseClient session (avbase::session::chrome_emulation)'
    return results


def compare(results: dict[str, Any], left_name: str, right_name: str) -> list[str]:
    left, right = results[left_name], results[right_name]
    lines = []
    for section in ('tls', 'http2', 'http1'):
        a_section, b_section = left.get(section, {}), right.get(section, {})
        if isinstance(a_section, list) or isinstance(b_section, list):
            a_section, b_section = {'lines': a_section}, {'lines': b_section}
        for key in sorted(set(a_section) | set(b_section)):
            if key in {'frames', 'headers'}:
                continue
            a, b = a_section.get(key), b_section.get(key)
            lines.append(
                f'{"same" if a == b else "DIFF"}  {section}.{key}'
                + ('' if a == b else f'\n      {left_name}: {a}\n      {right_name}: {b}')
            )
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n', 1)[0])
    parser.add_argument('--json', type=Path, help='write every capture here')
    args = parser.parse_args()
    results = run()
    if args.json:
        args.json.write_text(json.dumps(results, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(f'curl_cffi {results["curl_cffi"]["version"]} impersonate=chrome ({results["curl_cffi"]["impersonates"]})')
    for left, right in PAIRS:
        print(f'== {left} vs {right}')
        for line in compare(results, left, right):
            print(line)
    return 0


if __name__ == '__main__':
    sys.exit(main())
