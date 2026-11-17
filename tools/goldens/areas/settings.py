"""Deployment settings read from the environment (`settings.py`), cross-version contract 9.

Each case sets exactly the listed `EMBYX_MANAGER_*` variables (every other one unset) and
records the resulting `Settings` fields or the `ValueError` text.
"""

from __future__ import annotations

import dataclasses
import os
from typing import Any

from embyx_manager.settings import Settings

PREFIX = 'EMBYX_MANAGER_'
TOKEN = 'configured-at-runtime'  # noqa: S105 - a placeholder, not a secret

ENVIRONMENTS: list[dict[str, str]] = [
    {},
    {
        'DATABASE_URL': 'postgresql://db.internal/embyx',
        'MAX_ACTORS': '5',
        'MAX_VIDEOS': '50',
        'MAGNET_CONCURRENCY': '2',
    },
    {'ACTOR_ROOT': '/media/actor', 'APPLY_ENABLED': 'true', 'CLOUD_SOURCE_ROOTS': '/cloud/a'},
    {'MAX_ACTORS': '0'},
    {'MAX_ACTORS': '-3'},
    {'MAX_ACTORS': 'x'},
    {'MAX_ACTORS': ' 7 '},
    {'MAX_ACTORS': '+7'},
    {'MAX_ACTORS': '1_000'},
    {'MAX_ACTORS': '١٢'},
    {'MAX_ACTORS': ''},
    {'MAX_ACTORS': '7.0'},
    {'PORT': '0'},
    {'PORT': '65536'},
    {'PORT': '8080'},
    {'MAX_REQUEST_BYTES': '1'},
    {'MAGNET_CONCURRENCY': '0'},
    {'MAX_VIDEOS': '0'},
    {'HOST': '192.0.2.1'},
    {'HOST': '192.0.2.1', 'API_TOKEN': TOKEN},
    {'HOST': '192.0.2.1', 'API_TOKEN': TOKEN, 'TLS_TERMINATED': 'true'},
    {'HOST': '0.0.0.0', 'API_TOKEN': TOKEN, 'TLS_TERMINATED': 'TRUE'},  # noqa: S104 - a test value
    {'HOST': '0.0.0.0', 'API_TOKEN': '', 'TLS_TERMINATED': 'yes'},  # noqa: S104
    {'HOST': 'localhost'},
    {'HOST': 'LocalHost'},
    {'HOST': 'localhost.'},
    {'HOST': '127.0.0.1'},
    {'HOST': '127.1.2.3'},
    {'HOST': '::1'},
    {'HOST': '[::1]'},
    {'HOST': '::ffff:127.0.0.1'},
    {'HOST': '0:0:0:0:0:0:0:1'},
    {'HOST': '127.0.0.1 '},
    {'HOST': '::1%lo'},
    {'HOST': 'example.org'},
    {'HOST': 'LOCALHO\u017fT'},
    {'HOST': 'localhost\n'},
    {'HOST': '127.0.0.01'},
    {'HOST': '0127.0.0.1'},
    {'HOST': '127.1'},
    {'HOST': '127.0.0.1/8'},
    {'HOST': '::127.0.0.1'},
    {'HOST': '::ffff:7f00:1'},
    {'HOST': '0::1'},
    {'HOST': '::1%'},
    {'HOST': '\uff11\uff12\uff17.0.0.1'},
    {'HOST': '\u0661\u0662\u0667.0.0.1'},
    {'HOST': ''},
    {'TLS_TERMINATED': 'true'},
    {'TLS_TERMINATED': 'On'},
    {'TLS_TERMINATED': '0'},
    {'TLS_TERMINATED': 'off'},
    {'TLS_TERMINATED': 'No'},
    {'TLS_TERMINATED': 'maybe'},
    {'TLS_TERMINATED': ''},
    {'TLS_TERMINATED': ' true'},
    {'TLS_TERMINATED': 'İ'},
    # `casefold()` turns these ligatures and sharp s into ASCII letters.
    {'TLS_TERMINATED': 'o\ufb00'},
    {'TLS_TERMINATED': 'O\ufb00'},
    {'TLS_TERMINATED': 'Ye\u017f'},
    {'HOST': 'localho\ufb06'},
    {'HOST': 'LOCALHO\ufb05'},
    {'HOST': 'localho\u00df'},
    # Beyond u64, which Python takes and the port refuses (DIVERGENCES.md D13).
    {'PORT': '99999999999999999999'},
    {'PORT': '-' + '9' * 40},
    {'PORT': '-99999999999999999999'},
    {'PORT': '\x1c5'},
    {'PORT': '5\x1f'},
    {'PORT': '\u30005\x85'},
    {'PORT': ' -5 '},
    {'API_TOKEN': ''},
    {'API_TOKEN': TOKEN},
    {'TRANSCODE_PORT': '8001', 'TRANSCODE_TOKEN': 'worker-token'},
    {'TRANSCODE_PORT': ''},
    {'TRANSCODE_PORT': '0'},
    {'TRANSCODE_PORT': 'x'},
    {'TRANSCODE_TOKEN': ''},
    {'PORT': '0', 'MAX_ACTORS': '0'},
    {'HOST': '192.0.2.1', 'PORT': '0'},
    {'HOST': '192.0.2.1', 'TLS_TERMINATED': 'maybe'},
]


def _from_env(values: dict[str, str]) -> dict[str, Any]:
    saved = {key: value for key, value in os.environ.items() if key.startswith(PREFIX)}
    for key in saved:
        del os.environ[key]
    os.environ.update({PREFIX + key: value for key, value in values.items()})
    try:
        try:
            settings = Settings.from_env()
        except ValueError as exc:
            return {'error': {'type': type(exc).__name__, 'message': str(exc)}}
        return {'settings': dataclasses.asdict(settings)}
    finally:
        for key in [key for key in os.environ if key.startswith(PREFIX)]:
            del os.environ[key]
        os.environ.update(saved)


def _env() -> list[dict[str, Any]]:
    return [{'env': values, **_from_env(values)} for values in ENVIRONMENTS]


GOLDENS = {'settings/env.json': _env}
