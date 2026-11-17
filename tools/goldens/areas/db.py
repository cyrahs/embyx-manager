"""The schema (`db.py`): the 19 migrations statement for statement, cross-version contract 1.

The Rust port runs the same statements in the same order inside the same transaction, so
the statements are recorded verbatim (whitespace included); `tools/scenarios` compares the
schema both builds leave on a real server (`db/schema`).
"""

from __future__ import annotations

from typing import Any

from embyx_manager import db, locking


def _migrations() -> list[dict[str, Any]]:
    return [
        {
            'current_schema_version': db.CURRENT_SCHEMA_VERSION,
            'advisory_namespace': db.ADVISORY_NAMESPACE,
            'lock_keys': {
                'migration': db.MIGRATION_LOCK_KEY,
                'enqueue': db.ENQUEUE_LOCK_KEY,
                'mutation': db.MUTATION_LOCK_KEY,
                'locking.mutation': locking.MUTATION_LOCK_KEY,
                'locking.namespace': locking._ADVISORY_NAMESPACE,  # noqa: SLF001 - the constant under test
            },
            'unsupported_version_message': str(db.UnsupportedSchemaVersionError(20)),
        },
        *({'version': version, 'statements': list(db._MIGRATIONS[version])} for version in sorted(db._MIGRATIONS)),  # noqa: SLF001
    ]


GOLDENS = {'db/migrations.json': _migrations}
