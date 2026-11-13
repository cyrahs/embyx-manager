"""The config store (`config/store.py`), cross-version contract 2.

`config.store` loads a `ConfigStore` on the migrated database (seed rows may hold stored
sections, valid or not), reports the snapshot, then applies `args.updates` in order, each
`{section, values, expected_version?}`, and reports what each returned or raised. The rows
show the stored `value_json` bytes. A validation error is reported by its pydantic errors
(`type`, `loc`, `msg`), since `str(ValidationError)` is not part of the contract
(DIVERGENCES.md D11); the other errors by their message.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from embyx_manager.config.store import (
    ConfigStore,
    ConfigVersionConflictError,
    UnknownConfigSectionError,
    masked_values,
    secret_flags,
)
from embyx_manager.db import Database

if TYPE_CHECKING:
    from runner import Context


def _snapshot(store: ConfigStore) -> list[dict[str, Any]]:
    return [
        {'section': name, 'version': version, 'dump_json': value.model_dump_json()}
        for name, (value, version) in store.sections().items()
    ]


def _failure(error: Exception) -> dict[str, Any]:
    if isinstance(error, ValidationError):
        errors = error.errors(include_url=False, include_context=False, include_input=False)
        return {
            'type': 'ValidationError',
            'errors': [{'type': item['type'], 'loc': list(item['loc']), 'msg': item['msg']} for item in errors],
        }
    return {'type': type(error).__name__, 'message': str(error)}


async def store(ctx: Context) -> Any:
    database = Database(ctx.dsn)
    try:
        config = ConfigStore(database)
        await config.load()
        loaded = _snapshot(config)
        results = []
        for update in ctx.args.get('updates', []):
            try:
                value, version = await config.update(
                    update['section'], update['values'], expected_version=update.get('expected_version')
                )
            except (ValidationError, ConfigVersionConflictError, UnknownConfigSectionError) as error:
                results.append({'error': _failure(error)})
            else:
                results.append(
                    {
                        'version': version,
                        'dump_json': value.model_dump_json(),
                        'masked': masked_values(value),
                        'secret_flags': secret_flags(value),
                    }
                )
        return {'loaded': loaded, 'updates': results, 'final': _snapshot(config)}
    finally:
        await database.aclose()


OPERATIONS = {'config.store': store}
