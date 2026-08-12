"""Versioned configuration store backed by the ``app_config`` table.

Reads are served from an in-process snapshot so hot paths (URL building,
scheduler decisions) never block on the database; the snapshot refreshes
after local writes immediately and via ``refresh()`` on an interval so other
replicas converge within seconds. Updates use optimistic version checks.
"""

import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Any

from embyx_manager.config.models import SECTION_MODELS, ConfigSection
from embyx_manager.db import Database

LOGGER = logging.getLogger(__name__)


class ConfigVersionConflictError(Exception):
    def __init__(self, section: str, expected: int, actual: int) -> None:
        super().__init__(f'config section {section} version conflict: expected {expected}, stored {actual}')
        self.section = section
        self.expected = expected
        self.actual = actual


class UnknownConfigSectionError(KeyError):
    def __init__(self, section: str) -> None:
        super().__init__(section)
        self.section = section


class ConfigStore:
    def __init__(self, database: Database) -> None:
        self._database = database
        self._snapshot: dict[str, tuple[ConfigSection, int]] = {
            name: (model(), 0) for name, model in SECTION_MODELS.items()
        }
        self._write_lock = asyncio.Lock()
        self._loaded = False

    def get[SectionT: ConfigSection](self, section_type: type[SectionT]) -> SectionT:
        """Current value of a section from the in-process snapshot."""
        name = _section_name(section_type)
        value, _ = self._snapshot[name]
        if not isinstance(value, section_type):  # pragma: no cover - registry keeps types aligned
            msg = f'config section {name} has unexpected type'
            raise TypeError(msg)
        return value

    def get_with_version(self, section: str) -> tuple[ConfigSection, int]:
        if section not in self._snapshot:
            raise UnknownConfigSectionError(section)
        return self._snapshot[section]

    def sections(self) -> dict[str, tuple[ConfigSection, int]]:
        return dict(self._snapshot)

    async def refresh(self) -> None:
        """Reload every section from the database into the snapshot."""
        pool = await self._database.get_pool()
        rows = await pool.fetch('SELECT section, value_json, version FROM app_config')
        stored = {row['section']: (row['value_json'], row['version']) for row in rows}
        for name, model in SECTION_MODELS.items():
            if name not in stored:
                self._snapshot[name] = (model(), 0)
                continue
            value_json, version = stored[name]
            try:
                self._snapshot[name] = (model.model_validate_json(value_json), version)
            except ValueError:
                LOGGER.exception('stored config for section %s is invalid; keeping previous value', name)
        self._loaded = True

    async def load(self) -> None:
        if not self._loaded:
            await self.refresh()

    async def update(
        self,
        section: str,
        values: dict[str, Any],
        *,
        expected_version: int | None = None,
    ) -> tuple[ConfigSection, int]:
        """Merge and persist one section; empty secret values keep stored secrets."""
        model = SECTION_MODELS.get(section)
        if model is None:
            raise UnknownConfigSectionError(section)
        async with self._write_lock:
            pool = await self._database.get_pool()
            async with pool.acquire() as connection, connection.transaction():
                row = await connection.fetchrow(
                    'SELECT value_json, version FROM app_config WHERE section = $1 FOR UPDATE',
                    section,
                )
                if row is not None:
                    current = model.model_validate_json(row['value_json'])
                    current_version = row['version']
                else:
                    current = model()
                    current_version = 0
                if expected_version is not None and expected_version != current_version:
                    raise ConfigVersionConflictError(section, expected_version, current_version)

                merged = dict(current.model_dump())
                for key, value in values.items():
                    if key in model.SECRET_FIELDS and (value is None or value == ''):
                        continue  # keep the stored secret
                    merged[key] = value
                updated = model.model_validate(merged)
                new_version = current_version + 1
                await connection.execute(
                    """
                    INSERT INTO app_config (section, value_json, version, updated_at)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (section) DO UPDATE SET
                        value_json = excluded.value_json,
                        version = excluded.version,
                        updated_at = excluded.updated_at
                    """,
                    section,
                    updated.model_dump_json(),
                    new_version,
                    datetime.now(UTC),
                )
            self._snapshot[section] = (updated, new_version)
            return updated, new_version


def _section_name(section_type: type[ConfigSection]) -> str:
    for name, model in SECTION_MODELS.items():
        if model is section_type:
            return name
    msg = f'unregistered config section type: {section_type!r}'
    raise KeyError(msg)


def masked_values(section: ConfigSection) -> dict[str, Any]:
    """Public representation: secrets blanked, with per-field configured flags."""
    values = json.loads(section.model_dump_json())
    for field in section.SECRET_FIELDS:
        values[field] = ''
    return values


def secret_flags(section: ConfigSection) -> dict[str, bool]:
    return {field: bool(getattr(section, field)) for field in sorted(section.SECRET_FIELDS)}
