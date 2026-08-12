"""Structured run reporting shared by the monitor pipelines.

Each pipeline run gets a :class:`RunContext`: a cancellation token, counter
map, and a bounded log collector whose tail is persisted with the run so the
dashboard can show what happened without shipping whole log files.
"""

import asyncio
import logging
from collections import deque
from enum import StrEnum

MAX_LOG_LINES = 200
MAX_ERRORS = 50


class PipelineName(StrEnum):
    RSS = 'rss'
    ARCHIVE = 'archive'
    MAPPING = 'mapping'


class RunTrigger(StrEnum):
    SCHEDULED = 'scheduled'
    MANUAL = 'manual'
    WATCHDOG = 'watchdog'
    STARTUP = 'startup'


class RunState(StrEnum):
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELLED = 'cancelled'


class RunCancelledError(Exception):
    """Raised inside a pipeline when its stop token is set."""


class RunContext:
    def __init__(self, *, logger: logging.Logger) -> None:
        self._logger = logger
        self._lines: deque[str] = deque(maxlen=MAX_LOG_LINES)
        self._errors: list[str] = []
        self.stats: dict[str, int] = {}
        self._stop = asyncio.Event()

    # -- cancellation ----------------------------------------------------
    def request_stop(self) -> None:
        self._stop.set()

    @property
    def stop_requested(self) -> bool:
        return self._stop.is_set()

    def check_cancelled(self) -> None:
        if self._stop.is_set():
            raise RunCancelledError

    # -- counters ---------------------------------------------------------
    def add(self, counter: str, amount: int = 1) -> None:
        self.stats[counter] = self.stats.get(counter, 0) + amount

    def set(self, counter: str, value: int) -> None:
        self.stats[counter] = value

    # -- logging ----------------------------------------------------------
    def info(self, message: str, *args: object) -> None:
        self._logger.info(message, *args)
        self._record(message, args)

    def warning(self, message: str, *args: object) -> None:
        self._logger.warning(message, *args)
        self._record('WARNING: ' + message, args)

    def error(self, message: str, *args: object) -> None:
        self._logger.error(message, *args)
        rendered = self._record('ERROR: ' + message, args)
        if len(self._errors) < MAX_ERRORS:
            self._errors.append(rendered)

    def exception(self, message: str, *args: object) -> None:
        self._logger.exception(message, *args)  # noqa: LOG004 - called from callers' except blocks
        rendered = self._record('ERROR: ' + message, args)
        if len(self._errors) < MAX_ERRORS:
            self._errors.append(rendered)

    def _record(self, message: str, args: tuple[object, ...]) -> str:
        rendered = message % args if args else message
        self._lines.append(rendered)
        return rendered

    @property
    def log_tail(self) -> tuple[str, ...]:
        return tuple(self._lines)

    @property
    def errors(self) -> tuple[str, ...]:
        return tuple(self._errors)
