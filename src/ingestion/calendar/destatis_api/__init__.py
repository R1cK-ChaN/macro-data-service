"""Destatis calendar connector (issue #15 P2)."""

from __future__ import annotations

from .client import (
    DESTATIS_GENESIS_BASE_URL,
    DESTATIS_PASSWORD_ENV,
    DESTATIS_USERNAME_ENV,
    DestatisGenesisClient,
    DestatisGenesisError,
)
from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_destatis_calendar,
    schedule_destatis_calendar,
)
from .indicators import DestatisIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    DestatisCalendarEventRecord,
    DestatisCalendarRawRecord,
    DestatisGenesisParseError,
    DestatisObservation,
    parse_genesis_csv_table,
    parse_observation,
    parse_period,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    DESTATIS_RELEASE_TABLE_URL,
    DESTATIS_RELEASE_TZ,
    DestatisScheduleEntry,
    DestatisScheduleParseError,
    fetch_release_table_html,
    parse_release_table_html,
    schedule_entry_to_records,
)

__all__ = [
    "DESTATIS_GENESIS_BASE_URL",
    "DESTATIS_PASSWORD_ENV",
    "DESTATIS_RELEASE_TABLE_URL",
    "DESTATIS_RELEASE_TZ",
    "DESTATIS_USERNAME_ENV",
    "DestatisCalendarEventRecord",
    "DestatisCalendarRawRecord",
    "DestatisGenesisClient",
    "DestatisGenesisError",
    "DestatisGenesisParseError",
    "DestatisIndicatorSpec",
    "DestatisObservation",
    "DestatisScheduleEntry",
    "DestatisScheduleParseError",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "ScheduleRunSummary",
    "fetch_destatis_calendar",
    "fetch_release_table_html",
    "parse_genesis_csv_table",
    "parse_observation",
    "parse_period",
    "parse_release_table_html",
    "project_events",
    "project_schedule_events",
    "schedule_destatis_calendar",
    "schedule_entry_to_records",
    "store_raw",
]
