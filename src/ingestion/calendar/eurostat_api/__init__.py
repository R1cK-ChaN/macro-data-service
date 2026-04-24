"""Eurostat calendar connector (issue #15 P1)."""

from __future__ import annotations

from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_eurostat_calendar,
    schedule_eurostat_calendar,
)
from .indicators import EurostatIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    EurostatCalendarEventRecord,
    EurostatCalendarRawRecord,
    parse_observation,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    EUROSTAT_EVENTS_JSON_URL,
    EUROSTAT_RELEASE_CALENDAR_URL,
    EurostatScheduleEntry,
    EurostatScheduleParseError,
    fetch_release_calendar_json,
    parse_release_calendar_json,
    schedule_entry_to_records,
)

__all__ = [
    "EUROSTAT_EVENTS_JSON_URL",
    "EUROSTAT_RELEASE_CALENDAR_URL",
    "EurostatCalendarEventRecord",
    "EurostatCalendarRawRecord",
    "EurostatIndicatorSpec",
    "EurostatScheduleEntry",
    "EurostatScheduleParseError",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "ScheduleRunSummary",
    "fetch_eurostat_calendar",
    "fetch_release_calendar_json",
    "parse_observation",
    "parse_release_calendar_json",
    "project_events",
    "project_schedule_events",
    "schedule_entry_to_records",
    "schedule_eurostat_calendar",
    "store_raw",
]

