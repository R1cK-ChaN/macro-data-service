"""Spain INE calendar connector (issue #15 P3a)."""

from __future__ import annotations

from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_ine_calendar,
    schedule_ine_calendar,
)
from .indicators import (
    INE_PRESS_BASE_URL,
    INEIndicatorSpec,
    INDICATOR_REGISTRY,
    press_release_url,
)
from .parser import (
    INECalendarEventRecord,
    INECalendarRawRecord,
    INEPressReleaseParseError,
    INEValueObservation,
    parse_observation,
    parse_press_release_value,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    INE_CALENDAR_URL,
    INEScheduleEntry,
    INEScheduleParseError,
    fetch_calendar_html,
    fetch_press_release_html,
    parse_calendar_html,
    schedule_entry_to_records,
)

__all__ = [
    "INE_CALENDAR_URL",
    "INE_PRESS_BASE_URL",
    "INECalendarEventRecord",
    "INECalendarRawRecord",
    "INEIndicatorSpec",
    "INEPressReleaseParseError",
    "INEScheduleEntry",
    "INEScheduleParseError",
    "INEValueObservation",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "ScheduleRunSummary",
    "fetch_calendar_html",
    "fetch_ine_calendar",
    "fetch_press_release_html",
    "parse_calendar_html",
    "parse_observation",
    "parse_press_release_value",
    "press_release_url",
    "project_events",
    "project_schedule_events",
    "schedule_entry_to_records",
    "schedule_ine_calendar",
    "store_raw",
]
