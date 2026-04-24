"""HCOB / S&P Global Germany PMI calendar connector (issue #15 P5, schedule-only)."""

from .fetcher import (
    ScheduleRunSummary,
    schedule_hcob_calendar,
)
from .indicators import (
    HCOB_BASE_URL,
    HCOB_RELEASE_DATES_URL,
    HCOBIndicatorSpec,
    INDICATOR_REGISTRY,
    spec_for_calendar_title,
)
from .parser import (
    HCOBCalendarEventRecord,
    HCOBCalendarRawRecord,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    HCOB_RELEASE_TZ,
    HCOBScheduleEntry,
    HCOBScheduleParseError,
    fetch_release_dates_html,
    parse_release_dates_html,
    schedule_entry_to_records,
)

__all__ = [
    "HCOB_BASE_URL",
    "HCOB_RELEASE_DATES_URL",
    "HCOB_RELEASE_TZ",
    "HCOBCalendarEventRecord",
    "HCOBCalendarRawRecord",
    "HCOBIndicatorSpec",
    "HCOBScheduleEntry",
    "HCOBScheduleParseError",
    "INDICATOR_REGISTRY",
    "ScheduleRunSummary",
    "fetch_release_dates_html",
    "parse_release_dates_html",
    "project_events",
    "project_schedule_events",
    "schedule_entry_to_records",
    "schedule_hcob_calendar",
    "spec_for_calendar_title",
    "store_raw",
]
