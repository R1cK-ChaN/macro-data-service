"""NAR calendar connector (issue #13 P5)."""

from __future__ import annotations

from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_nar_calendar,
    schedule_nar_calendar,
)
from .indicators import (
    INDICATOR_REGISTRY,
    NAR_EXISTING_HOME_SALES_URL,
    NAR_PENDING_HOME_SALES_URL,
    NAR_SCHEDULE_URL,
    NARIndicatorSpec,
)
from .parser import (
    NARCalendarEventRecord,
    NARCalendarRawRecord,
    NARCurrentValue,
    NARResultsParseError,
    current_value_to_records,
    parse_current_value_html,
    parse_existing_home_sales_html,
    parse_pending_home_sales_html,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    NAR_RELEASE_TIME_LOCAL,
    NAR_RELEASE_TZ,
    NARScheduleEntry,
    NARScheduleParseError,
    fetch_current_html,
    fetch_schedule_html,
    parse_schedule_html,
    schedule_entry_to_records,
)

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "NARCalendarEventRecord",
    "NARCalendarRawRecord",
    "NARCurrentValue",
    "NARIndicatorSpec",
    "NARResultsParseError",
    "NARScheduleEntry",
    "NARScheduleParseError",
    "NAR_EXISTING_HOME_SALES_URL",
    "NAR_PENDING_HOME_SALES_URL",
    "NAR_RELEASE_TIME_LOCAL",
    "NAR_RELEASE_TZ",
    "NAR_SCHEDULE_URL",
    "ScheduleRunSummary",
    "current_value_to_records",
    "fetch_current_html",
    "fetch_nar_calendar",
    "fetch_schedule_html",
    "parse_current_value_html",
    "parse_existing_home_sales_html",
    "parse_pending_home_sales_html",
    "parse_schedule_html",
    "project_events",
    "project_schedule_events",
    "schedule_entry_to_records",
    "schedule_nar_calendar",
    "store_raw",
]
