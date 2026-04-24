"""Italy ISTAT calendar connector (issue #15 P3b)."""

from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_istat_calendar,
    schedule_istat_calendar,
)
from .indicators import (
    INDICATOR_REGISTRY,
    ISTAT_BASE_URL,
    ISTAT_PRESS_RELEASE_BASE_URL,
    ISTATIndicatorSpec,
    press_release_url,
)
from .parser import (
    ISTATCalendarEventRecord,
    ISTATCalendarRawRecord,
    ISTATPressReleaseParseError,
    ISTATValueObservation,
    parse_observation,
    parse_press_release_value,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    ISTAT_PRESS_CALENDAR_PAGE,
    ISTATScheduleEntry,
    ISTATScheduleParseError,
    discover_calendar_pdf_url,
    extract_calendar_pdf_text,
    fetch_calendar_pdf,
    fetch_press_release_html,
    parse_calendar_text,
    schedule_entry_to_records,
)

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "ISTAT_BASE_URL",
    "ISTAT_PRESS_CALENDAR_PAGE",
    "ISTAT_PRESS_RELEASE_BASE_URL",
    "ISTATCalendarEventRecord",
    "ISTATCalendarRawRecord",
    "ISTATIndicatorSpec",
    "ISTATPressReleaseParseError",
    "ISTATScheduleEntry",
    "ISTATScheduleParseError",
    "ISTATValueObservation",
    "ScheduleRunSummary",
    "discover_calendar_pdf_url",
    "extract_calendar_pdf_text",
    "fetch_calendar_pdf",
    "fetch_istat_calendar",
    "fetch_press_release_html",
    "parse_calendar_text",
    "parse_observation",
    "parse_press_release_value",
    "press_release_url",
    "project_events",
    "project_schedule_events",
    "schedule_entry_to_records",
    "schedule_istat_calendar",
    "store_raw",
]
