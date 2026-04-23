"""ISM calendar connector (issue #13 P2)."""

from __future__ import annotations

from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_ism_calendar,
    schedule_ism_calendar,
)
from .indicators import (
    INDICATOR_REGISTRY,
    ISM_RELEASE_CALENDAR_URL,
    ISM_REPORTS_URL,
    ISMIndicatorSpec,
)
from .parser import (
    ISMCalendarEventRecord,
    ISMCalendarRawRecord,
    ISMReportParseError,
    ISMReportValue,
    parse_report_html,
    report_value_to_records,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    ISMScheduleEntry,
    ISMScheduleParseError,
    discover_current_report_url,
    fetch_report_html,
    fetch_reports_landing_html,
    fetch_schedule_html,
    parse_schedule_html,
    schedule_entry_to_records,
)

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "ISMCalendarEventRecord",
    "ISMCalendarRawRecord",
    "ISMIndicatorSpec",
    "ISMReportParseError",
    "ISMReportValue",
    "ISM_RELEASE_CALENDAR_URL",
    "ISM_REPORTS_URL",
    "ISMScheduleEntry",
    "ISMScheduleParseError",
    "ScheduleRunSummary",
    "discover_current_report_url",
    "fetch_ism_calendar",
    "fetch_report_html",
    "fetch_reports_landing_html",
    "fetch_schedule_html",
    "parse_report_html",
    "parse_schedule_html",
    "project_events",
    "project_schedule_events",
    "report_value_to_records",
    "schedule_entry_to_records",
    "schedule_ism_calendar",
    "store_raw",
]
