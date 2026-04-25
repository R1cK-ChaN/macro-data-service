"""HCOB / S&P Global Germany PMI calendar connector (issues #15 P5, #23 values)."""

from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_hcob_calendar,
    schedule_hcob_calendar,
)
from .indicators import (
    HCOB_BASE_URL,
    HCOB_PRESS_RELEASES_URL,
    HCOB_RELEASE_DATES_URL,
    HCOBIndicatorSpec,
    INDICATOR_REGISTRY,
    spec_for_calendar_title,
    specs_for_calendar_title,
)
from .parser import (
    HCOBCalendarEventRecord,
    HCOBCalendarRawRecord,
    HCOBPressReleaseParseError,
    HCOBValueObservation,
    extract_press_release_value,
    parse_observation,
    parse_press_release_pdf,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    HCOB_RELEASE_TZ,
    HCOBResolvedPressRelease,
    HCOBScheduleEntry,
    HCOBScheduleParseError,
    fetch_press_release_pdf_text,
    fetch_press_releases_listing_html,
    fetch_release_dates_html,
    parse_release_dates_html,
    resolve_press_release_link,
    schedule_entry_to_records,
)

__all__ = [
    "FetchRunSummary",
    "HCOB_BASE_URL",
    "HCOB_PRESS_RELEASES_URL",
    "HCOB_RELEASE_DATES_URL",
    "HCOB_RELEASE_TZ",
    "HCOBCalendarEventRecord",
    "HCOBCalendarRawRecord",
    "HCOBIndicatorSpec",
    "HCOBPressReleaseParseError",
    "HCOBResolvedPressRelease",
    "HCOBScheduleEntry",
    "HCOBScheduleParseError",
    "HCOBValueObservation",
    "INDICATOR_REGISTRY",
    "ScheduleRunSummary",
    "extract_press_release_value",
    "fetch_hcob_calendar",
    "fetch_press_release_pdf_text",
    "fetch_press_releases_listing_html",
    "fetch_release_dates_html",
    "parse_observation",
    "parse_press_release_pdf",
    "parse_release_dates_html",
    "project_events",
    "project_schedule_events",
    "resolve_press_release_link",
    "schedule_entry_to_records",
    "schedule_hcob_calendar",
    "spec_for_calendar_title",
    "specs_for_calendar_title",
    "store_raw",
]
