"""Ifo calendar connector (issue #15 P4b)."""

from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_ifo_calendar,
    schedule_ifo_calendar,
)
from .indicators import (
    IFO_BASE_URL,
    IFO_PRESS_RELEASES_URL,
    IFO_SURVEY_URL,
    INDICATOR_REGISTRY,
    IfoIndicatorSpec,
    reference_label_en,
)
from .parser import (
    IfoCalendarEventRecord,
    IfoCalendarRawRecord,
    IfoPressReleaseParseError,
    IfoValueObservation,
    parse_observation,
    parse_press_release_value,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    IFO_DEFAULT_RELEASE_TIME,
    IFO_RELEASE_TZ,
    IfoResolvedPressRelease,
    IfoScheduleEntry,
    IfoScheduleParseError,
    fetch_latest_press_releases_html,
    fetch_press_release_html,
    fetch_release_dates_html,
    parse_release_dates_html,
    resolve_press_release_link,
    schedule_entry_to_records,
)

__all__ = [
    "FetchRunSummary",
    "IFO_BASE_URL",
    "IFO_DEFAULT_RELEASE_TIME",
    "IFO_PRESS_RELEASES_URL",
    "IFO_RELEASE_TZ",
    "IFO_SURVEY_URL",
    "INDICATOR_REGISTRY",
    "IfoCalendarEventRecord",
    "IfoCalendarRawRecord",
    "IfoIndicatorSpec",
    "IfoPressReleaseParseError",
    "IfoResolvedPressRelease",
    "IfoScheduleEntry",
    "IfoScheduleParseError",
    "IfoValueObservation",
    "ScheduleRunSummary",
    "fetch_ifo_calendar",
    "fetch_latest_press_releases_html",
    "fetch_press_release_html",
    "fetch_release_dates_html",
    "parse_observation",
    "parse_press_release_value",
    "parse_release_dates_html",
    "project_events",
    "project_schedule_events",
    "reference_label_en",
    "resolve_press_release_link",
    "schedule_entry_to_records",
    "schedule_ifo_calendar",
    "store_raw",
]
