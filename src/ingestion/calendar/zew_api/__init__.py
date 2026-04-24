"""ZEW calendar connector (issue #15 P4a)."""

from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_zew_calendar,
    schedule_zew_calendar,
)
from .indicators import (
    INDICATOR_REGISTRY,
    ZEW_BASE_URL,
    ZEW_PRESS_RELEASES_URL,
    ZEWIndicatorSpec,
    reference_label_en,
    release_dates_url,
)
from .parser import (
    ZEWCalendarEventRecord,
    ZEWCalendarRawRecord,
    ZEWPressReleaseParseError,
    ZEWValueObservation,
    parse_observation,
    parse_press_release_value,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    ZEW_DEFAULT_RELEASE_TIME,
    ZEW_RELEASE_TZ,
    ZEWResolvedPressRelease,
    ZEWScheduleEntry,
    ZEWScheduleParseError,
    fetch_latest_press_releases_html,
    fetch_press_release_html,
    fetch_release_dates_html,
    parse_release_dates_html,
    resolve_press_release_link,
    schedule_entry_to_records,
)

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "ScheduleRunSummary",
    "ZEW_BASE_URL",
    "ZEW_DEFAULT_RELEASE_TIME",
    "ZEW_PRESS_RELEASES_URL",
    "ZEW_RELEASE_TZ",
    "ZEWCalendarEventRecord",
    "ZEWCalendarRawRecord",
    "ZEWIndicatorSpec",
    "ZEWPressReleaseParseError",
    "ZEWResolvedPressRelease",
    "ZEWScheduleEntry",
    "ZEWScheduleParseError",
    "ZEWValueObservation",
    "fetch_latest_press_releases_html",
    "fetch_press_release_html",
    "fetch_release_dates_html",
    "fetch_zew_calendar",
    "parse_observation",
    "parse_press_release_value",
    "parse_release_dates_html",
    "project_events",
    "project_schedule_events",
    "reference_label_en",
    "release_dates_url",
    "resolve_press_release_link",
    "schedule_entry_to_records",
    "schedule_zew_calendar",
    "store_raw",
]
