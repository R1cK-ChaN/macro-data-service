"""GfK / NIM Consumer Climate calendar connector (issue #15 P4c)."""

from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_gfk_calendar,
    schedule_gfk_calendar,
)
from .indicators import (
    GFK_ALL_RELEASES_URL,
    GFK_BASE_URL,
    GFK_CONSUMER_CLIMATE_URL,
    INDICATOR_REGISTRY,
    GfKIndicatorSpec,
    reference_label_en,
)
from .parser import (
    GfKCalendarEventRecord,
    GfKCalendarRawRecord,
    GfKPressReleaseParseError,
    GfKValueObservation,
    parse_observation,
    parse_press_release_value,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    GFK_DEFAULT_RELEASE_TIME,
    GFK_RELEASE_TZ,
    GfKResolvedPressRelease,
    GfKScheduleEntry,
    GfKScheduleParseError,
    fetch_all_releases_html,
    fetch_press_release_html,
    fetch_release_dates_html,
    parse_release_dates_html,
    resolve_press_release_link,
    schedule_entry_to_records,
)

__all__ = [
    "FetchRunSummary",
    "GFK_ALL_RELEASES_URL",
    "GFK_BASE_URL",
    "GFK_CONSUMER_CLIMATE_URL",
    "GFK_DEFAULT_RELEASE_TIME",
    "GFK_RELEASE_TZ",
    "GfKCalendarEventRecord",
    "GfKCalendarRawRecord",
    "GfKIndicatorSpec",
    "GfKPressReleaseParseError",
    "GfKResolvedPressRelease",
    "GfKScheduleEntry",
    "GfKScheduleParseError",
    "GfKValueObservation",
    "INDICATOR_REGISTRY",
    "ScheduleRunSummary",
    "fetch_all_releases_html",
    "fetch_gfk_calendar",
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
    "schedule_gfk_calendar",
    "store_raw",
]
