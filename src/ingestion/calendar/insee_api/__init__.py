"""France INSEE calendar connector (issue #15 P3c)."""

from .fetcher import (
    FetchRunSummary,
    INSEEReleaseResolutionError,
    INSEEResolvedRelease,
    ScheduleRunSummary,
    fetch_insee_calendar,
    fetch_press_release_html,
    resolve_release_document,
    schedule_insee_calendar,
    search_release_documents,
)
from .indicators import (
    INDICATOR_REGISTRY,
    INSEE_BASE_URL,
    INSEE_PUBLICATION_CALENDAR_URL,
    INSEE_STATS_BASE_URL,
    INSEEIndicatorSpec,
    press_release_url,
    reference_label_en,
)
from .parser import (
    INSEECalendarEventRecord,
    INSEECalendarRawRecord,
    INSEEPressReleaseParseError,
    INSEEValueObservation,
    parse_observation,
    parse_press_release_value,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    INSEE_AGENDA_URL,
    INSEEScheduleEntry,
    INSEEScheduleParseError,
    fetch_agenda_json,
    parse_agenda_json,
    schedule_entry_to_records,
)

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "INSEE_AGENDA_URL",
    "INSEE_BASE_URL",
    "INSEE_PUBLICATION_CALENDAR_URL",
    "INSEE_STATS_BASE_URL",
    "INSEECalendarEventRecord",
    "INSEECalendarRawRecord",
    "INSEEIndicatorSpec",
    "INSEEPressReleaseParseError",
    "INSEEReleaseResolutionError",
    "INSEEResolvedRelease",
    "INSEEScheduleEntry",
    "INSEEScheduleParseError",
    "INSEEValueObservation",
    "ScheduleRunSummary",
    "fetch_agenda_json",
    "fetch_insee_calendar",
    "fetch_press_release_html",
    "parse_agenda_json",
    "parse_observation",
    "parse_press_release_value",
    "press_release_url",
    "project_events",
    "project_schedule_events",
    "reference_label_en",
    "resolve_release_document",
    "schedule_entry_to_records",
    "schedule_insee_calendar",
    "search_release_documents",
    "store_raw",
]
