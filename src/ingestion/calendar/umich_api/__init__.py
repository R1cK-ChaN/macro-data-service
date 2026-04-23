"""U Michigan calendar connector (issue #13 P3)."""

from __future__ import annotations

from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_umich_calendar,
    schedule_umich_calendar,
)
from .indicators import (
    INDICATOR_REGISTRY,
    UMICH_MAIN_URL,
    UMICH_SURVEY_INFO_URL,
    UMichIndicatorSpec,
)
from .parser import (
    UMichCalendarEventRecord,
    UMichCalendarRawRecord,
    UMichCurrentValue,
    UMichResultsParseError,
    current_value_to_records,
    event_anchor,
    normalize_release_stage,
    parse_current_results_html,
    stage_label,
    title_for_stage,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    UMICH_RELEASE_TIME_LOCAL,
    UMICH_RELEASE_TZ,
    UMichScheduleDocument,
    UMichScheduleEntry,
    UMichScheduleParseError,
    discover_release_dates_url,
    document_bytes_to_text,
    fetch_current_results_html,
    fetch_release_dates_document,
    fetch_survey_info_html,
    parse_release_dates_text,
    schedule_entry_to_records,
)

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "ScheduleRunSummary",
    "UMICH_MAIN_URL",
    "UMICH_RELEASE_TIME_LOCAL",
    "UMICH_RELEASE_TZ",
    "UMICH_SURVEY_INFO_URL",
    "UMichCalendarEventRecord",
    "UMichCalendarRawRecord",
    "UMichCurrentValue",
    "UMichIndicatorSpec",
    "UMichResultsParseError",
    "UMichScheduleDocument",
    "UMichScheduleEntry",
    "UMichScheduleParseError",
    "current_value_to_records",
    "discover_release_dates_url",
    "document_bytes_to_text",
    "event_anchor",
    "fetch_current_results_html",
    "fetch_release_dates_document",
    "fetch_survey_info_html",
    "fetch_umich_calendar",
    "normalize_release_stage",
    "parse_current_results_html",
    "parse_release_dates_text",
    "project_events",
    "project_schedule_events",
    "schedule_entry_to_records",
    "schedule_umich_calendar",
    "stage_label",
    "store_raw",
    "title_for_stage",
]
