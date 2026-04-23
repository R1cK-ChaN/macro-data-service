"""Conference Board calendar connector (issue #13 P4)."""

from __future__ import annotations

from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_conference_board_calendar,
    schedule_conference_board_calendar,
)
from .indicators import (
    CONFERENCE_BOARD_CALENDAR_URL,
    CONFERENCE_BOARD_CONSUMER_CONFIDENCE_URL,
    CONFERENCE_BOARD_LEADING_INDICATORS_URL,
    INDICATOR_REGISTRY,
    ConferenceBoardIndicatorSpec,
)
from .parser import (
    ConferenceBoardCalendarEventRecord,
    ConferenceBoardCalendarRawRecord,
    ConferenceBoardCurrentValue,
    ConferenceBoardResultsParseError,
    current_value_to_records,
    parse_consumer_confidence_html,
    parse_current_value_html,
    parse_leading_index_html,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    CONFERENCE_BOARD_RELEASE_TZ,
    ConferenceBoardScheduleEntry,
    ConferenceBoardScheduleParseError,
    fetch_calendar_json,
    fetch_indicator_html,
    parse_calendar_events_json,
    schedule_entry_to_records,
)

__all__ = [
    "CONFERENCE_BOARD_CALENDAR_URL",
    "CONFERENCE_BOARD_CONSUMER_CONFIDENCE_URL",
    "CONFERENCE_BOARD_LEADING_INDICATORS_URL",
    "CONFERENCE_BOARD_RELEASE_TZ",
    "ConferenceBoardCalendarEventRecord",
    "ConferenceBoardCalendarRawRecord",
    "ConferenceBoardCurrentValue",
    "ConferenceBoardIndicatorSpec",
    "ConferenceBoardResultsParseError",
    "ConferenceBoardScheduleEntry",
    "ConferenceBoardScheduleParseError",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "ScheduleRunSummary",
    "current_value_to_records",
    "fetch_calendar_json",
    "fetch_conference_board_calendar",
    "fetch_indicator_html",
    "parse_calendar_events_json",
    "parse_consumer_confidence_html",
    "parse_current_value_html",
    "parse_leading_index_html",
    "project_events",
    "project_schedule_events",
    "schedule_conference_board_calendar",
    "schedule_entry_to_records",
    "store_raw",
]
