"""Census Bureau calendar connector (issue #13 P1).

Projects Census Economic Indicators Time Series observations and the
Census list-view release calendar into ``cal_econ_raw`` /
``cal_econ_event``.
"""

from __future__ import annotations

from .client import (
    CensusEITSClient,
    CensusEITSError,
    CensusEITSObservation,
    CensusEITSResponseError,
)
from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_census_calendar,
    schedule_census_calendar,
)
from .indicators import (
    INDICATOR_REGISTRY,
    CensusIndicatorSpec,
)
from .parser import (
    CensusCalendarEventRecord,
    CensusCalendarRawRecord,
    parse_observation,
)
from .projector import (
    project_events,
    project_schedule_events,
    store_raw,
)
from .schedule import (
    CENSUS_CALENDAR_URL,
    CensusScheduleEntry,
    CensusScheduleParseError,
    fetch_schedule_html,
    parse_schedule_html,
    schedule_entry_to_records,
)

__all__ = [
    "CENSUS_CALENDAR_URL",
    "CensusCalendarEventRecord",
    "CensusCalendarRawRecord",
    "CensusEITSClient",
    "CensusEITSError",
    "CensusEITSObservation",
    "CensusEITSResponseError",
    "CensusIndicatorSpec",
    "CensusScheduleEntry",
    "CensusScheduleParseError",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "ScheduleRunSummary",
    "fetch_census_calendar",
    "fetch_schedule_html",
    "parse_observation",
    "parse_schedule_html",
    "project_events",
    "project_schedule_events",
    "schedule_census_calendar",
    "schedule_entry_to_records",
    "store_raw",
]
