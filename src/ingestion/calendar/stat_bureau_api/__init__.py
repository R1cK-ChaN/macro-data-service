"""Statistics Bureau / e-Stat calendar connector (issue #14 P2)."""

from __future__ import annotations

from .estat import (
    EStatValue,
    StatBureauValueParseError,
    estat_value_to_records,
    fetch_estat_value_json,
    parse_estat_value_json,
    time_code_for_month,
)
from .fetcher import (
    ALL_INDICATORS,
    FetchRunSummary,
    ValuesRunSummary,
    fetch_stat_bureau_calendar,
    fetch_stat_bureau_values,
)
from .indicators import INDICATOR_REGISTRY, StatBureauIndicatorSpec
from .parser import (
    ESTAT_STATS_DATA_URL,
    PROVIDER,
    STAT_BUREAU_CPI_SCHEDULE_URL,
    STAT_BUREAU_LFS_SCHEDULE_URL,
    STAT_BUREAU_RELEASE_TIME_LOCAL,
    STAT_BUREAU_RELEASE_TZ,
    StatBureauCalendarEventRecord,
    StatBureauCalendarRawRecord,
    build_estat_dbview_url,
)
from .projector import project_events, project_schedule_events, store_raw
from .scraper import (
    STAT_BUREAU_BROWSER_HEADERS,
    StatBureauCalendarParseError,
    StatBureauScheduleEntry,
    fetch_cpi_release_schedule_html,
    fetch_lfs_release_schedule_html,
    parse_cpi_release_schedule_html,
    parse_lfs_release_schedule_html,
    schedule_entry_to_records,
)

__all__ = [
    "ALL_INDICATORS",
    "ESTAT_STATS_DATA_URL",
    "EStatValue",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "STAT_BUREAU_BROWSER_HEADERS",
    "STAT_BUREAU_CPI_SCHEDULE_URL",
    "STAT_BUREAU_LFS_SCHEDULE_URL",
    "STAT_BUREAU_RELEASE_TIME_LOCAL",
    "STAT_BUREAU_RELEASE_TZ",
    "StatBureauCalendarEventRecord",
    "StatBureauCalendarParseError",
    "StatBureauCalendarRawRecord",
    "StatBureauIndicatorSpec",
    "StatBureauScheduleEntry",
    "StatBureauValueParseError",
    "ValuesRunSummary",
    "build_estat_dbview_url",
    "estat_value_to_records",
    "fetch_cpi_release_schedule_html",
    "fetch_estat_value_json",
    "fetch_lfs_release_schedule_html",
    "fetch_stat_bureau_calendar",
    "fetch_stat_bureau_values",
    "parse_cpi_release_schedule_html",
    "parse_estat_value_json",
    "parse_lfs_release_schedule_html",
    "project_events",
    "project_schedule_events",
    "schedule_entry_to_records",
    "store_raw",
    "time_code_for_month",
]
