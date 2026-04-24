"""METI calendar connector for issue #14 P5."""

from __future__ import annotations

from .fetcher import (
    ALL_INDICATORS,
    FetchRunSummary,
    ValuesRunSummary,
    fetch_meti_calendar,
    fetch_meti_values,
)
from .indicators import INDICATOR_REGISTRY, MetiIndicatorSpec
from .parser import (
    METI_IIP_RELEASE_CALENDAR_URL,
    METI_IIP_RESULTS_INDEX_URL,
    METI_RETAIL_PAGE_URL,
    METI_RELEASE_TZ,
    PROVIDER,
    MetiCalendarEventRecord,
    MetiCalendarRawRecord,
    build_iip_report_url,
)
from .projector import project_events, project_schedule_events, store_raw
from .reports import (
    IipReportValue,
    MetiReportParseError,
    RetailPageSummary,
    RetailReportValue,
    iip_value_to_records,
    parse_iip_report_html,
    parse_retail_current_page_html,
    parse_retail_outline_text,
    retail_value_to_records,
)
from .scraper import (
    METI_BROWSER_HEADERS,
    MetiCalendarParseError,
    MetiScheduleEntry,
    fetch_iip_release_calendar_xml,
    fetch_retail_schedule_html,
    parse_iip_release_calendar_xml,
    parse_retail_schedule_html,
    schedule_entry_to_records,
)

__all__ = [
    "ALL_INDICATORS",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "IipReportValue",
    "METI_BROWSER_HEADERS",
    "METI_IIP_RELEASE_CALENDAR_URL",
    "METI_IIP_RESULTS_INDEX_URL",
    "METI_RELEASE_TZ",
    "METI_RETAIL_PAGE_URL",
    "MetiCalendarEventRecord",
    "MetiCalendarParseError",
    "MetiCalendarRawRecord",
    "MetiIndicatorSpec",
    "MetiReportParseError",
    "MetiScheduleEntry",
    "PROVIDER",
    "RetailPageSummary",
    "RetailReportValue",
    "ValuesRunSummary",
    "build_iip_report_url",
    "fetch_iip_release_calendar_xml",
    "fetch_meti_calendar",
    "fetch_meti_values",
    "fetch_retail_schedule_html",
    "iip_value_to_records",
    "parse_iip_report_html",
    "parse_iip_release_calendar_xml",
    "parse_retail_current_page_html",
    "parse_retail_outline_text",
    "parse_retail_schedule_html",
    "project_events",
    "project_schedule_events",
    "retail_value_to_records",
    "schedule_entry_to_records",
    "store_raw",
]
