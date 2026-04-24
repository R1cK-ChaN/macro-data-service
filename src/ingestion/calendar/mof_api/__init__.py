"""Ministry of Finance (Japan) trade-statistics calendar connector
(issue #14 P4).

Projects the MoF monthly trade-statistics calendar into
``cal_econ_raw`` / ``cal_econ_event``. Single indicator anchor —
``TRADE_BALANCE``, the trade-balance headline published in the
Monthly Data (Provisional) release at 08:50 JST ~20 days after
each reference month.

Sources:

- ``customs.go.jp/toukei/calendar/calend_e.htm`` — schedule-side:
  the "Trade Statistics (Provisional, Detailed)" table's column 3
  ("Monthly Data") carries the Balance of Trade release dates
  forward for the current and next calendar years.
- ``customs.go.jp/toukei/shinbun/trade-st_e/<YYYY>/<YYYYMM>4e.xml``
  — value-side: each release publishes a structured XML feed whose
  ``<sogakutsuki name="pg1"> / <sashihiki> / <sogakutonen>`` cell
  holds the headline trade balance in millions of yen. Deficits
  render with the Japanese triangle (``△``) sign; the parser
  strips it to a plain minus.

``provider_event_id`` anchors on ``(indicator, reference_date)``
so the schedule and value sides share the same id, and the merge
CASE in the shared projector preserves the schedule-side datetime
on upsert. Schedule-side writes route through
:func:`project_schedule_events`; the value-side writer uses the
full :func:`project_events` upsert.
"""

from __future__ import annotations

from .fetcher import (
    ALL_INDICATORS,
    FetchRunSummary,
    TradeValuesRunSummary,
    fetch_mof_calendar,
    fetch_mof_trade_values,
)
from .indicators import INDICATOR_REGISTRY, MofIndicatorSpec
from .parser import (
    MOF_CALENDAR_URL,
    MOF_RELEASE_TIME_LOCAL,
    MOF_RELEASE_TZ,
    MOF_TRADE_REPORT_URL_TEMPLATE,
    MofCalendarEventRecord,
    MofCalendarRawRecord,
    PROVIDER,
    build_trade_report_url,
)
from .projector import project_events, project_schedule_events, store_raw
from .reports import (
    MofTradeReportParseError,
    TradeReportValue,
    fetch_trade_report_xml,
    parse_trade_report_xml,
    trade_report_to_records,
)
from .scraper import (
    MofCalendarEntry,
    MofCalendarParseError,
    fetch_mof_calendar_html,
    parse_mof_calendar_html,
    schedule_entry_to_records,
)

__all__ = [
    "ALL_INDICATORS",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "MOF_CALENDAR_URL",
    "MOF_RELEASE_TIME_LOCAL",
    "MOF_RELEASE_TZ",
    "MOF_TRADE_REPORT_URL_TEMPLATE",
    "MofCalendarEntry",
    "MofCalendarEventRecord",
    "MofCalendarParseError",
    "MofCalendarRawRecord",
    "MofIndicatorSpec",
    "MofTradeReportParseError",
    "PROVIDER",
    "TradeReportValue",
    "TradeValuesRunSummary",
    "build_trade_report_url",
    "fetch_mof_calendar",
    "fetch_mof_calendar_html",
    "fetch_mof_trade_values",
    "fetch_trade_report_xml",
    "parse_mof_calendar_html",
    "parse_trade_report_xml",
    "project_events",
    "project_schedule_events",
    "schedule_entry_to_records",
    "store_raw",
    "trade_report_to_records",
]
