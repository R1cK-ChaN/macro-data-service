"""Federal Reserve calendar connector (issue #9 P4, P4a, P4b-values).

Projects the FOMC meeting calendar, the Fed news-release schedule,
and FOMC statement values into ``cal_econ_raw`` / ``cal_econ_event``.

Sources:

- ``fomccalendars.htm`` — schedule-side: every FOMC meeting publishes
  a ``FOMC_RATE`` event at 14:00 ET on the closing day (DST-aware).
  SEP rides as a boolean on the FOMC row when the asterisk is present.
- ``/json/calendar.json`` — schedule-side: ``BEIGE_BOOK`` (~8/year,
  14:00 ET), ``FED_H41`` (weekly Thursday 16:30 ET), ``FED_H8``
  (weekly Friday 16:15 ET). SEP rows are explicitly excluded — SEP
  already rides on the FOMC meeting row. Replaces the former
  ``releasedates.htm`` scrape that 404'd at the 2026-04-22 live probe
  (issue #9 P4b-live-follow-up).
- ``pressreleases/monetary<YYYYMMDD>a.htm`` — value-side: the FOMC
  statement for a given closing date names the new target range for
  the federal funds rate; :func:`fetch_fed_statement_values` fills
  ``actual`` on existing schedule rows.

Scheduled Chair / Vice-Chair speeches and testimony belong in the
news pipeline and are out of scope for this connector.

The Fed publishes no authenticated calendar API. The FOMC and
statement pages are HTML scrapes that share a browser-UA header
bundle (the default ``python-requests`` UA receives 403s on the HTML
surfaces). The ``/json/calendar.json`` release feed has no UA gate
and is fetched with plain ``requests`` defaults.

Schedule-side writes route through
:func:`project_schedule_events` so the value-side scrape's
``actual`` isn't nulled out on a later schedule re-scrape; the
statement-value writer uses the full :func:`project_events` upsert.
"""

from __future__ import annotations

from .fetcher import (
    FetchRunSummary,
    ReleasedatesRunSummary,
    StatementValuesRunSummary,
    fetch_fed_calendar,
    fetch_fed_releasedates,
    fetch_fed_statement_values,
)
from .indicators import FedIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    FedCalendarEventRecord,
    FedCalendarRawRecord,
    FomcMeetingEntry,
    meeting_entry_to_records,
)
from .projector import project_events, project_schedule_events, store_raw
from .releases import (
    FED_CALENDAR_JSON_URL,
    FedCalendarJsonParseError,
    FedReleaseEntry,
    fetch_fed_calendar_json,
    parse_fed_calendar_json,
    release_entry_to_records,
)
from .scraper import (
    FOMC_CALENDAR_URL,
    FomcCalendarParseError,
    fetch_fomc_calendar_html,
    parse_fomc_calendar_html,
)
from .statements import (
    FOMC_STATEMENT_URL_TEMPLATE,
    FomcStatementParseError,
    StatementValue,
    build_statement_url,
    fetch_statement_html,
    parse_statement_html,
    statement_value_to_records,
)

__all__ = [
    "FED_CALENDAR_JSON_URL",
    "FOMC_CALENDAR_URL",
    "FOMC_STATEMENT_URL_TEMPLATE",
    "FedCalendarEventRecord",
    "FedCalendarJsonParseError",
    "FedCalendarRawRecord",
    "FedIndicatorSpec",
    "FedReleaseEntry",
    "FetchRunSummary",
    "FomcCalendarParseError",
    "FomcMeetingEntry",
    "FomcStatementParseError",
    "INDICATOR_REGISTRY",
    "ReleasedatesRunSummary",
    "StatementValue",
    "StatementValuesRunSummary",
    "build_statement_url",
    "fetch_fed_calendar",
    "fetch_fed_calendar_json",
    "fetch_fed_releasedates",
    "fetch_fed_statement_values",
    "fetch_fomc_calendar_html",
    "fetch_statement_html",
    "meeting_entry_to_records",
    "parse_fed_calendar_json",
    "parse_fomc_calendar_html",
    "parse_statement_html",
    "project_events",
    "project_schedule_events",
    "release_entry_to_records",
    "statement_value_to_records",
    "store_raw",
]
