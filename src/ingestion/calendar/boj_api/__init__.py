"""Bank of Japan calendar connector (issue #14 P1).

Projects the Monetary Policy Meeting (MPM) rate-decision calendar and
the statement-side policy-rate value into ``cal_econ_raw`` /
``cal_econ_event``.

Sources:

- ``boj.or.jp/en/mopo/mpmsche_minu/`` — schedule-side: every MPM
  publishes a ``BOJ_RATE`` event anchored on the meeting's closing day.
  BoJ decisions are announced around noon JST after the committee
  closes; we use 12:00 JST (03:00 UTC) as the scheduled convention.
- ``boj.or.jp/en/mopo/mpmdeci/state_<YYYY>/k<YYMMDD>a.htm`` —
  value-side: the statement page for a given closing date carries the
  new target policy rate in a stable sentence:

      "The Bank will encourage the uncollateralized overnight call
       rate to remain at around 0.5 percent."

  :func:`fetch_boj_statement_values` fills ``actual`` on existing
  schedule rows.

Scope for P1 is the rate-decision anchor only. Tankan (quarterly
business-sentiment survey) is the BoJ's second-largest calendar
indicator and ships in a follow-up slice — its schedule lives on a
different page (``boj.or.jp/en/statistics/outline/exp/tk/``) and its
value-side writer would have a different shape.

Schedule-side writes route through :func:`project_schedule_events` so
the value-side scrape's ``actual`` isn't nulled out on a later
schedule re-scrape; the statement-value writer uses the full
:func:`project_events` upsert.
"""

from __future__ import annotations

from .fetcher import (
    FetchRunSummary,
    StatementValuesRunSummary,
    fetch_boj_calendar,
    fetch_boj_statement_values,
)
from .indicators import INDICATOR_REGISTRY, BojIndicatorSpec
from .parser import (
    BojCalendarEventRecord,
    BojCalendarRawRecord,
    BojMpmEntry,
    mpm_entry_to_records,
)
from .projector import project_events, project_schedule_events, store_raw
from .scraper import (
    BOJ_MPM_CALENDAR_URL,
    BojMpmCalendarParseError,
    fetch_boj_mpm_calendar_html,
    parse_boj_mpm_calendar_html,
)
from .statements import (
    BOJ_STATEMENT_URL_TEMPLATE,
    BojStatementParseError,
    StatementValue,
    build_statement_url,
    fetch_statement_html,
    parse_statement_html,
    statement_value_to_records,
)

__all__ = [
    "BOJ_MPM_CALENDAR_URL",
    "BOJ_STATEMENT_URL_TEMPLATE",
    "BojCalendarEventRecord",
    "BojCalendarRawRecord",
    "BojIndicatorSpec",
    "BojMpmCalendarParseError",
    "BojMpmEntry",
    "BojStatementParseError",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "StatementValue",
    "StatementValuesRunSummary",
    "build_statement_url",
    "fetch_boj_calendar",
    "fetch_boj_mpm_calendar_html",
    "fetch_boj_statement_values",
    "fetch_statement_html",
    "mpm_entry_to_records",
    "parse_boj_mpm_calendar_html",
    "parse_statement_html",
    "project_events",
    "project_schedule_events",
    "statement_value_to_records",
    "store_raw",
]
