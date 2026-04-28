"""EODHD Calendar API scaffold (issue #8 slice 3).

Corporate-actions lane for ``cal_corp_*``. Covers the five EODHD calendar
endpoints:

- ``/api/calendar/earnings``     → subtype ``earnings``
- ``/api/calendar/trends``       → subtype ``earnings_trend``
- ``/api/calendar/ipos``         → subtype ``ipo``
- ``/api/calendar/splits``       → subtype ``split``
- ``/api/calendar/dividends``    → subtype ``dividend``

Public surface:

- :class:`EODHDAPIClient` — HTTP wrapper (auth, 429 retry, fmt=json).
- :func:`parse_earnings_row` / ``parse_ipo_row`` / ``parse_split_row``
  / ``parse_dividend_row`` / ``parse_trend_row`` — row → (raw, event).
- :func:`store_corp_raw` / :func:`project_corp_events` — persistence.
- :class:`CorpCalendarFetcher` — subtype-dispatched window fetcher.

Scaffold only: nothing auto-runs. Callers must pass ``dry_run=False``.
"""

from __future__ import annotations

from .backfill import (
    CorpBackfillRunner,
    CorpBackfillSummary,
    CorpWindow,
    PHASES as CORP_BACKFILL_PHASES,
    SUBTYPES_BACKFILLABLE,
    plan_corp_windows,
)
from .client import (
    EODHDAPIClient,
    EODHDAuthMissing,
    EODHDThrottled,
)
from .fetcher import CorpCalendarFetcher, CorpRunSummary, fetch_dividend_details
from .parser import (
    CalendarCorpEventRecord,
    CalendarCorpRawRecord,
    SUBTYPES,
    parse_dividend_detail_row,
    parse_dividend_row,
    parse_earnings_row,
    parse_ipo_row,
    parse_split_row,
    parse_trend_row,
    synthesize_provider_event_id,
)
from .projector import project_corp_events, store_corp_raw

__all__ = [
    "CORP_BACKFILL_PHASES",
    "CalendarCorpEventRecord",
    "CalendarCorpRawRecord",
    "CorpBackfillRunner",
    "CorpBackfillSummary",
    "CorpCalendarFetcher",
    "CorpRunSummary",
    "CorpWindow",
    "EODHDAPIClient",
    "EODHDAuthMissing",
    "EODHDThrottled",
    "SUBTYPES",
    "SUBTYPES_BACKFILLABLE",
    "fetch_dividend_details",
    "parse_dividend_detail_row",
    "parse_dividend_row",
    "parse_earnings_row",
    "parse_ipo_row",
    "parse_split_row",
    "parse_trend_row",
    "plan_corp_windows",
    "project_corp_events",
    "store_corp_raw",
    "synthesize_provider_event_id",
]
