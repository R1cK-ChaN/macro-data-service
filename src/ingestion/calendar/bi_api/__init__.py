"""Bank Indonesia (BI) calendar connector — issue #92 P1.

Bank Indonesia exposes the BI Board of Governors meeting history at
``bi.go.id/en/statistik/indikator/bi-rate.aspx`` as server-rendered
SharePoint HTML carrying every BI Board of Governors meeting (every
meeting, change OR hold). Each row pairs the announcement date
(``"DD Month YYYY"`` form, English-locale) with the absolute BI-Rate
(``"X.YZ %"``) and a press-release link. The slice ships value-bearing
rows for every meeting on day one — RBA / BCB / Banxico-style
coverage rather than the TCMB / SARB rate-change-only deferral.

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — the P1 indicator whitelist.
- :class:`BIRateDecision` — one parsed decision row.
- :func:`parse_rate_history` — HTML → decisions.
- :func:`fetch_bi_calendar` — orchestrates fetch + project.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_bi_calendar
from .indicators import BIIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    BI_BASE_URL,
    BI_RATE_HISTORY_URL,
    BI_RELEASE_TIME,
    BI_RELEASE_TZ,
    BICalendarEventRecord,
    BICalendarRawRecord,
    BIRateDecision,
    BIRateHistoryParseError,
    PROVIDER,
    decision_to_records,
    parse_rate_history,
)
from .projector import project_events, store_raw

__all__ = [
    "BI_BASE_URL",
    "BI_RATE_HISTORY_URL",
    "BI_RELEASE_TIME",
    "BI_RELEASE_TZ",
    "BICalendarEventRecord",
    "BICalendarRawRecord",
    "BIIndicatorSpec",
    "BIRateDecision",
    "BIRateHistoryParseError",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "decision_to_records",
    "fetch_bi_calendar",
    "parse_rate_history",
    "project_events",
    "store_raw",
]
