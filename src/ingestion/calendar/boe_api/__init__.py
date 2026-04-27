"""Bank of England calendar connector — issue #51 P1 (UK coverage).

The Bank of England publishes its full Bank Rate (Official Bank Rate)
history at ``/boeapps/database/Bank-Rate.asp`` — one row per MPC
rate-change decision with ``(date_changed, rate)`` columns. Each
row corresponds to a Monetary Policy Committee announcement; the
MPC announces decisions at 12:00 UK time on the published meeting
day.

P1 ships a single anchor — ``BOE_RATE`` — and treats the Bank
Rate history page as a combined schedule + value source. Each
parsed row produces one calendar event whose ``actual`` carries
the rate that took effect on the announcement day. Hold decisions
(no rate change) are absent from this page; covering them is a
future-slice task that needs the MPC summary feed.

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — single ``BOE_RATE`` spec.
- :class:`BoEMpcDecision` — one (date, rate) decision row.
- :func:`parse_bank_rate_html` — HTML → list of decisions.
- :func:`fetch_boe_calendar` — orchestrates fetch + project.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_boe_calendar
from .indicators import BoEIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    BOE_BANK_RATE_URL,
    BOE_RELEASE_TIME,
    BOE_RELEASE_TZ,
    BoECalendarEventRecord,
    BoECalendarRawRecord,
    BoEMpcDecision,
    BoERatePageParseError,
    PROVIDER,
    decision_to_records,
    parse_bank_rate_html,
)
from .projector import project_events, store_raw

__all__ = [
    "BOE_BANK_RATE_URL",
    "BOE_RELEASE_TIME",
    "BOE_RELEASE_TZ",
    "BoECalendarEventRecord",
    "BoECalendarRawRecord",
    "BoEIndicatorSpec",
    "BoEMpcDecision",
    "BoERatePageParseError",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "decision_to_records",
    "fetch_boe_calendar",
    "parse_bank_rate_html",
    "project_events",
    "store_raw",
]
