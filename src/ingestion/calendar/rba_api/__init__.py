"""Reserve Bank of Australia calendar connector — issue #53 P1.

The RBA exposes the Monetary Policy Board's cash-rate-target
decisions in a single HTML table at
``rba.gov.au/statistics/cash-rate/``. Each row pairs the
announcement date with the new target rate and a signed change
column; hold (no-change) decisions are present as ``0.00`` rows.

P1 ships a single anchor — ``RBA_RATE`` — and projects every MPB
announcement (change OR hold) as one calendar event at 14:30
AEST/AEDT (the RBA's documented post-meeting publication time).
This is a tighter coverage shape than the BoC Valet pattern, which
only signals on rate changes — the full RBA table makes the parity
whitelist achievable in P1 without a separate fixed-announcement-
dates feed.

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — single ``RBA_RATE`` spec.
- :class:`RBARateDecision` — one (date, rate, change) decision row.
- :func:`parse_cash_rate_table` — HTML → list of decisions.
- :func:`fetch_rba_calendar` — orchestrates fetch + project.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_rba_calendar
from .indicators import INDICATOR_REGISTRY, RBAIndicatorSpec
from .parser import (
    PROVIDER,
    RBA_BASE_URL,
    RBA_CASH_RATE_URL,
    RBA_RELEASE_TIME,
    RBA_RELEASE_TZ,
    RBACalendarEventRecord,
    RBACalendarRawRecord,
    RBACashRateParseError,
    RBARateDecision,
    decision_to_records,
    parse_cash_rate_table,
)
from .projector import project_events, store_raw

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "RBA_BASE_URL",
    "RBA_CASH_RATE_URL",
    "RBA_RELEASE_TIME",
    "RBA_RELEASE_TZ",
    "RBACalendarEventRecord",
    "RBACalendarRawRecord",
    "RBACashRateParseError",
    "RBAIndicatorSpec",
    "RBARateDecision",
    "decision_to_records",
    "fetch_rba_calendar",
    "parse_cash_rate_table",
    "project_events",
    "store_raw",
]
