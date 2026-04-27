"""Bank of Canada calendar connector — issue #52 P1 (Canada coverage).

The Bank of Canada exposes the daily Target for the Overnight
Rate (series ``V39079``) via the Valet API at
``bankofcanada.ca/valet/observations/V39079/json``. Each
observation pairs a date with the daily target rate; rate-change
days are the BoC Governing Council policy decisions the
connector projects into ``cal_econ_event``.

P1 ships a single anchor — ``BOC_RATE`` — and treats the Valet
observations list as a combined schedule + value source. Each
detected rate change produces one calendar event anchored on the
BoC *announcement* day (09:45 ET) — Valet's ``d`` is the first
business day at the new rate, which lags the announcement by one
business day; both dates are exposed on the parsed decision so
the projector publishes the announcement timestamp while the
audit payload carries the Valet effective date verbatim.

Hold (no-change) decisions are absent from this signal — the
daily series shows the same value before and after a hold; the
fixed-announcement-dates page is a follow-up slice.

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — single ``BOC_RATE`` spec.
- :class:`BoCRateDecision` — one (date, rate, previous_rate) decision row.
- :func:`parse_overnight_rate_observations` — JSON → list of decisions.
- :func:`fetch_boc_calendar` — orchestrates fetch + project.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_boc_calendar
from .indicators import BoCIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    BOC_OVERNIGHT_RATE_SERIES,
    BOC_RELEASE_TIME,
    BOC_RELEASE_TZ,
    BOC_VALET_BASE_URL,
    BOC_VALET_URL,
    BoCCalendarEventRecord,
    BoCCalendarRawRecord,
    BoCRateDecision,
    BoCValetParseError,
    PROVIDER,
    decision_to_records,
    parse_overnight_rate_observations,
)
from .projector import project_events, store_raw

__all__ = [
    "BOC_OVERNIGHT_RATE_SERIES",
    "BOC_RELEASE_TIME",
    "BOC_RELEASE_TZ",
    "BOC_VALET_BASE_URL",
    "BOC_VALET_URL",
    "BoCCalendarEventRecord",
    "BoCCalendarRawRecord",
    "BoCIndicatorSpec",
    "BoCRateDecision",
    "BoCValetParseError",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "decision_to_records",
    "fetch_boc_calendar",
    "parse_overnight_rate_observations",
    "project_events",
    "store_raw",
]
