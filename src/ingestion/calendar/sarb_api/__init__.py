"""SARB (South African Reserve Bank) calendar connector — issue #90 P1.

SARB exposes its repo-rate change history as a small JSON timeseries
served by the public ``custom.resbank.co.za/SarbWebApi`` indicator
service. The endpoint returns one row per **rate-change** decision
(holds are absent on this surface), each carrying the announcement
period and the new repo rate inline. The slice ships value-bearing
events for every rate-change row — TCMB-style coverage rather than
the full BCB / Banxico every-decision pattern.

The parity whitelist stays empty in P1 for the same compounding
reasons as TCMB: (a) the JSON ``Period`` is the rate's effective
date, which is one business day after the SARB MPC announcement
under the modern Thursday-meeting cadence — wiring ``(ZA, SARB_RATE)``
would generate a daily off-by-one MissingRelease alert; (b) hold
decisions are absent from this surface, so TE's hold-meeting rows
have no agency counterpart and would compound the alert. The future
P2 slice that ingests per-meeting MPC statement PDFs gives us
authoritative announcement dates AND hold-decision coverage; the
whitelist joins then.

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — the P1 indicator whitelist.
- :class:`SARBRateDecision` — one parsed decision row.
- :func:`parse_repo_rate_history` — JSON → decisions.
- :func:`fetch_sarb_calendar` — orchestrates fetch + project.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_sarb_calendar
from .indicators import INDICATOR_REGISTRY, SARBIndicatorSpec
from .parser import (
    PROVIDER,
    SARB_BASE_URL,
    SARB_PUBLIC_HISTORY_URL,
    SARB_RATE_HISTORY_URL,
    SARB_RELEASE_TIME,
    SARB_RELEASE_TZ,
    SARBCalendarEventRecord,
    SARBCalendarRawRecord,
    SARBRateDecision,
    SARBRateHistoryParseError,
    decision_to_records,
    parse_repo_rate_history,
)
from .projector import project_events, store_raw

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "SARB_BASE_URL",
    "SARB_PUBLIC_HISTORY_URL",
    "SARB_RATE_HISTORY_URL",
    "SARB_RELEASE_TIME",
    "SARB_RELEASE_TZ",
    "SARBCalendarEventRecord",
    "SARBCalendarRawRecord",
    "SARBIndicatorSpec",
    "SARBRateDecision",
    "SARBRateHistoryParseError",
    "decision_to_records",
    "fetch_sarb_calendar",
    "parse_repo_rate_history",
    "project_events",
    "store_raw",
]
