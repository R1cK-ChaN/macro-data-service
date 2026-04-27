"""Banco Central do Brasil calendar connector — issue #84 P1.

The BCB exposes the full Copom (Comitê de Política Monetária) decision
history as a single JSON service at
``bcb.gov.br/api/servico/sitebcb/historicotaxasjuros``. Each element
of the response carries a meeting number, announcement date,
effective period, the new target Selic rate, bias, and flags for
extraordinary / monocratic-presidential decisions. Hold (no-change)
decisions are present in the same shape as moves — the parity
whitelist is achievable in P1 without a separate fixed-meeting feed
(unlike the BoC pattern).

P1 ships a single anchor — ``BCB_RATE`` — and projects every Copom
announcement (change OR hold, regular OR extraordinary) as one
calendar event at 18:30 BRT (the BCB's documented post-meeting
publication time). Brazil dropped DST in 2019, so the BRT offset is
constant for every contemporary release; ``parse_scheduled_release_time``
against ``America/Sao_Paulo`` resolves the historical 2008–2018 DST
window for backfill rows.

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — single ``BCB_RATE`` spec.
- :class:`BCBRateDecision` — one (date, rate, change) decision row.
- :func:`parse_copom_history` — JSON → list of decisions.
- :func:`fetch_bcb_calendar` — orchestrates fetch + project.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_bcb_calendar
from .indicators import INDICATOR_REGISTRY, BCBIndicatorSpec
from .parser import (
    BCB_BASE_URL,
    BCB_COPOM_HISTORY_URL,
    BCB_COPOM_PUBLIC_URL,
    BCB_RELEASE_TIME,
    BCB_RELEASE_TZ,
    BCBCalendarEventRecord,
    BCBCalendarRawRecord,
    BCBCopomParseError,
    BCBRateDecision,
    PROVIDER,
    decision_to_records,
    parse_copom_history,
)
from .projector import project_events, store_raw

__all__ = [
    "BCB_BASE_URL",
    "BCB_COPOM_HISTORY_URL",
    "BCB_COPOM_PUBLIC_URL",
    "BCB_RELEASE_TIME",
    "BCB_RELEASE_TZ",
    "BCBCalendarEventRecord",
    "BCBCalendarRawRecord",
    "BCBCopomParseError",
    "BCBIndicatorSpec",
    "BCBRateDecision",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "decision_to_records",
    "fetch_bcb_calendar",
    "parse_copom_history",
    "project_events",
    "store_raw",
]
