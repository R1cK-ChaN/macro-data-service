"""Banxico (Banco de México) calendar connector — issue #88 P1.

Banxico's monetary-policy decision history page at
``banxico.org.mx/publicaciones-y-prensa/anuncios-de-las-decisiones-de-politica-monetaria/anuncios-politica-monetaria-t.html``
is server-rendered HTML carrying every Junta de Gobierno decision back
to 2000. The connector filters to the modern Tasa Objetivo regime
(introduced 21 January 2008), parses both hold and change rows, and
ships value-bearing events for every decision via a cumulative walk
seeded from the oldest hold (the running rate is then re-anchored on
each subsequent hold as a sanity check).

The slice is **schedule + value**: ``actual`` carries the new Tasa
Objetivo, ``previous`` carries the prior decision's rate, and the
parity whitelist for ``(MX, BANXICO_RATE)`` joins on day one. Mirrors
the BCB / RBA value-bearing pattern from #84 / #53.

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — the P1 indicator whitelist.
- :class:`BanxicoRateDecision` — one parsed decision row.
- :func:`parse_decisions_history` — HTML → decisions.
- :func:`fetch_banxico_calendar` — orchestrates fetch + project.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_banxico_calendar
from .indicators import INDICATOR_REGISTRY, BanxicoIndicatorSpec
from .parser import (
    BANXICO_BASE_URL,
    BANXICO_DECISIONS_URL,
    BANXICO_RELEASE_TIME,
    BANXICO_RELEASE_TZ,
    BanxicoCalendarEventRecord,
    BanxicoCalendarRawRecord,
    BanxicoDecisionsParseError,
    BanxicoRateDecision,
    PROVIDER,
    decision_to_records,
    parse_decisions_history,
)
from .projector import project_events, store_raw

__all__ = [
    "BANXICO_BASE_URL",
    "BANXICO_DECISIONS_URL",
    "BANXICO_RELEASE_TIME",
    "BANXICO_RELEASE_TZ",
    "BanxicoCalendarEventRecord",
    "BanxicoCalendarRawRecord",
    "BanxicoDecisionsParseError",
    "BanxicoIndicatorSpec",
    "BanxicoRateDecision",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "decision_to_records",
    "fetch_banxico_calendar",
    "parse_decisions_history",
    "project_events",
    "store_raw",
]
