"""IBGE (Instituto Brasileiro de Geografia e Estatística) calendar connector — issue #84 P1.

The IBGE release calendar at
``ibge.gov.br/calendario/mensal.html?mes=N&ano=YYYY`` is server-rendered
HTML carrying every scheduled and recently-published statistical
release for the requested month. Each event row pairs a
``data-divulgacao`` ISO-8601 publication timestamp (UTC−3 baked in)
with a product-anchored title link and a ``Período de referência``
line. The fetcher walks a rolling forward window (current month + next
three) so a daily sweep keeps the calendar's lookahead fresh.

P1 ships five headline indicators — IPCA, IPCA-15, Industrial
Production (PIM-PF Brasil), Unemployment Rate (PNAD-Contínua Mensal),
and GDP (Sistema de Contas Nacionais Trimestrais). The slice is
**schedule-only**: events publish with ``actual=NULL``. Per-release
press-release pages reachable from the calendar carry the value side;
parsing them is deferred to P2 (mirrors the ABS / KOSTAT schedule-only
pattern).

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — the P1 indicator whitelist.
- :class:`IBGEReleaseAnnouncement` — one parsed event row.
- :func:`parse_release_calendar` — HTML → announcements.
- :func:`fetch_ibge_calendar` — orchestrates fetch + project across
  the rolling month window.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_ibge_calendar
from .indicators import INDICATOR_REGISTRY, IBGEIndicatorSpec
from .parser import (
    IBGE_BASE_URL,
    IBGE_CALENDAR_URL_TEMPLATE,
    IBGE_RELEASE_TZ,
    IBGECalendarEventRecord,
    IBGECalendarParseError,
    IBGECalendarRawRecord,
    IBGEReleaseAnnouncement,
    PROVIDER,
    announcement_matches_spec,
    announcement_to_records,
    parse_release_calendar,
)
from .projector import project_events, store_raw

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "IBGE_BASE_URL",
    "IBGE_CALENDAR_URL_TEMPLATE",
    "IBGE_RELEASE_TZ",
    "IBGECalendarEventRecord",
    "IBGECalendarParseError",
    "IBGECalendarRawRecord",
    "IBGEIndicatorSpec",
    "IBGEReleaseAnnouncement",
    "PROVIDER",
    "announcement_matches_spec",
    "announcement_to_records",
    "fetch_ibge_calendar",
    "parse_release_calendar",
    "project_events",
    "store_raw",
]
