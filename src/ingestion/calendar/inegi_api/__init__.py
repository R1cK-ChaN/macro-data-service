"""INEGI (Instituto Nacional de Estadística y Geografía) calendar connector — issue #88 P1.

INEGI's release calendar is a SPA whose ``Calendar`` view is populated
by a POST to
``inegi.org.mx/app/api/saladeprensa/api/saladeprensa/ObtenerFechasTabla/v3``.
The fetcher walks one POST per **distinct** ``idPrograma`` referenced
by the active indicator set, posting a 90-day-back / 365-day-forward
date window. Indicators that share an idPrograma (CPI / INPC_15 both
key on 2353) are post-filtered against the same response by
:func:`announcement_matches_spec`.

P1 ships six headline indicators — CPI (INPC mensual), INPC_15
(quincenal mid-month preview), GDP (PIB Trimestral), Industrial
Production (IMAI), Unemployment Rate (ENOE mensual), and Trade Balance
(Balanza Comercial — Información oportuna). The slice is
**schedule-only**: events publish with ``actual=NULL``. Per-release
boletín PDFs reachable from each row's ``comunicadoEsUrlPdf`` carry the
value side; per-indicator value extraction is deferred to P2 alongside
that detail-page scrape.

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — the P1 indicator whitelist.
- :class:`INEGIReleaseAnnouncement` — one parsed event row.
- :func:`parse_release_calendar` — JSON → announcements.
- :func:`fetch_inegi_calendar` — orchestrates fetch + project across
  the rolling date window.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_inegi_calendar
from .indicators import INDICATOR_REGISTRY, INEGIIndicatorSpec
from .parser import (
    INEGI_BASE_URL,
    INEGI_CALENDAR_API_URL,
    INEGI_CALENDAR_PUBLIC_URL,
    INEGI_RELEASE_TZ,
    INEGICalendarEventRecord,
    INEGICalendarParseError,
    INEGICalendarRawRecord,
    INEGIReleaseAnnouncement,
    PROVIDER,
    announcement_matches_spec,
    announcement_to_records,
    parse_release_calendar,
)
from .projector import project_events, store_raw

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "INEGI_BASE_URL",
    "INEGI_CALENDAR_API_URL",
    "INEGI_CALENDAR_PUBLIC_URL",
    "INEGI_RELEASE_TZ",
    "INEGICalendarEventRecord",
    "INEGICalendarParseError",
    "INEGICalendarRawRecord",
    "INEGIIndicatorSpec",
    "INEGIReleaseAnnouncement",
    "PROVIDER",
    "announcement_matches_spec",
    "announcement_to_records",
    "fetch_inegi_calendar",
    "parse_release_calendar",
    "project_events",
    "store_raw",
]
