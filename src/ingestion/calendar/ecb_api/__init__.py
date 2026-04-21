"""ECB calendar connector (issue #9 P3 scaffold).

Projects ECB Data Portal SDMX observations into the
``cal_econ_raw`` / ``cal_econ_event`` two-lane calendar schema.

Scope for P3:

- ``ECB_MRO`` — Main Refinancing Operations fixed rate
  (``FM.B.U2.EUR.4F.KR.MRR_FR.LEV``).
- ``ECB_DFR`` — Deposit Facility Rate
  (``FM.B.U2.EUR.4F.KR.DFR.LEV``).
- ``ECB_MLF`` — Marginal Lending Facility Rate
  (``FM.B.U2.EUR.4F.KR.MLFR.LEV``).

All three ship together because the Governing Council moves them
simultaneously at each policy meeting — a trader watching one watches
all. Economic Bulletin dates and monetary-policy press conference
timing are calendar-only (no value-bearing feed); they land in the
P3a slice that scrapes ``ecb.europa.eu/press/calendars/``.

Public surface:

- :data:`INDICATOR_REGISTRY` — ECB SDMX series id → metadata map.
- :func:`parse_observation` — :class:`SDMXObservation` → record tuple.
- :func:`store_raw` / :func:`project_events` — idempotent writers.
- :func:`fetch_ecb_calendar` — whitelist iteration + ECB HTTP call
  + record projection. Nothing auto-runs; callers pass a client.

The HTTP transport layer is the existing
:class:`ingestion.timeseries.sdmx.providers.ecb.ECBClient` (points at
``data-api.ecb.europa.eu/service``; no auth required). This package
adds only the calendar-shaped projection on top.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_ecb_calendar
from .indicators import ECBIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    ECBCalendarEventRecord,
    ECBCalendarRawRecord,
    parse_observation,
)
from .projector import project_events, store_raw

__all__ = [
    "ECBCalendarEventRecord",
    "ECBCalendarRawRecord",
    "ECBIndicatorSpec",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "fetch_ecb_calendar",
    "parse_observation",
    "project_events",
    "store_raw",
]
