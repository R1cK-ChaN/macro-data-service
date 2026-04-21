"""BEA calendar connector (issue #9 P2 scaffold).

Projects BEA REST API observations into the
``cal_econ_raw`` / ``cal_econ_event`` two-lane calendar schema.

Scope for P2:

- ``GDP`` — NIPA ``T10101`` line 1 (Real Gross Domestic Product,
  Percent Change From Preceding Period, Quarterly SAAR).
- ``PERSONAL_INCOME`` — NIPA ``T20600`` line 1 (Personal Income,
  Monthly — the headline aggregate on BEA's Personal Income and
  Outlays release).

Both coordinates are unambiguously line-1 of a well-known NIPA table.
PCE ships on the same release as Personal Income but at a
not-yet-verified ``(table, line)`` coordinate — deferred to P2a so
the live BEA probe can confirm the exact mapping before the connector
labels rows as PCE. Trade Balance (ITA dataset) and Corporate Profits
(NIPA) land as later slices — ITA uses a different parameter surface
than NIPA, so the fetcher needs per-dataset shape awareness before it
widens.

Public surface:

- :data:`INDICATOR_REGISTRY` — BEA series-id → metadata map.
- :func:`parse_observation` — :class:`BEAObservation` → record tuple.
- :func:`store_raw` / :func:`project_events` — idempotent writers.
- :func:`fetch_bea_calendar` — whitelist iteration + BEA HTTP call
  + record projection. Nothing auto-runs; callers pass a client.

The HTTP transport layer is the existing
:class:`ingestion.timeseries.scrapers.bea.BEAClient` — reused verbatim
so auth, rate limiting (~100 req/min), and error handling are already
production-tested. This package adds only the calendar-shaped
projection on top.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_bea_calendar
from .indicators import BEAIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    BEACalendarEventRecord,
    BEACalendarRawRecord,
    parse_observation,
)
from .projector import project_events, store_raw

__all__ = [
    "BEACalendarEventRecord",
    "BEACalendarRawRecord",
    "BEAIndicatorSpec",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "fetch_bea_calendar",
    "parse_observation",
    "project_events",
    "store_raw",
]
