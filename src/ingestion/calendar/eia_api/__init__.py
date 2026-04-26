"""EIA weekly energy-stocks calendar connector — issue #50.

Pulls four high-volume weekly indicators from the open EIA v2 JSON
API into the ``cal_econ_raw`` / ``cal_econ_event`` two-lane schema:
US Crude Oil Stocks (excl. SPR), Gasoline Stocks, Distillate
Stocks, and Natural Gas Storage. Schedule and value land in one
pass — the API returns the period (week-ending date) alongside the
value, so a separate schedule scrape isn't needed.

Public surface:

- :data:`INDICATOR_REGISTRY` — canonical indicator → metadata map.
- :class:`EIAObservation` — one (indicator, period, value, unit).
- :func:`observation_to_records` — observation → (raw, event) tuple.
- :func:`fetch_eia_calendar` — orchestrates client.get_series +
  parser + projector. Dry-run returns the indicator plan.
- :func:`store_raw` / :func:`project_events` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_eia_calendar
from .indicators import EIAIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    EIACalendarEventRecord,
    EIACalendarRawRecord,
    EIAObservation,
    PROVIDER,
    observation_to_records,
)
from .projector import project_events, store_raw

__all__ = [
    "EIACalendarEventRecord",
    "EIACalendarRawRecord",
    "EIAIndicatorSpec",
    "EIAObservation",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "fetch_eia_calendar",
    "observation_to_records",
    "project_events",
    "store_raw",
]
