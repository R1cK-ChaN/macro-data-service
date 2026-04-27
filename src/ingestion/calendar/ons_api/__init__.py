"""ONS calendar connector — issue #51 P1 (UK coverage).

Office for National Statistics releases the bulk of UK headline
indicators (CPI, GDP, Unemployment Rate, Retail Sales, Trade
Balance) and exposes each one as a clean JSON timeseries at
``ons.gov.uk/<topic>/timeseries/<id>/<dataset>/data``. The JSON
carries the full history plus an ``updateDate`` field on each
observation pinpointing when the value was published.

Schedule and value land together: each fetch reads the latest
observation per indicator and projects both the release timestamp
(derived from ``updateDate`` against the canonical 07:00 UK
release convention) and the headline value into
``cal_econ_event``.

Public surface:

- :data:`INDICATOR_REGISTRY` — the P1 indicator whitelist.
- :class:`ONSValueObservation` — one (indicator, release, value).
- :func:`parse_timeseries_json` — JSON → observation.
- :func:`fetch_ons_calendar` — orchestrates per-indicator fetch.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_ons_calendar
from .indicators import INDICATOR_REGISTRY, ONSIndicatorSpec
from .parser import (
    ONS_RELEASE_TIME,
    ONS_RELEASE_TZ,
    ONSCalendarEventRecord,
    ONSCalendarRawRecord,
    ONSTimeseriesParseError,
    ONSValueObservation,
    PROVIDER,
    parse_timeseries_json,
    value_observation_to_records,
)
from .projector import project_events, store_raw

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "ONS_RELEASE_TIME",
    "ONS_RELEASE_TZ",
    "ONSCalendarEventRecord",
    "ONSCalendarRawRecord",
    "ONSIndicatorSpec",
    "ONSTimeseriesParseError",
    "ONSValueObservation",
    "PROVIDER",
    "fetch_ons_calendar",
    "parse_timeseries_json",
    "project_events",
    "store_raw",
    "value_observation_to_records",
]
