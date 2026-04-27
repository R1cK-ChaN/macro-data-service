"""Statistics Canada calendar connector — issue #52 P1 (Canada coverage).

Statistics Canada exposes every published series via the Web
Data Service (WDS) JSON API at
``www150.statcan.gc.ca/t1/wds/rest/``. The
``getDataFromVectorsAndLatestNPeriods`` endpoint accepts a list
of ``(vectorId, latestN)`` pairs and returns the latest
observations per vector, with each observation carrying both the
reference period (``refPer``) and the publication wall clock
(``releaseTime``, Eastern Time).

Schedule and value land together: each fetch reads the latest
observation per indicator and projects both the release timestamp
(derived from ``releaseTime`` against the canonical 08:30
``America/Toronto`` release convention) and the headline value
into ``cal_econ_event``.

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — the P1 indicator whitelist.
- :class:`StatCanValueObservation` — one (indicator, release, value).
- :func:`parse_vector_response` — JSON → observation.
- :func:`fetch_statcan_calendar` — orchestrates per-indicator fetch.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_statcan_calendar
from .indicators import INDICATOR_REGISTRY, StatCanIndicatorSpec
from .parser import (
    PROVIDER,
    STATCAN_RELEASE_TIME_DEFAULT,
    STATCAN_RELEASE_TZ,
    STATCAN_WDS_BASE_URL,
    StatCanCalendarEventRecord,
    StatCanCalendarRawRecord,
    StatCanValueObservation,
    StatCanWDSParseError,
    parse_vector_response,
    value_observation_to_records,
)
from .projector import project_events, store_raw

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "STATCAN_RELEASE_TIME_DEFAULT",
    "STATCAN_RELEASE_TZ",
    "STATCAN_WDS_BASE_URL",
    "StatCanCalendarEventRecord",
    "StatCanCalendarRawRecord",
    "StatCanIndicatorSpec",
    "StatCanValueObservation",
    "StatCanWDSParseError",
    "fetch_statcan_calendar",
    "parse_vector_response",
    "project_events",
    "store_raw",
    "value_observation_to_records",
]
