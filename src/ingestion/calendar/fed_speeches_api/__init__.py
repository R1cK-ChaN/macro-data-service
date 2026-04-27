"""Federal Reserve speeches calendar connector — issue #56 P1.

The Fed publishes one HTML archive per calendar year at
``federalreserve.gov/newsevents/speech/<YYYY>-speeches.htm`` listing
every Board / Vice Chair / Chair speech with delivery date, title,
speaker, and venue. Regional Reserve Bank president speeches live on
a different surface and are out of scope for the rate-setter focus
this slice ships.

P1 ships a single anchor — ``FED_SPEECHES`` — and projects every
parsed row as one calendar event with ``actual=NULL`` and
``event_time_precision='date'`` (the listing publishes the calendar
day, not a wall-clock delivery time). Schedule-only mirrors the
BOK / RBI deferral pattern: speeches don't have a "value" to fill;
they serve as event anchors for downstream research / impact
analysis.

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — single ``FED_SPEECHES`` spec.
- :class:`FedSpeech` — one parsed entry from the archive page.
- :func:`parse_speeches_archive` — HTML → list of speeches.
- :func:`fetch_fed_speeches_calendar` — orchestrates fetch + project.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_fed_speeches_calendar
from .indicators import INDICATOR_REGISTRY, FedSpeechesIndicatorSpec
from .parser import (
    FED_SPEECHES_BASE_URL,
    FED_SPEECHES_URL_TEMPLATE,
    FedSpeech,
    FedSpeechesArchiveParseError,
    FedSpeechesEventRecord,
    FedSpeechesRawRecord,
    PROVIDER,
    parse_speeches_archive,
    speech_to_records,
)
from .projector import project_events, store_raw

__all__ = [
    "FED_SPEECHES_BASE_URL",
    "FED_SPEECHES_URL_TEMPLATE",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "FedSpeech",
    "FedSpeechesArchiveParseError",
    "FedSpeechesEventRecord",
    "FedSpeechesIndicatorSpec",
    "FedSpeechesRawRecord",
    "fetch_fed_speeches_calendar",
    "parse_speeches_archive",
    "project_events",
    "speech_to_records",
    "store_raw",
]
