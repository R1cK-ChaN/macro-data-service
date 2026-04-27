"""Bank of England speeches calendar connector — issue #56 P1.

The BoE publishes its full speech archive at
``bankofengland.co.uk/sitemap/speeches`` as nested year/month
``<ul>`` blocks. Each leaf ``<li>`` carries an ``<a>`` whose ``href``
encodes the year and (since 2021) the month — older legacy entries
have a 2-segment slug with no month and are skipped in P1.

P1 ships a single anchor — ``BOE_SPEECHES`` — and projects every
parsed row as one calendar event with ``actual=NULL`` and
``event_time_precision='date'`` (anchored at the first day of the
listing month). Schedule-only mirrors the BOK / RBI deferral
pattern: speeches don't have a "value" to fill.

Day-of-month precision lives on each individual speech page; the
per-speech HTTP fan-out is deferred to a future slice if downstream
needs it.

Public surface mirrors the other ``<src>_api`` calendar packages.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_boe_speeches_calendar
from .indicators import INDICATOR_REGISTRY, BoeSpeechesIndicatorSpec
from .parser import (
    BOE_SPEECHES_BASE_URL,
    BOE_SPEECHES_SITEMAP_URL,
    BoeSpeech,
    BoeSpeechesEventRecord,
    BoeSpeechesRawRecord,
    BoeSpeechesSitemapParseError,
    PROVIDER,
    parse_speeches_sitemap,
    speech_to_records,
)
from .projector import project_events, store_raw

__all__ = [
    "BOE_SPEECHES_BASE_URL",
    "BOE_SPEECHES_SITEMAP_URL",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "BoeSpeech",
    "BoeSpeechesEventRecord",
    "BoeSpeechesIndicatorSpec",
    "BoeSpeechesRawRecord",
    "BoeSpeechesSitemapParseError",
    "fetch_boe_speeches_calendar",
    "parse_speeches_sitemap",
    "project_events",
    "speech_to_records",
    "store_raw",
]
