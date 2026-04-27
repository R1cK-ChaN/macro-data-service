"""European Central Bank speeches calendar connector — issue #56 P1.

The ECB exposes the official Executive Board speeches dataset as a
single pipe-separated CSV at
``ecb.europa.eu/press/key/shared/data/all_ECB_speeches.csv`` (per
the downloads page documentation). The CSV refreshes monthly,
covers Executive Board members only, and carries
``date|speakers|title|subtitle|contents`` columns.

P1 ships a single anchor — ``ECB_SPEECHES`` — and projects every
parsed row as one calendar event with ``actual=NULL`` and
``event_time_precision='date'`` (the CSV publishes the calendar
day, not a wall-clock delivery time). Schedule-only mirrors the
BOK / RBI deferral pattern; the CSV's ``contents`` transcript
column is collapsed to a boolean ``has_contents`` flag in the
raw payload — a future transcript-NLP slice would re-fetch the
CSV (single GET, monthly refresh) when needed.

Public surface mirrors the other ``<src>_api`` calendar packages.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_ecb_speeches_calendar
from .indicators import INDICATOR_REGISTRY, EcbSpeechesIndicatorSpec
from .parser import (
    ECB_SPEECHES_BASE_URL,
    ECB_SPEECHES_CSV_URL,
    EcbSpeech,
    EcbSpeechesCsvParseError,
    EcbSpeechesEventRecord,
    EcbSpeechesRawRecord,
    PROVIDER,
    parse_speeches_csv,
    speech_to_records,
)
from .projector import project_events, store_raw

__all__ = [
    "ECB_SPEECHES_BASE_URL",
    "ECB_SPEECHES_CSV_URL",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "EcbSpeech",
    "EcbSpeechesCsvParseError",
    "EcbSpeechesEventRecord",
    "EcbSpeechesIndicatorSpec",
    "EcbSpeechesRawRecord",
    "fetch_ecb_speeches_calendar",
    "parse_speeches_csv",
    "project_events",
    "speech_to_records",
    "store_raw",
]
