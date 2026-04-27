"""Bank of Japan speeches calendar connector — issue #56 P1.

The BoJ publishes one HTML archive per calendar year at
``boj.or.jp/en/about/press/koen_<YYYY>/index.htm``. Each row is a
three-cell ``<tr>`` carrying delivery date, ``FAMILY Given, Role``,
and the speech link. The parser keeps only rate-setting roles
(Governor + Deputy Governor + Member of the Policy Board) per the
issue's "rate-setters only" scope; Executive Directors / Counsellors
/ Officers are skipped.

P1 ships a single anchor — ``BOJ_SPEECHES`` — and projects every
parsed row as one calendar event with ``actual=NULL`` and
``event_time_precision='date'``.

Public surface mirrors the other ``<src>_api`` calendar packages.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_boj_speeches_calendar
from .indicators import INDICATOR_REGISTRY, BojSpeechesIndicatorSpec
from .parser import (
    BOJ_SPEECHES_BASE_URL,
    BOJ_SPEECHES_URL_TEMPLATE,
    BojSpeech,
    BojSpeechesArchiveParseError,
    BojSpeechesEventRecord,
    BojSpeechesRawRecord,
    PROVIDER,
    parse_speeches_archive,
    speech_to_records,
)
from .projector import project_events, store_raw

__all__ = [
    "BOJ_SPEECHES_BASE_URL",
    "BOJ_SPEECHES_URL_TEMPLATE",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "BojSpeech",
    "BojSpeechesArchiveParseError",
    "BojSpeechesEventRecord",
    "BojSpeechesIndicatorSpec",
    "BojSpeechesRawRecord",
    "fetch_boj_speeches_calendar",
    "parse_speeches_archive",
    "project_events",
    "speech_to_records",
    "store_raw",
]
