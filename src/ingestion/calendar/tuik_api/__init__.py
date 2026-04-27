"""TÜİK (Türkiye İstatistik Kurumu) calendar connector — issue #86 P1.

The Turkish national release calendar is exposed as a JSON service at
``www.tuik.gov.tr/Kurumsal/GetYillikHaberBulteniListesi?yil=YYYY``.
The endpoint returns a unified list — published (``yayindaOlanlarList``)
+ upcoming (``yayindaOlmayanlarList``) — covering every Turkish
official-statistics agency for the requested calendar year (TCMB,
BDDK, SPK, TÜİK, …, ~4000 rows per year). Each row carries the
responsible institution short code (``sorumluKisaAd``), release title
(``adi``), Istanbul-local datetime (``gTarih``), reference period
text (``donemi``), and a stable bulletin id (``id``).

The TÜİK connector filters to ``sorumluKisaAd == 'TÜİK'`` and matches
each row's ``adi`` against an indicator allowlist (CPI / PPI /
Industrial Production / Unemployment / GDP / Foreign Trade). The
slice is **schedule-only**: events publish with ``actual=NULL``. The
linked press-release page (``link``) carries the value side; that
detail-page scrape is deferred to P2 (mirrors the IBGE / KOSTAT /
MoSPI schedule-only pattern).

Public surface mirrors the other ``<src>_api`` calendar packages.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_tuik_calendar
from .indicators import INDICATOR_REGISTRY, TUIKIndicatorSpec
from .parser import (
    PROVIDER,
    TUIK_BASE_URL,
    TUIK_CALENDAR_URL_TEMPLATE,
    TUIK_RELEASE_TZ,
    TUIK_RESPONSIBLE_CODE,
    TUIKCalendarEventRecord,
    TUIKCalendarParseError,
    TUIKCalendarRawRecord,
    TUIKReleaseAnnouncement,
    announcement_matches_spec,
    announcement_to_records,
    parse_release_calendar,
)
from .projector import project_events, store_raw

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "TUIK_BASE_URL",
    "TUIK_CALENDAR_URL_TEMPLATE",
    "TUIK_RELEASE_TZ",
    "TUIK_RESPONSIBLE_CODE",
    "TUIKCalendarEventRecord",
    "TUIKCalendarParseError",
    "TUIKCalendarRawRecord",
    "TUIKIndicatorSpec",
    "TUIKReleaseAnnouncement",
    "announcement_matches_spec",
    "announcement_to_records",
    "fetch_tuik_calendar",
    "parse_release_calendar",
    "project_events",
    "store_raw",
]
