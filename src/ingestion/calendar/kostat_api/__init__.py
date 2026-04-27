"""KOSTAT (Statistics Korea) calendar connector — issue #55 P1 (Korea coverage).

Statistics Korea — the agency historically branded "KOSTAT" and now
sitting under the Ministry of Data and Statistics (MoDS) — publishes
its annual release schedule at
``https://mods.go.kr/menu.es?mid=a20301000000``. The page is plain
server-rendered HTML with one ``<table>`` per scheduled year. Each row
carries a publication-month abbreviation, a release title, the
release date as ``"Mon. DD (Day.)"`` text, and the responsible
production division.

The page heading (``"<h3>{year} Schedule</h3>"``) bounds every row's
publication year — the date column itself omits the year. The
reference period is parsed from the title (``"... in <Month> <Year>"``).

P1 ships three headline indicators — CPI, INDUSTRIAL_PRODUCTION,
UNEMPLOYMENT_RATE — matched against title substrings in the table
rows. The slice is **schedule-only**: events publish with
``actual=NULL``. Per-release press-release values live behind a
separate news-list URL surfaced via a JS handler; the value-side
scrape is deferred to P2 (mirrors the ABS / MoSPI schedule-only
pattern).

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — the P1 indicator whitelist.
- :class:`KOSTATReleaseAnnouncement` — one parsed schedule row.
- :func:`parse_release_schedule` — HTML → announcements.
- :func:`fetch_kostat_calendar` — orchestrates fetch + project.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_kostat_calendar
from .indicators import INDICATOR_REGISTRY, KOSTATIndicatorSpec
from .parser import (
    KOSTAT_BASE_URL,
    KOSTAT_RELEASE_SCHEDULE_URL,
    KOSTAT_RELEASE_TZ,
    KOSTATCalendarEventRecord,
    KOSTATCalendarParseError,
    KOSTATCalendarRawRecord,
    KOSTATReleaseAnnouncement,
    PROVIDER,
    announcement_matches_spec,
    announcement_to_records,
    parse_release_schedule,
)
from .projector import project_events, store_raw

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "KOSTAT_BASE_URL",
    "KOSTAT_RELEASE_SCHEDULE_URL",
    "KOSTAT_RELEASE_TZ",
    "KOSTATCalendarEventRecord",
    "KOSTATCalendarParseError",
    "KOSTATCalendarRawRecord",
    "KOSTATIndicatorSpec",
    "KOSTATReleaseAnnouncement",
    "PROVIDER",
    "announcement_matches_spec",
    "announcement_to_records",
    "fetch_kostat_calendar",
    "parse_release_schedule",
    "project_events",
    "store_raw",
]
