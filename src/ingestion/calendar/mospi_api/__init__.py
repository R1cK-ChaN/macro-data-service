"""MoSPI calendar connector — issue #54 P1 (India coverage).

The Ministry of Statistics and Programme Implementation publishes a
release calendar at ``mospi.gov.in/release-calendar``. The page is a
React SPA backed by a JSON endpoint at
``POST /api/release-calender/fetch-all-release-calender-Web``
(``{"lang": "en", "year": YYYY}``) returning ``data: [{id, title,
description, doc_url, year, month, day, week, level}]``. Each row
carries a scheduled release date and the indicator name in ``title``.

P1 ships three headline indicators — CPI, IIP, GDP — matched against
title substrings in the API response. The slice is **schedule-only**:
events publish with ``actual=NULL``. The MoSPI API does not expose
values directly; PDF parsing of the per-release document linked in
``description`` is deferred to P2. The parity harness still buckets
schedule-only MoSPI rows against TE rows for presence matching.

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — the P1 indicator whitelist.
- :class:`MoSPIReleaseAnnouncement` — one parsed schedule row.
- :func:`parse_release_calendar` — JSON → announcements.
- :func:`fetch_mospi_calendar` — orchestrates fetch + project.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_mospi_calendar
from .indicators import INDICATOR_REGISTRY, MoSPIIndicatorSpec
from .parser import (
    MOSPI_BASE_URL,
    MOSPI_RELEASE_CALENDAR_URL,
    MOSPI_RELEASE_TZ,
    MoSPICalendarEventRecord,
    MoSPICalendarParseError,
    MoSPICalendarRawRecord,
    MoSPIReleaseAnnouncement,
    PROVIDER,
    announcement_matches_spec,
    announcement_to_records,
    parse_release_calendar,
)
from .projector import project_events, store_raw

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "MOSPI_BASE_URL",
    "MOSPI_RELEASE_CALENDAR_URL",
    "MOSPI_RELEASE_TZ",
    "MoSPICalendarEventRecord",
    "MoSPICalendarParseError",
    "MoSPICalendarRawRecord",
    "MoSPIIndicatorSpec",
    "MoSPIReleaseAnnouncement",
    "PROVIDER",
    "announcement_matches_spec",
    "announcement_to_records",
    "fetch_mospi_calendar",
    "parse_release_calendar",
    "project_events",
    "store_raw",
]
