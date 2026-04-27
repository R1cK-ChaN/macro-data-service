"""ABS calendar connector — issue #53 P1 (Australia coverage).

Australian Bureau of Statistics publishes a release calendar at
``abs.gov.au/release-calendar/latest-releases`` listing the most
recent ~14 days of statistical releases. Each release card carries
a ``<time datetime>`` UTC publication timestamp, a headline link
whose URL slug encodes the reference period (``feb-2026`` /
``dec-2025``), and a ``release__subtitle`` topic label.

P1 ships three indicators — CPI, Unemployment Rate (Labour Force
headline), GDP (National Accounts) — matched against release-card
URL prefixes. The slice is **schedule-only**: events publish with
``actual=NULL``. ABS does expose values via SDMX at
``api.data.abs.gov.au`` but the per-indicator key parsing
(quarterly vs monthly reference-date alignment, headcount vs rate
distinction on Labour Force) is deferred to P2; the parity harness
still buckets schedule-only ABS rows against TE rows for presence
matching, which closes the immediate parity gap.

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — the P1 indicator whitelist.
- :class:`ABSReleaseAnnouncement` — one parsed release card.
- :func:`parse_release_calendar` — HTML → announcements.
- :func:`fetch_abs_calendar` — orchestrates fetch + project.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_abs_calendar
from .indicators import ABSIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    ABS_BASE_URL,
    ABS_RELEASE_CALENDAR_URL,
    ABS_RELEASE_TIME_LOCAL,
    ABS_RELEASE_TZ,
    ABSCalendarEventRecord,
    ABSCalendarParseError,
    ABSCalendarRawRecord,
    ABSReleaseAnnouncement,
    PROVIDER,
    announcement_matches_spec,
    announcement_to_records,
    parse_release_calendar,
)
from .projector import project_events, store_raw

__all__ = [
    "ABS_BASE_URL",
    "ABS_RELEASE_CALENDAR_URL",
    "ABS_RELEASE_TIME_LOCAL",
    "ABS_RELEASE_TZ",
    "ABSCalendarEventRecord",
    "ABSCalendarParseError",
    "ABSCalendarRawRecord",
    "ABSIndicatorSpec",
    "ABSReleaseAnnouncement",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "announcement_matches_spec",
    "announcement_to_records",
    "fetch_abs_calendar",
    "parse_release_calendar",
    "project_events",
    "store_raw",
]
