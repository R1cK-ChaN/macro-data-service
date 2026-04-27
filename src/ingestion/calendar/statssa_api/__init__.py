"""Stats SA (Statistics South Africa) calendar connector — issue #90 P1.

Stats SA publishes its release schedule through a per-month AJAX
endpoint at
``statssa.gov.za/wp-content/themes/umkhanyakude-v2.1/ajax_server.php?req=recently_scheduled_eddie_t``.
The connector POSTs once per month inside a rolling current + N
month-ahead window, parses each row, and matches against an indicator
allowlist anchored on the Stats SA Publication Number (``PPN``).

The slice is **schedule-only**: events publish with ``actual=NULL``;
per-release detail-page value extraction is deferred to P2 (mirrors
the IBGE / TÜİK / INEGI / KOSTAT / MoSPI deferral pattern).

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — the P1 indicator whitelist.
- :class:`StatsSAReleaseAnnouncement` — one parsed schedule row.
- :func:`parse_publication_schedule` — HTML → announcements.
- :func:`fetch_statssa_calendar` — orchestrates fetch + project.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_statssa_calendar
from .indicators import INDICATOR_REGISTRY, StatsSAIndicatorSpec
from .parser import (
    PROVIDER,
    STATSSA_BASE_URL,
    STATSSA_PUBLIC_SCHEDULE_URL,
    STATSSA_RELEASE_TZ,
    STATSSA_SCHEDULE_API_URL,
    StatsSACalendarEventRecord,
    StatsSACalendarParseError,
    StatsSACalendarRawRecord,
    StatsSAReleaseAnnouncement,
    announcement_matches_spec,
    announcement_to_records,
    parse_publication_schedule,
)
from .projector import project_events, store_raw

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "STATSSA_BASE_URL",
    "STATSSA_PUBLIC_SCHEDULE_URL",
    "STATSSA_RELEASE_TZ",
    "STATSSA_SCHEDULE_API_URL",
    "StatsSACalendarEventRecord",
    "StatsSACalendarParseError",
    "StatsSACalendarRawRecord",
    "StatsSAIndicatorSpec",
    "StatsSAReleaseAnnouncement",
    "announcement_matches_spec",
    "announcement_to_records",
    "fetch_statssa_calendar",
    "parse_publication_schedule",
    "project_events",
    "store_raw",
]
