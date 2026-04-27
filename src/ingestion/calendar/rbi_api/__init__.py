"""Reserve Bank of India calendar connector — issue #54 P1.

The RBI exposes the Monetary Policy Committee meeting schedule
inline on the ``annualpolicy.aspx`` page at
``rbi.org.in/scripts/annualpolicy.aspx``. The page embeds the latest
"Meeting Schedule of the Monetary Policy Committee" press release as
six bi-monthly meeting date triples per fiscal year (e.g. ``April 6,
7 and 8, 2026``); the third date in each triple is the closing day
when the Governor announces the policy repo rate decision at 10:00
IST.

P1 ships a single anchor — ``RBI_RATE`` — and projects every
scheduled MPC meeting (forward and current-FY past) as one calendar
event at 10:00 IST. The slice is **schedule-only**: the new repo
rate value lives inside each per-meeting Resolution press release;
parsing each PRID for the value is deferred to P2 (mirrors the
ABS schedule-only pattern).

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — single ``RBI_RATE`` spec.
- :class:`RBIMeeting` — one parsed MPC meeting from the schedule block.
- :func:`parse_meeting_schedule` — HTML → list of meetings.
- :func:`fetch_rbi_calendar` — orchestrates fetch + project.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_rbi_calendar
from .indicators import INDICATOR_REGISTRY, RBIIndicatorSpec
from .parser import (
    PROVIDER,
    RBI_ANNUAL_POLICY_URL,
    RBI_BASE_URL,
    RBI_RELEASE_TIME,
    RBI_RELEASE_TZ,
    RBICalendarEventRecord,
    RBICalendarRawRecord,
    RBIMeeting,
    RBIMeetingScheduleParseError,
    meeting_to_records,
    parse_meeting_schedule,
)
from .projector import project_events, store_raw

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "RBI_ANNUAL_POLICY_URL",
    "RBI_BASE_URL",
    "RBI_RELEASE_TIME",
    "RBI_RELEASE_TZ",
    "RBICalendarEventRecord",
    "RBICalendarRawRecord",
    "RBIIndicatorSpec",
    "RBIMeeting",
    "RBIMeetingScheduleParseError",
    "fetch_rbi_calendar",
    "meeting_to_records",
    "parse_meeting_schedule",
    "project_events",
    "store_raw",
]
