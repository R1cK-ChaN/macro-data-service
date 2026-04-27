"""Bank of Korea calendar connector — issue #55 P1.

The BOK exposes the Monetary Policy Board's policy-setting meeting
schedule inline on the Meeting Dates page at
``bok.or.kr/eng/main/contents.do?menuNo=400020``. The page renders one
``<h3>YYYY</h3>`` block per scheduled / past year, each followed by a
single ``<table>`` whose ``<td>`` cells carry the meeting date as
``"Jan.16 (Thu)"`` text. The MPB meets bi-monthly — eight meetings per
year on Jan, Feb, Apr, May, Jul, Aug, Oct, Nov.

P1 ships a single anchor — ``BOK_RATE`` — and projects every scheduled
MPB meeting (forward and past) as one calendar event at 09:50 KST,
matching the conventional time at which the Governor announces the
Base Rate decision. The slice is **schedule-only**: the new Base Rate
value lives inside each per-meeting Monetary Policy Decision press
release; parsing the press release is deferred to P2 (mirrors the ABS
schedule-only pattern).

Public surface mirrors the other ``<src>_api`` calendar packages:

- :data:`INDICATOR_REGISTRY` — single ``BOK_RATE`` spec.
- :class:`BOKMeeting` — one parsed MPB meeting from the schedule.
- :func:`parse_meeting_schedule` — HTML → list of meetings.
- :func:`fetch_bok_calendar` — orchestrates fetch + project.
- :func:`project_events` / :func:`store_raw` — idempotent writers.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_bok_calendar
from .indicators import INDICATOR_REGISTRY, BOKIndicatorSpec
from .parser import (
    BOK_BASE_URL,
    BOK_MEETING_DATES_URL,
    BOK_RELEASE_TIME,
    BOK_RELEASE_TZ,
    BOKCalendarEventRecord,
    BOKCalendarRawRecord,
    BOKMeeting,
    BOKMeetingScheduleParseError,
    PROVIDER,
    meeting_to_records,
    parse_meeting_schedule,
)
from .projector import project_events, store_raw

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "BOK_BASE_URL",
    "BOK_MEETING_DATES_URL",
    "BOK_RELEASE_TIME",
    "BOK_RELEASE_TZ",
    "BOKCalendarEventRecord",
    "BOKCalendarRawRecord",
    "BOKIndicatorSpec",
    "BOKMeeting",
    "BOKMeetingScheduleParseError",
    "fetch_bok_calendar",
    "meeting_to_records",
    "parse_meeting_schedule",
    "project_events",
    "store_raw",
]
