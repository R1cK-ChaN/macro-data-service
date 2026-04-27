"""Bank of Korea MPB meeting schedule HTML → calendar projection.

The BOK Meeting Dates page at
``bok.or.kr/eng/main/contents.do?menuNo=400020`` embeds the Monetary
Policy Board's policy-setting meeting schedule as a sequence of
year-headed tables. Each ``<h3>YYYY</h3>`` heading opens a year block
followed by a single ``<table>`` whose ``<td>`` cells carry the
meeting date as ``"Jan.16 (Thu)"`` text. The MPB meets bi-monthly
(eight meetings per year on Jan / Feb / Apr / May / Jul / Aug / Oct /
Nov), so each year's tables typically yield eight cell matches.

The parser walks every year heading on the page, slices the body
between consecutive headings (or until the page footer), and emits
one :class:`BOKMeeting` per parseable cell. Forward years that BOK
hasn't yet posted inline (e.g. 2026 dates currently live as a
``.hwp``/``.pdf`` attachment on a separate news article) are silently
absent — when BOK adds the inline ``<h3>2026</h3>`` block, the
connector picks it up automatically on the next sweep.

Schedule-only slice — values stay ``actual=NULL``. The new Base Rate
lives inside each per-meeting Monetary Policy Decision press
release as ``"the Base Rate at X.YZ percent"`` text; parsing the
press release is deferred to P2 (mirrors the ABS pattern).

``provider_event_id`` keys on the standard
``synthesize_event_id(provider, country, canonical, anchor)`` with the
announcement (meeting) date as the anchor, so the id stays stable
across re-scrapes. Reschedules of an already-announced meeting are
rare; if BOK does reschedule, the new closing date synthesises a new
id and the old row is left as a historical artefact (matches the
RBI / RBA anchor-stability deferral noted in their respective
issues).
"""

from __future__ import annotations

import hashlib
import html as html_lib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import INDICATOR_REGISTRY, BOKIndicatorSpec

PROVIDER = "bok"
BOK_RELEASE_TZ = "Asia/Seoul"
BOK_RELEASE_TIME = "09:50"
BOK_BASE_URL = "https://www.bok.or.kr"
BOK_MEETING_DATES_URL = f"{BOK_BASE_URL}/eng/main/contents.do?menuNo=400020"


class BOKMeetingScheduleParseError(ValueError):
    """BOK Meeting Dates page did not expose a parseable MPB schedule."""


_MONTH_TOKENS: dict[str, int] = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


# Year heading: ``<h3>2025</h3>``. The page also carries other
# ``<h3>...</h3>`` blocks (page navigation), so the regex requires
# exactly four digits and a closing tag — heading text like
# ``<h3>Schedule of the MPB's policy-setting meetings</h3>`` doesn't
# match.
_YEAR_HEADING_RE = re.compile(
    r"<h3>\s*(?P<year>\d{4})\s*</h3>",
    re.IGNORECASE,
)


# Date cell: ``Jan.16 (Thu)`` or ``Jan.16&nbsp;(Thu)``. The HTML
# unescape pass (in :func:`parse_meeting_schedule`) normalises the
# ``&nbsp;`` to a non-breaking space (``\xa0``) before matching, so
# the regex tolerates either whitespace shape between day and paren.
_CELL_RE = re.compile(
    r"\b(?P<month>"
    + "|".join(_MONTH_TOKENS.keys())
    + r")\.\s*(?P<day>\d{1,2})\s*\(\s*[A-Za-z]{3}\s*\)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class BOKMeeting:
    """One scheduled MPB meeting parsed from the schedule block."""

    year: int                 # year of the meeting (from the <h3> heading)
    month: int                # 1..12, parsed from the cell month abbreviation
    day: int                  # 1..31, parsed from the cell day
    month_token: str          # original-case month token from the page ("Jan")
    announcement_date: date   # combined ``date(year, month, day)``


@dataclass(frozen=True)
class BOKCalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class BOKCalendarEventRecord:
    provider: str
    provider_event_id: str
    event_time_utc: str
    event_time_precision: str
    reference_date: str | None
    reference_label: str
    country_code: str
    indicator_id: str | None
    category: str
    title: str
    importance: str | None
    currency: str
    unit: str
    actual: str | None
    previous: str | None
    revised: str | None
    forecast: str | None
    consensus_forecast: str | None
    ticker: str
    source: str
    source_url: str
    content_hash: str
    last_update_epoch_ms: int | None
    observed_at_epoch_ms: int


def parse_meeting_schedule(html: str | bytes) -> list[BOKMeeting]:
    """Walk the Meeting Dates HTML for embedded MPB schedule cells.

    Returns the meetings ordered by announcement date, deduplicated by
    date (the page sometimes repeats a heading near the top-of-page
    navigation widget). Raises
    :class:`BOKMeetingScheduleParseError` when the schedule heading is
    absent, no year headings parse, or zero meeting cells are found.
    """
    if isinstance(html, (bytes, bytearray)):
        html = html.decode("utf-8", errors="replace")
    text = html_lib.unescape(html)

    # The schedule section opens with
    # ``<h2>Schedule of the MPB's policy-setting meetings</h2>``.
    # Anchoring on this heading is mandatory — falling through to the
    # whole document would let an unrelated ``<h3>YYYY</h3>`` block
    # in the page navigation / archive widget sweep into the parse
    # and emit phantom meetings.
    schedule_start = re.search(
        r"<h2>\s*Schedule of the MPB's policy-setting meetings\s*</h2>",
        text,
        re.IGNORECASE,
    )
    if schedule_start is None:
        raise BOKMeetingScheduleParseError(
            "BOK Meeting Dates page missing the ``Schedule of the MPB's "
            "policy-setting meetings`` heading — DOM/API drift",
        )

    # The schedule section ends at the next ``<h1>`` or ``<h2>`` (the
    # live page follows it with a Korean ``<h2>내가 본 콘텐츠</h2>``
    # "viewed-content" sidebar widget). Bounding the body slice on
    # that boundary protects the year-block walk against future page
    # additions that put a year-shaped widget further down the page.
    body_start = schedule_start.end()
    section_end_match = re.search(
        r"<h[12][\s>]", text[body_start:],
    )
    if section_end_match is not None:
        body = text[body_start:body_start + section_end_match.start()]
    else:
        body = text[body_start:]

    headings = list(_YEAR_HEADING_RE.finditer(body))
    if not headings:
        raise BOKMeetingScheduleParseError(
            "BOK Meeting Dates page missing year headings — DOM/API drift",
        )

    meetings: list[BOKMeeting] = []
    seen: set[date] = set()
    for index, heading_match in enumerate(headings):
        try:
            year = int(heading_match.group("year"))
        except (TypeError, ValueError):
            continue
        block_start = heading_match.end()
        block_end = (
            headings[index + 1].start()
            if index + 1 < len(headings)
            else len(body)
        )
        block = body[block_start:block_end]
        for cell_match in _CELL_RE.finditer(block):
            month_token = cell_match.group("month")
            month = _MONTH_TOKENS.get(month_token.upper())
            if month is None:
                continue
            try:
                day = int(cell_match.group("day"))
            except (TypeError, ValueError):
                continue
            try:
                announcement_date = date(year, month, day)
            except ValueError:
                continue
            if announcement_date in seen:
                continue
            seen.add(announcement_date)
            meetings.append(BOKMeeting(
                year=year,
                month=month,
                day=day,
                month_token=month_token,
                announcement_date=announcement_date,
            ))

    if not meetings:
        raise BOKMeetingScheduleParseError(
            "BOK Meeting Dates page parsed zero meeting cells — "
            "layout drift",
        )

    meetings.sort(key=lambda m: m.announcement_date)
    return meetings


_HASH_FIELDS: tuple[str, ...] = (
    "year", "announcement_date",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def meeting_to_records(
    meeting: BOKMeeting,
    *,
    snapshot_epoch_ms: int,
    spec: BOKIndicatorSpec | None = None,
) -> tuple[BOKCalendarRawRecord, BOKCalendarEventRecord]:
    """Project a :class:`BOKMeeting` to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY["BOK_RATE"]

    scheduled = parse_scheduled_release_time(
        meeting.announcement_date,
        BOK_RELEASE_TIME,
        default_tz=BOK_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        meeting.announcement_date.isoformat(),
    )

    reference_label = meeting.announcement_date.strftime("%B %Y")
    payload: dict[str, Any] = {
        "kind":              "bok_mpb_meeting_schedule",
        "year":              meeting.year,
        "announcement_date": meeting.announcement_date.isoformat(),
        "month_token":       meeting.month_token,
        "day":               meeting.day,
        "event_time_utc":    event_time_utc,
        "source_url":        BOK_MEETING_DATES_URL,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = BOKCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = BOKCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=meeting.announcement_date.isoformat(),
        reference_label=reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="KRW",
        unit=resolved_spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Bank of Korea",
        source_url=BOK_MEETING_DATES_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "PROVIDER",
    "BOK_BASE_URL",
    "BOK_MEETING_DATES_URL",
    "BOK_RELEASE_TIME",
    "BOK_RELEASE_TZ",
    "BOKCalendarEventRecord",
    "BOKCalendarRawRecord",
    "BOKMeeting",
    "BOKMeetingScheduleParseError",
    "meeting_to_records",
    "parse_meeting_schedule",
]
