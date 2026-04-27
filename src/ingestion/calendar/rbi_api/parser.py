"""Reserve Bank of India MPC meeting schedule HTML → calendar projection.

The RBI ``annualpolicy.aspx`` page at
``rbi.org.in/scripts/annualpolicy.aspx`` embeds the latest "Meeting
Schedule of the Monetary Policy Committee" press release inline.
The schedule lists six bi-monthly MPC meeting date triples per
fiscal year (e.g. ``April 6, 7 and 8, 2026``); each triple closes
on its third date — RBI announces the policy repo rate decision at
10:00 IST on that closing day via the Governor's Statement.

The connector parses every triple it finds following the
``Meeting Schedule of the Monetary Policy Committee for YYYY-YYYY``
heading, anchoring each calendar event on the meeting's closing
day. Forward-looking and past-but-current-FY meetings both land on
the same code path — the page surfaces the full year in a single
block.

Schedule-only slice — values stay ``actual=NULL``. The new repo rate
lives inside each per-meeting Resolution press release as
``"policy repo rate at X.YZ percent"`` text, but parsing each PRID
page is deferred to P2 (mirrors the ABS schedule-only pattern).

``provider_event_id`` keys on the standard
``synthesize_event_id(provider, country, canonical, anchor)`` with
the announcement (closing) date as the anchor, so the id stays
stable across re-scrapes. Reschedules of an already-announced
meeting are rare; if RBI does reschedule, the new closing date
synthesises a new id and the old row is left as a historical
artefact (matches the Fed FOMC anchor-stability deferral noted in
the issue #9 P4 review).
"""

from __future__ import annotations

import hashlib
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

from .indicators import INDICATOR_REGISTRY, RBIIndicatorSpec

PROVIDER = "rbi"
RBI_RELEASE_TZ = "Asia/Kolkata"
RBI_RELEASE_TIME = "10:00"
RBI_BASE_URL = "https://www.rbi.org.in"
RBI_ANNUAL_POLICY_URL = f"{RBI_BASE_URL}/scripts/annualpolicy.aspx"


class RBIMeetingScheduleParseError(ValueError):
    """RBI annualpolicy.aspx did not expose a parseable MPC meeting schedule."""


_MONTH_TOKENS: dict[str, int] = {
    "JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4,
    "MAY": 5, "JUNE": 6, "JULY": 7, "AUGUST": 8,
    "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11, "DECEMBER": 12,
}

# Tag-stripper for the body block. ``annualpolicy.aspx`` mixes the
# inline schedule into a heavily-marked-up page, so the parser walks
# the tag-free representation rather than chasing nested element
# structures that drift between Brilliance refreshes of the RBI site.
_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")

# Locate the schedule heading; carry the FY label forward into the
# audit payload so re-runs against an updated heading land on a new
# content_hash even if the date triples haven't changed.
_SCHEDULE_HEADING_RE = re.compile(
    r"Meeting Schedule of the Monetary Policy Committee for "
    r"(?P<fy>\d{4}-\d{4})",
    re.IGNORECASE,
)

# Date triple — "April 6, 7 and 8, 2026". The middle day is optional:
# the historical RBI shape is three-day meetings, but the regex
# tolerates the two-day "April 7 and 8, 2030" form (no comma after
# d1) defensively in case the format ever shortens.
#
# Cross-month meetings are also supported — RBI's FY 2025-2026
# schedule had "September 29, 30 and October 1, 2025" where the
# closing day belongs to a different month than the opening days.
# When ``end_month`` matches, it overrides ``month`` for the
# closing-day date construction so the announcement anchors on
# the right month.
_MONTH_PATTERN = "|".join(_MONTH_TOKENS.keys())
_DATE_TRIPLE_RE = re.compile(
    r"(?P<month>" + _MONTH_PATTERN + r")\s+"
    r"(?P<d1>\d{1,2})"
    r"(?:\s*,\s*(?P<d2>\d{1,2}))?"
    r"\s+and\s+"
    r"(?:(?P<end_month>" + _MONTH_PATTERN + r")\s+)?"
    r"(?P<d3>\d{1,2})\s*,\s*"
    r"(?P<year>\d{4})",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class RBIMeeting:
    """One scheduled MPC meeting parsed from the schedule block."""

    fiscal_year: str       # "2026-2027"
    month_token: str       # original-case month token from the page ("April")
    days: tuple[int, ...]  # ordered meeting days, e.g. (6, 7, 8)
    year: int              # year of the closing day (the year on the schedule line)
    announcement_date: date  # closing-day datetime (also the announcement day)


@dataclass(frozen=True)
class RBICalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class RBICalendarEventRecord:
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


def parse_meeting_schedule(html: str | bytes) -> list[RBIMeeting]:
    """Walk annualpolicy.aspx for the embedded MPC meeting schedule.

    Returns the meetings ordered by announcement date. Raises
    :class:`RBIMeetingScheduleParseError` when the schedule heading
    is missing or zero meeting triples parse — RBI outage, layout
    drift, or an empty page.
    """
    if isinstance(html, (bytes, bytearray)):
        html = html.decode("utf-8", errors="replace")

    text = _TAG_RE.sub(" ", html)
    text = _WHITESPACE_RE.sub(" ", text).strip()

    heading_match = _SCHEDULE_HEADING_RE.search(text)
    if heading_match is None:
        raise RBIMeetingScheduleParseError(
            "RBI annualpolicy page missing the MPC meeting schedule heading "
            "— DOM/API drift",
        )
    fy = heading_match.group("fy")

    # The schedule block runs from the heading until the next major
    # navigation marker. The fiscal-year archive widget reliably
    # follows the schedule and starts with the same FY label
    # repeated as a tree node ("2026-2027 2025-2026 2024-2025 ...").
    # Bound the search there so a stray date inside the archive
    # widget doesn't get treated as a meeting.
    body_start = heading_match.end()
    archive_match = re.search(
        r"\b" + re.escape(fy) + r"\b", text[body_start:],
    )
    if archive_match is not None:
        body = text[body_start:body_start + archive_match.start()]
    else:
        # Defensive — if the archive widget shape changes, take a
        # capped slice so we don't sweep the entire page footer.
        body = text[body_start:body_start + 2000]

    meetings: list[RBIMeeting] = []
    seen: set[date] = set()
    for triple_match in _DATE_TRIPLE_RE.finditer(body):
        month_token = triple_match.group("month")
        month = _MONTH_TOKENS.get(month_token.upper())
        if month is None:
            continue
        try:
            d1 = int(triple_match.group("d1"))
            d3 = int(triple_match.group("d3"))
            year = int(triple_match.group("year"))
        except (TypeError, ValueError):
            continue
        d2_raw = triple_match.group("d2")
        days: tuple[int, ...]
        if d2_raw is not None:
            try:
                d2 = int(d2_raw)
            except (TypeError, ValueError):
                continue
            days = (d1, d2, d3)
        else:
            days = (d1, d3)
        # Cross-month meeting — closing day belongs to a different
        # month than the opening days (e.g. "September 29, 30 and
        # October 1, 2025"). When the closing month is January and
        # the opening month is December, the year on the trailing
        # ", YYYY" applies to the closing date and the opening days
        # belong to ``year - 1``; that case isn't on the historical
        # RBI calendar but is handled defensively.
        end_month_token = triple_match.group("end_month")
        if end_month_token is not None:
            end_month = _MONTH_TOKENS.get(end_month_token.upper())
            if end_month is None:
                continue
            close_month = end_month
            close_year = year
        else:
            close_month = month
            close_year = year
        try:
            announcement_date = date(close_year, close_month, d3)
        except ValueError:
            continue
        if announcement_date in seen:
            continue
        seen.add(announcement_date)
        meetings.append(RBIMeeting(
            fiscal_year=fy,
            month_token=month_token,
            days=days,
            year=year,
            announcement_date=announcement_date,
        ))

    if not meetings:
        raise RBIMeetingScheduleParseError(
            "RBI annualpolicy schedule block parsed zero meeting triples "
            "— layout drift?",
        )

    meetings.sort(key=lambda m: m.announcement_date)
    return meetings


_HASH_FIELDS: tuple[str, ...] = (
    "fiscal_year", "announcement_date", "days",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def meeting_to_records(
    meeting: RBIMeeting,
    *,
    snapshot_epoch_ms: int,
    spec: RBIIndicatorSpec | None = None,
) -> tuple[RBICalendarRawRecord, RBICalendarEventRecord]:
    """Project an :class:`RBIMeeting` to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY["RBI_RATE"]

    scheduled = parse_scheduled_release_time(
        meeting.announcement_date,
        RBI_RELEASE_TIME,
        default_tz=RBI_RELEASE_TZ,
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
        "kind":              "rbi_mpc_meeting_schedule",
        "fiscal_year":       meeting.fiscal_year,
        "announcement_date": meeting.announcement_date.isoformat(),
        "month_token":       meeting.month_token,
        "days":              list(meeting.days),
        "year":              meeting.year,
        "event_time_utc":    event_time_utc,
        "source_url":        RBI_ANNUAL_POLICY_URL,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = RBICalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = RBICalendarEventRecord(
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
        currency="INR",
        unit=resolved_spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Reserve Bank of India",
        source_url=RBI_ANNUAL_POLICY_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "PROVIDER",
    "RBI_ANNUAL_POLICY_URL",
    "RBI_BASE_URL",
    "RBI_RELEASE_TIME",
    "RBI_RELEASE_TZ",
    "RBICalendarEventRecord",
    "RBICalendarRawRecord",
    "RBIMeeting",
    "RBIMeetingScheduleParseError",
    "meeting_to_records",
    "parse_meeting_schedule",
]
