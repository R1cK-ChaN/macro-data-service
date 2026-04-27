"""MoSPI release-calendar JSON → calendar projection.

The MoSPI release calendar surface at
``mospi.gov.in/release-calendar`` is a React SPA backed by a JSON
endpoint at
``POST /api/release-calender/fetch-all-release-calender-Web``
(payload: ``{"lang": "en", "year": YYYY, "page": 1, "limit": 100}``).
The response shape is::

    {
      "success": true,
      "data": [
        {
          "id": 95,
          "title": "All India Index of Industrial Production (IIP)",
          "description": "<p><a href=\"...PDF\">...</a></p>",
          "doc_url": "uploads/release_calendar/...",
          "year": 2026, "month": 3, "week": 10, "day": 12,
          "level": "day"
        },
        ...
      ],
      "pagination": {...}
    }

Each row maps to a single :class:`MoSPIReleaseAnnouncement`. The
indicator matcher (in :mod:`fetcher`) inspects the lowercase title
substring; multiple title variations collapse into the same
canonical indicator (``"All India Consumer Price Index (CPI)"`` and
``"Press release of CPI for the month of ..."`` both → ``CPI``).

Schedule-only slice — values stay ``actual=NULL``. The API does not
expose values directly; PDF parsing of the per-release document
linked in ``description`` is deferred to P2.

``provider_event_id`` keys on the standard
``synthesize_event_id(provider, country, canonical, anchor)`` with
the reference month as the anchor (monthly indicators) or the
quarter-end date (quarterly indicators), so the id stays stable
across re-scrapes of the same release.
"""

from __future__ import annotations

import hashlib
import json
import re
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import INDICATOR_REGISTRY, MoSPIIndicatorSpec

PROVIDER = "mospi"
MOSPI_RELEASE_TZ = "Asia/Kolkata"
MOSPI_BASE_URL = "https://www.mospi.gov.in"
MOSPI_RELEASE_CALENDAR_URL = (
    f"{MOSPI_BASE_URL}/api/release-calender/fetch-all-release-calender-Web"
)


class MoSPICalendarParseError(ValueError):
    """MoSPI release-calendar JSON did not expose a parseable schedule row."""


# Strip embedded HTML tags from the API's ``description`` field. The
# server sometimes returns the title with stray ``<p>`` / ``<a>``
# wrappers; the substring matcher works on a tag-free representation.
_TAG_RE = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class MoSPIReleaseAnnouncement:
    """One scheduled release row parsed from the JSON response."""

    api_id: int                # API row id, kept verbatim for audit
    title: str                 # API ``title`` (substring-matched against the indicator registry)
    description_text: str      # tag-stripped ``description`` body
    description_html: str      # raw ``description`` HTML (preserved in the audit payload)
    year: int
    month: int                 # 1..12
    day: int                   # 1..31
    week: int | None           # API ``week`` (1..53, optional)
    level: str | None          # API ``level`` (e.g. "day", "month")
    doc_url: str | None        # relative doc path (often empty when description carries the link)


@dataclass(frozen=True)
class MoSPICalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class MoSPICalendarEventRecord:
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


def parse_release_calendar(
    payload: str | bytes | dict[str, Any],
) -> list[MoSPIReleaseAnnouncement]:
    """Walk the JSON response for scheduled release rows.

    Returns one :class:`MoSPIReleaseAnnouncement` per row whose
    ``year`` / ``month`` / ``day`` resolves to a real calendar date
    and whose ``title`` is non-empty. Rows missing ``day`` (level
    ``"month"`` placeholder rows on the API surface) are skipped —
    the connector cannot anchor a calendar event on an undated row.
    Raises :class:`MoSPICalendarParseError` when the response is
    not a success envelope or carries zero parseable rows.
    """
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", errors="replace")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MoSPICalendarParseError(
                f"MoSPI release-calendar response is not JSON: {exc}",
            ) from exc

    if not isinstance(payload, dict) or not payload.get("success"):
        message = payload.get("message") if isinstance(payload, dict) else "?"
        raise MoSPICalendarParseError(
            f"MoSPI release-calendar API returned non-success envelope: {message!r}",
        )

    rows = payload.get("data") or []
    if not isinstance(rows, list):
        raise MoSPICalendarParseError(
            "MoSPI release-calendar API ``data`` is not a list",
        )

    announcements: list[MoSPIReleaseAnnouncement] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = (row.get("title") or "").strip()
        year_raw = row.get("year")
        month_raw = row.get("month")
        day_raw = row.get("day")
        if not title or year_raw is None or month_raw is None or day_raw is None:
            continue
        try:
            year = int(year_raw)
            month = int(month_raw)
            day = int(day_raw)
        except (TypeError, ValueError):
            continue
        if not (1 <= month <= 12) or not (1 <= day <= 31):
            continue
        try:
            date(year, month, day)
        except ValueError:
            # Calendar-correctness guard — the API has been observed to
            # ship ``day=31`` on a 30-day month placeholder. Skip those
            # rows; the connector cannot anchor on a non-existent date.
            continue

        description_html = (row.get("description") or "")
        description_text = _strip_tags(description_html)
        api_id_raw = row.get("id")
        try:
            api_id = int(api_id_raw) if api_id_raw is not None else 0
        except (TypeError, ValueError):
            api_id = 0
        week_raw = row.get("week")
        try:
            week = int(week_raw) if week_raw is not None else None
        except (TypeError, ValueError):
            week = None
        announcements.append(MoSPIReleaseAnnouncement(
            api_id=api_id,
            title=title,
            description_text=description_text,
            description_html=description_html,
            year=year,
            month=month,
            day=day,
            week=week,
            level=row.get("level"),
            doc_url=row.get("doc_url") or None,
        ))

    if not announcements:
        raise MoSPICalendarParseError(
            "MoSPI release-calendar API parsed zero schedule rows — DOM/API drift",
        )
    return announcements


def _strip_tags(text: str) -> str:
    cleaned = _TAG_RE.sub(" ", text or "")
    return re.sub(r"\s+", " ", cleaned).strip()


def announcement_matches_spec(
    announcement: MoSPIReleaseAnnouncement,
    spec: MoSPIIndicatorSpec,
) -> bool:
    """True when any of the spec's lowercase title substrings appears in the row title."""
    haystack = announcement.title.lower()
    return any(needle in haystack for needle in spec.title_substrings)


_QUARTER_END_MONTHS: tuple[int, ...] = (3, 6, 9, 12)

_MONTH_NAMES: dict[str, int] = {
    "JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4,
    "MAY": 5, "JUNE": 6, "JULY": 7, "AUGUST": 8,
    "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11, "DECEMBER": 12,
}

# MoSPI release titles / descriptions consistently carry the reported
# period as ``"for the month of <Month> <Year>"`` (CPI in title;
# IIP buried in the description's anchor text). Parsing this directly
# is more reliable than a fixed release-to-reference lag — IIP shipped
# the Jan 2026 release on Mar 2 (M-2) and the Feb 2026 release on
# Mar 30 (M-1) of the same fixture year, so a fixed-lag heuristic
# would collide their ``provider_event_id``s.
_REF_MONTH_RE = re.compile(
    r"for the month of (?P<month>"
    + "|".join(_MONTH_NAMES.keys())
    + r")\s+(?P<year>\d{4})",
    re.IGNORECASE,
)


def _parse_reference_from_text(
    announcement: MoSPIReleaseAnnouncement,
) -> date | None:
    """Return the first day of the reference month parsed from row text.

    Searches the title first, then the description body (HTML stripped
    by the parser). Returns ``None`` when neither surface carries a
    ``"for the month of <Month> <Year>"`` marker — the caller falls
    back to the lag-based heuristic.
    """
    for text in (announcement.title, announcement.description_text):
        if not text:
            continue
        match = _REF_MONTH_RE.search(text)
        if match is None:
            continue
        month = _MONTH_NAMES.get(match.group("month").upper())
        if month is None:
            continue
        try:
            year = int(match.group("year"))
        except (TypeError, ValueError):
            continue
        return date(year, month, 1)
    return None


def _reference_for(
    announcement: MoSPIReleaseAnnouncement,
    spec: MoSPIIndicatorSpec,
) -> tuple[date, str]:
    """Resolve ``(reference_date, reference_label)`` for a release row.

    Monthly rows: anchor on the first day of the reference month
    parsed from the row's title or description text (``"for the month
    of <Month> <Year>"`` — the canonical MoSPI surface). Falls back
    to a release-month-minus-``reference_lag_months`` heuristic when
    the text marker is absent (CPI default lag = 1, IIP default = 2).
    Aligns with TE's reference-date convention so the parity
    comparator buckets MoSPI rows against TE rows for the same data
    period.

    Quarterly rows: anchor on the most recent quarter-end strictly
    before the release date so a Jan release of the First Advance
    Estimate lands on the Dec 31 (Q3 of FY) anchor rather than the
    publication month. Stage-distinct identity is preserved on the
    *release* side (each stage publishes on a different day, which
    flows into the audit payload and the parity event_date key).
    """
    release_dt = date(announcement.year, announcement.month, announcement.day)
    if spec.frequency == "monthly":
        parsed = _parse_reference_from_text(announcement)
        if parsed is not None:
            return parsed, parsed.strftime("%B %Y")
        ref_year = release_dt.year
        ref_month = release_dt.month - max(spec.reference_lag_months, 1)
        while ref_month <= 0:
            ref_month += 12
            ref_year -= 1
        ref = date(ref_year, ref_month, 1)
        return ref, ref.strftime("%B %Y")
    if spec.frequency == "quarterly":
        # Walk back to the most recent quarter-end month strictly
        # before the release month — release in January after a Q3
        # close (Sep-end) anchors on Dec 31 only if release_month > 12,
        # otherwise step back month-by-month until a quarter-end month
        # is reached.
        quarter_end_month = release_dt.month - 1
        quarter_end_year = release_dt.year
        if quarter_end_month <= 0:
            quarter_end_month = 12
            quarter_end_year -= 1
        while quarter_end_month not in _QUARTER_END_MONTHS:
            quarter_end_month -= 1
            if quarter_end_month <= 0:
                quarter_end_month = 12
                quarter_end_year -= 1
        last_day = monthrange(quarter_end_year, quarter_end_month)[1]
        ref = date(quarter_end_year, quarter_end_month, last_day)
        quarter = (quarter_end_month - 1) // 3 + 1
        return ref, f"Q{quarter} {quarter_end_year}"
    raise MoSPICalendarParseError(f"unsupported frequency: {spec.frequency!r}")


_HASH_FIELDS: tuple[str, ...] = (
    "indicator", "reference_date", "release_date", "title",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def announcement_to_records(
    announcement: MoSPIReleaseAnnouncement,
    *,
    spec: MoSPIIndicatorSpec,
    snapshot_epoch_ms: int,
) -> tuple[MoSPICalendarRawRecord, MoSPICalendarEventRecord]:
    """Project a matched announcement onto (raw, event) records."""
    reference_date, reference_label = _reference_for(announcement, spec)
    release_date = date(announcement.year, announcement.month, announcement.day)
    scheduled = parse_scheduled_release_time(
        release_date,
        spec.release_time_local,
        default_tz=MOSPI_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(spec.indicator)
    # Anchor choice differs by frequency. Monthly indicators have one
    # release per reference period — anchor on ``reference_date`` so
    # an upstream reschedule (release date moves April 13 → April 14
    # for the same March data) updates the existing row instead of
    # spawning a stale-date duplicate. Quarterly indicators (GDP)
    # publish multi-stage releases (First / Second / Third Advance
    # Estimates + Provisional) sharing the same quarter-end
    # reference_date — anchor on ``release_date`` so each stage gets
    # its own row.
    if spec.frequency == "monthly":
        anchor = reference_date.isoformat()
    else:
        anchor = release_date.isoformat()
    provider_event_id = synthesize_event_id(
        PROVIDER,
        spec.country_code,
        indicator_canonical,
        anchor,
    )

    payload: dict[str, Any] = {
        "kind":              "mospi_release_calendar",
        "indicator":         spec.indicator,
        "release_date":      release_date.isoformat(),
        "release_time_local": spec.release_time_local,
        "reference_date":    reference_date.isoformat(),
        "reference_label":   reference_label,
        "title":             announcement.title,
        "description_html":  announcement.description_html,
        "description_text":  announcement.description_text,
        "doc_url":           announcement.doc_url,
        "api_id":            announcement.api_id,
        "year":              announcement.year,
        "month":             announcement.month,
        "day":               announcement.day,
        "week":              announcement.week,
        "level":             announcement.level,
        "event_time_utc":    event_time_utc,
        "source_url":        MOSPI_RELEASE_CALENDAR_URL,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = MoSPICalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = MoSPICalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=reference_date.isoformat(),
        reference_label=reference_label,
        country_code=spec.country_code,
        indicator_id=None,
        category=spec.category,
        title=spec.title,
        importance=spec.importance,
        currency="INR",
        unit=spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Ministry of Statistics and Programme Implementation",
        source_url=MOSPI_RELEASE_CALENDAR_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "MOSPI_BASE_URL",
    "MOSPI_RELEASE_CALENDAR_URL",
    "MOSPI_RELEASE_TZ",
    "MoSPICalendarEventRecord",
    "MoSPICalendarParseError",
    "MoSPICalendarRawRecord",
    "MoSPIReleaseAnnouncement",
    "PROVIDER",
    "announcement_matches_spec",
    "announcement_to_records",
    "parse_release_calendar",
]
