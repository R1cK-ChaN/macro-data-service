"""KOSTAT release-schedule HTML → calendar projection.

The Statistics Korea release-schedule surface at
``mods.go.kr/menu.es?mid=a20301000000`` is server-rendered HTML with
one section per scheduled year. Each section opens with
``<h3>{year} Schedule </h3>`` and is followed by a single ``<table>``
whose body rows carry::

    <td class="span">{Month abbr}.</td>
    <td class="AGL">{title}</td>
    <td>{Mon. DD (Day.)}</td>
    <td>{division name (phone)}</td>

Each row maps to a single :class:`KOSTATReleaseAnnouncement`. The
indicator matcher (in :mod:`fetcher`) inspects the lowercase title
substring; per-indicator title shapes are stable
(``"The Consumer Price Index in <Month> <Year>"``,
``"Monthly Industrial Statistics in <Month> <Year>"``,
``"The Economically Active Population Survey in <Month> <Year>"``).

The page omits the publication year from the date column; the schedule
heading (``"<h3>YYYY Schedule</h3>"``) anchors every row in that page.
The parser captures the heading year and combines it with the parsed
month/day to form the release date.

Schedule-only slice — values stay ``actual=NULL``. The page does not
expose values directly; values live on separate news-list URLs reached
via a JS handler. P2 will parse those.

``provider_event_id`` keys on the standard
``synthesize_event_id(provider, country, canonical, anchor)`` with the
reference month as the anchor (all three P1 indicators are monthly),
so the id stays stable across re-scrapes of the same release.
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

from .indicators import INDICATOR_REGISTRY, KOSTATIndicatorSpec

PROVIDER = "kostat"
KOSTAT_RELEASE_TZ = "Asia/Seoul"
KOSTAT_BASE_URL = "https://mods.go.kr"
KOSTAT_RELEASE_SCHEDULE_URL = f"{KOSTAT_BASE_URL}/menu.es?mid=a20301000000"


class KOSTATCalendarParseError(ValueError):
    """KOSTAT release-schedule HTML did not expose a parseable schedule row."""


_MONTH_ABBR_TOKENS: dict[str, int] = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

_MONTH_FULL_TOKENS: dict[str, int] = {
    "JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4,
    "MAY": 5, "JUNE": 6, "JULY": 7, "AUGUST": 8,
    "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11, "DECEMBER": 12,
}


# Schedule heading: ``<h3>2026 Schedule</h3>``. Tolerates trailing
# whitespace inside the tag — the live capture has ``"<h3>2026
# Schedule </h3>"`` (trailing space before the closing tag).
_HEADING_RE = re.compile(
    r"<h3>\s*(?P<year>\d{4})\s+Schedule\s*</h3>",
    re.IGNORECASE,
)


# Row pair: title cell + date cell. The publication-month
# ``<td class="span">`` cell precedes both, but it carries the same
# information as the date cell's month abbreviation, so the parser
# anchors on the title/date pair and ignores the redundant span cell.
_ROW_RE = re.compile(
    r'<td class="AGL">(?P<title>[^<]+)</td>\s*'
    r'<td>(?P<date_text>[^<]+)</td>',
    re.IGNORECASE | re.DOTALL,
)


# Date text: ``Feb. 03 (Tue.)`` — month abbreviation, period, day,
# whitespace, paren-wrapped day-of-week. The day-of-week is informational
# only; we discard it after matching.
_DATE_RE = re.compile(
    r"(?P<month>"
    + "|".join(_MONTH_ABBR_TOKENS.keys())
    + r")\.\s*(?P<day>\d{1,2})\s*\(",
    re.IGNORECASE,
)


# Reference period: ``"... in <Month> <Year>"`` at the end of a title.
_REF_PERIOD_RE = re.compile(
    r"\bin\s+(?P<month>"
    + "|".join(_MONTH_FULL_TOKENS.keys())
    + r")\s+(?P<year>\d{4})\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class KOSTATReleaseAnnouncement:
    """One scheduled release row parsed from the schedule HTML."""

    schedule_year: int                   # publication year from the page heading
    publication_month: int               # 1..12, parsed from the date column
    publication_day: int                 # 1..31, parsed from the date column
    title: str                           # full title (substring-matched against the registry)
    reference_year: int | None           # parsed from the title (``"in <Month> <Year>"``)
    reference_month: int | None          # 1..12, parsed from the title


@dataclass(frozen=True)
class KOSTATCalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class KOSTATCalendarEventRecord:
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


def parse_release_schedule(
    html: str | bytes,
) -> list[KOSTATReleaseAnnouncement]:
    """Walk the schedule HTML for scheduled release rows.

    Returns one :class:`KOSTATReleaseAnnouncement` per parseable row.
    Raises :class:`KOSTATCalendarParseError` when the schedule heading
    is missing or zero rows parse — typical signal of a layout drift.

    The parser tolerates HTML entity escapes (``&nbsp;`` / ``&amp;``)
    in the title and date text by unescaping the input first; the row
    regex then matches against the unescaped form.
    """
    if isinstance(html, (bytes, bytearray)):
        html = html.decode("utf-8", errors="replace")
    text = html_lib.unescape(html)

    heading_match = _HEADING_RE.search(text)
    if heading_match is None:
        raise KOSTATCalendarParseError(
            "KOSTAT schedule page missing the ``<h3>YYYY Schedule</h3>``"
            " heading — DOM/API drift",
        )
    schedule_year = int(heading_match.group("year"))

    announcements: list[KOSTATReleaseAnnouncement] = []
    # Bound the row sweep to the body that follows the schedule heading
    # so a future page revision that adds an unrelated table above the
    # main schedule (legal disclaimer, archive widget) doesn't bleed
    # into the parse.
    body = text[heading_match.end():]

    for row_match in _ROW_RE.finditer(body):
        title_raw = row_match.group("title")
        date_text = row_match.group("date_text")
        title = re.sub(r"\s+", " ", title_raw).strip()
        if not title:
            continue
        date_match = _DATE_RE.search(date_text)
        if date_match is None:
            continue
        try:
            month = _MONTH_ABBR_TOKENS[date_match.group("month").upper()]
            day = int(date_match.group("day"))
        except (KeyError, ValueError):
            continue
        if not (1 <= day <= 31):
            continue
        try:
            date(schedule_year, month, day)
        except ValueError:
            # Calendar-correctness guard — skip a row whose date column
            # encodes an impossible day for the schedule year.
            continue

        ref_year, ref_month = _parse_reference_period(title)
        announcements.append(KOSTATReleaseAnnouncement(
            schedule_year=schedule_year,
            publication_month=month,
            publication_day=day,
            title=title,
            reference_year=ref_year,
            reference_month=ref_month,
        ))

    if not announcements:
        raise KOSTATCalendarParseError(
            "KOSTAT release-schedule HTML parsed zero schedule rows "
            "— layout drift",
        )
    return announcements


def _parse_reference_period(title: str) -> tuple[int | None, int | None]:
    """Return ``(year, month)`` from ``"... in <Month> <Year>"`` titles.

    Returns ``(None, None)`` when the title does not carry a parseable
    reference-period marker; the caller falls back to the publication
    month minus one as a defensive default (CPI/IIP/Employment all
    publish next-month-after-reference, so the lag-1 default holds).
    """
    match = _REF_PERIOD_RE.search(title)
    if match is None:
        return None, None
    month = _MONTH_FULL_TOKENS.get(match.group("month").upper())
    try:
        year = int(match.group("year"))
    except (TypeError, ValueError):
        return None, None
    if month is None:
        return None, None
    return year, month


def announcement_matches_spec(
    announcement: KOSTATReleaseAnnouncement,
    spec: KOSTATIndicatorSpec,
) -> bool:
    """True when any of the spec's lowercase title substrings appears in the row title."""
    haystack = announcement.title.lower()
    return any(needle in haystack for needle in spec.title_substrings)


def _reference_for(
    announcement: KOSTATReleaseAnnouncement,
) -> tuple[date, str]:
    """Resolve ``(reference_date, reference_label)`` for a release row.

    Anchors on the first day of the reference month. When the title
    carries a parseable ``"in <Month> <Year>"`` marker, that month wins;
    otherwise we fall back to the publication month minus one (lag-1
    is correct for all three P1 indicators).
    """
    if (
        announcement.reference_year is not None
        and announcement.reference_month is not None
    ):
        ref = date(announcement.reference_year, announcement.reference_month, 1)
    else:
        ref_year = announcement.schedule_year
        ref_month = announcement.publication_month - 1
        if ref_month <= 0:
            ref_month += 12
            ref_year -= 1
        ref = date(ref_year, ref_month, 1)
    return ref, ref.strftime("%B %Y")


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
    announcement: KOSTATReleaseAnnouncement,
    *,
    spec: KOSTATIndicatorSpec,
    snapshot_epoch_ms: int,
) -> tuple[KOSTATCalendarRawRecord, KOSTATCalendarEventRecord]:
    """Project a matched announcement onto (raw, event) records."""
    reference_date, reference_label = _reference_for(announcement)
    release_date = date(
        announcement.schedule_year,
        announcement.publication_month,
        announcement.publication_day,
    )
    scheduled = parse_scheduled_release_time(
        release_date,
        spec.release_time_local,
        default_tz=KOSTAT_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(spec.indicator)
    # Anchor on ``reference_date`` so a rescheduled release for the
    # same data period (publication moves Apr 02 → Apr 03 for the
    # March 2026 CPI) updates the existing row instead of spawning a
    # stale-date duplicate. All three P1 indicators are monthly with
    # one publication per reference period.
    provider_event_id = synthesize_event_id(
        PROVIDER,
        spec.country_code,
        indicator_canonical,
        reference_date.isoformat(),
    )

    payload: dict[str, Any] = {
        "kind":              "kostat_release_schedule",
        "indicator":         spec.indicator,
        "release_date":      release_date.isoformat(),
        "release_time_local": spec.release_time_local,
        "reference_date":    reference_date.isoformat(),
        "reference_label":   reference_label,
        "title":             announcement.title,
        "schedule_year":     announcement.schedule_year,
        "publication_month": announcement.publication_month,
        "publication_day":   announcement.publication_day,
        "event_time_utc":    event_time_utc,
        "source_url":        KOSTAT_RELEASE_SCHEDULE_URL,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = KOSTATCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = KOSTATCalendarEventRecord(
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
        currency="KRW",
        unit=spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Statistics Korea",
        source_url=KOSTAT_RELEASE_SCHEDULE_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "KOSTAT_BASE_URL",
    "KOSTAT_RELEASE_SCHEDULE_URL",
    "KOSTAT_RELEASE_TZ",
    "KOSTATCalendarEventRecord",
    "KOSTATCalendarParseError",
    "KOSTATCalendarRawRecord",
    "KOSTATReleaseAnnouncement",
    "PROVIDER",
    "announcement_matches_spec",
    "announcement_to_records",
    "parse_release_schedule",
]
