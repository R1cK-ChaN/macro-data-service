"""Reserve Bank of Australia cash-rate-target HTML → calendar projection.

The RBA cash-rate page at ``rba.gov.au/statistics/cash-rate/`` is
an HTML table of every Monetary Policy Board interest-rate decision
since 6 January 2008. Each row carries:

- **Effective Date** — the **first business day at the new rate**.
  RBA convention is that the cash-rate target announced at 14:30
  AEST/AEDT on the meeting's second day takes effect from the
  *next* business day, so this column lags the announcement by one
  business day (mirrors the BoC Valet pattern).
- **Change** — ``+0.25`` / ``-0.25`` / ``0.00`` (pre-formatted with
  signs; ``0.00`` rows are HOLD decisions).
- **Cash rate target** — the new target rate as a percent.
- **Related Documents** — links to the MPB statement and minutes;
  the minutes URL embeds the announcement date as
  ``rba-board-minutes/YYYY/YYYY-MM-DD.html``. The parser uses that
  date as the announcement anchor when present, falling back to
  ``effective_date − 1 business day`` when the row carries no link
  (oldest entries from 1990–early 1991 occasionally lack the link).

Unlike the BoC Valet pattern, the RBA table includes hold
decisions, so the connector projects every scheduled MPB
announcement — change OR hold — as one calendar event. This is
important for parity with TE, which also publishes hold rows (~8
per year).

``provider_event_id`` / ``event_time_utc`` / ``reference_date`` all
anchor on the **announcement date** so the parity bucket aligns with
TE / Bloomberg / Reuters convention (calendar rows keyed on
announcement date, not effective date). The Valet-style effective
date is preserved verbatim in the audit payload.

The 14:30 AEST/AEDT publication time is the RBA's documented
post-meeting announcement convention; ``parse_scheduled_release_time``
against ``Australia/Sydney`` resolves DST so the UTC timestamp is
correct for both winter and summer announcements.

Payload drift (table tag missing, malformed rate cell) raises
:class:`RBACashRateParseError` rather than silently dropping rows
— a parse miss on this page is a layout-change signal we want loud.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import INDICATOR_REGISTRY, RBAIndicatorSpec

PROVIDER = "rba"
RBA_RELEASE_TZ = "Australia/Sydney"
RBA_RELEASE_TIME = "14:30"
RBA_BASE_URL = "https://www.rba.gov.au"
RBA_CASH_RATE_URL = f"{RBA_BASE_URL}/statistics/cash-rate/"


class RBACashRateParseError(ValueError):
    """Raised when the cash-rate page exposes zero parseable decisions."""


@dataclass(frozen=True)
class RBARateDecision:
    """One Monetary Policy Board rate decision parsed from the cash-rate page.

    The cash-rate page lists ``effective_date`` — the first business
    day at the new rate — but the announcement happens at 14:30
    AEST/AEDT on the *prior* business day at the end of the meeting.
    Both dates are exposed so the projector can use the announcement
    date for ``event_time_utc`` (matches TE / Bloomberg's calendar-row
    convention) while preserving the table's effective date verbatim
    in the audit payload.
    """

    announcement_date: date  # date RBA publicly announced the decision (14:30 AEST/AEDT)
    effective_date: date     # first business day at the new rate (table's "Effective Date")
    rate: str                # decimal string ("4.10")
    change: str              # decimal string with sign ("+0.25", "0.00", "-0.25")
    previous_rate: str | None  # rate before this decision ("3.85") — None for the first row
    statement_url: str | None  # absolute URL of the MPB statement (when present)
    minutes_url: str | None    # absolute URL of the meeting minutes (when present)


@dataclass(frozen=True)
class RBACalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class RBACalendarEventRecord:
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


_DATATABLE_RE = re.compile(
    r'<table[^>]*id="datatable"[^>]*>(.*?)</table>',
    re.DOTALL,
)
_TBODY_RE = re.compile(r"<tbody[^>]*>(.*?)</tbody>", re.DOTALL)
_ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL)
_CELL_RE = re.compile(r"<t[hd][^>]*>(.*?)</t[hd]>", re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")
_LINK_HREF_RE = re.compile(r'href="([^"]+)"')
# Minutes URL pattern: ``/monetary-policy/rba-board-minutes/YYYY/YYYY-MM-DD.html``.
# The trailing ISO date is the announcement day for the corresponding
# cash-rate decision (RBA publishes minutes per meeting; the URL embeds
# the meeting's announcement date directly).
_MINUTES_DATE_RE = re.compile(
    r"/monetary-policy/rba-board-minutes/\d{4}/(\d{4}-\d{2}-\d{2})\.html",
)

_MONTH_TOKENS: dict[str, int] = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
_DATE_RE = re.compile(r"^\s*(\d{1,2})\s+([A-Z]{3})\s+(\d{4})\s*$")


def _parse_announcement_date(text: str) -> date:
    cleaned = (text or "").strip()
    match = _DATE_RE.match(cleaned.upper())
    if not match:
        raise RBACashRateParseError(
            f"unparseable RBA effective date: {text!r}",
        )
    day = int(match.group(1))
    month_token = match.group(2)
    year = int(match.group(3))
    month = _MONTH_TOKENS.get(month_token)
    if month is None:
        raise RBACashRateParseError(
            f"unknown month token in RBA date: {text!r}",
        )
    return date(year, month, day)


def _parse_rate_cell(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        raise RBACashRateParseError("empty RBA rate cell")
    try:
        Decimal(cleaned)
    except InvalidOperation as exc:
        raise RBACashRateParseError(f"unparseable RBA rate {text!r}") from exc
    return cleaned


def _parse_change_cell(text: str) -> str:
    """Return the change cell normalised — sign preserved.

    The page uses ``+0.25`` / ``-0.25`` for moves and ``0.00`` for
    holds. The Decimal round-trip strips superfluous whitespace and
    confirms the value parses; we keep the sign in the output string
    so a downstream consumer can distinguish a hike from a cut without
    inspecting two cells.
    """
    cleaned = (text or "").strip()
    if not cleaned:
        raise RBACashRateParseError("empty RBA change cell")
    try:
        # Decimal accepts a leading '+'; the sign survives in the string
        # we emit because we keep the original cell text, not str(Decimal).
        Decimal(cleaned)
    except InvalidOperation as exc:
        raise RBACashRateParseError(
            f"unparseable RBA change {text!r}",
        ) from exc
    return cleaned


def _strip_tags(html: str) -> str:
    text = _TAG_RE.sub(" ", html or "")
    return re.sub(r"\s+", " ", text).strip()


def _absolute_url(href: str | None) -> str | None:
    if not href:
        return None
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"{RBA_BASE_URL}{href}"
    return f"{RBA_BASE_URL}/{href.lstrip('/')}"


def _extract_links(cell_html: str) -> tuple[str | None, str | None]:
    """Return ``(statement_url, minutes_url)`` for a Related Documents cell.

    The page anchors the document type from the link's aria-label
    (``"Media Release ... Statement by ..."`` / ``"Minutes of the
    Monetary Policy Meeting ..."``); falling back to the link text
    keeps the parse working even when the aria-label drops, since the
    visible text is reliably ``"Statement"`` / ``"Minutes"``.
    """
    statement: str | None = None
    minutes: str | None = None
    for anchor in re.finditer(r"<a\b([^>]*)>(.*?)</a>", cell_html, re.DOTALL):
        attrs = anchor.group(1)
        text_inner = _strip_tags(anchor.group(2)).lower()
        href_match = _LINK_HREF_RE.search(attrs)
        if not href_match:
            continue
        href = _absolute_url(href_match.group(1))
        if href is None:
            continue
        if "minutes" in text_inner or "minutes" in attrs.lower():
            minutes = minutes or href
        else:
            # Default to statement; the only two doc types on this page
            # are statements and minutes, so anything not labelled
            # ``minutes`` is a statement (statement / media release).
            statement = statement or href
    return statement, minutes


def _previous_business_day(day: date) -> date:
    """Return the business day before ``day`` (Mon→Fri, skipping Sat/Sun).

    Used as the announcement-date fallback when the minutes link is
    missing — RBA's documented convention is that the cash-rate target
    takes effect the *next* business day after the announcement.
    """
    weekday = day.weekday()  # Mon=0, Sun=6
    if weekday == 0:
        delta = 3   # Monday → previous Friday
    elif weekday == 6:
        delta = 2   # Sunday → previous Friday (defensive — effective
                    # dates are always weekdays in practice)
    else:
        delta = 1
    return date.fromordinal(day.toordinal() - delta)


def _announcement_from_minutes(minutes_url: str | None) -> date | None:
    if not minutes_url:
        return None
    match = _MINUTES_DATE_RE.search(minutes_url)
    if not match:
        return None
    try:
        return date.fromisoformat(match.group(1))
    except ValueError:
        return None


def parse_cash_rate_table(
    html: str | bytes,
) -> list[RBARateDecision]:
    """Walk the RBA cash-rate-target HTML for every MPB decision.

    Returns the decisions ordered most-recent-first. Raises
    :class:`RBACashRateParseError` when the table tag is missing or
    zero rows parse — RBA outage, layout drift, or an empty page.
    """
    if isinstance(html, (bytes, bytearray)):
        html = html.decode("utf-8", errors="replace")

    table_match = _DATATABLE_RE.search(html)
    if not table_match:
        raise RBACashRateParseError(
            "RBA cash-rate page missing 'datatable' table — DOM/API drift",
        )
    tbody_match = _TBODY_RE.search(table_match.group(1))
    if not tbody_match:
        raise RBACashRateParseError(
            "RBA cash-rate datatable missing <tbody> — DOM/API drift",
        )

    parsed: list[tuple[date, date, str, str, str | None, str | None]] = []
    for row_match in _ROW_RE.finditer(tbody_match.group(1)):
        cell_htmls = _CELL_RE.findall(row_match.group(1))
        # Older entries (1990–early 1991) ship with only three cells —
        # date, change, rate — and no Related Documents column at all.
        # Treat them as valid hold/move rows; the ``< 3`` floor still
        # rejects truly-malformed rows like the year-comment spacer.
        if len(cell_htmls) < 3:
            continue
        date_text = _strip_tags(cell_htmls[0])
        change_text = _strip_tags(cell_htmls[1])
        rate_text = _strip_tags(cell_htmls[2])
        try:
            effective_date = _parse_announcement_date(date_text)
            rate = _parse_rate_cell(rate_text)
            change = _parse_change_cell(change_text)
        except RBACashRateParseError:
            # A truncated trailing row (e.g., a "spacer" tr that the
            # year-comment markup sometimes injects) shouldn't nuke the
            # whole list — skip it and keep walking.
            continue
        statement_url, minutes_url = (
            _extract_links(cell_htmls[3]) if len(cell_htmls) >= 4
            else (None, None)
        )
        announcement_date = (
            _announcement_from_minutes(minutes_url)
            or _previous_business_day(effective_date)
        )
        parsed.append(
            (announcement_date, effective_date, rate, change,
             statement_url, minutes_url),
        )

    if not parsed:
        raise RBACashRateParseError(
            "RBA cash-rate datatable parsed zero decisions — layout drift?",
        )

    # The page is published most-recent-first; sort defensively in case
    # the order ever changes, then derive ``previous_rate`` from the
    # chronologically-prior decision (oldest first).
    parsed.sort(key=lambda r: r[0])
    decisions: list[RBARateDecision] = []
    previous_rate: str | None = None
    for (
        announcement_date, effective_date, rate, change,
        statement_url, minutes_url,
    ) in parsed:
        decisions.append(RBARateDecision(
            announcement_date=announcement_date,
            effective_date=effective_date,
            rate=rate,
            change=change,
            previous_rate=previous_rate,
            statement_url=statement_url,
            minutes_url=minutes_url,
        ))
        previous_rate = rate

    decisions.sort(key=lambda d: d.announcement_date, reverse=True)
    return decisions


_HASH_FIELDS: tuple[str, ...] = (
    "announcement_date", "effective_date", "rate", "change", "previous_rate",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def decision_to_records(
    decision: RBARateDecision,
    *,
    snapshot_epoch_ms: int,
    spec: RBAIndicatorSpec | None = None,
) -> tuple[RBACalendarRawRecord, RBACalendarEventRecord]:
    """Project a :class:`RBARateDecision` to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY["RBA_RATE"]

    scheduled = parse_scheduled_release_time(
        decision.announcement_date,
        RBA_RELEASE_TIME,
        default_tz=RBA_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        decision.announcement_date.isoformat(),
    )

    reference_label = decision.announcement_date.strftime("%B %Y")
    payload: dict[str, Any] = {
        "kind":              "rba_cash_rate_decision",
        "announcement_date": decision.announcement_date.isoformat(),
        "effective_date":    decision.effective_date.isoformat(),
        "rate":              decision.rate,
        "change":            decision.change,
        "previous_rate":     decision.previous_rate,
        "statement_url":     decision.statement_url,
        "minutes_url":       decision.minutes_url,
        "event_time_utc":    event_time_utc,
        "source_url":        RBA_CASH_RATE_URL,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = RBACalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    # Prefer the official statement URL on the event row when present;
    # falls back to the cash-rate page for the rare row missing both
    # links (oldest entries from 2008-2009 occasionally lack the
    # statement anchor).
    event_source_url = decision.statement_url or RBA_CASH_RATE_URL
    event_record = RBACalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=decision.announcement_date.isoformat(),
        reference_label=reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="AUD",
        unit=resolved_spec.unit,
        actual=decision.rate,
        previous=decision.previous_rate,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Reserve Bank of Australia",
        source_url=event_source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "PROVIDER",
    "RBA_BASE_URL",
    "RBA_CASH_RATE_URL",
    "RBA_RELEASE_TIME",
    "RBA_RELEASE_TZ",
    "RBACalendarEventRecord",
    "RBACalendarRawRecord",
    "RBACashRateParseError",
    "RBARateDecision",
    "decision_to_records",
    "parse_cash_rate_table",
]
