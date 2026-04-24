"""Scrape ``customs.go.jp/toukei/calendar/calend_e.htm``.

The MoF Trade Statistics release calendar exposes four nested tables.
The first (``<caption>Trade Statistics (Provisional, Detailed)</caption>``)
is the only one carrying Balance of Trade release dates; the
Revised/Fixed and Other Trade Related tables cover follow-up and
ancillary surfaces we don't project.

Inside the Provisional table the shape is::

    ┌──────────────┬ First 10 days ┬ First 20 days ┬ Monthly Data ┬ Detailed ┐
    │ 2026 | Jan.  │ Jan.29        │ Feb.6         │ Feb.18       │ Feb.26   │
    │      | Feb.  │ Feb.26        │ Mar.6         │ Mar.18       │ Mar.27   │
    │      | Mar.  │ Mar.27        │ Apr.7         │ Apr.22       │ Apr.28   │
    │      | Fiscal│ ー            │ ー            │ Apr.22       │ Apr.28   │
    │      | Apr.  │ Apr.28        │ May.12        │ May.21       │ May.28   │
    │      │ ...                                                             │
    │ 2027 | Jan.  │ Jan.28,2027   │ Feb.5,2027    │ Feb.17,2027  │ Feb.25,2027 │
    └──────────────┴───────────────┴───────────────┴──────────────┴──────────┘

Column 3 ("Monthly Data") is the Balance of Trade release. We drop
the "Fiscal Year" and "Calendar Year" aggregation rows — those
aggregations publish on the same day as a matching month row (the
final month of the period) and would otherwise duplicate the
calendar event.

Release dates may omit the year when the release falls in the same
row-year (``"Jan.29"`` in the 2026 rowspan → Jan 29 2026) or when
the reference month is December and the release spills into the
next calendar year (``"Jan.22"`` in the 2025 Dec. row → Jan 22 2026).
Year-aware resolution: if the release-month number is less than
the reference-month number, bump the year by one; otherwise the
row-year carries through. Explicit ``"Jan.28,2027"`` overrides
always win.

Fetch + parse are separable — tests feed fixture HTML directly to
:func:`parse_mof_calendar_html`; live callers use
:func:`fetch_mof_calendar_html` with a browser-UA header bundle
(``customs.go.jp`` serves a plain HTML surface, but matching the
other Japan connectors' UA bundle protects against future WAF
changes).
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import MofIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    MOF_CALENDAR_URL,
    MOF_RELEASE_TIME_LOCAL,
    MOF_RELEASE_TZ,
    PROVIDER,
    MofCalendarEventRecord,
    MofCalendarRawRecord,
    build_trade_report_url,
)

logger = logging.getLogger(__name__)


class MofCalendarParseError(ValueError):
    """Raised when the MoF calendar DOM deviates from the expected shape.

    Loud-fail is deliberate — DOM drift silently dropping months
    would leave the Balance of Trade calendar sparse until a trader
    noticed a missing release.
    """


@dataclass(frozen=True)
class MofCalendarEntry:
    """One Balance-of-Trade release row parsed from the calendar.

    ``reference_date`` uses the first day of the reference month
    (e.g. ``date(2026, 3, 1)`` for the March 2026 data). ``release_date``
    is the projected publish day (col 3, Monthly Data). Aggregation
    rows (Fiscal Year / Calendar Year) are filtered out before
    projection.
    """

    reference_date: date
    reference_label: str              # "March 2026"
    release_date: date
    report_url: str                   # per-release XML URL


_MONTH_ABBREVS: dict[str, int] = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5,
    "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "sep": 9, "sept": 9,
    "oct": 10, "nov": 11, "dec": 12,
}
_MONTH_CELL_RE = re.compile(r"^(?P<month>[A-Za-z]{3,5})\.?$", re.IGNORECASE)
_DATE_CELL_RE = re.compile(
    r"(?P<month>[A-Za-z]{3,5})\.?\s*(?P<day>\d{1,2})\s*(?:,\s*(?P<year>\d{4}))?"
)
# Rows we ignore even though they appear in the same tbody: fiscal-
# year and calendar-year aggregations publish on the same day as the
# matching month (the final month of the period) and duplicating them
# would double-count the calendar event.
_AGGREGATION_ROW_MARKERS = ("fiscal year", "calendar year")


def _resolve_month(token: str) -> int:
    key = token.strip().lower().rstrip(".")
    month = _MONTH_ABBREVS.get(key)
    if month is None:
        raise MofCalendarParseError(f"unknown MoF month token: {token!r}")
    return month


_MONTH_CELL_SKIP = object()


def _parse_month_cell(text: str):
    """Classify a month cell.

    Returns one of:

    - ``_MONTH_CELL_SKIP`` when the cell is legitimately non-data:
      empty (header rows) or an aggregation-row label (Fiscal /
      Calendar Year) that would duplicate a matching monthly row.
    - An ``int`` 1..12 when the cell parses as a month.

    Anything else raises :class:`MofCalendarParseError` so DOM drift
    (e.g. BoJ swapping ``Feb.`` → ``February`` or injecting a new
    row kind) surfaces loudly rather than silently dropping a
    release from the calendar.
    """
    cleaned = text.replace("\xa0", " ").strip()
    if not cleaned:
        return _MONTH_CELL_SKIP
    lowered = cleaned.lower()
    if any(marker in lowered for marker in _AGGREGATION_ROW_MARKERS):
        return _MONTH_CELL_SKIP
    match = _MONTH_CELL_RE.match(cleaned)
    if match is None:
        raise MofCalendarParseError(
            f"MoF month cell unrecognised shape: {text!r}"
        )
    return _resolve_month(match.group("month"))


def _parse_release_date(
    text: str, *, reference_year: int, reference_month: int,
) -> date:
    """Parse a release-date cell.

    Two shapes:

    - ``"Jan.29"`` — implicit year. Release-month < reference-month
      means the release fell into the next calendar year (Dec-
      reference → Jan-release is January of reference_year + 1).
    - ``"Jan.28,2027"`` — explicit year overrides the implicit
      rule (used for rows in a rowspan group whose release year
      differs from the row-year heading).
    """
    cleaned = text.replace("\xa0", " ").strip()
    match = _DATE_CELL_RE.search(cleaned)
    if match is None:
        raise MofCalendarParseError(
            f"MoF release-date cell unparseable: {text!r}"
        )
    release_month = _resolve_month(match.group("month"))
    day = int(match.group("day"))
    explicit_year = match.group("year")
    if explicit_year is not None:
        year = int(explicit_year)
    else:
        # Release-year = reference-year + 1 when the release month
        # wraps past year-end (Dec → Jan).
        year = reference_year + (1 if release_month < reference_month else 0)
    try:
        return date(year=year, month=release_month, day=day)
    except ValueError as exc:
        raise MofCalendarParseError(
            f"invalid MoF release date in {text!r}"
        ) from exc


def _extract_row_year(cells: list[Any], row_year: int) -> tuple[int, list[Any]]:
    """Pop a year cell off the row if present.

    The tbody rows carry a ``<th rowspan=N>YYYY</th>`` on the first
    row of each year group. Subsequent rows in the same group lack
    that cell. The caller carries ``row_year`` forward so we can
    resolve subsequent rows without a leading year cell.
    """
    # ``cells`` is a mixed list of ``<th>`` and ``<td>`` elements in
    # document order. Recognise a year cell by a pure 4-digit
    # integer inside a ``<th>`` — the table uses that same header
    # to carry the year number for the entire group.
    if cells and cells[0].name == "th":
        text = cells[0].get_text(" ", strip=True)
        if re.match(r"^\d{4}$", text):
            return int(text), cells[1:]
    return row_year, cells


def _find_provisional_table(soup: BeautifulSoup):
    """Return the first table whose caption names the Provisional
    release surface. Skip the Revised/Fixed and Other Trade tables
    that follow on the same page."""
    for table in soup.find_all("table"):
        caption = table.find("caption")
        if caption is None:
            continue
        caption_text = caption.get_text(" ", strip=True).lower()
        if "provisional" in caption_text and "trade statistics" in caption_text:
            return table
    return None


def parse_mof_calendar_html(
    html: str, *, today: date | None = None,
) -> list[MofCalendarEntry]:
    """Extract Balance-of-Trade release rows from MoF calendar HTML.

    Walks the Provisional table's ``<tbody>``, tracks the current
    rowspan year, skips Fiscal/Calendar-Year aggregations, and
    picks column 3 (Monthly Data) as the release date.

    ``today`` is accepted but currently unused — kept on the
    signature for parity with the other calendar scrapers that
    sometimes clip future-year rows based on today's date.
    """
    del today  # unused; accepted for parity with other scrapers.
    soup = BeautifulSoup(html, "html.parser")
    table = _find_provisional_table(soup)
    if table is None:
        raise MofCalendarParseError(
            "MoF Provisional calendar table not found"
        )
    tbody = table.find("tbody")
    if tbody is None:
        raise MofCalendarParseError(
            "MoF Provisional calendar table has no <tbody>"
        )

    entries: list[MofCalendarEntry] = []
    current_year: int = 0
    for row in tbody.find_all("tr", recursive=False):
        cells = row.find_all(["th", "td"], recursive=False)
        if len(cells) < 2:
            continue
        current_year, cells = _extract_row_year(cells, current_year)
        if current_year <= 0:
            continue
        # After the year pop, the first remaining cell is the month
        # label; the next four are First-10 / First-20 / Monthly / Detailed.
        month_cell = cells[0]
        month_result = _parse_month_cell(month_cell.get_text(" ", strip=True))
        if month_result is _MONTH_CELL_SKIP:
            continue
        month_num = month_result
        date_cells = cells[1:]
        if len(date_cells) < 3:
            raise MofCalendarParseError(
                f"MoF monthly row has too few date cells "
                f"(year={current_year}, month={month_num}, got {len(date_cells)})"
            )
        monthly_text = date_cells[2].get_text(" ", strip=True)
        if not _DATE_CELL_RE.search(monthly_text):
            raise MofCalendarParseError(
                f"MoF Monthly Data cell unparseable "
                f"(year={current_year}, month={month_num}): {monthly_text!r}"
            )
        reference_date = date(year=current_year, month=month_num, day=1)
        release_date = _parse_release_date(
            monthly_text,
            reference_year=current_year,
            reference_month=month_num,
        )
        reference_label = reference_date.strftime("%B %Y")
        entries.append(
            MofCalendarEntry(
                reference_date=reference_date,
                reference_label=reference_label,
                release_date=release_date,
                report_url=build_trade_report_url(reference_date),
            )
        )
    return entries


# ──────────────────────────────────────────────────────────────────────────
# Schedule-side projection
# ──────────────────────────────────────────────────────────────────────────


_HASH_FIELDS: tuple[str, ...] = (
    "reference_date", "release_date", "event_time_utc",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def schedule_entry_to_records(
    entry: MofCalendarEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    specs: Iterable[MofIndicatorSpec] | None = None,
) -> list[tuple[MofCalendarRawRecord, MofCalendarEventRecord]]:
    """Project one schedule entry to ``(raw, event)`` tuples."""
    resolved_specs = list(specs) if specs is not None else list(
        INDICATOR_REGISTRY.values()
    )
    scheduled = parse_scheduled_release_time(
        entry.release_date,
        MOF_RELEASE_TIME_LOCAL,
        default_tz=MOF_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()
    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )

    out: list[tuple[MofCalendarRawRecord, MofCalendarEventRecord]] = []
    for spec in resolved_specs:
        indicator_canonical = canonicalize_indicator(spec.indicator)
        provider_event_id = synthesize_event_id(
            PROVIDER,
            spec.country_code,
            indicator_canonical,
            entry.reference_date.isoformat(),
        )
        payload: dict[str, Any] = {
            "kind":             "mof_trade_schedule",
            "indicator":        spec.indicator,
            "reference_date":   entry.reference_date.isoformat(),
            "reference_label":  entry.reference_label,
            "release_date":     entry.release_date.isoformat(),
            "report_url":       entry.report_url,
            "event_time_utc":   event_time_utc,
        }
        content_hash = _content_hash(payload)
        payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        raw_record = MofCalendarRawRecord(
            provider=PROVIDER,
            provider_event_id=provider_event_id,
            snapshot_epoch_ms=snapshot_epoch_ms,
            content_hash=content_hash,
            payload_json=payload_json,
            fetched_at=fetched_at,
        )
        event_record = MofCalendarEventRecord(
            provider=PROVIDER,
            provider_event_id=provider_event_id,
            event_time_utc=event_time_utc,
            event_time_precision="datetime",
            reference_date=entry.reference_date.isoformat(),
            reference_label=entry.reference_label,
            country_code=spec.country_code,
            indicator_id=None,
            category=spec.category,
            title=spec.title,
            importance=spec.importance,
            # BoT is denominated in yen; the trader lens wants JPY
            # on the currency filter so downstream JPY watchlists
            # pick the event up.
            currency="JPY",
            unit=spec.unit,
            actual=None,
            previous=None,
            revised=None,
            forecast=None,
            consensus_forecast=None,
            ticker="",
            source="Ministry of Finance Japan",
            # Point source_url at the per-release report URL so a
            # value-side re-seed doesn't flip provenance back to the
            # calendar index. Same pattern as BoJ Tankan P1a.
            source_url=entry.report_url,
            content_hash=content_hash,
            last_update_epoch_ms=None,
            observed_at_epoch_ms=observed,
        )
        out.append((raw_record, event_record))
    return out


# ──────────────────────────────────────────────────────────────────────────
# HTTP fetch
# ──────────────────────────────────────────────────────────────────────────


_MOF_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.1 Safari/605.1.15"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def fetch_mof_calendar_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the MoF release calendar page and return HTML text.

    The page advertises ``charset=ISO-8859-1`` in its ``<meta>`` and
    Content-Type; we defer to :attr:`requests.Response.text` which
    honours the response charset rather than hard-coding UTF-8.
    Forcing UTF-8 would replace any non-ASCII byte with ``�`` and
    break ``_find_provisional_table`` / ``_parse_month_cell`` on
    future encoding-sensitive DOM changes.
    """
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            MOF_CALENDAR_URL,
            headers=_MOF_BROWSER_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()
