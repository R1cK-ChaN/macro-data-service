"""Scrape ``esri.cao.go.jp/en/stat/stat-schedule-e.html``.

The ESRI release-schedule page exposes a single five-column table::

    ┌──────────────┬──────────────┬─────────────┬──────────────┬─────────────┐
    │ Indexes of   │ Indexes of   │ Machinery   │ Consumer     │ Business    │
    │ Business     │ Business     │ Orders      │ Confidence   │ Outlook     │
    │ Conditions   │ Conditions   │             │ Survey       │ Survey      │
    │ (Preliminary)│ (Revision)   │             │              │             │
    ├──────────────┼──────────────┼─────────────┼──────────────┼─────────────┤
    │ May 12,2026  │ Apr.27,2026  │ May 21,2026 │ Apr.30,2026  │ Jun.11,2026 │
    │   (Mar.)     │   (Feb.)     │   (Mar.)    │   (Apr.)     │   (Apr.-Jun.)│
    │ Jun.5        │ May 26       │ Jun.17      │ May 29       │ Sep.11      │
    │   (Apr.)     │   (Mar.)     │   (Apr.)    │   (May)      │   (Jul.-Sep.)│
    │ ...                                                                    │
    └──────────────┴──────────────┴─────────────┴──────────────┴─────────────┘

We project only the **Consumer Confidence Survey** column (index 3)
for P3. The other four surfaces (BC Preliminary / BC Revision /
Machinery Orders / Business Outlook) ride different release-times
and canonicals; they land in follow-on slices.

Release dates explicit on the first tbody row (``May 12,2026``) and
implicit on subsequent rows (``Jun.5``). Year-aware resolution
matches the MoF pattern: when the release-month number is less than
the previous row's release-month, bump the year by one; otherwise
the last-seen year carries through.

Reference-month cell is the second line of the cell (``(Apr.)``).
When the release month is January and the reference month is
December the reference-date year is release-year − 1; otherwise
they share a year. Consumer Confidence survey fieldwork always
references a single month, so quarter-style ``(Apr.-Jun.)`` cells
(Business Outlook only) never appear in the column we parse and
would raise :class:`CaoCalendarParseError` as a sanity guard.

Fetch + parse are separable — tests feed fixture HTML directly to
:func:`parse_cao_schedule_html`; live callers use
:func:`fetch_cao_schedule_html` with a browser-UA bundle.
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

from .indicators import CaoIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    CAO_CONSUMER_CONFIDENCE_RELEASE_TIME_LOCAL,
    CAO_CONSUMER_CONFIDENCE_URL,
    CAO_ESRI_SCHEDULE_URL,
    CAO_RELEASE_TZ,
    PROVIDER,
    CaoCalendarEventRecord,
    CaoCalendarRawRecord,
)

logger = logging.getLogger(__name__)


class CaoCalendarParseError(ValueError):
    """Raised when the ESRI schedule DOM deviates from the expected shape.

    Loud-fail is deliberate — silently dropping Consumer Confidence
    rows would leave the calendar sparse until a trader noticed a
    missing release.
    """


@dataclass(frozen=True)
class CaoConsumerConfidenceEntry:
    """One Consumer Confidence release row parsed from the schedule.

    ``reference_date`` uses the first day of the reference month
    (e.g. ``date(2026, 4, 1)`` for the April 2026 survey).
    ``release_date`` is the projected publish day.
    """

    reference_date: date
    reference_label: str              # "April 2026"
    release_date: date


# ``stat-schedule-e.html`` keeps Consumer Confidence in the fourth
# column (0-based 3). Resolved dynamically from the ``<thead>`` so
# column reordering surfaces as a loud parse error rather than a
# silent off-by-one.
_CONSUMER_CONFIDENCE_HEADER_FRAGMENT = "consumer confidence"

_MONTH_ABBREVS: dict[str, int] = {
    "jan": 1,  "feb": 2,  "mar": 3,  "apr": 4,
    "may": 5,  "jun": 6,  "june": 6,
    "jul": 7,  "july": 7,
    "aug": 8,  "sep": 9,  "sept": 9,
    "oct": 10, "nov": 11, "dec": 12,
}

# Release-date shape covers both explicit and implicit years:
#   "May 12,2026"   → month=May, day=12, year=2026
#   "Apr.27,2026"   → month=Apr, day=27, year=2026
#   "Jun.5"         → month=Jun, day=5, year=None (implicit)
_DATE_RE = re.compile(
    r"(?P<month>[A-Za-z]{3,5})\.?\s*(?P<day>\d{1,2})"
    r"(?:\s*,\s*(?P<year>\d{4}))?"
)
# Reference-month shape is always ``(<Month>[.])``. A range like
# ``(Apr.-Jun.)`` is quarterly (Business Outlook Survey only) and
# must not appear in the Consumer Confidence column — we fail loud
# if it ever does.
_REFERENCE_RE = re.compile(
    r"\(\s*(?P<month>[A-Za-z]{3,5})\.?\s*\)"
)
_REFERENCE_RANGE_RE = re.compile(
    r"\(\s*[A-Za-z]{3,5}\.?\s*[-–—]\s*[A-Za-z]{3,5}"
)


def _resolve_month(token: str) -> int:
    key = token.strip().lower().rstrip(".")
    month = _MONTH_ABBREVS.get(key)
    if month is None:
        raise CaoCalendarParseError(f"unknown CAO month token: {token!r}")
    return month


def _find_consumer_confidence_column(table) -> int:
    """Return the 0-based ``<td>`` index of the Consumer Confidence header.

    Walks the ``<thead>`` row and matches the column whose header text
    contains ``"consumer confidence"`` case-insensitively. Raises
    :class:`CaoCalendarParseError` when absent so a future header
    rename or column reorder surfaces as a loud parse error.
    """
    thead = table.find("thead")
    if thead is None:
        raise CaoCalendarParseError("ESRI schedule table has no <thead>")
    header_row = thead.find("tr")
    if header_row is None:
        raise CaoCalendarParseError(
            "ESRI schedule <thead> has no header <tr>"
        )
    headers = header_row.find_all("th")
    for idx, th in enumerate(headers):
        text = th.get_text(" ", strip=True).lower()
        if _CONSUMER_CONFIDENCE_HEADER_FRAGMENT in text:
            return idx
    raise CaoCalendarParseError(
        "ESRI schedule <thead> missing Consumer Confidence column"
    )


def _find_schedule_table(soup: BeautifulSoup):
    """Return the first ``<table>`` whose header row carries the
    Consumer Confidence column label. The page has a single table in
    the documented shape; picking by header fragment survives future
    layout changes that add surrounding tables."""
    for table in soup.find_all("table"):
        thead = table.find("thead")
        if thead is None:
            continue
        header_text = thead.get_text(" ", strip=True).lower()
        if _CONSUMER_CONFIDENCE_HEADER_FRAGMENT in header_text:
            return table
    return None


def _parse_release_cell(
    text: str,
    *,
    carry_year: int | None,
    last_release_month: int | None,
) -> tuple[date, str]:
    """Parse one Consumer Confidence cell into ``(release_date, reference_label)``.

    ``text`` is the cell's concatenated text (date + ``(Month)``
    reference line, typically separated by a ``<br>`` that BS4 flattens
    to whitespace). ``carry_year`` is the last-seen release year from
    a prior row in the same tbody — used when the current cell omits
    its year. ``last_release_month`` is the prior row's release-month
    (1..12) so the caller can detect a Dec→Jan implicit-year wrap.
    """
    cleaned = text.replace("\xa0", " ").strip()
    if not cleaned:
        raise CaoCalendarParseError("empty Consumer Confidence cell")

    if _REFERENCE_RANGE_RE.search(cleaned):
        raise CaoCalendarParseError(
            f"Consumer Confidence cell carries a month range "
            f"(Business Outlook shape?): {text!r}"
        )

    date_match = _DATE_RE.search(cleaned)
    if date_match is None:
        raise CaoCalendarParseError(
            f"Consumer Confidence release-date unparseable: {text!r}"
        )
    release_month = _resolve_month(date_match.group("month"))
    release_day = int(date_match.group("day"))
    explicit_year = date_match.group("year")
    if explicit_year is not None:
        release_year = int(explicit_year)
    elif carry_year is not None:
        # Dec→Jan wrap: if this row's release month is less than the
        # prior row's release month the table has crossed a calendar
        # year boundary and the implicit year must bump by one. Only
        # the first tbody row is obliged to carry an explicit year, so
        # without this check ``Jan.15`` after ``Dec.10,2026`` would
        # land as ``2026-01-15`` and the connector would write the
        # next January release under a stale provider_event_id.
        release_year = carry_year
        if last_release_month is not None and release_month < last_release_month:
            release_year = carry_year + 1
    else:
        raise CaoCalendarParseError(
            f"Consumer Confidence row omits year and no explicit "
            f"year seen yet: {text!r}"
        )
    try:
        release_date = date(
            year=release_year, month=release_month, day=release_day,
        )
    except ValueError as exc:
        raise CaoCalendarParseError(
            f"invalid Consumer Confidence release date in {text!r}"
        ) from exc

    ref_match = _REFERENCE_RE.search(cleaned)
    if ref_match is None:
        raise CaoCalendarParseError(
            f"Consumer Confidence reference-month unparseable: {text!r}"
        )
    reference_month = _resolve_month(ref_match.group("month"))
    # A January release references December of the prior year.
    reference_year = release_year - 1 if (
        release_month == 1 and reference_month == 12
    ) else release_year
    reference_date = date(year=reference_year, month=reference_month, day=1)
    reference_label = reference_date.strftime("%B %Y")

    # Cross-check: Consumer Confidence survey fieldwork is always
    # within the month immediately prior to the release month (or
    # the release month itself, e.g. ``Apr.30 → Apr.``). A gap > 1
    # month hints at DOM drift (we parsed the wrong column).
    gap = (release_year * 12 + release_month) - (
        reference_year * 12 + reference_month
    )
    if gap not in (0, 1):
        raise CaoCalendarParseError(
            f"Consumer Confidence reference/release gap "
            f"implausible ({gap} months): {text!r}"
        )

    return release_date, reference_label


def parse_cao_schedule_html(
    html: str, *, today: date | None = None,
) -> list[CaoConsumerConfidenceEntry]:
    """Extract Consumer Confidence release rows from the ESRI schedule HTML.

    ``today`` is accepted for parity with the other schedule scrapers;
    currently unused since the page only enumerates forward-looking
    releases.
    """
    del today  # unused; accepted for scraper-shape parity.
    soup = BeautifulSoup(html, "html.parser")
    table = _find_schedule_table(soup)
    if table is None:
        raise CaoCalendarParseError(
            "ESRI schedule: Consumer Confidence table not found"
        )
    column_idx = _find_consumer_confidence_column(table)
    tbody = table.find("tbody")
    if tbody is None:
        raise CaoCalendarParseError(
            "ESRI schedule table has no <tbody>"
        )

    entries: list[CaoConsumerConfidenceEntry] = []
    carry_year: int | None = None
    last_release_month: int | None = None
    seen_refs: set[date] = set()
    for row in tbody.find_all("tr", recursive=False):
        cells = row.find_all(["td", "th"], recursive=False)
        if len(cells) <= column_idx:
            continue
        cell_text = cells[column_idx].get_text(" ", strip=True)
        if not cell_text or cell_text.replace("\xa0", " ").strip() == "":
            # Trailing empty cells are legitimate when the table has
            # a ragged right edge (Business Outlook Survey's quarterly
            # rows leave gaps); skip silently for the CC column.
            continue
        try:
            release_date, reference_label = _parse_release_cell(
                cell_text,
                carry_year=carry_year,
                last_release_month=last_release_month,
            )
        except CaoCalendarParseError:
            raise
        carry_year = release_date.year
        last_release_month = release_date.month
        # Parse the reference-date once more from the label (cheap;
        # keeps _parse_release_cell's return shape tight).
        reference_date = datetime.strptime(reference_label, "%B %Y").date()
        if reference_date in seen_refs:
            # Duplicate reference month inside a single table pass
            # would mean the schedule lists the same survey twice —
            # shouldn't happen, but fail loud if it ever does.
            raise CaoCalendarParseError(
                f"duplicate Consumer Confidence reference month: "
                f"{reference_label}"
            )
        seen_refs.add(reference_date)
        entries.append(
            CaoConsumerConfidenceEntry(
                reference_date=reference_date,
                reference_label=reference_label,
                release_date=release_date,
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
    entry: CaoConsumerConfidenceEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    specs: Iterable[CaoIndicatorSpec] | None = None,
) -> list[tuple[CaoCalendarRawRecord, CaoCalendarEventRecord]]:
    """Project one schedule entry to ``(raw, event)`` tuples."""
    resolved_specs = list(specs) if specs is not None else list(
        INDICATOR_REGISTRY.values()
    )
    scheduled = parse_scheduled_release_time(
        entry.release_date,
        CAO_CONSUMER_CONFIDENCE_RELEASE_TIME_LOCAL,
        default_tz=CAO_RELEASE_TZ,
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

    out: list[tuple[CaoCalendarRawRecord, CaoCalendarEventRecord]] = []
    for spec in resolved_specs:
        indicator_canonical = canonicalize_indicator(spec.indicator)
        provider_event_id = synthesize_event_id(
            PROVIDER,
            spec.country_code,
            indicator_canonical,
            entry.reference_date.isoformat(),
        )
        payload: dict[str, Any] = {
            "kind":             "cao_consumer_confidence_schedule",
            "indicator":        spec.indicator,
            "reference_date":   entry.reference_date.isoformat(),
            "reference_label":  entry.reference_label,
            "release_date":     entry.release_date.isoformat(),
            "event_time_utc":   event_time_utc,
        }
        content_hash = _content_hash(payload)
        payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        raw_record = CaoCalendarRawRecord(
            provider=PROVIDER,
            provider_event_id=provider_event_id,
            snapshot_epoch_ms=snapshot_epoch_ms,
            content_hash=content_hash,
            payload_json=payload_json,
            fetched_at=fetched_at,
        )
        event_record = CaoCalendarEventRecord(
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
            # Consumer Confidence is a points-based diffusion index,
            # not JPY-denominated. Match Tankan / ISM / U Michigan.
            currency="",
            unit=spec.unit,
            actual=None,
            previous=None,
            revised=None,
            forecast=None,
            consensus_forecast=None,
            ticker="",
            source="Cabinet Office Japan (ESRI)",
            # Point ``source_url`` at the per-indicator landing page
            # (shouhi-e.html), not the schedule index. Value-side
            # writes share the same URL so a schedule re-seed never
            # flips historical provenance back to the schedule page.
            source_url=CAO_CONSUMER_CONFIDENCE_URL,
            content_hash=content_hash,
            last_update_epoch_ms=None,
            observed_at_epoch_ms=observed,
        )
        out.append((raw_record, event_record))
    return out


# ──────────────────────────────────────────────────────────────────────────
# HTTP fetch
# ──────────────────────────────────────────────────────────────────────────


# Shared browser-UA headers — matching the MoF / BoJ bundles so a
# CAO WAF change that starts key-bouncing plain ``python-requests``
# doesn't silently brick the connector. Used by both the schedule
# scraper here and the value scraper in :mod:`surveys`.
CAO_BROWSER_HEADERS: dict[str, str] = {
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


def fetch_cao_schedule_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the ESRI release-schedule page and return HTML text.

    CAO's English schedule declares ``charset=UTF-8`` — we still defer
    to :attr:`requests.Response.text` so decoding follows the
    response's own Content-Type header rather than hard-coding.
    """
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            CAO_ESRI_SCHEDULE_URL,
            headers=CAO_BROWSER_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()
