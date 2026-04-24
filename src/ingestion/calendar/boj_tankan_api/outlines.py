"""Scrape Tankan outline pages for the Large-Enterprises DI.

Each quarterly Tankan publishes a results-outline page at
``boj.or.jp/en/statistics/tk/yoshi/tk<YYMM>.htm``. The page carries a
``<h2> Business Conditions</h2>`` section whose first sub-table
(``<h3>Large Enterprises</h3>``) is the calendar-impact table:

    ┌─────────────────┬ Dec 2025 Survey ┬ March 2026 Survey ┬ Δ ┬ June 2026 (Forecast) ┬ Δ ┐
    │ Manufacturing   │                 │ (15)              │   │                      │   │
    │                 │ 16              │ 17                │+1 │ 14                   │-3 │
    │ Nonmanufacturing│                 │ (31)              │   │                      │   │
    │                 │ 36              │ 36                │ 0 │ 29                   │-7 │
    └─────────────────┴─────────────────┴───────────────────┴───┴──────────────────────┴───┘

The current-quarter actual sits at ``row2[col=1]`` for each sector
(the second ``<tr>`` of the rowspan group, second ``<td>``). The
previous quarter's actual sits at ``row2[col=0]``. The parenthesised
value in ``row1[col=1]`` is the prior survey's forecast *for this
quarter* — BoJ's own 3-month-ahead projection, not an analyst
consensus. We carry it on the ``forecast`` field since Tankan's own
forecast is the most widely-quoted prior-expectation proxy when the
release drops.

Fetch + parse + project are separable: tests feed fixture HTML to
:func:`parse_outline_html`; live callers drive
:func:`fetch_outline_html`. :func:`outline_value_to_records` emits
one ``(raw, event)`` tuple per indicator whose ``provider_event_id``
matches the schedule-side write exactly (same ``(indicator,
reference_date)`` anchor), so the DI value upserts onto the existing
schedule row through the shared projector's merge CASE.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)
from ingestion.calendar.boj_api.scraper import _BOJ_BROWSER_HEADERS

from .indicators import BojTankanIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    PROVIDER,
    TANKAN_RELEASE_TIME_LOCAL,
    TANKAN_RELEASE_TZ,
    TankanCalendarEventRecord,
    TankanCalendarRawRecord,
    build_outline_url,
)

logger = logging.getLogger(__name__)


class TankanOutlineParseError(Exception):
    """Outline page didn't carry a parseable Large-Enterprises DI block."""


@dataclass(frozen=True)
class SectorDI:
    """Diffusion-Index triple for a single enterprise sector."""

    sector: str                       # "manufacturing" / "nonmanufacturing"
    current: int                      # DI for this survey
    previous: int                     # DI for the previous survey
    forecast_prior: int | None        # "(N)" in parens: prior survey's forecast for this quarter
    forecast_next: int | None         # this survey's forecast for the next quarter


@dataclass(frozen=True)
class OutlineValue:
    """Parsed Tankan outline outcome."""

    reference_date: date
    release_date: date | None         # from the schedule index or caller; may be None
    large_mfg: SectorDI
    large_nonmfg: SectorDI


_SECTOR_HEADER_NORMALIZE = {
    "manufacturing":    "manufacturing",
    "nonmanufacturing": "nonmanufacturing",
    "non-manufacturing": "nonmanufacturing",
}


def _cell_int(text: str) -> int | None:
    """Parse a BoJ Tankan DI cell.

    Cells come in four shapes:

    - ``"17"`` / ``"+1"`` / ``"-3"`` — signed or unsigned integer.
    - ``"( 15)"`` — parenthesised (the prior survey's forecast).
    - ``"-"`` / ``"\xa0"`` / empty — no data (returns ``None``).
    """
    cleaned = text.replace("\xa0", " ").replace("(", " ").replace(")", " ").strip()
    if not cleaned or cleaned in {"-", "—", "–"}:
        return None
    match = re.match(r"^([+\-]?\d+)$", cleaned)
    if match is None:
        return None
    return int(match.group(1))


def _normalize_sector_header(text: str) -> str | None:
    key = text.strip().lower()
    return _SECTOR_HEADER_NORMALIZE.get(key)


def _find_large_enterprises_table(soup: BeautifulSoup):
    """Return the ``<table>`` under the first "Large Enterprises" section
    of the Business-Conditions block.

    Walks the document in order, tracks the most recent ``<h2>`` /
    ``<h3>`` headings, and returns the next ``<table>`` encountered
    while under ``Business Conditions`` > ``Large Enterprises``. That
    disambiguation matters — the same page carries "Large Enterprises"
    sub-tables under later sections (Sales, Current Profits, …) whose
    column semantics are different.
    """
    current_h2 = ""
    current_h3 = ""
    for node in soup.find_all(["h2", "h3", "table"]):
        if node.name == "h2":
            current_h2 = node.get_text(" ", strip=True)
            current_h3 = ""
            continue
        if node.name == "h3":
            current_h3 = node.get_text(" ", strip=True)
            continue
        # ``node.name == "table"``
        if (
            "business conditions" in current_h2.lower()
            and current_h3.lower() == "large enterprises"
        ):
            return node
    return None


def _extract_sector_rows(table) -> dict[str, tuple[list[str], list[str]]]:
    """Pull ``(row1_texts, row2_texts)`` per sector from the DI table.

    Each sector occupies two ``<tr>`` under a ``rowspan="2"`` ``<th>``
    header. Row 1 carries the "(N)" forecast parens; row 2 carries
    the actuals. Returns a dict keyed by normalized sector name
    (``"manufacturing"`` / ``"nonmanufacturing"``). Sectors not
    recognised are skipped silently — future BoJ edits (e.g. adding
    "Mining") won't break the parse.
    """
    tbody = table.find("tbody")
    if tbody is None:
        return {}
    rows = tbody.find_all("tr", recursive=False)
    out: dict[str, tuple[list[str], list[str]]] = {}
    i = 0
    while i < len(rows):
        th = rows[i].find("th")
        if th is None:
            # Orphan row without header — skip forward.
            i += 1
            continue
        sector = _normalize_sector_header(th.get_text(" ", strip=True))
        if sector is None or i + 1 >= len(rows):
            i += 1
            continue
        row1_cells = [
            cell.get_text(" ", strip=True)
            for cell in rows[i].find_all("td")
        ]
        row2_cells = [
            cell.get_text(" ", strip=True)
            for cell in rows[i + 1].find_all("td")
        ]
        out[sector] = (row1_cells, row2_cells)
        i += 2
    return out


def _extract_sector_di(
    row1: list[str],
    row2: list[str],
    sector: str,
) -> SectorDI:
    """Assemble a :class:`SectorDI` from paired row cells.

    Column layout in the Large-Enterprises DI table:

    - row1: ``[blank, "(N)" forecast, blank, blank, blank]``
    - row2: ``[previous actual, current actual, Δ, next forecast, Δ]``
    """
    if len(row2) < 2:
        raise TankanOutlineParseError(
            f"Tankan Large-Enterprises {sector} row missing DI cells: "
            f"row2={row2!r}"
        )
    previous = _cell_int(row2[0])
    current = _cell_int(row2[1])
    forecast_next = _cell_int(row2[3]) if len(row2) >= 4 else None
    forecast_prior = _cell_int(row1[1]) if len(row1) >= 2 else None
    if previous is None or current is None:
        raise TankanOutlineParseError(
            f"Tankan Large-Enterprises {sector} DI cells unparseable: "
            f"previous={row2[0]!r} current={row2[1]!r}"
        )
    return SectorDI(
        sector=sector,
        current=current,
        previous=previous,
        forecast_prior=forecast_prior,
        forecast_next=forecast_next,
    )


def parse_outline_html(html: str, reference_date: date) -> OutlineValue:
    """Extract Large-Enterprises DI values from a Tankan outline page.

    Raises :class:`TankanOutlineParseError` when the page lacks the
    expected Business-Conditions > Large-Enterprises block or when
    either sector's DI cells are unparseable — upstream drift must
    surface loudly rather than silently emit ``None`` onto an
    existing schedule row.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = _find_large_enterprises_table(soup)
    if table is None:
        raise TankanOutlineParseError(
            "Business-Conditions Large-Enterprises table not found on "
            f"Tankan outline page (reference={reference_date.isoformat()})"
        )
    sectors = _extract_sector_rows(table)
    if "manufacturing" not in sectors or "nonmanufacturing" not in sectors:
        raise TankanOutlineParseError(
            "Tankan Large-Enterprises table missing Manufacturing or "
            f"Nonmanufacturing rows (reference={reference_date.isoformat()})"
        )
    mfg = _extract_sector_di(*sectors["manufacturing"], "manufacturing")
    nonmfg = _extract_sector_di(*sectors["nonmanufacturing"], "nonmanufacturing")
    return OutlineValue(
        reference_date=reference_date,
        release_date=None,
        large_mfg=mfg,
        large_nonmfg=nonmfg,
    )


# ──────────────────────────────────────────────────────────────────────────
# HTTP fetch
# ──────────────────────────────────────────────────────────────────────────


def fetch_outline_html(
    reference_date: date,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the Tankan outline page for a given survey reference month."""
    url = build_outline_url(reference_date)
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(url, headers=_BOJ_BROWSER_HEADERS, timeout=timeout)
        response.raise_for_status()
        return response.content.decode("utf-8")
    finally:
        if owned_session:
            s.close()


# ──────────────────────────────────────────────────────────────────────────
# Value-side projection
# ──────────────────────────────────────────────────────────────────────────


_HASH_FIELDS: tuple[str, ...] = (
    "indicator", "current", "previous", "forecast_prior", "event_time_utc",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _format_di(value: int) -> str:
    """Stringify a DI value preserving sign convention."""
    return f"{value:+d}" if value < 0 else str(value)


def outline_value_to_records(
    value: OutlineValue,
    *,
    snapshot_epoch_ms: int,
    release_date: date | None = None,
    event_time_utc: str | None = None,
    observed_at_epoch_ms: int | None = None,
) -> list[tuple[TankanCalendarRawRecord, TankanCalendarEventRecord]]:
    """Project an :class:`OutlineValue` to ``(raw, event)`` tuples.

    Emits one tuple per indicator (Large Mfg + Large Non-Mfg). The
    ``provider_event_id`` matches the schedule-side write exactly
    (same ``(indicator, reference_date)`` anchor) so the upsert lands
    on the existing row through the shared projector's merge CASE.

    Event-time resolution, in order:

    - ``event_time_utc`` (caller-supplied ISO string). Used verbatim.
      This is what the value-side auto-discovery path passes in —
      it reads the stamp off the already-stored schedule row and
      hands it through so the upsert leaves the canonical
      (schedule-side) publish clock untouched.
    - ``release_date`` (caller-supplied date). Projected through
      ``parse_scheduled_release_time`` with the 08:50 JST convention.
    - Fallback — first day of the month following the reference
      month. Tankan always releases a few weeks after the reference
      quarter closes; this keeps the stamped timestamp plausible on
      ad-hoc reruns without schedule-side context.
    """
    if event_time_utc is None:
        effective_release = (
            release_date if release_date is not None
            else _default_release_for_reference(value.reference_date)
        )
        scheduled = parse_scheduled_release_time(
            effective_release,
            TANKAN_RELEASE_TIME_LOCAL,
            default_tz=TANKAN_RELEASE_TZ,
        )
        event_time_utc = scheduled.utc.isoformat()
    else:
        effective_release = release_date

    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()
    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    reference_label = value.reference_date.strftime("%B %Y Survey")
    outline_url = build_outline_url(value.reference_date)

    out: list[tuple[TankanCalendarRawRecord, TankanCalendarEventRecord]] = []
    for sector_di, spec in _iter_indicator_pairs(value):
        payload: dict[str, Any] = {
            "kind":             "boj_tankan_outline",
            "indicator":        spec.indicator,
            "reference_date":   value.reference_date.isoformat(),
            "release_date":     (
                effective_release.isoformat() if effective_release else None
            ),
            "current":          sector_di.current,
            "previous":         sector_di.previous,
            "forecast_prior":   sector_di.forecast_prior,
            "forecast_next":    sector_di.forecast_next,
            "event_time_utc":   event_time_utc,
            "outline_url":      outline_url,
        }
        content_hash = _content_hash(payload)
        payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        indicator_canonical = canonicalize_indicator(spec.indicator)
        provider_event_id = synthesize_event_id(
            PROVIDER,
            spec.country_code,
            indicator_canonical,
            value.reference_date.isoformat(),
        )
        raw_record = TankanCalendarRawRecord(
            provider=PROVIDER,
            provider_event_id=provider_event_id,
            snapshot_epoch_ms=snapshot_epoch_ms,
            content_hash=content_hash,
            payload_json=payload_json,
            fetched_at=fetched_at,
        )
        event_record = TankanCalendarEventRecord(
            provider=PROVIDER,
            provider_event_id=provider_event_id,
            event_time_utc=event_time_utc,
            event_time_precision="datetime",
            reference_date=value.reference_date.isoformat(),
            reference_label=reference_label,
            country_code=spec.country_code,
            indicator_id=None,
            category=spec.category,
            title=spec.title,
            importance=spec.importance,
            # DI is a points-based sentiment index; empty currency
            # mirrors Conference Board / U Michigan / ISM.
            currency="",
            unit=spec.unit,
            actual=_format_di(sector_di.current),
            previous=_format_di(sector_di.previous),
            revised=None,
            forecast=(
                _format_di(sector_di.forecast_prior)
                if sector_di.forecast_prior is not None else None
            ),
            consensus_forecast=None,
            ticker="",
            source="Bank of Japan",
            source_url=outline_url,
            content_hash=content_hash,
            last_update_epoch_ms=None,
            observed_at_epoch_ms=observed,
        )
        out.append((raw_record, event_record))
    return out


def _iter_indicator_pairs(
    value: OutlineValue,
) -> list[tuple[SectorDI, BojTankanIndicatorSpec]]:
    """Pair each sector's DI with its registry spec."""
    return [
        (value.large_mfg,    INDICATOR_REGISTRY["TANKAN_LARGE_MFG"]),
        (value.large_nonmfg, INDICATOR_REGISTRY["TANKAN_LARGE_NONMFG"]),
    ]


def _default_release_for_reference(reference: date) -> date:
    """Synthesize a release day when the caller can't supply one.

    Tankan reference quarters release in the early days of the
    following quarter (April for March survey, July for June survey,
    October for September survey, mid-December for December survey —
    see ``boj.or.jp/en/statistics/tk/yoshi/``). Picking the first
    day of the month following the reference month keeps the
    stamped event-time plausible when we're projecting a value
    without the schedule-side context (ad-hoc single-url reruns).
    """
    year = reference.year + (1 if reference.month == 12 else 0)
    month = 1 if reference.month == 12 else reference.month + 1
    return date(year=year, month=month, day=1)
