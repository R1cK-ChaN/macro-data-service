"""Scrape ``boj.or.jp/en/statistics/tk/yoshi/index.htm``.

The "List of TANKAN (Outline)" page is a single ``<table class="js-tbl">``
whose ``<tbody>`` carries one row per quarterly Tankan release, paired
with the URL of the result page:

    <tr>
      <td>Apr.&nbsp;&nbsp;1,&nbsp;2026</td>
      <td><a href="/en/statistics/tk/yoshi/tk2603.htm">March 2026 Survey</a></td>
    </tr>

The page is strictly **past-looking** — a row appears only once the
corresponding Tankan has been released. Every row projected by this
scraper is therefore a past schedule row; the outline scrape
(:mod:`outlines`) fills ``actual`` a short way after in the same
scheduler tick.

Two indicators ship per release: ``TANKAN_LARGE_MFG`` and
``TANKAN_LARGE_NONMFG``. The schedule scrape projects both, keyed on
distinct ``provider_event_id`` values so the value-side writer can
upsert on them independently.

Fetch + parse are separable — tests feed fixture HTML directly to
:func:`parse_tankan_schedule_html`; live callers use
:func:`fetch_tankan_yoshi_index_html` which reuses the BoJ
browser-UA header bundle from :mod:`ingestion.calendar.boj_api.scraper`.
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
from ingestion.calendar.boj_api.scraper import _BOJ_BROWSER_HEADERS

from .indicators import BojTankanIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    PROVIDER,
    TANKAN_RELEASE_TIME_LOCAL,
    TANKAN_RELEASE_TZ,
    TANKAN_YOSHI_INDEX_URL,
    TankanCalendarEventRecord,
    TankanCalendarRawRecord,
    build_outline_url,
    reference_date_from_yymm,
)

logger = logging.getLogger(__name__)


class TankanScheduleParseError(ValueError):
    """Raised when the Tankan yoshi-index DOM deviates from the expected shape.

    Loud-fail is deliberate — DOM drift on a quarterly surface is the
    most likely breakage mode, and silently dropping rows would let
    the calendar miss a Tankan release until a trader notices it's
    absent.
    """


@dataclass(frozen=True)
class TankanScheduleEntry:
    """One Tankan release row parsed from the yoshi-index page.

    ``release_date`` is the day the survey results published (first
    column of the table). ``reference_date`` is the first day of the
    survey's reference month, resolved from the result-page YYMM code.
    ``outline_url`` is the result-page URL; the value-side scrape
    walks this list and fetches each.
    """

    release_date: date
    reference_date: date
    reference_label: str             # "March 2026 Survey"
    yymm: str                        # "2603"
    outline_url: str                 # absolute URL


# "Apr.  1, 2026" / "July  1, 2025" / "Dec. 15, 2024".
# BoJ uses NBSPs to right-align the day within the column; we collapse
# them to plain spaces before the regex fires. "Sep" vs "Sept" both
# appear in BoJ surfaces, so the month alternation matches 3–5 letters.
_MONTH_ABBREVS: dict[str, int] = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5,
    "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "sep": 9, "sept": 9,
    "oct": 10, "nov": 11, "dec": 12,
}
_RELEASE_DATE_RE = re.compile(
    r"(?P<month>[A-Za-z]{3,5})\.?\s*(?P<day>\d{1,2}),\s*(?P<year>\d{4})"
)
_OUTLINE_URL_RE = re.compile(r"/tk/yoshi/tk(?P<yymm>\d{4})\.htm$")


def _resolve_month(token: str) -> int:
    key = token.strip().lower().rstrip(".")
    month = _MONTH_ABBREVS.get(key)
    if month is None:
        raise TankanScheduleParseError(f"unknown Tankan month token: {token!r}")
    return month


def _parse_release_date(cell_text: str) -> date:
    """Extract the release date from a yoshi-index first-column cell."""
    normalized = cell_text.replace("\xa0", " ").strip()
    normalized = re.sub(r"\s+", " ", normalized)
    match = _RELEASE_DATE_RE.search(normalized)
    if match is None:
        raise TankanScheduleParseError(
            f"Tankan release-date cell unparseable: {cell_text!r}"
        )
    month = _resolve_month(match.group("month"))
    day = int(match.group("day"))
    year = int(match.group("year"))
    try:
        return date(year=year, month=month, day=day)
    except ValueError as exc:
        raise TankanScheduleParseError(
            f"invalid Tankan release date in {cell_text!r}"
        ) from exc


def parse_tankan_schedule_html(html: str) -> list[TankanScheduleEntry]:
    """Extract :class:`TankanScheduleEntry` rows from the yoshi index.

    Looks for anchor tags pointing at ``/tk/yoshi/tkYYMM.htm`` —
    that URL shape is stable and uniquely identifies Tankan result
    pages (sibling navigation links use different sub-paths). Each
    anchor's row-level ``<td>`` neighbours carry the release-date
    cell. Missing either the date or the anchor on a row raises
    :class:`TankanScheduleParseError` so DOM drift surfaces loudly.
    """
    soup = BeautifulSoup(html, "html.parser")
    entries: list[TankanScheduleEntry] = []
    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"]).strip()
        match = _OUTLINE_URL_RE.search(href)
        if match is None:
            continue
        yymm = match.group("yymm")
        # Walk up to the enclosing <tr> and pull the sibling date cell.
        row = anchor.find_parent("tr")
        if row is None:
            raise TankanScheduleParseError(
                f"Tankan outline anchor has no enclosing <tr>: {href!r}"
            )
        cells = row.find_all("td")
        if len(cells) < 2:
            raise TankanScheduleParseError(
                f"Tankan yoshi row missing date cell: {href!r}"
            )
        release_date = _parse_release_date(cells[0].get_text(" ", strip=True))
        try:
            reference_date = reference_date_from_yymm(yymm)
        except ValueError as exc:
            raise TankanScheduleParseError(str(exc)) from exc
        label = anchor.get_text(" ", strip=True).rstrip()
        outline_url = build_outline_url(reference_date)
        entries.append(
            TankanScheduleEntry(
                release_date=release_date,
                reference_date=reference_date,
                reference_label=label,
                yymm=yymm,
                outline_url=outline_url,
            )
        )
    return entries


# ──────────────────────────────────────────────────────────────────────────
# Schedule-side projection
# ──────────────────────────────────────────────────────────────────────────


# Hash inputs for revision detection. A later re-scrape that shifts
# the release date (rare — BoJ occasionally rescheduled Tankan during
# COVID) must register as a new raw audit row.
_HASH_FIELDS: tuple[str, ...] = (
    "release_date", "reference_date", "event_time_utc",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def schedule_entry_to_records(
    entry: TankanScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    specs: Iterable[BojTankanIndicatorSpec] | None = None,
) -> list[tuple[TankanCalendarRawRecord, TankanCalendarEventRecord]]:
    """Project one schedule entry to ``(raw, event)`` tuples.

    Emits one tuple per indicator in the registry (two by default:
    ``TANKAN_LARGE_MFG`` + ``TANKAN_LARGE_NONMFG``). Each indicator
    gets its own ``provider_event_id`` so the outline-side writer can
    upsert the DI value independently per sector.
    """
    resolved_specs = list(specs) if specs is not None else list(
        INDICATOR_REGISTRY.values()
    )
    scheduled = parse_scheduled_release_time(
        entry.release_date,
        TANKAN_RELEASE_TIME_LOCAL,
        default_tz=TANKAN_RELEASE_TZ,
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

    out: list[tuple[TankanCalendarRawRecord, TankanCalendarEventRecord]] = []
    for spec in resolved_specs:
        indicator_canonical = canonicalize_indicator(spec.indicator)
        provider_event_id = synthesize_event_id(
            PROVIDER,
            spec.country_code,
            indicator_canonical,
            entry.reference_date.isoformat(),
        )
        payload: dict[str, Any] = {
            "kind":             "boj_tankan_schedule",
            "indicator":        spec.indicator,
            "release_date":     entry.release_date.isoformat(),
            "reference_date":   entry.reference_date.isoformat(),
            "reference_label":  entry.reference_label,
            "yymm":             entry.yymm,
            "outline_url":      entry.outline_url,
            "event_time_utc":   event_time_utc,
        }
        content_hash = _content_hash(payload)
        payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
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
            reference_date=entry.reference_date.isoformat(),
            reference_label=entry.reference_label,
            country_code=spec.country_code,
            indicator_id=None,
            category=spec.category,
            title=spec.title,
            importance=spec.importance,
            # DI is a points-based sentiment index, not a JPY-denominated
            # release. Matches Conference Board / U Michigan / ISM which
            # ship an empty currency. Stamping JPY here would leak into
            # the list_calendar_items currency filter.
            currency="",
            unit=spec.unit,
            actual=None,
            previous=None,
            revised=None,
            forecast=None,
            consensus_forecast=None,
            ticker="",
            source="Bank of Japan",
            # Point source_url at the release's own outline page, not
            # the yoshi index. The two-phase projector re-asserts this
            # URL on every schedule pass; pointing at the outline page
            # means a value-side sweep that re-seeds the schedule
            # doesn't clobber the canonical per-release source URL.
            source_url=entry.outline_url,
            content_hash=content_hash,
            last_update_epoch_ms=None,
            observed_at_epoch_ms=observed,
        )
        out.append((raw_record, event_record))
    return out


# ──────────────────────────────────────────────────────────────────────────
# HTTP fetch
# ──────────────────────────────────────────────────────────────────────────


def fetch_tankan_yoshi_index_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the Tankan yoshi-index page and return HTML text."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            TANKAN_YOSHI_INDEX_URL,
            headers=_BOJ_BROWSER_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.content.decode("utf-8")
    finally:
        if owned_session:
            s.close()
