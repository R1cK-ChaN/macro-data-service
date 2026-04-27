"""Bank of England Bank Rate history page → calendar projection.

The page at ``boeapps/database/Bank-Rate.asp`` carries a single
HTML table::

    Date Changed | Rate
    18 Dec 25    | 3.75
    07 Aug 25    | 4.00
    08 May 25    | 4.25
    ...

Each row is one MPC rate-change decision. Hold decisions are
absent — the page only lists changes — but every row is
guaranteed to be a real announcement-day rate. The MPC announces
decisions at **12:00 UK time** on the meeting day; this is the
shape traders price against.

``provider_event_id`` anchors on the announcement-day ISO date so
the id stays stable across schedule → value upgrade cycles
(future slices may pre-seed forward MPC dates from a separate
source; the value-side write must upsert onto the same id).

DOM drift (table missing, header row mis-named) raises
:class:`BoERatePageParseError` rather than silently dropping rows
— a single empty fetch on a Bank-Rate page is a layout-change
signal we want loud.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import BoEIndicatorSpec, INDICATOR_REGISTRY

PROVIDER = "boe"
BOE_RELEASE_TZ = "Europe/London"
BOE_RELEASE_TIME = "12:00"
BOE_BANK_RATE_URL = (
    "https://www.bankofengland.co.uk/boeapps/database/Bank-Rate.asp"
)


class BoERatePageParseError(ValueError):
    """Raised when the Bank Rate page exposes zero parseable rows."""


@dataclass(frozen=True)
class BoEMpcDecision:
    """One MPC Bank Rate decision parsed off the history page."""

    effective_date: date     # announcement / effective day
    rate: str                # decimal string ("3.75")


@dataclass(frozen=True)
class BoECalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class BoECalendarEventRecord:
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


_MONTH_ABBREVS: dict[str, int] = {
    "jan": 1,  "feb": 2,  "mar": 3,  "apr": 4,
    "may": 5,  "jun": 6,  "jul": 7,  "aug": 8,
    "sep": 9,  "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Date cell shape: ``"18 Dec 25"`` (zero-padded day, three-letter
# month, two-digit year). Spotted on a 2026-04-26 live capture.
_DATE_CELL_RE = re.compile(
    r"^\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{2,4})\s*$"
)
# Two-digit years on the BoE table go back to the 1970s. Anything
# 70-99 lives in the 1900s; 00-69 in the 2000s. Bank Rate started
# at 7.00% in 1694, but the table's earliest row is 1975 so the
# century cut-off below is safe in practice.
_TWO_DIGIT_PIVOT = 70


def _resolve_year(year_token: int) -> int:
    if year_token >= 100:
        return year_token
    if year_token >= _TWO_DIGIT_PIVOT:
        return 1900 + year_token
    return 2000 + year_token


def _parse_date_cell(text: str) -> date:
    match = _DATE_CELL_RE.match(text.strip())
    if not match:
        raise BoERatePageParseError(f"unparseable BoE date cell: {text!r}")
    day = int(match.group(1))
    month = _MONTH_ABBREVS.get(match.group(2)[:4].lower())
    if month is None:
        month = _MONTH_ABBREVS.get(match.group(2)[:3].lower())
    if month is None:
        raise BoERatePageParseError(f"unknown BoE month token: {text!r}")
    year_token = int(match.group(3))
    year = _resolve_year(year_token)
    try:
        return date(year, month, day)
    except ValueError as exc:
        raise BoERatePageParseError(
            f"invalid BoE date cell {text!r}"
        ) from exc


def _parse_rate_cell(text: str) -> str:
    cleaned = text.strip()
    if not cleaned:
        raise BoERatePageParseError("empty BoE rate cell")
    try:
        Decimal(cleaned)
    except InvalidOperation as exc:
        raise BoERatePageParseError(f"unparseable BoE rate {text!r}") from exc
    return cleaned


def parse_bank_rate_html(html: str | bytes) -> list[BoEMpcDecision]:
    """Walk the Bank Rate history table for parseable decision rows.

    Returns the decisions ordered most-recent-first (matches the
    upstream table's order). Raises :class:`BoERatePageParseError`
    when zero rows parse — DOM drift or anti-bot interstitial.
    """
    text = (
        html.decode("utf-8", errors="replace")
        if isinstance(html, (bytes, bytearray)) else html
    )
    soup = BeautifulSoup(text, "html.parser")
    decisions: list[BoEMpcDecision] = []
    seen: set[date] = set()
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        # Skip the table only if its header doesn't carry "Bank Rate"
        # nor "Rate" — the page renders two minor tables at the
        # bottom (chart legend, related links) without those headers.
        header_text = " ".join(
            c.get_text(" ", strip=True) for c in rows[0].find_all(["th", "td"])
        ).lower()
        if "rate" not in header_text or "date" not in header_text:
            continue
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            date_cell = cells[0].get_text(" ", strip=True)
            rate_cell = cells[1].get_text(" ", strip=True)
            if not date_cell or not rate_cell:
                continue
            try:
                effective = _parse_date_cell(date_cell)
                rate = _parse_rate_cell(rate_cell)
            except BoERatePageParseError:
                # Skip a single malformed row but keep walking — a
                # historical artefact (e.g. footnote row) shouldn't
                # nuke the entire table parse.
                continue
            if effective in seen:
                continue
            seen.add(effective)
            decisions.append(
                BoEMpcDecision(effective_date=effective, rate=rate),
            )
    if not decisions:
        raise BoERatePageParseError(
            "BoE Bank Rate page parsed zero decision rows — DOM drift "
            "or anti-bot interstitial"
        )
    decisions.sort(key=lambda d: d.effective_date, reverse=True)
    return decisions


_HASH_FIELDS: tuple[str, ...] = (
    "effective_date", "rate",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def decision_to_records(
    decision: BoEMpcDecision,
    *,
    snapshot_epoch_ms: int,
    spec: BoEIndicatorSpec | None = None,
) -> tuple[BoECalendarRawRecord, BoECalendarEventRecord]:
    """Project a :class:`BoEMpcDecision` to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY["BOE_RATE"]

    scheduled = parse_scheduled_release_time(
        decision.effective_date,
        BOE_RELEASE_TIME,
        default_tz=BOE_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        decision.effective_date.isoformat(),
    )

    reference_label = decision.effective_date.strftime("%B %Y")
    payload: dict[str, Any] = {
        "kind":           "boe_bank_rate_change",
        "effective_date": decision.effective_date.isoformat(),
        "rate":           decision.rate,
        "event_time_utc": event_time_utc,
        "source_url":     BOE_BANK_RATE_URL,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = BoECalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = BoECalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=decision.effective_date.isoformat(),
        reference_label=reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="GBP",
        unit=resolved_spec.unit,
        actual=decision.rate,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Bank of England",
        source_url=BOE_BANK_RATE_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "BOE_BANK_RATE_URL",
    "BOE_RELEASE_TIME",
    "BOE_RELEASE_TZ",
    "BoECalendarEventRecord",
    "BoECalendarRawRecord",
    "BoEMpcDecision",
    "BoERatePageParseError",
    "PROVIDER",
    "decision_to_records",
    "parse_bank_rate_html",
]
