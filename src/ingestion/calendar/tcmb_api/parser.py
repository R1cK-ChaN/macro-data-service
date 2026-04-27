"""TCMB 1-Week Repo rate-history HTML → calendar projection.

TCMB exposes the 1-Week Repo Auction Rate (the policy rate since
20 May 2010) as a static HTML table at
``tcmb.gov.tr/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Temel+Faaliyetler/
Para+Politikasi/Merkez+Bankasi+Faiz+Oranlari/1+Hafta+Repo``::

    <table id="midTable">
      <tbody>
        <tr class="MerkezBankasiTableHeader">
          <td>Tarih</td>
          <td>Borç Alma</td>     <!-- borrowing rate -->
          <td>Borç Verme</td>    <!-- lending rate = 1-week repo -->
        </tr>
        <tr class="zebra1">
          <td>20.05.2010</td>
          <td>-</td>
          <td>7.00</td>
        </tr>
        ...
      </tbody>
    </table>

The ``Borç Alma`` column is dashed because the 1-week repo is the
single relevant column on this page (the dual-rate corridor period
ended when the 1-week repo became the operational policy rate). The
parser reads the third column as the new policy rate; ``Borç Alma``
is preserved verbatim in the audit payload but does not influence
the projected value.

**Coverage is rate-change-only.** TCMB lists only meetings that
changed the rate on this surface; hold decisions are absent. A future
P2 slice can fold in ``/Duyurular/Basin/<year>/duy<year>-NN`` press
releases for hold-decision coverage and authoritative PPK
announcement dates.

Date format is ``DD.MM.YYYY`` (Turkish convention). The ``Tarih``
column is the **effective date** — the day the new rate takes effect
— which is the day after the PPK meeting for the modern Thursday-
meeting / Friday-effective cadence (e.g. 23 Jan 2026 effective
follows the 22 Jan 2026 PPK announcement). Older rows (2010-2014)
mix Thursday / Wednesday / Friday effective dates as TCMB shifted its
meeting schedule multiple times. The connector stores ``Tarih`` as
the calendar event's reference date verbatim; reconstructing the
exact PPK announcement date for every row needs the per-meeting
press release and is deferred to P2.

Time is set to 14:00 ``Europe/Istanbul`` — TCMB's documented
afternoon publication window. Türkiye observes UTC+3 year-round since
2016; ``parse_scheduled_release_time`` against ``Europe/Istanbul``
resolves the historical 2010-2016 DST window for the backfill.

``provider_event_id`` / ``event_time_utc`` / ``reference_date`` all
anchor on the effective date. Daily parity matching against TE
(announcement-date convention) is intentionally **deferred** — the
``(TR, TCMB_RATE)`` pair is not on the parity whitelist in P1
because (a) the off-by-one drift between effective and announcement
dates would generate false MissingRelease alerts and (b) the change-
only coverage means TE's hold rows have no agency counterpart, which
would compound the false alerts. Same deferral pattern as the BoC
Valet connector.
"""

from __future__ import annotations

import hashlib
import html as html_lib
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

from .indicators import INDICATOR_REGISTRY, TCMBIndicatorSpec

PROVIDER = "tcmb"
TCMB_RELEASE_TZ = "Europe/Istanbul"
# TCMB publishes the PPK decision after the meeting closes —
# documented as the afternoon of the meeting day. 14:00 TRT matches
# the practical announcement window observed across recent decisions
# (e.g. PPK 23.01.2026 announced ~14:00 TRT). Used as the default
# wall-clock release time when ``parse_scheduled_release_time``
# resolves the per-decision ``event_time_utc``.
TCMB_RELEASE_TIME = "14:00"
TCMB_BASE_URL = "https://www.tcmb.gov.tr"
TCMB_RATE_HISTORY_URL = (
    f"{TCMB_BASE_URL}/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Temel+Faaliyetler"
    f"/Para+Politikasi/Merkez+Bankasi+Faiz+Oranlari/1+Hafta+Repo"
)


class TCMBRateHistoryParseError(ValueError):
    """TCMB rate-history HTML did not expose a parseable rate table."""


@dataclass(frozen=True)
class TCMBRateDecision:
    """One PPK rate-change decision parsed from the 1-week repo table.

    ``effective_date`` is the ``Tarih`` column verbatim — the day the
    new rate takes effect, which under TCMB's modern cadence falls one
    business day *after* the PPK announcement. Reconstructing the
    exact announcement date for every backfill row needs the per-
    meeting press release; that scrape is deferred to P2.
    """

    effective_date: date
    rate: str                    # decimal string ("37.00")
    previous_rate: str | None    # rate before this decision (None for #1)
    borrowing_rate: str | None   # ``Borç Alma`` column verbatim ("-" → None)


@dataclass(frozen=True)
class TCMBCalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class TCMBCalendarEventRecord:
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


# Locate the rate-history table: ``<table id="midTable">``. Pinned to
# the literal id so a side table elsewhere on the page (footers /
# navigation) can't be mis-parsed as the rate history. ``DOTALL`` so
# multi-line cell content is captured.
_TABLE_RE = re.compile(
    r'<table[^>]*\bid\s*=\s*["\']midTable["\'][^>]*>'
    r'(?P<body>.*?)</table>',
    re.IGNORECASE | re.DOTALL,
)
_ROW_RE = re.compile(
    r'<tr[^>]*>(?P<cells>.*?)</tr>',
    re.IGNORECASE | re.DOTALL,
)
_TD_RE = re.compile(
    r'<td[^>]*>(?P<text>.*?)</td>',
    re.IGNORECASE | re.DOTALL,
)
_DATE_RE = re.compile(
    r'^\s*(?P<d>\d{1,2})\.(?P<m>\d{1,2})\.(?P<y>\d{4})\s*$',
)


def _strip_tags(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text).strip()


def _parse_date(value: str) -> date | None:
    """Convert ``DD.MM.YYYY`` text to a :class:`date`."""
    match = _DATE_RE.match(value)
    if match is None:
        return None
    try:
        return date(
            int(match.group("y")),
            int(match.group("m")),
            int(match.group("d")),
        )
    except ValueError:
        return None


def _parse_rate(value: str) -> str | None:
    """Validate and normalise a rate cell.

    ``Borç Alma`` cells are dashed (``"-"``) on the 1-week repo page
    because the 1-week repo is the only operational column. Returns
    ``None`` for dashed / empty cells, the trimmed decimal string for
    real numeric values. Raises :class:`TCMBRateHistoryParseError`
    when the string is non-empty but unparseable as a Decimal —
    layout drift signal.
    """
    text = value.strip()
    if not text or text == "-":
        return None
    try:
        Decimal(text)
    except InvalidOperation as exc:
        raise TCMBRateHistoryParseError(
            f"unparseable TCMB rate cell {value!r}",
        ) from exc
    return text


def parse_rate_history(html: str | bytes) -> list[TCMBRateDecision]:
    """Walk the 1-week repo rate-history HTML for every parseable decision.

    Returns the decisions ordered most-recent-first. Raises
    :class:`TCMBRateHistoryParseError` when the page shape is malformed
    (no ``midTable`` element, every row malformed) so a layout drift
    is loud rather than silent.
    """
    if isinstance(html, (bytes, bytearray)):
        html = html.decode("utf-8", errors="replace")
    text = html_lib.unescape(html)

    table_match = _TABLE_RE.search(text)
    if table_match is None:
        raise TCMBRateHistoryParseError(
            "TCMB 1-week repo page missing midTable element — DOM drift",
        )

    parsed: list[tuple[date, str, str | None]] = []
    body = table_match.group("body")
    for row_match in _ROW_RE.finditer(body):
        cells_html = row_match.group("cells")
        cells = [_strip_tags(c.group("text")) for c in _TD_RE.finditer(cells_html)]
        if len(cells) < 3:
            continue
        effective = _parse_date(cells[0])
        if effective is None:
            # Header row (``Tarih`` text) lands here; falls through.
            continue
        try:
            rate = _parse_rate(cells[2])
            borrowing = _parse_rate(cells[1])
        except TCMBRateHistoryParseError:
            # A truncated / corrupted row must not nuke the whole list
            # — skip it and keep walking. Mirrors the BCB / RBA parser
            # defensive shape.
            continue
        if rate is None:
            continue
        parsed.append((effective, rate, borrowing))

    if not parsed:
        raise TCMBRateHistoryParseError(
            "TCMB 1-week repo table parsed zero decisions — layout drift",
        )

    parsed.sort(key=lambda r: r[0])
    decisions: list[TCMBRateDecision] = []
    previous_rate: str | None = None
    for effective, rate, borrowing in parsed:
        decisions.append(TCMBRateDecision(
            effective_date=effective,
            rate=rate,
            previous_rate=previous_rate,
            borrowing_rate=borrowing,
        ))
        previous_rate = rate

    decisions.sort(key=lambda d: d.effective_date, reverse=True)
    return decisions


_HASH_FIELDS: tuple[str, ...] = (
    "effective_date", "rate", "previous_rate", "borrowing_rate",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def decision_to_records(
    decision: TCMBRateDecision,
    *,
    snapshot_epoch_ms: int,
    spec: TCMBIndicatorSpec | None = None,
) -> tuple[TCMBCalendarRawRecord, TCMBCalendarEventRecord]:
    """Project a :class:`TCMBRateDecision` to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY["TCMB_RATE"]

    scheduled = parse_scheduled_release_time(
        decision.effective_date,
        TCMB_RELEASE_TIME,
        default_tz=TCMB_RELEASE_TZ,
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
        "kind":              "tcmb_rate_decision",
        "effective_date": decision.effective_date.isoformat(),
        "rate":              decision.rate,
        "previous_rate":     decision.previous_rate,
        "borrowing_rate":    decision.borrowing_rate,
        "event_time_utc":    event_time_utc,
        "source_url":        TCMB_RATE_HISTORY_URL,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = TCMBCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = TCMBCalendarEventRecord(
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
        currency="TRY",
        unit=resolved_spec.unit,
        actual=decision.rate,
        previous=decision.previous_rate,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Türkiye Cumhuriyet Merkez Bankası",
        source_url=TCMB_RATE_HISTORY_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "PROVIDER",
    "TCMB_BASE_URL",
    "TCMB_RATE_HISTORY_URL",
    "TCMB_RELEASE_TIME",
    "TCMB_RELEASE_TZ",
    "TCMBCalendarEventRecord",
    "TCMBCalendarRawRecord",
    "TCMBRateDecision",
    "TCMBRateHistoryParseError",
    "decision_to_records",
    "parse_rate_history",
]
