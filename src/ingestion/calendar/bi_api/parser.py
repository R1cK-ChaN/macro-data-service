"""Bank Indonesia BI-Rate history HTML → calendar projection.

Bank Indonesia publishes the BI-Rate decision history at
``bi.go.id/en/statistik/indikator/bi-rate.aspx`` as a server-rendered
SharePoint table::

    <table class="table table-striped table-no-bordered table-lg">
      <thead>
        <tr class="table-header">
          <th>No</th>
          <th>Period</th>
          <th>BI-Rate</th>
          <th>Press Release Link</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>1</th>
          <td>22 April 2026</td>
          <td>4.75 %</td>
          <td><a href="/id/publikasi/.../sp_288426.aspx">View</a></td>
        </tr>
        ...
      </tbody>
    </table>

Each row is one BI Board of Governors meeting — change OR hold —
with the absolute new BI-Rate inline. The first ``Period`` is the
meeting closing day (``"DD Month YYYY"``, English-locale month name).

The table is paginated via ASP.NET ``__doPostBack`` — page 1 carries
the most recent ~10 decisions (covering ~14 months at the modern
monthly cadence). The connector ingests page 1 only in P1; backfill
of older pages (which would require posting the SharePoint VIEWSTATE
+ EVENTVALIDATION tokens) is deferred to a P2 follow-up. Page-1
coverage is enough to anchor parity from day one — the daily sweep
catches every new meeting, and the rolling backward window (handled
by ``cal_econ_event``'s idempotent upsert) keeps recent decisions
fresh across reschedules.

**Coverage is full meeting (change OR hold).** Unlike the TCMB / SARB
rate-history surfaces which list rate-changes only, BI's table carries
every Board of Governors decision, so the slice ships value-bearing
events for every meeting and ``(ID, BI_RATE)`` joins the parity
whitelist on day one.

Time is set to **14:00 ``Asia/Jakarta``** — Bank Indonesia's
documented afternoon publication window (Board of Governors
announcements typically post at 14:00–15:00 WIB on the meeting close
day). Indonesia is UTC+7 year-round (no DST), so the conversion to
UTC is a fixed −7 hour offset for every backfill row.

``provider_event_id`` / ``event_time_utc`` / ``reference_date`` all
anchor on the announcement date as published — no off-by-one between
effective and announcement convention (unlike TCMB / SARB), so the
parity whitelist lights up cleanly.
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

from .indicators import BIIndicatorSpec, INDICATOR_REGISTRY

PROVIDER = "bank-indonesia"
BI_RELEASE_TZ = "Asia/Jakarta"
# Bank Indonesia publishes the Board of Governors meeting decision in
# the afternoon — documented at 14:00 WIB. Used as the default wall-
# clock release time when ``parse_scheduled_release_time`` resolves
# the per-decision ``event_time_utc``.
BI_RELEASE_TIME = "14:00"
BI_BASE_URL = "https://www.bi.go.id"
BI_RATE_HISTORY_URL = (
    f"{BI_BASE_URL}/en/statistik/indikator/bi-rate.aspx"
)


class BIRateHistoryParseError(ValueError):
    """BI rate-history HTML did not expose a parseable rate table."""


@dataclass(frozen=True)
class BIRateDecision:
    """One BI Board of Governors decision parsed from the rate page."""

    announcement_date: date      # date Bank Indonesia announced the decision
    rate: str                    # decimal string ("4.75")
    previous_rate: str | None    # rate before this decision (None for #1)
    press_release_url: str       # /id/publikasi/.../sp_NNNNNN.aspx (or empty)


@dataclass(frozen=True)
class BICalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class BICalendarEventRecord:
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


# English month names — BI uses the English-locale page surface
# (`/en/...`); Indonesian-locale pages would use Bahasa Indonesia
# month names (`Januari`, `Februari`, ...) but the connector pins on
# the English variant.
_EN_MONTHS: dict[str, int] = {
    "january":   1, "february":  2, "march":      3,
    "april":     4, "may":       5, "june":       6,
    "july":      7, "august":    8, "september":  9,
    "october":  10, "november": 11, "december":  12,
}


# The rate table is anchored by the literal class string. Pinned to
# the full class list so a side table elsewhere on the page (page-1
# pagination header / footer) can't be mis-parsed as the rate table.
_TABLE_RE = re.compile(
    r'<table[^>]*class="table\s+table-striped\s+table-no-bordered\s+table-lg"[^>]*>'
    r'(?P<body>.*?)</table>',
    re.DOTALL | re.IGNORECASE,
)
_ROW_RE = re.compile(
    r'<tr[^>]*>(?P<cells>.*?)</tr>',
    re.DOTALL | re.IGNORECASE,
)
# Cell content captured verbatim; ``<th>`` and ``<td>`` row-header /
# data-cell mix on the same row — the rate table uses a numbered
# ``<th scope="row">`` for the row index plus ``<td>`` for everything
# else.
_CELL_RE = re.compile(
    r'<(?:t[hd])[^>]*>(?P<text>.*?)</(?:t[hd])>',
    re.DOTALL | re.IGNORECASE,
)
# ``DD Month YYYY`` — English-locale, day digits + spaces + month
# name + four-digit year. Defensive on whitespace amount.
_DATE_RE = re.compile(
    r'^\s*(?P<d>\d{1,2})\s+(?P<month>[A-Za-z]+)\s+(?P<y>\d{4})\s*$',
)
# ``X.YZ %`` — decimal rate followed by the percent literal. The
# percent sign is sometimes wrapped to a new line in the SharePoint
# render so the regex tolerates whitespace between the number and ``%``.
_RATE_RE = re.compile(
    r'^\s*(?P<rate>\d+(?:[\.,]\d+)?)\s*%?\s*$',
    re.DOTALL,
)
# Press-release anchor inside the fourth cell.
_PRESS_LINK_RE = re.compile(
    r'<a[^>]+href="(?P<href>[^"]+)"',
    re.IGNORECASE,
)


def _strip_tags(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text)


def _decode_entities(text: str) -> str:
    return html_lib.unescape(text)


def _parse_announcement_date(value: str) -> date | None:
    text = _decode_entities(value).strip()
    match = _DATE_RE.match(text)
    if match is None:
        return None
    month = _EN_MONTHS.get(match.group("month").lower())
    if month is None:
        return None
    try:
        return date(
            int(match.group("y")),
            month,
            int(match.group("d")),
        )
    except ValueError:
        return None


def _normalize_rate(value: str) -> str | None:
    """Validate and normalise a BI rate cell.

    Returns the decimal-validated string with a period decimal
    separator (``"4.75"``) or ``None`` for empty cells. Raises
    :class:`BIRateHistoryParseError` on a non-empty but unparseable
    value — layout drift signal.
    """
    text = _decode_entities(value).strip()
    if not text:
        return None
    match = _RATE_RE.match(text)
    if match is None:
        raise BIRateHistoryParseError(
            f"unparseable BI rate cell {value!r}",
        )
    candidate = match.group("rate").replace(",", ".")
    try:
        as_decimal = Decimal(candidate)
    except InvalidOperation as exc:
        raise BIRateHistoryParseError(
            f"unparseable BI rate {value!r}",
        ) from exc
    return f"{as_decimal.quantize(Decimal('0.01')):.2f}"


def _press_release_url(cell_html: str) -> str:
    match = _PRESS_LINK_RE.search(cell_html)
    if match is None:
        return ""
    href = _decode_entities(match.group("href"))
    if href.startswith(("http://", "https://")):
        return href
    return BI_BASE_URL + href


def parse_rate_history(html: str | bytes) -> list[BIRateDecision]:
    """Walk the BI-Rate history HTML for every parseable decision row.

    Returns the decisions ordered most-recent-first (matches the
    page's display order). Raises :class:`BIRateHistoryParseError`
    when the page shape is malformed (no rate table, every row
    malformed, or a non-empty rate cell that isn't parseable as a
    decimal) so a layout drift is loud rather than silent.
    """
    if isinstance(html, (bytes, bytearray)):
        html = html.decode("utf-8", errors="replace")

    table_match = _TABLE_RE.search(html)
    if table_match is None:
        raise BIRateHistoryParseError(
            "Bank Indonesia rate page missing the rate table — DOM drift",
        )

    parsed: list[tuple[date, str, str]] = []  # (announcement, rate, press_url)
    body = table_match.group("body")
    for row_match in _ROW_RE.finditer(body):
        cells_html = row_match.group("cells")
        cell_matches = list(_CELL_RE.finditer(cells_html))
        if len(cell_matches) < 4:
            # Header row uses ``<th>`` with one extra column header
            # but falls through here (length mismatch on the data-row
            # shape).
            continue
        cells = [c.group("text") for c in cell_matches]
        # Cells: [row index, period, rate, press-release link cell].
        announcement = _parse_announcement_date(_strip_tags(cells[1]))
        if announcement is None:
            continue
        try:
            rate = _normalize_rate(_strip_tags(cells[2]))
        except BIRateHistoryParseError:
            # A truncated / corrupted row must not nuke the whole list
            # — skip it and keep walking. Mirrors the Banxico / TCMB
            # parser defensive shape.
            continue
        if rate is None:
            continue
        press_url = _press_release_url(cells[3])
        parsed.append((announcement, rate, press_url))

    if not parsed:
        raise BIRateHistoryParseError(
            "Bank Indonesia rate table parsed zero decisions — layout drift",
        )

    parsed.sort(key=lambda r: r[0])
    decisions: list[BIRateDecision] = []
    previous_rate: str | None = None
    for announcement, rate, press_url in parsed:
        decisions.append(BIRateDecision(
            announcement_date=announcement,
            rate=rate,
            previous_rate=previous_rate,
            press_release_url=press_url,
        ))
        previous_rate = rate

    decisions.sort(key=lambda d: d.announcement_date, reverse=True)
    return decisions


_HASH_FIELDS: tuple[str, ...] = (
    "announcement_date", "rate", "previous_rate", "press_release_url",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def decision_to_records(
    decision: BIRateDecision,
    *,
    snapshot_epoch_ms: int,
    spec: BIIndicatorSpec | None = None,
) -> tuple[BICalendarRawRecord, BICalendarEventRecord]:
    """Project a :class:`BIRateDecision` to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY["BI_RATE"]

    scheduled = parse_scheduled_release_time(
        decision.announcement_date,
        BI_RELEASE_TIME,
        default_tz=BI_RELEASE_TZ,
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
        "kind":               "bi_rate_decision",
        "announcement_date":  decision.announcement_date.isoformat(),
        "rate":               decision.rate,
        "previous_rate":      decision.previous_rate,
        "press_release_url":  decision.press_release_url,
        "event_time_utc":     event_time_utc,
        "source_url":         BI_RATE_HISTORY_URL,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = BICalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = BICalendarEventRecord(
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
        currency="IDR",
        unit=resolved_spec.unit,
        actual=decision.rate,
        previous=decision.previous_rate,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Bank Indonesia",
        source_url=BI_RATE_HISTORY_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "BI_BASE_URL",
    "BI_RATE_HISTORY_URL",
    "BI_RELEASE_TIME",
    "BI_RELEASE_TZ",
    "BICalendarEventRecord",
    "BICalendarRawRecord",
    "BIRateDecision",
    "BIRateHistoryParseError",
    "PROVIDER",
    "decision_to_records",
    "parse_rate_history",
]
