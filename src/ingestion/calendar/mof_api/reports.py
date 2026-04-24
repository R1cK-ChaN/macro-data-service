"""Scrape MoF Trade Statistics monthly XML reports.

Each Monthly Data (Provisional) release publishes a structured XML
feed at ``customs.go.jp/toukei/shinbun/trade-st_e/<YYYY>/<YYYYMM>4e.xml``
whose first ``<sogakutsuki>`` element carries the headline totals::

    <sogakutsuki name="pg1">
      <kohyoymd>April 22, 2026</kohyoymd>
      <title>Value of Exports and Imports March 2026 (Provisional)</title>
      <taishoymtonen>March 2026</taishoymtonen>
      <export><sogakutonen>11,003,319</sogakutonen>...</export>
      <import><sogakutonen>10,336,342</sogakutonen>...</import>
      <sashihiki>
        <sogakutonen>666,977</sogakutonen>   <!-- current-month balance -->
        <sogakuzennen>529,809</sogakuzennen> <!-- prior-year same month -->
        <nobiritsu>25.9</nobiritsu>
      </sashihiki>
    </sogakutsuki>

Units are **millions of yen** (verify against TE: 666,977 million ¥
= ¥666.98 billion surplus for March 2026). ``<sashihiki>`` is
Japanese for "offset" — the trade balance. Deficit values render
with a leading ``△`` (the Japanese negative-sign convention); the
parser strips it to a plain minus.

Fetch + parse + project are separable: tests feed fixture XML to
:func:`parse_trade_report_xml`; live callers drive
:func:`fetch_trade_report_xml`. :func:`trade_report_to_records`
emits one ``(raw, event)`` tuple whose ``provider_event_id``
matches the schedule-side write exactly so the Balance of Trade
value upserts onto the existing schedule row through the shared
projector's merge CASE.
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
import xml.etree.ElementTree as ET

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import MofIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    MOF_RELEASE_TIME_LOCAL,
    MOF_RELEASE_TZ,
    PROVIDER,
    MofCalendarEventRecord,
    MofCalendarRawRecord,
    build_trade_report_url,
)
from .scraper import _MOF_BROWSER_HEADERS

logger = logging.getLogger(__name__)


class MofTradeReportParseError(Exception):
    """Trade XML didn't carry a parseable Balance of Trade block."""


@dataclass(frozen=True)
class TradeReportValue:
    """Parsed MoF trade report outcome."""

    reference_date: date
    reference_label: str              # "March 2026"
    release_date: date | None         # from <kohyoymd>
    balance_million_jpy: int          # current-month trade balance
    export_million_jpy: int           # current-month export value
    import_million_jpy: int           # current-month import value


_RELEASE_DATE_RE = re.compile(
    r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})"
)
_REFERENCE_MONTH_RE = re.compile(
    r"(?P<month>[A-Za-z]+)\s+(?P<year>\d{4})"
)
_MONTH_NAMES: dict[str, int] = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _resolve_month_name(name: str) -> int:
    key = name.strip().lower()
    month = _MONTH_NAMES.get(key)
    if month is None:
        raise MofTradeReportParseError(f"unknown month name: {name!r}")
    return month


def _parse_release_date(text: str) -> date | None:
    match = _RELEASE_DATE_RE.search(text or "")
    if match is None:
        return None
    month = _resolve_month_name(match.group("month"))
    return date(
        year=int(match.group("year")),
        month=month,
        day=int(match.group("day")),
    )


def _parse_reference_month(text: str) -> date:
    match = _REFERENCE_MONTH_RE.search(text or "")
    if match is None:
        raise MofTradeReportParseError(
            f"reference-month cell unparseable: {text!r}"
        )
    return date(
        year=int(match.group("year")),
        month=_resolve_month_name(match.group("month")),
        day=1,
    )


# MoF reports deficits with the Japanese triangle prefix ``△``
# (U+25B3) or the full-width triangle ``▲`` (U+25B2). Plain ASCII
# minus is never used on this surface, so we strip the triangle and
# stamp a regular ``-`` sign instead.
_TRIANGLE_CHARS = ("△", "▲", "△", "▲")


def _parse_yen_amount(text: str) -> int:
    """Parse a MoF yen cell into a signed integer (millions of yen)."""
    cleaned = (text or "").strip().replace(",", "").replace(" ", "")
    sign = 1
    for triangle in _TRIANGLE_CHARS:
        if cleaned.startswith(triangle):
            cleaned = cleaned[len(triangle):]
            sign = -1
    if cleaned.startswith("-"):
        cleaned = cleaned[1:]
        sign = -1
    if not cleaned:
        raise MofTradeReportParseError("empty yen amount")
    if not cleaned.isdigit():
        raise MofTradeReportParseError(f"non-numeric yen amount: {text!r}")
    return sign * int(cleaned)


def _first_child_text(parent, tag: str) -> str:
    child = parent.find(tag)
    if child is None or child.text is None:
        raise MofTradeReportParseError(
            f"missing <{tag}> under <{parent.tag}>"
        )
    return child.text


def parse_trade_report_xml(
    xml_text: str,
    *,
    reference_date: date | None = None,
) -> TradeReportValue:
    """Extract the Balance of Trade headline from a trade-report XML feed.

    Raises :class:`MofTradeReportParseError` when the feed lacks the
    ``<sogakutsuki name="pg1">`` block or when the balance cell is
    unparseable — upstream drift must surface loudly rather than
    silently stamp ``None`` onto an existing schedule row.

    ``reference_date`` is optional. When omitted, we derive it from
    the ``<taishoymtonen>`` cell ("March 2026"); when supplied, we
    trust the caller (used when we looked the release up by
    reference already and want to cross-check the XML).
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise MofTradeReportParseError(
            f"MoF trade XML parse failed: {exc}"
        ) from exc

    pg1 = root.find("sogakutsuki")
    if pg1 is None:
        raise MofTradeReportParseError(
            "MoF trade XML missing <sogakutsuki> root element"
        )

    # Reference month either comes from the caller or from the
    # <taishoymtonen> element; cross-check when both available so a
    # caller-supplied / filename-derived date catching a feed drift
    # surfaces loudly.
    ref_text = _first_child_text(pg1, "taishoymtonen")
    xml_reference = _parse_reference_month(ref_text)
    if reference_date is not None and reference_date != xml_reference:
        raise MofTradeReportParseError(
            f"MoF trade XML reference month mismatch: "
            f"caller={reference_date.isoformat()} "
            f"xml={xml_reference.isoformat()}"
        )
    resolved_reference = reference_date or xml_reference

    release_text = _first_child_text(pg1, "kohyoymd")
    release_date = _parse_release_date(release_text)

    sashihiki = pg1.find("sashihiki")
    if sashihiki is None:
        raise MofTradeReportParseError(
            "MoF trade XML missing <sashihiki> (balance) element"
        )
    balance_text = _first_child_text(sashihiki, "sogakutonen")
    balance = _parse_yen_amount(balance_text)

    export_el = pg1.find("export")
    if export_el is None:
        raise MofTradeReportParseError(
            "MoF trade XML missing <export> element under <sogakutsuki>"
        )
    import_el = pg1.find("import")
    if import_el is None:
        raise MofTradeReportParseError(
            "MoF trade XML missing <import> element under <sogakutsuki>"
        )
    export_text = _first_child_text(export_el, "sogakutonen")
    import_text = _first_child_text(import_el, "sogakutonen")

    return TradeReportValue(
        reference_date=resolved_reference,
        reference_label=ref_text.strip(),
        release_date=release_date,
        balance_million_jpy=balance,
        export_million_jpy=_parse_yen_amount(export_text),
        import_million_jpy=_parse_yen_amount(import_text),
    )


# ──────────────────────────────────────────────────────────────────────────
# HTTP fetch
# ──────────────────────────────────────────────────────────────────────────


def fetch_trade_report_xml(
    reference: date,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the Monthly Data (Provisional) XML for a reference month.

    MoF's trade XML feed declares ``encoding="UTF-8"`` in the XML
    prolog, but we still defer to :attr:`requests.Response.text` so
    the decode honours the response headers if they ever diverge
    from the prolog declaration.
    """
    url = build_trade_report_url(reference)
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(url, headers=_MOF_BROWSER_HEADERS, timeout=timeout)
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()


# ──────────────────────────────────────────────────────────────────────────
# Value-side projection
# ──────────────────────────────────────────────────────────────────────────


_HASH_FIELDS: tuple[str, ...] = (
    "indicator", "balance", "event_time_utc",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _default_release_for_reference(reference: date) -> date:
    """Synthesize a release day when the caller can't supply one.

    MoF Provisional monthlies publish ~20 days into the following
    month (Mar-Apr 22, Feb-Mar 18, etc.). Fall back to the 20th of
    the month after the reference — close enough to keep the
    stamped event-time plausible on ad-hoc reruns.
    """
    year = reference.year + (1 if reference.month == 12 else 0)
    month = 1 if reference.month == 12 else reference.month + 1
    return date(year=year, month=month, day=20)


def trade_report_to_records(
    value: TradeReportValue,
    *,
    snapshot_epoch_ms: int,
    event_time_utc: str | None = None,
    release_date: date | None = None,
    observed_at_epoch_ms: int | None = None,
    spec: MofIndicatorSpec | None = None,
) -> tuple[MofCalendarRawRecord, MofCalendarEventRecord]:
    """Project a :class:`TradeReportValue` into ``(raw, event)`` records.

    Event-time resolution mirrors the Tankan value-side writer:

    - ``event_time_utc`` (caller-supplied ISO string) — used
      verbatim. This is what value-side auto-discovery passes in
      to preserve the schedule-side publish clock.
    - ``release_date`` — projected through
      :func:`parse_scheduled_release_time` with 08:50 JST.
    - The XML's own ``<kohyoymd>`` — used when the caller supplies
      neither. Always available since MoF's XML always prints the
      release date in page one.
    - Fallback to the 20th-of-next-month helper for ad-hoc reruns
      without any release context.
    """
    resolved_spec = spec or INDICATOR_REGISTRY["TRADE_BALANCE"]

    if event_time_utc is None:
        effective_release = (
            release_date
            or value.release_date
            or _default_release_for_reference(value.reference_date)
        )
        scheduled = parse_scheduled_release_time(
            effective_release,
            MOF_RELEASE_TIME_LOCAL,
            default_tz=MOF_RELEASE_TZ,
        )
        event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        value.reference_date.isoformat(),
    )

    # Sign the actual with a leading "-" for deficits so downstream
    # display and arithmetic doesn't need to know about △. Match
    # TE's convention for JP BoT: plain signed integer, millions of
    # yen.
    actual_str = str(value.balance_million_jpy)
    report_url = build_trade_report_url(value.reference_date)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()
    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )

    payload: dict[str, Any] = {
        "kind":             "mof_trade_report",
        "indicator":        resolved_spec.indicator,
        "reference_date":   value.reference_date.isoformat(),
        "reference_label":  value.reference_label,
        "release_date":     (
            value.release_date.isoformat() if value.release_date else None
        ),
        "balance":          value.balance_million_jpy,
        "export":           value.export_million_jpy,
        "import":           value.import_million_jpy,
        "event_time_utc":   event_time_utc,
        "report_url":       report_url,
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
        reference_date=value.reference_date.isoformat(),
        reference_label=value.reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="JPY",
        unit=resolved_spec.unit,
        actual=actual_str,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Ministry of Finance Japan",
        source_url=report_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record
