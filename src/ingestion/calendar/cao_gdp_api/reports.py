"""Scrape CAO GDP report menus and parse the real GDP QoQ CSV."""

from __future__ import annotations

import calendar as _calendar
import csv
import hashlib
import io
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import SPEC_BY_STAGE, CaoGdpIndicatorSpec
from .parser import (
    CAO_GDP_RELEASE_TIME_LOCAL,
    CAO_GDP_RELEASE_TZ,
    CAO_GDP_SOURCE,
    PROVIDER,
    CaoGdpCalendarEventRecord,
    CaoGdpCalendarRawRecord,
)
from .scraper import CAO_GDP_BROWSER_HEADERS

logger = logging.getLogger(__name__)


class CaoGdpReportParseError(ValueError):
    """Raised when a GDP report menu or CSV changes shape."""


@dataclass(frozen=True)
class CaoGdpValue:
    """Headline real GDP QoQ value parsed from a CAO CSV."""

    reference_date: date
    reference_label: str
    actual: str
    csv_url: str


_GDP_QOQ_CSV_BASENAME_RE = re.compile(r"^ritu-jk\d{3,4}r?\.csv$", re.I)
_PERIOD_RE = re.compile(
    r"(?:(?P<year>\d{4})\s*/\s*)?"
    r"(?P<start>\d{1,2})\s*-\s*(?P<end>\d{1,2})\.?"
)
_QUARTER_BY_MONTHS = {
    (1, 3): 1,
    (4, 6): 2,
    (7, 9): 3,
    (10, 12): 4,
}


def _normalize_cell(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def parse_gdp_report_menu_html(html: str, *, report_url: str) -> str:
    """Return the real GDP QoQ CSV URL from a ``gdemenuea.html`` page."""
    soup = BeautifulSoup(html, "html.parser")
    matches: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"]).strip()
        basename = PurePosixPath(urlparse(href).path).name
        if not _GDP_QOQ_CSV_BASENAME_RE.match(basename):
            continue
        text = _normalize_cell(anchor.get_text(" ", strip=True)).lower()
        if "real, seasonally adjusted series" not in text:
            continue
        if "quarter-to-quarter" not in text:
            continue
        if "annualized" in text:
            continue
        matches.append(urljoin(report_url, href))
    if len(matches) != 1:
        raise CaoGdpReportParseError(
            f"expected one CAO GDP real QoQ CSV link, found {len(matches)}"
        )
    return matches[0]


def _reference_from_period_cell(
    cell: str,
    *,
    carry_year: int | None,
) -> tuple[date | None, int | None]:
    text = _normalize_cell(cell)
    match = _PERIOD_RE.search(text)
    if match is None:
        return None, carry_year
    year_raw = match.group("year")
    year = int(year_raw) if year_raw else carry_year
    if year is None:
        return None, carry_year
    start = int(match.group("start"))
    end = int(match.group("end"))
    quarter = _QUARTER_BY_MONTHS.get((start, end))
    if quarter is None:
        raise CaoGdpReportParseError(f"unexpected GDP CSV period cell: {cell!r}")
    last_day = _calendar.monthrange(year, end)[1]
    return date(year, end, last_day), year


def _gdp_column_index(rows: list[list[str]]) -> int:
    for row in rows:
        for idx, cell in enumerate(row):
            normalized = _normalize_cell(cell).replace(" ", "")
            if normalized == "GDP(ExpenditureApproach)":
                return idx
    raise CaoGdpReportParseError(
        "CAO GDP CSV missing GDP(Expenditure Approach) header"
    )


def _reference_label(reference: date) -> str:
    quarter = {3: 1, 6: 2, 9: 3, 12: 4}.get(reference.month)
    if quarter is None:
        raise CaoGdpReportParseError(
            f"reference date is outside a quarter end: {reference!r}"
        )
    return f"Q{quarter} {reference.year}"


def _clean_numeric(text: str) -> str:
    cleaned = _normalize_cell(text).replace(",", "")
    if cleaned in {"", "***", "-", "—", "–"}:
        raise CaoGdpReportParseError(
            f"CAO GDP headline value is empty or suppressed: {text!r}"
        )
    try:
        float(cleaned)
    except ValueError as exc:
        raise CaoGdpReportParseError(
            f"CAO GDP headline value is unparseable: {text!r}"
        ) from exc
    return cleaned


def parse_gdp_growth_csv(
    csv_text: str,
    *,
    reference_date: date,
    csv_url: str,
) -> CaoGdpValue:
    """Extract the headline real GDP QoQ percent-change value."""
    rows = list(csv.reader(io.StringIO(csv_text)))
    if not rows:
        raise CaoGdpReportParseError("CAO GDP CSV is empty")
    gdp_idx = _gdp_column_index(rows)
    carry_year: int | None = None
    for row in rows:
        if not row:
            continue
        ref, carry_year = _reference_from_period_cell(
            row[0],
            carry_year=carry_year,
        )
        if ref != reference_date:
            continue
        if len(row) <= gdp_idx:
            raise CaoGdpReportParseError(
                f"CAO GDP CSV row missing GDP column: {row!r}"
            )
        return CaoGdpValue(
            reference_date=reference_date,
            reference_label=_reference_label(reference_date),
            actual=_clean_numeric(row[gdp_idx]),
            csv_url=csv_url,
        )
    raise CaoGdpReportParseError(
        f"CAO GDP CSV missing reference period {reference_date.isoformat()}"
    )


_HASH_FIELDS: tuple[str, ...] = (
    "indicator",
    "release_stage",
    "reference_date",
    "actual",
    "event_time_utc",
    "csv_url",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _anchor(reference: date, release_stage: str) -> str:
    return f"{reference.isoformat()}|{release_stage}"


def gdp_value_to_records(
    value: CaoGdpValue,
    *,
    release_stage: str,
    snapshot_epoch_ms: int,
    report_url: str,
    event_time_utc: str | None = None,
    release_date: date | None = None,
    observed_at_epoch_ms: int | None = None,
    spec: CaoGdpIndicatorSpec | None = None,
) -> tuple[CaoGdpCalendarRawRecord, CaoGdpCalendarEventRecord]:
    """Project a parsed GDP value to ``(raw, event)`` records."""
    resolved_spec = spec or SPEC_BY_STAGE[release_stage]
    if event_time_utc is None:
        if release_date is None:
            raise CaoGdpReportParseError(
                "CAO GDP value projection needs event_time_utc or release_date"
            )
        scheduled = parse_scheduled_release_time(
            release_date,
            CAO_GDP_RELEASE_TIME_LOCAL,
            default_tz=CAO_GDP_RELEASE_TZ,
        )
        event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        _anchor(value.reference_date, release_stage),
    )
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()
    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    payload: dict[str, Any] = {
        "kind":            "cao_gdp_value",
        "indicator":       resolved_spec.indicator,
        "release_stage":   release_stage,
        "reference_date":  value.reference_date.isoformat(),
        "reference_label": value.reference_label,
        "actual":          value.actual,
        "event_time_utc":  event_time_utc,
        "report_url":      report_url,
        "csv_url":         value.csv_url,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)

    raw_record = CaoGdpCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = CaoGdpCalendarEventRecord(
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
        currency="",
        unit=resolved_spec.unit,
        actual=value.actual,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source=CAO_GDP_SOURCE,
        source_url=report_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


def fetch_gdp_report_menu_html(
    report_url: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET a per-release GDP report-menu page."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            report_url,
            headers=CAO_GDP_BROWSER_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()


def fetch_gdp_csv_text(
    csv_url: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET a CAO GDP CSV and decode its CP932 text."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            csv_url,
            headers=CAO_GDP_BROWSER_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.content.decode("cp932")
    finally:
        if owned_session:
            s.close()
