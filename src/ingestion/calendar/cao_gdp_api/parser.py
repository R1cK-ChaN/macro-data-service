"""Shared dataclasses and constants for the CAO GDP connector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from .indicators import FIRST_PRELIMINARY, SECOND_PRELIMINARY


PROVIDER = "cao"
CAO_GDP_RELEASE_TZ = "Asia/Tokyo"
CAO_GDP_RELEASE_TIME_LOCAL = "08:50"
CAO_GDP_ARCHIVE_INDEX_URL = (
    "https://www.esri.cao.go.jp/en/sna/data/sokuhou/files/toukei_top.html"
)
CAO_GDP_ARCHIVE_YEAR_URL_TEMPLATE = (
    "https://www.esri.cao.go.jp/en/sna/data/sokuhou/files/"
    "{year}/toukei_{year}.html"
)
CAO_GDP_REPORT_URL_TEMPLATE = (
    "https://www.esri.cao.go.jp/en/sna/data/sokuhou/files/"
    "{year}/qe{yy}{quarter}{suffix}/gdemenuea.html"
)
CAO_GDP_SOURCE = "Cabinet Office Japan (ESRI)"


@dataclass(frozen=True)
class CaoGdpCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class CaoGdpCalendarEventRecord:
    """One row destined for ``cal_econ_event``."""

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


def _quarter_from_reference(reference: date) -> int:
    if reference.month == 3:
        return 1
    if reference.month == 6:
        return 2
    if reference.month == 9:
        return 3
    if reference.month == 12:
        return 4
    raise ValueError(f"reference date is outside a quarter end: {reference!r}")


def build_report_url(reference: date, release_stage: str) -> str:
    """Construct the English GDP report-menu URL for a quarter + stage."""
    quarter = _quarter_from_reference(reference)
    suffix = "_2" if release_stage == SECOND_PRELIMINARY else ""
    if release_stage not in {FIRST_PRELIMINARY, SECOND_PRELIMINARY}:
        raise ValueError(f"unknown CAO GDP release stage: {release_stage!r}")
    return CAO_GDP_REPORT_URL_TEMPLATE.format(
        year=reference.year,
        yy=reference.year % 100,
        quarter=quarter,
        suffix=suffix,
    )
