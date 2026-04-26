"""Shared dataclasses and constants for the METI calendar connector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


PROVIDER = "meti"
METI_RELEASE_TZ = "Asia/Tokyo"
METI_IIP_RELEASE_TIME_LOCAL = "08:50"
METI_RETAIL_RELEASE_TIME_LOCAL = "08:50"
ESTAT_RELEASE_CALENDAR_URL = "https://www.e-stat.go.jp/release-calendar"
ESTAT_RELEASE_CALENDAR_DETAIL_URL_TEMPLATE = (
    "https://www.e-stat.go.jp/release-calendar/detail/{toukei_cd}/{stamp}"
)
ESTAT_IIP_TOUKEI_CD = "00550300"
METI_IIP_RESULTS_INDEX_URL = (
    "https://www.meti.go.jp/english/statistics/tyo/iip/kako_press.html"
)
METI_IIP_REPORT_URL_TEMPLATE = (
    "https://www.meti.go.jp/english/statistics/tyo/iip/"
    "b2020_{yyyymm}se.html"
)
METI_RETAIL_PAGE_URL = (
    "https://www.meti.go.jp/english/statistics/tyo/syoudou/index.html"
)


@dataclass(frozen=True)
class MetiCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class MetiCalendarEventRecord:
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


def build_iip_report_url(reference: date) -> str:
    """Construct the 2020-base preliminary IIP report URL."""
    return METI_IIP_REPORT_URL_TEMPLATE.format(
        yyyymm=f"{reference.year}{reference.month:02d}",
    )
