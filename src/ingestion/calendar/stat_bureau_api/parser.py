"""Shared dataclasses and constants for the Statistics Bureau connector."""

from __future__ import annotations

from dataclasses import dataclass


PROVIDER = "stat-bureau-jp"
STAT_BUREAU_RELEASE_TZ = "Asia/Tokyo"
STAT_BUREAU_RELEASE_TIME_LOCAL = "08:30"
STAT_BUREAU_CPI_SCHEDULE_URL = "https://www.stat.go.jp/english/data/cpi/1582.htm"
STAT_BUREAU_LFS_SCHEDULE_URL = "https://www.stat.go.jp/english/data/roudou/1543.htm"
ESTAT_STATS_DATA_URL = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"


@dataclass(frozen=True)
class StatBureauCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class StatBureauCalendarEventRecord:
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


def build_estat_dbview_url(stats_data_id: str) -> str:
    """Return the e-Stat browser URL for a statistical table id."""
    return f"https://www.e-stat.go.jp/en/dbview?sid={stats_data_id}"
