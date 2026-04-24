"""Shared dataclasses and constants for the Cabinet Office (ESRI) connector.

No HTTP here — the ESRI release-schedule scraper (:mod:`scraper`) and
the Consumer Confidence value scraper (:mod:`surveys`) both import
from this module.

Event-time shape:

Cabinet Office / ESRI publishes the Consumer Confidence Survey at
**14:00 JST** on the scheduled release day (disclosed in the footnote
of ``stat-schedule.html``, the Japanese twin of the English schedule).
JST has no DST so the stored UTC stamp is always 05:00 of the same
date.

``reference_date`` uses the first day of the **reference month** (the
month the survey fieldwork covers), not the release day. Release day
typically falls in the following calendar month (e.g. March survey
released early April), and analysts pivot on the reference period.

``provider_event_id`` anchors on ``(indicator, reference_date)`` so
the schedule-side write and the value-side write converge on the
same row.
"""

from __future__ import annotations

from dataclasses import dataclass


PROVIDER = "cao"
CAO_RELEASE_TZ = "Asia/Tokyo"
# Release-time constants per the ``stat-schedule.html`` footnote:
# "景気動向指数速報、景気動向指数改訂状況及び消費動向調査は14:00公表、
#  機械受注統計調査及び法人企業景気予測調査は8:50公表の予定です。"
# Consumer Confidence publishes at 14:00 JST; Machinery Orders and the
# Business Outlook Survey publish at 08:50 JST. Only Consumer Confidence
# lands in P3 — follow-up phases can add the other release-times.
CAO_CONSUMER_CONFIDENCE_RELEASE_TIME_LOCAL = "14:00"
CAO_ESRI_SCHEDULE_URL = (
    "https://www.esri.cao.go.jp/en/stat/stat-schedule-e.html"
)
CAO_CONSUMER_CONFIDENCE_URL = (
    "https://www.esri.cao.go.jp/en/stat/shouhi/shouhi-e.html"
)


@dataclass(frozen=True)
class CaoCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class CaoCalendarEventRecord:
    """One row destined for ``cal_econ_event`` (PIT projection)."""

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
