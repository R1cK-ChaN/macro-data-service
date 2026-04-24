"""Shared dataclasses and constants for the MoF Trade Statistics connector.

No HTTP here — the schedule scraper (:mod:`scraper`) and the value
scraper (:mod:`reports`) both import from this module.

Event-time shape:

The Trade Statistics Monthly Data (Provisional) release is the
Balance of Trade headline; MoF publishes it at **08:50 JST** on
the published release day (from the calendar table header). JST
has no DST so the stored UTC stamp is always 23:50 of the prior
date.

``reference_date`` uses the first day of the **reference month**
(the month being reported on), not the release day. Release day
is ~20 days later; analysts reading the event want to pivot on
the data's reference period.

``provider_event_id`` anchors on ``(indicator, reference_date)``
so the schedule-side write and the value-side write converge on
the same row.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


PROVIDER = "mof-jp"
MOF_RELEASE_TZ = "Asia/Tokyo"
MOF_RELEASE_TIME_LOCAL = "08:50"
MOF_CALENDAR_URL = (
    "https://www.customs.go.jp/toukei/calendar/calend_e.htm"
)
MOF_TRADE_REPORT_URL_TEMPLATE = (
    "https://www.customs.go.jp/toukei/shinbun/trade-st_e/{year}/{yyyymm}4e.xml"
)


@dataclass(frozen=True)
class MofCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class MofCalendarEventRecord:
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


def build_trade_report_url(reference: date) -> str:
    """Construct the Monthly Data (Provisional) XML URL.

    MoF keys the report filename on ``<YYYY><MM>4e.xml`` under the
    reference year's directory. The ``4`` stage code identifies the
    full-month Provisional release (vs ``1`` First 10 days, ``2``
    First 20 days, ``3`` unused in English feed).
    """
    yyyymm = f"{reference.year}{reference.month:02d}"
    return MOF_TRADE_REPORT_URL_TEMPLATE.format(
        year=reference.year,
        yyyymm=yyyymm,
    )
