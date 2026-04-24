"""Dataclasses and provider constant for the HCOB schedule connector.

P5 is schedule-only — no press-release value parser yet. Value side is
deferred to a follow-up (the S&P Global report-viewer URLs are not
deterministically resolvable without authenticated access).
"""

from __future__ import annotations

from dataclasses import dataclass

PROVIDER = "hcob"


@dataclass(frozen=True)
class HCOBCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class HCOBCalendarEventRecord:
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
