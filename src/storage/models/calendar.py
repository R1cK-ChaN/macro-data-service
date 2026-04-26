"""Storage records — calendar event records (cal_econ_event + indicator + alias + vintage).

Extracted out of src/storage/sqlite.py as part of issue #58 Tier 2.1A —
pure mechanical split, no behavior change. The records are re-exported by
storage.sqlite for backwards compatibility, so existing
``from storage.sqlite import XRecord`` consumers keep working.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class StoredEventRecord:
    source: str
    event_id: str
    timestamp: int
    country: str
    indicator: str
    category: str
    importance: str
    actual: str | None = None
    forecast: str | None = None
    previous: str | None = None
    revised_previous: str | None = None
    surprise: float | None = None
    currency: str = ""
    unit: str = ""
    raw_json: dict[str, Any] = field(default_factory=dict)
    indicator_id: str | None = None
    event_time_utc: str = ""
    event_time_precision: str = "datetime"


@dataclass(frozen=True)
class CalendarIndicatorRecord:
    indicator_id: str               # 'us.inflation.cpi_mom'
    canonical_name: str             # 'CPI MoM'
    topic: str
    country_code: str
    frequency: str                  # monthly, quarterly, etc.
    unit: str
    obs_family_id: str | None = None
    is_active: bool = True
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class CalendarIndicatorAliasRecord:
    alias_normalized: str           # lowercased, stripped
    indicator_id: str               # FK → calendar_indicator
    source: str                     # 'investing'|'forexfactory'|'tradingeconomics'
    country_code: str
    alias_original: str = ""
    created_at: str = ""


@dataclass(frozen=True)
class CalendarEventVintageRecord:
    event_id: str
    provider: str           # connector source ("trading_economics", "ec_bcs", ...)
    vintage_date: str       # source LastUpdate when available, else observed_at
    observed_at: str        # authoritative for PIT ordering (ISO-8601 UTC)
    actual: str | None
    forecast: str | None
    previous: str | None
    metadata: dict[str, Any] = field(default_factory=dict)
    source_url: str = ""    # snapshotted at insert; revision-aware (issue #36)
    evidence_archive_url: str | None = None  # Wayback snapshot URL when archived
