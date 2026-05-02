"""Storage records — indicator + obs-family + concept-map + release-schedule + central-bank-communication records.

Extracted out of src/storage/sqlite.py as part of issue #58 Tier 2.1A —
pure mechanical split, no behavior change. The records are re-exported by
storage.sqlite for backwards compatibility, so existing
``from storage.sqlite import XRecord`` consumers keep working.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any


@dataclass(frozen=True)
class CentralBankCommunicationRecord:
    source: str
    title: str
    url: str
    timestamp: int
    content_type: str
    speaker: str = ""
    summary: str = ""
    full_text: str = ""


@dataclass(frozen=True)
class IndicatorObservationRecord:
    series_id: str
    source: str
    date: str
    value: float
    metadata: dict[str, Any] = field(default_factory=dict)
    obs_family_id: str | None = None


@dataclass(frozen=True)
class IndicatorVintageRecord:
    series_id: str
    source: str
    observation_date: str   # the date being measured
    vintage_date: str       # when this measurement was published
    value: float
    metadata: dict[str, Any] = field(default_factory=dict)
    obs_family_id: str | None = None
    # `native_pit` (source exposes a real vintage_date, e.g. ALFRED),
    # `synthetic_snapshot` (we tag vintage_date = scrape_time, value-change
    # triggered), or `single_observation` (only seen once, no revision
    # context). Default is the safest tag — explicit fetchers should pass
    # `synthetic_snapshot` or `native_pit`.
    vintage_quality: str = "single_observation"


@dataclass(frozen=True)
class ObsRawRecord:
    """Audit-lane raw row for the macro time-series table (issue #69 slice 1).

    Mirrors ``CalendarRawRecord`` in shape — one row per HTTP response per
    ``(source, series_id)``. ``content_hash`` is sha256 over the
    canonicalized observations (sorted by date, query-time echo fields
    dropped) so re-fetching unchanged data dedupes via INSERT OR IGNORE.

    Why the audit lane: every value in ``indicators`` /
    ``indicator_vintages`` is reproducible from raw, so a parser bug or
    upstream schema rename can be fixed and re-projected without spending
    FRED/BLS/SDMX quota; restated observations land as new rows preserving
    the revision chain.
    """

    source: str             # 'fred' | 'bls' | 'eia' | 'imf' | 'eurostat' | …
    series_id: str          # source-native id, e.g. 'GDP' (FRED), 'CUUR0000SA0' (BLS)
    snapshot_epoch_ms: int  # when WE fetched this snapshot, UTC ms
    content_hash: str       # sha256 over canonicalized response
    payload_json: str       # full HTTP response body, verbatim
    fetched_at: str         # ISO-8601 UTC convenience column
    request_params_json: str = "{}"  # from/to/units/etc — needed to interpret partial responses


@dataclass(frozen=True)
class ObsSourceRecord:
    source_id: str          # 'fred', 'eia', 'treasury_fiscal'
    source_code: str
    source_name: str
    source_type: str        # data_aggregator, government_agency, central_bank, exchange, market_data
    country_code: str
    homepage_url: str
    api_base_url: str
    is_active: bool
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class ObsFamilyRecord:
    family_id: str                  # 'us.inflation.cpi_all'
    source_id: str                  # 'fred'
    provider_series_id: str         # 'CPIAUCSL' (matches indicators.series_id)
    canonical_name: str             # 'CPI All Urban Consumers'
    short_name: str
    unit: str                       # 'index', 'percent', 'billions_usd'
    frequency: str                  # daily, weekly, monthly, quarterly, annual, irregular
    seasonal_adjustment: str        # sa, nsa, saar, none
    country_code: str
    topic_code: str                 # inflation, employment, rates, energy, fiscal
    category: str                   # consumer_prices, treasury_yields
    is_active: bool
    has_vintages: bool
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class ObsFamilyDocumentRecord:
    family_id: str
    release_family_id: str
    relationship: str           # produced_by, derived_from, related_to
    created_at: str


@dataclass(frozen=True)
class ConceptMapRecord:
    """Cross-source indicator mapping — groups equivalent series under one concept."""
    concept_id: str             # 'CPI_US', 'GDP_REAL_US'
    source_id: str              # 'fred', 'bls', 'imf'
    provider_series_id: str     # 'CPIAUCSL', 'CUUR0000SA0'
    obs_family_id: str          # FK to obs_family.family_id (may be empty)
    priority: int = 0           # 1 = authoritative, 2 = secondary, 3 = tertiary
    role: str = "primary"       # primary, secondary, cross_check
    notes: str = ""
    created_at: str = ""


@dataclass(frozen=True)
class ResolvedObservation:
    """A single resolved value for a concept on a given date, with provenance."""
    concept_id: str
    date: str
    value: float
    source_id: str
    provider_series_id: str
    priority: int
    role: str
    alternates: int = 0         # how many other sources also had this date
    vintage: str = "initial"    # "initial", "revised", or "unknown"
    revision_count: int = 0     # number of vintage entries for this obs date


@dataclass(frozen=True)
class ReleaseScheduleRecord:
    concept_id: str           # PK, FK to concept_map
    rule_type: str            # "day_of_month", "weekday_of_month",
                              # "business_day_of_month", "quarter_lag", "daily",
                              # "weekly", "fixed_dates", "approximate_window"
    rule_json: dict[str, Any] # type-specific params
    frequency: str            # "daily", "weekly", "monthly", "quarterly", "annual"
    release_time_utc: str     # "12:30", "14:00", "" if unknown
    timezone: str             # "America/New_York", "" if unknown
    source_authority: str     # "manual", "fred_api", "calendar_events"
    confidence: str           # "exact", "pattern", "approximate"
    next_expected: str        # ISO datetime, precomputed
    last_released: str        # ISO datetime, updated after successful fetch
    last_checked: str         # ISO datetime, updated on every check
    is_active: bool = True
    notes: str = ""
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class ReleaseStatusRecord:
    """Tracks per-release availability status with retry state."""
    concept_id: str
    release_date: str           # expected release date (ISO)
    status: str                 # PENDING, WAITING, FETCHED, CONFIRMED, STALE, FAILED
    attempt_count: int = 0
    next_retry: str = ""        # ISO datetime for next retry
    last_attempt: str = ""      # ISO datetime of last fetch attempt
    source_used: str = ""       # source_id that provided data
    data_date: str = ""         # latest observation date actually fetched
    expected_period: str = ""   # minimum expected observation date
    provisional: bool = False   # True if using fallback source
    error: str = ""
    created_at: str = ""
    updated_at: str = ""
