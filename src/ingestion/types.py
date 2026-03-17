"""Unified types for the ingestion pipeline.

RawObservation / RawSeries — output of every fetcher adapter.
CanonicalRow — normalised observation ready for storage and cross-source comparison.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RawObservation:
    """A single raw data point from any source."""

    date: str  # YYYY-MM-DD (always normalised at the fetcher boundary)
    value: float
    provider_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RawSeries:
    """Unified output from any fetcher — a batch of observations for one series."""

    source: str  # 'fred', 'bls', 'eia', 'imf', …
    series_id: str  # provider series ID: 'CPIAUCSL', 'CUUR0000SA0'
    observations: tuple[RawObservation, ...]
    fetched_at: str  # ISO-8601 timestamp
    series_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CanonicalRow:
    """Fully normalised observation ready for storage and cross-source comparison."""

    series_id: str
    source: str
    date: str  # YYYY-MM-DD
    value: float
    country_code: str  # ISO 3166-1 alpha-2: 'US', 'CN', 'JP'
    frequency: str  # daily / weekly / monthly / quarterly / annual
    concept: str  # cpi, gdp, unemployment, …
    unit: str  # index, percent, billions_usd, …
    seasonal_adjustment: str  # sa / nsa / saar / none
    obs_family_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
