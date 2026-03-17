"""Fetcher adapters — thin wrappers converting source-specific clients to RawSeries.

Each adapter wraps an existing ``*Client`` and converts its observation type
into the unified ``RawSeries`` / ``RawObservation`` contract defined in
``ingestion.types``.
"""

from __future__ import annotations

from typing import Protocol

from ingestion.types import RawSeries


class Fetcher(Protocol):
    """Minimal interface every fetcher adapter must satisfy."""

    source_name: str

    def fetch(self, *, lookback_days: int = 365) -> list[RawSeries]: ...

    def fetch_series(
        self, series_id: str, *, lookback_days: int = 365
    ) -> RawSeries | None: ...


__all__ = ["Fetcher", "RawSeries"]
