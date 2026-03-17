"""Normalization layer — RawSeries → list[CanonicalRow].

Enriches raw fetcher output with country code, frequency, concept,
unit, and seasonal adjustment using the obs_family registry.
Falls back to series_metadata when no family entry exists.
"""

from __future__ import annotations

from typing import Any

from ingestion.types import CanonicalRow, RawSeries
from storage.sqlite import ObsFamilyRecord

# Frequency aliases used by various series configs
_FREQ_ALIASES: dict[str, str] = {
    "d": "daily",
    "w": "weekly",
    "m": "monthly",
    "q": "quarterly",
    "a": "annual",
    "Q": "quarterly",
    "M": "monthly",
    "A": "annual",
}


class Normalizer:
    """Converts ``RawSeries`` into ``CanonicalRow`` records.

    Parameters
    ----------
    family_lookup
        Mapping of ``(source_id, provider_series_id)`` → ``ObsFamilyRecord``.
        Built once from ``SQLiteEngineStore.list_obs_families()``.
    """

    def __init__(
        self,
        family_lookup: dict[tuple[str, str], ObsFamilyRecord] | None = None,
    ) -> None:
        self._lookup: dict[tuple[str, str], ObsFamilyRecord] = family_lookup or {}

    # ── public API ────────────────────────────────────────────────────

    def normalize(self, series: RawSeries) -> list[CanonicalRow]:
        """Convert a single ``RawSeries`` into a list of ``CanonicalRow``."""
        family = self._lookup.get((series.source, series.series_id))
        rows: list[CanonicalRow] = []
        for obs in series.observations:
            rows.append(
                CanonicalRow(
                    series_id=series.series_id,
                    source=series.source,
                    date=obs.date,
                    value=obs.value,
                    country_code=self._resolve_country(family, series),
                    frequency=self._resolve_frequency(family, series),
                    concept=self._resolve_concept(family, series),
                    unit=self._resolve_unit(family, series),
                    seasonal_adjustment=self._resolve_sa(family, series),
                    obs_family_id=family.family_id if family else None,
                    metadata={**obs.provider_metadata, **series.series_metadata},
                )
            )
        return rows

    def normalize_batch(self, batch: list[RawSeries]) -> list[CanonicalRow]:
        """Normalise multiple series at once."""
        rows: list[CanonicalRow] = []
        for series in batch:
            rows.extend(self.normalize(series))
        return rows

    # ── helpers to build the lookup ───────────────────────────────────

    @staticmethod
    def build_family_lookup(
        families: list[ObsFamilyRecord],
    ) -> dict[tuple[str, str], ObsFamilyRecord]:
        """Build the ``(source, series_id) → ObsFamilyRecord`` dict.

        Call with ``store.list_obs_families(active_only=False)``.
        """
        return {
            (f.source_id, f.provider_series_id): f
            for f in families
        }

    # ── field resolution ──────────────────────────────────────────────

    @staticmethod
    def _resolve_country(
        family: ObsFamilyRecord | None, series: RawSeries
    ) -> str:
        if family and family.country_code:
            return family.country_code
        meta = series.series_metadata
        # Try common metadata keys
        for key in ("country_code", "country", "ref_area"):
            val = meta.get(key, "")
            if val:
                return str(val)[:2].upper()
        return ""

    @staticmethod
    def _resolve_frequency(
        family: ObsFamilyRecord | None, series: RawSeries
    ) -> str:
        if family and family.frequency:
            return family.frequency
        raw = series.series_metadata.get("freq", "")
        return _FREQ_ALIASES.get(raw, raw)

    @staticmethod
    def _resolve_concept(
        family: ObsFamilyRecord | None, series: RawSeries
    ) -> str:
        if family and family.topic_code:
            return family.topic_code
        return series.series_metadata.get("category", "")

    @staticmethod
    def _resolve_unit(
        family: ObsFamilyRecord | None, series: RawSeries
    ) -> str:
        if family and family.unit:
            return family.unit
        return series.series_metadata.get("unit", "")

    @staticmethod
    def _resolve_sa(
        family: ObsFamilyRecord | None, series: RawSeries
    ) -> str:
        if family and family.seasonal_adjustment:
            return family.seasonal_adjustment
        return series.series_metadata.get("seasonal_adjustment", "none")
