"""Normalization layer — RawSeries → list[CanonicalRow].

Enriches raw fetcher output with country code, frequency, concept,
unit, and seasonal adjustment using the obs_family registry.
Falls back to series_metadata when no family entry exists.

**Date normalization** is the single point of truth: all observation
dates are coerced to ``YYYY-MM-DD`` using "period start" convention:

    daily      → as-is
    weekly     → Monday of the ISO week
    monthly    → first of month  (YYYY-MM-01)
    quarterly  → first of quarter (01-01, 04-01, 07-01, 10-01)
    annual     → first of year   (YYYY-01-01)
"""

from __future__ import annotations

import re
from datetime import date as _date
from typing import Any

from ingestion.types import CanonicalRow, RawSeries
from storage.sqlite import ObsFamilyRecord

# ── Frequency aliases ─────────────────────────────────────────────────

_FREQ_ALIASES: dict[str, str] = {
    "d": "daily",
    "w": "weekly",
    "m": "monthly",
    "q": "quarterly",
    "a": "annual",
    "D": "daily",
    "W": "weekly",
    "Q": "quarterly",
    "M": "monthly",
    "A": "annual",
}

# ── Quarter maps ──────────────────────────────────────────────────────

_QUARTER_START_MONTH: dict[int, int] = {1: 1, 2: 4, 3: 7, 4: 10}
_QUARTER_END_TO_START: dict[str, str] = {
    "03-31": "01-01",
    "06-30": "04-01",
    "09-30": "07-01",
    "12-31": "10-01",
}
_SEMESTER_START: dict[int, int] = {1: 1, 2: 7}


# ── Unified date normalization ────────────────────────────────────────

def normalize_observation_date(raw: str, frequency: str) -> str:
    """Normalize an observation date to ``YYYY-MM-DD`` using period-start.

    Convention (period start):
        daily      → YYYY-MM-DD (unchanged)
        weekly     → Monday of the ISO week
        monthly    → YYYY-MM-01 (first of month)
        quarterly  → first of quarter  (01-01 / 04-01 / 07-01 / 10-01)
        annual     → YYYY-01-01 (first of year)

    Handles diverse input formats from all providers:
        YYYY-MM-DD, YYYY-MM, YYYY, YYYY-Qn, YYYYQn,
        YYYY-Mn, YYYYMnn, YYYY-Sn, YYYY-Wnn
    """
    raw = raw.strip()
    if not raw:
        return raw

    # ── Already full date (YYYY-MM-DD) ────────────────────────────
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
    if m:
        if frequency == "quarterly":
            # Fix BEA-style quarter-end → quarter-start
            suffix = f"{m.group(2)}-{m.group(3)}"
            if suffix in _QUARTER_END_TO_START:
                return f"{m.group(1)}-{_QUARTER_END_TO_START[suffix]}"
            return raw
        if frequency == "annual":
            return f"{m.group(1)}-01-01"
        if frequency == "monthly":
            return f"{m.group(1)}-{m.group(2)}-01"
        return raw  # daily / weekly — keep as-is

    # ── YYYY-MM (monthly without day) ─────────────────────────────
    m = re.match(r"^(\d{4})-(\d{2})$", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"

    # ── Quarter formats: YYYY-Qn or YYYYQn ───────────────────────
    m = re.match(r"^(\d{4})-?Q(\d)$", raw)
    if m:
        year, q = m.group(1), int(m.group(2))
        month = _QUARTER_START_MONTH.get(q, 1)
        return f"{year}-{month:02d}-01"

    # ── IMF monthly: YYYY-Mnn ─────────────────────────────────────
    m = re.match(r"^(\d{4})-M(\d{2})$", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"

    # ── Eurostat monthly (no dash): YYYYMnn ───────────────────────
    m = re.match(r"^(\d{4})M(\d{2})$", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"

    # ── Semester: YYYY-Sn ─────────────────────────────────────────
    m = re.match(r"^(\d{4})-S([12])$", raw)
    if m:
        year, s = m.group(1), int(m.group(2))
        month = _SEMESTER_START.get(s, 1)
        return f"{year}-{month:02d}-01"

    # ── ISO week: YYYY-Wnn ────────────────────────────────────────
    m = re.match(r"^(\d{4})-W(\d{2})$", raw)
    if m:
        try:
            monday = _date.fromisocalendar(int(m.group(1)), int(m.group(2)), 1)
            return monday.isoformat()
        except ValueError:
            return raw

    # ── Bare year: YYYY ───────────────────────────────────────────
    m = re.match(r"^(\d{4})$", raw)
    if m:
        return f"{m.group(1)}-01-01"

    # ── Fallthrough: return as-is (log-worthy but don't crash) ────
    return raw


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
        # Resolve frequency once per series (same for all observations)
        frequency = self._resolve_frequency(family, series)
        rows: list[CanonicalRow] = []
        for obs in series.observations:
            rows.append(
                CanonicalRow(
                    series_id=series.series_id,
                    source=series.source,
                    date=normalize_observation_date(obs.date, frequency),
                    value=obs.value,
                    country_code=self._resolve_country(family, series),
                    frequency=frequency,
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
