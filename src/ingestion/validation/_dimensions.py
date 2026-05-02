from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from ._types import CheckResult, ValidationLayer, ValidationSeverity

# ── Known valid dimension values ─────────────────────────────────
# Derived from the obs_family seed data in sqlite.py.

VALID_FREQUENCIES: frozenset[str] = frozenset({
    "daily",
    "weekly",
    "monthly",
    "quarterly",
    "annual",
    "irregular",
})

VALID_UNITS: frozenset[str] = frozenset({
    "percent",
    "index",
    "thousands",
    "millions",
    "billions_usd",
    "millions_usd",
    "millions_eur",
    "usd_per_barrel",
    "usd_per_mmbtu",
    "thousand_barrels",
    "thousand_barrels_per_day",
    "ratio",
    "lcu",
    "usd",
    "hours",
    "net_tons",
    "percentage_points",
})

VALID_SEASONAL_ADJUSTMENTS: frozenset[str] = frozenset({
    "sa",
    "nsa",
    "saar",
    "none",
})

VALID_COUNTRY_CODES: frozenset[str] = frozenset({
    "US", "EU", "JP", "CN", "UK", "GB", "CH", "XX",
    "DE", "FR", "IT", "ES", "CA", "AU", "KR", "IN",
    "BR", "MX", "RU", "ZA", "TR", "SE", "NO", "DK",
    "PL", "NL", "BE", "AT", "IE", "FI", "PT", "GR",
    "NZ", "SG", "HK", "TW", "TH", "ID", "MY", "PH",
    "AR", "CL", "CO", "PE", "IL", "SA", "AE", "EG",
    "NG", "KE",
})

VALID_SOURCE_TYPES: frozenset[str] = frozenset({
    "data_aggregator",
    "government_agency",
    "central_bank",
    "market_data",
})

VALID_SOURCE_IDS: frozenset[str] = frozenset({
    "fred",
    "eia",
    "treasury_fiscal",
    "nyfed",
    "rateprobability",
    "imf",
    "eurostat",
    "bis",
    "ecb",
    "bundesbank",
    "mof_jp",
    "aisi",
    "ism",
    "redbook",
    "oecd",
    "worldbank",
    "bls",
    "unsd",
    "ilo",
})


def check_dimensions(
    source: str,
    families: list[Any],
) -> list[CheckResult]:
    """Validate dimension values on observation family records.

    Checks that frequency, unit, seasonal_adjustment, and country_code
    are drawn from known valid sets. Catches corrupted metadata before
    it propagates to downstream analytics.

    families: list of ObsFamilyRecord or dicts with the same keys.
    """
    results: list[CheckResult] = []
    now = datetime.now(UTC).isoformat()
    total = len(families)

    if total == 0:
        return results

    bad_frequency: list[str] = []
    bad_unit: list[str] = []
    bad_seasonal: list[str] = []
    bad_country: list[str] = []
    bad_source: list[str] = []

    for fam in families:
        if isinstance(fam, dict):
            fid = fam.get("family_id", "?")
            freq = fam.get("frequency", "")
            unit = fam.get("unit", "")
            sa = fam.get("seasonal_adjustment", "")
            cc = fam.get("country_code", "")
            sid = fam.get("source_id", "")
        else:
            fid = getattr(fam, "family_id", "?")
            freq = getattr(fam, "frequency", "")
            unit = getattr(fam, "unit", "")
            sa = getattr(fam, "seasonal_adjustment", "")
            cc = getattr(fam, "country_code", "")
            sid = getattr(fam, "source_id", "")

        if freq and freq not in VALID_FREQUENCIES:
            bad_frequency.append(f"{fid}={freq}")
        if unit and unit not in VALID_UNITS:
            bad_unit.append(f"{fid}={unit}")
        if sa and sa not in VALID_SEASONAL_ADJUSTMENTS:
            bad_seasonal.append(f"{fid}={sa}")
        if cc and cc not in VALID_COUNTRY_CODES:
            bad_country.append(f"{fid}={cc}")
        if sid and sid not in VALID_SOURCE_IDS:
            bad_source.append(f"{fid}={sid}")

    # ── Frequency ────────────────────────────────────────────────
    passed = len(bad_frequency) == 0
    results.append(
        CheckResult(
            check_name="dimension_frequency",
            layer=ValidationLayer.SCHEMA,
            passed=passed,
            severity=ValidationSeverity.ERROR if not passed else ValidationSeverity.INFO,
            message=(
                f"{source}: all {total} families have valid frequency"
                if passed
                else f"{source}: {len(bad_frequency)} invalid frequencies: {bad_frequency[:5]}"
            ),
            source=source,
            timestamp=now,
            details={"invalid": bad_frequency, "total": total},
        )
    )

    # ── Unit ─────────────────────────────────────────────────────
    passed = len(bad_unit) == 0
    results.append(
        CheckResult(
            check_name="dimension_unit",
            layer=ValidationLayer.SCHEMA,
            passed=passed,
            severity=ValidationSeverity.ERROR if not passed else ValidationSeverity.INFO,
            message=(
                f"{source}: all {total} families have valid unit"
                if passed
                else f"{source}: {len(bad_unit)} invalid units: {bad_unit[:5]}"
            ),
            source=source,
            timestamp=now,
            details={"invalid": bad_unit, "total": total},
        )
    )

    # ── Seasonal adjustment ──────────────────────────────────────
    passed = len(bad_seasonal) == 0
    results.append(
        CheckResult(
            check_name="dimension_seasonal_adjustment",
            layer=ValidationLayer.SCHEMA,
            passed=passed,
            severity=ValidationSeverity.ERROR if not passed else ValidationSeverity.INFO,
            message=(
                f"{source}: all {total} families have valid seasonal_adjustment"
                if passed
                else f"{source}: {len(bad_seasonal)} invalid seasonal adjustments: {bad_seasonal[:5]}"
            ),
            source=source,
            timestamp=now,
            details={"invalid": bad_seasonal, "total": total},
        )
    )

    # ── Country code ─────────────────────────────────────────────
    passed = len(bad_country) == 0
    results.append(
        CheckResult(
            check_name="dimension_country_code",
            layer=ValidationLayer.SCHEMA,
            passed=passed,
            severity=ValidationSeverity.WARNING if not passed else ValidationSeverity.INFO,
            message=(
                f"{source}: all {total} families have valid country_code"
                if passed
                else f"{source}: {len(bad_country)} unrecognized country codes: {bad_country[:5]}"
            ),
            source=source,
            timestamp=now,
            details={"invalid": bad_country, "total": total},
        )
    )

    # ── Source ID ────────────────────────────────────────────────
    passed = len(bad_source) == 0
    results.append(
        CheckResult(
            check_name="dimension_source_id",
            layer=ValidationLayer.SCHEMA,
            passed=passed,
            severity=ValidationSeverity.ERROR if not passed else ValidationSeverity.INFO,
            message=(
                f"{source}: all {total} families have valid source_id"
                if passed
                else f"{source}: {len(bad_source)} invalid source_ids: {bad_source[:5]}"
            ),
            source=source,
            timestamp=now,
            details={"invalid": bad_source, "total": total},
        )
    )

    return results
