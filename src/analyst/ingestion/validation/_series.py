from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from ._types import CheckResult, ValidationLayer, ValidationSeverity


def check_series_integrity(
    source: str,
    series_id: str,
    observations: list[Any],
    *,
    expected_count: int | None = None,
    expected_min_year: int | None = None,
    expected_max_year: int | None = None,
    max_missing_rate: float = 0.5,
) -> list[CheckResult]:
    """Validate integrity of a single time series.

    Checks row count, year coverage, and missing rate.
    Observations are expected to have `date` and `value` attributes or keys.
    """
    results: list[CheckResult] = []
    now = datetime.now(UTC).isoformat()
    actual_count = len(observations)

    # ── Row count check ──────────────────────────────────────────
    if expected_count is not None:
        passed = actual_count == expected_count
        results.append(
            CheckResult(
                check_name="series_row_count",
                layer=ValidationLayer.SERIES,
                passed=passed,
                severity=ValidationSeverity.ERROR if not passed else ValidationSeverity.INFO,
                message=(
                    f"{series_id}: {actual_count}/{expected_count} rows"
                    if passed
                    else f"{series_id}: expected {expected_count} rows, got {actual_count}"
                ),
                source=source,
                series_id=series_id,
                timestamp=now,
                details={"expected": expected_count, "actual": actual_count},
            )
        )

    if actual_count == 0:
        results.append(
            CheckResult(
                check_name="series_empty",
                layer=ValidationLayer.SERIES,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"{series_id}: series is empty",
                source=source,
                series_id=series_id,
                timestamp=now,
            )
        )
        return results

    # ── Extract dates and values ─────────────────────────────────
    dates: list[str] = []
    null_count = 0
    for obs in observations:
        if isinstance(obs, dict):
            d = obs.get("date", "")
            v = obs.get("value")
        else:
            d = getattr(obs, "date", "")
            v = getattr(obs, "value", None)
        if d:
            dates.append(str(d))
        if v is None:
            null_count += 1

    # ── Missing rate check ───────────────────────────────────────
    missing_rate = null_count / actual_count if actual_count > 0 else 0.0
    passed = missing_rate <= max_missing_rate
    results.append(
        CheckResult(
            check_name="series_missing_rate",
            layer=ValidationLayer.SERIES,
            passed=passed,
            severity=ValidationSeverity.WARNING if not passed else ValidationSeverity.INFO,
            message=f"{series_id}: missing rate {missing_rate:.1%} ({null_count}/{actual_count})",
            source=source,
            series_id=series_id,
            timestamp=now,
            details={
                "missing_rate": round(missing_rate, 4),
                "null_count": null_count,
                "total": actual_count,
            },
        )
    )

    # ── Year coverage check ──────────────────────────────────────
    years: list[int] = []
    for d in dates:
        try:
            years.append(int(d[:4]))
        except (ValueError, IndexError):
            pass

    if years:
        min_year = min(years)
        max_year = max(years)

        if expected_min_year is not None:
            passed = min_year <= expected_min_year
            results.append(
                CheckResult(
                    check_name="series_min_year",
                    layer=ValidationLayer.SERIES,
                    passed=passed,
                    severity=ValidationSeverity.WARNING if not passed else ValidationSeverity.INFO,
                    message=f"{series_id}: earliest year {min_year} (expected <= {expected_min_year})",
                    source=source,
                    series_id=series_id,
                    timestamp=now,
                    details={"min_year": min_year, "expected_min_year": expected_min_year},
                )
            )

        if expected_max_year is not None:
            passed = max_year >= expected_max_year
            results.append(
                CheckResult(
                    check_name="series_max_year",
                    layer=ValidationLayer.SERIES,
                    passed=passed,
                    severity=ValidationSeverity.WARNING if not passed else ValidationSeverity.INFO,
                    message=f"{series_id}: latest year {max_year} (expected >= {expected_max_year})",
                    source=source,
                    series_id=series_id,
                    timestamp=now,
                    details={"max_year": max_year, "expected_max_year": expected_max_year},
                )
            )

        year_span = max_year - min_year + 1
        unique_years = len(set(years))
        results.append(
            CheckResult(
                check_name="series_year_coverage",
                layer=ValidationLayer.SERIES,
                passed=True,
                severity=ValidationSeverity.INFO,
                message=f"{series_id}: {unique_years} unique years spanning {min_year}-{max_year} ({year_span} year range)",
                source=source,
                series_id=series_id,
                timestamp=now,
                details={
                    "unique_years": unique_years,
                    "min_year": min_year,
                    "max_year": max_year,
                    "year_span": year_span,
                },
            )
        )

    return results


def check_series_batch(
    source: str,
    series_map: dict[str, list[Any]],
    *,
    max_missing_rate: float = 0.5,
) -> list[CheckResult]:
    """Run integrity checks on multiple series from the same source."""
    results: list[CheckResult] = []
    for series_id, observations in series_map.items():
        results.extend(
            check_series_integrity(
                source,
                series_id,
                observations,
                max_missing_rate=max_missing_rate,
            )
        )
    return results
