from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ._store import ValidationStore
from ._types import CheckResult, ValidationLayer, ValidationSeverity


@dataclass(frozen=True)
class VolumeExpectation:
    """Expected observation count range for a data source.

    min_rows / max_rows define the acceptable range.
    Set max_rows=0 to skip the upper bound check.
    """

    source: str
    min_rows: int
    max_rows: int = 0
    description: str = ""


# Default expectations based on typical catalog sizes.
# These should be tuned per deployment.
DEFAULT_VOLUME_EXPECTATIONS: dict[str, VolumeExpectation] = {
    "fred": VolumeExpectation("fred", min_rows=500, description="FRED daily series"),
    "oecd": VolumeExpectation("oecd", min_rows=100, description="OECD macro series"),
    "worldbank": VolumeExpectation("worldbank", min_rows=100, description="World Bank indicators"),
    "imf": VolumeExpectation("imf", min_rows=50, description="IMF macro indicators"),
    "eurostat": VolumeExpectation("eurostat", min_rows=20, description="Eurostat EU data"),
    "bis": VolumeExpectation("bis", min_rows=20, description="BIS policy rates"),
    "ecb": VolumeExpectation("ecb", min_rows=20, description="ECB money supply"),
    "eia": VolumeExpectation("eia", min_rows=50, description="EIA energy data"),
    "treasury_fiscal": VolumeExpectation("treasury_fiscal", min_rows=20, description="Treasury fiscal"),
    "nyfed": VolumeExpectation("nyfed", min_rows=10, description="NY Fed rates"),
}


def check_volume(
    source: str,
    observation_count: int,
    expectation: VolumeExpectation | None = None,
    validation_store: ValidationStore | None = None,
) -> list[CheckResult]:
    """Validate total dataset volume against expected range.

    If no explicit expectation is provided, uses stored baseline
    with a 20% drop threshold for regression detection.
    """
    results: list[CheckResult] = []
    now = datetime.now(UTC).isoformat()

    if expectation is not None:
        # ── Check against explicit range ─────────────────────────
        below_min = observation_count < expectation.min_rows
        above_max = expectation.max_rows > 0 and observation_count > expectation.max_rows

        if below_min:
            results.append(
                CheckResult(
                    check_name="volume_below_minimum",
                    layer=ValidationLayer.SERIES,
                    passed=False,
                    severity=ValidationSeverity.ERROR,
                    message=(
                        f"{source}: {observation_count} observations, "
                        f"expected >= {expectation.min_rows}"
                    ),
                    source=source,
                    timestamp=now,
                    details={
                        "count": observation_count,
                        "min_expected": expectation.min_rows,
                    },
                )
            )
        elif above_max:
            results.append(
                CheckResult(
                    check_name="volume_above_maximum",
                    layer=ValidationLayer.SERIES,
                    passed=False,
                    severity=ValidationSeverity.WARNING,
                    message=(
                        f"{source}: {observation_count} observations, "
                        f"expected <= {expectation.max_rows}"
                    ),
                    source=source,
                    timestamp=now,
                    details={
                        "count": observation_count,
                        "max_expected": expectation.max_rows,
                    },
                )
            )
        else:
            results.append(
                CheckResult(
                    check_name="volume_in_range",
                    layer=ValidationLayer.SERIES,
                    passed=True,
                    severity=ValidationSeverity.INFO,
                    message=f"{source}: {observation_count} observations (expected range: {expectation.min_rows}-{expectation.max_rows or '∞'})",
                    source=source,
                    timestamp=now,
                    details={"count": observation_count},
                )
            )

    # ── Regression detection against stored baseline ─────────────
    if validation_store is not None:
        stored = validation_store.get_baseline(source, "_global", "volume_count")

        if stored is not None:
            baseline_count = stored.get("count", 0)
            if baseline_count > 0:
                ratio = observation_count / baseline_count
                drop_pct = (1 - ratio) * 100
                passed = ratio >= 0.8  # alert on >20% drop
                results.append(
                    CheckResult(
                        check_name="volume_regression",
                        layer=ValidationLayer.SERIES,
                        passed=passed,
                        severity=ValidationSeverity.ERROR if not passed else ValidationSeverity.INFO,
                        message=(
                            f"{source}: {observation_count} observations "
                            f"(baseline: {baseline_count}, "
                            f"{'dropped' if drop_pct > 0 else 'grew'} {abs(drop_pct):.1f}%)"
                        ),
                        source=source,
                        timestamp=now,
                        details={
                            "current_count": observation_count,
                            "baseline_count": baseline_count,
                            "ratio": round(ratio, 4),
                        },
                    )
                )

        # Update baseline
        validation_store.save_baseline(
            source, "_global", "volume_count",
            {"count": observation_count},
            now,
        )

    return results


def check_volume_batch(
    source_counts: dict[str, int],
    expectations: dict[str, VolumeExpectation] | None = None,
    validation_store: ValidationStore | None = None,
) -> list[CheckResult]:
    """Run volume checks for multiple sources."""
    expectations = expectations or DEFAULT_VOLUME_EXPECTATIONS
    results: list[CheckResult] = []
    for source, count in source_counts.items():
        exp = expectations.get(source)
        results.extend(
            check_volume(source, count, expectation=exp, validation_store=validation_store)
        )
    return results
