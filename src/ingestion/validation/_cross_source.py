from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ._types import CheckResult, ValidationLayer, ValidationSeverity


@dataclass(frozen=True)
class CrossSourcePair:
    """Defines two observation families that measure the same economic concept."""

    family_id_a: str
    family_id_b: str
    comparison_type: str  # "level", "direction", "correlation"
    tolerance_pct: float  # max allowed percentage difference for level checks
    description: str = ""


# ── Curated cross-source pairs ───────────────────────────────────
# These rely on obs_family IDs defined in sources.py.
# Only add pairs where the underlying concept is truly the same.

CROSS_SOURCE_PAIRS: list[CrossSourcePair] = [
    CrossSourcePair(
        "us.employment.unemployment",
        "us.employment.unemployment_oecd",
        "level",
        0.5,
        "US unemployment rate: FRED vs OECD",
    ),
    CrossSourcePair(
        "us.rates.fed_funds",
        "us.rates.policy_bis",
        "level",
        0.25,
        "US policy rate: FRED vs BIS",
    ),
    CrossSourcePair(
        "eu.rates.deposit_ecb",
        "eu.rates.policy_bis",
        "level",
        0.25,
        "EU policy rate: ECB vs BIS",
    ),
]


def check_cross_source(
    pair: CrossSourcePair,
    observations_a: list[dict[str, Any]],
    observations_b: list[dict[str, Any]],
    *,
    lookback_periods: int = 12,
) -> list[CheckResult]:
    """Compare two observation families that should measure the same thing.

    observations_a/b should be lists of dicts with 'date' and 'value' keys,
    sorted by date descending.
    """
    results: list[CheckResult] = []
    now = datetime.now(UTC).isoformat()

    if not observations_a or not observations_b:
        results.append(
            CheckResult(
                check_name="cross_source_data_available",
                layer=ValidationLayer.CROSS_SOURCE,
                passed=False,
                severity=ValidationSeverity.WARNING,
                message=(
                    f"Cross-source check skipped: {pair.family_id_a} has {len(observations_a)} obs, "
                    f"{pair.family_id_b} has {len(observations_b)} obs"
                ),
                source=f"{pair.family_id_a} vs {pair.family_id_b}",
                timestamp=now,
            )
        )
        return results

    # Build date-indexed lookups
    map_a: dict[str, float] = {}
    for obs in observations_a[:lookback_periods * 2]:
        d = obs.get("date", "")
        v = obs.get("value")
        if d and v is not None:
            map_a[d] = float(v)

    map_b: dict[str, float] = {}
    for obs in observations_b[:lookback_periods * 2]:
        d = obs.get("date", "")
        v = obs.get("value")
        if d and v is not None:
            map_b[d] = float(v)

    common_dates = sorted(set(map_a.keys()) & set(map_b.keys()), reverse=True)[
        :lookback_periods
    ]

    if not common_dates:
        results.append(
            CheckResult(
                check_name="cross_source_date_overlap",
                layer=ValidationLayer.CROSS_SOURCE,
                passed=False,
                severity=ValidationSeverity.WARNING,
                message=f"No overlapping dates between {pair.family_id_a} and {pair.family_id_b}",
                source=f"{pair.family_id_a} vs {pair.family_id_b}",
                timestamp=now,
            )
        )
        return results

    if pair.comparison_type == "level":
        diffs: list[float] = []
        for d in common_dates:
            va, vb = map_a[d], map_b[d]
            denom = max(abs(va), abs(vb), 1e-10)
            pct_diff = abs(va - vb) / denom * 100
            diffs.append(pct_diff)

        max_diff = max(diffs)
        avg_diff = sum(diffs) / len(diffs)
        passed = max_diff <= pair.tolerance_pct

        results.append(
            CheckResult(
                check_name="cross_source_level",
                layer=ValidationLayer.CROSS_SOURCE,
                passed=passed,
                severity=ValidationSeverity.WARNING if not passed else ValidationSeverity.INFO,
                message=(
                    f"{pair.description}: max diff {max_diff:.2f}%, avg {avg_diff:.2f}% "
                    f"(tolerance: {pair.tolerance_pct}%, {len(common_dates)} dates compared)"
                ),
                source=f"{pair.family_id_a} vs {pair.family_id_b}",
                timestamp=now,
                details={
                    "max_diff_pct": round(max_diff, 4),
                    "avg_diff_pct": round(avg_diff, 4),
                    "dates_compared": len(common_dates),
                    "tolerance_pct": pair.tolerance_pct,
                },
            )
        )

    elif pair.comparison_type == "direction":
        agreements = 0
        total = 0
        sorted_common = sorted(common_dates)
        for i in range(1, len(sorted_common)):
            d_prev, d_curr = sorted_common[i - 1], sorted_common[i]
            dir_a = map_a[d_curr] - map_a[d_prev]
            dir_b = map_b[d_curr] - map_b[d_prev]
            if (dir_a > 0 and dir_b > 0) or (dir_a < 0 and dir_b < 0) or (dir_a == 0 and dir_b == 0):
                agreements += 1
            total += 1

        if total > 0:
            agreement_rate = agreements / total
            passed = agreement_rate >= 0.7
            results.append(
                CheckResult(
                    check_name="cross_source_direction",
                    layer=ValidationLayer.CROSS_SOURCE,
                    passed=passed,
                    severity=ValidationSeverity.WARNING if not passed else ValidationSeverity.INFO,
                    message=(
                        f"{pair.description}: direction agreement {agreement_rate:.0%} "
                        f"({agreements}/{total} periods)"
                    ),
                    source=f"{pair.family_id_a} vs {pair.family_id_b}",
                    timestamp=now,
                    details={
                        "agreement_rate": round(agreement_rate, 4),
                        "agreements": agreements,
                        "total_periods": total,
                    },
                )
            )

    return results
