from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Callable

from ._types import CheckResult, ValidationLayer, ValidationSeverity


@dataclass(frozen=True)
class CatalogExpectation:
    """Defines a single catalog completeness check.

    api_total_fn: returns the API-reported total count (single request).
    fetched_count_fn: returns the count from a full catalog fetch.
    tolerance: allow this many items to be missing (e.g. blank entries).
    """

    entity_type: str
    api_total_fn: Callable[[], int]
    fetched_count_fn: Callable[[], int]
    tolerance: int = 0


def check_catalog_completeness(
    source: str,
    expectations: list[CatalogExpectation],
) -> list[CheckResult]:
    """Verify that catalog entity counts match API-reported totals.

    This generalizes the pattern from test_worldbank_data_validation.py
    into a runtime check usable by any source with catalog APIs.
    """
    results: list[CheckResult] = []
    now = datetime.now(UTC).isoformat()

    for exp in expectations:
        try:
            api_total = exp.api_total_fn()
        except Exception as exc:
            results.append(
                CheckResult(
                    check_name=f"catalog_{exp.entity_type}_api_total",
                    layer=ValidationLayer.CATALOG,
                    passed=False,
                    severity=ValidationSeverity.ERROR,
                    message=f"{source} {exp.entity_type}: failed to get API total: {exc}",
                    source=source,
                    timestamp=now,
                )
            )
            continue

        try:
            fetched = exp.fetched_count_fn()
        except Exception as exc:
            results.append(
                CheckResult(
                    check_name=f"catalog_{exp.entity_type}_fetch",
                    layer=ValidationLayer.CATALOG,
                    passed=False,
                    severity=ValidationSeverity.ERROR,
                    message=f"{source} {exp.entity_type}: failed to fetch catalog: {exc}",
                    source=source,
                    timestamp=now,
                )
            )
            continue

        diff = api_total - fetched
        passed = diff <= exp.tolerance
        results.append(
            CheckResult(
                check_name=f"catalog_{exp.entity_type}_completeness",
                layer=ValidationLayer.CATALOG,
                passed=passed,
                severity=ValidationSeverity.ERROR if not passed else ValidationSeverity.INFO,
                message=(
                    f"{source} {exp.entity_type}: API={api_total} Fetched={fetched} PASS"
                    if passed
                    else f"{source} {exp.entity_type}: API={api_total} Fetched={fetched} MISSING={diff}"
                ),
                source=source,
                timestamp=now,
                details={
                    "api_total": api_total,
                    "fetched": fetched,
                    "difference": diff,
                    "tolerance": exp.tolerance,
                },
            )
        )

    return results
