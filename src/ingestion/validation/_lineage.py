from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from ._types import CheckResult, ValidationLayer, ValidationSeverity


def check_lineage(
    source: str,
    observations: list[Any],
    *,
    require_family_id: bool = False,
) -> list[CheckResult]:
    """Validate that every observation has complete lineage fields.

    Every observation must have:
    - source (non-empty)
    - series_id (non-empty)
    - date (non-empty, valid format)

    Optionally checks obs_family_id when require_family_id=True.
    """
    results: list[CheckResult] = []
    now = datetime.now(UTC).isoformat()
    total = len(observations)

    if total == 0:
        return results

    missing_source = 0
    missing_series = 0
    missing_date = 0
    missing_family = 0
    invalid_date = 0

    for obs in observations:
        if isinstance(obs, dict):
            src = obs.get("source", "")
            sid = obs.get("series_id", "")
            d = obs.get("date", "")
            fid = obs.get("obs_family_id")
        else:
            src = getattr(obs, "source", "")
            sid = getattr(obs, "series_id", "")
            d = getattr(obs, "date", "")
            fid = getattr(obs, "obs_family_id", None)

        if not src:
            missing_source += 1
        if not sid:
            missing_series += 1
        if not d:
            missing_date += 1
        elif len(str(d)) < 4 or not str(d)[:4].isdigit():
            invalid_date += 1
        if require_family_id and not fid:
            missing_family += 1

    # ── Source lineage ───────────────────────────────────────────
    passed = missing_source == 0
    results.append(
        CheckResult(
            check_name="lineage_source",
            layer=ValidationLayer.SERIES,
            passed=passed,
            severity=ValidationSeverity.ERROR if not passed else ValidationSeverity.INFO,
            message=(
                f"{source}: all {total} observations have source"
                if passed
                else f"{source}: {missing_source}/{total} observations missing source"
            ),
            source=source,
            timestamp=now,
            details={"missing_source": missing_source, "total": total},
        )
    )

    # ── Series ID lineage ────────────────────────────────────────
    passed = missing_series == 0
    results.append(
        CheckResult(
            check_name="lineage_series_id",
            layer=ValidationLayer.SERIES,
            passed=passed,
            severity=ValidationSeverity.ERROR if not passed else ValidationSeverity.INFO,
            message=(
                f"{source}: all {total} observations have series_id"
                if passed
                else f"{source}: {missing_series}/{total} observations missing series_id"
            ),
            source=source,
            timestamp=now,
            details={"missing_series_id": missing_series, "total": total},
        )
    )

    # ── Date lineage ─────────────────────────────────────────────
    date_issues = missing_date + invalid_date
    passed = date_issues == 0
    results.append(
        CheckResult(
            check_name="lineage_date",
            layer=ValidationLayer.SERIES,
            passed=passed,
            severity=ValidationSeverity.ERROR if not passed else ValidationSeverity.INFO,
            message=(
                f"{source}: all {total} observations have valid date"
                if passed
                else f"{source}: {missing_date} missing + {invalid_date} invalid dates out of {total}"
            ),
            source=source,
            timestamp=now,
            details={
                "missing_date": missing_date,
                "invalid_date": invalid_date,
                "total": total,
            },
        )
    )

    # ── Family ID lineage (optional) ─────────────────────────────
    if require_family_id:
        passed = missing_family == 0
        results.append(
            CheckResult(
                check_name="lineage_family_id",
                layer=ValidationLayer.SERIES,
                passed=passed,
                severity=ValidationSeverity.WARNING if not passed else ValidationSeverity.INFO,
                message=(
                    f"{source}: all {total} observations have obs_family_id"
                    if passed
                    else f"{source}: {missing_family}/{total} observations missing obs_family_id"
                ),
                source=source,
                timestamp=now,
                details={"missing_family_id": missing_family, "total": total},
            )
        )

    return results
