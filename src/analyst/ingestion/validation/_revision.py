from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ._store import ValidationStore
from ._types import CheckResult, ValidationLayer, ValidationSeverity


@dataclass(frozen=True)
class RevisionSummary:
    """Summary of revision activity for a single series."""

    series_id: str
    source: str
    total_vintages: int
    revised_dates: int  # observation dates with >1 vintage
    total_revision_count: int  # total number of value changes
    max_revision_magnitude: float  # largest absolute change
    mean_revision_magnitude: float
    latest_vintage_date: str


def compute_revision_summary(
    series_id: str,
    source: str,
    vintages: list[dict[str, Any]],
) -> RevisionSummary:
    """Compute revision statistics from a list of vintage records.

    vintages: list of dicts with keys: observation_date, vintage_date, value.
    Should be sorted by (observation_date, vintage_date).
    """
    if not vintages:
        return RevisionSummary(
            series_id=series_id,
            source=source,
            total_vintages=0,
            revised_dates=0,
            total_revision_count=0,
            max_revision_magnitude=0.0,
            mean_revision_magnitude=0.0,
            latest_vintage_date="",
        )

    # Group vintages by observation_date
    by_obs_date: dict[str, list[tuple[str, float]]] = {}
    latest_vintage = ""
    for v in vintages:
        obs_date = v.get("observation_date", "")
        vint_date = v.get("vintage_date", "")
        value = v.get("value")
        if obs_date and vint_date and value is not None:
            by_obs_date.setdefault(obs_date, []).append((vint_date, float(value)))
            if vint_date > latest_vintage:
                latest_vintage = vint_date

    revised_dates = 0
    magnitudes: list[float] = []

    for obs_date, vints in by_obs_date.items():
        vints.sort(key=lambda x: x[0])  # sort by vintage_date
        if len(vints) < 2:
            continue
        # Check for actual value changes between consecutive vintages
        prev_value = vints[0][1]
        has_revision = False
        for _, value in vints[1:]:
            diff = abs(value - prev_value)
            if diff > 1e-10:
                magnitudes.append(diff)
                has_revision = True
            prev_value = value
        if has_revision:
            revised_dates += 1

    return RevisionSummary(
        series_id=series_id,
        source=source,
        total_vintages=len(vintages),
        revised_dates=revised_dates,
        total_revision_count=len(magnitudes),
        max_revision_magnitude=max(magnitudes) if magnitudes else 0.0,
        mean_revision_magnitude=(
            sum(magnitudes) / len(magnitudes) if magnitudes else 0.0
        ),
        latest_vintage_date=latest_vintage,
    )


def check_revisions(
    source: str,
    series_id: str,
    vintages: list[dict[str, Any]],
    validation_store: ValidationStore | None = None,
    *,
    max_revision_magnitude: float | None = None,
    max_revision_rate: float = 0.5,
) -> list[CheckResult]:
    """Validate revision patterns for a series.

    Checks:
    - Revision frequency (what fraction of dates have been revised)
    - Revision magnitude (largest single revision)
    - Regression against baseline revision rate
    """
    results: list[CheckResult] = []
    now = datetime.now(UTC).isoformat()

    summary = compute_revision_summary(series_id, source, vintages)

    if summary.total_vintages == 0:
        results.append(
            CheckResult(
                check_name="revision_no_vintages",
                layer=ValidationLayer.DATA_DIFF,
                passed=True,
                severity=ValidationSeverity.INFO,
                message=f"{series_id}: no vintage data available",
                source=source,
                series_id=series_id,
                timestamp=now,
            )
        )
        return results

    # ── Revision rate ────────────────────────────────────────────
    obs_dates_with_data = len({
        v.get("observation_date", "")
        for v in vintages
        if v.get("observation_date")
    })
    revision_rate = (
        summary.revised_dates / obs_dates_with_data
        if obs_dates_with_data > 0
        else 0.0
    )
    rate_ok = revision_rate <= max_revision_rate

    results.append(
        CheckResult(
            check_name="revision_rate",
            layer=ValidationLayer.DATA_DIFF,
            passed=rate_ok,
            severity=ValidationSeverity.WARNING if not rate_ok else ValidationSeverity.INFO,
            message=(
                f"{series_id}: {summary.revised_dates}/{obs_dates_with_data} dates revised "
                f"({revision_rate:.0%}, threshold: {max_revision_rate:.0%})"
            ),
            source=source,
            series_id=series_id,
            timestamp=now,
            details={
                "revised_dates": summary.revised_dates,
                "total_dates": obs_dates_with_data,
                "revision_rate": round(revision_rate, 4),
                "total_revisions": summary.total_revision_count,
            },
        )
    )

    # ── Revision magnitude ───────────────────────────────────────
    if summary.total_revision_count > 0:
        results.append(
            CheckResult(
                check_name="revision_magnitude",
                layer=ValidationLayer.DATA_DIFF,
                passed=True,
                severity=ValidationSeverity.INFO,
                message=(
                    f"{series_id}: max revision {summary.max_revision_magnitude:.4f}, "
                    f"mean {summary.mean_revision_magnitude:.4f} "
                    f"({summary.total_revision_count} total revisions)"
                ),
                source=source,
                series_id=series_id,
                timestamp=now,
                details={
                    "max_magnitude": summary.max_revision_magnitude,
                    "mean_magnitude": round(summary.mean_revision_magnitude, 6),
                    "revision_count": summary.total_revision_count,
                },
            )
        )

        if max_revision_magnitude is not None:
            mag_ok = summary.max_revision_magnitude <= max_revision_magnitude
            results.append(
                CheckResult(
                    check_name="revision_magnitude_threshold",
                    layer=ValidationLayer.DATA_DIFF,
                    passed=mag_ok,
                    severity=ValidationSeverity.WARNING if not mag_ok else ValidationSeverity.INFO,
                    message=(
                        f"{series_id}: max revision {summary.max_revision_magnitude:.4f} "
                        f"(threshold: {max_revision_magnitude:.4f})"
                    ),
                    source=source,
                    series_id=series_id,
                    timestamp=now,
                    details={
                        "max_magnitude": summary.max_revision_magnitude,
                        "threshold": max_revision_magnitude,
                    },
                )
            )

    # ── Baseline comparison ──────────────────────────────────────
    if validation_store is not None:
        stored = validation_store.get_baseline(source, series_id, "revision_summary")

        if stored is not None:
            baseline_rate = stored.get("revision_rate", 0.0)
            rate_increase = revision_rate - baseline_rate
            # Alert if revision rate jumped by >20 percentage points
            rate_spike = rate_increase > 0.2
            if rate_spike:
                results.append(
                    CheckResult(
                        check_name="revision_rate_spike",
                        layer=ValidationLayer.DATA_DIFF,
                        passed=False,
                        severity=ValidationSeverity.WARNING,
                        message=(
                            f"{series_id}: revision rate spiked from "
                            f"{baseline_rate:.0%} to {revision_rate:.0%} "
                            f"(+{rate_increase:.0%})"
                        ),
                        source=source,
                        series_id=series_id,
                        timestamp=now,
                        details={
                            "current_rate": round(revision_rate, 4),
                            "baseline_rate": round(baseline_rate, 4),
                            "increase": round(rate_increase, 4),
                        },
                    )
                )

        # Update baseline
        validation_store.save_baseline(
            source,
            series_id,
            "revision_summary",
            {
                "revision_rate": round(revision_rate, 4),
                "revised_dates": summary.revised_dates,
                "total_revisions": summary.total_revision_count,
                "max_magnitude": summary.max_revision_magnitude,
                "latest_vintage_date": summary.latest_vintage_date,
            },
            now,
        )

    return results


def check_revisions_batch(
    source: str,
    series_vintages: dict[str, list[dict[str, Any]]],
    validation_store: ValidationStore | None = None,
) -> list[CheckResult]:
    """Run revision checks for multiple series from the same source."""
    results: list[CheckResult] = []
    for series_id, vintages in series_vintages.items():
        results.extend(
            check_revisions(source, series_id, vintages, validation_store)
        )
    return results
