from __future__ import annotations

import dataclasses
import logging
import math
from datetime import UTC, datetime
from typing import Any

from ._store import ValidationStore
from ._types import (
    CheckResult,
    SeriesProfile,
    ValidationLayer,
    ValidationSeverity,
)

logger = logging.getLogger(__name__)


def compute_series_profile(
    series_id: str,
    source: str,
    observations: list[Any],
) -> SeriesProfile:
    """Compute statistical profile from a list of observations.

    Observations are expected to have `date` and `value` attributes or keys.
    """
    values: list[float] = []
    dates: list[str] = []
    null_count = 0
    total = len(observations)

    for obs in observations:
        if isinstance(obs, dict):
            v = obs.get("value")
            d = obs.get("date", "")
        else:
            v = getattr(obs, "value", None)
            d = getattr(obs, "date", "")

        if v is not None:
            try:
                values.append(float(v))
            except (ValueError, TypeError):
                null_count += 1
        else:
            null_count += 1

        if d:
            dates.append(str(d))

    if not values:
        return SeriesProfile(
            series_id=series_id,
            source=source,
            observation_count=total,
            date_min="",
            date_max="",
            value_mean=0.0,
            value_std=0.0,
            value_min=0.0,
            value_max=0.0,
            missing_rate=1.0,
            captured_at=datetime.now(UTC).isoformat(),
        )

    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n if n > 1 else 0.0
    std = math.sqrt(variance)
    sorted_dates = sorted(dates) if dates else []

    return SeriesProfile(
        series_id=series_id,
        source=source,
        observation_count=total,
        date_min=sorted_dates[0] if sorted_dates else "",
        date_max=sorted_dates[-1] if sorted_dates else "",
        value_mean=round(mean, 6),
        value_std=round(std, 6),
        value_min=min(values),
        value_max=max(values),
        missing_rate=round(null_count / total, 4) if total > 0 else 0.0,
        captured_at=datetime.now(UTC).isoformat(),
    )


def _profile_to_dict(p: SeriesProfile) -> dict[str, Any]:
    return dataclasses.asdict(p)


def _profile_from_dict(d: dict[str, Any]) -> SeriesProfile:
    return SeriesProfile(**d)


def check_anomalies(
    source: str,
    series_id: str,
    current_profile: SeriesProfile,
    validation_store: ValidationStore,
    *,
    mean_shift_sigma: float = 3.0,
    variance_ratio_bounds: tuple[float, float] = (0.5, 2.0),
    missing_rate_spike_threshold: float = 0.1,
) -> list[CheckResult]:
    """Detect statistical anomalies by comparing current profile to baseline.

    On first run, captures the baseline and returns INFO results.
    On subsequent runs, checks for mean shift, variance change, and missing rate spikes.
    """
    results: list[CheckResult] = []
    now = datetime.now(UTC).isoformat()

    stored = validation_store.get_baseline(source, series_id, "series_profile")

    if stored is None:
        validation_store.save_baseline(
            source,
            series_id,
            "series_profile",
            _profile_to_dict(current_profile),
            now,
        )
        results.append(
            CheckResult(
                check_name="anomaly_baseline_captured",
                layer=ValidationLayer.ANOMALY,
                passed=True,
                severity=ValidationSeverity.INFO,
                message=f"{series_id}: baseline profile captured ({current_profile.observation_count} obs)",
                source=source,
                series_id=series_id,
                timestamp=now,
            )
        )
        return results

    baseline = _profile_from_dict(stored)

    # ── Mean shift ───────────────────────────────────────────────
    if baseline.value_std > 0:
        shift = abs(current_profile.value_mean - baseline.value_mean)
        threshold = mean_shift_sigma * baseline.value_std
        passed = shift <= threshold
        results.append(
            CheckResult(
                check_name="anomaly_mean_shift",
                layer=ValidationLayer.ANOMALY,
                passed=passed,
                severity=ValidationSeverity.WARNING if not passed else ValidationSeverity.INFO,
                message=(
                    f"{series_id}: mean shifted by {shift:.4f} "
                    f"(threshold: {threshold:.4f}, {mean_shift_sigma}σ)"
                ),
                source=source,
                series_id=series_id,
                timestamp=now,
                details={
                    "current_mean": current_profile.value_mean,
                    "baseline_mean": baseline.value_mean,
                    "shift": round(shift, 6),
                    "threshold": round(threshold, 6),
                },
            )
        )

    # ── Variance change ──────────────────────────────────────────
    if baseline.value_std > 0:
        ratio = current_profile.value_std / baseline.value_std
        low, high = variance_ratio_bounds
        passed = low <= ratio <= high
        results.append(
            CheckResult(
                check_name="anomaly_variance_change",
                layer=ValidationLayer.ANOMALY,
                passed=passed,
                severity=ValidationSeverity.WARNING if not passed else ValidationSeverity.INFO,
                message=f"{series_id}: variance ratio {ratio:.3f} (bounds: [{low}, {high}])",
                source=source,
                series_id=series_id,
                timestamp=now,
                details={
                    "current_std": current_profile.value_std,
                    "baseline_std": baseline.value_std,
                    "ratio": round(ratio, 4),
                },
            )
        )

    # ── Missing rate spike ───────────────────────────────────────
    spike = current_profile.missing_rate - baseline.missing_rate
    passed = spike <= missing_rate_spike_threshold
    results.append(
        CheckResult(
            check_name="anomaly_missing_rate_spike",
            layer=ValidationLayer.ANOMALY,
            passed=passed,
            severity=ValidationSeverity.WARNING if not passed else ValidationSeverity.INFO,
            message=(
                f"{series_id}: missing rate {current_profile.missing_rate:.1%} "
                f"(baseline: {baseline.missing_rate:.1%}, delta: {spike:+.1%})"
            ),
            source=source,
            series_id=series_id,
            timestamp=now,
            details={
                "current_missing_rate": current_profile.missing_rate,
                "baseline_missing_rate": baseline.missing_rate,
                "spike": round(spike, 4),
            },
        )
    )

    # ── Range violation ──────────────────────────────────────────
    if baseline.value_max != 0:
        range_violation = False
        details: dict[str, Any] = {}
        if current_profile.value_max > baseline.value_max * 2:
            range_violation = True
            details["max_exceeded"] = {
                "current": current_profile.value_max,
                "baseline_2x": baseline.value_max * 2,
            }
        if baseline.value_min != 0 and current_profile.value_min < baseline.value_min * 2:
            # For negative minimums, *2 goes more negative; for positive, this is a tighter check
            if baseline.value_min > 0 and current_profile.value_min < baseline.value_min / 2:
                range_violation = True
                details["min_exceeded"] = {
                    "current": current_profile.value_min,
                    "baseline_half": baseline.value_min / 2,
                }

        results.append(
            CheckResult(
                check_name="anomaly_range_violation",
                layer=ValidationLayer.ANOMALY,
                passed=not range_violation,
                severity=ValidationSeverity.WARNING if range_violation else ValidationSeverity.INFO,
                message=(
                    f"{series_id}: value range [{current_profile.value_min}, {current_profile.value_max}] "
                    f"(baseline: [{baseline.value_min}, {baseline.value_max}])"
                ),
                source=source,
                series_id=series_id,
                timestamp=now,
                details=details,
            )
        )

    # ── Observation count drop ───────────────────────────────────
    if baseline.observation_count > 0:
        count_ratio = current_profile.observation_count / baseline.observation_count
        passed = count_ratio >= 0.8  # alert if >20% drop
        results.append(
            CheckResult(
                check_name="anomaly_observation_count",
                layer=ValidationLayer.ANOMALY,
                passed=passed,
                severity=ValidationSeverity.WARNING if not passed else ValidationSeverity.INFO,
                message=(
                    f"{series_id}: {current_profile.observation_count} observations "
                    f"(baseline: {baseline.observation_count}, ratio: {count_ratio:.2f})"
                ),
                source=source,
                series_id=series_id,
                timestamp=now,
                details={
                    "current_count": current_profile.observation_count,
                    "baseline_count": baseline.observation_count,
                    "ratio": round(count_ratio, 4),
                },
            )
        )

    # Update baseline with current profile
    validation_store.save_baseline(
        source,
        series_id,
        "series_profile",
        _profile_to_dict(current_profile),
        now,
    )

    return results


# ── Pre-store sanity check ──────────────────────────────────────────────

def check_value_sanity(
    incoming_value: float,
    series_id: str,
    source: str,
    recent_values: list[float],
    *,
    change_sigma: float = 4.0,
    min_history: int = 5,
) -> CheckResult | None:
    """Compare an incoming value against recent history.

    Flags if the period-over-period change is > ``change_sigma`` standard
    deviations from the recent change distribution.  Returns ``None`` when
    history is too short to judge.
    """
    if len(recent_values) < min_history:
        return None

    # Compute recent period-over-period changes (newest first in recent_values)
    changes: list[float] = []
    for i in range(len(recent_values) - 1):
        changes.append(recent_values[i] - recent_values[i + 1])

    if not changes:
        return None

    mean_chg = sum(changes) / len(changes)
    if len(changes) > 1:
        var = sum((c - mean_chg) ** 2 for c in changes) / (len(changes) - 1)
        std_chg = math.sqrt(var)
    else:
        std_chg = 0.0

    # New change: incoming vs most recent stored value
    new_change = incoming_value - recent_values[0]

    # If std is near zero (constant series), flag if any change at all
    if std_chg < 1e-9:
        passed = abs(new_change) < 1e-9
        threshold = 0.0
    else:
        threshold = change_sigma * std_chg
        passed = abs(new_change - mean_chg) <= threshold

    now = datetime.now(UTC).isoformat()
    severity = ValidationSeverity.INFO if passed else ValidationSeverity.WARNING

    if not passed:
        logger.warning(
            "SANITY: %s/%s value %.4f → %.4f (change=%.4f, mean_chg=%.4f, threshold=%.4f)",
            source, series_id, recent_values[0], incoming_value,
            new_change, mean_chg, threshold,
        )

    return CheckResult(
        check_name="sanity_value_change",
        layer=ValidationLayer.ANOMALY,
        passed=passed,
        severity=severity,
        message=(
            f"{series_id}: change {new_change:+.4f} "
            f"(mean_chg={mean_chg:.4f}, {change_sigma}σ threshold={threshold:.4f})"
        ),
        source=source,
        series_id=series_id,
        timestamp=now,
        details={
            "incoming_value": incoming_value,
            "previous_value": recent_values[0],
            "new_change": round(new_change, 6),
            "mean_change": round(mean_chg, 6),
            "std_change": round(std_chg, 6),
            "threshold": round(threshold, 6),
            "history_depth": len(recent_values),
        },
    )

    return results
