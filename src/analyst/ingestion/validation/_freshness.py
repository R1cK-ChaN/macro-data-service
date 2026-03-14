from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from ._types import CheckResult, ValidationLayer, ValidationSeverity


@dataclass(frozen=True)
class FreshnessExpectation:
    """Defines how fresh data from a source should be.

    max_staleness_days: alert if the latest data is older than this.
    For daily series (FRED, EIA), this is typically 7 days.
    For monthly series (CPI, unemployment), this is ~45 days.
    For annual series (World Bank development indicators), this is ~400 days.
    """

    source: str
    max_staleness_days: int
    series_id: str = ""  # empty = source-level check
    description: str = ""


# Defaults tuned for typical macro data update frequencies.
DEFAULT_FRESHNESS_EXPECTATIONS: dict[str, FreshnessExpectation] = {
    "fred": FreshnessExpectation("fred", max_staleness_days=7, description="FRED daily series"),
    "eia": FreshnessExpectation("eia", max_staleness_days=14, description="EIA energy data"),
    "nyfed": FreshnessExpectation("nyfed", max_staleness_days=7, description="NY Fed overnight rates"),
    "oecd": FreshnessExpectation("oecd", max_staleness_days=90, description="OECD macro (monthly/quarterly)"),
    "worldbank": FreshnessExpectation("worldbank", max_staleness_days=400, description="World Bank annual indicators"),
    "imf": FreshnessExpectation("imf", max_staleness_days=120, description="IMF quarterly data"),
    "eurostat": FreshnessExpectation("eurostat", max_staleness_days=90, description="Eurostat EU data"),
    "bis": FreshnessExpectation("bis", max_staleness_days=120, description="BIS quarterly data"),
    "ecb": FreshnessExpectation("ecb", max_staleness_days=45, description="ECB monthly data"),
    "treasury_fiscal": FreshnessExpectation("treasury_fiscal", max_staleness_days=7, description="Treasury daily data"),
}


def check_freshness(
    source: str,
    latest_date: str,
    expectation: FreshnessExpectation | None = None,
    *,
    reference_date: datetime | None = None,
) -> list[CheckResult]:
    """Check if the latest observation date is fresh enough.

    latest_date: ISO date string (YYYY-MM-DD) of the most recent observation.
    expectation: staleness threshold. If None, uses default.
    reference_date: comparison point (defaults to now UTC).
    """
    results: list[CheckResult] = []
    now_iso = datetime.now(UTC).isoformat()
    ref = reference_date or datetime.now(UTC)

    if not latest_date:
        results.append(
            CheckResult(
                check_name="freshness_no_data",
                layer=ValidationLayer.SERIES,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"{source}: no observation dates found",
                source=source,
                timestamp=now_iso,
            )
        )
        return results

    try:
        latest_dt = datetime.fromisoformat(latest_date.replace("Z", "+00:00"))
        if latest_dt.tzinfo is None:
            latest_dt = latest_dt.replace(tzinfo=UTC)
    except ValueError:
        # Try parsing just the date part
        try:
            latest_dt = datetime.strptime(latest_date[:10], "%Y-%m-%d").replace(tzinfo=UTC)
        except ValueError:
            results.append(
                CheckResult(
                    check_name="freshness_parse_error",
                    layer=ValidationLayer.SERIES,
                    passed=False,
                    severity=ValidationSeverity.WARNING,
                    message=f"{source}: cannot parse latest date '{latest_date}'",
                    source=source,
                    timestamp=now_iso,
                )
            )
            return results

    staleness = ref - latest_dt
    staleness_days = staleness.days

    exp = expectation or DEFAULT_FRESHNESS_EXPECTATIONS.get(source)
    if exp is None:
        # No expectation available; report staleness as info
        results.append(
            CheckResult(
                check_name="freshness_info",
                layer=ValidationLayer.SERIES,
                passed=True,
                severity=ValidationSeverity.INFO,
                message=f"{source}: latest data is {staleness_days} days old ({latest_date})",
                source=source,
                series_id=exp.series_id if exp else "",
                timestamp=now_iso,
                details={"latest_date": latest_date, "staleness_days": staleness_days},
            )
        )
        return results

    passed = staleness_days <= exp.max_staleness_days
    results.append(
        CheckResult(
            check_name="freshness_check",
            layer=ValidationLayer.SERIES,
            passed=passed,
            severity=ValidationSeverity.WARNING if not passed else ValidationSeverity.INFO,
            message=(
                f"{source}: latest data {latest_date}, "
                f"{staleness_days} days old "
                f"(threshold: {exp.max_staleness_days} days)"
            ),
            source=source,
            series_id=exp.series_id,
            timestamp=now_iso,
            details={
                "latest_date": latest_date,
                "staleness_days": staleness_days,
                "max_staleness_days": exp.max_staleness_days,
            },
        )
    )

    return results


def check_freshness_batch(
    source_latest_dates: dict[str, str],
    expectations: dict[str, FreshnessExpectation] | None = None,
    *,
    reference_date: datetime | None = None,
) -> list[CheckResult]:
    """Run freshness checks for multiple sources."""
    expectations = expectations or DEFAULT_FRESHNESS_EXPECTATIONS
    results: list[CheckResult] = []
    for source, latest_date in source_latest_dates.items():
        exp = expectations.get(source)
        results.extend(
            check_freshness(source, latest_date, exp, reference_date=reference_date)
        )
    return results
