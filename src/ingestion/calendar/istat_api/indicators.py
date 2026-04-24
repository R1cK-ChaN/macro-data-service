"""Italy ISTAT calendar indicator whitelist (issue #15 P3b).

P3b covers two Italian high-impact anchors:

- CPI provisional YoY
- GDP preliminary QoQ

Schedule-side rows come from ISTAT's annual press-release calendar PDF.
Value-side rows come from ISTAT English press-release pages.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class ISTATIndicatorSpec:
    """Downstream-shape metadata for one ISTAT calendar indicator."""

    series_id: str
    title: str
    indicator: str
    category: str
    unit: str
    country_code: str
    importance: str
    release_kind: str
    reference_cadence: str
    source_url: str


ISTAT_BASE_URL = "https://www.istat.it"
ISTAT_PRESS_RELEASE_BASE_URL = f"{ISTAT_BASE_URL}/en/press-release"


INDICATOR_REGISTRY: dict[str, ISTATIndicatorSpec] = {
    "ISTAT_CPI_PROVISIONAL_YOY": ISTATIndicatorSpec(
        series_id="ISTAT_CPI_PROVISIONAL_YOY",
        title="Italy CPI Provisional YoY",
        indicator="CPI",
        category="Inflation",
        unit="percent",
        country_code="IT",
        importance="high",
        release_kind="cpi_provisional",
        reference_cadence="monthly",
        source_url=f"{ISTAT_PRESS_RELEASE_BASE_URL}/consumer-prices-provisional-data-march-2026/",
    ),
    "ISTAT_GDP_PRELIMINARY_QOQ": ISTATIndicatorSpec(
        series_id="ISTAT_GDP_PRELIMINARY_QOQ",
        title="Italy GDP Preliminary QoQ",
        indicator="GDP",
        category="GDP Growth",
        unit="percent",
        country_code="IT",
        importance="high",
        release_kind="gdp_preliminary",
        reference_cadence="quarterly",
        source_url=f"{ISTAT_PRESS_RELEASE_BASE_URL}/preliminary-estimate-of-gdp-q4-2025/",
    ),
}


def _quarter(reference: date) -> int:
    return ((reference.month - 1) // 3) + 1


def press_release_url(spec: ISTATIndicatorSpec, reference: date) -> str:
    """Return ISTAT's English press-release URL for ``reference``."""
    if spec.release_kind == "cpi_provisional":
        month = calendar.month_name[reference.month].lower()
        return (
            f"{ISTAT_PRESS_RELEASE_BASE_URL}/"
            f"consumer-prices-provisional-data-{month}-{reference.year}/"
        )
    if spec.release_kind == "gdp_preliminary":
        return (
            f"{ISTAT_PRESS_RELEASE_BASE_URL}/"
            f"preliminary-estimate-of-gdp-q{_quarter(reference)}-{reference.year}/"
        )
    raise KeyError(f"unknown ISTAT release kind: {spec.release_kind!r}")
