"""METI indicator whitelist for issue #14 P5."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MetiIndicatorSpec:
    """Downstream metadata for one METI calendar indicator."""

    indicator: str
    country_code: str
    title: str
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, MetiIndicatorSpec] = {
    "INDUSTRIAL_PRODUCTION": MetiIndicatorSpec(
        indicator="INDUSTRIAL_PRODUCTION",
        country_code="JP",
        title="Industrial Production MoM Prel",
        unit="percent",
        importance="medium",
        category="Industrial Production",
    ),
    "RETAIL_SALES": MetiIndicatorSpec(
        indicator="RETAIL_SALES",
        country_code="JP",
        title="Retail Sales YoY",
        unit="percent",
        importance="medium",
        category="Retail Sales",
    ),
}
