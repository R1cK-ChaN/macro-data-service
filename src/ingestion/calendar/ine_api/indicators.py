"""Spain INE calendar indicator whitelist (issue #15 P3a).

P3a covers two Spanish high-impact anchors:

- CPI advance annual rate
- GDP advance quarter-on-quarter growth

Schedule-side rows come from INE's yearly publication calendar. Value-side
rows come from the official press-release pages linked by stable slugs.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class INEIndicatorSpec:
    """Downstream-shape metadata for one INE calendar indicator."""

    series_id: str
    indicator: str
    country_code: str
    title: str
    unit: str
    importance: str
    category: str
    source_url: str
    reference_cadence: str
    schedule_title: str
    release_kind: str


INE_PRESS_BASE_URL = "https://www.ine.es/dyngs/Prensa"


INDICATOR_REGISTRY: dict[str, INEIndicatorSpec] = {
    "INE_CPI_ADVANCE_YOY": INEIndicatorSpec(
        series_id="INE_CPI_ADVANCE_YOY",
        indicator="CPI",
        country_code="ES",
        title="Spain CPI Advance YoY",
        unit="percent",
        importance="high",
        category="Inflation",
        source_url=f"{INE_PRESS_BASE_URL}/adIPC0326.htm",
        reference_cadence="monthly",
        schedule_title="Indice de Precios de Consumo",
        release_kind="cpi_advance",
    ),
    "INE_GDP_ADVANCE_QOQ": INEIndicatorSpec(
        series_id="INE_GDP_ADVANCE_QOQ",
        indicator="GDP",
        country_code="ES",
        title="Spain GDP Advance QoQ",
        unit="percent",
        importance="high",
        category="GDP Growth",
        source_url=f"{INE_PRESS_BASE_URL}/avCNTR4T25.htm",
        reference_cadence="quarterly",
        schedule_title="Contabilidad Nacional Trimestral de Espana",
        release_kind="gdp_advance",
    ),
}


def press_release_url(spec: INEIndicatorSpec, reference: date) -> str:
    """Return INE's stable press-release URL for ``reference``."""
    year_suffix = f"{reference.year % 100:02d}"
    if spec.release_kind == "cpi_advance":
        return f"{INE_PRESS_BASE_URL}/adIPC{reference.month:02d}{year_suffix}.htm"
    if spec.release_kind == "gdp_advance":
        quarter = (reference.month - 1) // 3 + 1
        return f"{INE_PRESS_BASE_URL}/avCNTR{quarter}T{year_suffix}.htm"
    raise KeyError(f"unknown INE release kind: {spec.release_kind!r}")
