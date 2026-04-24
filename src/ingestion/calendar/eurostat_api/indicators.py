"""Eurostat series-id -> calendar-event metadata whitelist.

Issue #15 P1 covers three Euro-area anchors:

- HICP flash annual rate
- GDP flash quarter-on-quarter growth
- Unemployment rate

The value side uses Eurostat's JSON-stat endpoint via the existing
``EurostatClient``. The schedule side uses Eurostat's release-calendar
JSON feed and merges rows onto the same ``provider_event_id``.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True)
class EurostatIndicatorSpec:
    """Downstream-shape metadata for one Eurostat calendar series."""

    series_id: str
    dataset: str
    params: Mapping[str, str]
    indicator: str
    country_code: str
    title: str
    unit: str
    importance: str
    category: str
    source_url: str
    schedule_title_fragments: tuple[str, ...]
    reference_cadence: str


INDICATOR_REGISTRY: dict[str, EurostatIndicatorSpec] = {
    "EUROSTAT_HICP_FLASH_YOY": EurostatIndicatorSpec(
        series_id="EUROSTAT_HICP_FLASH_YOY",
        dataset="prc_hicp_fpd",
        params=MappingProxyType({
            "unit": "RCH_A",
            "coicop18": "TOTAL",
            "release": "FLS",
            "geo": "EA20",
        }),
        indicator="CPI",
        country_code="EU",
        title="Euro Area CPI Flash YoY",
        unit="percent",
        importance="high",
        category="Inflation",
        source_url=(
            "https://ec.europa.eu/eurostat/databrowser/view/"
            "prc_hicp_fpd/default/table?lang=en"
        ),
        schedule_title_fragments=("flash estimate inflation euro area",),
        reference_cadence="monthly",
    ),
    "EUROSTAT_GDP_FLASH_QOQ": EurostatIndicatorSpec(
        series_id="EUROSTAT_GDP_FLASH_QOQ",
        dataset="namq_10_gdp",
        params=MappingProxyType({
            "na_item": "B1GQ",
            "geo": "EA20",
            "unit": "CLV_PCH_PRE",
            "s_adj": "SCA",
        }),
        indicator="GDP",
        country_code="EU",
        title="Euro Area GDP Flash QoQ",
        unit="percent",
        importance="high",
        category="GDP Growth",
        source_url=(
            "https://ec.europa.eu/eurostat/databrowser/view/"
            "namq_10_gdp/default/table?lang=en"
        ),
        schedule_title_fragments=(
            "preliminary flash estimate gdp",
        ),
        reference_cadence="quarterly",
    ),
    "EUROSTAT_UNEMPLOYMENT_RATE": EurostatIndicatorSpec(
        series_id="EUROSTAT_UNEMPLOYMENT_RATE",
        dataset="une_rt_m",
        params=MappingProxyType({
            "age": "TOTAL",
            "sex": "T",
            "geo": "EA20",
            "s_adj": "SA",
            "unit": "PC_ACT",
        }),
        indicator="Unemployment Rate",
        country_code="EU",
        title="Euro Area Unemployment Rate",
        unit="percent",
        importance="high",
        category="Labor Market",
        source_url=(
            "https://ec.europa.eu/eurostat/databrowser/view/"
            "une_rt_m/default/table?lang=en"
        ),
        schedule_title_fragments=(
            "euro area unemployment",
            "unemployment rate",
        ),
        reference_cadence="monthly",
    ),
}

