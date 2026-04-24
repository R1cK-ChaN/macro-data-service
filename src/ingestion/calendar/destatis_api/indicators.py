"""Destatis calendar indicator whitelist (issue #15 P2).

P2 covers two German high-impact anchors:

- Consumer Price Index preliminary annual rate
- Gross Domestic Product flash quarter-on-quarter growth

Value-side rows come from GENESIS-Online tablefile CSV downloads.
Schedule-side rows come from Destatis' weekly release table.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True)
class DestatisIndicatorSpec:
    """Downstream-shape metadata for one Destatis calendar indicator."""

    series_id: str
    table_name: str
    indicator: str
    country_code: str
    title: str
    unit: str
    importance: str
    category: str
    source_url: str
    reference_cadence: str
    schedule_title_fragments: tuple[str, ...]
    row_match_fragments: tuple[str, ...]
    table_params: Mapping[str, str]


INDICATOR_REGISTRY: dict[str, DestatisIndicatorSpec] = {
    "DESTATIS_CPI_PREL_YOY": DestatisIndicatorSpec(
        series_id="DESTATIS_CPI_PREL_YOY",
        table_name="61111-0004",
        indicator="CPI",
        country_code="DE",
        title="Germany CPI Preliminary YoY",
        unit="percent",
        importance="high",
        category="Inflation",
        source_url=(
            "https://www-genesis.destatis.de/datenbank/online/"
            "statistic/61111/table/61111-0004"
        ),
        reference_cadence="monthly",
        schedule_title_fragments=(
            "verbraucherpreisindex",
            "vorlaeufige ergebnisse",
        ),
        row_match_fragments=(
            "gesamtindex",
            "verbraucherpreisindex",
            "vorjahreszeitraum",
        ),
        table_params=MappingProxyType({}),
    ),
    "DESTATIS_GDP_FLASH_QOQ": DestatisIndicatorSpec(
        series_id="DESTATIS_GDP_FLASH_QOQ",
        table_name="81000-0001",
        indicator="GDP",
        country_code="DE",
        title="Germany GDP Flash QoQ",
        unit="percent",
        importance="high",
        category="GDP Growth",
        source_url=(
            "https://www-genesis.destatis.de/datenbank/online/"
            "statistic/81000/table/81000-0001"
        ),
        reference_cadence="quarterly",
        schedule_title_fragments=(
            "bruttoinlandsprodukt",
            "schnellmeldung",
        ),
        row_match_fragments=(
            "bruttoinlandsprodukt",
            "saison",
            "kalenderbereinigt",
            "vorquartal",
        ),
        table_params=MappingProxyType({}),
    ),
}
