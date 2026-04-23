"""Census EITS series → calendar-event metadata whitelist.

The Census Bureau's Economic Indicators Time Series API exposes the
value side for the issue #13 P1 anchors. Each spec binds a stable local
series id to the API tuple that identifies one row in a dataset/year
response, plus the downstream calendar shape.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CensusIndicatorSpec:
    """Downstream-shape metadata for one Census EITS indicator."""

    series_id: str
    dataset: str
    data_type_code: str
    category_code: str
    seasonally_adj: str
    indicator: str
    country_code: str
    title: str
    unit: str
    importance: str
    category: str
    source_url: str
    schedule_title_fragments: tuple[str, ...]
    time_slot_id: str = "0"
    api_fetch: bool = True


INDICATOR_REGISTRY: dict[str, CensusIndicatorSpec] = {
    "CENSUS_EITS_MARTS_RETAIL_SALES_MOM": CensusIndicatorSpec(
        series_id="CENSUS_EITS_MARTS_RETAIL_SALES_MOM",
        dataset="marts",
        data_type_code="MPCSM",
        category_code="44X72",
        seasonally_adj="yes",
        indicator="Retail Sales MoM",
        country_code="US",
        title="Retail Sales MoM",
        unit="percent",
        importance="high",
        category="Consumer Spending",
        source_url="https://api.census.gov/data/timeseries/eits/marts",
        schedule_title_fragments=(
            "advance monthly sales for retail and food services",
        ),
    ),
    "CENSUS_EITS_ADVM3_DURABLE_GOODS_ORDERS_MOM": CensusIndicatorSpec(
        series_id="CENSUS_EITS_ADVM3_DURABLE_GOODS_ORDERS_MOM",
        dataset="advm3",
        data_type_code="MPCNO",
        category_code="DXT",
        seasonally_adj="yes",
        indicator="Durable Goods Orders MoM",
        country_code="US",
        title="Durable Goods Orders MoM",
        unit="percent",
        importance="high",
        category="Manufacturing",
        source_url="https://api.census.gov/data/timeseries/eits/advm3",
        schedule_title_fragments=(
            "advance report on durable goods",
        ),
    ),
    "CENSUS_EITS_RESCONST_HOUSING_STARTS": CensusIndicatorSpec(
        series_id="CENSUS_EITS_RESCONST_HOUSING_STARTS",
        dataset="resconst",
        data_type_code="TOTAL",
        category_code="ASTARTS",
        seasonally_adj="yes",
        indicator="Housing Starts",
        country_code="US",
        title="Housing Starts",
        unit="thousands",
        importance="high",
        category="Housing",
        source_url="https://api.census.gov/data/timeseries/eits/resconst",
        schedule_title_fragments=(
            "new residential construction",
            "housing starts",
        ),
    ),
    "CENSUS_EITS_RESCONST_BUILDING_PERMITS": CensusIndicatorSpec(
        series_id="CENSUS_EITS_RESCONST_BUILDING_PERMITS",
        dataset="resconst",
        data_type_code="TOTAL",
        category_code="APERMITS",
        seasonally_adj="yes",
        indicator="Building Permits",
        country_code="US",
        title="Building Permits",
        unit="thousands",
        importance="high",
        category="Housing",
        source_url="https://api.census.gov/data/timeseries/eits/resconst",
        schedule_title_fragments=(
            "new residential construction",
            "building permits",
        ),
    ),
}
