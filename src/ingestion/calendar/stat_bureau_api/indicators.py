"""Statistics Bureau indicator whitelist for issue #14 P2."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .parser import build_estat_dbview_url


@dataclass(frozen=True)
class StatBureauIndicatorSpec:
    """Downstream metadata and e-Stat coordinates for one indicator."""

    indicator: str
    country_code: str
    title: str
    unit: str
    importance: str
    category: str
    stats_data_id: str
    estat_params: Mapping[str, str]
    schedule_surface: str

    @property
    def source_url(self) -> str:
        return build_estat_dbview_url(self.stats_data_id)


INDICATOR_REGISTRY: dict[str, StatBureauIndicatorSpec] = {
    "CORE_CPI": StatBureauIndicatorSpec(
        indicator="CORE_CPI",
        country_code="JP",
        title="Core CPI YoY",
        unit="percent",
        importance="high",
        category="Inflation",
        stats_data_id="0003427113",
        estat_params={
            "cdTab": "3",
            "cdCat01": "0161",
            "cdArea": "00000",
        },
        schedule_surface="cpi",
    ),
    "UNEMPLOYMENT_RATE": StatBureauIndicatorSpec(
        indicator="UNEMPLOYMENT_RATE",
        country_code="JP",
        title="Unemployment Rate",
        unit="percent",
        importance="medium",
        category="Labour",
        stats_data_id="0003005865",
        estat_params={
            "cdTab": "02",
            "cdCat01": "000",
            "cdCat02": "08",
            "cdCat03": "0",
            "cdArea": "00000",
        },
        schedule_surface="lfs",
    ),
}
