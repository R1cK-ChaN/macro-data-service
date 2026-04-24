"""Indicator registry for the Ifo calendar connector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class IfoIndicatorSpec:
    """Static metadata for one Ifo economic-calendar indicator."""

    series_id: str
    title: str
    indicator: str
    category: str
    unit: str
    country_code: str
    importance: str
    release_kind: str
    source_url: str


IFO_BASE_URL = "https://www.ifo.de"
IFO_SURVEY_URL = f"{IFO_BASE_URL}/en/survey/ifo-business-climate-index-germany"
IFO_PRESS_RELEASES_URL = f"{IFO_BASE_URL}/en/press"


INDICATOR_REGISTRY: dict[str, IfoIndicatorSpec] = {
    "IFO_BUSINESS_CLIMATE": IfoIndicatorSpec(
        series_id="IFO_BUSINESS_CLIMATE",
        title="Germany Ifo Business Climate Index",
        indicator="Ifo Business Climate",
        category="Business Confidence",
        unit="index",
        country_code="DE",
        importance="high",
        release_kind="business_climate",
        source_url=IFO_SURVEY_URL,
    ),
}


_MONTH_NAMES: tuple[str, ...] = (
    "",
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)


def reference_label_en(reference: date) -> str:
    """Human-readable monthly reference label used by Ifo pages."""
    return f"{_MONTH_NAMES[reference.month]} {reference.year}"
