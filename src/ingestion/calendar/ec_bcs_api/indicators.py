"""Indicator registry for the European Commission BCS calendar connector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class EcBcsIndicatorSpec:
    """Static metadata for one EC BCS economic-calendar indicator."""

    series_id: str
    title: str
    indicator: str
    category: str
    unit: str
    country_code: str
    importance: str
    release_kind: str
    source_url: str


EC_BCS_BASE_URL = "https://economy-finance.ec.europa.eu"
EC_BCS_SURVEY_URL = (
    f"{EC_BCS_BASE_URL}/economic-forecast-and-surveys/business-and-consumer-surveys_en"
)
EC_BCS_PRESS_RELEASES_URL = (
    f"{EC_BCS_BASE_URL}/economic-forecast-and-surveys/business-and-consumer-surveys/"
    "download-business-and-consumer-survey-data/press-releases_en"
)


INDICATOR_REGISTRY: dict[str, EcBcsIndicatorSpec] = {
    "EC_BCS_ESI": EcBcsIndicatorSpec(
        series_id="EC_BCS_ESI",
        title="Euro Area Economic Sentiment Indicator",
        indicator="Economic Sentiment Indicator",
        category="Business Confidence",
        unit="index",
        country_code="EU",
        importance="high",
        release_kind="esi",
        source_url=EC_BCS_SURVEY_URL,
    ),
    "EC_BCS_CCI_FLASH": EcBcsIndicatorSpec(
        series_id="EC_BCS_CCI_FLASH",
        title="Euro Area Consumer Confidence Flash",
        indicator="Consumer Confidence Flash",
        category="Consumer Confidence",
        unit="balance",
        country_code="EU",
        importance="high",
        release_kind="cci_flash",
        source_url=EC_BCS_SURVEY_URL,
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
    """Human-readable monthly reference label used by the BCS releases."""
    return f"{_MONTH_NAMES[reference.month]} {reference.year}"
