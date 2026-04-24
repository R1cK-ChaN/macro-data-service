"""Indicator registry for the GfK / NIM Consumer Climate connector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class GfKIndicatorSpec:
    """Static metadata for one GfK / NIM economic-calendar indicator."""

    series_id: str
    title: str
    indicator: str
    category: str
    unit: str
    country_code: str
    importance: str
    release_kind: str
    source_url: str


GFK_BASE_URL = "https://www.nim.org"
GFK_CONSUMER_CLIMATE_URL = f"{GFK_BASE_URL}/en/consumer-climate"
GFK_ALL_RELEASES_URL = f"{GFK_BASE_URL}/en/consumer-climate/all-releases"


INDICATOR_REGISTRY: dict[str, GfKIndicatorSpec] = {
    "GFK_CONSUMER_CLIMATE": GfKIndicatorSpec(
        series_id="GFK_CONSUMER_CLIMATE",
        title="Germany GfK Consumer Climate",
        indicator="GfK Consumer Climate",
        category="Consumer Confidence",
        unit="index",
        country_code="DE",
        importance="high",
        release_kind="consumer_climate",
        source_url=GFK_CONSUMER_CLIMATE_URL,
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
    """Human-readable monthly reference label used by NIM press pages."""
    return f"{_MONTH_NAMES[reference.month]} {reference.year}"
