"""Indicator registry for the ZEW calendar connector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class ZEWIndicatorSpec:
    """Static metadata for one ZEW economic-calendar indicator."""

    series_id: str
    title: str
    indicator: str
    category: str
    unit: str
    country_code: str
    importance: str
    release_kind: str
    source_url: str


ZEW_BASE_URL = "https://www.zew.de"
ZEW_PRESS_RELEASES_URL = f"{ZEW_BASE_URL}/en/press/latest-press-releases"


def release_dates_url(year: int) -> str:
    """Return the annual ZEW economic-sentiment release-dates page."""
    return (
        f"{ZEW_PRESS_RELEASES_URL}/"
        f"{year}-release-dates-for-zew-indicator-of-economic-sentiment-fixed"
    )


INDICATOR_REGISTRY: dict[str, ZEWIndicatorSpec] = {
    "ZEW_ECONOMIC_SENTIMENT": ZEWIndicatorSpec(
        series_id="ZEW_ECONOMIC_SENTIMENT",
        title="Germany ZEW Economic Sentiment Index",
        indicator="ZEW Economic Sentiment",
        category="Business Confidence",
        unit="index",
        country_code="DE",
        importance="high",
        release_kind="economic_sentiment",
        source_url=ZEW_PRESS_RELEASES_URL,
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
    """Human-readable monthly reference label used by ZEW pages."""
    return f"{_MONTH_NAMES[reference.month]} {reference.year}"
