"""University of Michigan calendar-event metadata whitelist."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class UMichIndicatorSpec:
    """Downstream-shape metadata for one U Michigan indicator."""

    series_id: str
    indicator: str
    country_code: str
    title: str
    unit: str
    importance: str
    category: str
    source_url: str
    schedule_url: str
    value_fetch: bool = True


UMICH_MAIN_URL = "https://www.sca.isr.umich.edu/"
UMICH_SURVEY_INFO_URL = "https://data.sca.isr.umich.edu/survey-info.php"


INDICATOR_REGISTRY: dict[str, UMichIndicatorSpec] = {
    "UMICH_CONSUMER_SENTIMENT": UMichIndicatorSpec(
        series_id="UMICH_CONSUMER_SENTIMENT",
        indicator="Michigan Consumer Sentiment",
        country_code="US",
        title="Michigan Consumer Sentiment",
        unit="index",
        importance="high",
        category="Consumer",
        source_url=UMICH_MAIN_URL,
        schedule_url=UMICH_SURVEY_INFO_URL,
    ),
}
