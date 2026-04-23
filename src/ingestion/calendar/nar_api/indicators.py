"""NAR calendar-event metadata whitelist."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NARIndicatorSpec:
    """Downstream-shape metadata for one NAR housing indicator."""

    series_id: str
    indicator: str
    country_code: str
    title: str
    unit: str
    importance: str
    category: str
    source_url: str
    schedule_url: str
    schedule_title_fragment: str
    value_kind: str
    value_fetch: bool = True


NAR_SCHEDULE_URL = (
    "https://www.nar.realtor/newsroom/"
    "nar-statistical-news-release-schedule"
)
NAR_EXISTING_HOME_SALES_URL = (
    "https://www.nar.realtor/research-and-statistics/"
    "housing-statistics/existing-home-sales"
)
NAR_PENDING_HOME_SALES_URL = (
    "https://www.nar.realtor/research-and-statistics/"
    "housing-statistics/pending-home-sales"
)


INDICATOR_REGISTRY: dict[str, NARIndicatorSpec] = {
    "NAR_EXISTING_HOME_SALES": NARIndicatorSpec(
        series_id="NAR_EXISTING_HOME_SALES",
        indicator="Existing Home Sales",
        country_code="US",
        title="Existing Home Sales",
        unit="million",
        importance="high",
        category="Housing",
        source_url=NAR_EXISTING_HOME_SALES_URL,
        schedule_url=NAR_SCHEDULE_URL,
        schedule_title_fragment="Existing-Home Sales",
        value_kind="existing_home_sales",
    ),
    "NAR_PENDING_HOME_SALES_MOM": NARIndicatorSpec(
        series_id="NAR_PENDING_HOME_SALES_MOM",
        indicator="Pending Home Sales MoM",
        country_code="US",
        title="Pending Home Sales MoM",
        unit="%",
        importance="high",
        category="Housing",
        source_url=NAR_PENDING_HOME_SALES_URL,
        schedule_url=NAR_SCHEDULE_URL,
        schedule_title_fragment="Pending Home Sales Index",
        value_kind="pending_home_sales_mom",
    ),
}
