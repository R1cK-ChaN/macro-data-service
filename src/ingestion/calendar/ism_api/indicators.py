"""ISM calendar-event metadata whitelist."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ISMIndicatorSpec:
    """Downstream-shape metadata for one ISM indicator."""

    series_id: str
    indicator: str
    country_code: str
    title: str
    unit: str
    importance: str
    category: str
    source_url: str
    schedule_column_fragment: str
    report_path_fragment: str
    value_fetch: bool = True


ISM_REPORTS_URL = (
    "https://www.ismworld.org/supply-management-news-and-reports/"
    "reports/ism-pmi-reports/"
)
ISM_RELEASE_CALENDAR_URL = (
    "https://www.ismworld.org/supply-management-news-and-reports/"
    "reports/rob-report-calendar/"
)


INDICATOR_REGISTRY: dict[str, ISMIndicatorSpec] = {
    "ISM_MANUFACTURING_PMI": ISMIndicatorSpec(
        series_id="ISM_MANUFACTURING_PMI",
        indicator="ISM Manufacturing PMI",
        country_code="US",
        title="ISM Manufacturing PMI",
        unit="index",
        importance="high",
        category="Manufacturing",
        source_url=ISM_REPORTS_URL,
        schedule_column_fragment="manufacturing pmi",
        report_path_fragment="/reports/ism-pmi-reports/pmi/",
    ),
}
