"""Indicator registry for the HCOB / S&P Global Germany PMI connector."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HCOBIndicatorSpec:
    """Static metadata for one HCOB Germany PMI indicator."""

    series_id: str
    title: str
    indicator: str
    category: str
    unit: str
    country_code: str
    importance: str
    release_kind: str
    source_url: str
    calendar_match: str


HCOB_BASE_URL = "https://www.pmi.spglobal.com"
HCOB_RELEASE_DATES_URL = f"{HCOB_BASE_URL}/Public/Release/ReleaseDates?language=en"


INDICATOR_REGISTRY: dict[str, HCOBIndicatorSpec] = {
    "HCOB_FLASH_PMI": HCOBIndicatorSpec(
        series_id="HCOB_FLASH_PMI",
        title="Germany HCOB Flash PMI",
        indicator="HCOB Flash PMI",
        category="Business Confidence",
        unit="index",
        country_code="DE",
        importance="high",
        release_kind="pmi_flash",
        source_url=HCOB_RELEASE_DATES_URL,
        calendar_match="s&p global flash germany pmi",
    ),
    "HCOB_MANUFACTURING_PMI": HCOBIndicatorSpec(
        series_id="HCOB_MANUFACTURING_PMI",
        title="Germany HCOB Manufacturing PMI",
        indicator="HCOB Manufacturing PMI",
        category="Business Confidence",
        unit="index",
        country_code="DE",
        importance="high",
        release_kind="pmi_final",
        source_url=HCOB_RELEASE_DATES_URL,
        calendar_match="s&p global germany manufacturing pmi",
    ),
    "HCOB_SERVICES_PMI": HCOBIndicatorSpec(
        series_id="HCOB_SERVICES_PMI",
        title="Germany HCOB Services PMI",
        indicator="HCOB Services PMI",
        category="Business Confidence",
        unit="index",
        country_code="DE",
        importance="high",
        release_kind="pmi_final",
        source_url=HCOB_RELEASE_DATES_URL,
        calendar_match="s&p global germany services pmi",
    ),
}


def spec_for_calendar_title(normalized_title: str) -> HCOBIndicatorSpec | None:
    """Return the spec whose upstream calendar-row title matches, or ``None``."""
    for spec in INDICATOR_REGISTRY.values():
        if spec.calendar_match == normalized_title:
            return spec
    return None
