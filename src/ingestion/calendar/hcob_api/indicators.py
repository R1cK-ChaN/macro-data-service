"""Indicator registry for the HCOB / S&P Global Germany PMI connector."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HCOBIndicatorSpec:
    """Static metadata for one HCOB Germany PMI indicator.

    Fields:
      calendar_match     — normalized title used to match a row on the
                           release-dates page. Multiple specs may share
                           the same value (the flash trio collapses
                           under one upstream row).
      press_listing_match — normalized title used to match the row on
                           the press-release listing. The flash trio's
                           three series resolve to the SAME PDF, so
                           they share this value.
    """

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
    press_listing_match: str


HCOB_BASE_URL = "https://www.pmi.spglobal.com"
HCOB_RELEASE_DATES_URL = f"{HCOB_BASE_URL}/Public/Release/ReleaseDates?language=en"
HCOB_PRESS_RELEASES_URL = f"{HCOB_BASE_URL}/Public/Release/PressReleases?language=en"


# Upstream ships ONE "S&P Global Flash Germany PMI" row on flash day
# whose PDF carries all three headline indices. The schedule layer
# expands that single row into three series so TE parity is per-component;
# the value layer fetches the same PDF once and routes the three
# extractions back to the matching schedule row via provider_event_id.
_FLASH_CALENDAR_MATCH = "s&p global flash germany pmi"


INDICATOR_REGISTRY: dict[str, HCOBIndicatorSpec] = {
    "HCOB_FLASH_MANUFACTURING_PMI": HCOBIndicatorSpec(
        series_id="HCOB_FLASH_MANUFACTURING_PMI",
        title="Germany HCOB Flash Manufacturing PMI",
        indicator="HCOB Flash Manufacturing PMI",
        category="Business Confidence",
        unit="index",
        country_code="DE",
        importance="high",
        release_kind="pmi_flash_manufacturing",
        source_url=HCOB_RELEASE_DATES_URL,
        calendar_match=_FLASH_CALENDAR_MATCH,
        press_listing_match="s&p global flash germany pmi",
    ),
    "HCOB_FLASH_SERVICES_PMI": HCOBIndicatorSpec(
        series_id="HCOB_FLASH_SERVICES_PMI",
        title="Germany HCOB Flash Services PMI",
        indicator="HCOB Flash Services PMI",
        category="Business Confidence",
        unit="index",
        country_code="DE",
        importance="high",
        release_kind="pmi_flash_services",
        source_url=HCOB_RELEASE_DATES_URL,
        calendar_match=_FLASH_CALENDAR_MATCH,
        press_listing_match="s&p global flash germany pmi",
    ),
    "HCOB_FLASH_COMPOSITE_PMI": HCOBIndicatorSpec(
        series_id="HCOB_FLASH_COMPOSITE_PMI",
        title="Germany HCOB Flash Composite PMI",
        indicator="HCOB Flash Composite PMI",
        category="Business Confidence",
        unit="index",
        country_code="DE",
        importance="high",
        release_kind="pmi_flash_composite",
        source_url=HCOB_RELEASE_DATES_URL,
        calendar_match=_FLASH_CALENDAR_MATCH,
        press_listing_match="s&p global flash germany pmi",
    ),
    "HCOB_MANUFACTURING_PMI": HCOBIndicatorSpec(
        series_id="HCOB_MANUFACTURING_PMI",
        title="Germany HCOB Manufacturing PMI",
        indicator="HCOB Manufacturing PMI",
        category="Business Confidence",
        unit="index",
        country_code="DE",
        importance="high",
        release_kind="pmi_final_manufacturing",
        source_url=HCOB_RELEASE_DATES_URL,
        calendar_match="s&p global germany manufacturing pmi",
        press_listing_match="s&p global germany manufacturing pmi",
    ),
    "HCOB_SERVICES_PMI": HCOBIndicatorSpec(
        series_id="HCOB_SERVICES_PMI",
        title="Germany HCOB Services PMI",
        indicator="HCOB Services PMI",
        category="Business Confidence",
        unit="index",
        country_code="DE",
        importance="high",
        release_kind="pmi_final_services",
        source_url=HCOB_RELEASE_DATES_URL,
        calendar_match="s&p global germany services pmi",
        press_listing_match="s&p global germany services pmi",
    ),
}


def specs_for_calendar_title(normalized_title: str) -> list[HCOBIndicatorSpec]:
    """Return every spec whose upstream calendar-row title matches.

    The flash trio shares one upstream row, so this returns 3 specs for
    ``"s&p global flash germany pmi"`` and at most 1 for the finals.
    """
    return [
        spec for spec in INDICATOR_REGISTRY.values()
        if spec.calendar_match == normalized_title
    ]


def spec_for_calendar_title(normalized_title: str) -> HCOBIndicatorSpec | None:
    """Return ONE spec whose upstream calendar-row title matches, or ``None``.

    Kept for back-compat with the schedule-only test scaffold and any
    callers that only need a representative spec for the matched row.
    For schedule projection use :func:`specs_for_calendar_title` to
    fan-out the flash trio.
    """
    matches = specs_for_calendar_title(normalized_title)
    return matches[0] if matches else None
