"""France INSEE calendar indicator whitelist (issue #15 P3c).

P3c covers two French high-impact anchors:

- CPI provisional annual rate
- GDP first-estimate quarter-on-quarter growth

Schedule-side rows come from INSEE's publication-calendar JSON endpoint.
Value-side rows resolve released pages through INSEE search, then parse the
official ``Informations rapides`` pages.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class INSEEIndicatorSpec:
    """Downstream-shape metadata for one INSEE calendar indicator."""

    series_id: str
    title: str
    indicator: str
    category: str
    unit: str
    country_code: str
    importance: str
    release_kind: str
    reference_cadence: str
    family_id: str
    schedule_title_en: str
    schedule_title_fr: str
    source_url: str


INSEE_BASE_URL = "https://www.insee.fr"
INSEE_PUBLICATION_CALENDAR_URL = f"{INSEE_BASE_URL}/en/information/2107811"
INSEE_STATS_BASE_URL = f"{INSEE_BASE_URL}/en/statistiques"


INDICATOR_REGISTRY: dict[str, INSEEIndicatorSpec] = {
    "INSEE_CPI_PROVISIONAL_YOY": INSEEIndicatorSpec(
        series_id="INSEE_CPI_PROVISIONAL_YOY",
        title="France CPI Provisional YoY",
        indicator="CPI",
        category="Inflation",
        unit="percent",
        country_code="FR",
        importance="high",
        release_kind="cpi_provisional",
        reference_cadence="monthly",
        family_id="1250",
        schedule_title_en="Consumer price index - provisional results",
        schedule_title_fr="Indice des prix a la consommation - resultats provisoires",
        source_url=f"{INSEE_STATS_BASE_URL}/8964204",
    ),
    "INSEE_GDP_FIRST_ESTIMATE_QOQ": INSEEIndicatorSpec(
        series_id="INSEE_GDP_FIRST_ESTIMATE_QOQ",
        title="France GDP First Estimate QoQ",
        indicator="GDP",
        category="GDP Growth",
        unit="percent",
        country_code="FR",
        importance="high",
        release_kind="gdp_first_estimate",
        reference_cadence="quarterly",
        family_id="1251",
        schedule_title_en="Quarterly national accounts - first estimate",
        schedule_title_fr="Comptes nationaux trimestriels - premiere estimation",
        source_url=f"{INSEE_STATS_BASE_URL}/8733403",
    ),
}


_MONTH_NAMES: dict[int, str] = {
    idx: name for idx, name in enumerate(calendar.month_name) if name
}
_QUARTER_NAMES: dict[int, str] = {
    1: "first quarter",
    2: "second quarter",
    3: "third quarter",
    4: "fourth quarter",
}


def reference_label_en(spec: INSEEIndicatorSpec, reference: date) -> str:
    """Return INSEE's English search label for ``reference``."""
    if spec.reference_cadence == "monthly":
        return f"{_MONTH_NAMES[reference.month]} {reference.year}"
    if spec.reference_cadence == "quarterly":
        quarter = ((reference.month - 1) // 3) + 1
        return f"{_QUARTER_NAMES[quarter]} {reference.year}"
    raise KeyError(f"unknown INSEE cadence: {spec.reference_cadence!r}")


def press_release_url(document_id: int | str) -> str:
    """Return the INSEE statistics URL for a resolved release page id."""
    return f"{INSEE_STATS_BASE_URL}/{document_id}"
