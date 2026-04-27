"""KOSTAT indicator whitelist — issue #55 P1.

Three headline Korean indicators ship in P1, all served by the
KOSTAT release-schedule HTML page at
``mods.go.kr/menu.es?mid=a20301000000``. Each row carries a release
``title`` whose lowercase substring the fetcher matches against
``title_substrings`` to identify the indicator.

- **CPI** — Consumer Price Index. Title rows read
  ``"The Consumer Price Index in <Month> <Year>"``.
- **INDUSTRIAL_PRODUCTION** — Monthly Industrial Statistics (the
  Korean composite of mining, manufacturing, services-output, retail,
  and construction-investment indices that maps to TE's
  ``"South Korea Industrial Production"`` row). Title rows read
  ``"Monthly Industrial Statistics in <Month> <Year>"``.
- **UNEMPLOYMENT_RATE** — Economically Active Population Survey, the
  source of the headline unemployment rate. Title rows read
  ``"The Economically Active Population Survey in <Month> <Year>"``.

The schedule-only slice publishes events with ``actual=NULL``. The
page does not expose values directly; per-release press-release
values live on separate news-list URLs reached via a JS handler
(``onclick="schdulPop('NNN')"``) that posts to a JSON endpoint —
deferred to P2 alongside per-indicator value-side extraction.

Default release times follow Korean SDDS convention. CPI and
Industrial Production publish at 08:00 KST (one hour before market
open); the Economically Active Population Survey publishes at 12:00
KST. The spec allows a per-indicator override via
``release_time_local`` if a future indicator ships at a different
hour.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class KOSTATIndicatorSpec:
    """Downstream-shape metadata for one KOSTAT calendar indicator."""

    indicator: str           # canonical token ("CPI")
    country_code: str        # always "KR" for KOSTAT
    title: str               # cal_econ_event.title
    unit: str                # cal_econ_event.unit (placeholder for schedule-only slice)
    importance: str
    category: str
    title_substrings: tuple[str, ...]  # lowercase substrings — any match identifies the indicator
    frequency: str           # "monthly" (all three P1 indicators)
    release_time_local: str  # KST wall-clock release time ("08:00")


INDICATOR_REGISTRY: dict[str, KOSTATIndicatorSpec] = {
    "CPI": KOSTATIndicatorSpec(
        indicator="CPI",
        country_code="KR",
        title="South Korea Consumer Price Index",
        unit="index",
        importance="high",
        category="Prices",
        title_substrings=(
            "consumer price index",
        ),
        frequency="monthly",
        release_time_local="08:00",
    ),
    "INDUSTRIAL_PRODUCTION": KOSTATIndicatorSpec(
        indicator="INDUSTRIAL_PRODUCTION",
        country_code="KR",
        title="South Korea Industrial Production",
        unit="index",
        importance="high",
        category="Production",
        title_substrings=(
            "monthly industrial statistics",
        ),
        frequency="monthly",
        release_time_local="08:00",
    ),
    "UNEMPLOYMENT_RATE": KOSTATIndicatorSpec(
        indicator="UNEMPLOYMENT_RATE",
        country_code="KR",
        title="South Korea Unemployment Rate",
        unit="percent",
        importance="high",
        category="Labor",
        title_substrings=(
            "economically active population survey",
        ),
        frequency="monthly",
        release_time_local="12:00",
    ),
}


__all__ = ["INDICATOR_REGISTRY", "KOSTATIndicatorSpec"]
