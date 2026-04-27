"""MoSPI indicator whitelist — issue #54 P1.

Three headline Indian indicators ship in P1, all served by the
MoSPI release-calendar JSON API at
``mospi.gov.in/api/release-calender/fetch-all-release-calender-Web``.
Each event row carries a ``title`` field whose lowercase substring
the fetcher matches against ``title_substrings`` to identify the
indicator.

- **CPI** — All India Consumer Price Index. The API surfaces both
  ``"All India Consumer Price Index (CPI)"`` (forward schedule
  rows) and ``"Press release of CPI for the month of ..."``
  (post-publication rows); both flavours collapse into the ``CPI``
  bucket via the substring match.
- **INDUSTRIAL_PRODUCTION** — All India Index of Industrial
  Production (IIP). Forward-schedule and post-publication rows both
  contain ``"index of industrial production"`` / ``"iip"``.
- **GDP** — Quarterly / annual GDP estimates. Includes the four
  scheduled stages: First / Second / Third Advance Estimates +
  Provisional Estimates of Annual GDP. The substring matcher anchors
  on ``"gdp"`` so all four stages collapse into the same bucket
  (the ``reference_label`` differentiates the stage in the audit
  payload).

The schedule-only slice publishes events with ``actual=NULL``. The
MoSPI API does not expose values directly — each event's
``description`` HTML carries an anchor to the per-release PDF; PDF
parsing is deferred to P2 alongside per-indicator value-side
extraction.

Default release time is **17:30 IST** for all three indicators —
SDDS standard for Indian official statistics. The spec allows a
per-indicator override via ``release_time_local`` if a future
indicator ships at a different hour.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MoSPIIndicatorSpec:
    """Downstream-shape metadata for one MoSPI calendar indicator.

    ``reference_lag_months`` captures the publication lag between
    release date and reference period (CPI is M-1, IIP is M-2).
    Ignored for quarterly indicators — GDP uses the most recent
    quarter-end strictly before the release date as its reference
    anchor, distinguishing the four annual GDP staging releases
    (First / Second / Third Advance Estimates + Provisional) by
    their distinct release dates rather than by reference period.
    """

    indicator: str           # canonical token ("CPI")
    country_code: str        # always "IN" for MoSPI
    title: str               # cal_econ_event.title
    unit: str                # cal_econ_event.unit (placeholder for schedule-only slice)
    importance: str
    category: str
    title_substrings: tuple[str, ...]  # lowercase substrings — any match identifies the indicator
    frequency: str           # "monthly" | "quarterly"
    release_time_local: str  # IST wall-clock release time ("17:30")
    reference_lag_months: int  # months between release and reference period (CPI=1, IIP=2)


INDICATOR_REGISTRY: dict[str, MoSPIIndicatorSpec] = {
    "CPI": MoSPIIndicatorSpec(
        indicator="CPI",
        country_code="IN",
        title="India Consumer Price Index",
        unit="index",
        importance="high",
        category="Prices",
        title_substrings=(
            "consumer price index",
            "press release of cpi",
        ),
        frequency="monthly",
        release_time_local="17:30",
        reference_lag_months=1,
    ),
    "INDUSTRIAL_PRODUCTION": MoSPIIndicatorSpec(
        indicator="INDUSTRIAL_PRODUCTION",
        country_code="IN",
        title="India Index of Industrial Production",
        unit="index",
        importance="high",
        category="Production",
        title_substrings=(
            "index of industrial production",
            "iip",
        ),
        frequency="monthly",
        release_time_local="17:30",
        reference_lag_months=2,
    ),
    "GDP": MoSPIIndicatorSpec(
        indicator="GDP",
        country_code="IN",
        title="India GDP",
        unit="inr_crore_chained",
        importance="high",
        category="Growth",
        title_substrings=(
            "gdp",
            "gross domestic product",
            "national accounts",
        ),
        frequency="quarterly",
        release_time_local="17:30",
        reference_lag_months=0,
    ),
}


__all__ = ["INDICATOR_REGISTRY", "MoSPIIndicatorSpec"]
