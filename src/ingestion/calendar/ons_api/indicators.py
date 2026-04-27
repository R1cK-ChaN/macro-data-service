"""ONS indicator whitelist — issue #51 P1.

Three headline UK indicators ship in P1, all served via the ONS
public timeseries JSON endpoint
``ons.gov.uk/<path>/timeseries/<ts_id>/<dataset_id>/data``:

- **CPI** — annual rate of the all-items Consumer Prices Index
  (timeseries ``d7g7`` in dataset ``mm23``). Monthly, %.
- **UNEMPLOYMENT_RATE** — ILO unemployment rate, aged 16+ SA
  (``mgsx`` / ``lms``). Monthly rolling 3-month, %.
- **GDP** — quarter-on-quarter GDP growth, CVM SA
  (``ihyq`` / ``qna``). Quarterly, %.

Each spec carries the URL fragment plus a frequency hint so the
parser knows to read from ``months`` vs ``quarters`` on the JSON.
Retail Sales and Trade Balance (also issue #51 scope) defer to a
follow-up slice — their value-side comparators need extra unit
alignment (Trade Balance is currency / £m, Retail Sales is index
level vs TE's MoM percent), and shipping the parity-clean three
first keeps the review surface tight.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ONSIndicatorSpec:
    """Downstream-shape metadata for one ONS timeseries indicator."""

    indicator: str           # canonical token (``"CPI"``)
    country_code: str        # always ``"UK"`` for ONS
    title: str               # cal_econ_event.title
    unit: str                # cal_econ_event.unit
    importance: str
    category: str
    # URL components for the public JSON endpoint:
    #   https://www.ons.gov.uk/<path>/timeseries/<ts_id>/<dataset_id>/data
    path: str                # e.g. ``"economy/inflationandpriceindices"``
    ts_id: str               # e.g. ``"d7g7"``
    dataset_id: str          # e.g. ``"mm23"``
    frequency: str           # ``"months"`` or ``"quarters"``


INDICATOR_REGISTRY: dict[str, ONSIndicatorSpec] = {
    "CPI": ONSIndicatorSpec(
        indicator="CPI",
        country_code="UK",
        title="UK Inflation Rate",
        unit="percent",
        importance="high",
        category="Prices",
        path="economy/inflationandpriceindices",
        ts_id="d7g7",
        dataset_id="mm23",
        frequency="months",
    ),
    "UNEMPLOYMENT_RATE": ONSIndicatorSpec(
        indicator="UNEMPLOYMENT_RATE",
        country_code="UK",
        title="UK Unemployment Rate",
        unit="percent",
        importance="high",
        category="Labor",
        path="employmentandlabourmarket/peoplenotinwork/unemployment",
        ts_id="mgsx",
        dataset_id="lms",
        frequency="months",
    ),
    "GDP": ONSIndicatorSpec(
        indicator="GDP",
        country_code="UK",
        title="UK GDP Growth Rate",
        unit="percent",
        importance="high",
        category="Growth",
        path="economy/grossdomesticproductgdp",
        ts_id="ihyq",
        dataset_id="qna",
        frequency="quarters",
    ),
}


__all__ = ["INDICATOR_REGISTRY", "ONSIndicatorSpec"]
