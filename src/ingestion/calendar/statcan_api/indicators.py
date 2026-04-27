"""Statistics Canada indicator whitelist — issue #52 P1.

Three headline Canadian indicators ship in P1, all served by the
StatCan Web Data Service (WDS) per-vector latest-N endpoint
``www150.statcan.gc.ca/t1/wds/rest/getDataFromVectorsAndLatestNPeriods``:

- **CPI** — All-items Consumer Price Index, monthly index
  (vector ``41690973``, table 18-10-0004-01, ``2002=100``).
  Stored as the index level, mirroring the BLS pattern. The 12-
  month YoY rate that TE publishes for "Canada Inflation Rate"
  has no clean single-vector publication on WDS, so deriving it
  is a follow-up; the connector still produces one event per
  release with the upstream-native value.
- **UNEMPLOYMENT_RATE** — Labour Force Survey, total 15+,
  seasonally adjusted percentage (vector ``2062815``, table
  14-10-0287-01). Already a percent; directly comparable to TE's
  "Canada Unemployment Rate".
- **GDP** — Monthly real GDP, all industries, chained 2017
  dollars at annual rates (vector ``65201210``, table
  36-10-0434-01). Stored as the millions-CAD level. TE's
  "Canada GDP Growth Rate" is QoQ percent; bridging the two
  needs a derivation step deferred to a follow-up slice. Still
  emitted so the calendar carries the StatCan release-day
  anchor.

Each spec carries the WDS vector id plus a frequency hint so
the parser knows to walk the latest observation; future
extensions can add quarterly vectors by setting
``frequency='quarterly'``.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StatCanIndicatorSpec:
    """Downstream-shape metadata for one StatCan WDS indicator."""

    indicator: str           # canonical token (``"CPI"``)
    country_code: str        # always ``"CA"`` for StatCan
    title: str               # cal_econ_event.title
    unit: str                # cal_econ_event.unit
    importance: str
    category: str
    vector_id: int           # WDS vector id (e.g. ``41690973``)
    frequency: str           # ``"monthly"`` | ``"quarterly"``
    series_label: str        # short human-readable series description


INDICATOR_REGISTRY: dict[str, StatCanIndicatorSpec] = {
    "CPI": StatCanIndicatorSpec(
        indicator="CPI",
        country_code="CA",
        title="Canada Consumer Price Index",
        unit="index",
        importance="high",
        category="Prices",
        vector_id=41690973,
        frequency="monthly",
        series_label="All-items, Canada (2002=100)",
    ),
    "UNEMPLOYMENT_RATE": StatCanIndicatorSpec(
        indicator="UNEMPLOYMENT_RATE",
        country_code="CA",
        title="Canada Unemployment Rate",
        unit="percent",
        importance="high",
        category="Labor",
        vector_id=2062815,
        frequency="monthly",
        series_label="Labour Force Survey, 15+, seasonally adjusted",
    ),
    "GDP": StatCanIndicatorSpec(
        indicator="GDP",
        country_code="CA",
        title="Canada GDP",
        unit="millions_cad_saar",
        importance="high",
        category="Growth",
        vector_id=65201210,
        frequency="monthly",
        series_label="Monthly real GDP, all industries, chained 2017 CAD SAAR",
    ),
}


__all__ = ["INDICATOR_REGISTRY", "StatCanIndicatorSpec"]
