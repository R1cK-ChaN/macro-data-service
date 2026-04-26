"""EIA weekly energy-stocks indicator whitelist (issue #50).

Four weekly indicators ship in P1 — the high-volume series traders
watch on Wednesday (petroleum) and Thursday (natural gas) at 10:30
ET:

- **CRUDE_OIL_STOCKS** — U.S. Ending Stocks excluding SPR of Crude
  Oil (series ``WCESTUS1``). Wednesday 10:30 ET, weekly.
- **GASOLINE_STOCKS** — U.S. Ending Stocks of Total Gasoline
  (series ``WGTSTUS1``). Wednesday 10:30 ET, weekly.
- **DISTILLATE_STOCKS** — U.S. Ending Stocks of Distillate Fuel
  Oil (series ``WDISTUS1``). Wednesday 10:30 ET, weekly.
- **NATURAL_GAS_STORAGE** — U.S. Total Natural Gas Working
  Underground Storage (series ``NW2_EPG0_SWO_NUS_BCF``). Thursday
  10:30 ET, weekly.

Each spec carries the ``route`` + ``facets`` the fetcher needs to
hit the EIA v2 endpoint. Crude/Gasoline/Distillate share
``petroleum/sum/sndw/data/`` (filter by series facet); natural-gas
storage lives at ``natural-gas/stor/wkly/data/``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class EIAIndicatorSpec:
    """Downstream-shape metadata + EIA v2 endpoint config."""

    indicator: str           # canonical token ("CRUDE_OIL_STOCKS")
    country_code: str        # ISO-3166-1 alpha-2 ("US")
    title: str               # cal_econ_event.title
    unit: str                # cal_econ_event.unit (e.g. "thousand_barrels")
    importance: str          # low / medium / high
    category: str
    series_id: str           # EIA series id (``WCESTUS1`` / ``NW2_EPG0_…``)
    route: str               # EIA v2 dataset path ("petroleum/sum/sndw/data/")
    # Extra facets to pin the response to one series. The default
    # carries ``frequency=weekly`` because every indicator on this
    # whitelist is weekly cadence; per-spec entries narrow further by
    # series / area facets.
    facets: tuple[tuple[str, str], ...] = ()
    # Day-of-week (Mon=0 … Sun=6) and HH:MM US-Eastern release time.
    # EIA prints both petroleum (Wed 10:30 ET) and natural-gas (Thu
    # 10:30 ET) on a fixed cadence, no published forward calendar —
    # the connector derives the next release from the latest
    # observed period + 7 days.
    release_dow: int = 2          # Wednesday by default
    release_time_local: str = "10:30 AM ET"
    # Holiday-adjusted releases shift to the next business day; the
    # connector doesn't try to predict shifts — it trusts the
    # observed period dates from the API as the authoritative
    # release-week anchor.


INDICATOR_REGISTRY: dict[str, EIAIndicatorSpec] = {
    "CRUDE_OIL_STOCKS": EIAIndicatorSpec(
        indicator="CRUDE_OIL_STOCKS",
        country_code="US",
        title="US Crude Oil Stocks (excl. SPR)",
        unit="thousand_barrels",
        importance="high",
        category="Energy",
        series_id="WCESTUS1",
        route="petroleum/sum/sndw/data/",
        facets=(("series", "WCESTUS1"),),
        release_dow=2,
        release_time_local="10:30 AM ET",
    ),
    "GASOLINE_STOCKS": EIAIndicatorSpec(
        indicator="GASOLINE_STOCKS",
        country_code="US",
        title="US Gasoline Stocks",
        unit="thousand_barrels",
        importance="medium",
        category="Energy",
        series_id="WGTSTUS1",
        route="petroleum/sum/sndw/data/",
        facets=(("series", "WGTSTUS1"),),
        release_dow=2,
        release_time_local="10:30 AM ET",
    ),
    "DISTILLATE_STOCKS": EIAIndicatorSpec(
        indicator="DISTILLATE_STOCKS",
        country_code="US",
        title="US Distillate Stocks",
        unit="thousand_barrels",
        importance="medium",
        category="Energy",
        series_id="WDISTUS1",
        route="petroleum/sum/sndw/data/",
        facets=(("series", "WDISTUS1"),),
        release_dow=2,
        release_time_local="10:30 AM ET",
    ),
    "NATURAL_GAS_STORAGE": EIAIndicatorSpec(
        indicator="NATURAL_GAS_STORAGE",
        country_code="US",
        title="US Natural Gas Storage",
        unit="billion_cubic_feet",
        importance="high",
        category="Energy",
        series_id="NW2_EPG0_SWO_NUS_BCF",
        route="natural-gas/stor/wkly/data/",
        # The wkly NG endpoint exposes per-region rows; pin to the
        # NUS aggregate so the fetcher gets one row per period.
        facets=(("series", "NW2_EPG0_SWO_NUS_BCF"),),
        release_dow=3,                 # Thursday
        release_time_local="10:30 AM ET",
    ),
}


__all__ = ["EIAIndicatorSpec", "INDICATOR_REGISTRY"]
