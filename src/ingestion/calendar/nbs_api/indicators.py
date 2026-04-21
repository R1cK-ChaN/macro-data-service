"""NBS calendar indicator whitelist.

Each entry binds a canonical indicator token to the downstream shape
the projector writes into ``cal_econ_event``: country, display title,
unit, and trader-impact importance.

P5 ships a **single anchor** — ``CPI`` (China Consumer Price Index).
NBS CPI is the flagship China inflation release, watched globally as
input into PBOC policy expectations and the CNY carry story. Every
month the release lands at 09:30 Beijing time on a fixed day the NBS
publishes in its yearly "Regular Press Release Calendar" document.

PPI, GDP, Industrial Production, Fixed Asset Investment, Retail Sales,
Manufacturing PMI, and Non-manufacturing PMI are all reachable via the
same release-calendar page but ship in later slices — adding them is
mostly a whitelist extension once the scraper shape is validated
against a live probe. The scaffold keeps the surface tight so review
stays focused.

The shape mirrors :mod:`ingestion.calendar.bls_api.indicators`,
:mod:`ingestion.calendar.bea_api.indicators`,
:mod:`ingestion.calendar.ecb_api.indicators`, and
:mod:`ingestion.calendar.fed_api.indicators`.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NBSIndicatorSpec:
    """Downstream-shape metadata for a single NBS calendar indicator."""

    indicator: str           # canonical token ("CPI")
    country_code: str        # ISO-3166-1 alpha-2 ("CN")
    title: str               # human-readable, stored in cal_econ_event.title
    unit: str
    importance: str          # low / medium / high
    category: str            # free-text, mirrors TE's Category
    # Verbatim content-cell label the scraper must match in the
    # release-calendar table (``Monthly Report on Consumer Price Index
    # (CPI)``). Match is substring-based and case-insensitive so minor
    # upstream wording tweaks don't silently drop the indicator.
    label_fragment: str


INDICATOR_REGISTRY: dict[str, NBSIndicatorSpec] = {
    "CPI": NBSIndicatorSpec(
        indicator="CPI",
        country_code="CN",
        title="China Consumer Price Index",
        unit="index",
        importance="high",
        category="Prices",
        label_fragment="consumer price index",
    ),
}
