"""NBS calendar indicator whitelist.

Each entry binds a canonical indicator token to the downstream shape
the projector writes into ``cal_econ_event``: country, display title,
unit, and trader-impact importance.

P5 shipped one anchor — ``CPI`` (China Consumer Price Index). P5c
expands the whitelist with six more indicators, each verified against
the NBS 2026 yearly calendar via the live probe (2026-04-22):

- **PPI** — Industrial Producer Price Index (monthly, 09:30 CST).
- **Industrial Production** — "Monthly Report on Industrial Production
  Operation Above the Designated Size" (monthly, 10:00 CST, Feb
  combined into March per the Spring Festival adjustment).
- **Fixed Asset Investment** — "Monthly Report on Investment in Fixed
  Assets (Excluding Rural Households)" (monthly, 10:00 CST, Feb
  combined into March).
- **Retail Sales** — "Monthly Report on Total Retail Sales of Consumer
  Goods" (monthly, 10:00 CST, Feb combined into March).
- **Manufacturing PMI** — shares the NBS PMI row (monthly, 09:30 CST).
- **Non-Manufacturing PMI** — shares the NBS PMI row (monthly, 09:30
  CST). The ``label_fragment`` is identical to Manufacturing PMI
  because NBS publishes both together under the single
  "Purchasing Managers' Index (PMI)" release; the scraper emits two
  :class:`NBSReleaseEntry` rows per match so both indicators land as
  distinct calendar events.

GDP is *not* registered. NBS publishes quarterly GDP inside the
monthly "National Economic Performance" release (rows 1–2 on the
calendar), which mixes 20+ indicators into one release rather than
giving GDP a dedicated row. Registering
``label_fragment="national economic performance"`` as GDP would
over-emit (11 monthly events vs the 4 quarterly GDP releases that
matter to traders); handling that cadence needs a quarterly-month
filter that belongs in a dedicated follow-up slice.

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
    "PPI": NBSIndicatorSpec(
        indicator="PPI",
        country_code="CN",
        title="China Producer Price Index",
        unit="index",
        importance="high",
        category="Prices",
        # Matches NBS row "Monthly Report on Industrial Producer Price
        # Index"; intentionally NOT just "industrial" so it doesn't
        # collide with "Industrial Production Operation …".
        label_fragment="producer price index",
    ),
    "INDUSTRIAL_PRODUCTION": NBSIndicatorSpec(
        indicator="INDUSTRIAL_PRODUCTION",
        country_code="CN",
        title="China Industrial Production",
        unit="index",
        importance="high",
        category="Output",
        # Matches NBS row "Monthly Report on Industrial Production
        # Operation Above the Designated Size". "Operation" keeps
        # the match disjoint from the PPI row.
        label_fragment="industrial production operation",
    ),
    "FIXED_ASSET_INVESTMENT": NBSIndicatorSpec(
        indicator="FIXED_ASSET_INVESTMENT",
        country_code="CN",
        title="China Fixed Asset Investment",
        unit="percent_ytd",
        importance="medium",
        category="Investment",
        # Matches NBS row "Monthly Report on Investment in Fixed Assets
        # (Excluding Rural Households)".
        label_fragment="investment in fixed assets",
    ),
    "RETAIL_SALES": NBSIndicatorSpec(
        indicator="RETAIL_SALES",
        country_code="CN",
        title="China Retail Sales",
        unit="percent_yoy",
        importance="high",
        category="Consumption",
        # Matches NBS row "Monthly Report on Total Retail Sales of
        # Consumer Goods". The "consumer" inside the label_fragment
        # doesn't collide with the CPI row because the CPI fragment
        # requires "price index" as well.
        label_fragment="retail sales of consumer goods",
    ),
    "MANUFACTURING_PMI": NBSIndicatorSpec(
        indicator="MANUFACTURING_PMI",
        country_code="CN",
        title="China Manufacturing PMI",
        unit="index",
        importance="high",
        category="Activity",
        # Both MANUFACTURING_PMI and NON_MANUFACTURING_PMI share this
        # fragment so the scraper emits two entries for each PMI
        # release date (NBS publishes both PMI measures in the same
        # press release, at the same time, under one calendar row).
        label_fragment="purchasing managers",
    ),
    "NON_MANUFACTURING_PMI": NBSIndicatorSpec(
        indicator="NON_MANUFACTURING_PMI",
        country_code="CN",
        title="China Non-Manufacturing PMI",
        unit="index",
        importance="high",
        category="Activity",
        label_fragment="purchasing managers",
    ),
}
