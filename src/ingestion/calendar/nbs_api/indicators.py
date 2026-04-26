"""NBS calendar indicator whitelist.

Each entry binds a canonical indicator token to the downstream shape
the projector writes into ``cal_econ_event``: country, display title,
unit, and trader-impact importance.

P5 shipped one anchor — ``CPI`` (China Consumer Price Index). P5c
expanded the whitelist with six more indicators, each verified
against the NBS 2026 yearly calendar via the live probe (2026-04-22):

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

P5c-gdp adds **GDP** — quarterly, 10:00 CST — published under the
"National Economic Performance" row alongside monthly roll-ups. The
NBS 2026 calendar note 3 states explicitly: "Annual and quarterly
national economic performance will be released in January, April,
July, and October", with the other seven calendar entries on that
row being the monthly aggregate rather than a GDP release. The spec
carries ``publishing_months=frozenset({1, 4, 7, 10})`` so the
scraper emits exactly four GDP events per year, and
``reference_cadence="quarterly"`` so the projector anchors each
event on the end-of-prior-quarter instead of the release-month
first-of-month used for monthly indicators.

The shape mirrors :mod:`ingestion.calendar.bls_api.indicators`,
:mod:`ingestion.calendar.bea_api.indicators`,
:mod:`ingestion.calendar.ecb_api.indicators`, and
:mod:`ingestion.calendar.fed_api.indicators`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


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
    # Optional filter — emit entries only for months in this set. Used
    # by GDP, which shares the "National Economic Performance" row with
    # monthly roll-ups; restricting to {1, 4, 7, 10} keeps the four
    # quarterly GDP events and drops the seven monthly cadence entries.
    # ``None`` (default) means every non-empty month cell emits an
    # entry — current behavior for monthly indicators.
    publishing_months: frozenset[int] | None = None
    # ``"monthly"`` (default): reference_date = first-of-release-month,
    # reference_label = ``"YYYY-MM"``. ``"quarterly"``: reference_date =
    # end-of-prior-quarter relative to the release date, reference_label
    # = ``"YYYY-QN"``. Determines how the projector anchors the event's
    # reference period on ``cal_econ_event``.
    reference_cadence: Literal["monthly", "quarterly"] = "monthly"
    # Issue #49 — value-side fetcher.
    # ``listing_title_fragment`` is the lower-cased substring the
    # press-release listing carries (``"consumer price index in"``,
    # ``"industrial producer price indexes in"``). The value-side
    # resolver matches a listing row when its release-date + title
    # contains this fragment. ``None`` means no value-side coverage —
    # the indicator stays schedule-only (today: PMI / GDP / Manufacturing
    # PMI / Non-Manufacturing PMI). When set, the indicator also
    # appears in the value-fetch roster.
    listing_title_fragment: str | None = None


INDICATOR_REGISTRY: dict[str, NBSIndicatorSpec] = {
    "CPI": NBSIndicatorSpec(
        indicator="CPI",
        country_code="CN",
        title="China Consumer Price Index",
        # Issue #49 — value-side ships YoY %; old "index" reflected the
        # schedule-only legacy where ``actual`` stayed NULL.
        unit="percent_yoy",
        importance="high",
        category="Prices",
        label_fragment="consumer price index",
        listing_title_fragment="consumer price index in",
    ),
    "PPI": NBSIndicatorSpec(
        indicator="PPI",
        country_code="CN",
        title="China Producer Price Index",
        unit="percent_yoy",
        importance="high",
        category="Prices",
        # Matches NBS row "Monthly Report on Industrial Producer Price
        # Index"; intentionally NOT just "industrial" so it doesn't
        # collide with "Industrial Production Operation …".
        label_fragment="producer price index",
        # Press-release listing uses the plural "Indexes" — keep the
        # fragment short and stable.
        listing_title_fragment="industrial producer price indexes in",
    ),
    "INDUSTRIAL_PRODUCTION": NBSIndicatorSpec(
        indicator="INDUSTRIAL_PRODUCTION",
        country_code="CN",
        title="China Industrial Production",
        unit="percent_yoy",
        importance="high",
        category="Output",
        # Matches NBS row "Monthly Report on Industrial Production
        # Operation Above the Designated Size". "Operation" keeps
        # the match disjoint from the PPI row.
        label_fragment="industrial production operation",
        listing_title_fragment="industrial production operation in",
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
        # NBS publishes the FAI release with a "from January to <Month>"
        # title shape (cumulative YTD). Match the stable opening only.
        listing_title_fragment="investment in fixed assets from",
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
        # Listing carries either "in <Month>" or "from January to
        # <Month>" depending on whether the release covers the
        # single-month or YTD aggregate. The shared opening is enough
        # to disambiguate from CPI / IP.
        listing_title_fragment="total retail sales of consumer goods",
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
    "GDP": NBSIndicatorSpec(
        indicator="GDP",
        country_code="CN",
        title="China GDP",
        unit="percent_yoy",
        importance="high",
        category="Activity",
        # "National Economic Performance" is NBS's umbrella row — it
        # carries the quarterly GDP release alongside monthly roll-ups.
        # The month filter below is what separates the quarterly GDP
        # entries from the monthly aggregate entries; without it, GDP
        # would over-emit 11 events per year.
        label_fragment="national economic performance",
        # Per NBS 2026 calendar note 3: "Annual and quarterly national
        # economic performance will be released in January, April,
        # July, and October". Everything outside these four months on
        # this row is the monthly national-economic-performance release
        # (a different publication that isn't on our whitelist).
        publishing_months=frozenset({1, 4, 7, 10}),
        # January release covers the prior-year Q4 annual + quarterly;
        # April / July / October cover the immediately preceding
        # quarter. The projector translates release month → end-of-
        # prior-quarter reference date.
        reference_cadence="quarterly",
    ),
}
