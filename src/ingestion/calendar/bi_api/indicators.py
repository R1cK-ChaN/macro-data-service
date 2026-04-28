"""Bank Indonesia calendar indicator whitelist — issue #92 P1.

P1 ships a single anchor — ``BI_RATE`` — the policy rate set by the
BI Board of Governors at each monthly meeting. Bank Indonesia
re-anchored the policy rate in August 2016 (BI 7-Day Reverse Repo
Rate / BI7DRR) and rebranded it again in April 2024 (BI-Rate); both
labels point to the same operational rate and the public history page
lists them in a single contiguous timeseries.

The page at ``bi.go.id/en/statistik/indikator/bi-rate.aspx`` lists
every BI Board of Governors meeting — change OR hold — with the
absolute rate inline. The slice ships value-bearing rows on day one:
``actual = <new rate>``, ``previous = <prior decision's rate>``.
Mirrors the BCB / RBA / Banxico value-bearing pattern from
#84 / #53 / #88.

The shape mirrors :mod:`ingestion.calendar.banxico_api.indicators` /
:mod:`ingestion.calendar.bcb_api.indicators` so projector / fetcher
code stays polymorphic across central-bank connectors.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BIIndicatorSpec:
    """Downstream-shape metadata for a single Bank Indonesia indicator."""

    indicator: str           # canonical token ("BI_RATE")
    country_code: str        # ISO-3166 alpha-2 ("ID")
    title: str
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, BIIndicatorSpec] = {
    "BI_RATE": BIIndicatorSpec(
        indicator="BI_RATE",
        country_code="ID",
        title="Bank Indonesia Interest Rate Decision",
        unit="percent",
        importance="high",
        category="Monetary Policy",
    ),
}


__all__ = ["BIIndicatorSpec", "INDICATOR_REGISTRY"]
