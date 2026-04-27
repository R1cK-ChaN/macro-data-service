"""Reserve Bank of Australia calendar indicator whitelist — issue #53 P1.

P1 ships a single anchor — ``RBA_RATE`` — the cash rate target set by
the RBA Monetary Policy Board. The cash-rate page at
``rba.gov.au/statistics/cash-rate/`` lists every interest-rate
decision since January 2008, including hold (no-change) decisions,
in a single HTML table.

The shape mirrors :mod:`ingestion.calendar.boc_api.indicators` so
projector / fetcher code stays polymorphic across central-bank
connectors.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RBAIndicatorSpec:
    """Downstream-shape metadata for a single RBA calendar indicator."""

    indicator: str           # canonical token ("RBA_RATE")
    country_code: str        # ISO-3166 alpha-2 ("AU")
    title: str
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, RBAIndicatorSpec] = {
    "RBA_RATE": RBAIndicatorSpec(
        indicator="RBA_RATE",
        country_code="AU",
        title="RBA Interest Rate Decision",
        unit="percent",
        importance="high",
        category="Monetary Policy",
    ),
}


__all__ = ["RBAIndicatorSpec", "INDICATOR_REGISTRY"]
