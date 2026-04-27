"""Bank of Canada calendar indicator whitelist — issue #52 P1.

P1 ships a single anchor — ``BOC_RATE`` — the Target for the
Overnight Rate set by the BoC Governing Council. The Valet API
publishes the daily target rate; rate-change days are the
announcement days the connector projects into ``cal_econ_event``.

The shape mirrors :mod:`ingestion.calendar.boe_api.indicators` so
projector / fetcher code stays polymorphic across central-bank
connectors.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BoCIndicatorSpec:
    """Downstream-shape metadata for a single BoC calendar indicator."""

    indicator: str           # canonical token ("BOC_RATE")
    country_code: str        # ISO-3166 alpha-2 ("CA")
    title: str
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, BoCIndicatorSpec] = {
    "BOC_RATE": BoCIndicatorSpec(
        indicator="BOC_RATE",
        country_code="CA",
        title="BoC Interest Rate Decision",
        unit="percent",
        importance="high",
        category="Monetary Policy",
    ),
}


__all__ = ["BoCIndicatorSpec", "INDICATOR_REGISTRY"]
