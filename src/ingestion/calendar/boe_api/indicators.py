"""Bank of England calendar indicator whitelist — issue #51 P1.

P1 ships a single anchor — ``BOE_RATE`` — the Official Bank Rate
set by the Monetary Policy Committee. Every row on the BoE's
Bank Rate history page corresponds to one MPC rate-change
decision; this connector projects each row into a calendar event
that carries the new rate as ``actual``.

The shape mirrors :mod:`ingestion.calendar.fed_api.indicators` so
projector / fetcher code stays polymorphic across central-bank
connectors.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BoEIndicatorSpec:
    """Downstream-shape metadata for a single BoE calendar indicator."""

    indicator: str           # canonical token ("BOE_RATE")
    country_code: str        # ISO-3166 alpha-2 ("UK")
    title: str
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, BoEIndicatorSpec] = {
    "BOE_RATE": BoEIndicatorSpec(
        indicator="BOE_RATE",
        country_code="UK",
        title="BoE Interest Rate Decision",
        unit="percent",
        importance="high",
        category="Monetary Policy",
    ),
}


__all__ = ["BoEIndicatorSpec", "INDICATOR_REGISTRY"]
