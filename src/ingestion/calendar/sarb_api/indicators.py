"""SARB calendar indicator whitelist — issue #90 P1.

P1 ships a single anchor — ``SARB_RATE`` — the repo (repurchase) rate
set by the SARB Monetary Policy Committee at each policy meeting.

The history endpoint at
``custom.resbank.co.za/SarbWebApi/WebIndicators/Shared/GetTimeseriesObservations/MRDREPOR``
returns one row per **rate-change** decision (modern coverage extends
back to 2017-07-21 in the captured fixture); hold decisions are
absent. Each row carries an absolute rate inline so the slice ships
value-bearing events for every change row — same TCMB-style coverage
as #86. Hold-decision coverage and authoritative announcement dates
are deferred to P2.

The shape mirrors :mod:`ingestion.calendar.tcmb_api.indicators` /
:mod:`ingestion.calendar.banxico_api.indicators` so projector / fetcher
code stays polymorphic across central-bank connectors.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SARBIndicatorSpec:
    """Downstream-shape metadata for a single SARB calendar indicator."""

    indicator: str           # canonical token ("SARB_RATE")
    country_code: str        # ISO-3166 alpha-2 ("ZA")
    title: str
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, SARBIndicatorSpec] = {
    "SARB_RATE": SARBIndicatorSpec(
        indicator="SARB_RATE",
        country_code="ZA",
        title="SARB Interest Rate Decision",
        unit="percent",
        importance="high",
        category="Monetary Policy",
    ),
}


__all__ = ["INDICATOR_REGISTRY", "SARBIndicatorSpec"]
