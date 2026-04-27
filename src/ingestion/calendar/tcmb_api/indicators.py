"""TCMB calendar indicator whitelist — issue #86 P1.

P1 ships a single anchor — ``TCMB_RATE`` — the 1-Week Repo Auction
Rate set by the Para Politikası Kurulu (PPK / Monetary Policy
Committee) at each meeting. The rate-history HTML table at
``tcmb.gov.tr/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Temel+Faaliyetler/
Para+Politikasi/Merkez+Bankasi+Faiz+Oranlari/1+Hafta+Repo`` exposes
every rate-change announcement since 20 May 2010 (when the 1-week
repo became the policy rate). The slice is **schedule + value**: the
table carries the new rate inline next to the announcement date, so
each decision projects with both schedule (announcement date) and
value (new policy rate) populated in the same pass — RBA / BCB-style
coverage rather than the schedule-only deferral pattern.

The shape mirrors :mod:`ingestion.calendar.bcb_api.indicators` /
:mod:`ingestion.calendar.rba_api.indicators` so projector / fetcher
code stays polymorphic across central-bank connectors.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TCMBIndicatorSpec:
    """Downstream-shape metadata for a single TCMB calendar indicator."""

    indicator: str           # canonical token ("TCMB_RATE")
    country_code: str        # ISO-3166 alpha-2 ("TR")
    title: str
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, TCMBIndicatorSpec] = {
    "TCMB_RATE": TCMBIndicatorSpec(
        indicator="TCMB_RATE",
        country_code="TR",
        title="TCMB Interest Rate Decision",
        unit="percent",
        importance="high",
        category="Monetary Policy",
    ),
}


__all__ = ["INDICATOR_REGISTRY", "TCMBIndicatorSpec"]
