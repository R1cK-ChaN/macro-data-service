"""Banco Central do Brasil calendar indicator whitelist — issue #84 P1.

P1 ships a single anchor — ``BCB_RATE`` — the Selic target rate set by
the Comitê de Política Monetária (Copom) at each policy meeting. The
historical-rates JSON service at
``bcb.gov.br/api/servico/sitebcb/historicotaxasjuros`` exposes every
Copom decision since meeting #1 (June 1996), inclusive of hold (no-
change) decisions and rare extraordinary / monocratic-presidential
decisions. The slice is **schedule + value**: the JSON carries the
target Selic rate (``MetaSelic``) inline, so each Copom decision
projects with both schedule (announcement date) and value (new target
rate) populated in the same pass — RBA-style coverage rather than the
schedule-only deferral pattern.

The shape mirrors :mod:`ingestion.calendar.rba_api.indicators` /
:mod:`ingestion.calendar.boc_api.indicators` so projector / fetcher
code stays polymorphic across central-bank connectors.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BCBIndicatorSpec:
    """Downstream-shape metadata for a single BCB calendar indicator."""

    indicator: str           # canonical token ("BCB_RATE")
    country_code: str        # ISO-3166 alpha-2 ("BR")
    title: str
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, BCBIndicatorSpec] = {
    "BCB_RATE": BCBIndicatorSpec(
        indicator="BCB_RATE",
        country_code="BR",
        title="BCB Interest Rate Decision",
        unit="percent",
        importance="high",
        category="Monetary Policy",
    ),
}


__all__ = ["BCBIndicatorSpec", "INDICATOR_REGISTRY"]
