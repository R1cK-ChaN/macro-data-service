"""Banxico calendar indicator whitelist — issue #88 P1.

P1 ships a single anchor — ``BANXICO_RATE`` — the Tasa Objetivo (target
rate for the Tasa de Interés Interbancaria a 1 día / overnight
interbank rate) set by Banxico's Junta de Gobierno at each policy
meeting.

The decision history page at
``banxico.org.mx/publicaciones-y-prensa/anuncios-de-las-decisiones-de-politica-monetaria/anuncios-politica-monetaria-t.html``
is server-rendered HTML carrying every Banxico policy decision since
the modern Tasa Objetivo regime began on 21 January 2008. The slice is
**schedule + value**: the link text encodes either the absolute rate
(``"se mantiene sin cambio en 7.00 por ciento"``) for hold decisions
or the basis-point delta (``"disminuye en 25 puntos base"`` /
``"aumenta en 25 puntos base"`` / ``"se incrementa en 25 puntos base"``)
for change decisions. A cumulative walk seeded from the oldest
decision (which is itself a hold under the Tasa Objetivo regime)
yields the absolute rate for **every** decision; ``actual`` and
``previous`` populate inline — BCB-style coverage rather than the
TCMB / BoC schedule-only deferral pattern.

Pre-2008 rows on the same page describe the historical "corto"
liquidity-management instrument (``"El \"corto\" se aumenta a 350
millones de pesos"``); they're filtered out at parse time so the
canonical ``BANXICO_RATE`` series stays a clean Tasa Objetivo
timeseries.

The shape mirrors :mod:`ingestion.calendar.bcb_api.indicators` /
:mod:`ingestion.calendar.tcmb_api.indicators` so projector / fetcher
code stays polymorphic across central-bank connectors.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BanxicoIndicatorSpec:
    """Downstream-shape metadata for a single Banxico calendar indicator."""

    indicator: str           # canonical token ("BANXICO_RATE")
    country_code: str        # ISO-3166 alpha-2 ("MX")
    title: str
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, BanxicoIndicatorSpec] = {
    "BANXICO_RATE": BanxicoIndicatorSpec(
        indicator="BANXICO_RATE",
        country_code="MX",
        title="Banxico Interest Rate Decision",
        unit="percent",
        importance="high",
        category="Monetary Policy",
    ),
}


__all__ = ["BanxicoIndicatorSpec", "INDICATOR_REGISTRY"]
