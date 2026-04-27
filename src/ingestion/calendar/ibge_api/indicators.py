"""IBGE indicator whitelist — issue #84 P1.

Five headline Brazilian indicators ship in P1, all served by the IBGE
monthly release-calendar HTML at
``ibge.gov.br/calendario/mensal.html?mes=N&ano=YYYY``. Each event row
carries a release ``title`` (the linked product name) whose lowercase
substring the fetcher matches against ``title_substrings`` to identify
the indicator.

- **IPCA** — Índice Nacional de Preços ao Consumidor Amplo (the
  headline CPI). Title rows read
  ``"Índice Nacional de Preços ao Consumidor Amplo"`` (sometimes with
  a trailing period).
- **IPCA-15** — Índice Nacional de Preços ao Consumidor Amplo 15, the
  mid-month preview of IPCA. Title rows read
  ``"Índice Nacional de Preços ao Consumidor Amplo 15"``. The ``15``
  suffix is what distinguishes it from the headline IPCA in the same
  schedule, so the matcher anchors on the longer-prefix substring
  *first*: see :func:`announcement_matches_spec` for the priority rule.
- **PIM-PF** — Pesquisa Industrial Mensal: Produção Física - Brasil
  (Industrial Production, monthly). Title rows read
  ``"Pesquisa Industrial Mensal: Produção Física - Brasil"``.
- **PNAD-Continua-Mensal** — Pesquisa Nacional por Amostra de
  Domicílios Contínua Mensal (the headline unemployment rate). Title
  rows read
  ``"Pesquisa Nacional por Amostra de Domicílios Contínua Mensal"``.
- **PIB** — Sistema de Contas Nacionais Trimestrais (the quarterly
  GDP release; IBGE publishes PIB through this product). Title rows
  read ``"Sistema de Contas Nacionais Trimestrais"``.

The schedule-only slice publishes events with ``actual=NULL``. IBGE
exposes per-release press-release pages reachable from the calendar's
``<a href>`` link; per-indicator value extraction (IPCA index level,
PIM-PF index, Unemployment Rate, PIB QoQ %) is deferred to P2 alongside
that detail-page parser.

Default release time per indicator follows IBGE's release-calendar
``data-divulgacao`` timestamps. Most monthly indicators publish at
09:00 BRT; PIB and PIM-PF Brasil also publish at 09:00 BRT. The
``release_time_local`` field allows a per-indicator override if a
future indicator ships at a different hour.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IBGEIndicatorSpec:
    """Downstream-shape metadata for one IBGE calendar indicator."""

    indicator: str           # canonical token ("CPI")
    country_code: str        # always "BR" for IBGE
    title: str               # cal_econ_event.title
    unit: str                # cal_econ_event.unit (placeholder for schedule-only slice)
    importance: str
    category: str
    produto_ids: tuple[str, ...]  # IBGE product number(s) — stable per indicator
    frequency: str           # "monthly" / "quarterly"
    release_time_local: str  # BRT wall-clock release time ("09:00")


# Indicators are matched by ``data-produto-id`` (the IBGE product
# number embedded in the calendar's ``<a>`` link) rather than by
# title-substring. The product number is stable and unambiguous — IPCA
# (9256), IPCA-15 (9260), IPCA Especial (9270) are distinct ids — so a
# title-substring approach (``"índice nacional de preços ao consumidor
# amplo"`` is a prefix of every IPCA variant's display name) would
# false-match without an exclusion list. The product-id approach
# matches the rest of the IBGE catalog by definition.
INDICATOR_REGISTRY: dict[str, IBGEIndicatorSpec] = {
    "IPCA_15": IBGEIndicatorSpec(
        indicator="IPCA_15",
        country_code="BR",
        title="Brazil IPCA-15 Mid-month CPI",
        unit="index",
        importance="medium",
        category="Prices",
        produto_ids=("9260",),
        frequency="monthly",
        release_time_local="09:00",
    ),
    "CPI": IBGEIndicatorSpec(
        indicator="CPI",
        country_code="BR",
        title="Brazil Consumer Price Index",
        unit="index",
        importance="high",
        category="Prices",
        produto_ids=("9256",),
        frequency="monthly",
        release_time_local="09:00",
    ),
    "INDUSTRIAL_PRODUCTION": IBGEIndicatorSpec(
        indicator="INDUSTRIAL_PRODUCTION",
        country_code="BR",
        title="Brazil Industrial Production",
        unit="index",
        importance="high",
        category="Production",
        produto_ids=("9294",),
        frequency="monthly",
        release_time_local="09:00",
    ),
    "UNEMPLOYMENT_RATE": IBGEIndicatorSpec(
        indicator="UNEMPLOYMENT_RATE",
        country_code="BR",
        title="Brazil Unemployment Rate",
        unit="percent",
        importance="high",
        category="Labor",
        produto_ids=("9171",),
        frequency="monthly",
        release_time_local="09:00",
    ),
    "GDP": IBGEIndicatorSpec(
        indicator="GDP",
        country_code="BR",
        title="Brazil GDP",
        unit="index",
        importance="high",
        category="Growth",
        produto_ids=("9300",),
        frequency="quarterly",
        release_time_local="09:00",
    ),
}


__all__ = ["INDICATOR_REGISTRY", "IBGEIndicatorSpec"]
