"""INEGI indicator whitelist — issue #88 P1.

Six headline Mexican indicators ship in P1, all served by INEGI's
``saladeprensa`` release-calendar JSON service at
``inegi.org.mx/app/api/saladeprensa/api/saladeprensa/ObtenerFechasTabla/v3``.
Each row carries an ``idPrograma`` (the INEGI programme id we filter on
at request time) plus a ``programa`` long-form title and a Spanish
``periodo`` reference-period string. The matcher anchors on the
programme id; ``programa_includes`` and the indicator's declared
``frequency`` disambiguate variants that share an id.

- **CPI (INPC mensual)** — Índice Nacional de Precios al Consumidor.
  The headline monthly CPI; published at 06:00 hora local (Mexico City)
  on the 9th of every month, covering the prior month's data
  (``periodo`` reads ``"<MesNombre> de <YYYY>"``).
- **INPC_15 (INPC quincenal)** — same idPrograma as the monthly INPC,
  distinguished by a ``periodo`` that starts with ``"Primera
  quincena"``. Mid-month preview; published on the 24th of each month
  at 06:00 hora local. Mirrors the IBGE IPCA-15 vs IPCA distinction
  from #84.
- **GDP (PIB Trimestral)** — Producto Interno Bruto Trimestral. The
  headline quarterly GDP; published two months and three weeks after
  the reference quarter closes.
- **INDUSTRIAL_PRODUCTION (IMAI)** — Indicador Mensual de la Actividad
  Industrial. Monthly industrial-production index, lag-2 to data month.
- **UNEMPLOYMENT_RATE (ENOE mensual)** — Encuesta Nacional de
  Ocupación y Empleo. ENOE publishes a monthly headline (``periodo`` =
  ``"<MesNombre> de <YYYY>"``) and a more detailed quarterly bulletin
  (``periodo`` = ``"<Ordinal> trimestre de <YYYY>"``); the cadence
  filter pins this slice to the monthly headline.
- **TRADE_BALANCE (Balanza Comercial — Información oportuna)** —
  Balanza Comercial de Mercancías de México. INEGI publishes both an
  advance ``"Información oportuna"`` and a follow-up ``"Cifras
  revisadas"`` under the same idPrograma; the advance is what TE /
  Bloomberg / Reuters bucket as the headline release. The
  ``programa_includes`` filter pins to the advance variant; the
  revised-cifras follow-up is deferred to P2.

The schedule-only slice publishes events with ``actual=NULL``. INEGI's
per-release press-release (boletín) PDFs reachable from each row's
``comunicadoEsUrlPdf`` carry the value side; per-indicator value
extraction (CPI MoM, IMAI index, IGAE QoQ %, unemployment rate, trade
balance USD) is deferred to P2 alongside that detail-page scrape.

Default release time per indicator is 06:00 hora local (Mexico City)
— INEGI standardises every boletín de difusión at that wall-clock time
under its ``Calendario de difusión`` rules. The ``release_time_local``
field allows a per-indicator override if a future indicator ships at
a different hour.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class INEGIIndicatorSpec:
    """Downstream-shape metadata for one INEGI calendar indicator."""

    indicator: str           # canonical token ("CPI")
    country_code: str        # always "MX" for INEGI
    title: str               # cal_econ_event.title
    unit: str                # cal_econ_event.unit (placeholder for schedule-only slice)
    importance: str
    category: str
    tematica_ids: tuple[str, ...]   # idPrograma values from ObtenerFechasTabla
    frequency: str           # "monthly" / "quarterly" / "biweekly"
    release_time_local: str  # America/Mexico_City wall-clock release time ("06:00")
    # All substrings must appear (case-insensitive) in the row's
    # ``programa`` text — used to discriminate variants that share an
    # idPrograma (e.g. Trade Balance "Información oportuna" vs the
    # follow-up "Cifras revisadas").
    programa_includes: tuple[str, ...] = field(default_factory=tuple)


# Anchored on idPrograma (server-side filter at request time) plus a
# per-indicator ``frequency`` cadence filter on the row's ``periodo``
# text. Some indicators share an idPrograma but ship under different
# canonical tokens — INPC and INPC_15 both come from idPrograma 2353,
# disambiguated by whether ``periodo`` starts with ``"Primera quincena"``
# (biweekly) or matches the monthly month-name + year shape. The
# ``programa_includes`` filter sharpens variants that share an id and
# a cadence (Trade Balance advance vs revised, both monthly under
# idPrograma 2355).
INDICATOR_REGISTRY: dict[str, INEGIIndicatorSpec] = {
    "INPC_15": INEGIIndicatorSpec(
        indicator="INPC_15",
        country_code="MX",
        title="Mexico INPC Mid-month CPI",
        unit="index",
        importance="medium",
        category="Prices",
        tematica_ids=("2353",),
        frequency="biweekly",
        release_time_local="06:00",
    ),
    "CPI": INEGIIndicatorSpec(
        indicator="CPI",
        country_code="MX",
        title="Mexico Consumer Price Index",
        unit="index",
        importance="high",
        category="Prices",
        tematica_ids=("2353",),
        frequency="monthly",
        release_time_local="06:00",
    ),
    "GDP": INEGIIndicatorSpec(
        indicator="GDP",
        country_code="MX",
        title="Mexico GDP",
        unit="index",
        importance="high",
        category="Growth",
        tematica_ids=("2648",),
        frequency="quarterly",
        release_time_local="06:00",
        # PIBT publishes two same-day boletines per quarter — the
        # "Precios Constantes" (real / volume) variant carries the
        # headline GDP growth % that TE / Bloomberg / Reuters bucket
        # under Mexico GDP. The "Precios Corrientes" (nominal) variant
        # is a sister release that would collide on the same reference
        # date — pin to the real-GDP boletín for P1; nominal stays
        # deferrable to P2.
        programa_includes=("Precios Constantes",),
    ),
    "INDUSTRIAL_PRODUCTION": INEGIIndicatorSpec(
        indicator="INDUSTRIAL_PRODUCTION",
        country_code="MX",
        title="Mexico Industrial Production",
        unit="index",
        importance="high",
        category="Production",
        tematica_ids=("2466",),
        frequency="monthly",
        release_time_local="06:00",
    ),
    "UNEMPLOYMENT_RATE": INEGIIndicatorSpec(
        indicator="UNEMPLOYMENT_RATE",
        country_code="MX",
        title="Mexico Unemployment Rate",
        unit="percent",
        importance="high",
        category="Labor",
        tematica_ids=("2303",),
        frequency="monthly",
        release_time_local="06:00",
    ),
    "TRADE_BALANCE": INEGIIndicatorSpec(
        indicator="TRADE_BALANCE",
        country_code="MX",
        title="Mexico Balance of Trade",
        unit="USD",
        importance="high",
        category="Trade",
        tematica_ids=("2355",),
        frequency="monthly",
        release_time_local="06:00",
        programa_includes=("Información oportuna",),
    ),
}


__all__ = ["INDICATOR_REGISTRY", "INEGIIndicatorSpec"]
