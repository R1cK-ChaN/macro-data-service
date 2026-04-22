"""ECB series-id → calendar-event metadata whitelist.

Each entry binds an ECB SDMX series key to the downstream shape the
projector writes into ``cal_econ_event``: the canonical indicator
token, country code, display title, unit, and trader-impact importance.

P3 ships three anchors — the three ECB key policy rates published in
the ``FM`` dataflow ("Financial markets and interest rates"):

- **MRO** (Main Refinancing Operations fixed rate) — the rate at which
  the Eurosystem lends to banks against collateral.
- **DFR** (Deposit Facility Rate) — the rate on overnight deposits at
  the Eurosystem; the Governing Council's primary policy instrument
  since 2019.
- **MLFR** (Marginal Lending Facility Rate) — the rate on overnight
  credit extended to banks against collateral.

Economic Bulletin + monetary-policy press conference timing are
calendar-only (no time-series data); they land in the P3a slice that
scrapes ``ecb.europa.eu/press/calendars/`` directly. The scaffold
covers only the value-bearing ECB Data Portal path.

The shape mirrors :mod:`ingestion.calendar.bls_api.indicators` and
:mod:`ingestion.calendar.bea_api.indicators` so the projector /
fetcher can stay polymorphic across connectors.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ECBIndicatorSpec:
    """Downstream-shape metadata for a single ECB SDMX series."""

    series_id: str           # full SDMX id ("FM.B.U2.EUR.4F.KR.DFR.LEV"); matches SDMXObservation.series_id
    dataflow_id: str         # SDMX dataflow ("FM")
    series_key: str          # SDMX key ("B.U2.EUR.4F.KR.DFR.LEV"); what goes into the URL
    indicator: str           # canonical token ("ECB_DFR", "ECB_MRO", "ECB_MLF")
    country_code: str        # ISO-3166-1 alpha-2 (EU used throughout the codebase for Eurozone)
    title: str               # human-readable, stored in cal_econ_event.title
    unit: str
    importance: str          # low / medium / high
    category: str            # free-text, mirrors TE's Category


# ECB series keys follow the FM dataflow convention:
#   ``{freq}.{ref_area}.{currency}.{sector}.{instr_asset}.{rate_code}.{series_var}``
# For the three policy rates:
#   freq          = B  (business-daily; the ECB's policy rates publish
#                       on each business day regardless of whether a new
#                       decision landed)
#   ref_area      = U2 (Euro area)
#   currency      = EUR
#   sector        = 4F (financial markets)
#   instr_asset   = KR (key ECB interest rates)
#   rate_code     = MRR_FR (MRO fixed) | DFR (deposit) | MLFR (marginal)
#   series_var    = LEV (level)
#
# The P3a live probe will confirm each key round-trips through
# ECB Data Portal before any recurring fetch is scheduled. Shipping the
# three together here matches the ECB rate-decision release shape:
# every Governing Council decision publishes updates for all three
# rates simultaneously, so a trader watching one watches all.


INDICATOR_REGISTRY: dict[str, ECBIndicatorSpec] = {
    "FM.B.U2.EUR.4F.KR.MRR_FR.LEV": ECBIndicatorSpec(
        series_id="FM.B.U2.EUR.4F.KR.MRR_FR.LEV",
        dataflow_id="FM",
        series_key="B.U2.EUR.4F.KR.MRR_FR.LEV",
        indicator="ECB_MRO",
        country_code="EU",
        title="ECB Main Refinancing Operations Rate",
        unit="percent",
        importance="high",
        category="Monetary Policy",
    ),
    "FM.B.U2.EUR.4F.KR.DFR.LEV": ECBIndicatorSpec(
        series_id="FM.B.U2.EUR.4F.KR.DFR.LEV",
        dataflow_id="FM",
        series_key="B.U2.EUR.4F.KR.DFR.LEV",
        indicator="ECB_DFR",
        country_code="EU",
        title="ECB Deposit Facility Rate",
        unit="percent",
        importance="high",
        category="Monetary Policy",
    ),
    "FM.B.U2.EUR.4F.KR.MLFR.LEV": ECBIndicatorSpec(
        series_id="FM.B.U2.EUR.4F.KR.MLFR.LEV",
        dataflow_id="FM",
        series_key="B.U2.EUR.4F.KR.MLFR.LEV",
        indicator="ECB_MLF",
        country_code="EU",
        title="ECB Marginal Lending Facility Rate",
        unit="percent",
        importance="high",
        category="Monetary Policy",
    ),
}
