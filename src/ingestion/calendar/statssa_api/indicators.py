"""Stats SA calendar indicator whitelist — issue #90 P1.

Seven headline South African indicators ship in P1, all served by the
Stats SA Publication Schedule AJAX endpoint at
``statssa.gov.za/wp-content/themes/umkhanyakude-v2.1/ajax_server.php?req=recently_scheduled_eddie_t``.
The matcher anchors on the Stats SA Publication Number (``PPN``) — a
stable, opaque identifier (``P0141``, ``P0441``, …) that persists
across rebrands and survey-design changes — plus a per-indicator
``frequency`` cadence filter on the row's reference period text.

- **CPI (P0141)** — Consumer Price Index. Monthly headline inflation;
  published mid-month at 10:00 SAST covering the prior month's data.
- **PPI (P0142.1)** — Producer Price Index. Monthly producer-side
  inflation; lag-2 to data month.
- **GDP (P0441)** — Gross Domestic Product. Quarterly headline GDP;
  published two months after the reference quarter closes.
- **UNEMPLOYMENT_RATE (P0211)** — Quarterly Labour Force Survey
  (QLFS). Quarterly headline unemployment rate.
- **MINING_PRODUCTION (P2041)** — Mining: Production and sales.
  Monthly mining-output index; lag-2 to data month.
- **MANUFACTURING_PRODUCTION (P3041.2)** — Manufacturing: Production
  and sales. Monthly manufacturing-output index; lag-2 to data month.
- **RETAIL_SALES (P6242.1)** — Retail trade sales. Monthly retail
  trade volume; lag-2 to data month.

The schedule-only slice publishes events with ``actual=NULL``. The
per-release boletín pages reachable from each row's ``Download link``
URL (``?page_id=1854&PPN=<PPN>``) carry the value side; per-indicator
value extraction (CPI MoM/YoY, PPI YoY, GDP QoQ %, unemployment rate,
mining/manufacturing/retail volume index) is deferred to P2 alongside
that detail-page scrape.

Each row carries an explicit ``_start`` datetime in the AddThisEvent
metadata block (``DD-MM-YYYY HH:MM:SS``), so a per-indicator default
release time isn't needed for parser fallback. The shape mirrors
:mod:`ingestion.calendar.inegi_api.indicators` so projector / fetcher
code stays polymorphic across release-schedule connectors.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StatsSAIndicatorSpec:
    """Downstream-shape metadata for one Stats SA calendar indicator."""

    indicator: str           # canonical token ("CPI")
    country_code: str        # always "ZA" for Stats SA
    title: str               # cal_econ_event.title
    unit: str                # cal_econ_event.unit (placeholder for schedule-only slice)
    importance: str
    category: str
    ppn: str                 # Stats SA Publication Number ("P0141")
    frequency: str           # "monthly" / "quarterly"


# Anchored on PPN (Publication Number). The reference period filter
# splits indicators that publish at multiple cadences under different
# PPNs (Stats SA tends to assign distinct PPNs per cadence — unlike
# INEGI which shares idPrograma across cadences for the same survey),
# so the cadence filter here is a layout-drift safety net rather than
# a primary disambiguator.
INDICATOR_REGISTRY: dict[str, StatsSAIndicatorSpec] = {
    "CPI": StatsSAIndicatorSpec(
        indicator="CPI",
        country_code="ZA",
        title="South Africa Consumer Price Index",
        unit="index",
        importance="high",
        category="Prices",
        ppn="P0141",
        frequency="monthly",
    ),
    "PPI": StatsSAIndicatorSpec(
        indicator="PPI",
        country_code="ZA",
        title="South Africa Producer Price Index",
        unit="index",
        importance="high",
        category="Prices",
        ppn="P0142.1",
        frequency="monthly",
    ),
    "GDP": StatsSAIndicatorSpec(
        indicator="GDP",
        country_code="ZA",
        title="South Africa GDP",
        unit="index",
        importance="high",
        category="Growth",
        ppn="P0441",
        frequency="quarterly",
    ),
    "UNEMPLOYMENT_RATE": StatsSAIndicatorSpec(
        indicator="UNEMPLOYMENT_RATE",
        country_code="ZA",
        title="South Africa Unemployment Rate",
        unit="percent",
        importance="high",
        category="Labor",
        ppn="P0211",
        frequency="quarterly",
    ),
    "MINING_PRODUCTION": StatsSAIndicatorSpec(
        indicator="MINING_PRODUCTION",
        country_code="ZA",
        title="South Africa Mining Production",
        unit="index",
        importance="high",
        category="Production",
        ppn="P2041",
        frequency="monthly",
    ),
    "MANUFACTURING_PRODUCTION": StatsSAIndicatorSpec(
        indicator="MANUFACTURING_PRODUCTION",
        country_code="ZA",
        title="South Africa Manufacturing Production",
        unit="index",
        importance="high",
        category="Production",
        ppn="P3041.2",
        frequency="monthly",
    ),
    "RETAIL_SALES": StatsSAIndicatorSpec(
        indicator="RETAIL_SALES",
        country_code="ZA",
        title="South Africa Retail Sales",
        unit="index",
        importance="high",
        category="Trade",
        ppn="P6242.1",
        frequency="monthly",
    ),
}


__all__ = ["INDICATOR_REGISTRY", "StatsSAIndicatorSpec"]
