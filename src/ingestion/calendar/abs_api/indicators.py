"""ABS indicator whitelist — issue #53 P1.

Three headline Australian indicators ship in P1, all served by the
ABS release-calendar HTML at
``abs.gov.au/release-calendar/latest-releases``. Each release card
on that page carries a ``<time datetime>`` UTC timestamp, an
``<h3>`` headline link, and a ``release__subtitle`` topic name.
The fetcher matches release-card URL slugs against
``url_path_prefix`` to identify the indicator.

- **CPI** — Consumer Price Index, Australia (monthly cadence as of
  late 2025; ABS unified the monthly indicator with the historical
  quarterly product). URL path:
  ``/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia/<mon>-<year>``.
- **UNEMPLOYMENT_RATE** — Labour Force, Australia (headline release;
  the ``-detailed`` variant is intentionally excluded — it ships a
  week later with cross-tabs but doesn't move TE's headline figure).
  URL path:
  ``/statistics/labour/employment-and-unemployment/labour-force-australia/<mon>-<year>``.
- **GDP** — Australian National Accounts: National Income,
  Expenditure and Product (quarterly). URL path:
  ``/statistics/economy/national-accounts/australian-national-accounts-national-income-expenditure-and-product/<mon>-<year>``
  where ``<mon>`` is the last month of the quarter (``dec-2025`` =
  Q4 2025).

The schedule-only slice publishes events with ``actual=NULL``.
ABS does expose values via the SDMX data API at
``api.data.abs.gov.au``, but that surface needs per-indicator
SDMX key parsing (CPI ``1.10001.10.50.Q``, LF ``M3.1599.20.AUS.M``,
ANA chained-volume measure key) and quarterly-vs-monthly reference-
date alignment that the ``UNEMPLOYMENT_RATE`` rate-vs-headcount
distinction needs in particular. Deferred to P2 — the parity
harness still buckets schedule-only ABS rows against TE rows,
which closes the presence gap motivating issue #53.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ABSIndicatorSpec:
    """Downstream-shape metadata for one ABS release-calendar indicator."""

    indicator: str           # canonical token (``"CPI"``)
    country_code: str        # always ``"AU"`` for ABS
    title: str               # cal_econ_event.title
    unit: str                # cal_econ_event.unit (placeholder for schedule-only slice)
    importance: str
    category: str
    url_path_prefix: str     # release-card href prefix that identifies this indicator
    frequency: str           # ``"monthly"`` | ``"quarterly"``


# ABS' published release time is 11:30am Sydney/Melbourne on the
# release day. AEST = UTC+10 (winter), AEDT = UTC+11 (summer); the
# zoneinfo lookup against ``Australia/Sydney`` resolves the offset
# from the release date so the projector lands on a DST-correct UTC
# datetime.
INDICATOR_REGISTRY: dict[str, ABSIndicatorSpec] = {
    "CPI": ABSIndicatorSpec(
        indicator="CPI",
        country_code="AU",
        title="Australia Consumer Price Index",
        unit="index",
        importance="high",
        category="Prices",
        url_path_prefix=(
            "/statistics/economy/price-indexes-and-inflation/"
            "consumer-price-index-australia/"
        ),
        frequency="monthly",
    ),
    "UNEMPLOYMENT_RATE": ABSIndicatorSpec(
        indicator="UNEMPLOYMENT_RATE",
        country_code="AU",
        title="Australia Unemployment Rate",
        unit="percent",
        importance="high",
        category="Labor",
        url_path_prefix=(
            "/statistics/labour/employment-and-unemployment/"
            "labour-force-australia/"
        ),
        frequency="monthly",
    ),
    "GDP": ABSIndicatorSpec(
        indicator="GDP",
        country_code="AU",
        title="Australia GDP",
        unit="aud_millions_chained",
        importance="high",
        category="Growth",
        url_path_prefix=(
            "/statistics/economy/national-accounts/"
            "australian-national-accounts-national-income-expenditure-and-product/"
        ),
        frequency="quarterly",
    ),
}


__all__ = ["INDICATOR_REGISTRY", "ABSIndicatorSpec"]
