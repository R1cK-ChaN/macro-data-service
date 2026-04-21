"""Federal Reserve calendar indicator whitelist.

Each entry binds a canonical indicator token to the downstream shape the
projector writes into ``cal_econ_event``: country, display title, unit,
and trader-impact importance.

P4 ships a **single anchor** — ``FOMC_RATE`` (Federal Open Market
Committee rate decision). Every FOMC meeting produces one rate-decision
event; the Governing Council announces the target range at 14:00 ET on
the final day of the meeting, which is the single most traded US
macro release after CPI / NFP.

Beige Book, H.4.1 weekly balance sheet, H.8, Summary of Economic
Projections (the quarterly dot-plot subset of FOMC), and scheduled
Fed Chair / Vice-Chair speeches are deferred to P4a — they come from
a different upstream page (``federalreserve.gov/newsevents/releasedates.htm``)
with a different DOM, so splitting keeps review surface tight.

The shape mirrors :mod:`ingestion.calendar.bls_api.indicators`,
:mod:`ingestion.calendar.bea_api.indicators`, and
:mod:`ingestion.calendar.ecb_api.indicators` so projector/fetcher
code stays polymorphic across connectors.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FedIndicatorSpec:
    """Downstream-shape metadata for a single Fed calendar indicator."""

    indicator: str           # canonical token ("FOMC_RATE")
    country_code: str        # ISO-3166-1 alpha-2 ("US")
    title: str               # human-readable, stored in cal_econ_event.title
    unit: str
    importance: str          # low / medium / high
    category: str            # free-text, mirrors TE's Category


# Single anchor — the FOMC rate decision. The scraper (see ``scraper.py``)
# parses ``federalreserve.gov/monetarypolicy/fomccalendars.htm`` into a
# list of scheduled meetings; each meeting hits this indicator exactly
# once at 14:00 ET on the meeting's closing day.
INDICATOR_REGISTRY: dict[str, FedIndicatorSpec] = {
    "FOMC_RATE": FedIndicatorSpec(
        indicator="FOMC_RATE",
        country_code="US",
        title="FOMC Rate Decision",
        unit="percent",
        importance="high",
        category="Monetary Policy",
    ),
}
