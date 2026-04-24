"""Bank of Japan calendar indicator whitelist.

Single P1 anchor — ``BOJ_RATE`` (uncollateralized overnight call rate
target decided at each Monetary Policy Meeting). BoJ holds eight
scheduled MPMs per year; the policy-rate guideline announced at the
close of each meeting is the most market-moving Japan macro release
after CPI.

Tankan (quarterly business-sentiment survey) is the second-highest-
impact BoJ calendar indicator and ships in a follow-up slice — its
schedule and value pages live under a different URL branch from the
MPM calendar, so keeping P1 tight means tracking them separately.

Shape mirrors :mod:`ingestion.calendar.fed_api.indicators` so the
projector and fetcher code stays polymorphic across central-bank
connectors.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BojIndicatorSpec:
    """Downstream-shape metadata for a single BoJ calendar indicator."""

    indicator: str           # canonical token ("BOJ_RATE")
    country_code: str        # ISO-3166-1 alpha-2 ("JP")
    title: str               # human-readable, stored in cal_econ_event.title
    unit: str
    importance: str          # low / medium / high
    category: str            # free-text, mirrors TE's Category


INDICATOR_REGISTRY: dict[str, BojIndicatorSpec] = {
    "BOJ_RATE": BojIndicatorSpec(
        indicator="BOJ_RATE",
        country_code="JP",
        title="BoJ Interest Rate Decision",
        unit="percent",
        importance="high",
        category="Monetary Policy",
    ),
}
