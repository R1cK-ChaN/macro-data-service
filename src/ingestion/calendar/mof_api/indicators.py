"""MoF Trade Statistics calendar indicator whitelist (issue #14 P4).

Single high-impact anchor — Balance of Trade, the monthly headline
figure published by the Ministry of Finance on or around the 20th
of the following month. This is TE's highest-count Japan indicator
(151 "high" importance events in TE historical — see issue #14).

The MoF also publishes 10-day and 20-day preliminary trade figures,
but those are industry-watched sub-series and fall below TE's
"high importance" bar; shipping them would duplicate calendar rows
for the same reference month. We project only the Monthly Data
(Provisional) release — the "Balance of Trade" row TE shows.

Shape mirrors :mod:`ingestion.calendar.boj_api.indicators` so the
projector and fetcher code stays polymorphic across Japan-side
connectors.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MofIndicatorSpec:
    """Downstream-shape metadata for a single MoF calendar indicator."""

    indicator: str           # canonical token ("TRADE_BALANCE")
    country_code: str        # ISO-3166-1 alpha-2 ("JP")
    title: str               # human-readable, stored in cal_econ_event.title
    unit: str
    importance: str          # low / medium / high
    category: str            # free-text, mirrors TE's Category


INDICATOR_REGISTRY: dict[str, MofIndicatorSpec] = {
    "TRADE_BALANCE": MofIndicatorSpec(
        indicator="TRADE_BALANCE",
        country_code="JP",
        title="Balance of Trade",
        # MoF publishes the trade balance in millions of yen on
        # the XML feed (``<sashihiki><sogakutonen>``). We keep
        # the raw scale in storage and document the unit — TE
        # displays it as "JPY Billion" (value / 1000), but
        # downstream conversion belongs in the consumer.
        unit="JPY Million",
        importance="high",
        category="Trade",
    ),
}
