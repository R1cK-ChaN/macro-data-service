"""BLS series-id → calendar-event metadata whitelist.

Each entry binds an upstream series id to the downstream shape the
projector will write into ``cal_econ_event``: the canonical indicator
token, country code, display title, unit, and trader-impact importance.

P1 ships two anchors — CPI and NFP — the two releases with the highest
trader impact globally. Later phases expand the whitelist via P1a /
P1b slices.

Adding a series is a one-liner. Reject the urge to add a series that
our live BLS probe hasn't yet confirmed the payload shape for — the
risk is not wrong data, it's silent mis-canonicalisation into another
indicator's id.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BLSIndicatorSpec:
    """Downstream-shape metadata for a single BLS series."""

    series_id: str
    indicator: str           # canonical token ("CPI", "NFP", …)
    country_code: str        # ISO-3166-1 alpha-2
    title: str               # human-readable, stored in cal_econ_event.title
    unit: str
    importance: str          # low / medium / high
    category: str            # free-text, mirrors TE's Category


INDICATOR_REGISTRY: dict[str, BLSIndicatorSpec] = {
    "CUUR0000SA0": BLSIndicatorSpec(
        series_id="CUUR0000SA0",
        indicator="CPI",
        country_code="US",
        title="Consumer Price Index",
        unit="index",
        importance="high",
        category="Inflation",
    ),
    "CES0000000001": BLSIndicatorSpec(
        series_id="CES0000000001",
        indicator="NFP",
        country_code="US",
        title="Nonfarm Payrolls",
        unit="thousands",
        importance="high",
        category="Employment",
    ),
}
