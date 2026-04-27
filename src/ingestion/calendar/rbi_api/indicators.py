"""Reserve Bank of India calendar indicator whitelist — issue #54 P1.

P1 ships a single anchor — ``RBI_RATE`` — the policy repo rate set by
the RBI Monetary Policy Committee at each bi-monthly meeting. The
``annualpolicy.aspx`` page at ``rbi.org.in/scripts/annualpolicy.aspx``
embeds the "Meeting Schedule of the Monetary Policy Committee" press
release inline; the schedule lists six meeting date triples per
fiscal year. Each meeting closes on the third date — RBI announces
the rate decision at 10:00 IST on that closing day via the Governor's
Statement.

The shape mirrors :mod:`ingestion.calendar.rba_api.indicators` so
projector / fetcher code stays polymorphic across central-bank
connectors. The slice is **schedule-only**: the per-meeting Resolution
press release carries the new repo rate value as
``"policy repo rate at X.YZ percent"`` text, but parsing each PRID
page is deferred to P2 alongside the value-side scrape.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RBIIndicatorSpec:
    """Downstream-shape metadata for a single RBI calendar indicator."""

    indicator: str           # canonical token ("RBI_RATE")
    country_code: str        # ISO-3166 alpha-2 ("IN")
    title: str
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, RBIIndicatorSpec] = {
    "RBI_RATE": RBIIndicatorSpec(
        indicator="RBI_RATE",
        country_code="IN",
        title="RBI Interest Rate Decision",
        unit="percent",
        importance="high",
        category="Monetary Policy",
    ),
}


__all__ = ["RBIIndicatorSpec", "INDICATOR_REGISTRY"]
