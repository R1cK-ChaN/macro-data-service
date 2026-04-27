"""Bank of Korea calendar indicator whitelist — issue #55 P1.

P1 ships a single anchor — ``BOK_RATE`` — the Base Rate set by the
BOK Monetary Policy Board at each bi-monthly meeting. The Meeting
Dates page at ``bok.or.kr/eng/main/contents.do?menuNo=400020``
embeds yearly tables of MPB meeting dates inline. Each meeting closes
on the date listed in its cell — BOK announces the Base Rate
decision at 09:50 KST on that day via the Governor's statement.

The shape mirrors :mod:`ingestion.calendar.rba_api.indicators` /
:mod:`ingestion.calendar.rbi_api.indicators` so projector / fetcher
code stays polymorphic across central-bank connectors. The slice is
**schedule-only**: the new Base Rate value lives inside the per-
meeting Monetary Policy Decision press release; parsing it is
deferred to P2.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BOKIndicatorSpec:
    """Downstream-shape metadata for a single BOK calendar indicator."""

    indicator: str           # canonical token ("BOK_RATE")
    country_code: str        # ISO-3166 alpha-2 ("KR")
    title: str
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, BOKIndicatorSpec] = {
    "BOK_RATE": BOKIndicatorSpec(
        indicator="BOK_RATE",
        country_code="KR",
        title="BOK Interest Rate Decision",
        unit="percent",
        importance="high",
        category="Monetary Policy",
    ),
}


__all__ = ["BOKIndicatorSpec", "INDICATOR_REGISTRY"]
