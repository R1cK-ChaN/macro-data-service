"""Bank of Japan speeches calendar indicator whitelist — issue #56 P1.

P1 ships a single anchor — ``BOJ_SPEECHES`` — covering Policy Board
speeches listed on the per-year archive at
``boj.or.jp/en/about/press/koen_<YYYY>/index.htm``. The page lists
every public speech across all ranks (Governor / Deputy Governor /
Policy Board members / Executive Director / Executive Officer); the
parser filters to rate-setting roles only (Governor + Deputy
Governor + Policy Board) per the issue's "rate-setters only" scope.

Schedule-only — speeches don't have a value to fill. Mirrors the
BOK / RBI shape: each parsed row projects one calendar event with
``actual=None`` and ``event_time_precision='date'``.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BojSpeechesIndicatorSpec:
    """Downstream-shape metadata for the BoJ speeches calendar indicator."""

    indicator: str
    country_code: str
    title: str
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, BojSpeechesIndicatorSpec] = {
    "BOJ_SPEECHES": BojSpeechesIndicatorSpec(
        indicator="BOJ_SPEECHES",
        country_code="JP",
        title="BoJ Speech",
        unit="",
        importance="medium",
        category="Central Bank Communication",
    ),
}


__all__ = ["BojSpeechesIndicatorSpec", "INDICATOR_REGISTRY"]
