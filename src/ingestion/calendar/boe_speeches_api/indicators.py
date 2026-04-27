"""Bank of England speeches calendar indicator whitelist — issue #56 P1.

P1 ships a single anchor — ``BOE_SPEECHES`` — covering BoE speeches
listed on ``bankofengland.co.uk/sitemap/speeches``. The sitemap is
the authoritative public index of every published speech and is
fetched in one request. Country code is ``UK`` to match the existing
ONS / BoE Bank-Rate connectors.

Schedule-only — speeches don't have a value to fill. The slice
mirrors the BOK / RBI shape: each parsed row projects one calendar
event with ``actual=None`` and ``event_time_precision='date'``.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BoeSpeechesIndicatorSpec:
    """Downstream-shape metadata for the BoE speeches calendar indicator."""

    indicator: str
    country_code: str
    title: str
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, BoeSpeechesIndicatorSpec] = {
    "BOE_SPEECHES": BoeSpeechesIndicatorSpec(
        indicator="BOE_SPEECHES",
        country_code="UK",
        title="BoE Speech",
        unit="",
        importance="medium",
        category="Central Bank Communication",
    ),
}


__all__ = ["BoeSpeechesIndicatorSpec", "INDICATOR_REGISTRY"]
