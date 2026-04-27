"""ECB speeches calendar indicator whitelist — issue #56 P1.

P1 ships a single anchor — ``ECB_SPEECHES`` — covering every speech
published in the official ECB CSV at
``ecb.europa.eu/press/key/shared/data/all_ECB_speeches.csv``. The
CSV exposes only Executive Board members + Governing Council
speeches; per the ECB downloads page documentation: "Speakers are
ECB Executive Board members only", which already enforces the rate-
setter filter the issue asks for.

Schedule-only — speeches don't have a value to fill. The slice
mirrors the BOK / RBI shape: each parsed row projects one calendar
event with ``actual=None`` and ``event_time_precision='date'`` (the
CSV publishes the calendar day, not a wall-clock time).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EcbSpeechesIndicatorSpec:
    """Downstream-shape metadata for the ECB speeches calendar indicator."""

    indicator: str           # canonical token ("ECB_SPEECHES")
    country_code: str        # ECB / Eurostat convention is "EU"
    title: str
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, EcbSpeechesIndicatorSpec] = {
    "ECB_SPEECHES": EcbSpeechesIndicatorSpec(
        indicator="ECB_SPEECHES",
        country_code="EU",
        title="ECB Speech",
        unit="",
        importance="medium",
        category="Central Bank Communication",
    ),
}


__all__ = ["EcbSpeechesIndicatorSpec", "INDICATOR_REGISTRY"]
