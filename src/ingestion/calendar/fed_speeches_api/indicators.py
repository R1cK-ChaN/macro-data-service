"""Federal Reserve speeches calendar indicator whitelist — issue #56 P1.

P1 ships a single anchor — ``FED_SPEECHES`` — covering every speech
posted to the per-year archive at
``federalreserve.gov/newsevents/speech/<YYYY>-speeches.htm``. The page
only carries Board members + Vice Chairs + Chair (regional Reserve
Bank president speeches live on a different surface), so the page
itself enforces the rate-setter filter the issue asks for.

Schedule-only — speeches don't have a value to fill. The slice
mirrors the BOK / RBI schedule-only shape: each parsed row projects
one calendar event with ``actual=None`` and ``event_time_precision=
'date'`` (the per-year archive lists the calendar day, not a wall-
clock time of delivery).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FedSpeechesIndicatorSpec:
    """Downstream-shape metadata for the Fed speeches calendar indicator."""

    indicator: str           # canonical token ("FED_SPEECHES")
    country_code: str        # ISO-3166 alpha-2 ("US")
    title: str               # display label used in cal_econ_event.title
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, FedSpeechesIndicatorSpec] = {
    "FED_SPEECHES": FedSpeechesIndicatorSpec(
        indicator="FED_SPEECHES",
        country_code="US",
        title="Fed Speech",
        unit="",
        importance="medium",
        category="Central Bank Communication",
    ),
}


__all__ = ["FedSpeechesIndicatorSpec", "INDICATOR_REGISTRY"]
