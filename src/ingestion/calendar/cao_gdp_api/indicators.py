"""Cabinet Office / ESRI GDP calendar indicator whitelist.

P3a ships Japan's quarterly real GDP QoQ headline from the ESRI SNA
archive. ESRI publishes two market-facing stages for each reference
quarter:

- first preliminary estimate
- second preliminary estimate

Both stages share the same canonical indicator family (``GDP``), and
the stage is folded into ``provider_event_id`` through the anchor
``reference_date|stage`` so the two release rows stay distinct.
"""

from __future__ import annotations

from dataclasses import dataclass


FIRST_PRELIMINARY = "first_preliminary"
SECOND_PRELIMINARY = "second_preliminary"


@dataclass(frozen=True)
class CaoGdpIndicatorSpec:
    """Downstream-shape metadata for one CAO GDP release stage."""

    indicator: str
    release_stage: str
    country_code: str
    title: str
    unit: str
    importance: str
    category: str


INDICATOR_REGISTRY: dict[str, CaoGdpIndicatorSpec] = {
    "GDP_QOQ_FIRST_PRELIMINARY": CaoGdpIndicatorSpec(
        indicator="GDP Growth Rate QoQ Prel",
        release_stage=FIRST_PRELIMINARY,
        country_code="JP",
        title="GDP Growth Rate QoQ Prel",
        unit="percent",
        importance="high",
        category="Growth",
    ),
    "GDP_QOQ_SECOND_PRELIMINARY": CaoGdpIndicatorSpec(
        indicator="GDP Growth Rate QoQ Final",
        release_stage=SECOND_PRELIMINARY,
        country_code="JP",
        title="GDP Growth Rate QoQ Final",
        unit="percent",
        importance="high",
        category="Growth",
    ),
}

SPEC_BY_STAGE: dict[str, CaoGdpIndicatorSpec] = {
    spec.release_stage: spec for spec in INDICATOR_REGISTRY.values()
}
