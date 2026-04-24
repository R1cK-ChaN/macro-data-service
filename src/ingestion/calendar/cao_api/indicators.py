"""Cabinet Office (ESRI) calendar indicator whitelist (issue #14 P3).

Single anchor — the Consumer Confidence Index ("消費動向調査 /
Consumer Confidence Survey"), ESRI's highest-count Japan indicator
after BoT (142 "high" importance events in TE historical — see
issue #14 TBD-Repro resolution).

ESRI also publishes Machinery Orders, the Business Outlook Survey,
and the Indexes of Business Conditions on the same schedule page;
those ride separate canonical tokens and land in follow-up slices
(P3a — GDP, follow-on phases for the remaining surveys).

Release shape: monthly survey fieldwork conducted mid-month, results
published late-same-month or early-next-month at 14:00 JST.

The ``indicator`` string on this spec canonicalizes through the
shared alias table to ``CB_CONSUMER_CONFIDENCE`` — the pre-existing
canonical introduced for the US Conference Board connector. Country
disambiguation lives in ``provider_event_id`` (same pattern that
MoF's Balance of Trade reuses the BEA ``TRADE_BALANCE`` canonical),
so the ``CB_`` prefix is a naming wart rather than a collision.

Shape mirrors :mod:`ingestion.calendar.mof_api.indicators` so the
projector and fetcher stay polymorphic across Japan-side connectors.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CaoIndicatorSpec:
    """Downstream-shape metadata for a single CAO calendar indicator."""

    indicator: str           # canonicalizes to a token (``CB_CONSUMER_CONFIDENCE``)
    country_code: str        # ISO-3166-1 alpha-2 (``JP``)
    title: str               # human-readable, stored in ``cal_econ_event.title``
    unit: str
    importance: str          # low / medium / high
    category: str            # free-text, mirrors TE's Category


INDICATOR_REGISTRY: dict[str, CaoIndicatorSpec] = {
    # Dict key is an internal handle — the canonical token used in
    # ``provider_event_id`` is the output of
    # ``canonicalize_indicator(spec.indicator)``, which for this row
    # resolves to ``CB_CONSUMER_CONFIDENCE`` via the shared alias
    # table.
    "CONSUMER_CONFIDENCE": CaoIndicatorSpec(
        indicator="Consumer Confidence",
        country_code="JP",
        title="Consumer Confidence",
        # The Consumer Confidence Index is a 0–100 diffusion-style
        # points index (survey-respondent sentiment, 50 = neutral).
        # Match Tankan's DI shape — ``unit="points"``, no currency.
        unit="points",
        importance="high",
        category="Consumer",
    ),
}
