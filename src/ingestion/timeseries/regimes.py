"""Regime classifiers for timeseries observations.

Ported from information-layer ``data/macro_data_layer/src/vix_regime.py``
(issue #3 item 5). One series, two thresholds, three labels — deliberately
not a generic operator framework. Add new regimes here as separate small
classifiers when they're needed.

The resulting labels are written to ``obs_enrichment`` with ``key='regime'``
so downstream consumers can filter / group by market-stress state without
recomputing the threshold logic every query.
"""

from __future__ import annotations

import math


# VIX thresholds — see information/data/macro_data_layer/src/vix_regime.py
VIX_LOW_THRESHOLD = 15.0
VIX_STRESSED_THRESHOLD = 25.0


def classify_vix_regime(value: float | None) -> str | None:
    """Classify a VIX close into a regime label.

    Returns ``"low"`` (<15), ``"elevated"`` (15-25), or ``"stressed"``
    (>=25). ``None`` / NaN / inf → ``None`` so missing or malformed
    prints don't get a synthetic label — FRED returns NaN for
    closed-market or blank observations.
    """
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v):
        return None
    if v < VIX_LOW_THRESHOLD:
        return "low"
    if v < VIX_STRESSED_THRESHOLD:
        return "elevated"
    return "stressed"
