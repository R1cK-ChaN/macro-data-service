"""DOL Unemployment Insurance Weekly Claims indicator whitelist (issue #50).

Two indicators ship in P1 — the headline weekly UI Claims figures
the Department of Labor's Employment and Training Administration
publishes every Thursday at 8:30 AM Eastern:

- **INITIAL_CLAIMS** — Advance figure for seasonally-adjusted
  initial claims for the most-recent week ending. The press
  release's table line ``"Initial Claims (SA)  214,000  208,000
  …"`` carries the headline number traders watch.
- **CONTINUING_CLAIMS** — Advance number for seasonally-adjusted
  insured unemployment for the prior week (one week lag vs. the
  initial figure). Table line ``"Insured Unemployment (SA)  …"``.

Both indicators publish on the same release date but reference
different week-ending dates — Initial Claims for the
release-week-minus-5-days Saturday, Continuing Claims for the
prior Saturday.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DOLIndicatorSpec:
    """Downstream-shape metadata for a DOL UI Claims indicator."""

    indicator: str           # canonical token ("INITIAL_CLAIMS")
    country_code: str        # always "US"
    title: str               # cal_econ_event.title
    unit: str                # cal_econ_event.unit ("count")
    importance: str
    category: str
    # Number of days from the release date back to the indicator's
    # week-ending reference Saturday. Initial Claims publishes for
    # the week that ended the Saturday immediately before the
    # Thursday release (5 days back); Continuing Claims publishes
    # for the prior Saturday (12 days back). This is what TE uses
    # to bucket the rows; getting it right is what makes the parity
    # comparator match.
    reference_days_back: int


INDICATOR_REGISTRY: dict[str, DOLIndicatorSpec] = {
    "INITIAL_CLAIMS": DOLIndicatorSpec(
        indicator="INITIAL_CLAIMS",
        country_code="US",
        title="US Initial Jobless Claims",
        unit="count",
        importance="high",
        category="Labor",
        reference_days_back=5,   # Thu release → Saturday 5 days back
    ),
    "CONTINUING_CLAIMS": DOLIndicatorSpec(
        indicator="CONTINUING_CLAIMS",
        country_code="US",
        title="US Continuing Jobless Claims",
        unit="count",
        importance="high",
        category="Labor",
        reference_days_back=12,  # Continuing lags Initial by one week
    ),
}


__all__ = ["DOLIndicatorSpec", "INDICATOR_REGISTRY"]
