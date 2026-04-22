"""BLS series-id → calendar-event metadata whitelist.

Each entry binds an upstream series id to the downstream shape the
projector will write into ``cal_econ_event``: the canonical indicator
token, country code, display title, unit, and trader-impact importance.

P1c expands the P1 anchor pair (CPI + NFP) to the full BLS headline
set: Core CPI, PPI, Core PPI, Unemployment Rate, Average Hourly
Earnings, Average Weekly Hours, JOLTS Job Openings, Employment Cost
Index, and Nonfarm Business Labor Productivity.

Every series id here is already in use elsewhere in the repo
(``storage/sqlite.py`` concept-map + ``ingestion/timeseries/_config.py``
time-series seeds) — carrying them across keeps the provider
series-id surface consistent between the time-series lane and the
calendar lane.

Adding a series is a one-liner. Reject the urge to add a series that
our live BLS probe hasn't yet confirmed the payload shape for — the
risk is not wrong data, it's silent mis-canonicalisation into another
indicator's id.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BLSIndicatorSpec:
    """Downstream-shape metadata for a single BLS series."""

    series_id: str
    indicator: str           # canonical token ("CPI", "NFP", …)
    country_code: str        # ISO-3166-1 alpha-2
    title: str               # human-readable, stored in cal_econ_event.title
    unit: str
    importance: str          # low / medium / high
    category: str            # free-text, mirrors TE's Category
    # Include in the default ``fetch_bls_calendar`` iteration. Kept as
    # a switch so a future problem indicator can be quarantined out of
    # the recurring run without dropping it from the registry.
    api_fetch: bool = True
    # ``True`` for indicators whose BLS schedule page publishes more
    # than one release row per reference period (preliminary then
    # revised — Productivity and Costs today). The schedule-side rows
    # synthesise ``provider_event_id`` with a ``|<stage>`` suffix to
    # keep the two versions from colliding in ``cal_econ_event``. The
    # API returns a single bare-date observation per (year, period)
    # representing whichever stage is currently published, so the
    # fetcher rebases each staged observation onto the latest
    # already-released schedule row instead of writing under a
    # bare-date anchor that would orphan.
    staged_schedule: bool = False


INDICATOR_REGISTRY: dict[str, BLSIndicatorSpec] = {
    # ── US inflation ──────────────────────────────────────────────
    "CUUR0000SA0": BLSIndicatorSpec(
        series_id="CUUR0000SA0",
        indicator="CPI",
        country_code="US",
        title="Consumer Price Index",
        unit="index",
        importance="high",
        category="Inflation",
    ),
    "CUUR0000SA0L1E": BLSIndicatorSpec(
        series_id="CUUR0000SA0L1E",
        indicator="Core CPI",
        country_code="US",
        title="Consumer Price Index — All Items Less Food and Energy",
        unit="index",
        importance="high",
        category="Inflation",
    ),
    "WPSFD4": BLSIndicatorSpec(
        series_id="WPSFD4",
        indicator="PPI",
        country_code="US",
        title="Producer Price Index — Final Demand",
        unit="index",
        importance="high",
        category="Inflation",
    ),
    "WPSFD49116": BLSIndicatorSpec(
        series_id="WPSFD49116",
        indicator="Core PPI",
        country_code="US",
        title=(
            "Producer Price Index — Final Demand Less Foods, "
            "Energy, and Trade Services"
        ),
        unit="index",
        importance="high",
        category="Inflation",
    ),
    # ── US employment ────────────────────────────────────────────
    "CES0000000001": BLSIndicatorSpec(
        series_id="CES0000000001",
        indicator="NFP",
        country_code="US",
        title="Nonfarm Payrolls",
        unit="thousands",
        importance="high",
        category="Employment",
    ),
    "LNS14000000": BLSIndicatorSpec(
        series_id="LNS14000000",
        indicator="Unemployment Rate",
        country_code="US",
        title="Unemployment Rate",
        unit="percent",
        importance="high",
        category="Employment",
    ),
    "CES0500000003": BLSIndicatorSpec(
        series_id="CES0500000003",
        indicator="Average Hourly Earnings",
        country_code="US",
        title="Average Hourly Earnings — Total Private",
        unit="usd",
        importance="high",
        category="Employment",
    ),
    "CES0500000002": BLSIndicatorSpec(
        series_id="CES0500000002",
        indicator="Average Weekly Hours",
        country_code="US",
        title="Average Weekly Hours — Total Private",
        unit="hours",
        importance="low",
        category="Employment",
    ),
    "JTS000000000000000JOL": BLSIndicatorSpec(
        series_id="JTS000000000000000JOL",
        indicator="JOLTS",
        country_code="US",
        title="JOLTS Job Openings",
        unit="thousands",
        importance="medium",
        category="Employment",
    ),
    # ECI publishes quarterly. The BLS concept_map uses the
    # 12-month percent change series (``...A`` suffix) — same
    # carried here. The live probe confirms the catalog metadata
    # at first execute.
    "CIU1010000000000A": BLSIndicatorSpec(
        series_id="CIU1010000000000A",
        indicator="Employment Cost Index",
        country_code="US",
        title=(
            "Employment Cost Index — All Civilian Workers, "
            "Total Compensation (12-month % change)"
        ),
        unit="percent",
        importance="medium",
        category="Employment",
    ),
    # ── US productivity ──────────────────────────────────────────
    # Quarterly, with preliminary + revised releases per quarter —
    # ``staged_schedule=True`` drives the fetcher to rebase each API
    # observation onto the latest already-released schedule row so
    # the value lands on a stage-qualified id instead of an orphan
    # bare-date anchor. Schedule scraping unaffected.
    "PRS85006092": BLSIndicatorSpec(
        series_id="PRS85006092",
        indicator="Productivity",
        country_code="US",
        title="Nonfarm Business Sector Labor Productivity",
        unit="index",
        importance="low",
        category="Productivity",
        staged_schedule=True,
    ),
}
