"""Bank of Japan Tankan calendar indicator whitelist (issue #14 P1a).

Two anchors on the Large-Enterprises Business-Conditions Diffusion
Index published quarterly at
``boj.or.jp/en/statistics/tk/yoshi/tk<YYMM>.htm``:

- ``TANKAN_LARGE_MFG`` — Large Enterprises, Manufacturing. The single
  highest-impact Japan business-sentiment headline (52 "high"
  importance events in TE historical — see issue #14).
- ``TANKAN_LARGE_NONMFG`` — Large Enterprises, Non-Manufacturing.
  Ships alongside on the same page; same release clock, same
  authorship.

Medium and small-enterprise sub-indices sit in the same page but
fall below TE's "high importance" bar and are left out of the
calendar lane deliberately — they stay available to downstream via
the "comprehensive data set" feed when needed.

Shape mirrors :mod:`ingestion.calendar.boj_api.indicators` so the
projector and fetcher code stays polymorphic across BoJ surfaces.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BojTankanIndicatorSpec:
    """Downstream-shape metadata for a single Tankan calendar indicator."""

    indicator: str           # canonical token ("TANKAN_LARGE_MFG")
    sector: str              # "manufacturing" / "nonmanufacturing"
    country_code: str        # ISO-3166-1 alpha-2 ("JP")
    title: str               # human-readable, stored in cal_econ_event.title
    unit: str
    importance: str          # low / medium / high
    category: str            # free-text, mirrors TE's Category


INDICATOR_REGISTRY: dict[str, BojTankanIndicatorSpec] = {
    "TANKAN_LARGE_MFG": BojTankanIndicatorSpec(
        indicator="TANKAN_LARGE_MFG",
        sector="manufacturing",
        country_code="JP",
        title="Tankan Large Manufacturers Index",
        unit="points",
        importance="high",
        category="Business Survey",
    ),
    "TANKAN_LARGE_NONMFG": BojTankanIndicatorSpec(
        indicator="TANKAN_LARGE_NONMFG",
        sector="nonmanufacturing",
        country_code="JP",
        title="Tankan Large Non-Manufacturers Index",
        unit="points",
        importance="high",
        category="Business Survey",
    ),
}
