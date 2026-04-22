"""BEA series-id → calendar-event metadata whitelist.

Each entry binds an upstream NIPA / ITA coordinate to the downstream
shape the projector writes into ``cal_econ_event``: the canonical
indicator token, country code, display title, unit, and trader-impact
importance.

P2 shipped two anchors — Real GDP (NIPA ``T10101`` L1) and Personal
Income (NIPA ``T20600`` L1). P2b-live adds two:

- **PCE Price Index** — NIPA ``T20804`` L1 (monthly), the headline
  inflation metric published on the Personal Income and Outlays
  release. L1 verified via the 2026-04-22 live probe.
- **Corporate Profits** — NIPA ``T11200`` L13 "Corporate profits
  with IVA and CCAdj" (quarterly), published on the GDP second /
  third estimate releases. L13 verified via the 2026-04-22 probe.

Trade Balance is deferred. The issue-body hint called for the ITA
dataset (``BalGdsServ``); ITA exposes only ``A`` / ``QSA`` / ``QNSA``
frequencies at BEA's REST API and uses a different parameter surface
(``Indicator`` + ``AreaOrCountry``, no ``LineNumber``), so shipping
it needs a client extension. The NIPA near-equivalent ``T10105``
L15 "Net exports of goods and services" is quarterly *annualized*
from the GDP accounting — wrong semantics and scale for the monthly
"U.S. International Trade in Goods and Services" release traders
watch. Deferred to a separate slice that adds ITA support properly.

The shape mirrors :mod:`ingestion.calendar.bls_api.indicators` — each
entry is the minimum metadata needed so the parser can project an
observation without any upstream lookup. Adding a series is a
one-liner; resist the urge to add a series whose payload shape our
live probe hasn't confirmed — mis-mapping silently collapses two
indicators.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BEAIndicatorSpec:
    """Downstream-shape metadata for a single BEA table/line coordinate."""

    series_id: str           # BEA-synthesised id (matches BEAObservation.series_id)
    dataset: str             # BEA dataset name ("NIPA", "ITA", …)
    table: str               # BEA table code ("T10101", "T20600", …)
    line_number: str         # BEA line number within the table ("1", "2", …)
    frequency: str           # BEA frequency code ("Q", "M", "A")
    indicator: str           # canonical token ("GDP", "PCE", …)
    country_code: str        # ISO-3166-1 alpha-2
    title: str               # human-readable, stored in cal_econ_event.title
    unit: str
    importance: str          # low / medium / high
    category: str            # free-text, mirrors TE's Category
    # ``False`` for indicators whose schedule page publishes multiple
    # release stages per reference period (GDP: advance / second /
    # third). Mirrors the BLS-side opt-out (P1c Productivity): until
    # staged schedule/API merge semantics is designed, the API fetch
    # is schedule-only, still callable with explicit ``series_ids=``.
    api_fetch: bool = True


INDICATOR_REGISTRY: dict[str, BEAIndicatorSpec] = {
    # Real GDP — Percent Change From Preceding Period (SAAR). Line 1 of
    # NIPA Table 1.1.1 is the headline real-GDP growth figure the
    # advance / second / third estimate releases all publish.
    # ``api_fetch=False`` mirrors Productivity in P1c: GDP has three
    # staged releases per quarter, which the schedule scraper now
    # tracks stage-qualified — but the single API observation has no
    # clean bare-date anchor alignment with the staged schedule ids.
    # Schedule lane shipped; API lane deferred to a follow-up slice.
    "BEA_NIPA_T10101_1": BEAIndicatorSpec(
        series_id="BEA_NIPA_T10101_1",
        dataset="NIPA",
        table="T10101",
        line_number="1",
        frequency="Q",
        indicator="GDP",
        country_code="US",
        title="Real Gross Domestic Product",
        unit="percent",
        importance="high",
        category="Growth",
        api_fetch=False,
    ),
    # Personal Income — NIPA Table 2.6 (Personal Income and Its
    # Disposition, Monthly) line 1 is the headline aggregate. This is
    # the lead figure of the Personal Income and Outlays release;
    # PCE is reported on the same release but lives at a different
    # coordinate the P2a live probe will confirm.
    "BEA_NIPA_T20600_1": BEAIndicatorSpec(
        series_id="BEA_NIPA_T20600_1",
        dataset="NIPA",
        table="T20600",
        line_number="1",
        frequency="M",
        indicator="PERSONAL_INCOME",
        country_code="US",
        title="Personal Income",
        unit="billions_usd",
        importance="high",
        category="Income",
    ),
    # PCE Price Index — NIPA Table 2.8.4 (Price Indexes for Personal
    # Consumption Expenditures by Major Type of Product, Monthly) line
    # 1 is the headline PCE price-index level that the monthly Personal
    # Income and Outlays release publishes. Market-moving as the Fed's
    # favoured inflation gauge. Verified via 2026-04-22 live probe:
    # line 1 label ``"Personal consumption expenditures (PCE)"``, value
    # 125.417 at 2025-01.
    "BEA_NIPA_T20804_1": BEAIndicatorSpec(
        series_id="BEA_NIPA_T20804_1",
        dataset="NIPA",
        table="T20804",
        line_number="1",
        frequency="M",
        indicator="PCE",
        country_code="US",
        title="PCE Price Index",
        unit="index",
        importance="high",
        category="Prices",
    ),
    # Corporate Profits — NIPA Table 1.12 (National Income by Type of
    # Income, Quarterly) line 13 "Corporate profits with IVA and
    # CCAdj" is the headline quarterly corporate-profits figure the
    # GDP second / third estimate releases publish. Verified via the
    # 2026-04-22 live probe: line 13 label ``"Corporate profits with
    # IVA and CCAdj"``, value 3,922,871 at 2025-Q1.
    "BEA_NIPA_T11200_13": BEAIndicatorSpec(
        series_id="BEA_NIPA_T11200_13",
        dataset="NIPA",
        table="T11200",
        line_number="13",
        frequency="Q",
        indicator="CORPORATE_PROFITS",
        country_code="US",
        title="Corporate Profits",
        unit="millions_usd",
        importance="medium",
        category="Business",
    ),
}
