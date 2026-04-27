"""Agency attribution map for the daily TE parity tripwire (issue #22 P2).

Each :class:`AgencyDecl` binds:

* ``agency_id`` — short token used as the GitHub label suffix
  (``agency:BLS``).
* ``providers`` — the ``cal_econ_event.provider`` ids written by the
  connector(s) this agency owns. Joint agencies (Cabinet Office of
  Japan owns both ``cao`` and ``cao-gdp`` connectors) carry several
  ids.
* ``indicators`` — ``(country, canonical_indicator)`` pairs the agency
  is the official source for. Canonical tokens come from
  :func:`ingestion.calendar._official_shared.canonicalize_indicator`
  applied to the connector's release titles. **Only value-comparable
  indicators are listed** — pairs where TE and the connector publish
  the same measure (rate vs rate, index vs index). Indicators where
  the connector stores an index level while TE publishes a YoY rate
  (BLS CPI / NFP / Core CPI / PPI etc.) are omitted from the
  whitelist; the parity tripwire is value-equality, not derivation.
  Country codes match the value the upstream connector writes into
  ``cal_econ_event.country_code`` — euro-area uses ``EU`` (not
  ``EA``), aligned with the TE country map and the ECB / Eurostat /
  EC-BCS connector outputs.
* ``alignment`` — per-canonical TE-row normalisation rule. ``None``
  means TE and the agency publish the indicator in the same units and
  scale; the comparator strips trailing whitespace, ``%``, ``,`` and
  trailing zeros and then enforces exact Decimal equality. An entry
  declares a multiplicative scale factor and / or a unit-suffix to
  strip beyond the defaults.

The reverse map is built lazily by :func:`agency_for` /
:func:`provider_to_agency`. Constructing the indices at module import is
cheap (one ``dict`` walk over a fixed ~30-row table) and keeps the
parity comparator pure — every lookup is by ``(country, canonical)``,
no live introspection.

This module is the single source of truth on agency ownership; the per-
connector ``__init__.py`` files deliberately do not re-export the same
constants. Adding a new connector means appending one row here, not
editing 27 packages.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class AlignmentSpec:
    """Per-(country, canonical) TE-row normalisation rule.

    ``scale`` is multiplied into the TE value before comparison. Use it
    when TE publishes the indicator in different units than the
    official source — e.g. TE quotes Trade Balance in millions while
    Census quotes the same series in billions, so the agency-side rule
    declares ``scale=Decimal("0.001")`` to bring TE into Census units.

    ``strip_suffixes`` are removed from the TE actual string before
    parsing (in addition to the comparator's defaults: trailing ``%``,
    whitespace, commas). Useful for one-off TE quirks like a trailing
    ``"K"``/``"M"``/``"B"`` magnitude marker — list them explicitly so
    the comparator never silently mis-parses.
    """

    scale: str = "1"
    strip_suffixes: tuple[str, ...] = ()


@dataclass(frozen=True)
class AgencyDecl:
    agency_id: str
    label: str
    providers: tuple[str, ...]
    indicators: frozenset[tuple[str, str]]
    alignment: dict[tuple[str, str], AlignmentSpec] = field(
        default_factory=dict,
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

AGENCIES: tuple[AgencyDecl, ...] = (
    AgencyDecl(
        agency_id="DOL",
        label="US Department of Labor — Employment and Training Administration",
        # Provider attribution only — the parity whitelist is
        # deliberately empty in this slice. DOL writes raw counts
        # (``"214000"``) while TE renders the same figures with a
        # trailing "K" magnitude marker (``"214K"``); aligning the
        # two needs an :class:`AlignmentSpec` that strips ``"K"`` and
        # scales by 1000 before equality. The parity comparator
        # also keys buckets on ``canonicalize_indicator(title)``,
        # which today returns ``"JOBLESS_CLAIMS"`` for TE's
        # "Initial Jobless Claims" but ``"us initial jobless
        # claims"`` (lowercase fall-through) for the DOL-side
        # title — adding the canonicalize aliases is part of the
        # same follow-up. Once both are in, the indicator set
        # extends to the matched TE / DOL bucket keys.
        providers=("dol",),
        indicators=frozenset(),
    ),
    AgencyDecl(
        agency_id="EIA",
        label="US Energy Information Administration",
        # EIA weekly stocks publish raw counts in thousand-barrels
        # (petroleum) or billion-cubic-feet (natural gas). TE
        # publishes the same headline figures with a "M" / "B"
        # magnitude marker. Stay out of the parity whitelist until
        # alignment specs cover the suffix — the connector still
        # writes ``actual`` and the agency declaration still owns
        # the provider attribution.
        providers=("eia",),
        indicators=frozenset(),
    ),
    AgencyDecl(
        agency_id="BLS",
        label="US Bureau of Labor Statistics",
        # BLS stores raw observation values: CPI / Core CPI / PPI / Core PPI
        # are index levels, NFP is the total-employment level in thousands,
        # AHE is dollars/hour, AWH is hours/week, JOLTS is thousands of jobs,
        # Productivity is index. TE publishes YoY / MoM rates or monthly
        # changes for the same labels, so direct equality would always
        # mismatch. Only Unemployment Rate and ECI are stored as the same
        # percent figure TE prints; the rest stay out of the parity
        # whitelist until a derivation layer lands.
        providers=("bls",),
        indicators=frozenset({
            ("US", "UNEMPLOYMENT_RATE"),
            ("US", "ECI"),
        }),
    ),
    AgencyDecl(
        agency_id="BEA",
        label="US Bureau of Economic Analysis",
        # GDP is the only BEA value the connector ships as a percent
        # change (annualized QoQ) directly comparable to TE's
        # "GDP Growth Rate". PCE writes the index level, Personal
        # Income writes a level in billions, Corporate Profits writes
        # millions of USD — none directly comparable.
        providers=("bea",),
        indicators=frozenset({
            ("US", "GDP"),
        }),
    ),
    AgencyDecl(
        agency_id="CENSUS",
        label="US Census Bureau",
        # Retail Sales MoM and Durable Goods Orders MoM ship as percent
        # changes that align with TE's same-named labels. Housing Starts
        # / Building Permits store thousands SAAR, which TE renders with
        # an "M" / "K" magnitude marker; until alignment specs cover
        # those suffixes they stay out. Trade Balance is BLS / ITA
        # territory and Census doesn't currently emit a
        # ``TRADE_BALANCE`` actual.
        providers=("census",),
        indicators=frozenset({
            ("US", "RETAIL_SALES"),
            ("US", "DURABLE_GOODS_ORDERS"),
        }),
    ),
    AgencyDecl(
        agency_id="ISM",
        label="Institute for Supply Management",
        providers=("ism",),
        indicators=frozenset({
            ("US", "MFG_PMI"),
        }),
    ),
    AgencyDecl(
        agency_id="UMICH",
        label="University of Michigan Surveys of Consumers",
        providers=("umich",),
        indicators=frozenset({
            ("US", "MICHIGAN_SENTIMENT"),
        }),
    ),
    AgencyDecl(
        agency_id="CONFERENCE_BOARD",
        label="The Conference Board",
        providers=("conference-board",),
        indicators=frozenset({
            ("US", "CB_CONSUMER_CONFIDENCE"),
            ("US", "CB_LEADING_INDEX"),
        }),
    ),
    AgencyDecl(
        agency_id="NAR",
        label="National Association of Realtors",
        # Pending Home Sales MoM is a percent — directly comparable.
        # Existing Home Sales ships as million-SAAR and TE typically
        # appends an "M" suffix; deferred until alignment spec covers
        # the suffix.
        providers=("nar",),
        indicators=frozenset({
            ("US", "PENDING_HOME_SALES"),
        }),
    ),
    AgencyDecl(
        agency_id="FED",
        label="US Federal Reserve",
        # FOMC_RATE ships as a percent that matches TE's
        # "Fed Interest Rate Decision" actual. Beige Book / H.4.1 /
        # H.8 / SEP are calendar-events-without-numeric-values; the
        # comparator already skips ``actual IS NULL`` rows, so listing
        # them buys nothing.
        providers=("federal-reserve",),
        indicators=frozenset({
            ("US", "FOMC_RATE"),
        }),
    ),
    AgencyDecl(
        agency_id="ECB",
        label="European Central Bank",
        # ECB connectors persist ``country_code='EU'`` (the TE map
        # also points "Euro Area" → "EU"). The three policy rates are
        # the only value-bearing ECB indicators today; the bulletin /
        # press conference / monetary-policy-decision rows are events.
        providers=("ecb",),
        indicators=frozenset({
            ("EU", "ECB_MRO"),
            ("EU", "ECB_DFR"),
            ("EU", "ECB_MLF"),
        }),
    ),
    AgencyDecl(
        agency_id="EUROSTAT",
        label="Eurostat",
        providers=("eurostat",),
        indicators=frozenset({
            ("EU", "CPI"),
            ("EU", "GDP"),
            ("EU", "UNEMPLOYMENT_RATE"),
        }),
    ),
    AgencyDecl(
        agency_id="DESTATIS",
        label="German Federal Statistical Office",
        providers=("destatis",),
        indicators=frozenset({
            ("DE", "CPI"),
            ("DE", "GDP"),
        }),
    ),
    AgencyDecl(
        agency_id="ZEW",
        label="ZEW – Leibniz Centre for European Economic Research",
        providers=("zew",),
        indicators=frozenset({
            ("DE", "ZEW_SENTIMENT"),
        }),
    ),
    AgencyDecl(
        agency_id="IFO",
        label="ifo Institute",
        providers=("ifo",),
        indicators=frozenset({
            ("DE", "IFO_BUSINESS_CLIMATE"),
        }),
    ),
    AgencyDecl(
        agency_id="GFK",
        label="GfK / NIM Consumer Climate",
        providers=("gfk",),
        indicators=frozenset({
            ("DE", "GFK_CONSUMER_CLIMATE"),
        }),
    ),
    AgencyDecl(
        agency_id="HCOB",
        label="Hamburg Commercial Bank PMI",
        providers=("hcob",),
        indicators=frozenset({
            ("DE", "HCOB_FLASH_MANUFACTURING_PMI"),
            ("DE", "HCOB_FLASH_SERVICES_PMI"),
            ("DE", "HCOB_FLASH_COMPOSITE_PMI"),
            ("DE", "HCOB_MANUFACTURING_PMI"),
            ("DE", "HCOB_SERVICES_PMI"),
        }),
    ),
    AgencyDecl(
        agency_id="EC_BCS",
        label="European Commission Business and Consumer Surveys",
        providers=("ec-bcs",),
        indicators=frozenset({
            ("EU", "EC_BCS_ESI"),
            ("EU", "EC_BCS_CCI_FLASH"),
        }),
    ),
    AgencyDecl(
        agency_id="INSEE",
        label="French National Institute of Statistics",
        providers=("insee",),
        indicators=frozenset({
            ("FR", "CPI"),
            ("FR", "GDP"),
        }),
    ),
    AgencyDecl(
        agency_id="INE",
        label="Spanish National Statistics Institute",
        providers=("ine",),
        indicators=frozenset({
            ("ES", "CPI"),
            ("ES", "GDP"),
        }),
    ),
    AgencyDecl(
        agency_id="ISTAT",
        label="Italian National Institute of Statistics",
        providers=("istat",),
        indicators=frozenset({
            ("IT", "CPI"),
            ("IT", "GDP"),
        }),
    ),
    AgencyDecl(
        agency_id="MOF_JP",
        label="Japan Ministry of Finance",
        providers=("mof-jp",),
        indicators=frozenset({
            ("JP", "TRADE_BALANCE"),
        }),
    ),
    AgencyDecl(
        agency_id="CAO_JP",
        label="Japan Cabinet Office / ESRI",
        providers=("cao",),
        indicators=frozenset({
            ("JP", "CB_CONSUMER_CONFIDENCE"),
            ("JP", "GDP"),
        }),
    ),
    AgencyDecl(
        agency_id="METI",
        label="Japan Ministry of Economy, Trade and Industry",
        providers=("meti",),
        indicators=frozenset({
            ("JP", "INDUSTRIAL_PRODUCTION"),
            ("JP", "RETAIL_SALES"),
        }),
    ),
    AgencyDecl(
        agency_id="STAT_BUREAU_JP",
        label="Japan Statistics Bureau",
        providers=("stat-bureau-jp",),
        indicators=frozenset({
            ("JP", "CORE_CPI"),
            ("JP", "UNEMPLOYMENT_RATE"),
        }),
    ),
    AgencyDecl(
        agency_id="BOJ",
        label="Bank of Japan",
        providers=("boj",),
        indicators=frozenset({
            ("JP", "BOJ_RATE"),
            ("JP", "TANKAN_LARGE_MFG"),
            ("JP", "TANKAN_LARGE_NONMFG"),
        }),
    ),
    AgencyDecl(
        agency_id="ONS",
        label="UK Office for National Statistics",
        # CPI / Unemployment Rate / GDP all ship as percent values
        # directly comparable to TE's UK rows ("United Kingdom
        # Inflation Rate", "United Kingdom Unemployment Rate", "United
        # Kingdom GDP Growth Rate"). Country code is "UK" per the
        # storage-layer alias map (storage/sqlite.py: GB → UK).
        providers=("ons",),
        indicators=frozenset({
            ("UK", "CPI"),
            ("UK", "UNEMPLOYMENT_RATE"),
            ("UK", "GDP"),
        }),
    ),
    AgencyDecl(
        agency_id="BOE",
        label="Bank of England",
        # BOE_RATE ships as the percent value (3.75) directly
        # comparable to TE's "BoE Interest Rate Decision" actual.
        providers=("boe",),
        indicators=frozenset({
            ("UK", "BOE_RATE"),
        }),
    ),
    AgencyDecl(
        agency_id="STATCAN",
        label="Statistics Canada",
        # CPI ships as an index level (2002=100), GDP ships as
        # millions-CAD SAAR; neither is directly comparable to TE's
        # "Canada Inflation Rate" (YoY %) or "Canada GDP Growth Rate"
        # (QoQ %) without a derivation step. Only Unemployment Rate
        # is value-comparable today, mirroring the BLS pattern. A
        # follow-up slice can derive the YoY/QoQ rates and extend the
        # whitelist.
        providers=("statcan",),
        indicators=frozenset({
            ("CA", "UNEMPLOYMENT_RATE"),
        }),
    ),
    AgencyDecl(
        agency_id="BOC",
        label="Bank of Canada",
        # Provider attribution only — the parity whitelist stays empty
        # until the hold-decision source lands. The Valet ``V39079``
        # signal only surfaces *change* days; TE publishes a
        # "BoC Interest Rate Decision" row for every scheduled
        # announcement (~8 per year) including holds. Wiring
        # ``("CA", "BOC_RATE")`` into the whitelist before the BoC
        # fixed-announcement-dates page is ingested would flag every
        # hold day as a missing-release anomaly. Same deferral
        # pattern as the BoE Bank-Rate page (changes-only, hold
        # days stay outside this connector).
        providers=("boc",),
        indicators=frozenset(),
    ),
    AgencyDecl(
        agency_id="ABS",
        label="Australian Bureau of Statistics",
        # Provider attribution only — the P1 ABS slice ships
        # schedule-only events (``actual=NULL``) so wiring
        # ``("AU", CPI / UNEMPLOYMENT_RATE / GDP)`` into the parity
        # whitelist would trip the daily comparator's
        # parse_failed-on-missing-actual path on every release.
        # The next slice (P2) adds the ABS SDMX value lookup at
        # ``api.data.abs.gov.au``, which fills ``actual`` and lets
        # us extend the whitelist for the percent-typed series
        # (Unemployment Rate; CPI / GDP need YoY / QoQ derivation
        # like the StatCan / BLS pattern).
        providers=("abs",),
        indicators=frozenset(),
    ),
    AgencyDecl(
        agency_id="RBA",
        label="Reserve Bank of Australia",
        # RBA_RATE ships as the percent value (4.10) directly
        # comparable to TE's "RBA Interest Rate Decision" actual.
        # Unlike the BoC pattern, the RBA cash-rate page publishes
        # hold decisions in the same row format as moves, so the
        # parity whitelist is achievable in P1 without a separate
        # fixed-announcement-dates feed.
        providers=("rba",),
        indicators=frozenset({
            ("AU", "RBA_RATE"),
        }),
    ),
    AgencyDecl(
        agency_id="MOSPI",
        label="India Ministry of Statistics and Programme Implementation",
        # Provider attribution only — the P1 MoSPI slice ships
        # schedule-only events (``actual=NULL``) for CPI / IIP / GDP.
        # Wiring (IN, CPI / GDP / INDUSTRIAL_PRODUCTION) into the parity
        # whitelist would trip the parse_failed-on-missing-actual path
        # on every release. Same deferral as the ABS slice — the next
        # iteration parses the per-release PDF that the JSON API
        # surfaces in ``description`` to fill ``actual``.
        providers=("mospi",),
        indicators=frozenset(),
    ),
    AgencyDecl(
        agency_id="RBI",
        label="Reserve Bank of India",
        # Provider attribution only — the P1 RBI slice projects each
        # MPC meeting as a schedule-only event (``actual=NULL``). The
        # value (new repo rate) lives inside the per-meeting Resolution
        # press release; the value-side scrape is deferred to P2 to
        # match the ABS/BoC schedule-only deferral pattern. Once the
        # Resolution scrape lands, ``("IN", "RBI_RATE")`` joins the
        # whitelist.
        providers=("rbi",),
        indicators=frozenset(),
    ),
    AgencyDecl(
        agency_id="KOSTAT",
        label="Statistics Korea",
        # Provider attribution only — the P1 KOSTAT slice ships
        # schedule-only events (``actual=NULL``) for CPI / Industrial
        # Production / Unemployment Rate. Wiring (KR, CPI / GDP /
        # INDUSTRIAL_PRODUCTION / UNEMPLOYMENT_RATE) into the parity
        # whitelist would trip the parse_failed-on-missing-actual path
        # on every release. Same deferral as the ABS/MoSPI slice — P2
        # adds the per-release press-release scrape to fill ``actual``.
        providers=("kostat",),
        indicators=frozenset(),
    ),
    AgencyDecl(
        agency_id="FED_SPEECHES",
        label="US Federal Reserve — Board speeches",
        # Provider attribution only — the P1 Fed-speeches slice ships
        # schedule-only events (``actual=NULL``) anchoring each Board
        # / Vice Chair / Chair speech as a calendar event. There's no
        # numeric value to align against TE; the connector serves as
        # an event anchor for downstream research / impact analysis.
        # Same shape as the BOK / RBI / KOSTAT schedule-only deferral.
        providers=("fed-speeches",),
        indicators=frozenset(),
    ),
    AgencyDecl(
        agency_id="ECB_SPEECHES",
        label="European Central Bank — Executive Board speeches",
        # Provider attribution only — same schedule-only shape as
        # FED_SPEECHES. The ECB CSV's ``contents`` column carries
        # the full transcript text but is preserved in the raw row's
        # payload and not projected to ``cal_econ_event``.
        providers=("ecb-speeches",),
        indicators=frozenset(),
    ),
    AgencyDecl(
        agency_id="BOE_SPEECHES",
        label="Bank of England — Speeches",
        # Provider attribution only — same schedule-only shape. BoE
        # speeches anchor at month precision (the sitemap doesn't
        # publish day-level dates); per-speech HTTP fan-out for
        # day precision is deferred to a future slice.
        providers=("boe-speeches",),
        indicators=frozenset(),
    ),
    AgencyDecl(
        agency_id="BOJ_SPEECHES",
        label="Bank of Japan — Policy Board speeches",
        # Provider attribution only — same schedule-only shape. The
        # connector filters BoJ archive rows to rate-setting roles
        # (Governor + Deputy Governor + Member of the Policy Board);
        # Executive Directors / Officers / Counsellors are skipped
        # to match the issue's "rate-setters only" scope.
        providers=("boj-speeches",),
        indicators=frozenset(),
    ),
    AgencyDecl(
        agency_id="BOK",
        label="Bank of Korea",
        # Provider attribution only — the P1 BOK slice projects each
        # MPB meeting as a schedule-only event (``actual=NULL``). The
        # value (new Base Rate) lives inside the per-meeting Monetary
        # Policy Decision press release; the value-side scrape is
        # deferred to P2 (mirrors the RBI / RBA schedule-only deferral
        # pattern). Once the Decision scrape lands,
        # ``("KR", "BOK_RATE")`` joins the whitelist.
        providers=("bok",),
        indicators=frozenset(),
    ),
    AgencyDecl(
        agency_id="IBGE",
        label="Instituto Brasileiro de Geografia e Estatística",
        # Provider attribution only — the P1 IBGE slice ships
        # schedule-only events (``actual=NULL``) for IPCA / IPCA-15 /
        # Industrial Production / Unemployment Rate / GDP. Wiring the
        # ``(BR, ...)`` pairs into the parity whitelist would trip the
        # parse_failed-on-missing-actual path on every release. Same
        # deferral as the ABS / MoSPI / KOSTAT slice — P2 adds the per-
        # release detail-page scrape to fill ``actual``.
        providers=("ibge",),
        indicators=frozenset(),
    ),
    AgencyDecl(
        agency_id="BCB",
        label="Banco Central do Brasil",
        # The BCB Copom history JSON exposes target-Selic-rate +
        # announcement date inline (RBA-style coverage), so the P1
        # slice ships value-bearing events for every Copom decision —
        # change OR hold OR extraordinary OR monocratic-presidential.
        # The parity whitelist lights up in P1 without a separate
        # value-side scrape (unlike the BoC / RBI / BOK schedule-only
        # deferral).
        providers=("bcb",),
        indicators=frozenset({("BR", "BCB_RATE")}),
    ),
    # NBS deliberately stays out of the parity registry in this
    # slice. The schedule connector anchors NBS rows on the *release*
    # month (April 13 release → reference_date=2026-04-01) while
    # TE buckets CN CPI/PPI/IP/Retail Sales rows on the *data* month
    # (March 2026 → reference_date=2026-03-01). Wiring NBS into
    # ``agency_registry`` without first aligning the reference-date
    # convention would generate false-positive parity alerts every
    # day — the comparator would see a TE row for the data month
    # with no agency vintage in the same bucket. Issue #49's
    # value-side fetcher fills ``actual`` on the existing release-
    # month rows; a follow-up slice realigns the reference anchor
    # and then re-introduces the NBS agency declaration.
)


def _build_provider_index() -> dict[str, AgencyDecl]:
    out: dict[str, AgencyDecl] = {}
    for agency in AGENCIES:
        for provider in agency.providers:
            if provider in out:
                raise RuntimeError(
                    f"agency_registry: provider {provider!r} declared by both "
                    f"{out[provider].agency_id} and {agency.agency_id}",
                )
            out[provider] = agency
    return out


def _build_indicator_index() -> dict[tuple[str, str], AgencyDecl]:
    out: dict[tuple[str, str], AgencyDecl] = {}
    for agency in AGENCIES:
        for key in agency.indicators:
            # Same (country, canonical) declared by multiple agencies
            # would mean ambiguous attribution — disallow at import.
            if key in out:
                raise RuntimeError(
                    f"agency_registry: indicator {key!r} declared by both "
                    f"{out[key].agency_id} and {agency.agency_id}",
                )
            out[key] = agency
    return out


_PROVIDER_TO_AGENCY: dict[str, AgencyDecl] = _build_provider_index()
_INDICATOR_TO_AGENCY: dict[tuple[str, str], AgencyDecl] = _build_indicator_index()


def provider_to_agency(provider_id: str) -> AgencyDecl | None:
    """Look up the agency that owns ``provider_id``, ``None`` if unknown."""
    return _PROVIDER_TO_AGENCY.get(provider_id)


def agency_for(country: str, canonical: str) -> AgencyDecl | None:
    """Look up the agency that publishes ``(country, canonical)``.

    Returns ``None`` for indicators not yet wired into the registry —
    callers should treat those as out-of-scope for the parity tripwire,
    not as an error.
    """
    return _INDICATOR_TO_AGENCY.get((country, canonical))


def known_indicators() -> frozenset[tuple[str, str]]:
    """Set of all ``(country, canonical)`` pairs the registry tracks."""
    return frozenset(_INDICATOR_TO_AGENCY.keys())


__all__ = [
    "AGENCIES",
    "AgencyDecl",
    "AlignmentSpec",
    "agency_for",
    "known_indicators",
    "provider_to_agency",
]
