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
    # NBS is intentionally absent from the registry: the calendar
    # connector ships schedule rows only — ``actual`` stays ``NULL``
    # because no value-side fetcher has landed yet. Including NBS
    # would make every TE NBS row report as "agency vintage missing
    # first-release actual" until value ingestion ships, drowning the
    # signal we care about. Add NBS back when ``cal_econ_event`` for
    # provider ``nbs`` carries actuals.
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
